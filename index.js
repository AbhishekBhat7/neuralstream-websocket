require("dotenv").config();

const http = require("http");
const WebSocket = require("ws");
const { Pool } = require("pg");
const jwt = require("jsonwebtoken");

// ============================================
// DATABASE
// ============================================
let poolConfig;

if (process.env.DATABASE_URL) {
    console.log("Using DATABASE_URL connection");
    poolConfig = {
        connectionString: process.env.DATABASE_URL,
        ssl: { rejectUnauthorized: false },
        max: 10,
        idleTimeoutMillis: 30000,
        connectionTimeoutMillis: 5000,
    };
} else {
    poolConfig = {
        host: process.env.DB_HOST,
        port: parseInt(process.env.DB_PORT) || 5432,
        user: process.env.DB_USER,
        password: process.env.DB_PASSWORD,
        database: process.env.DB_NAME,
        ssl: false,
        max: 10,
        idleTimeoutMillis: 30000,
        connectionTimeoutMillis: 5000,
    };
}

const pool = new Pool(poolConfig);

pool.connect()
    .then(async (client) => {
        console.log("✓ Connected to PostgreSQL");
        const info = await client.query("SELECT current_database(), current_user");
        console.log("Database:", info.rows[0].current_database);
        console.log("User:", info.rows[0].current_user);
        const tables = await client.query(`
            SELECT tablename FROM pg_tables 
            WHERE schemaname = 'public' ORDER BY tablename
        `);
        console.log("Tables:", tables.rows.map(r => r.tablename).join(", "));
        client.release();
    })
    .catch(err => console.error("✗ DB connection failed:", err.message));

// ============================================
// PACKET PROTOCOL
// ============================================
//
// SINGLE PACKET (small data):
// Byte 0:      0x01 (type = SINGLE)
// Byte 1-4:    Uint32 sequence number
// Byte 5-12:   Float64 timestamp ms
// Byte 13-16:  Uint32 channel count
// Byte 17-20:  Uint32 sample rate Hz
// Byte 21-24:  Uint32 samples per channel
// Byte 25+:    Float32[] sample data
//              (Float32 = 4 bytes, faster than Float64 for streaming)
//
// CHUNKED PACKET (large data split by BLE MTU):
// Byte 0:      0x02=START 0x03=MIDDLE 0x04=END
// Byte 1-4:    Uint32 stream ID (same for all chunks of one packet)
// Byte 5-6:    Uint16 chunk index (0 based)
// Byte 7-8:    Uint16 total chunks
// Byte 9-12:   Uint32 total reassembled size
// Byte 13+:    payload bytes
//
// ============================================

const PTYPE = {
    SINGLE:       0x01,
    CHUNK_START:  0x02,
    CHUNK_MIDDLE: 0x03,
    CHUNK_END:    0x04
};

// Header sizes in bytes
const SINGLE_HDR  = 25;
const CHUNK_HDR   = 13;

// Limits
const MAX_PACKET_SIZE     = 50  * 1024 * 1024; // 50MB
const MAX_VIEWERS         = 50;                 // max viewers per session
const CHUNK_TIMEOUT_MS    = 8000;               // drop incomplete chunks after 8s
const VIEWER_BUFFER_HIGH  = 16 * 1024 * 1024;  // 16MB - pause host if viewer is slow
const HEARTBEAT_MS        = 20000;              // ping every 20s

// ============================================
// FAST BINARY PARSER
// Reads header without copying buffer
// ============================================
function readSingleHeader(buf) {
    // Need at least SINGLE_HDR bytes
    if (buf.length < SINGLE_HDR) return null;

    try {
        // Use DataView directly on buffer - no copy
        const dv = new DataView(
            buf.buffer,
            buf.byteOffset,
            buf.byteLength
        );
        return {
            type:            dv.getUint8(0),
            sequence:        dv.getUint32(1,  true),
            timestamp:       dv.getFloat64(5, true),
            channelCount:    dv.getUint32(13, true),
            sampleRate:      dv.getUint32(17, true),
            samplesPerCh:    dv.getUint32(21, true),
            // payload starts at byte 25
            payload:         buf.slice(SINGLE_HDR)
        };
    } catch {
        return null;
    }
}

function readChunkHeader(buf) {
    if (buf.length < CHUNK_HDR) return null;

    try {
        const dv = new DataView(
            buf.buffer,
            buf.byteOffset,
            buf.byteLength
        );
        return {
            type:        dv.getUint8(0),
            streamId:    dv.getUint32(1,  true),
            chunkIndex:  dv.getUint16(5,  true),
            totalChunks: dv.getUint16(7,  true),
            totalSize:   dv.getUint32(9,  true),
            payload:     buf.slice(CHUNK_HDR)
        };
    } catch {
        return null;
    }
}

// ============================================
// CHUNK REASSEMBLER
// Per session - handles multiple concurrent streams
// ============================================
class ChunkReassembler {
    constructor(sessionId) {
        this.sessionId  = sessionId;
        this.streams    = new Map();
        // streamId -> {
        //   chunks:      Map<index, Buffer>
        //   totalChunks: number
        //   totalSize:   number
        //   receivedAt:  number (timestamp)
        //   timer:       timeout handle
        // }

        this._cleanup = setInterval(
            () => this._sweepStale(),
            CHUNK_TIMEOUT_MS
        );
    }

    // Returns complete Buffer when all chunks received, else null
    push(buf) {
        const h = readChunkHeader(buf);
        if (!h) {
            console.warn(`[Reassembler] Bad chunk header session=${this.sessionId}`);
            return null;
        }

        const { streamId, chunkIndex, totalChunks, totalSize, payload } = h;

        // Reject oversized packets
        if (totalSize > MAX_PACKET_SIZE) {
            console.error(`[Reassembler] Packet too large ${totalSize} bytes`);
            return null;
        }

        // Init stream entry
        if (!this.streams.has(streamId)) {
            this.streams.set(streamId, {
                chunks:      new Map(),
                totalChunks,
                totalSize,
                receivedAt:  Date.now(),
                // Auto-expire this stream if not completed in time
                timer: setTimeout(() => {
                    if (this.streams.has(streamId)) {
                        const s = this.streams.get(streamId);
                        console.warn(
                            `[Reassembler] Timeout streamId=${streamId} ` +
                            `got=${s.chunks.size}/${totalChunks}`
                        );
                        this.streams.delete(streamId);
                    }
                }, CHUNK_TIMEOUT_MS)
            });
        }

        const stream = this.streams.get(streamId);

        // Duplicate chunk guard
        if (stream.chunks.has(chunkIndex)) {
            console.warn(`[Reassembler] Duplicate chunk ${chunkIndex} streamId=${streamId}`);
            return null;
        }

        stream.chunks.set(chunkIndex, payload);

        // All chunks received?
        if (stream.chunks.size !== totalChunks) {
            return null; // Still waiting
        }

        // All chunks received - reassemble in ORDER
        clearTimeout(stream.timer);
        this.streams.delete(streamId);

        const parts = [];
        for (let i = 0; i < totalChunks; i++) {
            const chunk = stream.chunks.get(i);
            if (!chunk) {
                // Gap detected - should not happen but guard anyway
                console.error(`[Reassembler] Gap at chunk ${i} streamId=${streamId}`);
                return null;
            }
            parts.push(chunk);
        }

        const complete = Buffer.concat(parts);

        console.log(
            `[Reassembler] Reassembled streamId=${streamId} ` +
            `chunks=${totalChunks} size=${complete.length}`
        );

        return complete;
    }

    _sweepStale() {
        const now = Date.now();
        this.streams.forEach((stream, streamId) => {
            if (now - stream.receivedAt > CHUNK_TIMEOUT_MS) {
                clearTimeout(stream.timer);
                this.streams.delete(streamId);
                console.warn(`[Reassembler] Swept stale streamId=${streamId}`);
            }
        });
    }

    destroy() {
        clearInterval(this._cleanup);
        this.streams.forEach(s => clearTimeout(s.timer));
        this.streams.clear();
    }
}

// ============================================
// GLOBAL STATS
// ============================================
const stats = {
    packetsSent:       0,
    bytesForwarded:    0,
    packetsDropped:    0,
    chunksReceived:    0,
    packetsReassembled:0,
    viewerSlowDrops:   0,

    record(bytes)  { this.packetsSent++;        this.bytesForwarded += bytes; },
    drop()         { this.packetsDropped++;                                   },
    chunk()        { this.chunksReceived++;                                   },
    reassembled()  { this.packetsReassembled++;                               },
    slowDrop()     { this.viewerSlowDrops++;                                  },

    snapshot() {
        return {
            packetsSent:        this.packetsSent,
            bytesForwarded:     this.bytesForwarded,
            mbForwarded:        (this.bytesForwarded / 1048576).toFixed(2),
            packetsDropped:     this.packetsDropped,
            chunksReceived:     this.chunksReceived,
            packetsReassembled: this.packetsReassembled,
            viewerSlowDrops:    this.viewerSlowDrops
        };
    }
};

// ============================================
// SESSION STORE
// ============================================
const sessions = new Map();
// sessionId (string) -> {
//   host:        ws | null
//   viewers:     Map<deviceId, ws>
//   reassembler: ChunkReassembler
//   stats: { packetsIn, bytesIn, packetsOut, chunksIn, reassembled }
// }

function getOrCreateSession(sessionId) {
    if (!sessions.has(sessionId)) {
        sessions.set(sessionId, {
            host:        null,
            viewers:     new Map(),
            reassembler: new ChunkReassembler(sessionId),
            stats: {
                packetsIn:   0,
                bytesIn:     0,
                packetsOut:  0,
                chunksIn:    0,
                reassembled: 0,
                startedAt:   new Date().toISOString()
            }
        });
    }
    return sessions.get(sessionId);
}

function destroySession(sessionId) {
    const s = sessions.get(sessionId);
    if (s) {
        s.reassembler.destroy();
        sessions.delete(sessionId);
        console.log(`[Session] Destroyed session=${sessionId}`);
    }
}

// ============================================
// BROADCAST - with backpressure check
// ============================================
function broadcastToViewers(sessionId, buf) {
    const session = sessions.get(sessionId);
    if (!session || session.viewers.size === 0) {
        stats.drop();
        return;
    }

    let sent = 0;
    let dropped = 0;

    session.viewers.forEach((viewer, deviceId) => {
        if (viewer.readyState !== WebSocket.OPEN) {
            dropped++;
            return;
        }

        // Backpressure check
        // If viewer socket buffer is too full, skip this packet
        // This prevents memory overflow when viewer is slow
        if (viewer.bufferedAmount > VIEWER_BUFFER_HIGH) {
            console.warn(
                `[Broadcast] Slow viewer deviceId=${deviceId} ` +
                `buffered=${viewer.bufferedAmount} bytes - dropping packet`
            );
            stats.slowDrop();
            dropped++;
            return;
        }

        try {
            viewer.send(buf);
            sent++;
        } catch (err) {
            console.error(`[Broadcast] Send error deviceId=${deviceId}:`, err.message);
            dropped++;
        }
    });

    if (sent > 0) {
        session.stats.packetsOut += sent;
        stats.record(buf.length * sent);
    }

    if (dropped > 0) {
        stats.drop();
    }
}

// ============================================
// NOTIFY HELPERS
// ============================================
function sendToHost(sessionId, obj) {
    const s = sessions.get(sessionId);
    if (s?.host?.readyState === WebSocket.OPEN) {
        s.host.send(JSON.stringify(obj));
    }
}

function sendToAllViewers(sessionId, obj) {
    const s = sessions.get(sessionId);
    if (!s) return;
    const msg = JSON.stringify(obj);
    s.viewers.forEach(v => {
        if (v.readyState === WebSocket.OPEN) v.send(msg);
    });
}

// ============================================
// HANDLE BINARY FROM HOST
// ============================================
function handleBinary(ws, buf) {
    const session = sessions.get(ws.sessionId);
    if (!session) return;

    // Track host stats
    ws._pktIn  = (ws._pktIn  || 0) + 1;
    ws._bytesIn= (ws._bytesIn|| 0) + buf.length;

    session.stats.packetsIn++;
    session.stats.bytesIn += buf.length;

    const ptype = buf[0];

    // ----------------------------------------
    // SINGLE - small packet, direct broadcast
    // ----------------------------------------
    if (ptype === PTYPE.SINGLE) {
        if (buf.length < SINGLE_HDR) {
            console.warn(`[Binary] Single packet too small: ${buf.length}`);
            stats.drop();
            return;
        }
        broadcastToViewers(ws.sessionId, buf);
        return;
    }

    // ----------------------------------------
    // CHUNKED - large BLE packet, reassemble
    // ----------------------------------------
    if (
        ptype === PTYPE.CHUNK_START  ||
        ptype === PTYPE.CHUNK_MIDDLE ||
        ptype === PTYPE.CHUNK_END
    ) {
        session.stats.chunksIn++;
        stats.chunk();

        const complete = session.reassembler.push(buf);
        if (complete) {
            session.stats.reassembled++;
            stats.reassembled();
            broadcastToViewers(ws.sessionId, complete);
        }
        return;
    }

    // Unknown type - broadcast raw (best effort)
    console.warn(`[Binary] Unknown ptype=0x${ptype.toString(16)} size=${buf.length}`);
    broadcastToViewers(ws.sessionId, buf);
}

// ============================================
// AUTH
// ============================================
function verifyToken(token) {
    try {
        return jwt.verify(token, process.env.JWT_SECRET);
    } catch (err) {
        console.error("[Auth] Token verify failed:", err.message);
        return null;
    }
}

// ============================================
// DATABASE
// ============================================
async function validateHost(sessionId, userId) {
    try {
        const r = await pool.query(
            `SELECT session_id FROM live_sessions
             WHERE session_id = $1
             AND   host_user_id = $2
             AND   status = 'ACTIVE'`,
            [sessionId, userId]
        );
        return r.rows.length > 0;
    } catch (err) {
        console.error("[DB] validateHost:", err.message);
        return false;
    }
}

async function dbJoinSession(sessionId, userId, deviceId) {
    await pool.query(
        `INSERT INTO session_viewers
            (session_id, viewer_user_id, device_id, status, joined_at)
         VALUES ($1,$2,$3,'CONNECTED',NOW())
         ON CONFLICT (session_id, device_id)
         DO UPDATE SET status='CONNECTED', joined_at=NOW(), left_at=NULL`,
        [sessionId, userId, deviceId]
    );
}

async function dbLeaveSession(sessionId, userId, deviceId) {
    await pool.query(
        `UPDATE session_viewers
         SET status='DISCONNECTED', left_at=NOW()
         WHERE session_id=$1 AND viewer_user_id=$2 AND device_id=$3`,
        [sessionId, userId, deviceId]
    );
}

async function dbEndSession(sessionId, userId) {
    await pool.query(
        `UPDATE live_sessions
         SET status='ENDED', ended_at=NOW()
         WHERE session_id=$1 AND host_user_id=$2`,
        [sessionId, userId]
    );
}

// ============================================
// HTTP SERVER
// ============================================
const httpServer = http.createServer(async (req, res) => {
    const url = req.url;

    if (url === "/health") {
        res.writeHead(200, { "Content-Type": "application/json" });
        res.end(JSON.stringify({
            status:         "ok",
            uptime:         process.uptime(),
            connections:    wss?.clients.size ?? 0,
            activeSessions: sessions.size,
            stats:          stats.snapshot(),
            timestamp:      new Date().toISOString()
        }));
        return;
    }

    if (url === "/sessions") {
        const out = [];
        sessions.forEach((s, id) => {
            out.push({
                sessionId:     id,
                hasHost:       s.host !== null,
                viewerCount:   s.viewers.size,
                viewers:       Array.from(s.viewers.keys()),
                pendingChunks: s.reassembler.streams.size,
                stats:         s.stats
            });
        });
        res.writeHead(200, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ sessions: out, global: stats.snapshot() }, null, 2));
        return;
    }

    if (url === "/db-check") {
        try {
            const t = await pool.query(
                `SELECT tablename FROM pg_tables WHERE schemaname='public'`
            );
            const ls = await pool.query(
                `SELECT session_id, host_user_id, status FROM live_sessions LIMIT 5`
            );
            res.writeHead(200, { "Content-Type": "application/json" });
            res.end(JSON.stringify({
                tables: t.rows.map(r => r.tablename),
                liveSessions: ls.rows
            }, null, 2));
        } catch (err) {
            res.writeHead(500, { "Content-Type": "application/json" });
            res.end(JSON.stringify({ error: err.message }));
        }
        return;
    }

    res.writeHead(200, { "Content-Type": "text/plain" });
    res.end("NeuralStream WebSocket Server");
});

// ============================================
// WEBSOCKET SERVER
// ============================================
const wss = new WebSocket.Server({
    server:           httpServer,
    path:             "/ws",
    maxPayload:       MAX_PACKET_SIZE,
    perMessageDeflate:false          // OFF - compression hurts real-time speed
});

console.log("WebSocket Server Initialized");

// ============================================
// HEARTBEAT - detect dead connections fast
// ============================================
const heartbeatTimer = setInterval(() => {
    wss.clients.forEach(ws => {
        if (ws.isAlive === false) {
            console.log(`[Heartbeat] Terminating dead client ${ws._id}`);
            return ws.terminate();
        }
        ws.isAlive = false;
        ws.ping();
    });
}, HEARTBEAT_MS);

// ============================================
// CONNECTION HANDLER
// ============================================
wss.on("connection", (ws, req) => {

    // Connection metadata
    ws._id       = crypto.randomUUID();
    ws._ip       = req.headers["x-forwarded-for"] || req.socket.remoteAddress || "?";
    ws.isAlive   = true;
    ws.isAuthenticated = false;
    ws.user      = null;
    ws.role      = null;    // "host" | "viewer" | null
    ws.sessionId = null;
    ws.deviceId  = null;
    ws._pktIn    = 0;
    ws._bytesIn  = 0;

    console.log(`[WS] Connected id=${ws._id} ip=${ws._ip} total=${wss.clients.size}`);

    ws.on("pong", () => { ws.isAlive = true; });
    ws.on("error", err => console.error(`[WS] Error id=${ws._id}:`, err.message));

    // Must authenticate within 15s
    const authTimer = setTimeout(() => {
        if (!ws.isAuthenticated) {
            console.log(`[WS] Auth timeout id=${ws._id}`);
            ws.close(1008, "Authentication timeout");
        }
    }, 15000);

    // ------------------------------------------
    // MESSAGE HANDLER
    // ------------------------------------------
    ws.on("message", async (raw, isBinary) => {
        ws.isAlive = true;

        try {

            // ======================================
            // BINARY PATH - hot path, minimal work
            // ======================================
            if (isBinary) {
                if (!ws.isAuthenticated || ws.role !== "host" || !ws.sessionId) {
                    console.warn(`[Binary] Rejected from id=${ws._id} role=${ws.role}`);
                    return;
                }
                handleBinary(ws, raw);
                return;
            }

            // ======================================
            // JSON PATH
            // ======================================
            let msg;
            try {
                msg = JSON.parse(raw.toString());
            } catch {
                ws.send(JSON.stringify({ type: "error", message: "Invalid JSON" }));
                return;
            }

            const { type } = msg;

            // ------------------------------------------
            // AUTH
            // ------------------------------------------
            if (type === "auth") {
                clearTimeout(authTimer);

                if (ws.isAuthenticated) {
                    ws.send(JSON.stringify({ type: "error", message: "Already authenticated" }));
                    return;
                }
                if (!msg.token) {
                    ws.send(JSON.stringify({ type: "error", message: "Token required" }));
                    return;
                }

                const decoded = verifyToken(msg.token);
                if (!decoded) {
                    ws.send(JSON.stringify({ type: "auth_failed", message: "Invalid token" }));
                    ws.close(1008, "Authentication failed");
                    return;
                }

                ws.isAuthenticated = true;
                ws.user = decoded;

                ws.send(JSON.stringify({
                    type:   "auth_success",
                    userId: decoded.userId
                }));

                console.log(`[Auth] Success userId=${decoded.userId}`);
                return;
            }

            // Auth required for all below
            if (!ws.isAuthenticated) {
                ws.send(JSON.stringify({ type: "error", message: "Authenticate first" }));
                return;
            }

            // ------------------------------------------
            // HOST JOIN
            // ------------------------------------------
            if (type === "host") {
                if (!msg.sessionId) {
                    ws.send(JSON.stringify({ type: "error", message: "sessionId required" }));
                    return;
                }

                const valid = await validateHost(msg.sessionId, ws.user.userId);
                if (!valid) {
                    ws.send(JSON.stringify({ type: "error", message: "Unauthorized host" }));
                    ws.close(1008, "Unauthorized");
                    return;
                }

                ws.role      = "host";
                ws.sessionId = String(msg.sessionId);

                const session    = getOrCreateSession(ws.sessionId);
                session.host     = ws;

                ws.send(JSON.stringify({
                    type:      "host_joined",
                    sessionId: ws.sessionId
                }));

                console.log(`[Host] Joined userId=${ws.user.userId} session=${ws.sessionId}`);
                return;
            }

            // ------------------------------------------
            // VIEWER JOIN
            // ------------------------------------------
            if (type === "viewer") {
                if (!msg.sessionId || !msg.deviceId) {
                    ws.send(JSON.stringify({
                        type:    "error",
                        message: "sessionId and deviceId required"
                    }));
                    return;
                }

                const session = getOrCreateSession(String(msg.sessionId));

                // Viewer limit guard
                if (session.viewers.size >= MAX_VIEWERS) {
                    ws.send(JSON.stringify({
                        type:    "error",
                        message: "Session is full"
                    }));
                    return;
                }

                try {
                    await dbJoinSession(msg.sessionId, ws.user.userId, msg.deviceId);
                } catch (err) {
                    ws.send(JSON.stringify({ type: "error", message: err.message }));
                    return;
                }

                ws.role      = "viewer";
                ws.sessionId = String(msg.sessionId);
                ws.deviceId  = msg.deviceId;

                session.viewers.set(ws.deviceId, ws);

                // Tell viewer they joined
                ws.send(JSON.stringify({
                    type:      "viewer_joined",
                    sessionId: ws.sessionId,
                    deviceId:  ws.deviceId
                }));

                // Tell host a viewer joined
                sendToHost(ws.sessionId, {
                    type:        "viewer_joined_session",
                    sessionId:   ws.sessionId,
                    deviceId:    ws.deviceId,
                    userId:      ws.user.userId,
                    viewerCount: session.viewers.size
                });

                console.log(`[Viewer] Joined userId=${ws.user.userId} device=${ws.deviceId} session=${ws.sessionId}`);
                return;
            }

            // ------------------------------------------
            // LEAVE - viewer or host exits cleanly
            // ------------------------------------------
            if (type === "leave") {
                await handleLeave(ws);
                return;
            }

            // ------------------------------------------
            // STREAM STATS - host queries session info
            // ------------------------------------------
            if (type === "stream_stats") {
                if (ws.role !== "host") {
                    ws.send(JSON.stringify({ type: "error", message: "Host only" }));
                    return;
                }

                const s = sessions.get(ws.sessionId);
                ws.send(JSON.stringify({
                    type:        "stream_stats",
                    sessionId:   ws.sessionId,
                    viewerCount: s?.viewers.size ?? 0,
                    viewers:     s ? Array.from(s.viewers.keys()) : [],
                    hostStats: {
                        packetsIn: ws._pktIn,
                        bytesIn:   ws._bytesIn,
                        mbIn:      (ws._bytesIn / 1048576).toFixed(2)
                    },
                    sessionStats: s?.stats ?? null
                }));
                return;
            }

            // ------------------------------------------
            // PING
            // ------------------------------------------
            if (type === "ping") {
                ws.send(JSON.stringify({ type: "pong", ts: Date.now() }));
                return;
            }

            ws.send(JSON.stringify({ type: "error", message: `Unknown type: ${type}` }));

        } catch (err) {
            console.error("[WS] Message handler error:", err.message, err.stack);
            try {
                ws.send(JSON.stringify({ type: "error", message: "Internal error" }));
            } catch (_) {}
        }
    });

    // ------------------------------------------
    // DISCONNECT
    // ------------------------------------------
    ws.on("close", async (code) => {
        clearTimeout(authTimer);
        console.log(`[WS] Closed id=${ws._id} code=${code} role=${ws.role}`);

        try {
            await handleLeave(ws, true /* silent */);
        } catch (err) {
            console.error("[WS] Close cleanup error:", err.message);
        }
    });
});

// ============================================
// LEAVE HANDLER (shared by "leave" msg + close)
// ============================================
async function handleLeave(ws, silent = false) {
    if (!ws.sessionId) return;

    const sessionId = ws.sessionId;
    const role      = ws.role;

    // Clear so we dont double-process
    ws.role      = null;
    ws.sessionId = null;

    if (role === "viewer") {
        try {
            await dbLeaveSession(sessionId, ws.user.userId, ws.deviceId);
        } catch (_) {}

        const s = sessions.get(sessionId);
        if (s) {
            s.viewers.delete(ws.deviceId);

            // Tell host viewer left
            sendToHost(sessionId, {
                type:        "viewer_left",
                sessionId,
                deviceId:    ws.deviceId,
                userId:      ws.user?.userId,
                viewerCount: s.viewers.size
            });

            // Clean up if empty
            if (!s.host && s.viewers.size === 0) {
                destroySession(sessionId);
            }
        }

        if (!silent) {
            try {
                ws.send(JSON.stringify({ type: "leave_success", sessionId }));
            } catch (_) {}
        }

        console.log(`[Leave] Viewer userId=${ws.user?.userId} deviceId=${ws.deviceId} session=${sessionId}`);

    } else if (role === "host") {
        try {
            await dbEndSession(sessionId, ws.user.userId);
        } catch (_) {}

        // Notify all viewers session ended
        sendToAllViewers(sessionId, {
            type:    "session_ended",
            sessionId,
            message: "Host ended the session"
        });

        const s = sessions.get(sessionId);
        if (s) {
            s.host = null;
            if (s.viewers.size === 0) {
                destroySession(sessionId);
            }
        }

        if (!silent) {
            try {
                ws.send(JSON.stringify({ type: "leave_success", sessionId }));
            } catch (_) {}
        }

        console.log(`[Leave] Host userId=${ws.user?.userId} session=${sessionId}`);
    }
}

// ============================================
// WS SERVER EVENTS
// ============================================
wss.on("error", err => console.error("[WSS] Server error:", err.message));

wss.on("close", () => {
    clearInterval(heartbeatTimer);
    console.log("[WSS] Server closed");
});

// ============================================
// START
// ============================================
const PORT = process.env.PORT || 3000;

httpServer.listen(PORT, "0.0.0.0", () => {
    console.log(`✓ NeuralStream WebSocket Server`);
    console.log(`  Port:     ${PORT}`);
    console.log(`  Health:   /health`);
    console.log(`  Sessions: /sessions`);
    console.log(`  WS:       /ws`);
});

// ============================================
// GRACEFUL SHUTDOWN
// ============================================
function shutdown(signal) {
    console.log(`[Shutdown] ${signal} received`);
    clearInterval(heartbeatTimer);

    wss.clients.forEach(ws => {
        try { ws.close(1001, "Server shutting down"); } catch (_) {}
    });

    sessions.forEach((_, id) => destroySession(id));

    wss.close(() => {
        pool.end(() => {
            httpServer.close(() => {
                console.log("[Shutdown] Clean exit");
                process.exit(0);
            });
        });
    });

    // Force exit after 5s if clean shutdown hangs
    setTimeout(() => process.exit(1), 5000);
}

process.on("SIGTERM", () => shutdown("SIGTERM"));
process.on("SIGINT",  () => shutdown("SIGINT"));