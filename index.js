// require("dotenv").config();

// const http = require("http");
// const WebSocket = require("ws");
// const { Pool } = require("pg");
// const jwt = require("jsonwebtoken");

// // ============================================
// // DATABASE
// // ============================================
// const pool = new Pool({
//     host: process.env.DB_HOST,
//     port: parseInt(process.env.DB_PORT) || 5432,
//     user: process.env.DB_USER,
//     password: process.env.DB_PASSWORD,
//     database: process.env.DB_NAME,
//     ssl: process.env.DB_SSL === "true" ? { rejectUnauthorized: false } : false,
//     max: 10,
//     idleTimeoutMillis: 30000,
//     connectionTimeoutMillis: 5000,
// });

// pool.connect()
//     .then(client => {
//         console.log("✓ Connected to PostgreSQL");
//         client.release();
//     })
//     .catch(err => {
//         console.error("✗ Database connection failed:", err.message);
//     });

// // ============================================
// // SESSION MANAGER
// // ============================================
// const sessions = new Map();
// // sessions = {
// //   sessionId: { host: ws, viewers: Set<ws> }
// // }

// function getSession(sessionId) {
//     if (!sessions.has(sessionId)) {
//         sessions.set(sessionId, { host: null, viewers: new Set() });
//     }
//     return sessions.get(sessionId);
// }

// function setHost(sessionId, ws) {
//     const session = getSession(sessionId);
//     session.host = ws;
// }

// function addViewer(sessionId, ws) {
//     const session = getSession(sessionId);
//     session.viewers.add(ws);
// }

// function removeClient(ws) {
//     sessions.forEach((session, sessionId) => {
//         if (session.host === ws) {
//             session.host = null;
//         }
//         session.viewers.delete(ws);

//         // Cleanup empty sessions
//         if (!session.host && session.viewers.size === 0) {
//             sessions.delete(sessionId);
//         }
//     });
// }

// function broadcastBinary(sessionId, data) {
//     const session = sessions.get(sessionId);
//     if (!session) return;

//     session.viewers.forEach((viewer) => {
//         if (viewer.readyState === WebSocket.OPEN) {
//             viewer.send(data);
//         }
//     });
// }

// // ============================================
// // AUTH
// // ============================================
// function verifyToken(token) {
//     try {
//         return jwt.verify(token, process.env.JWT_SECRET);
//     } catch (err) {
//         console.error("Token verify failed:", err.message);
//         return null;
//     }
// }

// // ============================================
// // SESSION VALIDATION
// // ============================================
// async function validateHost(sessionId, userId) {
//     try {
//         const result = await pool.query(
//             "SELECT id FROM sessions WHERE id = $1 AND host_user_id = $2",
//             [sessionId, userId]
//         );
//         return result.rows.length > 0;
//     } catch (err) {
//         console.error("validateHost error:", err.message);
//         return false;
//     }
// }

// async function joinSession(sessionId, userId, deviceId) {
//     try {
//         await pool.query(
//             `INSERT INTO session_viewers (session_id, user_id, device_id, joined_at)
//              VALUES ($1, $2, $3, NOW())
//              ON CONFLICT (session_id, device_id) DO UPDATE SET joined_at = NOW()`,
//             [sessionId, userId, deviceId]
//         );
//     } catch (err) {
//         console.error("joinSession error:", err.message);
//         throw new Error("Could not join session");
//     }
// }

// async function leaveSession(sessionId, userId, deviceId) {
//     try {
//         await pool.query(
//             `UPDATE session_viewers 
//              SET left_at = NOW() 
//              WHERE session_id = $1 AND user_id = $2 AND device_id = $3`,
//             [sessionId, userId, deviceId]
//         );
//     } catch (err) {
//         console.error("leaveSession error:", err.message);
//     }
// }

// async function endSession(sessionId, userId) {
//     try {
//         await pool.query(
//             `UPDATE sessions 
//              SET ended_at = NOW(), status = 'ended'
//              WHERE id = $1 AND host_user_id = $2`,
//             [sessionId, userId]
//         );
//     } catch (err) {
//         console.error("endSession error:", err.message);
//     }
// }

// // ============================================
// // HTTP SERVER
// // ============================================
// const httpServer = http.createServer((req, res) => {
//     // Health check endpoint
//     if (req.url === "/health") {
//         res.writeHead(200, { "Content-Type": "application/json" });
//         res.end(JSON.stringify({
//             status: "ok",
//             connections: wss ? wss.clients.size : 0,
//             sessions: sessions.size,
//             timestamp: new Date().toISOString()
//         }));
//         return;
//     }

//     res.writeHead(200, { "Content-Type": "text/plain" });
//     res.end("NeuralStream WebSocket Server");
// });

// // ============================================
// // WEBSOCKET SERVER
// // ============================================
// const wss = new WebSocket.Server({
//     server: httpServer,
//     path: "/ws",
//     maxPayload: 5 * 1024 * 1024,
//     perMessageDeflate: false
// });

// console.log("WebSocket Server Initialized");

// // ============================================
// // HEARTBEAT
// // ============================================
// const heartbeat = setInterval(() => {
//     console.log(`Heartbeat - Active clients: ${wss.clients.size}`);

//     wss.clients.forEach((ws) => {
//         if (ws.isAlive === false) {
//             console.log(`Terminating stale: ${ws._ip}`);
//             return ws.terminate();
//         }
//         ws.isAlive = false;
//         ws.ping();
//     });
// }, 30000);

// // ============================================
// // CONNECTION HANDLER
// // ============================================
// wss.on("connection", (ws, request) => {

//     const ip =
//         request.headers["x-forwarded-for"] ||
//         request.socket.remoteAddress ||
//         "unknown";

//     ws._ip = ip;
//     ws.isAlive = true;
//     ws.isAuthenticated = false;
//     ws.user = null;
//     ws.role = null;
//     ws.sessionId = null;
//     ws.deviceId = null;
//     ws.id = crypto.randomUUID();

//     console.log(`Connected: ${ip} | ID: ${ws.id}`);
//     console.log(`Total clients: ${wss.clients.size}`);

//     // Pong handler
//     ws.on("pong", () => {
//         ws.isAlive = true;
//     });

//     // Auth timeout - 15 seconds to authenticate
//     const authTimeout = setTimeout(() => {
//         if (!ws.isAuthenticated) {
//             console.log(`Auth timeout: ${ws.id}`);
//             ws.close(1008, "Authentication timeout");
//         }
//     }, 15000);

//     // ----------------------------------------
//     // Message Handler
//     // ----------------------------------------
//     ws.on("message", async (data, isBinary) => {
//         ws.isAlive = true;

//         try {
//             // Binary - telemetry broadcast
//             if (isBinary) {
//                 if (!ws.isAuthenticated || ws.role !== "host" || !ws.sessionId) {
//                     return;
//                 }
//                 broadcastBinary(ws.sessionId, data);
//                 return;
//             }

//             // Parse JSON
//             let message;
//             try {
//                 message = JSON.parse(data.toString());
//             } catch {
//                 ws.send(JSON.stringify({ type: "error", message: "Invalid JSON" }));
//                 return;
//             }

//             console.log(`Message from ${ws.id}: ${message.type}`);

//             // ----------------------------------------
//             // AUTH
//             // ----------------------------------------
//             if (message.type === "auth") {
//                 clearTimeout(authTimeout);

//                 if (ws.isAuthenticated) {
//                     ws.send(JSON.stringify({ type: "error", message: "Already authenticated" }));
//                     return;
//                 }

//                 if (!message.token) {
//                     ws.send(JSON.stringify({ type: "error", message: "Token missing" }));
//                     return;
//                 }

//                 const decoded = verifyToken(message.token);

//                 if (!decoded) {
//                     ws.send(JSON.stringify({ type: "auth_failed", message: "Invalid token" }));
//                     ws.close(1008, "Authentication failed");
//                     return;
//                 }

//                 ws.isAuthenticated = true;
//                 ws.user = decoded;

//                 console.log(`Auth success: ${decoded.userId}`);

//                 ws.send(JSON.stringify({
//                     type: "auth_success",
//                     userId: decoded.userId
//                 }));
//                 return;
//             }

//             // Require auth for everything below
//             if (!ws.isAuthenticated) {
//                 ws.send(JSON.stringify({ type: "error", message: "Authenticate first" }));
//                 return;
//             }

//             // ----------------------------------------
//             // HOST JOIN
//             // ----------------------------------------
//             if (message.type === "host") {
//                 if (!message.sessionId) {
//                     ws.send(JSON.stringify({ type: "error", message: "sessionId required" }));
//                     return;
//                 }

//                 const isValid = await validateHost(message.sessionId, ws.user.userId);

//                 if (!isValid) {
//                     ws.send(JSON.stringify({ type: "error", message: "Unauthorized host" }));
//                     ws.close(1008, "Unauthorized");
//                     return;
//                 }

//                 ws.role = "host";
//                 ws.sessionId = message.sessionId;
//                 setHost(message.sessionId, ws);

//                 ws.send(JSON.stringify({
//                     type: "host_joined",
//                     sessionId: message.sessionId
//                 }));

//                 console.log(`[HOST] User: ${ws.user.userId} Session: ${message.sessionId}`);
//                 return;
//             }

//             // ----------------------------------------
//             // VIEWER JOIN
//             // ----------------------------------------
//             if (message.type === "viewer") {
//                 if (!message.sessionId || !message.deviceId) {
//                     ws.send(JSON.stringify({
//                         type: "error",
//                         message: "sessionId & deviceId required"
//                     }));
//                     return;
//                 }

//                 try {
//                     await joinSession(message.sessionId, ws.user.userId, message.deviceId);
//                 } catch (err) {
//                     ws.send(JSON.stringify({ type: "error", message: err.message }));
//                     return;
//                 }

//                 ws.role = "viewer";
//                 ws.sessionId = message.sessionId;
//                 ws.deviceId = message.deviceId;
//                 addViewer(message.sessionId, ws);

//                 ws.send(JSON.stringify({
//                     type: "viewer_joined",
//                     sessionId: message.sessionId
//                 }));

//                 console.log(`[VIEWER] User: ${ws.user.userId} Device: ${message.deviceId}`);
//                 return;
//             }

//             // Unknown type
//             ws.send(JSON.stringify({
//                 type: "error",
//                 message: `Unknown message type: ${message.type}`
//             }));

//         } catch (err) {
//             console.error("Message error:", err.message);
//             ws.send(JSON.stringify({ type: "error", message: "Internal Server Error" }));
//         }
//     });

//     // ----------------------------------------
//     // Disconnect
//     // ----------------------------------------
//     ws.on("close", async (code) => {
//         console.log(`Disconnected: ${ws.id} Code: ${code}`);
//         clearTimeout(authTimeout);

//         try {
//             if (ws.role === "viewer" && ws.sessionId && ws.deviceId) {
//                 await leaveSession(ws.sessionId, ws.user.userId, ws.deviceId);
//             }

//             if (ws.role === "host" && ws.sessionId) {
//                 try {
//                     await endSession(ws.sessionId, ws.user.userId);
//                 } catch (_) {}
//             }

//             removeClient(ws);
//         } catch (err) {
//             console.error("Cleanup error:", err.message);
//         }
//     });

//     ws.on("error", (err) => {
//         console.error(`Socket error [${ws.id}]:`, err.message);
//     });
// });

// // ============================================
// // SERVER EVENTS
// // ============================================
// wss.on("error", (err) => {
//     console.error("WebSocket Server Error:", err.message);
// });

// wss.on("close", () => {
//     clearInterval(heartbeat);
//     console.log("WebSocket Server Closed");
// });

// // ============================================
// // START
// // ============================================
// const PORT = process.env.PORT || 3000;

// httpServer.listen(PORT, "0.0.0.0", () => {
//     console.log(`✓ WebSocket Server running on port ${PORT}`);
//     console.log(`  Health: http://localhost:${PORT}/health`);
//     console.log(`  WS: ws://localhost:${PORT}/ws`);
// });

// // Graceful shutdown
// process.on("SIGTERM", () => {
//     console.log("Shutting down...");
//     clearInterval(heartbeat);
//     wss.clients.forEach((ws) => ws.close(1001, "Server shutting down"));
//     wss.close();
//     pool.end();
//     httpServer.close();
// });


require("dotenv").config();

const http = require("http");
const WebSocket = require("ws");
const { Pool } = require("pg");
const jwt = require("jsonwebtoken");

// ============================================
// DATABASE - Use DATABASE_URL for Railway
// Falls back to individual vars for local
// ============================================
let poolConfig;

if (process.env.DATABASE_URL) {
    // Railway / Neon - use connection string
    console.log("Using DATABASE_URL connection");
    poolConfig = {
        connectionString: process.env.DATABASE_URL,
        ssl: { rejectUnauthorized: false },
        max: 10,
        idleTimeoutMillis: 30000,
        connectionTimeoutMillis: 5000,
    };
} else {
    // Local development
    console.log("Using individual DB vars");
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

        // Show which database we connected to
        const result = await client.query("SELECT current_database(), current_user");
        console.log("  Database:", result.rows[0].current_database);
        console.log("  User:", result.rows[0].current_user);

        client.release();
    })
    .catch(err => {
        console.error("✗ Database connection failed:", err.message);
    });

// ============================================
// SESSION MANAGER
// ============================================
const sessions = new Map();

function getSession(sessionId) {
    if (!sessions.has(sessionId)) {
        sessions.set(sessionId, { host: null, viewers: new Set() });
    }
    return sessions.get(sessionId);
}

function setHost(sessionId, ws) {
    const session = getSession(sessionId);
    session.host = ws;
}

function addViewer(sessionId, ws) {
    const session = getSession(sessionId);
    session.viewers.add(ws);
}

function removeClient(ws) {
    sessions.forEach((session, sessionId) => {
        if (session.host === ws) {
            session.host = null;
        }
        session.viewers.delete(ws);

        if (!session.host && session.viewers.size === 0) {
            sessions.delete(sessionId);
        }
    });
}

function broadcastBinary(sessionId, data) {
    const session = sessions.get(sessionId);
    if (!session) return;

    session.viewers.forEach((viewer) => {
        if (viewer.readyState === WebSocket.OPEN) {
            viewer.send(data);
        }
    });
}

// ============================================
// AUTH
// ============================================
function verifyToken(token) {
    try {
        const decoded = jwt.verify(token, process.env.JWT_SECRET);
        console.log("Token verified for userId:", decoded.userId);
        return decoded;
    } catch (err) {
        console.error("Token verify failed:", err.message);
        return null;
    }
}

// ============================================
// SESSION VALIDATION - With detailed logging
// ============================================
async function validateHost(sessionId, userId) {
    try {
        console.log(`validateHost - sessionId: ${sessionId} userId: ${userId}`);

        // First check what sessions exist
        const allSessions = await pool.query(
            "SELECT id, host_user_id, status FROM sessions LIMIT 5"
        );
        console.log("Existing sessions:", JSON.stringify(allSessions.rows));

        // Now validate
        const result = await pool.query(
            "SELECT id FROM sessions WHERE id = $1 AND host_user_id = $2",
            [sessionId, userId]
        );

        console.log(`validateHost result: ${result.rows.length} rows found`);
        return result.rows.length > 0;

    } catch (err) {
        console.error("validateHost error:", err.message);
        console.error("validateHost stack:", err.stack);
        return false;
    }
}

async function joinSession(sessionId, userId, deviceId) {
    try {
        await pool.query(
            `INSERT INTO session_viewers (session_id, user_id, device_id, joined_at)
             VALUES ($1, $2, $3, NOW())
             ON CONFLICT (session_id, device_id) 
             DO UPDATE SET joined_at = NOW()`,
            [sessionId, userId, deviceId]
        );
    } catch (err) {
        console.error("joinSession error:", err.message);
        throw new Error("Could not join session");
    }
}

async function leaveSession(sessionId, userId, deviceId) {
    try {
        await pool.query(
            `UPDATE session_viewers 
             SET left_at = NOW() 
             WHERE session_id = $1 AND user_id = $2 AND device_id = $3`,
            [sessionId, userId, deviceId]
        );
    } catch (err) {
        console.error("leaveSession error:", err.message);
    }
}

async function endSession(sessionId, userId) {
    try {
        await pool.query(
            `UPDATE sessions 
             SET ended_at = NOW(), status = 'ended'
             WHERE id = $1 AND host_user_id = $2`,
            [sessionId, userId]
        );
    } catch (err) {
        console.error("endSession error:", err.message);
    }
}

// ============================================
// HTTP SERVER
// ============================================
const httpServer = http.createServer((req, res) => {
    if (req.url === "/health") {
        res.writeHead(200, { "Content-Type": "application/json" });
        res.end(JSON.stringify({
            status: "ok",
            connections: wss ? wss.clients.size : 0,
            sessions: sessions.size,
            timestamp: new Date().toISOString()
        }));
        return;
    }

    res.writeHead(200, { "Content-Type": "text/plain" });
    res.end("NeuralStream WebSocket Server");
});

// ============================================
// WEBSOCKET SERVER
// ============================================
const wss = new WebSocket.Server({
    server: httpServer,
    path: "/ws",
    maxPayload: 5 * 1024 * 1024,
    perMessageDeflate: false
});

console.log("WebSocket Server Initialized");

// ============================================
// HEARTBEAT
// ============================================
const heartbeat = setInterval(() => {
    console.log(`Heartbeat - Active clients: ${wss.clients.size}`);

    wss.clients.forEach((ws) => {
        if (ws.isAlive === false) {
            console.log(`Terminating stale: ${ws._ip}`);
            return ws.terminate();
        }
        ws.isAlive = false;
        ws.ping();
    });
}, 30000);

// ============================================
// CONNECTION HANDLER
// ============================================
wss.on("connection", (ws, request) => {

    const ip =
        request.headers["x-forwarded-for"] ||
        request.socket.remoteAddress ||
        "unknown";

    ws._ip = ip;
    ws.isAlive = true;
    ws.isAuthenticated = false;
    ws.user = null;
    ws.role = null;
    ws.sessionId = null;
    ws.deviceId = null;
    ws.id = crypto.randomUUID();

    console.log(`Connected: ${ip} | ID: ${ws.id}`);
    console.log(`Total clients: ${wss.clients.size}`);

    ws.on("pong", () => {
        ws.isAlive = true;
    });

    // Auth timeout
    const authTimeout = setTimeout(() => {
        if (!ws.isAuthenticated) {
            console.log(`Auth timeout: ${ws.id}`);
            ws.close(1008, "Authentication timeout");
        }
    }, 15000);

    // ----------------------------------------
    // Message Handler
    // ----------------------------------------
    ws.on("message", async (data, isBinary) => {
        ws.isAlive = true;

        try {
            // Binary telemetry
            if (isBinary) {
                if (!ws.isAuthenticated || ws.role !== "host" || !ws.sessionId) return;
                broadcastBinary(ws.sessionId, data);
                return;
            }

            // Parse JSON
            let message;
            try {
                message = JSON.parse(data.toString());
            } catch {
                ws.send(JSON.stringify({ type: "error", message: "Invalid JSON" }));
                return;
            }

            console.log(`[${ws.id}] Message type: ${message.type}`);

            // ----------------------------------------
            // AUTH
            // ----------------------------------------
            if (message.type === "auth") {
                clearTimeout(authTimeout);

                if (ws.isAuthenticated) {
                    ws.send(JSON.stringify({ type: "error", message: "Already authenticated" }));
                    return;
                }

                if (!message.token) {
                    ws.send(JSON.stringify({ type: "error", message: "Token missing" }));
                    return;
                }

                const decoded = verifyToken(message.token);

                if (!decoded) {
                    ws.send(JSON.stringify({ type: "auth_failed", message: "Invalid token" }));
                    ws.close(1008, "Authentication failed");
                    return;
                }

                ws.isAuthenticated = true;
                ws.user = decoded;

                console.log(`Auth success: userId=${decoded.userId}`);

                ws.send(JSON.stringify({
                    type: "auth_success",
                    userId: decoded.userId
                }));
                return;
            }

            // Require auth
            if (!ws.isAuthenticated) {
                ws.send(JSON.stringify({ type: "error", message: "Authenticate first" }));
                return;
            }

            // ----------------------------------------
            // HOST JOIN
            // ----------------------------------------
            if (message.type === "host") {
                console.log(`Host request - sessionId: ${message.sessionId} userId: ${ws.user.userId}`);

                if (!message.sessionId) {
                    ws.send(JSON.stringify({ type: "error", message: "sessionId required" }));
                    return;
                }

                const isValid = await validateHost(message.sessionId, ws.user.userId);
                console.log(`Host validation result: ${isValid}`);

                if (!isValid) {
                    ws.send(JSON.stringify({ type: "error", message: "Unauthorized host" }));
                    ws.close(1008, "Unauthorized");
                    return;
                }

                ws.role = "host";
                ws.sessionId = message.sessionId;
                setHost(message.sessionId, ws);

                ws.send(JSON.stringify({
                    type: "host_joined",
                    sessionId: message.sessionId
                }));

                console.log(`[HOST] userId=${ws.user.userId} sessionId=${message.sessionId}`);
                return;
            }

            // ----------------------------------------
            // VIEWER JOIN
            // ----------------------------------------
            if (message.type === "viewer") {
                console.log(`Viewer request - sessionId: ${message.sessionId}`);

                if (!message.sessionId || !message.deviceId) {
                    ws.send(JSON.stringify({
                        type: "error",
                        message: "sessionId & deviceId required"
                    }));
                    return;
                }

                try {
                    await joinSession(message.sessionId, ws.user.userId, message.deviceId);
                } catch (err) {
                    ws.send(JSON.stringify({ type: "error", message: err.message }));
                    return;
                }

                ws.role = "viewer";
                ws.sessionId = message.sessionId;
                ws.deviceId = message.deviceId;
                addViewer(message.sessionId, ws);

                ws.send(JSON.stringify({
                    type: "viewer_joined",
                    sessionId: message.sessionId
                }));

                console.log(`[VIEWER] userId=${ws.user.userId} deviceId=${message.deviceId}`);
                return;
            }

            // Unknown
            ws.send(JSON.stringify({
                type: "error",
                message: `Unknown message type: ${message.type}`
            }));

        } catch (err) {
            console.error("Message error:", err.message);
            console.error(err.stack);
            ws.send(JSON.stringify({ type: "error", message: "Internal Server Error" }));
        }
    });

    // ----------------------------------------
    // Disconnect Cleanup
    // ----------------------------------------
    ws.on("close", async (code) => {
        console.log(`Disconnected: ${ws.id} Code: ${code}`);
        clearTimeout(authTimeout);

        try {
            if (ws.role === "viewer" && ws.sessionId && ws.deviceId) {
                await leaveSession(ws.sessionId, ws.user.userId, ws.deviceId);
            }
            if (ws.role === "host" && ws.sessionId) {
                try {
                    await endSession(ws.sessionId, ws.user.userId);
                } catch (_) {}
            }
            removeClient(ws);
        } catch (err) {
            console.error("Cleanup error:", err.message);
        }
    });

    ws.on("error", (err) => {
        console.error(`Socket error [${ws.id}]:`, err.message);
    });
});

wss.on("error", (err) => {
    console.error("WebSocket Server Error:", err.message);
});

wss.on("close", () => {
    clearInterval(heartbeat);
    console.log("WebSocket Server Closed");
});

// ============================================
// START SERVER
// ============================================
const PORT = process.env.PORT || 3000;

httpServer.listen(PORT, "0.0.0.0", () => {
    console.log(`✓ WebSocket Server on port ${PORT}`);
    console.log(`  Health: http://localhost:${PORT}/health`);
    console.log(`  WS:     ws://localhost:${PORT}/ws`);
});

process.on("SIGTERM", () => {
    clearInterval(heartbeat);
    wss.clients.forEach((ws) => ws.close(1001, "Server shutting down"));
    wss.close();
    pool.end();
    httpServer.close();
});