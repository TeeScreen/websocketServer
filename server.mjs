import WebSocket, { WebSocketServer } from "ws";
import http from "http";

const PORT = process.env.PORT || 5000;

// Render expects an HTTP server on $PORT; we attach the WS to it so health checks pass.
const server = http.createServer((req, res) => {
    if (req.url === "/health") {
        res.writeHead(200, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ ok: true, senders: senders.size }));
        return;
    }
    res.writeHead(200, { "Content-Type": "text/plain" });
    res.end("signaling-server up");
});

const wss = new WebSocketServer({ server, path: "/ws" });

server.listen(PORT, () => {
    console.log(`\n[WS] Signaling server listening on :${PORT} (path /ws)\n`);
});

// senderId -> { socket, viewers: Set<WebSocket> }
const senders = new Map();
// Viewers waiting for a sender that hasn't registered yet: senderId -> Set<WebSocket>
const pendingViewers = new Map();

function sendJSON(socket, obj) {
    if (socket.readyState === WebSocket.OPEN) {
        socket.send(JSON.stringify(obj));
    }
}

function logDivider() {
    console.log("------------------------------------------------------------");
}

// Heartbeat to keep idle Render WebSockets alive (they get killed after ~60s)
function heartbeat() { this.isAlive = true; }
const hbInterval = setInterval(() => {
    wss.clients.forEach(ws => {
        if (ws.isAlive === false) return ws.terminate();
        ws.isAlive = false;
        try { ws.ping(); } catch {}
    });
}, 25000);
wss.on("close", () => clearInterval(hbInterval));

wss.on("connection", socket => {
    console.log("[WS] New client connected");
    socket.isAlive = true;
    socket.on("pong", heartbeat);

    socket.on("message", raw => {
        // Cap message size (SDP rarely above ~32KB; refuse anything wild)
        if (raw.length > 256 * 1024) {
            console.warn("[WS] Oversized message, dropping");
            return;
        }

        let msg;
        try { msg = JSON.parse(raw.toString()); }
        catch {
            console.log("[WS] Invalid JSON");
            return;
        }

        const type = msg.type;

        // ----------------------------------------------------
        // 1. Sender registers
        // ----------------------------------------------------
        if (type === "register-sender") {
            const { senderId } = msg;
            if (!senderId) return;

            // Replace any existing sender with same ID (e.g. Unity reconnect)
            const prev = senders.get(senderId);
            if (prev && prev.socket !== socket) {
                try { prev.socket.close(); } catch {}
            }

            senders.set(senderId, { socket, viewers: new Set() });
            socket.role = "sender";
            socket.senderId = senderId;

            logDivider();
            console.log(`[WS] Sender registered: ${senderId} (total ${senders.size})`);
            logDivider();

            sendJSON(socket, { type: "sender-registered", senderId });

            // If viewers were waiting for this sender, attach them now and ask sender for an offer
            const waiting = pendingViewers.get(senderId);
            if (waiting && waiting.size > 0) {
                const entry = senders.get(senderId);
                waiting.forEach(v => {
                    if (v.readyState === WebSocket.OPEN) entry.viewers.add(v);
                });
                pendingViewers.delete(senderId);
                console.log(`[WS] Re-attached ${entry.viewers.size} pending viewer(s) to ${senderId}`);
                sendJSON(socket, { type: "viewer-request" });
            }
            return;
        }

        // ----------------------------------------------------
        // 2. Viewer requests a sender
        // ----------------------------------------------------
        if (type === "request-sender") {
            const { senderId } = msg;
            if (!senderId) return;

            socket.role = "viewer";
            socket.senderId = senderId;

            const senderEntry = senders.get(senderId);

            logDivider();
            console.log(`[WS] Viewer requested ${senderId} | available=${!!senderEntry}`);
            logDivider();

            if (!senderEntry) {
                // Park viewer; notify so UI can show "sender offline"
                if (!pendingViewers.has(senderId)) pendingViewers.set(senderId, new Set());
                pendingViewers.get(senderId).add(socket);
                sendJSON(socket, { type: "sender-unavailable", senderId });
                return;
            }

            senderEntry.viewers.add(socket);
            console.log(`[WS] Total viewers for ${senderId}: ${senderEntry.viewers.size}`);

            sendJSON(senderEntry.socket, { type: "viewer-request" });
            return;
        }

        // ----------------------------------------------------
        // 3. Sender offer SDP -> viewers
        // ----------------------------------------------------
        if (type === "sender-peer-id") {
            const senderId = socket.senderId;
            if (!senderId) return;
            const entry = senders.get(senderId);
            if (!entry) return;

            console.log(`[WS] Sender offer for ${senderId} -> ${entry.viewers.size} viewer(s)`);
            entry.viewers.forEach(v => sendJSON(v, { type: "deliver-peer-id", peerId: msg.peerId }));
            return;
        }

        // ----------------------------------------------------
        // 4. Viewer answer SDP -> sender
        // ----------------------------------------------------
        if (type === "viewer-answer") {
            const senderId = socket.senderId;
            const entry = senders.get(senderId);
            if (!entry) return;
            console.log(`[WS] Viewer answer -> sender ${senderId}`);
            sendJSON(entry.socket, { type: "viewer-answer", answer: msg.answer });
            return;
        }

        // ----------------------------------------------------
        // 5. Viewer ICE -> sender
        // ----------------------------------------------------
        if (type === "viewer-ice") {
            const senderId = socket.senderId;
            const entry = senders.get(senderId);
            if (!entry) return;
            sendJSON(entry.socket, { type: "viewer-ice", candidate: msg.candidate });
            return;
        }

        // ----------------------------------------------------
        // 6. Sender ICE -> viewers
        // ----------------------------------------------------
        if (type === "sender-ice") {
            const senderId = socket.senderId;
            const entry = senders.get(senderId);
            if (!entry) return;
            entry.viewers.forEach(v => sendJSON(v, { type: "sender-ice", candidate: msg.candidate }));
            return;
        }
    });

    socket.on("close", () => {
        if (socket.role === "sender" && socket.senderId) {
            const senderId = socket.senderId;
            const entry = senders.get(senderId);
            console.log(`[WS] Sender disconnected: ${senderId}`);
            if (entry) {
                entry.viewers.forEach(v => sendJSON(v, { type: "sender-disconnected", senderId }));
            }
            senders.delete(senderId);
        }

        if (socket.role === "viewer" && socket.senderId) {
            const entry = senders.get(socket.senderId);
            if (entry) entry.viewers.delete(socket);
            const pending = pendingViewers.get(socket.senderId);
            if (pending) pending.delete(socket);
            console.log(`[WS] Viewer disconnected from ${socket.senderId}`);
        }
    });
});
