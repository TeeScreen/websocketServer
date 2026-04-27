import WebSocket, { WebSocketServer } from "ws";

const PORT = process.env.PORT || 5000;
const wss = new WebSocketServer({ port: PORT });

console.log(`\n[WS] Signaling server running on port ${PORT}\n`);

// Active senders: senderId -> { socket, viewers: Set<WebSocket> }
const senders = new Map();

function sendJSON(socket, obj) {
    if (socket.readyState === WebSocket.OPEN) {
        socket.send(JSON.stringify(obj));
    }
}

function logDivider() {
    console.log("------------------------------------------------------------");
}

wss.on("connection", socket => {
    console.log("[WS] New client connected");

    socket.on("message", raw => {
        let msg;
        try {
            msg = JSON.parse(raw.toString());
        } catch {
            console.log("[WS] ❌ Invalid JSON:", raw.toString());
            return;
        }

        const type = msg.type;

        console.log(msg);

        // ----------------------------------------------------
        // 1. Sender registers
        // ----------------------------------------------------
        if (type === "register-sender") {
            const { senderId } = msg;
            if (!senderId) return;

            senders.set(senderId, {
                socket,
                viewers: new Set()
            });

            socket.role = "sender";
            socket.senderId = senderId;

            logDivider();
            console.log(`[WS] 📡 Sender Registered`);
            console.log(`     Sender ID: ${senderId}`);
            console.log(`     Total Senders: ${senders.size}`);
            logDivider();

            sendJSON(socket, {
                type: "sender-registered",
                senderId
            });

            return;
        }

        // ----------------------------------------------------
        // 2. Viewer requests a sender
        // ----------------------------------------------------
        if (type === "request-sender") {
            const { senderId } = msg;
            const senderEntry = senders.get(senderId);

            socket.role = "viewer";
            socket.senderId = senderId;

            logDivider();
            console.log(`[WS] 👀 Viewer Requested Sender`);
            console.log(`     Requested Sender: ${senderId}`);
            console.log(`     Sender Available: ${!!senderEntry}`);
            logDivider();

            if (!senderEntry) {
                sendJSON(socket, {
                    type: "sender-unavailable",
                    senderId
                });
                return;
            }

            senderEntry.viewers.add(socket);

            console.log(`[WS] Viewer attached to sender ${senderId}`);
            console.log(`     Total Viewers for ${senderId}: ${senderEntry.viewers.size}`);

            sendJSON(senderEntry.socket, {
                type: "viewer-request"
            });

            return;
        }

        // ----------------------------------------------------
        // 3. Sender sends offer SDP
        // ----------------------------------------------------
        if (type === "sender-peer-id") {
            const { peerId } = msg;
            const senderId = socket.senderId;
            if (!senderId) return;

            const senderEntry = senders.get(senderId);
            if (!senderEntry) return;

            logDivider();
            console.log(`[WS] 🎥 Sender Offer Received`);
            console.log(`     Sender: ${senderId}`);
            console.log(`     Viewers to notify: ${senderEntry.viewers.size}`);
            logDivider();

            senderEntry.viewers.forEach(viewerSocket => {
                sendJSON(viewerSocket, {
                    type: "deliver-peer-id",
                    peerId
                });
            });

            return;
        }

        // ----------------------------------------------------
        // 4. Viewer sends answer SDP
        // ----------------------------------------------------
        if (type === "viewer-answer") {
            const { answer } = msg;
            const senderId = socket.senderId;
            if (!senderId) return;

            const senderEntry = senders.get(senderId);
            if (!senderEntry) return;

            logDivider();
            console.log(`[WS] 🔄 Viewer Answer Received`);
            console.log(`     Sender: ${senderId}`);
            logDivider();

            sendJSON(senderEntry.socket, {
                type: "viewer-answer",
                answer
            });

            return;
        }

        // ----------------------------------------------------
        // 5. Viewer ICE → Sender
        // ----------------------------------------------------
        if (type === "viewer-ice") {
            const { candidate } = msg;
            const senderId = socket.senderId;

            const senderEntry = senders.get(senderId);
            if (!senderEntry) return;

            console.log(`[WS] ❄ Viewer ICE → Sender (${senderId})`);

            sendJSON(senderEntry.socket, {
                type: "viewer-ice",
                candidate
            });

            return;
        }

        // ----------------------------------------------------
        // 6. Sender ICE → Viewers
        // ----------------------------------------------------
        if (type === "sender-ice") {
            const { candidate } = msg;
            const senderId = socket.senderId;

            const senderEntry = senders.get(senderId);
            if (!senderEntry) return;

            console.log(`[WS] ❄ Sender ICE → Viewers (${senderId})`);

            senderEntry.viewers.forEach(viewerSocket => {
                sendJSON(viewerSocket, {
                    type: "sender-ice",
                    candidate
                });
            });

            return;
        }
    });

    // ----------------------------------------------------
    // Disconnect handling
    // ----------------------------------------------------
    socket.on("close", () => {
        if (socket.role === "sender" && socket.senderId) {
            const senderId = socket.senderId;
            const entry = senders.get(senderId);

            logDivider();
            console.log(`[WS] ❌ Sender Disconnected`);
            console.log(`     Sender ID: ${senderId}`);
            logDivider();

            if (entry) {
                entry.viewers.forEach(viewerSocket => {
                    sendJSON(viewerSocket, {
                        type: "sender-disconnected",
                        senderId
                    });
                });
            }

            senders.delete(senderId);
        }

        if (socket.role === "viewer" && socket.senderId) {
            const senderId = socket.senderId;
            const entry = senders.get(senderId);

            logDivider();
            console.log(`[WS] 👋 Viewer Disconnected`);
            console.log(`     From Sender: ${senderId}`);
            logDivider();

            if (entry) {
                entry.viewers.delete(socket);
            }
        }
    });
});
