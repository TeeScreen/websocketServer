import WebSocket, { WebSocketServer } from 'ws';

const PORT = 5000;

const server = new WebSocketServer({ port: PORT });

console.log(`[WS] WebSocket server running on port ${PORT}`);

server.on('connection', socket => {
    console.log('[WS] Client connected');

    socket.on('message', msg => {
        console.log('[WS] Received:', msg.toString());

        // Broadcast to all other clients
        server.clients.forEach(client => {
            if (client !== socket && client.readyState === WebSocket.OPEN) {
                client.send(msg);
            }
        });
    });

    socket.on('close', () => {
        console.log('[WS] Client disconnected');
    });
});
