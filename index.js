import { WebSocketServer } from "ws";
import { createServer } from "http";
import net from "net";

const BUFFER_SIZE = 131072;

function encode(type, streamId, payload) {
    const out = new Uint8Array(5 + payload.byteLength);
    const view = new DataView(out.buffer);
    view.setUint8(0, type);
    view.setUint32(1, streamId, true);
    out.set(new Uint8Array(payload), 5);
    return out.buffer;
}

function continuePacket(streamId) {
    const p = new Uint8Array(4);
    new DataView(p.buffer).setUint32(0, BUFFER_SIZE, true);
    return encode(0x03, streamId, p.buffer);
}

function closePacket(streamId, reason = 0x02) {
    return encode(0x04, streamId, new Uint8Array([reason]).buffer);
}

async function handleWisp(ws) {
    const streams = new Map();

    ws.send(Buffer.from(continuePacket(0)));

    ws.on("message", async (data) => {
        try {
            const buf = data instanceof Buffer ? data.buffer.slice(data.byteOffset, data.byteOffset + data.byteLength) : data;
            const view = new DataView(buf);
            const type = view.getUint8(0);
            const streamId = view.getUint32(1, true);
            const payload = buf.slice(5);

            if (type === 0x01) { // CONNECT
                const pv = new DataView(payload);
                const port = pv.getUint16(1, true);
                const hostname = Buffer.from(payload.slice(3)).toString().trim();
                console.log(`[wisp] CONNECT ${hostname}:${port}`);

                try {
                    const socket = new net.Socket();
                    streams.set(streamId, { socket, hostname, port });

                    socket.connect(port, hostname, () => {
                        console.log(`[wisp] opened ${hostname}:${port}`);
                        ws.send(Buffer.from(continuePacket(streamId)));
                    });

                    socket.on("data", (chunk) => {
                        try {
                            ws.send(Buffer.from(encode(0x02, streamId, chunk.buffer.slice(chunk.byteOffset, chunk.byteOffset + chunk.byteLength))));
                        } catch (e) {
                            console.error(`[wisp] send error ${hostname}:${port}:`, e);
                        }
                    });

                    socket.on("close", () => {
                        console.log(`[wisp] closed ${hostname}:${port}`);
                        try { ws.send(Buffer.from(closePacket(streamId, 0x02))); } catch {}
                        streams.delete(streamId);
                    });

                    socket.on("error", (e) => {
                        console.error(`[wisp] socket error ${hostname}:${port}:`, e);
                        try { ws.send(Buffer.from(closePacket(streamId, 0x03))); } catch {}
                        streams.delete(streamId);
                    });

                } catch (e) {
                    console.error(`[wisp] connect error ${hostname}:${port}:`, e);
                    ws.send(Buffer.from(closePacket(streamId, 0x42)));
                }

            } else if (type === 0x02) { // DATA
                const stream = streams.get(streamId);
                if (stream) {
                    try {
                        stream.socket.write(Buffer.from(payload));
                    } catch (e) {
                        console.error(`[wisp] write error stream ${streamId}:`, e);
                    }
                }

            } else if (type === 0x04) { // CLOSE
                const stream = streams.get(streamId);
                if (stream) {
                    console.log(`[wisp] closing stream ${streamId} ${stream.hostname}:${stream.port}`);
                    try { stream.socket.destroy(); } catch {}
                    streams.delete(streamId);
                }
            } else {
                console.warn(`[wisp] unknown packet type 0x${type.toString(16)} stream ${streamId}`);
            }
        } catch (e) {
            console.error("[wisp] message handler error:", e);
        }
    });

    ws.on("close", (code, reason) => {
        console.log("[wisp] WebSocket closed:", code, reason.toString());
        for (const { socket } of streams.values()) {
            try { socket.destroy(); } catch {}
        }
        streams.clear();
    });

    ws.on("error", (e) => {
        console.error("[wisp] WebSocket error:", e);
    });
}

const server = createServer((req, res) => {
    res.writeHead(200);
    res.end("Wisp server running");
});

const wss = new WebSocketServer({ server });

wss.on("connection", (ws) => {
    handleWisp(ws);
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
    console.log(`Wisp server listening on port ${PORT}`);
});