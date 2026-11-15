// Public relay for the Proof Gatekeeper agent: Render free web service.
// Platform traffic hits this public URL; it is forwarded over a persistent
// WebSocket to the agent server running on the VM, and responses come back.
// No secrets here. Start: node server.js (PORT env, default 10000).
const http = require('http');
const crypto = require('crypto');
const { WebSocketServer } = require('ws');

const PORT = Number(process.env.PORT || 10000);
let client = null; // active VM-side WebSocket
const pending = new Map(); // id -> { resolve, timer }

function sendToClient(msg) {
  return new Promise((resolve, reject) => {
    if (!client || client.readyState !== 1) return reject(new Error('no vm client connected'));
    const id = crypto.randomUUID();
    const timer = setTimeout(() => { pending.delete(id); reject(new Error('vm timeout')); }, 55000);
    pending.set(id, { resolve, timer });
    client.send(JSON.stringify({ ...msg, id }));
  });
}

const wss = new WebSocketServer({ noServer: true });
wss.on('connection', (ws) => {
  client = ws;
  console.log('vm client connected');
  ws.on('message', (data) => {
    let msg; try { msg = JSON.parse(String(data)); } catch { return; }
    const p = pending.get(msg.id);
    if (p) { clearTimeout(p.timer); pending.delete(msg.id); p.resolve(msg); }
  });
  ws.on('close', () => { if (client === ws) { client = null; console.log('vm client disconnected'); } });
  ws.on('error', () => {});
});

const server = http.createServer(async (req, res) => {
  if (req.url === '/healthz') { res.writeHead(200, { 'content-type': 'application/json' }); return res.end(JSON.stringify({ ok: true, service: 'gk-relay', vm: Boolean(client) })); }
  const chunks = [];
  req.on('data', (c) => chunks.push(c));
  req.on('end', async () => {
    const body = Buffer.concat(chunks);
    try {
      const reply = await sendToClient({
        type: 'http', method: req.method, url: req.url,
        headers: req.headers, body: body.toString('base64'),
      });
      const out = Buffer.from(reply.body || '', 'base64');
      res.writeHead(reply.status || 502, reply.headers || {});
      res.end(out);
    } catch (e) {
      res.writeHead(502, { 'content-type': 'application/json' });
      res.end(JSON.stringify({ ok: false, error: e.message }));
    }
  });
});

server.on('upgrade', (req, socket, head) => {
  if (req.url === '/relay') wss.handleUpgrade(req, socket, head, (ws) => wss.emit('connection', ws, req));
  else socket.destroy();
});
server.listen(PORT, () => console.log('relay listening on ' + PORT));
