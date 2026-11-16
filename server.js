// Public relay for the Proof Gatekeeper agent: Render free web service.
// Plain-HTTPS long-polling design (the VM's egress policy blocks WebSocket
// upgrades, so no WebSocket is used anywhere).
//
// Platform -> relay: any HTTP request (except /healthz, /poll, /result).
//   It is queued and handed to the VM client on its next GET /poll.
// VM client -> relay: GET /poll (held up to 45s), then POST /result {id,...}.
// Relay -> platform: the client's result, or 502 on timeout.
// No secrets here. Start: node server.js (PORT env, default 10000).
const http = require('http');
const crypto = require('crypto');

const PORT = Number(process.env.PORT || 10000);
const WAIT_TIMEOUT_MS = 55000; // platform request waits this long for a result
const POLL_TIMEOUT_MS = 3000; // client poll held this long when idle (short: the VM's egress proxy kills long-held connections within seconds)

// id -> { resolve, timer } for platform requests waiting on a result
const waiters = new Map();
// [{ res, timer }] polls waiting for a platform request
const polls = [];
// platform requests queued while no poll is waiting: [{ id, req }]
const queued = [];
let lastPollAt = 0;

function deliverNext() {
  while (polls.length && queued.length) {
    const poll = polls.shift();
    clearTimeout(poll.timer);
    const q = queued.shift();
    poll.res.writeHead(200, { 'content-type': 'application/json' });
    poll.res.end(JSON.stringify({ type: 'http', id: q.id, method: q.req.method, url: q.req.url, headers: q.req.headers, body: q.req.body }));
  }
}

const server = http.createServer((req, res) => {
  if (req.url === '/healthz') {
    res.writeHead(200, { 'content-type': 'application/json' });
    return res.end(JSON.stringify({ ok: true, service: 'gk-relay', vm: Date.now() - lastPollAt < 60000 }));
  }
  if (req.url === '/poll' && req.method === 'GET') {
    lastPollAt = Date.now();
    const timer = setTimeout(() => {
      const i = polls.findIndex((p) => p.res === res);
      if (i >= 0) polls.splice(i, 1);
      res.writeHead(200, { 'content-type': 'application/json' });
      res.end(JSON.stringify({ type: 'noop' }));
    }, POLL_TIMEOUT_MS);
    polls.push({ res, timer });
    deliverNext();
    return;
  }
  if (req.url === '/result' && req.method === 'POST') {
    const chunks = [];
    req.on('data', (c) => chunks.push(c));
    req.on('end', () => {
      let msg; try { msg = JSON.parse(Buffer.concat(chunks).toString()); } catch { res.writeHead(400); return res.end(); }
      const w = waiters.get(msg.id);
      if (w) { clearTimeout(w.timer); waiters.delete(msg.id); w.resolve(msg); }
      res.writeHead(200); res.end();
    });
    return;
  }
  // platform request: queue it, wait for the client result
  const chunks = [];
  req.on('data', (c) => chunks.push(c));
  req.on('end', () => {
    const id = crypto.randomUUID();
    const entry = { id, req: { method: req.method, url: req.url, headers: req.headers, body: Buffer.concat(chunks).toString('base64') } };
    const promise = new Promise((resolve) => {
      const timer = setTimeout(() => { waiters.delete(id); resolve(null); }, WAIT_TIMEOUT_MS);
      waiters.set(id, { resolve, timer });
    });
    queued.push(entry);
    deliverNext();
    promise.then((reply) => {
      if (!reply) { res.writeHead(502, { 'content-type': 'application/json' }); return res.end(JSON.stringify({ ok: false, error: 'vm timeout' })); }
      const out = Buffer.from(reply.body || '', 'base64');
      res.writeHead(reply.status || 502, reply.headers || {});
      res.end(out);
    });
  });
});

server.listen(PORT, () => console.log('relay listening on ' + PORT));
