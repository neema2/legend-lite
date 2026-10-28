// A FAKE COINBASE FEED (//datacube:deephaven_feed_test): a WebSocket server on the loopback, written by hand -- the few
// frames of RFC 6455 the market world's feed (demo/deephaven/market/market.groovy `CoinbaseFeed`) reads -- that answers
// its subscription as Coinbase Exchange does, and misbehaves as a feed can: each message in parts, a message no feed
// sends whole, a price that is not a number, and a socket that goes quiet in the middle of a message.

import { createHash } from 'node:crypto';
import { createServer } from 'node:http';

const GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';

/** A server frame (never masked): `fin`, its opcode, its payload. */
function frame(fin, opcode, payload) {
  const n = payload.length;
  const head = n < 126 ? Buffer.from([(fin ? 0x80 : 0) | opcode, n])
    : n < 65536 ? Buffer.from([(fin ? 0x80 : 0) | opcode, 126, n >> 8, n & 0xff])
      : (() => { const h = Buffer.alloc(10); h[0] = (fin ? 0x80 : 0) | opcode; h[1] = 127; h.writeBigUInt64BE(BigInt(n), 2); return h; })();
  return Buffer.concat([head, payload]);
}

/** Each whole client frame in `buf` (always masked), and what is left of it. */
function frames(buf) {
  const out = [];
  for (;;) {
    if (buf.length < 2) return [out, buf];
    const opcode = buf[0] & 0x0f;
    let n = buf[1] & 0x7f;
    let at = 2;
    if (n === 126) { if (buf.length < 4) return [out, buf]; n = buf.readUInt16BE(2); at = 4; }
    else if (n === 127) { if (buf.length < 10) return [out, buf]; n = Number(buf.readBigUInt64BE(2)); at = 10; }
    const masked = (buf[1] & 0x80) !== 0;
    const mask = masked ? buf.subarray(at, at + 4) : null;
    if (masked) at += 4;
    if (buf.length < at + n) return [out, buf];
    const payload = Buffer.from(buf.subarray(at, at + n));
    if (mask) for (let i = 0; i < n; i++) payload[i] ^= mask[i % 4];
    out.push({ opcode, payload });
    buf = buf.subarray(at + n);
  }
}

/**
 * The feed, listening: `url` its address; `sockets()` how many are open now, `most` the most open at once, `opened`
 * how many were ever; `stall()` makes the socket open now send the first part of a message and nothing more.
 * Every socket sends, every `everyMs`, a ticker and a match for each product it was subscribed to, each in three parts.
 * The first socket begins with a message that is not JSON and a ticker whose bid is not a number.
 */
export async function fakeFeed({ everyMs = 250 } = {}) {
  const open = new Set();
  const state = { most: 0, opened: 0 };
  const server = createServer((_req, res) => { res.writeHead(426); res.end(); });
  server.on('upgrade', (req, socket) => {
    const accept = createHash('sha1').update(`${req.headers['sec-websocket-key']}${GUID}`).digest('base64');
    socket.write(`HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ${accept}\r\n\r\n`);
    const conn = { socket, n: ++state.opened, stalled: false, timer: null };
    open.add(conn);
    state.most = Math.max(state.most, open.size);
    const gone = () => { open.delete(conn); clearInterval(conn.timer); };
    // the client's close: an http server's socket is half open (allowHalfOpen), so its end is said by 'end' alone
    socket.on('end', () => { gone(); socket.end(); });
    socket.on('close', gone);
    socket.on('error', gone);
    const send = (text, parts = 1) => {
      if (socket.destroyed) return;
      const b = Buffer.from(text);
      const size = Math.ceil(b.length / parts);
      for (let i = 0; i < parts; i++) {
        socket.write(frame(i === parts - 1, i === 0 ? 1 : 0, b.subarray(i * size, Math.min(b.length, (i + 1) * size))));
      }
    };
    conn.half = (text) => { if (!socket.destroyed) socket.write(frame(false, 1, Buffer.from(text.slice(0, Math.floor(text.length / 2))))); };
    let buf = Buffer.alloc(0);
    socket.on('data', (d) => {
      let got;
      [got, buf] = frames(Buffer.concat([buf, d]));
      for (const f of got) {
        if (f.opcode === 8) { socket.end(frame(true, 8, Buffer.alloc(0))); gone(); return; }
        if (f.opcode === 9) { socket.write(frame(true, 10, f.payload)); continue; }
        if (f.opcode !== 1) continue;
        const m = JSON.parse(f.payload.toString());
        if (m.type !== 'subscribe') continue;
        const products = m.product_ids;
        if (conn.n === 1) {
          send('{"type":"ticker","product_id":');
          send(JSON.stringify(ticker(products[0], 'x')));
        }
        let px = 100;
        const beat = () => {
          if (conn.stalled) return;
          px += 0.5;
          for (const p of products) {
            send(JSON.stringify(ticker(p, String(px))), 3);
            send(JSON.stringify({ type: 'match', product_id: p, time: new Date().toISOString(), price: String(px), size: '0.5', side: 'buy' }), 3);
          }
        };
        beat();
        conn.timer = setInterval(beat, everyMs);
      }
    });
  });
  const port = await new Promise((done) => server.listen(0, '127.0.0.1', () => done(server.address().port)));
  return {
    url: `ws://127.0.0.1:${port}/`,
    sockets: () => open.size,
    get most() { return state.most; },
    get opened() { return state.opened; },
    stall: () => {
      for (const c of open) {
        c.stalled = true;
        c.half(JSON.stringify(ticker('BTC-USD', '1')));
      }
    },
    close: () => { for (const c of open) c.socket.destroy(); server.close(); },
  };
}

function ticker(product, bid) {
  return {
    type: 'ticker', product_id: product, time: new Date().toISOString(), open_24h: '100',
    best_bid: bid, best_ask: String(Number(bid) + 1 || 'x'), best_bid_size: '1', best_ask_size: '1', price: bid,
  };
}
