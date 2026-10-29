// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

// A scratch spike, not product code: the smallest Kafka endpoint Deephaven's reader (kafka-clients 8.3.2-ccs, assign +
// seek + poll) accepts, serving one topic of JSON quotes a simulated world appends in this process. Read-only: no
// Produce, no consumer groups. One version of each API, the newest without the "flexible" encoding:
// ApiVersions 3-4, Metadata 8, ListOffsets 5, Fetch 11. Records in Kafka's record batch v2, CRC-32C checked by the client.
import net from 'node:net';

const PORT = Number(process.env.PORT ?? 19092);
const RATE = Number(process.env.RATE ?? 2000); // records a second
const SECONDS = Number(process.env.SECONDS ?? 60);
const KEEP = Number(process.env.KEEP ?? 2_000_000); // the bounded history, records
const TOPIC = 'quotes';
const NODE = 0;

// ---- the log: one partition, offsets from 0, the oldest dropped past KEEP ----
const log = []; // { ts, value: Buffer }
let logStart = 0; // the offset of log[0]
let waiters = [];
const hw = () => logStart + log.length;
function append(ts, value) {
  log.push({ ts, value });
  if (log.length > KEEP * 1.1) { const drop = log.length - KEEP; log.splice(0, drop); logStart += drop; }
}

// ---- the world: 100 names, a random walk, RATE quotes a second in 10 ms ticks ----
const names = Array.from({ length: 100 }, (_, i) => 'S' + String(i).padStart(3, '0'));
const mid = names.map(() => 100 + Math.random() * 50);
let seq = 0;
const perTick = RATE / 100;
let owed = 0;
const world = setInterval(() => {
  owed += perTick;
  const now = Date.now();
  for (; owed >= 1; owed--) {
    const i = Math.floor(Math.random() * names.length);
    mid[i] *= 1 + (Math.random() - 0.5) * 0.001;
    const half = 0.01;
    const v = { Sym: names[i], Bid: +(mid[i] - half).toFixed(4), Ask: +(mid[i] + half).toFixed(4), Seq: seq++ };
    append(now, Buffer.from(JSON.stringify(v)));
  }
  const w = waiters; waiters = []; for (const f of w) f();
}, 10);

// ---- bytes ----
class W {
  constructor() { this.b = Buffer.alloc(1024); this.n = 0; }
  room(k) { if (this.n + k > this.b.length) { const nb = Buffer.alloc(Math.max(this.b.length * 2, this.n + k)); this.b.copy(nb, 0, 0, this.n); this.b = nb; } }
  i8(v) { this.room(1); this.b.writeInt8(v, this.n); this.n += 1; return this; }
  i16(v) { this.room(2); this.b.writeInt16BE(v, this.n); this.n += 2; return this; }
  i32(v) { this.room(4); this.b.writeInt32BE(v, this.n); this.n += 4; return this; }
  u32(v) { this.room(4); this.b.writeUInt32BE(v, this.n); this.n += 4; return this; }
  i64(v) { this.room(8); this.b.writeBigInt64BE(BigInt(v), this.n); this.n += 8; return this; }
  uv(v) { while (v >= 0x80) { this.i8u((v % 128) | 0x80); v = Math.floor(v / 128); } return this.i8u(v); }
  i8u(v) { this.room(1); this.b[this.n++] = v; return this; }
  zz(v) { return this.uv(v >= 0 ? 2 * v : -2 * v - 1); }
  str(s) { if (s === null) return this.i16(-1); const x = Buffer.from(s); this.i16(x.length); return this.raw(x); }
  raw(x) { this.room(x.length); x.copy(this.b, this.n); this.n += x.length; return this; }
  done() { return this.b.subarray(0, this.n); }
}
class R {
  constructor(b) { this.b = b; this.p = 0; }
  i8() { return this.b.readInt8(this.p++); }
  i16() { const v = this.b.readInt16BE(this.p); this.p += 2; return v; }
  i32() { const v = this.b.readInt32BE(this.p); this.p += 4; return v; }
  i64() { const v = this.b.readBigInt64BE(this.p); this.p += 8; return Number(v); }
  str() { const n = this.i16(); if (n < 0) return null; const s = this.b.toString('utf8', this.p, this.p + n); this.p += n; return s; }
  uv() { let v = 0, m = 1, x; do { x = this.b[this.p++]; v += (x & 0x7f) * m; m *= 128; } while (x & 0x80); return v; }
  tags() { for (let n = this.uv(); n > 0; n--) { this.uv(); this.p += this.uv(); } }
  arr(f) { const n = this.i32(); const out = []; for (let i = 0; i < n; i++) out.push(f()); return n < 0 ? null : out; }
}

const CRC = new Int32Array(256);
for (let n = 0; n < 256; n++) { let c = n; for (let k = 0; k < 8; k++) c = c & 1 ? (c >>> 1) ^ 0x82f63b78 : c >>> 1; CRC[n] = c; }
function crc32c(b, from, to) { let c = ~0; for (let i = from; i < to; i++) c = CRC[(c ^ b[i]) & 0xff] ^ (c >>> 8); return ~c >>> 0; }

/** Records [from, to) of the log as one record batch v2, uncompressed, CreateTime. */
function batch(from, to) {
  const first = log[from - logStart], last = log[to - 1 - logStart];
  const body = new W();
  for (let o = from; o < to; o++) {
    const r = log[o - logStart];
    const rec = new W().i8(0).zz(r.ts - first.ts).zz(o - from).zz(-1).zz(r.value.length).raw(r.value).zz(0).done();
    body.zz(rec.length).raw(rec);
  }
  const w = new W();
  w.i64(from).i32(0).i32(-1).i8(2).u32(0).i16(0).i32(to - 1 - from).i64(first.ts).i64(last.ts).i64(-1).i16(-1).i32(-1)
    .i32(to - from).raw(body.done());
  const b = w.done();
  b.writeInt32BE(b.length - 12, 8);
  b.writeUInt32BE(crc32c(b, 21, b.length), 17);
  return b;
}

// ---- the APIs ----
const stats = { requests: {}, fetched: 0, fetchBytes: 0 };
const ADVERTISED = [[18, 3, 4], [3, 8, 8], [2, 5, 5], [1, 11, 11]];

function apiVersions(w) {
  w.i16(0).uv(ADVERTISED.length + 1);
  for (const [k, lo, hi] of ADVERTISED) w.i16(k).i16(lo).i16(hi).uv(0);
  w.i32(0).uv(0);
}

function metadata(r, w) {
  const topics = r.arr(() => r.str());
  w.i32(0).i32(1).i32(NODE).str('127.0.0.1').i32(PORT).str(null).str('legend-shim').i32(NODE);
  const asked = topics ?? [TOPIC];
  w.i32(asked.length);
  for (const t of asked) {
    if (t !== TOPIC) { w.i16(3).str(t).i8(0).i32(0).i32(-2147483648); continue; }
    w.i16(0).str(t).i8(0).i32(1).i16(0).i32(0).i32(NODE).i32(-1).i32(1).i32(NODE).i32(1).i32(NODE).i32(0)
      .i32(-2147483648);
  }
  w.i32(-2147483648);
}

function listOffsets(r, w) {
  r.i32(); r.i8();
  const topics = r.arr(() => ({ name: r.str(), parts: r.arr(() => ({ p: r.i32(), e: r.i32(), ts: r.i64() })) }));
  w.i32(0).i32(topics.length);
  for (const t of topics) {
    w.str(t.name).i32(t.parts.length);
    for (const q of t.parts) {
      let off;
      if (q.ts === -2) off = logStart;
      else if (q.ts === -1) off = hw();
      else { const i = log.findIndex((x) => x.ts >= q.ts); off = i < 0 ? hw() : logStart + i; }
      w.i32(q.p).i16(t.name === TOPIC && q.p === 0 ? 0 : 3).i64(-1).i64(off).i32(-1);
    }
  }
}

async function fetch(r, w) {
  r.i32(); const maxWait = r.i32(); r.i32(); r.i32(); r.i8(); r.i32(); r.i32();
  const topics = r.arr(() => ({
    name: r.str(),
    parts: r.arr(() => ({ p: r.i32(), e: r.i32(), off: r.i64(), ls: r.i64(), max: r.i32() })),
  }));
  const wanted = topics.flatMap((t) => t.parts.filter(() => t.name === TOPIC).map((q) => q.off));
  if (wanted.length && wanted.every((o) => o >= hw())) {
    await new Promise((res) => { const t = setTimeout(res, maxWait); waiters.push(() => { clearTimeout(t); res(); }); });
  }
  w.i32(0).i16(0).i32(0).i32(topics.length);
  for (const t of topics) {
    w.str(t.name).i32(t.parts.length);
    for (const q of t.parts) {
      const ok = t.name === TOPIC && q.p === 0;
      const inRange = ok && q.off >= logStart && q.off <= hw();
      w.i32(q.p).i16(!ok ? 3 : inRange ? 0 : 1).i64(hw()).i64(hw()).i64(logStart).i32(-1).i32(-1);
      if (!inRange || q.off === hw()) { w.i32(0); continue; }
      let to = q.off, bytes = 61;
      while (to < hw() && bytes < q.max) { bytes += log[to - logStart].value.length + 12; to++; }
      const b = batch(q.off, to);
      stats.fetched += to - q.off; stats.fetchBytes += b.length;
      w.i32(b.length).raw(b);
    }
  }
}

async function answer(frame) {
  const r = new R(frame);
  const key = r.i16(), version = r.i16(), corr = r.i32();
  r.str();
  if (key === 18 && version >= 3) r.tags();
  stats.requests[key] = (stats.requests[key] ?? 0) + 1;
  const w = new W().i32(0).i32(corr);
  if (key === 18) apiVersions(w);
  else if (key === 3 && version === 8) metadata(r, w);
  else if (key === 2 && version === 5) listOffsets(r, w);
  else if (key === 1 && version === 11) await fetch(r, w);
  else { console.error(`refused: api ${key} v${version}`); return null; }
  const out = w.done();
  out.writeInt32BE(out.length - 4, 0);
  return out;
}

const server = net.createServer((sock) => {
  let pending = Buffer.alloc(0);
  let chain = Promise.resolve();
  sock.on('data', (d) => {
    pending = Buffer.concat([pending, d]);
    while (pending.length >= 4) {
      const n = pending.readInt32BE(0);
      if (pending.length < 4 + n) break;
      const frame = pending.subarray(4, 4 + n);
      pending = pending.subarray(4 + n);
      // answers go back in the order asked, a fetch's wait holding the ones behind it
      chain = chain.then(() => answer(frame)).then((out) => { if (out) sock.write(out); else sock.destroy(); });
    }
  });
  sock.on('error', () => {});
});
server.listen(PORT, '127.0.0.1', () => console.log(`shim listening on ${PORT}, ${RATE}/s`));

const cpu0 = process.cpuUsage();
const t0 = Date.now();
setTimeout(() => {
  clearInterval(world);
  const cpu = process.cpuUsage(cpu0);
  const secs = (Date.now() - t0) / 1000;
  console.log(JSON.stringify({
    produced: seq, secs, fetched: stats.fetched, fetchMB: +(stats.fetchBytes / 1e6).toFixed(1),
    requests: stats.requests, cpuPct: +((cpu.user + cpu.system) / 1e4 / secs).toFixed(1),
    rssMB: +(process.memoryUsage().rss / 1e6).toFixed(0),
  }));
  process.exit(0);
}, SECONDS * 1000);
