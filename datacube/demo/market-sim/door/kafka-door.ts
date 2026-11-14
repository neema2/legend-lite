// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

// THE DOOR (docs/DESK_DATA_ROADMAP_2026_10_29.md B1, from the spike in experiments/kafka-door-spike): the smallest Kafka
// endpoint Deephaven's reader takes (kafka-clients 8.3.2-ccs: assign, seek and poll, never a consumer group), serving
// the market world's tables as topics -- each one the topic the model says its dataset reads (the planner's
// `deephaven` request, op `topics`). An appended topic drops what is past its retention; a keyed one keeps the latest
// record of each key, as Kafka's compacted topics do. Read-only: no Produce, no consumer groups, one partition a
// topic. It answers four requests, one version each, the newest without Kafka's "flexible" encoding -- ApiVersions
// 3-4, Metadata 8, ListOffsets 5, Fetch 11 -- and refuses any other by name, closing the connection. Records are Kafka's
// record batch v2, uncompressed, CRC-32C. Node alone: node:net, no library.

import net from 'node:net';

/** A topic as the door keeps it: its name, the key a compacted topic keeps each latest record of (empty: appended), and
 *  an appended topic's retention -- its latest `keepRows` records, or those of the last `keepForMs` -- none when null. */
export interface DoorTopic {
  readonly topic: string;
  readonly compacted: boolean;
  readonly keepRows: number | null;
  readonly keepForMs: number | null;
}

/** One record of a topic's log: its offset, its time (ms), its key (a compacted topic's) and its value. */
export interface DoorRecord {
  readonly offset: number;
  readonly ts: number;
  readonly key: Buffer | null;
  readonly value: Buffer;
}

/** The requests the door answers, as Kafka's ApiVersions advertises them: [api key, lowest, highest]. */
const ADVERTISED: readonly (readonly [number, number, number])[] = [[18, 3, 4], [3, 8, 8], [2, 5, 5], [1, 11, 11]];
const API_NAMES: Readonly<Record<number, string>> = { 18: 'ApiVersions', 3: 'Metadata', 2: 'ListOffsets', 1: 'Fetch' };
/** The requests a client might send that the door does not answer, by their names: a refusal says which. */
const REFUSED_NAMES: Readonly<Record<number, string>> = {
  ...API_NAMES, 0: 'Produce', 8: 'OffsetCommit', 9: 'OffsetFetch', 10: 'FindCoordinator', 11: 'JoinGroup', 12: 'Heartbeat',
  13: 'LeaveGroup', 14: 'SyncGroup', 19: 'CreateTopics', 22: 'InitProducerId',
};

/** Kafka's error codes the door answers with. */
const NONE = 0;
const OFFSET_OUT_OF_RANGE = 1;
const UNKNOWN_TOPIC_OR_PARTITION = 3;

/** One topic's log: one partition, offsets from 0, gaps where compaction took a record. */
class TopicLog {
  readonly spec: DoorTopic;
  #entries: DoorRecord[] = [];
  /** The offset the next record takes: the high watermark. */
  next = 0;
  /** A compacted topic's latest offset of each key. */
  readonly #latest = new Map<string, number>();
  #superseded = 0;

  constructor(spec: DoorTopic) {
    this.spec = spec;
  }

  /** The log's start: an appended topic's first record held (the next offset when it holds none, its retention moving
   *  it on); a compacted topic's 0, as Kafka's is -- compaction takes records out, it does not move the start, and a
   *  read from before a record compaction took is the next record kept. */
  get start(): number {
    return this.spec.compacted ? 0 : this.#entries[0]?.offset ?? this.next;
  }

  get size(): number {
    return this.#entries.length;
  }

  append(ts: number, key: Buffer | null, value: Buffer): void {
    const offset = this.next++;
    this.#entries.push({ offset, ts, key, value });
    if (this.spec.compacted) {
      if (key === null) throw new Error(`the compacted topic ${this.spec.topic} takes a record with a key, not one without`);
      const k = key.toString('latin1');
      if (this.#latest.has(k)) this.#superseded++;
      this.#latest.set(k, offset);
      // compacted once the records superseded outnumber the latest: the log at most about twice its keys
      if (this.#superseded > Math.max(1024, this.#latest.size)) this.#compact();
      return;
    }
    const { keepRows, keepForMs } = this.spec;
    // trimmed past a tenth over its retention, so a trim is not a splice a record
    if (keepRows !== null && this.#entries.length > keepRows + Math.max(1, Math.floor(keepRows / 10))) {
      this.#entries.splice(0, this.#entries.length - keepRows);
    }
    if (keepForMs !== null && this.#entries.length > 0 && this.#entries[0]!.ts < ts - keepForMs * 1.1) {
      this.#entries.splice(0, this.#from(ts - keepForMs));
    }
  }

  #compact(): void {
    this.#entries = this.#entries.filter((e) => this.#latest.get(e.key!.toString('latin1')) === e.offset);
    this.#superseded = 0;
  }

  /** The index of the first record at or after `offset`. */
  #at(offset: number): number {
    let lo = 0;
    let hi = this.#entries.length;
    while (lo < hi) {
      const mid = (lo + hi) >>> 1;
      if (this.#entries[mid]!.offset < offset) lo = mid + 1;
      else hi = mid;
    }
    return lo;
  }

  /** The index of the first record at or after `ts` (records are in time order: each appended at its own time). */
  #from(ts: number): number {
    let lo = 0;
    let hi = this.#entries.length;
    while (lo < hi) {
      const mid = (lo + hi) >>> 1;
      if (this.#entries[mid]!.ts < ts) lo = mid + 1;
      else hi = mid;
    }
    return lo;
  }

  /** The offset of the first record at or after `ts`; the high watermark when none is. */
  offsetAt(ts: number): number {
    return this.#entries[this.#from(ts)]?.offset ?? this.next;
  }

  /** The records from `offset` on, up to about `maxBytes`: at least one when there is one. */
  read(offset: number, maxBytes: number): DoorRecord[] {
    const out: DoorRecord[] = [];
    let bytes = 61;
    for (let i = this.#at(offset); i < this.#entries.length; i++) {
      const e = this.#entries[i]!;
      bytes += e.value.length + (e.key?.length ?? 0) + 12;
      if (out.length > 0 && bytes > maxBytes) break;
      out.push(e);
    }
    return out;
  }
}

/** What the door has answered, by request, and what it has served. */
export interface DoorCounts {
  readonly requests: Record<string, number>;
  fetched: number;
  fetchBytes: number;
  refused: number;
}

export class KafkaDoor {
  readonly #logs = new Map<string, TopicLog>();
  #waiters: (() => void)[] = [];
  readonly #server: net.Server;
  #port = 0;
  readonly counts: DoorCounts = { requests: {}, fetched: 0, fetchBytes: 0, refused: 0 };
  /** Where a refusal is said: the console's error stream unless the caller says otherwise. */
  readonly #say: (message: string) => void;

  constructor(topics: readonly DoorTopic[], say: (message: string) => void = (m) => { console.error(m); }) {
    for (const t of topics) {
      if (this.#logs.has(t.topic)) throw new Error(`the door holds the topic ${t.topic} once, not twice`);
      if (t.compacted && (t.keepRows !== null || t.keepForMs !== null)) {
        throw new Error(`the compacted topic ${t.topic} keeps each key's latest record, so it has no retention`);
      }
      this.#logs.set(t.topic, new TopicLog(t));
    }
    this.#say = say;
    this.#server = net.createServer((socket) => { this.#serve(socket); });
  }

  /** Listens on the loopback, on `port` (0: any free one); the port it listens on. */
  listen(port = 0): Promise<number> {
    return new Promise((resolve, reject) => {
      this.#server.once('error', reject);
      this.#server.listen(port, '127.0.0.1', () => {
        this.#port = (this.#server.address() as net.AddressInfo).port;
        resolve(this.#port);
      });
    });
  }

  close(): Promise<void> {
    for (const w of this.#waiters) w();
    this.#waiters = [];
    return new Promise((resolve) => { this.#server.close(() => { resolve(); }); });
  }

  /** A record appended to `topic` at `ts` (ms): `key` a compacted topic's (its key columns' values), `value` its JSON. */
  append(topic: string, ts: number, key: string | null, value: string): void {
    const log = this.#logs.get(topic);
    if (log === undefined) throw new Error(`the door holds no topic ${topic}`);
    log.append(ts, key === null ? null : Buffer.from(key), Buffer.from(value));
  }

  /** Each waiting fetch told that records arrived (once a batch of appends is in). */
  arrived(): void {
    const w = this.#waiters;
    this.#waiters = [];
    for (const f of w) f();
  }

  /** How many records a topic holds now, and the next offset it gives. */
  held(topic: string): { readonly records: number; readonly next: number } {
    const log = this.#logs.get(topic);
    if (log === undefined) throw new Error(`the door holds no topic ${topic}`);
    return { records: log.size, next: log.next };
  }

  #serve(socket: net.Socket): void {
    let pending = Buffer.alloc(0);
    let chain = Promise.resolve();
    socket.on('data', (d: Buffer) => {
      pending = Buffer.concat([pending, d]);
      while (pending.length >= 4) {
        const n = pending.readInt32BE(0);
        if (pending.length < 4 + n) break;
        const frame = pending.subarray(4, 4 + n);
        pending = pending.subarray(4 + n);
        // answers go back in the order asked, a fetch's wait holding the ones behind it
        chain = chain.then(() => this.answer(frame)).then((out) => {
          if (out === null) socket.destroy();
          else socket.write(out);
        }, () => { socket.destroy(); });
      }
    });
    socket.on('error', () => { /* the client went */ });
  }

  /** One request's answer, framed; null for a request the door refuses (said by name). */
  async answer(frame: Buffer): Promise<Buffer | null> {
    const r = new Reader(frame);
    const key = r.i16();
    const version = r.i16();
    const correlation = r.i32();
    r.str();
    const known = ADVERTISED.find(([k]) => k === key);
    // ApiVersions is answered at any version (a client asks it first at its newest and reads the versions answered)
    if (known === undefined || (key !== 18 && (version < known[1] || version > known[2]))) {
      this.counts.refused++;
      this.#say(`the door refuses ${REFUSED_NAMES[key] ?? `the request with API key ${key}`} at version ${version}: it`
        + ' answers ApiVersions 3-4, Metadata 8, ListOffsets 5 and Fetch 11');
      return null;
    }
    if (key === 18 && version >= 3) r.tags();
    const name = API_NAMES[key]!;
    this.counts.requests[name] = (this.counts.requests[name] ?? 0) + 1;
    const w = new Writer().i32(0).i32(correlation);
    if (key === 18) this.#apiVersions(w);
    else if (key === 3) this.#metadata(r, w);
    else if (key === 2) this.#listOffsets(r, w);
    else await this.#fetchRecords(r, w);
    const out = w.done();
    out.writeInt32BE(out.length - 4, 0);
    return out;
  }

  #apiVersions(w: Writer): void {
    w.i16(NONE).uv(ADVERTISED.length + 1);
    for (const [k, lo, hi] of ADVERTISED) w.i16(k).i16(lo).i16(hi).uv(0);
    w.i32(0).uv(0);
  }

  #metadata(r: Reader, w: Writer): void {
    const asked = r.arr(() => r.str()) ?? [...this.#logs.keys()];
    w.i32(0).i32(1).i32(0).str('127.0.0.1').i32(this.#port).str(null).str('legend-door').i32(0);
    w.i32(asked.length);
    for (const t of asked) {
      if (t === null || !this.#logs.has(t)) {
        w.i16(UNKNOWN_TOPIC_OR_PARTITION).str(t).i8(0).i32(0).i32(-2147483648);
        continue;
      }
      // one partition, led by the door: its replicas and in-sync replicas the door alone
      w.i16(NONE).str(t).i8(0).i32(1).i16(NONE).i32(0).i32(0).i32(-1).i32(1).i32(0).i32(1).i32(0).i32(0)
        .i32(-2147483648);
    }
    w.i32(-2147483648);
  }

  #listOffsets(r: Reader, w: Writer): void {
    r.i32();
    r.i8();
    const topics = r.arr(() => ({ name: r.str(), parts: r.arr(() => ({ p: r.i32(), epoch: r.i32(), ts: r.i64() })) ?? [] }))
      ?? [];
    w.i32(0).i32(topics.length);
    for (const t of topics) {
      w.str(t.name).i32(t.parts.length);
      const log = t.name === null ? undefined : this.#logs.get(t.name);
      for (const q of t.parts) {
        if (log === undefined || q.p !== 0) {
          w.i32(q.p).i16(UNKNOWN_TOPIC_OR_PARTITION).i64(-1).i64(-1).i32(-1);
          continue;
        }
        // -2 the earliest, -1 the latest, else the first record at or after the time
        const offset = q.ts === -2 ? log.start : q.ts === -1 ? log.next : log.offsetAt(q.ts);
        w.i32(q.p).i16(NONE).i64(-1).i64(offset).i32(-1);
      }
    }
  }

  async #fetchRecords(r: Reader, w: Writer): Promise<void> {
    r.i32();
    const maxWait = r.i32();
    r.i32();
    const maxBytes = r.i32();
    r.i8();
    r.i32();
    r.i32();
    const topics = r.arr(() => ({
      name: r.str(),
      parts: r.arr(() => ({ p: r.i32(), epoch: r.i32(), offset: r.i64(), logStart: r.i64(), max: r.i32() })) ?? [],
    })) ?? [];
    const wanted = topics.flatMap((t) => {
      const log = t.name === null ? undefined : this.#logs.get(t.name);
      return log === undefined ? [] : t.parts.filter((q) => q.p === 0).map((q) => ({ log, offset: q.offset }));
    });
    if (wanted.length > 0 && wanted.every((x) => x.offset >= x.log.next)) {
      await new Promise<void>((resolve) => {
        const timer = setTimeout(resolve, maxWait);
        this.#waiters.push(() => { clearTimeout(timer); resolve(); });
      });
    }
    w.i32(0).i16(NONE).i32(0).i32(topics.length);
    let budget = maxBytes > 0 ? maxBytes : Number.MAX_SAFE_INTEGER;
    for (const t of topics) {
      w.str(t.name).i32(t.parts.length);
      const log = t.name === null ? undefined : this.#logs.get(t.name);
      for (const q of t.parts) {
        if (log === undefined || q.p !== 0) {
          w.i32(q.p).i16(UNKNOWN_TOPIC_OR_PARTITION).i64(-1).i64(-1).i64(-1).i32(-1).i32(-1).i32(-1);
          continue;
        }
        const inRange = q.offset >= log.start && q.offset <= log.next;
        w.i32(q.p).i16(inRange ? NONE : OFFSET_OUT_OF_RANGE).i64(log.next).i64(log.next).i64(log.start).i32(-1)
          .i32(-1);
        const records = inRange ? log.read(q.offset, Math.min(q.max, budget)) : [];
        if (records.length === 0) {
          w.i32(0);
          continue;
        }
        const b = batch(records);
        budget -= b.length;
        this.counts.fetched += records.length;
        this.counts.fetchBytes += b.length;
        w.i32(b.length).raw(b);
      }
    }
  }
}

/** Records as one record batch v2 (Kafka's message format 2), uncompressed, CreateTime, their offsets their own. */
export function batch(records: readonly DoorRecord[]): Buffer {
  const first = records[0]!;
  const last = records[records.length - 1]!;
  const body = new Writer();
  for (const e of records) {
    const rec = new Writer().i8(0).zz(e.ts - first.ts).zz(e.offset - first.offset);
    if (e.key === null) rec.zz(-1);
    else rec.zz(e.key.length).raw(e.key);
    rec.zz(e.value.length).raw(e.value).zz(0);
    const bytes = rec.done();
    body.zz(bytes.length).raw(bytes);
  }
  const w = new Writer();
  // base offset, length (set below), leader epoch, magic, crc (below), attributes, last offset delta, first and last
  // times, producer id and epoch, base sequence, the records' count
  w.i64(first.offset).i32(0).i32(-1).i8(2).u32(0).i16(0).i32(last.offset - first.offset).i64(first.ts).i64(last.ts)
    .i64(-1).i16(-1).i32(-1).i32(records.length).raw(body.done());
  const b = w.done();
  b.writeInt32BE(b.length - 12, 8);
  b.writeUInt32BE(crc32c(b, 21, b.length), 17);
  return b;
}

const CRC = new Int32Array(256);
for (let n = 0; n < 256; n++) {
  let c = n;
  for (let k = 0; k < 8; k++) c = c & 1 ? (c >>> 1) ^ 0x82f63b78 : c >>> 1;
  CRC[n] = c;
}

/** CRC-32C (Castagnoli) of b[from, to): the checksum a record batch carries. */
export function crc32c(b: Uint8Array, from = 0, to = b.length): number {
  let c = ~0;
  for (let i = from; i < to; i++) c = CRC[(c ^ b[i]!) & 0xff]! ^ (c >>> 8);
  return ~c >>> 0;
}

/** Kafka's big-endian encoding, written. */
export class Writer {
  #b = Buffer.alloc(1024);
  #n = 0;

  #room(k: number): void {
    if (this.#n + k <= this.#b.length) return;
    const nb = Buffer.alloc(Math.max(this.#b.length * 2, this.#n + k));
    this.#b.copy(nb, 0, 0, this.#n);
    this.#b = nb;
  }

  i8(v: number): this { this.#room(1); this.#b.writeInt8(v, this.#n); this.#n += 1; return this; }
  i16(v: number): this { this.#room(2); this.#b.writeInt16BE(v, this.#n); this.#n += 2; return this; }
  i32(v: number): this { this.#room(4); this.#b.writeInt32BE(v, this.#n); this.#n += 4; return this; }
  u32(v: number): this { this.#room(4); this.#b.writeUInt32BE(v, this.#n); this.#n += 4; return this; }
  i64(v: number): this { this.#room(8); this.#b.writeBigInt64BE(BigInt(v), this.#n); this.#n += 8; return this; }
  /** An unsigned varint. */
  uv(v: number): this {
    let x = v;
    while (x >= 0x80) {
      this.#byte((x % 128) | 0x80);
      x = Math.floor(x / 128);
    }
    return this.#byte(x);
  }
  /** A zigzag varint (a record's fields). */
  zz(v: number): this { return this.uv(v >= 0 ? 2 * v : -2 * v - 1); }
  /** A string, its length an int16 (-1: null). */
  str(s: string | null): this {
    if (s === null) return this.i16(-1);
    const x = Buffer.from(s);
    return this.i16(x.length).raw(x);
  }
  raw(x: Uint8Array): this { this.#room(x.length); this.#b.set(x, this.#n); this.#n += x.length; return this; }
  #byte(v: number): this { this.#room(1); this.#b[this.#n++] = v; return this; }
  done(): Buffer { return this.#b.subarray(0, this.#n); }
}

/** Kafka's big-endian encoding, read. */
export class Reader {
  readonly #b: Buffer;
  p = 0;

  constructor(b: Buffer) {
    this.#b = b;
  }

  i8(): number { const v = this.#b.readInt8(this.p); this.p += 1; return v; }
  i16(): number { const v = this.#b.readInt16BE(this.p); this.p += 2; return v; }
  i32(): number { const v = this.#b.readInt32BE(this.p); this.p += 4; return v; }
  u32(): number { const v = this.#b.readUInt32BE(this.p); this.p += 4; return v; }
  i64(): number { const v = this.#b.readBigInt64BE(this.p); this.p += 8; return Number(v); }
  str(): string | null {
    const n = this.i16();
    if (n < 0) return null;
    const s = this.#b.toString('utf8', this.p, this.p + n);
    this.p += n;
    return s;
  }
  uv(): number {
    let v = 0;
    let m = 1;
    let x: number;
    do {
      x = this.#b[this.p++]!;
      v += (x & 0x7f) * m;
      m *= 128;
    } while (x & 0x80);
    return v;
  }
  zz(): number { const u = this.uv(); return u % 2 === 0 ? u / 2 : -(u + 1) / 2; }
  bytes(n: number): Buffer { const b = this.#b.subarray(this.p, this.p + n); this.p += n; return b; }
  /** A request's tagged fields, skipped. */
  tags(): void {
    for (let n = this.uv(); n > 0; n--) {
      this.uv();
      this.p += this.uv();
    }
  }
  arr<T>(f: () => T): T[] | null {
    const n = this.i32();
    if (n < 0) return null;
    const out: T[] = [];
    for (let i = 0; i < n; i++) out.push(f());
    return out;
  }
}
