// THE DOOR (datacube/demo/market-sim/door, docs/DESK_DATA_ROADMAP_2026_10_29.md B1): its protocol spoken over a socket
// by a client of the test's own, as Deephaven's reader speaks it -- versions asked, a topic's metadata, offsets by end
// and by time, records fetched as a record batch whose checksum is CRC-32C -- an appended topic keeping its retention, a
// compacted one each key's latest, a fetch at the end waiting for records, and a request the door does not answer
// refused by name. Then the world's rows as records, and the desk model's topics as the planner answers them.

import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import net from 'node:net';
import { after, before, describe, it } from 'node:test';

import { runfileFromEnv } from '../../tools/js/runfiles.mts';
import { doorSink, doorTopics, durationMs, topicsOf, type StreamTopic } from '../demo/market-sim/door/door-feed.ts';
import { crc32c, KafkaDoor, Reader, Writer } from '../demo/market-sim/door/kafka-door.ts';
import { MARKET_TRADE, WORLD } from '../demo/market-sim/tables.ts';
import { DESK_MODEL, ON_DEEPHAVEN } from '../demo/deephaven/market/model.ts';
import { plannerFor } from './catalog-builder.ts';

/** A Kafka client of the test's own: one connection, one request at a time. */
class Client {
  readonly #socket: net.Socket;
  #pending = Buffer.alloc(0);
  #waiting: ((frame: Buffer | null) => void) | null = null;
  #correlation = 0;
  closed = false;

  private constructor(socket: net.Socket) {
    this.#socket = socket;
    socket.on('data', (d: Buffer) => {
      this.#pending = Buffer.concat([this.#pending, d]);
      if (this.#pending.length < 4) return;
      const n = this.#pending.readInt32BE(0);
      if (this.#pending.length < 4 + n) return;
      const frame = this.#pending.subarray(4, 4 + n);
      this.#pending = this.#pending.subarray(4 + n);
      this.#waiting?.(frame);
    });
    socket.on('close', () => { this.closed = true; this.#waiting?.(null); });
  }

  static connect(port: number): Promise<Client> {
    return new Promise((resolve, reject) => {
      const s = net.connect(port, '127.0.0.1', () => { resolve(new Client(s)); });
      s.on('error', reject);
    });
  }

  /** A request of `api` at `version`, its body written by `body`; the answer's body (after its correlation id), or null
   *  when the door closed the connection instead. */
  ask(api: number, version: number, body: (w: Writer) => void): Promise<Reader | null> {
    const correlation = ++this.#correlation;
    const w = new Writer().i32(0).i16(api).i16(version).i32(correlation).str('test');
    if (api === 18 && version >= 3) w.uv(0);
    body(w);
    const frame = w.done();
    frame.writeInt32BE(frame.length - 4, 0);
    return new Promise((resolve) => {
      this.#waiting = (answer) => {
        this.#waiting = null;
        if (answer === null) {
          resolve(null);
          return;
        }
        const r = new Reader(answer);
        assert.equal(r.i32(), correlation, 'the answer is the request\'s');
        resolve(r);
      };
      this.#socket.write(frame);
    });
  }

  close(): void {
    this.#socket.destroy();
  }
}

/** A ListOffsets v5 of one partition of `topic` at `ts` (-2 the earliest, -1 the latest): its error and offset. */
async function offsetOf(c: Client, topic: string, ts: number): Promise<{ error: number; offset: number }> {
  const r = (await c.ask(2, 5, (w) => w.i32(-1).i8(0).i32(1).str(topic).i32(1).i32(0).i32(-1).i64(ts)))!;
  r.i32();
  r.i32();
  r.str();
  r.i32();
  r.i32();
  const error = r.i16();
  r.i64();
  return { error, offset: r.i64() };
}

interface Fetched {
  readonly error: number;
  readonly highWatermark: number;
  readonly logStart: number;
  readonly records: { offset: number; ts: number; key: string | null; value: string }[];
}

/** A Fetch v11 of one partition of `topic` from `offset`, waiting up to `waitMs`: its records, each batch's checksum
 *  checked. */
async function fetchRecords(c: Client, topic: string, offset: number, waitMs = 0): Promise<Fetched> {
  const r = (await c.ask(1, 11, (w) => w.i32(-1).i32(waitMs).i32(1).i32(1 << 20).i8(0).i32(0).i32(-1)
    .i32(1).str(topic).i32(1).i32(0).i32(-1).i64(offset).i64(-1).i32(1 << 20).i32(0).str('')))!;
  r.i32();
  assert.equal(r.i16(), 0);
  r.i32();
  r.i32();
  r.str();
  r.i32();
  r.i32();
  const error = r.i16();
  const highWatermark = r.i64();
  r.i64();
  const logStart = r.i64();
  r.i32();
  r.i32();
  const size = r.i32();
  const records: Fetched['records'] = [];
  if (size > 0) {
    const batch = r.bytes(size);
    const b = new Reader(batch);
    const base = b.i64();
    const length = b.i32();
    assert.equal(length, batch.length - 12, 'the batch\'s length is its bytes past it');
    b.i32();
    assert.equal(b.i8(), 2, 'record batch v2');
    const crc = b.u32();
    assert.equal(crc, crc32c(batch, 21, batch.length), 'the batch\'s checksum is its CRC-32C');
    b.i16();
    b.i32();
    const firstTs = b.i64();
    b.i64();
    b.i64();
    b.i16();
    b.i32();
    const count = b.i32();
    for (let i = 0; i < count; i++) {
      b.zz();
      b.i8();
      const tsDelta = b.zz();
      const offsetDelta = b.zz();
      const keyLength = b.zz();
      const key = keyLength < 0 ? null : b.bytes(keyLength).toString();
      const value = b.bytes(b.zz()).toString();
      b.zz();
      records.push({ offset: base + offsetDelta, ts: firstTs + tsDelta, key, value });
    }
  }
  return { error, highWatermark, logStart, records };
}

describe('the door', () => {
  const refusals: string[] = [];
  let door: KafkaDoor;
  let port: number;
  let client: Client;

  before(async () => {
    door = new KafkaDoor([
      { topic: 'tp.trades.v1', compacted: false, keepRows: 50, keepForMs: null },
      { topic: 'tp.quotes.v1', compacted: true, keepRows: null, keepForMs: null },
      { topic: 'tp.news.v1', compacted: false, keepRows: null, keepForMs: 10_000 },
    ], (m) => refusals.push(m));
    port = await door.listen();
    client = await Client.connect(port);
  });
  after(async () => {
    client.close();
    await door.close();
  });

  it('checks a batch with CRC-32C, the Castagnoli polynomial', () => {
    assert.equal(crc32c(Buffer.from('123456789')), 0xe3069283);
  });

  it('answers the versions it speaks, and its topics\' metadata: one partition each, an unknown topic said so', async () => {
    const v = (await client.ask(18, 3, () => { /* no body */ }))!;
    assert.equal(v.i16(), 0);
    const apis = Array.from({ length: v.uv() - 1 }, () => {
      const a = [v.i16(), v.i16(), v.i16()];
      v.uv();
      return a;
    });
    assert.deepEqual(apis, [[18, 3, 4], [3, 8, 8], [2, 5, 5], [1, 11, 11]]);
    const m = (await client.ask(3, 8, (w) => w.i32(2).str('tp.trades.v1').str('nope').i8(0).i8(0).i8(0)))!;
    m.i32();
    assert.equal(m.i32(), 1, 'one broker: the door');
    m.i32();
    assert.equal(m.str(), '127.0.0.1');
    assert.equal(m.i32(), port);
    m.str();
    m.str();
    m.i32();
    assert.equal(m.i32(), 2);
    assert.equal(m.i16(), 0);
    assert.equal(m.str(), 'tp.trades.v1');
    m.i8();
    assert.equal(m.i32(), 1, 'one partition');
    assert.equal(m.i16(), 0);
    assert.equal(m.i32(), 0, 'partition 0');
    assert.equal(m.i32(), 0, 'led by the door');
    m.i32();
    assert.deepEqual(m.arr(() => m.i32()), [0], 'its replicas: the door');
    assert.deepEqual(m.arr(() => m.i32()), [0], 'in sync: the door');
    assert.deepEqual(m.arr(() => m.i32()), [], 'none offline');
    m.i32();
    assert.equal(m.i16(), 3, 'an unknown topic: UNKNOWN_TOPIC_OR_PARTITION');
    assert.equal(m.str(), 'nope');
  });

  it('serves an appended topic\'s records from an offset, in order, and keeps its latest rows', async () => {
    for (let i = 0; i < 40; i++) door.append('tp.trades.v1', 1000 + i, null, JSON.stringify({ SEQ: i }));
    door.arrived();
    const all = await fetchRecords(client, 'tp.trades.v1', 0);
    assert.equal(all.error, 0);
    assert.equal(all.highWatermark, 40);
    assert.deepEqual(all.records.map((r) => (JSON.parse(r.value) as { SEQ: number }).SEQ), Array.from({ length: 40 }, (_, i) => i));
    assert.deepEqual(all.records.map((r) => r.offset), Array.from({ length: 40 }, (_, i) => i));
    assert.equal(all.records[5]!.ts, 1005);
    // past its retention by a tenth: the oldest go, and an offset before the first is out of range
    for (let i = 40; i < 120; i++) door.append('tp.trades.v1', 1000 + i, null, JSON.stringify({ SEQ: i }));
    const { records, next } = door.held('tp.trades.v1');
    assert.equal(next, 120);
    assert.ok(records >= 50 && records <= 55, `its latest 50, a tenth over before a trim: ${records}`);
    const start = await offsetOf(client, 'tp.trades.v1', -2);
    assert.equal(start.offset, 120 - records);
    assert.equal((await fetchRecords(client, 'tp.trades.v1', 0)).error, 1, 'OFFSET_OUT_OF_RANGE');
    assert.equal((await offsetOf(client, 'tp.trades.v1', -1)).offset, 120);
    // by time: the first record at or after it
    assert.equal((await offsetOf(client, 'tp.trades.v1', 1100)).offset, 100);
  });

  it('keeps a compacted topic\'s latest record of each key, the offsets with the gaps compaction leaves', async () => {
    const last = new Map<string, number>();
    for (let i = 0; i < 3000; i++) {
      const sym = `S${i % 7}`;
      last.set(sym, i);
      door.append('tp.quotes.v1', 2000 + i, JSON.stringify([sym]), JSON.stringify({ SYM: sym, N: i }));
    }
    assert.ok(door.held('tp.quotes.v1').records < 3000, 'compacted');
    const read: { offset: number; key: string | null; value: string }[] = [];
    for (let at = 0; at < 3000;) {
      const f = await fetchRecords(client, 'tp.quotes.v1', at);
      if (f.records.length === 0) break;
      read.push(...f.records);
      at = f.records[f.records.length - 1]!.offset + 1;
    }
    const latest = new Map<string, number>();
    for (const r of read) latest.set((JSON.parse(r.key!) as string[])[0]!, (JSON.parse(r.value) as { N: number }).N);
    assert.deepEqual(latest, last, 'each key\'s latest record, read from the beginning');
    assert.ok(read.every((r, i) => i === 0 || r.offset > read[i - 1]!.offset), 'in offset order');
  });

  it('a fetch at the end waits for records, and returns when they arrive', async () => {
    const t0 = performance.now();
    const empty = await fetchRecords(client, 'tp.news.v1', 0, 150);
    assert.equal(empty.records.length, 0);
    assert.ok(performance.now() - t0 >= 140, 'it waited its wait');
    const waiting = fetchRecords(client, 'tp.news.v1', 0, 5000);
    setTimeout(() => {
      door.append('tp.news.v1', Date.now(), null, '{"H":"one"}');
      door.arrived();
    }, 50);
    const f = await waiting;
    assert.deepEqual(f.records.map((r) => r.value), ['{"H":"one"}']);
  });

  it('refuses a request it does not answer by name, and closes the connection', async () => {
    const other = await Client.connect(port);
    assert.equal(await other.ask(0, 9, (w) => w.i16(-1)), null, 'Produce: the connection closed');
    assert.match(refusals.at(-1)!, /^the door refuses Produce at version 9: it answers ApiVersions 3-4, Metadata 8/);
    const again = await Client.connect(port);
    assert.equal(await again.ask(3, 12, (w) => w.i32(-1)), null);
    assert.match(refusals.at(-1)!, /refuses Metadata at version 12/);
    assert.equal(door.counts.refused, 2);
  });
});

describe('the world\'s rows through the door', () => {
  it('writes each row as one record of its topic\'s columns, each value as Deephaven\'s JSON reader takes it', async () => {
    const topics: StreamTopic[] = [
      { table: 'MARKET_TRADE', topic: 'tp.trades', columns: MARKET_TRADE_COLUMNS, key: [], keepRows: 100, keepFor: null },
      { table: 'world', topic: 'market.WorldStatus.world', columns: [{ name: 'item', type: 'java.lang.String' },
        { name: 'state', type: 'java.lang.String' }, { name: 'detail', type: 'java.lang.String' }],
      key: ['item'], keepRows: null, keepFor: null },
    ];
    const door = new KafkaDoor(doorTopics(topics));
    const port = await door.listen();
    const { sink, counts } = doorSink(door, topics, () => 5000);
    await sink(MARKET_TRADE, 2, {
      PRINT_ID: { text: ['P1', 'P2'], pick: (i) => i },
      SEQUENCE_NUMBER: { integers: (i) => i + 1 },
      PRINT_TIME: { instants: () => Date.UTC(2026, 10, 14, 14, 30) },
      PRICE: { doubles: (i) => (i === 0 ? 101.25 : Number.NaN) },
      IS_OFF_EXCHANGE: { truths: (i) => i === 1 },
    });
    await sink(WORLD, 2, { item: { text: ['feed'], pick: () => 0 }, state: { text: ['on', 'off'], pick: (i) => i } });
    // a table no stream reads is written to no topic
    await sink({ name: 'EXCHANGE', shape: {} }, 3, {});
    assert.deepEqual(counts.records, { 'tp.trades': 2, 'market.WorldStatus.world': 2 });
    assert.deepEqual(counts.keys, { 'market.WorldStatus.world': 1 });
    const client = await Client.connect(port);
    const trades = await fetchRecords(client, 'tp.trades', 0);
    assert.deepEqual(trades.records.map((r) => JSON.parse(r.value)), [
      { PRINT_ID: 'P1', SEQUENCE_NUMBER: 1, PRINT_TIME: Date.UTC(2026, 10, 14, 14, 30), PRICE: 101.25, QUANTITY: null,
        IS_OFF_EXCHANGE: false },
      { PRINT_ID: 'P2', SEQUENCE_NUMBER: 2, PRINT_TIME: Date.UTC(2026, 10, 14, 14, 30), PRICE: null, QUANTITY: null,
        IS_OFF_EXCHANGE: true },
    ]);
    assert.ok(trades.records.every((r) => r.key === null && r.ts === 5000));
    const world = await fetchRecords(client, 'market.WorldStatus.world', 0);
    assert.deepEqual(world.records.map((r) => r.key), ['["feed"]', '["feed"]'], 'keyed by the dataset\'s key');
    client.close();
    await door.close();
  });

  it('reads a retention\'s duration, and refuses one it cannot', () => {
    assert.equal(durationMs('PT8H'), 8 * 3_600_000);
    assert.equal(durationMs('P1DT30M'), 24 * 3_600_000 + 30 * 60_000);
    assert.equal(durationMs('PT0.5S'), 500);
    assert.throws(() => durationMs('P1M'), /no duration of days, hours, minutes and seconds/);
    assert.throws(() => durationMs('PT'), /no duration/);
  });
});

const MARKET_TRADE_COLUMNS = ['PRINT_ID', 'SEQUENCE_NUMBER', 'PRINT_TIME', 'PRICE', 'QUANTITY', 'IS_OFF_EXCHANGE']
  .map((name) => ({ name, type: 'java.lang.String' }));

describe('the desk model\'s topics, as the planner answers them', () => {
  it('names each stream\'s topic as its topic store\'s table, the world\'s status its ingest\'s own', async () => {
    const model = DESK_MODEL(readFileSync(runfileFromEnv('TRADING_MODEL'), 'utf8'),
      readFileSync(runfileFromEnv('MARKET_PURE'), 'utf8'), readFileSync(runfileFromEnv('ENVIRONMENTS_PURE'), 'utf8'));
    const planner = plannerFor(model, ON_DEEPHAVEN.runtime);
    const store = topicsOf(await planner.deephaven('topics', { model, database: 'store::DB' }));
    assert.deepEqual(store.map((t) => [t.table, t.topic, t.key.length > 0 ? 'keyed' : `last ${t.keepRows}`]), [
      ['MARKET_TRADE', 'tp.us-equities.trades.v3', 'last 25000'],
      ['MARKET_QUOTE', 'tp.us-equities.nbbo.v2', 'keyed'],
      ['FX_RATE', 'tp.fx.spot.v1', 'keyed'],
      ['NEWS_STORY', 'vendor.newswire.stories.v1', 'last 5000'],
      ['TRADE_ORDER', 'oms.equities.orders.v4', 'keyed'],
      ['EXECUTION', 'oms.equities.fills.v4', 'last 50000'],
      ['ORDER_ROUTE', 'oms.equities.routes.v4', 'last 50000'],
      ['RFQ', 'oms.fx.rfqs.v2', 'keyed'],
    ]);
    const world = topicsOf(await planner.deephaven('topics', { model, database: 'market::World' }));
    assert.deepEqual(world.map((t) => [t.table, t.topic, t.key]), [['world', 'market.WorldStatus.world', ['item']]]);
    // each column of a stream with its Deephaven type: an instant an Instant, a day a LocalDate (read as text)
    const trade = store[0]!.columns;
    assert.deepEqual(trade.slice(0, 3), [{ name: 'PRINT_ID', type: 'java.lang.String' },
      { name: 'SEQUENCE_NUMBER', type: 'int' }, { name: 'PRINT_TIME', type: 'java.time.Instant' }]);
  });
});
