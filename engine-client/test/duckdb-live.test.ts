// The live DuckDB (duckdb-live.ts, docs/DUCKDB_LIVE_2026_10_25.md L1) against a real DuckDB-WASM, its tables an ingest's
// datasets landed by the planner's plans (an ingest's plans, docs/DESK_DATA_ROADMAP_2026_10_29.md C0): what a grid's
// reader is told and when, what runs again at an emit and what does not, a keyed table's latest row a key, a table's
// retention, and one snapshot an emit -- every statement each connection is sent, counted. What a test reads of a table
// is its own SQL, on a connection of its own: a test is not a host (tools/guards/host_sql.txt reads product sources).

import assert from 'node:assert/strict';
import { after, before, describe, it } from 'node:test';

import { DuckDbEngine, type ArrowishConnection } from '../src/duckdb.ts';
import type { RawTable } from '../src/engine.ts';
import { LiveDuckDb, type DuckDbLiveReader, type LiveRows } from '../src/duckdb-live.ts';
import type { WasmGrammar } from '../src/legend/wasm-grammar.ts';
import type { LiveEvent } from '../src/live.ts';
import { startNodeDuckDb, type NodeDuckDb } from '../src/node-require.ts';
import type { Plan } from '../src/relation-type.ts';
import { runfilesFromEnv } from '../../tools/js/runfiles.mts';
import { loadPlanner } from './planner.ts';
import { unplanned } from './unplanned.ts';

let db: NodeDuckDb;

/** The planner, which writes every statement the live engine runs (D5). */
let planner: WasmGrammar;

/** Every statement sent on a connection the live engine opened, by the connection (0: the writer's, then each reader's). */
const sent: string[][] = [];

/** Called with each statement once it is sent, on whichever connection: a test's write landing at a chosen moment. */
let afterSent: ((sql: string) => void) | null = null;

/** A connection of `db`'s that counts what it is sent. */
function counted(): ArrowishConnection {
  const conn = db.connect() as ArrowishConnection;
  const mine: string[] = [];
  sent.push(mine);
  return new Proxy(conn, {
    get(target, property) {
      const member: unknown = Reflect.get(target, property);
      if (typeof member !== 'function') return member;
      return (...args: unknown[]) => {
        const answer = (member as (...a: unknown[]) => unknown).apply(target, args);
        if (property === 'query' || property === 'send') {
          mine.push(String(args[0]));
          afterSent?.(String(args[0]));
        }
        return answer;
      };
    },
  });
}

/** The tables' ingest: four datasets of one Stream, appended (the prints keeping their latest 10) or latest by key. */
const MODEL = `###Lakehouse
Ingest live::Feed Stream
[
  PRINTS( SYMBOL: Varchar(8), SEQ: BigInt[1], PRICE: Double ) pk=[ SEQ ] writeMode=AppendOnly retention=LastRows(10);
  NEWS( HEADLINE: Varchar(200)[1] ) pk=[ HEADLINE ] writeMode=AppendOnly;
  QUOTES( SYMBOL: Varchar(8)[1], BID: Double, ASK: Double ) pk=[ SYMBOL ] writeMode=LatestByKey;
  TAPE( SEQ: BigInt[1], PRINT_TIME: Timestamp, LIVE: Boolean ) pk=[ SEQ ] writeMode=AppendOnly;
]

###Relational
Database live::DB
(
  include Ingest live::Feed
)
`;

/** A dataset's landing, asked of the planner as the tab's live market asks it. */
const ask = (table: string, model = MODEL) => planner.ingestPlans({
  databaseType: 'DuckDB', model, database: 'live::DB', schema: 'default', table, held: 'batch',
});

/** A live engine of its own, its tables made by their plans and declared: emitted by the test, no timer. `whole`: its
 *  connections read each result in one round trip (duckdb.ts), as the tab's live market's are. */
async function opened(whole = false): Promise<LiveDuckDb> {
  sent.length = 0;
  const live = await LiveDuckDb.open({ connect: () => new DuckDbEngine(counted(), { whole }), host: planner, timer: false });
  await live.declare(await Promise.all(['PRINTS', 'NEWS', 'QUOTES', 'TAPE'].map(async (name) => ({ name, held: 'batch', plans: await ask(name) }))));
  return live;
}

/** A table read by the test itself, on a connection of its own. */
async function look(sql: string): Promise<RawTable> {
  const engine = new DuckDbEngine(db.connect() as ArrowishConnection);
  try {
    return await engine.run(unplanned(sql), 0);
  } finally {
    await engine.close();
  }
}

const SYMBOLS = ['AAPL', 'MSFT', 'NVDA'];

/** `n` prints, sequenced from `from`, each of SYMBOLS[i % 3] (or `symbol`), priced 100 + its sequence. */
function prints(from: number, n: number, symbol?: number): [number, LiveRows] {
  return [n, {
    SYMBOL: { text: SYMBOLS, pick: (i) => symbol ?? i % 3 },
    SEQ: { integers: (i) => from + i },
    PRICE: { doubles: (i) => 100 + from + i },
  }];
}

const plan = (sql: string, ...columns: [string, string][]): Plan => ({ sql: unplanned(sql), columns: columns.map(([name, type]) => ({ name, type })) });
const COUNT = plan('SELECT count(*) AS n FROM PRINTS', ['n', 'Integer']);
const NEWS = plan('SELECT count(*) AS n FROM NEWS', ['n', 'Integer']);
const value = (r: { columns: readonly { values: readonly unknown[] }[] }): unknown => {
  const v = r.columns[0]?.values[0];
  return typeof v === 'bigint' ? Number(v) : v;
};

/** A reader that has read `plans` in the view of epoch 1, followed, and what it is told. */
async function following(live: LiveDuckDb, ...plans: Plan[]): Promise<{ reader: DuckDbLiveReader; told: LiveEvent[] }> {
  const reader = await live.reader();
  for (const p of plans) await reader.execute(p, 1);
  await reader.follow(1);
  const told: LiveEvent[] = [];
  reader.onLive((e) => told.push(e));
  return { reader, told };
}

before(async () => {
  planner = await loadPlanner();
  db = await startNodeDuckDb(runfilesFromEnv('DUCKDB_EXTENSIONS'));
});

describe('the live DuckDB', () => {
  let live: LiveDuckDb;
  after(async () => { await live?.close(); });

  it('a followed result runs again when a table it reads is appended to, and its reader is told; one over another table does not', async () => {
    live = await opened();
    const { reader, told } = await following(live, COUNT, NEWS);
    const readerSent = sent[1]!;
    const before = readerSent.length;

    await live.write('PRINTS', ...prints(0, 3));
    await live.emit();

    assert.equal(told.length, 1, 'told once');
    assert.equal(told[0]!.ended, null);
    assert.deepEqual(readerSent.slice(before).filter((s) => s.startsWith('SELECT')), [COUNT.sql], 'only the result over PRINTS ran again');
    // the grid's run at the tick: answered from what the emit kept, nothing sent
    const ticked = readerSent.length;
    const count = await reader.execute(COUNT, 2);
    const news = await reader.execute(NEWS, 2);
    assert.equal(value(count), 3);
    assert.equal(value(news), 0);
    assert.equal(count.elapsedMs, 0);
    assert.equal(count.epoch, 2);
    assert.equal(readerSent.length, ticked, 'nothing ran for the tick');
    await live.close();
  });

  it('a reader whose grid is not shown rests: an emit runs none of its results, until its grid runs again', async () => {
    // 2026-10-27, the desk in the tab: every tab visited, every hidden grid ran at each emit (an emit 7.1 s)
    live = await opened();
    const { reader, told } = await following(live, COUNT);
    const readerSent = sent[1]!;
    reader.rest();
    assert.equal(reader.following, 0, 'resting, it follows nothing');
    const before = readerSent.length;
    await live.write('PRINTS', ...prints(0, 3));
    await live.emit();
    assert.equal(told.length, 0, 'a resting reader is told nothing');
    assert.equal(readerSent.slice(before).filter((s) => s.startsWith('SELECT')).length, 0, 'nothing of it ran at the emit');
    // its sheet shown again: the grid runs -- a tick of the view it had, no view landing and no `follow` told (the
    // user, 2026-10-29: "stops ticking if you change tabs and come back") -- and it follows again what it followed
    assert.equal(value(await reader.execute(COUNT, 1)), 3);
    assert.equal(reader.following, 1, 'running again, it follows again');
    await live.write('PRINTS', ...prints(3, 2));
    await live.emit();
    assert.equal(told.length, 1, 'following again, told');
    assert.equal(value(await reader.execute(COUNT, 3)), 5);
    await live.close();
  });

  it('an emit with no change runs nothing and tells nothing; a change that leaves a result as it was runs it, untold', async () => {
    live = await opened();
    const msft = plan("SELECT max(PRICE) AS top FROM PRINTS WHERE SYMBOL = 'MSFT'", ['top', 'Float']);
    const { told } = await following(live, msft);
    await live.emit();
    assert.equal(live.counts.reruns, 0);
    await live.write('PRINTS', ...prints(0, 2, 0)); // AAPL's: MSFT's top is as it was (none)
    await live.emit();
    assert.equal(live.counts.reruns, 1);
    assert.equal(live.counts.unchanged, 1);
    assert.equal(told.length, 0);
    await live.write('PRINTS', ...prints(2, 1, 1)); // MSFT's
    await live.emit();
    assert.equal(told.length, 1);
    await live.close();
  });

  it('a keyed table holds the latest row a key: a batch\'s last row of a key wins, a later batch replaces it', async () => {
    live = await opened();
    const quotes = (rows: [number, number, number][]): [number, LiveRows] => [rows.length, {
      SYMBOL: { text: SYMBOLS, pick: (i) => rows[i]![0] },
      BID: { doubles: (i) => rows[i]![1] },
      ASK: { doubles: (i) => rows[i]![2] },
    }];
    await live.write('QUOTES', ...quotes([[0, 1, 2], [1, 10, 11], [0, 3, 4]]));
    await live.write('QUOTES', ...quotes([[1, 12, 13], [2, 20, 21]]));
    const book = await look('SELECT SYMBOL, BID, ASK FROM QUOTES ORDER BY SYMBOL');
    assert.deepEqual(book.columns.map((c) => c.values), [['AAPL', 'MSFT', 'NVDA'], [3, 12, 20], [4, 13, 21]]);
    await assert.rejects(live.write('NOPE', ...prints(0, 1)), /NOPE is not a live table: declare it first/);
    await live.close();
  });

  it('a table keeps its retention, its latest rows by arrival, at the plan\'s cadence; a trim is a change; a time retention is refused here', async () => {
    live = await opened();
    const { told } = await following(live, COUNT);
    await live.write('PRINTS', ...prints(0, 25));
    await live.emit();
    const kept = await look('SELECT min(SEQ) AS lo, max(SEQ) AS hi, count(*) AS n FROM PRINTS');
    assert.deepEqual(kept.columns.map((c) => Number(c.values[0])), [15, 24, 10]);
    assert.equal(told.length, 1);
    assert.equal(live.counts.trims, 1);
    // a duration reads when each row arrived, which a DuckDB table does not record
    await assert.rejects(ask('PRINTS', MODEL.replace('retention=LastRows(10)', "retention=Last('PT30S')")),
      /keeps Last\('PT30S'\), which reads when each row arrived/);
    await live.close();
  });

  it('a result that reads the clock runs again at every emit, told only when it changes', async () => {
    live = await opened();
    // now() a TIMESTAMP: this session has no time zone loaded (the planner's sets one: engine.ts setUpSession)
    const recent = plan('SELECT count(*) AS n FROM TAPE WHERE PRINT_TIME > CAST(now() AS TIMESTAMP) - INTERVAL 1 HOUR', ['n', 'Integer']);
    const { told } = await following(live, recent);
    await live.emit();
    await live.emit();
    assert.equal(live.counts.reruns, 2, 'each emit ran it');
    assert.equal(told.length, 0, 'the same count: untold');
    await live.close();
  });

  it('what a reader follows is its view\'s: a result read before the view followed is let go, one read after it is followed', async () => {
    live = await opened();
    const reader = await live.reader();
    await reader.execute(NEWS, 1);
    await reader.follow(1);
    assert.equal(reader.following, 1);
    await reader.execute(COUNT, 2);
    await reader.follow(2);
    assert.equal(reader.following, 1, 'the view of epoch 2 read only COUNT');
    // a chart drawn after its grid's view: followed as it is read
    const top = plan('SELECT max(PRICE) AS top FROM PRINTS', ['top', 'Float']);
    await reader.execute(top, 2);
    assert.equal(reader.following, 2);
    const told: LiveEvent[] = [];
    reader.onLive((e) => told.push(e));
    await live.write('NEWS', 1, { HEADLINE: { text: ['Fed holds'], pick: () => 0 } });
    await live.emit();
    assert.equal(told.length, 0, 'NEWS is no longer followed');
    await live.close();
  });

  it('an emit\'s runs read one snapshot: a write landing among them is seen by none, and runs them again at the next', async () => {
    live = await opened();
    const again = plan('SELECT count(*) AS m FROM PRINTS', ['m', 'Integer']);
    const { reader } = await following(live, COUNT, again);
    await live.write('PRINTS', ...prints(0, 2));
    // a write on a connection of its own the moment the emit's first run is sent, before its second
    const elsewhere = db.connect() as ArrowishConnection;
    afterSent = (sql) => {
      if (sql !== COUNT.sql) return;
      afterSent = null;
      void elsewhere.query(unplanned("INSERT INTO PRINTS VALUES ('NVDA', 99, 1.0)"));
    };
    await live.emit();
    assert.equal(afterSent, null, 'the write landed among the emit\'s runs');
    assert.equal(value(await reader.execute(COUNT, 2)), 2);
    assert.equal(value(await reader.execute(again, 2)), 2, 'both read the moment before it');
    assert.equal(value(await look('SELECT count(*) AS n FROM PRINTS')), 3);
    await live.close();
  });

  it('a batch lands by its plan alone: each column by its name, whatever its order, every statement the plan\'s', async () => {
    live = await opened(true);
    const writer = sent[0]!;
    const before = writer.length;
    await live.write('PRINTS', ...prints(0, 3));
    // the same columns in another order: by name (by position it would put prices in SEQ)
    const [n, rows] = prints(3, 2);
    await live.write('PRINTS', n, { PRICE: rows.PRICE!, SYMBOL: rows.SYMBOL!, SEQ: rows.SEQ! });
    const landed = writer.slice(before);
    const { land } = await ask('PRINTS');
    assert.ok(landed.length > 0 && landed.every((sql) => land.includes(JSON.stringify(sql))), `the land plan's statements alone: ${landed.join(' | ')}`);
    const table = await look('SELECT SYMBOL, SEQ, PRICE FROM PRINTS ORDER BY SEQ');
    assert.deepEqual(table.columns.map((c) => c.values.map((v) => (typeof v === 'bigint' ? Number(v) : v))), [
      ['AAPL', 'MSFT', 'NVDA', 'AAPL', 'MSFT'], [0, 1, 2, 3, 4], [100, 101, 102, 103, 104],
    ]);
    // a reader over whole connections: followed, told, and its tick answered from what the emit kept
    const { reader, told } = await following(live, COUNT);
    await live.write('PRINTS', ...prints(5, 1));
    await live.emit();
    assert.equal(told.length, 1);
    const ticked = await reader.execute(COUNT, 2);
    assert.equal(value(ticked), 6);
    assert.equal(ticked.elapsedMs, 0);
    await live.close();
  });

  it('a closed reader is told nothing, and its connection is closed', async () => {
    live = await opened();
    const { reader, told } = await following(live, COUNT);
    await reader.close();
    await live.write('PRINTS', ...prints(0, 1));
    await live.emit();
    assert.equal(told.length, 0);
    assert.equal(live.counts.reruns, 0);
    await live.close();
    await assert.rejects(live.reader(), /the live DuckDB is closed/);
  });
});
