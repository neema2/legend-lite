// DEEPHAVEN FROM THE BROWSER, the page (//datacube:deephaven_test; docs/DEEPHAVEN_PLAN_2026_10_22.md DH21, L3b): served
// from a site that is not the Deephaven server's, it plans each query with the WebAssembly planner on a Deephaven
// runtime, sends it with the tab's sender (engine-client/src/deephaven.ts) straight to the server, and reads the rows
// with the watch (deephaven-live.ts) -- no server of legend-lite's anywhere. The data is six rows with nulls in every
// column, seeded through the test server's console; the known answers are worked out by hand from them, so the test
// needs no second database. Every check runs here; the driver (deephaven.mjs) reports what the page found.

import { DeephavenSession, type DeephavenServer } from '../../../engine-client/src/deephaven.ts';
import { watch, type LiveRows } from '../../../engine-client/src/deephaven-live.ts';
// @ts-ignore the module's runtime, served beside the page from //wasm:planner
import { load } from './wasm-gc-module-runtime.js';

/** The part of Deephaven's JS library the seeding uses: a console session of the test server's. */
interface SeedingClient {
  login(credentials: { readonly type: string; readonly token: string }): Promise<void>;
  getAsIdeConnection(): Promise<{
    startSession(type: string): Promise<{ runCode(code: string): Promise<{ readonly error?: string }> }>;
  }>;
  disconnect(): void;
}

interface Outcome {
  readonly name: string;
  readonly ok: boolean;
  readonly message: string;
}

const verdict: { outcomes: Outcome[]; setupError?: string; done: boolean } = { outcomes: [], done: false };
(window as unknown as { __deephaven: typeof verdict }).__deephaven = verdict;

const MODEL = (port: number): string => `###Relational
Database risk::Ref
(
  Table trades (id INTEGER PRIMARY KEY, region VARCHAR(16), country VARCHAR(16), desk VARCHAR(16),
                year INTEGER, qty BIGINT, notional DOUBLE, pnl DOUBLE, instrument VARCHAR(16))
  Table instrument (iid VARCHAR(16) PRIMARY KEY, sector VARCHAR(16), ccy VARCHAR(3) NOT NULL)
  Table ticks (sym VARCHAR(8), px DOUBLE)
)
###Connection
RelationalDatabaseConnection risk::Conn
{
  store: risk::Ref;
  type: Deephaven;
  specification: Deephaven { host: '127.0.0.1'; port: ${port}; useTls: true; };
  auth: PreSharedKey { keyVaultReference: 'deephaven.psk'; };
}
###Runtime
Runtime risk::RT
{
  mappings: [];
  connections: [ risk::Ref: [ c: risk::Conn ] ];
}`;

// the six rows, as the console builds them (a long column's and a double's null are Deephaven's sentinels)
const SEED = `import static io.deephaven.util.QueryConstants.*
trades = newTable(
  intCol("id", 1, 2, 3, 4, 5, 6),
  stringCol("region", "EMEA", "APAC", "EMEA", null, "AMER", "EMEA"),
  stringCol("country", "UK", "JP", "FR", "US", "BR", "UK"),
  stringCol("desk", "Rates", "Rates", "FX", "FX", "Credit", null),
  intCol("year", 2023, 2023, 2024, 2022, 2023, 2024),
  longCol("qty", 10L, 5L, NULL_LONG, 20L, 1L, 3L),
  doubleCol("notional", 100.0d, 200.0d, 50.0d, NULL_DOUBLE, 10.0d, 30.0d),
  doubleCol("pnl", 5.0d, -3.0d, NULL_DOUBLE, 2.0d, 0.0d, 1.5d),
  stringCol("instrument", "I1", "I2", "I1", "I3", null, "I2"))
instrument = newTable(
  stringCol("iid", "I1", "I2", "I3"),
  stringCol("sector", "Tech", null, "Banks"),
  stringCol("ccy", "USD", "EUR", "JPY"))
ticks = timeTable("PT0.2S").update("sym = \`S\` + (ii % 3)", "px = (double) (ii % 7)")
`;

const T = '#>{risk::Ref.trades}#';

/** Each query and its answer, worked out by hand from the six rows: the platform's sort puts a null last ascending
 *  and first descending; a sum or an average skips a null; a comparison with a null is not true. */
const KNOWN: readonly { readonly query: string; readonly rows: readonly (readonly unknown[])[] }[] = [
  { query: `${T}->groupBy(~[desk], ~[total:x|$x.notional:y|$y->sum()])->sort(~desk->ascending())`,
    rows: [['Credit', 10], ['FX', 50], ['Rates', 300], [null, 30]] },
  { query: `${T}->filter(x|$x.region == 'EMEA')->select(~[id, pnl])->sort(~id->ascending())`,
    rows: [[1, 5], [3, null], [6, 1.5]] },
  { query: `${T}->join(#>{risk::Ref.instrument}#, meta::pure::functions::relation::JoinKind.LEFT,`
      + ` {a, b | $a.instrument == $b.iid})->groupBy(~[sector], ~[q:x|$x.qty:y|$y->sum()])->sort(~sector->ascending())`,
    rows: [['Banks', 20], ['Tech', 10], [null, 9]] },
  { query: `${T}->groupBy(~[region], ~[n:x|$x.id:y|$y->count(), a:x|$x.pnl:y|$y->average()])`
      + `->sort(~region->ascending())`,
    rows: [['AMER', 1, 0], ['APAC', 1, -3], ['EMEA', 3, 3.25], [null, 1, 2]] },
  { query: `${T}->extend(~[sign:x|if($x.pnl > 0.0, |'UP', |'DOWN')])->select(~[id, sign])->sort(~id->ascending())`,
    rows: [[1, 'UP'], [2, 'DOWN'], [3, 'DOWN'], [4, 'UP'], [5, 'DOWN'], [6, 'UP']] },
  { query: `${T}->select(~[id, pnl])->sort(~pnl->descending())->limit(3)`,
    rows: [[3, null], [1, 5], [4, 2]] },
  { query: `${T}->select(~[region])->distinct()->sort(~region->ascending())`,
    rows: [['AMER'], ['APAC'], ['EMEA'], [null]] },
];

/** DataCube's shapes, L2's eighteen (the two the compiler refuses, a string operation on a nullable column, are not
 *  the target's): each must plan, be sent and be created, and answer rows. */
const SHAPES: readonly string[] = [
  `${T}->groupBy(~[region, country], ~['2023__|__total':x|if($x.year == 2023, |$x.notional, |[]):y|$y->sum(),`
    + ` total:x|$x.notional:y|$y->sum()])->sort([~region->ascending(), ~country->ascending()])`,
  `${T}->filter(x|$x.region == 'EMEA')->select(~[year])->distinct()->sort([~year->descending()])->limit(501)`,
  `${T}->extend(~[__root__:x|'[ROOT]'])->groupBy(~[__root__], ~[total:x|$x.notional:y|$y->sum(), n:x|$x.id:y|$y->count()])`,
  `${T}->groupBy(~[desk], ~[a:x|$x.pnl:y|$y->average(), c:x|$x.instrument:y|$y->distinct()->count(),`
    + ` mx:x|$x.pnl:y|$y->max(), mn:x|$x.pnl:y|$y->min(), n:x|$x.pnl:y|$y->count()])->sort(~desk->ascending())`,
  `${T}->groupBy(~[desk], ~[p:x|$x.pnl:y|$y->sum(), n:x|$x.notional:y|$y->sum()])->extend(~[ratio:x|$x.p / $x.n])`
    + `->filter(x|$x.p > 0.0)->sort(~desk->ascending())`,
  `${T}->filter(x|$x.region->in(['EMEA', 'APAC']))->select(~[id, region, pnl])`
    + `->sort([~pnl->descending(), ~id->ascending()])->limit(10)`,
  `${T}->groupBy(~[desk], ~[s:x|$x.pnl:y|$y->stdDevSample(), m:x|$x.pnl:y|$y->median()])->sort(~desk->ascending())`,
  `#>{risk::Ref.instrument}#->join(${T}, meta::pure::functions::relation::JoinKind.INNER,`
    + ` {a, b | $a.iid == $b.instrument})->select(~[iid, sector, id, qty])->sort([~id->ascending()])`,
  `${T}->select(~[id, pnl])->sort([~pnl->ascending(), ~id->ascending()])->limit(15)`,
  `${T}->filter(x|$x.region->isEmpty())->select(~[id, desk])->sort(~id->ascending())`,
  `${T}->join(#>{risk::Ref.instrument}#, meta::pure::functions::relation::JoinKind.LEFT,`
    + ` {a, b | $a.instrument == $b.iid})->groupBy(~[sector], ~[s:x|$x.pnl:y|$y->sum(), n:x|$x.id:y|$y->count()])`
    + `->sort(~sector->ascending())`,
  `${T}->filter(x|$x.qty > 1000000000)->groupBy(~[], ~[t:x|$x.notional:y|$y->sum(), n:x|$x.id:y|$y->count()])`,
  `${T}->filter(x|!($x.pnl > 0.0))->select(~[id])->sort(~id->ascending())`,
  `${T}->filter(x|($x.pnl > 0.0) || ($x.qty < 10))->select(~[id])->sort(~id->ascending())`,
  `${T}->extend(~[up:x|$x.pnl > 0.0])->select(~[id, up])->sort(~id->ascending())`,
  `${T}->select(~[region, desk])->distinct()->sort([~region->ascending(), ~desk->descending()])`,
  `${T}->extend(~[sign:x|if($x.pnl > 0.0, |'UP', |'DOWN')])->select(~[id, sign])->sort(~id->ascending())`,
  `${T}->groupBy(~[desk, year], ~[q:x|$x.qty:y|$y->sum()])->filter(x|$x.q > 100)`
    + `->sort([~desk->ascending(), ~year->ascending()])`,
];

/** A cell as the page compares it: a Deephaven long (a wrapper) as its number. */
const plain = (v: unknown): unknown => (v !== null && typeof v === 'object' && 'asNumber' in v
  ? (v as { asNumber(): number }).asNumber() : v);

async function check(name: string, run: () => Promise<string | null>): Promise<void> {
  try {
    const wrong = await run();
    verdict.outcomes.push({ name, ok: wrong === null, message: wrong ?? '' });
  } catch (e) {
    verdict.outcomes.push({ name, ok: false, message: String((e as Error)?.stack ?? e) });
  }
}

try {
  const config = await (await fetch('config.json')).json() as { url: string; psk: string };
  const server: DeephavenServer = {
    url: config.url,
    signIn: { type: 'io.deephaven.authentication.psk.PskAuthenticationHandler', token: config.psk },
  };
  const model = MODEL(Number(new URL(config.url).port));
  const teavm = await load('classes.wasm', { stackDeobfuscator: { enabled: false } });
  const exports = teavm.exports as {
    planOrError(model: string, query: string, runtime: string): string;
    deephavenOrError(op: string, args: string): string;
  };

  // the data, through the console of the test's server, as a person would seed it (never a deployment's)
  const dh = (await import(/* @vite-ignore */ `${config.url}/jsapi/dh-core.js`) as {
    default: { CoreClient: new (url: string) => SeedingClient };
  }).default;
  const seeder = new dh.CoreClient(config.url);
  await seeder.login({ type: server.signIn.type, token: server.signIn.token });
  const seeded = await (await (await seeder.getAsIdeConnection()).startSession('groovy')).runCode(SEED);
  if (seeded.error) throw new Error(`seeding failed: ${seeded.error}`);

  const planner = async (op: string, args: Readonly<Record<string, unknown>>): Promise<string> =>
    exports.deephavenOrError(op, JSON.stringify(args));
  const session = await DeephavenSession.open(server, planner);
  const planOf = (query: string): string => {
    const planned = exports.planOrError(model, query, 'risk::RT');
    if (!planned.startsWith('OK\n')) throw new Error(`the planner refused it: ${planned.replace(/\n/g, ' | ')}`);
    return (JSON.parse(planned.slice(3)) as { sql: string }).sql;
  };
  const answer = async (query: string): Promise<LiveRows> => {
    const table = await session.publish(planOf(query));
    try {
      return await new Promise<LiveRows>((resolve, reject) => {
        let opened: { close(): void } | null = null;
        let first: LiveRows | null = null;
        watch(server, table.name, (rows) => {
          first ??= rows;
          opened?.close();
          resolve(rows);
        }, { first: 0, last: 9999 }).then((w) => {
          opened = w;
          if (first) w.close();
        }, reject);
      });
    } finally {
      await table.release();
    }
  };

  for (const k of KNOWN) {
    await check(`answers ${k.query}`, async () => {
      const got = (await answer(k.query)).rows.map((r) => r.map(plain));
      return JSON.stringify(got) === JSON.stringify(k.rows) ? null
        : `expected ${JSON.stringify(k.rows)}\n   got ${JSON.stringify(got)}`;
    });
  }
  await check(`all ${SHAPES.length} DataCube shapes are created and answer`, async () => {
    const failed: string[] = [];
    for (const q of SHAPES) {
      try {
        await answer(q);
      } catch (e) {
        failed.push(`${q}: ${String(e)}`);
      }
    }
    return failed.length === 0 ? null : failed.join('\n');
  });
  await check('a grouping over a ticking table ticks', async () => {
    const table = await session.publish(planOf(
      "#>{risk::Ref.ticks}#->groupBy(~[sym], ~[total:x|$x.px:y|$y->sum()])->sort(~sym->ascending())"));
    const seen: string[] = [];
    const w = await watch(server, table.name, (rows) => seen.push(JSON.stringify(rows.rows.map((r) => r.map(plain)))),
      { first: 0, last: 9, intervalMs: 200 });
    await new Promise((r) => setTimeout(r, 3000));
    w.close();
    await table.release();
    const distinct = new Set(seen).size;
    return seen.length >= 5 && distinct >= 3 ? null : `${seen.length} updates, ${distinct} distinct, in 3 s`;
  });
  await check('a refused plan names the server\'s own reason', async () => {
    try {
      await session.publish('{"ops": [{"view": {"sourceId": {"ticket": {"ticket": "cy9ub3N1Y2g="}},'
        + ' "columnSpecs": ["x = 1"]}}]}');
      return 'a plan reading a table the server does not have was accepted';
    } catch (e) {
      return /DeephavenRefused/.test(String(e)) ? null : `refused otherwise: ${String(e)}`;
    }
  });
  await session.close();
  seeder.disconnect();
} catch (e) {
  verdict.setupError = String((e as Error)?.stack ?? e);
}
verdict.done = true;
