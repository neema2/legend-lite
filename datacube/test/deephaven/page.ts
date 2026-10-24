// DEEPHAVEN FROM THE BROWSER, the page (//datacube:deephaven_test; docs/DEEPHAVEN_PLAN_2026_10_22.md DH21, L3b): served
// from a site that is not the Deephaven server's, it plans each query with the WebAssembly planner on a Deephaven
// runtime, sends it with the tab's sender (engine-client/src/deephaven.ts) straight to the server, and reads the rows
// with the tab's live reader (deephaven-live.ts), as DataCube's engine follows a result -- no server of legend-lite's
// anywhere. The data is six rows with nulls in every
// column, seeded through the test server's console; the known answers are worked out by hand from them, so the test
// needs no second database. Every check runs here; the driver (deephaven.mjs) reports what the page found.

import { DeephavenSession, type DeephavenServer } from '../../../engine-client/src/deephaven.ts';
import { LiveClient, type Followed, type FollowedRows } from '../../../engine-client/src/deephaven-live.ts';
import { followedTable } from '../../../engine-client/src/deephaven-rows.ts';
import { relationColumns, type Plan } from '../../../engine-client/src/relation-type.ts';
import { openServerLog } from '../../demo/deephaven/server-log.ts';
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
  Table fills (fid INTEGER PRIMARY KEY, tm TIMESTAMP NOT NULL, ts TIMESTAMP, d DATE, px DOUBLE NOT NULL)
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
// dates and times (docs/DEEPHAVEN_TRADER_DEMOS_2026_10_24.md K1): a null date-time and a null date, a leap day, and an
// instant just before the epoch, whose bucket is the one before it
fills = newTable(
  intCol("fid", 1, 2, 3, 4, 5),
  instantCol("tm", java.time.Instant.parse("2026-10-24T09:30:15.250Z"), java.time.Instant.parse("2026-10-24T09:34:59.999Z"),
    java.time.Instant.parse("2026-10-26T23:59:59Z"), java.time.Instant.parse("2024-02-29T12:00:00Z"),
    java.time.Instant.parse("1969-12-31T23:59:30Z")),
  instantCol("ts", java.time.Instant.parse("2026-10-24T09:30:15.250Z"), null, java.time.Instant.parse("2026-01-01T00:00:00Z"),
    null, java.time.Instant.parse("2026-03-31T23:00:00Z")),
  col("d", [java.time.LocalDate.parse("2026-10-24"), java.time.LocalDate.parse("2026-02-28"), null,
    java.time.LocalDate.parse("2024-12-31"), java.time.LocalDate.parse("1969-12-31")] as java.time.LocalDate[]),
  doubleCol("px", 10.0d, 11.0d, 12.0d, 13.0d, 14.0d))
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
  // A PIVOT WITH NO FORMULA (P1): a cell's count the server's count of its rows (0 where none), its sum and average
  // one aggregation over the desk and the year, joined back (null where no row is the cell's, or none has a value)
  { query: `${T}->groupBy(~[desk], ~['2023__|__n':x|if($x.year == 2023, |1, |[]):y|$y->count(),`
      + ` '2023__|__s':x|if($x.year == 2023, |$x.notional, |[]):y|$y->sum(),`
      + ` '2024__|__n':x|if($x.year == 2024, |1, |[]):y|$y->count(),`
      + ` '2024__|__s':x|if($x.year == 2024, |$x.notional, |[]):y|$y->sum(),`
      + ` '2024__|__p':x|if($x.year == 2024, |$x.pnl, |[]):y|$y->average()])->sort(~desk->ascending())`,
    rows: [['Credit', 1, 10, 0, null, null], ['FX', 0, null, 1, 50, null], ['Rates', 2, 300, 0, null, null],
      [null, 0, null, 1, 30, 1.5]] },
  // FILTERS DEEPHAVEN READS WITHOUT COMPILING (DH25): a column against a literal, each comparison, over columns with nulls
  ...([
    // Pure's != is null-safe: a null region differs from 'EMEA' (SQL's IS DISTINCT FROM)
    ["$x.region != 'EMEA'", [2, 4, 5]],
    ['$x.pnl < 2.0', [2, 5, 6]],
    ['$x.qty <= 5', [2, 5, 6]],
    ['$x.pnl > -3.5', [1, 2, 4, 5, 6]],
    ['$x.notional == 50.0', [3]],
    ['$x.year != 2023', [3, 4, 6]],
    ["($x.year >= 2023) && ($x.desk == 'Rates')", [1, 2]],
    ["$x.desk->in(['Rates', 'FX']) && ($x.qty > 4)", [1, 2, 4]],
    ["!$x.region->in(['EMEA', 'APAC'])", [4, 5]],
    // a value with a comma: Deephaven's `in` splits its list on commas, so this one stays a formula
    ["$x.country->in(['UK', 'J,P'])", [1, 6]],
  ] as const).map(([condition, ids]) => ({
    query: `${T}->filter(x|${condition})->select(~[id])->sort(~id->ascending())`,
    rows: ids.map((id) => [id]),
  })),
  // a group's unique value (the server's own unique aggregation): EMEA's desks are Rates and FX (and a null, never a
  // value), so none; each other region's one desk
  { query: `${T}->groupBy(~[region], ~[d:x|$x.desk:y|$y->uniqueValueOnly()])->sort(~region->ascending())`,
    rows: [['AMER', 'Credit'], ['APAC', 'Rates'], ['EMEA', null], [null, 'FX']] },
  // the null-safe != as a value, not a filter: a formula, its null half the desk's (a null desk differs from 'FX')
  { query: `${T}->extend(~[d:x|$x.desk != 'FX'])->select(~[id, d])->sort(~id->ascending())`,
    rows: [[1, true], [2, true], [3, false], [4, false], [5, true], [6, true]] },
];

const F = '#>{risk::Ref.fills}#';
const RM = 'meta::pure::functions::math::mathUtility::rowMapper';

/** DATES AND TIMES (docs/DEEPHAVEN_TRADER_DEMOS_2026_10_24.md K1, DH29): each answer DuckDB's over the same five rows --
 *  the planner's DuckDB SQL for the query, run by DuckDB 1.4.4 (2026-10-24) -- so the server is held to the platform's
 *  reference lane: an instant its UTC wall-clock, a date its day. */
const TIMES: readonly { readonly query: string; readonly rows: readonly (readonly unknown[])[] }[] = [
  { query: `${F}->extend(~[b5:x|$x.tm->timeBucket(5, DurationUnit.MINUTES), bw:x|$x.tm->timeBucket(1, DurationUnit.WEEKS), bd:x|$x.tm->timeBucket(1, DurationUnit.DAYS)])->select(~[fid, b5, bw, bd])->sort(~fid->ascending())`,
    rows: [[1, "2026-10-24T09:30:00", "2026-10-19T00:00:00", "2026-10-24T00:00:00"], [2, "2026-10-24T09:30:00", "2026-10-19T00:00:00", "2026-10-24T00:00:00"], [3, "2026-10-26T23:55:00", "2026-10-26T00:00:00", "2026-10-26T00:00:00"], [4, "2024-02-29T12:00:00", "2024-02-26T00:00:00", "2024-02-29T00:00:00"], [5, "1969-12-31T23:55:00", "1969-12-29T00:00:00", "1969-12-31T00:00:00"]] },
  { query: `${F}->extend(~[y:x|$x.tm->year(), m:x|$x.tm->monthNumber(), dd:x|$x.tm->dayOfMonth(), h:x|$x.tm->hour(), mi:x|$x.tm->minute(), s:x|$x.tm->second()])->select(~[fid, y, m, dd, h, mi, s])->sort(~fid->ascending())`,
    rows: [[1, 2026, 10, 24, 9, 30, 15], [2, 2026, 10, 24, 9, 34, 59], [3, 2026, 10, 26, 23, 59, 59], [4, 2024, 2, 29, 12, 0, 0], [5, 1969, 12, 31, 23, 59, 30]] },
  { query: `${F}->extend(~[w:x|$x.tm->dayOfWeekNumber(), q:x|$x.tm->quarterNumber(), doy:x|$x.tm->dayOfYear(), wk:x|$x.tm->weekOfYear(), th:x|$x.ts->toOne()->hour()])->select(~[fid, w, q, doy, wk, th])->sort(~fid->ascending())`,
    rows: [[1, 6, 4, 297, 43, 9], [2, 6, 4, 297, 43, null], [3, 1, 4, 299, 44, 0], [4, 4, 1, 60, 9, null], [5, 3, 4, 365, 1, 23]] },
  { query: `${F}->extend(~[dp:x|$x.tm->datePart(), fw:x|$x.tm->firstDayOfWeek(), fm:x|$x.tm->firstDayOfMonth(), fq:x|$x.tm->firstDayOfQuarter(), fy:x|$x.tm->firstDayOfYear(), fh:x|$x.tm->firstMinuteOfHour()])->select(~[fid, dp, fw, fm, fq, fy, fh])->sort(~fid->ascending())`,
    rows: [[1, "2026-10-24", "2026-10-19", "2026-10-01", "2026-10-01", "2026-01-01", "2026-10-24T09:00:00"], [2, "2026-10-24", "2026-10-19", "2026-10-01", "2026-10-01", "2026-01-01", "2026-10-24T09:00:00"], [3, "2026-10-26", "2026-10-26", "2026-10-01", "2026-10-01", "2026-01-01", "2026-10-26T23:00:00"], [4, "2024-02-29", "2024-02-26", "2024-02-01", "2024-01-01", "2024-01-01", "2024-02-29T12:00:00"], [5, "1969-12-31", "1969-12-29", "1969-12-01", "1969-10-01", "1969-01-01", "1969-12-31T23:00:00"]] },
  { query: `${F}->extend(~[dm:x|$x.d->toOne()->monthNumber(), dw:x|$x.d->toOne()->dayOfWeekNumber(), dfm:x|$x.d->toOne()->firstDayOfMonth(), dadj:x|$x.d->toOne()->adjust(1, DurationUnit.MONTHS), db:x|$x.d->toOne()->timeBucket(1, DurationUnit.WEEKS)])->select(~[fid, dm, dw, dfm, dadj, db])->sort(~fid->ascending())`,
    rows: [[1, 10, 6, "2026-10-01", "2026-11-24", "2026-10-19"], [2, 2, 6, "2026-02-01", "2026-03-28", "2026-02-23"], [3, null, null, null, null, null], [4, 12, 2, "2024-12-01", "2025-01-31", "2024-12-30"], [5, 12, 3, "1969-12-01", "1970-01-31", "1969-12-29"]] },
  { query: `${F}->extend(~[a5:x|$x.tm->adjust(5, DurationUnit.MINUTES), ad:x|$x.tm->adjust(3, DurationUnit.DAYS), am:x|$x.tm->adjust(1, DurationUnit.MONTHS), ay:x|$x.tm->adjust(-1, DurationUnit.YEARS)])->select(~[fid, a5, ad, am, ay])->sort(~fid->ascending())`,
    rows: [[1, "2026-10-24T09:35:15.25", "2026-10-27T09:30:15.25", "2026-11-24T09:30:15.25", "2025-10-24T09:30:15.25"], [2, "2026-10-24T09:39:59.999", "2026-10-27T09:34:59.999", "2026-11-24T09:34:59.999", "2025-10-24T09:34:59.999"], [3, "2026-10-27T00:04:59", "2026-10-29T23:59:59", "2026-11-26T23:59:59", "2025-10-26T23:59:59"], [4, "2024-02-29T12:05:00", "2024-03-03T12:00:00", "2024-03-29T12:00:00", "2023-02-28T12:00:00"], [5, "1970-01-01T00:04:30", "1970-01-03T23:59:30", "1970-01-31T23:59:30", "1968-12-31T23:59:30"]] },
  { query: `${F}->filter(x|$x.tm >= %2026-10-24T09:30:15.250+0000)->select(~[fid])->sort(~fid->ascending())`,
    rows: [[1], [2], [3]] },
  { query: `${F}->filter(x|$x.ts < %2026-06-01T00:00:00.000+0000)->select(~[fid])->sort(~fid->ascending())`,
    rows: [[3], [5]] },
  { query: `${F}->filter(x|$x.d->isNotEmpty() && ($x.d->toOne() > %2025-01-01))->select(~[fid])->sort(~fid->ascending())`,
    rows: [[1], [2]] },
  { query: `${F}->extend(~[b:x|$x.tm->timeBucket(1, DurationUnit.DAYS)])->groupBy(~[b], ~[n:x|$x.fid:y|$y->count(), hi:x|$x.px:y|$y->max()])->sort(~b->ascending())`,
    rows: [["1969-12-31T00:00:00", 1, 14.0], ["2024-02-29T00:00:00", 1, 13.0], ["2026-10-24T00:00:00", 2, 11.0], ["2026-10-26T00:00:00", 1, 12.0]] },
  // a match, a membership and Pure's null-safe != on a date and date-times: each in quotes, as the server's
  // filter parser reads them
  { query: `${F}->filter(x|$x.d->isNotEmpty() && ($x.d->toOne() == %2026-10-24))->select(~[fid])->sort(~fid->ascending())`,
    rows: [[1]] },
  { query: `${F}->filter(x|$x.tm->in([%2024-02-29T12:00:00.000+0000, %2026-10-26T23:59:59.000+0000]))->select(~[fid])->sort(~fid->ascending())`,
    rows: [[3], [4]] },
  { query: `${F}->filter(x|$x.ts != %2026-01-01T00:00:00.000+0000)->select(~[fid])->sort(~fid->ascending())`,
    rows: [[1], [2], [4], [5]] },
  // a day's candle (K3a): its open and close the price at its first and last fill (the server's sortedFirst and
  // sortedLast by the fill's number), its high and low, its count
  { query: `${F}->extend(~[b:x|$x.tm->timeBucket(1, DurationUnit.DAYS)])->groupBy(~[b], ~[o:x|${RM}($x.px, $x.fid):y|$y->minBy(), hi:x|$x.px:y|$y->max(), lo:x|$x.px:y|$y->min(), c:x|${RM}($x.px, $x.fid):y|$y->maxBy(), n:x|$x.fid:y|$y->count()])->sort(~b->ascending())`,
    rows: [["1969-12-31T00:00:00", 14.0, 14.0, 14.0, 14.0, 1], ["2024-02-29T00:00:00", 13.0, 13.0, 13.0, 13.0, 1], ["2026-10-24T00:00:00", 10.0, 11.0, 10.0, 11.0, 2], ["2026-10-26T00:00:00", 12.0, 12.0, 12.0, 12.0, 1]] },
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

/** A followed table's rows, row by row. */
const byRow = (rows: FollowedRows): unknown[][] =>
  Array.from({ length: rows.values[0]?.length ?? 0 }, (_, r) => rows.values.map((column) => column[r]));

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
  const planned = (query: string): Plan => {
    const plan = exports.planOrError(model, query, 'risk::RT');
    if (!plan.startsWith('OK\n')) throw new Error(`the planner refused it: ${plan.replace(/\n/g, ' | ')}`);
    // the module's answer, {sql, type}, as DataCube's planner reads it (lite-planner.ts): the SQL and the columns
    const body = JSON.parse(plan.slice(3)) as { sql: string; type: Parameters<typeof relationColumns>[0] };
    return { sql: body.sql, columns: relationColumns(body.type) };
  };
  const planOf = (query: string): string => planned(query).sql;
  const live = await LiveClient.open(server, (why) => { verdict.setupError ??= `the live reader ended: ${why}`; });
  /** A query's result kept alive and published, and its rows as the live reader first hands them over, each row as
   *  `read` takes them. */
  const answer = async (query: string, read: (rows: FollowedRows) => unknown[][] = byRow,
    sql = planOf(query)): Promise<unknown[][]> => {
    const kept = await session.keepAlive(sql);
    try {
      const name = await session.publishKept(kept.exportId);
      return await new Promise<unknown[][]>((resolve, reject) => {
        let followed: Followed | null = null;
        let first: unknown[][] | null = null;
        const late = setTimeout(() => reject(new Error(`no rows in 10 s for ${query}`)), 10_000);
        live.follow(name, (rows) => {
          if (first) return;
          try {
            first = read(rows);
          } catch (e) {
            // a row the reader refuses fails the check, said, not a wait for rows
            clearTimeout(late);
            reject(e);
            return;
          }
          clearTimeout(late);
          followed?.close();
          resolve(first);
        }).then((f) => {
          followed = f;
          if (first) f.close();
        }, (e: unknown) => {
          clearTimeout(late);
          reject(e);
        });
      });
    } finally {
      kept.release();
    }
  };

  for (const k of KNOWN) {
    await check(`answers ${k.query}`, async () => {
      const got = (await answer(k.query)).map((r) => r.map(plain));
      return JSON.stringify(got) === JSON.stringify(k.rows) ? null
        : `expected ${JSON.stringify(k.rows)}\n   got ${JSON.stringify(got)}`;
    });
  }
  // a date or a date-time is read by its plan's column type, as DataCube's engine reads it (followedTable): an instant
  // its nanoseconds' UTC wall-clock, a date its day
  for (const k of TIMES) {
    await check(`answers ${k.query}`, async () => {
      const plan = planned(k.query);
      const got = await answer(k.query, (rows) => {
        const table = followedTable(rows, plan, 0, 0);
        return Array.from({ length: table.rowCount }, (_, r) => table.columns.map((c) => c.values[r]));
      }, plan.sql);
      return JSON.stringify(got) === JSON.stringify(k.rows) ? null
        : `expected ${JSON.stringify(k.rows)}\n   got ${JSON.stringify(got)}`;
    });
  }
  // A FILTER ON A NEW DATE-TIME OR DATE COMPILES NOTHING (DH25, K1): each in quotes, a range or match filter the
  // server reads as written, so a value never filtered on before costs the server no compile (a formula's literal,
  // measured 240 ms each)
  await check('a filter on a date-time or a date never filtered on before compiles nothing on the server', async () => {
    const log = await openServerLog(config.url, config.psk);
    const on = (tm: string, d: string): string => `${F}->filter(x|($x.tm >= %${tm}+0000) && $x.d->isNotEmpty()`
      + ` && ($x.d->toOne() < %${d}))->select(~[fid])->sort(~fid->ascending())`;
    // the filter's shape made once: its guards and its sort's
    await answer(on('2024-01-01T00:00:00.000', '2026-12-31'));
    const mark = Date.now();
    const got = await answer(on('2024-03-01T12:34:56.789', '2026-10-01'));
    const compiled = (await log.since(mark)).filter((op) => op.compiling);
    if (JSON.stringify(got) !== JSON.stringify([[2]])) return `answered ${JSON.stringify(got)}, not [[2]]`;
    return compiled.length === 0 ? null
      : `${compiled.length} compilations: ${compiled.map((op) => op.description).join(' | ')}`;
  });
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
    const kept = await session.keepAlive(planOf(
      "#>{risk::Ref.ticks}#->groupBy(~[sym], ~[total:x|$x.px:y|$y->sum()])->sort(~sym->ascending())"));
    const seen: string[] = [];
    const followed = await live.follow(await session.publishKept(kept.exportId),
      (rows) => seen.push(JSON.stringify(byRow(rows).map((r) => r.map(plain)))), 200);
    await new Promise((r) => setTimeout(r, 3000));
    followed.close();
    kept.release();
    const distinct = new Set(seen).size;
    return seen.length >= 5 && distinct >= 3 ? null : `${seen.length} updates, ${distinct} distinct, in 3 s`;
  });
  await check('a refused plan names the server\'s own reason', async () => {
    try {
      await session.keepAlive('{"ops": [{"view": {"sourceId": {"ticket": {"ticket": "cy9ub3N1Y2g="}},'
        + ' "columnSpecs": ["x = 1"]}}]}');
      return 'a plan reading a table the server does not have was accepted';
    } catch (e) {
      return /DeephavenRefused/.test(String(e)) ? null : `refused otherwise: ${String(e)}`;
    }
  });
  live.close();
  await session.close();
  seeder.disconnect();
} catch (e) {
  verdict.setupError = String((e as Error)?.stack ?? e);
}
verdict.done = true;
