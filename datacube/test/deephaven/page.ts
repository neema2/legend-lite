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
  Table prints (pid INTEGER PRIMARY KEY, sym VARCHAR(8), tm TIMESTAMP NOT NULL, px DOUBLE, sz DOUBLE NOT NULL)
  Table books (bid INTEGER PRIMARY KEY, lim DECIMAL(18,2), used DECIMAL(18,2) NOT NULL)
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
// prints for windows (docs/DEEPHAVEN_TRADER_DEMOS_2026_10_24.md K6): two names and a print of none, two prints at one
// time (a tie), prints exactly a minute and exactly thirty seconds apart (a window's bounds), prices that are null
def at = { String t -> java.time.Instant.parse("2026-10-24T" + t + "Z") }
prints = newTable(
  intCol("pid", 1, 2, 3, 4, 5, 6, 7, 8, 9),
  stringCol("sym", "AAA", "AAA", "BBB", "AAA", "AAA", "BBB", null, "AAA", "BBB"),
  instantCol("tm", at("09:30:00"), at("09:30:30"), at("09:30:30"), at("09:31:00"), at("09:31:00"), at("09:32:00"),
    at("09:32:30"), at("09:33:00.001"), at("09:33:00")),
  doubleCol("px", 10.0d, 10.5d, 20.0d, NULL_DOUBLE, 11.0d, 19.5d, 5.0d, 10.0d, NULL_DOUBLE),
  doubleCol("sz", 100.0d, 200.0d, 50.0d, 300.0d, 100.0d, 75.0d, 10.0d, 50.0d, 25.0d))
// books for decimals (the stress corpus's 1,582 services over a DECIMAL column): a BigDecimal column each, one null
def dec = { String v -> v == null ? null : new java.math.BigDecimal(v) }
books = newTable(
  intCol("bid", 1, 2, 3),
  col("lim", [dec("1000.50"), dec(null), dec("0.10")] as java.math.BigDecimal[]),
  col("used", [dec("250.25"), dec("0.00"), dec("12.34")] as java.math.BigDecimal[]))
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

const P = '#>{risk::Ref.prints}#';

/** WINDOWS (docs/DEEPHAVEN_TRADER_DEMOS_2026_10_24.md K6): each answer DuckDB's over the same nine prints -- the
 *  planner's DuckDB SQL for the query, run by DuckDB 1.4.4 (2026-10-24) -- so the server's sort and updateBy are held to
 *  the platform's reference lane. */
const WINDOWS: readonly { readonly query: string; readonly rows: readonly (readonly unknown[])[] }[] = [
  // running figures along a time with ties (SQL's default frame): the two prints at 09:31 take the figure at the last of them; a null price is not counted or averaged; a print of no name is a partition of its own
  { query: `${P}->extend(over(~sym, ~tm->ascending()), ~[vol:{p,w,r|$r.sz}:y|$y->plus(), hi:{p,w,r|$r.px}:y|$y->max(), n:{p,w,r|$r.px}:y|$y->count(), a:{p,w,r|$r.px}:y|$y->average()])->select(~[pid, vol, hi, n, a])->sort(~pid->ascending())`,
    rows: [[1, 100.0, 10.0, 1, 10.0], [2, 300.0, 10.5, 2, 10.25], [3, 50.0, 20.0, 1, 20.0], [4, 700.0, 11.0, 3, 10.5], [5, 700.0, 11.0, 3, 10.5], [6, 125.0, 20.0, 2, 19.75], [7, 10.0, 5.0, 1, 5.0], [8, 750.0, 11.0, 4, 10.375], [9, 150.0, 20.0, 2, 19.75]] },
  // a running VWAP: the running sums of price times size and of size, ROWS from the partition's start, and their quotient
  { query: `${P}->extend(over(~sym, ~pid->ascending(), rows(unbounded(), 0)), ~[pv:{p,w,r|$r.px->toOne() * $r.sz}:y|$y->plus(), v:{p,w,r|$r.sz}:y|$y->plus()])->extend(~vwap:x|$x.pv / $x.v)->select(~[pid, pv, v, vwap])->sort(~pid->ascending())`,
    rows: [[1, 1000.0, 100.0, 10.0], [2, 3100.0, 300.0, 10.333333333333334], [3, 1000.0, 50.0, 20.0], [4, 3100.0, 600.0, 5.166666666666667], [5, 4200.0, 700.0, 6.0], [6, 2462.5, 125.0, 19.7], [7, 50.0, 10.0, 5.0], [8, 4700.0, 750.0, 6.266666666666667], [9, 2462.5, 150.0, 16.416666666666668]] },
  // a minute's volume and a 30 seconds' ahead, windows of time: a print exactly a minute before, and exactly 30 seconds after, is in its window
  { query: `${P}->extend(over(~sym, ~tm->ascending(), _range(-1, DurationUnit.MINUTES, 0, DurationUnit.MINUTES)), ~[v1:{p,w,r|$r.sz}:y|$y->plus(), n1:{p,w,r|$r.px}:y|$y->count()])->extend(over(~sym, ~tm->ascending(), _range(0, DurationUnit.SECONDS, 30, DurationUnit.SECONDS)), ~f30:{p,w,r|$r.sz}:y|$y->plus())->select(~[pid, v1, n1, f30])->sort(~pid->ascending())`,
    rows: [[1, 100.0, 1, 300.0], [2, 300.0, 2, 600.0], [3, 50.0, 1, 50.0], [4, 700.0, 3, 400.0], [5, 700.0, 3, 400.0], [6, 75.0, 1, 75.0], [7, 10.0, 1, 10.0], [8, 50.0, 1, 50.0], [9, 100.0, 1, 25.0]] },
  // the previous and the next price, the size two prints back, and the previous print's name: null where there is none
  { query: `${P}->extend(over(~sym, ~pid->ascending()), ~[prev:{p,w,r|$p->lag($r).px}, next:{p,w,r|$p->lead($r).px}, back2:{p,w,r|$p->lag($r, 2).sz}])->extend(over(~pid->ascending()), ~ps:{p,w,r|$p->lag($r).sym})->select(~[pid, prev, next, back2, ps])->sort(~pid->ascending())`,
    rows: [[1, null, 10.5, null, null], [2, 10.0, null, null, "AAA"], [3, null, 19.5, null, "AAA"], [4, 10.5, 11.0, 100.0, "BBB"], [5, null, 10.0, 200.0, "AAA"], [6, 20.0, null, null, "AAA"], [7, null, null, null, "BBB"], [8, 11.0, null, 300.0, null], [9, 19.5, null, 50.0, "AAA"]] },
  // a moving average of three prints, a count and a volume of a print and its neighbours, and the two prints before
  { query: `${P}->extend(over(~sym, ~pid->ascending(), rows(-2, 0)), ~m3:{p,w,r|$r.px}:y|$y->average())->extend(over(~sym, ~pid->ascending(), rows(-1, 1)), ~[c3:{p,w,r|$r.px}:y|$y->count(), s3:{p,w,r|$r.sz}:y|$y->plus()])->extend(over(~sym, ~pid->ascending(), rows(-2, -1)), ~b2:{p,w,r|$r.sz}:y|$y->plus())->select(~[pid, m3, c3, s3, b2])->sort(~pid->ascending())`,
    rows: [[1, 10.0, 2, 300.0, null], [2, 10.25, 2, 600.0, 100.0], [3, 20.0, 2, 125.0, null], [4, 10.25, 2, 600.0, 300.0], [5, 10.75, 2, 450.0, 500.0], [6, 19.75, 2, 150.0, 50.0], [7, 5.0, 1, 10.0, null], [8, 10.5, 2, 150.0, 400.0], [9, 19.75, 1, 100.0, 125.0]] },
  // a print's place by price, highest first (a null price first, the platform's order descending), then by number
  { query: `${P}->extend(over(~sym, [~px->descending(), ~pid->ascending()]), ~rn:{p,w,r|$p->rowNumber($r)})->select(~[pid, rn])->sort(~pid->ascending())`,
    rows: [[1, 4], [2, 3], [3, 2], [4, 1], [5, 2], [6, 3], [7, 1], [8, 5], [9, 1]] },
  // a name's whole figures beside each print
  { query: `${P}->extend(over(~sym), ~[tot:{p,w,r|$r.sz}:y|$y->plus(), np:{p,w,r|$r.px}:y|$y->count(), ap:{p,w,r|$r.px}:y|$y->average(), mx:{p,w,r|$r.px}:y|$y->max()])->select(~[pid, tot, np, ap, mx])->sort(~pid->ascending())`,
    rows: [[1, 750.0, 4, 10.375, 11.0], [2, 750.0, 4, 10.375, 11.0], [3, 150.0, 2, 19.75, 20.0], [4, 750.0, 4, 10.375, 11.0], [5, 750.0, 4, 10.375, 11.0], [6, 150.0, 2, 19.75, 20.0], [7, 10.0, 1, 5.0, 5.0], [8, 750.0, 4, 10.375, 11.0], [9, 150.0, 2, 19.75, 20.0]] },
  // the lowest price so far
  { query: `${P}->extend(over(~pid->ascending()), ~lo:{p,w,r|$r.px}:y|$y->min())->select(~[pid, lo])->sort(~pid->ascending())`,
    rows: [[1, 10.0], [2, 10.0], [3, 10.0], [4, 10.0], [5, 10.0], [6, 10.0], [7, 5.0], [8, 5.0], [9, 5.0]] },
  // the running volume of a minute's bars: a window over a group
  { query: `${P}->extend(~[b:x|$x.tm->timeBucket(1, DurationUnit.MINUTES)])->groupBy(~[b], ~[v:x|$x.sz:y|$y->sum()])->extend(over(~b->ascending()), ~cv:{p,w,r|$r.v}:y|$y->plus())->sort(~b->ascending())`,
    rows: [["2026-10-24T09:30:00", 350.0, 350.0], ["2026-10-24T09:31:00", 400.0, 750.0], ["2026-10-24T09:32:00", 85.0, 835.0], ["2026-10-24T09:33:00", 75.0, 910.0]] },
  // a price's change from the previous print: a window read in a formula
  { query: `${P}->extend(over(~sym, ~pid->ascending()), ~chg:{p,w,r|$r.px->toOne() - $p->lag($r).px->toOne()})->select(~[pid, chg])->sort(~pid->ascending())`,
    rows: [[1, null], [2, 0.5], [3, null], [4, null], [5, null], [6, -0.5], [7, null], [8, -1.0], [9, null]] },
  // the two biggest prints of each name (a top of each group: a row number filtered, SQL's QUALIFY, with no compiling)
  { query: `${P}->extend(over(~sym, [~sz->descending(), ~pid->ascending()]), ~rn:{p,w,r|$p->rowNumber($r)})->filter(x|$x.rn <= 2)->select(~[pid, sym, rn])->sort(~pid->ascending())`,
    rows: [[2, "AAA", 2], [3, "BBB", 2], [4, "AAA", 1], [6, "BBB", 1], [7, null, 1]] },
  // a print's share of its name's volume: a window over the whole partition, read in a formula
  { query: `${P}->extend(over(~sym), ~tot:{p,w,r|$r.sz}:y|$y->plus())->extend(~share:x|$x.sz / $x.tot->toOne())->select(~[pid, share])->sort(~pid->ascending())`,
    rows: [[1, 0.13333333333333333], [2, 0.26666666666666666], [3, 0.3333333333333333], [4, 0.4], [5, 0.13333333333333333], [6, 0.5], [7, 1.0], [8, 0.06666666666666667], [9, 0.16666666666666666]] },
  // a print's rank by price, highest first: its ties one rank (a null price first, descending); its dense rank, percent rank and cumulative distribution
  { query: `${P}->extend(over(~sym, ~px->descending()), ~[rk:{p,w,r|$p->rank($w, $r)}, dr:{p,w,r|$p->denseRank($w, $r)}, pr:{p,w,r|$p->percentRank($w, $r)}, cd:{p,w,r|$p->cumulativeDistribution($w, $r)}])->select(~[pid, rk, dr, pr, cd])->sort(~pid->ascending())`,
    rows: [[1, 4, 4, 0.75, 1.0], [2, 3, 3, 0.5, 0.6], [3, 2, 2, 0.5, 0.6666666666666666], [4, 1, 1, 0.0, 0.2], [5, 2, 2, 0.25, 0.4], [6, 3, 3, 1.0, 1.0], [7, 1, 1, 0.0, 1.0], [8, 4, 4, 0.75, 1.0], [9, 1, 1, 0.0, 0.3333333333333333]] },
  // a rank along a time with ties: the two prints at 09:31 one rank, the next print's rank after both (dense: after one)
  { query: `${P}->extend(over(~sym, ~tm->ascending()), ~[rk:{p,w,r|$p->rank($w, $r)}, dr:{p,w,r|$p->denseRank($w, $r)}])->select(~[pid, rk, dr])->sort(~pid->ascending())`,
    rows: [[1, 1, 1], [2, 2, 2], [3, 1, 1], [4, 3, 3], [5, 3, 3], [6, 2, 2], [7, 1, 1], [8, 5, 4], [9, 3, 3]] },
  // buckets (NTILE) along each name's prints, the first buckets a print larger; a print's cumulative distribution
  { query: `${P}->extend(over(~sym, ~pid->ascending()), ~[n2:{p,w,r|$p->ntile($r, 2)}, n3:{p,w,r|$p->ntile($r, 3)}, cd:{p,w,r|$p->cumulativeDistribution($w, $r)}])->select(~[pid, n2, n3, cd])->sort(~pid->ascending())`,
    rows: [[1, 1, 1, 0.2], [2, 1, 1, 0.4], [3, 1, 1, 0.3333333333333333], [4, 1, 2, 0.6], [5, 2, 2, 0.8], [6, 1, 2, 0.6666666666666666], [7, 1, 1, 1.0], [8, 2, 3, 1.0], [9, 2, 3, 1.0]] },
  // the first and the last price to the row along a time (SQL's default frame: the last of the row's ties)
  { query: `${P}->extend(over(~sym, ~tm->ascending()), ~[f:{p,w,r|$p->first($w, $r).px}, l:{p,w,r|$p->last($w, $r).px}])->select(~[pid, f, l])->sort(~pid->ascending())`,
    rows: [[1, 10.0, 10.0], [2, 10.0, 10.5], [3, 20.0, 20.0], [4, 10.0, 11.0], [5, 10.0, 11.0], [6, 20.0, 19.5], [7, 5.0, 5.0], [8, 10.0, 10.0], [9, 20.0, null]] },
  // the first and the last price of each name's whole partition (a null last price stays null)
  { query: `${P}->extend(over(~sym, ~pid->ascending(), rows(unbounded(), unbounded())), ~[f:{p,w,r|$p->first($w, $r).px}, l:{p,w,r|$p->last($w, $r).px}])->select(~[pid, f, l])->sort(~pid->ascending())`,
    rows: [[1, 10.0, 10.0], [2, 10.0, 10.0], [3, 20.0, null], [4, 10.0, 10.0], [5, 10.0, 10.0], [6, 20.0, null], [7, 5.0, 5.0], [8, 10.0, 10.0], [9, 20.0, null]] },
  // the first name and the last price of a print and its neighbours: a frame of rows
  { query: `${P}->extend(over(~sym, ~pid->ascending(), rows(-1, 1)), ~[f:{p,w,r|$p->first($w, $r).sym}, l:{p,w,r|$p->last($w, $r).px}])->select(~[pid, f, l])->sort(~pid->ascending())`,
    rows: [[1, "AAA", 10.5], [2, "AAA", null], [3, "BBB", 19.5], [4, "AAA", 11.0], [5, "AAA", 10.0], [6, "BBB", null], [7, null, 5.0], [8, "AAA", 10.0], [9, "BBB", null]] },
  // the last price to the row, ROWS: the row's own
  { query: `${P}->extend(over(~sym, ~pid->ascending(), rows(unbounded(), 0)), ~l:{p,w,r|$p->last($w, $r).px})->select(~[pid, l])->sort(~pid->ascending())`,
    rows: [[1, 10.0], [2, 10.5], [3, 20.0], [4, null], [5, 11.0], [6, 19.5], [7, 5.0], [8, 10.0], [9, null]] },
  // the first price and the last print of the last minute, to the row and its ties: a frame of time
  { query: `${P}->extend(over(~sym, ~tm->ascending(), _range(-1, DurationUnit.MINUTES, 0, DurationUnit.MINUTES)), ~[f:{p,w,r|$p->first($w, $r).px}, l:{p,w,r|$p->last($w, $r).pid}])->select(~[pid, f, l])->sort(~pid->ascending())`,
    rows: [[1, 10.0, 1], [2, 10.0, 2], [3, 20.0, 3], [4, 10.0, 5], [5, 10.0, 5], [6, 19.5, 6], [7, 5.0, 7], [8, 10.0, 8], [9, 19.5, 9]] },
  // buckets and percent ranks across every print: no partition
  { query: `${P}->extend(over(~pid->ascending()), ~[nt:{p,w,r|$p->ntile($r, 4)}, pr:{p,w,r|$p->percentRank($w, $r)}])->select(~[pid, nt, pr])->sort(~pid->ascending())`,
    rows: [[1, 1, 0.0], [2, 1, 0.125], [3, 1, 0.25], [4, 2, 0.375], [5, 2, 0.5], [6, 3, 0.625], [7, 3, 0.75], [8, 4, 0.875], [9, 4, 1.0]] },
];

/** DataCube's shapes, L2's eighteen (the two the compiler refuses, a string operation on a nullable column, are not
 *  the target's): each must plan, be sent and be created, and answer rows. */
const I = '#>{risk::Ref.instrument}#';
const B = '#>{risk::Ref.books}#';

/** SCALARS (docs/DEEPHAVEN_TRADER_DEMOS_2026_10_24.md §2, the stress corpus's TF_TradeScalars): a screen's computed
 *  column, each answer DuckDB's over the same rows -- every one a formula, Deephaven's own library and String's and
 *  BigDecimal's own methods (DH33). */
const SCALARS: readonly { readonly query: string; readonly rows: readonly (readonly unknown[])[] }[] = [
  // round half-even (SqlFn's contract: 12.5 and 2.5 to the even below), to a scale; mod with the divisor's sign, rem
  // with the dividend's
  { query: `${P}->extend(~[r0:x|($x.sz / 4.0)->round(), r2:x|($x.sz / 3.0)->round(2), m:x|$x.pid->mod(3), `
      + `n:x|$x.pid->rem(3), nm:x|(0 - $x.pid)->mod(4), nr:x|(0 - $x.pid)->rem(4)])->select(~[pid, r0, r2, `
      + `m, n, nm, nr])->sort(~pid->ascending())`,
    // rem is Pure's Number (rem(Number, Number)), which the tab reads as exact text, as it reads a Decimal
    rows: [[1, 25, 33.33, 1, '1', 3, '-1'], [2, 50, 66.67, 2, '2', 2, '-2'], [3, 12, 16.67, 0, '0', 1, '-3'],
      [4, 75, 100, 1, '1', 0, '0'], [5, 25, 33.33, 2, '2', 3, '-1'], [6, 19, 25, 0, '0', 2, '-2'],
      [7, 2, 3.33, 1, '1', 1, '-3'], [8, 12, 16.67, 2, '2', 0, '0'], [9, 6, 8.33, 0, '0', 3, '-1']] },
  // the platform's SUBSTRING, one-based, a start below 1 held at 1 and an index past the end held there; contains;
  // a null string kept null
  { query: `${I}->extend(~[h:x|$x.ccy->substring(1, 3), z:x|$x.ccy->substring(0, 2), e:x|$x.ccy->substring(2), `
      + `c:x|$x.ccy->contains('U'), s:x|$x.sector->toOne()->substring(1, 2)])->select(~[iid, h, z, e, c, `
      + `s])->sort(~iid->ascending())`,
    rows: [['I1', 'USD', 'US', 'SD', true, 'Te'], ['I2', 'EUR', 'EU', 'UR', true, null],
      ['I3', 'JPY', 'JP', 'PY', false, 'Ba']] },
  // a whole number as its digits, a null kept null
  { query: `${T}->extend(~[t:x|$x.id->toString(), q:x|$x.qty->toOne()->toString()])->select(~[id, t, `
      + `q])->sort(~id->ascending())`,
    rows: [[1, '1', '10'], [2, '2', '5'], [3, '3', null], [4, '4', '20'], [5, '5', '1'], [6, '6', '3']] },
  // a DECIMAL column read as it is (exact text, as the tab reads a BigDecimal), and as a Float by its own doubleValue
  { query: `${B}->extend(~[u:x|$x.used->toFloat() * 2.0, l:x|$x.lim->toOne()->toFloat()])->select(~[bid, used, `
      + `u, l])->sort(~bid->ascending())`,
    rows: [[1, '250.25', 500.5, 1000.5], [2, '0', 0, null], [3, '12.34', 24.68, 0.1]] },
];

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
  for (const k of [...TIMES, ...WINDOWS, ...SCALARS]) {
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
