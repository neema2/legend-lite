// THE PURE LIBRARY SPIKE (the user, 2026-10-27: "try to implement all the chart functions and see if everything holds",
// before the drop-columns primitive): one function of each family of the timeseries tile's functions, written as a Pure
// function generic over any table (docs/SEMANTICS_REGISTER.md S112), run over the same prices as
// timeseries-queries.test.ts and held to the numbers today's writer (timeseries-query.ts) gives for the same expression.
// What it finds is docs/PURE_LIBRARY_SPIKE_2026_10_27.md: which families a Pure function says as they are, and what
// each of the others needs. Working columns a function cannot drop yet are left in its answer (ts_*), as written.

import assert from 'node:assert/strict';
import { after, before, describe, it } from 'node:test';
import { openDatabase } from '#test-host';

import { DuckDbEngine } from '../../engine-client/src/duckdb.ts';
import type { ResultTable } from '../../engine-client/src/result.ts';
import type { CubeSnapshot } from '../src/snapshot.ts';
import { ingestFile, type DuckDbFiles } from '../src/upload.ts';
import { plannerFor } from './catalog-builder.ts';
import type { LitePlanner } from '../src/lite-planner.ts';
import { type TimeseriesSpec, type TsSeries } from '../src/timeseries-spec.ts';
import { parseExpr } from '../src/timeseries-expr.ts';
import { seriesQuery, TS_DATE, TS_LOWER, TS_UPPER, TS_VALUE, type TsInputs } from '../src/timeseries-query.ts';
import { lambda } from '../../pure-protocol/src/index.ts';
import { DATACUBE_LIBRARY } from '../src/pure-library-text.ts';

// -- the prices of timeseries-queries.test.ts: two instruments over 40 weekdays from 2024-01-01, and a third on fewer --

function weekdays(n: number): string[] {
  const out: string[] = [];
  for (let d = new Date(Date.UTC(2024, 0, 1)); out.length < n; d = new Date(d.getTime() + 86400000)) {
    if (d.getUTCDay() !== 0 && d.getUTCDay() !== 6) out.push(d.toISOString().slice(0, 10));
  }
  return out;
}

const DAYS = weekdays(40);
const round = (v: number): number => Math.round(v * 100) / 100;
const AAA = DAYS.map((_, i) => round(100 * (1 + 0.03 * Math.sin(i / 2) + 0.004 * i)));
const BBB = DAYS.map((_, i) => round(50 * (1 + 0.02 * Math.cos(i / 1.7) + 0.002 * i + 0.01 * Math.sin(i / 3))));
const CCC = DAYS.map((_, i) => round(20 + i * 0.1 + (i % 4) * 0.3));
const VOL = DAYS.map((_, i) => 1000 + 37 * i);

const CSV = [
  'symbol,day,close,volume',
  ...DAYS.flatMap((d, i) => [
    `AAA,${d},${AAA[i]},${VOL[i]}`,
    `BBB,${d},${BBB[i]},${VOL[i]! * 2}`,
    ...(i % 3 === 0 ? [`CCC,${d},${CCC[i]},${VOL[i]! * 3}`] : []),
  ]),
].join('\n');

/** The spike's functions: each generic over the table, its line (a column it has a line per value of), its date, the
 *  column(s) it reads and the column(s) it writes. */
const SPIKE = `###Pure
// multi-step windows: a mean and a deviation over the window, then a row's figure of them (zscores)
function spike::zscores<T,L,A,V,O>(t: Relation<T>[1], line: ColSpec<L⊆T>[1], date: ColSpec<A⊆T>[1], value: ColSpec<V⊆T>[1], w: Integer[1], out: ColSpec<O>[1]): Relation<T+O>[1]
{
  $t->rename($line, ~ts_l)->rename($date, ~ts_d)->rename($value, ~ts_v)
    ->extend(over(~ts_l, [~ts_d->ascending()], rows(1 - $w, 0)), ~[ts_m:{p,f,r|$r.ts_v}:y|$y->average(), ts_s:{p,f,r|$r.ts_v}:y|$y->stdDevSample(), ts_n:{p,f,r|$r.ts_v}:y|$y->count()])
    ->extend(~ts_o:x|if(($x.ts_n->toOne() < $w) || ($x.ts_s->toOne() == 0), |[], |($x.ts_v->toOne() - $x.ts_m->toOne()) / $x.ts_s->toOne()))
    ->without(~[ts_m, ts_s, ts_n])
    ->rename(~ts_l, $line)->rename(~ts_d, $date)->rename(~ts_v, $value)->rename(~ts_o, $out)
}

// several outputs: a band's lower, upper and width (bollinger_bands)
function spike::bollinger<T,L,A,V,O1,O2,O3>(t: Relation<T>[1], line: ColSpec<L⊆T>[1], date: ColSpec<A⊆T>[1], value: ColSpec<V⊆T>[1], w: Integer[1], k: Float[1], lower: ColSpec<O1>[1], upper: ColSpec<O2>[1], width: ColSpec<O3>[1]): Relation<Any>[1]
{
  $t->rename($line, ~ts_l)->rename($date, ~ts_d)->rename($value, ~ts_v)
    ->extend(over(~ts_l, [~ts_d->ascending()], rows(1 - $w, 0)), ~[ts_m:{p,f,r|$r.ts_v}:y|if($y->count() < $w, |[], |$y->average()), ts_s:{p,f,r|$r.ts_v}:y|if(($y->count() < $w) || ($y->count() < 2), |[], |$y->stdDevSample())])
    ->extend(~[ts_lo:x|$x.ts_m->toOne() - $k * $x.ts_s->toOne(), ts_hi:x|$x.ts_m->toOne() + $k * $x.ts_s->toOne()])
    ->extend(~ts_wd:x|$x.ts_hi->toOne() - $x.ts_lo->toOne())
    ->rename(~ts_l, $line)->rename(~ts_d, $date)->rename(~ts_v, $value)->rename(~ts_lo, $lower)->rename(~ts_hi, $upper)->rename(~ts_wd, $width)
}

// a base over the whole line: the value at the line's first date with one (index, no anchor)
function spike::index<T,L,A,V,O>(t: Relation<T>[1], line: ColSpec<L⊆T>[1], date: ColSpec<A⊆T>[1], value: ColSpec<V⊆T>[1], initial: Float[1], out: ColSpec<O>[1]): Relation<Any>[1]
{
  $t->rename($line, ~ts_l)->rename($date, ~ts_d)->rename($value, ~ts_v)
    ->extend(over(~ts_l, [~ts_d->ascending()], rows(unbounded(), unbounded())), ~ts_f:{p,f,r|if($r.ts_v->isNotEmpty(), |$r.ts_d, |[])}:y|$y->min())
    ->extend(over(~ts_l, [~ts_d->ascending()], rows(unbounded(), unbounded())), ~ts_b:{p,f,r|if($r.ts_d == $r.ts_f, |$r.ts_v, |[])}:y|$y->max())
    ->extend(~ts_o:x|if($x.ts_b->toOne() == 0, |[], |$x.ts_v->toOne() / $x.ts_b->toOne() * $initial))
    ->rename(~ts_l, $line)->rename(~ts_d, $date)->rename(~ts_v, $value)->rename(~ts_o, $out)
}

// two columns of one table (a pair's returns, aligned by the caller): their correlation over the window, on the dates
// both have one
function spike::correlation<T,L,A,X,Y,O>(t: Relation<T>[1], line: ColSpec<L⊆T>[1], date: ColSpec<A⊆T>[1], x: ColSpec<X⊆T>[1], y: ColSpec<Y⊆T>[1], w: Integer[1], out: ColSpec<O>[1]): Relation<Any>[1]
{
  $t->rename($line, ~ts_l)->rename($date, ~ts_d)->rename($x, ~ts_x)->rename($y, ~ts_y)
    ->extend(~[ts_px:r|if($r.ts_y->isEmpty(), |[], |$r.ts_x), ts_py:r|if($r.ts_x->isEmpty(), |[], |$r.ts_y)])
    ->extend(over(~ts_l, [~ts_d->ascending()], rows(1 - $w, 0)), ~[ts_c:{p,f,r|rowMapper($r.ts_px, $r.ts_py)}:c|$c->corr(), ts_n:{p,f,r|$r.ts_px}:y|$y->count()])
    ->extend(~ts_o:x|if(($x.ts_n->toOne() < $w) || ($x.ts_n->toOne() < 2), |[], |$x.ts_c))
    ->rename(~ts_l, $line)->rename(~ts_d, $date)->rename(~ts_x, $x)->rename(~ts_y, $y)->rename(~ts_o, $out)
}

// two tables of different columns: each one's line, date and value, joined on the line and the date (a pair aligned
// on the dates both have, as a tile's intersect is)
function spike::pairOf<T,U,LA,AA,VA,LB,AB,VB,OA,OB>(a: Relation<T>[1], la: ColSpec<LA⊆T>[1], da: ColSpec<AA⊆T>[1], va: ColSpec<VA⊆T>[1], b: Relation<U>[1], lb: ColSpec<LB⊆U>[1], db: ColSpec<AB⊆U>[1], vb: ColSpec<VB⊆U>[1], outA: ColSpec<OA>[1], outB: ColSpec<OB>[1]): Relation<Any>[1]
{
  $a->rename($la, ~ts_l)->rename($da, ~ts_d)->rename($va, ~ts_a)->select(~[ts_l, ts_d, ts_a])
    ->join($b->rename($lb, ~ts_lb)->rename($db, ~ts_db)->rename($vb, ~ts_b)->select(~[ts_lb, ts_db, ts_b]), meta::pure::functions::relation::JoinKind.INNER, {x,y|$x.ts_d == $y.ts_db})
    ->rename(~ts_l, $la)->rename(~ts_d, $da)->rename(~ts_a, $outA)->rename(~ts_b, $outB)
}

// a self-join over each value's window (percentiles: the share of the window below each value, those equal counted half)
function spike::percentiles<T,L,A,V,O>(t: Relation<T>[1], line: ColSpec<L⊆T>[1], date: ColSpec<A⊆T>[1], value: ColSpec<V⊆T>[1], w: Integer[1], out: ColSpec<O>[1]): Relation<Any>[1]
{
  let base = $t->rename($line, ~ts_l)->rename($date, ~ts_d)->rename($value, ~ts_v)
    ->extend(over(~ts_l, [~ts_d->ascending()]), ~ts_rn:{p,f,r|$p->rowNumber($r)})
    ->extend(over(~ts_l, [~ts_d->ascending()], rows(1 - $w, 0)), ~ts_n:{p,f,r|$r.ts_v}:y|$y->count());
  let pairs = $base->select(~[ts_l, ts_rn, ts_v])
    ->join($base->select(~[ts_l, ts_rn, ts_v])->rename(~ts_l, ~ts_ol)->rename(~ts_rn, ~ts_orn)->rename(~ts_v, ~ts_ov), meta::pure::functions::relation::JoinKind.INNER, {a,b|($a.ts_l == $b.ts_ol) && ($b.ts_orn > ($a.ts_rn - $w)) && ($b.ts_orn <= $a.ts_rn)})
    ->groupBy(~[ts_l, ts_rn], ~[ts_lt:x|if($x.ts_ov->toOne() < $x.ts_v->toOne(), |1, |[]):y|$y->count(), ts_le:x|if($x.ts_ov->toOne() <= $x.ts_v->toOne(), |1, |[]):y|$y->count(), ts_c:x|$x.ts_ov:y|$y->count()])
    ->rename(~ts_l, ~ts_gl)->rename(~ts_rn, ~ts_grn);
  $base->join($pairs, meta::pure::functions::relation::JoinKind.LEFT, {a,b|($a.ts_l == $b.ts_gl) && ($a.ts_rn == $b.ts_grn)})
    ->extend(~ts_o:x|if($x.ts_v->isEmpty() || ($x.ts_c->toOne() == 0) || ($x.ts_n->toOne() < $w), |[], |($x.ts_lt->toOne() + $x.ts_le->toOne()) / (2 * $x.ts_c->toOne()) * 100))
    ->rename(~ts_l, $line)->rename(~ts_d, $date)->rename(~ts_v, $value)->rename(~ts_o, $out);
}

// a window of a calendar period: each date's values from n units back, by a join of the line to itself on dates
function spike::meanOver<T,L,A,V,O>(t: Relation<T>[1], line: ColSpec<L⊆T>[1], date: ColSpec<A⊆T>[1], value: ColSpec<V⊆T>[1], n: Integer[1], unit: meta::pure::functions::date::DurationUnit[1], out: ColSpec<O>[1]): Relation<Any>[1]
{
  let base = $t->rename($line, ~ts_l)->rename($date, ~ts_d)->rename($value, ~ts_v);
  let windows = $base->select(~[ts_l, ts_d])->extend(~ts_from:x|$x.ts_d->toOne()->adjust(0 - $n, $unit))
    ->join($base->select(~[ts_l, ts_d, ts_v])->rename(~ts_l, ~ts_ol)->rename(~ts_d, ~ts_od), meta::pure::functions::relation::JoinKind.INNER, {a,b|($a.ts_l == $b.ts_ol) && ($b.ts_od > $a.ts_from) && ($b.ts_od <= $a.ts_d)})
    ->groupBy(~[ts_l, ts_d], ~ts_m:x|$x.ts_v:y|$y->average())
    ->rename(~ts_l, ~ts_gl)->rename(~ts_d, ~ts_gd);
  $base->join($windows, meta::pure::functions::relation::JoinKind.LEFT, {a,b|($a.ts_l == $b.ts_gl) && ($a.ts_d == $b.ts_gd)})
    ->extend(over(~ts_l, [~ts_d->ascending()], rows(unbounded(), unbounded())), ~ts_first:{p,f,r|if($r.ts_v->isNotEmpty(), |$r.ts_d, |[])}:y|$y->min())
    ->extend(~ts_o:x|if($x.ts_d->toOne() < $x.ts_first->toOne()->adjust($n, $unit), |[], |$x.ts_m))
    ->rename(~ts_l, $line)->rename(~ts_d, $date)->rename(~ts_v, $value)->rename(~ts_o, $out);
}

// a value as of a calendar period back: an as-of join of the line to its own valued dates
function spike::lagBack<T,L,A,V,O>(t: Relation<T>[1], line: ColSpec<L⊆T>[1], date: ColSpec<A⊆T>[1], value: ColSpec<V⊆T>[1], n: Integer[1], unit: meta::pure::functions::date::DurationUnit[1], out: ColSpec<O>[1]): Relation<Any>[1]
{
  let base = $t->rename($line, ~ts_l)->rename($date, ~ts_d)->rename($value, ~ts_v);
  $base->extend(~ts_at:x|$x.ts_d->toOne()->adjust(0 - $n, $unit))
    ->asOfJoin($base->filter(x|$x.ts_v->isNotEmpty())->select(~[ts_l, ts_d, ts_v])->rename(~ts_l, ~ts_bl)->rename(~ts_d, ~ts_bd)->rename(~ts_v, ~ts_o), {a,b|$a.ts_at >= $b.ts_bd}, {a,b|$a.ts_l == $b.ts_bl})
    ->rename(~ts_l, $line)->rename(~ts_d, $date)->rename(~ts_v, $value)->rename(~ts_o, $out);
}

// a figure of each whole line, a table of one row a line (max_drawdown over every date): a group by the line
function spike::maxDrawdownOf<T,L,A,V,O>(t: Relation<T>[1], line: ColSpec<L⊆T>[1], date: ColSpec<A⊆T>[1], value: ColSpec<V⊆T>[1], out: ColSpec<O>[1]): Relation<Any>[1]
{
  $t->rename($line, ~ts_l)->rename($date, ~ts_d)->rename($value, ~ts_v)
    ->extend(over(~ts_l, [~ts_d->ascending()], rows(unbounded(), 0)), ~ts_pk:{p,f,r|$r.ts_v}:y|$y->max())
    ->extend(~ts_dd:x|if($x.ts_pk->toOne() == 0, |[], |$x.ts_v->toOne() / $x.ts_pk->toOne() - 1))
    ->groupBy(~[ts_l], ~ts_o:x|$x.ts_dd:y|$y->min())
    ->rename(~ts_l, $line)->rename(~ts_o, $out)
}

// a partition by the line and a figure of its own (consecutive: a run of values above 0, counted to each date)
function spike::consecutive<T,L,A,V,O>(t: Relation<T>[1], line: ColSpec<L⊆T>[1], date: ColSpec<A⊆T>[1], value: ColSpec<V⊆T>[1], out: ColSpec<O>[1]): Relation<Any>[1]
{
  $t->rename($line, ~ts_l)->rename($date, ~ts_d)->rename($value, ~ts_v)
    ->extend(~ts_yes:x|if($x.ts_v->isNotEmpty() && ($x.ts_v->toOne() > 0), |1, |0))
    ->extend(~ts_no:x|1 - $x.ts_yes->toOne())
    ->extend(over(~ts_l, [~ts_d->ascending()], rows(unbounded(), 0)), ~ts_grp:{p,f,r|$r.ts_no}:y|$y->plus())
    ->extend(over(~[ts_l, ts_grp], [~ts_d->ascending()], rows(unbounded(), 0)), ~ts_o:{p,f,r|$r.ts_yes}:y|$y->plus())
    ->rename(~ts_l, $line)->rename(~ts_d, $date)->rename(~ts_v, $value)->rename(~ts_o, $out)
}

// the deepest pipeline: an exponential average in closed form (exponential_moving_average), in blocks of W values
function spike::ema<T,L,A,V,O>(t: Relation<T>[1], line: ColSpec<L⊆T>[1], date: ColSpec<A⊆T>[1], value: ColSpec<V⊆T>[1], decay: Float[1], out: ColSpec<O>[1]): Relation<Any>[1]
{
  $t->rename($line, ~ts_l)->rename($date, ~ts_d)->rename($value, ~ts_v)
    ->extend(over(~ts_l, [~ts_d->ascending()], rows(unbounded(), 0)), ~ts_cnt:{p,f,r|$r.ts_v}:y|$y->count())
    ->extend(~ts_k:x|if($x.ts_v->isEmpty() || (($x.ts_cnt->toOne() - 1) < 0), |[], |$x.ts_cnt->toOne() - 1))
    ->extend(~ts_w:x|ceiling((-60.0 * log(2.0)) / log($decay)))
    ->extend(~ts_blk:x|floor($x.ts_k->toOne() / $x.ts_w->toOne()))
    ->extend(~ts_pos:x|$x.ts_k->toOne() - ($x.ts_blk->toOne() * $x.ts_w->toOne()))
    ->extend(~ts_x:x|if($x.ts_k->toOne() == 0, |$x.ts_v->toOne() / (1.0 - $decay), |$x.ts_v))
    ->extend(~ts_u:x|$x.ts_x->toOne() * pow($decay, 0 - $x.ts_pos->toOne()))
    ->extend(over(~[ts_l, ts_blk], [~ts_d->ascending()], rows(unbounded(), 0)), ~ts_s:{p,f,r|$r.ts_u}:y|$y->plus())
    ->extend(~ts_in:x|((1.0 - $decay) * pow($decay, $x.ts_pos->toOne())) * $x.ts_s->toOne())
    ->extend(over(~[ts_l, ts_blk], [~ts_d->ascending()], rows(unbounded(), unbounded())), ~ts_t:{p,f,r|$r.ts_u}:y|$y->plus())
    ->extend(~ts_has:x|if($x.ts_k->isEmpty(), |0, |1))
    ->extend(over(~[ts_l, ts_has], [~ts_d->ascending()]), ~ts_before:{p,f,r|$p->lag($r).ts_t})
    ->extend(~ts_c0:x|if($x.ts_pos->toOne() == 0, |$x.ts_before, |[]))
    ->extend(over(~[ts_l, ts_blk], [~ts_d->ascending()], rows(unbounded(), unbounded())), ~ts_car:{p,f,r|$r.ts_c0}:y|$y->max())
    ->extend(~ts_o:x|if($x.ts_k->isEmpty(), |[], |if($x.ts_car->isEmpty(), |$x.ts_in, |$x.ts_in->toOne() + (pow($decay, $x.ts_pos->toOne() + 1) * (((1.0 - $decay) * pow($decay, $x.ts_w->toOne() - 1)) * $x.ts_car->toOne())))))
    ->rename(~ts_l, $line)->rename(~ts_d, $date)->rename(~ts_v, $value)->rename(~ts_o, $out)
}

// a return of a type told by a word: does the expansion fold the word's comparison and keep one branch?
function spike::returnsOf<T,L,A,V,O>(t: Relation<T>[1], line: ColSpec<L⊆T>[1], date: ColSpec<A⊆T>[1], value: ColSpec<V⊆T>[1], type: String[1], out: ColSpec<O>[1]): Relation<Any>[1]
{
  $t->rename($line, ~ts_l)->rename($date, ~ts_d)->rename($value, ~ts_v)
    ->extend(over(~ts_l, [~ts_d->ascending()]), ~ts_o:{p,f,r|if($type == 'log', |log($r.ts_v->toOne()) - log($p->lag($r).ts_v->toOne()), |$r.ts_v->toOne() / $p->lag($r).ts_v->toOne() - 1)})
    ->rename(~ts_l, $line)->rename(~ts_d, $date)->rename(~ts_v, $value)->rename(~ts_o, $out)
}

// a declared return of the table and one column, its body leaking two more: is the declaration held to the body?
function spike::leaks<T,L,A,V,O>(t: Relation<T>[1], line: ColSpec<L⊆T>[1], date: ColSpec<A⊆T>[1], value: ColSpec<V⊆T>[1], out: ColSpec<O>[1]): Relation<T+O>[1]
{
  $t->rename($value, ~ts_v)->extend(~[ts_a:x|1, ts_o:x|$x.ts_v])->rename(~ts_v, $value)->rename(~ts_o, $out)
}
`;

const TS: TimeseriesSpec = { sources: [{ alias: 'px', cube: 'prices', date: 'day', key: 'symbol' }], series: [] };
const spec = (expr: string): TimeseriesSpec => ({ ...TS, series: [{ id: 'A', expr: parseExpr(expr) } as TsSeries] });

const writer = plannerFor('', 'local::RT');
let engine: DuckDbEngine;
let planner: LitePlanner;
let spiked: LitePlanner;
let inputs: TsInputs;
let files: DuckDbFiles;
/** The prices' relation as Pure text (`#>{...}#`), what each spike query reads. */
let source = '';

before(async () => {
  const db = await openDatabase();
  engine = new DuckDbEngine(db.connection);
  files = db.files;
  const r = await ingestFile(engine, files, new File([CSV], 'prices.csv'), writer);
  planner = plannerFor(r.model, r.runtime);
  spiked = plannerFor(`${r.model}\n${DATACUBE_LIBRARY}\n${SPIKE}`, r.runtime);
  const snapshot: CubeSnapshot = {
    source: { query: r.source },
    columns: [
      { name: 'symbol', type: 'String' },
      { name: 'day', type: 'StrictDate' },
      { name: 'close', type: 'Float' },
      { name: 'volume', type: 'Integer', kind: 'measure' },
    ],
    derived: [],
    rows: ['symbol'],
    pivotOn: [],
    sorts: [],
    epoch: 1,
  };
  inputs = new Map([['px', { snapshot, filter: undefined }]]);
  source = (await planner.print(lambda([], r.source), 'STANDARD')).trim().replace(/^\|/, '');
});

after(async () => {
  await engine?.close();
});

const num = (v: unknown): number | null => (v === null || v === undefined ? null : Number(v));

function column(t: ResultTable, name: string): unknown[] {
  const c = t.columns.find((x) => x.name === name);
  assert.ok(c, `the answer has a column '${name}' (it has ${t.columns.map((x) => x.name).join(', ')})`);
  return c.values.map((v) => (typeof v === 'bigint' ? Number(v) : v));
}

/** Today's writer's line of an expression over AAA, by date (oldest first). */
async function today(expr: string, value = TS_VALUE): Promise<Map<string, number | null>> {
  const ts = spec(expr);
  const q = seriesQuery(inputs, ts, ts.series[0]!, {}, {});
  const t = await engine.execute(await planner.plan(q.query), 0);
  const dates = column(t, TS_DATE).map((d) => String(d).slice(0, 10));
  const values = column(t, value).map(num);
  return new Map(dates.map((d, i) => [d, values[i]!] as const));
}

/** A spike query's answer: Pure text over the prices (`$prices`), run; its AAA rows' `out` by date. */
async function spikeLine(text: string, out: string, symbol = 'AAA'): Promise<Map<string, number | null>> {
  const query = `|${text.replaceAll('$prices', source)}`;
  const t = await engine.execute(await spiked.planText(query), 0);
  const symbols = column(t, 'symbol');
  const dates = column(t, 'day').map((d) => String(d).slice(0, 10));
  const values = column(t, out).map(num);
  return new Map(dates.flatMap((d, i) => (symbols[i] === symbol ? [[d, values[i]!] as const] : [])));
}

function same(got: Map<string, number | null>, want: Map<string, number | null>, what: string, tolerance = 1e-9): void {
  assert.deepEqual([...got.keys()].sort(), [...want.keys()].sort(), `${what}: the same dates`);
  for (const [d, w] of want) {
    const g = got.get(d) ?? null;
    if (w === null || g === null) {
      assert.equal(g, w, `${what} at ${d}`);
      continue;
    }
    assert.ok(Math.abs(g - w) <= tolerance * Math.max(1, Math.abs(w)), `${what} at ${d}: ${g} where today's writer has ${w}`);
  }
}

describe('the Pure library spike: one function of each family, held to today\'s writer', () => {
  it('prints the source it reads', () => {
    assert.match(source, /#>\{/);
  });

  it('multi-step windows (zscores): a mean and a deviation, then a row\'s figure', async () => {
    same(await spikeLine('$prices->spike::zscores(~symbol, ~day, ~close, 10, ~z)', 'z'), await today('zscores(close[AAA], 10)'), 'zscores');
  });

  it('several outputs (bollinger_bands): lower, upper and width', async () => {
    const q = '$prices->spike::bollinger(~symbol, ~day, ~close, 5, 2.0, ~lo, ~hi, ~wd)';
    same(await spikeLine(q, 'lo'), await today('bollinger_bands(close[AAA], 5, 2)', TS_LOWER), 'lower');
    same(await spikeLine(q, 'hi'), await today('bollinger_bands(close[AAA], 5, 2)', TS_UPPER), 'upper');
  });

  it('a base over the whole line (index): its first value', async () => {
    same(await spikeLine('$prices->spike::index(~symbol, ~day, ~close, 100.0, ~ix)', 'ix'), await today('index(close[AAA], 100)'), 'index');
  });

  it('two tables of different columns, aligned (pairOf), then two columns of one (correlation of their returns)', async () => {
    const returns = (s: string, r: string): string =>
      `$prices->filter(x|$x.symbol == '${s}')->timeseries::returns(over(~symbol, [~day->ascending()]), ~close, 1, timeseries::Returns.simple, ~${r})`;
    const q = `${returns('AAA', 'ra')}->select(~[symbol, day, ra])->spike::pairOf(~symbol, ~day, ~ra, ${returns('BBB', 'rb')}->select(~[symbol, day, rb])->rename(~symbol, ~sb), ~sb, ~day, ~rb, ~ra2, ~rb2)`
      + '->spike::correlation(~symbol, ~day, ~ra2, ~rb2, 10, ~c)';
    same(await spikeLine(q, 'c'), await today('correlation(close[AAA], close[BBB], 10)'), 'correlation');
  });

  it('a self-join over each window (percentiles)', async () => {
    same(await spikeLine('$prices->spike::percentiles(~symbol, ~day, ~close, 5, ~pr)', 'pr'), await today('percentiles(close[AAA], 5)'), 'percentiles');
  });

  it('a window of a calendar period (mean over 2w): the line joined to itself on dates', async () => {
    same(await spikeLine('$prices->spike::meanOver(~symbol, ~day, ~close, 2, meta::pure::functions::date::DurationUnit.WEEKS, ~m)', 'm'),
      await today('mean(close[AAA], 2w)'), 'mean over 2w');
  });

  it('a value as of a calendar period back (lag 1m): an as-of join', async () => {
    same(await spikeLine('$prices->spike::lagBack(~symbol, ~day, ~close, 1, meta::pure::functions::date::DurationUnit.MONTHS, ~b)', 'b'),
      await today('lag(close[AAA], 1m)'), 'lag 1m');
  });

  it('a figure of each whole line (max drawdown): a group by the line, its working columns gone with the group', async () => {
    const t = await engine.execute(await spiked.planText(`|${source}->spike::maxDrawdownOf(~symbol, ~day, ~close, ~mdd)`), 0);
    assert.deepEqual(t.columns.map((c) => c.name), ['symbol', 'mdd'], 'a group keeps only its keys and figures');
    const of = new Map(column(t, 'symbol').map((s, i) => [s, num(column(t, 'mdd')[i])] as const));
    // the writer's running figure at the line's last date (its answer comes latest first)
    const line = await today('max_drawdown(close[AAA])');
    const last = [...line.keys()].sort().at(-1)!;
    same(new Map([['all', of.get('AAA')!]]), new Map([['all', line.get(last)!]]), 'max drawdown');
  });

  it('a partition by the line and a figure of its own (consecutive), over a library call (diff)', async () => {
    same(await spikeLine('$prices->timeseries::diff(over(~symbol, [~day->ascending()]), ~close, 1, ~dx)->spike::consecutive(~symbol, ~day, ~dx, ~run)', 'run'),
      await today('consecutive(diff(close[AAA]))'), 'consecutive');
  });

  it('the deepest pipeline (an exponential average in closed form): fifteen steps, partitions by its own columns', async () => {
    same(await spikeLine('$prices->spike::ema(~symbol, ~day, ~close, 0.9, ~e)', 'e'), await today('exponential_moving_average(close[AAA], 0.9)'), 'ema');
  });
});

// -- the gaps: what the families above need that Pure's relations, or lite, do not give yet --

describe('the Pure library spike: the gaps, as they are today', () => {
  it('GAP 1, closed by without: a function drops its working columns, its answer the table\'s and its own', async () => {
    const t = await engine.execute(await spiked.planText(`|${source}->spike::zscores(~symbol, ~day, ~close, 10, ~z)`), 0);
    assert.deepEqual(t.columns.map((c) => c.name), ['symbol', 'day', 'close', 'volume', 'z']);
  });

  it('a function over its own answer: the first call\'s working columns are gone, so the second makes them again', async () => {
    const t = await engine.execute(await spiked.planText(`|${source}->spike::zscores(~symbol, ~day, ~close, 10, ~z)->spike::zscores(~symbol, ~day, ~z, 5, ~zz)`), 0);
    assert.deepEqual(t.columns.map((c) => c.name), ['symbol', 'day', 'close', 'volume', 'z', 'zz']);
  });

  it('GAP 2, closed: a caller\'s column of a working name is the caller\'s, the function\'s own renamed at the call', async () => {
    const t = await engine.execute(await spiked.planText(`|${source}->extend(~ts_m:x|1)->spike::zscores(~symbol, ~day, ~close, 10, ~z)`), 0);
    assert.deepEqual(t.columns.map((c) => c.name), ['symbol', 'day', 'close', 'volume', 'ts_m', 'z']);
    assert.ok(column(t, 'ts_m').every((v) => num(v) === 1), 'the caller\'s ts_m untouched');
    same(await spikeLine('$prices->extend(~ts_m:x|1)->spike::zscores(~symbol, ~day, ~close, 10, ~z)', 'z'),
      await today('zscores(close[AAA], 10)'), 'zscores beside a caller\'s ts_m');
  });

  it('GAP 3, closed: a declared return is held to each call, so a body leaking a column is refused by name', async () => {
    await assert.rejects(spiked.planText(`|${source}->spike::leaks(~symbol, ~day, ~close, ~o)`),
      (e: Error) => /spike::leaks/.test(e.message) && /\[ts_a\] not declared/.test(e.message), 'the leak is named');
  });

  it('a word told as a parameter folds: one function of a type keeps one branch, the SQL the other\'s none', async () => {
    const log = await spiked.planText(`|${source}->spike::returnsOf(~symbol, ~day, ~close, 'log', ~r)`);
    const simple = await spiked.planText(`|${source}->spike::returnsOf(~symbol, ~day, ~close, 'simple', ~r)`);
    assert.match(log.sql, /LN\(|LOG\(/i);
    assert.doesNotMatch(simple.sql, /LN\(|LOG\(/i, 'the simple return\'s SQL has no log: the word was compared, never sent');
    same(await spikeLine('$prices->spike::returnsOf(~symbol, ~day, ~close, \'simple\', ~r)', 'r'), await today('returns(close[AAA])'), 'returns');
  });

  it('the SQL a library function plans to, beside today\'s writer\'s, for the same figure', async () => {
    const ts = spec('exponential_moving_average(close[AAA], 0.9)');
    const writer_ = await planner.plan(seriesQuery(inputs, ts, ts.series[0]!, {}, {}).query);
    const library = await spiked.planText(`|${source}->spike::ema(~symbol, ~day, ~close, 0.9, ~e)`);
    console.log(`SQL characters: today's writer ${writer_.sql.length}, the library's ${library.sql.length}`);
    assert.ok(library.sql.length < writer_.sql.length * 3, 'the same order of size');
  });
});
