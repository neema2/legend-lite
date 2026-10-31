// THE TIMESERIES OVER THE TRADING MODEL'S CLASSES (docs/DATACUBE_TIMESERIES_DESIGN_2026_10_25.md §11, Phase 6; the user,
// 2026-10-27: "start all the charts from Class models projected to Relation ... the same Stress data model"): the
// markets sample's days in the stress corpus's own tables (stress-markets.ts), read through its classes by
// stress::AllMapping -- `marketdata::MarketQuote.all()->project(~[ticker: x|$x.instrument.ticker, ...])` -- and each
// series of a tile over them held to the same series over the sample's one table: the class is a source as a table
// is, every figure the database's. The model is the markets project as the site serves it (BUILD.bazel
// :markets_project, config.json `projects[]`): the one trading model, the Deephaven line's text
// (//scripts/corpus:trading_model), and the sample's days as its Data; the example page over it
// (examples/markets-classes.ts) opened the page's way, every series of it run, and each answer held to its frozen
// answer (timeseries-frozen.ts; docs/TS_ONE_TREE_PLAN_2026_10_31.md §6, step 1).

import assert from 'node:assert/strict';
import { readdirSync, readFileSync } from 'node:fs';
import { join } from 'node:path';
import { after, before, describe, it } from 'node:test';
import { openDatabase } from '#test-host';

import { DuckDbEngine } from '../../engine-client/src/duckdb.ts';
import { runfileFromEnv } from '../../tools/js/runfiles.mts';
import { derive, element, fn, lambda, lit, projectObjects, property, variable, type ValueSpecification } from '../../pure-protocol/src/index.ts';
import { MARKETS_PROJECT, marketsClassesPage, SAVED_GRIDS, savedQuerySource, savedQueryTree } from '../src/examples/markets-classes.ts';
import type { LitePlanner } from '../src/lite-planner.ts';
import { savedQueryOpened } from '../src/saved-queries.ts';
import { marketsCsv } from '../src/samples.ts';
import type { CubeSnapshot } from '../src/snapshot.ts';
import { sourceColumns } from '../src/source-columns.ts';
import { STRESS_MARKET_COLUMNS, STRESS_MARKET_TABLES, stressMarketCsvs, stressMarketsData } from '../src/stress-markets.ts';
import { parseExpr } from '../src/timeseries-expr.ts';
import { marksQuery, seriesQuery, TS_DATE, TS_SPLIT, TS_TEXT, TS_VALUE, type TsInputs } from '../src/timeseries-query.ts';
import type { TimeseriesSpec, TsAnnotation } from '../src/timeseries-spec.ts';
import { ingestFile, type DuckDbFiles } from '../src/upload.ts';
import { plannerFor } from './catalog-builder.ts';
import { drawnColumns, frozenOf, holdFrozen, type Frozen } from './timeseries-frozen.ts';

/** The markets project as config.json lists it: its model files, each as the site serves it. */
const CONFIGURED = (JSON.parse(readFileSync(runfileFromEnv('DEMO_CONFIG'), 'utf8')) as {
  projects: { groupId: string; artifactId: string; versionId: string; models: string[] }[];
}).projects.find((p) => p.groupId === MARKETS_PROJECT.groupId && p.artifactId === MARKETS_PROJECT.artifactId && p.versionId === MARKETS_PROJECT.versionId);
const SERVED = runfileFromEnv('MARKETS_PROJECT');
const served = (url: string): string => readFileSync(join(SERVED, url.slice('./projects/markets/'.length)), 'utf8');
const MODEL = served('./projects/markets/trading-model.pure');
const writer = plannerFor('', 'local::RT');

/** The model a page holds: the project's files joined as the page joins them (app/sources.ts openedRecord) -- the
 *  trading model and the markets sample's rows as its Data. */
const PROJECT = (CONFIGURED?.models ?? []).map(served).join('\n');

/** The table's database (the markets sample read as one file) and the project's own (its Data's), each its own. */
let engine: DuckDbEngine;
let projectEngine: DuckDbEngine;
let files: DuckDbFiles;
let corpus: LitePlanner;
let table: LitePlanner;
let classes: TsInputs;
let referenced: TsInputs;
let rows: TsInputs;
/** How long the trading model took to be held, and its first query to plan: what a page waits for. */
const measured: Record<string, number> = {};
/** How many lines each split series of the example page drew, by its tile and its id. */
const drawn = new Map<string, number>();
/** Each series of the example page, its answer frozen, by its tile and its id. */
const answers = new Map<string, Frozen>();

/** `$x.a.b...`: a path of properties from the row. */
const path = (...names: string[]): ValueSpecification => names.reduce<ValueSpecification>((v, n) => property(v, n), variable('x'));

/** A quote a day through the corpus's classes: its instrument's ticker, its time, its close. */
const QUOTES = projectObjects(fn('getAll', element('marketdata::MarketQuote')), [
  derive('ticker', lambda(['x'], path('instrument', 'ticker'))),
  derive('quoteTime', lambda(['x'], path('quoteTime'))),
  derive('closePrice', lambda(['x'], path('closePrice'))),
]).node;

/** A quote a day with its instrument's reference data, through the associations: its sector's name and its listing's
 *  exchange's (cross::Instrument_Sector, cross::Instrument_Exchange), and the day's volume. */
const REFERENCED = projectObjects(fn('getAll', element('marketdata::MarketQuote')), [
  derive('ticker', lambda(['x'], path('instrument', 'ticker'))),
  derive('sector', lambda(['x'], path('instrument', 'sector', 'name'))),
  derive('exchange', lambda(['x'], path('instrument', 'exchange', 'name'))),
  derive('quoteTime', lambda(['x'], path('quoteTime'))),
  derive('closePrice', lambda(['x'], path('closePrice'))),
  derive('volume', lambda(['x'], path('volume'))),
]).node;

const snapshotOf = (query: ValueSpecification, columns: CubeSnapshot['columns']): CubeSnapshot => ({
  source: { query }, columns, derived: [], rows: [], pivotOn: [], sorts: [], epoch: 1,
});

before(async () => {
  const db = await openDatabase();
  engine = new DuckDbEngine(db.connection);
  files = db.files;
  const flat = await ingestFile(engine, files, new File([marketsCsv()], 'prices.csv'), writer, { table: 'prices' });
  table = plannerFor(flat.model, flat.runtime);
  // the project's rows, its Data element's, in a database of its own as a page makes it (C5, project-data.ts): each
  // table made by the store's declaration
  let t0 = performance.now();
  corpus = plannerFor(PROJECT, 'stress::RT');
  projectEngine = new DuckDbEngine((await openDatabase()).connection);
  const project = await corpus.projectData(PROJECT, projectEngine.databaseType);
  for (const sql of [...project.open, ...project.use, ...project.fill]) await projectEngine.run(sql, 0);
  measured['the model held and its Data filled (ms)'] = performance.now() - t0;
  classes = new Map([['px', { snapshot: snapshotOf(QUOTES, [
    { name: 'ticker', type: 'String' }, { name: 'quoteTime', type: 'DateTime' }, { name: 'closePrice', type: 'Float' },
  ]), filter: undefined }]]);
  referenced = new Map([['px', { snapshot: snapshotOf(REFERENCED, [
    { name: 'ticker', type: 'String' }, { name: 'sector', type: 'String' }, { name: 'exchange', type: 'String' },
    { name: 'quoteTime', type: 'DateTime' }, { name: 'closePrice', type: 'Float' }, { name: 'volume', type: 'Float' },
  ]), filter: undefined }]]);
  rows = new Map([['px', { snapshot: snapshotOf(flat.source, [
    { name: 'symbol', type: 'String' }, { name: 'asset_class', type: 'String' }, { name: 'date', type: 'StrictDate' },
    { name: 'open', type: 'Float' }, { name: 'high', type: 'Float' }, { name: 'low', type: 'Float' },
    { name: 'close', type: 'Float' }, { name: 'volume', type: 'Integer' },
  ]), filter: undefined }]]);
  t0 = performance.now();
  await corpus.plan(lambda([], fn('limit', QUOTES, lit.integer(1))));
  measured['a first query planned (ms)'] = performance.now() - t0;
  t0 = performance.now();
  await corpus.plan(lambda([], fn('limit', QUOTES, lit.integer(2))));
  measured['a query planned after (ms)'] = performance.now() - t0;
});

after(async () => {
  // the planning cost of a page over the trading model, said where the run is read
  console.log(`trading model: ${(MODEL.length / 1e6).toFixed(2)} MB, its Data ${((PROJECT.length - MODEL.length) / 1e6).toFixed(2)} MB; `
    + Object.entries(measured).map(([k, v]) => `${k} ${v.toFixed(0)}`).join('; '));
  await engine?.close();
  await projectEngine?.close();
});

type Over = 'classes' | 'referenced' | 'rows';
type Line = { dates: string[]; values: (number | null)[] };

/** A tile of one series over the class (`classes`, `referenced`: with its reference data) or the table (`rows`),
 *  daily, a line per value of `split` when it is given. */
const spec = (text: string, over: Over, split?: string): TimeseriesSpec => ({
  sources: [over === 'rows'
    ? { alias: 'px', cube: 'prices', date: 'date', key: 'symbol' }
    : { alias: 'px', cube: 'quotes', date: 'quoteTime', key: 'ticker' }],
  series: [{ id: 'A', expr: parseExpr(over === 'rows' ? text : text.replaceAll('close[', 'closePrice[')), ...(split !== undefined ? { split } : {}) }],
  grain: 'day',
});

/** The series' lines, each by its split's value ('' when it has none), earliest first, as the database answers them. */
async function lines(text: string, over: Over, split?: string): Promise<Map<string, Line>> {
  const ts = spec(text, over, split);
  const planner = over === 'rows' ? table : corpus;
  const q = seriesQuery(over === 'rows' ? rows : over === 'classes' ? classes : referenced, ts, ts.series[0]!);
  const t = await (over === 'rows' ? engine : projectEngine).execute(await planner.plan(q.query), 0);
  const col = (name: string): unknown[] => t.columns.find((c) => c.name === name)!.values as unknown[];
  const dates = col(TS_DATE);
  const values = col(TS_VALUE);
  const keys = split !== undefined ? col(TS_SPLIT).map(String) : dates.map(() => '');
  const out = new Map<string, Line>();
  dates.forEach((d, i) => {
    const l = out.get(keys[i]!) ?? { dates: [], values: [] };
    out.set(keys[i]!, l);
    l.dates.unshift(String(d).slice(0, 10));
    l.values.unshift(values[i] === null ? null : Number(values[i]));
  });
  return out;
}

async function line(text: string, over: Over): Promise<Line> {
  return (await lines(text, over)).get('')!;
}

/** Two lines the same: the same dates, each value within a part in 10⁹ (a sum's order is the database's). */
function same(over: Line, flat: Line, what: string): void {
  assert.ok(flat.values.filter((v) => v !== null).length > 100, `${what}: the sample has the figures`);
  assert.deepEqual(over.dates, flat.dates, `${what}: the same dates`);
  over.values.forEach((v, i) => {
    const w = flat.values[i]!;
    if (v === null || w === null) assert.equal(v, w, `${what} ${over.dates[i]}: ${v} where ${w}`);
    else assert.ok(Math.abs(v - w) <= 1e-9 * Math.max(1, Math.abs(w)), `${what} ${over.dates[i]}: ${v}, not ${w}`);
  });
}

describe('the markets project, as config.json lists it and the site serves it', () => {
  it('its files the trading model and its Data, in that order, each served', () => {
    assert.ok(CONFIGURED, 'config.json lists the markets project');
    assert.deepEqual(CONFIGURED.models, ['./projects/markets/trading-model.pure', './projects/markets/markets-data.pure']);
    assert.deepEqual(readdirSync(SERVED).sort(), ['markets-data.pure', 'trading-model.pure'], 'the site serves those two');
    assert.equal(served('./projects/markets/markets-data.pure'), stressMarketsData(), 'its Data the generator\'s');
    assert.equal(PROJECT, `${MODEL}\n${stressMarketsData()}`);
  });
});

describe('the trading model\'s tables, as its store declares them', () => {
  it('each table the generator writes has the store\'s columns, in its order', () => {
    // the corpus's one store: its tables after its name (a linked project's store may name a table so too)
    const store = MODEL.slice(MODEL.indexOf('Database store::DB'));
    for (const t of STRESS_MARKET_TABLES) {
      const declared = new RegExp(`Table ${t} \\(([^\\n]*)\\)\\n`).exec(store);
      assert.ok(declared, `the store declares ${t}`);
      const names = declared[1]!.split(/,\s*/).map((c) => c.trim().split(/\s+/)[0]);
      assert.deepEqual([...STRESS_MARKET_COLUMNS[t]], names, `${t}'s columns`);
    }
  });
});

describe('a timeseries over MarketQuote through stress::AllMapping is the timeseries over the table', () => {
  for (const text of ['close[AAPL]', 'volatility(close[AAPL], 22)', 'correlation(close[AAPL], close[MSFT], 63)',
    'returns(close[NVDA], 5)', 'beta(close[JPM], close[SPX], 2m)']) {
    it(text, async () => {
      const [over, flat] = await Promise.all([line(text, 'classes'), line(text, 'rows')]);
      same(over, flat, text);
    });
  }
});

// D5: the reference data a quote reaches through its instrument -- its sector, its exchange -- as a series' matches and
// its split; the flat table has neither, so each is held to the instruments it names, added or one by one.
describe('a series matched and split by the reference data the associations reach', () => {
  it('close[sector="Energy"] is XOM\'s close: the one instrument of its sector', async () => {
    same(await line('close[sector="Energy"]', 'referenced'), await line('close[XOM]', 'rows'), 'Energy');
  });

  it('a sector\'s volume summed is its instruments\' volumes added', async () => {
    same(await line('volume[sector="Information Technology"].sum', 'referenced'),
      await line('volume[AAPL].sum + volume[MSFT].sum + volume[NVDA].sum', 'rows'), 'IT volume');
  });

  it('split by ticker within a sector: a line per instrument of it, each its own returns', async () => {
    const over = await lines('returns(close[sector="Information Technology"], 1)', 'referenced', 'ticker');
    assert.deepEqual([...over.keys()].sort(), ['AAPL', 'MSFT', 'NVDA'], 'the sector\'s instruments');
    for (const t of over.keys()) same(over.get(t)!, await line(`returns(close[${t}], 1)`, 'rows'), t);
  });

  it('split by exchange: a line per listing venue, each the volume of the instruments listed there', async () => {
    const over = await lines('volume.sum', 'referenced', 'exchange');
    assert.deepEqual([...over.keys()].sort(), ['Cboe Options Exchange', 'NYSE Arca', 'Nasdaq', 'New York Stock Exchange', 'Off exchange'], 'the exchanges listing them');
    same(over.get('New York Stock Exchange')!, await line('volume[JPM].sum + volume[XOM].sum', 'rows'), 'NYSE');
    same(over.get('Nasdaq')!, await line('volume[NDX].sum + volume[AAPL].sum + volume[MSFT].sum + volume[NVDA].sum + volume[TLT].sum', 'rows'), 'Nasdaq');
  });
});

// D3: the example page's grids opened as the page opens a saved query (app/sources.ts openedRecord: the project's
// model, saved-queries.ts savedQueryOpened -- its context, its enumerations as names), and every series of its tiles
// planned by the grid's planner and run on the project's own database.
describe('the example page over the classes, opened the page\'s way', () => {
  /** Each grid opened: its planner (its project's model, its mapping) and its rows as a tile reads them. */
  const opened = new Map<string, { planner: LitePlanner; snapshot: CubeSnapshot }>();
  const planners: LitePlanner[] = [];
  /** The page's own module, cold: a planner of its own (each module holds its own models), its grids another on it. */
  let page: LitePlanner;

  before(async () => {
    page = plannerFor('', 'local::RT');
    planners.push(page);
    const reading = { parse: (text: string) => page.parse(text), elements: (text: string) => page.modelElements(text) };
    for (const g of SAVED_GRIDS) {
      const t0 = performance.now();
      const q = savedQuerySource(g).query;
      let planner: LitePlanner | undefined;
      const { source } = await savedQueryOpened(q, PROJECT, reading, (runtime, how) => {
        // the page's planner of a source's own model: another on the page's module (demo/planners.ts `another`)
        planner = page.withModel(PROJECT, runtime, how);
        planners.push(planner);
        return planner;
      });
      const columns = await sourceColumns(planner!, source);
      opened.set(g.id, { planner: planner!, snapshot: snapshotOf(source, columns.map((c) => ({ name: c.name, type: c.type }))) });
      measured[`${g.id} opened as a saved query (ms)`] = performance.now() - t0;
      // then its project's database, as the page asks for it (app/sources.ts openedRecord): the model held by now
      const t1 = performance.now();
      const statements = await page.projectData(PROJECT, projectEngine.databaseType);
      measured[`${g.id}'s project database said (ms)`] = performance.now() - t1;
      assert.equal(statements.name, (await corpus.projectData(PROJECT, projectEngine.databaseType)).name, 'the one project database');
    }
  });

  after(() => {
    for (const p of planners) p.dispose();
  });

  it('each grid\'s saved query is its text\'s tree, and answers the columns the page writes down', async () => {
    for (const g of SAVED_GRIDS) {
      const parsed = await corpus.parse(savedQuerySource(g).query.content);
      assert.deepEqual(parsed.body[0], savedQueryTree(g), `${g.id}: its text parsed is the cube's query`);
      assert.deepEqual(opened.get(g.id)!.snapshot.columns.map((c) => `${c.name}:${c.type}`),
        g.columns.map((c) => `${c.name}:${c.type}`), `${g.id}: the columns its source records`);
    }
  });

  for (const v of marketsClassesPage().views) {
    if (v.kind !== 'timeseries') continue;
    it(`${v.title}: every series answers from the project's rows`, async () => {
      const inputs: TsInputs = new Map(v.spec.sources.map((src) => [src.alias, { snapshot: opened.get(src.cube)!.snapshot, filter: undefined }] as const));
      const planner = opened.get(v.spec.sources[0]!.cube)!.planner;
      for (const series of v.spec.series) {
        const q = seriesQuery(inputs, v.spec, series);
        const t0 = performance.now();
        const plan = await planner.plan(q.query);
        measured[`${v.id} ${series.id} planned (ms)`] = performance.now() - t0;
        const t = await projectEngine.execute(plan, 0);
        const values = t.columns.find((c) => c.name === TS_VALUE)!.values as unknown[];
        const lines = series.split !== undefined ? new Set(t.columns.find((c) => c.name === TS_SPLIT)!.values as unknown[]).size : 1;
        // a column's own aggregate (`volume.sum`: a sector's day of volume) is one value a date and line, a frequency
        // or none (docs/TS_LANGUAGE_BUILD_2026_10_30.md P6: its writer had read each row as a point there)
        const dates = t.columns.find((c) => c.name === TS_DATE)!.values as unknown[];
        const split = t.columns.find((c) => c.name === TS_SPLIT)?.values as unknown[] | undefined;
        const at = dates.map((d, i) => `${String(d).slice(0, 10)}|${split ? String(split[i]) : ''}`);
        if (JSON.stringify(series.expr).includes('"fn":"') || v.spec.grain !== undefined) {
          assert.equal(new Set(at).size, at.length, `${series.id} (${series.name}): one value a date and line`);
        }
        assert.ok(values.filter((x) => x !== null).length >= 20 * lines, `${series.id} (${series.name}): ${values.length} rows, ${lines} lines`);
        drawn.set(`${v.id} ${series.id}`, lines);
        answers.set(`${v.id} ${series.id}`, frozenOf(t, TS_DATE, q.split === undefined ? undefined : TS_SPLIT, drawnColumns(q)));
      }
    });
  }

  it('every series of the page answers as it was frozen', () => {
    holdFrozen('TS_ANSWERS', answers, 'markets-classes-answers');
  });

  it('the trades tile reads both grids, and its notional by sector a line per sector the bank trades in', async () => {
    const v = marketsClassesPage().views.find((x) => x.id === 'trades-vs-close')!;
    assert.ok(v.kind === 'timeseries' && v.spec.sources.length === 2);
    assert.equal(drawn.get('trades-vs-close D'), 5, 'Technology, Financials, Energy, Commodities, Rates');
    assert.equal(drawn.get('venues A'), 5, 'every listing venue');
    assert.equal(drawn.get('sectors A'), 3, 'the three technology names');
  });

  // D4: the trades as marks from the trades grid's rows (TsMarks) -- each the TRADE table's row, through the mapping
  it('the trades as marks: each Apple buy and sell at its price on its day, its quantity beside it, as TRADE holds it', async () => {
    const v = marketsClassesPage().views.find((x) => x.id === 'trades-vs-close')!;
    assert.ok(v.kind === 'timeseries');
    const [header, ...lines] = stressMarketCsvs().TRADE.trim().split('\n');
    const at = (name: string): number => header!.split(',').indexOf(name);
    const rows = lines.map((l) => l.split(','));
    const trades = opened.get('trades')!;
    for (const [i, side, code] of [[0, 'BUY', 'B'], [1, 'SELL', 'S']] as const) {
      const marks: TsAnnotation = v.spec.annotations![i]!;
      if (marks.kind !== 'marks') throw new Error(`annotation ${i} is no marks`);
      assert.equal(marks.label, side === 'BUY' ? 'Bought' : 'Sold');
      const q = marksQuery(new Map([['tr', { snapshot: trades.snapshot, filter: undefined }]]), v.spec, marks);
      const t = await projectEngine.execute(await trades.planner.plan(q), 0);
      const col = (name: string): unknown[] => t.columns.find((c) => c.name === name)!.values as unknown[];
      const want = rows.filter((r) => r[at('INSTRUMENT_ID')] === 'INS-AAPL' && r[at('SIDE')] === code)
        .map((r) => [r[at('TRADE_DATE')]!, Number(r[at('PRICE')]), Number(r[at('QUANTITY')])] as const)
        .sort((a, b) => b[0].localeCompare(a[0]));
      assert.ok(want.length > 20, `${side}: the sample's Apple trades`);
      assert.deepEqual(col(TS_DATE).map((d, k) => [String(d).slice(0, 10), Number(col(TS_VALUE)[k]), Number(col(TS_TEXT)[k])]), want, `${side}: each a TRADE row, latest first`);
    }
  });
});
