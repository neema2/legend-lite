// THE TIMESERIES OVER THE TRADING MODEL'S CLASSES (docs/DATACUBE_TIMESERIES_DESIGN_2026_10_25.md §11, Phase 6; the user,
// 2026-10-27: "start all the charts from Class models projected to Relation ... the same Stress data model"): the
// markets sample's days in the stress corpus's own tables (stress-markets.ts), read through its classes by
// stress::AllMapping -- `marketdata::MarketQuote.all()->project(~[ticker: x|$x.instrument.ticker, ...])` -- and each
// series of a tile over them held to the same series over the sample's one table: the class is a source as a table
// is, every figure the database's. The model is the markets project as the site serves it (BUILD.bazel
// :markets_project, demo/releases/index.json): the one trading model, the Deephaven line's text
// (//scripts/corpus:trading_model), and the sample's days as its Data; the example page over it
// (examples/markets-classes.ts) opened the page's way, every series of it run, and each answer held to its frozen
// answer (timeseries-frozen.ts; docs/TS_ONE_TREE_PLAN_2026_10_31.md §6, step 1).

import assert from 'node:assert/strict';
import { readdirSync, readFileSync } from 'node:fs';
import { join } from 'node:path';
import { after, before, describe, it } from 'node:test';
import { openDatabase } from '#test-host';

import { DuckDbEngine } from '../../engine-client/src/duckdb.ts';
import { runLoad } from '../../engine-client/src/plan-runner.ts';
import { runfileFromEnv } from '../../tools/js/runfiles.mts';
import { derive, element, fn, lambda, lit, projectObjects, property, variable, type ValueSpecification } from '../../pure-protocol/src/index.ts';
import { MARKETS_PROJECT, marketsClassesPage, SAVED_GRIDS, savedQuerySource, savedQueryTree } from '../src/examples/markets-classes.ts';
import type { LitePlanner } from '../src/lite-planner.ts';
import { savedQueryOpened } from '../src/saved-queries.ts';
import { marketsCsv } from '../src/samples.ts';
import type { CubeSnapshot } from '../src/snapshot.ts';
import { sourceColumns } from '../src/source-columns.ts';
import { OWN_MARKET_TABLES, STRESS_MARKET_COLUMNS, STRESS_MARKET_TABLES, stressMarketCsvs, stressMarketsData } from '../src/stress-markets.ts';
import { wholeOf } from '../src/timeseries-expr.ts';
import { marksQuery, seriesQuery, seriesSources, TS_DATE, TS_SPLIT, TS_TEXT, TS_VALUE, type TsInputs } from '../src/timeseries-query.ts';
import { readsBase, tsIsland, tsSeriesQuery } from '../src/timeseries-ts.ts';
import type { TimeseriesSpec, TsAnnotation, TsView } from '../src/timeseries-spec.ts';
import { ingestFile, type DuckDbFiles } from '../src/upload.ts';
import { plannerFor, readPageTexts, readText } from './catalog-builder.ts';
import { drawnColumns, frozenOf, holdFrozen, type Frozen } from './timeseries-frozen.ts';
import { readSeriesTexts, treeOf } from '../src/timeseries-text.ts';

/** The markets project as the site releases it (demo/releases/index.json, docs/ONE_CONNECTION_DESK_2026_10_28.md D5):
 *  its model files, each as the site serves it. */
const RELEASED = (JSON.parse(readFileSync(runfileFromEnv('RELEASES_INDEX'), 'utf8')) as {
  versions: { groupId: string; artifactId: string; versionId: string; files: string[] }[];
}).versions.find((p) => p.groupId === MARKETS_PROJECT.groupId && p.artifactId === MARKETS_PROJECT.artifactId && p.versionId === MARKETS_PROJECT.versionId);
const SERVED = runfileFromEnv('MARKETS_PROJECT');
const served = (file: string): string => readFileSync(join(SERVED, file.slice('markets/'.length)), 'utf8');
const MODEL = served('markets/trading-model.pure');
/** The project's own elements: the macro class, its store, the copies' mapping and runtime (2026-11-07, a'). */
const OWN = served('markets/markets-model.pure');
const writer = plannerFor('', 'local::RT');

/** The model a page holds: the project's files joined as the page joins them (app/sources.ts openedRecord) -- the
 *  trading model and the markets sample's rows as its Data. */
const PROJECT = (RELEASED?.files ?? []).map(served).join('\n');

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
  for (const sql of [...project.open, ...project.use]) await projectEngine.run(sql, 0);
  // its rows: the plan's ONCE load, run by the hosts' runner (B3)
  await runLoad(project.load!, (sql) => projectEngine.run(sql, 0));
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
async function spec(text: string, over: Over, split?: string): Promise<TimeseriesSpec> {
  const ts: TimeseriesSpec = {
    sources: [over === 'rows'
      ? { alias: 'px', cube: 'prices', date: 'date', key: 'symbol' }
      : { alias: 'px', cube: 'quotes', date: 'quoteTime', key: 'ticker' }],
    series: [{ id: 'A', text: over === 'rows' ? text : text.replaceAll('close[', 'closePrice['), ...(split !== undefined ? { split } : {}) }],
    grain: 'day',
  };
  await readSeriesTexts(ts.series, readText);
  return ts;
}

/** The series' lines, each by its split's value ('' when it has none), earliest first, as the database answers them. */
async function lines(text: string, over: Over, split?: string): Promise<Map<string, Line>> {
  const ts = await spec(text, over, split);
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

describe('the markets project, as the site releases and serves it', () => {
  it('its files the trading model and its Data, in that order, each served', () => {
    assert.ok(RELEASED, 'the site releases the markets project (demo/releases/index.json)');
    assert.deepEqual(RELEASED.files, ['markets/trading-model.pure', 'markets/markets-model.pure', 'markets/markets-data.pure']);
    assert.deepEqual(readdirSync(SERVED).sort(), ['markets-data.pure', 'markets-model.pure', 'trading-model.pure'],
      'the site serves those three');
    assert.equal(served('markets/markets-data.pure'), stressMarketsData(), 'its Data the generator\'s');
    assert.equal(PROJECT, `${MODEL}\n${OWN}\n${stressMarketsData()}`);
  });
});

describe('the trading model\'s tables, as its store declares them', () => {
  it('each table the generator writes has the store\'s columns, in its order', () => {
    // the corpus's one store: its tables after its name (a linked project's store may name a table so too); the
    // project's own table (the macro series) its own store's (markets-model.pure)
    const store = MODEL.slice(MODEL.indexOf('Database store::DB'));
    const own = OWN.slice(OWN.indexOf('Database markets::DB'));
    // a table's declaration up to its closing parenthesis (a column's type has its own: VARCHAR(20)), over lines
    const declaredIn = (text: string, t: string): string | undefined => {
      const start = text.indexOf(`Table ${t} (`);
      if (start < 0) return undefined;
      let depth = 0;
      for (let i = start + `Table ${t} `.length; i < text.length; i++) {
        if (text[i] === '(') depth += 1;
        else if (text[i] === ')' && --depth === 0) return text.slice(start + `Table ${t} (`.length, i);
      }
      return undefined;
    };
    // 2026-11-14 (main's markets merged into the Deephaven branch, whose corpus declares the desk's tables by ingests,
    // the desk roadmap's A2): a table an ingest's dataset makes from a source -- a view it pulls, a topic it reads --
    // has its source's columns, in its order
    const sourceOf = (t: string): string | undefined => {
      const m = new RegExp(`\\b${t}\\(\\s*\\|#>\\{[\\w:]+\\.(?:"([^"]+)"|(\\w+))\\}#\\s*\\)`).exec(MODEL);
      return m === null ? undefined : m[1] !== undefined ? `"${m[1]}"` : m[2];
    };
    for (const t of STRESS_MARKET_TABLES) {
      const source = OWN_MARKET_TABLES.includes(t) ? undefined : sourceOf(t);
      const declared = source !== undefined ? declaredIn(MODEL, source) : declaredIn(OWN_MARKET_TABLES.includes(t) ? own : store, t);
      assert.ok(declared !== undefined, `the store declares ${t}`);
      const names = declared.split(/,(?![^(]*\))/).map((c) => c.trim().split(/\s+/)[0]);
      assert.deepEqual([...STRESS_MARKET_COLUMNS[t]], names, `${t}'s columns`);
    }
  });
});

describe('a timeseries over MarketQuote through stress::AllMapping is the timeseries over the table', () => {
  for (const text of ["close['AAPL']", "volatility(close['AAPL'], w => 22, type => 'simple')", "correlation(close['AAPL'], close['MSFT'], w => 63)",
    "returns(close['NVDA'], 5, 'simple')", "beta(close['JPM'], close['SPX'], w => 2mo)"]) {
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
    same(await line("close[sector = 'Energy']", 'referenced'), await line("close['XOM']", 'rows'), 'Energy');
  });

  it('a sector\'s volume summed is its instruments\' volumes added', async () => {
    same(await line("sum(volume[sector = 'Information Technology'])", 'referenced'),
      await line("sum(volume['AAPL']) + sum(volume['MSFT']) + sum(volume['NVDA'])", 'rows'), 'IT volume');
  });

  it('split by ticker within a sector: a line per instrument of it, each its own returns', async () => {
    const over = await lines("returns(close[sector = 'Information Technology'], 1, 'simple')", 'referenced', 'ticker');
    assert.deepEqual([...over.keys()].sort(), ['AAPL', 'MSFT', 'NVDA'], 'the sector\'s instruments');
    for (const t of over.keys()) same(over.get(t)!, await line(`returns(close['${t}'], 1, 'simple')`, 'rows'), t);
  });

  it('split by exchange: a line per listing venue, each the volume of the instruments listed there', async () => {
    const over = await lines('sum(volume)', 'referenced', 'exchange');
    assert.deepEqual([...over.keys()].sort(), ['Cboe Options Exchange', 'NYSE Arca', 'Nasdaq', 'New York Stock Exchange', 'Off exchange'], 'the exchanges listing them');
    same(over.get('New York Stock Exchange')!, await line("sum(volume['JPM']) + sum(volume['XOM'])", 'rows'), 'NYSE');
    same(over.get('Nasdaq')!, await line("sum(volume['NDX']) + sum(volume['AAPL']) + sum(volume['MSFT']) + sum(volume['NVDA']) + sum(volume['TLT'])", 'rows'), 'Nasdaq');
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

  // the class page's series' texts read by core before its tiles are walked (A3 step 3)
  before(async () => { await readPageTexts(marketsClassesPage()); });

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
        if (JSON.stringify(treeOf(series, v.spec.series)).includes('"fn":"') || v.spec.grain !== undefined) {
          assert.equal(new Set(at).size, at.length, `${series.id} (${series.name}): one value a date and line`);
        }
        assert.ok(values.filter((x) => x !== null).length >= 20 * lines, `${series.id} (${series.name}): ${values.length} rows, ${lines} lines`);
        drawn.set(`${v.id} ${series.id}`, lines);
        answers.set(`${v.id} ${series.id}`, frozenOf(t, TS_DATE, q.split === undefined ? undefined : TS_SPLIT, drawnColumns(q)));
      }
    });
  }

  it('each series is one #TS statement over the classes, and reads back to the same rows (Show #TS, D12)', async () => {
    const said: string[] = [];
    const unshown = new Map<string, number>();
    for (const v of marketsClassesPage().views) {
      if (v.kind !== 'timeseries') continue;
      const inputs: TsInputs = new Map(v.spec.sources.map((src) => [src.alias, { snapshot: opened.get(src.cube)!.snapshot, filter: undefined }] as const));
      const planner = opened.get(v.spec.sources[0]!.cube)!.planner;
      for (const series of v.spec.series) {
        const sent = tsSeriesQuery(inputs, v.spec, series);
        const shown = await planner.tsStatement(sent);
        if (shown.statement === undefined) {
          unshown.set(shown.refused, (unshown.get(shown.refused) ?? 0) + 1);
          continue;
        }
        // the grids' class paths named once, by WITH: no let, no Pure
        assert.match(shown.statement, /^WITH \w+ AS \(SELECT .* FROM \w+::\w+/, shown.statement);
        const alone = lambda([], tsIsland(shown.statement));
        assert.equal((await planner.tsStatement(alone)).statement, shown.statement, 'printed again, the same text');
        const want = await projectEngine.execute(await planner.plan(sent), 0);
        const got = await projectEngine.execute(await planner.plan(alone), 0);
        assert.deepEqual(got.columns.map((c) => [c.name, c.values]), want.columns.map((c) => [c.name, c.values]),
          `${v.id} ${series.id}: ${shown.statement}`);
        said.push(shown.statement);
      }
    }
    assert.ok(said.length >= 7, `${said.length} statements`);
    // the series not one statement yet, by reason: each count only allowed to shrink (D12). None since A6
    // (2026-11-03): an enumeration read as its values' names prints as its cast to text, CAST(side AS text)
    assert.deepEqual(Object.fromEntries(unshown), {});
  });

  it('a grid with calculated columns is one #TS statement too, each column its Postgres, read back to the same rows (A6)', async () => {
    // what a person types into the grid's calculated-column box, read by the compiler (the grid's own lambdas): a
    // division (a Float's, never Postgres's integer one), text joined (+ as ||), an if (a CASE)
    const base = referenced.get('px')!.snapshot;
    const derived = [
      { name: 'turnover', lambda: await corpus.parse('x|$x.closePrice->toOne() * $x.volume->toOne() / 1000000') },
      { name: 'listing', lambda: await corpus.parse("x|$x.ticker->toOne() + '@' + $x.exchange->toOne()") },
      { name: 'tone', lambda: await corpus.parse('x|if($x.closePrice > 100.0, |1.0, |-1.0)') },
    ];
    const inputs: TsInputs = new Map([['px', { snapshot: { ...base, derived, columns: [...base.columns,
      { name: 'turnover', type: 'Float' }, { name: 'listing', type: 'String' }, { name: 'tone', type: 'Float' }] }, filter: undefined }]]);
    const ts: TimeseriesSpec = {
      sources: [{ alias: 'px', cube: 'quotes', date: 'quoteTime', key: 'ticker' }],
      series: [
        { id: 'A', text: 'mean(sum(turnover), 21)', split: 'listing' },
        { id: 'B', text: "sum(tone[sector = 'Energy'])" },
      ],
      grain: 'day',
    };
    await readSeriesTexts(ts.series, readText);
    for (const series of ts.series) {
      const sent = tsSeriesQuery(inputs, ts, series);
      const shown = await corpus.tsStatement(sent);
      if (shown.statement === undefined) assert.fail(`${series.id}: ${shown.refused}`);
      assert.match(shown.statement, / \|\| '@' \|\| .* AS listing, CASE WHEN /, shown.statement);
      const alone = lambda([], tsIsland(shown.statement));
      assert.equal((await corpus.tsStatement(alone)).statement, shown.statement, 'printed again, the same text');
      const want = await projectEngine.execute(await corpus.plan(sent), 0);
      const got = await projectEngine.execute(await corpus.plan(alone), 0);
      assert.ok(want.columns[0]!.values.length > 20, `${series.id}: ${want.columns[0]!.values.length} rows`);
      assert.deepEqual(got.columns.map((c) => [c.name, c.values]), want.columns.map((c) => [c.name, c.values]),
        `${series.id}: ${shown.statement}`);
    }
  });

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

// THE COPIES OVER CLASSES (docs/TS_DECISIONS_2026_11_07.md §4, a', 2026-11-07, the user: "copies of the charts that start
// with classes"): every series of the markets page and of the gallery, their grids the markets project's classes
// (src/examples/on-classes.ts), answers as the pages over the samples froze it (timeseries-ts-parity's
// gallery-answers.json, the same ids and anchors) -- the same digests, so a figure over the classes is the figure over
// the tables.
describe('the markets page and the gallery over classes answer as over the samples', () => {
  /** The date a base-reading series is also answered from (timeseries-ts-parity's). */
  const ANCHOR = '2022-03-15';
  const uncompared = (v: TsView): TsView => {
    const { compare, ...rest } = v;
    void compare;
    return rest;
  };
  it('every series of both copies answers as frozen over the samples', async () => {
    const { CLASS_GRIDS, classQueryTree, galleryOnClassesPage, marketsOnClassesPage } = await import('../src/examples/on-classes.ts');
    const over = plannerFor(PROJECT, 'markets::RT');
    const got = new Map<string, Frozen>();
    for (const page of [await readPageTexts(await marketsOnClassesPage()), await readPageTexts(await galleryOnClassesPage())]) {
      // each grid's saved query read as the page reads it: its class's query, the cube's columns and grouping
      const grids: TsInputs = new Map(page.cubes.map((c) => [c.id, { snapshot: { ...c.cube.query,
        source: { query: classQueryTree(CLASS_GRIDS.find((g) => g.id === c.id)!) }, epoch: 1 }, filter: undefined }]));
      for (const view of page.views) {
        if (view.kind !== 'timeseries') continue;
        // the series as the tile computes it, before its view's compare (timeseries-ts-parity's)
        const ts: TimeseriesSpec = view.spec.view === undefined ? view.spec : { ...view.spec, view: uncompared(view.spec.view) };
        for (const series of ts.series) {
          for (const anchor of readsBase(wholeOf(series, ts.series)) ? [undefined, ANCHOR] : [undefined]) {
            const id = `${view.id} ${series.id}${anchor === undefined ? '' : ` from ${anchor}`}`;
            const inputs: TsInputs = new Map(seriesSources(ts, series)
              .map((a) => [a, grids.get(ts.sources.find((x) => x.alias === a)!.cube)!] as const));
            const q = seriesQuery(inputs, ts, series, anchor === undefined ? {} : { from: anchor }, {}, null);
            const t = await projectEngine.execute(await over.plan(q.query), 0);
            got.set(id, frozenOf(t, TS_DATE, q.split === undefined ? undefined : TS_SPLIT, drawnColumns(q)));
          }
        }
      }
    }
    holdFrozen('GALLERY_ANSWERS', got, 'gallery-on-classes-answers');
  });
});
