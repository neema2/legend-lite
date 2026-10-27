// The example pages (docs/DATACUBE_CHARTS_DESIGN_2026_10_09.md §3.3), read back as a saved page is read: each opens,
// every view sits on a sheet, and every chart on it is one its cube can draw. (chart-queries.test.ts plans and runs each
// chart's query on the sample itself.)

import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { EXAMPLE_PAGES } from '../src/examples/catalog.ts';
import { EXAMPLE_CUBE, SALES_SPECS } from '../src/examples/charts-dashboard.ts';
import { CHART_MARKS, chartProblems } from '../src/chart-spec.ts';
import { chartsGalleryPage } from '../src/examples/charts-gallery.ts';
import { pageToJson, readPage } from '../src/page-document.ts';
import { tiles } from '../src/layout/bands.ts';
import { COUNTRY_COLUMNS, countriesCsv, MARKET_COLUMNS, RATE_COLUMNS, salesCsv, SALES_COLUMNS } from '../src/samples.ts';
import { COUNTRY_SPECS, prepJoinsPage } from '../src/examples/prep-joins.ts';
import { MARKET_SPECS, RATE_SPECS, marketsPage } from '../src/examples/markets.ts';
import { timeseriesGalleryPage } from '../src/examples/timeseries-gallery.ts';
import { timeseriesProblems, TS_FUNCTIONS } from '../src/timeseries-expr.ts';
import type { TimeseriesSpec, TsExpr } from '../src/timeseries-spec.ts';
import { frozenInput, openCube } from '../src/cube-document.ts';
import { element } from '../../pure-protocol/src/index.ts';
import { layoutTiles } from '../src/page-document.ts';
import { prepFlowPage } from '../src/examples/prep-flow.ts';
import { boxSays } from '../src/flow-boxes.ts';
import { framedBy } from '../src/layout/flow.ts';
import { liveGrids } from '../src/live-inputs.ts';

/** The pages of charts over the sales sample; the join example is read below. */
const CHART_PAGES = EXAMPLE_PAGES.filter((e) => e.id.startsWith('charts-'));

describe('the example pages', () => {
  it('each is a page of charts, the join example, a timeseries example or the flow example', () => {
    assert.deepEqual(EXAMPLE_PAGES.filter((e) => !CHART_PAGES.includes(e)).map((e) => e.id), ['prep-joins', 'markets', 'timeseries-gallery', 'prep-flow']);
  });

  // a flow to open, rather than one to build first (docs/DATACUBE_FLOW_MODE_2026_10_24.md)
  it('the flow example: its boxes each the tool it is, read live, in its containers, its Output writing a CSV', async () => {
    const page = readPage(pageToJson(await prepFlowPage()));
    assert.equal(page.sheets.length, 4);
    const layout = page.sheets[0]!.layout;
    assert.ok(layout.kind === 'flow');
    const cube = (id: string) => page.cubes.find((c) => c.id === id)!.cube;
    const tools = Object.fromEntries(page.views.filter((v) => v.kind === 'grid').map((v) => [v.id, boxSays(cube(v.id)).tool]));
    assert.deepEqual(tools, {
      'grid-1': 'input', 'grid-2': 'input', 'grid-3': 'filter', 'grid-4': 'join', 'grid-5': 'formula', 'grid-6': 'summarize',
      'grid-7': 'browse', 'grid-8': 'unique',
      // its Reshape sheet: a Sample, a Select, a Transpose and a Summarize
      'grid-9': 'sample', 'grid-10': 'select', 'grid-11': 'transpose', 'grid-12': 'summarize',
      // its Match sheet: a Join's L read by a Browse and counted, its R, and a Find Replace
      'grid-13': 'browse', 'grid-14': 'countRecords', 'grid-15': 'browse', 'grid-16': 'findReplace',
      // its Checks sheet: three regions' Filters, a Union of the three, a Date Filter and a Test
      'grid-17': 'filter', 'grid-18': 'filter', 'grid-19': 'filter', 'grid-20': 'union', 'grid-21': 'dateFilter', 'grid-22': 'test',
      // and a Cross Tab whose columns are fixed to two of the three regions (FL2, FL3)
      'grid-23': 'crosstab',
    });
    assert.deepEqual(cube('grid-23').query.pivotValues, ['AMER', 'EMEA'], 'its columns fixed: two of the three regions');
    assert.equal(boxSays(cube('grid-23')).says, 'region across, by channel');
    assert.deepEqual(liveGrids(cube('grid-20').source), ['grid-17', 'grid-18', 'grid-19'], 'a Union of three, read live');
    const checksLayout = page.sheets[3]!.layout;
    assert.ok(checksLayout.kind === 'flow');
    assert.deepEqual(checksLayout.boxes.find((b) => b.tile === 'grid-21')?.output, { format: 'csv', name: 'orders of 2024.csv' }, "2024's orders written");
    assert.deepEqual(['grid-20', 'grid-21', 'grid-22'].map((id) => boxSays(cube(id)).says), [
      'the rows of all 3, columns by name', 'sold from 2024-01-01 to 2024-12-31',
      'order_id repeats no value, country has no empty value, discount from 0 to 0.15',
    ]);
    assert.deepEqual(['grid-14', 'grid-16'].map((id) => boxSays(cube(id)).says), ['its rows counted: Count', 'country replaced by their_country']);
    assert.deepEqual(['grid-13', 'grid-15'].map((id) => { const s = cube(id).source; return s._type === 'rows' && 'grid' in s.input ? `${s.input.grid}:${s.input.port}` : null; }),
      ['grid-4:left', 'grid-4:right'], "the Join's L and R, read live");
    assert.deepEqual(['grid-9', 'grid-10', 'grid-11', 'grid-12'].map((id) => boxSays(cube(id)).says), [
      'last 2 of each region by revenue', 'revenue → sales; 12 of 17 columns left out', 'sales, cost, profit to rows', 'by measure',
    ]);
    assert.deepEqual(['grid-10', 'grid-11', 'grid-12'].map((id) => liveGrids(cube(id).source)), [['grid-9'], ['grid-10'], ['grid-11']]);
    // each box after the first reads the box before it live
    assert.deepEqual(['grid-3', 'grid-4', 'grid-5', 'grid-6', 'grid-7', 'grid-8'].map((id) => liveGrids(cube(id).source)),
      [['grid-1'], ['grid-3', 'grid-2'], ['grid-4'], ['grid-5'], ['grid-6'], ['grid-1']]);
    assert.deepEqual(framedBy(layout, 'text-1'), ['grid-1', 'grid-2'], 'the Inputs container holds the two inputs');
    assert.deepEqual(framedBy(layout, 'text-2'), ['grid-3', 'grid-4', 'grid-5'], 'the Prepare one, the steps between');
    assert.deepEqual(framedBy(layout, 'text-4'), ['grid-6', 'grid-7'], 'the Report one, the summary and its Output');
    assert.deepEqual(layout.boxes.find((b) => b.tile === 'grid-7')?.output, { format: 'csv', name: 'revenue by continent.csv' });
    // its samples rebuilt on opening
    for (const [id, sample] of [['grid-1', 'sales'], ['grid-2', 'countries']] as const) {
      const source = cube(id).source;
      assert.ok(source._type === 'file' && source.sample?.id === sample, `${id} is the ${sample} sample`);
    }
  });

  for (const example of CHART_PAGES) {
    it(`${example.label}: opens as a saved page, every view on a sheet, every chart drawable`, async () => {
      const page = readPage(pageToJson(await example.build()));
      assert.ok(page.sheets.length >= 2, 'a sheet per family, and the grid');
      const placed = page.sheets.flatMap((s) => layoutTiles(s.layout));
      assert.deepEqual([...placed].sort(), page.views.map((v) => v.id).sort(), 'every view once, on one sheet');
      const charts = page.views.filter((v) => v.kind === 'chart');
      assert.ok(charts.length >= 20, `simple to complex: ${charts.length} charts`);
      for (const v of charts) {
        if (v.kind !== 'chart') continue;
        assert.deepEqual(chartProblems(v.spec, EXAMPLE_CUBE), [], `${v.title}`);
      }
      // the sample it is drawn from regenerates on opening
      const source = page.cubes[0]!.cube.source;
      assert.ok(source._type === 'file' && source.sample?.id === 'sales');
    });
  }

  // the user, 2026-10-23: one gallery of every chart kind, listed first, each sheet fitting the screen
  it('the gallery: every kind DataCube draws once, titled by its kind, first in the Open window, each sheet fitting its window', async () => {
    const page = await chartsGalleryPage();
    const charts = page.views.filter((v) => v.kind === 'chart');
    // a kind's own chart is `g-<kind>`: each kind once (the dashboard's KPI and gauge among them)
    const own = charts.filter((v) => v.id.startsWith('g-'));
    assert.deepEqual(own.map((v) => (v.kind === 'chart' ? v.spec.mark : '')).sort(), CHART_MARKS.map((m) => m.value).sort(),
      'each kind once');
    const dashboard = page.sheets.find((s) => s.name === 'KPI dashboard');
    assert.ok(dashboard, 'a KPI dashboard sheet');
    const onDashboard = new Set(layoutTiles(dashboard.layout));
    for (const v of own) {
      if (v.kind !== 'chart' || onDashboard.has(v.id)) continue;
      const label = CHART_MARKS.find((m) => m.value === v.spec.mark)!.label;
      assert.ok(v.title.startsWith(`${label}: `), `${v.title}: titled by its kind`);
    }
    assert.equal(EXAMPLE_PAGES[0]!.id, 'charts-gallery');
    assert.deepEqual(page.sheets.map((s) => s.layout.kind === 'bands' && s.layout.fit), page.sheets.map(() => true), 'every sheet fits its window');
  });

  // the user, 2026-10-23: "a really nice layout of maybe 5-6-7-8 KPIs on top and then some layout of other things that
  // people usually have on a BI KPI dashboard"
  it('the gallery\'s KPI dashboard: a strip of eight KPIs across the top, each by month; the trend, the mix and the rest under it', async () => {
    const page = await chartsGalleryPage();
    const dashboard = page.sheets.find((s) => s.name === 'KPI dashboard')!;
    assert.ok(dashboard.layout.kind === 'bands');
    const [strip, ...rest] = dashboard.layout.bands;
    const view = (id: string) => page.views.find((v) => v.id === id);
    const top = tiles({ fit: true, bands: [strip!] });
    assert.equal(top.length, 8, 'eight KPIs across');
    for (const id of top) {
      const v = view(id);
      assert.ok(v?.kind === 'chart' && v.spec.mark === 'kpi' && v.spec.grain === 'month', `${id}: a KPI by month`);
    }
    // some against a target, one of them better lower; the bands under it charts of other kinds
    const specs = top.map((id) => view(id)).flatMap((v) => (v?.kind === 'chart' ? [v.spec] : []));
    assert.ok(specs.filter((s) => s.options.target !== undefined).length >= 4);
    assert.ok(specs.some((s) => s.options.better === 'lower'));
    const under = tiles({ fit: true, bands: rest });
    assert.ok(under.length >= 5 && under.every((id) => { const v = view(id); return v?.kind === 'chart' && v.spec.mark !== 'kpi'; }));
    const heights = dashboard.layout.bands.map((b) => b.height);
    assert.ok(Math.abs(heights.reduce((a, b) => a + b, 0) - 1) < 1e-9, 'the bands fill the sheet');
    assert.ok(heights[0]! < heights[1]!, 'the strip the shortest band');
  });

  it('the sample\'s columns are the page\'s, in order', () => {
    assert.deepEqual(SALES_SPECS.map((c) => c.name), [...SALES_COLUMNS]);
    assert.deepEqual(COUNTRY_SPECS.map((c) => c.name), [...COUNTRY_COLUMNS]);
    assert.deepEqual(MARKET_SPECS.map((c) => c.name), [...MARKET_COLUMNS]);
    assert.deepEqual(RATE_SPECS.map((c) => c.name), [...RATE_COLUMNS]);
  });

  // the timeseries tile's example (docs/DATACUBE_TIMESERIES_DESIGN_2026_10_25.md §9): every tile drawable over its grids'
  // columns (test/timeseries-queries.test.ts runs each of its series on the samples)
  it('the markets example: two samples, four sheets of timeseries, each tile drawable over its grids\' columns', async () => {
    const page = readPage(pageToJson(await marketsPage()));
    assert.deepEqual(page.sheets.map((s) => s.name), ['Performance', 'Risk', 'Technicals', 'Rates']);
    const placed = page.sheets.flatMap((s) => layoutTiles(s.layout));
    assert.deepEqual([...placed].sort(), page.views.map((v) => v.id).sort(), 'every view once, on one sheet');
    for (const [id, sample] of [['prices', 'markets'], ['rates', 'rates']] as const) {
      const source = page.cubes.find((c) => c.id === id)!.cube.source;
      assert.ok(source._type === 'file' && source.sample?.id === sample, `${id} is the ${sample} sample`);
    }
    const columns = new Map([['prices', MARKET_SPECS], ['rates', RATE_SPECS]] as const);
    const tilesOf = page.views.filter((v) => v.kind === 'timeseries');
    assert.equal(tilesOf.length, 6);
    for (const v of tilesOf) {
      if (v.kind !== 'timeseries') continue;
      const byAlias = new Map(v.spec.sources.map((s) => [s.alias, columns.get(s.cube as 'prices' | 'rates')!.map((c) => ({ name: c.name, type: c.type }))] as const));
      assert.deepEqual(timeseriesProblems(v.spec, byAlias), [], v.title ?? v.id);
    }
    // one tile reads both grids, in one query
    assert.ok(tilesOf.some((v) => v.kind === 'timeseries' && v.spec.sources.length === 2));
  });

  // the user, 2026-10-25: "a real saved page with multiple tabs showing all of these" -- every function of the catalog,
  // each on its family's sheet, every tile drawable over its grids' columns (test/timeseries-queries.test.ts runs them)
  it('the timeseries gallery: a sheet per family, each using every function of its family, the ways to view, the data', async () => {
    const page = readPage(pageToJson(await timeseriesGalleryPage()));
    const families = [...new Set(TS_FUNCTIONS.map((f) => f.family))];
    assert.deepEqual(page.sheets.map((s) => s.name), [...families, 'Ways to view', 'The data']);
    const placed = page.sheets.flatMap((s) => layoutTiles(s.layout));
    assert.deepEqual([...placed].sort(), page.views.map((v) => v.id).sort(), 'every view once, on one sheet');
    assert.deepEqual(page.sheets.map((s) => s.layout.kind === 'bands' && s.layout.fit), page.sheets.map(() => true), 'every sheet fits its window');
    const view = (id: string) => page.views.find((v) => v.id === id);
    const specsOn = (sheet: string): TimeseriesSpec[] => layoutTiles(page.sheets.find((s) => s.name === sheet)!.layout)
      .flatMap((id) => { const v = view(id); return v?.kind === 'timeseries' ? [v.spec] : []; });
    const called = (e: TsExpr): string[] => e.kind === 'call' ? [e.fn, ...e.args.flatMap(called)]
      : e.kind === 'binary' ? [...called(e.left), ...called(e.right)] : [];
    const calledOn = (sheet: string): Set<string> => new Set(specsOn(sheet).flatMap((ts) => ts.series.flatMap((x) => called(x.expr))));
    for (const family of families) {
      const missing = TS_FUNCTIONS.filter((f) => f.family === family && !calledOn(family).has(f.name)).map((f) => f.name);
      assert.deepEqual(missing, [], `${family}: every function of it on its sheet`);
    }
    // every tile drawable over its grids' columns; the ways to view read a split, a frequency, a log scale, a compare
    // and two grids at once
    const columns = new Map([['prices', MARKET_SPECS], ['rates', RATE_SPECS]] as const);
    const all = page.views.flatMap((v) => (v.kind === 'timeseries' ? [v] : []));
    assert.ok(all.length >= 30, `${all.length} tiles`);
    for (const v of all) {
      const byAlias = new Map(v.spec.sources.map((s) => [s.alias, columns.get(s.cube as 'prices' | 'rates')!.map((c) => ({ name: c.name, type: c.type }))] as const));
      assert.deepEqual(timeseriesProblems(v.spec, byAlias), [], v.title ?? v.id);
    }
    const ways = specsOn('Ways to view');
    assert.ok(ways.some((ts) => ts.series.some((x) => x.split !== undefined)), 'a line per symbol');
    assert.ok(ways.some((ts) => ts.grain === 'month'), 'a month at a time');
    assert.ok(ways.some((ts) => ts.panes?.some((p) => p.log)), 'a log scale');
    assert.ok(ways.some((ts) => ts.view?.compare === 'percent'), 'every line from one start');
    assert.deepEqual([...new Set(ways.flatMap((ts) => ts.series.map((x) => x.style?.kind ?? 'line')))].sort(), ['area', 'bars', 'dots', 'line', 'step']);
    assert.ok(ways.some((ts) => ts.sources.length === 2), 'two grids');
    // the data sheet: the two grids every tile reads, each its sample rebuilt on opening
    assert.deepEqual(layoutTiles(page.sheets.at(-1)!.layout), ['prices', 'rates']);
    for (const [id, sample] of [['prices', 'markets'], ['rates', 'rates']] as const) {
      const source = page.cubes.find((c) => c.id === id)!.cube.source;
      assert.ok(source._type === 'file' && source.sample?.id === sample, `${id} is the ${sample} sample`);
    }
  });

  // the user, 2026-10-23: join prep was on the site, but no page showed it
  it('the join example: two samples, their join by ISO code and the orders it leaves out, each reopened from its samples', async () => {
    const page = readPage(pageToJson(await prepJoinsPage()));
    assert.deepEqual(page.sheets.map((s) => s.name), ['Two tables', 'Joined', 'No match']);
    const placed = page.sheets.flatMap((s) => layoutTiles(s.layout));
    assert.deepEqual([...placed].sort(), page.views.map((v) => v.id).sort(), 'every view once, on one sheet');
    const cube = (id: string) => page.cubes.find((c) => c.id === id)!.cube;
    // the two tables: each a sample, rebuilt on opening
    for (const [id, sample] of [['orders', 'sales'], ['countries', 'countries']] as const) {
      const source = cube(id).source;
      assert.ok(source._type === 'file' && source.sample?.id === sample, `${id} is the ${sample} sample`);
    }
    // the joins: each over the two tables as their grids were, by ISO code
    const joined = cube('joined').source;
    const unmatched = cube('unmatched').source;
    assert.ok(joined._type === 'join' && unmatched._type === 'join');
    for (const j of [joined, unmatched]) {
      assert.deepEqual([frozenInput(j.left, j.name).source, frozenInput(j.right, j.name).source], [cube('orders').source, cube('countries').source]);
      assert.ok('on' in j.how && 'rows' in j.how.on);
      assert.deepEqual(j.how.on.rows, [{ left: 'country_code', op: '==', right: 'code' }]);
    }
    assert.deepEqual([joined.how.form, joined.how.form === 'join' && joined.how.kind, unmatched.how.form], ['join', 'left', 'anti']);
    // their country, a name the orders have, comes out prefixed; the rest as they are
    assert.deepEqual(joined.columns.slice(SALES_SPECS.length).map((c) => c.name), ['code', 'their_country', 'continent', 'capital', 'population_m']);
    assert.deepEqual(unmatched.columns.map((c) => c.name), [...SALES_COLUMNS], 'an anti join: my columns only');
    // the chart is of the join, and drawable from it
    const chart = page.views.find((v) => v.kind === 'chart');
    assert.ok(chart?.kind === 'chart' && chart.cube === 'joined');
    // opened as the tab opens it over the join's columns: nothing left out, grouped by continent, the chart drawable
    const saved = cube('joined');
    const reopened = openCube(saved, { query: element('joined') }, saved.query.columns);
    assert.deepEqual([reopened.notes, reopened.changed], [[], false]);
    assert.deepEqual(reopened.snapshot.rows, ['continent', 'their_country']);
    assert.deepEqual(chartProblems(chart.spec, reopened.snapshot), []);
  });

  it('the countries sample: eleven, Singapore (sold in) left out, Mexico and South Africa (not sold in) in', () => {
    const rows = countriesCsv().trim().split('\n').slice(1).map((l) => l.split(',')[0]);
    assert.equal(rows.length, 11);
    const sold = new Set(salesCsv(6000).trim().split('\n').slice(1).map((l) => l.split(',')[3]));
    assert.deepEqual([...sold].filter((c) => !rows.includes(c!)), ['SGP']);
    assert.deepEqual(rows.filter((c) => !sold.has(c)), ['MEX', 'ZAF']);
  });
});
