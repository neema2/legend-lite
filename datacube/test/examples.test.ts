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
import { COUNTRY_COLUMNS, countriesCsv, salesCsv, SALES_COLUMNS } from '../src/samples.ts';
import { COUNTRY_SPECS, prepJoinsPage } from '../src/examples/prep-joins.ts';
import { openCube } from '../src/cube-document.ts';
import { element } from '../../pure-protocol/src/index.ts';

/** The pages of charts over the sales sample; the join example is read below. */
const CHART_PAGES = EXAMPLE_PAGES.filter((e) => e.id.startsWith('charts-'));

describe('the example pages', () => {
  it('each is a page of charts, or the join example', () => {
    assert.deepEqual(EXAMPLE_PAGES.filter((e) => !CHART_PAGES.includes(e)).map((e) => e.id), ['prep-joins']);
  });

  for (const example of CHART_PAGES) {
    it(`${example.label}: opens as a saved page, every view on a sheet, every chart drawable`, async () => {
      const page = readPage(pageToJson(await example.build()));
      assert.ok(page.sheets.length >= 2, 'a sheet per family, and the grid');
      const placed = page.sheets.flatMap((s) => tiles(s.layout));
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
    const onDashboard = new Set(tiles(dashboard.layout));
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
  });

  // the user, 2026-10-23: join prep was on the site, but no page showed it
  it('the join example: two samples, their join by ISO code and the orders it leaves out, each reopened from its samples', async () => {
    const page = readPage(pageToJson(await prepJoinsPage()));
    assert.deepEqual(page.sheets.map((s) => s.name), ['Two tables', 'Joined', 'No match']);
    const placed = page.sheets.flatMap((s) => tiles(s.layout));
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
      assert.deepEqual([j.left.source, j.right.source], [cube('orders').source, cube('countries').source]);
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
