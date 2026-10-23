// Chart queries, PLANNED AND RUN: the query a chart asks (chart-spec.ts) through legend-lite's own planner, on a real
// DuckDB, its numbers read back. The unit tests (chart-echarts.test.ts) say what the query is shaped like; these say the
// planner takes it and the database answers what the chart means -- a group-level column, a preset, a reference line
// (docs/DATACUBE_CHARTS_DESIGN_2026_10_09.md §3.1, §3.2).

import assert from 'node:assert/strict';
import { after, before, describe, it } from 'node:test';
import { openDatabase } from '#test-host';

import { DuckDbEngine } from '../../engine-client/src/duckdb.ts';
import type { ResultTable } from '../../engine-client/src/result.ts';
import { boxMeasures, chartQuery, DEFAULT_OPTIONS, GRID_MARKS, measureName, type ChartSpec } from '../src/chart-spec.ts';
import { marimekkoNames, paretoName, ringTotalName, waffleNames, waterfallNames } from '../src/calc-presets.ts';
import type { CubeSnapshot } from '../src/snapshot.ts';
import { ingestFile, type DuckDbFiles } from '../src/upload.ts';
import { plannerFor } from './catalog-builder.ts';
import { row } from './lite-compiler.ts';
import type { LitePlanner } from '../src/lite-planner.ts';
import { from } from '../../pure-protocol/src/index.ts';

// Six sales: two regions, three months -- small enough to check every number by hand.
const CSV = [
  'region,sold,revenue,cost',
  'EMEA,2024-01-15,100,60',
  'EMEA,2024-02-10,200,150',
  'EMEA,2024-03-05,300,200',
  'AMER,2024-01-20,400,100',
  'AMER,2024-02-25,50,40',
  'AMER,2024-03-30,150,90',
].join('\n');

const writer = plannerFor('', 'local::RT');
let engine: DuckDbEngine;
let planner: LitePlanner;
let cube: CubeSnapshot;

before(async () => {
  const db = await openDatabase();
  engine = new DuckDbEngine(db.connection);
  const files: DuckDbFiles = db.files;
  const r = await ingestFile(engine, files, new File([CSV], 'sales.csv'), writer);
  planner = plannerFor(r.model, r.runtime);
  cube = {
    source: { query: r.source },
    columns: [
      { name: 'region', type: 'String' },
      { name: 'sold', type: 'StrictDate' },
      { name: 'revenue', type: 'Integer', kind: 'measure' },
      { name: 'cost', type: 'Integer', kind: 'measure' },
    ],
    derived: [],
    rows: ['region'],
    pivotOn: [],
    sorts: [],
    epoch: 1,
  };
});

after(async () => {
  await engine?.close();
});

/** The chart's rows, as the planner plans its query and the database answers it. */
async function rows(spec: ChartSpec, s: CubeSnapshot = cube): Promise<ResultTable> {
  const { query } = chartQuery(s, spec);
  return engine.execute(await planner.plan(query), 0);
}

/** A column of the answer, by name. */
function values(t: ResultTable, name: string): unknown[] {
  const c = t.columns.find((x) => x.name === name);
  assert.ok(c, `the answer has a column '${name}' (it has ${t.columns.map((x) => x.name).join(', ')})`);
  return c.values.map((v) => (typeof v === 'bigint' ? Number(v) : v));
}

const BY_REGION: ChartSpec = {
  version: 2,
  mark: 'bar',
  x: 'region',
  y: [{ column: 'revenue', fn: 'sum' }],
  // in the order across, so each answer is read in a known order
  options: { ...DEFAULT_OPTIONS, sort: { by: 'x', direction: 'asc' } },
};

describe('a group-level column, planned and run', () => {
  it('the cube\'s margin is the ratio of the groups\' sums, not an average of the rows\' ratios', async () => {
    const margin = await row('($x.revenue - $x.cost) / $x.revenue');
    const s: CubeSnapshot = { ...cube, groupDerived: [{ name: 'margin', lambda: margin, type: 'Float' }] };
    const t = await rows({ ...BY_REGION, y: [{ column: 'margin', fn: 'value' }] }, s);
    assert.deepEqual(values(t, 'region'), ['AMER', 'EMEA']);
    // AMER: (600 - 230) / 600; EMEA: (600 - 410) / 600
    assert.deepEqual(values(t, 'margin').map((v) => Number(v).toFixed(4)), [(370 / 600).toFixed(4), (190 / 600).toFixed(4)]);
  });

  it('beside an aggregated measure, each named as the chart reads it', async () => {
    const margin = await row('$x.revenue - $x.cost');
    const s: CubeSnapshot = { ...cube, groupDerived: [{ name: 'profit', lambda: margin, type: 'Integer' }] };
    const spec: ChartSpec = { ...BY_REGION, y: [{ column: 'revenue', fn: 'max' }, { column: 'profit', fn: 'value', mark: 'line' }] };
    const t = await rows(spec, s);
    assert.deepEqual(values(t, measureName(spec.y[0]!)), [400, 300]);
    assert.deepEqual(values(t, 'profit'), [370, 190]);
  });
});

describe('presets, planned and run (docs/DATACUBE_CHARTS_DESIGN_2026_10_09.md §3.2)', () => {
  const BY_MONTH: ChartSpec = { ...BY_REGION, x: 'sold', grain: 'month' };

  it('a date by month: the compiler\'s firstDayOfMonth, one row per month', async () => {
    const t = await rows(BY_MONTH);
    assert.deepEqual(values(t, 'sold__month').map((v) => String(v).slice(0, 10)), ['2024-01-01', '2024-02-01', '2024-03-01']);
    assert.deepEqual(values(t, 'sum_revenue'), [500, 250, 450]);
  });

  it('by quarter and by year: one row each here', async () => {
    for (const grain of ['quarter', 'year'] as const) {
      const t = await rows({ ...BY_MONTH, grain });
      assert.deepEqual(values(t, `sold__${grain}`).map((v) => String(v).slice(0, 10)), ['2024-01-01'], grain);
      assert.deepEqual(values(t, 'sum_revenue'), [1200], grain);
    }
  });

  it('a number by bins of a width: floor(v / w) * w', async () => {
    const t = await rows({ ...BY_REGION, x: 'revenue', bins: { width: 200 }, y: [{ column: 'cost', fn: 'count' }] });
    // 50, 100, 150 -> 0; 200, 300 -> 200; 400 -> 400
    assert.deepEqual(values(t, 'revenue__bin'), [0, 200, 400]);
    assert.deepEqual(values(t, 'count'), [3, 2, 1]);
  });

  it('a number by a count of bins over its range: the database finds the range and each edge', async () => {
    const t = await rows({ ...BY_REGION, x: 'revenue', bins: { count: 2 }, y: [{ column: 'cost', fn: 'count' }] });
    // the range 50..400, two bins 175 wide: [50, 225) holds 50, 100, 150, 200; [225, 400] holds 300, 400 (the maximum
    // in the last bin, not one of its own)
    assert.deepEqual(values(t, 'revenue__bin').map(Number), [50, 225]);
    assert.deepEqual(values(t, 'count'), [4, 2]);
  });

  it('a running total along the months, restarting per region', async () => {
    const t = await rows({ ...BY_MONTH, split: 'region', y: [{ column: 'revenue', fn: 'sum', showAs: { kind: 'runningTotal' } }],
      options: { ...BY_MONTH.options, sort: { by: 'x', direction: 'asc' } } });
    const got = values(t, 'region').map((r, i) => `${String(r)} ${String(values(t, 'sold__month')[i]).slice(0, 7)} ${String(values(t, 'runningTotal_sum_revenue')[i])}`);
    assert.deepEqual(got.sort(), [
      'AMER 2024-01 400', 'AMER 2024-02 450', 'AMER 2024-03 600',
      'EMEA 2024-01 100', 'EMEA 2024-02 300', 'EMEA 2024-03 600',
    ]);
  });

  it('% of total, the change from the month before (absolute and as a ratio), a moving average, a rank', async () => {
    const shown = async (kind: 'percentOfTotal' | 'difference' | 'percentChange' | 'movingAverage' | 'rank') => {
      const t = await rows({ ...BY_MONTH, y: [{ column: 'revenue', fn: 'sum', showAs: kind === 'movingAverage' ? { kind, rows: 2 } : { kind } }] });
      return values(t, `${kind}_sum_revenue`).map((v) => (v === null ? null : Number(Number(v).toFixed(4))));
    };
    // the months' sums: 500, 250, 450 of 1200
    assert.deepEqual(await shown('percentOfTotal'), [500 / 1200, 250 / 1200, 450 / 1200].map((v) => Number(v.toFixed(4))));
    assert.deepEqual(await shown('difference'), [null, -250, 200]);
    assert.deepEqual(await shown('percentChange'), [null, -0.5, 0.8]);
    assert.deepEqual(await shown('movingAverage'), [500, 375, 350]);
    assert.deepEqual(await shown('rank'), [1, 3, 2]);
  });
});

describe('the presets in the grid, planned and run: along its own rows', () => {
  it('a show-as over the grid\'s level is a group-level window in the order the level shows', async () => {
    const { showAsColumns } = await import('../src/calc-presets.ts');
    const { levelLambda } = await import('../src/query.ts');
    const s: CubeSnapshot = {
      ...cube,
      rows: ['region'],
      sorts: [{ column: 'region', direction: 'asc' }],
      groupDerived: [
        ...showAsColumns({ kind: 'runningTotal' }, 'revenue', [], [], 'revenue_runningTotal'),
        ...showAsColumns({ kind: 'percentOfTotal' }, 'revenue', [], [], 'revenue_share'),
      ],
    };
    const t = await engine.execute(await planner.plan(levelLambda(s, { level: 1, parent: [] })), 0);
    assert.deepEqual(values(t, 'region'), ['AMER', 'EMEA']);
    assert.deepEqual(values(t, 'revenue_runningTotal'), [600, 1200]);
    assert.deepEqual(values(t, 'revenue_share').map(Number), [0.5, 0.5]);
  });

  it('a date by its period is a row-stage column the grid can group by', async () => {
    const { grainColumn } = await import('../src/calc-presets.ts');
    const { levelLambda } = await import('../src/query.ts');
    const s: CubeSnapshot = {
      ...cube,
      derived: [grainColumn('sold', 'quarter', 'sold_quarter')],
      rows: ['sold_quarter'],
      sorts: [{ column: 'sold_quarter', direction: 'asc' }],
    };
    const t = await engine.execute(await planner.plan(levelLambda(s, { level: 1, parent: [] })), 0);
    assert.deepEqual(values(t, 'sold_quarter').map((v) => String(v).slice(0, 10)), ['2024-01-01']);
    assert.deepEqual(values(t, 'revenue'), [1200]);
  });
});

describe('reference lines, planned and run: the database\'s statistics over the chart\'s rows', () => {
  it('the average, maximum and minimum of a measure over every row the chart draws', async () => {
    const t = await rows({ ...BY_REGION, x: 'sold', grain: 'month', layers: [{ kind: 'stat', stat: 'average' }, { kind: 'extremes' }] });
    // the months' sums 500, 250, 450: average 400, max 500, min 250 -- the same on every row
    assert.deepEqual(values(t, 'average_of_sum_revenue').map(Number), [400, 400, 400]);
    assert.deepEqual(values(t, 'max_of_sum_revenue').map(Number), [500, 500, 500]);
    assert.deepEqual(values(t, 'min_of_sum_revenue').map(Number), [250, 250, 250]);
  });

  it('a hover\'s and a label\'s measures beside the plotted one, each its own aggregate of one column', async () => {
    const t = await rows({ ...BY_REGION, tooltip: [{ column: 'revenue', fn: 'max' }], label: { column: 'cost', fn: 'sum' } });
    assert.deepEqual(values(t, 'sum_revenue'), [600, 600]);
    assert.deepEqual(values(t, 'max_revenue'), [400, 300]);
    assert.deepEqual(values(t, 'sum_cost'), [230, 410]);
  });
});

describe('the example pages\' charts, planned and run on their own sample (docs/DATACUBE_CHARTS_DESIGN_2026_10_09.md §3.3)', () => {
  it('the sample reads as the page declares it, and every chart\'s query plans and answers', async () => {
    const { chartsDashboardPage, EXAMPLE_CUBE, EXAMPLE_ROWS, SALES_SPECS } = await import('../src/examples/charts-dashboard.ts');
    const { chartsKindsPage } = await import('../src/examples/charts-kinds.ts');
    const { salesCsv } = await import('../src/samples.ts');
    const db = await openDatabase();
    const local = new DuckDbEngine(db.connection);
    try {
      const r = await ingestFile(local, db.files, new File([salesCsv(EXAMPLE_ROWS)], 'sample-sales.csv'), writer);
      const sales = plannerFor(r.model, r.runtime);
      // the page's column types are the compiler's reading of the file
      const all = await local.execute(await sales.plan(from(r.source).limit(1).lambda()), 0);
      assert.deepEqual(all.columns.map((c) => `${c.name}:${c.type}`), SALES_SPECS.map((c) => `${c.name}:${c.type}`));
      const cube: CubeSnapshot = { ...EXAMPLE_CUBE, source: { query: r.source } };
      // batch 2's page and batch 3's; a chart of the grid as shown (a sunburst, a tree) has no query of its own
      const views = [...(await chartsDashboardPage()).views, ...(await chartsKindsPage()).views];
      for (const v of views) {
        if (v.kind !== 'chart' || GRID_MARKS.has(v.spec.mark)) continue;
        const { query } = chartQuery(cube, v.spec);
        const t = await local.execute(await sales.plan(query), 0);
        assert.ok(t.rowCount > 0, `${v.title}: rows`);
        // a value on a bin's edge is in its own bin: 0.15 / 0.05 is 2.9999999999999996 in floating point, and the
        // database rounds the quotient before its floor (calc-presets.ts floorOf)
        // a bubble map groups by each place's latitude and longitude with its name: the sample's ten cities, each once
        if (v.id === 'k-bubbles') {
          assert.equal(t.rowCount, 10);
          assert.equal(new Set(values(t, 'city')).size, 10);
          assert.deepEqual(values(t, 'lat').filter((_, i) => values(t, 'city')[i] === 'Tokyo').map(Number), [35.68]);
        }
        if (v.id === 'b-discount') {
          assert.deepEqual(values(t, 'discount__bin').map((d) => Number(Number(d).toFixed(2))).sort(), [0, 0.05, 0.1, 0.15, 0.2]);
        }
      }
    } finally {
      await local.close();
    }
  });
});

describe('the database\'s edges: a period\'s end, a bin\'s upper edge (the audit, batch 2)', () => {
  it('each month ends where the next begins: the database\'s adjust()', async () => {
    const { AXIS_UPPER } = await import('../src/chart-spec.ts');
    const t = await rows({ ...BY_REGION, x: 'sold', grain: 'month' });
    assert.deepEqual(values(t, AXIS_UPPER).map((v) => String(v).slice(0, 10)), ['2024-02-01', '2024-03-01', '2024-04-01']);
  });

  it('a bin\'s upper edge is the next edge; the maximum\'s, one bin past it, so the last bin keeps it', async () => {
    const { AXIS_UPPER } = await import('../src/chart-spec.ts');
    const byWidth = await rows({ ...BY_REGION, x: 'revenue', bins: { width: 200 }, y: [{ column: 'cost', fn: 'count' }] });
    assert.deepEqual(values(byWidth, AXIS_UPPER).map(Number), [200, 400, 600]);
    const byCount = await rows({ ...BY_REGION, x: 'revenue', bins: { count: 2 }, y: [{ column: 'cost', fn: 'count' }] });
    // range 50..400, 175 wide: the first bin ends at 225; the last holds 400 itself, so it ends past it
    assert.deepEqual(values(byCount, AXIS_UPPER).map(Number), [225, 575]);
  });

  it('a % change from nothing is empty, not a division by zero', async () => {
    const db = await openDatabase();
    const local = new DuckDbEngine(db.connection);
    try {
      const csv = 'region,sold,revenue,cost\nEMEA,2024-01-15,0,0\nEMEA,2024-02-10,200,100\n';
      const r = await ingestFile(local, db.files, new File([csv], 'zero.csv'), writer);
      const zero = plannerFor(r.model, r.runtime);
      const { query } = chartQuery({ ...cube, source: { query: r.source } },
        { ...BY_REGION, x: 'sold', grain: 'month', y: [{ column: 'revenue', fn: 'sum', showAs: { kind: 'percentChange' } }] });
      const t = await local.execute(await zero.plan(query), 0);
      assert.deepEqual(values(t, 'percentChange_sum_revenue'), [null, null]);
    } finally {
      await local.close();
    }
  });
});

describe('a bin and its filter agree by construction (the DataCube + Python line\'s review)', () => {
  it('values on every edge: each bar\'s click keeps exactly its own rows, width and count bins alike', async () => {
    const { AXIS_UPPER, markConditions } = await import('../src/chart-spec.ts');
    const db = await openDatabase();
    const local = new DuckDbEngine(db.connection);
    try {
      // discounts ON the edges of 0.05-wide bins, where a product of floats (3 * 0.05 = 0.15000000000000002) misplaces them
      const discounts = [0, 0.05, 0.1, 0.15, 0.15, 0.2, 0.25, 0.3, 0.35, 0.35, 0.35, 0.4];
      const csv = `region,discount\n${discounts.map((d, i) => `${i % 2 ? 'EMEA' : 'AMER'},${d.toFixed(2)}`).join('\n')}\n`;
      const r = await ingestFile(local, db.files, new File([csv], 'edges.csv'), writer);
      const p = plannerFor(r.model, r.runtime);
      const edges: CubeSnapshot = {
        source: { query: r.source },
        columns: [{ name: 'region', type: 'String' }, { name: 'discount', type: 'Float', kind: 'measure' }],
        derived: [], rows: [], pivotOn: [], sorts: [], epoch: 1,
      };
      for (const bins of [{ width: 0.05 }, { count: 4 }]) {
        const spec: ChartSpec = { ...BY_REGION, x: 'discount', bins, y: [{ column: 'discount', fn: 'count' }] };
        const bars = await local.execute(await p.plan(chartQuery(edges, spec).query), 0);
        const at = values(bars, 'discount__bin');
        const counts = values(bars, 'count').map(Number);
        const uppers = values(bars, AXIS_UPPER);
        assert.equal(counts.reduce((a, b) => a + b, 0), discounts.length, `${JSON.stringify(bins)}: every row in a bin`);
        for (let i = 0; i < at.length; i++) {
          // the bar's click, run as the grid's filter: its rows, and none of another bar's
          const filter = markConditions(spec, edges, { discount: at[i] as number }, uppers[i] as number);
          const kept = await local.execute(await p.plan(chartQuery({ ...edges, filter: { kind: 'and', children: filter } },
            { ...BY_REGION, x: 'region', y: [{ column: 'discount', fn: 'count' }] }).query), 0);
          const total = values(kept, 'count').map(Number).reduce((a, b) => a + b, 0);
          assert.equal(total, counts[i], `${JSON.stringify(bins)}: the bar at ${String(at[i])} holds ${counts[i]}, its click keeps ${total}`);
        }
        if (bins.width !== undefined) {
          // and each edge is the decimal it means: 0.15's bar holds both 0.15s
          assert.deepEqual(at.map(Number), [0, 0.05, 0.1, 0.15, 0.2, 0.25, 0.3, 0.35, 0.4]);
          assert.deepEqual(counts, [1, 1, 1, 2, 1, 1, 1, 3, 1]);
        }
      }
    } finally {
      await local.close();
    }
  });
});

describe('batch 3\'s kinds, planned and run: the database computes each kind\'s own figures', () => {
  it('a waterfall by month: each bar\'s end the running total in the chart\'s order, its start that end less its value', async () => {
    const t = await rows({ ...BY_REGION, mark: 'waterfall', x: 'sold', grain: 'month', options: { ...DEFAULT_OPTIONS, sort: { by: 'x', direction: 'asc' } } });
    const { start, end } = waterfallNames('sum_revenue');
    assert.deepEqual(values(t, 'sum_revenue'), [500, 250, 450]);
    assert.deepEqual(values(t, end).map(Number), [500, 750, 1200]);
    assert.deepEqual(values(t, start).map(Number), [0, 500, 750]);
  });
  it('a Pareto: the running share from the largest, ties in the category\'s order -- the rows in that same order', async () => {
    const t = await rows({ ...BY_REGION, mark: 'pareto', options: { ...DEFAULT_OPTIONS, sort: { by: 'y', direction: 'desc' } } });
    // EMEA and AMER both sold 600: AMER first, by name, in the share and in the rows alike
    assert.deepEqual(values(t, 'region'), ['AMER', 'EMEA']);
    assert.deepEqual(values(t, paretoName('sum_revenue')).map(Number), [0.5, 1]);
  });
  it('a pie in two rings: the inner ring each region\'s total over its split\'s values', async () => {
    const t = await rows({ ...BY_REGION, mark: 'pie', split: 'sold' });
    const regions = values(t, 'region');
    const totals = values(t, ringTotalName('sum_revenue')).map(Number);
    regions.forEach((r, i) => assert.equal(totals[i], 600, `${String(r)}'s inner ring`));
    assert.equal(t.rowCount, 6);
  });
  it('a gauge: one row, the measure over every row', async () => {
    const { x: _x, ...spec } = BY_REGION;
    const t = await rows({ ...spec, mark: 'gauge', options: { ...DEFAULT_OPTIONS, target: 2000 } });
    assert.deepEqual(values(t, 'sum_revenue').map(Number), [1200]);
  });
  it('small multiples: a share of each panel\'s own total, and its own average line', async () => {
    const t = await rows({
      ...BY_REGION, x: 'sold', grain: 'month', facet: { column: 'region' },
      y: [{ column: 'revenue', fn: 'sum', showAs: { kind: 'percentOfTotal' } }],
      layers: [{ kind: 'stat', stat: 'average' }],
    });
    const regions = values(t, 'region');
    const shares = values(t, 'percentOfTotal_sum_revenue').map(Number);
    // each region's months add to the whole of that region: EMEA 100/200/300 of 600, AMER 400/50/150 of 600
    for (const r of ['EMEA', 'AMER']) {
      const sum = shares.filter((_, i) => regions[i] === r).reduce((a, b) => a + b, 0);
      assert.ok(Math.abs(sum - 1) < 1e-9, `${r}'s shares add to one`);
    }
    // the average line is each panel's: 200 a month in each
    assert.deepEqual([...new Set(values(t, 'average_of_percentOfTotal_sum_revenue').map((v) => Number(v).toFixed(6)))], [(1 / 3).toFixed(6)]);
  });
});

describe('batch 4, planned and run: a box plot\'s figures are the database\'s', () => {
  it('each region\'s least, quartiles (continuous, as Pure\'s percentile), median and most, ordered by the median', async () => {
    const t = await rows({ ...BY_REGION, mark: 'boxplot', y: boxMeasures('revenue'), options: { ...DEFAULT_OPTIONS, sort: { by: 'y', direction: 'desc' } } });
    // EMEA sold 100, 200, 300 and AMER 50, 150, 400: a quartile between two sales is the point between them
    assert.deepEqual(values(t, 'region'), ['EMEA', 'AMER']);
    assert.deepEqual(values(t, 'min_revenue').map(Number), [100, 50]);
    assert.deepEqual(values(t, 'percentile25_revenue').map(Number), [150, 100]);
    assert.deepEqual(values(t, 'median_revenue').map(Number), [200, 150]);
    assert.deepEqual(values(t, 'percentile75_revenue').map(Number), [250, 275]);
    assert.deepEqual(values(t, 'max_revenue').map(Number), [300, 400]);
  });
  it('a pivot cell on a quartile: each region\'s own rows only (the cell\'s condition inside the map), and the total over all', async () => {
    const { levelLambda, pinnedPivotFacts } = await import('../src/query.ts');
    const s: CubeSnapshot = {
      ...cube,
      rows: [],
      pivotOn: ['region'],
      pivotValues: ['AMER', 'EMEA'],
      pivotTotal: { placement: 'right' },
      columns: cube.columns.map((c) => (c.name === 'cost' ? { ...c, aggregate: 'percentile25' as const } : c)),
    };
    const t = await engine.execute(await planner.plan(levelLambda(s, undefined, pinnedPivotFacts(s) ?? undefined)), 0);
    const cell = (part: string): number => {
      const c = t.columns.find((x) => x.name.includes(part) && x.name.includes('cost'));
      assert.ok(c, `a column for ${part} (the answer has ${t.columns.map((x) => x.name).join(', ')})`);
      return Number(c.values[0]);
    };
    // AMER's costs 40, 90, 100 and EMEA's 60, 150, 200; every cost 40, 60, 90, 100, 150, 200
    assert.equal(cell('AMER'), 65);
    assert.equal(cell('EMEA'), 105);
    assert.equal(cell('total'), 67.5);
  });
  it('a grid column on the first or third quartile, planned and run at its level', async () => {
    const { levelLambda } = await import('../src/query.ts');
    const s: CubeSnapshot = {
      ...cube,
      columns: cube.columns.map((c) => (c.name === 'cost' ? { ...c, aggregate: 'percentile25' as const }
        : c.name === 'revenue' ? { ...c, aggregate: 'percentile75' as const } : c)),
      sorts: [{ column: 'region', direction: 'asc' }],
    };
    const t = await engine.execute(await planner.plan(levelLambda(s, { level: 1, parent: [] })), 0);
    // AMER's costs 40, 90, 100 and EMEA's 60, 150, 200; revenues 50, 150, 400 and 100, 200, 300
    assert.deepEqual(values(t, 'region'), ['AMER', 'EMEA']);
    assert.deepEqual(values(t, 'cost').map(Number), [65, 105]);
    assert.deepEqual(values(t, 'revenue').map(Number), [275, 250]);
  });
});

describe('batch 3\'s leftovers, planned and run: the database lays out each cell', () => {
  it('a Marimekko: each region as wide as its share of the whole, each sale up it as tall as its share of the region', async () => {
    const t = await rows({ ...BY_REGION, mark: 'marimekko', split: 'sold' });
    const n = marimekkoNames('sum_revenue');
    const at = (region: string, sold: string): number[] => {
      const i = values(t, 'region').findIndex((r, k) => r === region && String(values(t, 'sold')[k]).startsWith(sold));
      return [n.x0, n.x1, n.y0, n.y1].map((c) => Number(Number(values(t, c)[i]).toFixed(4)));
    };
    // AMER sold 400, 50, 150 and EMEA 100, 200, 300: 600 each of 1,200, AMER first across
    assert.deepEqual(at('AMER', '2024-01-20'), [0, 0.5, 0, 0.6667]);
    assert.deepEqual(at('AMER', '2024-02-25'), [0, 0.5, 0.6667, 0.75]);
    assert.deepEqual(at('AMER', '2024-03-30'), [0, 0.5, 0.75, 1]);
    assert.deepEqual(at('EMEA', '2024-03-05'), [0.5, 1, 0.5, 1]);
  });
  it('a waffle: each region the cells from its running share before it to its running share through it, a hundred in all', async () => {
    const t = await rows({ ...BY_REGION, mark: 'waffle', options: { ...DEFAULT_OPTIONS, sort: { by: 'y', direction: 'desc' } } });
    const { from, to } = waffleNames('sum_revenue');
    // 600 each: AMER first by name
    assert.deepEqual(values(t, 'region'), ['AMER', 'EMEA']);
    assert.deepEqual(values(t, from).map(Number), [0, 50]);
    assert.deepEqual(values(t, to).map(Number), [50, 100]);
  });
  it('a whole of nothing: the waffle\'s ranges and the Marimekko\'s shares empty, never a division by zero', async () => {
    // a value that nets to nothing in every region: each sale less itself
    const flat: CubeSnapshot = { ...cube, derived: [{ name: 'flat', lambda: await row('$x.revenue->toOne() - $x.revenue->toOne()') }] };
    const waffle = await rows({ ...BY_REGION, mark: 'waffle', y: [{ column: 'flat', fn: 'sum' }] }, flat);
    const { from, to } = waffleNames('sum_flat');
    assert.deepEqual(values(waffle, to), [null, null]);
    assert.deepEqual(values(waffle, from).map(Number), [0, 0]);
    const mekko = await rows({ ...BY_REGION, mark: 'marimekko', split: 'sold', y: [{ column: 'flat', fn: 'sum' }] }, flat);
    const n = marimekkoNames('sum_flat');
    assert.ok([n.x0, n.x1, n.y0, n.y1].every((c) => values(mekko, c).every((v) => v === null)));
  });
  it('a Gantt: each region\'s first and last sale, the dates the database answers', async () => {
    const t = await rows({ ...BY_REGION, mark: 'gantt', y: [{ column: 'sold', fn: 'min' }, { column: 'sold', fn: 'max' }] });
    assert.deepEqual(values(t, 'region'), ['AMER', 'EMEA']);
    assert.deepEqual(values(t, 'min_sold').map((d) => String(d).slice(0, 10)), ['2024-01-20', '2024-01-15']);
    assert.deepEqual(values(t, 'max_sold').map((d) => String(d).slice(0, 10)), ['2024-03-30', '2024-03-05']);
  });
});
