// THE SAVED-PAGE CORPUS, MADE (docs/DATACUBE_COMMAND_API_HOMEWORK_2026_10_17.md §11, part 3): real page documents, each
// written by DataCube's own writers -- never typed by hand -- kept as fixtures so a change to how a page is written
// shows as a diff of these files (test/page-corpus.test.ts reads them; python/tests/test_page.py loads the frame page).
// Run it when a version changes, deliberately, and read the diff before committing it:
//   cd datacube && node --experimental-strip-types --disable-warning=ExperimentalWarning test/fixtures/pages/make.ts
//
// - trades.page.json: a page over the frame `trades`, as Python's and the notebook's pages are: two grids (grouped
//   with per-column Aggregations, a weighted average, a sort, a filter, a width and an appearance setting; pivoted with
//   totals, a total's own heatmap, a sort on the total, a hidden column), a live chart and a frozen one stacked, two
//   sheets.
// - charts-dashboard.page.json, charts-kinds.page.json: the site's example pages (src/examples/), every chart kind and
//   preset over the sales sample, calculated columns at each level among them.

import { writeFileSync } from 'node:fs';
import { pathToFileURL } from 'node:url';

import { element } from '../../../../pure-protocol/src/index.ts';
import { defaultChart } from '../../../src/chart-spec.ts';
import { DEFAULT_CONFIGURATION, withConfiguration, type CubeConfiguration } from '../../../src/config.ts';
import { frameSource, rawRows, writeCube } from '../../../src/cube-document.ts';
import { openedState } from '../../../src/cube-state.ts';
import { chartsDashboardPage } from '../../../src/examples/charts-dashboard.ts';
import { chartsKindsPage } from '../../../src/examples/charts-kinds.ts';
import { clickSort, setColumn, setGrouping, setPivot, type GridDraft } from '../../../src/grid-edits.ts';
import { NEW_SHEET, add, asSaved, stackTiles } from '../../../src/layout/bands.ts';
import { pageToJson, writePageOf, type PageDocument, type PageView } from '../../../src/page-document.ts';
import { pivotTotalColumn, type CubeSnapshot } from '../../../src/snapshot.ts';
import { TreeState } from '../../../src/tree.ts';

/** The frame's columns, as Python types a frame of text, dates, floats and integers. */
export const TRADES_COLUMNS: CubeSnapshot['columns'] = [
  { name: 'region', type: 'String' },
  { name: 'desk', type: 'String' },
  { name: 'traded', type: 'StrictDate' },
  { name: 'notional', type: 'Float' },
  { name: 'qty', type: 'Integer' },
];

/** A cube of the frame as a page holds one: as the screen holds a cube it opens (cube-state.ts openedState). */
function cubeOf(d: GridDraft) {
  const s = openedState(d.snapshot, d.config, TreeState.empty());
  return writeCube({ name: 'trades', source: frameSource('trades', TRADES_COLUMNS), snapshot: s.snapshot,
    configuration: s.configuration, tree: s.tree });
}

const patched = (d: GridDraft, patch: Parameters<typeof withConfiguration>[1]): GridDraft =>
  ({ ...d, config: withConfiguration(d.config, patch) });

/** The frame page: what a person (or Python) builds over a frame, with each kind of setting a page keeps. */
function tradesPage(): PageDocument {
  const start = (): GridDraft => ({
    snapshot: rawRows(element('trades'), TRADES_COLUMNS),
    config: { ...DEFAULT_CONFIGURATION, reportTitle: 'trades' } as CubeConfiguration,
  });
  // grouped: per-column Aggregations (an average, a weighted average), a sort, a filter, a width, an appearance setting
  let a = setGrouping(start(), ['region', 'desk']);
  a = setColumn(a, 'qty', { aggregate: 'average' });
  a = setColumn(a, 'notional', { aggregate: 'wavg', weight: 'qty' });
  a = clickSort(a, 'notional', 'desc', false);
  a = { ...a, snapshot: { ...a.snapshot, filter: { kind: 'condition', column: 'region', operator: 'notEqual', value: 'APAC' } } };
  a = patched(a, { columns: { qty: { width: 90 } }, appearance: { fontSize: 13 } });
  // pivoted: totals on, a total's own heatmap, a sort on the total, a hidden column
  const total = pivotTotalColumn('notional');
  let b = setPivot(setGrouping(start(), ['region']), ['desk']);
  b = clickSort(b, total, 'desc', false);
  b = patched(b, { columns: { [total]: { heatmap: { from: '#ffffff', to: '#ff8a65' } }, qty: { hidden: true } } });

  const cubeA = cubeOf(a);
  const cubeB = cubeOf(b);
  const live = defaultChart(cubeA.query as unknown as CubeSnapshot);
  const frozen = defaultChart(cubeB.query as unknown as CubeSnapshot);
  if (!live || !frozen) throw new Error('the frame page\'s grids make no chart');
  const views: PageView[] = [
    { id: 'grid', kind: 'grid', cube: 'grid', title: 'Positions' },
    { id: 'grid-2', kind: 'grid', cube: 'grid-2', title: 'By desk' },
    { id: 'chart', kind: 'chart', cube: 'grid', title: 'Average qty by region', spec: live },
    { id: 'chart-2', kind: 'chart', cube: 'grid-2', title: 'By desk, frozen', spec: { ...frozen, frozen: true } },
  ];
  // the first sheet: the grid and its chart side by side, the frozen chart stacked behind the live one
  const first = stackTiles(add(add(add(NEW_SHEET, 'grid'), 'chart', 'grid'), 'chart-2', 'chart'), ['chart', 'chart-2']);
  return writePageOf({
    name: 'Corpus trades',
    cubes: [{ id: 'grid', cube: cubeA }, { id: 'grid-2', cube: cubeB }],
    views: {
      views,
      sheets: [
        { id: 'sheet-1', name: 'Positions', layout: { kind: 'bands', ...asSaved(first) } },
        { id: 'sheet-2', layout: { kind: 'bands', ...asSaved(add(NEW_SHEET, 'grid-2')) } },
      ],
    },
  });
}

/** Every page of the corpus, by its file's name. */
export async function corpus(): Promise<Record<string, PageDocument>> {
  return {
    'trades.page.json': tradesPage(),
    'charts-dashboard.page.json': await chartsDashboardPage(),
    'charts-kinds.page.json': await chartsKindsPage(),
  };
}

// run as a script: the fixtures written beside it, each as DataCube writes a page (pageToJson), ending in a newline
if (process.argv[1] !== undefined && import.meta.url === pathToFileURL(process.argv[1]).href) {
  for (const [name, page] of Object.entries(await corpus())) {
    writeFileSync(new URL(name, import.meta.url), `${pageToJson(page)}\n`);
    console.log(`wrote ${name}`);
  }
}
