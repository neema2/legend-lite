// THE SHARE LINK'S DICTIONARY, made from the product's own vocabulary (docs/DATACUBE_SAVE_SHARE_2026_09_28.md,
// milestone 1b). A share link is the saved page, deflated against a PRESET DICTIONARY that the app ships
// and never sends: text the compressor may refer back to, so the words every page repeats -- the
// document's keys, its kinds, the protocol's node shapes, operators, functions, types -- cost almost
// nothing in a link. Nothing user-specific is in it: no column name, no value, no title.
//
// Made HERE, from the product's tables (the compiler's types and aggregates, the filter operators, the
// calculated-column and window functions, the chart marks) through the product's own writers (the
// cube's, the page's, the protocol's) -- never typed by hand. And made ONCE per version: a link names
// the dictionary it was made with (`p1.`), so a version's dictionary never changes after it ships
// (src/share/link-p1.ts, pinned by its hash in test/share-link.test.ts). A new vocabulary is `p2`:
// `bazel run //datacube:cut_link_dictionary` writes it (datacube/BUILD.bazel names the next version).

import { col, fn, lambda, lit, toJson } from '../../../pure-protocol/src/index.ts';
import { CALC_FUNCTIONS } from '../../src/calc.ts';
import { CHART_MARKS, type ChartSpec } from '../../src/chart-spec.ts';
import { DEFAULT_CONFIGURATION, type ColumnConfiguration, type CubeConfiguration } from '../../src/config.ts';
import type { ColumnFormat } from '../../src/format.ts';
import { writeCube, type FileSource } from '../../src/cube-document.ts';
import { OFFER_FACTS } from '../../src/generated/offer-facts.ts';
import { pageToJson, writePage, type PageViews } from '../../src/page-document.ts';
import { WINDOW_FUNCTIONS, type CubeSnapshot } from '../../src/snapshot.ts';
import { TreeState } from '../../src/tree.ts';
import { OPERATORS } from '../../src/ui/filter-editor.ts';

/** Every format field, set (a field added later fails the build here, to be weighed for the next version). */
const FORMAT: Required<ColumnFormat> = {
  kind: 'number', locale: '', minimumFractionDigits: 0, maximumFractionDigits: 2, decimals: 2,
  displayCommas: true, currency: '', numberScale: 'thousands', scale: 1, unit: '', nullText: '',
  negativeParens: true, fontCase: 'uppercase',
};

/** Every column setting, set: a setting added later fails the build here, to be weighed for the next version. */
const COLUMN: Required<ColumnConfiguration> = {
  displayName: '', kind: 'dimension', aggregateFn: 'sum', aggregationParameters: [], hidden: true,
  blurred: true, pinned: 'left', widthMode: 'fixed', width: 120, minWidth: 40, maxWidth: 400,
  format: FORMAT,
  appearance: DEFAULT_CONFIGURATION.appearance,
  heatmap: { from: '#ffffff', to: '#ff8a65' },
  excludedFromPivot: true, pivotSortDirection: 'desc', pivotStatisticColumnFunction: 'sum',
  displayAsLink: true, linkLabelParameter: '',
};

/** The document's vocabulary: a page around a cube that uses every kind of thing a cube holds, empty. */
function templatePage(): string {
  const types = Object.keys(OFFER_FACTS);
  const aggregates = Object.keys(Object.values(OFFER_FACTS)[0]?.aggregates ?? {});
  const columns = types.map((type) => ({ name: '', type, kind: 'dimension' as const }));
  const snapshot: CubeSnapshot = {
    source: { query: lambda([], lit.integer(1)) } as unknown as CubeSnapshot['source'],
    columns,
    derived: [{ name: '', lambda: lambda(['x'], fn('toOne', col('x', 'a'))), kind: 'measure' }],
    rows: [''],
    pivotOn: [''],
    measures: aggregates.map((a) => ({ name: '', column: '', fn: a })) as CubeSnapshot['measures'],
    sorts: [{ column: '', direction: 'asc' }, { column: '', direction: 'desc' }],
    filter: { kind: 'and', children: [
      ...OPERATORS.map((o) => ({ kind: 'condition' as const, column: '', operator: o.op, value: '' })),
      { kind: 'or', children: [] }, { kind: 'not', child: { kind: 'condition', column: '', operator: 'equal', value: '' } },
    ] } as CubeSnapshot['filter'],
    epoch: 1,
  } as CubeSnapshot;
  const configuration: CubeConfiguration = {
    ...DEFAULT_CONFIGURATION,
    reportTitle: '',
    showRootAggregation: !DEFAULT_CONFIGURATION.showRootAggregation,
    columns: { '': COLUMN },
  };
  const source: FileSource = { _type: 'file', name: '', format: 'csv', size: 0, sha256: '', columns: types.map((type) => ({ name: '', type })) };
  const cube = writeCube({ name: '', source, snapshot, configuration, tree: TreeState.empty().withExpandTo(1) });
  const chart = (mark: ChartSpec['mark']): ChartSpec => ({
    version: 1, mark, x: '', y: [{ column: '', fn: 'sum' }], split: '',
    options: { orientation: 'vertical', stack: 'none', sort: { by: 'y', direction: 'desc' }, limit: 20, labels: true, legend: 'bottom' },
  });
  const views: PageViews = {
    views: [{ id: 'grid', kind: 'grid', cube: 'cube' }, ...CHART_MARKS.map((m) => ({ id: '', kind: 'chart' as const, cube: 'cube', title: '', spec: chart(m.value) }))],
    layout: { kind: 'grid', cols: 12, arranged: true, tiles: [{ id: 'grid', x: 0, y: 0, w: 8, h: 6 }, { id: '', x: 8, y: 0, w: 4, h: 3 }] },
  };
  return pageToJson(writePage({ name: '', cube, views }));
}

/** The protocol's vocabulary as the writer spells it: a calculated column's shapes, and each function by name. */
function protocolWords(): string {
  const x = 'x';
  const shapes = toJson(lambda([x], fn('if', fn('greaterThan', col(x, 'a'), lit.integer(1)),
    lambda([], fn('plus', fn('toOne', col(x, 'a')), lit.float(1.5), lit.string(''))),
    lambda([], fn('divide', fn('times', col(x, 'a'), lit.decimal('2.5')), fn('minus', col(x, 'a'), lit.boolean(true)))))));
  const functions = [...new Set([...CALC_FUNCTIONS.map((f) => f.name), ...WINDOW_FUNCTIONS.map((f) => f.fn),
    'plus', 'minus', 'times', 'divide', 'equal', 'greaterThan', 'greaterThanEqual', 'lessThan', 'lessThanEqual',
    'and', 'or', 'not', 'if', 'in', 'isEmpty', 'isNotEmpty', 'toOne', 'toMany', 'to', 'cast', 'get'])];
  return shapes + functions.map((f) => `{"_type":"func","function":"${f}","parameters":[`).join('');
}

/** The words that differ between pages, in the contexts they appear: operators, aggregates, types, marks. */
function enumWords(): string {
  const types = Object.keys(OFFER_FACTS);
  const aggregates = Object.keys(Object.values(OFFER_FACTS)[0]?.aggregates ?? {});
  return [
    ...OPERATORS.map((o) => `"operator":"${o.op}"`),
    ...aggregates.map((a) => `"fn":"${a}"`),
    ...types.map((t) => `"type":"${t}"`),
    ...CHART_MARKS.map((m) => `"mark":"${m.value}"`),
  ].join(',');
}

/**
 * The dictionary: the rarer words first, the document's own shape last -- deflate reaches back
 * cheapest to what is nearest, and the shape is what every page repeats most.
 */
export function makeDictionary(): string {
  return enumWords() + protocolWords() + templatePage();
}

if (process.argv[1]?.endsWith('make.ts')) {
  const version = process.argv[2] ?? 'p1';
  const text = makeDictionary();
  process.stdout.write(`// THE SHARE LINK'S DICTIONARY, version ${version} -- FROZEN: links made with it must open forever, so
// these bytes never change (pinned by their hash in test/share-link.test.ts). Made once by
// datacube/tools/link-dictionary/make.ts from the product's own vocabulary; a new vocabulary is a
// new version, never an edit here. No column name, value or title is in it.

export const LINK_DICTIONARY_${version.toUpperCase()} = ${JSON.stringify(text)};
`);
}
