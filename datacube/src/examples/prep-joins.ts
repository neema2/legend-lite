// THE JOIN EXAMPLE (the user, 2026-10-23: "I thought we added ... join data prep -- I don't see the join prep on deployed
// site"): two tables of the sales sample's world -- its orders, and the countries it sells in -- joined as the Join…
// window joins them: every order with its country's facts beside it, grouped by continent, with a chart of it; and the
// orders whose country the table lacks. Built by DataCube's own writers (cube-document.ts writeCube, join.ts joinTitle
// and theirNames), as the other example pages are; it opens as a saved page does, each sample rebuilt, each join made
// again by the tab (sources.ts openedJoin). test/examples.test.ts reads it back; demo/verify-join.mjs opens it.

import { element } from '../../../pure-protocol/src/index.ts';
import { DEFAULT_CONFIGURATION, type CubeConfiguration } from '../config.ts';
import { fileSource, writeCube, type CubeDocument, type FileSource, type JoinInput, type JoinSource } from '../cube-document.ts';
import { openedState } from '../cube-state.ts';
import { joinTitle, theirNames, type JoinHow } from '../join.ts';
import { writePageOf, type PageDocument, type PageSheet, type PageView } from '../page-document.ts';
import { COUNTRIES_SAMPLE, sampleById, sampleFileName } from '../samples.ts';
import type { ColumnSpec, CubeSnapshot } from '../snapshot.ts';
import { TreeState } from '../tree.ts';
import { chart, EXAMPLE_ROWS, EXAMPLE_SAMPLE, REVENUE, SALES_SPECS } from './charts-dashboard.ts';

/** The countries sample's columns as the compiler types them (test/examples.test.ts holds them to the sample's). */
export const COUNTRY_SPECS: readonly ColumnSpec[] = [
  { name: 'code', type: 'String' },
  { name: 'country', type: 'String' },
  { name: 'continent', type: 'String' },
  { name: 'capital', type: 'String' },
  { name: 'population_m', type: 'Float', kind: 'measure' },
];

/** What each order is joined to its country by: its ISO code, a key with one spelling each side (the Join… window
 *  suggests the column both name `country`; a code is the surer key). */
const ON = { rows: [{ left: 'country_code', op: '==', right: 'code' }] } as const;
const ALL_OF_MINE: JoinHow = { form: 'join', kind: 'left', on: ON, prefix: 'their_', keep: null };
const MINE_WITH_NONE: JoinHow = { form: 'anti', on: ON };

/** A sample's file as a cube records it -- `rows` of it, else its own number -- built here and hashed, so the page reopens
 *  over the same rows rebuilt. */
export async function sampleSource(id: string, columns: readonly ColumnSpec[], rows?: number): Promise<FileSource> {
  const sample = sampleById(id);
  if (!sample) throw new Error(`no sample '${id}'`);
  rows ??= sample.defaultRows;
  const bytes = new TextEncoder().encode(sample.build(rows));
  return fileSource({ name: sampleFileName(sample), size: bytes.length, arrayBuffer: async () => bytes.buffer as ArrayBuffer },
    'csv', columns, { id, rows });
}

/** A table's rows as they are: no grouping, the columns given. */
const flat = (table: string, columns: readonly ColumnSpec[], over: Partial<CubeSnapshot> = {}): CubeSnapshot => ({
  source: { query: element(table) }, columns, derived: [], rows: [], pivotOn: [], sorts: [], epoch: 1, ...over,
});

/** A cube written as the screen holds one it opens (cube-state.ts openedState): the corpus reopens it the same. */
function cubeOf(name: string, source: CubeDocument['source'], snapshot: CubeSnapshot, configuration: CubeConfiguration): CubeDocument {
  const state = openedState(snapshot, configuration, TreeState.fromPaths([], false));
  return writeCube({ name, source, snapshot: state.snapshot, configuration: state.configuration, tree: state.tree });
}

const MONEY = { format: { kind: 'currency', currency: 'USD', decimals: 0 } } as const;
const ORDERS: CubeConfiguration = { ...DEFAULT_CONFIGURATION, reportTitle: 'Orders', columns: { revenue: MONEY, cost: MONEY, profit: MONEY, target: MONEY } };

/** The page: the two tables; their join, grouped by continent, with a chart of it; and the orders with no country. */
export async function prepJoinsPage(): Promise<PageDocument> {
  const salesFile = await sampleSource(EXAMPLE_SAMPLE, SALES_SPECS, EXAMPLE_ROWS);
  const countriesFile = await sampleSource(COUNTRIES_SAMPLE, COUNTRY_SPECS);
  const orders = cubeOf('Orders', salesFile, flat(EXAMPLE_SAMPLE, SALES_SPECS), ORDERS);
  const countries = cubeOf('Countries', countriesFile, flat(COUNTRIES_SAMPLE, COUNTRY_SPECS), { ...DEFAULT_CONFIGURATION, reportTitle: 'Countries' });
  // each input a grid as it was joined: its name, its source, its saved query (cube-document.ts JoinInput)
  const mine: JoinInput = { name: 'Orders', source: salesFile, query: orders.query };
  const theirs: JoinInput = { name: 'Countries', source: countriesFile, query: countries.query };
  // their columns as the join names them: a name mine has (country) prefixed
  const named = theirNames(SALES_SPECS.map((c) => c.name), COUNTRY_SPECS.map((c) => c.name), 'their_');
  const joinedColumns = [...SALES_SPECS, ...COUNTRY_SPECS.map((c) => ({ ...c, name: named.get(c.name)! }))];
  const join = (how: JoinHow, columns: readonly ColumnSpec[]): JoinSource => ({
    _type: 'join', name: joinTitle(how, mine.name, theirs.name), left: mine, right: theirs, how,
    columns: columns.map((c) => ({ name: c.name, type: c.type })),
  });
  const joinedSource = join(ALL_OF_MINE, joinedColumns);
  const unmatchedSource = join(MINE_WITH_NONE, SALES_SPECS);
  // an order's country's population is its country's, not a sum over its orders: no measure here
  const perOrder = joinedColumns.map((c) => (c.name === 'population_m' ? { ...c, kind: 'dimension' as const } : c));
  // what an order is, beside what the join brought: its other columns hidden (Columns puts any back)
  const shown = new Set(['country_code', 'country', 'code', 'continent', 'capital', 'population_m', 'units', 'revenue', 'profit']);
  const columns = { ...ORDERS.columns };
  for (const c of perOrder) if (!shown.has(c.name)) columns[c.name] = { ...columns[c.name], hidden: true };
  const joined = cubeOf(joinedSource.name, joinedSource,
    flat('joined', perOrder, { rows: ['continent', 'their_country'], sorts: [{ column: 'revenue', direction: 'desc' }] }),
    { ...ORDERS, reportTitle: 'Orders by continent', columns });
  const unmatched = cubeOf(unmatchedSource.name, unmatchedSource,
    flat('unmatched', SALES_SPECS, { rows: ['country'], sorts: [{ column: 'revenue', direction: 'desc' }] }),
    { ...ORDERS, reportTitle: 'Orders whose country the table lacks' });
  const views: PageView[] = [
    { id: 'orders', kind: 'grid', cube: 'orders', title: 'Orders' },
    { id: 'countries', kind: 'grid', cube: 'countries', title: 'Countries' },
    { id: 'joined', kind: 'grid', cube: 'joined', title: `${joinedSource.name}: all of mine, their country beside each` },
    { ...chart('by-continent', 'Revenue by continent (the unnamed bar: orders whose country the table lacks)',
      { x: 'continent', y: [REVENUE], options: { labels: true } }), cube: 'joined' },
    { id: 'unmatched', kind: 'grid', cube: 'unmatched', title: `${unmatchedSource.name}: mine with none` },
  ];
  const sheets: PageSheet[] = [
    { id: 'sheet-1', name: 'Two tables', layout: { kind: 'bands', fit: true, bands: [
      { height: 0.5, node: { tile: 'orders' } }, { height: 0.5, node: { tile: 'countries' } }] } },
    { id: 'sheet-2', name: 'Joined', layout: { kind: 'bands', fit: true, bands: [
      { height: 0.5, node: { tile: 'joined' } }, { height: 0.5, node: { tile: 'by-continent' } }] } },
    { id: 'sheet-3', name: 'No match', layout: { kind: 'bands', fit: true, bands: [{ height: 1, node: { tile: 'unmatched' } }] } },
  ];
  return writePageOf({
    name: 'Joining tables',
    cubes: [{ id: 'orders', cube: orders }, { id: 'countries', cube: countries }, { id: 'joined', cube: joined }, { id: 'unmatched', cube: unmatched }],
    views: { views, sheets },
  });
}
