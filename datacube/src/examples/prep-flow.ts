// THE FLOW EXAMPLE (docs/DATACUBE_FLOW_MODE_2026_10_24.md: a flow sheet to open, rather than one to build first): the
// sales sample's orders and its countries, prepared on a canvas as Alteryx prepares them -- two Inputs in a container,
// the big orders filtered and joined live to their countries and a margin calculated in another, the result summarized
// by continent and written out as a CSV by an Output, the top order of each country kept by a Unique, and a comment
// saying where to start. Built by DataCube's own writers (cube-document.ts writeCube, join.ts theirNames, unique.ts's
// shape), as the other example pages are; it opens as a saved page does, each sample rebuilt and each box a document
// until it is shown. A second sheet, Reshape, shows the tools that reshape rows: the top orders of each region kept by a
// Sample, chosen and renamed by a Select, their money made rows by a Transpose and summed by measure -- each made by its
// tool's own rule (row-sample.ts, select-columns.ts, transpose.ts), applied as the screen applies it (grid-commands.ts).
// A third, Match, shows what a join leaves and finds: the big orders no country is found for (the Join's L) and how many
// (Count Records), the countries no big order is of (its R), and every order's country as the countries name it (a Find
// Replace) -- count-records.ts's and find-replace.ts's rules. A fourth, Checks, takes the orders apart by region and puts
// them together again (a Union of three), keeps 2024's (a Date Filter) and checks every order (a Test: its id unique, its
// country never empty, its discount at most 15% -- the one a run finds failing): checks.ts's rule, the Date Filter's two
// days as its window writes them. test/examples.test.ts reads it back.

import { element, fn, lambda, property, variable } from '../../../pure-protocol/src/index.ts';
import { DEFAULT_CONFIGURATION, type CubeConfiguration } from '../config.ts';
import { writeCube, type CubeDocument, type CubeSource, type JoinSource, type RowsSource } from '../cube-document.ts';
import { openedState } from '../cube-state.ts';
import { theirNames, type JoinHow } from '../join.ts';
import type { FlowBox } from '../layout/flow.ts';
import { writePageOf, type PageDocument, type PageView } from '../page-document.ts';
import { COUNTRIES_SAMPLE } from '../samples.ts';
import type { ColumnSpec, CubeSnapshot, DerivedColumn } from '../snapshot.ts';
import { TreeState } from '../tree.ts';
import { uniqueCommands } from '../unique.ts';
import { applyGridCommands, type GridCommand } from '../grid-commands.ts';
import { rowSampleCommands } from '../row-sample.ts';
import { selectCommands } from '../select-columns.ts';
import { transposeCommands } from '../transpose.ts';
import { countRecordsCommands } from '../count-records.ts';
import { findReplaceCommands } from '../find-replace.ts';
import { checkCommands } from '../checks.ts';
import { EXAMPLE_ROWS, EXAMPLE_SAMPLE, SALES_SPECS } from './charts-dashboard.ts';
import { COUNTRY_SPECS, sampleSource } from './prep-joins.ts';

/** A box's rows as they come: no grouping, the columns given, its query's own steps over it. */
const flat = (name: string, columns: readonly ColumnSpec[], over: Partial<CubeSnapshot> = {}): CubeSnapshot => ({
  source: { query: element(name) }, columns, derived: [], rows: [], pivotOn: [], sorts: [], epoch: 1, ...over,
});

/** A box written as the screen holds one it opens (cube-state.ts openedState). */
function box(name: string, source: CubeSource, snapshot: CubeSnapshot, configuration: CubeConfiguration = DEFAULT_CONFIGURATION): CubeDocument {
  const state = openedState(snapshot, configuration, TreeState.fromPaths([], false));
  return writeCube({ name, source, snapshot: state.snapshot, configuration: state.configuration, tree: state.tree });
}

/** A box set by its tool's own commands (`commands`, made of the snapshot), applied as the screen applies them. */
function stepped(name: string, source: CubeSource, snapshot: CubeSnapshot, configuration: CubeConfiguration,
  commands: (s: CubeSnapshot) => GridCommand[]): CubeDocument {
  const draft = applyGridCommands({ snapshot, config: configuration }, commands(snapshot));
  return box(name, source, draft.snapshot, draft.config);
}

/** A box over another box's rows, read live: its input the grid of that id on the page. */
const rowsOf = (grid: string, name: string, columns: readonly ColumnSpec[]): RowsSource => ({
  _type: 'rows', name, input: { grid }, columns: columns.map((c) => ({ name: c.name, type: c.type })),
});

const x = variable('x');
/** Each order's margin: its profit over its revenue, row by row (a Formula box). */
const MARGIN: DerivedColumn = {
  name: 'margin', lambda: lambda(['x'], fn('divide', fn('toOne', property(x, 'profit')), fn('toOne', property(x, 'revenue')))),
  type: 'Float', kind: 'dimension',
};
/** Each order joined to its country by its ISO code, every order kept (a Join box, read live). */
const BY_CODE: JoinHow = { form: 'join', kind: 'left', on: { rows: [{ left: 'country_code', op: '==', right: 'code' }] }, prefix: 'their_', keep: null };

const MONEY = { format: { kind: 'currency', currency: 'USD', decimals: 0 } } as const;
const MONEYED: CubeConfiguration = { ...DEFAULT_CONFIGURATION, columns: { revenue: MONEY, cost: MONEY, profit: MONEY, target: MONEY } };

/** The page: one flow sheet, its boxes and its notes where a person would lay them out. */
export async function prepFlowPage(): Promise<PageDocument> {
  const salesFile = await sampleSource(EXAMPLE_SAMPLE, SALES_SPECS, EXAMPLE_ROWS);
  const countriesFile = await sampleSource(COUNTRIES_SAMPLE, COUNTRY_SPECS);
  const named = theirNames(SALES_SPECS.map((c) => c.name), COUNTRY_SPECS.map((c) => c.name), 'their_');
  const joined: readonly ColumnSpec[] = [...SALES_SPECS, ...COUNTRY_SPECS.map((c) => ({ ...c, name: named.get(c.name)! }))];
  const withMargin: readonly ColumnSpec[] = [...joined, { name: 'margin', type: 'Float', kind: 'dimension' }];
  const SUMMED = ['continent', 'units', 'revenue', 'cost', 'profit'];
  const summed = withMargin.filter((c) => SUMMED.includes(c.name));
  // a country's population is its country's, never summed over its orders
  const perOrder = joined.map((c) => (c.name === 'population_m' ? { ...c, kind: 'dimension' as const } : c));
  const join: JoinSource = {
    _type: 'join', name: 'Orders with their country', left: { grid: 'grid-3' }, right: { grid: 'grid-2' }, how: BY_CODE,
    columns: joined.map((c) => ({ name: c.name, type: c.type })),
  };
  const cubes: { id: string; cube: CubeDocument }[] = [
    { id: 'grid-1', cube: box('Orders', salesFile, flat(EXAMPLE_SAMPLE, SALES_SPECS), MONEYED) },
    { id: 'grid-2', cube: box('Countries', countriesFile, flat(COUNTRIES_SAMPLE, COUNTRY_SPECS)) },
    // the orders over $1,000
    { id: 'grid-3', cube: box('Big orders', rowsOf('grid-1', 'Orders', SALES_SPECS), flat('orders', SALES_SPECS, {
      filter: { kind: 'condition', column: 'revenue', operator: 'greaterThan', value: 1000 },
    }), MONEYED) },
    { id: 'grid-4', cube: box(join.name, join, flat('joined', perOrder), MONEYED) },
    { id: 'grid-5', cube: box('Margin', rowsOf('grid-4', join.name, perOrder), flat('margin', perOrder, { derived: [MARGIN] }), MONEYED) },
    // a row a continent, its money summed -- the other columns left out, so the Output after it writes what it shows
    { id: 'grid-6', cube: box('By continent', rowsOf('grid-5', 'Margin', withMargin), flat('by continent', withMargin, {
      rows: ['continent'], sorts: [{ column: 'revenue', direction: 'desc' }],
    }), { ...MONEYED, columns: { ...MONEYED.columns, ...Object.fromEntries(withMargin
      .filter((c) => !SUMMED.includes(c.name)).map((c) => [c.name, { hidden: true }])) } }) },
    // what the summary shows, its Output's input: the continent and its sums
    { id: 'grid-7', cube: box('Revenue by continent', rowsOf('grid-6', 'By continent', summed), flat('written', summed), MONEYED) },
    // the top order of each country: a row number per country by revenue, its first kept, the number hidden (Unique)
    { id: 'grid-8', cube: stepped('Top order of each country', rowsOf('grid-1', 'Orders', SALES_SPECS), flat('top', SALES_SPECS), MONEYED,
      (s) => uniqueCommands(s, { keys: ['country'], by: { column: 'revenue', direction: 'desc' } })) },
  ];
  // RESHAPE: the top two orders of each region by revenue, chosen and renamed, their money made rows and summed by measure
  const chosen: readonly ColumnSpec[] = [
    { name: 'order_id', type: 'Integer', kind: 'dimension' }, { name: 'region', type: 'String' },
    { name: 'sales', type: 'Float', kind: 'measure' }, { name: 'cost', type: 'Float', kind: 'measure' }, { name: 'profit', type: 'Float', kind: 'measure' },
  ];
  const turned: readonly ColumnSpec[] = [
    { name: 'order_id', type: 'Integer', kind: 'dimension' }, { name: 'region', type: 'String' },
    { name: 'measure', type: 'String' }, { name: 'amount', type: 'Float', kind: 'measure' },
  ];
  cubes.push(
    { id: 'grid-9', cube: stepped('Top orders of each region', rowsOf('grid-1', 'Orders', SALES_SPECS), flat('top orders', SALES_SPECS), MONEYED,
      (s) => rowSampleCommands(s, { kind: 'last', n: 2, by: 'revenue', groups: ['region'] })) },
    { id: 'grid-10', cube: stepped('Chosen', rowsOf('grid-9', 'Top orders of each region', SALES_SPECS), flat('chosen', SALES_SPECS), MONEYED,
      (s) => selectCommands(s, { keep: ['order_id', 'region', 'revenue', 'cost', 'profit'], rename: { revenue: 'sales' } })) },
    { id: 'grid-11', cube: stepped('Money to rows', rowsOf('grid-10', 'Chosen', chosen), flat('to rows', chosen), DEFAULT_CONFIGURATION,
      (s) => transposeCommands(s, { columns: ['sales', 'cost', 'profit'], keys: ['order_id', 'region'], name: 'measure', value: 'amount' })) },
    { id: 'grid-12', cube: box('By measure', rowsOf('grid-11', 'Money to rows', turned), flat('by measure', turned, {
      rows: ['measure'], sorts: [{ column: 'amount', direction: 'desc' }],
    }), { ...DEFAULT_CONFIGURATION, columns: { amount: MONEY } }) },
  );
  // MATCH: what the join leaves on each side, how many it leaves of the orders, and the orders' countries named as the
  // countries name them -- every order joined to its country by its code, their name alone brought, replacing its own
  const named2 = theirNames(SALES_SPECS.map((c) => c.name), ['country'], 'their_');
  const official: readonly ColumnSpec[] = [...SALES_SPECS, { name: named2.get('country')!, type: 'String' }];
  const byName: JoinSource = {
    _type: 'join', name: 'Orders, their country named', left: { grid: 'grid-1' }, right: { grid: 'grid-2' },
    how: { form: 'join', kind: 'left', on: { rows: [{ left: 'country_code', op: '==', right: 'code' }] }, prefix: 'their_', keep: ['country'] },
    columns: official.map((c) => ({ name: c.name, type: c.type })),
  };
  const port = (grid: string, side: 'left' | 'right', name: string, columns: readonly ColumnSpec[]): RowsSource => ({
    _type: 'rows', name, input: { grid, port: side }, columns: columns.map((c) => ({ name: c.name, type: c.type })),
  });
  cubes.push(
    { id: 'grid-13', cube: box('Big orders with no country', port('grid-4', 'left', 'Big orders', SALES_SPECS), flat('unmatched', SALES_SPECS), MONEYED) },
    { id: 'grid-14', cube: stepped('How many', rowsOf('grid-13', 'Big orders with no country', SALES_SPECS), flat('how many', SALES_SPECS),
      DEFAULT_CONFIGURATION, (s) => countRecordsCommands(s)) },
    { id: 'grid-15', cube: box('Countries with no big order', port('grid-4', 'right', 'Countries', COUNTRY_SPECS), flat('no order', COUNTRY_SPECS)) },
    { id: 'grid-16', cube: stepped('Countries as named', byName, flat('named', official), MONEYED,
      (s) => findReplaceCommands(s, { column: 'country', with: named2.get('country')! })) },
  );
  // CHECKS: the orders of each region, unioned again; those of 2024; each order checked
  const region = (id: string, name: string): { id: string; cube: CubeDocument } => ({ id, cube: box(`${name} orders`,
    rowsOf('grid-1', 'Orders', SALES_SPECS), flat(name, SALES_SPECS, { filter: { kind: 'condition', column: 'region', operator: 'equal', value: name } }), MONEYED) });
  const regions: JoinSource = {
    _type: 'join', name: 'Every region again', left: { grid: 'grid-17' }, right: { grid: 'grid-18' }, more: [{ grid: 'grid-19' }],
    how: { form: 'append' }, columns: SALES_SPECS.map((c) => ({ name: c.name, type: c.type })),
  };
  cubes.push(
    region('grid-17', 'EMEA'), region('grid-18', 'APAC'), region('grid-19', 'AMER'),
    { id: 'grid-20', cube: box(regions.name, regions, flat('every region', SALES_SPECS), MONEYED) },
    // the days of 2024, both kept: the Date Filter's two conditions, as its window writes them (date-ranges.ts)
    { id: 'grid-21', cube: box('Orders of 2024', rowsOf('grid-20', regions.name, SALES_SPECS), flat('2024', SALES_SPECS, { filter: { kind: 'and', children: [
      { kind: 'condition', column: 'sold', operator: 'greaterThanEqual', value: '2024-01-01' },
      { kind: 'condition', column: 'sold', operator: 'lessThanEqual', value: '2024-12-31' },
    ] } }), MONEYED) },
    { id: 'grid-22', cube: stepped('Orders checked', rowsOf('grid-20', regions.name, SALES_SPECS), flat('checked', SALES_SPECS), DEFAULT_CONFIGURATION,
      (s) => checkCommands(s, [{ kind: 'unique', column: 'order_id' }, { kind: 'notEmpty', column: 'country' },
        { kind: 'between', column: 'discount', min: 0, max: 0.15 }])) },
    // revenue by channel across the regions, its columns fixed to two of the three (FL2): its third said (FL3)
    { id: 'grid-23', cube: stepped('Revenue by channel', rowsOf('grid-20', regions.name, SALES_SPECS), flat('by channel', SALES_SPECS), DEFAULT_CONFIGURATION,
      () => [
        { kind: 'configure', patch: { columns: Object.fromEntries(['units', 'cost', 'profit', 'discount', 'target']
          .map((c) => [c, { excludedFromPivot: true, hidden: true }])) } },
        { kind: 'group', rows: ['channel'] },
        { kind: 'pivot', columns: ['region'] },
        { kind: 'pinPivot', values: ['AMER', 'EMEA'] },
      ]) },
  );
  const grids: PageView[] = cubes.map((c) => ({ id: c.id, kind: 'grid', cube: c.id, title: c.cube.name }));
  const notes: PageView[] = [
    { id: 'text-1', kind: 'text', text: 'Inputs' },
    { id: 'text-2', kind: 'text', text: 'Prepare' },
    { id: 'text-4', kind: 'text', text: 'Report' },
    { id: 'text-3', kind: 'text', text: '**Start here.** Click a box to see its rows below, and its settings beside. Drop a tool from the left onto a box to add a step after it; drag from a box\'s right edge to another box to link them; ▶ Run counts every box and writes the Output.' },
    { id: 'text-5', kind: 'text', text: '**Reshape.** The two biggest orders of each region (a Sample), their columns chosen and revenue renamed sales (a Select), their money made rows (a Transpose) and summed by measure.' },
    { id: 'text-7', kind: 'text', text: '**Checks.** The orders of each region, taken apart and put together again (a Union of three); those of 2024 (a Date Filter); and every order checked -- its id unique, its country never empty, its discount at most 15% (a Test). Run it: one check fails, and says how many orders fail it. Set the Test to stop a run, and run it again: 2024\'s orders are not written. Revenue by channel across the regions has its columns fixed to AMER and EMEA (a Cross Tab): its rows\' APAC is said, a value with no column of its own -- Load values again to add it.' },
    { id: 'text-6', kind: 'text', text: '**Match.** The big orders whose country code finds no country -- the Join\'s L -- and how many (Count Records); the countries no big order is of (its R); and each order\'s country named as the countries file names it (Find Replace).' },
  ];
  const reshape: FlowBox[] = [
    { tile: 'grid-9', x: 40, y: 60 },
    { tile: 'grid-10', x: 288, y: 60 },
    { tile: 'grid-11', x: 536, y: 60 },
    { tile: 'grid-12', x: 536, y: 210 },
    { tile: 'text-5', x: 40, y: 210 },
  ];
  const match: FlowBox[] = [
    { tile: 'grid-13', x: 40, y: 60 },
    { tile: 'grid-14', x: 288, y: 60 },
    { tile: 'grid-15', x: 40, y: 210 },
    { tile: 'grid-16', x: 288, y: 210 },
    { tile: 'text-6', x: 40, y: 360 },
  ];
  const checks: FlowBox[] = [
    { tile: 'grid-17', x: 40, y: 60 },
    { tile: 'grid-18', x: 40, y: 180 },
    { tile: 'grid-19', x: 40, y: 300 },
    { tile: 'grid-20', x: 288, y: 180 },
    { tile: 'grid-21', x: 536, y: 60, output: { format: 'csv', name: 'orders of 2024.csv' } },
    { tile: 'grid-23', x: 536, y: 180 },
    { tile: 'grid-22', x: 536, y: 300 },
    { tile: 'text-7', x: 40, y: 430 },
  ];
  // three columns wide, so the whole of it is in sight beside the settings: its inputs, its steps, its report
  const boxes: FlowBox[] = [
    { tile: 'text-1', x: 20, y: 30, frame: { w: 224, h: 290 } },
    { tile: 'text-2', x: 268, y: 30, frame: { w: 472, h: 290 } },
    { tile: 'text-4', x: 268, y: 340, frame: { w: 472, h: 140 } },
    { tile: 'grid-1', x: 40, y: 80 },
    { tile: 'grid-2', x: 40, y: 230 },
    { tile: 'grid-3', x: 288, y: 80 },
    { tile: 'grid-4', x: 536, y: 80 },
    { tile: 'grid-5', x: 536, y: 230 },
    { tile: 'grid-6', x: 288, y: 390 },
    { tile: 'grid-7', x: 536, y: 390, output: { format: 'csv', name: 'revenue by continent.csv' } },
    { tile: 'grid-8', x: 40, y: 390 },
    { tile: 'text-3', x: 20, y: 520 },
  ];
  return writePageOf({
    name: 'A data flow',
    cubes,
    views: { views: [...grids, ...notes], sheets: [
      { id: 'sheet-1', name: 'Sales prep', layout: { kind: 'flow', boxes } },
      { id: 'sheet-2', name: 'Reshape', layout: { kind: 'flow', boxes: reshape } },
      { id: 'sheet-3', name: 'Match', layout: { kind: 'flow', boxes: match } },
      { id: 'sheet-4', name: 'Checks', layout: { kind: 'flow', boxes: checks } },
    ] },
  });
}
