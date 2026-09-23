// The operations this cube can emit, as data.
//
// Shared because two harnesses ask different questions of the same
// list: `verify-engine.mjs` asks whether a real legend-engine COMPILES
// each one, and `verify-engine-differential.mjs` asks whether the
// engine and DuckDB-WASM AGREE on the rows it returns. Copying the
// list would let the two drift, and then a case dropped from one would
// look like a pass in the other.
//
// The SOURCE is a parameter because the same data lives behind
// different stores per plane: a DuckDB table on the local plane, an H2
// schema on the engine plane.

/** Every case, over `source`. */
export function casesFor(SOURCE) {
  const COLUMNS = [
    { name: 'region', type: 'String' },
    { name: 'desk', type: 'String' },
    { name: 'book', type: 'String' },
    { name: 'year', type: 'Integer', kind: 'dimension' },
    { name: 'qtr', type: 'String' },
    { name: 'notional', type: 'Float' },
    { name: 'pnl', type: 'Float' },
    { name: 'qty', type: 'Integer' },
  ];

  const BASE = {
    source: { expression: SOURCE },
    columns: COLUMNS,
    derived: [],
    rows: [],
    pivotOn: [],
    measures: [],
    sorts: [],
    epoch: 1,
  };

  const cube = (over = {}) => ({ ...BASE, ...over });
  const SUM = { name: 'notional', column: 'notional', fn: 'sum' };

  /** A filter case: one operator, with a value that suits it. */
  const filterCase = (operator, extra = {}) => ({
    name: `filter: ${operator}`,
    snapshot: cube({
      measures: [SUM],
      rows: ['region'],
      filter: { kind: 'condition', column: 'region', operator, ...extra },
    }),
  });

  const AGGREGATES = ['sum', 'count', 'average', 'min', 'max', 'median',
    'stdDevSample', 'stdDevPopulation', 'varianceSample',
    'variancePopulation', 'joinStrings', 'wavg', 'unique'];

  const CASES = [
    // -- the shapes ----------------------------------------------------
    { name: 'detail rows', snapshot: cube({}) },
    {
      name: 'sort ascending',
      snapshot: cube({ sorts: [{ column: 'region', direction: 'asc' }] }),
    },
    {
      name: 'sort descending, two columns',
      snapshot: cube({
        sorts: [
          { column: 'region', direction: 'desc' },
          { column: 'desk', direction: 'asc' },
        ],
      }),
    },
    {
      name: 'group by one dimension',
      snapshot: cube({ rows: ['region'], measures: [SUM] }),
    },
    {
      name: 'group by three dimensions',
      snapshot: cube({ rows: ['region', 'desk', 'book'], measures: [SUM] }),
    },
    {
      name: 'group by with NO measures',
      snapshot: cube({ rows: ['region'] }),
    },
    {
      name: 'the grand total (no keys)',
      snapshot: cube({ measures: [SUM] }),
    },
    {
      name: 'a level scope, with parent conditions',
      snapshot: cube({ rows: ['region', 'desk'], measures: [SUM] }),
      scope: { level: 2, parent: ['EMEA'] },
    },
    {
      name: 'column pivot',
      snapshot: cube({ pivotOn: ['year'], measures: [SUM] }),
    },
    {
      name: 'pivot AND group by, through the cast',
      snapshot: cube({
        rows: ['region'],
        pivotOn: ['year'],
        measures: [SUM],
        pivotCast: [
          { name: '2021__|__notional', measure: 'notional' },
          { name: '2022__|__notional', measure: 'notional' },
        ],
      }),
    },
    {
      name: 'a derived column',
      snapshot: cube({
        rows: ['region'],
        measures: [SUM],
        derived: [{ name: 'big', expression: '$x.notional > 100' }],
      }),
    },
    {
      name: 'a row window (offset and limit)',
      snapshot: cube({ window: { offset: 10, limit: 20 } }),
    },
    {
      name: 'a string value with a quote in it',
      snapshot: cube({
        rows: ['region'],
        measures: [SUM],
        filter: {
          kind: 'condition', column: 'desk', operator: 'equal',
          value: "O'Brien's desk",
        },
      }),
    },
    {
      name: 'and / or / not, nested',
      snapshot: cube({
        rows: ['region'],
        measures: [SUM],
        filter: {
          kind: 'and',
          children: [
            {
              kind: 'or',
              children: [
                { kind: 'condition', column: 'region', operator: 'equal',
                  value: 'EMEA' },
                { kind: 'condition', column: 'region', operator: 'equal',
                  value: 'AMER' },
              ],
            },
            {
              kind: 'not',
              child: { kind: 'condition', column: 'qty', operator: 'lessThan',
                value: 10 },
            },
          ],
        },
      }),
    },
    // -- every aggregate ----------------------------------------------
    ...AGGREGATES.map((fn) => ({
      name: `aggregate: ${fn}`,
      snapshot: cube({
        rows: ['region'],
        measures: [{
          name: 'agg',
          column: fn === 'joinStrings' || fn === 'unique' ? 'desk' : 'notional',
          fn,
          ...(fn === 'wavg' ? { weight: 'qty' } : {}),
        }],
      }),
    })),
    // -- every filter operator ----------------------------------------
    filterCase('equal', { value: 'EMEA' }),
    filterCase('notEqual', { value: 'EMEA' }),
    filterCase('lessThan', { value: 'EMEA' }),
    filterCase('lessThanEqual', { value: 'EMEA' }),
    filterCase('greaterThan', { value: 'EMEA' }),
    filterCase('greaterThanEqual', { value: 'EMEA' }),
    filterCase('isEmpty'),
    filterCase('isNotEmpty'),
    filterCase('contains', { value: 'EM' }),
    filterCase('notContains', { value: 'EM' }),
    filterCase('startsWith', { value: 'E' }),
    filterCase('notStartsWith', { value: 'E' }),
    filterCase('endsWith', { value: 'A' }),
    filterCase('notEndsWith', { value: 'A' }),
    filterCase('in', { value: ['EMEA', 'AMER'] }),
    filterCase('notIn', { value: ['EMEA', 'AMER'] }),
    filterCase('equalCaseInsensitive', { value: 'emea' }),
    filterCase('notEqualCaseInsensitive', { value: 'emea' }),
    filterCase('containsCaseInsensitive', { value: 'em' }),
    filterCase('startsWithCaseInsensitive', { value: 'e' }),
    filterCase('endsWithCaseInsensitive', { value: 'a' }),
    filterCase('inCaseInsensitive', { value: ['emea', 'amer'] }),
    filterCase('notInCaseInsensitive', { value: ['emea', 'amer'] }),
    filterCase('equalColumn', { rightColumn: 'desk' }),
    filterCase('equalCaseInsensitiveColumn', { rightColumn: 'desk' }),
    filterCase('notEqualColumn', { rightColumn: 'desk' }),
    filterCase('notEqualCaseInsensitiveColumn', { rightColumn: 'desk' }),
    filterCase('lessThanColumn', { rightColumn: 'desk' }),
    filterCase('lessThanEqualColumn', { rightColumn: 'desk' }),
    filterCase('greaterThanColumn', { rightColumn: 'desk' }),
    filterCase('greaterThanEqualColumn', { rightColumn: 'desk' }),
  ];
  return CASES;
}

/** The table as the cube sees it, on either plane. */
export const ENGINE_COLUMNS = [
  { name: 'region', type: 'String' },
  { name: 'desk', type: 'String' },
  { name: 'book', type: 'String' },
  { name: 'year', type: 'Integer', kind: 'dimension' },
  { name: 'qtr', type: 'String' },
  { name: 'notional', type: 'Float' },
  { name: 'pnl', type: 'Float' },
  { name: 'qty', type: 'Integer' },
];
