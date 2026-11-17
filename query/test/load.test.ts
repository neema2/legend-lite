// The form's round trip: a query built, printed as Pure, parsed by legend-lite, and read back is
// the same query. And what the form cannot show is said, never guessed.

import { strict as assert } from 'node:assert';
import { describe, it } from 'node:test';
import { buildLambda } from '../src/builder/build.ts';
import { loadLambda } from '../src/builder/load.ts';
import { emptyQuery, type FilterNode, type QueryState } from '../src/builder/state.ts';
import { demoModel, grammar } from './lite.ts';

const { graph } = await demoModel();

const SOURCE = {
  kind: 'class', class: 'demo::trading::Trade', mapping: 'demo::trading::TradingMapping', runtime: 'demo::trading::Runtime',
} as const;

/** The query with its generated ids blanked, for comparison. */
function shape(q: QueryState): unknown {
  const node = (n: FilterNode): unknown => (n.kind === 'group' ? { ...n, id: '', children: n.children.map(node) } : { ...n, id: '' });
  return {
    ...q,
    columns: q.columns.map((c) => ({ ...c, id: '' })),
    ...(q.filter ? { filter: node(q.filter) } : {}),
    ...(q.postFilter ? { postFilter: node(q.postFilter) } : {}),
    ...(q.windows ? { windows: q.windows.map((w) => ({ ...w, id: '' })) } : {}),
  };
}

async function roundTrip(q: QueryState): Promise<void> {
  for (const withFrom of [true, false]) {
    const text = await grammar.lambdaText(buildLambda(graph, q, { withFrom }), 'PRETTY');
    const parsed = await grammar.lambdaJson(text);
    const back = loadLambda(graph, parsed, withFrom ? undefined : { mapping: q.source.mapping, runtime: q.source.runtime });
    assert.ok(back.ok, back.ok ? '' : `${back.reason}\n${text}`);
    assert.deepEqual(shape(back.query), shape(q), text);
  }
}

const col = (name: string, ...path: string[]) => ({ id: name, name, path: path.map((property) => ({ property })) });

describe('load', () => {
  it('reads back columns, a nested filter with exists, parameters, groupBy and every option', async () => {
    await roundTrip({
      ...emptyQuery(SOURCE),
      parameters: [{ name: 'minQty', type: 'Integer', multiplicity: { lowerBound: 1, upperBound: 1 } }],
      columns: [col('Ticker', 'product', 'ticker'), { ...col('Quantity (sum)', 'quantity'), aggregate: 'sum' }, { ...col('Trades', 'tradeId'), aggregate: 'distinctCount' }],
      filter: {
        kind: 'group', id: 'g', op: 'or', children: [
          { kind: 'condition', id: 'a', path: [{ property: 'side' }], operator: 'notEqual', value: { kind: 'enum', enumeration: 'demo::trading::Side', value: 'SELL' } },
          {
            kind: 'group', id: 'g2', op: 'and', children: [
              { kind: 'condition', id: 'b', path: [{ property: 'quantity' }], operator: 'greaterThanEqual', value: { kind: 'variable', name: 'minQty' } },
              { kind: 'condition', id: 'c', path: [{ property: 'trader' }, { property: 'trades' }, { property: 'status' }], operator: 'notIn', value: { kind: 'list', values: [{ kind: 'string', value: 'REJECTED' }] } },
              { kind: 'condition', id: 'd', path: [{ property: 'tradeDate' }], operator: 'lessThan', value: { kind: 'dateFunction', function: { kind: 'adjust', from: 'today', amount: -7, unit: 'DAYS' } } },
              { kind: 'condition', id: 'e', path: [{ property: 'trader' }, { property: 'title' }], operator: 'isNotEmpty' },
              { kind: 'condition', id: 'f', path: [{ property: 'price' }], operator: 'lessThan', value: { kind: 'float', value: '-1.5' } },
            ],
          },
        ],
      },
      options: { distinct: true, sort: [{ column: 'Quantity (sum)', direction: 'desc' }, { column: 'Ticker', direction: 'asc' }], limit: 100, slice: { start: 2, end: 10 } },
    });
  });

  it('reads back calculated columns, window columns and a post-filter', async () => {
    const derivation = await grammar.lambdaJson('x|$x.quantity->toFloat() * $x.price');
    await roundTrip({
      ...emptyQuery(SOURCE),
      columns: [col('Ticker', 'product', 'ticker'), col('Quantity', 'quantity'), { id: 'n', name: 'Notional', path: [], derivation }],
      windows: [
        { id: 'w1', name: 'Rank', op: 'rank', partition: ['Ticker'], sort: { column: 'Quantity', direction: 'desc' } },
        { id: 'w2', name: 'Ticker Total', op: 'sum', column: 'Quantity', partition: ['Ticker'] },
        { id: 'w3', name: 'Running', op: 'sum', column: 'Quantity', partition: [], sort: { column: 'Quantity', direction: 'asc' } },
      ],
      postFilter: { kind: 'group', id: 'p', op: 'and', children: [
        { kind: 'condition', id: 'pc', path: [{ property: 'Rank' }], operator: 'lessThanEqual', value: { kind: 'integer', value: '2' } },
      ] },
      options: { distinct: false, sort: [{ column: 'Ticker', direction: 'asc' }] },
    });
  });

  it('reads back a graph fetch, checked or not', async () => {
    for (const checked of [false, true]) {
      await roundTrip({
        ...emptyQuery(SOURCE),
        graph: { checked, tree: [{ property: 'tradeId', children: [] }, { property: 'product', children: [{ property: 'ticker', children: [] }] }] },
        filter: { kind: 'group', id: 'g', op: 'and', children: [
          { kind: 'condition', id: 'c', path: [{ property: 'status' }], operator: 'equal', value: { kind: 'string', value: 'EXECUTED' } },
        ] },
      });
    }
  });

  it('aggregates every column with aggregate(), which legend-engine runs (groupBy with no key it cannot)', async () => {
    await roundTrip({
      ...emptyQuery(SOURCE),
      columns: [{ ...col('Trades', 'tradeId'), aggregate: 'count' }, { ...col('Quantity', 'quantity'), aggregate: 'sum' }],
    });
  });

  it('reads back constants: simple ones as values, any other expression as calculated', async () => {
    await roundTrip({
      ...emptyQuery(SOURCE),
      columns: [col('id', 'tradeId')],
      parameters: [{ name: 'min', type: 'Integer', multiplicity: { lowerBound: 1, upperBound: 1 } }],
      constants: [
        { name: 'tickers', type: 'String', value: { kind: 'list', values: [{ kind: 'string', value: 'AAPL' }, { kind: 'string', value: 'MSFT' }] } },
        { name: 'from', type: 'StrictDate', value: { kind: 'dateFunction', function: { kind: 'adjust', from: 'today', amount: -10, unit: 'YEARS' } } },
        { name: 'twice', calculated: (await grammar.lambdaJson('|$min * 2')).body[0]! },
      ],
      filter: { kind: 'group', id: '', op: 'and', children: [
        { kind: 'condition', id: '', path: [{ property: 'product' }, { property: 'ticker' }], operator: 'in', value: { kind: 'variable', name: 'tickers' } },
        { kind: 'condition', id: '', path: [{ property: 'tradeDate' }], operator: 'greaterThan', value: { kind: 'variable', name: 'from' } },
        { kind: 'condition', id: '', path: [{ property: 'quantity' }], operator: 'greaterThan', value: { kind: 'variable', name: 'twice' } },
      ] },
    });
  });

  it('reads back milestoning: dates on the class (a parameter or a literal), every version, dates on a property', async () => {
    const date = { name: 'businessDate', type: 'Date', multiplicity: { lowerBound: 1, upperBound: 1 } } as const;
    const ratings = { ...emptyQuery({ ...SOURCE, class: 'demo::trading::FirmRating' }), columns: [col('grade', 'grade')], parameters: [date] };
    await roundTrip({ ...ratings, milestoning: { kind: 'asOf', dates: { businessDate: { kind: 'variable', name: 'businessDate' } } } });
    await roundTrip({ ...ratings, parameters: [], milestoning: { kind: 'asOf', dates: { businessDate: { kind: 'strictDate', value: '2024-01-15' } } } });
    await roundTrip({ ...ratings, parameters: [], milestoning: { kind: 'allVersions' } });
    await roundTrip({
      ...emptyQuery({ ...SOURCE, class: 'demo::trading::ProductLimit' }), columns: [col('max', 'maxQuantity')],
      milestoning: { kind: 'asOf', dates: { processingDate: { kind: 'variable', name: 'asOf' }, businessDate: { kind: 'strictDate', value: '2024-06-01' } } },
      constants: [{ name: 'asOf', type: 'StrictDate', value: { kind: 'dateFunction', function: { kind: 'today' } } }],
    });
    await roundTrip({
      ...emptyQuery({ ...SOURCE, class: 'demo::trading::Firm' }), parameters: [date],
      columns: [{ id: '', name: 'grade', path: [{ property: 'rating', args: [{ kind: 'variable', name: 'businessDate' }] }, { property: 'grade' }] }],
    });
    // a temporal class's all() takes its dates
    const r = loadLambda(graph, await grammar.lambdaJson('|demo::trading::FirmRating.all()->project(~[g:x|$x.grade])'), { mapping: 'm', runtime: 'r' });
    assert.equal(r.ok, false);
    if (!r.ok) assert.match(r.reason, /0 date\(s\) where its milestoning takes 1/);
  });

  it("reads back a derived property's arguments, in a column and a condition", async () => {
    await roundTrip({
      ...emptyQuery(SOURCE),
      columns: [col('id', 'tradeId'), { id: '', name: 'n', path: [{ property: 'notionalAt', args: [{ kind: 'float', value: '1.1' }] }] }],
      parameters: [{ name: 'size', type: 'Integer', multiplicity: { lowerBound: 1, upperBound: 1 } }],
      filter: { kind: 'group', id: '', op: 'and', children: [
        { kind: 'condition', id: '', path: [{ property: 'isAtLeast', args: [{ kind: 'variable', name: 'size' }] }], operator: 'equal', value: { kind: 'boolean', value: true } },
      ] },
    });
  });

  it('reads back percentile settings and a weighted average with its weight', async () => {
    await roundTrip({
      ...emptyQuery(SOURCE),
      columns: [
        col('side', 'side'),
        { ...col('median', 'price'), aggregate: 'percentile', percentile: { value: 50, ascending: true, continuous: true } },
        { ...col('p12.5', 'price'), aggregate: 'percentile', percentile: { value: 12.5, ascending: false, continuous: true } },
        { ...col('vwap', 'price'), aggregate: 'wavg', weight: 'qty' },
        col('qty', 'quantity'),
      ],
    });
    // no keys: aggregate()
    await roundTrip({
      ...emptyQuery(SOURCE),
      columns: [{ ...col('vwap', 'price'), aggregate: 'wavg', weight: 'qty' }, col('qty', 'quantity')],
    });
  });

  it('refuses a wavg without its row mapper, and a row mapper without wavg', async () => {
    const base = '|demo::trading::Trade.all()->project(~[p:x|$x.price, q:x|$x.quantity])->groupBy(~[q], ~[';
    for (const agg of ['p:x|$x.p:y|$y->wavg()]', 'p:x|wavgRowMapper($x.p, $x.q):y|$y->sum()]']) {
      const r = loadLambda(graph, await grammar.lambdaJson(`${base}${agg})`), { mapping: 'm', runtime: 'r' });
      assert.equal(r.ok, false, agg);
      if (!r.ok) assert.match(r.reason, /go together/, agg);
    }
  });

  it("opens the data space's curated queries in the form", async () => {
    const ds = graph.dataSpaces.get('demo::trading::TradingDataSpace')!;
    for (const e of ds.executables ?? []) {
      if (e._type !== 'dataSpaceTemplateExecutable') continue;
      const r = loadLambda(graph, e.query, { mapping: 'demo::trading::TradingMapping', runtime: 'demo::trading::Runtime' });
      assert.ok(r.ok, r.ok ? '' : `${e.title}: ${r.reason}`);
    }
  });

  it('keeps what it cannot show as text, saying why', async () => {
    for (const [text, why] of [
      ['|demo::trading::Trade.all()->project([x|$x.tradeId], [\'id\'])', /project\(~\[/],
      ['{|1; demo::trading::Trade.all()->project(~[id:x|$x.tradeId]);}', /a statement other than let/],
      ['|demo::trading::Trade.all()->project(~[id:x|$x.tradeId])->extend(~b:x|$x.id + 1)', /extend\(\)/],
    ] as const) {
      const r = loadLambda(graph, await grammar.lambdaJson(text), { mapping: 'm', runtime: 'r' });
      assert.equal(r.ok, false, text);
      if (!r.ok) assert.match(r.reason, why, text);
    }
  });
});
