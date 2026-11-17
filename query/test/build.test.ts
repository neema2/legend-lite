// The builder's lambdas, checked by legend-lite's own compiler (the WASM planner): each prints
// as the Pure a person would write, and types against the demo model.

import { strict as assert } from 'node:assert';
import { describe, it } from 'node:test';
import { buildLambda, BuildError } from '../src/builder/build.ts';
import { addColumn } from '../src/app/actions.ts';
import { queryOn, stepFor } from '../src/builder/milestoning.ts';
import { emptyQuery, type QueryState } from '../src/builder/state.ts';
import { demoModel, grammar } from './lite.ts';

const { context, graph } = await demoModel();

const SOURCE = {
  kind: 'class', class: 'demo::trading::Trade', mapping: 'demo::trading::TradingMapping', runtime: 'demo::trading::Runtime',
} as const;

function trades(overrides: Partial<QueryState>): QueryState {
  return { ...emptyQuery(SOURCE), ...overrides };
}

const col = (name: string, ...path: string[]) => ({ id: name, name, path: path.map((property) => ({ property })) });

async function text(q: QueryState, withFrom = false): Promise<string> {
  return grammar.lambdaText(buildLambda(graph, q, { withFrom }), 'STANDARD');
}

async function columns(q: QueryState): Promise<string[]> {
  const t = await grammar.relationType(context, buildLambda(graph, q, { withFrom: true }));
  return t.columns.map((c) => `${c.name}:${c.genericType.rawType._type === 'packageableType' ? c.genericType.rawType.fullPath : '?'}`);
}

describe('build', () => {
  it('projects columns across to-one navigation and a derived property', async () => {
    const q = trades({ columns: [col('id', 'tradeId'), col('ticker', 'product', 'ticker'), col('trader', 'trader', 'fullName')] });
    assert.equal(await text(q),
      '|demo::trading::Trade.all()->project(~[id:x|$x.tradeId, ticker:x|$x.product.ticker, trader:x|$x.trader.fullName])');
    assert.deepEqual(await columns(q), ['id:Integer', 'ticker:String', 'trader:String']);
  });

  it('filters with an enum, a to-many exists and a parameter; appends from() for execution', async () => {
    const q = trades({
      columns: [col('id', 'tradeId')],
      parameters: [{ name: 'minQty', type: 'Integer', multiplicity: { lowerBound: 1, upperBound: 1 } }],
      filter: {
        kind: 'group', id: 'g', op: 'and', children: [
          { kind: 'condition', id: 'c1', path: [{ property: 'side' }], operator: 'equal', value: { kind: 'enum', enumeration: 'demo::trading::Side', value: 'BUY' } },
          { kind: 'condition', id: 'c2', path: [{ property: 'quantity' }], operator: 'greaterThan', value: { kind: 'variable', name: 'minQty' } },
          { kind: 'condition', id: 'c3', path: [{ property: 'trader' }, { property: 'trades' }, { property: 'status' }], operator: 'in', value: { kind: 'list', values: [{ kind: 'string', value: 'EXECUTED' }, { kind: 'string', value: 'SETTLED' }] } },
        ],
      },
    });
    assert.equal(await text(q, true),
      "minQty: Integer[1]|demo::trading::Trade.all()->filter(x|(($x.side == demo::trading::Side.BUY) && ($x.quantity > $minQty)) && $x.trader.trades->exists(x_1|$x_1.status->in(['EXECUTED', 'SETTLED'])))->project(~[id:x|$x.tradeId])->from(demo::trading::TradingMapping, demo::trading::Runtime)");
    // a parameterized lambda types as its body, as legend-engine types it
    assert.deepEqual(await columns(q), ['id:Integer']);
  });

  it('groups by the columns with no aggregate, then distinct, sort, limit and slice', async () => {
    const q = trades({
      columns: [col('ticker', 'product', 'ticker'), { ...col('qty', 'quantity'), aggregate: 'sum' }],
      options: { distinct: true, sort: [{ column: 'qty', direction: 'desc' }], limit: 10, slice: { start: 0, end: 5 } },
    });
    assert.equal(await text(q),
      '|demo::trading::Trade.all()->project(~[ticker:x|$x.product.ticker, qty:x|$x.quantity])->groupBy(~[ticker], ~[qty:x|$x.qty:y|$y->sum()])->distinct()->sort([~qty->descending()])->limit(10)->slice(0, 5)');
    assert.deepEqual(await columns(q), ['ticker:String', 'qty:Integer']);
  });

  it('milestoning: a temporal class as of its date parameters, every version, and dates written on a property into one', async () => {
    const on = (cls: string) => queryOn(graph, { ...SOURCE, class: `demo::trading::${cls}` });
    const rating = on('FirmRating');
    assert.deepEqual(rating.parameters, [{ name: 'businessDate', type: 'Date', multiplicity: { lowerBound: 1, upperBound: 1 } }]);
    const graded = { ...rating, columns: [col('grade', 'grade')] };
    assert.equal(await text(graded), 'businessDate: Date[1]|demo::trading::FirmRating.all($businessDate)->project(~[grade:x|$x.grade])');
    assert.deepEqual(await columns(graded), ['grade:String']);
    assert.equal(await text({ ...graded, milestoning: { kind: 'allVersions' } }), 'businessDate: Date[1]|demo::trading::FirmRating.allVersions()->project(~[grade:x|$x.grade])');
    const limits = { ...on('ProductLimit'), columns: [col('max', 'maxQuantity')] };
    assert.equal(await text(limits), '{processingDate: Date[1], businessDate: Date[1]|demo::trading::ProductLimit.all($processingDate, $businessDate)->project(~[max:x|$x.maxQuantity])}');
    // Firm has no dates: a property into FirmRating writes them, and the query gains the parameter
    const firms = on('Firm');
    const step = stepFor(graph, firms, 'demo::trading::Firm', [], graph.property('demo::trading::Firm', 'rating')!, () => assert.fail('no derived defaults'));
    assert.deepEqual(step, { property: 'rating', args: [{ kind: 'variable', name: 'businessDate' }] });
    const q = addColumn(firms, [step, { property: 'grade' }], false);
    assert.equal(await text(q), 'businessDate: Date[1]|demo::trading::Firm.all()->project(~[rating_grade:x|$x.rating($businessDate).grade])');
    assert.deepEqual(await columns(q), ['rating_grade:String']);
    assert.throws(() => buildLambda(graph, { ...graded, milestoning: { kind: 'asOf', dates: {} } }, { withFrom: false }), /business date/);
    assert.throws(() => buildLambda(graph, { ...graded, milestoning: { kind: 'asOf', dates: { businessDate: { kind: 'dateFunction', function: { kind: 'today' } } } } }, { withFrom: false }), /make a constant/);
  });

  it("passes a derived property's arguments: literals, a parameter and a constant", async () => {
    const q = trades({
      columns: [col('id', 'tradeId'), { id: 'n', name: 'n', path: [{ property: 'notionalAt', args: [{ kind: 'variable', name: 'fx' }] }] }],
      parameters: [{ name: 'size', type: 'Integer', multiplicity: { lowerBound: 1, upperBound: 1 } }],
      constants: [{ name: 'fx', type: 'Float', value: { kind: 'float', value: '1.1' } }],
      filter: { kind: 'group', id: 'g', op: 'and', children: [
        { kind: 'condition', id: 'c1', path: [{ property: 'isAtLeast', args: [{ kind: 'variable', name: 'size' }] }], operator: 'equal', value: { kind: 'boolean', value: true } },
        { kind: 'condition', id: 'c2', path: [{ property: 'notionalAt', args: [{ kind: 'float', value: '0.5' }] }], operator: 'greaterThan', value: { kind: 'float', value: '1000.0' } },
      ] },
    });
    assert.equal(await text(q),
      '{size: Integer[1]|\nlet fx = 1.1;\ndemo::trading::Trade.all()->filter(x|($x.isAtLeast($size) == true) && ($x.notionalAt(0.5) > 1000.0))->project(~[id:x|$x.tradeId, n:x|$x.notionalAt($fx)]);\n}');
    assert.deepEqual(await columns(q), ['id:Integer', 'n:Float']);
  });

  it('writes constants as lets ahead of the query, used as variables', async () => {
    const q = trades({
      columns: [col('id', 'tradeId'), { id: 'big', name: 'big', path: [], derivation: await grammar.lambdaJson('x|$x.quantity > $threshold') }],
      constants: [
        { name: 'threshold', type: 'Integer', value: { kind: 'integer', value: '1000000' } },
        { name: 'side', type: 'demo::trading::Side', value: { kind: 'enum', enumeration: 'demo::trading::Side', value: 'BUY' } },
        { name: 'cutoff', calculated: (await grammar.lambdaJson('|adjust(today(), -1, DurationUnit.YEARS)')).body[0]! },
      ],
      filter: { kind: 'group', id: 'g', op: 'and', children: [
        { kind: 'condition', id: 'c1', path: [{ property: 'side' }], operator: 'equal', value: { kind: 'variable', name: 'side' } },
        { kind: 'condition', id: 'c2', path: [{ property: 'tradeDate' }], operator: 'greaterThan', value: { kind: 'variable', name: 'cutoff' } },
      ] },
    });
    assert.equal(await text(q),
      "{|\nlet threshold = 1000000;\nlet side = demo::trading::Side.BUY;\nlet cutoff = today()->adjust(-1, DurationUnit.YEARS);\ndemo::trading::Trade.all()->filter(x|($x.side == $side) && ($x.tradeDate > $cutoff))->project(~[id:x|$x.tradeId, big:x|$x.quantity > $threshold]);\n}");
    assert.deepEqual(await columns(q), ['id:Integer', 'big:Boolean']);
  });

  it("writes percentile and wavg as upstream's query builder does, the weight consumed", async () => {
    const q = trades({
      columns: [
        col('side', 'side'),
        { ...col('median', 'price'), aggregate: 'percentile' },
        { ...col('p90', 'price'), aggregate: 'percentile', percentile: { value: 90, ascending: false, continuous: false } },
        { ...col('vwap', 'price'), aggregate: 'wavg', weight: 'qty' },
        col('qty', 'quantity'),
      ],
    });
    assert.equal(await text(q),
      '|demo::trading::Trade.all()->project(~[side:x|$x.side, median:x|$x.price, p90:x|$x.price, vwap:x|$x.price, qty:x|$x.quantity])->groupBy(~[side], ~[median:x|$x.median:y|$y->percentile(0.5), p90:x|$x.p90:y|$y->percentile(0.9, false, false), vwap:x|$x.vwap->wavgRowMapper($x.qty):y|$y->wavg()])');
    assert.deepEqual(await columns(q), ['side:demo::trading::Side', 'median:Number', 'p90:Number', 'vwap:Float']);
    assert.throws(() => buildLambda(graph, trades({ columns: [{ ...col('vwap', 'price'), aggregate: 'wavg' }] }), { withFrom: false }), BuildError);
    assert.throws(() => buildLambda(graph, trades({ columns: [{ ...col('p', 'price'), aggregate: 'percentile', percentile: { value: 101, ascending: true, continuous: true } }] }), { withFrom: false }), BuildError);
  });

  it('types window columns, a calculated column and a post-filter with lite', async () => {
    const q = trades({
      columns: [col('ticker', 'product', 'ticker'), col('qty', 'quantity'), { id: 'd', name: 'double', path: [], derivation: await grammar.lambdaJson('x|$x.quantity * 2') }],
      windows: [{ id: 'w', name: 'rk', op: 'denseRank', partition: ['ticker'], sort: { column: 'qty', direction: 'desc' } },
        { id: 'w2', name: 'n', op: 'count', column: 'qty', partition: ['ticker'] }],
      postFilter: { kind: 'group', id: 'p', op: 'and', children: [{ kind: 'condition', id: 'c', path: [{ property: 'rk' }], operator: 'equal', value: { kind: 'integer', value: '1' } }] },
    });
    assert.equal(await text(q),
      '|demo::trading::Trade.all()->project(~[ticker:x|$x.product.ticker, qty:x|$x.quantity, double:x|$x.quantity * 2])->extend(over(~[ticker], [~qty->descending()]), ~rk:{p, w, r|$p->denseRank($w, $r)})->extend(over(~[ticker]), ~n:{p, w, r|$r.qty}:y|$y->count())->filter(r|$r.rk == 1)');
    assert.deepEqual(await columns(q), ['ticker:String', 'qty:Integer', 'double:Integer', 'rk:Integer', 'n:Integer']);
  });

  it('builds a graph fetch, a preview taking its rows first', async () => {
    const q = trades({ graph: { checked: false, tree: [{ property: 'tradeId', children: [] }, { property: 'trader', children: [{ property: 'lastName', children: [] }] }] } });
    assert.equal(await grammar.lambdaText(buildLambda(graph, q, { withFrom: true, previewLimit: 5 }), 'STANDARD'),
      '|demo::trading::Trade.all()->take(5)->graphFetch(#{demo::trading::Trade{tradeId,trader{lastName}}}#)->serialize(#{demo::trading::Trade{tradeId,trader{lastName}}}#)->from(demo::trading::TradingMapping, demo::trading::Runtime)');
  });

  it('refuses a query with no columns, and a path the model does not have', () => {
    assert.throws(() => buildLambda(graph, trades({}), { withFrom: false }), BuildError);
    assert.throws(() => buildLambda(graph, trades({
      columns: [col('x', 'tradeId')],
      filter: { kind: 'group', id: 'g', op: 'and', children: [{ kind: 'condition', id: 'c', path: [{ property: 'nope' }, { property: 'x' }], operator: 'isEmpty' }] },
    }), { withFrom: false }), /no property 'nope'/);
  });
});
