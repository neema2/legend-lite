// Measures declared in the model (src/model-measures.ts, src/model-measure-commands.ts; docs/DATACUBE_BI_EXCEL_PARITY_2026_10_24.md §3.4): read from the
// compiler's elements -- a function of a class's instances stereotyped BI.measure -- and added to a grid over the class
// as the steps a person would make, each aggregate a column with its Aggregation, the rest a group-level column; a body
// that does not take apart so refused in words. The elements here are shaped as the compiler gives them (a probe of
// legend-lite's own modelElements, 2026-10-24).

import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import type { ValueSpecification } from '../../pure-protocol/src/index.ts';
import { classRelation } from '../src/class-source.ts';
import { classTree, measuresOf, type ClassElement } from '../src/class-tree.ts';
import { formatOfCode, modelMeasureCommands } from '../src/model-measure-commands.ts';
import { modelMeasures, type ModelMeasure } from '../src/model-measures.ts';
import { CubeRefusal, type CubeSnapshot } from '../src/snapshot.ts';

const type = (fullPath: string) => ({ rawType: { _type: 'packageableType', fullPath } });
const ONE = { lowerBound: 1, upperBound: 1 };
const MANY = { lowerBound: 0 };
const v = (name: string): ValueSpecification => ({ _type: 'var', name }) as ValueSpecification;
const p = (of: ValueSpecification, name: string): ValueSpecification => ({ _type: 'property', property: name, parameters: [of] }) as ValueSpecification;
const f = (fn: string, ...parameters: ValueSpecification[]): ValueSpecification => ({ _type: 'func', function: fn, parameters }) as ValueSpecification;
const int = (value: number): ValueSpecification => ({ _type: 'integer', value }) as ValueSpecification;

/** A function element as the compiler gives one: its compiled name, its parameter, its body, its stereotypes and tags. */
function measure(name: string, body: ValueSpecification, tags: Record<string, string> = {}, of = 'm::Trade', stereotyped = true,
  profile = 'meta::legend::bi::BI'): unknown {
  return {
    _type: 'function', package: 'm::measures', name: `${name}_${of.slice(of.lastIndexOf(':') + 1)}_MANY__Float_1_`,
    parameters: [{ _type: 'var', name: 't', genericType: type(of), multiplicity: MANY }],
    returnGenericType: type('Float'), returnMultiplicity: ONE, body: [body],
    stereotypes: stereotyped ? [{ profile, value: 'measure' }] : [],
    taggedValues: Object.entries(tags).map(([tag, value]) => ({
      tag: { profile: tag === 'doc' ? 'meta::pure::profiles::doc' : profile, value: tag }, value })),
  };
}

const CLASSES: ClassElement[] = [
  { _type: 'class', package: 'm', name: 'Trade', properties: [
    { name: 'notional', genericType: type('Float'), multiplicity: { lowerBound: 0, upperBound: 1 } },
    { name: 'side', genericType: type('m::Side'), multiplicity: ONE },
  ] },
  { _type: 'class', package: 'm', name: 'Customer', properties: [{ name: 'region', genericType: type('String'), multiplicity: ONE }] },
  { _type: 'Enumeration', package: 'm', name: 'Side' },
  { _type: 'association', package: 'm', name: 'TradeCustomer', properties: [
    { name: 'customer', genericType: type('m::Customer'), multiplicity: { lowerBound: 0, upperBound: 1 } },
    { name: 'trades', genericType: type('m::Trade'), multiplicity: MANY },
  ] },
];
const TREE = classTree(CLASSES);
const filter = (of: ValueSpecification, pred: ValueSpecification): ValueSpecification =>
  f('filter', of, { _type: 'lambda', parameters: [v('x')], body: [pred] } as ValueSpecification);

const ELEMENTS = [
  ...CLASSES,
  measure('grossNotional', f('sum', p(v('t'), 'notional')), { format: '$#,##0', doc: 'Gross notional booked' }),
  measure('bigNotional', f('sum', p(filter(v('t'), f('greaterThan', p(v('x'), 'notional'), int(60))), 'notional'))),
  measure('averageNotional', f('divide', f('m::measures::grossNotional', v('t')), f('count', v('t'))), { format: '#,##0.00' }),
  measure('regions', f('count', p(p(v('t'), 'customer'), 'region'))),
  measure('bigTrades', f('count', filter(v('t'), f('greaterThan', p(v('x'), 'notional'), int(60))))),
  measure('loop', f('m::measures::loop', v('t'))),
  measure('bare', p(v('t'), 'notional')),
  measure('bySide', f('sum', p(filter(v('t'), f('equal', p(v('x'), 'side'), int(1))), 'notional'))),
  measure('notAMeasure', f('sum', p(v('t'), 'notional')), {}, 'm::Trade', false),
  // a project's own BI profile (its store refuses a meta:: path): a measure as the platform's is
  measure('netNotional', f('sum', p(v('t'), 'notional')), { format: '0.0%' }, 'm::Trade', true, 'm::profiles::BI'),
  // another profile's measure stereotype: none
  measure('notOurs', f('sum', p(v('t'), 'notional')), {}, 'm::Trade', true, 'other::Metrics'),
];
const MEASURES = modelMeasures(ELEMENTS);
const byName = (name: string): ModelMeasure => MEASURES.find((m) => m.name === name)!;

const GRID: CubeSnapshot = {
  source: { query: classRelation('m::Trade', ['notional']) },
  columns: [{ name: 'notional', type: 'Float', kind: 'measure' }],
  derived: [], rows: [], pivotOn: [], sorts: [], epoch: 1,
};

describe("the model's measures: functions of a class's instances stereotyped BI.measure", () => {
  it("each by its name, its path, its class and parameter, its format and words; the platform's BI profile or a project's own; an unstereotyped function none", () => {
    assert.deepEqual(MEASURES.map((m) => m.name), ['grossNotional', 'bigNotional', 'averageNotional', 'regions', 'bigTrades', 'loop', 'bare', 'bySide', 'netNotional']);
    const gross = byName('grossNotional');
    assert.deepEqual([gross.path, gross.of, gross.param, gross.format, gross.doc],
      ['m::measures::grossNotional', 'm::Trade', 't', '$#,##0', 'Gross notional booked']);
    assert.equal(byName('netNotional').format, '0.0%', "a project's own BI profile's tags read as the platform's");
    assert.deepEqual(measuresOf(MEASURES, () => [], 'm::Customer'), []);
    assert.equal(measuresOf(MEASURES, () => ['m::Trade'], 'm::BigTrade').length, MEASURES.length, "a supertype's measures are the class's too");
  });

  it('an excel format code as a column format, the code itself, or none: never a guess', () => {
    assert.deepEqual(formatOfCode('$#,##0'), { kind: 'currency', currency: 'USD', decimals: 0 });
    assert.deepEqual(formatOfCode('0.00%'), { kind: 'percent', decimals: 2 });
    assert.deepEqual(formatOfCode('#,##0.0'), { kind: 'number', decimals: 1 });
    // any other code DataCube draws, kept whole; one it does not, left to the column's own
    assert.deepEqual(formatOfCode('#,##0,,"M"'), { kind: 'number', code: '#,##0,,"M"' });
    assert.deepEqual(formatOfCode('[Red]0'), { kind: 'number', code: '[Red]0' });
    assert.equal(formatOfCode('[>100]0'), undefined);
  });
});

describe('a model measure added as the steps a person would make', () => {
  it('one aggregate: a column of its own name with its Aggregation, and the format its tag says', () => {
    assert.deepEqual(modelMeasureCommands(GRID, TREE, 'm::Trade', MEASURES, byName('grossNotional')), [
      { kind: 'calculate', level: 'row', column: { name: 'grossNotional',
        lambda: { _type: 'lambda', parameters: [{ _type: 'var', name: 'x' }], body: [{ _type: 'property', property: 'notional', parameters: [{ _type: 'var', name: 'x' }] }] },
        kind: 'measure' } },
      { kind: 'column', name: 'grossNotional', change: { aggregate: 'sum' } },
      { kind: 'configure', patch: { columns: { grossNotional: { format: { kind: 'currency', currency: 'USD', decimals: 0 } } } } },
    ]);
  });

  it("a filter's rows only: the value kept where the predicate holds, empty where not", () => {
    const [calc] = modelMeasureCommands(GRID, TREE, 'm::Trade', MEASURES, byName('bigNotional'));
    assert.match(JSON.stringify(calc), /"function":"if".*"function":"greaterThan"/);
  });

  it('arithmetic of aggregates, another measure inlined: each a hidden column, the measure a group-level column over them', () => {
    const commands = modelMeasureCommands(GRID, TREE, 'm::Trade', MEASURES, byName('averageNotional'));
    assert.deepEqual(commands.map((c) => (c.kind === 'calculate' ? `${c.level}:${c.column.name}` : c.kind === 'column' ? `agg:${c.change.aggregate}` : c.kind)), [
      'row:averageNotional__1', 'agg:sum', 'row:averageNotional__2', 'agg:count', 'group:averageNotional', 'configure',
    ]);
    const settings = (commands.at(-1) as unknown as { patch: { columns: Record<string, unknown> } }).patch.columns;
    assert.deepEqual(settings, { averageNotional__1: { hidden: true }, averageNotional__2: { hidden: true },
      averageNotional: { format: { kind: 'number', decimals: 2 } } });
  });

  it("a count of values or of a filter's rows: 1 where there is one, else 0, summed -- a grid's count counts its rows", () => {
    const [calc, column] = modelMeasureCommands(GRID, TREE, 'm::Trade', MEASURES, byName('bigTrades'));
    assert.match(JSON.stringify(calc), /"function":"if","parameters":\[\{"_type":"func","function":"greaterThan".*"value":1\}\]\}.*"value":0\}/);
    assert.deepEqual(column, { kind: 'column', name: 'bigTrades', change: { aggregate: 'sum' } });
    // the instances counted, no filter: the grid's own count of its rows
    const average = modelMeasureCommands(GRID, TREE, 'm::Trade', MEASURES, byName('averageNotional'));
    assert.deepEqual(average[3], { kind: 'column', name: 'averageNotional__2', change: { aggregate: 'count' } });
  });

  it('a path through a to-one end: the related column it reads, added hidden', () => {
    const commands = modelMeasureCommands(GRID, TREE, 'm::Trade', MEASURES, byName('regions'));
    assert.deepEqual(commands[0], { kind: 'calculate', level: 'row', column: { name: 'customer_region', related: { path: ['customer', 'region'] }, kind: 'dimension' } });
    // counted where it has a value: a trade with no customer's region is none
    assert.match(JSON.stringify(commands[1]), /"function":"if","parameters":\[\{"_type":"func","function":"isNotEmpty"/);
    assert.deepEqual(commands[2], { kind: 'column', name: 'regions', change: { aggregate: 'sum' } });
    assert.deepEqual(commands.at(-1), { kind: 'configure', patch: { columns: { customer_region: { hidden: true } } } });
    // the grid already reading it: read, not added again
    const reading: CubeSnapshot = { ...GRID, derived: [{ name: 'customer_region', related: { path: ['customer', 'region'] }, kind: 'dimension' }] };
    assert.ok(!modelMeasureCommands(reading, TREE, 'm::Trade', MEASURES, byName('regions')).some((c) => c.kind === 'calculate' && c.column.related));
  });

  it('refused in words: a measure calling itself, its instances read outside an aggregate, a property no column reads, a name taken', () => {
    const refused = (name: string, words: RegExp, s = GRID): void => {
      assert.throws(() => modelMeasureCommands(s, TREE, 'm::Trade', MEASURES, byName(name)),
        (e: unknown) => e instanceof CubeRefusal && words.test(e.message), String(words));
    };
    refused('loop', /loop calls loop: a measure calling itself/);
    refused('bare', /it reads notional outside an aggregate/);
    refused('bySide', /side is no column of the grid's class/);
    refused('grossNotional', /the grid has a column named grossNotional already/,
      { ...GRID, derived: [{ name: 'grossNotional', lambda: { _type: 'lambda', parameters: [], body: [] } as never }] });
  });
});
