// A column's values gathered by hand into named groups (snapshot.ts ValueGroups; docs/DATACUBE_BI_EXCEL_PARITY_2026_10_24.md
// P3, Tableau's groups): its expression built at query time from the Filter Values window's own test (query.ts
// groupsLambda), read from JSON and judged against the grid as a command (grid-commands.ts), refused in words.

import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { element, lambda, lit, property, variable } from '../../pure-protocol/src/index.ts';
import { DEFAULT_CONFIGURATION } from '../src/config.ts';
import { derivedReads } from '../src/cube-document.ts';
import { applyGridCommand, readGridCommand, type GridCommand } from '../src/grid-commands.ts';
import { groupsLambda } from '../src/query.ts';
import { CubeRefusal, type CubeSnapshot } from '../src/snapshot.ts';

const x = variable('x');
const GRID: CubeSnapshot = {
  source: { query: element('trades') },
  columns: [{ name: 'desk', type: 'String', kind: 'dimension' }, { name: 'year', type: 'Integer', kind: 'dimension' },
    { name: 'qty', type: 'Integer', kind: 'measure' }],
  derived: [], rows: [], pivotOn: [], sorts: [], epoch: 1,
};
const draft = { snapshot: GRID, config: DEFAULT_CONFIGURATION };
const refused = (f: () => unknown, words: RegExp): void => {
  assert.throws(f, (e: unknown) => e instanceof CubeRefusal && words.test(e.message), String(words));
};
const calculate = (groups: unknown, level = 'row', name = 'desk_group'): GridCommand =>
  readGridCommand({ kind: 'calculate', level, column: { name, groups, kind: 'dimension' } });

describe("a value-groups column's expression: each group's test, its name where it holds, the rest", () => {
  it('in order, a group of one value by equality, of several by in, the blanks by isEmpty; the rest the value itself', () => {
    // each literal typed by its column, as the planner has typed it
    const made = groupsLambda({ of: 'desk', groups: [{ name: 'Macro', values: ['FX', 'Rates'] }, { name: 'Odd', values: ['EQ', null] }] }, () => 'String');
    const desk = property(x, 'desk');
    const [body] = made.body as unknown as [{ function: string; parameters: unknown[] }];
    assert.equal(body.function, 'if');
    assert.match(JSON.stringify(body.parameters[0]), /^\{"_type":"func","function":"in".*"FX".*"Rates"/);
    assert.deepEqual(body.parameters[1], lambda([], lit.string('Macro')));
    const inner = (body.parameters[2] as { body: [{ function: string; parameters: unknown[] }] }).body[0];
    assert.match(JSON.stringify(inner.parameters[0]), /"function":"or".*"function":"equal".*"EQ".*"function":"isEmpty"/);
    // the rest: the value itself
    assert.deepEqual(inner.parameters[2], lambda([], desk));
  });

  it('a number column: its literals typed as its own, the rest under its name', () => {
    const made = groupsLambda({ of: 'year', groups: [{ name: 'Recent', values: [2025, 2026] }], other: 'Earlier' }, (c) => (c === 'year' ? 'Integer' : undefined));
    assert.match(JSON.stringify(made), /"_type":"integer","value":2025/);
    assert.match(JSON.stringify(made), /"value":"Earlier"/);
  });
});

describe('read from JSON and judged against the grid, in words', () => {
  it('kept as given: a row-level dimension', () => {
    const made = applyGridCommand(draft, calculate({ of: 'desk', groups: [{ name: 'Macro', values: ['FX', 'Rates'] }] }));
    assert.deepEqual(made.snapshot.derived, [{ name: 'desk_group', groups: { of: 'desk', groups: [{ name: 'Macro', values: ['FX', 'Rates'] }] }, kind: 'dimension' }]);
  });

  it('refused: a value in two groups, a name twice, no values, a blank name, a group-level column, no such column, a number column\'s rest unnamed', () => {
    refused(() => calculate({ of: 'desk', groups: [{ name: 'A', values: ['FX'] }, { name: 'B', values: ['FX'] }] }), /"FX" are in A and B: a value is in one group/);
    refused(() => calculate({ of: 'desk', groups: [{ name: 'A', values: ['FX'] }, { name: 'A', values: ['EQ'] }] }), /two groups are named A/);
    refused(() => calculate({ of: 'desk', groups: [{ name: 'A', values: [] }] }), /the group A holds at least one value/);
    refused(() => calculate({ of: 'desk', groups: [{ name: ' ', values: ['FX'] }] }), /a group's name is some text, not blank/);
    refused(() => calculate({ of: 'desk', groups: [] }), /at least one group/);
    refused(() => applyGridCommand(draft, calculate({ of: 'desk', groups: [{ name: 'A', values: ['FX'] }] }, 'group')), /a row-level column, before the grouping/);
    refused(() => applyGridCommand(draft, calculate({ of: 'book', groups: [{ name: 'A', values: ['FX'] }] })), /'book' is no column of the grid's rows/);
    refused(() => applyGridCommand(draft, calculate({ of: 'year', groups: [{ name: 'Recent', values: [2026] }] })), /year is Integer: its values in no group need a name \(other\)/);
  });
});

describe('the column a groups column reads, as every derived kind says', () => {
  it("named among its reads (a saved page drops it with that column); a rename of that column carried into it", () => {
    const g = { name: 'desk_group', groups: { of: 'desk', groups: [{ name: 'Macro', values: ['FX'] }] }, kind: 'dimension' as const };
    assert.deepEqual([...derivedReads(g)], ['desk']);
    const withBook: CubeSnapshot = { ...GRID, derived: [{ name: 'book', lambda: lambda(['x'], property(x, 'desk')), kind: 'dimension' }] };
    const made = applyGridCommand({ snapshot: withBook, config: DEFAULT_CONFIGURATION },
      readGridCommand({ kind: 'calculate', level: 'row', column: { ...g, groups: { ...g.groups, of: 'book' } } }));
    const renamed = applyGridCommand(made, readGridCommand({ kind: 'editCalculated', name: 'book', level: 'row',
      column: { name: 'desk_copy', lambda: lambda(['x'], property(x, 'desk')), kind: 'dimension' } }));
    assert.equal(renamed.snapshot.derived.find((d) => d.name === 'desk_group')?.groups?.of, 'desk_copy');
  });
});
