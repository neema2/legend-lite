// THE COMMANDS BETWEEN TWO STATES OF A GRID TAKE ONE TO THE OTHER (src/grid-between.ts; docs/DATACUBE_LIVE_EDITING_2026_10_24.md
// §3, W1): over grids edited at random by every command kind (test/grid-random.ts), for any two states of a walk,
// forward and back, the commands between them applied to the one make the other -- as the document each would write
// (its saved form) -- and the same grid gives none. An undo is such a pair: the state after a step, back to the one
// before it, through commands alone. Also the vocabulary the diff needs: a calculated column put back in its place
// (`before`), a collapsed tree put back (`tree`), a kind and a pivot exclusion cleared, a removed column's settings gone.

import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { DEFAULT_CONFIGURATION } from '../src/config.ts';
import { gridBetween } from '../src/grid-between.ts';
import { applyGridCommand, applyGridCommands, readGridCommand, type GridCommand } from '../src/grid-commands.ts';
import { CubeRefusal } from '../src/snapshot.ts';
import { TreeState } from '../src/tree.ts';
import { toJson } from '../../pure-protocol/src/index.ts';
import { CUBE, randomWalk, savedForm, start, type Walked } from './grid-random.ts';
import { stand } from './fake-planner.ts';

const SEEDS = 300;
const STEPS = 10;

const refused = (f: () => unknown, words: RegExp): void => {
  assert.throws(f, (e: unknown) => e instanceof CubeRefusal && words.test(e.message), `refused in words: ${words}`);
};

describe('the commands between two states of a grid take one to the other', () => {
  it(`${SEEDS} walks of ${STEPS} edits: every pair of states, forward and back; the same state, no command`, () => {
    let pairs = 0;
    let commands = 0;
    for (let seed = 1; seed <= SEEDS; seed += 1) {
      const { states, steps } = randomWalk(seed, STEPS);
      const why = (i: number, j: number): string => `seed ${seed}, ${i} -> ${j}: ${steps.slice(Math.min(i, j), Math.max(i, j)).join(' -> ')}`;
      for (let i = 0; i < states.length; i += 1) {
        assert.deepEqual(gridBetween(states[i]!, states[i]!), [], `seed ${seed}, state ${i}: the same grid`);
        // each neighbour, and the walk's ends (an undo of one step, of several, of all)
        const others = [i + 1, i + 2, states.length - 1].filter((j) => j > i && j < states.length);
        for (const j of others) {
          for (const [a, b] of [[states[i]!, states[j]!], [states[j]!, states[i]!]] as [Walked, Walked][]) {
            const between = gridBetween(a, b);
            commands += between.length;
            pairs += 1;
            // what the tab tells, the keeper reads as the same commands
            for (const c of between) assert.equal(toJson(readGridCommand(JSON.parse(toJson(c)))), toJson(c), why(i, j));
            assert.deepEqual(savedForm(applyGridCommands(a, between)), savedForm(b), `${why(i, j)}: ${between.map((c) => c.kind).join(', ')}`);
          }
        }
      }
    }
    assert.ok(pairs > SEEDS * STEPS, `${pairs} pairs`);
    assert.ok(commands > pairs, `${commands} commands over ${pairs} pairs`);
  });

  it('an undo is such a pair: a step back is a few commands, applied as any edit, in its words', () => {
    const d = start();
    // a number made a dimension (its type's kind is measure: written down), grouped by, a group opened
    const grouped = applyGridCommands(d, [
      { kind: 'column', name: 'qty', change: { kind: 'dimension' } },
      { kind: 'group', rows: ['qty'] },
      { kind: 'open', path: ['EMEA'], open: true },
    ]);
    const back = gridBetween(grouped, d);
    assert.deepEqual(back.map((c) => c.kind), ['group', 'configure', 'tree']);
    assert.deepEqual(back[1], { kind: 'configure', patch: { columns: { qty: { kind: null, excludedFromPivot: null } } } }, 'the kind cleared, its exclusion with it');
    assert.deepEqual(back[2], { kind: 'tree', open: [], showTotals: false }, 'the tree as it was');
    assert.deepEqual(savedForm(applyGridCommands(grouped, back)), savedForm(d));
  });
});

describe('the vocabulary the diff needs', () => {
  const d = start();
  const calc = (name: string): GridCommand['kind'] extends never ? never : Extract<GridCommand, { kind: 'calculate' }>['column'] =>
    ({ name, lambda: stand('$x.qty * 2'), kind: 'measure', type: 'Integer' });

  it('a calculated column goes before the one named, at its level; one named that is not there is refused', () => {
    const three = applyGridCommands(d, [
      { kind: 'calculate', level: 'row', column: calc('a') },
      { kind: 'calculate', level: 'row', column: calc('c') },
      { kind: 'calculate', level: 'row', column: calc('b'), before: 'c' },
    ]);
    assert.deepEqual(three.snapshot.derived.map((x) => x.name), ['big', 'fee', 'a', 'b', 'c']);
    // edited before another: moved there; at another level, before one of that level
    const moved = applyGridCommand(three, { kind: 'editCalculated', name: 'c', level: 'row', column: calc('c'), before: 'a' });
    assert.deepEqual(moved.snapshot.derived.map((x) => x.name), ['big', 'fee', 'c', 'a', 'b']);
    const regrouped = applyGridCommand(three, { kind: 'editCalculated', name: 'a', level: 'group', column: { name: 'a', lambda: stand('$x.fee') }, before: 'share' });
    assert.deepEqual([regrouped.snapshot.derived.map((x) => x.name), regrouped.snapshot.groupDerived?.map((x) => x.name)], [['big', 'fee', 'b', 'c'], ['a', 'share']]);
    refused(() => applyGridCommand(three, { kind: 'calculate', level: 'row', column: calc('z'), before: 'share' }), /'share' is not a calculated column at the row level/);
    refused(() => applyGridCommand(three, { kind: 'editCalculated', name: 'a', level: 'row', column: calc('a'), before: 'a' }), /cannot go before itself/);
    // read from JSON as the same command, with and without it
    const withBefore: GridCommand = { kind: 'calculate', level: 'row', column: calc('z'), before: 'a' };
    assert.deepEqual(readGridCommand(JSON.parse(JSON.stringify(withBefore))), withBefore);
    refused(() => readGridCommand({ kind: 'calculate', level: 'row', column: calc('z'), before: 3 }), /the calculated column it goes before is text/);
  });

  it("a removed calculated column's settings go with it: nothing kept where nothing reads it", () => {
    const set = applyGridCommands(d, [
      { kind: 'calculate', level: 'row', column: calc('a') },
      { kind: 'configure', patch: { columns: { a: { width: 120, displayName: 'A' } } } },
    ]);
    assert.equal(set.config.columns['a']?.width, 120);
    const removed = applyGridCommand(set, { kind: 'removeCalculated', name: 'a' });
    assert.equal(removed.config.columns['a'], undefined);
    // a page saved before kept them: cleared, not set, for a column the grid has not
    const stale = { ...removed, config: { ...removed.config, columns: { ...removed.config.columns, a: { width: 90 } } } };
    assert.equal(applyGridCommand(stale, { kind: 'configure', patch: { columns: { a: { width: null } } } }).config.columns['a'], undefined);
    refused(() => applyGridCommand(stale, { kind: 'configure', patch: { columns: { a: { width: 100 } } } }), /'a' is not a column of the grid/);
  });

  it('a kind cleared leaves the configuration and the query both; a pivot exclusion cleared, the configuration', () => {
    const made = applyGridCommands(d, [
      { kind: 'column', name: 'qty', change: { kind: 'dimension' } },
      { kind: 'configure', patch: { columns: { notional: { excludedFromPivot: true } } } },
    ]);
    assert.equal(made.config.columns['qty']?.kind, 'dimension');
    assert.equal(made.config.columns['qty']?.excludedFromPivot, true);
    const cleared = applyGridCommand(made, { kind: 'configure', patch: { columns: { qty: { kind: null }, notional: { excludedFromPivot: null } } } });
    assert.deepEqual(cleared.config.columns, DEFAULT_CONFIGURATION.columns);
    assert.deepEqual(cleared.snapshot.columns.find((c) => c.name === 'qty'), { name: 'qty', type: 'Integer' });
    // folded first (the screen's state), the query's kind goes too
    const folded = { ...made, snapshot: { ...made.snapshot, columns: made.snapshot.columns.map((c) => (c.name === 'qty' ? { ...c, kind: 'dimension' as const, excludedFromPivot: true } : c)) } };
    assert.deepEqual(applyGridCommand(folded, { kind: 'configure', patch: { columns: { qty: { kind: null } } } }).snapshot.columns.find((c) => c.name === 'qty'), { name: 'qty', type: 'Integer' });
    refused(() => applyGridCommand(applyGridCommand(made, { kind: 'group', rows: ['qty'] }), { kind: 'configure', patch: { columns: { qty: { kind: null } } } }),
      /'qty' is grouped or pivoted on/);
    refused(() => applyGridCommand(d, { kind: 'configure', patch: { columns: { fee: { kind: null } } } }), /'fee' is a calculated column: its kind is set in its editor/);
  });

  it('the tree set whole, in the saved tree\'s shape: what puts a collapsed tree back', () => {
    const shown = applyGridCommands(d, [
      { kind: 'column', name: 'region', change: { kind: 'dimension' } },
      { kind: 'column', name: 'desk', change: { kind: 'dimension' } },
      { kind: 'group', rows: ['region', 'desk'] },
      { kind: 'configure', patch: { initialExpandToLevel: 1, showRootAggregation: true } },
      { kind: 'open', path: ['EMEA'], open: false },
      { kind: 'open', path: ['AMER', 'FX'], open: true },
    ]);
    const was = shown.tree;
    const collapsed = applyGridCommand(shown, { kind: 'collapseAll' });
    assert.notEqual(collapsed.tree.key, was.key);
    const back = applyGridCommand(collapsed, { kind: 'tree', open: [['AMER'], ['AMER', 'FX']], showTotals: true, expandTo: 1, closed: [['EMEA']] });
    assert.equal(back.tree.key, was.key);
    assert.equal(applyGridCommand(back, { kind: 'tree', open: [], showTotals: false }).tree.key, TreeState.empty().key);
    refused(() => applyGridCommand(shown, { kind: 'tree', open: [[]], showTotals: false }), /the grand total is no group to open/);
    refused(() => applyGridCommand(shown, { kind: 'tree', open: [], showTotals: false, expandTo: -1 }), /a whole number, 0 or more/);
    assert.throws(() => applyGridCommand({ snapshot: CUBE, config: DEFAULT_CONFIGURATION }, { kind: 'tree', open: [], showTotals: false }), (e: unknown) => !(e instanceof CubeRefusal));
    const command: GridCommand = { kind: 'tree', open: [['EMEA', null]], showTotals: true, expandTo: 2, closed: [['AMER']] };
    assert.deepEqual(readGridCommand(JSON.parse(JSON.stringify(command))), command);
    refused(() => readGridCommand({ kind: 'tree', open: 'EMEA', showTotals: true }), /the groups open is a list of groups' paths/);
    refused(() => readGridCommand({ kind: 'tree', open: [], showTotals: 'yes' }), /showTotals is true or false/);
    refused(() => readGridCommand({ kind: 'tree', open: [], showTotals: true, expandTo: 1.5 }), /a whole number, 0 or more/);
  });
});
