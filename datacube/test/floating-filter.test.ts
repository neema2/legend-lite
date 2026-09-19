import assert from 'node:assert/strict';
import { beforeEach, describe, it } from 'node:test';
import { JSDOM } from 'jsdom';

import {
  FloatingFilterRow,
  conjuncts,
  floatingText,
  isComplex,
  mentions,
  parseFloating,
  renderFloating,
  withFloating,
} from '../src/grid/floating-filter.ts';
import { filterExpression } from '../src/serialize.ts';
import type { FilterCondition, FilterNode } from '../src/snapshot.ts';

const eq = (column: string, value: unknown): FilterCondition => ({
  kind: 'condition',
  column,
  operator: 'equal',
  value: value as never,
});

describe('parsing what the user typed', () => {
  it('reads a comparator prefix on a numeric column', () => {
    assert.deepEqual(parseFloating('n', 'Float', '>100'), {
      kind: 'condition',
      column: 'n',
      operator: 'greaterThan',
      value: 100,
    });
    assert.deepEqual(parseFloating('n', 'Integer', '>= 5'), {
      kind: 'condition',
      column: 'n',
      operator: 'greaterThanEqual',
      value: 5,
    });
  });

  it('prefers the LONGER comparator', () => {
    // '>=' must not be read as '>' followed by '=5'.
    assert.equal(parseFloating('n', 'Float', '>=5')?.operator, 'greaterThanEqual');
    assert.equal(parseFloating('n', 'Float', '<=5')?.operator, 'lessThanEqual');
    assert.equal(parseFloating('n', 'Float', '!=5')?.operator, 'notEqual');
  });

  it('a bare number on a numeric column is equality', () => {
    assert.equal(parseFloating('n', 'Float', '42')?.operator, 'equal');
  });

  it('a string column gets a case-insensitive contains', () => {
    // The only default that is useful more often than it is wrong.
    assert.deepEqual(parseFloating('s', 'String', 'em'), {
      kind: 'condition',
      column: 's',
      operator: 'containsCaseInsensitive',
      value: 'em',
    });
  });

  it('a blank box narrows NOTHING', () => {
    assert.equal(parseFloating('s', 'String', '   '), null);
  });

  it('non-numeric text on a numeric column narrows nothing', () => {
    // Guessing `contains` on a number is how a filter silently stops
    // meaning what it says.
    assert.equal(parseFloating('n', 'Float', 'abc'), null);
    assert.equal(parseFloating('n', 'Float', '>abc'), null);
  });
});

describe('rendering back into the box', () => {
  it('round-trips every comparator', () => {
    for (const text of ['>100', '>=100', '<100', '<=100', '!=100', '=100']) {
      const parsed = parseFloating('n', 'Float', text);
      assert.notEqual(parsed, null, text);
      assert.equal(renderFloating(parsed as FilterCondition, 'Float'), text);
    }
  });

  it('shows a string contains as bare text', () => {
    const c = parseFloating('s', 'String', 'em') as FilterCondition;
    assert.equal(renderFloating(c, 'String'), 'em');
  });

  it('declines an operator a box cannot express', () => {
    assert.equal(
      renderFloating(
        { kind: 'condition', column: 's', operator: 'startsWith', value: 'x' },
        'String',
      ),
      null,
    );
    assert.equal(
      renderFloating(
        { kind: 'condition', column: 'n', operator: 'isEmpty' },
        'Float',
      ),
      null,
    );
  });
});

describe('living in the same filter tree', () => {
  it('flattens a top-level AND and nothing else', () => {
    assert.deepEqual(conjuncts(undefined), []);
    assert.deepEqual(conjuncts(eq('a', 1)), [eq('a', 1)]);
    assert.deepEqual(
      conjuncts({ kind: 'and', children: [eq('a', 1), eq('b', 2)] }),
      [eq('a', 1), eq('b', 2)],
    );
    const or: FilterNode = { kind: 'or', children: [eq('a', 1)] };
    assert.deepEqual(conjuncts(or), [or], 'an OR is one conjunct, not two');
  });

  it('finds a column anywhere inside a node', () => {
    const nested: FilterNode = {
      kind: 'not',
      child: { kind: 'or', children: [eq('a', 1), eq('b', 2)] },
    };
    assert.equal(mentions(nested, 'b'), true);
    assert.equal(mentions(nested, 'z'), false);
  });

  it('adds a condition beside what is already there', () => {
    const before: FilterNode = eq('a', 1);
    const after = withFloating(
      before,
      'b',
      parseFloating('b', 'Float', '>5') as FilterCondition,
    );
    assert.deepEqual(conjuncts(after).length, 2);
  });

  it('replaces its own condition rather than stacking them', () => {
    let f = withFloating(undefined, 'n', parseFloating('n', 'Float', '>5'));
    f = withFloating(f, 'n', parseFloating('n', 'Float', '>9'));
    assert.deepEqual(conjuncts(f), [
      { kind: 'condition', column: 'n', operator: 'greaterThan', value: 9 },
    ]);
  });

  it('clearing a box removes only that condition', () => {
    let f: FilterNode | undefined = {
      kind: 'and',
      children: [eq('a', 1), eq('b', 2)],
    };
    f = withFloating(f, 'a', null);
    assert.deepEqual(f, eq('b', 2), 'a single survivor is not left in a group');
  });

  it('returns the SAME filter when nothing changed', () => {
    // A box that emits an identical condition on every keystroke
    // must not re-run the query.
    const f: FilterNode = eq('a', 1);
    assert.equal(withFloating(f, 'a', eq('a', 1)), f);
    assert.equal(withFloating(f, 'z', null), f);
  });

  it('the box reads back what the editor wrote', () => {
    // One tree, so the two cannot drift.
    const f = withFloating(undefined, 'n', parseFloating('n', 'Float', '>=7'));
    assert.equal(floatingText(f, 'n', 'Float'), '>=7');
  });

  it('is BLANK, not wrong, for a filter it cannot show', () => {
    const f: FilterNode = {
      kind: 'or',
      children: [eq('n', 1), eq('n', 2)],
    };
    assert.equal(floatingText(f, 'n', 'Float'), '');
    assert.equal(isComplex(f, 'n', 'Float'), true);
  });

  it('a negated condition counts as complex', () => {
    const f: FilterNode = { kind: 'not', child: eq('n', 1) };
    assert.equal(isComplex(f, 'n', 'Float'), true);
  });

  it('an unfiltered column is not complex', () => {
    assert.equal(isComplex(eq('a', 1), 'b', 'Float'), false);
  });

  it('what it builds is serialisable Pure', () => {
    // The point of sharing the tree: the floating filter's output
    // goes through the same serialiser as everything else.
    const f = withFloating(undefined, 'region', {
      kind: 'condition',
      column: 'region',
      operator: 'containsCaseInsensitive',
      value: 'em',
    });
    assert.ok(filterExpression(f as FilterNode).includes('region'));
  });
});

describe('the row', () => {
  let dom: JSDOM;
  let doc: Document;
  let emitted: (FilterNode | undefined)[];
  let timers: (() => void)[];

  beforeEach(() => {
    dom = new JSDOM('<!doctype html><body></body>');
    doc = dom.window.document;
    emitted = [];
    timers = [];
  });

  const make = (filter?: FilterNode): FloatingFilterRow => {
    const row = new FloatingFilterRow(doc, {
      onChange: (f) => emitted.push(f),
      setTimeoutFn: (fn) => {
        timers.push(fn);
        return timers.length;
      },
      clearTimeoutFn: () => {},
    });
    row.setFilter(filter);
    return row;
  };
  const input = (cell: HTMLElement): HTMLInputElement =>
    cell.querySelector('input') as HTMLInputElement;
  const type = (el: HTMLInputElement, text: string): void => {
    el.value = text;
    el.dispatchEvent(new dom.window.Event('input'));
  };

  it('DEBOUNCES rather than querying per keystroke', () => {
    const row = make();
    const cell = row.cell({ name: 'region', type: 'String', filterable: true });
    type(input(cell), 'e');
    type(input(cell), 'em');
    assert.deepEqual(emitted, [], 'nothing yet');
    (timers.at(-1) as () => void)();
    assert.equal(emitted.length, 1);
  });

  it('commits at once on Enter', () => {
    const row = make();
    const cell = row.cell({ name: 'region', type: 'String', filterable: true });
    const el = input(cell);
    el.value = 'emea';
    el.dispatchEvent(
      new dom.window.KeyboardEvent('keydown', { key: 'Enter', bubbles: true }),
    );
    assert.equal(emitted.length, 1);
  });

  it('Escape clears the box without closing anything above it', () => {
    const row = make({
      kind: 'condition',
      column: 'region',
      operator: 'containsCaseInsensitive',
      value: 'x',
    });
    const cell = row.cell({ name: 'region', type: 'String', filterable: true });
    let bubbled = false;
    doc.body.append(cell);
    doc.body.addEventListener('keydown', () => {
      bubbled = true;
    });
    const el = input(cell);
    el.dispatchEvent(
      new dom.window.KeyboardEvent('keydown', { key: 'Escape', bubbles: true }),
    );
    assert.equal(el.value, '');
    assert.equal(bubbled, false);
  });

  it('shows no box for a pivoted column', () => {
    // Its name is a path, not a column the engine knows.
    const row = make();
    const cell = row.cell({
      name: '2023__|__total',
      type: 'Float',
      filterable: false,
    });
    assert.equal(cell.querySelector('input'), null);
  });

  it('DISABLES the box on a filter it cannot show, rather than blanking it', () => {
    // A blank box on a filtered column reads as "not filtered".
    const row = make({ kind: 'or', children: [eq('n', 1), eq('n', 2)] });
    const cell = row.cell({ name: 'n', type: 'Float', filterable: true });
    assert.equal(input(cell).disabled, true);
    assert.equal(input(cell).placeholder, 'filtered');
  });

  it('opens showing the filter already in force', () => {
    const row = make({
      kind: 'condition',
      column: 'n',
      operator: 'greaterThan',
      value: 5,
    });
    const cell = row.cell({ name: 'n', type: 'Float', filterable: true });
    assert.equal(input(cell).value, '>5');
  });
});
