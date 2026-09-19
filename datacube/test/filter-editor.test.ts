import assert from 'node:assert/strict';
import { beforeEach, describe, it } from 'node:test';
import { JSDOM } from 'jsdom';

import {
  FilterEditor,
  OPERATORS,
  fromFilterNode,
  addTo,
  newCondition,
  newGroup,
  operandKind,
  parseList,
  parseValue,
  toFilter,
  updateNode,
} from '../src/ui/filter-editor.ts';
import { filterExpression } from '../src/serialize.ts';
import type { FilterNode } from '../src/snapshot.ts';

const COLUMNS = ['region', 'desk', 'notional'];

describe('operator table', () => {
  it('covers all 31 operators exactly once', () => {
    assert.equal(OPERATORS.length, 31);
    assert.equal(new Set(OPERATORS.map((o) => o.op)).size, 31);
  });

  it('gives every operator an arity', () => {
    // The operator decides the input; a missing arity means a text
    // box appears for `is null` and the query is wrong.
    assert.equal(operandKind('isEmpty'), 'none');
    assert.equal(operandKind('in'), 'list');
    assert.equal(operandKind('equalColumn'), 'column');
    assert.equal(operandKind('contains'), 'single');
  });
});

describe('parseValue', () => {
  it('types numbers as numbers, so comparisons are numeric', () => {
    // '9' > '10' as text; 9 < 10 as numbers.
    assert.equal(parseValue('10'), 10);
    assert.equal(parseValue('-2.5'), -2.5);
  });

  it('honours quotes as the escape hatch for numeric-looking text', () => {
    // An account code like 00123 must stay text.
    assert.equal(parseValue("'00123'"), '00123');
    assert.equal(parseValue('"10"'), '10');
  });

  it('types booleans', () => {
    assert.equal(parseValue('true'), true);
    assert.equal(parseValue('false'), false);
  });

  it('drops empty entries from a list rather than sending them', () => {
    assert.deepEqual(parseList('a, ,b,'), ['a', 'b']);
    assert.deepEqual(parseList('1, 2'), [1, 2]);
  });
});

describe('toFilter', () => {
  it('is undefined when nothing is usable', () => {
    assert.equal(toFilter(newGroup()), undefined);
    // A half-typed condition narrows nothing rather than narrowing
    // wrongly.
    assert.equal(toFilter(newGroup([newCondition('region')])), undefined);
  });

  it('unwraps a lone condition rather than wrapping it in a group', () => {
    let tree = newGroup([newCondition('region')]);
    tree = updateNode(tree, tree.children[0]!.id, { text: 'EMEA' });
    assert.deepEqual(toFilter(tree), {
      kind: 'condition',
      column: 'region',
      operator: 'equal',
      value: 'EMEA',
    });
  });

  it('joins several conditions with the group operator', () => {
    let tree = newGroup([newCondition('region'), newCondition('desk')]);
    tree = updateNode(tree, tree.children[0]!.id, { text: 'EMEA' });
    tree = updateNode(tree, tree.children[1]!.id, { text: 'Rates' });
    tree = updateNode(tree, tree.id, { join: 'or' });
    const f = toFilter(tree);
    assert.equal(f?.kind, 'or');
    assert.equal(filterExpression(f!), "($x.region == 'EMEA' || $x.desk == 'Rates')");
  });

  it('expresses A AND NOT (B OR C), which a flat list cannot', () => {
    // The reason the editor is a tree: `not` lives on every node,
    // groups included, exactly as DataCube models it.
    let inner = newGroup([newCondition('desk'), newCondition('desk')]);
    inner = updateNode(inner, inner.children[0]!.id, { text: 'Rates' });
    inner = updateNode(inner, inner.children[1]!.id, { text: 'Credit' });
    inner = updateNode(inner, inner.id, { join: 'or', not: true });

    let root = newGroup([newCondition('region')]);
    root = updateNode(root, root.children[0]!.id, { text: 'EMEA' });
    root = addTo(root, root.id, inner);

    assert.equal(
      filterExpression(toFilter(root)!),
      "($x.region == 'EMEA' && !(($x.desk == 'Rates' || $x.desk == 'Credit')))",
    );
  });

  it('negates a single condition', () => {
    let tree = newGroup([newCondition('region')]);
    tree = updateNode(tree, tree.children[0]!.id, { text: 'EMEA', not: true });
    assert.equal(filterExpression(toFilter(tree)!), "!($x.region == 'EMEA')");
  });

  it('drops a group whose children are all incomplete', () => {
    const tree = newGroup([newGroup([newCondition('region')])]);
    assert.equal(toFilter(tree), undefined);
  });

  it('needs no value for a nullary operator', () => {
    let tree = newGroup([newCondition('region')]);
    tree = updateNode(tree, tree.children[0]!.id, { operator: 'isEmpty' });
    assert.equal(filterExpression(toFilter(tree)!), '$x.region->isEmpty()');
  });

  it('needs a second column for a column operator', () => {
    let tree = newGroup([newCondition('region')]);
    tree = updateNode(tree, tree.children[0]!.id, { operator: 'equalColumn' });
    assert.equal(toFilter(tree), undefined, 'incomplete until a column is picked');
    tree = updateNode(tree, tree.children[0]!.id, { rightColumn: 'desk' });
    assert.equal(filterExpression(toFilter(tree)!), '$x.region == $x.desk');
  });
});

describe('FilterEditor DOM', () => {
  let dom: JSDOM;
  let host: HTMLElement;
  let editor: FilterEditor;
  let last: unknown;

  beforeEach(() => {
    dom = new JSDOM('<!doctype html><div id="f"></div>');
    host = dom.window.document.getElementById('f') as unknown as HTMLElement;
    last = 'unset';
    editor = new FilterEditor(host, {
      columns: COLUMNS,
      onChange: (f) => {
        last = f;
      },
    });
  });

  it('starts with just an add control', () => {
    assert.equal(host.querySelectorAll('.dc-filter-row').length, 0);
    assert.equal(host.querySelector('.dc-filter-btn')?.textContent, 'Add filter');
  });

  it('adds a condition row with column, operator and value', () => {
    editor.addCondition();
    const row = host.querySelector('.dc-filter-row');
    assert.ok(row);
    assert.ok(row.querySelector('.dc-filter-column'));
    assert.ok(row.querySelector('.dc-filter-op'));
    assert.ok(row.querySelector('.dc-filter-value'));
    assert.ok(row.querySelector('.dc-filter-not'));
  });

  it('swaps the value input when the operator arity changes', () => {
    editor.addCondition();
    const id = editor.tree.children[0]!.id;

    editor.update(id, { operator: 'isEmpty' });
    assert.equal(
      host.querySelector('.dc-filter-value'),
      null,
      'a nullary operator takes no input at all',
    );

    editor.update(id, { operator: 'equalColumn' });
    assert.equal(
      host.querySelector('select.dc-filter-value')?.tagName,
      'SELECT',
      'a column operator takes a column picker, not a text box',
    );

    editor.update(id, { operator: 'in' });
    const input = host.querySelector('input.dc-filter-value');
    assert.equal(input?.getAttribute('placeholder'), 'a, b, c');
  });

  it('indents nested groups', () => {
    editor.addCondition();
    editor.addGroup();
    const levels = [...host.querySelectorAll('.dc-filter-row')].map((r) =>
      (r as HTMLElement).style.getPropertyValue('--dc-filter-level'),
    );
    // The root's own conditions sit at 0; a nested group and its
    // children sit deeper.
    assert.deepEqual(levels, ['0', '1', '2']);
  });

  it('marks a negated node for assistive technology', () => {
    editor.addCondition();
    const id = editor.tree.children[0]!.id;
    const not = () => host.querySelector('.dc-filter-not');
    assert.equal(not()?.getAttribute('aria-pressed'), 'false');
    editor.update(id, { not: true });
    assert.equal(not()?.getAttribute('aria-pressed'), 'true');
  });

  it('emits the filter on every edit, and undefined when cleared', () => {
    editor.addCondition();
    const id = editor.tree.children[0]!.id;
    editor.update(id, { text: 'EMEA' });
    assert.deepEqual(last, {
      kind: 'condition',
      column: 'region',
      operator: 'equal',
      value: 'EMEA',
    });
    editor.clear();
    assert.equal(last, undefined);
  });

  it('removes only the node asked for', () => {
    editor.addCondition();
    editor.addCondition();
    const first = editor.tree.children[0]!.id;
    editor.remove(first);
    assert.equal(editor.tree.children.length, 1);
    assert.notEqual(editor.tree.children[0]!.id, first);
  });
});

describe('opening on a filter that already exists', () => {
  // Without the inverse, the editor opens EMPTY on a filtered cube
  // and the user's first change writes that emptiness back --
  // silently dropping a filter they can see in force on the grid
  // behind the dialog.

  const roundTrip = (f: FilterNode): FilterNode | undefined =>
    toFilter(
      (() => {
        const d = fromFilterNode(f);
        return d.kind === 'group' && !d.not ? d : newGroup([d]);
      })(),
    );

  it('round-trips a bare condition', () => {
    const f: FilterNode = {
      kind: 'condition',
      column: 'region',
      operator: 'equal',
      value: 'EMEA',
    };
    assert.deepEqual(roundTrip(f), f);
  });

  it('round-trips a nested group', () => {
    const f: FilterNode = {
      kind: 'and',
      children: [
        { kind: 'condition', column: 'a', operator: 'greaterThan', value: 5 },
        {
          kind: 'or',
          children: [
            { kind: 'condition', column: 'b', operator: 'equal', value: 'x' },
            { kind: 'condition', column: 'b', operator: 'equal', value: 'y' },
          ],
        },
      ],
    };
    assert.deepEqual(roundTrip(f), f);
  });

  it('round-trips a negation, which is a NODE here and a FLAG there', () => {
    const f: FilterNode = {
      kind: 'not',
      child: {
        kind: 'or',
        children: [
          { kind: 'condition', column: 'b', operator: 'equal', value: 'x' },
          { kind: 'condition', column: 'b', operator: 'equal', value: 'y' },
        ],
      },
    };
    assert.deepEqual(roundTrip(f), f);
  });

  it('collapses a double negation rather than showing it', () => {
    // The one place the round trip is not literal: NOT NOT x and x
    // are the same filter and the editor should show the simpler.
    const inner: FilterNode = {
      kind: 'condition',
      column: 'a',
      operator: 'equal',
      value: 1,
    };
    const f: FilterNode = { kind: 'not', child: { kind: 'not', child: inner } };
    assert.deepEqual(roundTrip(f), inner);
  });

  it('round-trips a list', () => {
    const f: FilterNode = {
      kind: 'condition',
      column: 'region',
      operator: 'in',
      value: ['EMEA', 'APAC'],
    };
    assert.deepEqual(roundTrip(f), f);
  });

  it('requotes a string that LOOKS numeric', () => {
    // Otherwise reopening the editor silently turns an account code
    // into a number, and the filter stops matching.
    const f: FilterNode = {
      kind: 'condition',
      column: 'account',
      operator: 'equal',
      value: '0042',
    };
    assert.deepEqual(roundTrip(f), f);
  });

  it('keeps a valueless operator valueless', () => {
    const f: FilterNode = {
      kind: 'condition',
      column: 'a',
      operator: 'isEmpty',
    };
    assert.deepEqual(roundTrip(f), f);
  });

  it('does NOT add a bracket level on every reopen', () => {
    const f: FilterNode = {
      kind: 'and',
      children: [
        { kind: 'condition', column: 'a', operator: 'equal', value: 1 },
        { kind: 'condition', column: 'b', operator: 'equal', value: 2 },
      ],
    };
    assert.deepEqual(roundTrip(roundTrip(f) as FilterNode), f);
  });
});
