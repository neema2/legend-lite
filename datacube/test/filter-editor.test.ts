import assert from 'node:assert/strict';
import { beforeEach, describe, it } from 'node:test';
import { JSDOM } from 'jsdom';

import {
  FilterEditor,
  OPERATORS,
  gutterIndent,
  rowIndent,
  fromFilterNode,
  addTo,
  newCondition,
  newGroup,
  operandKind,
  operatorsFor,
  parseList,
  parseValue,
  toFilter,
  updateNode,
} from '../src/ui/filter-editor.ts';
import { filterExpression } from '../src/serialize.ts';
import type { FilterNode } from '../src/snapshot.ts';

const COLUMNS = [
  { name: 'region', type: 'String' },
  { name: 'desk', type: 'String' },
  { name: 'notional', type: 'Float' },
  { name: 'trade_date', type: 'StrictDate' },
  { name: 'settled', type: 'Boolean' },
];

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
      onApply: (f) => {
        last = f;
        return null;
      },
    });
  });

  it('starts on DataCube\'s empty state, not an empty tree', () => {
    assert.equal(host.querySelectorAll('.dc-filter-row').length, 0);
    assert.match(
      host.querySelector('.dc-filter-empty')?.textContent ?? '',
      /No filter is specified/,
    );
    assert.equal(
      host.querySelector('.dc-filter-btn')?.textContent,
      'Create New Filter',
    );
  });

  it('the root is itself a group row, reading "All of"', () => {
    // The whole point of the rewrite: AND/OR is a heading over the
    // list it combines, not a dropdown at the bottom of a flat list.
    editor.addCondition();
    const rows = [...host.querySelectorAll('.dc-filter-row')];
    assert.equal(rows.length, 2, 'the group row plus one condition');
    assert.ok(rows[0]?.classList.contains('dc-filter-group'));
    const join = rows[0]?.querySelector('.dc-filter-join') as HTMLSelectElement;
    assert.equal(join.options[0]?.textContent, 'All of');
    assert.equal(join.options[1]?.textContent, 'Any of');
  });

  it('puts the operator word BETWEEN siblings, from the second on', () => {
    editor.addCondition();
    editor.addCondition();
    const words = [...host.querySelectorAll('.dc-filter-joinword')].map(
      (w) => w.textContent,
    );
    assert.deepEqual(words, ['and'], 'one word, between the two conditions');

    editor.update(editor.tree.id, { join: 'or' });
    assert.deepEqual(
      [...host.querySelectorAll('.dc-filter-joinword')].map((w) => w.textContent),
      ['or'],
    );
  });

  it('keeps siblings ALIGNED when one of them carries the word', () => {
    // The word is inside the fixed-width lead, never a sibling of
    // the controls. In flow it pushed everything after it, so a row
    // carrying `or` sat further right than the row above it and the
    // columns stopped lining up.
    editor.addCondition();
    editor.addCondition();
    const rows = [...host.querySelectorAll('.dc-filter-row')].slice(1);
    assert.equal(rows.length, 2);

    const indents = rows.map((r) =>
      (r as HTMLElement).style.getPropertyValue('--dc-f-indent'),
    );
    assert.equal(indents[0], indents[1], 'same indent');

    // Same structure either side of the lead: the word is inside it.
    for (const row of rows) {
      const kids = [...row.children].map((c) => c.className.split(' ')[0]);
      assert.deepEqual(
        kids.slice(0, 2),
        ['dc-filter-lead', 'dc-filter-controller'],
        'the controller follows the lead directly, word or no word',
      );
    }
    const word = host.querySelector('.dc-filter-joinword') as HTMLElement;
    assert.equal(word.parentElement?.className.includes('dc-filter-lead'), true);
    assert.equal(
      word.parentElement?.classList.contains('dc-has-word'),
      true,
      'the lead is marked, so CSS can shorten the stub instead of growing',
    );
  });

  it('the root group has no controller; a nested one does', () => {
    // There is nothing to insert the root after, nothing to remove
    // it from, and no group to put it inside.
    editor.addGroup();
    const rows = [...host.querySelectorAll('.dc-filter-row.dc-filter-group')];
    assert.equal(rows[0]?.querySelector('.dc-filter-controller'), null);
    assert.notEqual(rows[1]?.querySelector('.dc-filter-controller'), null);
  });

  it('adds a condition row with column, operator and value', () => {
    editor.addCondition();
    // The FIRST row is now the root group; the condition is under it.
    const row = host.querySelectorAll('.dc-filter-row')[1];
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
    assert.ok(host.querySelector('button.dc-filter-value.dc-filter-list'),
      'a list operator takes a list of entries');
  });

  it('indents the tree: root, its children, their children', () => {
    editor.addCondition();
    editor.addGroup();
    const levels = [...host.querySelectorAll('.dc-filter-row')].map((r) =>
      (r as HTMLElement).style.getPropertyValue('--dc-filter-level'),
    );
    // Root group at 0; its condition and its sub-group at 1; the
    // sub-group's own condition at 2.
    assert.deepEqual(levels, ['0', '1', '1', '2']);
  });

  it('places rows on DataCube\'s geometry', () => {
    // The numbers are what make the connector stubs meet the gutter
    // line; approximating them makes the tree stop reading as one.
    assert.equal(rowIndent(0), 10);
    assert.equal(rowIndent(1), 46);
    assert.equal(rowIndent(2), 142);
    assert.equal(gutterIndent(0), 16);
    assert.equal(gutterIndent(1), 112);
  });

  it('the + button inserts JUST AFTER, not at the end', () => {
    // Appending instead puts the new condition somewhere the user
    // was not looking, which in a deep tree means losing it.
    editor.addCondition();
    editor.addCondition();
    const first = editor.tree.children[0]!.id;
    editor.update(first, { text: 'EMEA' });
    editor.insertAfter(first);
    assert.equal(editor.tree.children.length, 3);
    assert.equal(editor.tree.children[1]!.id !== first, true);
    assert.equal(editor.tree.children[0]!.id, first);
    // A COPY, as upstream's `+` inserts: the match is unchanged until
    // the copy is edited.
    const copy = editor.tree.children[1] as { column: string; text: string };
    assert.equal(copy.column, 'region');
    assert.equal(copy.text, 'EMEA');
  });

  it('the group button makes an OR of the node and a copy, keeping its meaning', () => {
    // The only way to get from A AND B to A AND (B OR C) without
    // deleting and retyping B. Upstream's layer: OR, because a
    // sub-group is for relaxing, and a copy, because x OR x is x.
    editor.addCondition();
    editor.update(editor.tree.children[0]!.id, { text: 'EMEA', not: true });
    const before = editor.filter;
    editor.layer(editor.tree.children[0]!.id);

    const wrapped = editor.tree.children[0]! as {
      kind: string; join: string; children: readonly { text?: string; not: boolean }[];
    };
    assert.equal(wrapped.kind, 'group');
    assert.equal(wrapped.join, 'or');
    assert.equal(wrapped.children.length, 2);
    assert.equal(wrapped.children[1]?.text, 'EMEA');
    assert.equal(wrapped.children[1]?.not, true);
    const after = editor.filter as { kind: string };
    assert.equal(after.kind, 'or');
    assert.ok(before);
  });

  it('will not layer the root, which IS the outermost group', () => {
    editor.addCondition();
    const before = editor.tree;
    editor.layer(editor.tree.id);
    assert.equal(editor.tree, before);
  });

  it('selects a node on click, and clears on the background', () => {
    editor.addCondition();
    const row = host.querySelectorAll('.dc-filter-row')[1] as HTMLElement;
    row.dispatchEvent(new dom.window.MouseEvent('click', { bubbles: true }));
    assert.equal(editor.selected, editor.tree.children[0]!.id);
    assert.equal(
      (host.querySelectorAll('.dc-filter-row')[1] as HTMLElement).classList
        .contains('dc-selected'),
      true,
    );

    (host.querySelector('.dc-filter-tree') as HTMLElement).dispatchEvent(
      new dom.window.MouseEvent('click', { bubbles: true }),
    );
    assert.equal(editor.selected, null);
  });

  it('marks a negated node for assistive technology', () => {
    editor.addCondition();
    const id = editor.tree.children[0]!.id;
    const not = () => host.querySelector('.dc-filter-not');
    assert.equal(not()?.getAttribute('aria-pressed'), 'false');
    editor.update(id, { not: true });
    assert.equal(not()?.getAttribute('aria-pressed'), 'true');
  });

  it('publishes only on APPLY, and undefined when cleared', async () => {
    editor.addCondition();
    const id = editor.tree.children[0]!.id;
    editor.update(id, { text: 'EMEA' });
    assert.equal(last, 'unset', 'an edit reached the cube before Apply');
    await editor.apply();
    assert.deepEqual(last, {
      kind: 'condition',
      column: 'region',
      operator: 'equal',
      value: 'EMEA',
    });
    last = 'unset';
    await editor.apply();
    assert.equal(last, 'unset', 'an unchanged Apply published again');
    editor.clear();
    await editor.apply();
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

describe('the editor follows the column TYPE', () => {
  let dom: JSDOM;
  let host: HTMLElement;
  let editor: FilterEditor;
  let refusal: string | null;
  let closed: boolean;
  let last: unknown;

  beforeEach(() => {
    dom = new JSDOM('<!doctype html><div id="f"></div>');
    host = dom.window.document.getElementById('f') as unknown as HTMLElement;
    refusal = null;
    closed = false;
    last = 'unset';
    editor = new FilterEditor(host, {
      columns: COLUMNS,
      onApply: (f) => { last = f; return refusal; },
      onClose: () => { closed = true; },
    });
    editor.addCondition();
  });
  const id = (): string => editor.tree.children[0]!.id;
  const ops = (): string[] =>
    [...(host.querySelector('.dc-filter-op') as HTMLSelectElement).options]
      .map((o) => o.value);

  it('offers only the operators the column\'s type takes', () => {
    assert.ok(ops().includes('contains'));
    editor.setColumn(id(), 'notional');
    assert.ok(!ops().includes('contains'), 'contains offered on a number');
    assert.ok(ops().includes('lessThan'));
    editor.setColumn(id(), 'settled');
    assert.deepEqual(ops().filter((o) => o.startsWith('less')), []);
    assert.ok(ops().includes('equal'));
  });

  it('switching column keeps a compatible operator, else takes the first, and resets the value', () => {
    editor.setOperator(id(), 'contains');
    editor.update(id(), { text: 'EM' });
    editor.setColumn(id(), 'desk');
    let node = editor.tree.children[0] as { operator: string; text: string };
    assert.equal(node.operator, 'contains');
    assert.equal(node.text, '');
    editor.setColumn(id(), 'notional');
    node = editor.tree.children[0] as { operator: string; text: string };
    assert.equal(node.operator, 'equal');
    assert.equal(node.text, '0', 'a number starts at zero, as upstream');
  });

  it('a number field evaluates arithmetic', () => {
    editor.setColumn(id(), 'notional');
    const input = host.querySelector('.dc-filter-number') as HTMLInputElement;
    input.value = '1e6 * 3';
    input.dispatchEvent(new dom.window.KeyboardEvent('keydown', { key: 'Enter' }));
    assert.deepEqual((editor.filter as { value: unknown }).value, 3_000_000);
  });

  it('a boolean is a checkbox', () => {
    editor.setColumn(id(), 'settled');
    const box = host.querySelector('.dc-filter-bool') as HTMLInputElement;
    assert.ok(box);
    box.checked = true;
    box.dispatchEvent(new dom.window.Event('change'));
    assert.equal((editor.filter as { value: unknown }).value, true);
  });

  it('a date offers Date, Date Time, Today and Now', () => {
    editor.setColumn(id(), 'trade_date');
    const mode = host.querySelector('.dc-filter-date-mode') as HTMLSelectElement;
    assert.deepEqual([...mode.options].map((o) => o.textContent),
      ['Date', 'Date Time', 'Today', 'Now']);
    assert.ok((editor.filter as { value: unknown }).value instanceof Date,
      'an absolute date is a Date, so it is written as a date literal');
    mode.value = 'today';
    mode.dispatchEvent(new dom.window.Event('change'));
    assert.deepEqual((editor.filter as { value: unknown }).value, { relative: 'today' });
    assert.equal(host.querySelector('.dc-filter-date'), null, 'no picker for Today');
  });

  it('a list is typed entries, added and removed', () => {
    editor.setOperator(id(), 'in');
    (host.querySelector('.dc-filter-list') as HTMLButtonElement).click();
    const add = (v: string): void => {
      const input = host.querySelector('.dc-filter-listadd') as HTMLInputElement;
      input.value = v;
      input.dispatchEvent(new dom.window.KeyboardEvent('keydown', { key: 'Enter' }));
    };
    add('EMEA');
    add('APAC');
    assert.deepEqual((editor.filter as { value: unknown }).value, ['EMEA', 'APAC']);
    (host.querySelector('.dc-filter-listdel') as HTMLButtonElement).click();
    assert.deepEqual((editor.filter as { value: unknown }).value, ['APAC']);
  });

  it('compares with a column OF THE SAME TYPE only', () => {
    editor.setColumn(id(), 'notional');
    editor.setOperator(id(), 'lessThanColumn');
    const pick = host.querySelector('.dc-filter-rightcolumn') as HTMLSelectElement;
    assert.deepEqual([...pick.options].map((o) => o.value), ['', 'notional']);
  });

  it('removing a node flattens a group left with one child', () => {
    editor.update(id(), { text: 'EMEA' });
    editor.layer(id());
    const group = editor.tree.children[0] as { id: string;
      children: readonly { id: string }[] };
    editor.remove(group.children[1]!.id);
    assert.equal(editor.tree.children[0]!.kind, 'condition',
      'a one-child group stayed a group');
  });

  it('a refused Apply keeps the window open and says why IN it', async () => {
    editor.update(id(), { text: 'EMEA' });
    refusal = 'the planner refused';
    (host.querySelector('.dc-filter-ok') as HTMLButtonElement).click();
    await new Promise((r) => setTimeout(r, 0));
    assert.equal(closed, false);
    assert.match(host.querySelector('.dc-filter-problem')?.textContent ?? '',
      /refused/);
    refusal = null;
    (host.querySelector('.dc-filter-ok') as HTMLButtonElement).click();
    await new Promise((r) => setTimeout(r, 0));
    assert.equal(closed, true);
    assert.ok(last !== 'unset');
  });

  it('Cancel closes without publishing', () => {
    editor.update(id(), { text: 'EMEA' });
    (host.querySelector('.dc-filter-cancel') as HTMLButtonElement).click();
    assert.equal(closed, true);
    assert.equal(last, 'unset');
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

describe('TODAY and NOW', () => {
  // Upstream's advanced values (DataCubeOperationAdvancedValueType):
  // a date relative to when the query runs.
  it('parse from their call spelling only', () => {
    assert.deepEqual(parseValue('today()'), { relative: 'today' });
    assert.deepEqual(parseValue('now()'), { relative: 'now' });
    assert.equal(parseValue('today'), 'today');
    assert.equal(parseValue("'today()'"), 'today()');
  });

  it('render as the Pure functions, so the date moves with the day', () => {
    assert.equal(
      filterExpression({ kind: 'condition', column: 'trade_date',
        operator: 'lessThan', value: { relative: 'today' } }),
      '$x.trade_date < today()',
    );
  });
});

describe('operatorsFor a Variant', () => {
  it('offers only presence, in either spelling of the type', () => {
    // Text operators on JSON compile to nonsense; ordering compares
    // JSON text. Whether the value is there at all is the question a
    // Variant answers without extracting anything.
    for (const t of ['Variant', 'meta::pure::metamodel::variant::Variant']) {
      assert.deepEqual(operatorsFor(t), ['isEmpty', 'isNotEmpty']);
    }
  });
});
