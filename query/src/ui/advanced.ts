// The builder beyond columns and filters (census §5.3): calculated columns (a lambda of the row),
// window columns (a rank or an aggregate over a partition, in an order), the filter on the
// result's columns (post-filter), and the graph fetch tree (objects as JSON).

import type { AppContext } from '../app/context.ts';
import { mapNode, prune, PROPERTY_DRAG } from '../app/actions.ts';
import type { Session } from '../app/session.ts';
import { resultColumns, type ResultColumn } from '../app/types.ts';
import { buildLambda } from '../builder/build.ts';
import {
  freshId, queryVariables, RANKING, type Condition, type GraphNode, type Group, type Operator, type ProjectionColumn, type PropertyPath,
  type QueryState, type WindowColumn, type WindowOp,
} from '../builder/state.ts';
import { humanize, isNumericFamily, primitiveFamily, simpleName } from '../model/graph.ts';
import { dialog, h, mount, type Child } from './dom.ts';
import { OPERATOR_LABELS, operatorsFor } from './filter.ts';
import { defaultValue, valueEditor } from './values.ts';

// ------------------------------------------------------------------ calculated columns

/** Create or edit a calculated column: a name and a lambda of the row, compiled before it is applied. */
export async function calculatedDialog(app: AppContext, session: Session, existing?: ProjectionColumn): Promise<void> {
  const name = h('input', { class: 'q-input', value: existing?.name ?? 'Calculated', style: 'width:100%' });
  const body = h('textarea', { class: 'q-textarea', rows: 5, spellcheck: false, placeholder: "x|$x.quantity->toFloat() * $x.price" });
  const error = h('div');
  body.value = existing?.derivation ? await app.engine.queryPrint(existing.derivation, 'standard') : 'x|';
  dialog(existing ? `Edit ${existing.name}` : 'New calculated column', (d) => ({
    body: [
      h('label', null, 'Name', name),
      h('label', null, 'Value, as a lambda of the row ', h('code', null, '$x'), body),
      h('div', { class: 'q-faint' }, 'Any Pure expression of the source class: ', h('code', null, "x|$x.firstName + ' ' + $x.lastName"), ', ', h('code', null, 'x|$x.price * 1.1'), '.'),
      error,
    ],
    foot: [
      h('button', { class: 'q-btn', onclick: () => d.close() }, 'Cancel'),
      h('button', {
        class: 'q-btn primary',
        onclick: async () => {
          const n = name.value.trim();
          const taken = session.query.columns.some((c) => c.name === n && c.id !== existing?.id);
          if (!n || taken) { error.replaceChildren(h('div', { class: 'q-error' }, n ? 'Another column has this name.' : 'A column needs a name.')); return; }
          try {
            const text = body.value.trim().startsWith('x|') || /^\{?\s*\w+\s*\|/.test(body.value.trim()) ? body.value.trim() : `x|${body.value.trim()}`;
            const derivation = await app.engine.queryParse(text);
            if (derivation.parameters.length !== 1) throw new Error('the value is a lambda of one row, x|...');
            const column: ProjectionColumn = { id: existing?.id ?? freshId('col'), name: n, path: [], derivation };
            const next = (q: QueryState): QueryState => ({
              ...q, columns: existing ? q.columns.map((c) => (c.id === existing.id ? column : c)) : [...q.columns, column],
            });
            // compile it (the tab's planner) before it is applied: a mistake is said here, not at run time
            await resultColumns(app, session, next(session.query));
            session.update(next);
            d.close();
          } catch (e) {
            error.replaceChildren(h('div', { class: 'q-error-box' }, (e as Error).message));
          }
        },
      }, existing ? 'Apply' : 'Add'),
    ],
  }), { wide: true });
}

// ------------------------------------------------------------------ window columns

const WINDOW_OPS: readonly { op: WindowOp; label: string }[] = [
  { op: 'rank', label: 'rank' }, { op: 'denseRank', label: 'dense rank' }, { op: 'rowNumber', label: 'row number' },
  { op: 'percentRank', label: 'percent rank' }, { op: 'sum', label: 'sum' }, { op: 'count', label: 'count' },
  { op: 'min', label: 'min' }, { op: 'max', label: 'max' }, { op: 'average', label: 'average' },
];

export function windowLabel(w: WindowColumn): string {
  const op = WINDOW_OPS.find((o) => o.op === w.op)!.label;
  const of = w.column ? ` of ${w.column}` : '';
  const by = w.partition.length > 0 ? ` by ${w.partition.join(', ')}` : '';
  const order = w.sort ? `, ${w.sort.column} ${w.sort.direction === 'asc' ? '↑' : '↓'}` : '';
  return `${op}${of}${by}${order}`;
}

export async function windowDialog(app: AppContext, session: Session, existing?: WindowColumn): Promise<void> {
  let cols: ResultColumn[];
  try {
    cols = await resultColumns(app, session, { ...session.query, windows: (session.query.windows ?? []).filter((w) => w.id !== existing?.id) });
  } catch (e) {
    dialog('Window column', (d) => ({ body: h('div', { class: 'q-error-box' }, (e as Error).message), foot: h('button', { class: 'q-btn', onclick: () => d.close() }, 'Close') }));
    return;
  }
  const name = h('input', { class: 'q-input', value: existing?.name ?? 'Rank', style: 'width:100%' });
  const op = h('select', { class: 'q-select' }, WINDOW_OPS.map((o) => h('option', { value: o.op, selected: o.op === (existing?.op ?? 'rank') }, o.label)));
  const numeric = cols.filter((c) => isNumericFamily(primitiveFamily(c.type)) || primitiveFamily(c.type) === 'date');
  const column = h('select', { class: 'q-select' }, cols.map((c) => h('option', { value: c.name, selected: c.name === existing?.column }, c.name)));
  const partition = h('div', { style: 'display:flex; flex-wrap:wrap; gap:8px' }, cols.map((c) => h('label', { style: 'display:flex; gap:4px; align-items:center' },
    h('input', { type: 'checkbox', value: c.name, checked: existing?.partition.includes(c.name) ?? false }), c.name)));
  const sortCol = h('select', { class: 'q-select' }, h('option', { value: '' }, '(no order)'),
    cols.map((c) => h('option', { value: c.name, selected: c.name === existing?.sort?.column }, c.name)));
  const sortDir = h('select', { class: 'q-select' }, h('option', { value: 'asc', selected: existing?.sort?.direction !== 'desc' }, 'ascending'),
    h('option', { value: 'desc', selected: existing?.sort?.direction === 'desc' }, 'descending'));
  const columnRow = h('div', { class: 'q-field', style: 'grid-template-columns:110px 1fr' }, h('label', null, 'Of column'), column);
  const showColumn = (): void => { columnRow.style.display = RANKING.has(op.value as WindowOp) ? 'none' : ''; };
  op.addEventListener('change', () => {
    showColumn();
    if (!existing) name.value = humanize(op.value);
    if (!RANKING.has(op.value as WindowOp) && op.value !== 'count' && numeric.length > 0 && !numeric.some((c) => c.name === column.value)) column.value = numeric[0]!.name;
  });
  showColumn();
  const error = h('div', { class: 'q-error' });
  dialog(existing ? `Edit ${existing.name}` : 'New window column', (d) => ({
    body: [
      h('div', { class: 'q-field', style: 'grid-template-columns:110px 1fr' }, h('label', null, 'Name'), name),
      h('div', { class: 'q-field', style: 'grid-template-columns:110px 1fr' }, h('label', null, 'Function'), op),
      columnRow,
      h('div', { class: 'q-field', style: 'grid-template-columns:110px 1fr' }, h('label', null, 'Partition by'), partition),
      h('div', { class: 'q-field', style: 'grid-template-columns:110px 1fr' }, h('label', null, 'Order by'), h('span', { style: 'display:flex; gap:6px' }, sortCol, sortDir)),
      error,
    ],
    foot: [
      h('button', { class: 'q-btn', onclick: () => d.close() }, 'Cancel'),
      h('button', {
        class: 'q-btn primary',
        onclick: async () => {
          const n = name.value.trim();
          const clash = cols.some((c) => c.name === n) || (session.query.windows ?? []).some((w) => w.name === n && w.id !== existing?.id);
          if (!n || clash) { error.textContent = n ? 'Another column has this name.' : 'A column needs a name.'; return; }
          const o = op.value as WindowOp;
          if ((o === 'rank' || o === 'denseRank' || o === 'percentRank' || o === 'rowNumber') && !sortCol.value) { error.textContent = 'A ranking needs an order.'; return; }
          const w: WindowColumn = {
            id: existing?.id ?? freshId('w'), name: n, op: o,
            ...(RANKING.has(o) ? {} : { column: column.value }),
            partition: [...partition.querySelectorAll('input:checked')].map((i) => (i as HTMLInputElement).value),
            ...(sortCol.value ? { sort: { column: sortCol.value, direction: sortDir.value as 'asc' | 'desc' } } : {}),
          };
          const next = (q: QueryState): QueryState => ({
            ...q, windows: existing ? (q.windows ?? []).map((x) => (x.id === existing.id ? w : x)) : [...(q.windows ?? []), w],
          });
          try {
            await resultColumns(app, session, next(session.query));
            session.update(next);
            d.close();
          } catch (e) {
            error.textContent = (e as Error).message;
          }
        },
      }, existing ? 'Apply' : 'Add'),
    ],
  }));
}

export function renderWindows(app: AppContext, session: Session): Child {
  const windows = session.query.windows ?? [];
  if (windows.length === 0) return null;
  return [
    h('div', { class: 'q-faint', style: 'margin:10px 2px 4px; font-size:11px; text-transform:uppercase; letter-spacing:.5px' }, 'Window columns'),
    windows.map((w) => h('div', { class: 'q-col', style: 'grid-template-columns:16px 1fr auto auto' },
      h('span', { class: 'grip' }, '∿'),
      h('div', null, h('b', null, w.name), h('div', { class: 'path' }, windowLabel(w))),
      h('button', { class: 'q-icon-btn', title: 'Edit', onclick: () => void windowDialog(app, session, w) }, '✎'),
      h('button', { class: 'q-icon-btn', title: 'Remove', onclick: () => session.update((q) => ({ ...q, windows: (q.windows ?? []).filter((x) => x.id !== w.id) })) }, '✕'))),
  ];
}

// ------------------------------------------------------------------ graph fetch

/** Insert a path into the tree, creating the intermediate nodes. */
export function addToTree(tree: readonly GraphNode[], path: PropertyPath): GraphNode[] {
  const [first, ...rest] = path;
  if (!first) return [...tree];
  const existing = tree.find((n) => n.property === first.property);
  if (existing) {
    return tree.map((n) => (n === existing ? { ...n, children: addToTree(n.children, rest) } : n));
  }
  return [...tree, { property: first.property, children: addToTree([], rest) }];
}

function removeFromTree(tree: readonly GraphNode[], path: readonly string[]): GraphNode[] {
  const [first, ...rest] = path;
  return tree.flatMap((n) => {
    if (n.property !== first) return [n];
    if (rest.length === 0) return [];
    return [{ ...n, children: removeFromTree(n.children, rest) }];
  });
}

export function renderGraph(session: Session, drop: (el: HTMLElement, f: (p: PropertyPath) => void) => void): HTMLElement {
  const g = session.query.graph!;
  const graph = session.project.graph;
  const rows: Child[] = [];
  const walk = (nodes: readonly GraphNode[], owner: string, prefix: string[], depth: number): void => {
    for (const n of nodes) {
      const p = graph.property(owner, n.property);
      const path = [...prefix, n.property];
      rows.push(h('div', { class: 'q-node', style: `padding-left:${depth * 16 + 4}px` },
        h('span', { class: 'ico' }, p?.kind === 'class' ? 'C' : '·'),
        h('span', { class: 'label' }, humanize(n.property)),
        h('span', { class: 'q-faint mono', style: 'font-size:11px' }, p ? simpleName(p.type) : '?'),
        h('span', { class: 'q-spacer' }),
        h('button', { class: 'q-icon-btn', title: 'Remove', onclick: () => session.update((q) => ({ ...q, graph: { ...q.graph!, tree: removeFromTree(q.graph!.tree, path) } })) }, '✕')));
      if (p?.kind === 'class') walk(n.children, p.type, path, depth + 1);
    }
  };
  walk(g.tree, session.query.source.class, [], 0);
  const body = h('div', { class: 'q-drop' },
    h('div', { class: 'q-node', style: 'font-weight:600' }, h('span', { class: 'ico' }, 'C'), simpleName(session.query.source.class)),
    rows.length === 0 ? h('div', { class: 'q-hint' }, 'Drag properties here to fetch them as JSON.') : rows,
    h('label', { style: 'display:flex; gap:6px; align-items:center; margin-top:10px', title: "Report a constraint's violations with each object rather than failing" },
      h('input', { type: 'checkbox', checked: g.checked, onchange: (e: Event) => session.update((q) => ({ ...q, graph: { ...q.graph!, checked: (e.target as HTMLInputElement).checked } })) }),
      'Checked (report constraint violations)'));
  drop(body, (path) => session.update((q) => ({ ...q, graph: { ...q.graph!, tree: addToTree(q.graph!.tree, path.map((s) => ({ property: s.property }))) } })));
  return body;
}

// ------------------------------------------------------------------ post-filter

/** The filter on the result's columns: conditions naming a column, typed by the compiler. */
export function renderPostFilter(container: HTMLElement, app: AppContext, session: Session): void {
  const graph = session.project.graph;
  const root: Group = session.query.postFilter ?? { kind: 'group', id: freshId('g'), op: 'and', children: [] };
  const setRoot = (f: (g: Group) => Group): void => session.update((q) => {
    const next = prune(f(q.postFilter ?? root));
    const { postFilter: _drop, ...rest } = q;
    void _drop;
    return next.children.length === 0 ? rest : { ...q, postFilter: next };
  });
  mount(container, h('div', { class: 'q-hint' }, h('span', { class: 'q-spinner' })));
  resultColumns(app, session).then((cols) => {
    const typeOf = (name: string): string => cols.find((c) => c.name === name)?.type ?? 'String';
    const row = (c: Condition): HTMLElement => {
      const colName = c.path[0]?.property ?? '';
      const type = typeOf(colName);
      const ops = operatorsFor(graph, type, true);
      return h('div', { class: 'q-cond' },
        h('select', {
          class: 'q-select', 'aria-label': 'Column',
          onchange: (e: Event) => {
            const name = (e.target as HTMLSelectElement).value;
            setRoot((g) => mapNode(g, c.id, () => ({ ...c, path: [{ property: name }], operator: 'equal', value: defaultValue(graph, typeOf(name), false) })));
          },
        }, cols.map((x) => h('option', { value: x.name, selected: x.name === colName }, x.name))),
        h('select', {
          class: 'q-select', 'aria-label': 'Operator',
          onchange: (e: Event) => {
            const op = (e.target as HTMLSelectElement).value as Operator;
            setRoot((g) => mapNode(g, c.id, () => {
              const { value: _v, ...rest } = c;
              void _v;
              return op === 'isEmpty' || op === 'isNotEmpty' ? { ...rest, operator: op }
                : { ...rest, operator: op, value: defaultValue(graph, type, op === 'in' || op === 'notIn') };
            }));
          },
        }, ops.map((o) => h('option', { value: o, selected: o === c.operator }, OPERATOR_LABELS[o]))),
        c.operator === 'isEmpty' || c.operator === 'isNotEmpty' ? null : valueEditor({
          graph, type, many: c.operator === 'in' || c.operator === 'notIn', value: c.value, variables: queryVariables(session.query),
          onChange: (v) => setRoot((g) => mapNode(g, c.id, (n) => ({ ...(n as Condition), value: v }))),
        }),
        h('span', { class: 'q-spacer' }),
        h('button', { class: 'q-icon-btn', title: 'Remove', onclick: () => setRoot((g) => mapNode(g, c.id, () => undefined)) }, '✕'));
    };
    // upstream's group node, as the filter's: AND/OR at the left, the conditions to its right
    const rows = root.children.filter((c): c is Condition => c.kind === 'condition').map(row);
    mount(container,
      root.children.length > 1
        ? h('div', { class: 'q-group' },
          h('div', { class: 'q-group__op' },
            h('button', {
              class: 'q-editable', title: `${root.op === 'and' ? 'All' : 'Any'} of these -- click to switch between AND and OR`,
              onclick: () => setRoot((g) => ({ ...g, op: g.op === 'and' ? 'or' : 'and' })),
            }, root.op.toUpperCase())),
          h('div', { class: 'q-group__children' }, rows))
        : rows,
      cols.length === 0 ? h('div', { class: 'q-hint' }, 'Add columns first.') : h('button', {
        class: 'q-btn small', style: 'margin-top:6px',
        onclick: () => {
          const first = cols[0]!;
          const cond: Condition = { kind: 'condition', id: freshId('c'), path: [{ property: first.name }], operator: 'equal', value: defaultValue(graph, first.type, false) };
          session.update((q) => ({ ...q, postFilter: { ...(q.postFilter ?? root), children: [...(q.postFilter ?? root).children, cond] } }));
        },
      }, '+ Condition on a result column'));
  }, (e: Error) => mount(container, h('div', { class: 'q-error-box' }, e.message)));
}

/** A graph fetch cannot be sent while the builder has table-only parts: what they are. */
export function tableOnlyParts(q: QueryState): string[] {
  const out: string[] = [];
  if (q.columns.length > 0) out.push('columns');
  if ((q.windows ?? []).length > 0) out.push('window columns');
  if (q.postFilter) out.push('the results filter');
  return out;
}

/** The lambda typed for a relation (throws a BuildError with the reason). */
export function checkBuild(session: Session): string | undefined {
  try {
    buildLambda(session.project.graph, session.query, { withFrom: true });
    return undefined;
  } catch (e) {
    return (e as Error).message;
  }
}

export { PROPERTY_DRAG };
