// The columns (upstream's "fetch structure", TDS side, census §5.3): drop properties here, rename
// them in place, drag to reorder, pick an aggregate (the others become group keys), remove. The
// header's options chips open sort / distinct / limit / slice.

import { addColumn, PROPERTY_DRAG, propertyAt } from '../app/actions.ts';
import type { Session } from '../app/session.ts';
import { withDateParameters } from '../builder/milestoning.ts';
import { queryVariables, type AggregateOp, type GraphNode, type PercentileOptions, type ProjectionColumn, type PropertyPath, type QueryState, type SortSpec, type Value } from '../builder/state.ts';
import { isNumericFamily, milestoningDates, primitiveFamily, simpleName } from '../model/graph.ts';
import { blankPlaceholder, dialog, h, icon, mount, panelAction, panelHeader, select, showMenu, type Child } from './dom.ts';
import type { AppContext } from '../app/context.ts';
import { addToTree, calculatedDialog, renderGraph, renderWindows, windowDialog } from './advanced.ts';
import { argumentsButton } from './arguments.ts';
import { valueEditor, valueLabel } from './values.ts';

const AGGREGATES: readonly { op: AggregateOp; label: string; fits: (family: string, isEnum: boolean) => boolean }[] = [
  { op: 'count', label: 'count', fits: () => true },
  { op: 'distinctCount', label: 'distinct count', fits: () => true },
  { op: 'sum', label: 'sum', fits: (f) => isNumericFamily(f as never) },
  { op: 'average', label: 'average', fits: (f) => isNumericFamily(f as never) },
  { op: 'min', label: 'min', fits: (f) => isNumericFamily(f as never) || f === 'date' },
  { op: 'max', label: 'max', fits: (f) => isNumericFamily(f as never) || f === 'date' },
  { op: 'stdDevPopulation', label: 'std dev (population)', fits: (f) => isNumericFamily(f as never) },
  { op: 'stdDevSample', label: 'std dev (sample)', fits: (f) => isNumericFamily(f as never) },
  { op: 'joinStrings', label: 'join', fits: (f, e) => f === 'string' && !e },
  { op: 'percentile', label: 'percentile', fits: (f) => isNumericFamily(f as never) },
  { op: 'wavg', label: 'weighted average', fits: (f) => isNumericFamily(f as never) },
];

export function aggregateLabel(op: AggregateOp): string {
  return AGGREGATES.find((a) => a.op === op)!.label;
}

const LABELLED = 'grid-template-columns:110px 1fr';

/** A percentile's settings, as upstream's: the percentile, its order and whether it interpolates. */
function percentileDialog(c: ProjectionColumn, apply: (p: PercentileOptions) => void): void {
  const now = c.percentile ?? { value: 50, ascending: true, continuous: true };
  const value = h('input', { class: 'q-input', type: 'number', min: '0', max: '100', step: 'any', value: String(now.value), 'aria-label': 'Percentile' });
  const ascending = h('input', { type: 'checkbox', checked: now.ascending, id: 'q-pct-asc' });
  const continuous = h('input', { type: 'checkbox', checked: now.continuous, id: 'q-pct-cont' });
  dialog(`Percentile of ${c.name}`, (d) => ({
    body: [
      h('div', { class: 'q-field', style: LABELLED }, h('label', null, 'Percentile'), value),
      h('div', { class: 'q-field', style: LABELLED }, h('label', { for: 'q-pct-asc' }, 'Ascending'), ascending),
      h('div', { class: 'q-field', style: LABELLED }, h('label', { for: 'q-pct-cont' }, 'Continuous'), continuous),
    ],
    foot: [
      h('button', { class: 'q-btn', onclick: () => d.close() }, 'Cancel'),
      h('button', {
        class: 'q-btn primary',
        onclick: () => {
          const v = Number(value.value);
          if (value.value.trim() === '' || !(v >= 0 && v <= 100)) { value.classList.add('q-error'); value.title = 'A percentile is between 0 and 100'; return; }
          d.close();
          apply({ value: v, ascending: ascending.checked, continuous: continuous.checked });
        },
      }, 'Apply'),
    ],
  }));
}

/** A weighted average's weight: another numeric column, consumed by the aggregate. */
function weightDialog(c: ProjectionColumn, candidates: readonly string[], apply: (weight: string) => void): void {
  let chosen = c.weight !== undefined && candidates.includes(c.weight) ? c.weight : candidates[0];
  dialog(`Weighted average of ${c.name}`, (d) => ({
    body: candidates.length === 0
      ? h('div', null, 'A weighted average weighs by another numeric column that is not aggregated. Add one to the columns first.')
      : h('div', { class: 'q-field', style: LABELLED }, h('label', null, 'Weighted by'),
        select(chosen, candidates.map((n) => ({ value: n, label: n })), (v) => { chosen = v; }, { 'aria-label': 'Weight column' })),
    foot: [
      h('button', { class: 'q-btn', onclick: () => d.close() }, 'Cancel'),
      h('button', { class: 'q-btn primary', disabled: chosen === undefined, onclick: () => { if (chosen === undefined) return; d.close(); apply(chosen); } }, 'Apply'),
    ],
  }));
}

/** Accept a property drop on an element: highlight while over, call `onPath` on drop. */
export function propertyDropZone(el: HTMLElement, onPath: (path: PropertyPath) => void): void {
  el.addEventListener('dragover', (e) => {
    if (e.dataTransfer?.types.includes(PROPERTY_DRAG)) { e.preventDefault(); el.classList.add('over'); }
  });
  el.addEventListener('dragleave', (e) => {
    if (!el.contains(e.relatedTarget as Node)) el.classList.remove('over');
  });
  el.addEventListener('drop', (e) => {
    el.classList.remove('over');
    const raw = e.dataTransfer?.getData(PROPERTY_DRAG);
    if (!raw) return;
    e.preventDefault();
    e.stopPropagation();
    onPath(JSON.parse(raw) as PropertyPath);
  });
}

export function renderColumns(container: HTMLElement, app: AppContext, session: Session, humanized: () => boolean): void {
  const q = session.query;
  if (q.graph) {
    mount(container,
      panelHeader('fetch structure', [modeToggle(session, humanized)]),
      h('div', { class: 'q-panel__content' }, renderGraph(session, propertyDropZone)));
    return;
  }
  const graph = session.project.graph;
  const update = (f: (q: QueryState) => QueryState): void => session.update(f);
  let dragging: string | undefined;

  // a column's type: a calculated column's is the compiler's, so aggregate choices treat it as a number
  const typeOf = (c: ProjectionColumn): { family: string; isEnum: boolean; typeText: string } => {
    if (c.derivation) return { family: 'number', isEnum: false, typeText: 'calculated' };
    try {
      const { prop } = propertyAt(graph, q.source.class, c.path);
      return { family: primitiveFamily(prop.type), isEnum: graph.enumerations.has(prop.type), typeText: simpleName(prop.type) };
    } catch (e) {
      return { family: 'other', isEnum: false, typeText: (e as Error).message };
    }
  };
  const weights = new Set(q.columns.flatMap((o) => (o.aggregate === 'wavg' && o.weight ? [o.weight] : [])));
  // a weighted average weighs by another numeric column that is not itself aggregated
  const weightCandidates = (c: ProjectionColumn): string[] =>
    q.columns.filter((o) => o.id !== c.id && o.aggregate === undefined && isNumericFamily(typeOf(o).family as never)).map((o) => o.name);

  const row = (c: ProjectionColumn): HTMLElement => {
    const { family, isEnum, typeText } = typeOf(c);
    // upstream shows the column's property behind an info icon, not under its name
    const info = h('span', { class: 'q-col__info', title: c.derivation ? 'a calculated column' : `\$x.${c.path.map((s) => s.property).join('.')} : ${typeText}` }, icon('info'));
    if (c.derivation) void app.engine.queryPrint(c.derivation, 'standard').then((t) => { info.title = t; }, () => undefined);
    const name = h('input', {
      class: 'q-input', value: c.name, 'aria-label': 'Column name',
      onchange: () => {
        const v = name.value.trim();
        const dup = q.columns.some((o) => o.id !== c.id && o.name === v);
        if (!v || dup) { name.classList.add('q-error'); name.title = v ? 'Another column has this name' : 'A column needs a name'; return; }
        update((s) => ({
          ...s,
          columns: s.columns.map((o) => (o.id === c.id ? { ...o, name: v } : o.weight === c.name ? { ...o, weight: v } : o)),
          options: { ...s.options, sort: s.options.sort.map((x) => (x.column === c.name ? { ...x, column: v } : x)) },
        }));
      },
    });
    const aggButton = h('button', {
      class: `q-btn small q-col__agg${c.aggregate ? ' primary' : ''}`, title: 'Aggregate',
      onclick: (e: MouseEvent) => showMenu(e.clientX, e.clientY, [
        { label: '(none) — group by this column', action: () => setAggregate(undefined) },
        'separator',
        ...AGGREGATES.filter((a) => a.fits(family, isEnum)).map((a) => ({
          label: a.op === 'percentile' || a.op === 'wavg' ? `${a.label}…` : a.label,
          action: () => (a.op === 'percentile' ? percentileDialog(c, (p) => setAggregate('percentile', { percentile: p }))
            : a.op === 'wavg' ? weightDialog(c, weightCandidates(c), (w) => setAggregate('wavg', { weight: w }))
            : setAggregate(a.op)),
        })),
      ]),
    }, weights.has(c.name) ? 'weight' : c.aggregate ? aggregateLabel(c.aggregate) : icon('sigma'));
    if (weights.has(c.name)) aggButton.title = 'The weight of a weighted average: neither grouped nor shown';
    const setAggregate = (op: AggregateOp | undefined, settings: Pick<ProjectionColumn, 'percentile' | 'weight'> = {}): void => update((s) => ({
      ...s,
      columns: s.columns.map((o) => {
        if (o.id !== c.id) return o;
        const base = o.name.replace(/ \([^)]*\)$/, '');
        const { aggregate: _a, percentile: _p, weight: _w, ...rest } = o;
        void _a; void _p; void _w;
        return op ? { ...rest, ...settings, aggregate: op, name: `${base} (${aggregateLabel(op)})` } : { ...rest, name: base };
      }),
    }));
    const el = h('div', {
      class: 'q-col', draggable: 'true',
      ondragstart: (e: DragEvent) => { dragging = c.id; e.dataTransfer?.setData('text/plain', c.name); },
      ondragover: (e: DragEvent) => { if (dragging) e.preventDefault(); },
      ondrop: (e: DragEvent) => {
        if (!dragging || dragging === c.id) return;
        e.preventDefault();
        const from = dragging;
        update((s) => {
          const cols = [...s.columns];
          const moving = cols.findIndex((x) => x.id === from);
          const [m] = cols.splice(moving, 1);
          cols.splice(cols.findIndex((x) => x.id === c.id), 0, m!);
          return { ...s, columns: cols };
        });
      },
    },
    info,
    h('div', { class: 'q-col__name' }, name),
    c.derivation ? h('button', { class: 'q-icon-btn', title: 'Edit the calculation', onclick: () => void calculatedDialog(app, session, c) }, icon('calculator')) : null,
    argumentsButton(session, c.path, (path) => update((s) => ({ ...s, columns: s.columns.map((o) => (o.id === c.id ? { ...o, path } : o)) }))),
    aggButton,
    h('button', {
      class: 'q-col__remove', title: 'Remove column', 'aria-label': 'Remove column',
      onclick: () => update((s) => ({
        ...s,
        columns: s.columns.filter((o) => o.id !== c.id),
        options: { ...s.options, sort: s.options.sort.filter((x) => x.column !== c.name) },
      })),
    }, icon('times')));
    return el;
  };

  const body = h('div', { class: 'q-drop' },
    q.columns.length === 0
      ? blankPlaceholder('Add a projection column', 'Drag and drop properties here, or double-click them in the explorer')
      : q.columns.map(row),
    renderWindows(app, session));
  propertyDropZone(body, (path) => update((s) => addColumn(s, path, humanized())));

  mount(container,
    panelHeader('fetch structure', [modeToggle(session, humanized)], [
      h('button', { class: 'q-panel__action q-panel__action--text', title: 'Add a calculated column', onclick: () => void calculatedDialog(app, session) }, icon('calculator'), ' ƒx'),
      h('button', { class: 'q-panel__action q-panel__action--text', title: 'Add a window column (rank, running total…)', onclick: () => void windowDialog(app, session) }, icon('sigma'), ' Window'),
      panelAction('trash', 'Remove every column', () => update((s) => ({ ...s, columns: [], options: { ...s.options, sort: [] } })), q.columns.length === 0)]),
    h('div', { class: 'q-panel__content' }, h('div', { class: 'q-tds-toolbar' }, optionChips(session)), body));
}

/** The result options as chips; clicking opens their dialog. */
function optionChips(session: Session): Child {
  const o = session.query.options;
  const chips: Child[] = [];
  if (o.distinct) chips.push(h('span', { class: 'q-chip accent' }, 'distinct'));
  for (const s of o.sort) chips.push(h('span', { class: 'q-chip accent' }, `${s.column} ${s.direction === 'asc' ? '↑' : '↓'}`));
  if (o.limit !== undefined) chips.push(h('span', { class: 'q-chip accent' }, `limit ${o.limit}`));
  if (o.slice) chips.push(h('span', { class: 'q-chip accent' }, `rows ${o.slice.start}–${o.slice.end}`));
  const m = session.query.milestoning;
  if (m) {
    chips.push(h('span', { class: 'q-chip accent', title: 'Milestoning: the versions of the class the query reads' },
      m.kind === 'allVersions' ? 'all versions' : `as of ${Object.values(m.dates).map(valueLabel).join(', ')}`));
  }
  return h('span', { class: 'q-options' },
    h('button', { class: 'q-editable', onclick: () => optionsDialog(session) }, icon('cog'), ' Set Query Options'), chips);
}

export function optionsDialog(session: Session): void {
  const q = session.query;
  let sort: SortSpec[] = [...q.options.sort];
  const distinct = h('input', { type: 'checkbox', checked: q.options.distinct, id: 'q-opt-distinct' });
  const limit = h('input', { class: 'q-input', type: 'number', min: '1', value: q.options.limit === undefined ? '' : String(q.options.limit), placeholder: 'no limit', style: 'width:120px' });
  const sliceStart = h('input', { class: 'q-input', type: 'number', min: '0', value: q.options.slice ? String(q.options.slice.start) : '', style: 'width:90px', placeholder: 'from' });
  const sliceEnd = h('input', { class: 'q-input', type: 'number', min: '0', value: q.options.slice ? String(q.options.slice.end) : '', style: 'width:90px', placeholder: 'to' });
  const sortList = h('div');
  const error = h('div', { class: 'q-error' });
  const drawSort = (): void => {
    mount(sortList,
      sort.map((s, i) => h('div', { style: 'display:flex; gap:6px; align-items:center; margin-bottom:4px' },
        h('select', { class: 'q-select', onchange: (e: Event) => { sort[i] = { ...s, column: (e.target as HTMLSelectElement).value }; } },
          q.columns.map((c) => h('option', { value: c.name, selected: c.name === s.column }, c.name))),
        h('button', { class: 'q-btn small', onclick: () => { sort[i] = { ...s, direction: s.direction === 'asc' ? 'desc' : 'asc' }; drawSort(); } }, s.direction === 'asc' ? 'Ascending ↑' : 'Descending ↓'),
        h('button', { class: 'q-icon-btn', onclick: () => { sort = sort.filter((_, j) => j !== i); drawSort(); } }, '✕'))),
      q.columns.length > 0
        ? h('button', { class: 'q-btn small', onclick: () => { sort.push({ column: q.columns[0]!.name, direction: 'asc' }); drawSort(); } }, '+ Sort by')
        : h('span', { class: 'q-faint' }, 'Add columns to sort by them.'));
  };
  drawSort();
  // milestoning (upstream's section of the same dialog): the class's dates, or every version
  const graph = session.project.graph;
  const temporal = graph.temporalOf(q.source.class);
  let milestoning = q.milestoning;
  const milestoningBox = h('div');
  const drawMilestoning = (): void => {
    if (temporal === undefined || milestoning === undefined) return;
    const all = h('input', {
      type: 'checkbox', id: 'q-opt-allversions', checked: milestoning.kind === 'allVersions',
      onchange: () => {
        milestoning = all.checked ? { kind: 'allVersions' }
          : { kind: 'asOf', dates: Object.fromEntries(milestoningDates(temporal).map((d): [string, Value] => [d, { kind: 'variable', name: d }])) };
        drawMilestoning();
      },
    });
    const current = milestoning;
    mount(milestoningBox,
      h('b', null, 'Milestoning'),
      h('label', { style: 'display:flex; gap:6px; align-items:center' }, all, 'Query all milestoned versions of the root class'),
      current.kind === 'asOf' ? milestoningDates(temporal).map((d) => h('div', { class: 'q-field', style: 'grid-template-columns:130px 1fr' },
        h('label', null, d === 'businessDate' ? 'Business date' : 'Processing date'),
        valueEditor({
          graph, type: 'Date', many: false, value: current.dates[d], variables: queryVariables(q), relativeDates: false,
          onChange: (v) => { milestoning = { kind: 'asOf', dates: { ...current.dates, [d]: v } }; drawMilestoning(); },
        }))) : null);
  };
  drawMilestoning();
  dialog('Query options', (d) => ({
    body: [
      temporal !== undefined ? milestoningBox : null,
      h('div', null, h('b', null, 'Sort'), sortList),
      h('label', { style: 'display:flex; gap:6px; align-items:center' }, distinct, 'Eliminate duplicate rows (distinct)'),
      h('div', { class: 'q-field', style: 'grid-template-columns:120px 1fr' }, h('label', null, 'Limit results'), limit),
      h('div', { class: 'q-field', style: 'grid-template-columns:120px 1fr' }, h('label', null, 'Slice rows'), h('span', null, sliceStart, ' – ', sliceEnd)),
      error,
    ],
    foot: [
      h('button', { class: 'q-btn', onclick: () => d.close() }, 'Cancel'),
      h('button', {
        class: 'q-btn primary',
        onclick: () => {
          const lim = limit.value === '' ? undefined : Number(limit.value);
          const hasSlice = sliceStart.value !== '' || sliceEnd.value !== '';
          const start = Number(sliceStart.value || 0), end = Number(sliceEnd.value || 0);
          if (lim !== undefined && (!Number.isInteger(lim) || lim < 1)) { error.textContent = 'The limit is a whole number of rows, at least 1.'; return; }
          if (hasSlice && !(Number.isInteger(start) && Number.isInteger(end) && start >= 0 && end > start)) { error.textContent = 'A slice runs from a row to a later row.'; return; }
          session.update((s) => withDateParameters({
            ...s,
            ...(milestoning ? { milestoning } : {}),
            options: {
              sort, distinct: distinct.checked,
              ...(lim !== undefined ? { limit: lim } : {}),
              ...(hasSlice ? { slice: { start, end } } : {}),
            },
          }));
          d.close();
        },
      }, 'Apply'),
    ],
  }));
}

/** Table (columns) or Objects (graph fetch): switching carries the property paths across. */
function modeToggle(session: Session, humanized: () => boolean): HTMLElement {
  const graphMode = session.query.graph !== undefined;
  const toTable = (): void => session.update((q) => {
    const paths: PropertyPath[] = [];
    const walk = (nodes: readonly GraphNode[], prefix: PropertyPath): void => {
      for (const n of nodes) {
        const path = [...prefix, { property: n.property }];
        if (n.children.length === 0) paths.push(path); else walk(n.children, path);
      }
    };
    walk(q.graph?.tree ?? [], []);
    const { graph: _g, ...rest } = q;
    void _g;
    return paths.reduce((acc, p) => addColumn(acc, p, humanized()), { ...rest, columns: [] } as QueryState);
  });
  const toGraph = (): void => session.update((q) => {
    const tree = q.columns.filter((c) => !c.derivation && c.path.length > 0).reduce<GraphNode[]>((t, c) => addToTree(t, c.path), []);
    const { windows: _w, postFilter: _p, ...rest } = q;
    void _w; void _p;
    return { ...rest, columns: [], graph: { tree, checked: false }, options: { sort: [], distinct: false } };
  });
  // upstream's mode pills (QueryBuilderFetchStructurePanel)
  return h('span', { class: 'q-modes' },
    h('button', { class: `q-mode${graphMode ? '' : ' on'}`, onclick: () => graphMode && toTable(), title: 'Rows of columns' }, 'Tabular Data Structure'),
    h('button', { class: `q-mode${graphMode ? ' on' : ''}`, onclick: () => !graphMode && toGraph(), title: 'Objects as JSON (graph fetch)' }, 'Graph Fetch'));
}
