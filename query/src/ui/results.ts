// Results (census §7): run, stop, see whether the query changed since; the SQL; CSV export of
// every row. A relation's rows are a DataCube over the query (app/cube.ts) -- its grid, paging,
// sorting, grouping, pivots and formats. A graph fetch's objects show as JSON, with a preview
// limit (an overflow says so). A query the cube cannot take as its source (one with `let`s) runs
// on the engine into a plain grid that sorts, copies, and filters by (or out) a cell's value.

import type { CubeApp } from '../../../datacube/src/embed.ts';
import { openCube } from '../app/cube.ts';
import { DEFAULT_PREVIEW, executeInput, executionLambda, run, sqlOf } from '../app/run.ts';
import type { AppContext } from '../app/context.ts';
import type { Session } from '../app/session.ts';
import { mapGroup } from '../app/actions.ts';
import { freshId, type Condition, type Value } from '../builder/state.ts';
import { isTds, type ExecutionResult, type TdsResult } from '../backend/wire.ts';
import { primitiveFamily, isNumericFamily, standardPrimitive } from '../model/graph.ts';
import { propertyAt } from '../app/actions.ts';
import { dialog, h, icon, menuButton, mount, panelHeader, showMenu, toast } from './dom.ts';
import { cellText, plural } from './format.ts';
import { missingValues } from './params.ts';

export class Results {
  readonly element = h('div', { class: 'q-results' });
  readonly #app: AppContext;
  readonly #session: Session;
  #limit = DEFAULT_PREVIEW;
  #sort: { column: number; dir: 1 | -1 } | undefined;
  #selected: { row: number; col: number } | undefined;
  /** The results as a DataCube (a relation's rows), in the element it draws into. */
  #cube: { readonly app: CubeApp; readonly host: HTMLElement } | undefined;
  /** Which run is the latest: an older cube still opening is dropped when it lands. */
  #runs = 0;
  /** The element a cube is opening into, shown while it types its source. */
  #opening: HTMLElement | undefined;
  /** How a relation's rows show: the plain grid (the default) or a DataCube over the query. */
  #view: 'grid' | 'cube' = 'grid';
  /** The cube's row count and time, in the bar; each view the cube lands updates it in place. */
  readonly #cubeStatus = h('span', null);

  constructor(app: AppContext, session: Session) {
    this.#app = app;
    this.#session = session;
  }

  /**
   * Run the query. A relation's rows show in the plain grid, or -- when the person picks DataCube
   * -- as a DataCube over the query, which runs (and re-runs) its own queries. A graph fetch's
   * objects run on the engine and show as JSON.
   */
  run(): void {
    const missing = missingValues(this.#session);
    if (missing.length > 0) {
      toast(`Set a value for ${missing.map((p) => `$${p.name}`).join(', ')} first (Parameters panel).`);
      return;
    }
    this.#sort = undefined;
    this.#selected = undefined;
    this.#closeCube();
    const mine = (this.#runs += 1);
    const session = this.#session;
    if (this.#view === 'grid' || this.#objects()) {
      void run(this.#app, session, this.#limit);
      return;
    }
    const abort = new AbortController();
    const queryHash = session.hash();
    // the cube measures its grid as it draws, so it opens in the page: render places the host
    const host = h('div', { class: 'q-cube' });
    this.#opening = host;
    session.setRun({ status: 'running', started: performance.now(), abort });
    this.#cubeStatus.textContent = '';
    openCube(this.#app, session, host, (view) => {
      if (mine !== this.#runs) return;
      this.#cubeStatus.textContent = `${plural(view.rows.rowCount, 'row')} in ${Math.round(view.rows.elapsedMs)} ms`;
    }).then((cube) => {
      if (mine !== this.#runs) { cube?.dispose(); return; }
      this.#opening = undefined;
      if (!cube) { void run(this.#app, session, this.#limit); return; }
      this.#cube = { app: cube, host };
      session.setRun({ status: 'cube', queryHash });
      // the first query: the cube runs (and re-runs) the rest itself
      void cube.open();
    }, (e: unknown) => {
      if (mine !== this.#runs) return;
      this.#opening = undefined;
      session.setRun({ status: 'error', message: e instanceof Error ? e.message : String(e) });
    });
  }

  /** Let go of the cube: its document listeners and any query in flight. */
  dispose(): void {
    this.#runs += 1;
    this.#closeCube();
  }

  /** Is the query a graph fetch built in the form (its answer is objects, never a grid)? */
  #objects(): boolean {
    return this.#session.query.graph !== undefined && !this.#session.text;
  }

  /** Show a relation's rows the other way; what is on screen is shown again, that way. */
  #show(view: 'grid' | 'cube'): void {
    if (view === this.#view) return;
    this.#view = view;
    const r = this.#session.run;
    if (r.status === 'done' || r.status === 'cube') this.run();
    else this.render();
  }

  #closeCube(): void {
    this.#cube?.app.dispose();
    this.#cube = undefined;
    this.#opening = undefined;
  }

  render(): void {
    const r = this.#session.run;
    const running = r.status === 'running';
    const limitInput = h('input', {
      class: 'q-input', type: 'number', min: '1', value: String(this.#limit), style: 'width:70px', title: 'Rows to preview', 'aria-label': 'Preview row limit',
      onchange: () => { const n = Number(limitInput.value); if (Number.isInteger(n) && n > 0) this.#limit = n; },
    });
    const status: (Node | string)[] = [];
    if (running) status.push(h('span', { class: 'q-spinner' }), ' Running…');
    else if (r.status === 'done') {
      const n = isTds(r.result) ? r.result.result.rows.length : jsonCount(r.result);
      const over = isTds(r.result) && r.limit !== undefined && n > r.limit;
      status.push(h('span', null, `${plural(over ? r.limit! : n, isTds(r.result) ? 'row' : 'object')} in ${r.ms} ms`));
      if (over) status.push(h('span', { class: 'q-chip', style: 'color:var(--warn)' }, `showing the first ${r.limit} — more exist`));
    }
    else if (r.status === 'cube') status.push(this.#cubeStatus);
    if (this.#session.stale) status.push(h('span', { class: 'q-chip', style: 'color:var(--warn)' }, 'the query changed since — run again'));
    const stop = (): void => {
      if (this.#opening) {
        this.dispose();
        this.#session.setRun({ status: 'error', message: 'Stopped.' });
      } else if (r.status === 'running') r.abort.abort();
    };
    // a DataCube pages its rows itself; the preview limit is for the plain grid and objects
    const objects = this.#objects();
    // upstream's results header (QueryBuilderResultPanel): "results", the SQL, the row count and
    // time; at the right the preview row limit, Run Query (green), Export
    const preview = this.#view === 'grid' || objects
      ? [h('span', { class: 'q-labelled' }, h('span', { class: 'q-labelled__label' }, 'preview row limit'), limitInput)]
      : [];
    const viewTab = (view: 'grid' | 'cube', label: string, title: string): HTMLElement => h('button', {
      class: `q-mode${this.#view === view ? ' on' : ''}`, title, onclick: () => this.#show(view),
    }, label);
    const views = objects ? [] : [h('span', { class: 'q-modes', role: 'group', 'aria-label': 'Show rows as' },
      viewTab('grid', 'Grid', 'The rows, plainly: sort, copy, filter by a value'),
      viewTab('cube', 'DataCube', 'The rows in a DataCube: group, pivot, format, chart'))];
    const bar = panelHeader('results', [
      h('button', { class: 'q-panel__action q-panel__action--text', title: 'Show the executed SQL', onclick: () => void this.#showSql() }, 'SQL'),
      h('span', { class: 'q-results__analytics' }, status),
      ...views,
    ], [
      ...preview,
      running
        ? h('button', { class: 'q-run q-run--stop', onclick: stop }, 'Stop')
        : h('button', { class: 'q-run', title: 'Run Query (Ctrl+Enter)', onclick: () => this.run() }, icon('play'), 'Run Query'),
      menuButton(['Export', icon('caretDown')], () => [
        { label: 'CSV (every row)', action: () => void this.#exportCsv() },
      ], { class: 'q-export', title: 'Export the result' }),
    ]);
    bar.classList.add('q-results-bar');
    let body: Node;
    if (r.status === 'error') body = h('div', { style: 'padding:12px' }, h('div', { class: 'q-error-box' }, r.message));
    else if (r.status === 'cube' && this.#cube) body = this.#cube.host;
    else if (r.status === 'done') body = isTds(r.result) ? this.#grid(r.result, r.limit) : h('pre', { class: 'q-json' }, JSON.stringify(r.result.values, null, 2));
    else if (running && this.#opening) body = this.#opening;
    else if (running) body = h('div', { class: 'q-hint' }, 'Running…');
    else body = h('div', { class: 'q-hint' }, 'Run the query to see its rows (Ctrl+Enter).');
    // a cube already on screen stays put (its scroll, its open groups): only the bar is redrawn
    const shown = this.element.firstElementChild;
    if (body === this.#cube?.host && shown && body.parentElement?.parentElement === this.element) {
      shown.replaceWith(bar);
      return;
    }
    mount(this.element, bar, h('div', { class: 'q-results-body' }, body));
  }

  #grid(t: TdsResult, limit: number | undefined): HTMLElement {
    const cols = t.result.columns;
    const types = new Map(t.builder.columns.map((c) => [c.name, c.type ?? '']));
    let rows = t.result.rows.slice(0, limit ?? t.result.rows.length).map((r) => r.values);
    const s = this.#sort;
    if (s) {
      rows = [...rows].sort((a, b) => {
        const x = a[s.column], y = b[s.column];
        if (x === y) return 0;
        if (x === null || x === undefined) return 1;
        if (y === null || y === undefined) return -1;
        return (x < y ? -1 : 1) * s.dir;
      });
    }
    const numeric = cols.map((c) => isNumericFamily(primitiveFamily(standardPrimitive(types.get(c) ?? ''))));
    const table = h('table', { class: 'q-grid', tabindex: 0,
      onkeydown: (e: KeyboardEvent) => {
        if ((e.metaKey || e.ctrlKey) && e.key === 'c' && this.#selected) {
          void navigator.clipboard?.writeText(cellText(rows[this.#selected.row]?.[this.#selected.col]));
          toast('Copied');
        }
      } },
    h('thead', null, h('tr', null, cols.map((c, i) => h('th', {
      title: `${c}${types.get(c) ? ` : ${types.get(c)}` : ''} — click to sort`,
      onclick: () => {
        this.#sort = this.#sort?.column === i ? (this.#sort.dir === 1 ? { column: i, dir: -1 } : undefined) : { column: i, dir: 1 };
        this.render();
      },
    }, c, s?.column === i ? (s.dir === 1 ? ' ↑' : ' ↓') : '')))),
    h('tbody', null, rows.map((row, ri) => h('tr', null, row.map((v, ci) => {
      const td = h('td', {
        class: [numeric[ci] ? 'num' : '', v === null ? 'null' : '', this.#selected?.row === ri && this.#selected.col === ci ? 'sel' : ''].join(' '),
        title: v === null ? 'null' : cellText(v),
        onclick: () => { this.#selected = { row: ri, col: ci }; table.querySelectorAll('td.sel').forEach((x) => x.classList.remove('sel')); td.classList.add('sel'); },
        oncontextmenu: (e: MouseEvent) => { e.preventDefault(); this.#cellMenu(e, cols, row, ci); },
      }, v === null ? 'null' : cellText(v));
      return td;
    })))));
    return table;
  }

  /** Filter by (or out) a cell's value: a condition on the column's property, when it has one. */
  #cellMenu(e: MouseEvent, cols: readonly string[], row: readonly unknown[], ci: number): void {
    const session = this.#session;
    const col = session.text ? undefined : session.query.columns.find((c) => c.name === cols[ci] && c.aggregate === undefined);
    const v = row[ci];
    const filterBy = (negate: boolean): void => {
      if (!col) return;
      const graph = session.project.graph;
      const { prop } = propertyAt(graph, session.query.source.class, col.path);
      let value: Value | undefined;
      if (graph.enumerations.has(prop.type)) value = { kind: 'enum', enumeration: prop.type, value: String(v) };
      else {
        switch (standardPrimitive(prop.type)) {
          case 'String': value = { kind: 'string', value: String(v) }; break;
          case 'Integer': value = { kind: 'integer', value: String(v) }; break;
          case 'Float': case 'Number': value = { kind: 'float', value: String(v) }; break;
          case 'Decimal': value = { kind: 'decimal', value: String(v) }; break;
          case 'Boolean': value = { kind: 'boolean', value: v === true }; break;
          case 'StrictDate': case 'Date': value = { kind: 'strictDate', value: String(v).slice(0, 10) }; break;
          case 'DateTime': value = { kind: 'dateTime', value: String(v).replace(' ', 'T').slice(0, 19) }; break;
          default: value = undefined;
        }
      }
      const c: Condition | undefined = v === null || v === undefined
        ? { kind: 'condition', id: freshId('c'), path: col.path, operator: negate ? 'isNotEmpty' : 'isEmpty' }
        : value ? { kind: 'condition', id: freshId('c'), path: col.path, operator: negate ? 'notEqual' : 'equal', value } : undefined;
      if (!c) { toast('This column cannot be filtered by value.'); return; }
      session.update((q) => (q.filter
        ? { ...q, filter: mapGroup(q.filter, q.filter.id, (g) => ({ ...g, children: [...g.children, c] })) }
        : { ...q, filter: { kind: 'group', id: freshId('g'), op: 'and', children: [c] } }));
      this.run();
    };
    showMenu(e.clientX, e.clientY, [
      { label: `Filter by ${v === null ? 'empty' : `“${cellText(v)}”`}`, action: () => filterBy(false), disabled: !col },
      { label: `Filter out ${v === null ? 'empty' : `“${cellText(v)}”`}`, action: () => filterBy(true), disabled: !col },
      'separator',
      { label: 'Copy value', action: () => void navigator.clipboard?.writeText(cellText(v)) },
      { label: 'Copy row', action: () => void navigator.clipboard?.writeText(row.map(cellText).join('\t')) },
      { label: 'Copy row with headers', action: () => void navigator.clipboard?.writeText(`${cols.join('\t')}\n${row.map(cellText).join('\t')}`) },
    ]);
  }

  async #showSql(): Promise<void> {
    const pre = h('pre', { class: 'q-json', style: 'white-space:pre-wrap' }, 'Planning…');
    dialog('Executed SQL', (d) => ({
      body: pre,
      foot: [
        h('button', { class: 'q-btn', onclick: () => void navigator.clipboard?.writeText(pre.textContent ?? '').then(() => toast('Copied')) }, 'Copy'),
        h('button', { class: 'q-btn primary', onclick: () => d.close() }, 'Close'),
      ],
    }), { wide: true });
    try {
      pre.textContent = formatSql(await sqlOf(this.#app, this.#session));
    } catch (e) {
      pre.textContent = (e as Error).message;
      pre.classList.add('q-error');
    }
  }

  /** Every row (no preview limit), as CSV written here from the engine's result. */
  async #exportCsv(): Promise<void> {
    const missing = missingValues(this.#session);
    if (missing.length > 0) { toast(`Set a value for ${missing.map((p) => `$${p.name}`).join(', ')} first.`); return; }
    toast('Exporting…');
    try {
      const l = executionLambda(this.#session, undefined);
      const result = await this.#app.engine.execute(executeInput(this.#session, l));
      if (!isTds(result)) { toast('Only tabular results export as CSV.'); return; }
      const esc = (v: unknown): string => {
        const t = v === null || v === undefined ? '' : typeof v === 'object' ? JSON.stringify(v) : String(v);
        return /[",\n]/.test(t) ? `"${t.replace(/"/g, '""')}"` : t;
      };
      const csv = [result.result.columns.map(esc).join(','), ...result.result.rows.map((r) => r.values.map(esc).join(','))].join('\n');
      const a = h('a', { href: URL.createObjectURL(new Blob([csv], { type: 'text/csv' })), download: `${this.#session.saved?.name ?? 'query'}.csv` });
      a.click();
      setTimeout(() => URL.revokeObjectURL(a.href), 5000);
    } catch (e) {
      toast(`Export failed: ${(e as Error).message}`, 5000);
    }
  }
}

function jsonCount(r: ExecutionResult): number {
  const v = (r as { values: unknown }).values;
  return Array.isArray(v) ? v.length : v === null || v === undefined ? 0 : 1;
}

/** SQL with a line per clause, for reading. */
export function formatSql(sql: string): string {
  return sql
    .replace(/\s+(from|where|group by|order by|having|limit|offset|left outer join|inner join|left join|join|union all|union)\s+/gi, (_m, kw: string) => `\n${kw.toUpperCase()} `)
    .replace(/^select\s+/i, 'SELECT ');
}
