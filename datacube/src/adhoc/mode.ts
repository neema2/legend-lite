// Ad Hoc Analysis mode, on screen: the grid the session answers, the POV
// bar above it, and the operations on its members -- double-click to
// zoom in, the right-click menu for the rest, the Member Selection and
// Options windows.
//
// The app owns the windows and the switch in and out; this owns what
// is drawn inside the mode and turns a gesture into ONE session call.
// Every answer is the session's (queried through the cube's runner),
// so nothing here computes a number.

import { buildColumnModel, type ColumnModel } from '../grid/columns.ts';
import { DataGrid } from '../grid/grid.ts';
import type { FormatterCache } from '../format.ts';
import { bounds, type CellRange } from '../selection.ts';
import type { MenuGroup, MenuItem } from '../ui/menu.ts';
import { MenuView } from '../ui/menu-view.ts';
import type { WindowOptions } from '../ui/window.ts';
import { buildMemberSelection } from './member-selection.ts';
import { buildOptionsPanel } from './options-panel.ts';
import {
  memberLabel,
  memberOfSegment,
  segmentLabel,
  type AdHocView,
} from './query.ts';
import type { AdHocSession } from './session.ts';
import { MEASURES, type Axis, type MemberPath, type ZoomLevel } from './state.ts';

export interface AdHocModeOptions {
  readonly formatters: FormatterCache;
  readonly rowHeight: number;
  /** Open a window in the app (its title is its identity). */
  readonly showWindow: (
    title: string,
    build: (host: HTMLElement, close: () => void) => void,
    size?: WindowOptions,
  ) => void;
  /** A task on the status bar; the returned function ends it. */
  readonly startTask: (description: string) => () => void;
  readonly status: (text: string, kind?: 'ok' | 'warn' | 'error') => void;
  readonly reportFailure: (error: unknown) => void;
  /** Leave the mode: the app puts the cube's own grid back. */
  readonly onExit: () => void;
  readonly writeClipboard?: (text: string) => void | Promise<void>;
}

/** The member a gesture is on: which dimension, which member, which axis. */
interface Target {
  readonly dimension: string;
  readonly member: MemberPath;
  readonly axis: Axis | 'pov';
}

export const MEMBER_SELECTION_WINDOW: WindowOptions = {
  width: 640, height: 480, minWidth: 420, minHeight: 300,
};
export const OPTIONS_WINDOW: WindowOptions = {
  width: 420, height: 520, minWidth: 320, minHeight: 360,
};

export class AdHocMode {
  readonly session: AdHocSession;
  readonly #options: AdHocModeOptions;
  readonly #doc: Document;
  readonly #root: HTMLElement;
  readonly #pov: HTMLElement;
  readonly #grid: DataGrid;
  readonly #menu: MenuView;
  #model: ColumnModel | null = null;
  #selection: CellRange | null = null;
  #busy = 0;

  constructor(host: HTMLElement, session: AdHocSession, options: AdHocModeOptions) {
    this.session = session;
    this.#options = options;
    this.#doc = host.ownerDocument;
    this.#root = host;
    host.classList.add('dc-adhoc');
    this.#pov = this.#div(host, 'dc-adhoc-pov');
    this.#pov.setAttribute('role', 'toolbar');
    this.#pov.setAttribute('aria-label', 'Point of view');
    const gridHost = this.#div(host, 'dc-adhoc-grid');
    this.#grid = new DataGrid(gridHost, options.formatters, {
      rowHeight: options.rowHeight,
      autoFit: true,
      // Double-click or Enter on a row member zooms in on it.
      onActivateCell: (row, column) => this.#onActivateCell(row, column),
      onDoubleClickCell: (row, column) => this.#onActivateCell(row, column),
      onHeaderActivate: (path, level) => this.#onHeaderActivate(path, level),
      onSelectionChange: (range) => {
        this.#selection = range;
      },
      ...(options.writeClipboard ? { writeClipboard: options.writeClipboard } : {}),
    });
    this.#menu = new MenuView(this.#doc, { onSelect: (item) => this.#onMenu(item) });
    gridHost.addEventListener('contextmenu', (event) => this.#onContextMenu(event));
    this.#paintPov();
  }

  /** Query the grid as it stands (the first paint, and Refresh). */
  refresh(): Promise<void> {
    return this.#run('Refreshing...', () => this.session.refresh());
  }

  undo(): Promise<void> {
    if (!this.session.canUndo) {
      this.#options.status('Nothing to undo', 'warn');
      return Promise.resolve();
    }
    return this.#run('Undo...', () => this.session.undo());
  }

  redo(): Promise<void> {
    if (!this.session.canRedo) {
      this.#options.status('Nothing to redo', 'warn');
      return Promise.resolve();
    }
    return this.#run('Redo...', () => this.session.redo());
  }

  get view(): AdHocView | null {
    return this.session.view;
  }

  get busy(): boolean {
    return this.#busy > 0;
  }

  destroy(): void {
    this.#menu.close();
    this.#grid.destroy();
    this.#root.replaceChildren();
    this.#root.classList.remove('dc-adhoc');
  }

  // -- running one step ---------------------------------------------------

  /**
   * One session step, with a task on the status bar while it queries.
   * A failure is reported and leaves the grid on the last answer; a
   * step overtaken by a newer one paints nothing.
   */
  async #run(task: string, step: () => Promise<AdHocView | null>): Promise<void> {
    const end = this.#options.startTask(task);
    this.#busy += 1;
    this.#grid.setBusy(true);
    try {
      const view = await step();
      if (view) this.#paint(view);
      this.#paintPov();
      if (this.session.grid.options.navigateWithoutData && view !== this.session.view) {
        this.#options.status('Navigating without data -- Refresh to query', 'warn');
      }
    } catch (error) {
      this.#options.reportFailure(error);
    } finally {
      this.#busy -= 1;
      if (this.#busy === 0) this.#grid.setBusy(false);
      end();
    }
  }

  #paint(view: AdHocView): void {
    const columnDims = this.session.grid.columns.length;
    const model = buildColumnModel(
      view.table,
      view.rowDimensions,
      view.lastSegments,
      { headerLabel: segmentLabel },
      Math.max(0, columnDims - 1),
    );
    this.#model = model;
    this.#grid.setColumns(model);
    this.#grid.setRows(view.table, 0, view.table.rowCount);
    const pending = this.session.grid.options.navigateWithoutData ? ' (not refreshed)' : '';
    this.#options.status(
      `${view.table.rowCount} rows, ${view.columnTuples.length} columns${pending}`);
  }

  // -- the POV bar ------------------------------------------------------------

  #paintPov(): void {
    this.#pov.replaceChildren();
    const label = this.#div(this.#pov, 'dc-adhoc-pov-label');
    label.textContent = 'POV';
    const pov = this.session.grid.pov;
    const names = Object.keys(pov);
    if (names.length === 0) {
      const none = this.#div(this.#pov, 'dc-adhoc-pov-empty');
      none.textContent = 'Every dimension is on the grid';
    }
    for (const dimension of names) {
      const member = pov[dimension] as MemberPath;
      const chip = this.#doc.createElement('button');
      chip.type = 'button';
      chip.className = 'dc-adhoc-pov-chip';
      chip.dataset['dimension'] = dimension;
      chip.title = `${dimension}: click to choose its member; right-click to move it onto the grid`;
      const name = this.#doc.createElement('span');
      name.className = 'dc-adhoc-pov-dim';
      name.textContent = dimension;
      const value = this.#doc.createElement('span');
      value.className = 'dc-adhoc-pov-member';
      value.textContent = memberLabel(dimension, member);
      chip.append(name, value);
      chip.addEventListener('click', () => this.openMemberSelection(dimension, 'pov'));
      chip.addEventListener('contextmenu', (event) => {
        event.preventDefault();
        this.#menu.show(this.#povMenu(dimension, member), event.clientX, event.clientY);
      });
      this.#pov.append(chip);
    }
    const tools = this.#div(this.#pov, 'dc-adhoc-pov-tools');
    const button = (text: string, title: string, onClick: () => void): void => {
      const b = this.#doc.createElement('button');
      b.type = 'button';
      b.className = 'dc-adhoc-tool';
      b.textContent = text;
      b.title = title;
      b.addEventListener('click', onClick);
      tools.append(b);
    };
    button('Refresh', 'Query the grid as it stands', () => void this.refresh());
    button('Options...', 'Zoom, suppression and indentation', () => this.openOptions());
    button('Exit', 'Back to the cube', () => this.#options.onExit());
  }

  #povMenu(dimension: string, member: MemberPath): MenuGroup[] {
    const target = { dimension, member, axis: 'pov' as const };
    return [
      {
        label: dimension,
        items: [
          { id: 'adhoc.members', label: 'Member Selection...', adhoc: target },
          { id: 'adhoc.povToRows', label: 'Move to Rows', adhoc: target },
          { id: 'adhoc.povToColumns', label: 'Move to Columns', adhoc: target },
        ],
      },
      this.#generalGroup(),
    ];
  }

  // -- gestures on the grid -----------------------------------------------

  /** A row member double-clicked zooms in on it, at the Zoom In level. */
  #onActivateCell(row: number, column: number): void {
    const view = this.session.view;
    if (!view || column >= view.rowDimensions.length) return;
    const dimension = view.rowDimensions[column] as string;
    const member = view.rowTuples[row]?.[column];
    if (member) void this.#zoomIn(dimension, member);
  }

  /** A column member's header double-clicked zooms in on it. */
  #onHeaderActivate(path: readonly string[], level: number): void {
    const target = this.#columnTarget(path, level);
    if (target) void this.#zoomIn(target.dimension, target.member);
  }

  /**
   * The column member a header cell shows, or null for a row
   * dimension's header. Checked against the tuples the grid SHOWS, so a
   * member value that happens to spell a dimension name is still read
   * as the member it is.
   */
  #columnTarget(path: readonly string[], level: number): Target | null {
    const view = this.session.view;
    const columns = this.session.grid.columns;
    const dimension = columns[level]?.dimension;
    if (!view || dimension === undefined || path.length !== level + 1) return null;
    const members = path.map(memberOfSegment);
    const shown = view.columnTuples.some((t) =>
      members.every((m, i) => same(t[i] ?? null, m)));
    return shown ? { dimension, member: members[level] as MemberPath, axis: 'columns' } : null;
  }

  #rowTarget(row: number, column: number): Target | null {
    const view = this.session.view;
    if (!view || column < 0 || column >= view.rowDimensions.length) return null;
    const member = view.rowTuples[row]?.[column];
    return member
      ? { dimension: view.rowDimensions[column] as string, member, axis: 'rows' }
      : null;
  }

  #onContextMenu(event: MouseEvent): void {
    event.preventDefault();
    const el = event.target as Element | null;
    if (!el || typeof el.closest !== 'function') return;
    let target: Target | null = null;
    const th = el.closest<HTMLElement>('.dc-th');
    if (th) {
      const segments = th.dataset['segments'];
      const level = Number(th.dataset['level'] ?? '-1');
      if (segments) target = this.#columnTarget(JSON.parse(segments) as string[], level);
      // A row dimension's own header: the dimension at its top member.
      const column = th.dataset['column'];
      if (!target && column && this.session.view?.rowDimensions.includes(column)) {
        target = { dimension: column, member: [], axis: 'rows' };
      }
    } else {
      const cell = el.closest<HTMLElement>('.dc-cell');
      const row = cell?.closest<HTMLElement>('.dc-row');
      const column = cell?.dataset['column'];
      const view = this.session.view;
      if (cell && row && column !== undefined && view && this.#model) {
        const abs = Number(row.getAttribute('aria-rowindex') ?? '0') - this.#model.depth - 1;
        target = this.#rowTarget(abs, view.rowDimensions.indexOf(column));
      }
    }
    const groups = target ? [this.#memberGroup(target), this.#generalGroup()]
      : [this.#generalGroup()];
    this.#menu.show(groups, event.clientX, event.clientY);
  }

  /** The members selected with `target` on its row dimension; itself when none. */
  #selectedWith(target: Target): MemberPath[] {
    const view = this.session.view;
    if (target.axis !== 'rows' || !view || !this.#selection) return [target.member];
    const d = view.rowDimensions.indexOf(target.dimension);
    const b = bounds(this.#selection);
    if (d < b.left || d > b.right) return [target.member];
    const picked: MemberPath[] = [];
    for (let r = b.top; r <= b.bottom; r++) {
      const m = view.rowTuples[r]?.[d];
      if (m && !picked.some((p) => same(p, m))) picked.push(m);
    }
    return picked.some((p) => same(p, target.member)) ? picked : [target.member];
  }

  #memberGroup(target: Target): MenuGroup {
    const deepest = this.session.deepest(target.dimension);
    const measures = target.dimension === MEASURES;
    const canZoomIn = !measures && target.member.length < deepest;
    const canZoomOut = !measures && target.member.length > 0;
    // An axis keeps at least one dimension, so the last one cannot leave.
    const axis = target.axis === 'pov' ? [] : this.session.grid[target.axis];
    const canLeave = axis.length > 1;
    const selected = this.#selectedWith(target);
    const withTarget = { ...target, selected };
    const off = (b: boolean): { disabled?: true } => (b ? {} : { disabled: true });
    const items: MenuItem[] = [
      {
        label: 'Zoom In',
        ...off(canZoomIn),
        submenu: [
          { id: 'adhoc.zoomNext', label: 'Next Level', adhoc: withTarget, ...off(canZoomIn) },
          { id: 'adhoc.zoomAll', label: 'All Levels', adhoc: withTarget, ...off(canZoomIn) },
          { id: 'adhoc.zoomBottom', label: 'Bottom Level', adhoc: withTarget, ...off(canZoomIn) },
        ],
      },
      { id: 'adhoc.zoomOut', label: 'Zoom Out', adhoc: withTarget, ...off(canZoomOut) },
      {
        id: 'adhoc.keepOnly',
        label: selected.length > 1 ? `Keep Only (${selected.length})` : 'Keep Only',
        adhoc: withTarget,
      },
      {
        id: 'adhoc.removeOnly',
        label: selected.length > 1 ? `Remove Only (${selected.length})` : 'Remove Only',
        adhoc: withTarget,
      },
      { id: 'adhoc.members', label: 'Member Selection...', adhoc: withTarget },
      {
        id: 'adhoc.pivot',
        label: target.axis === 'rows' ? 'Pivot to Columns' : 'Pivot to Rows',
        adhoc: withTarget,
        ...off(canLeave),
      },
      { id: 'adhoc.pivotToPov', label: 'Pivot to POV', adhoc: withTarget, ...off(canLeave) },
    ];
    return { label: `${target.dimension}: ${memberLabel(target.dimension, target.member)}`, items };
  }

  #generalGroup(): MenuGroup {
    return {
      label: '',
      items: [
        { id: 'adhoc.refresh', label: 'Refresh' },
        { id: 'adhoc.undo', label: 'Undo', ...(this.session.canUndo ? {} : { disabled: true }) },
        { id: 'adhoc.redo', label: 'Redo', ...(this.session.canRedo ? {} : { disabled: true }) },
        { id: 'adhoc.options', label: 'Options...' },
        { id: 'adhoc.exit', label: 'Exit Ad Hoc Analysis' },
      ],
    };
  }

  #onMenu(item: MenuItem): void {
    const t = item.adhoc;
    const s = this.session;
    switch (item.id) {
      case 'adhoc.zoomNext':
      case 'adhoc.zoomAll':
      case 'adhoc.zoomBottom':
        if (t?.member) {
          const level: ZoomLevel = item.id === 'adhoc.zoomNext' ? 'next'
            : item.id === 'adhoc.zoomAll' ? 'all' : 'bottom';
          void this.#zoomIn(t.dimension, t.member, level);
        }
        return;
      case 'adhoc.zoomOut':
        if (t?.member) void this.#run('Zooming out...', () => s.zoomOut(t.dimension, t.member as MemberPath));
        return;
      case 'adhoc.keepOnly':
        if (t) void this.#run('Keep Only...', () => s.keepOnly(t.dimension, t.selected ?? [t.member ?? []]));
        return;
      case 'adhoc.removeOnly':
        if (t) void this.#run('Remove Only...', () => s.removeOnly(t.dimension, t.selected ?? [t.member ?? []]));
        return;
      case 'adhoc.pivot':
        if (t) void this.#run('Pivoting...', () => s.pivot(t.dimension));
        return;
      case 'adhoc.pivotToPov':
        if (t) void this.#run('Pivoting to POV...', () => s.pivotToPov(t.dimension));
        return;
      case 'adhoc.povToRows':
      case 'adhoc.povToColumns':
        if (t) {
          const axis: Axis = item.id === 'adhoc.povToRows' ? 'rows' : 'columns';
          void this.#run('Moving onto the grid...', () => s.povToAxis(t.dimension, axis));
        }
        return;
      case 'adhoc.members':
        if (t) this.openMemberSelection(t.dimension, s.grid.pov[t.dimension] ? 'pov' : 'axis');
        return;
      case 'adhoc.refresh':
        void this.refresh();
        return;
      case 'adhoc.undo':
        void this.undo();
        return;
      case 'adhoc.redo':
        void this.redo();
        return;
      case 'adhoc.options':
        this.openOptions();
        return;
      case 'adhoc.exit':
        this.#options.onExit();
        return;
      default:
    }
  }

  #zoomIn(dimension: string, member: MemberPath, level?: ZoomLevel): Promise<void> {
    if (dimension === MEASURES || member.length >= this.session.deepest(dimension)) {
      this.#options.status(`${memberLabel(dimension, member)} has no members beneath it`, 'warn');
      return Promise.resolve();
    }
    return this.#run('Zooming in...', () => this.session.zoomIn(dimension, member, level));
  }

  // -- windows ------------------------------------------------------------

  /**
   * Member Selection for a dimension: many members for an axis, ONE for
   * the POV. Its tree is the source's members, looked up as it opens.
   */
  openMemberSelection(dimension: string, place: 'axis' | 'pov'): void {
    const s = this.session;
    const grid = s.grid;
    const onAxis = [...grid.rows, ...grid.columns].find((a) => a.dimension === dimension);
    const selected = place === 'pov'
      ? [grid.pov[dimension] ?? []]
      : [...(onAxis?.members ?? [])];
    this.#options.showWindow(`Member Selection: ${dimension}`, (host, close) =>
      buildMemberSelection(host, {
        dimension,
        mode: place === 'pov' ? 'one' : 'many',
        selected,
        deepest: s.deepest(dimension),
        members: (under, depth) => s.members(dimension, under, depth),
        onFailure: (e) => this.#options.reportFailure(e),
        onOk: (picked) => {
          close();
          if (place === 'pov') {
            const one = picked[0];
            if (one) void this.#run('Setting the POV...', () => s.setPov(dimension, one));
          } else {
            void this.#run('Selecting members...', () => s.selectMembers(dimension, picked));
          }
        },
        onClose: close,
      }), MEMBER_SELECTION_WINDOW);
  }

  openOptions(): void {
    this.#options.showWindow('Ad Hoc Options', (host, close) =>
      buildOptionsPanel(host, {
        values: this.session.grid.options,
        onOk: (patch) => {
          close();
          void this.#run('Applying options...', () => this.session.setOptions(patch));
        },
        onClose: close,
      }), OPTIONS_WINDOW);
  }

  #div(parent: HTMLElement, className: string): HTMLElement {
    const el = this.#doc.createElement('div');
    el.className = className;
    parent.append(el);
    return el;
  }
}

function same(a: MemberPath | null, b: MemberPath): boolean {
  return a !== null && a.length === b.length && a.every((v, i) => b[i] === v);
}
