// The chart window: a chart of the cube, and the options that shape it.
//
// Left, the options (mark, what goes across, what is plotted and how it is
// aggregated, the split, orientation, stacking, order, how many, labels,
// legend); right, the chart. The chart follows the cube: its query is the
// cube's own, regrouped (`chart-spec.ts`), so a filter added to the grid
// narrows the chart, and a click on a mark (a bar, a slice, a cell) adds
// that mark's values to the cube's filter.

import {
  CHART_MARKS,
  chartColumns,
  chartProblems,
  chartQuery,
  defaultChart,
  followCube,
  type ChartMark,
  type ChartSpec,
} from '../chart-spec.ts';
import { chartOption, themeOf, treemapOfGrid, type GridShown, type LabelOf, type MarkKey } from '../chart-option.ts';
// ECharts (about 218 KB gzipped) is loaded the first time a chart draws, never by a grid alone (plan F7)
import type { ChartPicture, MountedChart } from '../chart-render.ts';
import type { ResultTable } from '../../../engine-client/src/result.ts';
import type { AggregateFn, CubeSnapshot } from '../snapshot.ts';
import type { Lambda } from '../../../pure-protocol/src/index.ts';
import { checkbox, dropdown, field, numberInput } from './form.ts';
import { aggregatesFor } from './panel-column.ts';

export interface ChartPanelOptions {
  /** The cube as it is NOW (the chart follows it). */
  readonly snapshot: () => CubeSnapshot;
  /** Run a query the cube did not plan, through the cube's own runner. */
  readonly run: (query: Lambda, snapshot: CubeSnapshot, signal: AbortSignal) => Promise<ResultTable>;
  /** How a value is written: the grid's formatter. */
  readonly label: LabelOf;
  /** A mark was clicked: filter the cube to its values. */
  readonly onPick: (key: MarkKey) => void;
  /** Where to start; the cube's own grouping suggests one otherwise. */
  readonly initial?: ChartSpec;
  /** Upstream-style debounce between an edit and the query; a test passes 0. */
  readonly debounceMs?: number;
  /** Whether the options start open (a chart tile starts with the chart alone). */
  readonly formOpen?: boolean;
  /** The chart froze or went live (by the Freeze button, or by an edit to its grouping). */
  readonly onFrozen?: (frozen: boolean) => void;
  /** The user changed the chart (an option in its form). */
  readonly onSpec?: () => void;
  /** What the grid shows now: a treemap draws exactly that, not a query of its own. */
  readonly shown?: () => GridShown | null;
}

export class ChartPanel {
  readonly #root: HTMLElement;
  readonly #doc: Document;
  readonly #options: ChartPanelOptions;
  #spec: ChartSpec | null;
  #form!: HTMLElement;
  #canvas!: HTMLElement;
  #status!: HTMLElement;
  #chart: MountedChart | null = null;
  #timer: ReturnType<typeof setTimeout> | undefined;
  #inflight: AbortController | null = null;
  #disposed = false;
  /** A treemap's rows as last drawn: what a frozen one keeps. */
  #shownKept: GridShown | null = null;

  constructor(root: HTMLElement, options: ChartPanelOptions) {
    this.#root = root;
    this.#doc = root.ownerDocument;
    this.#options = options;
    this.#spec = options.initial ?? defaultChart(options.snapshot());
    this.#build();
    this.refresh(0);
  }

  /** The spec being drawn (to save it, later). */
  get spec(): ChartSpec | null {
    return this.#spec;
  }

  /** Whether the chart keeps its own grouping, or follows the cube's pivots. */
  get frozen(): boolean {
    return this.#spec?.frozen === true;
  }

  /** Freeze the chart's grouping, or make it live again (it regroups the way the cube is now). */
  setFrozen(frozen: boolean): void {
    if (!this.#spec || this.frozen === frozen) return;
    const { frozen: _was, ...rest } = this.#spec;
    this.#spec = frozen ? { ...rest, frozen: true } : rest;
    this.#options.onFrozen?.(frozen);
    this.refresh(0);
  }

  /** Draw `spec` instead (a pinned chart updated from the live one): its form follows. */
  setSpec(spec: ChartSpec): void {
    const wasFrozen = this.frozen;
    this.#spec = spec;
    this.#paintForm();
    if (this.frozen !== wasFrozen) this.#options.onFrozen?.(this.frozen);
    this.refresh(0);
  }

  /** The cube changed (a new filter, a new pivot, a new calculated column): draw again, regrouped if live. */
  refresh(delay = this.#options.debounceMs ?? 150): void {
    if (this.#spec) {
      const followed = followCube(this.#spec, this.#options.snapshot());
      if (followed !== this.#spec) {
        this.#spec = followed;
        this.#paintForm();
      }
    }
    clearTimeout(this.#timer);
    this.#timer = setTimeout(() => void this.#draw(), delay);
  }

  /** Show or hide the options; answers whether they are now shown. */
  toggleForm(): boolean {
    this.#form.hidden = !this.#form.hidden;
    return !this.#form.hidden;
  }

  dispose(): void {
    this.#disposed = true;
    clearTimeout(this.#timer);
    this.#inflight?.abort();
    this.#chart?.dispose();
    this.#chart = null;
  }

  // -- layout ---------------------------------------------------------------

  #build(): void {
    const doc = this.#doc;
    this.#root.replaceChildren();
    this.#root.classList.add('dc-chartpanel');
    this.#form = doc.createElement('div');
    this.#form.className = 'dc-chartpanel-form';
    const right = doc.createElement('div');
    right.className = 'dc-chartpanel-main';
    this.#canvas = doc.createElement('div');
    this.#canvas.className = 'dc-chartpanel-chart';
    this.#canvas.setAttribute('role', 'img');
    this.#status = doc.createElement('div');
    this.#status.className = 'dc-chartpanel-status';
    this.#status.setAttribute('role', 'status');
    right.append(this.#canvas, this.#status);
    this.#root.append(this.#form, right);
    this.#form.hidden = this.#options.formOpen === false;
    this.#paintForm();
  }

  #set(change: Partial<ChartSpec>, redrawForm = false): void {
    if (!this.#spec) return;
    // a hand on the grouping of a live chart freezes it: the next pivot
    // change would otherwise undo the choice
    const grouping = 'x' in change || 'y' in change || 'split' in change;
    const freeze = grouping && !this.frozen && this.#spec.mark !== 'scatter';
    this.#spec = { ...this.#spec, ...change, ...(freeze ? { frozen: true } : {}) };
    if (freeze) this.#options.onFrozen?.(true);
    this.#options.onSpec?.();
    if (redrawForm) this.#paintForm();
    this.refresh();
  }

  #setOption<K extends keyof ChartSpec['options']>(key: K, value: ChartSpec['options'][K]): void {
    if (!this.#spec) return;
    this.#set({ options: { ...this.#spec.options, [key]: value } });
  }

  #paintForm(): void {
    const doc = this.#doc;
    const form = this.#form;
    form.replaceChildren();
    const s = this.#options.snapshot();
    const spec = this.#spec;
    if (!spec) {
      form.textContent = 'This cube has no column to chart.';
      return;
    }
    const { dimensions, measures } = chartColumns(s);
    const dims = dimensions.map((c) => ({ value: c.name, label: c.name }));
    const scatter = spec.mark === 'scatter';
    const across = scatter
      ? [...measures, ...dimensions].map((c) => ({ value: c.name, label: c.name }))
      : dims;
    const first = spec.y[0];
    const typeOf = new Map([...dimensions, ...measures].map((c) => [c.name, c.type]));
    const fns = (first ? aggregatesFor(typeOf.get(first.column)) : [])
      .filter((a) => a.value !== 'wavg')
      .map((a) => ({ value: a.value, label: a.label }));

    const markField = field(doc, 'Chart:', dropdown<ChartMark>(doc, spec.mark, CHART_MARKS, (v) => {
      if (v) this.#set({ mark: v }, true);
    }));
    if (spec.mark === 'treemap') {
      // what is on screen decides the rest: nothing else to choose
      const note = doc.createElement('p');
      note.className = 'dc-chartpanel-note';
      note.textContent = 'Draws the grid\'s rows as shown: each a block, sized by the first number column the grid shows. Expand a group to see inside it.';
      form.append(markField, note);
      return;
    }
    form.append(
      markField,
      field(doc, scatter ? 'X:' : 'Across:', dropdown<string>(doc, spec.x, across, (v) => {
        if (v) this.#set({ x: v });
      })),
      // every handler reads the spec as it is AT THE EVENT, never the one
      // this form was painted from: a stale copy undid the edit before it
      field(doc, scatter ? 'Y:' : 'Value:', dropdown<string>(
        doc, first?.column, measures.map((c) => ({ value: c.name, label: c.name })),
        (v) => {
          if (v) this.#set({ y: [{ column: v, fn: this.#spec?.y[0]?.fn ?? 'sum' }] }, true);
        },
      )),
      ...(scatter ? [] : [field(doc, 'Aggregate:', dropdown<AggregateFn>(
        doc, first?.fn, fns,
        (v) => {
          const now = this.#spec?.y[0];
          if (v && now) this.#set({ y: [{ column: now.column, fn: v }] });
        },
      ))]),
      field(doc, spec.mark === 'heatmap' ? 'Rows:' : 'Split by:', dropdown<string>(
        doc, spec.split, dims,
        (v) => {
          if (!this.#spec) return;
          const { split: _old, ...rest } = this.#spec;
          const freeze = !this.frozen && this.#spec.mark !== 'scatter';
          this.#spec = { ...(v ? { ...rest, split: v } : rest), ...(freeze ? { frozen: true } : {}) };
          if (freeze) this.#options.onFrozen?.(true);
          this.#options.onSpec?.();
          this.refresh();
        },
        { allowNone: spec.mark !== 'heatmap' },
      )),
    );
    if (spec.mark === 'bar') {
      form.append(field(doc, 'Orientation:', dropdown(doc, spec.options.orientation, [
        { value: 'vertical', label: 'Vertical' },
        { value: 'horizontal', label: 'Horizontal' },
      ] as const, (v) => { if (v) this.#setOption('orientation', v); })));
    }
    if (spec.mark === 'bar' || spec.mark === 'area') {
      form.append(field(doc, 'Stack:', dropdown(doc, spec.options.stack, [
        { value: 'none', label: 'None' },
        { value: 'stacked', label: 'Stacked' },
        { value: 'percent', label: '100%' },
      ] as const, (v) => { if (v) this.#setOption('stack', v); })));
    }
    if (!scatter) {
      form.append(
        field(doc, 'Order by:', dropdown(doc, `${spec.options.sort.by}:${spec.options.sort.direction}`, [
          { value: 'y:desc', label: 'Value, largest first' },
          { value: 'y:asc', label: 'Value, smallest first' },
          { value: 'x:asc', label: 'Category, A to Z' },
          { value: 'x:desc', label: 'Category, Z to A' },
        ] as const, (v) => {
          if (!v) return;
          const [by, direction] = v.split(':') as ['x' | 'y', 'asc' | 'desc'];
          this.#setOption('sort', { by, direction });
        })),
        field(doc, 'At most:', numberInput(doc, spec.options.limit, (v) => {
          if (v !== undefined && v >= 1) this.#setOption('limit', Math.floor(v));
        }, { min: 1, max: 5000, step: 10, width: 80 })),
      );
    }
    form.append(
      field(doc, 'Legend:', dropdown(doc, spec.options.legend, [
        { value: 'top', label: 'Top' },
        { value: 'bottom', label: 'Bottom' },
        { value: 'right', label: 'Right' },
        { value: 'none', label: 'None' },
      ] as const, (v) => { if (v) this.#setOption('legend', v); })),
      checkbox(doc, 'Show values', spec.options.labels, (v) => this.#setOption('labels', v)),
    );
    if (!scatter) {
      const hint = doc.createElement('p');
      hint.className = 'dc-chartpanel-hint';
      hint.textContent = 'Click a mark on the chart to filter the cube to it.';
      form.append(hint);
    }
  }

  // -- drawing ----------------------------------------------------------------

  /** The chart as drawn now, as a picture, for an export of the page; null before it has drawn. */
  picture(): ChartPicture | null {
    return this.#chart?.picture() ?? null;
  }

    async #draw(): Promise<void> {
    if (this.#disposed) return;
    const spec = this.#spec;
    const cube = this.#options.snapshot();
    if (!spec) return;
    // A TREEMAP DRAWS THE GRID AS SHOWN, with no query of its own (the old treemap's way, the user
    // 2026-09-30): following, the rows on screen now; frozen, the rows it last drew.
    if (spec.mark === 'treemap') {
      const now = this.frozen && this.#shownKept ? this.#shownKept : this.#options.shown?.() ?? this.#shownKept;
      if (!now) {
        this.#say('A treemap draws the grid\'s rows; the grid has none to draw yet.', true);
        return;
      }
      this.#shownKept = now;
      if (!this.#chart) {
        const { mountChart } = await import('../chart-render.ts');
        if (this.#disposed) return;
        this.#chart ??= mountChart(this.#canvas, (key) => this.#options.onPick(key));
      }
      const drawing = treemapOfGrid(now, themeOf(this.#canvas), this.#options.label);
      this.#chart.show(drawing);
      this.#say(drawing.notes.join(' '));
      return;
    }
    const problems = chartProblems(spec, cube);
    if (problems.length > 0) {
      this.#say(problems.join(' '), true);
      return;
    }
    this.#inflight?.abort();
    const abort = new AbortController();
    this.#inflight = abort;
    // busy, not a "Loading" line: a line that comes and goes resizes the chart twice
    this.#canvas.setAttribute('aria-busy', 'true');
    try {
      const { query, snapshot } = chartQuery(cube, spec);
      const rows = await this.#options.run(query, snapshot, abort.signal);
      if (abort.signal.aborted || this.#disposed) return;
      if (!this.#chart) {
        const { mountChart } = await import('../chart-render.ts');
        if (abort.signal.aborted || this.#disposed) return;
        this.#chart ??= mountChart(this.#canvas, (key) => this.#options.onPick(key));
      }
      const drawing = chartOption(spec, rows, themeOf(this.#canvas), this.#options.label);
      this.#chart.show(drawing);
      const capped = rows.rowCount >= (spec.mark === 'scatter' ? Infinity : spec.options.limit);
      this.#say([
        ...drawing.notes,
        ...(capped ? [`The first ${spec.options.limit} by the chosen order; raise "At most" to see more.`] : []),
      ].join(' '));
    } catch (e) {
      if (abort.signal.aborted || this.#disposed) return;
      this.#say(e instanceof Error ? e.message : String(e), true);
    } finally {
      if (this.#inflight === abort) this.#canvas.removeAttribute('aria-busy');
    }
  }

  #say(text: string, bad = false): void {
    // said only when there is something to say: a small chart needs its height
    this.#status.hidden = text === '';
    this.#status.textContent = text;
    this.#status.classList.toggle('dc-chartpanel-bad', bad);
  }
}
