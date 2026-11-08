// THE CHART FACE (B3, docs/TS_DECISIONS_2026_11_07.md §4, the builders: "an integrated #TS and charting api together in
// typescript"): a timeseries tile built in code -- its sources by grid, each series a term of the builder
// (timeseries-builder.ts) plotted and given back as a handle another series reads by name, its frequency, dates, range,
// view, panes and annotations -- each call ONE of the tile's changes (timeseries-edit.ts TsChange), made by the one rule
// every change is made by (editTimeseries): a third front of it, beside the screen and Python, never a path of its own.
// What it builds is the tile's spec, told to a page as the page's own commands are (addTimeseries, setTimeseries); its
// changes are kept in the order made, each one a viewer's or Python's would be.
//
// Its series' texts are the builder's own (timeseries-text.ts writtenText: the language's spelling, read back by core).
// No part of any startup bundle: a person imports it with the builder.
//
// PURE: no DOM, no planner.

import type { TimeGrain } from './chart-spec.ts';
import { Term } from './timeseries-builder-core.ts';
import { editTimeseries, type TsChange, type TsGridColumns, type TsSeriesTold, type TsSlots } from './timeseries-edit.ts';
import type { TimeseriesSpec, TsAlign, TsAnnotation, TsMatch, TsRange, TsSeries, TsView } from './timeseries-spec.ts';
import { writtenText } from './timeseries-text.ts';

/** How a series is plotted: everything a series is but its id and its text (the term's). */
export type TsPlotOptions = Omit<TsSeriesTold, 'id' | 'text'>;

/** A series plotted: the term another series reads it by (`A`), and its id. */
export class Plotted extends Term {
  readonly id: string;

  constructor(id: string) {
    super({ kind: 'ref', series: id });
    this.id = id;
  }
}

/** A pane's settings as the pane change tells them (its height, scale and axis; null takes a hand-set end off). */
export type TsPaneOptions = Omit<Extract<TsChange, { kind: 'pane' }>, 'kind' | 'index'>;

/** A TIMESERIES TILE BUILT IN CODE: each method one change of the tile, refused in the rule's own words. */
export class Chart {
  #spec: TimeseriesSpec;
  readonly #changes: TsChange[] = [];

  /** A tile as it is (`spec`), or one of no source yet. */
  constructor(spec: TimeseriesSpec = { sources: [], series: [] }) {
    this.#spec = spec;
  }

  /** The tile's spec as its changes made it. */
  get spec(): TimeseriesSpec {
    return this.#spec;
  }

  /** Its changes, in the order made: each one the screen's or Python's would be. */
  get changes(): readonly TsChange[] {
    return this.#changes;
  }

  /** `change` made by the tile's one rule. */
  #edit(change: TsChange): this {
    this.#spec = editTimeseries(this.#spec, change);
    this.#changes.push(change);
    return this;
  }

  /** A grid of the page read as a source: its dates, its instruments, the columns its series read. */
  read(grid: TsGridColumns): this {
    return this.#edit({ kind: 'read', grid });
  }

  /** A source renamed, or its date or instrument column told (null: none). */
  source(alias: string, told: { readonly rename?: string; readonly date?: string; readonly key?: string | null }): this {
    return this.#edit({ kind: 'source', alias, ...told });
  }

  /** A source taken off: refused while a series reads it. */
  unread(alias: string): this {
    return this.#edit({ kind: 'unread', alias });
  }

  /** `term` plotted, as `options` say: the series plotted, which a later term reads by its name. */
  plot(term: Term, options: TsPlotOptions = {}): Plotted {
    const before = new Set(this.#spec.series.map((s) => s.id));
    this.#edit({ kind: 'plot', series: { ...options, text: writtenText(term.expr, this.#spec.series) } });
    const made = this.#spec.series.find((s) => !before.has(s.id));
    if (made === undefined) throw new Error('a series plotted added none');
    return new Plotted(made.id);
  }

  /** The series `of` plotted again as `term` (its options as told; hidden still unless told). */
  replot(of: Plotted | string, term: Term, options: TsPlotOptions = {}): Plotted {
    const id = typeof of === 'string' ? of : of.id;
    this.#edit({ kind: 'plot', replace: true, series: { ...options, id, text: writtenText(term.expr, this.#spec.series) } });
    return new Plotted(id);
  }

  /** The series `of` taken off: refused while another reads it. */
  unplot(of: Plotted | string): this {
    return this.#edit({ kind: 'unplot', id: typeof of === 'string' ? of : of.id });
  }

  /** The series `of` hidden (its legend entry kept), or shown again. */
  hide(of: Plotted | string, hidden = true): this {
    return this.#edit({ kind: 'hide', id: typeof of === 'string' ? of : of.id, hidden });
  }

  /** Each period's value (null: every observation). */
  grain(grain: TimeGrain | null): this {
    return this.#edit({ kind: 'grain', grain });
  }

  /** How two series' dates are combined (null: the dates both have). */
  align(align: TsAlign | null): this {
    return this.#edit({ kind: 'align', align });
  }

  /** The dates shown (null: every one). */
  range(range: TsRange | null): this {
    return this.#edit({ kind: 'range', range });
  }

  /** The view's settings told, each replacing its own (null takes it off). */
  view(view: { readonly [K in keyof TsView]?: TsView[K] | null }): this {
    return this.#edit({ kind: 'view', view });
  }

  /** Pane `index`'s height, scale or axis. */
  pane(index: number, options: TsPaneOptions): this {
    return this.#edit({ kind: 'pane', index, ...options });
  }

  /** An instrument read as another in every series that reads it (of source `alias` alone where told). */
  swap(from: string, to: string, alias?: string): this {
    return this.#edit({ kind: 'swap', from, to, ...(alias !== undefined ? { alias } : {}) });
  }

  /** An annotation drawn over the series: a level, an event, a period, a line, a note, marks, a projection. */
  annotate(annotation: TsAnnotation): this {
    return this.#edit({ kind: 'annotate', annotation });
  }

  /** The annotation at `index` taken off. */
  unannotate(index: number): this {
    return this.#edit({ kind: 'unannotate', index });
  }

  /** Every period shaded where `column` is not 0 (FRED's recession bars): a condition series of its own. */
  shade(column: string, told: { readonly source?: string; readonly where?: readonly TsMatch[]; readonly name?: string } = {}): Plotted {
    const before = new Set(this.#spec.series.map((s) => s.id));
    this.#edit({ kind: 'shade', column, ...told });
    const made = this.#spec.series.find((s) => !before.has(s.id))!;
    return new Plotted(made.id);
  }

  /** The tile's settings told whole, each replacing what it had (null takes it off). */
  slots(slots: TsSlots): this {
    return this.#edit({ kind: 'slots', slots });
  }

  /** The series plotted, by id. */
  series(): readonly TsSeries[] {
    return this.#spec.series;
  }
}
