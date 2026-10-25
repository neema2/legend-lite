// The chart window: a chart of the cube, and the options that shape it.
//
// Left, the options (mark, what goes across, what is plotted and how it is
// aggregated, the split, orientation, stacking, order, how many, labels,
// legend); right, the chart. The chart follows the cube: its query is the
// cube's own, regrouped (`chart-spec.ts`), so a filter added to the grid
// narrows the chart, and a click on a mark (a bar, a slice, a cell) adds
// that mark's values to the cube's filter.

import {
  axisName,
  brushConditions,
  chartProblems,
  chartQuery,
  chartState,
  chartStateOf,
  defaultChart,
  drilledView,
  drillDown,
  drillLevels,
  drillUp,
  editChart,
  followCube,
  GRID_MARKS,
  KIND_MARKS,
  MAP_MARKS,
  kpiQueries,
  markConditions,
  MAX_FACETS,
  plotsRows,
  type ChartChange,
  type ChartMark,
  type ChartSpec,
  type ChartState,
} from '../chart-spec.ts';
import type { FormHost } from './chart-form.ts';
// the drawing (chart-option.ts) comes with ECharts, the first time a chart draws: a grid alone fetches neither
import type { Brushed, GridShown, LabelOf, MarkKey, UnitOf } from '../chart-option.ts';
// ECharts (about 218 KB gzipped) is loaded the first time a chart draws, never by a grid alone (plan F7)
import type { ChartPicture, MountedChart } from '../chart-render.ts';
import type { FeedPick, FeedView } from './chart-feed.ts';
import type { ResultTable, Scalar } from '../../../engine-client/src/result.ts';
import type { CubeSnapshot, FilterNode } from '../snapshot.ts';
import { toJson, type Lambda } from '../../../pure-protocol/src/index.ts';
import { lazily } from '../lazily.ts';

/** The kinds with an axis of categories or time across, whose pointer a page's other charts follow (a crosshair). */
const CROSSHAIRED: ReadonlySet<ChartMark> = new Set(['bar', 'line', 'area', 'heatmap', 'lollipop', 'dumbbell', 'range', 'waterfall', 'pareto']);

/** A span of time as its button says it: 1m, 5m, 1h, 1d -- or All. */
export function spanLabel(seconds: number): string {
  if (seconds === 0) return 'All';
  if (seconds % 86_400 === 0) return `${seconds / 86_400}d`;
  if (seconds % 3600 === 0) return `${seconds / 3600}h`;
  if (seconds % 60 === 0) return `${seconds / 60}m`;
  return `${seconds}s`;
}

/** A span of time in words: "5 minutes", "1 hour". */
function spanWords(seconds: number): string {
  const [n, unit] = seconds % 86_400 === 0 ? [seconds / 86_400, 'day'] : seconds % 3600 === 0 ? [seconds / 3600, 'hour']
    : seconds % 60 === 0 ? [seconds / 60, 'minute'] : [seconds, 'second'];
  return `${n} ${unit}${n === 1 ? '' : 's'}`;
}

/** Whether batch 3's rarer ECharts charts are registered on this page (chart-render-kinds.ts, fetched once). */
let kindsRegistered = false;
/** Whether the maps' ECharts and the world's countries are registered on this page (chart-render-maps.ts, fetched once). */
let mapsRegistered = false;

/** How long a click on a chart that drills waits for a second click (a double-click drills instead of filtering). */
const DOUBLE_CLICK_MS = 250;

export interface ChartPanelOptions {
  /** The cube as it is NOW (the chart follows it). */
  readonly snapshot: () => CubeSnapshot;
  /** Run a query the cube did not plan, through the cube's own runner. */
  readonly run: (query: Lambda, snapshot: CubeSnapshot, signal: AbortSignal) => Promise<ResultTable>;
  /** How a value is written: the grid's formatter. */
  readonly label: LabelOf;
  /** A column's unit as the grid formats it: two units, two value axes. */
  readonly unit?: UnitOf;
  /** A mark was clicked: filter the cube to these conditions (its values; a period's or a bin's range). */
  readonly onPick: (conditions: FilterNode[]) => void;
  /** Where to start; the cube's own grouping suggests one otherwise. */
  readonly initial?: ChartSpec;
  /** The page's crosshair group: the page's charts with the same column across share a crosshair. */
  readonly crosshair?: string;
  /** Upstream-style debounce between an edit and the query; a test passes 0. */
  readonly debounceMs?: number;
  /** Whether the options start open (a chart tile starts with the chart alone). */
  readonly formOpen?: boolean;
  /** The chart's state changed (ChartState: by its pill, or pinned by an edit to its grouping). */
  readonly onState?: (state: ChartState) => void;
  /** The user changed the chart (an option in its form). */
  readonly onSpec?: () => void;
  /** What the grid shows now: a treemap draws exactly that, not a query of its own. */
  readonly shown?: () => GridShown | null;
  /** A feed's card picked, or a ticker chip on it (K15): its row, for the page to follow where the view links it. */
  readonly onRow?: (pick: FeedPick) => void;
}

export class ChartPanel {
  readonly #root: HTMLElement;
  readonly #doc: Document;
  readonly #options: ChartPanelOptions;
  #spec: ChartSpec | null;
  #form!: HTMLElement;
  #canvas!: HTMLElement;
  #status!: HTMLElement;
  /** A drilled chart's path, above it (batch 2 item 10). */
  #crumbs!: HTMLElement;
  /** The spans of time it offers, above it (K14: a stock chart's 1m, 5m, 1h, All). */
  #spans!: HTMLElement;
  #clickTimer: ReturnType<typeof setTimeout> | undefined;
  /** The spec changed while the form was closed: painted when it is next shown. */
  #formStale = false;
  /** What the options form (ui/chart-form.ts) reads and changes. */
  readonly #formHost: FormHost = ((panel: ChartPanel): FormHost => ({
    get doc() { return panel.#doc; },
    spec: () => this.#spec,
    set: (change, redraw) => this.#set(change, redraw),
    setOption: (key, value) => this.#setOption(key, value),
    snapshot: () => this.#options.snapshot(),
  }))(this);
  #chart: MountedChart | null = null;
  /** Whether the chart was made after batch 3's rarer ECharts charts were registered (`#mount`). */
  #chartKnowsKinds = false;
  #chartKnowsMaps = false;
  /** A KPI's trend line, mounted under its number (the number is the canvas's own, not a chart). */
  #trend: MountedChart | null = null;
  /** A feed's cards (K15), the canvas's own, kept across ticks; its keyword box above it, and the keyword it asked for. */
  #feed: FeedView | null = null;
  #search!: HTMLInputElement;
  #searched = '';
  #timer: ReturnType<typeof setTimeout> | undefined;
  #inflight: AbortController | null = null;
  #disposed = false;
  /** A treemap's rows as last drawn: what a frozen one keeps. */
  #shownKept: GridShown | null = null;
  /** What the chart's last draw read (its query, but its epoch): a tick's draw of the same holds its scale. */
  #drawnQuery: string | undefined;

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

  /** Whether the chart keeps its own grouping (pinned or frozen), or follows the cube's pivots (dynamic). */
  get frozen(): boolean {
    return this.#spec?.frozen === true;
  }

  /** Its state, as its pill says it: dynamic, pinned or frozen (chart-spec.ts chartStateOf). */
  get state(): ChartState {
    return this.#spec ? chartStateOf(this.#spec) : 'dynamic';
  }

  /**
   * Put the chart in a state (chart-spec.ts chartState): dynamic, it regroups the way the cube is now -- before it says
   * so, so what it says is what it draws --; pinned, its grouping its own; frozen, the grid's filter now kept too.
   */
  setState(state: ChartState): void {
    if (!this.#spec || this.state === state) return;
    this.#spec = chartState(this.#spec, state, this.#options.snapshot());
    this.#options.onState?.(state);
    this.refresh(0);
  }

  /** Draw `spec` instead (a pinned chart updated from the live one): its form follows. */
  setSpec(spec: ChartSpec): void {
    const was = this.state;
    this.#spec = spec;
    this.#paintForm();
    if (this.state !== was) this.#options.onState?.(this.state);
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
    this.#timer = setTimeout(() => {
      this.#timer = undefined;
      void this.#draw();
    }, delay);
  }

  /**
   * A TICK (docs/DEEPHAVEN_PLAN_2026_10_22.md V3, DH27): its grid's rows changed, nothing the chart asks did -- drawn
   * again now, its query read again (answered from the rows its grid's engine follows live), with no debounce and no
   * animation. A tick while it draws draws once more after; one while an edit's redraw is pending is that redraw's.
   */
  tick(): void {
    if (this.#disposed || !this.#spec || this.#timer !== undefined) return;
    if (this.#ticking) {
      this.#tickAgain = true;
      return;
    }
    this.#ticking = true;
    void this.#draw(true).finally(() => {
      this.#ticking = false;
      if (this.#tickAgain) {
        this.#tickAgain = false;
        this.tick();
      }
    });
  }

  /** Whether a tick's draw runs, and whether another tick came meanwhile. */
  #ticking = false;
  #tickAgain = false;

  /** Show or hide the options; answers whether they are now shown. */
  toggleForm(): boolean {
    this.#form.hidden = !this.#form.hidden;
    if (!this.#form.hidden && this.#formStale) this.#paintForm();
    return !this.#form.hidden;
  }

  dispose(): void {
    this.#disposed = true;
    clearTimeout(this.#timer);
    clearTimeout(this.#clickTimer);
    this.#inflight?.abort();
    this.#chart?.dispose();
    this.#chart = null;
    this.#trend?.dispose();
    this.#trend = null;
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
    this.#crumbs = doc.createElement('nav');
    this.#crumbs.className = 'dc-chartpanel-crumbs';
    this.#crumbs.setAttribute('aria-label', 'Drill path');
    this.#crumbs.hidden = true;
    this.#spans = doc.createElement('div');
    this.#spans.className = 'dc-chartpanel-spans';
    this.#spans.setAttribute('role', 'group');
    this.#spans.setAttribute('aria-label', 'Span of time shown');
    this.#spans.hidden = true;
    // A FEED'S KEYWORD (K15): its stories with it in the headline or the body, any case -- the feed's own selection on
    // its grid (onPick), so it is kept with the page and said on the tile's chip as any chart's is
    this.#search = doc.createElement('input');
    this.#search.type = 'search';
    this.#search.className = 'dc-chartpanel-search';
    this.#search.placeholder = 'Keyword';
    this.#search.setAttribute('aria-label', 'Filter the feed to a keyword');
    this.#search.hidden = true;
    let typed: ReturnType<typeof setTimeout> | undefined;
    this.#search.addEventListener('input', () => {
      clearTimeout(typed);
      typed = setTimeout(() => this.#keyword(this.#search.value), 400);
    });
    this.#search.addEventListener('keydown', (e) => {
      if (e.key !== 'Enter') return;
      clearTimeout(typed);
      this.#keyword(this.#search.value);
    });
    right.append(this.#crumbs, this.#spans, this.#search, this.#canvas, this.#status);
    this.#root.append(this.#form, right);
    this.#form.hidden = this.#options.formOpen === false;
    this.#paintForm();
  }

  /** The keyword typed into a feed's box, asked of its grid: none, its selection taken off. */
  #keyword(text: string): void {
    const feed = this.#spec?.feed;
    const words = text.trim();
    if (!feed || words === this.#searched) return;
    this.#searched = words;
    const has = (column: string): FilterNode => ({ kind: 'condition', column, operator: 'containsCaseInsensitive', value: words });
    this.#options.onPick(words === '' ? []
      : [feed.body !== undefined ? { kind: 'or', children: [has(feed.headline), has(feed.body)] } : has(feed.headline)]);
  }

  /**
   * The chart's selection on its grid now, as the page has it (a page reopened, a chip cleared, an undo): a feed's box
   * says its keyword -- but while the person types in it.
   */
  selected(conditions: readonly FilterNode[]): void {
    const one = conditions.length === 1 ? conditions[0] : undefined;
    const first = one?.kind === 'or' ? one.children[0] : one;
    const words = first?.kind === 'condition' && first.operator === 'containsCaseInsensitive' && typeof first.value === 'string'
      ? first.value : '';
    this.#searched = words;
    if (this.#doc.activeElement !== this.#search) this.#search.value = words;
  }

  #set(change: ChartChange, redrawForm = false): void {
    if (!this.#spec) return;
    // changed by hand as a chart Python makes is (chart-spec.ts editChart): a hand on the grouping of a live chart
    // freezes it, as the next pivot change would otherwise undo the choice
    const was = this.state;
    this.#spec = editChart(this.#spec, change, this.#options.snapshot());
    if (this.state !== was) this.#options.onState?.(this.state);
    this.#options.onSpec?.();
    if (redrawForm) this.#paintForm();
    // its span buttons say the span shown, whatever changed it
    this.#paintSpans();
    this.refresh();
  }

  #setOption<K extends keyof ChartSpec['options']>(key: K, value: ChartSpec['options'][K] | null): void {
    this.#set({ options: { [key]: value } });
  }

  /**
   * The options form (ui/chart-form.ts): fetched the first time it is shown -- a grid alone, or a chart whose options
   * stay closed, never downloads it -- and painted again whenever the spec changes while it is shown.
   */
  #paintForm(): void {
    this.#paintCrumbs();
    this.#paintSpans();
    this.#search.hidden = this.#spec?.mark !== 'feed';
    if (this.#form.hidden) {
      this.#formStale = true;
      return;
    }
    this.#formStale = false;
    void lazily(() => import('./chart-form.ts')).then(({ paintChartForm }) => {
      if (this.#disposed || this.#form.hidden) return;
      paintChartForm(this.#formHost, this.#form);
    });
  }




  /**
   * THE SPANS OF TIME (K14): a button per span the chart offers (`options.spans`), the one shown pressed -- a press shows
   * that span, the latest that many seconds of its time across, following the latest mark as it ticks (chart-option.ts);
   * a change of the chart's, kept with it and undone as any.
   */
  #paintSpans(): void {
    const spans = this.#spec?.options.spans;
    this.#spans.hidden = spans === undefined;
    if (spans === undefined) {
      this.#spans.replaceChildren();
      return;
    }
    const shown = this.#spec?.options.span ?? 0;
    this.#spans.replaceChildren(...spans.map(({ seconds, grain }) => {
      const b = this.#doc.createElement('button');
      b.type = 'button';
      b.className = 'dc-chartpanel-span';
      b.textContent = spanLabel(seconds);
      b.dataset['span'] = String(seconds);
      b.setAttribute('aria-pressed', String(seconds === shown));
      b.title = seconds === 0 ? 'All of it' : `The latest ${spanWords(seconds)}`;
      // the span, and the period it is drawn by: one step of the chart's
      b.addEventListener('click', () => {
        const spec = this.#spec;
        if (!spec || ((spec.options.span ?? 0) === seconds && (grain === undefined || spec.grain === grain))) return;
        this.#set({ ...(grain !== undefined && grain !== spec.grain ? { grain } : {}), options: { span: seconds } });
      });
      return b;
    }));
  }

  // -- drawing ----------------------------------------------------------------

  /** The chart as drawn now, as a picture, for an export of the page; null before it has drawn. */
  picture(): ChartPicture | null {
    return this.#chart?.picture() ?? null;
  }

  async #draw(ticked = false): Promise<void> {
    if (this.#disposed) return;
    const spec = this.#spec;
    const cube = this.#options.snapshot();
    if (!spec) return;
    // what stops it is said before anything is drawn, a chart of the grid as shown too (a slot it does not draw); a
    // query still answering would draw over the words: it is let go
    const problems = chartProblems(spec, cube);
    if (problems.length > 0) {
      this.#inflight?.abort();
      this.#inflight = null;
      this.#say(problems.join(' '), true);
      return;
    }
    if (spec.mark === 'feed') {
      await this.#drawFeed(spec, cube, ticked);
      return;
    }
    this.#clearFeed();
    // A TREEMAP DRAWS THE GRID AS SHOWN, with no query of its own (the old treemap's way, the user
    // 2026-09-30): following, the rows on screen now; frozen, the rows it last drew. So do a sunburst and a tree.
    if (GRID_MARKS.has(spec.mark)) {
      // a query of another kind still answering would draw over it: it is let go
      this.#inflight?.abort();
      this.#inflight = null;
      this.#clearKpi();
      const now = this.frozen && this.#shownKept ? this.#shownKept : this.#options.shown?.() ?? this.#shownKept;
      if (!now) {
        // a wait, not a problem: the chart draws when the grid's rows come (a view landing refreshes it)
        this.#say(`A ${spec.mark} draws the grid's rows; the grid has none to draw yet.`);
        return;
      }
      this.#shownKept = now;
      const chart = await this.#mount(spec.mark !== 'treemap');
      if (this.#disposed || !chart) return;
      const { treemapOfGrid, themeOf } = await lazily(() => import('../chart-option.ts'));
      if (this.#disposed) return;
      const mark = spec.mark;
      let drawing;
      if (mark === 'sunburst' || mark === 'tree') {
        const { gridKindOption } = await lazily(() => import('../chart-kinds.ts'));
        if (this.#disposed) return;
        drawing = gridKindOption(mark, now, themeOf(this.#canvas), this.#options.label);
      } else {
        drawing = treemapOfGrid(now, themeOf(this.#canvas), this.#options.label, spec.colour
          ? { column: spec.colour.column, ...(spec.options.colourReach !== undefined ? { reach: spec.options.colourReach } : {}) }
          : undefined);
      }
      chart.show(drawing, { ticked });
      this.#say(drawing.notes.join(' '));
      return;
    }
    if (spec.mark === 'kpi') {
      await this.#drawKpi(spec, cube);
      return;
    }
    this.#clearKpi();
    this.#inflight?.abort();
    const abort = new AbortController();
    this.#inflight = abort;
    // busy, not a "Loading" line: a line that comes and goes resizes the chart twice; a tick is not busy
    if (!ticked) this.#canvas.setAttribute('aria-busy', 'true');
    try {
      const { query, snapshot } = chartQuery(cube, spec);
      const rows = await this.#options.run(query, snapshot, abort.signal);
      if (abort.signal.aborted || this.#disposed) return;
      const kinds = KIND_MARKS.has(spec.mark) || spec.facet !== undefined;
      const maps = MAP_MARKS.has(spec.mark);
      const chart = await this.#mount(kinds, maps);
      if (abort.signal.aborted || this.#disposed || !chart) return;
      const { chartOption, themeOf } = await lazily(() => import('../chart-option.ts'));
      if (abort.signal.aborted || this.#disposed) return;
      let drawing;
      if (maps) {
        // batch 5's maps: their drawing and the world's countries, fetched with the first of them
        const { mapOption } = await lazily(() => import('../chart-maps.ts'));
        if (abort.signal.aborted || this.#disposed) return;
        drawing = mapOption(spec, rows, themeOf(this.#canvas), this.#options.label);
      } else if (kinds) {
        // batch 3's kinds and small multiples: their drawing and the rarer ECharts charts, fetched with the first of them
        const { kindOption, facetOption } = await lazily(() => import('../chart-kinds.ts'));
        if (abort.signal.aborted || this.#disposed) return;
        drawing = spec.facet !== undefined
          ? facetOption(spec, rows, themeOf(this.#canvas), this.#options.label, this.#options.unit)
          : kindOption(spec, rows, themeOf(this.#canvas), this.#options.label);
      } else {
        drawing = chartOption(spec, rows, themeOf(this.#canvas), this.#options.label, this.#options.unit);
      }
      // a tick's draw holds the scale and the window the last draw left -- of the same query only: one that reads
      // anything else (another stock picked, a span, a filter) is drawn afresh, whatever brought it (K14: a scale held
      // across a change of stock kept the old one's prices in view)
      const { epoch: _epoch, ...reads } = snapshot as typeof snapshot & { readonly epoch?: number };
      // written as the protocol writes it: a snapshot holds exact numbers, which JSON.stringify refuses
      const key = toJson(reads as never);
      chart.show(drawing, { ticked: ticked && key === this.#drawnQuery });
      this.#drawnQuery = key;
      // the page's charts with the same column across (at the same grain) share a crosshair: those with an axis of
      // categories or time
      const across = axisName(drilledView(spec).spec);
      chart.setGroup(this.#options.crosshair !== undefined && across !== undefined && CROSSHAIRED.has(spec.mark) && spec.facet === undefined
        ? `${this.#options.crosshair}|${across}` : undefined);
      // small multiples ask for each panel's most, times the most panels (chart-spec.ts chartQuery)
      const asked = plotsRows(spec) ? Infinity : spec.options.limit * (spec.facet ? MAX_FACETS : 1);
      const capped = rows.rowCount >= asked;
      this.#say([
        ...drawing.notes,
        ...(capped ? [spec.facet
          ? `The first ${asked} rows across the charts; filter the cube to see the rest.`
          : `The first ${spec.options.limit} by the chosen order; raise "At most" to see more.`] : []),
      ].join(' '));
    } catch (e) {
      if (abort.signal.aborted || this.#disposed) return;
      this.#say(e instanceof Error ? e.message : String(e), true);
    } finally {
      if (this.#inflight === abort) this.#canvas.removeAttribute('aria-busy');
    }
  }

  /**
   * The chart's ECharts instance, made the first time it is needed. Given `kinds`, batch 3's rarer charts are
   * registered first (chart-render-kinds.ts): ECharts builds an instance's layout stages when the instance is made, so
   * one made before they were registered cannot lay a funnel out -- it is made again. Null when the panel went meanwhile.
   */
  async #mount(kinds: boolean, maps = false): Promise<MountedChart | null> {
    if (kinds) {
      await lazily(() => import('../chart-render-kinds.ts'));
      kindsRegistered = true;
      if (this.#chart && !this.#chartKnowsKinds) {
        this.#chart.dispose();
        this.#chart = null;
      }
    }
    // the maps likewise: an instance made before the map chart was registered cannot lay a map out; the shapes come
    // with the drawing (chart-maps.ts), registered once
    if (maps && !mapsRegistered) {
      const [{ registerWorld }, { WORLD }] = await Promise.all([lazily(() => import('../chart-render-maps.ts')), lazily(() => import('../chart-maps.ts'))]);
      registerWorld(WORLD.name, WORLD.shapes);
      mapsRegistered = true;
    }
    if (maps) {
      if (this.#chart && !this.#chartKnowsMaps) {
        this.#chart.dispose();
        this.#chart = null;
      }
    }
    if (!this.#chart) {
      const { mountChart } = await lazily(() => import('../chart-render.ts'));
      if (this.#disposed) return null;
      this.#chart ??= mountChart(this.#canvas, (key, upper) => this.#pick(key, upper), { onDrill: (key) => this.#drill(key), onBrush: (took) => this.#brush(took), onKind: (mark) => this.#kind(mark) });
      this.#chartKnowsKinds = kindsRegistered;
      this.#chartKnowsMaps = mapsRegistered;
    }
    return this.#chart;
  }

  /**
   * A KPI: its number as the grid writes it, against its target (good or not), and under them its trend's line -- its
   * value from one query, the trend from another, both the cube's own (chart-spec.ts `kpiQueries`).
   */
  async #drawKpi(spec: ChartSpec, cube: CubeSnapshot): Promise<void> {
    this.#inflight?.abort();
    const abort = new AbortController();
    this.#inflight = abort;
    this.#canvas.setAttribute('aria-busy', 'true');
    try {
      const { total, trend } = kpiQueries(cube, spec);
      const [rows, line] = await Promise.all([
        this.#options.run(total.query, total.snapshot, abort.signal),
        trend ? this.#options.run(trend.query, trend.snapshot, abort.signal) : Promise.resolve(null),
      ]);
      if (abort.signal.aborted || this.#disposed) return;
      const { kpiDrawing, themeOf } = await lazily(() => import('../chart-option.ts'));
      if (abort.signal.aborted || this.#disposed) return;
      const drawing = kpiDrawing(spec, rows, line, themeOf(this.#canvas), this.#options.label);
      // a chart drawn before (another kind) gives way to the number
      this.#chart?.dispose();
      this.#chart = null;
      this.#trend?.dispose();
      this.#trend = null;
      const doc = this.#doc;
      const div = (className: string, text?: string): HTMLDivElement => {
        const el = doc.createElement('div');
        el.className = className;
        if (text !== undefined) el.textContent = text;
        return el;
      };
      // a stat tile: the headline (in full on a hover); against its target, said and metered; its latest period
      // against the one before; its trend's line
      const box = div('dc-kpi');
      const value = div('dc-kpi-value', drawing.value);
      if (drawing.exact !== drawing.value) value.title = drawing.exact;
      box.append(value);
      /** A line of the tile: its difference, coloured by good or bad (and said by its arrow), then the rest in words. */
      const said = (className: string, delta: string, words: string, good: boolean | null): HTMLDivElement => {
        const line = div(className);
        if (delta) line.append(div(`dc-kpi-delta${good === null ? '' : good ? ' dc-kpi-good' : ' dc-kpi-bad'}`, delta));
        line.append(div('dc-kpi-words', words));
        return line;
      };
      if (drawing.target) {
        const { delta, words, goal, good, reached } = drawing.target;
        box.append(said('dc-kpi-target', delta, words, good));
        // how much of the target is reached, the target at the meter's end
        const meter = div(`dc-kpi-meter ${good ? 'dc-kpi-good' : 'dc-kpi-bad'}`);
        meter.setAttribute('role', 'img');
        meter.setAttribute('aria-label', `${Math.round(reached * 100)}% of the target, ${goal}`);
        const track = div('dc-kpi-meter-track');
        const fill = div('dc-kpi-meter-fill');
        fill.style.width = `${(reached * 100).toFixed(1)}%`;
        track.append(fill);
        meter.append(track, div('dc-kpi-goal', goal));
        box.append(meter);
      }
      if (drawing.reference) box.append(said('dc-kpi-reference', drawing.reference.delta, drawing.reference.words, drawing.reference.good));
      if (drawing.change) box.append(said('dc-kpi-change', drawing.change.delta, drawing.change.words, drawing.change.good));
      const place = div('dc-kpi-trend');
      if (drawing.trend) box.append(place);
      this.#canvas.replaceChildren(box);
      this.#canvas.setAttribute('aria-label', drawing.description);
      this.#canvas.classList.add('dc-chartpanel-kpi');
      if (drawing.trend) {
        const { mountChart } = await lazily(() => import('../chart-render.ts'));
        if (abort.signal.aborted || this.#disposed) return;
        this.#trend = mountChart(place);
        this.#trend.show(drawing.trend);
      }
      this.#say(drawing.notes.join(' '));
    } catch (e) {
      if (abort.signal.aborted || this.#disposed) return;
      this.#say(e instanceof Error ? e.message : String(e), true);
    } finally {
      if (this.#inflight === abort) this.#canvas.removeAttribute('aria-busy');
    }
  }

  /**
   * A FEED (K15): its query's rows -- the cube's under its filter, the newest first (chart-spec.ts feedSnapshot) -- as
   * cards, drawn again on each tick with the cards it had kept (ui/chart-feed.ts, fetched with the first feed).
   */
  async #drawFeed(spec: ChartSpec, cube: CubeSnapshot, ticked: boolean): Promise<void> {
    this.#inflight?.abort();
    const abort = new AbortController();
    this.#inflight = abort;
    if (!ticked) this.#canvas.setAttribute('aria-busy', 'true');
    try {
      const { query, snapshot } = chartQuery(cube, spec);
      const rows = await this.#options.run(query, snapshot, abort.signal);
      if (abort.signal.aborted || this.#disposed || spec.feed === undefined) return;
      const { FeedView, feedCards } = await lazily(() => import('./chart-feed.ts'));
      if (abort.signal.aborted || this.#disposed) return;
      if (this.#feed === null) {
        // a chart or a KPI drawn before gives way to the cards
        this.#clearKpi();
        this.#chart?.dispose();
        this.#chart = null;
        this.#feed = new FeedView(this.#doc, (pick) => this.#options.onRow?.(pick));
        this.#canvas.replaceChildren(this.#feed.element);
        this.#canvas.classList.add('dc-chartpanel-feed');
        // a list of stories to read and pick, not a picture
        this.#canvas.setAttribute('role', 'region');
      }
      const cards = feedCards(spec.feed, rows, this.#options.label);
      this.#feed.draw(cards, `News: ${cards.length === 0 ? 'no stories' : `the latest ${cards.length}, the newest first`}`);
      this.#canvas.setAttribute('aria-label', 'News feed');
      this.#say(cards.length === 0 ? 'No story matches its filter yet.' : '');
    } catch (e) {
      if (abort.signal.aborted || this.#disposed) return;
      this.#say(e instanceof Error ? e.message : String(e), true);
    } finally {
      if (this.#inflight === abort) this.#canvas.removeAttribute('aria-busy');
    }
  }

  /** A feed drawn before gives way to a chart: its cards gone. */
  #clearFeed(): void {
    if (this.#feed === null) return;
    this.#feed = null;
    this.#canvas.replaceChildren();
    this.#canvas.classList.remove('dc-chartpanel-feed');
    this.#canvas.setAttribute('role', 'img');
  }

  /** A KPI drawn before gives way to a chart: its number and its trend gone. */
  #clearKpi(): void {
    if (!this.#canvas.classList.contains('dc-chartpanel-kpi')) return;
    this.#trend?.dispose();
    this.#trend = null;
    this.#canvas.replaceChildren();
    this.#canvas.classList.remove('dc-chartpanel-kpi');
  }

  /**
   * A clicked mark, as conditions on the cube's own columns (chart-spec.ts `markConditions`). On a chart that drills, a
   * click waits a moment: a second click makes it a double-click, which drills instead of filtering.
   */
  #pick(key: MarkKey, upper?: Scalar): void {
    const spec = this.#spec;
    if (!spec) return;
    clearTimeout(this.#clickTimer);
    const filter = (): void => this.#options.onPick(markConditions(spec, this.#options.snapshot(), key, upper));
    if (drillLevels(spec, this.#options.snapshot()) === null) {
      filter();
      return;
    }
    this.#clickTimer = setTimeout(filter, DOUBLE_CLICK_MS);
  }



  /** The toolbox drew the chart the other way: bars or a line, kept as the chart's kind. */
  #kind(mark: 'bar' | 'line'): void {
    if (!this.#spec || this.#spec.mark === mark) return;
    this.#set({ mark }, true);
  }

  /** A brush took a range across, or a scatter's rectangle: the grid filtered to it; cleared, the filter taken off. */
  #brush(took: Brushed | null): void {
    const spec = this.#spec;
    if (!spec) return;
    clearTimeout(this.#clickTimer);
    this.#options.onPick(took === null ? [] : brushConditions(spec, this.#options.snapshot(), took));
  }

  /** A double-clicked mark: the chart one level down its grid's groupings (batch 2 item 10), if there is one. */
  #drill(key: MarkKey): void {
    clearTimeout(this.#clickTimer);
    const spec = this.#spec;
    if (!spec) return;
    const across = drilledView(spec).spec.x;
    if (across === undefined || !(across in key)) return;
    const next = drillDown(spec, this.#options.snapshot(), key[across] ?? null);
    if (!next) return;
    this.#spec = next;
    this.#options.onSpec?.();
    this.#paintCrumbs();
    this.refresh(0);
  }

  /**
   * Where a drilled chart is: its top level, then each value drilled into, each a way back up to it; the level shown
   * last. Hidden for a chart that does not drill; a chart that could says how.
   */
  #paintCrumbs(): void {
    const doc = this.#doc;
    const crumbs = this.#crumbs;
    crumbs.replaceChildren();
    const spec = this.#spec;
    const levels = spec ? drillLevels(spec, this.#options.snapshot()) : null;
    crumbs.hidden = !spec || levels === null;
    if (!spec || levels === null) return;
    const path = spec.drill?.path ?? [];
    const label = this.#options.label;
    const step = (text: string, depth: number | null): void => {
      const el = doc.createElement(depth === null ? 'span' : 'button');
      el.className = depth === null ? 'dc-chartpanel-crumb-here' : 'dc-chartpanel-crumb';
      el.textContent = text;
      if (depth !== null) {
        (el as HTMLButtonElement).type = 'button';
        el.title = 'Back up to here';
        el.addEventListener('click', () => {
          if (!this.#spec) return;
          this.#spec = drillUp(this.#spec, depth!);
          this.#options.onSpec?.();
          this.#paintCrumbs();
          this.refresh(0);
        });
      }
      if (crumbs.childElementCount > 0) crumbs.append(doc.createTextNode(' \u203a '));
      crumbs.append(el);
    };
    if (path.length === 0) {
      step(`Double-click a mark to drill into ${levels[1] ?? ''}`, null);
      crumbs.classList.add('dc-chartpanel-crumbs-hint');
      return;
    }
    crumbs.classList.remove('dc-chartpanel-crumbs-hint');
    step(`All ${levels[0]}`, 0);
    path.forEach((v, i) => step(label(v, levels[i]!, undefined), i + 1 < path.length ? i + 1 : null));
    step(`by ${levels[path.length]}`, null);
  }

  #say(text: string, bad = false): void {
    // said only when there is something to say: a small chart needs its height; written only when it changed -- a
    // tick says what it said, and its text laid out again would cost the tick
    this.#status.hidden = text === '';
    if (this.#status.textContent !== text) this.#status.textContent = text;
    this.#status.classList.toggle('dc-chartpanel-bad', bad);
  }
}
