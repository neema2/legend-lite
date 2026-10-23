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
} from '../chart-spec.ts';
import type { FormHost } from './chart-form.ts';
// the drawing (chart-option.ts) comes with ECharts, the first time a chart draws: a grid alone fetches neither
import type { Brushed, GridShown, LabelOf, MarkKey, UnitOf } from '../chart-option.ts';
// ECharts (about 218 KB gzipped) is loaded the first time a chart draws, never by a grid alone (plan F7)
import type { ChartPicture, MountedChart } from '../chart-render.ts';
import type { ResultTable, Scalar } from '../../../engine-client/src/result.ts';
import type { CubeSnapshot, FilterNode } from '../snapshot.ts';
import type { Lambda } from '../../../pure-protocol/src/index.ts';

/** The kinds with an axis of categories or time across, whose pointer a page's other charts follow (a crosshair). */
const CROSSHAIRED: ReadonlySet<ChartMark> = new Set(['bar', 'line', 'area', 'heatmap', 'lollipop', 'dumbbell', 'range', 'waterfall', 'pareto']);

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
  /** A drilled chart's path, above it (batch 2 item 10). */
  #crumbs!: HTMLElement;
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
    // following again, it takes the grid's grouping now -- before it says so, so what it says is what it draws
    this.#spec = frozen ? { ...rest, frozen: true } : followCube(rest, this.#options.snapshot());
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
    right.append(this.#crumbs, this.#canvas, this.#status);
    this.#root.append(this.#form, right);
    this.#form.hidden = this.#options.formOpen === false;
    this.#paintForm();
  }

  #set(change: ChartChange, redrawForm = false): void {
    if (!this.#spec) return;
    // changed by hand as a chart Python makes is (chart-spec.ts editChart): a hand on the grouping of a live chart
    // freezes it, as the next pivot change would otherwise undo the choice
    const wasFrozen = this.frozen;
    this.#spec = editChart(this.#spec, change, this.#options.snapshot());
    if (this.frozen && !wasFrozen) this.#options.onFrozen?.(true);
    this.#options.onSpec?.();
    if (redrawForm) this.#paintForm();
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
    if (this.#form.hidden) {
      this.#formStale = true;
      return;
    }
    this.#formStale = false;
    void import('./chart-form.ts').then(({ paintChartForm }) => {
      if (this.#disposed || this.#form.hidden) return;
      paintChartForm(this.#formHost, this.#form);
    });
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
    // what stops it is said before anything is drawn, a chart of the grid as shown too (a slot it does not draw); a
    // query still answering would draw over the words: it is let go
    const problems = chartProblems(spec, cube);
    if (problems.length > 0) {
      this.#inflight?.abort();
      this.#inflight = null;
      this.#say(problems.join(' '), true);
      return;
    }
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
      const { treemapOfGrid, themeOf } = await import('../chart-option.ts');
      if (this.#disposed) return;
      const mark = spec.mark;
      let drawing;
      if (mark === 'sunburst' || mark === 'tree') {
        const { gridKindOption } = await import('../chart-kinds.ts');
        if (this.#disposed) return;
        drawing = gridKindOption(mark, now, themeOf(this.#canvas), this.#options.label);
      } else {
        drawing = treemapOfGrid(now, themeOf(this.#canvas), this.#options.label);
      }
      chart.show(drawing);
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
    // busy, not a "Loading" line: a line that comes and goes resizes the chart twice
    this.#canvas.setAttribute('aria-busy', 'true');
    try {
      const { query, snapshot } = chartQuery(cube, spec);
      const rows = await this.#options.run(query, snapshot, abort.signal);
      if (abort.signal.aborted || this.#disposed) return;
      const kinds = KIND_MARKS.has(spec.mark) || spec.facet !== undefined;
      const maps = MAP_MARKS.has(spec.mark);
      const chart = await this.#mount(kinds, maps);
      if (abort.signal.aborted || this.#disposed || !chart) return;
      const { chartOption, themeOf } = await import('../chart-option.ts');
      if (abort.signal.aborted || this.#disposed) return;
      let drawing;
      if (maps) {
        // batch 5's maps: their drawing and the world's countries, fetched with the first of them
        const { mapOption } = await import('../chart-maps.ts');
        if (abort.signal.aborted || this.#disposed) return;
        drawing = mapOption(spec, rows, themeOf(this.#canvas), this.#options.label);
      } else if (kinds) {
        // batch 3's kinds and small multiples: their drawing and the rarer ECharts charts, fetched with the first of them
        const { kindOption, facetOption } = await import('../chart-kinds.ts');
        if (abort.signal.aborted || this.#disposed) return;
        drawing = spec.facet !== undefined
          ? facetOption(spec, rows, themeOf(this.#canvas), this.#options.label, this.#options.unit)
          : kindOption(spec, rows, themeOf(this.#canvas), this.#options.label);
      } else {
        drawing = chartOption(spec, rows, themeOf(this.#canvas), this.#options.label, this.#options.unit);
      }
      chart.show(drawing);
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
      await import('../chart-render-kinds.ts');
      kindsRegistered = true;
      if (this.#chart && !this.#chartKnowsKinds) {
        this.#chart.dispose();
        this.#chart = null;
      }
    }
    // the maps likewise: an instance made before the map chart was registered cannot lay a map out; the shapes come
    // with the drawing (chart-maps.ts), registered once
    if (maps && !mapsRegistered) {
      const [{ registerWorld }, { WORLD }] = await Promise.all([import('../chart-render-maps.ts'), import('../chart-maps.ts')]);
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
      const { mountChart } = await import('../chart-render.ts');
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
      const { kpiDrawing, themeOf } = await import('../chart-option.ts');
      if (abort.signal.aborted || this.#disposed) return;
      const drawing = kpiDrawing(spec, rows, line, themeOf(this.#canvas), this.#options.label);
      // a chart drawn before (another kind) gives way to the number
      this.#chart?.dispose();
      this.#chart = null;
      this.#trend?.dispose();
      this.#trend = null;
      const doc = this.#doc;
      const box = doc.createElement('div');
      box.className = 'dc-kpi';
      const value = doc.createElement('div');
      value.className = 'dc-kpi-value';
      value.textContent = drawing.value;
      box.append(value);
      if (drawing.target) {
        const against = doc.createElement('div');
        against.className = `dc-kpi-target ${drawing.target.good ? 'dc-kpi-good' : 'dc-kpi-bad'}`;
        against.textContent = drawing.target.text;
        box.append(against);
      }
      const place = doc.createElement('div');
      place.className = 'dc-kpi-trend';
      if (drawing.trend) box.append(place);
      this.#canvas.replaceChildren(box);
      this.#canvas.setAttribute('aria-label', drawing.description);
      this.#canvas.classList.add('dc-chartpanel-kpi');
      if (drawing.trend) {
        const { mountChart } = await import('../chart-render.ts');
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
    // said only when there is something to say: a small chart needs its height
    this.#status.hidden = text === '';
    this.#status.textContent = text;
    this.#status.classList.toggle('dc-chartpanel-bad', bad);
  }
}
