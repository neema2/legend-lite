// The chart's options form (ui/chart-panel.ts's): what a chart shows and how, as fields. Its own module, fetched
// the first time a chart's options are shown -- a grid alone, or a chart whose options stay closed, never downloads it
// (test/bundle-budget.test.ts) -- so the fields each batch adds (docs/DATACUBE_CHARTS_DESIGN_2026_10_09.md §3) grow a
// chunk, not the grid's startup. Every handler reads the spec as it is AT THE EVENT, through its host, never the one
// the form was painted from.

import {
  boxMeasures,
  candleMeasures,
  CHART_MARKS,
  chartColumns,
  GRID_MARKS,
  LAYER_STATS,
  NO_CHART,
  SHOW_AS_KINDS,
  SPLIT_NEEDED,
  grainsOf,
  UNSPLIT,
  type ChartChange,
  type ChartFeed,
  type ChartLayer,
  type ChartLayerKind,
  type ChartMark,
  type ChartMeasure,
  type ChartSpec,
  type MeasureMark,
  type ShowAsKind,
  type TimeGrain,
} from '../chart-spec.ts';
import { SECOND_COLUMN, type AggregateFn, type CubeSnapshot } from '../snapshot.ts';
import { checkbox, dropdown, field, foldable, numberInput, section, type Choice, type FoldState } from './form.ts';
import { aggregatesFor } from './panel-column.ts';
import { GRAIN_LABELS, SHOW_AS_LABELS } from '../calc-presets.ts';
import { familyOf, isNumeric, isTemporal } from '../../../engine-client/src/types.ts';
import { chromeIcon } from './tile-icons.ts';

/** What each kind calls its split. Which kinds draw one series (no split) and which need one are the rulebook's. */
const SPLIT_LABELS: Readonly<Partial<Record<ChartMark, string>>> = {
  heatmap: 'Rows:', sankey: 'Flows to:', chord: 'Flows to:', stream: 'Layers:', pie: 'Outer ring:', marimekko: 'Made of:',
};

/**
 * The sections folded to start (the user, 2026-10-25: "more of a square"): the rarer ones, opened by a click and kept
 * open by the chart after -- unless one holds a setting, which starts it open, marked.
 */
const FOLDED_TO_START = ['Small multiples', 'Reference lines', 'Look and interaction'] as const;

/** The most values one chart plots side by side (bars, lines). */
const MAX_MEASURES = 6;

/** The spec as it is at the event: a form is painted, and its handlers run, only while its chart has one. */
function current(h: FormHost): ChartSpec {
  const spec = h.spec();
  if (!spec) throw new Error('the chart has no spec');
  return spec;
}

/** What the form reads and changes: its panel's spec, through the panel. */
export interface FormHost {
  readonly doc: Document;
  /** Which of its sections are folded, kept by its panel as the form is painted again (absent: none kept). */
  readonly folds?: FoldState;
  /** The spec as it is now. */
  spec(): ChartSpec | null;
  /**
   * The spec changed by hand (chart-spec.ts editChart: a slot told as null taken away, a hand on the grouping of a live
   * chart freezing it); `redraw` paints the form again.
   */
  set(change: ChartChange, redraw?: boolean): void;
  /** One option changed by hand; null takes it away (the chart's default again). */
  setOption<K extends keyof ChartSpec['options']>(key: K, value: ChartSpec['options'][K] | null): void;
  /** The cube as it is now. */
  snapshot(): CubeSnapshot;
}

/** A candlestick's order -- its open and close the price at the first and the last of it, a number column -- and its
 *  volume, a column's sum or none. */
function candleFields(h: FormHost, spec: ChartSpec, numbers: readonly { value: string; label: string }[]): HTMLElement[] {
  const price = spec.y[0]?.column;
  const order = spec.y[0]?.weight;
  const volume = spec.y[4]?.column;
  return [
    field(h.doc, 'Open and close by:', dropdown<string>(h.doc, order, numbers,
      (v) => price !== undefined && h.set({ y: candleMeasures(price, v ?? undefined, volume) }, true))),
    field(h.doc, 'Volume:', dropdown<string>(h.doc, volume, numbers,
      (v) => price !== undefined && h.set({ y: candleMeasures(price, order, v ?? undefined) }, true), { allowNone: true })),
  ];
}

/** A FEED'S SLOTS (K15): which of the cube's columns fills each, its headline and time required, the rest each or none. */
function feedFields(h: FormHost, spec: ChartSpec, columns: readonly { readonly name: string; readonly type?: string }[]): HTMLElement[] {
  const feed = spec.feed;
  const of = (keep: (type: string) => boolean): { value: string; label: string }[] =>
    columns.filter((c) => c.type !== undefined && keep(c.type)).map((c) => ({ value: c.name, label: c.name }));
  const text = of((t) => familyOf(t) === 'text');
  const any = columns.map((c) => ({ value: c.name, label: c.name }));
  /** One slot told: the feed as it is at the event with it changed (none: taken away); a headline or time, required. */
  const slot = (key: Exclude<keyof ChartFeed, 'tags'>, label: string, choices: { value: string; label: string }[],
    required = false): HTMLElement => field(h.doc, label, dropdown<string>(h.doc, feed?.[key], choices, (v) => {
    if (required && !v) return;
    const next: Record<string, unknown> = { ...current(h)?.feed };
    if (v) next[key] = v;
    else delete next[key];
    if (typeof next['headline'] === 'string' && typeof next['time'] === 'string') h.set({ feed: next as unknown as ChartFeed });
  }, { allowNone: !required }));
  const tags = feed?.tags ?? [];
  const tag = (i: number): HTMLElement => field(h.doc, `Tag ${i + 1}:`, dropdown<string>(h.doc, tags[i], any, (v) => {
    const now = current(h)?.feed;
    if (!now) return;
    const next = [...(now.tags ?? [])];
    if (v) next[i] = v;
    else next.splice(i, 1);
    const kept = next.filter((t) => t !== undefined);
    const { tags: _tags, ...rest } = now;
    h.set({ feed: kept.length > 0 ? { ...rest, tags: kept } : rest });
  }, { allowNone: true }));
  return [
    slot('headline', 'Headline:', text, true),
    slot('time', 'Time:', of(isTemporal), true),
    slot('source', 'Source:', any),
    tag(0),
    tag(1),
    slot('weight', 'Weight:', any),
    slot('tickers', 'Tickers:', text),
    slot('tone', 'Tone:', of(isNumeric)),
    slot('body', 'Body:', text),
    slot('key', 'Order ties by:', any),
    field(h.doc, 'At most:', numberInput(h.doc, spec.options.limit, (v) => {
      if (v !== undefined && Number.isInteger(v) && v >= 1) h.setOption('limit', v);
    }, { width: 110 })),
  ];
}

export function paintChartForm(h: FormHost, form: HTMLElement): void {
  const doc = h.doc;
    form.replaceChildren();
  const s = h.snapshot();
  const spec = current(h);
  if (!spec) {
    form.textContent = NO_CHART;
    return;
  }
  const { dimensions, measures, groupLevel } = chartColumns(s, spec ?? undefined);
  const dims = dimensions.map((c) => ({ value: c.name, label: c.name }));
  const scatter = spec.mark === 'scatter';
  // a box plot plots one number column's five figures (chart-spec.ts BOX_FNS): the column is its one choice
  const box = spec.mark === 'boxplot';
  // a candlestick plots a price's open, high, low and close (candleMeasures): the price, the order its open and close
  // are taken along, and a volume if any
  const candle = spec.mark === 'candlestick';
  const candleOrder = candle ? spec.y[0]?.weight : undefined;
  const candleVolume = candle ? spec.y[4]?.column : undefined;
  // a map's column across names each row's country; a bubble map's names each place, put where two columns say
  const map = spec.mark === 'map';
  const bubbles = spec.mark === 'bubblemap';
  // a Gantt's values are dates -- each the least or greatest of a date column -- not numbers
  const gantt = spec.mark === 'gantt';
  const dates = [...new Set([...dimensions, ...measures].filter((c) => isTemporal(c.type)).map((c) => c.name))]
    .map((name) => ({ value: name, label: name }));
  const across = scatter
    ? [...measures, ...dimensions].map((c) => ({ value: c.name, label: c.name }))
    : dims;
  const first = spec.y[0];
  const typeOf = new Map([...dimensions, ...measures].map((c) => [c.name, c.type]));
  const fns = (first && first.fn !== 'value' ? aggregatesFor(typeOf.get(first.column)) : [])
    .filter((a) => SECOND_COLUMN[a.value]?.required !== true)
    .map((a) => ({ value: a.value, label: a.label }));
  // what a measure can be: a number column, aggregated; or a group-level column, plotted as it is (`fn: 'value'`)
  const GROUP = 'group:';
  const plotted = [
    ...measures.map((c) => ({ value: c.name, label: c.name })),
    ...groupLevel.map((d) => ({ value: `${GROUP}${d.name}`, label: `${d.name} (group level)` })),
  ];
  const plottedValue = first ? (first.fn === 'value' ? `${GROUP}${first.column}` : first.column) : undefined;
  const pick = (v: string | undefined, fn: AggregateFn | undefined): void => {
    if (!v) return;
    const y = v.startsWith(GROUP)
      ? { column: v.slice(GROUP.length), fn: 'value' as const }
      : { column: v, fn: fn ?? 'sum' };
    h.set({ y: [y] }, true);
  };
  const currentFn = (): AggregateFn | undefined => {
    const f = h.spec()?.y[0]?.fn;
    return f === 'value' ? undefined : f;
  };

  // ITS KIND FIRST, then its fields in titled sections that fold (the Properties window's, docs/DATACUBE_UI_REVIEW_2026_10_24.md
  // §2): what it reads, more values, small multiples, its layout, reference lines, its labels and legend, how it looks
  // -- each section left out where the kind has nothing in it
  const kind = doc.createElement('div');
  kind.className = 'dc-chartpanel-kind';
  kind.append(field(doc, 'Chart:', dropdown<ChartMark>(doc, spec.mark, CHART_MARKS, (v) => {
    if (!v || !h.spec()) return;
    // a kind takes what it needs and leaves what it has no place for (editChart's asMark)
    h.set({ mark: v }, true);
  })));
  form.append(kind);
  // which of the rarer sections hold a setting one of their fields shows: marked as changed, and open to start -- never
  // for a setting no field there shows, which the form could not take off (the 2026-10-25 review)
  const looks = spec.mark === 'bar' || spec.mark === 'line' || spec.mark === 'area' || spec.mark === 'scatter';
  const series = spec.mark === 'bar' || spec.mark === 'line' || spec.mark === 'area';
  const holds: Readonly<Record<string, boolean>> = {
    'Small multiples': series && spec.facet !== undefined,
    'Reference lines': series && referencesHold(spec),
    'Look and interaction': looks && looksHold(spec),
  };
  const folds = h.folds ?? {};
  folds.folded ??= FOLDED_TO_START.filter((title) => !holds[title]);
  // TWO COLUMNS, each section in its own (the user, 2026-10-25: "more of a square"): what it reads on the left, how it
  // is drawn on the right -- fixed, so folding one never moves another across, as columns balanced by the browser did
  // (the 2026-10-25 review); one under the other where the window is narrow (app.css .dc-chartpanel-columns)
  const sides = [doc.createElement('div'), doc.createElement('div')] as const;
  for (const side of sides) side.className = 'dc-chartpanel-column';
  const group = (title: string, side: 0 | 1, ...children: readonly HTMLElement[]): void => {
    if (children.length === 0) return;
    const block = section(doc, title, ...children);
    foldable(doc, folds, block, holds[title] === true);
    sides[side].append(block);
  };
  /** The sections, in their columns, onto the form: an empty column left out, the other then the whole width. */
  const columns = (): void => {
    const used = sides.filter((side) => side.childElementCount > 0);
    if (used.length === 0) return;
    const both = doc.createElement('div');
    both.className = 'dc-chartpanel-columns';
    both.append(...used);
    form.append(both);
  };
  if (spec.mark === 'kpi') {
    // a number, what it measures, its trend by a column (or none), and a target with which way is good
    group('Data', 0,
      field(doc, 'Value:', dropdown<string>(doc, first?.column, measures.map((c) => ({ value: c.name, label: c.name })),
        (v) => { if (v) h.set({ y: [{ column: v, fn: currentFn() ?? 'sum' }] }, true); })),
      field(doc, 'Aggregate:', dropdown<AggregateFn>(doc, currentFn(), fns, (v) => {
        const now = h.spec()?.y[0];
        if (v && now) h.set({ y: [{ column: now.column, fn: v }] });
      })),
      field(doc, 'Trend by:', dropdown<string>(doc, spec.x, dims, (v) => h.set({ x: v ?? null }), { allowNone: true })));
    group('Target', 1,
      field(doc, 'Target:', numberInput(doc, spec.options.target, (v) => h.setOption('target', v ?? null), { width: 110 })),
      choice(h, 'Good when:', spec.options.better ?? 'higher', [
        { value: 'higher', label: 'Above it' },
        { value: 'lower', label: 'Below it' },
      ] as const, (v) => h.setOption('better', v)));
    columns();
    return;
  }
  if (spec.mark === 'gauge') {
    // one value against its target, the scale's end: a dial, or a progress ring
    group('Data', 0,
      field(doc, 'Value:', dropdown<string>(doc, first?.column, measures.map((c) => ({ value: c.name, label: c.name })),
        (v) => { if (v) h.set({ y: [{ column: v, fn: currentFn() ?? 'sum' }] }, true); })),
      field(doc, 'Aggregate:', dropdown<AggregateFn>(doc, currentFn(), fns, (v) => {
        const now = h.spec()?.y[0];
        if (v && now) h.set({ y: [{ column: now.column, fn: v }] });
      })));
    group('Target', 1,
      field(doc, 'Target:', numberInput(doc, spec.options.target, (v) => h.setOption('target', v ?? null), { width: 110 })),
      choice(h, 'Good when:', spec.options.better ?? 'higher', [
        { value: 'higher', label: 'At or above' },
        { value: 'lower', label: 'At or below' },
      ] as const, (v) => h.setOption('better', v)),
      checkbox(doc, 'As a progress ring', spec.options.ring === true, (v) => setFlag(h, 'ring', v)));
    columns();
    return;
  }
  if (spec.mark === 'matrix') {
    // rows' values against each other: what they are, and a colour per value of the split
    const numbers = measures.map((c) => ({ value: c.name, label: c.name }));
    group('Data', 0,
      field(doc, 'Value 1:', dropdown<string>(doc, first?.column, numbers, (v) => {
        if (!v || !h.spec()) return;
        h.set({ y: [{ column: v, fn: 'sum' }, ...current(h).y.slice(1)] }, true);
      })),
      ...measureRows(h, spec, numbers, typeOf, false),
      field(doc, 'Colour by:', dropdown<string>(doc, spec.split, dims, (v) => h.set({ split: v ?? null }),
        { allowNone: true })));
    columns();
    return;
  }
  if (spec.mark === 'feed') {
    // its rows into its cards: each slot a column of the cube
    group('Data', 0, ...feedFields(h, spec, [...new Map([...dimensions, ...measures].map((c) => [c.name, c])).values()]));
    columns();
    return;
  }
  if (GRID_MARKS.has(spec.mark)) {
    // what is on screen decides the rest: nothing else to choose
    const note = doc.createElement('p');
    note.className = 'dc-chartpanel-note';
    note.textContent = spec.mark === 'treemap'
      ? 'Draws the grid\'s rows as shown: each a block, sized by the first number column the grid shows. Expand a group to see inside it.'
      : spec.mark === 'sunburst'
        ? 'Draws the grid\'s rows as shown: each group a ring, its rows outside it, sized by the first number column the grid shows. Expand a group to see its ring.'
        : 'Draws the grid\'s rows as shown: each group a node, its rows its branches, valued by the first number column the grid shows. Expand a group to grow its branch.';
    form.append(note);
    if (spec.mark === 'treemap') {
      // a block coloured by a value the grid shows, either side of zero (a day's change): its reach a fixed number or
      // the largest drawn
      group('Colour', 0,
        field(doc, 'Colour by:', dropdown<string>(doc, spec.colour?.column, measures.map((c) => ({ value: c.name, label: c.name })),
          (v) => h.set({ colour: v ? { column: v, fn: 'value' } : null }), { allowNone: true })),
        field(doc, 'Colour reach:', numberInput(doc, spec.options.colourReach, (v) => h.setOption('colourReach', v !== undefined && v > 0 ? v : null),
          { width: 110 })));
      columns();
    }
    return;
  }
  const data: HTMLElement[] = [
    field(doc, scatter ? 'X:' : map ? 'Country:' : bubbles ? 'Place:' : 'Across:', dropdown<string>(doc, spec.x, across, (v) => {
      // a new column across takes its own order, and a grain or bins go with the old one (editChart)
      if (v) h.set({ x: v }, true);
    })),
    ...presetFields(h, spec, typeOf.get(spec.x ?? '')),
    // every handler reads the spec as it is AT THE EVENT, never the one
    // this form was painted from: a stale copy undid the edit before it
    field(doc, scatter ? 'Y:' : gantt ? 'Start:' : 'Value:', dropdown<string>(
      doc, scatter || box || candle || gantt ? first?.column : plottedValue,
      gantt ? dates : scatter || box || candle ? measures.map((c) => ({ value: c.name, label: c.name })) : plotted,
      (v) => (box ? v && h.set({ y: boxMeasures(v) }, true)
        : candle ? v && h.set({ y: candleMeasures(v, candleOrder, candleVolume) }, true)
        // a Gantt's start is a date column's least, unless its aggregate already gives a date (the phase 7 audit)
        : gantt ? v && h.set({ y: [{ column: v, fn: currentFn() === 'max' ? 'max' : 'min' }, ...current(h).y.slice(1)] }, true)
        : pick(v, currentFn())),
    )),
    ...(candle ? candleFields(h, spec, measures.map((c) => ({ value: c.name, label: c.name }))) : []),
    ...(scatter || box || candle || first?.fn === 'value' ? [] : [field(doc, 'Aggregate:', dropdown<AggregateFn>(
      doc, currentFn(), fns,
      (v) => {
        const now = h.spec()?.y[0];
        if (v && now) h.set({ y: [{ ...now, fn: v }] });
      },
    ))]),
    ...(UNSPLIT.has(spec.mark) ? [] : [field(doc, SPLIT_LABELS[spec.mark] ?? 'Split by:', dropdown<string>(
      doc, spec.split, dims,
      (v) => h.set({ split: v ?? null }),
      { allowNone: SPLIT_NEEDED[spec.mark] === undefined },
    ))]),
  ];
  const more: HTMLElement[] = [];
  const multiples: HTMLElement[] = [];
  const layout: HTMLElement[] = [];
  const references: HTMLElement[] = [];
  if (bubbles) {
    // where each place is: a number column of its latitude, another of its longitude (the chart groups by them)
    // a number column once, whether a dimension or a measure (chartColumns lists a numeric dimension among both)
    const numbers = [...new Set([...dimensions, ...measures].filter((c) => isNumeric(c.type)).map((c) => c.name))]
      .map((name) => ({ value: name, label: name }));
    const place = (which: 'lat' | 'lon', text: string): HTMLElement => field(doc, text, dropdown<string>(
      doc, spec.at?.[which], numbers, (v) => {
        // the one chosen so far, or both: the rules ask for the other by name until it is chosen
        const { [which]: _old, ...other } = current(h).at ?? {};
        const next = v ? { ...other, [which]: v } : other;
        h.set({ at: Object.keys(next).length > 0 ? next : null });
      }, { allowNone: true },
    ));
    data.push(place('lat', 'Latitude:'), place('lon', 'Longitude:'));
  }
  if (spec.mark === 'bar' || spec.mark === 'line' || spec.mark === 'area') {
    more.push(...measureRows(h, spec, plotted, typeOf));
    references.push(...layerFields(h, spec, plotted));
    // SMALL MULTIPLES: a chart per value of a column, in a row of charts or a column of them (both: a matrix)
    const facetField = (which: 'column' | 'row', text: string): HTMLElement => field(doc, text, dropdown<string>(
      doc, spec.facet?.[which], dims.filter((d) => d.value !== spec.x && d.value !== spec.split), (v) => {
        if (!h.spec()) return;
        const { [which]: _old, ...other } = current(h).facet ?? {};
        const next = v ? { ...other, [which]: v } : other;
        h.set({ facet: Object.keys(next).length > 0 ? next : null });
      }, { allowNone: true },
    ));
    multiples.push(facetField('column', 'A chart per:'), facetField('row', 'And a row per:'));
  }
  if (scatter) {
    // A BUBBLE: each point sized by a third column of its row
    data.push(field(doc, 'Size by:', dropdown<string>(
      doc, spec.size?.column, measures.map((c) => ({ value: c.name, label: c.name })),
      (v) => h.set({ size: v ? { column: v, fn: 'sum' } : null }), { allowNone: true },
    )));
  }
  // the kinds of several values, none drawn as another mark: a dumbbell's two ends, a radar's spokes
  if (spec.mark === 'dumbbell' || spec.mark === 'range' || spec.mark === 'radar' || spec.mark === 'parallel' || spec.mark === 'polar') {
    more.push(...measureRows(h, spec, plotted, typeOf, false));
  }
  // a Gantt's end: a date column's greatest, as its start is one's least
  if (gantt) more.push(...measureRows(h, spec, dates, typeOf, false, 'max'));
  if (spec.mark === 'bar' || spec.mark === 'lollipop' || spec.mark === 'dumbbell' || spec.mark === 'range' || spec.mark === 'polar'
    || box) {
    layout.push(choice(h, 'Orientation:', spec.options.orientation, spec.mark === 'polar' ? [
      { value: 'vertical', label: 'Around' },
      { value: 'horizontal', label: 'Radial' },
    ] as const : [
      { value: 'vertical', label: 'Vertical' },
      { value: 'horizontal', label: 'Horizontal' },
    ] as const, (v) => h.setOption('orientation', v)));
  }
  if (spec.mark === 'pie') layout.push(checkbox(doc, 'Half a ring', spec.options.half === true, (v) => setFlag(h, 'half', v)));
  if (spec.mark === 'bar' || spec.mark === 'area' || spec.mark === 'polar') {
    layout.push(choice(h, 'Stack:', spec.options.stack, [
      { value: 'none', label: 'None' },
      { value: 'stacked', label: 'Stacked' },
      { value: 'percent', label: '100%' },
    ] as const, (v) => h.setOption('stack', v)));
  }
  if (!scatter) {
    layout.push(
      field(doc, 'Order by:', dropdown(doc, `${spec.options.sort.by}:${spec.options.sort.direction}`, [
        { value: 'y:desc', label: 'Value, largest first' },
        { value: 'y:asc', label: 'Value, smallest first' },
        { value: 'x:asc', label: 'Category, A to Z' },
        { value: 'x:desc', label: 'Category, Z to A' },
      ] as const, (v) => {
        if (!v) return;
        const [by, direction] = v.split(':') as ['x' | 'y', 'asc' | 'desc'];
        h.setOption('sort', { by, direction });
      })),
      field(doc, 'At most:', numberInput(doc, spec.options.limit, (v) => {
        if (v !== undefined && v >= 1) h.setOption('limit', Math.floor(v));
      }, { min: 1, max: 5000, step: 10, width: 80 })),
    );
  }
  const labels: HTMLElement[] = [
    // a box plot writes none of its five figures on its box (the phase 5 audit), nor a map its values on its places:
    // no checkbox that does nothing
    ...(box || map || bubbles || gantt || spec.mark === 'marimekko' || spec.mark === 'waffle'
      ? [] : [checkbox(doc, 'Show values', spec.options.labels, (v) => h.setOption('labels', v))]),
    ...(looks ? labelFields(h, spec) : []),
    // a map draws no legend (its colour scale is its key), nor a Gantt (one series): no choice of where it goes
    ...(map || bubbles || gantt ? [] : [choice(h, 'Legend:', spec.options.legend, [
      { value: 'top', label: 'Top' },
      { value: 'bottom', label: 'Bottom' },
      { value: 'right', label: 'Right' },
      { value: 'none', label: 'None' },
    ] as const, (v) => h.setOption('legend', v))]),
  ];
  group('Data', 0, ...data);
  group('More values', 0, ...more);
  group('Small multiples', 0, ...multiples);
  group('Layout', 1, ...layout);
  group('Reference lines', 1, ...references);
  group('Labels and legend', 1, ...labels);
  group('Look and interaction', 1, ...(looks ? lookFields(h, spec) : []));
  columns();
  if (!scatter) {
    const hint = doc.createElement('p');
    hint.className = 'dc-chartpanel-hint';
    hint.textContent = 'Click a mark on the chart to filter the cube to it.';
    form.append(hint);
  }
}

/**
 * THE MEASURES AFTER THE FIRST (version 2): each its own column and aggregate -- or a group-level column as it is --,
 * its own kind of mark and its value axis (by unit unless told), shown as it says; one added or taken away. A split
 * draws the first measure once per value, so the others wait until it is taken off.
 */
function measureRows(h: FormHost, spec: ChartSpec, plotted: readonly { value: string; label: string }[], typeOf: ReadonlyMap<string, string | undefined>, combination = true, added: AggregateFn = 'sum'): HTMLElement[] {
  const doc = h.doc;
  const GROUP = 'group:';
  const out: HTMLElement[] = [];
  const replace = (i: number, m: ChartMeasure | null): void => {
    if (!h.spec()) return;
    const y = [...current(h).y];
    if (m === null) y.splice(i, 1);
    else y[i] = m;
    h.set({ y }, true);
  };
  spec.y.forEach((painted, i) => {
    if (i === 0) return;
    // a handler changes the value as it is AT THE EVENT, never as it was painted
    const m = painted;
    const now = (): ChartMeasure => current(h).y[i] ?? painted;
    const row = doc.createElement('div');
    row.className = 'dc-chartpanel-measure';
    const value = m.fn === 'value' ? `${GROUP}${m.column}` : m.column;
    const fns = (m.fn === 'value' ? [] : aggregatesFor(typeOf.get(m.column)))
      .filter((a) => SECOND_COLUMN[a.value]?.required !== true).map((a) => ({ value: a.value, label: a.label }));
    row.append(
      field(doc, `Value ${i + 1}:`, dropdown<string>(doc, value, plotted, (v) => {
        if (!v) return;
        replace(i, v.startsWith(GROUP) ? { ...now(), column: v.slice(GROUP.length), fn: 'value' }
          : { ...now(), column: v, fn: now().fn === 'value' ? 'sum' : now().fn });
      })),
      ...(m.fn === 'value' ? [] : [field(doc, 'Aggregate:', dropdown<AggregateFn>(doc, m.fn, fns, (v) => {
        if (v) replace(i, { ...now(), fn: v });
      }))]),
      ...(!combination ? [] : [field(doc, 'Drawn as:', dropdown<MeasureMark>(doc, m.mark ?? (spec.mark === 'bar' ? 'bar' : spec.mark as MeasureMark), [
        { value: 'bar', label: 'Bars' }, { value: 'line', label: 'Line' }, { value: 'area', label: 'Area' },
      ], (v) => { if (v) replace(i, { ...now(), mark: v }); })),
      field(doc, 'Axis:', dropdown<string>(doc, m.axis ?? 'auto', [
        { value: 'auto', label: 'By its unit' }, { value: 'left', label: 'Left' }, { value: 'right', label: 'Right' },
      ], (v) => {
        const { axis: _old, ...rest } = now();
        replace(i, v === 'left' || v === 'right' ? { ...rest, axis: v } : rest);
      }))]),
    );
    // its x at the card's corner, quiet until hovered (the Join window's condition rows')
    const remove = doc.createElement('button');
    remove.type = 'button';
    remove.className = 'dc-chartpanel-remove';
    remove.append(chromeIcon(doc, 'close'));
    remove.title = `Take value ${i + 1} off the chart`;
    remove.setAttribute('aria-label', remove.title);
    remove.addEventListener('click', () => replace(i, null));
    row.append(remove);
    out.push(row);
  });
  // a link, as the Join window's "+ Add a condition"
  const add = doc.createElement('button');
  add.type = 'button';
  add.className = 'dc-chartpanel-add';
  add.textContent = '+ Add a value';
  add.disabled = spec.split !== undefined || spec.y.length >= MAX_MEASURES;
  add.title = spec.split !== undefined
    ? 'A split draws one value per series: take the split off to plot more values.'
    : 'Another value on the chart: as a line, say, beside the bars.';
  add.addEventListener('click', () => {
    const now = current(h);
    const first = now?.y[0];
    if (!now || !first) return;
    const next = plotted.find((p) => !p.value.startsWith(GROUP) && p.value !== first.column) ?? plotted[0];
    if (!next) return;
    h.set({ y: [...now.y, { column: next.value, fn: added, ...(combination ? { mark: 'line' as const } : {}) }] }, true);
  });
  out.push(add);
  return out;
}

/**
 * REFERENCE LINES AND COLOUR BY VALUE (version 2's layers, over the first value): a target where it is told, the
 * average where the database says, the highest and lowest pinned, the marks below a threshold in the bad colour; and
 * what a hover lists besides, and what a mark's label says.
 */
function layerFields(h: FormHost, spec: ChartSpec, plotted: readonly { value: string; label: string }[]): HTMLElement[] {
  const doc = h.doc;
  // the first value's layer of a kind, in the spec as it is AT THE EVENT (a target set twice replaces itself)
  const ownIn = (layers: readonly ChartLayer[] | undefined, kind: ChartLayerKind): ChartLayer | undefined =>
    (layers ?? []).find((l) => l.kind === kind && (l.kind === 'band' || (l.measure ?? 0) === 0));
  const own = (kind: ChartLayerKind): ChartLayer | undefined => ownIn(spec.layers, kind);
  const setLayer = (kind: ChartLayerKind, layer: ChartLayer | null): void => {
    if (!h.spec()) return;
    const was = ownIn(current(h).layers, kind);
    const rest = (current(h).layers ?? []).filter((l) => l !== was);
    const next = layer ? [...rest, layer] : rest;
    h.set({ layers: next.length > 0 ? next : null });
  };
  const target = own('line');
  const threshold = own('threshold');
  const stat = own('stat');
  const measures = plotted.filter((p) => !p.value.startsWith('group:'));
  const extraOf = (m: ChartMeasure | undefined): string | undefined => m?.column;
  return [
    field(doc, 'Target:', numberInput(doc, target?.kind === 'line' ? target.value : undefined, (v) => {
      setLayer('line', v === undefined ? null : { kind: 'line', value: v, label: 'Target' });
    }, { width: 110 })),
    field(doc, 'Line at:', dropdown<string>(doc, stat?.kind === 'stat' ? stat.stat : undefined,
      LAYER_STATS.map((st) => ({ value: st, label: { average: 'The average', min: 'The minimum', max: 'The maximum' }[st] })), (v) => {
        setLayer('stat', v === 'average' || v === 'min' || v === 'max' ? { kind: 'stat', stat: v } : null);
      }, { allowNone: true })),
    checkbox(doc, 'Pin the highest and lowest', own('extremes') !== undefined, (on) => setLayer('extremes', on ? { kind: 'extremes' } : null)),
    field(doc, 'Bad below:', numberInput(doc, threshold?.kind === 'threshold' ? threshold.value : undefined, (v) => {
      setLayer('threshold', v === undefined ? null : { kind: 'threshold', value: v });
    }, { width: 110 })),
    field(doc, 'Also in hover:', dropdown<string>(doc, extraOf(spec.tooltip?.[0]), measures,
      (v) => h.set({ tooltip: v ? [{ column: v, fn: 'sum' }] : null }), { allowNone: true })),
    field(doc, 'Label with:', dropdown<string>(doc, extraOf(spec.label), measures,
      (v) => h.set({ label: v ? { column: v, fn: 'sum' } : null }), { allowNone: true })),
  ];
}

/**
 * The presets across and up (calc-presets.ts, docs/DATACUBE_CHARTS_DESIGN_2026_10_09.md §3.2): a date across by its
 * period, a number across by its bins, the measure shown as a running total, a share, a change, a moving average or
 * a rank -- each kept in the spec as its words, expanded to calculated columns when the query is built.
 */
function presetFields(h: FormHost, spec: ChartSpec, xType: string | undefined): HTMLElement[] {
  const doc = h.doc;
  if (spec.mark === 'scatter' || spec.mark === 'pie' && spec.x === undefined) return [];
  const out: HTMLElement[] = [];
  if (isTemporal(xType)) {
    out.push(field(doc, 'By:', dropdown<TimeGrain>(doc, spec.grain,
      grainsOf(xType).map((g) => ({ value: g, label: GRAIN_LABELS[g] })), (v) => h.set({ grain: v ?? null }),
      { allowNone: true })));
  } else if (isNumeric(xType)) {
    out.push(field(doc, 'Bin width:', numberInput(doc, spec.bins?.width,
      (v) => h.set({ bins: v !== undefined && v > 0 ? { width: v } : null }), { min: 0, width: 90 })));
  }
  const first = spec.y[0];
  if (first && spec.mark !== 'pie' && spec.mark !== 'heatmap') {
    out.push(field(doc, 'Show as:', dropdown<ShowAsKind>(doc, first.showAs?.kind,
      SHOW_AS_KINDS.map((k) => ({ value: k, label: SHOW_AS_LABELS[k] })), (v) => {
        const now = h.spec()?.y[0];
        if (!now || !h.spec()) return;
        const { showAs: _old, ...measure } = now;
        h.set({ y: [v ? { ...measure, showAs: { kind: v } } : measure, ...current(h).y.slice(1)] }, true);
      }, { allowNone: true })));
    if (first.showAs?.kind === 'movingAverage') {
      out.push(field(doc, 'Over rows:', numberInput(doc, first.showAs.rows ?? 3, (v) => {
        const now = h.spec()?.y[0];
        if (!now || !h.spec() || v === undefined || v < 1) return;
        h.set({ y: [{ ...now, showAs: { kind: 'movingAverage', rows: Math.floor(v) } }, ...current(h).y.slice(1)] });
      }, { min: 1, max: 365, step: 1, width: 70 })));
    }
  }
  return out;
}

/** A few short choices, one chosen, all in sight (`segmented`, below): labelled as a field is. */
function choice<T extends string>(h: FormHost, label: string, value: T, choices: readonly { value: T; label: string }[],
  onChange: (value: T) => void): HTMLElement {
  return field(h.doc, label, segmented(h.doc, value, choices, onChange, { label: label.replace(/:$/, '') }));
}

/** Where a mark's value is written, and turned: batch 2 item 12's, beside Show values. */
function labelFields(h: FormHost, spec: ChartSpec): HTMLElement[] {
  const o = spec.options;
  return [
    choice(h, 'Labels:', o.labelPosition ?? 'outside', [
      { value: 'outside', label: 'Beyond the mark' }, { value: 'inside', label: 'Inside it' },
    ] as const, (v) => h.setOption('labelPosition', v)),
    choice(h, 'Turn labels:', String(o.labelRotate ?? 0), [
      { value: '0', label: 'No' }, { value: '45', label: '45\u00b0' }, { value: '90', label: '90\u00b0' },
    ], (v) => h.setOption('labelRotate', v === '0' ? null : Number(v))),
  ];
}

/** Whether the reference lines' fields show a setting (`layerFields`): the first value's target, line, extremes or
 *  threshold, a value added to the hover, a mark's label. A layer none of them shows -- a band, another value's target,
 *  both Python's -- is not one. */
function referencesHold(spec: ChartSpec): boolean {
  const own = (kind: ChartLayerKind): boolean =>
    (spec.layers ?? []).some((l) => l.kind !== 'band' && l.kind === kind && (l.measure ?? 0) === 0);
  return own('line') || own('stat') || own('extremes') || own('threshold') || spec.tooltip?.[0] !== undefined
    || spec.label !== undefined;
}

/** Whether a chart draws a line or an area -- the smooth and step options apply to it (`lookFields`). */
function drawsLines(spec: ChartSpec): boolean {
  return spec.mark === 'line' || spec.mark === 'area' || spec.y.some((m) => m.mark === 'line' || m.mark === 'area');
}

/** Whether the look's fields show a setting (`lookFields`): smooth or step only on a chart of lines, a log axis only off
 *  a scatter -- a setting kept from another kind, with no field to take it off, is not one. */
function looksHold(spec: ChartSpec): boolean {
  const o = spec.options;
  return (drawsLines(spec) && (o.smooth === true || o.step === true))
    || (spec.mark !== 'scatter' && (o.logAxis === true || o.fitAxis === true))
    || o.labelPosition !== undefined || o.labelRotate !== undefined
    || o.dim === false || o.decal === true || o.zoom !== undefined || o.brush === true || o.toolbox === true;
}

/**
 * THE CHEAPER OPTIONS (batch 2 item 12): how a line is drawn, the value axis, hovering, pattern fills, zoom, brushing,
 * the toolbox. Each an option of the spec, off (or automatic) unless set.
 */
function lookFields(h: FormHost, spec: ChartSpec): HTMLElement[] {
  const doc = h.doc;
  const o = spec.options;
  const flag = <K extends 'smooth' | 'step' | 'logAxis' | 'fitAxis' | 'decal' | 'brush' | 'toolbox'>(key: K, text: string): HTMLElement =>
    checkbox(doc, text, o[key] === true, (on) => setFlag(h, key, on));
  return [
    ...(drawsLines(spec) ? [flag('smooth', 'Smooth lines'), flag('step', 'Step lines')] : []),
    ...(spec.mark !== 'scatter' ? [flag('logAxis', 'Log value axis'), flag('fitAxis', 'Value axis fitted to the values (not from zero)')] : []),
    field(doc, 'Labels:', dropdown(doc, o.labelPosition ?? 'outside', [
      { value: 'outside', label: 'Beyond each mark' }, { value: 'inside', label: 'Inside each mark' },
    ] as const, (v) => { if (v) h.setOption('labelPosition', v); })),
    field(doc, 'Turn labels:', dropdown<string>(doc, String(o.labelRotate ?? 0), [
      { value: '0', label: 'No' }, { value: '45', label: '45\u00b0' }, { value: '90', label: '90\u00b0' },
    ], (v) => { if (v !== undefined) h.setOption('labelRotate', v === '0' ? null : Number(v)); })),
    checkbox(doc, 'Dim the rest on hover', o.dim !== false, (on) => h.setOption('dim', on ? null : false)),
    flag('decal', 'Pattern fills'),
    choice(h, 'Zoom slider:', o.zoom === undefined ? 'auto' : o.zoom ? 'on' : 'off', [
      { value: 'auto', label: 'When long' }, { value: 'on', label: 'Always' }, { value: 'off', label: 'Never' },
    ], (v) => h.setOption('zoom', v === 'auto' ? null : v === 'on')),
    flag('brush', 'Drag to filter (brush)'),
    flag('toolbox', 'Toolbox: picture, data, CSV'),
  ];
}

/** An on/off option: on is said, off is left out (the spec stays as small as what it means). */
function setFlag(h: FormHost, key: 'smooth' | 'step' | 'logAxis' | 'fitAxis' | 'decal' | 'brush' | 'toolbox' | 'half' | 'ring', on: boolean): void {
  h.setOption(key, on ? true : null);
}

/**
 * A few choices side by side, one of them chosen: a segmented control, the Join window's kinds made small (the chosen
 * one sky-edged, app.css .dc-segmented) -- here, in the options' own chunk, as only they use it (the grid's startup
 * fetches none of it: test/bundle-budget.test.ts). Where a dropdown hid two or four short words behind a click, all of them are in
 * sight. A radio group to assistive technology and the keyboard: Tab lands on the chosen one, the arrows move the choice.
 */
function segmented<T extends string>(
  doc: Document,
  value: T,
  choices: readonly Choice<T>[],
  onChange: (value: T) => void,
  options: { label?: string } = {},
): HTMLElement {
  const group = doc.createElement('div');
  group.className = 'dc-segmented';
  group.setAttribute('role', 'radiogroup');
  if (options.label !== undefined) group.setAttribute('aria-label', options.label);
  const buttons = choices.map((c) => {
    const b = doc.createElement('button');
    b.className = 'dc-segment';
    b.type = 'button';
    b.textContent = c.label;
    b.setAttribute('role', 'radio');
    b.dataset['value'] = c.value;
    // its words in full on a hover, where a narrow column cuts them short
    b.title = c.label;
    return b;
  });
  const show = (chosen: T, focus = false): void => {
    buttons.forEach((b, i) => {
      const on = choices[i]!.value === chosen;
      b.setAttribute('aria-checked', String(on));
      b.classList.toggle('dc-chosen', on);
      b.tabIndex = on ? 0 : -1;
      if (on && focus) b.focus();
    });
    // none of them the value (an option the choices do not name): the first is still reached by Tab
    if (!choices.some((c) => c.value === chosen) && buttons[0]) buttons[0].tabIndex = 0;
  };
  const choose = (chosen: T, focus = false): void => {
    if (buttons.some((b, i) => choices[i]!.value === chosen && b.getAttribute('aria-checked') === 'true')) return;
    show(chosen, focus);
    onChange(chosen);
  };
  show(value);
  buttons.forEach((b, i) => {
    b.addEventListener('click', () => choose(choices[i]!.value));
    b.addEventListener('keydown', (e) => {
      // the arrows move the choice and wrap; Home and End go to the first and the last (a radio group's keys)
      const to = e.key === 'Home' ? 0 : e.key === 'End' ? choices.length - 1
        : e.key === 'ArrowRight' || e.key === 'ArrowDown' ? (i + 1) % choices.length
          : e.key === 'ArrowLeft' || e.key === 'ArrowUp' ? (i - 1 + choices.length) % choices.length : undefined;
      if (to === undefined) return;
      e.preventDefault();
      choose(choices[to]!.value, true);
    });
  });
  group.append(...buttons);
  return group;
}
