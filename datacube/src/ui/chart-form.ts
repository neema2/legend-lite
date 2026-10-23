// The chart's options form (ui/chart-panel.ts's): what a chart shows and how, as fields. Its own module, fetched
// the first time a chart's options are shown -- a grid alone, or a chart whose options stay closed, never downloads it
// (test/bundle-budget.test.ts) -- so the fields each batch adds (docs/DATACUBE_CHARTS_DESIGN_2026_10_09.md §3) grow a
// chunk, not the grid's startup. Every handler reads the spec as it is AT THE EVENT, through its host, never the one
// the form was painted from.

import {
  boxMeasures,
  CHART_MARKS,
  chartColumns,
  GRID_MARKS,
  LAYER_STATS,
  NO_CHART,
  SHOW_AS_KINDS,
  SPLIT_NEEDED,
  TIME_GRAINS,
  UNSPLIT,
  type ChartChange,
  type ChartLayer,
  type ChartLayerKind,
  type ChartMark,
  type ChartMeasure,
  type ChartSpec,
  type MeasureMark,
  type ShowAsKind,
  type TimeGrain,
} from '../chart-spec.ts';
import type { AggregateFn, CubeSnapshot } from '../snapshot.ts';
import { checkbox, dropdown, field, numberInput } from './form.ts';
import { aggregatesFor } from './panel-column.ts';
import { GRAIN_LABELS, SHOW_AS_LABELS } from '../calc-presets.ts';
import { isNumeric, isTemporal } from '../../../engine-client/src/types.ts';

/** What each kind calls its split. Which kinds draw one series (no split) and which need one are the rulebook's. */
const SPLIT_LABELS: Readonly<Partial<Record<ChartMark, string>>> = {
  heatmap: 'Rows:', sankey: 'Flows to:', chord: 'Flows to:', stream: 'Layers:', pie: 'Outer ring:', marimekko: 'Made of:',
};

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
    .filter((a) => a.value !== 'wavg')
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

  const markField = field(doc, 'Chart:', dropdown<ChartMark>(doc, spec.mark, CHART_MARKS, (v) => {
    if (!v || !h.spec()) return;
    // a kind takes what it needs and leaves what it has no place for (editChart's asMark)
    h.set({ mark: v }, true);
  }));
  if (spec.mark === 'kpi') {
    // a number, what it measures, its trend by a column (or none), and a target with which way is good
    form.append(
      markField,
      field(doc, 'Value:', dropdown<string>(doc, first?.column, measures.map((c) => ({ value: c.name, label: c.name })),
        (v) => { if (v) h.set({ y: [{ column: v, fn: currentFn() ?? 'sum' }] }, true); })),
      field(doc, 'Aggregate:', dropdown<AggregateFn>(doc, currentFn(), fns, (v) => {
        const now = h.spec()?.y[0];
        if (v && now) h.set({ y: [{ column: now.column, fn: v }] });
      })),
      field(doc, 'Trend by:', dropdown<string>(doc, spec.x, dims, (v) => h.set({ x: v ?? null }), { allowNone: true })),
      field(doc, 'Target:', numberInput(doc, spec.options.target, (v) => h.setOption('target', v ?? null), { width: 110 })),
      field(doc, 'Good when:', dropdown(doc, spec.options.better ?? 'higher', [
        { value: 'higher', label: 'Above target' },
        { value: 'lower', label: 'Below target' },
      ] as const, (v) => { if (v) h.setOption('better', v); })),
    );
    return;
  }
  if (spec.mark === 'gauge') {
    // one value against its target, the scale's end: a dial, or a progress ring
    form.append(
      markField,
      field(doc, 'Value:', dropdown<string>(doc, first?.column, measures.map((c) => ({ value: c.name, label: c.name })),
        (v) => { if (v) h.set({ y: [{ column: v, fn: currentFn() ?? 'sum' }] }, true); })),
      field(doc, 'Aggregate:', dropdown<AggregateFn>(doc, currentFn(), fns, (v) => {
        const now = h.spec()?.y[0];
        if (v && now) h.set({ y: [{ column: now.column, fn: v }] });
      })),
      field(doc, 'Target:', numberInput(doc, spec.options.target, (v) => h.setOption('target', v ?? null), { width: 110 })),
      field(doc, 'Good when:', dropdown(doc, spec.options.better ?? 'higher', [
        { value: 'higher', label: 'At or above target' },
        { value: 'lower', label: 'At or below target' },
      ] as const, (v) => { if (v) h.setOption('better', v); })),
      checkbox(doc, 'As a progress ring', spec.options.ring === true, (v) => setFlag(h, 'ring', v)),
    );
    return;
  }
  if (spec.mark === 'matrix') {
    // rows' values against each other: what they are, and a colour per value of the split
    const numbers = measures.map((c) => ({ value: c.name, label: c.name }));
    form.append(
      markField,
      field(doc, 'Value 1:', dropdown<string>(doc, first?.column, numbers, (v) => {
        if (!v || !h.spec()) return;
        h.set({ y: [{ column: v, fn: 'sum' }, ...current(h).y.slice(1)] }, true);
      })),
      ...measureRows(h, spec, numbers, typeOf, false),
      field(doc, 'Colour by:', dropdown<string>(doc, spec.split, dims, (v) => h.set({ split: v ?? null }),
        { allowNone: true })),
    );
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
    form.append(markField, note);
    return;
  }
  form.append(
    markField,
    field(doc, scatter ? 'X:' : map ? 'Country:' : bubbles ? 'Place:' : 'Across:', dropdown<string>(doc, spec.x, across, (v) => {
      // a new column across takes its own order, and a grain or bins go with the old one (editChart)
      if (v) h.set({ x: v }, true);
    })),
    ...presetFields(h, spec, typeOf.get(spec.x ?? '')),
    // every handler reads the spec as it is AT THE EVENT, never the one
    // this form was painted from: a stale copy undid the edit before it
    field(doc, scatter ? 'Y:' : gantt ? 'Start:' : 'Value:', dropdown<string>(
      doc, scatter || box || gantt ? first?.column : plottedValue,
      gantt ? dates : scatter || box ? measures.map((c) => ({ value: c.name, label: c.name })) : plotted,
      (v) => (box ? v && h.set({ y: boxMeasures(v) }, true)
        // a Gantt's start is a date column's least, unless its aggregate already gives a date (the phase 7 audit)
        : gantt ? v && h.set({ y: [{ column: v, fn: currentFn() === 'max' ? 'max' : 'min' }, ...current(h).y.slice(1)] }, true)
        : pick(v, currentFn())),
    )),
    ...(scatter || box || first?.fn === 'value' ? [] : [field(doc, 'Aggregate:', dropdown<AggregateFn>(
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
  );
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
    form.append(place('lat', 'Latitude:'), place('lon', 'Longitude:'));
  }
  if (spec.mark === 'bar' || spec.mark === 'line' || spec.mark === 'area') {
    form.append(...measureRows(h, spec, plotted, typeOf), ...layerFields(h, spec, plotted));
    // SMALL MULTIPLES: a chart per value of a column, in a row of charts or a column of them (both: a matrix)
    const facetField = (which: 'column' | 'row', text: string): HTMLElement => field(doc, text, dropdown<string>(
      doc, spec.facet?.[which], dims.filter((d) => d.value !== spec.x && d.value !== spec.split), (v) => {
        if (!h.spec()) return;
        const { [which]: _old, ...other } = current(h).facet ?? {};
        const next = v ? { ...other, [which]: v } : other;
        h.set({ facet: Object.keys(next).length > 0 ? next : null });
      }, { allowNone: true },
    ));
    form.append(facetField('column', 'A chart per:'), facetField('row', 'And a row per:'));
  }
  if (scatter) {
    // A BUBBLE: each point sized by a third column of its row
    form.append(field(doc, 'Size by:', dropdown<string>(
      doc, spec.size?.column, measures.map((c) => ({ value: c.name, label: c.name })),
      (v) => h.set({ size: v ? { column: v, fn: 'sum' } : null }), { allowNone: true },
    )));
  }
  // the kinds of several values, none drawn as another mark: a dumbbell's two ends, a radar's spokes
  if (spec.mark === 'dumbbell' || spec.mark === 'range' || spec.mark === 'radar' || spec.mark === 'parallel' || spec.mark === 'polar') {
    form.append(...measureRows(h, spec, plotted, typeOf, false));
  }
  // a Gantt's end: a date column's greatest, as its start is one's least
  if (gantt) form.append(...measureRows(h, spec, dates, typeOf, false, 'max'));
  if (spec.mark === 'bar' || spec.mark === 'lollipop' || spec.mark === 'dumbbell' || spec.mark === 'range' || spec.mark === 'polar'
    || box) {
    form.append(field(doc, 'Orientation:', dropdown(doc, spec.options.orientation, spec.mark === 'polar' ? [
      { value: 'vertical', label: 'Around (a polar bar)' },
      { value: 'horizontal', label: 'Radial bars' },
    ] as const : [
      { value: 'vertical', label: 'Vertical' },
      { value: 'horizontal', label: 'Horizontal' },
    ] as const, (v) => { if (v) h.setOption('orientation', v); })));
  }
  if (spec.mark === 'pie') form.append(checkbox(doc, 'Half a ring', spec.options.half === true, (v) => setFlag(h, 'half', v)));
  if (spec.mark === 'bar' || spec.mark === 'area' || spec.mark === 'polar') {
    form.append(field(doc, 'Stack:', dropdown(doc, spec.options.stack, [
      { value: 'none', label: 'None' },
      { value: 'stacked', label: 'Stacked' },
      { value: 'percent', label: '100%' },
    ] as const, (v) => { if (v) h.setOption('stack', v); })));
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
        h.setOption('sort', { by, direction });
      })),
      field(doc, 'At most:', numberInput(doc, spec.options.limit, (v) => {
        if (v !== undefined && v >= 1) h.setOption('limit', Math.floor(v));
      }, { min: 1, max: 5000, step: 10, width: 80 })),
    );
  }
  if (spec.mark === 'bar' || spec.mark === 'line' || spec.mark === 'area' || spec.mark === 'scatter') form.append(...lookFields(h, spec));
  form.append(
    // a map draws no legend (its colour scale is its key), nor a Gantt (one series): no choice of where it goes
    ...(map || bubbles || gantt ? [] : [field(doc, 'Legend:', dropdown(doc, spec.options.legend, [
      { value: 'top', label: 'Top' },
      { value: 'bottom', label: 'Bottom' },
      { value: 'right', label: 'Right' },
      { value: 'none', label: 'None' },
    ] as const, (v) => { if (v) h.setOption('legend', v); }))]),
    // a box plot writes none of its five figures on its box (the phase 5 audit), nor a map its values on its places:
    // no checkbox that does nothing
    ...(box || map || bubbles || gantt || spec.mark === 'marimekko' || spec.mark === 'waffle'
      ? [] : [checkbox(doc, 'Show values', spec.options.labels, (v) => h.setOption('labels', v))]),
  );
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
      .filter((a) => a.value !== 'wavg').map((a) => ({ value: a.value, label: a.label }));
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
    const remove = doc.createElement('button');
    remove.type = 'button';
    remove.className = 'dc-btn dc-btn-small';
    remove.textContent = '\u00d7';
    remove.title = `Take value ${i + 1} off the chart`;
    remove.setAttribute('aria-label', remove.title);
    remove.addEventListener('click', () => replace(i, null));
    row.append(remove);
    out.push(row);
  });
  const add = doc.createElement('button');
  add.type = 'button';
  add.className = 'dc-btn dc-btn-small dc-chartpanel-add';
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
  const heading = doc.createElement('div');
  heading.className = 'dc-chartpanel-heading';
  heading.textContent = 'Reference lines';
  const measures = plotted.filter((p) => !p.value.startsWith('group:'));
  const extraOf = (m: ChartMeasure | undefined): string | undefined => m?.column;
  return [
    heading,
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
      TIME_GRAINS.map((g) => ({ value: g, label: GRAIN_LABELS[g] })), (v) => h.set({ grain: v ?? null }),
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

/**
 * THE CHEAPER OPTIONS (batch 2 item 12): how a line is drawn, the value axis, where labels sit, hovering, pattern
 * fills, zoom, brushing, the toolbox. Each an option of the spec, off (or automatic) unless set.
 */
function lookFields(h: FormHost, spec: ChartSpec): HTMLElement[] {
  const doc = h.doc;
  const o = spec.options;
  const heading = doc.createElement('div');
  heading.className = 'dc-chartpanel-heading';
  heading.textContent = 'Look and interaction';
  const flag = <K extends 'smooth' | 'step' | 'logAxis' | 'decal' | 'brush' | 'toolbox'>(key: K, text: string): HTMLElement =>
    checkbox(doc, text, o[key] === true, (on) => setFlag(h, key, on));
  const lines = spec.mark === 'line' || spec.mark === 'area' || spec.y.some((m) => m.mark === 'line' || m.mark === 'area');
  return [
    heading,
    ...(lines ? [flag('smooth', 'Smooth lines'), flag('step', 'Step lines')] : []),
    ...(spec.mark !== 'scatter' ? [flag('logAxis', 'Log value axis')] : []),
    field(doc, 'Labels:', dropdown(doc, o.labelPosition ?? 'outside', [
      { value: 'outside', label: 'Beyond each mark' }, { value: 'inside', label: 'Inside each mark' },
    ] as const, (v) => { if (v) h.setOption('labelPosition', v); })),
    field(doc, 'Turn labels:', dropdown<string>(doc, String(o.labelRotate ?? 0), [
      { value: '0', label: 'No' }, { value: '45', label: '45\u00b0' }, { value: '90', label: '90\u00b0' },
    ], (v) => { if (v !== undefined) h.setOption('labelRotate', v === '0' ? null : Number(v)); })),
    checkbox(doc, 'Dim the rest on hover', o.dim !== false, (on) => h.setOption('dim', on ? null : false)),
    flag('decal', 'Pattern fills'),
    field(doc, 'Zoom slider:', dropdown<string>(doc, o.zoom === undefined ? 'auto' : o.zoom ? 'on' : 'off', [
      { value: 'auto', label: 'For a long series' }, { value: 'on', label: 'Always' }, { value: 'off', label: 'Never' },
    ], (v) => { if (v !== undefined) h.setOption('zoom', v === 'auto' ? null : v === 'on'); })),
    flag('brush', 'Drag to filter (brush)'),
    flag('toolbox', 'Toolbox: picture, data, CSV'),
  ];
}

/** An on/off option: on is said, off is left out (the spec stays as small as what it means). */
function setFlag(h: FormHost, key: 'smooth' | 'step' | 'logAxis' | 'decal' | 'brush' | 'toolbox' | 'half' | 'ring', on: boolean): void {
  h.setOption(key, on ? true : null);
}
