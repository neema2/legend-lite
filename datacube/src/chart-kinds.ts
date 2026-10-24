// BATCH 3'S KINDS (docs/DATACUBE_CHARTS_DESIGN_2026_10_09.md §3): a chart spec and its rows as the ECharts option of
// a kind the first seven do not draw -- lollipop, dumbbell, range, waterfall, Pareto, funnel, rose, polar bar, radar,
// parallel coordinates, Sankey, chord, streamgraph, calendar, gauge, batch 4's box plot, and batch 3's leftovers (a
// Marimekko, a waffle, a Gantt) -- and the grid's rows as a sunburst or a tree.
//
// Its own module, fetched with the first chart of one of these kinds (ui/chart-panel.ts), as is the registration of the
// rarer ECharts charts it draws with (chart-render-kinds.ts): a page of bars never downloads either. PURE and
// library-free, as chart-option.ts is: a plain object, tested in node as SVG.
//
// THE DATABASE COMPUTES, THIS LAYS OUT (Java orchestrates, the database executes): every figure drawn is a column of
// the chart's query -- a waterfall's starts and ends, a Pareto's cumulative share (chart-spec.ts chartSnapshot,
// calc-presets.ts) -- or the grid's own row (a sunburst's rings). What happens here is placement: a category's index,
// a value's position, a calendar's years, all by comparing what the rows say.

import { AXIS_UPPER, axisName, drilledView, facetColumns, markLabel, MAX_FACETS, measureName, rangesAcross, type ChartMeasure, type ChartSpec } from './chart-spec.ts';
import { marimekkoNames, paretoName, WAFFLE_CELLS, waffleNames, waterfallNames } from './calc-presets.ts';
import {
  asOption,
  chartOption,
  column,
  compareScalars,
  defaultLabel,
  describe,
  distinct,
  escapeHtml,
  gridTree,
  keyText,
  legendOf,
  LIGHT_THEME,
  localTime,
  MAX_SCATTER_SERIES,
  MAX_SERIES,
  measureText,
  measureTitle,
  periodText,
  toolboxOf,
  type ChartDrawing,
  type ChartTheme,
  type GridNode,
  type GridShown,
  type LabelOf,
  type MarkKey,
  type UnitOf,
} from './chart-option.ts';
import type { ResultTable, Scalar } from '../../engine-client/src/result.ts';
import { numberOf } from '../../engine-client/src/values.ts';
import { UI_LOCALE } from '../../engine-client/src/locale.ts';
import { isTemporal } from '../../engine-client/src/types.ts';

/** The most years a calendar draws, one under another. */
export const MAX_CALENDAR_YEARS = 3;

/** The most flows a Sankey or a chord draws: past it, a knot. */
export const MAX_FLOWS = 200;

/** A kind of batch 3's, drawn from its query's rows. */
export function kindOption(
  drilled: ChartSpec,
  rows: ResultTable,
  theme: ChartTheme = LIGHT_THEME,
  label: LabelOf = defaultLabel,
): ChartDrawing {
  const spec = drilledView(drilled).spec;
  const notes: string[] = [];
  const text = { color: theme.inkSecondary, fontFamily: theme.font };
  const axisCommon = {
    axisLine: { lineStyle: { color: theme.axis } },
    axisTick: { show: false },
    axisLabel: { color: theme.muted, hideOverlap: true },
    splitLine: { lineStyle: { color: theme.grid, width: 1, type: 'solid' as const } },
  };
  const tooltip = { backgroundColor: theme.surface, borderColor: theme.axis, textStyle: { color: theme.ink }, confine: true };
  const base = {
    backgroundColor: 'transparent',
    textStyle: text,
    color: [...theme.series],
    aria: { enabled: true, label: { enabled: false }, decal: { show: spec.options.decal === true } },
    grid: { left: 8, right: 16, top: 40, bottom: 8, containLabel: true },
    ...toolboxOf(spec, rows, theme),
  };
  const first = spec.y[0];
  const empty = (why: string): ChartDrawing => ({ option: asOption(base), keyAt: () => null, notes: [why], description: why });
  if (!first) return empty('Nothing to draw.');
  const valueCol = (m: ChartMeasure) => column(rows, measureName(m));
  const num = (m: ChartMeasure, i: number): number | null => {
    const c = valueCol(m);
    return numberOf(c?.values[i] ?? null, c?.type);
  };
  const write = (m: ChartMeasure, v: number | null): string => measureText(m, v, rows, label);

  // -- one value: a gauge ------------------------------------------------------------------------------------------
  if (spec.mark === 'gauge') return gauge(spec, rows, theme, label, base, tooltip);
  // -- rows, every pair of their values: a scatter matrix --------------------------------------------------------
  if (spec.mark === 'matrix') return matrix(spec, rows, theme, label, base, tooltip, axisCommon);

  const across = axisName(spec);
  const xCol = across !== undefined ? column(rows, across) : undefined;
  if (!xCol) return empty('Nothing to draw.');
  // the rows arrive in the chart's order (its query's sort): the categories keep it
  const categories = distinct(xCol.values);
  const catLabels = categories.map((c) => (spec.grain !== undefined && c !== null ? periodText(String(c), spec.grain) : label(c, spec.x!, xCol.type)));
  const rowOf = new Map<string, number>();
  xCol.values.forEach((v, i) => { if (!rowOf.has(keyText(v))) rowOf.set(keyText(v), i); });
  const at = (ci: number): number => rowOf.get(keyText(categories[ci]!)) ?? -1;
  const keyOf = (ci: number): MarkKey | null => (categories[ci] === undefined ? null : { [spec.x!]: categories[ci]! });
  const horizontal = spec.options.orientation === 'horizontal';
  // a hover: the category, the values drawn, and the further measures asked for, as the grid writes each
  const hover = (ci: number, drawn: readonly ChartMeasure[], row = at(ci)): string => {
    const r = row;
    return [
      `<b>${escapeHtml(catLabels[ci] ?? '')}</b>`,
      ...drawn.map((m) => `${escapeHtml(measureTitle(m))}: <b>${escapeHtml(write(m, num(m, r)))}</b>`),
      ...(spec.tooltip ?? []).map((m) => `&nbsp;&nbsp;${escapeHtml(measureTitle(m))}: ${escapeHtml(write(m, num(m, r)))}`),
    ].join('<br>');
  };
  const categoryAxis = { ...axisCommon, type: 'category' as const, data: catLabels, splitLine: { show: false } };
  const valueAxis = (m: ChartMeasure) => ({
    ...axisCommon, type: 'value' as const, scale: spec.mark === 'dumbbell' || spec.mark === 'range' || spec.mark === 'boxplot',
    axisLabel: { ...axisCommon.axisLabel, formatter: (v: number) => write(m, v) },
  });
  const axes = (m: ChartMeasure) => (horizontal
    ? { xAxis: valueAxis(m), yAxis: { ...categoryAxis, inverse: true } }
    : { xAxis: categoryAxis, yAxis: valueAxis(m) });
  const itemTooltip = (drawn: readonly ChartMeasure[]) => ({
    ...tooltip, trigger: 'item' as const,
    formatter: (p: { dataIndex: number; data?: { ci?: number } }) => hover(p.data?.ci ?? p.dataIndex, drawn),
  });
  // A PERIOD'S OR A BIN'S END (chart-spec.ts AXIS_UPPER), the database's, for a click on a category of periods or bins
  // to filter to its range (markConditions): the greatest its rows say -- compared, never computed (chartOption's rule).
  // The clicked item says its category where its index does not (a calendar's day, in its year's series)
  const upperCol = rangesAcross(spec) ? column(rows, AXIS_UPPER) : undefined;
  const upperAt = (dataIndex: number, datum?: unknown): Scalar | undefined => {
    const ci = (datum as { ci?: number } | undefined)?.ci ?? dataIndex;
    if (!upperCol || categories[ci] === undefined) return undefined;
    const want = keyText(categories[ci]!);
    let best: Scalar | undefined;
    xCol.values.forEach((xv, i) => {
      const u = upperCol.values[i] ?? null;
      if (keyText(xv) !== want || u === null) return;
      if (best === undefined || best === null || compareScalars(u, best, upperCol.type) > 0) best = u;
    });
    return best;
  };
  const done = (option: object, keyAt: ChartDrawing['keyAt'] = (_s, i) => keyOf(i)): ChartDrawing => ({
    option: asOption(option), keyAt, notes, description: describe(spec, rows), ...(upperCol ? { upperAt } : {}),
  });
  // a point on a category: [category, value] -- or [value, category] across a horizontal chart
  const pointAt = (ci: number, v: number | null): (number | null)[] => (horizontal ? [v, ci] : [ci, v]);

  switch (spec.mark) {
    // -- a stem and a dot per category ------------------------------------------------------------------------------
    case 'lollipop': {
      const values = categories.map((_, ci) => num(first, at(ci)));
      const colour = theme.series[0]!;
      return done({
        ...base,
        tooltip: itemTooltip([first]),
        ...axes(first),
        series: [
          { type: 'bar', name: measureTitle(first), data: values, barWidth: 2, itemStyle: { color: colour }, silent: true, tooltip: { show: false } },
          {
            type: 'scatter', name: measureTitle(first), symbolSize: 14, itemStyle: { color: colour, borderColor: theme.surface, borderWidth: 2 },
            data: values.map((v, ci) => pointAt(ci, v)),
            label: { show: spec.options.labels, position: horizontal ? 'right' : 'top', color: theme.inkSecondary, formatter: (p: { dataIndex: number }) => write(first, values[p.dataIndex] ?? null) },
          },
        ],
      });
    }

    // -- two values per category: a dot each and the line between them (dumbbell), or the bar from one to the other --
    case 'dumbbell':
    case 'range': {
      const [lo, hi] = [spec.y[0]!, spec.y[1]!];
      const pairs = categories.map((_, ci) => [num(lo, at(ci)), num(hi, at(ci))] as const);
      const dumbbell = spec.mark === 'dumbbell';
      const span = {
        type: 'custom',
        name: `${measureTitle(lo)} to ${measureTitle(hi)}`,
        // [category, one value, the other]: drawn between the two places on the value axis -- placed, not computed
        data: pairs.map(([a, b], ci) => ({ value: [ci, a, b], ci })),
        encode: horizontal ? { y: 0, x: [1, 2] } : { x: 0, y: [1, 2] },
        renderItem: (_params: unknown, api: CustomApi) => {
          const ci = api.value(0) as number;
          const a = api.value(1) as number | null;
          const b = api.value(2) as number | null;
          if (a === null || b === null || Number.isNaN(a) || Number.isNaN(b)) return null;
          const from = api.coord(horizontal ? [a, ci] : [ci, a]);
          const to = api.coord(horizontal ? [b, ci] : [ci, b]);
          if (dumbbell) {
            return { type: 'line', shape: { x1: from[0], y1: from[1], x2: to[0], y2: to[1] }, style: { stroke: theme.axis, lineWidth: 3 } };
          }
          const band = api.size(horizontal ? [0, 1] : [1, 0]) as number[];
          const width = Math.min(24, (horizontal ? band[1]! : band[0]!) * 0.6);
          const rect = horizontal
            ? { x: Math.min(from[0]!, to[0]!), y: from[1]! - width / 2, width: Math.abs(to[0]! - from[0]!), height: width }
            : { x: from[0]! - width / 2, y: Math.min(from[1]!, to[1]!), width, height: Math.abs(to[1]! - from[1]!) };
          return { type: 'rect', shape: { ...rect, r: 3 }, style: { fill: theme.series[0], stroke: theme.surface, lineWidth: 1 } };
        },
        z: 1,
      };
      const dots = [lo, hi].map((m, k) => ({
        type: 'scatter', name: measureTitle(m), symbolSize: dumbbell ? 14 : 0, z: 2,
        itemStyle: { color: theme.series[k]!, borderColor: theme.surface, borderWidth: 2 },
        data: pairs.map((p, ci) => ({ value: pointAt(ci, p[k]!), ci })),
        label: {
          show: spec.options.labels, position: horizontal ? (k === 0 ? 'left' : 'right') : (k === 0 ? 'bottom' : 'top'),
          color: theme.inkSecondary, formatter: (p: { dataIndex: number }) => write(m, pairs[p.dataIndex]?.[k] ?? null),
        },
      }));
      return done({
        ...base,
        tooltip: itemTooltip([lo, hi]),
        // the legend names the two ends, not the line between them
        legend: { ...legendOf(spec, dumbbell ? 2 : 0, theme), data: [lo, hi].map(measureTitle) },
        ...axes(lo),
        series: [span, ...dots],
      }, (_s, i) => keyOf(i));
    }

    // -- the spread of a column per category: a box from its first quartile to its third, the median across it, a
    // whisker to its least and its most -- five figures the database computed (BOX_FNS), placed here ----------------
    case 'boxplot': {
      const five = spec.y.slice(0, 5);
      const median = five[2] ?? first;
      const boxes = categories.map((_, ci) => five.map((m) => num(m, at(ci))));
      const colour = theme.series[0]!;
      const words = ['least', 'first quartile', 'median', 'third quartile', 'most'];
      // a category with no numbers has no box: ECharts' empty value, never a box drawn at zero
      const data = boxes.map((b, ci) => ({ value: b.some((v) => v === null) ? ['-', '-', '-', '-', '-'] : b, ci }));
      return done({
        ...base,
        tooltip: {
          ...tooltip, trigger: 'item' as const,
          formatter: (p: { dataIndex: number; data?: { ci?: number } }) => {
            const ci = p.data?.ci ?? p.dataIndex;
            return [
              `<b>${escapeHtml(catLabels[ci] ?? '')}</b>`,
              ...five.map((m, k) => `${words[k]}: <b>${escapeHtml(write(m, boxes[ci]?.[k] ?? null))}</b>`),
            ].join('<br>');
          },
        },
        ...axes(median),
        series: [{
          type: 'boxplot', name: first.column, data, boxWidth: [8, 40],
          itemStyle: { color: theme.surface, borderColor: colour, borderWidth: 1.5 },
          emphasis: { itemStyle: { borderColor: colour, borderWidth: 2.5 } },
        }],
      });
    }

    // -- shares as cells: each column as wide as its share of the whole, each cell up it as tall as its share of the
    // column -- four edges per cell, the database's (calc-presets.ts marimekkoColumns), placed here --------------------
    case 'marimekko': {
      const names = marimekkoNames(measureName(first));
      const splitCol = spec.split !== undefined ? column(rows, spec.split) : undefined;
      const edge = { x0: column(rows, names.x0), x1: column(rows, names.x1), y0: column(rows, names.y0), y1: column(rows, names.y1) };
      if (!splitCol || !edge.x0 || !edge.x1 || !edge.y0 || !edge.y1) return empty('Nothing to draw.');
      // parts of a whole: a value below zero has no place in one (its cell would be drawn upside down, its column's
      // total less than its parts) -- said, the chart not drawn (the phase 7 audit)
      const below = xCol.values.filter((_, i) => (num(first, i) ?? 0) < 0).length;
      if (below > 0) return empty(`A Marimekko shows parts of a whole: ${below} ${below === 1 ? 'value is' : 'values are'} below zero.`);
      const ciOf = new Map(categories.map((c, ci) => [keyText(c), ci] as const));
      const at4 = (i: number): [number, number, number, number] | null => {
        const v = [edge.x0!, edge.x1!, edge.y0!, edge.y1!].map((c) => numberOf(c.values[i] ?? null, c.type));
        return v.every((n) => n !== null && Number.isFinite(n)) ? v as [number, number, number, number] : null;
      };
      const groups = distinct(splitCol.values);
      if (groups.length > MAX_SERIES) notes.push(`Showing the first ${MAX_SERIES} of ${groups.length} parts.`);
      const shown = groups.slice(0, MAX_SERIES);
      const unplaced: string[] = [];
      const percent = (v: number): string => new Intl.NumberFormat(UI_LOCALE, { style: 'percent', maximumFractionDigits: 0 }).format(v);
      const series = shown.map((g, k) => ({
        type: 'custom',
        name: label(g, spec.split!, splitCol.type),
        itemStyle: { color: theme.series[k % theme.series.length]! },
        data: xCol.values.flatMap((xv, i) => {
          if (keyText(splitCol.values[i] ?? null) !== keyText(g)) return [];
          const e = at4(i);
          if (e === null) {
            unplaced.push(`${catLabels[ciOf.get(keyText(xv)) ?? -1] ?? label(xv, spec.x!, xCol.type)} / ${label(g, spec.split!, splitCol.type)}`);
            return [];
          }
          // its category, for a click on a period or a bin to find its end (upperAt): its index is its part's series'
          return [{ value: e, row: i, ci: ciOf.get(keyText(xv)), key: { [spec.x!]: xv, [spec.split!]: g } }];
        }),
        renderItem: (_params: unknown, api: CustomApi) => {
          const p0 = api.coord([api.value(0) as number, api.value(3) as number]);
          const p1 = api.coord([api.value(1) as number, api.value(2) as number]);
          return {
            type: 'rect',
            shape: { x: p0[0]!, y: p0[1]!, width: p1[0]! - p0[0]!, height: p1[1]! - p0[1]! },
            style: { fill: api.visual('color'), stroke: theme.surface, lineWidth: 1 },
          };
        },
        encode: { x: [0, 1], y: [2, 3] },
      }));
      if (unplaced.length > 0) notes.push(`${unplaced.length} ${unplaced.length === 1 ? 'cell' : 'cells'} not drawn (no share): ${unplaced.slice(0, 5).join(', ')}.`);
      // each column named under it, at its own start: placed by the database's edges
      // a period named as the axis names it (Jan 2024), as the other kinds do
      const named = categories.map((_, ci) => [ci, at(ci)] as const).filter(([, i]) => i >= 0 && at4(i) !== null)
        .map(([ci, i]) => ({ value: [at4(i)![0], at4(i)![1]], text: catLabels[ci] ?? '' }));
      const columnNames = {
        type: 'custom', name: spec.x!, silent: true, tooltip: { show: false },
        data: named,
        renderItem: (params: { dataIndex: number }, api: CustomApi) => {
          const p0 = api.coord([api.value(0) as number, 0]);
          const p1 = api.coord([api.value(1) as number, 0]);
          return {
            type: 'text',
            style: {
              x: p0[0]! + 4, y: p0[1]! + 6, text: named[params.dataIndex]?.text ?? '', fill: theme.muted, fontSize: 11,
              width: Math.max(0, p1[0]! - p0[0]! - 8), overflow: 'truncate',
            },
          };
        },
        encode: { x: [0, 1] },
      };
      const shareAxis = { ...axisCommon, type: 'value' as const, min: 0, max: 1, axisLabel: { ...axisCommon.axisLabel, formatter: percent } };
      return done({
        ...base,
        grid: { ...base.grid, bottom: 28 },
        tooltip: {
          ...tooltip, trigger: 'item' as const,
          formatter: (p: { data?: { row?: number; key?: MarkKey } }) => {
            const i = p.data?.row;
            if (i === undefined) return '';
            const ci = ciOf.get(keyText(xCol.values[i] ?? null));
            return [`<b>${escapeHtml(catLabels[ci ?? -1] ?? '')}</b> / ${escapeHtml(label(splitCol.values[i] ?? null, spec.split!, splitCol.type))}`,
              `${escapeHtml(measureTitle(first))}: <b>${escapeHtml(write(first, num(first, i)))}</b>`].join('<br>');
          },
        },
        legend: { ...legendOf(spec, shown.length, theme), data: shown.map((g) => label(g, spec.split!, splitCol.type)) },
        // the columns' names stand under them, in the band an axis's labels would take: the widths show the shares
        xAxis: { ...shareAxis, splitLine: { show: false }, axisLabel: { show: false } },
        yAxis: shareAxis,
        series: [...series, columnNames],
      }, (_s, _i, datum) => (datum as { key?: MarkKey } | undefined)?.key ?? null);
    }

    // -- a hundred cells, ten by ten: each category the cells from its running share before it to its running share
    // through it, the database's (calc-presets.ts waffleColumns); each cell coloured by the range that holds it -------
    case 'waffle': {
      const names = waffleNames(measureName(first));
      const fromCol = column(rows, names.from);
      const toCol = column(rows, names.to);
      if (!fromCol || !toCol) return empty('Nothing to draw.');
      const below = categories.filter((_, ci) => (num(first, at(ci)) ?? 0) < 0).length;
      if (below > 0) return empty(`A waffle shows parts of a whole: ${below} ${below === 1 ? 'value is' : 'values are'} below zero.`);
      // a whole of nothing has no cells to share (the database's ranges empty)
      if (categories.every((_, ci) => toCol.values[at(ci)] === null || toCol.values[at(ci)] === undefined)) {
        return empty('The values add up to nothing: a waffle has no hundred to share.');
      }
      if (categories.length > MAX_SERIES) notes.push(`${categories.length} categories: a waffle reads ${MAX_SERIES} or fewer best.`);
      const ranges = categories.map((_, ci) => {
        const f = numberOf(fromCol.values[at(ci)] ?? null, fromCol.type);
        const t = numberOf(toCol.values[at(ci)] ?? null, toCol.type);
        return f === null || t === null ? null : [f, t] as const;
      });
      const empties = categories.filter((_, ci) => ranges[ci] === null || ranges[ci]![1] <= ranges[ci]![0]);
      if (empties.length > 0) notes.push(`${empties.length} ${empties.length === 1 ? 'category fills' : 'categories fill'} no cell (no share, or less than none): ${empties.slice(0, 5).map((c) => label(c, spec.x!, xCol.type)).join(', ')}.`);
      const side = Math.sqrt(WAFFLE_CELLS);
      const cellsOf = (ci: number): { value: [number, number]; ci: number }[] => {
        const r = ranges[ci];
        const out: { value: [number, number]; ci: number }[] = [];
        for (let c = 0; c < WAFFLE_CELLS; c++) if (r && c >= r[0] && c < r[1]) out.push({ value: [c % side, Math.floor(c / side)], ci });
        return out;
      };
      const ticks = Array.from({ length: side }, (_, k) => k);
      const hiddenAxis = { type: 'category' as const, data: ticks, show: false };
      return done({
        ...base,
        tooltip: { ...tooltip, trigger: 'item' as const, formatter: (p: { data?: { ci?: number } }) => (p.data?.ci === undefined ? '' : hover(p.data.ci, [first])) },
        legend: { ...legendOf(spec, categories.length, theme), data: catLabels },
        // the cells square (the lesser of a cell's width and height), the grid the panel's box
        grid: { ...base.grid, containLabel: false },
        xAxis: hiddenAxis,
        yAxis: { ...hiddenAxis, inverse: true },
        series: categories.map((_, ci) => ({
          type: 'custom', name: catLabels[ci], itemStyle: { color: theme.series[ci % theme.series.length]! },
          data: cellsOf(ci),
          renderItem: (_params: unknown, api: CustomApi) => {
            const centre = api.coord([api.value(0) as number, api.value(1) as number]);
            const size = api.size([1, 1]) as number[];
            const s = Math.min(size[0]!, size[1]!) * 0.86;
            return { type: 'rect', shape: { x: centre[0]! - s / 2, y: centre[1]! - s / 2, width: s, height: s, r: 2 }, style: { fill: api.visual('color') } };
          },
        })),
      }, (_s, _i, datum) => {
        const ci = (datum as { ci?: number } | undefined)?.ci;
        return ci === undefined ? null : keyOf(ci);
      });
    }

    // -- a start and an end per row, on a time axis: each bar from one to the other ----------------------------------
    case 'gantt': {
      const [startM, endM] = [spec.y[0]!, spec.y[1]!];
      const startCol = valueCol(startM);
      const endCol = valueCol(endM);
      if (!startCol || !endCol) return empty('Nothing to draw.');
      // a date's text as the time axis places it (chart-option.ts localTime: as ECharts reads a date with no zone)
      const time = (c: typeof startCol, i: number): number | null => {
        const v = c.values[i];
        return v === null || v === undefined ? null : localTime(String(v));
      };
      const undated: string[] = [];
      const backwards: string[] = [];
      const bars = categories.flatMap((_, ci) => {
        const a = time(startCol, at(ci));
        const b = time(endCol, at(ci));
        if (a === null || b === null || Number.isNaN(a) || Number.isNaN(b)) {
          undated.push(catLabels[ci] ?? '');
          return [];
        }
        if (b < a) backwards.push(catLabels[ci] ?? '');
        return [{ value: [ci, a, b], ci }];
      });
      if (backwards.length > 0) notes.push(`${backwards.length} ${backwards.length === 1 ? 'row ends' : 'rows end'} before ${backwards.length === 1 ? 'it starts' : 'they start'}: ${backwards.slice(0, 5).join(', ')}.`);
      if (undated.length > 0) notes.push(`${undated.length} ${undated.length === 1 ? 'row has' : 'rows have'} no start or no end: ${undated.slice(0, 5).join(', ')}.`);
      const dateText = (m: ChartMeasure, c: typeof startCol, i: number): string => label(c.values[i] ?? null, m.column, c.type);
      return done({
        ...base,
        tooltip: {
          ...tooltip, trigger: 'item' as const,
          formatter: (p: { data?: { ci?: number } }) => {
            const ci = p.data?.ci;
            if (ci === undefined) return '';
            return [`<b>${escapeHtml(catLabels[ci] ?? '')}</b>`,
              `${escapeHtml(measureTitle(startM))}: <b>${escapeHtml(dateText(startM, startCol, at(ci)))}</b>`,
              `${escapeHtml(measureTitle(endM))}: <b>${escapeHtml(dateText(endM, endCol, at(ci)))}</b>`].join('<br>');
          },
        },
        xAxis: { ...axisCommon, type: 'time' as const },
        yAxis: { ...categoryAxis, inverse: true },
        series: [{
          type: 'custom', name: `${measureTitle(startM)} to ${measureTitle(endM)}`,
          data: bars,
          encode: { x: [1, 2], y: 0 },
          renderItem: (_params: unknown, api: CustomApi) => {
            const ci = api.value(0) as number;
            const from = api.coord([api.value(1) as number, ci]);
            const to = api.coord([api.value(2) as number, ci]);
            const band = (api.size([0, 1]) as number[])[1]!;
            const h = Math.min(20, band * 0.6);
            return {
              type: 'rect',
              shape: { x: Math.min(from[0]!, to[0]!), y: from[1]! - h / 2, width: Math.max(2, Math.abs(to[0]! - from[0]!)), height: h, r: 3 },
              style: { fill: theme.series[0], stroke: theme.surface, lineWidth: 1 },
            };
          },
        }],
      }, (_s, _i, datum) => {
        const ci = (datum as { ci?: number } | undefined)?.ci;
        return ci === undefined ? null : keyOf(ci);
      });
    }

    // -- a bridge: each bar from where the one before ended, then the total ------------------------------------------
    case 'waterfall': {
      const base0 = measureName(first);
      const { start, end } = waterfallNames(base0);
      const starts = column(rows, start);
      const ends = column(rows, end);
      const bars = categories.map((_, ci) => {
        const r = at(ci);
        return [numberOf(starts?.values[r] ?? null, starts?.type), numberOf(ends?.values[r] ?? null, ends?.type)] as const;
      });
      // the total is where the last bar ends: the database's running total, read, not added up here
      const last = bars[bars.length - 1]?.[1] ?? null;
      // the last bar is the whole only when every category came; cut by "At most", it is the total of those shown
      const whole = rows.rowCount < spec.options.limit;
      const totalName = whole ? 'Total' : 'Total shown';
      const labels = [...catLabels, totalName];
      const good = theme.series[0]!;
      const bad = theme.bad ?? theme.series[1]!;
      const total = theme.series[2] ?? theme.series[0]!;
      const data = [
        ...bars.map(([a, b], ci) => ({ value: [ci, a, b], ci })),
        { value: [bars.length, 0, last], ci: -1 },
      ];
      return done({
        ...base,
        tooltip: {
          ...tooltip, trigger: 'item',
          formatter: (p: { data?: { ci?: number } }) => (p.data?.ci === -1
            ? `<b>${totalName}</b><br>${escapeHtml(write(first, last))}` : hover(p.data?.ci ?? 0, [first])),
        },
        xAxis: { ...categoryAxis, data: labels },
        yAxis: valueAxis(first),
        series: [{
          type: 'custom', name: measureTitle(first), data,
          encode: { x: 0, y: [1, 2] },
          renderItem: (_params: unknown, api: CustomApi) => {
            const ci = api.value(0) as number;
            const a = api.value(1) as number | null;
            const b = api.value(2) as number | null;
            if (a === null || b === null || Number.isNaN(a) || Number.isNaN(b)) return null;
            const from = api.coord([ci, a]);
            const to = api.coord([ci, b]);
            const band = api.size([1, 0]) as number[];
            const width = Math.min(32, band[0]! * 0.6);
            // a rise in the first colour, a fall in the bad one, the total in its own: which is compared, not computed
            const fill = ci === bars.length ? total : b >= a ? good : bad;
            const top = Math.min(from[1]!, to[1]!);
            const rect = {
              type: 'rect',
              shape: { x: from[0]! - width / 2, y: top, width, height: Math.max(1, Math.abs(to[1]! - from[1]!)), r: 2 },
              style: { fill, stroke: theme.surface, lineWidth: 1 },
            };
            if (!spec.options.labels) return rect;
            // its own value written above it: the category's, or the total's
            const own = ci === bars.length ? last : num(first, at(ci));
            return {
              type: 'group',
              children: [rect, {
                type: 'text', silent: true,
                style: { text: write(first, own), x: from[0]!, y: top - 4, align: 'center', verticalAlign: 'bottom', fill: theme.inkSecondary, fontSize: 11 },
              }],
            };
          },
        }],
      }, (_s, i) => keyOf(i));
    }

    // -- the vital few: bars from the largest, and their cumulative share --------------------------------------------
    case 'pareto': {
      const shareCol = column(rows, paretoName(measureName(first)));
      const values = categories.map((_, ci) => num(first, at(ci)));
      const shares = categories.map((_, ci) => numberOf(shareCol?.values[at(ci)] ?? null, shareCol?.type));
      const percent = (v: number | null): string => (v === null ? '' : new Intl.NumberFormat(UI_LOCALE, { style: 'percent', maximumFractionDigits: 1 }).format(v));
      return done({
        ...base,
        tooltip: {
          ...tooltip, trigger: 'axis', axisPointer: { type: 'shadow' },
          formatter: (ps: { dataIndex: number }[]) => {
            const ci = ps[0]?.dataIndex ?? 0;
            return `${hover(ci, [first])}<br>Cumulative share: <b>${escapeHtml(percent(shares[ci] ?? null))}</b>`;
          },
        },
        legend: legendOf(spec, 2, theme),
        xAxis: categoryAxis,
        yAxis: [
          valueAxis(first),
          { ...axisCommon, type: 'value', min: 0, max: 1, splitLine: { show: false }, axisLabel: { ...axisCommon.axisLabel, formatter: (v: number) => percent(v) } },
        ],
        series: [
          {
            type: 'bar', name: measureTitle(first), data: values, barMaxWidth: 24, itemStyle: { borderRadius: [4, 4, 0, 0] },
            label: { show: spec.options.labels, position: 'top', color: theme.inkSecondary, formatter: (p: { dataIndex: number }) => write(first, values[p.dataIndex] ?? null) },
          },
          {
            type: 'line', name: 'Cumulative share', yAxisIndex: 1, data: shares, symbol: 'circle', symbolSize: 6,
            lineStyle: { width: 2 },
            // the 80% line: where the vital few end
            markLine: { silent: true, symbol: 'none', lineStyle: { color: theme.muted, type: 'dashed' }, label: { formatter: '80%', color: theme.muted }, data: [{ yAxis: 0.8 }] },
          },
        ],
      });
    }

    // -- stages, narrowing; petals --------------------------------------------------------------------------------------
    case 'funnel':
    case 'rose': {
      const data = categories.map((_, ci) => ({ name: catLabels[ci]!, value: num(first, at(ci)), ci }));
      if (data.length > MAX_SERIES * 2) notes.push(`${data.length} ${spec.mark === 'funnel' ? 'stages' : 'petals'}: a bar reads this many better.`);
      const series = spec.mark === 'funnel' ? {
        type: 'funnel', name: measureTitle(first), data, sort: 'none', gap: 2, left: '10%', width: '80%', top: 40, bottom: 8,
        minSize: '8%', itemStyle: { borderColor: theme.surface, borderWidth: 1 },
        label: { show: true, position: 'inside', color: '#ffffff', formatter: (p: { name: string; dataIndex: number }) => (spec.options.labels ? `${p.name}: ${write(first, data[p.dataIndex]?.value ?? null)}` : p.name) },
      } : {
        type: 'pie', name: measureTitle(first), data, roseType: 'area', radius: ['12%', '72%'],
        itemStyle: { borderColor: theme.surface, borderWidth: 2, borderRadius: 4 },
        label: { show: true, color: theme.inkSecondary, formatter: (p: { name: string; dataIndex: number }) => (spec.options.labels ? `${p.name}: ${write(first, data[p.dataIndex]?.value ?? null)}` : p.name) },
      };
      return done({
        ...base,
        tooltip: itemTooltip([first]),
        legend: legendOf(spec, data.length, theme),
        series: [series],
      });
    }

    // -- bars around a circle: a polar bar (each category an angle), or radial bars (each a ring) ---------------------
    case 'polar': {
      const splitCol = spec.split !== undefined ? column(rows, spec.split) : undefined;
      const groups = splitCol ? distinct(splitCol.values).slice(0, MAX_SERIES) : [null];
      if (splitCol && distinct(splitCol.values).length > MAX_SERIES) notes.push(`Showing the first ${MAX_SERIES} series.`);
      const plotted = splitCol ? [first] : spec.y.slice(0, MAX_SERIES);
      // each series' own row per category: a split's bar and its hover read the same row
      const rowsOf: number[][] = [];
      const series = (splitCol ? groups : plotted).map((g, k) => {
        const m = splitCol ? first : plotted[k]!;
        const own = categories.map((c) => xCol.values.findIndex((v, i) => keyText(v) === keyText(c) && (!splitCol || keyText(splitCol.values[i] ?? null) === keyText(g as Scalar))));
        rowsOf.push(own);
        const values = own.map((r) => (r < 0 ? null : num(m, r)));
        return {
          type: 'bar', coordinateSystem: 'polar', name: splitCol ? label(g as Scalar, spec.split!, splitCol.type) : measureTitle(m),
          data: values, ...(spec.options.stack !== 'none' ? { stack: 'total' } : {}),
          itemStyle: { borderColor: theme.surface, borderWidth: 1 },
          emphasis: { focus: 'series' },
        };
      });
      // a ring's name sits where its bar starts: on the surface's colour, so a bar under it does not hide it
      const categoryRing = {
        type: 'category', data: catLabels, z: 10, axisLine: { lineStyle: { color: theme.axis } },
        axisLabel: { color: theme.inkSecondary, ...(horizontal ? { backgroundColor: theme.surface, padding: [1, 4], borderRadius: 2 } : {}) },
      };
      const valueRing = { type: 'value', axisLabel: { color: theme.muted, formatter: (v: number) => write(first, v) }, splitLine: { lineStyle: { color: theme.grid } } };
      return done({
        ...base,
        tooltip: { ...tooltip, trigger: 'item', formatter: (p: { dataIndex: number; seriesIndex: number; seriesName: string; value: number | null }) => `${escapeHtml(p.seriesName)}<br>${hover(p.dataIndex, plotted, splitCol ? rowsOf[p.seriesIndex]?.[p.dataIndex] ?? -1 : at(p.dataIndex))}` },
        legend: legendOf(spec, series.length, theme),
        polar: { radius: [24, '78%'] },
        // radial (horizontal): each category a ring, its value an arc; else each category an angle, its value a radius
        ...(horizontal ? { radiusAxis: categoryRing, angleAxis: { ...valueRing, startAngle: 90 } } : { angleAxis: { ...categoryRing, startAngle: 90 }, radiusAxis: valueRing }),
        series,
      }, (s, i) => {
        const key = keyOf(i);
        if (!key || !splitCol) return key;
        return { ...key, [spec.split!]: groups[s] ?? null };
      });
    }

    // -- a shape per category, a spoke per value -----------------------------------------------------------------------
    case 'radar': {
      const shown = categories.slice(0, MAX_SERIES);
      if (categories.length > MAX_SERIES) notes.push(`Showing the first ${MAX_SERIES} of ${categories.length} shapes: more cannot be told apart.`);
      return done({
        ...base,
        tooltip: { ...tooltip, trigger: 'item', formatter: (p: { dataIndex: number }) => hover(p.dataIndex, spec.y) },
        legend: legendOf(spec, shown.length, theme),
        radar: {
          indicator: spec.y.map((m) => ({ name: measureTitle(m) })),
          radius: '68%',
          axisName: { color: theme.inkSecondary },
          splitLine: { lineStyle: { color: theme.grid } },
          splitArea: { show: false },
          axisLine: { lineStyle: { color: theme.axis } },
        },
        series: [{
          type: 'radar',
          data: shown.map((_, ci) => ({ name: catLabels[ci]!, value: spec.y.map((m) => num(m, at(ci))) })),
          symbolSize: 6,
          lineStyle: { width: 2 },
          areaStyle: { opacity: 0.12 },
          emphasis: { focus: 'self' },
        }],
      }, (_s, i) => keyOf(i));
    }

    // -- a line per category through an axis per value -------------------------------------------------------------
    case 'parallel': {
      const dims = spec.y.map((m, k) => ({
        dim: k + 1, name: measureTitle(m), nameTextStyle: text,
        axisLabel: { color: theme.muted, formatter: (v: number) => write(m, v) },
        axisLine: { lineStyle: { color: theme.axis } },
      }));
      return done({
        ...base,
        tooltip: { ...tooltip, trigger: 'item', formatter: (p: { dataIndex: number }) => hover(p.dataIndex, spec.y) },
        parallel: { left: 80, right: 60, top: 48, bottom: 24 },
        parallelAxis: [
          { dim: 0, name: spec.x, type: 'category', data: catLabels, nameTextStyle: text, axisLabel: { color: theme.muted }, axisLine: { lineStyle: { color: theme.axis } } },
          ...dims,
        ],
        series: [{
          type: 'parallel', lineStyle: { width: 2, opacity: 0.6 }, emphasis: { lineStyle: { width: 3, opacity: 1 } },
          data: categories.map((_, ci) => [ci, ...spec.y.map((m) => num(m, at(ci)))]),
        }],
      }, (_s, i) => keyOf(i));
    }

    // -- flows: from the column across to the split -------------------------------------------------------------------
    case 'sankey':
    case 'chord': {
      const splitCol = column(rows, spec.split!);
      if (!splitCol) return empty('Nothing to draw.');
      // a node per value on each side, named by side so a value on both (a desk that is a source and a target) is two
      const nodes = new Map<string, { name: string; text: string; key: MarkKey }>();
      const node = (side: 'from' | 'to', v: Scalar, column: string, type: string | undefined): string => {
        const id = `${side}:${keyText(v)}`;
        if (!nodes.has(id)) nodes.set(id, { name: id, text: label(v, column, type), key: { [column]: v } });
        return id;
      };
      const links: { source: string; target: string; value: number; key: MarkKey; row: number }[] = [];
      let dropped = 0;
      let capped = 0;
      for (let i = 0; i < rows.rowCount; i++) {
        const v = num(first, i);
        const from = xCol.values[i] ?? null;
        const to = splitCol.values[i] ?? null;
        // a flow has a width: none, or a negative one, is not drawn (and counted)
        if (v === null || v <= 0) { dropped += 1; continue; }
        if (links.length >= MAX_FLOWS) { capped += 1; continue; }
        links.push({
          source: node('from', from, spec.x!, xCol.type), target: node('to', to, spec.split!, splitCol.type), value: v,
          key: { [spec.x!]: from, [spec.split!]: to }, row: i,
        });
      }
      if (dropped > 0) notes.push(`${dropped} ${dropped === 1 ? 'flow' : 'flows'} not shown: zero, negative or empty has no width.`);
      if (capped > 0) notes.push(`Showing the first ${MAX_FLOWS} flows of ${MAX_FLOWS + capped}.`);
      const nodeList = [...nodes.values()];
      const textOf = new Map(nodeList.map((n) => [n.name, n.text]));
      const formatter = (p: { dataType?: string; data?: { source?: string; target?: string; value?: number; name?: string; row?: number } }) => {
        if (p.dataType === 'edge' && p.data) {
          return `${escapeHtml(textOf.get(p.data.source ?? '') ?? '')} → ${escapeHtml(textOf.get(p.data.target ?? '') ?? '')}: <b>${escapeHtml(write(first, num(first, p.data.row ?? -1)))}</b>`;
        }
        return escapeHtml(textOf.get(p.data?.name ?? '') ?? '');
      };
      const common = {
        data: nodeList.map((n) => ({ name: n.name, key: n.key })),
        links: links.map((l) => ({ source: l.source, target: l.target, value: l.value, key: l.key, row: l.row })),
        label: { color: theme.ink, formatter: (p: { name: string }) => textOf.get(p.name) ?? '' },
        emphasis: { focus: 'adjacency' },
      };
      const series = spec.mark === 'sankey'
        ? { type: 'sankey', ...common, left: 8, right: 96, top: 16, bottom: 16, nodeGap: 10, lineStyle: { color: 'gradient', opacity: 0.35 }, itemStyle: { borderColor: theme.surface } }
        : { type: 'chord', ...common, radius: ['62%', '68%'], center: ['50%', '52%'], lineStyle: { color: 'source', opacity: 0.35 }, label: { ...common.label, show: true } };
      return done({ ...base, tooltip: { ...tooltip, trigger: 'item', formatter }, series: [series] },
        (_s, _i, datum) => (datum as { key?: MarkKey } | undefined)?.key ?? null);
    }

    // -- layers flowing along time ------------------------------------------------------------------------------------
    case 'stream': {
      const splitCol = column(rows, spec.split!);
      if (!splitCol) return empty('Nothing to draw.');
      const groups = distinct(splitCol.values);
      if (groups.length > MAX_SERIES) notes.push(`Showing the first ${MAX_SERIES} of ${groups.length} layers.`);
      const shown = new Set(groups.slice(0, MAX_SERIES).map(keyText));
      const data: [string, number, string][] = [];
      for (let i = 0; i < rows.rowCount; i++) {
        const g = splitCol.values[i] ?? null;
        const v = num(first, i);
        const t = xCol.values[i] ?? null;
        if (!shown.has(keyText(g)) || v === null || t === null) continue;
        data.push([String(t), v, label(g, spec.split!, splitCol.type)]);
      }
      return done({
        ...base,
        tooltip: { ...tooltip, trigger: 'axis', axisPointer: { type: 'line', lineStyle: { color: theme.axis } } },
        legend: legendOf(spec, shown.size, theme),
        singleAxis: { type: 'time', top: 40, bottom: 32, axisLabel: { color: theme.muted }, axisLine: { lineStyle: { color: theme.axis } }, splitLine: { show: false } },
        series: [{ type: 'themeRiver', data, label: { show: false }, emphasis: { focus: 'series' } }],
      }, () => null);
    }

    // -- a cell per day ----------------------------------------------------------------------------------------------
    case 'calendar': {
      if (!isTemporal(xCol.type)) return empty(`'${spec.x}' is not a date.`);
      const cells: { value: [string, number | null]; ci: number }[] = categories.flatMap((c, ci) =>
        (c === null ? [] : [{ value: [String(c).slice(0, 10), num(first, at(ci))] as [string, number | null], ci }]));
      // the years the days fall in, in order: compared as text, each a calendar under the one before
      const years = [...new Set(cells.map((d) => d.value[0].slice(0, 4)))].sort();
      if (years.length > MAX_CALENDAR_YEARS) notes.push(`Showing the latest ${MAX_CALENDAR_YEARS} of ${years.length} years.`);
      const drawn = years.slice(-MAX_CALENDAR_YEARS);
      let lo = Infinity;
      let hi = -Infinity;
      for (const d of cells) {
        const v = d.value[1];
        if (v === null) continue;
        if (v < lo) lo = v;
        if (v > hi) hi = v;
      }
      // the calendars share the box above the colour scale, one under another
      const height = 78 / drawn.length;
      return done({
        ...base,
        tooltip: { ...tooltip, trigger: 'item', formatter: (p: { data?: { ci?: number } }) => hover(p.data?.ci ?? 0, [first]) },
        visualMap: {
          min: Number.isFinite(lo) ? lo : 0, max: Number.isFinite(hi) ? hi : 1, calculable: true, orient: 'horizontal',
          left: 'center', bottom: 0, inRange: { color: [...theme.sequential] }, textStyle: { color: theme.muted },
          formatter: (v: number) => write(first, v),
        },
        calendar: drawn.map((y, k) => ({
          range: y, top: `${k * height + 8}%`, left: 56, right: 16, height: `${height - 8}%`, cellSize: ['auto', 'auto'],
          itemStyle: { borderColor: theme.surface, borderWidth: 2, color: theme.grid },
          splitLine: { lineStyle: { color: theme.axis } },
          yearLabel: { color: theme.inkSecondary, fontSize: 12, fontWeight: 'normal', margin: 28 },
          monthLabel: { color: theme.muted, fontSize: 11, fontWeight: 'normal' },
          dayLabel: { color: theme.muted, fontSize: 10, fontWeight: 'normal', firstDay: 1, nameMap: ['S', 'M', 'T', 'W', 'T', 'F', 'S'] },
        })),
        series: drawn.map((y, k) => ({
          type: 'heatmap', coordinateSystem: 'calendar', calendarIndex: k, name: measureTitle(first),
          data: cells.filter((d) => d.value[0].startsWith(y)),
        })),
      }, (_s, _i, datum) => {
        const ci = (datum as { ci?: number } | undefined)?.ci;
        return ci === undefined ? null : keyOf(ci);
      });
    }
  }
  throw new Error(`a ${markLabel(spec.mark)} is not drawn by kindOption`);
}

/**
 * A SCATTER MATRIX: each pair of the values plotted against each other, row by row -- a scatter in each cell off the
 * diagonal (the column's value across, the row's up), each value's name on the diagonal; a colour per value of the
 * split, as a scatter's. Its rows are the cube's rows, not groups.
 */
function matrix(
  spec: ChartSpec,
  rows: ResultTable,
  theme: ChartTheme,
  label: LabelOf,
  base: object,
  tooltip: object,
  axisCommon: Record<string, unknown>,
): ChartDrawing {
  const notes: string[] = [];
  const n = spec.y.length;
  const cols = spec.y.map((m) => column(rows, m.column));
  const splitCol = spec.split !== undefined ? column(rows, spec.split) : undefined;
  const groups = splitCol ? distinct(splitCol.values) : [null];
  if (groups.length > MAX_SCATTER_SERIES) notes.push(`Showing the first ${MAX_SCATTER_SERIES} of ${groups.length} groups: more cannot be told apart.`);
  const shown = groups.slice(0, MAX_SCATTER_SERIES);
  const inGroup = (i: number, g: Scalar): boolean => !splitCol || keyText(splitCol.values[i] ?? null) === keyText(g);
  const value = (k: number, i: number): number | null => numberOf(cols[k]?.values[i] ?? null, cols[k]?.type);
  // the cells share the box under the legend, room left below for the bottom row's labels
  const size = 88 / n;
  const grids: object[] = [];
  const xAxes: object[] = [];
  const yAxes: object[] = [];
  const series: object[] = [];
  const titles: object[] = [];
  for (let r = 0; r < n; r++) {
    for (let c = 0; c < n; c++) {
      const g = grids.length;
      grids.push({ left: `${c * (100 / n) + 5}%`, top: `${r * size + 7}%`, width: `${100 / n - 6}%`, height: `${size - 4}%` });
      // the left column's cells say their row's values, the bottom row's their column's
      xAxes.push({ ...axisCommon, gridIndex: g, type: 'value', scale: true, axisLabel: { ...(axisCommon['axisLabel'] as object), show: r === n - 1, fontSize: 9 }, splitNumber: 3 });
      yAxes.push({ ...axisCommon, gridIndex: g, type: 'value', scale: true, axisLabel: { ...(axisCommon['axisLabel'] as object), show: c === 0, fontSize: 9 }, splitNumber: 3 });
      if (r === c) {
        // a row's value as it is: its column's name (a scatter's rows are not aggregated)
        titles.push({ text: spec.y[r]!.column, left: `${c * (100 / n) + 5 + (100 / n - 6) / 2}%`, top: `${r * size + 7 + (size - 4) / 2 - 2}%`, textAlign: 'center', textStyle: { fontSize: 12, fontWeight: 'normal', color: theme.ink } });
        continue;
      }
      shown.forEach((grp, k) => {
        const data: [number, number][] = [];
        for (let i = 0; i < rows.rowCount; i++) {
          if (!inGroup(i, grp)) continue;
          const xv = value(c, i);
          const yv = value(r, i);
          if (xv !== null && yv !== null) data.push([xv, yv]);
        }
        series.push({
          type: 'scatter', xAxisIndex: g, yAxisIndex: g, symbolSize: 4, data,
          name: splitCol ? label(grp, spec.split!, splitCol.type) : 'rows',
          itemStyle: { color: theme.series[k % theme.series.length], opacity: 0.6 },
          // which pair this cell is, for its hover
          pair: [c, r],
        });
      });
    }
  }
  const write = (k: number, v: number): string => label(v, spec.y[k]!.column, cols[k]?.type);
  return {
    option: asOption({
      ...base,
      grid: grids,
      xAxis: xAxes,
      yAxis: yAxes,
      title: titles,
      legend: legendOf(spec, splitCol ? shown.length : 0, theme),
      tooltip: {
        ...tooltip, trigger: 'item',
        formatter: (p: { seriesIndex: number; seriesName?: string; value?: [number, number] }) => {
          const pair = (series[p.seriesIndex] as { pair?: [number, number] } | undefined)?.pair;
          if (!pair || !p.value) return '';
          return [...(splitCol ? [`<b>${escapeHtml(p.seriesName ?? '')}</b>`] : []),
            `${escapeHtml(spec.y[pair[0]]!.column)}: ${escapeHtml(write(pair[0], p.value[0]))}`,
            `${escapeHtml(spec.y[pair[1]]!.column)}: ${escapeHtml(write(pair[1], p.value[1]))}`].join('<br>');
        },
      },
      series,
    }),
    keyAt: () => null,
    notes,
    description: describe(spec, rows),
  };
}

/** The part of ECharts' custom series' API a span uses. */
interface CustomApi {
  value(dim: number): unknown;
  coord(point: readonly (number | null)[]): number[];
  size(delta: readonly number[]): number[] | number;
  visual(key: string): unknown;
}

/** A GAUGE: the measure over every row (one row, as a KPI's total), against its target, the scale's end. */
function gauge(
  spec: ChartSpec,
  rows: ResultTable,
  theme: ChartTheme,
  label: LabelOf,
  base: object,
  tooltip: object,
): ChartDrawing {
  const first = spec.y[0]!;
  const c = column(rows, measureName(first));
  const raw = c?.values[0] ?? null;
  const v = numberOf(raw, c?.type);
  const target = spec.options.target!;
  // the value and its target as headlines, as a KPI writes them ("$35.7m of $40m")
  const written = measureText(first, v, rows, label, true);
  const goal = measureText(first, target, rows, label, true);
  const reached = v !== null && target > 0 ? Math.round((v / target) * 100) : null;
  const notes: string[] = [];
  // past the scale's end the needle stops there: said, not hidden
  if (v !== null && v > target) notes.push(`${written} is past the target (${goal}), the gauge's end.`);
  if (v !== null && v < 0) notes.push(`${written} is below zero, the gauge's start.`);
  const higher = spec.options.better !== 'lower';
  const good = v !== null && (higher ? v >= target : v <= target);
  const colour = good ? theme.series[2] ?? theme.series[0]! : theme.series[0]!;
  const ring = spec.options.ring === true;
  const series = ring ? {
    type: 'gauge', startAngle: 90, endAngle: -270, min: 0, max: target, radius: '86%',
    pointer: { show: false },
    progress: { show: true, roundCap: true, width: 14, itemStyle: { color: colour } },
    axisLine: { roundCap: true, lineStyle: { width: 14, color: [[1, theme.grid]] } },
    splitLine: { show: false }, axisTick: { show: false }, axisLabel: { show: false },
    title: { offsetCenter: [0, '26%'], color: theme.inkSecondary, fontSize: 13 },
    detail: { offsetCenter: [0, '-6%'], color: theme.ink, fontSize: 30, fontWeight: 600, formatter: () => written },
    data: [{ value: v, name: reached === null ? `of ${goal}` : `${reached}% of ${goal}` }],
  } : {
    type: 'gauge', min: 0, max: target, radius: '92%', center: ['50%', '60%'], startAngle: 200, endAngle: -20, splitNumber: 4,
    progress: { show: true, width: 12, itemStyle: { color: colour } },
    axisLine: { lineStyle: { width: 12, color: [[1, theme.grid]] } },
    pointer: { length: '60%', width: 4, itemStyle: { color: theme.ink } },
    anchor: { show: true, size: 10, itemStyle: { color: theme.ink } },
    splitLine: { length: 8, lineStyle: { color: theme.axis } }, axisTick: { show: false },
    axisLabel: { color: theme.muted, distance: 18, formatter: (n: number) => label(n, first.column, c?.type, true) },
    title: { offsetCenter: [0, '32%'], color: theme.muted, fontSize: 12 },
    detail: { offsetCenter: [0, '62%'], color: theme.ink, fontSize: 26, fontWeight: 600, formatter: () => written },
    data: [{ value: v, name: measureTitle(first) }],
  };
  return {
    option: asOption({ ...base, tooltip: { ...tooltip, trigger: 'item', formatter: () => `${escapeHtml(measureTitle(first))}: <b>${escapeHtml(written)}</b> of ${escapeHtml(goal)}` }, series: [series] }),
    keyAt: () => null,
    notes,
    description: `Gauge: ${measureTitle(first)} is ${written}, against a target of ${goal}.`,
  };
}

/**
 * THE GRID'S ROWS AS SHOWN as a sunburst (rings, outermost the deepest level shown) or a tree (each group a node, its
 * visible rows its children): what the treemap draws, every node valued by the grid's own row -- a group's ring says
 * its total, the database's, never a sum of the rings outside it.
 */
export function gridKindOption(
  mark: 'sunburst' | 'tree',
  shown: GridShown,
  theme: ChartTheme = LIGHT_THEME,
  label: LabelOf = defaultLabel,
): ChartDrawing {
  const notes: string[] = [];
  const { data, dropped, value } = gridTree(shown, label, true);
  if (dropped > 0) notes.push(`${dropped} ${dropped === 1 ? 'row' : 'rows'} not shown: zero or negative has no ${mark === 'sunburst' ? 'angle' : 'size'}.`);
  if (!value) notes.push('The grid shows no numeric column to value the nodes by.');
  const tooltip = {
    backgroundColor: theme.surface, borderColor: theme.axis, textStyle: { color: theme.ink }, confine: true, trigger: 'item',
    formatter: (p: { name?: string; data?: { text?: string } }) =>
      (p.data?.text !== undefined ? `${escapeHtml(p.name ?? '')}: ${escapeHtml(p.data.text)}` : escapeHtml(p.name ?? '')),
  };
  const base = {
    backgroundColor: 'transparent',
    textStyle: { fontFamily: theme.font, color: theme.ink },
    color: [...theme.series],
    aria: { enabled: true, label: { enabled: false } },
    tooltip,
  };
  const series = mark === 'sunburst' ? {
    type: 'sunburst', data, radius: ['12%', '92%'], sort: undefined, nodeClick: false,
    // a ring's value is its own (the grid's group row): the rings outside it need not fill it
    itemStyle: { borderColor: theme.surface, borderWidth: 2 },
    label: { color: '#ffffff', minAngle: 8, overflow: 'truncate' },
    emphasis: { focus: 'ancestor' },
  } : {
    type: 'tree',
    data: [{ name: value?.label ?? value?.name ?? 'All', children: data, key: {} }],
    top: 16, left: 80, bottom: 16, right: 160, orient: 'LR', symbolSize: 8, expandAndCollapse: false, initialTreeDepth: -1,
    label: { position: 'left', verticalAlign: 'middle', align: 'right', color: theme.ink, formatter: (p: { name: string; data?: { text?: string } }) => (p.data?.text ? `${p.name}  ${p.data.text}` : p.name) },
    leaves: { label: { position: 'right', verticalAlign: 'middle', align: 'left' } },
    lineStyle: { color: theme.axis, width: 1.5 },
    itemStyle: { color: theme.series[0], borderColor: theme.series[0] },
  };
  return {
    option: asOption({ ...base, series: [series] }),
    keyAt: (_s, _i, datum) => {
      const key = (datum as Pick<GridNode, 'key'> | undefined)?.key;
      return key && Object.keys(key).length > 0 ? key : null;
    },
    notes,
    description: `${mark === 'sunburst' ? 'Sunburst' : 'Tree'} of ${value?.label ?? value?.name ?? 'nothing'} by the grid's rows as shown.`,
  };
}

/**
 * SMALL MULTIPLES (a facet): a chart per value of the facet's columns -- by row, by column, or a matrix of both --
 * each drawn by the chart's own drawing (chart-option.ts chartOption) from its own rows, laid out as one picture: a
 * grid per panel, its axes and series renumbered, one legend, one hover. The value axes are SHARED, so panels compare:
 * each runs to the most any panel shows (found by comparing, not computed); a stack's panels keep their own scale,
 * said.
 */
export function facetOption(
  drilled: ChartSpec,
  rows: ResultTable,
  theme: ChartTheme = LIGHT_THEME,
  label: LabelOf = defaultLabel,
  unit?: UnitOf,
): ChartDrawing {
  const spec = drilledView(drilled).spec;
  const facet = spec.facet!;
  const notes: string[] = [];
  const rowCol = facet.row !== undefined ? column(rows, facet.row) : undefined;
  const colCol = facet.column !== undefined ? column(rows, facet.column) : undefined;
  const rowValues = rowCol ? distinct(rowCol.values) : [null];
  const colValues = colCol ? distinct(colCol.values) : [null];
  // the panels that have rows, row by row
  const panels: { r: Scalar; c: Scalar; at: number[]; ri: number; cj: number }[] = [];
  for (const [ri, r] of rowValues.entries()) {
    for (const [cj, c] of colValues.entries()) {
      const at: number[] = [];
      for (let i = 0; i < rows.rowCount; i++) {
        if (rowCol && keyText(rowCol.values[i] ?? null) !== keyText(r)) continue;
        if (colCol && keyText(colCol.values[i] ?? null) !== keyText(c)) continue;
        at.push(i);
      }
      if (at.length > 0) panels.push({ r, c, at, ri, cj });
    }
  }
  if (panels.length > MAX_FACETS) notes.push(`Showing the first ${MAX_FACETS} of ${panels.length} charts.`);
  const drawn = panels.slice(0, MAX_FACETS);
  // a matrix keeps its columns; a facet of columns alone wraps; of rows alone, one under another
  const across = colCol && rowCol ? colValues.length : colCol ? Math.ceil(Math.sqrt(drawn.length)) : 1;
  // a matrix keeps each panel in its own row's and column's cell, an empty pair an empty cell; else they flow
  const matrixed = rowCol !== undefined && colCol !== undefined;
  const down = matrixed ? Math.max(1, ...drawn.map((p) => p.ri + 1)) : Math.ceil(drawn.length / across);
  const cellOf = (k: number): { col: number; row: number } => (matrixed
    ? { col: drawn[k]!.cj, row: drawn[k]!.ri } : { col: k % across, row: Math.floor(k / across) });
  const { facet: _f, ...single } = spec;
  const subs = drawn.map((p, k) => {
    const sub: ResultTable = {
      ...rows,
      columns: rows.columns.map((c) => ({ ...c, values: p.at.map((i) => c.values[i]) })) as ResultTable['columns'],
      rowCount: p.at.length,
    };
    // each panel its own top N, as one chart would be
    if (sub.rowCount > spec.options.limit && !notes.some((n) => n.startsWith('Each chart shows'))) {
      notes.push(`Each chart shows its first ${spec.options.limit} by the chosen order; raise "At most" to see more.`);
    }
    const limited: ResultTable = sub.rowCount <= spec.options.limit ? sub : {
      ...sub,
      columns: sub.columns.map((c) => ({ ...c, values: c.values.slice(0, spec.options.limit) })) as ResultTable['columns'],
      rowCount: spec.options.limit,
    };
    const one: ChartSpec = {
      ...single,
      options: { ...spec.options, zoom: false, brush: false, toolbox: false, legend: k === 0 ? spec.options.legend : 'none' },
    };
    return chartOption(one, limited, theme, label, unit);
  });
  for (const s of subs) for (const n of s.notes) if (!notes.includes(n)) notes.push(n);
  const name = (p: { r: Scalar; c: Scalar }): string => [
    ...(rowCol ? [label(p.r, facet.row!, rowCol.type)] : []),
    ...(colCol ? [label(p.c, facet.column!, colCol.type)] : []),
  ].join(' · ');
  type Axis = Record<string, unknown>;
  const list = (v: unknown): Axis[] => (v === undefined ? [] : Array.isArray(v) ? v as Axis[] : [v as Axis]);
  // THE SHARED SCALE: the least and the most any panel's first value axis shows, by comparing their values
  const stacked = spec.options.stack !== 'none';
  let lo = Infinity;
  let hi = -Infinity;
  for (const s of subs) {
    for (const series of list((s.option as { series?: unknown }).series)) {
      if (series['yAxisIndex'] === 1 || series['xAxisIndex'] === 1) continue;
      for (const d of (series['data'] as unknown[] | undefined) ?? []) {
        const v = Array.isArray(d) ? d[1] : d;
        if (typeof v !== 'number') continue;
        if (v < lo) lo = v;
        if (v > hi) hi = v;
      }
    }
  }
  const shared = !stacked && Number.isFinite(hi);
  if (stacked && drawn.length > 1) notes.push('Each stacked chart keeps its own scale.');
  const horizontal = spec.mark === 'bar' && spec.options.orientation === 'horizontal';
  const cellW = 100 / across;
  const top = 8;
  const grids: Axis[] = [];
  const titles: Axis[] = [];
  const xAxes: Axis[] = [];
  const yAxes: Axis[] = [];
  const series: Axis[] = [];
  const offsets: { series: number; panel: number }[] = [];
  const colours = new Map<string, string>();
  // each panel's series in the page's order, as their places in the panel's own drawing
  const perms: number[][] = [];
  subs.forEach((s, k) => {
    const o = s.option as Record<string, unknown>;
    const { col, row } = cellOf(k);
    const left = col * cellW;
    const y0 = top + row * (100 - top) / down;
    grids.push({ left: `${left + 1}%`, width: `${cellW - 3}%`, top: `${y0 + 4}%`, height: `${(100 - top) / down - 9}%`, containLabel: true });
    titles.push({ text: name(drawn[k]!), left: `${left + 1}%`, top: `${y0}%`, textStyle: { fontSize: 12, fontWeight: 'normal', color: theme.inkSecondary } });
    const xs = list(o['xAxis']);
    const ys = list(o['yAxis']);
    const valueAxes = horizontal ? xs : ys;
    // the shared end is the data's own figure, not a round one: its label would read oddly, so it is not written
    const scale = (a: Axis, i: number): Axis => (shared && i === 0 && valueAxes.includes(a) && a['type'] === 'value'
      ? { ...a, max: hi, ...(lo < 0 ? { min: lo } : {}), axisLabel: { ...(a['axisLabel'] as Axis | undefined), showMaxLabel: false } } : a);
    const xBase = xAxes.length;
    const yBase = yAxes.length;
    xs.forEach((a, i) => xAxes.push({ ...scale(a, i), gridIndex: k }));
    ys.forEach((a, i) => yAxes.push({ ...scale(a, i), gridIndex: k }));
    offsets.push({ series: series.length, panel: k });
    // A SERIES IS THE SAME IN EVERY PANEL: its colour, by its name, so one legend reads them all; and its place in the
    // order (a stack's bottom), the order the panels first met the names in -- each panel's own order kept beside it
    const own = list(o['series']);
    for (const sr of own) {
      const name = String(sr['name'] ?? '');
      if (!colours.has(name)) colours.set(name, theme.series[colours.size % theme.series.length]!);
    }
    const names = [...colours.keys()];
    const order = own.map((_, i) => i).sort((a, b) => names.indexOf(String(own[a]!['name'] ?? '')) - names.indexOf(String(own[b]!['name'] ?? '')));
    perms.push(order);
    for (const sr of order.map((i) => own[i]!)) {
      const colour = colours.get(String(sr['name'] ?? ''))!;
      Object.assign(sr, {
        itemStyle: { ...(sr['itemStyle'] as Axis | undefined), color: colour },
        ...(sr['type'] === 'line' ? { lineStyle: { ...(sr['lineStyle'] as Axis | undefined), color: colour } } : {}),
        ...(sr['areaStyle'] ? { areaStyle: { ...(sr['areaStyle'] as Axis), color: colour } } : {}),
      });
      // a stack is a panel's own: the same stack's name in two panels would add one panel's bars to the other's
      series.push({ ...sr, ...(typeof sr['stack'] === 'string' ? { stack: `${sr['stack']}-${k}` } : {}), xAxisIndex: xBase + ((sr['xAxisIndex'] as number | undefined) ?? 0), yAxisIndex: yBase + ((sr['yAxisIndex'] as number | undefined) ?? 0) });
    }
  });
  const panelOf = (si: number): { at: number; panel: number } => {
    let found = offsets[0]!;
    for (const o of offsets) if (o.series <= si) found = o;
    return { at: perms[found.panel]![si - found.series] ?? si - found.series, panel: found.panel };
  };
  const first = subs[0]?.option as Record<string, unknown> | undefined;
  const tooltip = first?.['tooltip'] as Record<string, unknown> | undefined;
  return {
    option: asOption({
      backgroundColor: 'transparent',
      textStyle: { color: theme.inkSecondary, fontFamily: theme.font },
      color: [...theme.series],
      aria: { enabled: true, label: { enabled: false }, decal: { show: spec.options.decal === true } },
      title: titles,
      grid: grids,
      xAxis: xAxes,
      yAxis: yAxes,
      legend: first?.['legend'],
      // one hover: each panel's own, given its own series' numbers
      tooltip: {
        ...tooltip,
        formatter: (params: unknown) => {
          const ps = (Array.isArray(params) ? params : [params]) as { seriesIndex: number }[];
          const head = ps[0];
          if (!head) return '';
          const { panel } = panelOf(head.seriesIndex);
          const own = (subs[panel]?.option as { tooltip?: { formatter?: (p: unknown) => string } }).tooltip?.formatter;
          const local = ps.filter((p) => panelOf(p.seriesIndex).panel === panel).map((p) => ({ ...p, seriesIndex: panelOf(p.seriesIndex).at }));
          return own ? own(local) : '';
        },
      },
      ...toolboxOf(spec, rows, theme),
      series,
    }),
    keyAt: (si, di, datum) => {
      const { at, panel } = panelOf(si);
      const key = subs[panel]?.keyAt(at, di, datum);
      if (!key) return null;
      const p = drawn[panel]!;
      return { ...key, ...(rowCol ? { [facet.row!]: p.r } : {}), ...(colCol ? { [facet.column!]: p.c } : {}) };
    },
    notes,
    description: `${describe(single, rows).replace(/\.$/, '')}, a chart per ${facetColumns(spec).join(' and ')}: ${drawn.length} charts.`,
  };
}
