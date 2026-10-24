// BATCH 5'S MAPS (docs/DATACUBE_CHARTS_DESIGN_2026_10_09.md §4): a chart spec and its rows as a map -- each country
// coloured by its value (`map`, a choropleth), or a bubble at each place's latitude and longitude, sized by its value
// (`bubblemap`) -- on Natural Earth's countries (maps/world-110m.ts), in the Equal Earth projection.
//
// Its own module, as chart-kinds.ts is, the shapes with it: fetched with the first map (ui/chart-panel.ts, with
// chart-render-maps.ts registering ECharts' map and geo, and the shapes this hands it), so a page with no map never
// downloads a border.
//
// THE DATABASE COMPUTES, THIS PLACES: every figure drawn is a column of the chart's query; what happens here is finding
// each row's region by comparing what it says with the shapes' codes and names (maps/shapes.ts), and placing a bubble
// at the latitude and longitude its row says. A row that finds no region is said, never placed by a guess.

import { axisName, drilledView, measureName, type ChartSpec } from './chart-spec.ts';
import {
  asOption,
  column,
  defaultLabel,
  describe,
  escapeHtml,
  LIGHT_THEME,
  measureText,
  measureTitle,
  toolboxOf,
  type ChartDrawing,
  type ChartTheme,
  type LabelOf,
  type MarkKey,
} from './chart-option.ts';
import { WORLD_110M } from './maps/world-110m.ts';
import { equalEarth, equalEarthInverse, regionFinder, WORLD_MAP } from './maps/shapes.ts';
import type { ResultTable, Scalar } from '../../engine-client/src/result.ts';
import { numberOf } from '../../engine-client/src/values.ts';

/** The most of the values a map says it could not place, by name: past it, how many more. */
const NAMED_MISSING = 5;

const findRegion = regionFinder(WORLD_110M);

/** The shapes every map draws on, and their map's name: registered with ECharts by chart-render-maps.ts. */
export const WORLD = { name: WORLD_MAP, shapes: WORLD_110M } as const;

/** A map, drawn from its query's rows. */
export function mapOption(
  drilled: ChartSpec,
  rows: ResultTable,
  theme: ChartTheme = LIGHT_THEME,
  label: LabelOf = defaultLabel,
): ChartDrawing {
  const spec = drilledView(drilled).spec;
  const notes: string[] = [];
  const base = {
    backgroundColor: 'transparent',
    textStyle: { color: theme.inkSecondary, fontFamily: theme.font },
    aria: { enabled: true, label: { enabled: false } },
    ...toolboxOf(spec, rows, theme),
  };
  const tooltip = { backgroundColor: theme.surface, borderColor: theme.axis, textStyle: { color: theme.ink }, confine: true };
  const empty = (why: string): ChartDrawing => ({ option: asOption(base), keyAt: () => null, notes: [why], description: why });
  const first = spec.y[0];
  const across = axisName(spec);
  const xCol = across !== undefined ? column(rows, across) : undefined;
  if (!first || !xCol) return empty('Nothing to draw.');
  const valueCol = column(rows, measureName(first));
  const valueAt = (i: number): number | null => numberOf(valueCol?.values[i] ?? null, valueCol?.type);
  const write = (v: number | null): string => measureText(first, v, rows, label);
  const placeText = (v: Scalar): string => label(v, spec.x!, xCol.type);
  // the countries, drawn beneath every map: the regions with no value in the map's own grey
  const geo = {
    map: WORLD_MAP,
    projection: { project: equalEarth, unproject: equalEarthInverse },
    roam: true,
    left: 8, right: 8, top: 8, bottom: 40,
    itemStyle: { areaColor: theme.grid, borderColor: theme.surface, borderWidth: 0.5 },
    emphasis: { label: { show: false }, itemStyle: { areaColor: theme.axis } },
    select: { disabled: true },
  };
  const keyOf = (i: number): MarkKey | null => {
    const v = xCol.values[i];
    return v === undefined || v === null ? null : { [spec.x!]: v };
  };
  const said = (missing: readonly string[], what: string): void => {
    if (missing.length === 0) return;
    const named = missing.slice(0, NAMED_MISSING).join(', ');
    const more = missing.length > NAMED_MISSING ? ` and ${missing.length - NAMED_MISSING} more` : '';
    notes.push(`${missing.length} ${missing.length === 1 ? what : `${what}s`} not on the map: ${named}${more}.`);
  };
  const hover = (i: number): string => [
    `<b>${escapeHtml(placeText(xCol.values[i] ?? null))}</b>`,
    `${escapeHtml(measureTitle(first))}: <b>${escapeHtml(write(valueAt(i)))}</b>`,
  ].join('<br>');
  // the least and the most the rows say, compared, for the scale's ends
  let least = Infinity;
  let most = -Infinity;
  const seen = (v: number | null): void => {
    if (v === null) return;
    if (v < least) least = v;
    if (v > most) most = v;
  };
  const scale = (dimension: number | undefined, inRange: object) => (Number.isFinite(least) ? {
    visualMap: {
      ...(dimension !== undefined ? { dimension } : {}),
      min: least, max: most === least ? least + 1 : most,
      calculable: true, orient: 'horizontal', left: 'center', bottom: 0, inRange,
      textStyle: { color: theme.muted },
      formatter: (v: number) => write(v),
    },
  } : {});

  if (spec.mark === 'map') {
    // each row's country by its code or name; a country named by two rows keeps the first, and says so
    const data: { name: string; value: number | '-'; row: number }[] = [];
    const missing: string[] = [];
    const twice: string[] = [];
    const placed = new Set<string>();
    xCol.values.forEach((v, i) => {
      const region = v === null ? undefined : findRegion(String(v));
      if (region === undefined) {
        missing.push(placeText(v));
        return;
      }
      if (placed.has(region)) {
        twice.push(placeText(v));
        return;
      }
      placed.add(region);
      const value = valueAt(i);
      seen(value);
      data.push({ name: region, value: value ?? '-', row: i });
    });
    said(missing, 'value');
    if (twice.length > 0) notes.push(`${twice.join(', ')} named a country another row named first: the first is drawn.`);
    return {
      option: asOption({
        ...base,
        tooltip: { ...tooltip, trigger: 'item', formatter: (p: { data?: { row?: number } }) => (p.data?.row === undefined ? '' : hover(p.data.row)) },
        ...scale(undefined, { color: [...theme.sequential] }),
        series: [{ type: 'map', name: measureTitle(first), ...geo, data }],
      }),
      keyAt: (_s, _i, datum) => {
        const row = (datum as { row?: number } | undefined)?.row;
        return row === undefined ? null : keyOf(row);
      },
      notes,
      description: describe(spec, rows),
    };
  }

  // A BUBBLE MAP: each place at the latitude and longitude its row says, sized by its value
  const at = spec.at;
  if (at?.lat === undefined || at.lon === undefined) return empty('Nothing to draw.');
  const latCol = column(rows, at.lat);
  const lonCol = column(rows, at.lon);
  const points: { value: [number, number, number]; row: number }[] = [];
  const unplaced: string[] = [];
  const unvalued: string[] = [];
  xCol.values.forEach((v, i) => {
    const lat = numberOf(latCol?.values[i] ?? null, latCol?.type);
    const lon = numberOf(lonCol?.values[i] ?? null, lonCol?.type);
    const value = valueAt(i);
    if (lat === null || lon === null || lat < -90 || lat > 90 || lon < -180 || lon > 180) {
      unplaced.push(placeText(v));
      return;
    }
    // a place with no value has no bubble: said, as a scatter's point with no size is
    if (value === null) {
      unvalued.push(placeText(v));
      return;
    }
    seen(value);
    points.push({ value: [lon, lat, value], row: i });
  });
  said(unplaced, 'place');
  if (unvalued.length > 0) {
    const named = unvalued.slice(0, NAMED_MISSING).join(', ');
    const more = unvalued.length > NAMED_MISSING ? ` and ${unvalued.length - NAMED_MISSING} more` : '';
    notes.push(`${unvalued.length} ${unvalued.length === 1 ? 'place has' : 'places have'} no ${measureTitle(first)} to size by: ${named}${more}.`);
  }
  return {
    option: asOption({
      ...base,
      tooltip: { ...tooltip, trigger: 'item', formatter: (p: { data?: { row?: number } }) => (p.data?.row === undefined ? '' : hover(p.data.row)) },
      geo,
      ...scale(2, { symbolSize: [6, 36] }),
      series: [{
        type: 'scatter', coordinateSystem: 'geo', name: measureTitle(first), data: points,
        itemStyle: { color: theme.series[0], opacity: 0.7, borderColor: theme.surface, borderWidth: 1 },
      }],
    }),
    keyAt: (_s, _i, datum) => {
      const row = (datum as { row?: number } | undefined)?.row;
      return row === undefined ? null : keyOf(row);
    },
    notes,
    description: describe(spec, rows),
  };
}
