// THE CHART GALLERY (the user, 2026-10-23: "add all the new chart types to our chart demo ... one gallery is great and
// turn on fit to page so users don't need to scroll"): one chart of each kind DataCube draws (chart-spec.ts
// CHART_MARKS), titled by its kind, over the sales sample -- a sheet per family, each fitting its window -- and a last
// sheet laid out as a BI dashboard, a strip of KPIs on top (KPI_DASHBOARD). Built as the other example pages are
// (charts-dashboard.ts salesPage); test/examples.test.ts holds it to every kind, each drawable.

import { boxMeasures, MAP_LIMIT, type ChartMark } from '../chart-spec.ts';
import type { PageDocument } from '../page-document.ts';
import { BY_TIME, chart, PROFIT, REVENUE, salesPage, type ExampleSheet } from './charts-dashboard.ts';

const COST = { column: 'cost', fn: 'sum' } as const;
const UNITS = { column: 'units', fn: 'sum' } as const;
const TARGET = { column: 'target', fn: 'sum' } as const;

/** A kind's chart, its id and title naming it: `g-<kind>`, "<Kind>: <what it shows>". */
const of = (mark: ChartMark, label: string, says: string, spec: Parameters<typeof chart>[2] = {}) =>
  chart(`g-${mark}`, `${label}: ${says}`, { mark, ...spec });

type Spec = Parameters<typeof chart>[2];

/** A KPI of the strip: by month, so it has a trend and its latest month against the one before. */
const kpi = (id: string, title: string, y: NonNullable<Spec['y']>, options: Spec['options'] = {}) =>
  chart(id, title, { mark: 'kpi', x: 'sold', grain: 'month', y, options });

/**
 * THE KPI DASHBOARD (the user, 2026-10-23: "a really nice layout of maybe 5-6-7-8 KPIs on top and then some layout of
 * other things that people usually have on a BI KPI dashboard"): a strip of eight KPIs -- each against its target where
 * it has one (a cost and a discount better lower), its latest month against the one before, its trend -- then the
 * trend against the target with the channel mix and the target's gauge beside it, then the top products, the regions
 * by channel and the map. The gallery's KPI and gauge are among them.
 */
const KPI_DASHBOARD: ExampleSheet = {
  name: 'KPI dashboard',
  views: [
    kpi('g-kpi', 'Revenue', [REVENUE], { target: 40_000_000 }),
    kpi('k-profit', 'Profit', [PROFIT], { target: 12_000_000 }),
    kpi('k-margin', 'Margin', [{ column: 'margin', fn: 'value' }], { target: 0.35 }),
    kpi('k-orders', 'Orders', [{ column: 'order_id', fn: 'count' }]),
    kpi('k-units', 'Units sold', [UNITS]),
    kpi('k-order', 'Average order', [{ column: 'revenue', fn: 'average' }]),
    kpi('k-cost', 'Cost against budget', [COST], { target: 24_000_000, better: 'lower' }),
    kpi('k-discount', 'Average discount', [{ column: 'discount', fn: 'average' }], { target: 0.08, better: 'lower' }),
    chart('k-trend', 'Revenue against target, by month', {
      x: 'sold', grain: 'month', y: [REVENUE, { ...TARGET, mark: 'line' }], options: { ...BY_TIME, legend: 'top' },
    }),
    chart('k-channel', 'Revenue by channel', { mark: 'pie', x: 'channel', options: { legend: 'bottom' } }),
    chart('g-gauge', 'Revenue against target', { mark: 'gauge', options: { target: 40_000_000, ring: true } }),
    chart('k-products', 'Top products by revenue', {
      x: 'product', options: { orientation: 'horizontal', sort: { by: 'y', direction: 'desc' }, limit: 10, labels: true },
    }),
    chart('k-regions', 'Revenue by region and channel', { x: 'region', split: 'channel', options: { stack: 'stacked', legend: 'top' } }),
    chart('k-map', 'Revenue by country', { mark: 'map', x: 'country_code', options: { limit: MAP_LIMIT } }),
  ],
  bands: [
    { height: 0.24, tiles: ['g-kpi', 'k-profit', 'k-margin', 'k-orders', 'k-units', 'k-order', 'k-cost', 'k-discount'].map((id) => [id, 1 / 8] as const) },
    { height: 0.4, tiles: [['k-trend', 0.5], ['k-channel', 0.25], ['g-gauge', 0.25]] },
    { height: 0.36, tiles: [['k-products', 1 / 3], ['k-regions', 1 / 3], ['k-map', 1 / 3]] },
  ],
};

export const GALLERY_SHEETS: readonly ExampleSheet[] = [
  {
    name: 'Bars and lines',
    views: [
      of('bar', 'Bar', 'revenue by region', { x: 'region' }),
      of('line', 'Line', 'revenue by month', { x: 'sold', grain: 'month', options: BY_TIME }),
      of('area', 'Area', 'revenue by month and channel, stacked', {
        x: 'sold', grain: 'month', split: 'channel', options: { ...BY_TIME, stack: 'stacked' },
      }),
      of('scatter', 'Scatter', 'each sale\'s cost against its revenue', { x: 'cost', y: [REVENUE] }),
      of('lollipop', 'Lollipop', 'units by product', { x: 'product', y: [UNITS], options: { orientation: 'horizontal' } }),
      of('dumbbell', 'Dumbbell', 'cost against revenue by country', { x: 'country', y: [COST, REVENUE], options: { orientation: 'horizontal' } }),
    ],
  },
  {
    name: 'Comparing',
    views: [
      of('range', 'Range', 'the smallest and largest sale by category', {
        x: 'category', y: [{ column: 'revenue', fn: 'min' }, { column: 'revenue', fn: 'max' }],
      }),
      of('waterfall', 'Waterfall', 'profit by category, adding up', { x: 'category', y: [PROFIT], options: { labels: true } }),
      of('pareto', 'Pareto', 'revenue by product, the vital few', { x: 'product' }),
      of('funnel', 'Funnel', 'revenue by channel', { x: 'channel', options: { labels: true } }),
      of('heatmap', 'Heatmap', 'revenue by month and region', { x: 'sold', grain: 'month', split: 'region', options: BY_TIME }),
      of('gantt', 'Gantt', 'when each product first and last sold', {
        x: 'product', y: [{ column: 'sold', fn: 'min' }, { column: 'sold', fn: 'max' }], options: { sort: { by: 'y', direction: 'asc' } },
      }),
    ],
  },
  {
    name: 'Part of a whole',
    views: [
      of('pie', 'Pie', 'revenue by channel', { x: 'channel', options: { labels: true } }),
      of('treemap', 'Treemap', 'the grid as shown, as blocks', { x: 'region' }),
      of('sunburst', 'Sunburst', 'the grid as shown, as rings', { x: 'region' }),
      of('tree', 'Tree', 'the grid as shown, as branches', { x: 'region' }),
      of('waffle', 'Waffle', 'revenue by channel, in a hundred cells', { x: 'channel' }),
      of('marimekko', 'Marimekko', 'revenue by region, each made of its channels', {
        x: 'region', split: 'channel', options: { sort: { by: 'x', direction: 'asc' } },
      }),
    ],
  },
  {
    name: 'Circles and flows',
    views: [
      of('rose', 'Rose', 'revenue by category', { x: 'category' }),
      of('polar', 'Polar bar', 'revenue by category and channel, stacked', { x: 'category', split: 'channel', options: { stack: 'stacked' } }),
      of('radar', 'Radar', 'each region\'s revenue, cost, profit and target', { x: 'region', y: [REVENUE, COST, PROFIT, TARGET] }),
      of('sankey', 'Sankey', 'revenue from each region to each channel', { x: 'region', split: 'channel' }),
      of('chord', 'Chord', 'revenue between categories and channels', { x: 'category', split: 'channel' }),
      of('parallel', 'Parallel', 'each product through its revenue, cost, profit and units', { x: 'product', y: [REVENUE, COST, PROFIT, UNITS] }),
    ],
  },
  {
    name: 'Time and spread',
    views: [
      of('stream', 'Streamgraph', 'revenue by week, by category', { x: 'sold', grain: 'week', split: 'category', options: BY_TIME }),
      of('calendar', 'Calendar', 'revenue by day', { x: 'sold', options: BY_TIME }),
      of('boxplot', 'Box plot', 'the spread of each sale\'s revenue by category', { x: 'category', y: boxMeasures('revenue') }),
      of('matrix', 'Scatter matrix', 'each sale\'s revenue, cost and profit against each other', {
        y: [REVENUE, COST, PROFIT], split: 'channel',
      }),
    ],
  },
  {
    name: 'Maps',
    views: [
      of('map', 'Map', 'revenue by country', { x: 'country_code', options: { limit: MAP_LIMIT } }),
      of('bubblemap', 'Bubble map', 'revenue by city, at each city', { x: 'city', at: { lat: 'lat', lon: 'lon' }, options: { limit: MAP_LIMIT } }),
    ],
  },
  KPI_DASHBOARD,
];

/** The page: a sheet per family, each of its kinds once, three to a row, every sheet fitting its window. */
export async function chartsGalleryPage(): Promise<PageDocument> {
  return salesPage('Chart gallery', GALLERY_SHEETS, [['EMEA'], ['AMER'], ['APAC']], { fit: true, perRow: 3 });
}
