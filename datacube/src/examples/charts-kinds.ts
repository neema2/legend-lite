// The example page of batch 3 (docs/DATACUBE_CHARTS_DESIGN_2026_10_09.md §3, §3.3): every new kind over the sales
// sample, a sheet per family, each simple to complex -- and batch 4's box plots, with a histogram, on Statistics,
// batch 5's maps on Maps, and batch 3's leftovers (a waffle and a Marimekko, a Gantt) with their families. Built as batch 2's page is (charts-dashboard.ts salesPage), so it
// moves with the spec; test/examples.test.ts reads it back and finds every chart drawable, and
// test/chart-queries.test.ts plans and runs each chart's query on the sample.

import { boxMeasures, MAP_LIMIT } from '../chart-spec.ts';
import type { PageDocument } from '../page-document.ts';
import { BY_TIME, chart, PROFIT, REVENUE, salesPage, type ExampleSheet } from './charts-dashboard.ts';

const COST = { column: 'cost', fn: 'sum' } as const;
const UNITS = { column: 'units', fn: 'sum' } as const;
const TARGET = { column: 'target', fn: 'sum' } as const;

const SHEETS: readonly ExampleSheet[] = [
  {
    name: 'Comparing',
    views: [
      chart('k-lollipop', 'Units by product: a lollipop', { mark: 'lollipop', x: 'product', y: [UNITS], options: { orientation: 'horizontal' } }),
      chart('k-dumbbell', 'Cost against revenue by country: a dumbbell', {
        mark: 'dumbbell', x: 'country', y: [COST, REVENUE], options: { orientation: 'horizontal' },
      }),
      chart('k-range', 'The smallest and largest sale by category: a range', {
        mark: 'range', x: 'category', y: [{ column: 'revenue', fn: 'min' }, { column: 'revenue', fn: 'max' }],
      }),
      chart('k-waterfall', 'Profit by category, adding up: a waterfall', { mark: 'waterfall', x: 'category', y: [PROFIT], options: { labels: true } }),
      chart('k-pareto', 'Revenue by product, the vital few: a Pareto', { mark: 'pareto', x: 'product' }),
      chart('k-bump', 'Each region\'s rank by revenue, month by month: a bump chart', {
        mark: 'line', x: 'sold', grain: 'quarter', split: 'region', y: [{ ...REVENUE, showAs: { kind: 'rank' } }],
        options: { ...BY_TIME, labels: true },
      }),
    ],
  },
  {
    name: 'Around a circle',
    views: [
      chart('k-rose', 'Revenue by category: a rose', { mark: 'rose', x: 'category' }),
      chart('k-polar', 'Revenue by category and channel: a polar bar, stacked', {
        mark: 'polar', x: 'category', split: 'channel', options: { stack: 'stacked' },
      }),
      chart('k-radial', 'Units by region: radial bars', { mark: 'polar', x: 'region', y: [UNITS], options: { orientation: 'horizontal' } }),
      chart('k-half', 'Revenue by channel: half a ring', { mark: 'pie', x: 'channel', options: { half: true, labels: true } }),
    ],
  },
  {
    name: 'Part of a whole',
    views: [
      chart('k-funnel', 'Revenue by channel: a funnel', { mark: 'funnel', x: 'channel', options: { labels: true } }),
      chart('k-rings', 'Revenue by region, then channel: a pie in two rings', { mark: 'pie', x: 'region', split: 'channel' }),
      chart('k-sunburst', 'The grid as shown, as rings: a sunburst', { mark: 'sunburst', x: 'region' }),
      chart('k-tree', 'The grid as shown, as branches: a tree', { mark: 'tree', x: 'region' }),
      chart('k-treemap', 'The grid as shown, as blocks: a treemap', { mark: 'treemap', x: 'region' }),
      chart('k-waffle', 'Revenue by channel, in a hundred cells: a waffle', { mark: 'waffle', x: 'channel' }),
      chart('k-mekko', 'Revenue by region, each made of its channels: a Marimekko', {
        mark: 'marimekko', x: 'region', split: 'channel', options: { sort: { by: 'x', direction: 'asc' } },
      }),
    ],
  },
  {
    name: 'Flows',
    views: [
      chart('k-sankey', 'Revenue from each region to each channel: a Sankey', { mark: 'sankey', x: 'region', split: 'channel' }),
      chart('k-sankey2', 'Revenue from each category to each region: a Sankey', { mark: 'sankey', x: 'category', split: 'region' }),
      chart('k-chord', 'Revenue between categories and channels: a chord diagram', { mark: 'chord', x: 'category', split: 'channel' }),
    ],
  },
  {
    name: 'Over time',
    views: [
      chart('k-stream', 'Revenue by week, by category: a streamgraph', { mark: 'stream', x: 'sold', grain: 'week', split: 'category', options: BY_TIME }),
      chart('k-calendar', 'Revenue by day: a calendar', { mark: 'calendar', x: 'sold', options: BY_TIME }),
      chart('k-slope', 'Revenue by region, one year to the next: a slope chart', {
        mark: 'line', x: 'sold', grain: 'year', split: 'region', options: { ...BY_TIME, labels: true },
      }),
      chart('k-band', 'The smallest and largest sale each month: a range', {
        mark: 'range', x: 'sold', grain: 'month', y: [{ column: 'revenue', fn: 'min' }, { column: 'revenue', fn: 'max' }], options: BY_TIME,
      }),
      chart('k-heatmap', 'Revenue by month and region: a heatmap', {
        mark: 'heatmap', x: 'sold', grain: 'month', split: 'region', options: BY_TIME,
      }),
      chart('k-gantt', 'When each product first and last sold: a Gantt', {
        mark: 'gantt', x: 'product', y: [{ column: 'sold', fn: 'min' }, { column: 'sold', fn: 'max' }],
        options: { sort: { by: 'y', direction: 'asc' } },
      }),
    ],
  },
  {
    name: 'Many measures',
    views: [
      chart('k-radar', 'Each region\'s revenue, cost, profit and target: a radar', { mark: 'radar', x: 'region', y: [REVENUE, COST, PROFIT, TARGET] }),
      chart('k-parallel', 'Each product through its revenue, cost, profit and units: parallel coordinates', {
        mark: 'parallel', x: 'product', y: [REVENUE, COST, PROFIT, UNITS],
      }),
      chart('k-matrix', 'Each sale\'s revenue, cost, profit and discount against each other: a scatter matrix', {
        mark: 'matrix', y: [REVENUE, COST, PROFIT, { column: 'discount', fn: 'sum' }], split: 'channel',
      }),
    ],
  },
  {
    name: 'Statistics',
    views: [
      chart('k-box', 'The spread of each sale\'s revenue by category: a box plot', {
        mark: 'boxplot', x: 'category', y: boxMeasures('revenue'),
      }),
      chart('k-box-side', 'The spread of each sale\'s discount by region, side on: a box plot', {
        mark: 'boxplot', x: 'region', y: boxMeasures('discount'), options: { orientation: 'horizontal' },
      }),
      chart('k-box-month', 'The spread of each sale\'s revenue, month by month: a box plot', {
        mark: 'boxplot', x: 'sold', grain: 'month', y: boxMeasures('revenue'), options: BY_TIME,
      }),
      chart('k-histogram', 'How many sales of each size: a histogram, in 20 bins', {
        x: 'revenue', bins: { count: 20 }, y: [{ column: 'order_id', fn: 'count' }], options: BY_TIME,
      }),
    ],
  },
  {
    name: 'Maps',
    views: [
      chart('k-map', 'Revenue by country, by its ISO code: a map', {
        mark: 'map', x: 'country_code', options: { limit: MAP_LIMIT },
      }),
      chart('k-map-names', 'Profit by country, by its name: a map', {
        mark: 'map', x: 'country', y: [PROFIT], options: { limit: MAP_LIMIT },
      }),
      chart('k-map-share', 'Each country\'s share of the units sold: a map', {
        mark: 'map', x: 'country_code', y: [{ ...UNITS, showAs: { kind: 'percentOfTotal' } }], options: { limit: MAP_LIMIT },
      }),
      chart('k-bubbles', 'Revenue by city, at each city: a bubble map', {
        mark: 'bubblemap', x: 'city', at: { lat: 'lat', lon: 'lon' }, options: { limit: MAP_LIMIT },
      }),
      chart('k-bubbles-orders', 'Orders by city: a bubble map', {
        mark: 'bubblemap', x: 'city', at: { lat: 'lat', lon: 'lon' }, y: [{ column: 'order_id', fn: 'count' }],
        options: { limit: MAP_LIMIT },
      }),
    ],
  },
  {
    name: 'Small multiples',
    views: [
      chart('k-facet-lines', 'Revenue by month, a chart per region, on one scale', {
        mark: 'line', x: 'sold', grain: 'month', facet: { column: 'region' }, options: BY_TIME,
      }),
      chart('k-facet-bars', 'Revenue by category and channel, a chart per region', {
        x: 'category', split: 'channel', facet: { column: 'region' }, options: { stack: 'stacked' },
      }),
      chart('k-sparklines', 'Revenue by quarter, a chart per country', {
        mark: 'area', x: 'sold', grain: 'quarter', facet: { column: 'country' }, options: BY_TIME,
      }),
      chart('k-bubble', 'Each sale\'s cost against its revenue, sized by its units: a bubble chart', {
        mark: 'scatter', x: 'cost', y: [REVENUE], size: UNITS, split: 'channel',
      }),
    ],
  },
  {
    name: 'One value',
    views: [
      chart('k-gauge', 'Revenue against its target: a gauge', { mark: 'gauge', options: { target: 40_000_000 } }),
      chart('k-ring', 'Profit against its target: a progress ring', { mark: 'gauge', y: [PROFIT], options: { target: 15_000_000, ring: true } }),
      chart('k-kpi', 'Revenue against its target, and its trend by month: a KPI', {
        mark: 'kpi', x: 'sold', grain: 'month', options: { target: 40_000_000 },
      }),
    ],
  },
];

/** The page: a sheet per family of batch 3's kinds, and the grid they are views of, its regions open. */
export async function chartsKindsPage(): Promise<PageDocument> {
  return salesPage('More chart kinds', SHEETS, [['EMEA'], ['AMER'], ['APAC']]);
}
