// THE EXAMPLE PAGES (docs/DATACUBE_CHARTS_DESIGN_2026_10_09.md §3.3): a saved page per batch of charts, simple to complex,
// a sheet per family, and one of joins -- each over samples DataCube makes itself, so it needs no file and no network.
// This list is small and loads with the Open window; each page is built by a module of its own, fetched when opened.

import type { PageDocument } from '../page-document.ts';
import { lazily } from '../lazily.ts';

export interface ExamplePage {
  readonly id: string;
  /** Shown in the Open window. */
  readonly label: string;
  /** What it shows. */
  readonly about: string;
  /** The page, built: its own chunk, fetched now. */
  build(): Promise<PageDocument>;
}

export const EXAMPLE_PAGES: readonly ExamplePage[] = [
  {
    id: 'charts-gallery',
    label: 'Chart gallery: every kind',
    about: 'One chart of each of the 32 kinds DataCube draws -- bars, lines and areas, Paretos and waterfalls, pies, '
      + 'treemaps and sunbursts, Sankeys and chords, calendars and box plots, maps and gauges -- a sheet per family, each '
      + 'fitting the screen, over the sales sample.',
    build: async () => (await lazily(() => import('./charts-gallery.ts'))).chartsGalleryPage(),
  },
  {
    id: 'charts-dashboard',
    label: 'Charts: dashboard-grade',
    about: 'Bar and line together, reference lines and colour by value, time and zoom, drill-down and brushing, '
      + 'formats and labels, bins and periods -- over the sales sample.',
    build: async () => (await lazily(() => import('./charts-dashboard.ts'))).chartsDashboardPage(),
  },
  {
    id: 'charts-kinds',
    label: 'Charts: more kinds',
    about: 'Lollipops, dumbbells, ranges, waterfalls, Paretos and bump charts; roses, polar bars and half rings; funnels, '
      + 'two-ring pies, sunbursts, trees, treemaps, waffles and Marimekkos; Sankeys and chords; streamgraphs, calendars, '
      + 'heatmaps, slope charts and Gantts; '
      + 'radars, parallel coordinates and a scatter matrix; box plots and a histogram; maps of countries and of '
      + 'cities; small multiples and bubbles; gauges and KPIs -- over '
      + 'the sales sample.',
    build: async () => (await lazily(() => import('./charts-kinds.ts'))).chartsKindsPage(),
  },
  {
    // the user, 2026-10-23: join prep was on the site, but nothing showed it
    id: 'prep-joins',
    label: 'Joining tables',
    about: 'The sales sample\'s orders and a table of countries, joined by ISO code as Join… joins them: every order with '
      + 'its country\'s continent, capital and population beside it, by continent with a chart of it; and the orders '
      + 'whose country the table lacks.',
    build: async () => (await lazily(() => import('./prep-joins.ts'))).prepJoinsPage(),
  },
  {
    id: 'markets',
    label: 'Markets: timeseries',
    about: 'Ten instruments\' daily prices and a Treasury curve as timeseries: performance rebased, drawdowns, realized '
      + 'volatility, correlation and beta, Bollinger bands, moving averages and RSI, the 10s2s spread, and a bond fund '
      + 'against its yield read across two grids in one query -- gs-quant\'s functions, every figure the database\'s.',
    build: async () => (await lazily(() => import('./markets.ts'))).marketsPage(),
  },
  {
    // the user, 2026-10-25: "a real saved page with multiple tabs showing all of these", as the chart gallery is
    id: 'timeseries-gallery',
    label: 'Timeseries gallery: every function',
    about: 'Every function of the timeseries tile, a sheet per family -- returns, levels, rolling statistics, risk, '
      + 'relationships, technicals and math -- then the ways to read a tile: a line per symbol, a month at a time, a log '
      + 'scale, every line from one start, its styles, and two grids in one query; over the markets and rates samples.',
    build: async () => (await lazily(() => import('./timeseries-gallery.ts'))).timeseriesGalleryPage(),
  },
  {
    // a flow to open, rather than one to build first (docs/DATACUBE_FLOW_MODE_2026_10_24.md)
    id: 'prep-flow',
    label: 'A data flow',
    about: 'The orders and their countries prepared on a flow\'s canvas, as Alteryx prepares them: two Inputs, the big '
      + 'orders filtered, joined live to their countries and given a margin, summarized by continent and written out as '
      + 'a CSV, the top order of each country kept -- each box a step, its rows below it when clicked.',
    build: async () => (await lazily(() => import('./prep-flow.ts'))).prepFlowPage(),
  },
];
