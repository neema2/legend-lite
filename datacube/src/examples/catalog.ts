// THE EXAMPLE PAGES (docs/DATACUBE_CHARTS_DESIGN_2026_10_09.md §3.3): a saved page per batch of charts, simple to complex,
// a sheet per family, over a sample DataCube makes itself -- so it needs no file and no network. This list is small and
// loads with the Open window; each page is built by a module of its own, fetched when it is opened.

import type { PageDocument } from '../page-document.ts';

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
    id: 'charts-dashboard',
    label: 'Charts: dashboard-grade',
    about: 'Bar and line together, reference lines and colour by value, time and zoom, drill-down and brushing, '
      + 'formats and labels, bins and periods -- over the sales sample.',
    build: async () => (await import('./charts-dashboard.ts')).chartsDashboardPage(),
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
    build: async () => (await import('./charts-kinds.ts')).chartsKindsPage(),
  },
];
