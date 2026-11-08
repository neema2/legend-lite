// THE #TS BUILDER (docs/TS_DECISIONS_2026_11_07.md §4, the builders; the order's step 2): what a person imports to write
// a series as a gs-quant notebook does -- `volatility(column('close', 'SPX'), 22)`, `spx.div(ndx)` -- each term's #TS
// text core's to read (Term.text). Its words generated (generated/ts-builder-words.ts), its terms, windows and SQL's
// words per point the core's (timeseries-builder-core.ts); the chart those terms are plotted on its chart face's
// (timeseries-chart.ts, B3: a tile built in code, each call one of the tile's changes). No part of the page's startup
// bundle.

export * from './timeseries-builder-core.ts';
export * from './generated/ts-builder-words.ts';
export { Chart, Plotted, type TsPaneOptions, type TsPlotOptions } from './timeseries-chart.ts';
