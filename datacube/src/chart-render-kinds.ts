// The rarer ECharts charts batch 3's kinds draw with (chart-kinds.ts), registered when the first of them draws: its own
// chunk beside chart-render.ts's, so a page of bars and lines never downloads them (test/bundle-budget.test.ts).
// chart-render.ts stays the one file that mounts a chart; this only adds to what ECharts can draw.
//
// Each by its own module, which registers it as it loads (ECharts' `lib/chart/<kind>.js`: `use(install)`), never
// through the `echarts/charts` index chart-render.ts imports: an index both chunks imported would be code they share,
// and the bundler would put every chart in it -- a page's first bar chart would download the Sankey.

// a waterfall's, a dumbbell's and a range's spans
import 'echarts/lib/chart/custom.js';
// a box plot's boxes and whiskers
import 'echarts/lib/chart/boxplot.js';
import 'echarts/lib/chart/funnel.js';
import 'echarts/lib/chart/gauge.js';
import 'echarts/lib/chart/parallel.js';
import 'echarts/lib/chart/radar.js';
import 'echarts/lib/chart/sankey.js';
import 'echarts/lib/chart/chord.js';
import 'echarts/lib/chart/sunburst.js';
import 'echarts/lib/chart/themeRiver.js';
import 'echarts/lib/chart/tree.js';
// a polar bar's circle, a calendar's days, a radar's spokes, parallel axes, a streamgraph's one time axis
import 'echarts/lib/component/polar.js';
import 'echarts/lib/component/calendar.js';
import 'echarts/lib/component/radar.js';
import 'echarts/lib/component/parallel.js';
import 'echarts/lib/component/singleAxis.js';
