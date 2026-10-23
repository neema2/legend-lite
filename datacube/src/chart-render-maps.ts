// The maps' ECharts (chart-maps.ts): ECharts' map chart and its geo component, registered when the first map draws --
// its own chunk beside chart-render-kinds.ts's, so a page with no map never downloads them (test/bundle-budget.test.ts).
// chart-render.ts stays the one file that mounts a chart; this only adds to what ECharts can draw, each by its own
// module (as chart-render-kinds.ts says why). The shapes come with the drawing (chart-maps.ts WORLD), handed in here:
// imported by both chunks, they would be a third, shared one.

import { registerMap } from 'echarts/core';
import 'echarts/lib/chart/map.js';
import 'echarts/lib/component/geo.js';

/** The world's countries registered under their map's name, once: ECharts draws a map by the name it was given. */
export function registerWorld(name: string, shapes: object): void {
  registerMap(name, shapes as Parameters<typeof registerMap>[1]);
}
