// ECharts' painters by their own modules (chart-render.ts), as its charts are registered by theirs: never through the
// `echarts/renderers` index, which imports both -- the SVG one would be code a first chart fetches once a picture's
// export uses it (test/bundle-budget.test.ts). The package exports the paths (`./*`) and ships no types for them: each
// an installer, of the kind ECharts' own `use` takes.
declare module 'echarts/lib/renderer/installCanvasRenderer.js' {
  import type { use } from 'echarts/core';
  export const install: Extract<Parameters<typeof use>[0], (...args: never[]) => void>;
}
declare module 'echarts/lib/renderer/installSVGRenderer.js' {
  import type { use } from 'echarts/core';
  export const install: Extract<Parameters<typeof use>[0], (...args: never[]) => void>;
}
