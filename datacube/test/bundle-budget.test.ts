// The page's bundle, against its budget (plan F7). A grid-only page must not download ECharts:
// the chart renderer is reached only by a dynamic import, the first time a chart draws
// (ui/chart-panel.ts), so esbuild puts it in a chunk the bundle does not import at startup.
// And the bundle a grid loads has a size budget, so growth is a decision, not an accident.

import assert from 'node:assert/strict';
import { readdirSync, readFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { describe, it } from 'node:test';
import { gzipSync } from 'node:zlib';
import { runfileFromEnv } from '../../tools/js/runfiles.mts';

// the bundle and its chunk directory, side by side as esbuild wrote them (BUILD.bazel names bundle.js in BUNDLE)
const DEMO = dirname(runfileFromEnv('BUNDLE'));
const CHUNKS = join(DEMO, 'chunks-bundle');

/** What `file` imports statically: `import ... from "./x.js"` and bare `import "./x.js"`, resolved. */
function staticImports(file: string): string[] {
  const text = readFileSync(file, 'utf8');
  const dir = dirname(file);
  // statement-level only: a dynamic `import("./x.js")` is what makes a chunk lazy
  return [...text.matchAll(/^\s*(?:import|export)\s[^;]*?from\s*"(\.[^"]+)"|^\s*import\s*"(\.[^"]+)"/gm)]
    .map((m) => join(dir, m[1] ?? m[2]!));
}

/** Every file a page loads before any chart: its bundle and what it imports, transitively. */
function startup(bundle = join(DEMO, 'bundle.js')): string[] {
  const seen = new Set<string>();
  const walk = (file: string): void => {
    if (seen.has(file)) return;
    seen.add(file);
    staticImports(file).forEach(walk);
  };
  walk(bundle);
  return [...seen];
}

/** The page's grid-only download, gzipped, in bytes: what it costs before any chart. Raise it on purpose.
 *  350,000 -> 352,000 (2026-10-02, store types step 7): Postgres's catalog rules -- DataCube writes a
 *  Postgres table's model by Postgres's own rules (generated/catalog-facts.ts), about 0.5 KB gzipped.
 *  Not lowered with the TypeScript writer's removal (2026-10-08, the model written by legend-lite's
 *  module): a budget is raised on purpose, and measured down when the user asks.
 *  352,000 -> 368,000 (2026-10-09, DataCube pages phase 2, docs/DATACUBE_PAGES_DESIGN_2026_10_09.md §6): every page
 *  is a page of its own from the start, its one grid a tile on its board -- the page (page/page-app.ts), the board
 *  (page/cube-page.ts, layout/band-board.ts) and the charts' panel load at startup, where phase 1 fetched them with the
 *  first chart (startup then ~333 KB). Measured 364,791. The layouts are still fetched when first opened, ECharts
 *  when a chart first draws. */
const BUDGET = 368_000;

/** The engine page's (demo/engine.html: one cube on an engine that runs its queries, Python's) startup download,
 *  gzipped: the grid and the remote client, no DuckDB-WASM and no compiler. Set 2026-10-08 at its first measure
 *  (288,876 bytes) and the app's headroom (about 4%), so its growth is a decision too. */
const ENGINE_BUDGET = 300_000;

/** A notebook's cube (legend_lite.notebook.DataCube): DataCube's module and its styles (widget.js, widget.css),
 *  gzipped, which the widget's loader fetches over the kernel's channel once per notebook page. Set 2026-10-08 at its
 *  first measure (461,746 bytes: ECharts within, as the module is one file) and about 4% headroom. And the loader
 *  itself, which anywidget sends with every widget: small. */
const WIDGET_BUDGET = 480_000;
const LOADER_BUDGET = 10_000;

describe('the page loads ECharts only when a chart draws', () => {
  it('no file the page loads at startup contains ECharts', () => {
    const files = startup();
    for (const f of files) {
      assert.ok(!readFileSync(f, 'utf8').includes('node_modules/echarts/'), `${f} carries ECharts`);
    }
  });

  it('ECharts is in a chunk beside the bundle, reached by a dynamic import', () => {
    const chunks = readdirSync(CHUNKS).map((f) => join(CHUNKS, f));
    const lazy = chunks.filter((f) => readFileSync(f, 'utf8').includes('node_modules/echarts/'));
    assert.equal(lazy.length, 1, 'one chunk holds the chart renderer');
    const name = lazy[0]!.slice(CHUNKS.length + 1).replace('.', '\\.');
    // from the page's own chunk (page/cube-page.ts, itself fetched when a cube first gets a page), or the bundle
    const importers = [join(DEMO, 'bundle.js'), ...chunks]
      .filter((f) => new RegExp(`import\\("\\./(?:chunks-bundle/)?${name}"\\)`).test(readFileSync(f, 'utf8')));
    assert.ok(importers.length > 0, 'something imports the chart renderer, dynamically');
  });

  it('the layouts (ui/layout-picker.ts) are fetched the first time they are opened, not at startup', () => {
    for (const f of startup()) {
      assert.ok(!readFileSync(f, 'utf8').includes('src/ui/layout-picker.ts'), `${f} carries the layout picker`);
    }
    const lazy = readdirSync(CHUNKS).filter((f) => readFileSync(join(CHUNKS, f), 'utf8').includes('src/ui/layout-picker.ts'));
    assert.equal(lazy.length, 1, 'one chunk holds the layout picker');
  });

  it(`a grid-only page downloads at most ${BUDGET.toLocaleString()} bytes of script, gzipped`, () => {
    const size = startup().reduce((sum, f) => sum + gzipSync(readFileSync(f)).length, 0);
    assert.ok(size <= BUDGET, `the grid's script is ${size.toLocaleString()} bytes gzipped, over its budget of ${BUDGET.toLocaleString()}`);
  });

  it(`the engine page downloads at most ${ENGINE_BUDGET.toLocaleString()} bytes of script, gzipped, and no ECharts`, () => {
    const files = startup(runfileFromEnv('ENGINE_BUNDLE'));
    for (const f of files) {
      assert.ok(!readFileSync(f, 'utf8').includes('node_modules/echarts/'), `${f} carries ECharts`);
    }
    const size = files.reduce((sum, f) => sum + gzipSync(readFileSync(f)).length, 0);
    assert.ok(size <= ENGINE_BUDGET,
      `the engine page's script is ${size.toLocaleString()} bytes gzipped, over its budget of ${ENGINE_BUDGET.toLocaleString()}`);
  });
});

describe('a notebook\'s cube is one module, fetched once, and its loader small', () => {
  const module = runfileFromEnv('WIDGET');
  const styles = join(dirname(module), 'widget.css');

  it('the module imports nothing beside itself: a module imported from a blob URL can load no chunk', () => {
    const text = readFileSync(module, 'utf8');
    assert.deepEqual([...text.matchAll(/\bimport\s*\(\s*["'`]\.|\bfrom\s*["']\.|\bimport\s*["']\./g)].map((m) => m[0]), []);
  });

  it(`the module and its styles are at most ${WIDGET_BUDGET.toLocaleString()} bytes gzipped, with no DuckDB-WASM`, () => {
    const text = readFileSync(module, 'utf8');
    assert.ok(!/duckdb/i.test(text), 'the notebook cube runs nothing in the page: no DuckDB-WASM');
    const size = gzipSync(readFileSync(module)).length + gzipSync(readFileSync(styles)).length;
    assert.ok(size <= WIDGET_BUDGET,
      `a notebook cube's module is ${size.toLocaleString()} bytes gzipped, over its budget of ${WIDGET_BUDGET.toLocaleString()}`);
  });

  it('its styles fetch nothing: a notebook page has no site to fetch a font or an image from', () => {
    const css = readFileSync(styles, 'utf8');
    assert.ok(css.includes('.dc-grid'), 'DataCube\'s styles');
    assert.ok(!/url\((?!data:)|@font-face/.test(css), 'no font or image fetched by URL');
  });

  it(`the loader anywidget sends with every widget is at most ${LOADER_BUDGET.toLocaleString()} bytes`, () => {
    const size = readFileSync(runfileFromEnv('WIDGET_LOADER')).length;
    assert.ok(size <= LOADER_BUDGET, `the loader is ${size.toLocaleString()} bytes, over its budget of ${LOADER_BUDGET.toLocaleString()}`);
  });
});
