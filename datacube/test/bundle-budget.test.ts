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

/** Every file the page loads before any chart: the bundle and what it imports, transitively. */
function startup(): string[] {
  const seen = new Set<string>();
  const walk = (file: string): void => {
    if (seen.has(file)) return;
    seen.add(file);
    staticImports(file).forEach(walk);
  };
  walk(join(DEMO, 'bundle.js'));
  return [...seen];
}

/** The page's grid-only download, gzipped, in bytes: what it costs before any chart. Raise it on purpose.
 *  350,000 -> 352,000 (2026-10-02, store types step 7): Postgres's catalog rules -- DataCube writes a
 *  Postgres table's model by Postgres's own rules (generated/catalog-facts.ts), about 0.5 KB gzipped. */
const BUDGET = 352_000;

describe('the page loads ECharts only when a chart draws', () => {
  it('no file the page loads at startup contains ECharts', () => {
    const files = startup();
    for (const f of files) {
      assert.ok(!readFileSync(f, 'utf8').includes('node_modules/echarts/'), `${f} carries ECharts`);
    }
  });

  it('ECharts is in a chunk beside the bundle, reached by a dynamic import', () => {
    const lazy = readdirSync(CHUNKS).map((f) => join(CHUNKS, f))
      .filter((f) => readFileSync(f, 'utf8').includes('node_modules/echarts/'));
    assert.equal(lazy.length, 1, 'one chunk holds the chart renderer');
    const name = lazy[0]!.slice(CHUNKS.length + 1);
    assert.match(readFileSync(join(DEMO, 'bundle.js'), 'utf8'), new RegExp(`import\\("\\./chunks-bundle/${name.replace('.', '\\.')}"\\)`));
  });

  it(`a grid-only page downloads at most ${BUDGET.toLocaleString()} bytes of script, gzipped`, () => {
    const size = startup().reduce((sum, f) => sum + gzipSync(readFileSync(f)).length, 0);
    assert.ok(size <= BUDGET, `the grid's script is ${size.toLocaleString()} bytes gzipped, over its budget of ${BUDGET.toLocaleString()}`);
  });
});
