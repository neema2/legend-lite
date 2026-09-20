// Guardrails on the shape of the source, in the spirit of the repo's
// own CodeShapeGuardrailTest: each one exists because the mistake it
// bans actually happened here.

import assert from 'node:assert/strict';
import { existsSync, readdirSync, readFileSync, statSync } from 'node:fs';
import { join } from 'node:path';
import { describe, it } from 'node:test';

function sources(dir: string, out: string[] = []): string[] {
  for (const name of readdirSync(dir)) {
    const p = join(dir, name);
    if (statSync(p).isDirectory()) sources(p, out);
    else if (name.endsWith('.ts')) out.push(p);
  }
  return out;
}

const FILES = sources('src');

function isComment(line: string): boolean {
  const t = line.trim();
  return t.startsWith('//') || t.startsWith('*') || t.startsWith('/*');
}

describe('source guardrails', () => {
  it('finds source files at all', () => {
    // A guardrail that scans nothing passes for the wrong reason.
    assert.ok(FILES.length > 10, `${FILES.length} files`);
  });

  it('has no cross-realm DOM instanceof checks', () => {
    // This bit twice. The constructor belongs to the document's
    // realm, so in an iframe -- or a test DOM -- the check is false
    // and the code silently does nothing. Duck-type the method
    // actually used instead.
    const bad: string[] = [];
    for (const file of FILES) {
      readFileSync(file, 'utf8')
        .split('\n')
        .forEach((line, i) => {
          if (isComment(line)) return;
          if (
            /instanceof\s+(HTMLElement|Element|Node|HTMLInputElement)\b/.test(
              line,
            )
          ) {
            bad.push(`${file}:${i + 1}`);
          }
        });
    }
    assert.deepEqual(bad, [], `duck-type instead: ${bad.join(', ')}`);
  });

  it('has no literal control characters in source', () => {
    // Two separators were written as literal NUL bytes by accident.
    // An invisible byte survives no reformat and no code review, and
    // it silently breaks every search for the code around it.
    const control = new RegExp('[\\u0000-\\u0008\\u000b\\u000c\\u000e-\\u001f]');
    const bad: string[] = [];
    for (const file of FILES) {
      if (control.test(readFileSync(file, 'utf8'))) bad.push(file);
    }
    assert.deepEqual(bad, [], `use an escape sequence: ${bad.join(', ')}`);
  });

  it('keeps the demo-only planner out of the product', () => {
    // "One planner" is an architectural commitment, and a convenient
    // second one is exactly how such commitments rot.
    const bad = FILES.filter((f) =>
      /DemoOnlyPlanner|filterToSql/.test(readFileSync(f, 'utf8')),
    );
    assert.deepEqual(bad, [], `demo code in src: ${bad.join(', ')}`);
  });
});

describe('there is exactly one planner, and no way to fall back to another', () => {
  // A shim planner cost three real bugs: snap building SQL by hand,
  // the snapped plane never redirecting, and the SQL panel showing
  // Pure. Every one was invisible because the demo silently ran on a
  // fake whose source was a bare SQL identifier, so the broken paths
  // happened to work.
  //
  // The rule is NOT "no fake may exist" -- a test injecting a stub is
  // fine, because that choice is static and never ships. The rule is
  // that shipped code must not be able to CHOOSE a planner at
  // runtime.
  const shipped = [...sources('src'), ...sources('demo')];

  it('scans both shipped trees', () => {
    assert.ok(shipped.length > 10, `${shipped.length} files`);
  });

  // The two shipped implementations, and why two is not a betrayal of
  // "one planner".
  //
  // The rule bans a second IMPLEMENTATION -- a thing that must AGREE
  // with legend-lite about null ordering, coercion and aggregates,
  // and will therefore eventually disagree. It does not ban a second
  // TRANSPORT to the same implementation. `LegendLitePlanner` POSTs
  // to /engine/plan, whose handler calls Compiler.plan;
  // `WasmPlanner` calls that same Compiler.plan compiled to
  // WebAssembly. There is one planner and two ways to reach it.
  //
  // That argument is only worth anything if something checks it, so
  // the next test requires the differentials that do.
  const PLANNERS = [join('src', 'planner.ts'), join('src', 'wasm-planner.ts')];

  it('implements Planner in exactly the two blessed shipped files', () => {
    const impls = shipped.filter((f) =>
      /implements\s+Planner\b/.test(readFileSync(f, 'utf8')),
    );
    assert.deepEqual(
      impls.sort(),
      [...PLANNERS].sort(),
      `a second planner is a second thing that must agree with the ` +
        `first about null ordering, coercion and aggregates. A new ` +
        `transport to legend-lite's own Compiler.plan may join this ` +
        `list; a planner that COMPUTES SQL itself may not: ${impls.join(', ')}`,
    );
  });

  it('keeps a differential behind the second transport', () => {
    // The allowance above rests entirely on the two transports being
    // checked against each other. If these harnesses go, the
    // allowance is unearned and the second file has to go with them.
    for (const f of ['demo/verify-wasm-planner.ts',
      '../research/wasm/differential.mjs']) {
      assert.ok(existsSync(f), `the wasm planner's differential is gone (${f})`
        + ' — either restore it or drop src/wasm-planner.ts');
    }
  });

  it('never lets a bundle pick its planner at runtime', () => {
    // A URL parameter or env lookup feeding planner construction is
    // the same defect as a catch-block fallback: nobody can tell from
    // the screen which planner produced the SQL, so a divergence
    // hides. The demo picks by ENTRY POINT instead -- main.ts wires
    // the server, main-wasm.ts wires the browser build -- which is a
    // build-time decision, visible in the bundle.
    const bad: string[] = [];
    for (const file of shipped) {
      const text = readFileSync(file, 'utf8');
      if (!/\bnew\s+\w*Planner\b/.test(text)) continue;
      const code = text.split('\n').filter((l) => !isComment(l)).join('\n');
      if (/URLSearchParams|process\.env|location\.search/.test(code)
          && /\bnew\s+\w*Planner\b/.test(code)) {
        // Only flag when the two are in the same FUNCTION-ish span;
        // main.ts legitimately reads ?remote= for the data source.
        const spans = code.split(/\n(?=(?:export )?(?:async )?function )/);
        for (const span of spans) {
          if (/\bnew\s+\w*Planner\b/.test(span)
              && /URLSearchParams|process\.env|location\.search/.test(span)) {
            bad.push(file);
            break;
          }
        }
      }
    }
    assert.deepEqual(bad, [], `runtime planner choice in: ${bad.join(', ')}`);
  });

  it('never recovers from an unreachable planner by substituting one', () => {
    // The specific shape that was here: a health probe, a catch, and
    // a fake returned from it.
    const bad: string[] = [];
    for (const file of shipped) {
      const text = readFileSync(file, 'utf8');
      // A `catch` block that constructs or returns a planner.
      if (/catch\s*(\([^)]*\))?\s*\{[^}]*\bnew\s+\w*Planner\b/s.test(text)) {
        bad.push(file);
      }
    }
    assert.deepEqual(bad, [], `fallback planner in: ${bad.join(', ')}`);
  });

  it('the server entry REFUSES to run without the engine', () => {
    // Not "warns and carries on": constructs nothing, so there is no
    // grid of invented numbers to mistake for real ones.
    const demo = readFileSync(join('demo', 'main-server.ts'), 'utf8');
    assert.ok(
      /throw new Error\([^)]*legend-lite is not answering/s.test(demo),
      'the server entry must throw when the engine is absent',
    );
  });

  it('the default entry REFUSES to run without its planner module', () => {
    // Same rule, different absence. main.ts plans in the tab, so
    // there is no server to be down -- but the 4 MB module can be
    // missing, and the failure has to land BEFORE the grid exists.
    // `warmUp()` is what forces that: it loads and instantiates, so
    // an unavailable module rejects startup instead of surfacing as
    // a half-rendered grid on the user's first interaction.
    const demo = readFileSync(join('demo', 'main.ts'), 'utf8');
    assert.ok(
      /await\s+planner\.warmUp\(\)/.test(demo),
      'main.ts must await warmUp() before handing the planner to boot,'
        + ' so a missing module fails startup rather than the first query',
    );
    const warm = demo.indexOf('warmUp()');
    const handed = demo.search(/return\s*\{\s*planner/);
    assert.ok(
      warm >= 0 && handed > warm,
      'warmUp() must come BEFORE the planner is handed to boot',
    );
  });

  it('runs the in-browser planner OFF the main thread', () => {
    // Measured, not theoretical. Building the boot layer is ~600ms of
    // synchronous WebAssembly with no yield point. On the main thread
    // it froze the page AND starved DuckDB: its startup slipped from
    // 409ms to 1038ms, and time-to-first-row got WORSE (1009 -> 1175)
    // when the planner was started earlier to "overlap" it. On a
    // worker the two genuinely run at once: 828ms.
    const demo = readFileSync(join('demo', 'main.ts'), 'utf8');
    assert.ok(
      /workerUrl\s*:/.test(demo),
      'demo/main.ts must give WasmPlanner a workerUrl — without it the'
        + ' boot layer blocks the main thread and startup gets slower,'
        + ' not faster',
    );
  });

  it('keeps the shim gone from every entry point', () => {
    for (const f of ['main.ts', 'main-server.ts', 'boot.ts']) {
      assert.equal(
        /DemoOnlyPlanner/.test(readFileSync(join('demo', f), 'utf8')),
        false,
        `the shim must be gone, not merely unreferenced (${f})`,
      );
    }
  });
});

describe('nothing is built and left unreachable', () => {
  // The audit that prompted the editor found most of this codebase
  // built, tested and unreachable: the context menu, the heatmap,
  // the rich exporters, drill-through, selection statistics, saved
  // views and named dimensions each had exactly ONE user -- the file
  // that defined it. They were libraries, not features, and no test
  // noticed, because every one of them had good unit tests.
  //
  // So: a module that exists to be used by the product must be
  // imported by the product, and called. app.ts is the product.
  const APP = readFileSync(join('src', 'app.ts'), 'utf8');

  const REACHABLE: readonly [string, string][] = [
    ['the context menu', './ui/menu.ts'],
    ['the menu renderer', './ui/menu-view.ts'],
    ['the heatmap', './style.ts'],
    ['the rich exporters', './export-rich.ts'],
    ['plain export', './export.ts'],
    ['drill-through', './drill.ts'],
    ['selection statistics', './selection.ts'],
    ['saved views', './persist.ts'],
    ['named dimensions', './dimensions.ts'],
    ['the editor', './ui/editor.ts'],
    ['the filter editor', './ui/filter-editor.ts'],
    ['the drag zones', './ui/pivot-panel.ts'],
    ['the columns tool panel', './ui/columns-panel.ts'],
    ['the configuration', './config.ts'],
  ];

  for (const [what, module] of REACHABLE) {
    it(`reaches ${what}`, () => {
      assert.ok(
        APP.includes(`from '${module}'`),
        `src/app.ts does not import ${module}; ${what} is unreachable`,
      );
    });
  }

  it('actually calls what it imports', () => {
    // Importing a module and never calling it would satisfy the
    // check above while leaving the feature just as unreachable.
    for (const call of [
      'buildMenu(',
      'applyMenuAction(',
      'heatColour(',
      'columnRange(',
      'toSpreadsheetML(',
      'toHtml(',
      'toCsv(',
      'drillQuery(',
      'selectionStats(',
      'availableDimensions(',
      'new CubeEditor(',
      'new FilterEditor(',
      'new PivotPanel(',
      'new MenuView(',
    ]) {
      assert.ok(APP.includes(call), `src/app.ts never calls ${call}`);
    }
  });

  it('binds the context menu to a contextmenu event', () => {
    // The exact hole the audit found: the menu was built and tested
    // as data, and `grep -c contextmenu src/grid/grid.ts` returned 0.
    assert.ok(APP.includes("addEventListener('contextmenu'"));
  });
});
