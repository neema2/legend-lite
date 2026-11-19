// THE DIST HOLDS WHAT ITS PAGES IMPORT (PR #118 fix round, 2026-11-18): //datacube:dist is a folder for any static host,
// so every file a page reaches by a relative URL must be in it. Read from the pages themselves: each page's module
// script, then every module it reaches -- a static import, a dynamic import() and a `new URL('./...', import.meta.url)`
// (the planner's worker, the legend-engine plane's own bundle, demo/planners.ts) -- each resolved against the file that
// names it. The find that made it: the engine plane's bundle (legend-engine.js, chunks-legend-engine/) was built into
// the site but never copied into the dist, so the app failed to start in engine mode ("Failed to fetch dynamically
// imported module"); //datacube:app serves this same folder (site = ":dist").
import assert from 'node:assert/strict';
import { existsSync, mkdirSync, mkdtempSync, readFileSync, statSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join, relative, resolve } from 'node:path';
import { test } from 'node:test';
import { runfileFromEnv } from '../../../tools/js/runfiles.mts';

const DIST = runfileFromEnv('DIST');

// a relative specifier a module names: `from"./x"`, `import"./x"`, `import("./x")`, `new URL("./x",import.meta.url)` --
// of a file a browser loads (a module, its WebAssembly, JSON or styles): the TypeScript a page writes out as text (Show
// TypeScript's import lines, inside a string of a chunk) names .ts files no browser fetches
const LOADED = String.raw`\.\.?\/[^"'\s]+\.(?:m?js|wasm|json|css)`;
const SPECIFIERS = [
  new RegExp(String.raw`\bfrom\s*["'](${LOADED})["']`, 'g'),
  new RegExp(String.raw`\bimport\s*["'](${LOADED})["']`, 'g'),
  new RegExp(String.raw`\bimport\s*\(\s*["'](${LOADED})["']\s*\)`, 'g'),
  new RegExp(String.raw`\bnew\s+URL\(\s*["'](${LOADED})["']\s*,\s*import\.meta\.url\s*\)`, 'g'),
];

/** The relative files `text` names, each resolved against `from`'s folder. */
function named(text: string, from: string): string[] {
  return SPECIFIERS.flatMap((re) => [...text.matchAll(re)].map((m) => resolve(dirname(from), m[1]!)));
}

/** `file`'s path under `base` with `/` between its parts, on every OS: what the test names and compares (Windows'
 *  `relative()` writes `\`, as datacube/test/guardrails.test.ts normalizes it; PR #118's fix round, the audit's
 *  BUILD-GUARDS-1). The files themselves are compared as `resolve` and `join` give them, the same on each side. */
function under(base: string, file: string): string {
  return relative(base, file).replace(/\\/g, '/');
}

/** Every file reached from `roots`, and each one reached that the folder `base` lacks (with who names it). */
function walk(base: string, roots: string[]): { reached: Set<string>; missing: string[] } {
  const reached = new Set<string>();
  const missing: string[] = [];
  const todo = roots.map((f) => ({ file: f, by: '(a page)' }));
  while (todo.length) {
    const { file, by } = todo.pop()!;
    if (reached.has(file)) continue;
    reached.add(file);
    if (!existsSync(file) || !statSync(file).isFile()) {
      missing.push(`${under(base, file)} (named by ${by})`);
      continue;
    }
    if (!/\.m?js$/.test(file)) continue;   // a module, .js or .mjs, names what it loads; a .wasm, .json or .css none
    for (const next of named(readFileSync(file, 'utf8'), file)) todo.push({ file: next, by: under(base, file) });
  }
  return { reached, missing };
}

const PAGES = ['index.html', 'engine.html'];

/** Each page's module scripts, by their src. */
function pageScripts(): string[] {
  return PAGES.flatMap((page) => {
    const html = readFileSync(join(DIST, page), 'utf8');
    const srcs = [...html.matchAll(/<script\b[^>]*\btype="module"[^>]*\bsrc="([^"]+)"/g)].map((m) => m[1]!);
    assert.ok(srcs.length > 0, `${page} names no module script`);
    return srcs.map((s) => resolve(DIST, s));
  });
}

test('every file a page of the dist imports by a relative URL is in the dist', () => {
  const { reached, missing } = walk(DIST, pageScripts());
  assert.deepEqual(missing, [], `the dist lacks what its pages import:\n  ${missing.join('\n  ')}`);
  // the walk reaches what it is for: the planner's worker and the legend-engine plane's bundle, both by
  // `new URL(..., import.meta.url)` (demo/planners.ts), and a split chunk of each page's bundle
  for (const f of ['planner-worker.js', 'legend-engine.js']) {
    assert.ok(reached.has(join(DIST, f)), `the walk never reached ${f}: its URL is no longer read this way`);
  }
  for (const d of ['chunks-bundle', 'chunks-bundle-engine', 'chunks-legend-engine']) {
    assert.ok([...reached].some((f) => under(DIST, f).startsWith(`${d}/`)), `the walk reached no file of ${d}/`);
  }
});

test('the walk names a file a module imports that the folder lacks', () => {
  // its bite: a folder whose page module reaches a chunk that is there, and names a worker and a plane that are not
  const dir = mkdtempSync(join(process.env.TEST_TMPDIR ?? tmpdir(), 'dist-'));
  writeFileSync(join(dir, 'page.js'),
    'import{a}from"./chunks/a.js";new URL("./worker.js",import.meta.url);import("./plane.js");import"./lib.mjs";'
    // TypeScript written as text, which no browser fetches: not followed
    + 'const shown="import { x } from \'./src/x.ts\';";');
  const chunk = join(dir, 'chunks');
  mkdirSync(chunk);
  // a chunk naming a file in a folder of its own that is not there: named by its path with `/` on every OS
  writeFileSync(join(chunk, 'a.js'), 'import"../shared.js";import"./deep/none.js";export const a=1;');
  writeFileSync(join(dir, 'shared.js'), '');
  // a reached .mjs module is read too: what it imports is the dist's to hold (the review of PR #118's fix round)
  writeFileSync(join(dir, 'lib.mjs'), 'export{b}from"./gone.js";');
  const { reached, missing } = walk(dir, [join(dir, 'page.js')]);
  assert.deepEqual(missing.sort(), ['chunks/deep/none.js (named by chunks/a.js)', 'gone.js (named by lib.mjs)',
    'plane.js (named by page.js)', 'worker.js (named by page.js)']);
  assert.ok(reached.has(join(dir, 'shared.js')), 'a chunk\'s own import, resolved against the chunk, is followed');
});
