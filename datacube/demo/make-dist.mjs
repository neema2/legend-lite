// Build a folder you can drop on any static host.
//
//   bazel build //datacube:dist   -> bazel-bin/datacube/dist/
//
// There is no backend to deploy. The planner is WebAssembly and so is
// DuckDB, so a DataCube deployment is a directory of files: put it
// behind nginx, S3, GitHub Pages, anything, and the URL is the
// product. That is the whole point of the in-browser planner -- it is
// not a convenience for the demo, it is what removes the server.

import { cp, mkdir, readFile, rm, writeFile } from 'node:fs/promises';
import { join, resolve } from 'node:path';

// A BUILD ACTION (datacube/BUILD.bazel, dist): Bazel builds the bundles
// and runtimes into <site-root>/demo, and names the directory to fill.
//   Usage: node demo/make-dist.mjs <site-root> <dist-dir>
if (process.argv.length !== 4) {
  console.error('usage: make-dist.mjs <site-root> <dist-dir>');
  process.exit(2);
}
const ROOT = resolve(process.argv[2]);
const DIST = resolve(process.argv[3]);
const v = (f) => join(ROOT, 'demo', 'vendor', f);

await rm(DIST, { recursive: true, force: true });
await mkdir(join(DIST, 'vendor'), { recursive: true });

// The planner's worker, the page's model, and the two WebAssembly runtimes.
for (const f of ['planner-worker.js', 'trades.pure']) {
  await cp(join(ROOT, 'demo', f), join(DIST, f));
}
for (const f of ['classes.wasm', 'wasm-gc-module-runtime.js',
  'duckdb-eh.wasm', 'duckdb-mvp.wasm',
  'duckdb-browser-eh.worker.js', 'duckdb-browser-mvp.worker.js']) {
  await cp(v(f), join(DIST, 'vendor', f));
}
// the pages' type: fonts.css and the files it names (vendor/fonts, Roboto from //legend-art:fonts), served with the site
await cp(join(ROOT, 'demo', 'fonts.css'), join(DIST, 'fonts.css'));
await cp(v('fonts'), join(DIST, 'vendor', 'fonts'), { recursive: true });

// THE PAGES, each with its bundle and what that loads when it needs it (ECharts, the first time a chart draws): the
// app (index.html), and one cube on an engine that runs its queries (engine.html: Python's engine serves this folder,
// docs/DATACUBE_PYTHON_SHOW_DESIGN_2026_10_08.md). Each links ../src/*.css, which does not exist in a flat
// deployment; they are inlined so the folder is self-contained.
for (const [page, bundle] of [['index.html', 'bundle'], ['engine.html', 'bundle-engine']]) {
  await cp(join(ROOT, 'demo', `${bundle}.js`), join(DIST, `${bundle}.js`));
  await cp(join(ROOT, 'demo', `chunks-${bundle}`), join(DIST, `chunks-${bundle}`), { recursive: true });
  let html = await readFile(join(ROOT, 'demo', page), 'utf8');
  const links = [...html.matchAll(/<link rel="stylesheet" href="\.\.\/([^"]+)">/g)];
  let css = '';
  for (const m of links) {
    css += `/* ${m[1]} */\n${await readFile(join(ROOT, m[1]), 'utf8')}\n`;
  }
  html = html.replace(/<link rel="stylesheet" href="\.\.\/[^"]+">\n?/g, '');
  html = html.replace('</head>', `<style>\n${css}</style>\n</head>`);
  await writeFile(join(DIST, page), html);
}

const { size } = await import('node:fs').then((fs) =>
  fs.promises.stat(join(DIST, 'vendor', 'classes.wasm')));
console.log(`dist/ is ready — a static site, no backend.`);
console.log(`  planner ${(size / 1e6).toFixed(1)} MB, plus duckdb-wasm`);

