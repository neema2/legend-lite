// Build a folder you can drop on any static host.
//
//   npm run dist        -> datacube/dist/
//
// There is no backend to deploy. The planner is WebAssembly and so is
// DuckDB, so a DataCube deployment is a directory of files: put it
// behind nginx, S3, GitHub Pages, anything, and the URL is the
// product. That is the whole point of the in-browser planner -- it is
// not a convenience for the demo, it is what removes the server.

import { execFileSync } from 'node:child_process';
import { cp, mkdir, readFile, rm, writeFile } from 'node:fs/promises';
import { existsSync } from 'node:fs';
import { join, resolve } from 'node:path';

const ROOT = resolve(new URL('.', import.meta.url).pathname, '..');
const DIST = join(ROOT, 'dist');

function run(label, script) {
  process.stdout.write(`  ${label}… `);
  execFileSync('npm', ['run', script], { cwd: ROOT, stdio: 'pipe' });
  console.log('done');
}

const v = (f) => join(ROOT, 'demo', 'vendor', f);
if (!existsSync(v('duckdb-eh.wasm'))) run('duckdb-wasm binaries', 'demo:vendor');
if (!existsSync(v('classes.wasm'))) {
  run('legend-lite core', 'planner:core');
  run('planner → wasm', 'planner:build');
  run('vendoring the planner', 'planner:vendor');
}
run('bundles', 'build:demo');

await rm(DIST, { recursive: true, force: true });
await mkdir(join(DIST, 'vendor'), { recursive: true });

// The page, its bundles, its model, and the two WebAssembly runtimes.
for (const f of ['bundle.js', 'planner-worker.js', 'trades.pure']) {
  await cp(join(ROOT, 'demo', f), join(DIST, f));
}
for (const f of ['classes.wasm', 'wasm-gc-module-runtime.js',
  'duckdb-eh.wasm', 'duckdb-mvp.wasm',
  'duckdb-browser-eh.worker.js', 'duckdb-browser-mvp.worker.js']) {
  await cp(v(f), join(DIST, 'vendor', f));
}

// index.html links ../src/*.css, which does not exist in a flat
// deployment; inline them so the folder is self-contained.
let html = await readFile(join(ROOT, 'demo', 'index.html'), 'utf8');
const links = [...html.matchAll(/<link rel="stylesheet" href="\.\.\/([^"]+)">/g)];
let css = '';
for (const m of links) {
  css += `/* ${m[1]} */\n${await readFile(join(ROOT, m[1]), 'utf8')}\n`;
}
html = html.replace(/<link rel="stylesheet" href="\.\.\/[^"]+">\n?/g, '');
html = html.replace('</head>', `<style>\n${css}</style>\n</head>`);
await writeFile(join(DIST, 'index.html'), html);

const { size } = await import('node:fs').then((fs) =>
  fs.promises.stat(join(DIST, 'vendor', 'classes.wasm')));
console.log(`\ndist/ is ready — a static site, no backend.`);
console.log(`  planner ${(size / 1e6).toFixed(1)} MB, plus duckdb-wasm`);
console.log('\n  npx serve dist        # or copy dist/ to any web host');
console.log('  Point it at data with ?remote=<url>&format=parquet\n');
