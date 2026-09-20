// How long until the user sees a row?
//
// The only startup number that matters is wall-clock from navigation
// to the first rendered row, in a real browser, with a cold HTTP
// cache. Everything else -- module compile time, prelude parse --
// is attribution, useful for deciding what to fix but not a measure
// of what anyone experiences.
//
//   npm run measure:startup
import { createServer } from 'node:http';
import { readFile } from 'node:fs/promises';
import { extname, join, normalize } from 'node:path';
import { chromium } from 'playwright';

const ROOT = new URL('..', import.meta.url).pathname;
const TYPES = { '.html': 'text/html', '.js': 'text/javascript',
  '.wasm': 'application/wasm', '.pure': 'text/plain', '.css': 'text/css' };
const PAGE = process.env.PAGE ?? 'index.html';
const RUNS = Number(process.env.RUNS ?? 5);

const server = createServer(async (req, res) => {
  const p = (req.url ?? '/').split('?')[0];
  const rel = normalize(p === '/' ? '/demo/index.html' : p)
    .replace(/^(\.\.[/])+/, '');
  try {
    const body = await readFile(join(ROOT, rel));
    res.writeHead(200, {
      'Content-Type': TYPES[extname(rel)] ?? 'application/octet-stream',
      // No caching: every run is a first visit, which is the case
      // worth optimising. A warm cache flatters the numbers.
      'Cache-Control': 'no-store',
    });
    res.end(body);
  } catch { res.writeHead(404).end('nope'); }
});
await new Promise((r) => server.listen(0, '127.0.0.1', r));
const { port } = server.address();

const browser = await chromium.launch();
const samples = [];
for (let i = 0; i < RUNS; i++) {
  const ctx = await browser.newContext({ bypassCSP: true });
  const page = await ctx.newPage();
  const t0 = Date.now();
  await page.goto(`http://127.0.0.1:${port}/demo/${PAGE}`, { waitUntil: 'commit' });
  await page.waitForFunction(
    () => document.querySelectorAll('.dc-row').length > 0,
    { timeout: 120_000 },
  );
  samples.push(Date.now() - t0);
  if (i === 0) {
    // One run's internal marks, to say WHICH wait is the critical
    // path rather than only how long the whole thing took.
    const marks = await page.evaluate(() =>
      performance.getEntriesByType('mark')
        .filter((m) => m.name.startsWith('dc:'))
        .map((m) => [m.name, Math.round(m.startTime)]));
    console.log('  marks (ms from navigation):');
    for (const [name, at] of marks) {
      console.log(`    ${name.padEnd(20)} ${String(at).padStart(6)}`);
    }
  }
  await ctx.close();
}
await browser.close();
server.close();

samples.sort((a, b) => a - b);
const mid = samples[Math.floor(samples.length / 2)];
console.log(`${PAGE}: navigation → first row`);
console.log(`  runs   ${samples.join(', ')} ms`);
console.log(`  median ${mid} ms   (min ${samples[0]}, max ${samples[samples.length - 1]})`);
