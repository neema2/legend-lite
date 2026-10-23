// THE LIVE RISK DEMO, run in the pinned Chromium (//datacube:deephaven_demo_test; docs/DEEPHAVEN_PLAN_2026_10_22.md D0).
// Node only drives: it starts the pinned Deephaven with the demo's start-up script (its reference data and price
// simulator), serves the demo page (demo/deephaven/) on the loopback, and checks what the page shows: the grid ticks,
// its positions per desk and sector are the seeding's own (worked out here from the same formulas), and when Deephaven
// restarts the page says so and comes back by itself. Screenshots of each stage are the test's outputs.

// first: points Playwright at the Chromium Bazel fetched
import '../../../tools/browser/pinned-chromium.mjs';
import { spawn } from 'node:child_process';
import { readFileSync } from 'node:fs';
import { createServer } from 'node:http';
import { join } from 'node:path';
import { chromium } from 'playwright';
import { runfileFromEnv, runfileNamed } from '../../../tools/js/runfiles.mts';

const fixture = runfileFromEnv('DEEPHAVEN_FIXTURE');
const files = {
  '/': ['text/html', readFileSync(runfileFromEnv('DEMO_PAGE'))],
  '/risk.js': ['text/javascript', readFileSync(runfileFromEnv('DEMO_BUNDLE'))],
  '/wasm-gc-module-runtime.js': ['text/javascript', readFileSync(runfileNamed('PLANNER', 'wasm-gc-module-runtime.js'))],
  '/classes.wasm': ['application/wasm', readFileSync(runfileNamed('PLANNER', 'classes.wasm'))],
};
const outputs = process.env.TEST_UNDECLARED_OUTPUTS_DIR ?? process.env.TEST_TMPDIR ?? '.';

// the seeding's own positions per desk and sector (demo/deephaven/risk.groovy): position i is book (i / 40) % 8 and
// instrument i % 40; book b's desk is b % 4, instrument n's sector n % 4
const DESKS = ['Rates', 'FX', 'Credit', 'Equities'];
const SECTORS = ['Tech', 'Energy', 'Banks', 'Retail'];
const expected = new Map();
for (let i = 0; i < 400; i++) {
  const key = `${DESKS[(Math.floor(i / 40) % 8) % 4]}|${SECTORS[(i % 40) % 4]}`;
  expected.set(key, (expected.get(key) ?? 0) + 1);
}

const server = spawn(fixture, [], { stdio: ['pipe', 'pipe', 'pipe'], env: { ...process.env } });
server.stderr.on('data', (b) => {
  for (const line of String(b).split('\n')) if (line && !/ INFO | DEBUG /.test(line)) process.stderr.write(`[deephaven] ${line}\n`);
});
let said = '';
const lines = [];
server.stdout.on('data', (b) => {
  said += String(b);
  const all = said.split('\n');
  said = all.pop() ?? '';
  for (const l of all) if (l.startsWith('{')) lines.push(JSON.parse(l));
});
const next = async (what) => {
  for (let i = 0; i < 1800 && lines.length === 0; i++) await new Promise((r) => setTimeout(r, 100));
  if (lines.length === 0) throw new Error(`the fixture said nothing ${what}`);
  return lines.shift();
};
server.on('exit', (code) => console.log(`the Deephaven fixture exited (${code})`));
const config = await next('on starting');
files['/config.json'] = ['application/json', JSON.stringify({ url: config.url, psk: config.psk })];

const site = createServer((req, res) => {
  const f = files[new URL(req.url, 'http://page').pathname];
  if (!f) {
    res.writeHead(404);
    res.end();
    return;
  }
  res.writeHead(200, { 'content-type': f[0] });
  res.end(f[1]);
});
const sitePort = await new Promise((done) => site.listen(0, '127.0.0.1', () => done(site.address().port)));

let failed = false;
const fail = (why) => { console.log(`FAIL ${why}`); failed = true; };
const ok = (what) => console.log(`ok   ${what}`);
let browser;
try {
  browser = await chromium.launch();
  const page = await (await browser.newContext({ ignoreHTTPSErrors: true, viewport: { width: 900, height: 900 } })).newPage();
  page.on('pageerror', (e) => fail(`page error: ${e.message}`));
  await page.goto(`http://127.0.0.1:${sitePort}/`);
  const state = () => page.evaluate(() => window.__demo);
  const until = async (test, what, ms = 120_000) => {
    const end = Date.now() + ms;
    while (Date.now() < end) {
      const s = await state();
      if (test(s)) return s;
      await new Promise((r) => setTimeout(r, 200));
    }
    throw new Error(`not ${what} in ${ms / 1000} s: ${JSON.stringify(await state()).slice(0, 400)}`);
  };
  const counts = (s) => {
    const wrong = [];
    if (s.rows.length !== expected.size) wrong.push(`${s.rows.length} rows, not ${expected.size}`);
    for (const [desk, sector, mv, n] of s.rows) {
      if (expected.get(`${desk}|${sector}`) !== n) wrong.push(`${desk}/${sector}: ${n} positions, not ${expected.get(`${desk}|${sector}`)}`);
      if (!(mv > 0)) wrong.push(`${desk}/${sector}: market value ${mv}`);
    }
    return wrong;
  };

  const first = await until((s) => s.status === 'live' && s.updates >= 3, 'live');
  await page.screenshot({ path: join(outputs, '1-live.png'), fullPage: true });
  const wrong = counts(first);
  if (wrong.length) fail(`the grid's positions:\n  ${wrong.join('\n  ')}`); else ok(`${first.rows.length} desk and sector rows, each with the seeding's positions`);
  const mvs = JSON.stringify(first.rows.map((r) => r[2]));
  const later = await until((s) => s.updates >= first.updates + 5, 'ticking', 20_000);
  if (JSON.stringify(later.rows.map((r) => r[2])) === mvs) fail('the market values did not change'); else ok(`it ticks: ${later.updates - first.updates} updates in a few seconds, the values moving`);
  await page.screenshot({ path: join(outputs, '2-ticking.png'), fullPage: true });

  // a restart: Deephaven keeps nothing across one; its start-up script seeds again, and the page must come back alone
  server.stdin.write('restart\n');
  const down = await until((s) => s.status === 'reconnecting', 'reconnecting after the restart', 60_000);
  ok(`the page saw the server go (${down.error.slice(0, 80)})`);
  await page.screenshot({ path: join(outputs, '3-reconnecting.png'), fullPage: true });
  const restarted = await next('on restarting');
  if (!restarted.restarted) fail(`the fixture answered ${JSON.stringify(restarted)}`);
  const back = await until((s) => s.status === 'live' && s.reconnects >= 1 && s.updates >= later.updates + 3, 'live again', 120_000);
  const again = counts(back);
  if (again.length) fail(`after the restart:\n  ${again.join('\n  ')}`); else ok(`back after the restart (${back.reconnects} reconnects), the same positions`);
  await page.screenshot({ path: join(outputs, '4-back.png'), fullPage: true });
} catch (e) {
  fail(String(e?.stack ?? e));
} finally {
  await browser?.close();
  site.close();
  server.stdin.end();
  await new Promise((r) => setTimeout(r, 2000));
  if (server.exitCode === null) server.kill('SIGKILL');
}
process.exit(failed ? 1 : 0);
