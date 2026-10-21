// The spike's measurements, in headless Chromium against a running server and connector
// (./run.sh server; ./run.sh connector). Writes results.json and screenshots to <outDir>.
//
//   node measure.mjs <outDir> [--server-pid PID] [--playwright /path/to/playwright/index.mjs]
//
// Freshness is the browser's clock minus the newest trade timestamp in the update (the LastTs
// column): the age of the newest data when it reaches the page. The server and the browser share
// one machine, so their clocks agree; across machines this needs a clock-offset correction.

import { mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';

const args = process.argv.slice(2);
const outDir = args[0];
if (!outDir) throw new Error('usage: node measure.mjs <outDir> [--server-pid PID] [--playwright PATH]');
const opt = (name, fallback) => {
  const i = args.indexOf(name);
  return i >= 0 ? args[i + 1] : fallback;
};
const serverPid = opt('--server-pid', null);
const { chromium } = await import(opt('--playwright', 'playwright'));
mkdirSync(outDir, { recursive: true });

const BASE = 'http://localhost:8090';
const PLANS = ['by_symbol', 'grand_total', 'sym_venue', 'slippage_aj'];
const results = { when: new Date().toISOString(), scenarios: {} };

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const pct = (xs, p) => {
  if (xs.length === 0) return null;
  const s = [...xs].sort((a, b) => a - b);
  return s[Math.min(s.length - 1, Math.floor(p * s.length))];
};
/** CPU seconds the server process has used so far (utime + stime from /proc). */
const cpuSeconds = () => {
  if (!serverPid) return null;
  const f = readFileSync(`/proc/${serverPid}/stat`, 'utf8').split(') ')[1].split(' ');
  return (Number(f[11]) + Number(f[12])) / 100;
};

const browser = await chromium.launch();

async function openViewer(plans = PLANS) {
  const page = await browser.newPage({ viewport: { width: 1500, height: 1100 } });
  page.on('pageerror', (e) => console.log('[pageerror]', e.message));
  const cdp = await page.context().newCDPSession(page);
  await cdp.send('Network.enable');
  // The JS API multiplexes every call over one WebSocket; binary frames arrive base64-encoded.
  const traffic = { bytes: 0, frames: 0 };
  cdp.on('Network.webSocketFrameReceived', (e) => {
    const p = e.response.payloadData;
    traffic.bytes += e.response.opcode === 2 ? Math.floor((p.length * 3) / 4) : p.length;
    traffic.frames += 1;
  });
  await page.goto(`${BASE}/?open=${plans.join(',')}`);
  await page.waitForFunction((n) => Object.keys(window.spikeMetrics?.panels ?? {}).length === n
    && Object.values(window.spikeMetrics.panels).every((p) => p.events > 0), plans.length, { timeout: 60000 });
  return { page, traffic };
}

const snapshot = (page) => page.evaluate(() => JSON.parse(JSON.stringify(window.spikeMetrics)));

/** Runs for `ms` at an update interval and reports each panel's rates and freshness. */
async function steady(page, interval, ms) {
  await page.selectOption('#interval', String(interval));
  await sleep(1500); // let the new interval take effect
  const before = await snapshot(page);
  const cpu0 = cpuSeconds();
  await sleep(ms);
  const after = await snapshot(page);
  const cpu1 = cpuSeconds();
  const out = { interval, seconds: ms / 1000, serverCpuCores: cpu0 === null ? null : +((cpu1 - cpu0) / (ms / 1000)).toFixed(2), panels: {} };
  for (const name of PLANS) {
    const a = before.panels[name];
    const b = after.panels[name];
    const added = b.freshness.length - a.freshness.length;
    const fresh = added > 0 ? b.freshness.slice(-added) : [];
    out.panels[name] = {
      updatesPerSecond: +((b.events - a.events) / (ms / 1000)).toFixed(1),
      changedCellsPerSecond: +((b.changedCells - a.changedCells) / (ms / 1000)).toFixed(1),
      freshnessMs: { p50: pct(fresh, 0.5), p95: pct(fresh, 0.95), p99: pct(fresh, 0.99), n: fresh.length },
    };
  }
  return out;
}

// 1. Opening: connect, run each plan, fetch, first data.
const { page, traffic } = await openViewer();
const opened = await snapshot(page);
results.scenarios.open = {
  connectMillis: opened.connectMillis,
  panels: Object.fromEntries(PLANS.map((n) => {
    const p = opened.panels[n];
    return [n, { publishMillis: p.publishMillis, handleMillis: p.handleMillis, fetchMillis: p.fetchMillis, firstDataMillis: p.firstDataMillis, rows: p.size }];
  })),
};
await page.screenshot({ path: join(outDir, 'page.png'), fullPage: true });

// 2. Steady state at three update intervals.
results.scenarios.steady = [];
for (const interval of [1000, 250, 100]) {
  results.scenarios.steady.push(await steady(page, interval, 20000));
}

// 3. Bytes on the wire: a page holding by_symbol alone, its viewport (38 rows) against the whole
//    table (500 rows), at 250 ms.
await page.selectOption('#interval', '250');
const solo = await openViewer(['by_symbol']);
async function bytesFor(first, last) {
  await solo.page.evaluate(([f, l]) => window.forceWindow('by_symbol', f, l), [first, last]);
  await sleep(3000);
  const b0 = solo.traffic.bytes;
  const f0 = solo.traffic.frames;
  await sleep(15000);
  return {
    bytesPerSecond: Math.round((solo.traffic.bytes - b0) / 15),
    framesPerSecond: +((solo.traffic.frames - f0) / 15).toFixed(1),
  };
}
results.scenarios.traffic = {
  note: 'WebSocket bytes per second from Deephaven to a page holding by_symbol alone, at 250 ms',
  viewportRows0to37: await bytesFor(0, 37),
  wholeTableRows0to499: await bytesFor(0, 499),
};
await solo.page.close();

// 4. Scrolling moves the subscription: time from the scroll to data at the new rows.
const scrollStart = Date.now();
await page.evaluate(() => {
  document.querySelector('section[data-plan="by_symbol"] .grid').scrollTop = 300 * 22;
});
await page.waitForFunction(() => window.spikeMetrics.panels.by_symbol.offset >= 280, null, { timeout: 15000 });
const scrolled = await snapshot(page);
results.scenarios.scroll = {
  scrollToDataMillis: Date.now() - scrollStart,
  viewport: scrolled.panels.by_symbol.viewport,
  firstRowOffset: scrolled.panels.by_symbol.offset,
};
await page.screenshot({ path: join(outDir, 'scrolled.png') });

// 5. A second viewer of the same plans: the connector answers the live tables it already built.
const second = await openViewer();
await sleep(3000);
const s2 = await snapshot(second.page);
results.scenarios.secondViewer = Object.fromEntries(PLANS.map((n) => [n, {
  reused: s2.panels[n].reused, sharedTicket: s2.panels[n].sharedTicket, events: s2.panels[n].events,
}]));

// 6. Lifetime: the connector releases its hold on grand_total. Do subscribed viewers keep ticking,
//    and can a new viewer still fetch the ticket?
const g0 = (await snapshot(page)).panels.grand_total.events;
const released = await page.evaluate(() => window.releaseFromConnector('grand_total'));
await sleep(5000);
const g1 = (await snapshot(page)).panels.grand_total.events;
const lateFetch = await page.evaluate(async (ticket) => {
  try {
    const client = new (await import('/jsapi/dh-core.js')).default.CoreClient((await (await fetch('/api/config')).json()).deephaven);
    const cfg = await (await fetch('/api/config')).json();
    await client.login(cfg.login);
    const ide = await client.getAsIdeConnection();
    const t = await ide.getSharedObject(ticket, 'Table');
    return { fetched: true, size: t.size };
  } catch (e) {
    return { fetched: false, error: String(e && e.message ? e.message : e).slice(0, 300) };
  }
}, s2.panels.grand_total.sharedTicket);
results.scenarios.release = {
  released,
  existingViewerEventsDuring5sAfterRelease: g1 - g0,
  newViewerFetchAfterRelease: lateFetch,
};

writeFileSync(join(outDir, 'results.json'), JSON.stringify(results, null, 2));
console.log(JSON.stringify(results, null, 2));
await browser.close();
