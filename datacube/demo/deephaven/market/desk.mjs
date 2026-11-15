// THE TRADING DESK, by hand (docs/DEEPHAVEN_TRADER_DEMOS_2026_10_24.md, the trading desk):
//
//   bazel run //datacube:deephaven_desk            then open the URL it prints
//
// It starts the market world behind lite's Kafka door (equities simulated, crypto from Coinbase's public feed where this
// machine reaches it, simulated where it does not), installs the desk's deployment for the pinned Deephaven -- its plan
// beside lite's loader, which makes every table from the plan (docs/DEPLOYMENT_DESIGN_2026_11_14.md step 6) -- starts
// the server on it, and serves DataCube's own page -- the one
// //datacube:serve serves -- with its config.json naming that server: the page opens on the trading desk, one saved
// page of ten tabs, and Open…'s Examples list it first, above the chart pages. Every query is planned in the tab and
// run on the Deephaven server, ticking. The world's size is the process's own environment (MARKET_NAMES, MARKET_QUOTES,
// MARKET_TRADES; MARKET_FEED=off for no Coinbase feed). The server's update cycle is 250 ms -- four updates a second, a
// trading screen's pace (the tests' 100 ms ran each tile ten times a second, which kept a 4-core machine's page busy:
// 2026-10-26) -- or DESK_CYCLE_MS; its heap half the machine's memory, 4g to 16g, or DESK_HEAP (below: a long show
// needs it);
// DH_SERVER_FLAGS passes the server flags of its own (-DQueryCompiler.logEnabledDefault=true logs each formula it
// compiles). Type `restart` and Enter to restart Deephaven; Ctrl-C stops.
// Its certificate is mkcert's where mkcert is installed, so a browser on a machine that ran `mkcert -install` asks
// nothing (../mkcert.mjs); without mkcert it is the test server's own, and the browser asks once to accept it (open the
// Deephaven URL it prints first).

import { readFileSync } from 'node:fs';
import { tmpdir, totalmem } from 'node:os';
import { join } from 'node:path';
import { createInterface } from 'node:readline';
import { runfileDirUrl, runfileFromEnv } from '../../../../tools/js/runfiles.mts';
import { LitePlanner } from '../../../src/lite-planner.ts';
import { mkcertIdentity } from '../mkcert.mjs';
import { startDeephaven } from '../site.mjs';
import { serveDesk } from './desk-site.mjs';
import { DESK_MODEL, ON_DEEPHAVEN } from './model.ts';
import { startDeskOnDeephaven } from './on-deephaven.ts';

/** The desk's model and its one planner, the in-thread WebAssembly module (the planner is chosen here, from no setting:
 *  datacube's guardrail, one place a planner is chosen). */
function deskPlanner() {
  const model = DESK_MODEL(readFileSync(runfileFromEnv('TRADING_MODEL'), 'utf8'),
    readFileSync(runfileFromEnv('MARKET_PURE'), 'utf8'), readFileSync(runfileFromEnv('ENVIRONMENTS_PURE'), 'utf8'));
  return { model, planner: new LitePlanner({ model, runtime: ON_DEEPHAVEN.runtime, assetBaseUrl: runfileDirUrl('WASM_PLANNER') }) };
}

/** The world's size and pace, from the environment: each optional (the world's own defaults). */
function worldSettings() {
  const env = process.env;
  const sized = { names: env.MARKET_NAMES, quotes: env.MARKET_QUOTES, trades: env.MARKET_TRADES };
  return {
    ...Object.fromEntries(Object.entries(sized).filter(([, v]) => v !== undefined).map(([k, v]) => [k, Number(v)])),
    ...(env.MARKET_FEED === 'off' ? { feed: 'off' } : {}),
  };
}

const identity = mkcertIdentity();
const { model, planner } = deskPlanner();
const desk = await startDeskOnDeephaven({ model, planner, dir: join(tmpdir(), 'legend-desk'),
  loader: runfileFromEnv('DESK_LOADER'), settings: worldSettings() });
console.log(`The market world publishes behind the door on 127.0.0.1:${desk.feed.port}: ${desk.plan.tables.length} tables`
  + ' in the deployment\'s plan.');
const deephaven = startDeephaven({ quiet: true, fixture: 'MARKET_FIXTURE', env: desk.installed.env, args: [
  `--jvm_flag=-Dembedded.deephaven.application=${desk.installed.app}`, ...identity.args,
  `--jvm_flag=-Dembedded.deephaven.cycleMs=${process.env.DESK_CYCLE_MS ?? 250}`,
  `--jvm_flag=-Dembedded.deephaven.heap=${heap}`,
  ...(process.env.DH_SERVER_FLAGS ? [`--jvm_flag=-Dembedded.deephaven.serverFlags=${process.env.DH_SERVER_FLAGS}`] : [])] });
const config = await deephaven.next('on starting');
const port = Number(process.argv.find((a) => a.startsWith('--port='))?.slice(7) ?? 0);
const site = await serveDesk(config, port);
console.log(`\nDeephaven: ${config.url} ${identity.said}`);
console.log(`The trading desk, in DataCube: ${site.desk}`);
console.log(`DataCube itself (the desk under Open… ▸ Examples): ${site.url}/demo/index.html`);
console.log(`Deephaven's heap: ${heap}${process.env.DESK_HEAP === undefined ? ' (half this machine\'s memory; DESK_HEAP sets it)' : ''}`
  + (showMinutes === null ? '' : `: about ${showMinutes >= 120 ? `${(showMinutes / 60).toFixed(1)} hours` : `${showMinutes} minutes`}`
    + ' of trading at the world\'s defaults before its collector slows (the server keeps every trade).') + '\n');
console.log('Type restart and Enter to restart Deephaven; Ctrl-C to stop.');

const input = createInterface({ input: process.stdin });
input.on('line', (line) => {
  if (line.trim() === 'restart') {
    console.log('Restarting Deephaven...');
    deephaven.restart();
    void deephaven.next('on restarting').then(() => console.log('Deephaven is back: the desk reconnects by itself.'));
  }
});
const stop = () => {
  site.close();
  void deephaven.stop().then(() => desk.feed.close()).then(() => process.exit(0));
};
process.on('SIGINT', stop);
process.on('SIGTERM', stop);
