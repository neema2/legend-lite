// THE TRADING DESK, by hand (docs/DEEPHAVEN_TRADER_DEMOS_2026_10_24.md, the trading desk):
//
//   bazel run //datacube:deephaven_desk            then open the URL it prints
//
// It starts the pinned Deephaven running the market world (market.groovy: equities simulated, crypto from Coinbase's
// public feed where the server reaches it, simulated where it does not) and serves DataCube's own page -- the one
// //datacube:serve serves -- with its config.json naming that server: the page opens on the trading desk, one saved
// page of ten tabs, and Open…'s Examples list it first, above the chart pages. Every query is planned in the tab and
// run on the Deephaven server, ticking. The world's size is the process's own environment (MARKET_NAMES, MARKET_QUOTES,
// MARKET_TRADES, MARKET_FEED: market.groovy says each). The server's update cycle is 250 ms -- four updates a second, a
// trading screen's pace (the tests' 100 ms ran each tile ten times a second, which kept a 4-core machine's page busy:
// 2026-10-26) -- or DESK_CYCLE_MS; its heap 4g (DESK_HEAP: the tests' 1g ran out once every tab's tables were made);
// DH_SERVER_FLAGS passes the server flags of its own (-DQueryCompiler.logEnabledDefault=true logs each formula it
// compiles). Type `restart` and Enter to restart Deephaven; Ctrl-C stops.
// Its certificate is mkcert's where mkcert is installed, so a browser on a machine that ran `mkcert -install` asks
// nothing (../mkcert.mjs); without mkcert it is the test server's own, and the browser asks once to accept it (open the
// Deephaven URL it prints first).

import { createInterface } from 'node:readline';
import { mkcertIdentity } from '../mkcert.mjs';
import { startDeephaven } from '../site.mjs';
import { serveDesk } from './desk-site.mjs';

const identity = mkcertIdentity();
const deephaven = startDeephaven({ quiet: true, fixture: 'MARKET_FIXTURE', args: [...identity.args,
  `--jvm_flag=-Dembedded.deephaven.cycleMs=${process.env.DESK_CYCLE_MS ?? 250}`,
  `--jvm_flag=-Dembedded.deephaven.heap=${process.env.DESK_HEAP ?? '4g'}`,
  ...(process.env.DH_SERVER_FLAGS ? [`--jvm_flag=-Dembedded.deephaven.serverFlags=${process.env.DH_SERVER_FLAGS}`] : [])] });
const config = await deephaven.next('on starting');
const port = Number(process.argv.find((a) => a.startsWith('--port='))?.slice(7) ?? 0);
const site = await serveDesk(config, port);
console.log(`\nDeephaven: ${config.url} ${identity.said}`);
console.log(`The trading desk, in DataCube: ${site.desk}`);
console.log(`DataCube itself (the desk under Open… ▸ Examples): ${site.url}/demo/index.html\n`);
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
  void deephaven.stop().then(() => process.exit(0));
};
process.on('SIGINT', stop);
process.on('SIGTERM', stop);
