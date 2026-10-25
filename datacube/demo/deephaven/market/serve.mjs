// THE TRADER SCREENS, by hand (docs/DEEPHAVEN_TRADER_DEMOS_2026_10_24.md K4, K8-K13):
//
//   bazel run //datacube:deephaven_traders            then open the URL it prints
//
// It starts the pinned Deephaven running the market world (market.groovy: equities simulated, crypto from Coinbase's
// public feed where the server reaches it, simulated where it does not) and serves DataCube's trader screens on the
// loopback -- each screen a saved DataCube page, opened from the page's menu (Trader screens ▸), every query planned
// in the tab and run on the Deephaven server, ticking. The world's size is the process's own environment
// (MARKET_NAMES, MARKET_QUOTES, MARKET_TRADES, MARKET_FEED: market.groovy says each). Type `restart` and Enter to
// restart Deephaven; Ctrl-C stops. The test server's certificate is its own: the browser asks once to accept it (open
// the Deephaven URL it prints first, or start the browser with --ignore-certificate-errors).

import { createInterface } from 'node:readline';
import { startDeephaven } from '../site.mjs';
import { serveTraders } from './site.mjs';

const deephaven = startDeephaven({ quiet: true, fixture: 'MARKET_FIXTURE' });
const config = await deephaven.next('on starting');
const port = Number(process.argv.find((a) => a.startsWith('--port='))?.slice(7) ?? 0);
const site = await serveTraders(config, port);
console.log(`\nDeephaven: ${config.url} (its certificate is the test server's own: accept it once)`);
console.log(`The trader screens: ${site.url}/ (each screen: ${site.url}/#ticker, ...)\n`);
console.log('Type restart and Enter to restart Deephaven; Ctrl-C to stop.');

const input = createInterface({ input: process.stdin });
input.on('line', (line) => {
  if (line.trim() === 'restart') {
    console.log('Restarting Deephaven...');
    deephaven.restart();
    void deephaven.next('on restarting').then(() => console.log('Deephaven is back: the screens reconnect by themselves.'));
  }
});
const stop = () => {
  site.close();
  void deephaven.stop().then(() => process.exit(0));
};
process.on('SIGINT', stop);
process.on('SIGTERM', stop);
