// THE LIVE RISK DEMO, by hand (docs/DEEPHAVEN_PLAN_2026_10_22.md D0, DH23):
//
//   bazel run //datacube:deephaven_demo            then open the URL it prints
//
// It starts the pinned Deephaven with the demo's start-up script (its reference data and price simulator, marked
// temporary) and serves DataCube on Deephaven on the loopback -- DataCube's own app, every query planned in the tab and
// run on the Deephaven server, ticking, and back by itself after a restart. Type `restart` and Enter to restart
// Deephaven; Ctrl-C stops. Its certificate is mkcert's where mkcert is installed, so a browser on a machine that ran
// `mkcert -install` asks nothing (mkcert.mjs); without mkcert it is the test server's own, and the browser asks once to
// accept it (open the Deephaven URL it prints first).

import { createInterface } from 'node:readline';
import { mkcertIdentity } from './mkcert.mjs';
import { serveDemo, startDeephaven } from './site.mjs';

const identity = mkcertIdentity();
const deephaven = startDeephaven({ quiet: true, args: identity.args });
const config = await deephaven.next('on starting');
const port = Number(process.argv.find((a) => a.startsWith('--port='))?.slice(7) ?? 0);
const site = await serveDemo(config, port);
console.log(`\nDeephaven: ${config.url} ${identity.said}`);
console.log(`DataCube on Deephaven: ${site.url}/\n`);
console.log('Type restart and Enter to restart Deephaven; Ctrl-C to stop.');

const input = createInterface({ input: process.stdin });
input.on('line', (line) => {
  if (line.trim() === 'restart') {
    console.log('Restarting Deephaven...');
    deephaven.restart();
    void deephaven.next('on restarting').then(() => console.log('Deephaven is back: the cube reconnects by itself.'));
  }
});
const stop = () => {
  site.close();
  void deephaven.stop().then(() => process.exit(0));
};
process.on('SIGINT', stop);
process.on('SIGTERM', stop);
