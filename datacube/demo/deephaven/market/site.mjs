// THE TRADER SCREENS' SITE (docs/DEEPHAVEN_TRADER_DEMOS_2026_10_24.md K4, K8-K13), for the test
// (test/deephaven/traders.mjs; the hand tool went 2026-10-26): the trader screens' page (traders.html) served at / on
// the loopback from Bazel's runfiles, beside DataCube's styles, the planner's module, the saved pages' module, the one trading model the page
// holds, and the market world's address and key (config.json). The server itself is started by ../site.mjs's
// startDeephaven.

import { readFileSync } from 'node:fs';
import { createServer } from 'node:http';
import { runfileFromEnv, runfileNamed } from '../../../../tools/js/runfiles.mts';

/** Serves the trader screens on the loopback, at `port` (0: any free one), with `config` as config.json. */
export async function serveTraders(config, port = 0) {
  const style = (name) => ['text/css', readFileSync(runfileNamed('CUBE_STYLES', name))];
  const files = {
    '/': ['text/html', readFileSync(runfileFromEnv('TRADERS_PAGE'))],
    '/traders.js': ['text/javascript', readFileSync(runfileFromEnv('TRADERS_BUNDLE'))],
    '/app.css': style('app.css'),
    '/grid.css': style('grid.css'),
    '/theme.css': style('theme.css'),
    '/wasm-gc-module-runtime.js': ['text/javascript', readFileSync(runfileNamed('PLANNER', 'wasm-gc-module-runtime.js'))],
    '/classes.wasm': ['application/wasm', readFileSync(runfileNamed('PLANNER', 'classes.wasm'))],
    // the saved pages' rules (Open…, Save), the tab's own store's module
    '/queries/wasm-gc-module-runtime.js': ['text/javascript', readFileSync(runfileNamed('QUERIES', 'wasm-gc-module-runtime.js'))],
    '/queries/classes.wasm': ['application/wasm', readFileSync(runfileNamed('QUERIES', 'classes.wasm'))],
    // the one trading model (//scripts/corpus:trading_model), which the page holds
    '/trading-model.pure': ['text/plain; charset=utf-8', readFileSync(runfileFromEnv('TRADING_MODEL'))],
    '/config.json': ['application/json', JSON.stringify({ url: config.url, psk: config.psk })],
  };
  const site = createServer((req, res) => {
    const f = files[new URL(req.url, 'http://page').pathname]; // portable: an HTTP route, never a file's path
    if (!f) {
      res.writeHead(404);
      res.end();
      return;
    }
    res.writeHead(200, { 'content-type': f[0] });
    res.end(f[1]);
  });
  const at = await new Promise((done) => site.listen(port, '127.0.0.1', () => done(site.address().port)));
  return { url: `http://127.0.0.1:${at}`, close: () => site.close() };
}
