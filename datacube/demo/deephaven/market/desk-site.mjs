// THE TRADING DESK'S SITE (docs/DEEPHAVEN_TRADER_DEMOS_2026_10_24.md, the trading desk), for the hand tool (desk.mjs)
// and the test (test/deephaven/desk.mjs): DataCube's own site -- the page the tab opens, its bundle and chunks, its
// vendor folder, its projects -- served as //datacube:serve serves it, with its config.json naming the market world's
// Deephaven (page-config.ts `market`: its address, its test key, the one trading model and the desk's own, served beside
// the page). The server itself is started by ../site.mjs's startDeephaven.

import { readFileSync } from 'node:fs';
import { join } from 'node:path';
import { runfileFromEnv } from '../../../../tools/js/runfiles.mts';
import { serve, siteRoot } from '../../harness.mjs';

/** The address of the desk on a served site: DataCube's page, the trading desk opened (its `#example=`). */
export const DESK_PATH = '/demo/index.html#example=trading-desk';

/** Serves DataCube's site on the loopback, at `port` (0: any free one), its config naming the market `config`. */
export async function serveDesk(config, port = 0) {
  const root = siteRoot();
  const own = JSON.parse(readFileSync(join(root, 'demo', 'config.json'), 'utf8'));
  // the site's own settings, a market added: the test server's address and key, the model served beside the page
  const configJson = JSON.stringify({
    ...own, market: { url: config.url, psk: config.psk, model: './trading-model.pure', deskModel: './market.pure' },
  });
  const model = readFileSync(runfileFromEnv('TRADING_MODEL'));
  // the desk's own model: the screens' functions, the options' and the runtime (market.pure)
  const deskModel = readFileSync(runfileFromEnv('MARKET_PURE'));
  const site = await serve(root, {
    port,
    headers: { 'Cache-Control': 'no-cache' },
    route: (req, res, url) => {
      if (url.pathname === '/demo/config.json') { // portable: an HTTP route, never a file's path
        res.writeHead(200, { 'Content-Type': 'application/json', 'Cache-Control': 'no-cache' });
        res.end(configJson);
        return true;
      }
      if (url.pathname === '/demo/trading-model.pure') { // portable: an HTTP route, never a file's path
        res.writeHead(200, { 'Content-Type': 'text/plain; charset=utf-8', 'Cache-Control': 'no-cache' });
        res.end(model);
        return true;
      }
      if (url.pathname === '/demo/market.pure') { // portable: an HTTP route, never a file's path
        res.writeHead(200, { 'Content-Type': 'text/plain; charset=utf-8', 'Cache-Control': 'no-cache' });
        res.end(deskModel);
        return true;
      }
      if (url.pathname === '/') { // portable: an HTTP route, never a file's path
        res.writeHead(302, { Location: DESK_PATH });
        res.end();
        return true;
      }
      return false;
    },
  });
  return { url: site.origin, desk: `${site.origin}${DESK_PATH}`, close: () => site.close() };
}
