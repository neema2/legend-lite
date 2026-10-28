// THE TRADING DESK'S SITE (docs/DEEPHAVEN_TRADER_DEMOS_2026_10_24.md, the trading desk), for the hand tool (desk.mjs)
// and the test (test/deephaven/desk.mjs): DataCube's own site -- the page the tab opens, its bundle and chunks, its
// vendor folder, its projects -- served as //datacube:serve serves it, with its config.json holding the market world's
// Deephaven's test key in its vault and running the desk on its Deephaven context (page-config.ts `vault`, `contexts`),
// the one trading model, the desk's own and its environments served beside the page. The server itself is started by
// ../site.mjs's startDeephaven, at a port of its own, so the environments served are this site's copy of the desk's
// (environments.pure), its Deephaven context at that server's address: a test's input
// (docs/ONE_CONNECTION_DESK_2026_10_28.md D2).

import { readFileSync } from 'node:fs';
import { join } from 'node:path';
import { runfileFromEnv } from '../../../../tools/js/runfiles.mts';
import { serve, siteRoot } from '../../harness.mjs';
import { environmentsAt } from './environments.mjs';

/** The address of the desk on a served site: DataCube's page, the trading desk opened (its `#example=`). */
export const DESK_PATH = '/demo/index.html#example=trading-desk';

/** Serves DataCube's site on the loopback, at `port` (0: any free one), its config naming the market `config`. */
export async function serveDesk(config, port = 0) {
  const root = siteRoot();
  const own = JSON.parse(readFileSync(join(root, 'demo', 'config.json'), 'utf8'));
  // the site's own settings, the test server's key in its vault by the reference the desk's Deephaven connection names,
  // and the desk's data space run on its Deephaven context (docs/ONE_CONNECTION_DESK_2026_10_28.md step 5): the desk's
  // files are served beside the page, its environments this site's copy
  const configJson = JSON.stringify({
    ...own, vault: { 'deephaven.psk': config.psk }, contexts: { 'market::TradingDesk': 'Deephaven' },
  });
  // the desk's environments as this site releases them: its Deephaven context at this test's server
  // (docs/ONE_CONNECTION_DESK_2026_10_28.md D5: the trader screens' version, in the site's releases folder)
  const environments = environmentsAt(readFileSync(runfileFromEnv('ENVIRONMENTS_PURE'), 'utf8'), new URL(config.url));
  const site = await serve(root, {
    port,
    headers: { 'Cache-Control': 'no-cache' },
    route: (req, res, url) => {
      if (url.pathname === '/demo/config.json') { // portable: an HTTP route, never a file's path
        res.writeHead(200, { 'Content-Type': 'application/json', 'Cache-Control': 'no-cache' });
        res.end(configJson);
        return true;
      }
      if (url.pathname === '/demo/releases/trader-screens/environments.pure') { // portable: an HTTP route, never a file's path
        res.writeHead(200, { 'Content-Type': 'text/plain; charset=utf-8', 'Cache-Control': 'no-cache' });
        res.end(environments);
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
