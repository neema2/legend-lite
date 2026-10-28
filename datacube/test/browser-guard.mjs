// The browser tests' guard (tools/browser/pinned-chromium.mjs): a page in a browser test cannot reach
// extensions.duckdb.org, so a page that still downloads a DuckDB extension from there fails its test instead of passing
// while that site is up. This fails when a launch stops being guarded -- a Playwright upgrade the guard's wrapper no
// longer reaches, or the guard changed: each of Playwright's launches must meet an unknown host. (A desk with no
// network passes it unguarded; CI and a connected desk do not.)

// first: points Playwright at the Chromium Bazel fetched, and guards its launches
import '../../tools/browser/pinned-chromium.mjs';
import { join } from 'node:path';
import { chromium } from 'playwright';

const EXTENSION = 'https://extensions.duckdb.org/v1.5.4/wasm_eh/icu.duckdb_extension.wasm';
let failed = false;

async function unreachable(how, open) {
  const { page, close } = await open();
  try {
    const said = await page.goto(EXTENSION).then(() => 'it loaded', (e) => e.message);
    if (said.includes('net::ERR_NAME_NOT_RESOLVED')) {
      console.log(`ok   a page of ${how} cannot reach extensions.duckdb.org`);
    } else {
      failed = true;
      console.log(`FAIL a page of ${how} reached extensions.duckdb.org, or failed otherwise: ${said}`);
    }
  } finally {
    await close();
  }
}

await unreachable('chromium.launch()', async () => {
  const browser = await chromium.launch();
  return { page: await browser.newPage(), close: () => browser.close() };
});
await unreachable('chromium.launchPersistentContext()', async () => {
  const context = await chromium.launchPersistentContext(join(process.env.TEST_TMPDIR ?? '.', 'profile'));
  return { page: await context.newPage(), close: () => context.close() };
});
await unreachable('chromium.launchServer()', async () => {
  const server = await chromium.launchServer();
  const browser = await chromium.connect(server.wsEndpoint());
  return { page: await browser.newPage(), close: async () => { await browser.close(); await server.close(); } };
});
if (failed) process.exit(1);
