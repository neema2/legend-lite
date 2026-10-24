// A HARNESS OVER A WAREHOUSE TABLE, LIVE: the same checks a harness runs on a file in the tab's
// DuckDB, run on a table a warehouse serves -- a Postgres catalog's included, where the planner
// writes Postgres SQL and Postgres answers (docs/POSTGRES_DIALECT_HOMEWORK_2026_10_01.md, "the
// DataCube audit").
//
//   WAREHOUSE=http://127.0.0.1:8772 WAREHOUSE_OBJECT=public.trades PORT=8022 \
//     bazel run //datacube:verify_features
//
// WAREHOUSE_USER / WAREHOUSE_PASSWORD (default alice / secret) sign in. The page is served on
// PORT, which the warehouse must allow (`--allow-origin http://127.0.0.1:PORT`). The table should
// hold the harness's own sample (`sampleCsv`, 5,000 rows, seed 20260920) under the sample's
// column names and types, so the checks written against it read the same data. WAREHOUSE_SNAP=1
// snaps the table as soon as it opens: the same checks then run on its copy in the tab's DuckDB.

/** The warehouse to read from, or undefined: the harness opens its file, as always. */
export const WAREHOUSE = process.env.WAREHOUSE
  ? {
    url: process.env.WAREHOUSE,
    user: process.env.WAREHOUSE_USER ?? 'alice',
    password: process.env.WAREHOUSE_PASSWORD ?? 'secret',
    object: process.env.WAREHOUSE_OBJECT ?? 'public.trades',
    /** WAREHOUSE_SNAP=1: snapped as soon as it opens, so every check runs on the copy in the tab. */
    snap: process.env.WAREHOUSE_SNAP === '1',
  }
  : undefined;

/**
 * Open the warehouse's table in place of the page, as a person does: New > Blank Page, Add a data
 * source, Database, sign in (once: the page keeps the session), then the table.
 */
export async function openWarehouseTable(page) {
  if (!WAREHOUSE) throw new Error('no WAREHOUSE');
  if (!(await page.locator('.dc-titlebar-menu').isVisible())) {
    await page.evaluate(() => window.__dataCube.edit([{ kind: 'configure', patch: { showTitleBar: true } }], 'title bar'));
    await page.locator('.dc-titlebar-menu').waitFor({ timeout: 10_000 });
  }
  await page.click('.dc-titlebar-menu');
  await page.locator('.dc-menu .dc-menu-item', { has: page.locator(':scope > .dc-menu-label:text-is("Create")') }).hover();
  await page.locator('.dc-menu .dc-menu-item', { has: page.locator(':scope > .dc-menu-label:text-is("Blank Page")') }).click();
  await page.locator('.dc-blank').waitFor({ timeout: 10_000 });
  await page.click('.dc-blank .dc-primary');
  await page.locator('.dc-picker').waitFor({ timeout: 10_000 });
  await page.locator('.dc-picker-tab[data-section="database"]').click();
  const row = page.locator(`.dc-picker-row[data-object=${JSON.stringify(WAREHOUSE.object)}]`);
  const form = page.locator('.dc-picker-form input');
  await Promise.race([row.waitFor({ timeout: 10_000 }), form.first().waitFor({ timeout: 10_000 })]);
  if (!(await row.count())) {
    await form.nth(0).fill(WAREHOUSE.url);
    await form.nth(1).fill(WAREHOUSE.user);
    await form.nth(2).fill(WAREHOUSE.password);
    await page.click('.dc-picker-form .dc-primary');
    await row.waitFor({ timeout: 30_000 });
  }
  await row.click();
  await page.locator('.dc-picker').waitFor({ state: 'detached', timeout: 60_000 });
  if (WAREHOUSE.snap) {
    // the plane button, as a person snaps: the rows copied into the tab, every query planned for it
    await page.locator('.dc-titlebar-toggle').waitFor({ timeout: 60_000 });
    await page.waitForFunction(() => document.querySelectorAll('.dc-row').length > 0, null, { timeout: 60_000 });
    await page.click('.dc-titlebar-toggle');
    await page.waitForFunction(() => document.querySelector('.dc-titlebar-toggle')?.classList.contains('dc-on')
      && !window.__dataCube.busy, null, { timeout: 60_000 });
  }
}
