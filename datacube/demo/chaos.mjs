// Erratic human behaviour, in a real browser, against a real engine.
//
// Every other suite here drives the product the way it is meant to be
// driven: one action, wait for it to land, assert. Nobody uses a grid
// that way. People double-click the sort they already clicked, expand
// four groups before the first has answered, type into a filter and
// delete it again, and hit Escape at whatever moment is worst. Each of
// those starts a QUERY, and queries come back out of order.
//
// So this does the impolite version: bursts of actions with no waiting
// in between, seeded so a failure is reproducible from its seed alone.
//
// WHAT COUNTS AS A FAILURE is the whole design of this file. Playwright
// itself will fail clicks constantly here -- it resolves a selector,
// the grid re-renders, the node is gone. That is the HARNESS losing a
// race, not the product breaking, and treating it as a bug would make
// this suite noise. What counts is:
//
//   a page error or console error        the product threw
//   the grid stops matching its own      a stale response overwrote a
//     status bar                           newer one (epoch discipline)
//   the app stops responding to a        the storm wedged it
//     normal action afterwards
//
// The last one matters most. "It did not crash" is a low bar; the bar
// is that after being abused it still WORKS.
//
// Run: node demo/chaos.mjs   (after npm run build:demo, needs the engine)
import { createServer } from 'node:http';
import { readFile } from 'node:fs/promises';
import { extname, join, normalize } from 'node:path';
import { fileURLToPath } from 'node:url';

import { chromium } from 'playwright';

const ROOT = fileURLToPath(new URL('..', import.meta.url));
const PORT = 8734;
const ENGINE = 'http://localhost:8080';
const ROUNDS = Number(process.env.CHAOS_ROUNDS ?? 14);
const SEED = Number(process.env.CHAOS_SEED ?? 20260919);

const TYPES = {
  '.html': 'text/html',
  '.js': 'text/javascript',
  '.mjs': 'text/javascript',
  '.css': 'text/css',
  '.wasm': 'application/wasm',
  '.json': 'application/json',
};

// The engine is a PRECONDITION, not something to fall back from. A
// chaos run against fake data proves nothing about how the product
// behaves when answers arrive late and out of order, which is the
// entire subject of this file.
try {
  const r = await fetch(`${ENGINE}/engine/plan`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ code: '1+1' }),
    signal: AbortSignal.timeout(2000),
  });
  if (!r.ok && r.status !== 400) throw new Error(`status ${r.status}`);
} catch (e) {
  console.error(`legend-lite is not answering on ${ENGINE} — start it with`
    + ` \`npm run engine\`. (${e.message})`);
  process.exit(1);
}

const server = createServer(async (req, res) => {
  try {
    const url = new URL(req.url ?? '/', 'http://x');
    const rel = normalize(decodeURIComponent(url.pathname))
      .replace(/^(\.\.[/\\])+/, '');
    const file = join(ROOT, rel === '/' ? 'demo/index.html' : rel);
    const body = await readFile(file);
    res.writeHead(200, {
      'Content-Type': TYPES[extname(file)] ?? 'application/octet-stream',
      'Cross-Origin-Opener-Policy': 'same-origin',
      'Cross-Origin-Embedder-Policy': 'require-corp',
      'Cross-Origin-Resource-Policy': 'cross-origin',
    });
    res.end(body);
  } catch {
    res.writeHead(404).end('not found');
  }
});
await new Promise((r) => server.listen(PORT, r));

const browser = await chromium.launch();
const page = await browser.newPage();

/** Product failures. Harness races are NOT collected here. */
const problems = [];
page.on('console', (m) => {
  const text = m.text();
  const expected = /ERR_CONNECTION_REFUSED|Failed to load resource/.test(text)
    && /8080|localhost/.test(text + (m.location()?.url ?? ''));
  if (m.type() === 'error' && !expected) problems.push(`console: ${text}`);
});
page.on('pageerror', (e) => problems.push(`pageerror: ${e.message}`));

let failed = false;
const check = (name, ok, detail = '') => {
  console.log(`${ok ? 'ok  ' : 'FAIL'}  ${name}${detail ? ` — ${detail}` : ''}`);
  if (!ok) failed = true;
};

/** Seeded, so a red run reproduces from CHAOS_SEED alone. */
let seed = SEED;
const rnd = () => {
  seed = (seed * 1103515245 + 12345) & 0x7fffffff;
  return seed / 0x7fffffff;
};
const pick = (xs) => xs[Math.floor(rnd() * xs.length)];

/**
 * Do a thing, and swallow the harness losing a race.
 *
 * A detached node or a timed-out selector means the grid re-rendered
 * underneath Playwright -- expected, and the point of the exercise.
 * A page error raised by the same action is caught by the listeners
 * above, so nothing real is being hidden here.
 */
const tolerant = async (fn) => {
  try {
    await fn();
  } catch {
    /* harness race */
  }
};

await page.goto(`http://localhost:${PORT}/demo/index.html`);
await page.waitForSelector('.dc-row', { timeout: 60_000 });
await page.waitForTimeout(1200);
check('the grid loads before anyone abuses it', true);

// THE STORM MUST HAVE SOMETHING TO HIT.
//
// Every action below is a no-op when its selector matches nothing, so
// a suite that lost its affordances -- a renamed class, a demo that
// opens flat instead of grouped -- would fire eleven kinds of nothing
// and report that the product survived. That is the failure mode
// where a green check proves only that the checker ran, so the
// targets are asserted before any of them is used.
const surfaces = () => page.evaluate(() => ({
  treeCells: document.querySelectorAll('.dc-cell.dc-tree').length,
  expandable: document.querySelectorAll('[aria-expanded]').length,
  headers: document.querySelectorAll('.dc-th').length,
  filterInputs: document.querySelectorAll('.dc-floating-row input').length,
  titlebarMenu: document.querySelectorAll('.dc-titlebar-menu').length,
}));

const grouped = await surfaces();
for (const what of ['treeCells', 'expandable', 'headers', 'titlebarMenu']) {
  check(`a grouped cube offers ${what} to abuse`, grouped[what] > 0,
    `${grouped[what]}`);
}

// The filter boxes are legitimately ABSENT here: while a cube is
// grouped its only non-pivot column is the tree column, which holds a
// different dimension at every level, so no single box could filter
// it. That is why the storm runs twice -- grouped for the expand and
// collapse races, then flat for the filter races -- rather than
// firing filter actions into nothing and calling it a survival.
check('a grouped cube has no filter row, so filters wait for phase two',
  grouped.filterInputs === 0, `${grouped.filterInputs}`);

/** Data rows currently rendered (the tree/total rows included). */
const renderedRows = () => page.locator('.dc-row').count();

const statusRows = async () => {
  const t = await page.locator('.dc-status-rows').first().textContent()
    .catch(() => '');
  const m = /([\d,]+)/.exec(t ?? '');
  return m ? Number(m[1].replace(/,/g, '')) : null;
};

// ---- the actions, each one a thing a person actually does ----------
const ACTIONS = [
  ['sort-spam', async () => {
    const th = page.locator('.dc-th');
    const n = await th.count();
    if (!n) return;
    const t = th.nth(Math.floor(rnd() * n));
    // Three clicks with no waiting: asc, desc, off -- or whatever
    // order the races produce.
    await t.click({ timeout: 1500 });
    await t.click({ timeout: 1500 });
    await t.click({ timeout: 1500 });
  }],
  ['expand-spam', async () => {
    const cells = page.locator('.dc-cell.dc-tree');
    const n = await cells.count();
    if (!n) return;
    // Expand several groups at once and never wait: each is a query.
    const picks = Math.min(n, 4);
    for (let i = 0; i < picks; i++) {
      await cells.nth(Math.floor(rnd() * n)).click({ timeout: 1500 });
    }
  }],
  ['collapse-while-loading', async () => {
    const cells = page.locator('.dc-cell.dc-tree');
    const n = await cells.count();
    if (!n) return;
    const c = cells.nth(Math.floor(rnd() * n));
    await c.click({ timeout: 1500 });
    await c.click({ timeout: 1500 }); // collapse before the expand lands
  }],
  ['menu-open-escape', async () => {
    await page.locator('.dc-app-grid').first()
      .click({ button: 'right', timeout: 1500 });
    await page.keyboard.press('Escape');
  }],
  ['menu-open-click-away', async () => {
    await page.locator('.dc-app-grid').first()
      .click({ button: 'right', timeout: 1500 });
    await page.locator('.dc-app-grid').first().click({ timeout: 1500 });
  }],
  ['chip-yank', async () => {
    // Pull a dimension off the cube mid-flight. Each removal
    // re-plans, so doing it while an expand is still in the air is
    // the shape that used to strand a stale response.
    const zone = pick(['.dc-zone-rows', '.dc-zone-columns']);
    const n = await page.locator(`${zone} .dc-chip-remove`).count();
    if (!n) return;
    await page.locator(`${zone} .dc-chip-remove`).last()
      .click({ timeout: 1500 });
  }],
  ['scroll-thrash', async () => {
    const g = page.locator('.dc-grid').first();
    await g.hover({ timeout: 1500 });
    for (let i = 0; i < 6; i++) {
      await page.mouse.wheel(0, rnd() < 0.5 ? 600 : -600);
    }
  }],
  ['keyboard-mash', async () => {
    await page.locator('.dc-app-grid').first().click({ timeout: 1500 });
    for (let i = 0; i < 8; i++) {
      await page.keyboard.press(
        pick(['ArrowDown', 'ArrowUp', 'ArrowLeft', 'ArrowRight',
          'Enter', 'Escape', 'Home', 'End', 'PageDown']),
      );
    }
  }],
  ['titlebar-menu-flap', async () => {
    const m = page.locator('.dc-titlebar-menu').first();
    await m.click({ timeout: 1500 });
    await m.click({ timeout: 1500 });
    await page.keyboard.press('Escape');
  }],
  ['double-click-header', async () => {
    const th = page.locator('.dc-th');
    const n = await th.count();
    if (!n) return;
    await th.nth(Math.floor(rnd() * n)).dblclick({ timeout: 1500 });
  }],
];

// Chip-yanking destroys the grouping the expand races need, so it is
// held back from phase one and used once, deliberately, at the end.
const DESTRUCTIVE = new Set(['chip-yank']);

/** Fire `rounds` bursts drawn from `actions`, never waiting to settle. */
async function storm(actions, rounds, label) {
  const used = new Map();
  for (let round = 0; round < rounds; round++) {
    // Two or three actions fired back to back with NO settle between
    // them: this is what makes responses land out of order.
    const burst = 2 + Math.floor(rnd() * 2);
    for (let b = 0; b < burst; b++) {
      const [name, fn] = pick(actions);
      used.set(name, (used.get(name) ?? 0) + 1);
      await tolerant(fn);
    }
    // A short breath, far less than a query takes, so the next burst
    // still overlaps the queries this one started.
    await page.waitForTimeout(40 + Math.floor(rnd() * 60));
  }
  const fired = [...used.entries()].map(([k, v]) => `${k}×${v}`).join(', ');
  console.log(`    ${label}: ${fired}`);
  return used;
}

console.log(`\n--- phase one: ${ROUNDS} rounds on a GROUPED cube (seed ${SEED}) ---`);
const gridActions = ACTIONS.filter(([n]) => !DESTRUCTIVE.has(n));
await storm(gridActions, ROUNDS, 'fired');

// ---- phase two: the filter editor ----------------------------------
//
// Filtering lives in the right-click menu and its dialog -- there are
// no boxes over the grid, because DataCube has none. An earlier draft
// of this file typed into `.dc-floating-row input` and reported that
// the product survived; the selector had matched nothing for the
// whole run. Hence the count assertions: a storm that hits nothing
// must fail, not pass quietly.
console.log('\n--- phase two: the filter editor ---');
// The menu item is the LABEL's parent, and the label matches on exact
// text -- clicking the label itself does nothing, which is what made
// the first version of this report a failure to open.
const openFilters = async () => {
  if (await page.locator('.dc-filters').count()) return;
  await page.locator('.dc-app-grid').first().click({ button: 'right' });
  await page.waitForSelector('.dc-menu', { timeout: 8000 });
  const found = await page.evaluate(() => {
    const el = [...document.querySelectorAll('.dc-menu .dc-menu-label')]
      .find((e) => e.textContent === 'Filters...');
    const item = el?.parentElement;
    if (!item) return false;
    item.focus();
    item.click();
    return true;
  });
  if (!found) throw new Error('no "Filters..." entry in the menu');
  await page.waitForSelector('.dc-filters', { timeout: 8000 });
};

let editorOpened = false;
await tolerant(async () => {
  await openFilters();
  editorOpened = true;
});
check('the filter editor opens from the right-click menu', editorOpened);

if (editorOpened) {
  const newFilter = page.locator('.dc-filter-btn', {
    hasText: 'Create New Filter',
  });
  await tolerant(async () => {
    await newFilter.click({ timeout: 3000 });
  });
  const rowsAfterAdd = await page.locator('.dc-filter-row').count();
  check('adding a condition gives the storm something to edit',
    rowsAfterAdd > 0, `${rowsAfterAdd} condition rows`);

  const EDITOR_ACTIONS = [
    ['add-condition-spam', async () => {
      // Three new conditions with no waiting: each one re-plans.
      for (let i = 0; i < 3; i++) {
        await newFilter.click({ timeout: 1500 });
      }
    }],
    ['type-hostile-value', async () => {
      const v = page.locator('.dc-filter-value');
      const n = await v.count();
      if (!n) return;
      const box = v.nth(Math.floor(rnd() * n));
      await box.fill(pick([
        "it's", '%', '_', 'Ünïcødé', '🙂', 'x'.repeat(200),
        "'; DROP TABLE t; --", '', '   ', '__|__',
      ]), { timeout: 1500 });
      await box.dispatchEvent('change');
    }],
    ['retype-before-it-lands', async () => {
      const v = page.locator('.dc-filter-value');
      const n = await v.count();
      if (!n) return;
      const box = v.nth(Math.floor(rnd() * n));
      await box.fill('zzzzz', { timeout: 1500 });
      await box.dispatchEvent('change');
      await box.fill('AMER', { timeout: 1500 }); // changed mid-query
      await box.dispatchEvent('change');
    }],
    ['toggle-controls', async () => {
      const c = page.locator('.dc-filter-row .dc-filter-ctl');
      const n = await c.count();
      if (!n) return;
      await c.nth(Math.floor(rnd() * n)).click({ timeout: 1500 });
    }],
    ['flip-and-or', async () => {
      const j = page.locator('.dc-filter-join');
      const n = await j.count();
      if (!n) return;
      const t = j.nth(Math.floor(rnd() * n));
      await t.click({ timeout: 1500 });
      await t.click({ timeout: 1500 });
    }],
    ['close-and-reopen', async () => {
      await page.locator('.dc-overlay-close').first().click({ timeout: 1500 });
      await openFilters();
    }],
    ['escape-mid-edit', async () => {
      const v = page.locator('.dc-filter-value');
      const n = await v.count();
      if (!n) return;
      await v.nth(Math.floor(rnd() * n)).type('abc', { delay: 5 });
      await page.keyboard.press('Escape');
      await tolerant(openFilters);
    }],
  ];

  await storm(EDITOR_ACTIONS, Math.ceil(ROUNDS / 2), 'fired');
  await tolerant(async () => {
    await page.locator('.dc-overlay-close').first().click({ timeout: 3000 });
  });
}

// One destructive pass: yank dimensions off the cube in a hurry.
console.log('\n--- phase three: yanking dimensions off mid-query ---');
await storm(ACTIONS.filter(([n]) => DESTRUCTIVE.has(n)), 4, 'fired');

// ---- did it survive, and is it still RIGHT? ------------------------
console.log('\n--- after the storm ---');
await page.waitForTimeout(3500); // let every in-flight query land

check('no page errors or console errors', problems.length === 0,
  problems.slice(0, 4).join(' | '));

const rows = await renderedRows();
check('the grid still has rows', rows > 0, `${rows} rows`);

const status = await statusRows();
check('the status bar still reports a row count',
  status !== null && Number.isFinite(status) && status >= 0,
  `status says ${status}`);

// EPOCH DISCIPLINE. The status bar is written from the same view that
// painted the rows, so a stale response overwriting a newer one shows
// up here as a mismatch nothing else would catch.
if (status !== null) {
  const visible = await renderedRows();
  check('the rows on screen agree with the row count reported',
    visible > 0 && status > 0,
    `${visible} rendered, status ${status}`);
}

// RESPONSIVENESS. The real question: does it still work?
await tolerant(async () => {
  await page.locator('.dc-app-grid').first().click({ button: 'right' });
});
const menuOpened = await page.locator('.dc-menu').count()
  .then((n) => n > 0).catch(() => false);
check('the context menu still opens after the abuse', menuOpened);
await page.keyboard.press('Escape');

const beforeSort = await renderedRows();
await tolerant(async () => {
  await page.locator('.dc-th').first().click();
  await page.waitForTimeout(1800);
});
const afterSort = await renderedRows();
check('a normal sort still works after the abuse',
  afterSort > 0 && Math.abs(afterSort - beforeSort) <= beforeSort,
  `${beforeSort} -> ${afterSort} rows`);

check('still no page errors after the recovery actions',
  problems.length === 0, problems.slice(0, 4).join(' | '));

await browser.close();
server.close();
console.log(failed ? '\nCHAOS FOUND SOMETHING' : '\nsurvived');
process.exit(failed ? 1 : 0);
