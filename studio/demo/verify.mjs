// //studio:verify_test: Legend Studio end to end in the pinned Chromium (//tools/browser), the whole loop --
// the demo projects published, a workspace on trading compiled in the tab with its dependencies from Depot, an
// element added and saved, a review created and committed onto the project line, a version released --
// twice: with no server at all (level 0: the SDLC and Depot are WebAssembly in the page), and against the model
// home's server over a git repository on disk (level 1; SDLC_SERVER, started by sdlc-client's test helper).
// Screenshots go to the test's undeclared outputs (bazel-testlogs/studio/verify_test/test.outputs/).

import '../../tools/browser/pinned-chromium.mjs';
import { strict as assert } from 'node:assert';
import { mkdirSync } from 'node:fs';
import { createServer } from 'node:http';
import { readFile, stat } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { extname, join, sep } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { chromium } from 'playwright';

import { startSdlcServer } from '../../sdlc-client/test/sdlc-server.ts';

// the site: this package's own files, beside this harness (as datacube/demo/verify-smoke.mjs serves its own)
const ROOT = fileURLToPath(new URL('..', import.meta.url)).replace(/[\\/]$/, '');

const OUT = process.env.TEST_UNDECLARED_OUTPUTS_DIR ?? join(process.env.TEST_TMPDIR ?? tmpdir(), 'studio-verify');
mkdirSync(OUT, { recursive: true });

const TYPES = {
  '.html': 'text/html', '.js': 'text/javascript', '.css': 'text/css', '.wasm': 'application/wasm',
  '.json': 'application/json', '.pure': 'text/plain', '.woff2': 'font/woff2',
};
const site = createServer(async (req, res) => {
  const { pathname } = new URL(req.url ?? '/', 'http://x');
  try {
    const base = pathToFileURL(`${ROOT}${sep}`);
    const file = fileURLToPath(new URL(`.${pathname}`, base));
    if (!file.startsWith(ROOT)) throw new Error('outside');
    await stat(file);
    res.writeHead(200, { 'Content-Type': TYPES[extname(file)] ?? 'application/octet-stream' }).end(await readFile(file));
  } catch {
    res.writeHead(404).end('not found');
  }
});
// port 0: the system's choice, never a guess that collides
const SITE = await new Promise((resolve) => site.listen(0, '127.0.0.1', () => resolve(`http://127.0.0.1:${site.address().port}`)));

const TRADING = 'org.finos.lite.demo:trading';
const PARTY = 'org.finos.lite.demo:party';

async function loop(browser, name, query) {
  const page = await browser.newPage({ viewport: { width: 1440, height: 900 } });
  // generous waits: under a full gate run the machine is loaded, and the in-tab compile and the page's SDLC take
  // longer than Playwright's 30 s default (//studio:verify_test failed only then, 2026-10-05)
  page.setDefaultTimeout(120_000);
  const errors = [];
  page.on('pageerror', (e) => errors.push(e.message));
  const shot = (n) => page.screenshot({ path: join(OUT, `${name}-${n}.png`) });
  // the status bar's problems button says when the compiler is done and how many errors it found (data-state,
  // data-errors): upstream's bar shows icons and counts, not words
  const waitCompiled = () => page.waitForFunction(() => { const b = document.querySelector('[data-testid=problems-count]'); return b?.dataset.state === 'idle' && b.dataset.errors === '0'; }, undefined, { timeout: 120_000 });
  const statusOf = async (id) => ((await page.getByTestId(id).textContent()) ?? '').trim();
  const waitStatus = (id, pattern) => page.waitForFunction(([id, source]) => new RegExp(source).test(document.querySelector(`[data-testid=${id}]`)?.textContent ?? ''), [id, pattern.source], { timeout: 120_000 });
  try {
    await page.goto(`${SITE}/demo/index.html${query}`);
    // 1. the demo projects, published through the SDLC (every release through the compile gate)
    await page.getByTestId('load-demo').click({ timeout: 60_000 });
    await waitStatus('demo-status', /Demo projects published/);
    await page.getByTestId('project-selector').click();
    await page.locator(`[data-testid=project-selector-menu] [data-id="${TRADING}"]`).click();
    await shot('1-projects');
    // 2. a workspace on trading: it compiles in the tab, with its dependencies from Depot
    await page.getByTestId('new-workspace').click();
    await page.locator('.dialog input').fill('dev');
    await shot('1-dialog');
    await page.locator('.dialog .btn-primary').click();
    await page.waitForSelector('[data-testid=explorer] .element');
    await waitCompiled();
    // 3. an element using a type from a dependency, then saved
    await page.getByTestId('new-element').click();
    await page.getByTestId('new-path').fill('demo::trading::Desk');
    await page.locator('.dialog .btn-primary').click();
    await page.locator('.monaco-editor .view-lines').click();
    await page.keyboard.press('ControlOrMeta+A');
    // one input event, as a paste: keystroke by keystroke, Monaco's bracket auto-closing raced the typed '}' (a
    // stray second brace, 2026-10-04)
    await page.keyboard.insertText('// a trading desk, quoting in one currency\nClass demo::trading::Desk\n{\nname: String[1];\nbase: demo::types::Currency[1];\n}\n');
    await waitCompiled();
    // the element search (Ctrl + P, plan A7): 'trade pa' narrows to the association, Enter opens it; then back to Desk
    await page.keyboard.press('ControlOrMeta+P');
    await page.getByTestId('element-search').fill('trade pa');
    assert.deepEqual(await page.locator('[data-testid=element-search-list] .search-modal__item').evaluateAll((els) => els.map((e) => e.dataset.path)),
      ['demo::trading::Trade_Party']);
    await page.keyboard.press('Enter');
    await page.waitForSelector('.tab.active[title="demo::trading::Trade_Party"]');
    await page.keyboard.press('ControlOrMeta+P');
    await page.getByTestId('element-search').fill('desk');
    await page.keyboard.press('Enter');
    await page.waitForSelector('.tab.active[title="demo::trading::Desk"]');
    // a local change's diff (plan A7): the new class against nothing
    await page.locator('[data-activity=changes]').click();
    await page.locator('.diff-item[data-path="demo::trading::Desk"]').click();
    await page.getByTestId('diff-view').locator('.monaco-diff-editor').waitFor();
    await page.waitForFunction(() => (document.querySelector('[data-testid=diff-view] .editor.modified .view-lines')?.textContent ?? '').includes('demo::trading::Desk'));
    await page.getByTestId('diff-close').click();
    await page.locator('[data-activity=explorer]').click();
    // a delete undone before the save (plan A7): Trade removed, then restored from the explorer, unchanged
    await page.locator('[data-testid=explorer] .element[data-path="demo::trading::Trade"]').click();
    await page.getByTestId('delete-element').click();
    await page.locator('.dialog .btn-primary').click();
    await page.locator('[data-testid=explorer] .element.removed[data-path="demo::trading::Trade"]').click();
    await page.waitForSelector('.tab.active[title="demo::trading::Trade"]');
    await waitStatus('changes-count', /^1 unpushed change$/);
    await waitCompiled();
    // whole-project text mode (F8, plan A7): a class added at the end of the text becomes its own element on leaving
    await page.locator('.monaco-editor .view-lines').click();
    await page.keyboard.press('F8');
    await page.getByTestId('text-mode').locator('.monaco-editor').waitFor();
    await page.getByTestId('text-mode').locator('.view-lines').click();
    await page.keyboard.press('ControlOrMeta+End');
    await page.keyboard.insertText('\n###Pure\n// a book of trades\nClass demo::trading::Book\n{\nname: String[1];\n}\n');
    await page.getByTestId('text-mode-leave').click();
    await page.getByTestId('text-mode').waitFor({ state: 'detached' });
    await page.locator('[data-testid=explorer] .element[data-path="demo::trading::Book"]').waitFor();
    await waitStatus('changes-count', /^2 unpushed changes$/);
    // the model importer (F2): the same class pasted with a desk replaces it -- still one Book
    await page.getByTestId('activity-menu').click();
    await page.getByTestId('menu-import').click();
    await page.getByTestId('import-text').fill('Class demo::trading::Book\n{\n  name: String[1];\n  desk: demo::trading::Desk[1];\n}\n');
    await page.locator('.dialog .btn-primary').click();
    await page.waitForFunction(() => document.querySelectorAll('[data-testid=explorer] .element[data-path="demo::trading::Book"]').length === 1);
    await page.locator('[data-testid=explorer] .element[data-path="demo::trading::Book"]').click();
    await page.waitForFunction(() => (document.querySelector('.monaco-editor .view-lines')?.textContent ?? '').includes('desk'));
    await waitCompiled();
    await page.getByTestId('delete-element').click();
    await page.locator('.dialog .btn-primary').click();
    await waitStatus('changes-count', /^1 unpushed change$/);
    // a change discarded (plan A7): Trade_Party edited, then its change discarded from Local Changes
    await page.locator('[data-testid=explorer] .element[data-path="demo::trading::Trade_Party"]').click();
    await page.locator('.monaco-editor .view-lines').click();
    await page.keyboard.press('ControlOrMeta+Home');
    await page.keyboard.insertText('// touched\n');
    await waitStatus('changes-count', /^2 unpushed changes$/);
    await page.locator('[data-activity=changes]').click();
    const touched = page.locator('.diff-item[data-path="demo::trading::Trade_Party"]');
    await touched.hover();
    await touched.getByTestId('discard-change').click();
    await waitStatus('changes-count', /^1 unpushed change$/);
    await page.locator('[data-activity=explorer]').click();
    // rename or move (plan A7): Desk moved to demo::desks::TradingDesk, and back -- its declaration follows
    for (const [from, to] of [['demo::trading::Desk', 'demo::desks::TradingDesk'], ['demo::desks::TradingDesk', 'demo::trading::Desk']]) {
      await page.locator(`[data-testid=explorer] .element[data-path="${from}"]`).click();
      await page.getByTestId('rename-element').click();
      await page.getByTestId('rename-path').fill(to);
      await page.locator('.dialog .btn-primary').click();
      await page.locator(`[data-testid=explorer] .element[data-path="${to}"]`).waitFor();
      await waitCompiled();
    }
    await page.getByTestId('save-status').click();
    await page.locator('.dialog .btn-primary').click();
    await waitStatus('changes-count', /no changes detected/);
    // the status bar's problems counts open upstream's Problems panel
    await page.getByTestId('problems-count').click();
    await page.getByText('No problems have been detected in the workspace.').waitFor();
    await shot('2-saved');
    await page.locator('.panel-group__action[title=Close]').click();
    // 3b. a function, run in the tab (plan A3): the planner writes its SQL, DuckDB here runs it on party's own rows --
    // its Data element, which trading has through its dependency (plan A2)
    await page.getByTestId('new-element').click();
    await page.getByTestId('new-kind').selectOption({ label: 'Function' });
    await page.getByTestId('new-path').fill('demo::trading::parties');
    await page.locator('.dialog .btn-primary').click();
    await page.locator('.monaco-editor .view-lines').click();
    await page.keyboard.press('ControlOrMeta+A');
    await page.keyboard.insertText('// every party, by name: run in the tab\nfunction demo::trading::parties(): meta::pure::metamodel::relation::Relation<Any>[1]\n{\ndemo::party::Party.all()->project(~[name: p | $p.name, country: p | $p.country])->from(demo::party::PartyMapping, demo::party::Runtime)\n}\n');
    await waitCompiled();
    await page.getByTestId('run-function').click();
    await page.waitForFunction(() => !/^Running/.test(document.querySelector('[data-testid=run-status]')?.textContent ?? 'Running'), undefined, { timeout: 120_000 });
    const ran = await statusOf('run-status');
    if (!/^5 rows in \d+ ms/.test(ran)) throw new Error(`the function's run said: ${ran}`);
    assert.ok((await page.getByTestId('run-rows').textContent()).includes('Banque Lumière'), 'the run shows the party rows');
    await shot('2b-ran');
    // 3c. a service, run in the tab: its query, with its mapping and runtime (plan A3)
    await page.getByTestId('new-element').click();
    await page.getByTestId('new-kind').selectOption({ label: 'Service' });
    await page.getByTestId('new-path').fill('demo::trading::PartiesService');
    await page.locator('.dialog .btn-primary').click();
    await page.locator('.monaco-editor .view-lines').click();
    await page.keyboard.press('ControlOrMeta+A');
    await page.keyboard.insertText("###Service\nService demo::trading::PartiesService\n{\npattern: '/trading/parties';\ndocumentation: 'Every party, by name.';\nexecution: Single\n{\nquery: |demo::party::Party.all()->project(~[name: p | $p.name]);\nmapping: demo::party::PartyMapping;\nruntime: demo::party::Runtime;\n}\n}\n");
    await waitCompiled();
    await page.getByTestId('run-function').click();
    await page.waitForFunction(() => !/^Running/.test(document.querySelector('[data-testid=run-status]')?.textContent ?? 'Running'), undefined, { timeout: 120_000 });
    const served = await statusOf('run-status');
    if (!/^5 rows in \d+ ms/.test(served)) throw new Error(`the service's run said: ${served}`);
    // 3f. the service's query in Query's builder (plan A5): opened on its one column, a column added in the form and run
    // there, saved into the service's text (Save Query), and that text run
    await page.getByTestId('open-query-builder').click();
    await page.waitForSelector('[data-testid=query-builder] .q-col', { timeout: 120_000 });
    assert.equal(await page.locator('[data-testid=query-builder] .q-col').count(), 1, "the builder shows the service's one column");
    await page.dblclick("[data-testid=query-builder] .q-node:has-text('Country')");
    await page.locator('[data-testid=query-builder] .q-col').nth(1).waitFor();
    await page.click('[data-testid=query-builder] button.q-run');
    await page.waitForFunction(() => document.querySelector('[data-testid=query-builder] .q-error-box')
      || /(^|\D)5 rows? in \d+ ms/.test(document.querySelector('[data-testid=query-builder] .q-results-bar')?.textContent ?? ''), undefined, { timeout: 120_000 });
    const built = await page.$('[data-testid=query-builder] .q-error-box');
    if (built) throw new Error(`the builder's run on the service's query said: ${await built.textContent()}`);
    await shot('2c-builder');
    await page.getByTestId('builder-keep').click();
    await page.waitForFunction(() => !document.querySelector('[data-testid=query-builder] .q-chip--status'), undefined, { timeout: 60_000 });
    await page.getByTestId('builder-close').click();
    await page.waitForSelector('[data-testid=query-builder]', { state: 'detached' });
    const kept = (await page.locator('.monaco-editor .view-lines').textContent()).replace(/\s+/g, ' ');
    assert.match(kept, /query: \|demo::party::Party\.all\(\).*\$\w+\.country/, `the service's text holds the built query: ${kept}`);
    assert.match(kept, /mapping: demo::party::PartyMapping;/, "the rest of the service's text is as written");
    await waitCompiled();
    // 3c's result is still shown (5 rows too): wait for this run's to replace it
    const before = await page.getByTestId('run-status').elementHandle();
    await page.getByTestId('run-function').click();
    await page.waitForFunction((old) => !old.isConnected
      && !/^Running/.test(document.querySelector('[data-testid=run-status]')?.textContent ?? 'Running'), before, { timeout: 120_000 });
    const rebuilt = await statusOf('run-status');
    if (!/^5 rows in \d+ ms/.test(rebuilt)) throw new Error(`the service's run, its query from the builder, said: ${rebuilt}`);
    assert.equal(await page.getByTestId('run-rows').locator('thead th').count(), 2, 'the run shows the two columns built');
    // 3d. a function with a parameter: Run asks for its value, as Pure, and binds it (plan A3)
    await page.getByTestId('new-element').click();
    await page.getByTestId('new-kind').selectOption({ label: 'Function' });
    await page.getByTestId('new-path').fill('demo::trading::partiesNamed');
    await page.locator('.dialog .btn-primary').click();
    await page.locator('.monaco-editor .view-lines').click();
    await page.keyboard.press('ControlOrMeta+A');
    await page.keyboard.insertText('function demo::trading::partiesNamed(prefix: String[1]): meta::pure::metamodel::relation::Relation<Any>[1]\n{\ndemo::party::Party.all()->filter(p | $p.name->startsWith($prefix))->project(~[name: p | $p.name])->from(demo::party::PartyMapping, demo::party::Runtime)\n}\n');
    await waitCompiled();
    await page.getByTestId('run-function').click();
    await page.locator('.dialog input[data-param=prefix]').fill("'K'");
    await page.locator('.dialog .btn-primary').click();
    await page.waitForFunction(() => !/^Running/.test(document.querySelector('[data-testid=run-status]')?.textContent ?? 'Running'), undefined, { timeout: 120_000 });
    const named = await statusOf('run-status');
    if (!/^1 row in \d+ ms/.test(named)) throw new Error(`the run with prefix 'K' said: ${named}`);
    assert.ok((await page.getByTestId('run-rows').textContent()).includes('Kestrel Partners'), "prefix 'K' finds Kestrel");
    // 3e. the SQL playground: SQL on the tab's DuckDB, the model's own rows loaded (plan A3)
    await page.locator('[data-panel-tab=sql]').click();
    await page.getByTestId('sql-text').fill('SELECT COUNT(*) AS n FROM "PARTY"."PARTY"');
    await page.getByTestId('sql-run').click();
    await page.getByTestId('sql-status').waitFor();
    assert.match(await statusOf('sql-status'), /^1 row in \d+ ms/);
    assert.equal((await page.getByTestId('sql-rows').locator('tbody td').first().textContent())?.trim(), '5');
    // 3g. the Data panel (plan A2): party's table from its test data (5 rows), then from a person's CSV -- which the next
    // run keeps -- then reset to the model's own rows
    const partyTable = page.locator('[data-testid=data-tables] tr[data-table="PARTY.PARTY"]');
    await page.locator('[data-panel-tab=data]').click();
    await partyTable.locator('[data-source=test]').waitFor({ timeout: 60_000 });
    assert.equal((await partyTable.locator('.data-tables__rows').textContent())?.trim(), '5');
    await partyTable.locator('input[type=file]').setInputFiles({ name: 'two-parties.csv', mimeType: 'text/csv',
      buffer: Buffer.from('ID,NAME,COUNTRY\n1,Osprey Fund,NZ\n2,Wattle Capital,AU\n') });
    await partyTable.locator('[data-source=file]').waitFor({ timeout: 60_000 });
    assert.equal((await partyTable.locator('.data-tables__rows').textContent())?.trim(), '2');
    await page.locator('[data-panel-tab=sql]').click();
    await page.getByTestId('sql-run').click();
    await page.waitForFunction(() => document.querySelector('[data-testid=sql-rows] tbody td')?.textContent?.trim() === '2', undefined, { timeout: 60_000 });
    await page.locator('[data-panel-tab=data]').click();
    await partyTable.getByTestId('data-reset').click();
    await partyTable.locator('[data-source=test]').waitFor({ timeout: 60_000 });
    assert.equal((await partyTable.locator('.data-tables__rows').textContent())?.trim(), '5');
    await page.getByTestId('save-status').click();
    await page.locator('.dialog .btn-primary').click();
    await waitStatus('changes-count', /no changes detected/);
    await page.locator('.panel-group__action[title=Close]').click();
    // 4. a review, committed onto the project line (the workspace closes)
    await page.locator('[data-activity=review]').click();
    await page.getByTestId('review-title').fill('Add the desk');
    await page.getByTestId('create-review').click();
    // approvals: approved by this user, the button now revokes
    await page.getByTestId('approve-review').click();
    await page.waitForFunction(() => /^Approved by /.test(document.querySelector('[data-testid=review-approvals]')?.textContent ?? ''));
    assert.equal(await page.getByTestId('approve-review').textContent(), 'Revoke approval');
    // the review's changes (plan A7): what it brings, each opening its diff
    await page.locator('[data-testid=review-changes] .diff-item[data-path="demo::trading::Desk"]').click();
    await page.getByTestId('diff-view').locator('.monaco-diff-editor').waitFor();
    assert.match(await page.locator('[data-testid=diff-view] .diff-view__title').textContent(), /^Desk \(new\)$/);
    await page.getByTestId('diff-close').click();
    await page.getByTestId('commit-review').click({ timeout: 30_000 });
    await page.waitForSelector('[data-testid=new-workspace]', { timeout: 60_000 });
    // 5. a version of the line's head, released through the gate
    await page.getByTestId('new-workspace').click();
    await page.locator('.dialog input').fill('release');
    await page.locator('.dialog .btn-primary').click();
    await page.waitForSelector('[data-testid=explorer] .element');
    assert.ok((await page.getByTestId('explorer').textContent()).includes('Desk'), 'the committed element is on the project line');
    await page.locator('[data-activity=project]').click();
    await page.locator('[data-project-tab=release]').click();
    await page.getByTestId('release-notes').fill('the desk');
    await page.getByTestId('release-minor').click();
    await page.waitForFunction(() => /1\.1\.0/.test(document.querySelector('[data-testid=latest-release]')?.textContent ?? ''), null, { timeout: 60_000 });
    await shot('3-released');
    await page.locator('[data-project-tab=versions]').click();
    await page.getByTestId('versions').waitFor();
    assert.match(await page.getByTestId('versions').textContent(), /1\.1\.0/);
    await page.locator('[data-project-tab=overview]').click();
    await page.getByTestId('dependencies').waitFor();
    assert.match(await page.getByTestId('dependencies').textContent(), /org\.finos\.lite\.demo:party : 1\.0\.0/);
    // upstream's project viewer (plan B5): version 1.1.0 opened read-only from Versions -- its elements, nothing to write
    await page.locator('[data-project-tab=versions]').click();
    await page.locator('[data-testid=versions] [data-version="1.1.0"]').click();
    await waitStatus('viewing', /^1\.1\.0 \(read only\)$/);
    await page.locator('[data-testid=explorer] .element[data-path="demo::trading::Desk"]').click();
    await waitCompiled();
    for (const id of ['new-element', 'save-status', 'rename-element', 'delete-element']) assert.equal(await page.getByTestId(id).count(), 0, `${id} is not offered`);
    assert.equal(await page.locator('[data-activity=changes]').count(), 0, 'no local changes to view');
    // 6. a mapping executed in Query's builder (plan A5): party's own mapping, a query on the class it maps, run in the
    // tab on party's rows; closed with nothing to keep
    const toSetup = async () => {
      await page.getByTestId('activity-menu').click();
      await page.getByTestId('menu-back').click();
    };
    const newPartyWorkspace = async (id) => {
      await page.getByTestId('project-selector').click();
      await page.locator(`[data-testid=project-selector-menu] [data-id="${PARTY}"]`).click();
      await page.getByTestId('new-workspace').click();
      await page.locator('.dialog input').fill(id);
      await page.locator('.dialog .btn-primary').click();
      await page.waitForSelector('[data-testid=explorer] .element');
    };
    await toSetup();
    await newPartyWorkspace('exec');
    // 5b. a workspace updated onto its project line (plan A7): 'memo', made after 'exec', adds a class through a review,
    // so 'exec' is behind the line; Local Changes offers the update, and after it 'exec' has the class
    await toSetup();
    await newPartyWorkspace('memo');
    await page.getByTestId('new-element').click();
    await page.getByTestId('new-path').fill('demo::party::Memo');
    await page.locator('.dialog .btn-primary').click();
    await page.locator('.monaco-editor .view-lines').click();
    await page.keyboard.press('ControlOrMeta+A');
    await page.keyboard.insertText('Class demo::party::Memo\n{\ntext: String[1];\n}\n');
    await waitCompiled();
    await page.getByTestId('save-status').click();
    await page.locator('.dialog .btn-primary').click();
    await waitStatus('changes-count', /no changes detected/);
    await page.locator('[data-activity=review]').click();
    await page.getByTestId('review-title').fill('Add Memo');
    await page.getByTestId('create-review').click();
    await page.getByTestId('commit-review').click({ timeout: 30_000 });
    await page.waitForSelector('[data-testid=new-workspace]', { timeout: 60_000 });
    await page.getByTestId('workspace-selector').click();
    await page.locator('[data-testid=workspace-selector-menu] [data-id="exec"]').click();
    await page.getByTestId('go').click();
    await page.waitForSelector('[data-testid=explorer] .element');
    assert.equal(await page.locator('[data-testid=explorer] .element[data-path="demo::party::Memo"]').count(), 0, 'exec does not have Memo yet');
    await page.locator('[data-activity=changes]').click();
    await page.getByTestId('workspace-outdated').locator('button').click();
    await page.locator('[data-activity=explorer]').click();
    await page.locator('[data-testid=explorer] .element[data-path="demo::party::Memo"]').waitFor({ timeout: 60_000 });
    await page.locator('[data-activity=changes]').click();
    await page.getByTestId('changes').waitFor();
    assert.equal(await page.getByTestId('workspace-outdated').count(), 0, 'exec is up to date');
    // the workspace's history (plan A7): the review's merge newest, then the commit it brought, which created Memo
    await page.locator('[data-activity=project]').click();
    await page.locator('[data-project-tab=history]').click();
    const revisions = page.locator('[data-testid=revisions] .revision-item');
    await revisions.first().waitFor();
    assert.match(await revisions.first().textContent(), /^Add Memo \[review\]/);
    await revisions.nth(1).click();
    await page.locator('[data-testid=revision-changes] .diff-item[data-path="demo::party::Memo"]').click();
    assert.match(await page.locator('[data-testid=diff-view] .diff-view__title').textContent(), /^Memo \(new\)$/);
    await page.getByTestId('diff-close').click();
    // 5c. a conflict resolved (plan A7): exec gives Memo pages, memo2 gives it an author and lands first; exec's update
    // conflicts, Memo is resolved to both in the merge view, and the resolution accepted
    const writeMemo = async (fields) => {
      await page.locator('[data-activity=explorer]').click();
      await page.locator('[data-testid=explorer] .element[data-path="demo::party::Memo"]').click();
      await page.locator('.monaco-editor .view-lines').click();
      await page.keyboard.press('ControlOrMeta+A');
      await page.keyboard.insertText(`Class demo::party::Memo\n{\ntext: String[1];\n${fields}\n}\n`);
      await waitCompiled();
      await page.getByTestId('save-status').click();
      await page.locator('.dialog .btn-primary').click();
      await waitStatus('changes-count', /no changes detected/);
    };
    await writeMemo('pages: Integer[1];');
    await toSetup();
    await newPartyWorkspace('memo2');
    await writeMemo('author: String[1];');
    await page.locator('[data-activity=review]').click();
    await page.getByTestId('review-title').fill('Memo author');
    await page.getByTestId('create-review').click();
    await page.getByTestId('commit-review').click({ timeout: 30_000 });
    await page.waitForSelector('[data-testid=new-workspace]', { timeout: 60_000 });
    await page.getByTestId('workspace-selector').click();
    await page.locator('[data-testid=workspace-selector-menu] [data-id="exec"]').click();
    await page.getByTestId('go').click();
    await page.waitForSelector('[data-testid=explorer] .element');
    await page.locator('[data-activity=changes]').click();
    await page.getByTestId('workspace-outdated').locator('button').click();
    await page.locator('[data-testid=conflicts] .diff-item[data-path="demo::party::Memo"]').click();
    await page.getByTestId('merge-view').locator('.monaco-diff-editor').waitFor();
    await page.locator('[data-testid=merge-view] .editor.modified .view-lines').click();
    await page.keyboard.press('ControlOrMeta+A');
    await page.keyboard.insertText('Class demo::party::Memo\n{\n  text: String[1];\n  author: String[1];\n  pages: Integer[1];\n}\n');
    await page.getByTestId('merge-use').click();
    await page.getByTestId('accept-resolution').click();
    await page.getByTestId('conflict-resolution').waitFor({ state: 'detached', timeout: 60_000 });
    await page.locator('[data-activity=explorer]').click();
    await page.locator('[data-testid=explorer] .element[data-path="demo::party::Memo"]').click();
    await page.waitForFunction(() => /author.*pages/s.test(document.querySelector('.monaco-editor .view-lines')?.textContent ?? ''));
    await waitCompiled();
    await page.locator('[data-activity=explorer]').click();
    await page.locator('[data-testid=explorer] .element[data-path="demo::party::PartyMapping"]').click();
    await waitCompiled();
    await page.getByTestId('open-query-builder').click();
    await page.waitForSelector('[data-testid=query-builder] .q-node', { timeout: 120_000 });
    assert.match(await page.locator('[data-testid=query-builder] .q-builder__title').textContent(), /^Mapping execution: demo::party::PartyMapping$/);
    for (const p of ['Name', 'Country']) await page.dblclick(`[data-testid=query-builder] .q-node:has-text('${p}')`);
    await page.click('[data-testid=query-builder] button.q-run');
    await page.waitForFunction(() => document.querySelector('[data-testid=query-builder] .q-error-box')
      || /(^|\D)5 rows? in \d+ ms/.test(document.querySelector('[data-testid=query-builder] .q-results-bar')?.textContent ?? ''), undefined, { timeout: 120_000 });
    const executed = await page.$('[data-testid=query-builder] .q-error-box');
    if (executed) throw new Error(`the mapping's execution said: ${await executed.textContent()}`);
    assert.equal(await page.locator('[data-testid=query-builder] [data-testid=builder-keep]').count(), 0, 'a mapping execution keeps nothing');
    await page.getByTestId('builder-close').click();
    await page.waitForSelector('[data-testid=query-builder]', { state: 'detached' });
    // the activity bar's sun/moon switch: upstream's default-light, kept, and back
    await page.getByTestId('theme-toggle').click();
    assert.equal(await page.evaluate(() => document.documentElement.dataset.theme), 'default-light');
    await shot('4-light');
    await page.getByTestId('theme-toggle').click();
    assert.equal(await page.evaluate(() => document.documentElement.dataset.theme), undefined);
    assert.deepEqual(errors, []);
    console.log(`${name}: the loop passed (${await page.getByTestId('problems-count').getAttribute('data-errors')} errors)`);
  } catch (e) {
    await shot('failed');
    throw new Error(`${name}: ${e.message}\npage errors: ${errors.join('\n')}`);
  } finally {
    await page.close();
  }
}

const browser = await chromium.launch();
let home;
try {
  // level 0: nothing but the page (each browser context has its own IndexedDB)
  await loop(await browser.newContext().then((c) => ({ newPage: (o) => c.newPage(o) })), 'page', '');
  // level 1: the model home over HTTP, a git repository on disk
  home = await startSdlcServer('studio-verify');
  await loop(await browser.newContext().then((c) => ({ newPage: (o) => c.newPage(o) })), 'server',
    `?sdlc=${encodeURIComponent(`${home.base}/sdlc/api`)}`);
  console.log(`studio verify: passed; screenshots in ${OUT}`);
} finally {
  await browser.close();
  site.close();
  await home?.stop();
}
