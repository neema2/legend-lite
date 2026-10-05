// Leg B through the APP (docs/DATACUBE_LEG_B_STATE_OWNER_2026_09_28.md): while Ad Hoc Analysis is on, the shell acts on it (B5b, B5c). Each test is
// an audit entry reproduced the way a person meets it, over the fixture's gated engine.

import assert from 'node:assert/strict';
import { afterEach, beforeEach, describe, it } from 'node:test';

import { app, dom, engine, menu, root, settle, setUp, tearDown } from './cube-fixture.ts';

beforeEach(setUp);
afterEach(tearDown);

describe('while Ad Hoc Analysis is on, the shell acts on it (B5b)', () => {
  const hamburger = (): HTMLElement[] => {
    (root.querySelector('.dc-titlebar-menu') as HTMLElement).dispatchEvent(new dom.window.MouseEvent('click', { bubbles: true }));
    const items = [...dom.window.document.querySelectorAll<HTMLElement>('.dc-menu .dc-menu-item')];
    (root.querySelector('.dc-titlebar-menu') as HTMLElement).dispatchEvent(new dom.window.MouseEvent('click', { bubbles: true }));
    return items;
  };
  const entry = (label: string): HTMLElement | undefined =>
    hamburger().find((el) => el.querySelector('.dc-menu-label')?.textContent === label);
  const disabled = (label: string): boolean => entry(label)?.getAttribute('aria-disabled') === 'true';
  const enter = async (): Promise<void> => {
    await app.enterAdHoc();
    await settle();
    assert.ok(app.adhoc, 'in Ad Hoc');
  };

  it('the menu\'s Undo and Redo say what AD HOC can undo, not the hidden cube (P2-284)', async () => {
    menu('EMEA', 'Ascending');
    await settle();
    assert.equal(disabled('Undo'), false, 'the cube has a step');
    await enter();
    assert.equal(disabled('Undo'), true, 'Ad Hoc has none yet');
    await (app.adhoc ?? assert.fail('no mode')).session.setOptions({ suppressMissingRows: false });
    assert.equal(disabled('Undo'), false, 'now it has one');
  });

  it('cube-only entries are not offered, and Ctrl-E opens nothing (P2-289)', async () => {
    await enter();
    for (const label of ['Properties...', 'Hide Drag Zones', 'Show Drag Zones']) {
      const e = entry(label);
      if (e) assert.equal(e.getAttribute('aria-disabled'), 'true', `${label} is the cube's`);
    }
    dom.window.document.dispatchEvent(new dom.window.KeyboardEvent('keydown', { key: 'e', ctrlKey: true, bubbles: true }));
    assert.equal(root.querySelector('[data-window="Properties"]'), null, 'Ctrl-E opened the hidden cube\'s Properties');
  });

  it('the status bar is Ad Hoc\'s: no cube Filter link, and Ad Hoc\'s own counts (P2-287, P2-290)', async () => {
    await enter();
    const bar = root.querySelector('.dc-app-stats') as HTMLElement;
    assert.equal(bar.querySelector('.dc-status-filter'), null, 'the cube\'s Filter link under the Ad Hoc grid');
    const view = app.adhoc?.view ?? assert.fail('no Ad Hoc view');
    assert.match(bar.textContent ?? '', new RegExp(`${view.table.rowCount} rows`), `the Ad Hoc counts: ${bar.textContent}`);
  });

  it('Settings reach Ad Hoc: Max History Stack Size and Debug Mode (P2-283)', async () => {
    await enter();
    app.openSettings();
    const input = root.querySelector('[data-setting="dataCube.editor.maxHistoryStackSize"] input') as HTMLInputElement;
    input.value = '10';
    input.dispatchEvent(new dom.window.Event('change'));
    const debug = root.querySelector('[data-setting="dataCube.debugger.enableDebugMode"] input') as HTMLInputElement;
    debug.checked = true;
    debug.dispatchEvent(new dom.window.Event('change'));
    ([...root.querySelectorAll('button')].find((b) => b.textContent === 'OK') ?? assert.fail('no OK')).click();
    const s = (app.adhoc ?? assert.fail('no mode')).session;
    for (let i = 0; i < 12; i += 1) await s.setOptions({ suppressMissingRows: i % 2 === 0 });
    let undos = 0;
    while (s.canUndo && undos < 50) { await s.undo(); undos += 1; }
    assert.equal(undos, 10, 'the Ad Hoc history holds what Settings says');
    const logged: unknown[] = [];
    const was = console.debug;
    console.debug = (...args: unknown[]) => { logged.push(args[0]); };
    try {
      await app.adhoc?.refresh();
      await settle();
    } finally {
      console.debug = was;
    }
    assert.ok(logged.some((l) => String(l).includes('query')), `Ad Hoc queries are logged in Debug Mode: ${logged.join(' | ')}`);
  });

  it('saving from the Cubes window in Ad Hoc says it cannot keep the Ad Hoc layout (P2-288)', async () => {
    await enter();
    assert.match(app.saveRefusal() ?? '', /Ad Hoc/, 'saving would keep the hidden cube and say "saved"');
  });
});

describe('Ad Hoc starts from what is ON SCREEN (B5c, P2-291)', () => {
  it('entering while a filter is still being applied does not carry the pending filter into the session', async () => {
    engine.gate.hold = true;
    menu('EMEA', "Add Filter: region = 'EMEA'");
    await settle();
    assert.ok(app.snapshot.filter, 'the filter is pending');
    void app.enterAdHoc();
    await settle();
    const grid = (app.adhoc ?? assert.fail('no Ad Hoc')).session.grid;
    const pinned = JSON.stringify(grid).includes('EMEA');
    engine.gate.hold = false;
    engine.gate.releaseAll();
    await settle();
    assert.equal(pinned, false, 'EMEA pinned in Ad Hoc from a filter the cube had not accepted');
  });
});
