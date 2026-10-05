// Leg B through the APP (docs/DATACUBE_LEG_B_STATE_OWNER_2026_09_28.md): a cube ends cleanly; late answers never win (B3). Each test is
// an audit entry reproduced the way a person meets it, over the fixture's gated engine.

import assert from 'node:assert/strict';
import { afterEach, beforeEach, describe, it } from 'node:test';

import { app, dom, engine, menu, remount, root, settle, setUp, tearDown } from './cube-fixture.ts';

beforeEach(setUp);
afterEach(tearDown);

describe('lifecycle (B3)', () => {
  it('a disposed cube stops: its query is cancelled and nothing reaches the host after (P2-105)', async () => {
    const heard: string[] = [];
    await remount({ onView: () => heard.push('view'), onChange: () => heard.push('change'),
      onStatus: (text) => heard.push(`status ${text}`) });
    engine.gate.hold = true;
    menu('EMEA', 'Ascending');
    await settle();
    heard.length = 0;
    app.dispose();
    engine.gate.hold = false;
    engine.gate.releaseAll();
    await settle();
    assert.deepEqual(heard, [], 'a late answer reached the host of a cube it no longer has');
    assert.equal(app.state.busy, false, 'the change in flight was cancelled');
    dom.window.document.dispatchEvent(new dom.window.KeyboardEvent('keydown', { key: 'z', ctrlKey: true, bubbles: true }));
    await settle();
    assert.deepEqual(heard, [], 'and its shortcuts are gone');
  });

  it('two drill-throughs: the LATER one is shown, however the answers arrive (P2-131)', async () => {
    const cell = (text: string): HTMLElement => [...root.querySelectorAll<HTMLElement>('.dc-cell')]
      .find((c) => c.textContent?.trim().replace(/^[▸▾]/, '').startsWith(text)) ?? assert.fail(`no cell ${text}`);
    // a cell is focused by a click and ACTIVATED by Enter: that is the drill
    const drill = (text: string): void => {
      cell(text).dispatchEvent(new dom.window.MouseEvent('click', { bubbles: true, detail: 1 }));
      (root.querySelector('.dc-app-grid .dc-grid') ?? root.querySelector('.dc-app-grid') as Element)
        .dispatchEvent(new dom.window.KeyboardEvent('keydown', { key: 'Enter', bubbles: true }));
    };
    engine.gate.hold = true;
    engine.tag = true;
    const before = engine.queries;
    drill('EMEA');
    await settle();
    drill('AMER');
    await settle();
    assert.equal(engine.gate.held.length, 2, `two drills asked (${engine.queries - before} queries)`);
    const second = engine.queries;
    engine.gate.release(1);
    await settle();
    engine.gate.release(0);
    await settle();
    engine.gate.hold = false;
    engine.tag = false;
    const shown = root.querySelector('.dc-drill')?.textContent ?? '';
    assert.match(shown, new RegExp(`\\b${second}\\b`), `the later drill's rows: ${shown}`);
  });

  it('Escape in a text field stays in the field: the window and its draft stay (P2-221)', async () => {
    app.openEditor();
    const win = root.querySelector('[data-window="Properties"]') as HTMLElement;
    assert.ok(win, 'the Properties window');
    ([...win.querySelectorAll<HTMLElement>('.dc-editor-tab')].find((t) => t.textContent === 'General Properties')
      ?? assert.fail('no General tab')).dispatchEvent(new dom.window.MouseEvent('click', { bubbles: true }));
    const field = win.querySelector<HTMLInputElement>('input[type="text"], input:not([type])')
      ?? assert.fail('no text field in the Properties window');
    field.value = 'a draft';
    field.dispatchEvent(new dom.window.KeyboardEvent('keydown', { key: 'Escape', bubbles: true }));
    assert.ok(root.querySelector('[data-window="Properties"]:not([hidden])'), 'the window is still open');
    assert.equal(field.value, 'a draft', 'and the draft with it');
    win.dispatchEvent(new dom.window.KeyboardEvent('keydown', { key: 'Escape', bubbles: true }));
    assert.equal(root.querySelector('[data-window="Properties"]:not([hidden])'), null, 'Escape on the window itself closes it');
  });
});
