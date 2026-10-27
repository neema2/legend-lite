// Leg B through the APP (docs/DATACUBE_LEG_B_STATE_OWNER_2026_09_28.md): a cube ends cleanly; late answers never win (B3). Each test is
// an audit entry reproduced the way a person meets it, over the fixture's gated engine.

import assert from 'node:assert/strict';
import { afterEach, beforeEach, describe, it } from 'node:test';

import { DEFAULT_CONFIGURATION } from '../src/config.ts';
import { SNAPSHOT, app, dom, engine, menu, remount, root, settle, setUp, statuses, tearDown } from './cube-fixture.ts';

beforeEach(setUp);
afterEach(tearDown);

describe('lifecycle (B3)', () => {
  it("the status bar's chips say what the view shown is, as its readout does, not a change still running (the 2026-10-25 audit)", async () => {
    // a Top N in flight: a repaint of the bar (a note) said "Top 10 by notional" beside the last query's rows, and its x
    // acted on a change not shown yet
    engine.gate.hold = true;
    void app.edit([{ kind: 'topN', topN: { by: 'notional', n: 10 } }], 'Top 10');
    await settle();
    app.note('a note meanwhile');
    assert.equal(root.querySelector('.dc-status-topn'), null, 'not yet: its view has not landed');
    engine.gate.hold = false;
    engine.gate.releaseAll();
    await settle();
    assert.match(root.querySelector('.dc-status-topn')?.textContent ?? '', /^Top 10 by notional/, 'once it has');
  });

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
    // the rows are a grid's (the drill-through window shows them as one): its cells, read apart
    const shown = [...root.querySelectorAll('.dc-drill .dc-cell')].map((c) => c.textContent).join(' ');
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

// A GRID THAT CHECKS BEFORE ITS FIRST VIEW (CubeAppOptions.prepares; the user, 2026-10-26): a saved page's saved query on
// live rows, typed when its grid first reads them (app/sources.ts reopenedSavedQuery) -- a grid on a sheet not shown,
// when its sheet is (opensWhen).
describe('a grid that checks before its first view (prepares)', () => {
  it('nothing answered: checked once its first view is due, before its rows are read; it reads them as it was made', async () => {
    let due!: () => void;
    const opensWhen = new Promise<void>((resolve) => { due = resolve; });
    const checked: number[] = [];
    const before = engine.queries;
    await remount({ opensWhen, prepares: async () => { checked.push(engine.queries - before); return undefined; } });
    assert.deepEqual(checked, [], 'nothing checked before its first view is due');
    assert.equal(engine.queries, before, 'nor read');
    due();
    await settle();
    assert.deepEqual(checked, [0], 'checked once, before its rows were read');
    assert.equal(engine.queries, before + 1);
    assert.deepEqual(app.snapshot.rows, ['region', 'desk']);
    assert.equal(app.canUndo, false);
  });

  it('a start answered: the grid starts there, its notes on its status bar, as no step of its history', async () => {
    const before = engine.queries;
    await remount({ prepares: async () => ({
      start: { snapshot: { ...SNAPSHOT, rows: ['desk'] }, configuration: DEFAULT_CONFIGURATION },
      notes: ['its column trader is gone'],
    }) });
    assert.equal(engine.queries, before + 1, 'one first view, from the start answered');
    assert.deepEqual(app.snapshot.rows, ['desk']);
    assert.equal(app.canUndo, false);
    assert.equal(root.querySelector('.dc-status-note-text')?.textContent, 'its column trader is gone');
  });

  it('a refusal: said as a first query\'s would be, and nothing read', async () => {
    const before = engine.queries;
    statuses.length = 0;
    await remount({ prepares: async () => { throw new Error('the saved query no longer compiles'); } });
    assert.equal(engine.queries, before);
    assert.ok(statuses.some(([text, kind]) => kind === 'error' && text === 'the saved query no longer compiles'), JSON.stringify(statuses));
  });
});
