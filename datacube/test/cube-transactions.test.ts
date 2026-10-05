// Leg B through the APP (docs/DATACUBE_LEG_B_STATE_OWNER_2026_09_28.md): a change is one transaction, whole gestures, history (B1b, B1c, B2). Each test is
// an audit entry reproduced the way a person meets it, over the fixture's gated engine.

import assert from 'node:assert/strict';
import { afterEach, beforeEach, describe, it } from 'node:test';

import { DEFAULT_CONFIGURATION } from '../src/config.ts';
import { setHeaderDrag } from '../src/ui/pivot-panel.ts';
import {
  app, busy, chevron, dom, engine, groupRow, menu, menuItems, remount, root, settle, setUp, statuses, tearDown, unhandled, zoneChips,
} from './cube-fixture.ts';

beforeEach(setUp);
afterEach(tearDown);

describe('a refused change is not a change (B1b)', () => {
  it('leaves no undo step and nothing of itself behind (P2-100)', async () => {
    engine.gate.fail = 'the engine said no';
    menu('EMEA', 'Ascending');
    await settle();
    assert.deepEqual(app.snapshot.sorts, [], 'the refused sort is gone');
    assert.equal(app.canUndo, false, 'a change that never landed is no step');
    engine.gate.fail = null;
    menu('EMEA', 'Descending');
    await settle();
    await app.undo();
    await settle();
    assert.deepEqual(app.snapshot.sorts, [], 'undo returns to what was on screen, never to the refused sort');
  });

  it('a refused expand leaves the group closed (P2-101)', async () => {
    engine.gate.fail = 'the engine said no';
    chevron('EMEA').dispatchEvent(new dom.window.MouseEvent('mousedown', { bubbles: true }));
    chevron('EMEA').dispatchEvent(new dom.window.MouseEvent('click', { bubbles: true }));
    await settle();
    assert.equal(app.tree.isOpen(['EMEA']), false, 'the tree is as the screen shows it');
  });

  it('a refused zone change puts the zones back (P2-102)', async () => {
    const before = zoneChips();
    engine.gate.fail = 'the engine said no';
    menu('EMEA', 'Remove Vertical Pivot on region');
    await settle();
    assert.deepEqual(app.snapshot.rows, ['region', 'desk']);
    assert.deepEqual(zoneChips(), before, 'the zones show the cube, not the refused layout');
  });

  it('a presentation change runs no query, so nothing can refuse it (P2-114)', async () => {
    const ran = engine.queries;
    engine.gate.fail = 'the engine said no';
    await app.applyConfiguration({ columns: { desk: { pinned: 'left' } } });
    await settle();
    assert.equal(engine.queries, ran, 'no query');
    assert.equal(app.configuration.columns['desk']?.pinned, 'left');
    assert.deepEqual(unhandled, [], 'nothing thrown into the void');
  });
});

describe('a group is SET open or closed, never toggled (B1b)', () => {
  it('two quick clicks on a closed group, while the first is still running, leave it open (P2-127)', async () => {
    engine.gate.hold = true;
    const click = (): void => {
      chevron('EMEA').dispatchEvent(new dom.window.MouseEvent('mousedown', { bubbles: true }));
      chevron('EMEA').dispatchEvent(new dom.window.MouseEvent('click', { bubbles: true }));
    };
    click();
    await settle();
    click(); // the screen still shows EMEA closed: the person asks to OPEN it, again
    await settle();
    engine.gate.hold = false;
    engine.gate.releaseAll();
    await settle();
    assert.equal(app.tree.isOpen(['EMEA']), true);
  });
});

describe('machine re-runs are not steps (B1b)', () => {
  it('opening again (a host re-running the cube) records nothing (P2-99)', async () => {
    await app.open();
    await settle();
    assert.equal(app.canUndo, false);
  });
});

describe('busy is the owner\'s (B1b)', () => {
  it('a superseded run ending does not turn busy off while the newer one runs (P2-104)', async () => {
    engine.gate.hold = true;
    menu('EMEA', 'Ascending');
    await settle();
    const first = engine.gate.held.length;
    assert.ok(first > 0, 'the first change is running');
    menu('EMEA', 'Descending');
    await settle();
    assert.ok(busy());
    for (let i = 0; i < first; i += 1) engine.gate.release(i);
    await settle();
    assert.equal(busy(), true, 'the newer change is still running');
    engine.gate.hold = false;
    engine.gate.releaseAll();
    await settle();
    assert.equal(busy(), false);
  });
});

describe('undo puts back everything it covers (B1b)', () => {
  it('the zones and the title bar come back with the state (P2-108)', async () => {
    const zones = zoneChips();
    menu('EMEA', 'Remove Vertical Pivot on region');
    await settle();
    assert.notDeepEqual(zoneChips(), zones);
    await app.applyConfiguration({ showTitleBar: false });
    await settle();
    assert.equal(root.querySelector('.dc-titlebar')?.classList.contains('dc-collapsed'), true, 'the title bar folds');
    await app.undo();
    await settle();
    assert.equal(root.querySelector('.dc-titlebar')?.classList.contains('dc-collapsed'), false, 'the title bar is back');
    await app.undo();
    await settle();
    assert.deepEqual(app.snapshot.rows, ['region', 'desk']);
    assert.deepEqual(zoneChips(), zones, 'the zones show the state undo went back to');
  });
});

describe('the title bar is rebuilt only when what it shows changes', () => {
  it('a query starting and landing leaves the title bar\'s elements in place', async () => {
    const bar = root.querySelector('.dc-titlebar-menu');
    assert.ok(bar, 'the menu button');
    menu('EMEA', 'Ascending');
    await settle();
    assert.equal(root.querySelector('.dc-titlebar-menu'), bar, 'the same button: not rebuilt');
    await app.applyConfiguration({ reportTitle: 'renamed' });
    assert.notEqual(root.querySelector('.dc-titlebar-menu'), bar, 'rebuilt when its title changed');
  });
});

describe('the shortcuts are registered once (B1b)', () => {
  it('after the title bar is rebuilt, one Ctrl-Z is ONE undo step (P2-220)', async () => {
    menu('EMEA', 'Ascending');
    await settle();
    menu('EMEA', 'Descending');
    await settle();
    // each fold rebuilds the title bar
    for (const hidden of [true, false, true, false]) {
      await app.applyConfiguration({ showDragZones: !hidden });
      await settle();
    }
    dom.window.document.dispatchEvent(new dom.window.KeyboardEvent('keydown', { key: 'z', ctrlKey: true, bubbles: true }));
    await settle();
    assert.deepEqual(app.snapshot.sorts, [{ column: 'region', direction: 'desc' }], 'the sorts were not undone');
    assert.equal(app.configuration.showDragZones, false, 'the last fold was');
  });
});

describe('a whole gesture is one change (B1c)', () => {
  it('a chip dragged from Row Groups to Column Labels is ONE change: refused, it names one (P2-103)', async () => {
    const zones = zoneChips();
    engine.gate.fail = 'the engine said no';
    setHeaderDrag({ column: 'desk', from: 'rows' }, root.querySelector('.dc-zone-rows'));
    (root.querySelector('.dc-app-side .dc-zone-columns') as HTMLElement)
      .dispatchEvent(new dom.window.MouseEvent('drop', { bubbles: true, cancelable: true }));
    await settle();
    assert.deepEqual([app.snapshot.rows, app.snapshot.pivotOn], [['region', 'desk'], []], 'the cube as it was');
    assert.deepEqual(zoneChips(), zones);
    assert.equal(statuses.some(([t]) => /changes were undone/.test(t)), false,
      `one gesture, one change: ${statuses.map(([t]) => t).join(' | ')}`);
    engine.gate.fail = null;
    setHeaderDrag({ column: 'desk', from: 'rows' }, root.querySelector('.dc-zone-rows'));
    (root.querySelector('.dc-app-side .dc-zone-columns') as HTMLElement)
      .dispatchEvent(new dom.window.MouseEvent('drop', { bubbles: true, cancelable: true }));
    await settle();
    assert.deepEqual([app.snapshot.rows, app.snapshot.pivotOn], [['region'], ['desk']]);
    await app.undo();
    await settle();
    assert.deepEqual([app.snapshot.rows, app.snapshot.pivotOn], [['region', 'desk'], []], 'one undo takes the whole move back');
  });
});

describe('what is read off the rows on screen reads the state ON SCREEN (B1c)', () => {
  it('a right-click while a KIND change runs reads the kinds of the rows on screen (P2-110, the menu\'s column facts)', async () => {
    engine.gate.hold = true;
    void app.applyConfiguration({ columns: { region: { kind: 'measure' } } });
    await settle();
    assert.equal(app.state.busy, true, 'the kind change is running');
    const cell = [...root.querySelectorAll<HTMLElement>('.dc-cell')]
      .find((c) => c.textContent?.trim().replace(/^[▸▾]/, '') === 'EMEA') ?? assert.fail('no EMEA');
    cell.dispatchEvent(new dom.window.MouseEvent('contextmenu', { bubbles: true }));
    const labels = menuItems().map((i) => i.querySelector('.dc-menu-label')?.textContent ?? '');
    assert.ok(labels.includes("Add Filter: region = 'EMEA'"),
      `EMEA on screen is still a region group: ${labels.filter((l) => l.startsWith('Add Filter')).join(' | ') || 'no filters offered'}`);
    engine.gate.hold = false;
    engine.gate.releaseAll();
    await settle();
  });

  it('a right-click while a regroup runs names the column the clicked row belongs to (P2-110)', async () => {
    engine.gate.hold = true;
    menu('EMEA', 'Remove Vertical Pivot on region');
    await settle();
    assert.deepEqual(app.snapshot.rows, ['desk'], 'the regroup is pending');
    const cell = [...root.querySelectorAll<HTMLElement>('.dc-cell')]
      .find((c) => c.textContent?.trim().replace(/^[▸▾]/, '') === 'EMEA');
    assert.ok(cell, 'EMEA is still on screen');
    cell.dispatchEvent(new dom.window.MouseEvent('contextmenu', { bubbles: true }));
    const labels = menuItems().map((i) => i.querySelector('.dc-menu-label')?.textContent ?? '');
    assert.ok(labels.includes("Add Filter: region = 'EMEA'"), labels.filter((l) => l.startsWith('Add Filter')).join(' | '));
    engine.gate.hold = false;
    engine.gate.releaseAll();
    await settle();
  });
});

describe('a Properties Apply is ONE transaction, the tree included (P2-169)', () => {
  const overlay = (): HTMLElement => root.querySelector('.dc-app-overlay') as HTMLElement;
  const tab = (name: string): void => {
    ([...overlay().querySelectorAll<HTMLElement>('.dc-editor-tab')].find((b) => b.textContent === name)
      ?? assert.fail(`no tab ${name}`)).dispatchEvent(new dom.window.MouseEvent('click', { bubbles: true }));
  };
  const apply = (): void => {
    ([...overlay().querySelectorAll<HTMLButtonElement>('.dc-editor-footer button')]
      .find((b) => b.textContent === 'Apply') ?? assert.fail('no Apply')).click();
  };
  /** Sort by desk, and flip "Show root aggregation", in ONE Apply. */
  const edit = (): boolean => {
    app.openEditor();
    tab('Sorts');
    const desk = [...overlay().querySelectorAll<HTMLElement>('.dc-pane-available .dc-selector-row')]
      .find((r) => r.dataset['column'] === 'desk') ?? assert.fail('no desk to sort by');
    desk.dispatchEvent(new dom.window.MouseEvent('dblclick', { bubbles: true }));
    tab('General Properties');
    const root$ = [...overlay().querySelectorAll<HTMLElement>('.dc-check')]
      .find((l) => l.textContent === 'Show root aggregation')?.querySelector('input') ?? assert.fail('no root toggle');
    root$.checked = !root$.checked;
    root$.dispatchEvent(new dom.window.Event('change'));
    apply();
    return root$.checked;
  };

  it('lands with every edit: the sort AND the root total (the tree\'s own refresh used to land the old snapshot over the draft)', async () => {
    const showTotals = edit();
    await settle();
    assert.deepEqual(app.snapshot.sorts.map((x) => x.column), ['desk'], 'the draft\'s sort survived');
    assert.equal(app.tree.showTotals, showTotals, 'and the root total changed with it');
    await app.undo();
    await settle();
    assert.deepEqual(app.snapshot.sorts, [], 'one undo takes the whole Apply back');
    assert.equal(app.tree.showTotals, !showTotals);
  });

  it('refused, nothing of it stays: not the sort, not the root total', async () => {
    const before = app.tree.showTotals;
    engine.gate.fail = 'the engine said no';
    edit();
    await settle();
    assert.deepEqual(app.snapshot.sorts, []);
    assert.equal(app.tree.showTotals, before);
    assert.equal(app.configuration.showRootAggregation, before);
  });
});

describe('history through the app (B2)', () => {
  const sort = async (label: 'Ascending' | 'Descending'): Promise<void> => {
    menu('EMEA', label);
    await settle();
  };
  const ctrlZ = (): void => {
    dom.window.document.dispatchEvent(new dom.window.KeyboardEvent('keydown', { key: 'z', ctrlKey: true, bubbles: true }));
  };

  it('a change made while an Undo runs: no mixed state, and undo goes back to where it was made (P2-106)', async () => {
    await sort('Ascending');
    engine.gate.hold = true;
    void app.undo();
    await settle();
    assert.deepEqual(app.snapshot.sorts, [], 'the undo is on screen, pending');
    menu('EMEA', 'Remove Vertical Pivot on region');
    await settle();
    engine.gate.hold = false;
    engine.gate.releaseAll();
    await settle();
    assert.deepEqual([app.snapshot.rows, app.snapshot.sorts], [['desk'], []], 'the undo AND the change landed');
    assert.equal(app.canRedo, false, 'a new change ends the redo branch, as always');
    await app.undo();
    await settle();
    assert.deepEqual([app.snapshot.rows, app.snapshot.sorts], [['region', 'desk'], []],
      'undo returns to the state the change was made on, never to the undone sort');
  });

  it('two quick Ctrl-Z while the first runs are two steps, each to a state that was on screen (P2-107)', async () => {
    await sort('Ascending');
    await sort('Descending');
    engine.gate.hold = true;
    ctrlZ();
    await settle();
    ctrlZ();
    await settle();
    engine.gate.hold = false;
    engine.gate.releaseAll();
    await settle();
    assert.deepEqual(app.snapshot.sorts, [], 'two presses, two steps');
    await app.redo();
    await settle();
    assert.deepEqual(app.snapshot.sorts, [{ column: 'region', direction: 'asc' }], 'redo walks them back in order');
  });

  it('collapsing a group opened by the expand level is a step, and undo opens it again (P2-109)', async () => {
    await remount({ configuration: { ...DEFAULT_CONFIGURATION, initialExpandToLevel: 1 } });
    assert.equal(groupRow('EMEA')?.getAttribute('aria-expanded'), 'true', 'opened by the expand level');
    chevron('EMEA').dispatchEvent(new dom.window.MouseEvent('mousedown', { bubbles: true }));
    chevron('EMEA').dispatchEvent(new dom.window.MouseEvent('click', { bubbles: true }));
    await settle();
    assert.equal(app.tree.isOpen(['EMEA']), false);
    assert.equal(app.canUndo, true, 'a collapse from the expand level is a change');
    await app.undo();
    await settle();
    assert.equal(app.tree.isOpen(['EMEA']), true, 'undone');
  });

  it('Settings > Max History Stack Size takes effect at once', async () => {
    app.openSettings();
    const input = root.querySelector('[data-setting="dataCube.editor.maxHistoryStackSize"] input') as HTMLInputElement;
    input.value = '10';
    input.dispatchEvent(new dom.window.Event('change'));
    ([...root.querySelectorAll('button')].find((b) => b.textContent === 'OK') ?? assert.fail('no OK')).click();
    for (let i = 0; i < 12; i += 1) {
      await app.applyConfiguration({ reportTitle: `title ${i}` });
    }
    assert.equal(app.state.historyDepth.past, 10);
  });

  it('the menu offers Redo only when there is one to take', async () => {
    const redo = (): HTMLElement | undefined => {
      (root.querySelector('.dc-titlebar-menu') as HTMLElement).dispatchEvent(new dom.window.MouseEvent('click', { bubbles: true }));
      const item = [...dom.window.document.querySelectorAll<HTMLElement>('.dc-menu .dc-menu-item')]
        .find((el) => el.querySelector('.dc-menu-label')?.textContent === 'Redo');
      (root.querySelector('.dc-titlebar-menu') as HTMLElement).dispatchEvent(new dom.window.MouseEvent('click', { bubbles: true }));
      return item;
    };
    await sort('Ascending');
    assert.equal(redo()?.getAttribute('aria-disabled'), 'true', 'nothing to redo');
    await app.undo();
    await settle();
    assert.notEqual(redo()?.getAttribute('aria-disabled'), 'true', 'one to redo');
    await sort('Descending');
    assert.equal(redo()?.getAttribute('aria-disabled'), 'true', 'a new change ended it');
  });
});
