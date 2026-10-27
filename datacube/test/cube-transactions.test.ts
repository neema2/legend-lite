// Leg B through the APP (docs/DATACUBE_LEG_B_STATE_OWNER_2026_09_28.md): a change is one transaction, whole gestures, history (B1b, B1c, B2). Each test is
// an audit entry reproduced the way a person meets it, over the fixture's gated engine.

import assert from 'node:assert/strict';
import { afterEach, beforeEach, describe, it } from 'node:test';

import { DEFAULT_CONFIGURATION } from '../src/config.ts';
import { gridBetween } from '../src/grid-between.ts';
import { applyGridCommands, type GridCommand } from '../src/grid-commands.ts';
import type { GridDraft } from '../src/grid-edits.ts';
import { stand } from './fake-planner.ts';
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

  it("a group's caret is drawn in the chrome's one set, and turns as it opens (found 4 of docs/DATACUBE_UI_REVIEW_2026_10_24.md)", async () => {
    const mark = (): string | null => chevron('EMEA').querySelector('svg')?.getAttribute('data-icon') ?? null;
    assert.equal(mark(), 'chevronRight');
    assert.equal(chevron('EMEA').textContent, '', 'no glyph typed');
    chevron('EMEA').dispatchEvent(new dom.window.MouseEvent('mousedown', { bubbles: true }));
    chevron('EMEA').dispatchEvent(new dom.window.MouseEvent('click', { bubbles: true }));
    await settle();
    assert.equal(app.tree.isOpen(['EMEA']), true);
    assert.equal(mark(), 'chevronDown');
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
    await app.edit([{ kind: 'configure', patch: { columns: { desk: { pinned: 'left' } } } }], 'settings');
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
    await app.edit([{ kind: 'configure', patch: { showTitleBar: false } }], 'settings');
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
    await app.edit([{ kind: 'configure', patch: { reportTitle: 'renamed' } }], 'settings');
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
      await app.edit([{ kind: 'configure', patch: { showDragZones: !hidden } }], 'settings');
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
    // a grouped column's kind is locked (grid-edits.ts setColumn): the kind change comes with the regroup that frees it,
    // as one edit -- the way a person makes it (Row Groups, then Column Properties > Column Kind)
    void app.edit([{ kind: 'group', rows: ['desk'] }, { kind: 'column', name: 'region', change: { kind: 'measure' } }], 'kind');
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
      await app.edit([{ kind: 'configure', patch: { reportTitle: `title ${i}` } }], 'settings');
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

describe('the commands told are the ones that landed, in the order they landed (One API S1: T1-T4, G13)', () => {
  let told: GridCommand[];
  beforeEach(async () => {
    told = [];
    await remount({ onCommand: (command) => told.push(command), signInAgain: async () => {} });
  });
  const kinds = (): string[] => told.map((c) => c.kind);
  /** The grid as it is now, as its commands edit it. */
  const now = (): GridDraft => ({ snapshot: app.snapshot, config: app.configuration, tree: app.tree });
  /** The commands told, applied to the grid as it was, make the grid on screen: nothing is between the replay and it
   *  (D19 W3: an undo is told as the commands between, as a change is). */
  const replays = (before: GridDraft): void => {
    assert.deepEqual(gridBetween(applyGridCommands(before, told), now()), [], `the commands told (${kinds().join(', ')}) replay to another grid than the screen's`);
  };
  const release = async (): Promise<void> => {
    engine.gate.hold = false;
    engine.gate.releaseAll();
    await settle();
  };
  const twice = { name: 'twice', lambda: stand('$x.notional * 2'), kind: 'measure' as const };

  it('a setting of the column the change in flight adds is no refusal: told after the change that adds it (T1)', async () => {
    engine.gate.hold = true;
    void app.edit([{ kind: 'calculate', level: 'row', column: twice }], 'calculate');
    await settle();
    const pin: GridCommand = { kind: 'configure', patch: { columns: { twice: { pinned: 'left' } } } };
    assert.equal(await app.edit([pin], 'settings'), null, 'the column is on screen: the setting is no refusal');
    assert.deepEqual(statuses.filter(([, kind]) => kind === 'warn'), [], 'nothing said refused');
    assert.deepEqual(kinds(), [], 'nothing landed yet');
    await release();
    assert.equal(app.configuration.columns['twice']?.pinned, 'left');
    assert.deepEqual(told, [{ kind: 'calculate', level: 'row', column: twice }, pin], 'told as they landed: the column, then its setting');
  });

  it('a setting made while a change runs is told again after it, the order the grid took them in (T2)', async () => {
    engine.gate.hold = true;
    // a change of the query (the row limit) that also pins desk right
    const change: GridCommand = { kind: 'configure', patch: { maxRows: 50, columns: { desk: { pinned: 'right' } } } };
    void app.edit([change], 'settings');
    await settle();
    // a setting alone, on top of it: desk pinned left, at once
    const pin: GridCommand = { kind: 'configure', patch: { columns: { desk: { pinned: 'left' } } } };
    await app.edit([pin], 'settings');
    await settle();
    await release();
    assert.equal(app.configuration.columns['desk']?.pinned, 'left', 'the setting made last is the one on screen');
    // replayed in the order told, a host's grid ends where this one did: pinned left, the limit 50
    let pinned: unknown;
    let maxRows: unknown;
    for (const c of told) {
      if (c.kind !== 'configure') continue;
      const p = c.patch as { maxRows?: number; columns?: Record<string, { pinned?: string }> };
      if (p.maxRows !== undefined) maxRows = p.maxRows;
      if (p.columns?.['desk']?.pinned !== undefined) pinned = p.columns['desk'].pinned;
    }
    assert.deepEqual([pinned, maxRows], ['left', 50], `replayed in the order told (${JSON.stringify(told)})`);
  });

  it('a change made while an Undo runs lands with it: told as the commands between the grid before and the one landed, the undo\'s among them (T3; D19 W3)', async () => {
    menu('EMEA', 'Ascending');
    await settle();
    told = [];
    const before = now();
    engine.gate.hold = true;
    void app.undo();
    await settle();
    menu('EMEA', 'Remove Vertical Pivot on region');
    await settle();
    await release();
    assert.deepEqual([app.snapshot.rows, app.snapshot.sorts], [['desk'], []], 'the undo AND the change landed');
    // told the change alone, a host would keep the sort the undo took away: the undo is told too, as the commands between
    assert.deepEqual([...new Set(kinds())].sort(), ['group', 'sorts'], 'the sort undone and the group removed, both told');
    replays(before);
  });

  it('a setting made while an undo runs, then an undo that lands with no query: told as the commands between, the setting never told again on top of it (the S7 confirmation audit G2-1; D19 W3)', async () => {
    const before = now();
    // two steps whose queries differ, the second's query the first's before it (no sort, a sort, none again)
    await app.edit([{ kind: 'sorts', sorts: [{ column: 'region', direction: 'asc' }] }], 'sort');
    await settle();
    await app.edit([{ kind: 'sorts', sorts: [] }], 'sort');
    await settle();
    // an undo whose query runs, held; a setting laid on it, told at once
    engine.gate.hold = true;
    void app.undo();
    await settle();
    const pin: GridCommand = { kind: 'configure', patch: { columns: { desk: { pinned: 'left' } } } };
    await app.edit([pin], 'settings');
    await settle();
    // a second undo: back to the start, whose query is the one on screen -- it lands at once, no query, told as the
    // commands between as any move is
    void app.undo();
    await settle();
    await release();
    replays(before);
    told = [];
    await app.edit([{ kind: 'sorts', sorts: [{ column: 'desk', direction: 'desc' }] }], 'sort');
    await settle();
    assert.deepEqual(kinds(), ['sorts'], 'the next change told alone: the setting was told once, with the move it rode or as the move took it away');
  });

  it("another editor's commands are that editor's own: never told back, the person's made meanwhile are (T4)", async () => {
    engine.gate.hold = true;
    const theirs = app.applyGridCommands([{ kind: 'group', rows: ['desk'] }], 'Python');
    await settle();
    menu('A', 'Ascending');
    await settle();
    await release();
    await theirs;
    assert.deepEqual(app.snapshot.rows, ['desk'], "the other editor's change landed");
    assert.deepEqual(kinds().filter((k) => k === 'group'), [], "the other editor's command is not told back to it");
    assert.ok(kinds().length > 0, "the person's own sort is told");
  });

  it('the state a grid hands out is what it reads, no way to change it (G11; the S1 audit\'s 13)', () => {
    const state = app.state as unknown as Record<string, unknown>;
    assert.deepEqual([state['busy'], state['historyDepth']], [false, { past: 0, future: 0 }]);
    for (const way of ['change', 'undo', 'redo', 'retryRefused', 'refresh', 'cancel']) assert.equal(state[way], undefined, way);
  });

  it("another editor's commands that change nothing are not told back either (the S1 audit's 3)", async () => {
    // the grid grouped by region and desk already: grouping it so again changes nothing
    assert.equal(await app.applyGridCommands([{ kind: 'group', rows: ['region', 'desk'] }], 'Python'), null);
    await settle();
    assert.deepEqual(kinds(), []);
  });

  it('a change refused because the sign-in expired, run again after signing in: its commands told (G13)', async () => {
    engine.expired = true;
    menu('EMEA', 'Ascending');
    await settle();
    assert.deepEqual(app.snapshot.sorts, [], 'refused: not on screen');
    assert.deepEqual(kinds(), [], 'refused: nothing told');
    engine.expired = false;
    const form = dom.window.document.querySelector<HTMLFormElement>('.dc-alert-signin');
    assert.ok(form, 'the cube offers to sign in again');
    form.dispatchEvent(new dom.window.Event('submit', { bubbles: true, cancelable: true }));
    await settle();
    assert.deepEqual(app.snapshot.sorts, [{ column: 'region', direction: 'asc' }], 'run again: the sort landed');
    assert.deepEqual(told, [{ kind: 'sorts', sorts: [{ column: 'region', direction: 'asc' }] }], 'and its command is told, as the sort it was');
  });

  it('run again after signing in, the refused change lands on the grid as it is now, what was made since kept (the S1 audit\'s 8, 9)', async () => {
    engine.expired = true;
    menu('EMEA', 'Ascending');
    await settle();
    engine.expired = false;
    // a setting made after the refusal: the grid as it is now carries it
    const pin: GridCommand = { kind: 'configure', patch: { columns: { desk: { pinned: 'left' } } } };
    await app.edit([pin], 'settings');
    await settle();
    const form = dom.window.document.querySelector<HTMLFormElement>('.dc-alert-signin');
    assert.ok(form, 'the cube offers to sign in again');
    form.dispatchEvent(new dom.window.Event('submit', { bubbles: true, cancelable: true }));
    await settle();
    assert.deepEqual(app.snapshot.sorts, [{ column: 'region', direction: 'asc' }], 'the sort landed');
    assert.equal(app.configuration.columns['desk']?.pinned, 'left', 'and the setting made since is kept');
    assert.deepEqual(kinds(), ['configure', 'sorts'], 'each told as it landed');
  });
});
