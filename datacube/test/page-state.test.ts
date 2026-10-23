// ONE PAGE STATE (src/page-state.ts; docs/DATACUBE_PAGE_STATE_2026_10_18.md): a page edit is one step of one history,
// undone and redone whole -- commands applied here, or an edit its owner made and said -- a new step clearing what was
// undone; what the page's own owners change kept with no step; the ids a page has held never given out again.

import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { tiles } from '../src/layout/bands.ts';
import { applyPageCommand, applyToState as apply, idsOf, type PageDraft } from '../src/page-commands.ts';
import type { PageDocument } from '../src/page-document.ts';
import { PageState } from '../src/page-state.ts';
import { CubeRefusal } from '../src/snapshot.ts';
import { corpus } from './fixtures/pages/make.ts';

// the saved-page corpus's frame page: grids grid and grid-2 and two charts on sheet-1, grid-2 alone on sheet-2
const trades = (await corpus())['trades.page.json']!;
const start = (page: PageDocument = trades): PageDraft => ({ page, held: idsOf(page) });
const sheets = (s: PageState): string[] => s.draft.page.sheets.map((x) => x.id);

describe('a page edit is one step of one history', () => {
  it('commands applied as one step: undo takes them all back, redo does them again, each by its label', () => {
    const state = new PageState(start());
    assert.equal(state.undoLabel, undefined);
    const applied = apply(state, [{ kind: 'addSheet', name: 'More' }, { kind: 'moveTile', tile: 'chart', sheet: 'sheet-3', columns: null }], 'Move to Sheet');
    assert.deepEqual(applied[0], { kind: 'addSheet', id: 'sheet-3', name: 'More' }, 'the commands as applied: the id given');
    assert.deepEqual(sheets(state), ['sheet-1', 'sheet-2', 'sheet-3']);
    assert.equal(state.undoLabel, 'Move to Sheet');
    const back = state.undo()!;
    assert.equal(back.label, 'Move to Sheet');
    assert.deepEqual(back.draft.page, trades, 'the page as it was, both commands undone');
    assert.deepEqual([state.undoLabel, state.redoLabel], [undefined, 'Move to Sheet']);
    assert.equal(state.undo(), undefined, 'nothing more to undo');
    const again = state.redo()!;
    assert.deepEqual(tiles(again.draft.page.sheets[2]!.layout), ['chart']);
    assert.equal(state.redo(), undefined);
  });

  it('a new step after an undo clears what was undone; a refused one changes nothing', () => {
    const state = new PageState(start());
    apply(state, [{ kind: 'renameSheet', sheet: 'sheet-1', name: 'One' }], 'Rename Sheet');
    apply(state, [{ kind: 'renameSheet', sheet: 'sheet-2', name: 'Two' }], 'Rename Sheet');
    state.undo();
    apply(state, [{ kind: 'removeTile', tile: 'chart' }], 'Remove');
    assert.equal(state.redoLabel, undefined, 'the rename undone is not done again');
    const before = state.draft;
    assert.throws(() => apply(state, [{ kind: 'removeSheet', sheet: 'sheet-9' }], 'Delete Sheet'), CubeRefusal);
    assert.equal(state.draft, before);
    assert.equal(state.undoLabel, 'Remove', 'the refused step is no step');
    state.undo();
    state.undo();
    assert.deepEqual(state.draft.page, trades);
  });

  it('an edit its owner made: begun as a step, the page kept as the owner made it, undone to the page before', () => {
    const state = new PageState(start());
    // the screen moved chart onto sheet-2, then says so: the step begun, then the page it made kept
    const made = applyPageCommand(state.draft, { kind: 'moveTile', tile: 'chart', sheet: 'sheet-2', columns: null }).draft;
    state.begin('Move to Sheet');
    state.keep(made);
    assert.equal(state.draft.page, made.page);
    assert.deepEqual(state.undo()!.draft.page, trades);
    assert.equal(state.redo()!.draft.page, made.page);
  });

  it('what the page\'s own owners change is kept with no step: an undo leaves it in no step\'s way', () => {
    const state = new PageState(start());
    apply(state, [{ kind: 'renameSheet', sheet: 'sheet-1', name: 'One' }], 'Rename Sheet');
    // a grid's own edit reached the page: no step
    const renamed = { ...state.draft.page, name: 'Desk view' };
    state.keep({ page: renamed, held: state.draft.held });
    assert.equal(state.undoLabel, 'Rename Sheet');
    state.undo();
    assert.equal(state.undoLabel, undefined);
  });

  it('the ids a page has held only grow: a step undone gives its ids back to no one; redone, its tiles come back', () => {
    const state = new PageState(start());
    apply(state, [{ kind: 'addSheet', name: null }], 'Add Sheet');
    state.undo();
    assert.ok(state.draft.held.includes('sheet-3'), 'sheet-3 was held');
    const [added] = apply(state, [{ kind: 'addSheet', name: null }], 'Add Sheet');
    assert.deepEqual(added, { kind: 'addSheet', id: 'sheet-4', name: null }, 'never sheet-3 again');
    state.undo();
    state.redo();
    assert.deepEqual(sheets(state), ['sheet-1', 'sheet-2', 'sheet-4']);
    // a page kept by its owner with fewer ids than it has held keeps them all
    state.keep({ page: state.draft.page, held: idsOf(state.draft.page) });
    assert.ok(state.draft.held.includes('sheet-3'));
  });

  it('keeps as many steps as it is told, the oldest let go', () => {
    const state = new PageState(start(), 2);
    for (const name of ['A', 'B', 'C']) apply(state, [{ kind: 'renameSheet', sheet: 'sheet-1', name }], `Rename to ${name}`);
    assert.equal(state.undo()!.label, 'Rename to C');
    assert.equal(state.undo()!.label, 'Rename to B');
    assert.equal(state.undo(), undefined, 'the first step let go');
    assert.equal(state.draft.page.sheets[0]!.name, 'A');
  });
});
