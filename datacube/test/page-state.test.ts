// ONE PAGE STATE (src/page-state.ts; docs/DATACUBE_PAGE_STATE_2026_10_18.md; docs/DATACUBE_LIVE_EDITING_2026_10_24.md
// §3, W4): a page edit is one step of one history, kept as the commands that made it and the ones that take it back
// (page-between.ts), undone and redone by applying them to the page AS IT IS NOW -- told as edits, so the history
// survives another editor's change (an undo past it puts back what the step changed, and that alone), and a step whose
// target is gone is dead: dropped and said, never guessed at; what the page's own owners change kept with no step; the
// ids a page has held never given out again -- a tile a step brings back is a new tile, every step renamed to follow.

import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { applyPageCommand, applyToState as apply, idsOf, type PageCommand, type PageDraft } from '../src/page-commands.ts';
import type { PageDocument } from '../src/page-document.ts';
import { PageState, type PageDead, type PageMoved } from '../src/page-state.ts';
import { CubeRefusal } from '../src/snapshot.ts';
import { corpus } from './fixtures/pages/make.ts';
import { canonical, renamedPage } from './page-same.ts';
import { layoutTiles } from '../src/page-document.ts';

// the saved-page corpus's frame page: grids grid and grid-2 and two charts on sheet-1, grid-2 alone on sheet-2
const trades = (await corpus())['trades.page.json']!;
const start = (page: PageDocument = trades): PageDraft => ({ page, held: idsOf(page) });
const sheets = (s: PageState): string[] => s.draft.page.sheets.map((x) => x.id);
const named = (s: PageState, id: string): string | undefined => s.draft.page.sheets.find((x) => x.id === id)?.name;

/** A move that landed (not dead, not nothing). */
function moved(m: PageMoved | PageDead | undefined): PageMoved {
  assert.ok(m !== undefined, 'a step to move');
  assert.ok('applied' in m, `the step landed, not dead: ${'dead' in m ? m.dead : ''}`);
  return m;
}

describe('a page edit is one step of one history', () => {
  it('commands applied as one step: undo takes them all back, redo does them again, each by its label, each as the commands between', () => {
    const state = new PageState(start());
    assert.equal(state.undoLabel, undefined);
    const applied = apply(state, [{ kind: 'addSheet', name: 'More' }, { kind: 'moveTile', tile: 'chart', sheet: 'sheet-3', columns: null }], 'Move to Sheet');
    assert.deepEqual(applied[0], { kind: 'addSheet', id: 'sheet-3', name: 'More' }, 'the commands as applied: the id given');
    assert.deepEqual(sheets(state), ['sheet-1', 'sheet-2', 'sheet-3']);
    assert.equal(state.undoLabel, 'Move to Sheet');
    const back = moved(state.undo());
    assert.equal(back.label, 'Move to Sheet');
    assert.deepEqual(state.draft.page, trades, 'the page as it was, both commands undone');
    assert.ok(back.applied.some((c) => c.kind === 'removeSheet' && c.sheet === 'sheet-3'), `the undo is the commands between: ${JSON.stringify(back.applied)}`);
    assert.deepEqual(back.renamed, {}, 'nothing brought back: nothing renamed');
    assert.deepEqual([state.undoLabel, state.redoLabel], [undefined, 'Move to Sheet']);
    assert.equal(state.undo(), undefined, 'nothing more to undo');
    // redone: the sheet comes back as a new sheet (B12: sheet-3 was held), the move following it, the rename said
    const again = moved(state.redo());
    assert.deepEqual(again.renamed, { 'sheet-3': 'sheet-4' });
    assert.deepEqual(sheets(state), ['sheet-1', 'sheet-2', 'sheet-4']);
    assert.deepEqual(layoutTiles(state.draft.page.sheets[2]!.layout), ['chart']);
    assert.equal(state.redo(), undefined);
    // and the steps follow the rename: undone again, it is sheet-4 that goes
    const once = moved(state.undo());
    assert.ok(once.applied.some((c) => c.kind === 'removeSheet' && c.sheet === 'sheet-4'), JSON.stringify(once.applied));
    assert.deepEqual(state.draft.page, trades);
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
    // the removal undone brings the chart back as a new chart (B12): the page as it was, up to that id
    const brought = moved(state.undo());
    moved(state.undo());
    assert.deepEqual(canonical(state.draft.page, 'history'), canonical(renamedPage(trades, brought.renamed), 'history'));
  });

  it('an edit its owner made and said: a step of the commands that made it, undone to the page before', () => {
    const state = new PageState(start());
    // the screen moved chart onto sheet-2, then says so: the step is the command, from the page before to the one made
    const before = state.draft;
    const command: PageCommand = { kind: 'moveTile', tile: 'chart', sheet: 'sheet-2', columns: null };
    const made = applyPageCommand(before, command).draft;
    state.step('Move to Sheet', [command], before, made);
    assert.equal(state.draft.page, made.page);
    moved(state.undo());
    assert.deepEqual(state.draft.page, trades);
    assert.deepEqual(moved(state.redo()).applied, [command]);
    assert.deepEqual(state.draft.page, made.page);
  });

  it('what the page\'s own owners change is kept with no step: an undo leaves it in no step\'s way', () => {
    const state = new PageState(start());
    apply(state, [{ kind: 'renameSheet', sheet: 'sheet-1', name: 'One' }], 'Rename Sheet');
    // a grid's own edit reached the page: no step
    const renamed = { ...state.draft.page, name: 'Desk view' };
    state.keep({ page: renamed, held: state.draft.held });
    assert.equal(state.undoLabel, 'Rename Sheet');
    moved(state.undo());
    assert.equal(state.undoLabel, undefined);
    assert.equal(state.draft.page.name, 'Desk view', 'the owner\'s change stands: the undo put back the sheet\'s name alone');
  });

  it('the ids a page has held only grow: a step undone gives its ids back to no one; redone, its tiles come back as new ones', () => {
    const state = new PageState(start());
    apply(state, [{ kind: 'addSheet', name: null }], 'Add Sheet');
    state.undo();
    assert.ok(state.draft.held.includes('sheet-3'), 'sheet-3 was held');
    const [added] = apply(state, [{ kind: 'addSheet', name: null }], 'Add Sheet');
    assert.deepEqual(added, { kind: 'addSheet', id: 'sheet-4', name: null }, 'never sheet-3 again');
    state.undo();
    moved(state.redo());
    assert.deepEqual(sheets(state), ['sheet-1', 'sheet-2', 'sheet-5'], 'redone, a new sheet: sheet-4 was held too (B12)');
    // a page kept by its owner with fewer ids than it has held keeps them all
    state.keep({ page: state.draft.page, held: idsOf(state.draft.page) });
    assert.ok(state.draft.held.includes('sheet-3'));
    assert.ok(state.draft.held.includes('sheet-4'));
  });

  it('keeps as many steps as it is told, the oldest let go', () => {
    const state = new PageState(start(), 2);
    for (const name of ['A', 'B', 'C']) apply(state, [{ kind: 'renameSheet', sheet: 'sheet-1', name }], `Rename to ${name}`);
    assert.equal(moved(state.undo()).label, 'Rename to C');
    assert.equal(moved(state.undo()).label, 'Rename to B');
    assert.equal(state.undo(), undefined, 'the first step let go');
    assert.equal(named(state, 'sheet-1'), 'A');
  });
});

describe("the history survives another editor's change (D19 W4): a step is commands, applied to the page as it is now", () => {
  it("an undo past another editor's change puts back what the step changed, and that alone (set semantics)", () => {
    const state = new PageState(start());
    const was = named(state, 'sheet-1') ?? null;
    apply(state, [{ kind: 'renameSheet', sheet: 'sheet-1', name: 'Mine' }], 'Rename Sheet');
    // the other editor's change arrives, applied by the rules and kept with no step (page-app.ts follow)
    state.keep(applyPageCommand(state.draft, { kind: 'renameSheet', sheet: 'sheet-2', name: 'Theirs' }).draft);
    const back = moved(state.undo());
    assert.deepEqual(back.applied, [{ kind: 'renameSheet', sheet: 'sheet-1', name: was }], 'the undo told as the one command between');
    assert.equal(named(state, 'sheet-1'), was ?? undefined);
    assert.equal(named(state, 'sheet-2'), 'Theirs', "the other editor's change stands");
    moved(state.redo());
    assert.deepEqual([named(state, 'sheet-1'), named(state, 'sheet-2')], ['Mine', 'Theirs']);
  });

  it("a step whose target the other editor took off is dead: dropped and said, in the commands' words", () => {
    const state = new PageState(start());
    apply(state, [{ kind: 'renameTile', tile: 'chart', title: 'Mine' }], 'Rename');
    state.keep(applyPageCommand(state.draft, { kind: 'removeTile', tile: 'chart' }).draft);
    const dead = state.undo();
    assert.ok(dead !== undefined && 'dead' in dead, 'the step is dead');
    assert.equal(dead.label, 'Rename');
    assert.match(dead.dead, /chart/);
    assert.equal(state.undoLabel, undefined, 'dropped');
    assert.equal(state.redoLabel, undefined, 'and not kept to redo either');
    assert.equal(state.draft.page.views.find((v) => v.id === 'chart'), undefined, 'the page as it is: nothing put back');
  });

  it('a tile a step brings back is a new tile (B12): its id given out afresh, every step renamed to follow, the rename said', () => {
    const state = new PageState(start());
    const grid = trades.views.find((v) => v.kind === 'grid')!;
    apply(state, [{ kind: 'removeTile', tile: grid.id }], 'Remove');
    assert.ok(state.reachable.has(grid.id), 'the grid the undo can bring back, by the id it had (its host keeps its maker)');
    const back = moved(state.undo());
    const fresh = back.renamed[grid.id];
    assert.ok(fresh !== undefined && fresh !== grid.id, `given out afresh: ${JSON.stringify(back.renamed)}`);
    const brought = state.draft.page.views.find((v) => v.id === fresh);
    assert.ok(brought && brought.kind === 'grid', 'the grid is back, under its new id');
    assert.equal(state.draft.page.views.find((v) => v.id === grid.id), undefined, 'never under the old one');
    assert.ok(back.applied.some((c) => c.kind === 'addGrid' && c.id === fresh), 'the commands applied name the id given');
    // the steps follow: redone, it is the new tile that goes
    assert.deepEqual(moved(state.redo()).applied, [{ kind: 'removeTile', tile: fresh }]);
    assert.ok(state.reachable.has(fresh), 'and reachable by its new id');
    assert.ok(!state.reachable.has(grid.id), 'the old id is no one\'s');
  });

  // the review's A1 (docs/TIMESERIES_REVIEW_2026_10_25.md): the timeseries brought back named the grid by the id the
  // plan's simulation gave it, renamed again as the plan was applied -- refused under an editor's tag, as every live
  // page has; silently the wrong grid with none
  for (const [what, tag, between] of [['no tag', undefined, false], ["an editor's tag", 'abc12', false], ['a step between', undefined, true]] as const) {
    it(`a sheet of a grid and the timeseries reading it deleted, undone (${what}): the tile reads the grid brought back`, () => {
      const state = new PageState(start());
      state.tag = tag;
      const spec = { sources: [{ alias: 'bydesk', cube: 'grid-2', date: 'traded', key: 'desk' }], series: [{ id: 'A', expr: { kind: 'column', column: 'notional' } }] } as const;
      apply(state, [{ kind: 'addTimeseries', sheet: 'sheet-2', near: 'grid-2', spec, title: null, columns: null }], 'Add Timeseries');
      apply(state, [{ kind: 'removeSheet', sheet: 'sheet-2' }], 'Delete Sheet');
      if (between) apply(state, [{ kind: 'addSheet', name: 'More' }], 'Add Sheet');
      if (between) moved(state.undo());
      moved(state.undo());
      const page = state.draft.page;
      const series = page.views.find((v) => v.kind === 'timeseries');
      assert.ok(series?.kind === 'timeseries', 'the tile is back');
      const shown = new Set(page.views.flatMap((v) => (v.kind === 'grid' ? [v.cube] : [])));
      assert.deepEqual(series.spec.sources.map((s) => shown.has(s.cube)), [true], `its source a grid of the page shows: ${series.spec.sources[0]!.cube}`);
      const sheet = page.sheets.find((s) => layoutTiles(s.layout).includes(series.id))!;
      const beside = layoutTiles(sheet.layout).filter((t) => t !== series.id);
      const grid = page.views.find((v) => v.kind === 'grid' && beside.includes(v.id));
      assert.ok(grid?.kind === 'grid' && grid.cube === series.spec.sources[0]!.cube, 'the grid back beside it, the one it reads');
    });
  }

  it("a rename the keeper tells (a write made again on top): a later move names the tile as it is now", () => {
    const state = new PageState(start());
    apply(state, [{ kind: 'addSheet', name: 'Mine' }], 'Add Sheet');
    // the keeper gave the sheet out afresh (the other editor took sheet-3 meanwhile): the page as the keeper has it kept,
    // and the history told the rename
    const theirs = { ...state.draft.page, sheets: state.draft.page.sheets.map((s) => (s.id === 'sheet-3' ? { ...s, id: 'sheet-7' } : s)) };
    state.rename({ 'sheet-3': 'sheet-7' });
    state.keep({ page: theirs, held: [...state.draft.held, 'sheet-7'] });
    const back = moved(state.undo());
    assert.deepEqual(back.applied, [{ kind: 'removeSheet', sheet: 'sheet-7' }]);
    assert.deepEqual(sheets(state), ['sheet-1', 'sheet-2']);
  });
});
