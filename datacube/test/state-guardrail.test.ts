// Leg B's guardrail (docs/DATACUBE_LEG_B_STATE_OWNER_2026_09_28.md): the cube's state -- its
// snapshot, configuration, open groups and history -- has ONE owner, `CubeStateOwner`
// (src/cube-state.ts). Before it, the app and the controller each held a copy, each wrote its
// own, and a refusal put back one of them (P2-100). A "cache for convenience" is how a second
// owner comes back, so this holds, mechanically, that nothing else keeps or assigns it.

import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { Sources } from '../../tools/js/runfiles.mts';
import { sites } from './one-api/scan.ts';

// the sources this reads, as the BUILD target declares them (Bazel workplan P3-29)
const SOURCES = new Sources('SOURCES', 'datacube');

const OWNER = 'src/state-owner.ts';
/** The two that held copies: the app and the controller. */
const HOLDERS = ['src/app.ts', 'src/cube.ts'];
/** The cube's state, by the names the holders used for it. */
const STATE = '(snapshot|config|configuration|tree|history|lastState|past|future|view|treeRows)';

const code = (file: string): { line: string; at: number }[] =>
  SOURCES.read(file).split('\n').map((line, i) => ({ line, at: i + 1 }))
    .filter(({ line }) => {
      const t = line.trim();
      return !(t.startsWith('//') || t.startsWith('*') || t.startsWith('/*'));
    });

describe('one owner of the cube\'s state', () => {
  it('the app and the controller declare no field of it (a getter reading the owner is fine)', () => {
    const field = new RegExp(`^\\s*(readonly\\s+)?#${STATE}\\b\\s*[:=;]`);
    const bad = HOLDERS.flatMap((file) => code(file)
      .filter(({ line }) => field.test(line)).map(({ line, at }) => `${file}:${at}: ${line.trim()}`));
    assert.deepEqual(bad, []);
  });

  it('the app and the controller assign none of it', () => {
    // The editors' DRAFTS (the Filter window's condition tree, the Properties draft) and the Ad
    // Hoc session are not the cube's state; B4 and B5 bring them under the same rules.
    const assign = new RegExp(`this\\.#${STATE}\\s*=(?!=)`);
    const bad = HOLDERS.flatMap((file) => code(file)
      .filter(({ line }) => assign.test(line)).map(({ line, at }) => `${file}:${at}: ${line.trim()}`));
    assert.deepEqual(bad, []);
  });

  it('there is one history: the owner\'s', () => {
    const bad = SOURCES.under('src', '.ts').filter((f) => f !== OWNER)
      .flatMap((file) => code(file)
        .filter(({ line }) => /new UndoStack\b|class \w*History\b/.test(line))
        .map(({ line, at }) => `${file}:${at}: ${line.trim()}`));
    assert.deepEqual(bad, []);
  });

  it('scans what it claims to', () => {
    // a guardrail that scans nothing passes for the wrong reason
    for (const file of HOLDERS) assert.ok(code(file).length > 100, file);
    assert.ok(code(OWNER).some(({ line }) => /this\.#committed = /.test(line)), 'the owner does assign it');
  });
});

// S3's guardrail (docs/DATACUBE_ONE_API_PLAN_2026_10_21.md S3, "the board produced by the commands"): a page's state has
// one owner too -- its PageState, changed by the commands' one step (page-commands.ts applyToState) or kept as its owners
// have it (keep: a grid's own edits) -- and the board (page/cube-page.ts) is a view of it: its structure changes only
// where it is put at a page (sync, restore) or draws what no page holds (a chart with no spec yet, a tile a gesture
// made). A
// gesture that moves a tile or a sheet itself is a second owner coming back: each place is pinned here, with why.

/**
 * Every place the board's structure changes, by function, and how many times there (S3 audit's 13: a second place in a
 * pinned function is a new place too): its sheets, its tiles' places and names, its charts' specs, selections and the
 * grids they read, the grids it holds or keeps. Anything else is a gesture changing the page itself.
 */
const BOARD = /(\bboard\.(?:add|remove|setLayout|rename)\(|#newSheet\(|#sheets\.(?:splice|push|sort)\(|#pageLayouts\.(?:set|delete)\(|chart\.conditions = |sheet\.name = |\.name = s\.name|#kept\.(?:set|delete)\(|\.link\.(?:grid|detached) = |delete chart\.link\.detached|#charts\.(?:set|delete|clear)\(|#grids\.(?:set|delete)\(|panel\.setSpec\()/g;
const SYNC = 'the board put at a page';
// 2026-10-22 (S6 D, K11: D7's Show Grid replaced Open in grid and its Update): the editing grid's sites went with it --
// #openEditor's two, #closeEditor's, #keptFor's three, sync's setChartCube query and its Update grid let go -- and the
// #pendingKept pattern; nothing new: Show Grid makes its grid as a gesture does (addGridOver, placed by sync)
const BOARD_SITES: Record<string, string | readonly [string, number]> = {
  'constructor:#newSheet(': "a page's first sheet",
  'constructor:board.setLayout(': "a cube alone's own grid, the whole board",
  'constructor:#pageLayouts.set(': 'its layout, the page\'s',
  'constructor:#grids.set(': "a cube alone's own grid",
  '#newSheet:#newSheet(': '(its own header)',
  '#newSheet:board.setLayout(': "a new sheet's board at bands.ts NEW_SHEET",
  '#newSheet:#pageLayouts.set(': "a new sheet's layout (bands.ts NEW_SHEET)",
  '#newSheet:#sheets.push(': 'a sheet made: by sync, setSheets, or the constructor',
  '#place:board.add(': 'a tile on its board: by sync, restore, a placeholder, a chart with no spec yet',
  '#take:board.remove(': 'a tile off its board: by sync (#drop), a chart with no spec yet',
  '#synced:#newSheet(': SYNC,
  // 2026-10-22 (the S7 audit's P-1): twice -- its selection as the step has it, and a chart the step moved off a grid
  // with a selection (another editor's followGrid), off that grid as the step took it off (ChartSource.unselected)
  '#synced:chart.conditions = ': [SYNC, 2],
  '#synced:board.remove(': `${SYNC}: a tile onto its sheet`,
  '#synced:board.add(': `${SYNC}: a tile onto its sheet`,
  '#synced:board.rename(': SYNC,
  '#synced:panel.setSpec(': SYNC,
  '#synced:#sheets.splice(': SYNC,
  '#synced:#pageLayouts.delete(': SYNC,
  '#synced:.name = s.name': SYNC,
  '#synced:#sheets.sort(': SYNC,
  '#synced:board.setLayout(': SYNC,
  '#synced:#pageLayouts.set(': SYNC,
  '#synced:#charts.clear(': `${SYNC}: the charts in its order`,
  '#synced:#charts.set(': `${SYNC}: the charts in its order`,
  '#synced:#kept.delete(': `${SYNC}: a grid kept for a detached chart, made again as a grid of the page's`,
  '#synced:.link.grid = ': [`${SYNC}: a chart reading the grid the step says (a removal undone, a Show Grid)`, 2],
  '#synced:delete chart.link.detached': [`${SYNC}: a chart reading the grid the step says (a removal undone, a Show Grid)`, 2],
  'keepGrid:#kept.set(': 'a grid kept off the board for a detached chart: by restore and sync',
  '#releaseKept:#kept.delete(': 'a kept grid no chart reads any more, let go',
  'addGridOver:#grids.set(': 'a grid made: by a gesture (placed by sync) or by sync',
  'addGridOver:#grids.delete(': 'a grid whose addition was refused, let go',
  'openChart:#charts.set(': 'a chart made: by a gesture (placed by sync), by restore, or by sync',
  '#dropChart:#charts.delete(': 'a chart off the board as sync drops it',
  '#dropGrid:#grids.delete(': 'a grid off the board as sync drops it',
  '#dropGrid:#charts.delete(': 'a chart following its grid goes with it as sync takes it off (removeTile\'s rule)',
  '#dropGrid:#kept.set(': 'a frozen chart left detached as sync takes its grid off',
  '#dropGrid:.link.grid = ': 'a frozen chart left detached as sync takes its grid off',
  '#dropGrid:.link.detached = ': 'a frozen chart left detached as sync takes its grid off',
  '#dropGrid:chart.conditions = ': 'a frozen chart left detached as sync takes its grid off',
  'dispose:#charts.clear(': 'the board taken down',
  'moveToSheet:board.remove(': "a chart with no spec yet moved: no page holds it",
  'moveToSheet:board.add(': "a chart with no spec yet moved: no page holds it",
  'setSheets:#newSheet(': 'a saved page put back',
  'setSheets:sheet.name = ': 'a saved page put back',
  'setSheets:board.remove(': 'a saved page put back',
  'setSheets:board.add(': 'a saved page put back',
  'setSheets:#sheets.splice(': 'a saved page put back',
  'setSheets:#pageLayouts.delete(': 'a saved page put back',
  'setSheets:#sheets.sort(': 'a saved page put back',
  'restore:board.setLayout(': 'a saved page put back',
  'restore:board.rename(': 'a saved page put back',
  'restore:#pageLayouts.set(': 'a saved page put back',
  '#reconcileGrid:chart.conditions = ': "the screen's reconcile (G8's note): the same rule as the page's, kept by keep (a cube alone's too: F6)",
  'undoLayout:board.setLayout(': 'a layout undone by hand, as a drag: told as its layout, applied, synced',
  'redoLayout:board.setLayout(': 'a layout redone by hand, as a drag: told as its layout, applied, synced',
};

/** Every place a page's state changes, by its owner: the commands' step is page-commands.ts's (applyToState). */
// (dropSteps joined 2026-10-24: a refused batch's steps off the history, the D19 audit's fourth round)
const PAGE_STATE = /(this\.#state(?:\.(?:keep|begin|step|undo|redo|putBack|rename|dropSteps)\(| = )|\bstate\.(?:keep|begin|step)\()/g;
const STATE_SITES: Record<string, Record<string, string>> = {
  'src/page/page-app.ts': {
    'constructor:this.#state = ': 'a page made',
    'clear:this.#state = ': 'a blank page',
    // 2026-10-23 (the user's bulletproofing round: the start offered "Undo Add Grid", which left the page blank): the page
    // as it starts, its first grid made, with no history behind it
    'started:this.#state = ': 'a page as it starts: its first grid made, no step to undo',
    'restore:this.#state = ': 'a saved page put back',
    // (follow:this.#state.keep went 2026-10-24 with PageApp.follow, the middle version: another editor's change reaches
    // the page by the converge's put-back alone, below, its history kept -- docs/DATACUBE_LIVE_EDITING_REDESIGN_2026_10_24.md)
    // 2026-10-24 (D19 W7, docs/DATACUBE_LIVE_EDITING_2026_10_24.md §2): a screen converging on its keeper's page -- put back
    // at the page as the keeper has it, its history kept (live-converge.ts). renamedBy went the same day (the middle
    // version, docs/DATACUBE_LIVE_EDITING_REDESIGN_2026_10_24.md): every id an edit makes is its editor's own, so the
    // keeper renames none and no step follows a rename of the keeper's
    'putBack:this.#state.putBack(': "the keeper's page, put back under the screen (a converge's board), the history kept",
    // 2026-10-24 (the D19 audit's fourth round, docs/DATACUBE_ONE_API_PLAN_2026_10_21.md As built): a refused batch's
    // page steps never made the keeper's page, so their steps come off the history at the put-back (an undo of a refused
    // addition took the OTHER editor's tile under that id off); the pending edits made again after a converge apply by
    // the planned rule (page-between.ts applyPlanned: a fresh id when the keeper gave the planned one out), no step
    'putBack:this.#state.dropSteps(': "a refused batch's steps off the history: they never made the keeper's page",
    'reapply:this.#state.keep(': "the pending edits made again after a converge (their steps are in the history already), by the planned rule: an id given out meanwhile gives way to a fresh one",
    'setTitle:this.#state.keep(': "the page's name, its host's (no step): N1, a page opened named",
    'onChange:this.#state.keep(': "what its owners change (a grid's own edits), no step",
    '#move:this.#state.undo(': 'Undo (S4; D19 W4: the step\'s inverse commands, told)',
    '#move:this.#state.redo(': 'Redo (S4; D19 W4: the step\'s commands again, told)',
    '#move:this.#state.keep(': 'the screen put at a step: its grids made again write their own cubes',
    '#applyFilterCommand:this.#state.keep(': 'the page as its owners have it, before the filter command is applied',
    // 2026-10-22 (docs/DATACUBE_ONE_API_PLAN_2026_10_21.md P15): a model action's record applied by the commands' one
    // step (no step of the history: D5), the page as its owners have it kept first, as before a filter command
    // 2026-10-22 (the S7 audit's H-4, P-5): moved into #applyUnstepped, which a rename and a save (savedAs) apply by too
    '#applyUnstepped:this.#state.keep(': 'the page as its owners have it, before its host\'s commands (a rename, a save, a model action) are applied, no step',
  },
  'src/app.ts': {
    '#keepBoard:state.keep(': "a cube alone's board: what its owners change (its grid's own edits, a view: F6), no step",
  },
};

describe("one owner of a page's state; the board a view of it (S3)", () => {
  it("the board's structure changes only where it is put at a page, or draws what no page holds", () => {
    const found = sites(SOURCES.read('src/page/cube-page.ts'), BOARD);
    assert.deepEqual([...found.keys()].filter((k) => BOARD_SITES[k] === undefined), [],
      'a new place the board changes itself: a gesture tells its commands, the owner applies them, sync puts the board there');
    assert.deepEqual(Object.keys(BOARD_SITES).filter((k) => !found.has(k)), [], 'pinned places that are gone: take them off');
    const count = (k: string): number => { const pin = BOARD_SITES[k]!; return typeof pin === 'string' ? 1 : pin[1]; };
    assert.deepEqual([...found].filter(([k, n]) => BOARD_SITES[k] !== undefined && n !== count(k)).map(([k, n]) => `${k} ${n}x`), [],
      'a pinned place made more (or fewer) times than pinned');
  });

  it("a page's state changes by the commands' one step, or is kept as its owners have it", () => {
    for (const [file, allowed] of Object.entries(STATE_SITES)) {
      const found = sites(SOURCES.read(file), PAGE_STATE);
      assert.deepEqual([...found.keys()].filter((k) => allowed[k] === undefined), [], `${file}: a new place a page's state changes`);
      assert.deepEqual(Object.keys(allowed).filter((k) => !found.has(k)), [], `${file}: pinned places that are gone`);
    }
    // a step begun anywhere but the commands' one step is an edit with no command
    const begun = SOURCES.under('src', '.ts').filter((f) => f !== 'src/page-commands.ts' && f !== 'src/page-state.ts')
      .filter((f) => /\.begin\((?:label|edit\.label|['`])/.test(SOURCES.read(f)) && /PageState|#state|state\.begin/.test(SOURCES.read(f)));
    assert.deepEqual(begun, [], 'a page step begun outside applyToState');
  });
});
