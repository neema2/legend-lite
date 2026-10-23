// THE ONE API LEDGERS, STATIC HALF (docs/DATACUBE_ONE_API_PLAN_2026_10_21.md §3). Every known place where the tenet --
// every change to a saved page is a command, applied by one function, and the browser and Python do the same -- does not
// hold yet is listed here by its item in the plan's register (§4), and only there. A new one fails the build; a listed
// one that is gone fails it too, until its item is closed and its entry taken off; and each ledger's size is pinned, so
// growing one is an edit of its ceiling, made only with a dated note (the plan's §1.1, AGENTS.md's guard-file rule).
// The register itself is held to every ledger (the last test): an open item is in some ledger, a closed one in none,
// and no item leaves the register without being closed.
//
//   L1  what changes a grid's state; who reaches the grid's public ways in (its callers across the app); who calls each
//       listed gap; the screen's Snap; the host's own writes of the page -- a write counted only where its function
//       tells no command, so the fix (a told command) is what takes it off
//   L8  every command kind against the screen, Python, the kind lists, both readers, the rebase's renaming; every
//       source kind against what Python reads and adds; the headless exports Python never asks
//   L10 (the browser's half) where the two sides keep different rules, and the gaps no scan reaches: each pinned by the
//       code that is the gap (a marker), so fixing it fails here until its item is closed
//   L11 screen code builds no command (S8): none written out in a screen file; pageCommand only for the person's input;
//       every function that makes commands the screen calls asked by headless too, or the screen's own, said why
//   L12 every place screen code branches on its host (D23): listed with what the other host does there -- its own path
//       to the same thing, or why it has none -- so a thing that works on one host only is decided, never missed
//
// The behavioural half is one-api-gestures.test.ts (L2, L3); Python's is python/tests/test_one_api.py (L4-L7, L9, L10).

import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { Sources } from '../../tools/js/runfiles.mts';
import { DEFAULT_CONFIGURATION } from '../src/config.ts';
import { configurationOf, cubeToJson, readCube, treeOf, writeCube } from '../src/cube-document.ts';
import type { CubeSnapshot } from '../src/snapshot.ts';
import { TreeState } from '../src/tree.ts';
import {
  codeLines, enclosingAt, exportedFunctions, functionBody, heldTo, listed, pythonCode, sites, unionKinds, unionTypes,
} from './one-api/scan.ts';

const SOURCES = new Sources('SOURCES', 'datacube');

const PLAN = '../docs/DATACUBE_ONE_API_PLAN_2026_10_21.md';
const PY = '../python/legend_lite';

/** A gap, by the register's items; `count` where one place holds it more than once. An entry with no item is allowed
 *  for good, with `why` (the command path itself, its plumbing, following another editor, a named exception N1-N3). */
interface Entry {
  readonly items: readonly string[];
  readonly count?: number;
  readonly why?: string;
}

/** Each ledger's size, pinned: it shrinks with each fix (lower it then); it grows only with a dated note here. */
// 2026-10-22 (docs/DATACUBE_ONE_API_PLAN_2026_10_21.md D10, the user's sign-off): MARKERS 9 -> 10 -- S1's audit finding
// 10 registered as P15, pinned by its code
// 2026-10-22 (S5, B3/B9, D11): L1_HOST 10 -> 8, MARKERS 5 -> 4 -- one unnamed new page (PageApp.clear: newPage) names
// itself, no host's setTitle(''); one name (the page's, no host's current.name)
// 2026-10-22 (S5, X1-X4, D14): L8_SOURCES 5 -> 3, MARKERS 4 -> 3 -- a file and a remote file read and added on a Python
// page (host-tables.ts leafOnHost; page.grid(file=, remote=)), and kept by the keeper while a page reads them (pageReads)
// 2026-10-22 (S5, X1 X2 X5 B7, D13): MARKERS 3 -> 2 -- every source kind read on a Python page (host-tables.ts leafOnHost:
// a class through its model home, a saved query through its depot, a warehouse table under the engine's session), its
// rows where the host's engine reads them; the refusal of the rest gone with them
// 2026-10-22 (S5, B1): MARKERS 2 -> 1 -- a page's filters reach Python's view, rows and export (headless grid.ts lensOf:
// page-filters.ts gridLens, the screen's own rule)
// 2026-10-22 (S5, B11): MARKERS 1 -> 0, L8_UNASKED 2 -> 1 -- the tab's Join… on a Python page, each of its window's asks
// answered by Python over its tables (engine.py join.json: headless joinSides, joinQuestions, joinCube; join-flow.ts
// hostJoinFlow), tested by host-join-flow.test.ts and test_page.py Joined
// 2026-10-22 (S6 B: K1 K2 K8 K10 K12 Z19, D21's names): L8_KINDS 11 -> 6, L8_UNASKED 1 -> 0, UNCHECKED 7 -> 4 -- Python
// adds a condition (grid.add_filter: headless gridAddFilter), moves and removes a sheet (sheet.move, page.remove(sheet)),
// sets and takes off a page filter (page.filter: headless pageFilter, page.remove_filter); replaces and clears sorts
// (grid.sorts, grid.clear_sort: a clearSort command the menu's Clear Sort builds too), copies a grid (grid.copy: headless
// gridCopied, cube-document.ts cubeCopied, the screen's copy held the same by one-api-gestures.test.ts) and opens a
// saved page by its id (ll.Page.open: headless pageOfRecord, page-document.ts savedPage, the tab's Open… rule); tested by
// test_page.py Abilities and OverClasses
// 2026-10-22 (S6 C: K4 K6): UNCHECKED 4 -> 2 -- Python adds any column the column editor makes (grid.window,
// child_aggregate, explode, calculate(level='group'): headless gridColumn over column-draft.ts), the header menu's
// presets by the one rule the menu now asks too (grid.grain, show_as, bins: grid-presets.ts presetCommands), and uses and
// drills a named dimension (grid.dimension, drill, drill_up: dimensions.ts); tested by test_page.py Abilities and
// grid-presets.test.ts
// 2026-10-22 (S6 D: K7 K9, D8's names): L8_KINDS 6 -> 4, UNCHECKED 2 -> 1 -- Python selects a chart's mark named by its
// value across and its series' (chart.select: headless chartPick, the click's own rules -- chart-spec.ts namedMark and
// markConditions, page-filters.ts classPick, which the screen's click now asks too -- as the grid's refilter and the
// page's select), and drills a chart down and back up (chart.drill, drill_up: headless chartDrill, chart-spec.ts
// drillDown and drillUp, as setChart); tested by test_page.py Abilities and OverClasses, and one-api-gestures.test.ts
// (the screen's click told as chartPick makes it)
// 2026-10-22 (S6 D: K11, D7's Show Grid): L8_KINDS 4 -> 3, UNCHECKED 1 -> 0 -- Open in grid and its Update are gone,
// and setChartCube with them; Show Grid makes a frozen or detached chart's grid of its own grouping by chart-spec.ts
// chartGrid -- the screen's gesture (addGrid, followGrid, setChart) and Python's chart.grid() (headless chartOwnGrid) by
// the same rule; the telling pattern below names followGrid in setChartCube's place; tested by test_page.py Abilities,
// one-api-gestures.test.ts (four Show Grid gestures), page-commands.test.ts (followGrid)
// 2026-10-22 (S6 E: X4): L8_SOURCES 3 -> 0 -- Python adds a grid over a class (page.grid(cls=...)), a saved query
// (page.grid(query=...)) and a warehouse table (page.grid(warehouse=..., table=..., catalog=...)) as the tab adds one,
// by headless sourceCube (givenSource: class-source.ts classAt, query-store sharedPart; host-tables.ts leafOnHost);
// tested by test_page.py OverClasses, OverWarehouse, OverSavedQueries and PublishedQueries (F33, end to end)
// 2026-10-22 (S7, the final audit's H-10, G-1, P-1, Y-9): no ceiling moves; the scans see more -- a host write whose
// function's own lines tell nothing (scan.ts ownBodyAt: an inner function that tells no longer hides it), any assignment
// of the host's `current`, an optional call of a listed gap (`blankPage?.(`), savedAs as a told command; the grid's
// state changed through an alias of its owner refused outright; the grid's refreshes (the same state run again) said
// why they are not listed; the reasons of entries the audit found worded wrong, corrected
// 2026-10-22 (S7, the user's ask: "why are clickSort exempt? ... a single sort API that UI actions call that also can be
// called programmatically"): L8_KINDS 3 -> 1 -- a header's sort click is a command made by one rule (header-sort.ts
// headerSortCommand) the screen's click and Python's grid.toggle_sort / toggle_group_sort ask (headless gridHeaderSort);
// the "nature" entries were wrong. F71. And L8_KINDS 1 -> 0: Python's measure is made by the steps a person makes it by
// (grid-measures.ts measureCommands: `column`, or `calculate` and `column`), the `measures` kind deleted -- a page open
// on it is told what a person's steps tell. F72
const CEILINGS: Record<string, number> = {
  L1_GRID: 5, L1_THEN: 3, L1_ROUTES: 5, L1_REACH: 6, L1_PLANE: 3, L1_HOST: 8,
  L8_KINDS: 0, L8_SOURCES: 0, L8_UNASKED: 0, MARKERS: 0, UNCHECKED: 0,
};

function pinned(name: string, size: number): void {
  assert.equal(size, CEILINGS[name], `${name} holds ${size} entries, its ceiling says ${CEILINGS[name]}: lower the ceiling `
    + 'with the fix that shrank it; raise it only with a dated note naming the task');
}

// -- L1 -----------------------------------------------------------------------------------------------------------

const PLUMBING = 'the one way a change reaches the state owner: no gap of its own';

/** Every call in the grid (app.ts) that changes its state, by where it is made. Its owner's `refresh` is none: the same
 *  state run again, its rows read afresh (state-owner.ts refresh: change with the state as it is, forced). */
const GRID_CALLS = /this\.(#change|#owner\.change|#owner\.undo|#owner\.redo|#owner\.retryRefused)\(/g;
/** The owner reached through another name: a change no scan above would see (the S7 audit's G-1 notes). */
const OWNER_ALIAS = /(?<!#)\bowner\.(?:change|undo|redo|retryRefused)\(/;
const L1_GRID: Record<string, Entry> = {
  '#change:#owner.change': { items: [], why: PLUMBING },
  '#applyCommands:#change': { items: [], why: 'the command path: applyGridCommands, told by onCommand' },
  '#undo:#owner.undo': { items: [], why: 'N2: an undo is told as a restore (onRestore), the page written whole at its version' },
  '#redo:#owner.redo': { items: [], why: 'N2: as an undo' },
  // a retry stays a retry: the refused change run again, its commands kept and told when it lands
  // (cube-transactions.test.ts G13)
  'submit:#owner.retryRefused': { items: [], why: 'the refused change run again (signed in again): the commands it was told as they land, or, an undo or a redo among it, a restore (N2)' },
};

/** Every call of the command path, by where it is made: the ones with a change of their own beside the commands. */
const GRID_THEN = /this\.(#applyCommands)\(/g;
const L1_THEN: Record<string, Entry> = {
  '#apply:#applyCommands': { items: [], why: 'the screen\'s edits: commands alone' },
  'applyGridCommands:#applyCommands': { items: [], why: "another editor's commands: never told back (pinned below)" },
  '#applyDraft:#applyCommands': { items: [], why: "Properties Apply: the draft's commands alone, the tree moved by configure's rule" },
};
/** The allowed calls' exact text: a change beside the commands added there is a new gap, not a quiet edit. */
const THEN_TEXT: Record<string, string> = {
  '#apply:#applyCommands': 'const { refused, outcome } = await this.#applyCommands(commands, label);',
  // 2026-10-22 (the S7 confirmation audit's third round's 1): what a page step of the person's own said already is a
  // recorded step of the grid's history, told nothing ('said'); another editor's, neither ('theirs')
  'applyGridCommands:#applyCommands': "const { refused, outcome } = await this.#applyCommands(commands, label, own ? 'said' : 'theirs');",
  '#applyDraft:#applyCommands': "const { refused, outcome } = await this.#applyCommands(commands, 'Properties');",
};

/** The grid's public ways in that change its state or copies, called from anywhere in the app but the grid itself. */
const ROUTES = /\.((?:state\.(?:change|undo|redo|retryRefused|refresh))|change|refilter|applyGridCommands|followSnap|followLive|controller\.\w+)\(/g;
const ROUTE_FILES = (f: string): boolean => f.startsWith('src/') && f.endsWith('.ts') && f !== 'src/app.ts'
  && f !== 'src/state-owner.ts' && !f.startsWith('src/adhoc/') && !f.startsWith('src/headless/');
const L1_ROUTES: Record<string, Entry> = {
  'src/page/cube-page.ts:#select:refilter': { items: [], why: "a chart's mark clicked, or its chip: the grid's refilter command, told beside the page's select (page-filters.ts chartPicked, Python's chartPick's rule); a chart removed or moved by a step is no click: its grid follows the step (ChartSource.unselected)" },
  'src/page/page-app.ts:follow:followSnap': { items: [], why: "following another editor's snap" },
  'src/page/page-app.ts:follow:followLive': { items: [], why: "following another editor's going live" },
  'src/app/datacube-app.ts:openSaved:followSnap': { items: [], why: 'N1: a saved page opened, its copies as it says' },
  'src/app/datacube-app.ts:applyGrid:applyGridCommands': { items: [], why: "another editor's commands, applied" },
};

/** Every call of a listed gap, by where it is called from: a new gesture reaching an old gap is a new entry. */
const GAP_CALLS_HOST = /(?<![\w.#]|function )(inPlace|openSaved|blankPage)(?:\?\.)?\(/g;
const L1_REACH: Record<string, Entry> = {
  'datacube-app.ts:openInPlace:inPlace': { items: [], why: "N1, D1: a page of this tab's own -- on a page another editor serves, that one left as it is" },
  'datacube-app.ts:open:openSaved': { items: [], why: "N1, D1: the Open… window's page, of this tab's own; and openLive's `open`, the page its other editor serves opened whole as its keeper has it (replacing)", count: 2 },
  'datacube-app.ts:openText:openSaved': { items: [], why: "N1, D1: a page of this tab's own -- on a page another editor serves, that one left as it is" },
  'datacube-app.ts:examplesList:openSaved': { items: [], why: "N1, D1: a page of this tab's own -- on a page another editor serves, that one left as it is" },
  'datacube-app.ts:openDataCube:openSaved': { items: [], why: "N1, D1: a page of this tab's own -- on a page another editor serves, that one left as it is" },
  'datacube-app.ts:openDataCube:blankPage': { items: [], why: "N1, D1: New ▸ Blank Page, a new page of this tab's own (newPage, D11), the page another editor serves left as it is; and a start that opened nothing, said -- never once the page its other editor serves is open (the S7 audit's H-1)", count: 3 },
};

/** What a function tells: any command, by any way the app tells one. A write in a function that tells one is no gap. */
// (the page's rename and a grid's own move of its rows are told by the page: page-app.ts rename, cube-page.ts on 'moved')
const TELLS = /kind: '(?:renamePage|snap|live|modelInto|modelPin|modelKeep|rebuilt|followGrid|select)'|channel\??\.edit\(|onPageCommand\??\.\(|#tell\(|#apply\(|page\.rename\(|page\.savedAs\(|#emit\('moved'\)/;
const quiet = (body: string): boolean => !TELLS.test(body);

/** Where the grid says the rows it reads moved, or moves them (a snap, going live): the page's copies change. */
const GRID_PLANE = /this\.(#emit\('plane'\)|#controller\.(?:snapAndRun|goLive|follow|forget)\()/g;
const L1_PLANE: Record<string, Entry> = {
  "#planeMoved:#emit('plane')": { items: [], why: "following another editor's snap or going live" },
  'followSnap:#controller.follow(': { items: [], why: "following another editor's snap" },
  'followLive:#controller.forget(': { items: [], why: "following another editor's going live" },
};

/** The host's own writes of the page (app/datacube-app.ts) in a function that tells no command. */
const HOST_CALLS = /(current\s*=(?!=)|page\.setTitle\(|page\.clear\(\)|page\.restore\()/g;
const L1_HOST: Record<string, Entry> = {
  'inPlace:page.clear()': { items: [], why: "N1, D1: a page of this tab's own -- on a page another editor serves, that one left as it is" },
  'inPlace:current =': { items: [], why: "N1, D1: a page of this tab's own -- on a page another editor serves, that one left as it is", count: 2 },
  'openSaved:page.restore(': { items: [], why: "N1, D1: a page of this tab's own -- on a page another editor serves, that one left as it is" },
  'openSaved:current =': { items: [], why: "N1, D1: a page of this tab's own -- on a page another editor serves, that one left as it is", count: 2 },
  'openSaved:page.setTitle(': { items: [], why: "N1, D1: a page of this tab's own -- on a page another editor serves, that one left as it is" },
  'blankPage:page.clear()': { items: [], why: "N1, D1: a new page of this tab's own (newPage, D11) -- on a page another editor serves, that one left as it is" },
  'blankPage:current =': { items: [], why: "N1, D1: a new page of this tab's own (newPage, D11) -- on a page another editor serves, that one left as it is" },
  'followLive:current =': { items: [], why: "following another editor's page: \"changed since saved\" from here, the host's bookkeeping (the saved page it is is the page's own: savedAs)" },
};

describe('L1: every change of a grid, a copy or the page made other than by command is listed', () => {
  const app = SOURCES.read('src/app.ts');
  const host = SOURCES.read('src/app/datacube-app.ts');

  it('the grid', () => {
    assert.deepEqual(heldTo(sites(app, GRID_CALLS), L1_GRID, 'app.ts'), []);
    assert.doesNotMatch(codeLines(app).join('\n'), OWNER_ALIAS, "the grid's state changed through another name for its owner");
    pinned('L1_GRID', Object.keys(L1_GRID).length);
  });

  it('the command path, and the allowed calls of it as they are written', () => {
    assert.deepEqual(heldTo(sites(app, GRID_THEN), L1_THEN, 'app.ts'), []);
    for (const [key, text] of Object.entries(THEN_TEXT)) {
      assert.ok(app.includes(text), `${key} is no longer written "${text}": a change beside the commands is a new gap`);
    }
    pinned('L1_THEN', Object.keys(L1_THEN).length);
  });

  it("the grid's public ways in, and who calls them", () => {
    const found = new Map<string, number>();
    for (const f of SOURCES.under('src', '.ts').filter(ROUTE_FILES)) {
      for (const [key, n] of sites(SOURCES.read(f), ROUTES)) found.set(`${f}:${key}`, n);
    }
    // the owner and the controller handed out at all (each a way to change the state with no command): G11 closed
    // them, the state shown only as what it reads -- handing either out again is a new gap
    if (/get state\(\): CubeStateOwner\b/.test(app)) found.set('src/app.ts:get state()', 1);
    if (/get controller\(\)/.test(app)) found.set('src/app.ts:get controller()', 1);
    assert.ok(app.includes("export type CubeStateView = Pick<CubeStateOwner, 'busy' | 'historyDepth'>;"),
      'the state a grid shows is what it reads (busy, historyDepth): a way to change it there is a new gap (G11)');
    assert.deepEqual(heldTo(found, L1_ROUTES, 'routes'), []);
    pinned('L1_ROUTES', Object.keys(L1_ROUTES).length);
    // a chart's selection off its grid as a step took it off (ChartSource.unselected: another editor's change, as it were
    // -- never told back): only where the board follows a step that removed or moved a chart (the S7 audit's P-1); a call
    // anywhere else is a gesture changing a grid with no command
    const following = new Map<string, number>();
    for (const f of SOURCES.under('src', '.ts').filter(ROUTE_FILES)) {
      for (const [key, n] of sites(SOURCES.read(f), /\.(unselected)\(/g)) following.set(`${f}:${key}`, n);
    }
    assert.deepEqual(Object.fromEntries(following), {
      'src/page/cube-page.ts:#dropChart:unselected': 1, 'src/page/cube-page.ts:#synced:unselected': 1,
    });
    // the grid's own calls of its way in for another editor's commands (applyGridCommands: no step, nothing told) --
    // ROUTE_FILES leaves the grid out, so a gesture of its own reaching it changed the grid with nothing told unseen (the
    // S7 confirmation audit's G2-3): only the selection a page step took off, followed as another editor's step is
    assert.deepEqual(Object.fromEntries(sites(app, /this\.(applyGridCommands)\(/g)), { 'unselected:applyGridCommands': 1 });
  });

  it("a new grid's configuration, made by one rule (G15 closed)", () => {
    // cube-document.ts freshConfiguration: titled by its source, capped by gridCapped -- a second spelling (a grid's
    // configuration titled by a source's label) is a new rule; an example's own configured grid is none
    const spelled = SOURCES.under('src', '.ts').filter((f) => f !== 'src/cube-document.ts'
      && /reportTitle:\s*[\w.]*\blabel\b/.test(SOURCES.read(f)));
    assert.deepEqual(spelled, [], "a new grid's configuration spelled outside freshConfiguration (cube-document.ts)");
  });

  it('who calls each listed gap', () => {
    const found = new Map<string, number>();
    for (const [key, n] of sites(host, GAP_CALLS_HOST)) found.set(`datacube-app.ts:${key}`, n);
    assert.deepEqual(heldTo(found, L1_REACH, 'callers'), []);
    pinned('L1_REACH', Object.keys(L1_REACH).length);
  });

  it('the rows a grid reads (Snap, Go Live), where no command is told', () => {
    assert.deepEqual(heldTo(sites(app, GRID_PLANE, quiet), L1_PLANE, 'app.ts'), []);
    pinned('L1_PLANE', Object.keys(L1_PLANE).length);
  });

  it("the host's own writes of the page, where no command is told", () => {
    assert.deepEqual(heldTo(sites(host, HOST_CALLS, quiet), L1_HOST, 'datacube-app.ts'), []);
    pinned('L1_HOST', Object.keys(L1_HOST).length);
  });
});

// -- L8 -----------------------------------------------------------------------------------------------------------

/** A command kind's cells: made by the screen, by Python (itself, or by a headless function it asks). */
type Cell = 'screen' | 'python';
/** The missing cells of each kind, by item; `nature` where the kind is one side's by its nature (reviewed at S7). */
const L8_KINDS: Record<string, Partial<Record<Cell, Entry>>> = {
};

/** Each source kind's missing cells: Python reads a page over it (headless gridBase), Python adds a grid over it. */
const L8_SOURCES: Record<string, { readonly read?: Entry; readonly add?: Entry }> = {};
/** The headless functions by which Python adds a grid over a source kind today -- pinned: a new entry is S5/S6's,
 *  with a test that adds such a grid from Python. */
// 2026-10-22 (S5, X1-X4, D14): a file and a remote file added by Python too (page.grid(file=..., remote=...): leafCube
// over the file read in, where the host reads it), tested by test_page.py OverFiles
// 2026-10-22 (S6 E, X4): a class, a saved query and a warehouse table added by Python too (page.grid(cls=..., query=...,
// warehouse=...): sourceCube over the source where the host reads it), tested by test_page.py OverClasses, OverWarehouse,
// OverSavedQueries and PublishedQueries
const PYTHON_ADDS: Record<string, string> = {
  frame: 'gridCube', join: 'joinCube', file: 'leafCube', remoteFile: 'leafCube',
  class: 'sourceCube', savedQuery: 'sourceCube', warehouseTable: 'sourceCube',
};

/** Headless exports Python never asks (`call('<name>'`): each one Python should. */
const L8_UNASKED: Record<string, Entry> = {};

/**
 * WHAT IS NOT CHECKED YET, SAID: what the screen does that Python's API cannot, with no command kind of its own to
 * hold it by (the commands exist). S6 agrees each method's name with the user and writes its test first; until then
 * these are register entries no test holds -- listed so, and pinned, never mistaken for checked.
 */
const UNCHECKED_UNTIL_S6: Record<string, string> = {
};

describe('L8: every command kind and source kind, against who makes and reads it', () => {
  const grid = SOURCES.read('src/grid-commands.ts');
  const page = SOURCES.read('src/page-commands.ts');
  const gridKinds = unionKinds(grid, 'GridCommand');
  const pageKinds = unionKinds(page, 'PageCommand');
  // Python's own code: its generated builders (_commands.py, one per kind: D17) make nothing until it calls one
  const ownPython = SOURCES.under(PY, '.py').filter((f) => !f.endsWith('/_commands.py'));
  const python = ownPython.map((f) => pythonCode(SOURCES.read(f))).join('\n');
  const exported = SOURCES.under('src/headless', '.ts').flatMap((f) => exportedFunctions(SOURCES.read(f)));
  const asked = (name: string): boolean => python.includes(`'${name}'`);
  /** Python's own code calls the generated builder of `kind` (`_commands.grid_add_sort(`, `_commands.page_move_sheet(`). */
  const builds = (kind: string): boolean =>
    new RegExp(`\\b_commands\\.(?:grid|page)_${kind.replace(/[A-Z]/g, (c) => `_${c.toLowerCase()}`)}\\(`).test(python);
  /** The modules that make commands: each one naming the command types or telling page edits (a new one is read too). */
  const producers = SOURCES.under('src', '.ts')
    .filter((f) => !f.startsWith('src/headless/') && !['src/grid-commands.ts', 'src/page-commands.ts', 'src/page-rebase.ts'].includes(f))
    .filter((f) => /\b(?:GridCommand|PageCommand)\b|#tell\(/.test(SOURCES.read(f)));
  // a trailing comment is no producer
  const screen = producers.map((f) => codeLines(SOURCES.read(f)).map((l) => l.replace(/\s\/\/\s.*$/, '')).join('\n')).join('\n')
    // the Properties window's commands are built by draftCommands, the screen's
    + grid.slice(grid.indexOf('export function draftCommands'), grid.indexOf('function settingsPatch'))
    // and what a page opened again tells of its cubes opened over their frames (rebuiltCommands, the screen's)
    + functionBody(page, 'rebuiltCommands');
  // a command written out, or built by pageCommand -- a page command whose every field is the person's input, as Python's
  // generated builder builds it (page-gestures.ts; 2026-10-22, S8)
  const made = (text: string, kind: string): boolean => new RegExp(`(?<!readonly )kind: '${kind}'|\\bpageCommand\\('${kind}'`).test(text);
  // 2026-10-22 (S5, Z4-Z7): a headless model action makes the page's model commands by the record the screen keeps
  // (page-model.ts PageModel, which a model action's export makes), so what PageModel makes, it makes
  const pageModel = SOURCES.read('src/page-model.ts');
  // 2026-10-22 (the S7 audit's Y-1, Y-9): a chart's pick made by the one rule the screen's click asks too (page-filters.ts
  // chartPicked), which headless chartPick calls
  const filters = SOURCES.read('src/page-filters.ts');
  // its whole body, to its closing brace at the margin (its parameters' type closes at the margin first)
  const pickRule = filters.slice(filters.indexOf('export function chartPicked'), filters.indexOf('\n}\n', filters.indexOf('export function chartPicked')));
  // 2026-10-22 (the user: one sort API; S8: screen code builds no command): a gesture made by the one table the
  // screen's controls ask too (grid-gestures.ts gridGesture, gridGestureGiven -- a header's sort click by header-sort.ts
  // headerSortCommand), which headless gridGesture asks
  const gestureRule = SOURCES.read('src/grid-gestures.ts') + SOURCES.read('src/header-sort.ts');
  // and a page's gesture Python asks by name (page-gestures.ts PAGE_GESTURES: Show Grid's steps), which headless
  // pageGesture calls -- the table and the functions it calls, not the module's other gestures (the S8 audit's 1)
  const pageGestures = SOURCES.read('src/page-gestures.ts');
  const pageTable = pageGestures.slice(pageGestures.indexOf('export const PAGE_GESTURES'));
  const tableCalls = [...new Set([...pageTable.matchAll(/\b(\w+)\(/g)].map((m) => m[1]!))]
    .filter((n) => new RegExp(`^export function ${n}\\b`, 'm').test(pageGestures));
  const pageGestureRule = [pageTable, ...tableCalls.map((n) => functionBody(pageGestures, n))].join('\n');
  const exportMakes = (body: string, kind: string): boolean => made(body, kind) || (/\bnew PageModel\(/.test(body) && made(pageModel, kind))
    || (/\bchartPicked\(/.test(body) && made(pickRule, kind)) || (/\bgridGestureGiven\(/.test(body) && made(gestureRule, kind))
    || (/\bPAGE_GESTURES\[/.test(body) && made(pageGestureRule, kind));

  it('each command kind is in its list, read by its reader, renamed by the rebase', () => {
    assert.deepEqual(listed(grid, 'GRID_COMMAND_KINDS'), gridKinds, 'GRID_COMMAND_KINDS is every grid command kind');
    assert.deepEqual(listed(page, 'PAGE_COMMAND_KINDS'), pageKinds, 'PAGE_COMMAND_KINDS is every page command kind');
    const gridReader = functionBody(grid, 'readGridCommand');
    const pageReader = functionBody(page, 'readPageCommand');
    const renaming = functionBody(SOURCES.read('src/page-rebase.ts'), 'renamedCommand');
    assert.deepEqual(gridKinds.filter((k) => !gridReader.includes(`case '${k}'`)), [], 'grid kinds readGridCommand cannot read');
    assert.deepEqual(pageKinds.filter((k) => !pageReader.includes(`case '${k}'`)), [], 'page kinds readPageCommand cannot read');
    assert.deepEqual(pageKinds.filter((k) => !renaming.includes(`case '${k}'`)), [], 'page kinds the rebase does not rename');
  });

  it('Python writes a command only by its generated builder (D17): none of its own', () => {
    const kinds = [...gridKinds, ...pageKinds];
    const own = ownPython.flatMap((f) => {
      const code = pythonCode(SOURCES.read(f));
      return kinds.filter((k) => new RegExp(`['"]kind['"]\\s*:\\s*['"]${k}['"]`).test(code)).map((k) => `${f}: ${k}`);
    });
    assert.deepEqual(own, [], 'a command Python writes itself: build it by its _commands builder');
  });

  it('each command kind is made by the screen and by Python, or listed', () => {
    // what the page's table makes is credited, never the module's other gestures (the S8 audit's 1)
    assert.ok(made(pageGestureRule, 'followGrid') && made(pageGestureRule, 'setChart') && !made(pageGestureRule, 'layout'));
    const problems: string[] = [];
    for (const kind of [...gridKinds, ...pageKinds]) {
      const has: Record<Cell, boolean> = {
        screen: made(screen, kind),
        python: builds(kind) || exported.some((e) => exportMakes(e.body, kind) && asked(e.name)),
      };
      for (const cell of ['screen', 'python'] as const) {
        const entry = L8_KINDS[kind]?.[cell];
        if (!has[cell] && !entry) problems.push(`${kind}: no ${cell} makes it -- make one, or list it with its item`);
        if (has[cell] && entry) problems.push(`${kind}: the ${cell} makes it now -- close its item, take it off`);
      }
    }
    for (const kind of Object.keys(L8_KINDS)) if (![...gridKinds, ...pageKinds].includes(kind)) problems.push(`${kind}: no such kind`);
    assert.deepEqual(problems, []);
    pinned('L8_KINDS', Object.keys(L8_KINDS).length);
  });

  it('each source kind is read and added by Python, or listed', () => {
    const kinds = unionTypes(SOURCES.read('src/cube-document.ts'), 'CubeSource');
    // what a page on the host holds is one rule, which gridBase, gridOpened and the keeper ask (headless host-tables.ts
    // onHost: a join of the leaves leafOnHost reads)
    const onHostBody = exported.find((e) => e.name === 'onHost')!.body;
    const leafRule = exported.find((e) => e.name === 'leafOnHost')!.body;
    /** A leaf kind the host reads: its own case -- up to the next case, never a later one's (the review's F32) --
     *  returns where it is read. */
    const leafReads = (kind: string): boolean => {
      const at = leafRule.indexOf(`case '${kind}':`);
      if (at < 0) return false;
      const rest = leafRule.slice(at + `case '${kind}':`.length);
      const next = rest.search(/\bcase '/);
      return (next < 0 ? rest : rest.slice(0, next)).includes('return ');
    };
    const reads = (kind: string): boolean => (kind === 'join' ? onHostBody.includes('{ join: source') : leafReads(kind));
    const problems: string[] = [];
    for (const kind of kinds) {
      const read = reads(kind);
      const add = PYTHON_ADDS[kind] !== undefined && asked(PYTHON_ADDS[kind]);
      const entry = L8_SOURCES[kind];
      if (read === (entry?.read !== undefined)) problems.push(`${kind}: Python ${read ? 'reads it now -- close' : 'cannot read it -- list'} its item`);
      if (add === (entry?.add !== undefined)) problems.push(`${kind}: Python ${add ? 'adds it now -- close' : 'cannot add it -- list'} its item`);
    }
    for (const kind of Object.keys(L8_SOURCES)) if (!kinds.includes(kind)) problems.push(`${kind}: no such source kind`);
    assert.deepEqual(problems, []);
    assert.deepEqual(PYTHON_ADDS, {
      frame: 'gridCube', join: 'joinCube', file: 'leafCube', remoteFile: 'leafCube',
      class: 'sourceCube', savedQuery: 'sourceCube', warehouseTable: 'sourceCube',
    }, 'a new way for Python to add a grid comes with its test');
    pinned('L8_SOURCES', Object.keys(L8_SOURCES).length);
  });

  it("each headless export Python never asks is listed", () => {
    const entry = SOURCES.read('src/headless/entry.ts');
    const names = [...entry.matchAll(/^export \{([^}]*)\};/gm)].flatMap((m) => m[1]!.split(',').map((n) => n.trim()))
      .filter((n) => n.length > 0 && n !== 'REQUEST_KINDS' && !n.startsWith('host'));
    const unasked = new Map(names.filter((n) => !asked(n)).map((n) => [`${n}`, 1]));
    assert.deepEqual(heldTo(unasked, L8_UNASKED, 'headless'), []);
    pinned('L8_UNASKED', Object.keys(L8_UNASKED).length);
  });

  it('what is not checked yet is said, and only shrinks', () => {
    pinned('UNCHECKED', Object.keys(UNCHECKED_UNTIL_S6).length);
  });
});

// -- L11: screen code builds no command ------------------------------------------------------------------------------
//
// THE SCREEN DECIDES NO GESTURE'S MEANING (docs/DATACUBE_ONE_API_PLAN_2026_10_21.md S8; the user, 2026-10-22: "Most
// important piece of the audits are if they are finding new places where we don't have real API for datacube
// functionality and have somehow coupled the UI to the implementation instead ... Is there any realistic way to find all
// and guard against this"). Every command the screen applies is made outside its files: by a gesture's one function
// (grid-gestures.ts, page-gestures.ts), which Python asks too (headless gridGesture, pageGesture), by another rule
// module (grid-presets.ts, header-sort.ts, page-filters.ts, page-model.ts), or -- a page command whose every field is the
// person's input -- by pageCommand, as Python's generated builder writes it. Closed at 0 (S8): nothing here is a gap.

/** The screen's files: what draws a page and takes a person's gestures -- its directories, and the screen's files kept
 *  beside the rules (the S8 audit's 4): the grid's DOM, Ad Hoc mode's screens, the charts' drawing, and the model
 *  actions' flows, which open their windows (their headless twins in headless/model.ts share PageModel). layout/bands.ts
 *  is the bands' model, the page commands' own (page-commands.ts, page-rebase.ts read it), no screen's. */
const SCREEN_FILES = [
  'src/app.ts', 'src/layout/band-board.ts', 'src/grid/grid.ts', 'src/adhoc/mode.ts', 'src/adhoc/options-panel.ts',
  'src/adhoc/member-selection.ts', 'src/chart-render.ts', 'src/create-class.ts', 'src/model-actions.ts',
];
const isScreen = (f: string): boolean => SCREEN_FILES.includes(f) || ['src/page/', 'src/app/', 'src/ui/'].some((d) => f.startsWith(d));
/** A command kind's spelling in a screen file that is no command, by file and kind: why. */
const NOT_COMMANDS: Record<string, string> = {
  'src/layout/band-board.ts:arrange': "the hand's move the board reports (LayoutMove), told by page-gestures.ts movedCommand",
  'src/layout/band-board.ts:stack': "the hand's move the board reports (LayoutMove), told by page-gestures.ts movedCommand",
  'src/ui/filter-editor.ts:group': "a draft of the filter editor's own (an and/or group of conditions), no grid's command",
};
/** The page commands a screen file builds by pageCommand: each a kind whose every field is what the person typed, picked
 *  or pointed at, or what the board measures or numbers -- nothing to decide, as Python's builder writes it. */
const PASS_THROUGH: Record<string, string> = {
  renameTile: 'the title as typed (renameTile judges it)',
  addSheet: "a new sheet's id, as the page numbers ids (page-ids.ts nextPageId), unnamed",
  removeSheet: 'the sheet deleted',
  removeTile: "the tile whose x was clicked (removeTile's rule takes what goes with it)",
  snap: "the grid and the copy its Snap made", live: 'the grid gone live, or opened live with its copy unread',
  addGrid: "the new grid's cube, where it is dropped, and how many tiles fit side by side there (the board's measure)",
  addChart: "the chart's spec as its panel made it (chart-spec.ts editChart), beside its grid",
  setChart: "the spec as the chart's panel made it (chart-spec.ts editChart)",
  renamePage: 'the name as typed (renamePage judges it)', savedAs: "the page store's record the page was saved as",
  setPageFilter: "the filter as the page's filter bar made it", removePageFilter: 'the page filter whose x was clicked',
};
/** Functions that make commands a screen file calls and no headless function asks, each the screen's own (2026-10-22,
 *  S8): a person's input passed through, a form's edits, the board's hand or a tab's drag. */
const SCREEN_OWN_MAKERS: Record<string, string> = {
  pageCommand: 'the person\'s input passed through: the kinds PASS_THROUGH holds, each Python\'s builder writes too',
  draftCommands: "the Properties window's edits (its draft against the grid it opened on): a form's; Python sets the same by configure and column",
  layoutCommand: "a layout the board drew, its editing grid's place left out (an editing grid is the board's moment)",
  movedCommand: "what the hand did on the board (a stack, a preset picked), its editing grid's place left out",
  renameSheetCommand: "a sheet tab's rename, nothing told when it changes nothing (renameSheet's own rule names it: Python's sheet.rename tells it as given)",
  moveSheetCommand: "a sheet tab dragged, nothing told where it is (moveSheet's own rule places it: Python's sheet.move tells it as given)",
  moveToSheetCommands: "a tile dropped on another sheet's tab, or on New Sheet (Python makes the sheet, then moves tiles onto it)",
  selectionRefilters: "the tab's page undo and redo: a step brought back puts its charts' selections back on their grids (G8); Python's page has no undo",
  rebuiltCommands: "the tab opening the page Python serves: a cube it opened over the host's frames as they are now told back (P11); Python's own reopening tells its rebuilt as it reopens (_current)",
};

/** A call's arguments: the text between `(` at `open` and its closing `)`, strings and nesting skipped. */
function callArguments(text: string, open: number): string {
  let depth = 0;
  let quote: string | null = null;
  for (let i = open; i < text.length; i++) {
    const c = text[i]!;
    if (quote !== null) {
      if (c === '\\') i++;
      else if (c === quote) quote = null;
    } else if (c === "'" || c === '"' || c === '`') quote = c;
    else if (c === '(' || c === '[' || c === '{') depth++;
    else if (c === ')' || c === ']' || c === '}') {
      depth--;
      if (depth === 0) return text.slice(open + 1, i);
    }
  }
  return text.slice(open + 1);
}
/** A gesture's arguments a screen gives, computed: a list made (spread, filtered, mapped ...) or the grid's state read
 *  -- what the gesture would decide, decided before it (the S8 review's blind spot: `zones.set` given `[...rows, c]`). */
const DECIDED = /\[\s*\.\.\.|\.(?:filter|map|flatMap|concat|slice|splice|reduce|sort|toSorted|reverse|includes|indexOf|find)\(|this\.#(?:snapshot|config|shown|view|owner)\b/;

describe('L11: screen code builds no command', () => {
  const kinds = [...unionKinds(SOURCES.read('src/grid-commands.ts'), 'GridCommand'), ...unionKinds(SOURCES.read('src/page-commands.ts'), 'PageCommand')];
  const screens = SOURCES.under('src', '.ts').filter(isScreen);
  const code = (f: string): string => codeLines(SOURCES.read(f)).map((l) => l.replace(/\s\/\/\s.*$/, '')).join('\n');

  it('no screen file writes a command out', () => {
    // a kind as any spelling writes it: quoted either way, spaced or not
    const found = screens.flatMap((f) => kinds.filter((k) => new RegExp(`(?<!readonly )\\bkind\\s*:\\s*['"]${k}['"]`).test(code(f)))
      .map((k) => `${f}:${k}`)).filter((at) => NOT_COMMANDS[at] === undefined);
    assert.deepEqual(found, [], 'a command a screen file writes: make it by its gesture function (grid-gestures.ts, page-gestures.ts)');
    for (const at of Object.keys(NOT_COMMANDS)) {
      const [file, kind] = at.split(':') as [string, string];
      assert.ok(new RegExp(`\\bkind\\s*:\\s*['"]${kind}['"]`).test(code(file)), `${at}: listed, and no longer there -- take it off`);
    }
  });

  it('pageCommand builds only what is the person\'s input', () => {
    // a kind chosen in screen code (`pageCommand(x ? 'snap' : 'live', ...)`) is a decision: each names its kind
    const chosen = screens.flatMap((f) => [...code(f).matchAll(/\bpageCommand\(\s*(?!['"]\w+['"]\s*,)/g)].map(() => f));
    assert.deepEqual(chosen, [], 'pageCommand given a kind it does not name: name it, or make the command by a gesture function');
    const built = new Set(screens.flatMap((f) => [...code(f).matchAll(/\bpageCommand\(\s*['"](\w+)['"]/g)].map((m) => m[1]!)));
    assert.deepEqual([...built].filter((k) => PASS_THROUGH[k] === undefined).sort(), [],
      "a command a screen builds by pageCommand that decides something: make it by a gesture function");
    assert.deepEqual(Object.keys(PASS_THROUGH).filter((k) => !built.has(k)).sort(), [], 'listed, and no screen builds it so: take it off');
    for (const k of Object.keys(PASS_THROUGH)) assert.ok(kinds.includes(k), `${k}: no such command`);
  });

  it("a screen's gesture is given what the person did, not what it means: no list made, no state of the grid read", () => {
    const decided = screens.flatMap((f) => {
      const text = code(f);
      return [...text.matchAll(/#gesture(?:Given)?\(/g)].flatMap((m) => {
        const args = callArguments(text, m.index! + m[0].length - 1);
        // the arguments object: what follows the gesture's name
        const given = args.slice(args.indexOf(',') + 1);
        const object = given.trimStart().startsWith('{') ? callArguments(given, given.indexOf('{') - 0) : '';
        return DECIDED.test(object) ? [`${f}: #gesture(${args.split(',')[0]}, ...) decides ${object.match(DECIDED)![0]}`] : [];
      });
    });
    assert.deepEqual(decided, [], "a gesture's meaning decided in screen code: pass what the person did, let the gesture decide");
    // the scan sees what it looks for
    assert.ok(DECIDED.test("{ rows: [...this.#snapshot.rows, c] }") && DECIDED.test('{ order: order.filter((c) => c) }'));
    assert.ok(!DECIDED.test('{ order, ...(added !== undefined ? { added } : {}) }'), 'an argument given or not is no decision');
  });

  it('every function that makes commands a screen calls is one Python asks too, or the screen\'s own, said why', () => {
    // the makers: every function of a rule module that returns commands (its return type names them) and is given none
    // (an applier -- page-commands.ts applyToState -- returns the commands it applied)
    const rules = SOURCES.under('src', '.ts').filter((f) => !isScreen(f) && !f.startsWith('src/headless/'));
    const makers = new Map<string, string>();
    for (const f of rules) {
      const text = SOURCES.read(f);
      // async too (a Promise of commands: the S8 audit's 2); a maker with no return type written is not seen
      for (const m of text.matchAll(/^export (?:async )?function (\w+)(?:<[^>]*>)?\(([^]*?)\)\s*:\s*([^{;]*?(?:\{[^{}]*\}[^{;]*?)*)\s*\{\n/gm)) {
        const given = /:\s*(?:readonly\s+)?(?:GridCommand|PageCommand)(?:\[\])?\s*(?:[,=]|$)/.test(m[2]!.trim());
        if (/\b(?:GridCommand|PageCommand)\b/.test(m[3]!) && !given) makers.set(m[1]!, f);
      }
    }
    // what headless calls: directly (its own name for an import too), or through a gesture table it dispatches by name
    // read as code: no comment, and no function's own declaration counted as its call (the S8 audit's 3)
    const headless = SOURCES.under('src/headless', '.ts').map(code).join('\n').replace(/\bfunction\s+\w+\s*(?=[(<])/g, 'function ');
    const aliases = new Map([...headless.matchAll(/\b(\w+) as (\w+)\b/g)].map((m) => [m[2]!, m[1]!]));
    const called = new Set([...headless.matchAll(/\b(\w+)\(/g)].map((m) => aliases.get(m[1]!) ?? m[1]!));
    const table = SOURCES.read('src/page-gestures.ts');
    const dispatched = table.slice(table.indexOf('export const PAGE_GESTURES'));
    if (/\bPAGE_GESTURES\[/.test(headless)) for (const m of dispatched.matchAll(/\b(\w+)\(/g)) called.add(m[1]!);
    // the grid's gestures have two doors to one table (grid-gestures.ts `made`): a control's, its arguments typed
    // (gridGesture), and a gesture named by text (gridGestureGiven), which headless asks
    if (called.has('gridGestureGiven')) called.add('gridGesture');
    // what the screen calls, however it has it (imported, loaded with its module later, a page's rules handed over)
    const used = new Set(screens.flatMap((f) => [...code(f).matchAll(/\b(\w+)\(/g)].map((m) => m[1]!)).filter((n) => makers.has(n)));
    assert.deepEqual([...used].filter((n) => !called.has(n) && SCREEN_OWN_MAKERS[n] === undefined).sort(), [],
      "a function the screen makes commands by that Python cannot ask: export it headless and ask it from Python, or say why it is the screen's own");
    assert.deepEqual(Object.keys(SCREEN_OWN_MAKERS).filter((n) => !used.has(n) || called.has(n)).sort(), [],
      "listed as the screen's own, and the screen calls it no more, or Python asks it now: take it off");
    // the gestures the screen's controls make, every one Python can ask by name
    assert.ok(called.has('gridGestureGiven') && makers.has('gridGesture'), 'grid-gestures.ts, which headless gridGesture asks');
  });
});

// -- L12: every branch on the host, with what the other host does there ---------------------------------------------

/** What a host's database may or may not do (app/services.ts DatabaseService's optional members), and the services a
 *  host may or may not give. */
const DATABASE_ABILITIES = 'ingestFile|forgetFile|mountRemote|frame|snap|release|held|place|opened|made|join';
const HOST_SERVICES = 'pages|warehouse|modelHome|queries|live';

/** What only some hosts give, as screen code reads it off what gives it: the page's own engine (the tab's planner and
 *  database), its model writer, each ability of its database a host may not have, its services, its address, a grid's
 *  rows run by the host (its runner, wherever it is read: the grid's options, a source's rows, a grid's place). */
const HOST_CAPABILITIES = String.raw`engine\.local\b|engine\.modelWriting\b|database\.(?:${DATABASE_ABILITIES})\b|`
  + String.raw`services\.(?:${HOST_SERVICES})\b|browser\.address\b|(?:options|rows|place|o)\.runner\b`;

/** Code as a scan reads it: each quoted string blanked, a template's text blanked and its `${...}`s kept. */
function scannedCode(line: string): string {
  return line
    .replace(/`(?:[^`\\]|\\.)*`/g, (t) => ` ${[...t.matchAll(/\$\{([^}]*)\}/g)].map((m) => m[1]).join(' ; ')} `)
    .replace(/'(?:[^'\\]|\\.)*'|"(?:[^"\\]|\\.)*"/g, "''")
    .replace(/\s\/\/\s.*$/, '');
}

/**
 * Every place screen code branches on its host, as `file:function:capability` counted: each read of a capability
 * (HOST_CAPABILITIES) -- or of a name a file holds one in (`const local = services.engine.local`, `const { ingestFile }
 * = database`, `this.#runner = options.runner`), or of an ability through a name holding the database -- in a condition:
 * after `if (`, `!`, `?`, `&&`, `||` or a spread `...(`, or before `?`, `&&`, `||`, `===`, `!==` or a call made only where
 * it is there (`?.(`). Every such read on a line, each its own. The hosts' own files (src/services) give the capabilities
 * and headless has one host, so neither is screen code.
 */
function hostBranches(): Map<string, number> {
  return hostBranchesOf(SOURCES.under('src', '.ts')
    .filter((f) => !f.startsWith('src/headless/') && !f.startsWith('src/generated/') && !f.startsWith('src/services/'))
    .map((f) => [f, SOURCES.read(f)] as const));
}

/** hostBranches over `files` (each its name and its text). */
function hostBranchesOf(files: readonly (readonly [string, string])[]): Map<string, number> {
  const found = new Map<string, number>();
  const capabilityNames: Readonly<Record<string, string>> = {
    'services.engine.database': DATABASE_ABILITIES, database: DATABASE_ABILITIES, 'services.engine': 'local|modelWriting',
    services: HOST_SERVICES, options: 'runner', rows: 'runner', place: 'runner', o: 'runner',
  };
  for (const [f, text] of files) {
    const lines = codeLines(text).map(scannedCode);
    const held = new Set<string>();
    const databases = new Set<string>();
    for (const [at, l] of lines.entries()) {
      // a name holding a capability itself, or whether it is there -- never a value made from one (`o.runner ? a : b`),
      // its expression going on on the next line among them
      if (/^\s*(?:\?|:|&&|\|\||\.)/.test(lines[at + 1] ?? '')) continue;
      const cap = String.raw`(?:services\.)?(?:engine\.)?(?:${HOST_CAPABILITIES})`;
      const exactly = String.raw`(?:${cap}(?:\s*[!=]==\s*undefined)?|${cap}\s*\?[^:]+:\s*undefined)\s*;?\s*$`;
      const m = new RegExp(String.raw`(?:const|let) (\w+)(?:\s*:[^=]+)?\s*=\s*${exactly}`).exec(l);
      if (m) held.add(m[1]!);
      const field = new RegExp(String.raw`this\.(#\w+)\s*=\s*${exactly}`).exec(l);
      if (field) held.add(field[1]!);
      const db = /(?:const|let) (\w+)(?:\s*:[^=]+)?\s*=\s*services\.engine\.database\b/.exec(l);
      if (db) databases.add(db[1]!);
      const parts = /(?:const|let) \{([^}]+)\}\s*=\s*(services\.engine\.database|services\.engine|services|database|options|rows|place|o)\b/.exec(l);
      if (parts) {
        for (const part of parts[1]!.split(',')) {
          const [from, to] = part.split(':').map((x) => x.trim()) as [string, string | undefined];
          if (parts[2] === 'services.engine' && from === 'database') databases.add(to ?? from);
          else if (new RegExp(`^(?:${capabilityNames[parts[2]!]})$`).test(from)) held.add(to ?? from);
        }
      }
    }
    const names = [...held].map((h) => (h.startsWith('#') ? String.raw`(?<=this\.)${h}\b` : String.raw`(?<![.\w#])${h}\b`));
    const through = [...databases].filter((d) => d !== 'database')
      .map((d) => String.raw`(?<![.\w#])${d}\.(?:${DATABASE_ABILITIES})\b`);
    const capability = new RegExp([HOST_CAPABILITIES, ...names, ...through].join('|'), 'g');
    lines.forEach((line, i) => {
      for (const m of line.matchAll(capability)) {
        const before = line.slice(0, m.index).replace(/(?:[\w#$]+\??\.)+$/, '');
        const after = line.slice(m.index + m[0].length);
        const tested = /(?:\bif \(!?|[!?]\s*|&&\s*|\|\|\s*|\.\.\.\(\s*)$/.test(before);
        const testing = /^(?:\?\.[\w.]+)?\s*(?:\?(?!\.)|&&|\|\||!==|===)|^\?\.\(/.test(after);
        if (!tested && !testing) continue;
        const key = `${f}:${enclosingAt(lines, i).name}:${m[0]}`;
        found.set(key, (found.get(key) ?? 0) + 1);
      }
    });
  }
  return found;
}

const BOTH = 'both:';
const NATURE = 'by nature:';

/**
 * L12 (the user, 2026-10-23, after the class grid on a Python host was found missing: "Why do we keep finding more
 * places where ... the two hosts diverge?", then "Check host branch now"; D23): each branch on the host, with what the
 * other host does there -- `both:` its own path to the same thing, or `by nature:` why it has none. A new branch fails
 * until it is listed here, so whether the other host needs it is decided as it is written (K16 was one never asked).
 */
const HOST_BRANCHES: Readonly<Record<string, { readonly count?: number; readonly other: string; readonly ability?: HostAbility }>> = {
  // -- the app shell (datacube-app.ts) --
  'src/app/datacube-app.ts:rowsOf:local': { count: 2, other: `${BOTH} a grid's rows run by the host's runner (Python's), or the tab's planner and database`, ability: 'rows' },
  'src/app/datacube-app.ts:hostMenu:services.pages': { other: `${BOTH} Save and Open… where the host keeps pages (the tab's browser store, Python's folder)`, ability: 'pages' },
  'src/app/datacube-app.ts:hostMenu:browser.address': { other: `${NATURE} Share… needs an address others can open; Python's is this machine's loopback` },
  'src/app/datacube-app.ts:empty:services.live': { other: `${NATURE} the empty page's words: a page Python serves is filled from Python` },
  'src/app/datacube-app.ts:empty:services.pages': { other: `${BOTH} the empty page offers a saved page where the host keeps pages`, ability: 'pages' },
  'src/app/datacube-app.ts:openDataCube:services.pages': { count: 3, other: `${BOTH} Open…, a blank page and "unsaved changes" where the host keeps pages`, ability: 'pages' },
  'src/app/datacube-app.ts:openDataCube:live': { count: 2, other: `${NATURE} a page another editor serves is followed; the tab's own page has no other editor` },
  'src/app/datacube-app.ts:leaveServed:live': { other: `${NATURE} leaving a page another editor serves` },
  'src/app/datacube-app.ts:saveTo:pages': { other: `${BOTH} saved where the host keeps pages; Python's page.save() by the same rule (pageSaving)`, ability: 'pages' },
  'src/app/datacube-app.ts:openDataCube:pages': { count: 3, other: `${BOTH} the saved pages' window, Open… and Save where the host keeps pages`, ability: 'pages' },
  'src/app/datacube-app.ts:copyShareLink:address': { other: `${NATURE} a link needs an address others can open` },
  'src/app/datacube-app.ts:makeClass:modelHome': { count: 2, other: `${BOTH} Create a Class… where the host has a model home and a writer (the tab's, Python's)`, ability: 'model' },
  'src/app/datacube-app.ts:modelActions:modelHome': { count: 2, other: `${BOTH} the model actions where the host has a model home and a writer`, ability: 'model' },
  'src/app/datacube-app.ts:openDataCube:engine.local': { other: `${BOTH} Add a Grid over a class: opened here, or made by the host (K16)`, ability: 'class' },
  'src/app/datacube-app.ts:openDataCube:localEngine': { other: `${BOTH} Join…: the tab's own join, or the host's (B11)`, ability: 'join' },
  'src/app/datacube-app.ts:offered:localEngine': { other: `${BOTH} Join… offered on a grid of the tab's tables, or one the host opened`, ability: 'join' },
  // -- the sources (sources.ts) --
  'src/app/sources.ts:picksSources:database.mountRemote': { other: `${BOTH} the picker offered where the host opens any kind of source`, ability: 'remote' },
  'src/app/sources.ts:picksSources:services.warehouse': { count: 2, other: `${BOTH} the picker offered where the host opens any kind of source`, ability: 'warehouse' },
  'src/app/sources.ts:pageSources:services.warehouse': { count: 2, other: `${BOTH} a warehouse read through the tab's own pair, or by a host that reads its tables (C3)`, ability: 'warehouse' },
  'src/app/sources.ts:pageSources:local': { count: 2, other: `${BOTH} a remote file read by the page's database or the host's`, ability: 'remote' },
  'src/app/sources.ts:openedTable:local': { other: `${BOTH} the tab's warehouse table; the host's is openedHostTable`, ability: 'warehouse' },
  'src/app/sources.ts:openedRemote:database.mountRemote': { other: `${BOTH} mounted by whichever database reads it`, ability: 'remote' },
  'src/app/sources.ts:openedRemote:local': { other: `${BOTH} snapped into the tab's copy, or the host's`, ability: 'snap' },
  'src/app/sources.ts:openedJoin:local': { other: `${BOTH} a saved join opened in the tab; on a host's page, by the host (reopened)`, ability: 'join' },
  'src/app/sources.ts:openedFrame:database.frame': { other: `${NATURE} a dataframe lives in Python` },
  'src/app/sources.ts:openedRecord:queries': { other: `${BOTH} a saved query read from the host's query store`, ability: 'queries' },
  'src/app/sources.ts:openedRecord:modelHome': { other: `${BOTH} a query of an unconfigured project read through the host's model home`, ability: 'queries' },
  'src/app/sources.ts:openedClass:modelHome': { other: `${BOTH} a class read through the tab's model home; on a host's page, made by the host (K16)`, ability: 'class' },
  'src/app/sources.ts:openedGivenClass:local': { other: `${BOTH} a named class opened here, or made by the host (K16, K17)`, ability: 'class' },
  'src/app/sources.ts:openedGivenClass:writing': { other: `${BOTH} the tab reads a class through its place's runtime; the host through its own`, ability: 'class' },
  'src/app/sources.ts:openedGivenClass:database.made': { other: `${BOTH} the host's half of a named class (K16)`, ability: 'class' },
  'src/app/sources.ts:reopened:database.opened': { count: 2, other: `${BOTH} a grid of the host's page opened by the host (H-2, H-3)`, ability: 'reopen' },
  'src/app/sources.ts:reopened:local': { other: `${BOTH} a saved join opened by the tab, or the host`, ability: 'join' },
  'src/app/sources.ts:pick:database.ingestFile': { other: `${BOTH} Files and Examples where a database takes a file (the tab's, Python's)`, ability: 'files' },
  'src/app/sources.ts:pick:files': { count: 2, other: `${BOTH} Files and Examples where a database takes a file`, ability: 'files' },
  'src/app/sources.ts:pick:queries': { other: `${BOTH} Saved queries where the host has a query store`, ability: 'queries' },
  'src/app/sources.ts:pick:database.mountRemote': { other: `${BOTH} Remote files where a database reads them`, ability: 'remote' },
  'src/app/sources.ts:pick:modelHome': { other: `${BOTH} Classes where the tab reads classes or the host makes them (K17)`, ability: 'class' },
  'src/app/sources.ts:savedSection:queries': { other: `${NATURE} a store watched for another tab's save: the browser's own store` },
  'src/app/sources.ts:savedSection:modelHome': { other: `${BOTH} a query's unconfigured project read through the host's model home`, ability: 'queries' },
  'src/app/sources.ts:savedSection:address': { other: `${NATURE} a query's link needs an address others can open` },
  'src/app/sources.ts:snapTarget:local': { other: `${BOTH} a grid snaps into the tab's copy, or the host's`, ability: 'snap' },
  'src/app/sources.ts:frameAgain:database.frame': { other: `${NATURE} a dataframe lives in Python` },
  // -- found by the review's widened scan (D23): runners read off a source's rows or a grid's place, destructured
  // abilities, a database's place, every read on a line --
  'src/app.ts:#sourceName:o.runner': { other: `${BOTH} where the rows are, said: run by the host's engine, or the tab's`, ability: 'rows' },
  'src/app/datacube-app.ts:makeClass:writing': { other: `${BOTH} Create a Class… where the host writes models (the tab's writer, Python's)`, ability: 'model' },
  'src/app/datacube-app.ts:modelActions:writing': { other: `${BOTH} the model actions where the host writes models`, ability: 'model' },
  'src/app/datacube-app.ts:openDataCube:database.made': { other: `${BOTH} Add a Grid over a class: the host's half (K16)`, ability: 'class' },
  'src/app/datacube-app.ts:openDataCube:hostJoin': { other: `${BOTH} Join…: the host's join (B11)`, ability: 'join' },
  'src/app/datacube-app.ts:rowsOf:place.runner': { other: `${BOTH} a grid's rows run by the host's runner, or the tab's planner and database`, ability: 'rows' },
  'src/app/join-sources.ts:forget:database.forgetFile': { other: `${BOTH} a joined file's table let go by the database that took it`, ability: 'files' },
  'src/app/join-sources.ts:leafTable:database.ingestFile': { other: `${BOTH} a join's file read by the database that takes files`, ability: 'files' },
  'src/app/join-sources.ts:leafTable:database.mountRemote': { other: `${BOTH} a join's remote file mounted by the database that reads them`, ability: 'remote' },
  'src/app/sources.ts:databaseSection:database.place': { other: `${BOTH} where a warehouse table's Snap lands, said: the tab, or the host`, ability: 'warehouse' },
  'src/app/sources.ts:dropUnread:database.forgetFile': { other: `${BOTH} a file no grid reads let go by the database that took it`, ability: 'files' },
  'src/app/sources.ts:hostSnapOf:database.place': { other: `${BOTH} a grid snaps into the host's copy where it gives one, else the tab's`, ability: 'snap' },
  'src/app/sources.ts:hostSnapOf:held': { other: `${BOTH} a copy the host holds followed, where it gives one`, ability: 'snap' },
  'src/app/sources.ts:hostSnapOf:release': { other: `${BOTH} a copy the host holds let go, where it gives one`, ability: 'snap' },
  'src/app/sources.ts:hostSnapOf:snap': { other: `${BOTH} a grid snaps into the host's copy where it gives one, else the tab's`, ability: 'snap' },
  'src/app/sources.ts:openedClass:rows.runner': { other: `${BOTH} a class's first row read by the host's runner, or the tab's engine`, ability: 'class' },
  'src/app/sources.ts:openedFile:database.forgetFile': { other: `${BOTH} a file read in, let go by the database that took it`, ability: 'files' },
  'src/app/sources.ts:openedFile:forgetFile': { other: `${BOTH} a file read in, let go by the database that took it`, ability: 'files' },
  'src/app/sources.ts:openedFile:ingestFile': { other: `${BOTH} a file read into the database that takes files (the tab's, Python's)`, ability: 'files' },
  'src/app/sources.ts:openedOnHost:rows.runner': { other: `${BOTH} a grid the host opened, snapped where the host copies it`, ability: 'reopen' },
  'src/app/sources.ts:openedRemote:rows.runner': { other: `${BOTH} a remote file's grid snapped into the tab's copy, or the host's`, ability: 'remote' },
  'src/app/sources.ts:pick:database.made': { other: `${BOTH} Classes where the host makes a class's grid (K17)`, ability: 'class' },
  'src/app/sources.ts:pick:engine.modelWriting': { other: `${BOTH} Classes where the tab reads classes through its place's runtime (K17)`, ability: 'class' },
  'src/app/sources.ts:pick:local': { other: `${BOTH} Classes where the tab reads classes itself, or the host makes them (K17)`, ability: 'class' },
  'src/app/sources.ts:picksSources:database.ingestFile': { other: `${BOTH} the picker offered where a database takes files`, ability: 'files' },
  'src/app/sources.ts:picksSources:local': { other: `${BOTH} the picker offered where the tab reads saved queries itself, or the host does`, ability: 'queries' },
  'src/app/sources.ts:picksSources:services.queries': { other: `${BOTH} the picker offered where the host has a query store`, ability: 'queries' },
  'src/app/sources.ts:rowsPlace:rows.runner': { other: `${BOTH} a grid's place: the host's runner, or the tab's planner and engine`, ability: 'rows' },
  'src/app/sources.ts:typer:rows.runner': { other: `${BOTH} a source's columns typed by the host's runner, or the tab's planner`, ability: 'rows' },
  'src/app/sources.ts:openedFrame:rows.runner': { other: `${NATURE} a dataframe lives in Python: a frame's grid is read by its runner` },
  // -- a grid (app.ts) --
  'src/app.ts:constructor:options.runner': { count: 3, other: `${BOTH} a grid's rows run by the host's runner, or the tab's planner`, ability: 'rows' },
  'src/app.ts:#signInOffer:options.runner': { other: `${BOTH} signed in again by the tab's warehouse engine, or the host's own sign-in`, ability: 'warehouse' },
  'src/app.ts:#snapPill:options.runner': { count: 2, other: `${BOTH} Snap into the tab's copy, or the host's`, ability: 'snap' },
  'src/app.ts:paint:options.runner': { count: 2, other: `${BOTH} where the rows are, said for the tab's or the host's`, ability: 'rows' },
};

/** What a `both:` branch is a way to on each host: an ability, proved on each (HOST_ABILITIES). */
type HostAbility = 'rows' | 'pages' | 'model' | 'class' | 'join' | 'warehouse' | 'remote' | 'files' | 'snap' | 'queries'
  | 'reopen';

/** A test, `file::name`: a test file of DataCube's (under test/) or the tab's libraries' and the title of one of its
 *  `it`s, `describe`s, browser `check`s or conformance runs, or a Python test file (under ../python/tests/) and one of
 *  its test methods. */
type Proof = string;

/**
 * L12's proof (the user, 2026-10-23: "Do the strict and missing tests"): each ability a `both:` branch is a way to, and
 * the tests that run it on each host -- the tab's own path (`tab`), the Python host's (`python`: Python itself, at
 * least one) -- and, where the two hosts do it by an implementation each (`two`: the tab's engine and Python's, two
 * builds of the compiler over two DuckDBs, two copies of a Snap), the test that runs the same input both ways and
 * holds them to one result (`parity`). `shared`: one implementation, DataCube's own, run on both (the host runs it
 * headless), so no two can drift; its tests on each host are proof it runs there.
 */
const HOST_ABILITIES: Readonly<Record<HostAbility, {
  readonly kind: 'shared' | 'two';
  readonly tab: readonly Proof[];
  readonly python: readonly Proof[];
  readonly parity?: readonly Proof[];
}>> = {
  // one compiler in two builds (WebAssembly in the tab, native in Python), over two DuckDBs
  rows: {
    kind: 'two',
    tab: ['test/runner.test.ts::plans then executes locally, and says which'],
    python: ['../python/tests/test_engine.py::test_a_grid_is_planned_over_the_model_held_run_here_and_typed_by_its_plan',
      '../python/tests/test_headless.py::test_a_frames_grid_is_read_by_the_engines_plan_and_run_for_its_view_rows_and_export'],
    parity: ['test/python-engine/page.ts::every cube case answers the same through PlanAndRun on Python and in the tab'],
  },
  // the store's rules, legend-lite's Java, in two builds (the page's WebAssembly, Python's native library): one
  // conformance suite run against each
  pages: {
    kind: 'two',
    tab: ["test/saved-pages/page.test.ts::the page's own saved pages: what a server here cannot be asked"],
    python: ['../python/tests/test_page.py::test_saved_pages_are_saved_as_new_listed_and_deleted_as_datacubes_save_as_and_open_do',
      '../python/tests/test_page.py::test_a_saved_page_is_opened_by_its_id_as_datacubes_open_opens_it'],
    parity: ["test/saved-pages/page.test.ts::the page's own store, legend-lite's rules in WebAssembly",
      "test/python-engine/saved-pages.test.ts::Python's saved pages, the native library's"],
  },
  // DataCube's model actions (model-plans.ts, model-writer.ts) on both, written into a model home that is legend-lite's
  // SDLC in two builds (the page's WebAssembly, Python's native library): one conformance suite run against each
  model: {
    kind: 'two',
    tab: ['test/create-class.test.ts::Create a Class… writes its sample by the one writer'],
    python: ['../python/tests/test_model.py::test_a_grid_made_a_class_then_an_association_and_a_data_space'],
    parity: ["../sdlc-client/test/wasm.test.ts::the page's SDLC, compiled from Java",
      "test/python-engine/model-home.test.ts::Python's model home, the native library's"],
  },
  // class-source.ts classOpened and cube-document.ts rebuilt on both (host-tables.ts reads the class by them), over a
  // project read from the model home's SDLC in two builds (its conformance run against each); the compiler and DuckDB
  // under them are `rows`'
  class: {
    kind: 'two',
    tab: ['test/class-source.test.ts::opened by classOpened'],
    python: ['../python/tests/test_page.py::test_a_grid_is_added_over_a_class_as_the_tabs_add_a_grid_adds_one',
      '../python/tests/test_page.py::test_a_tab_on_the_page_asks_python_for_a_grid_over_a_class_as_page_grid_makes_one'],
    parity: ["../sdlc-client/test/wasm.test.ts::the page's SDLC, compiled from Java",
      "test/python-engine/model-home.test.ts::Python's model home, the native library's",
      'test/python-engine/page.ts::every cube case answers the same through PlanAndRun on Python and in the tab'],
  },
  // join-open.ts openJoin on both (the host's join.ts answers by it)
  join: {
    kind: 'shared',
    tab: ['test/join-open.test.ts::a join opened over one model, its rows from DuckDB',
      "test/host-join-flow.test.ts::Join… on a host's page: each of the window's answers the host's (B11)"],
    python: ['../python/tests/test_headless.py::test_two_frames_joined_as_the_tabs_join_joins_them',
      '../python/tests/test_page.py::test_a_tab_on_the_page_asks_python_each_of_its_join_windows_answers'],
  },
  // a driver each (the tab's, Python's), held to one recorded corpus of the warehouse's calls
  warehouse: {
    kind: 'two',
    tab: ['test/warehouse-session-http.test.ts::signing in is one step with its table list'],
    python: ['../python/tests/test_warehouse.py::test_every_scenario'],
    parity: ["../engine-client/test/warehouse-corpus.test.ts::the tab's warehouse driver makes the corpus's calls",
      '../python/tests/test_warehouse.py::test_every_scenario'],
  },
  // the one mount (upload.ts mountRemoteView) over two DuckDBs, each its own httpfs
  remote: {
    kind: 'two',
    tab: ['test/remote.test.ts::choosing a format', "test/python-engine/page.ts::a remote file reads the same on Python's page and in the tab"],
    python: ['../python/tests/test_headless.py::test_a_remote_parquet_and_csv_are_read_through_views_their_models_served'],
    parity: ["test/python-engine/page.ts::a remote file reads the same on Python's page and in the tab"],
  },
  // the one reader (upload.ts ingestFile) over two DuckDBs
  files: {
    kind: 'two',
    tab: ['test/upload.test.ts::ingestFile with JSON'],
    python: ['../python/tests/test_headless.py::test_a_csv_is_read_into_its_table_its_model_served_then_let_go_of'],
    parity: ["test/python-engine/page.ts::every reader file reads the same on Python's page and in the tab"],
  },
  // a copy each (the tab's SnapManager, Python's snap.json), modelled by the one writer (upload.ts modelLanded)
  snap: {
    kind: 'two',
    tab: ['test/plane.test.ts::snapping goes through the planner'],
    python: ['../python/tests/test_engine.py::test_a_frames_grid_snaps_too_a_copy_of_its_base_as_the_frame_was',
      '../python/tests/test_page.py::test_a_snapped_grid_reads_its_copy_whatever_its_frame_does_and_live_again_reads_the_frame'],
    parity: ['test/python-engine/page.ts::a grid snaps the same copy on Python and in the tab'],
  },
  // saved-queries.ts savedQueryOpened on both (host-tables.ts reads a query by it), from a query store that is
  // legend-lite's in two builds (its conformance run against each); its rows are `rows`'
  queries: {
    kind: 'two',
    tab: ['test/saved-queries.test.ts::a saved query as a source: each fixture, as the README says it runs'],
    python: ['../python/tests/test_page.py::test_a_grid_is_added_over_a_saved_query_by_its_id'],
    parity: ["../query-store/test/wasm.test.ts::the page's own store, legend-lite's rules in WebAssembly",
      "test/python-engine/saved-queries.test.ts::Python's saved queries, the native library's"],
  },
  // cube-document.ts rebuilt and cubeOpenedAgain on both (the host's gridOpened opens by them)
  reopen: {
    kind: 'shared',
    tab: ["test/cube-page-shell.test.ts::a page of the tab's own on a host opens a file grid the tab's way",
      'test/page-corpus.test.ts::the saved-page corpus (test/fixtures/pages)',
      "test/cube-page-shell.test.ts::on a host's page, a saved grid over a file is opened by the host over the table it holds"],
    python: ['../python/tests/test_page.py::test_saved_and_loaded_over_its_frames_and_reopened_when_one_is_served_again'],
  },
};

/** Whether `proof` names a test that is there: its file among the sources, the name a test of it. */
function proved(proof: Proof): string | undefined {
  const [file, name] = proof.split('::') as [string, string | undefined];
  if (name === undefined || name === '') return `${proof}: a proof is file::name`;
  if (!SOURCES.has(file)) return `${proof}: no such test file`;
  const lines = SOURCES.read(file).split('\n');
  const found = file.endsWith('.py')
    ? lines.some((l) => l.trimStart().startsWith(`def ${name}(`))
    : lines.some((l) => /\b(?:it|describe|check|conformance)\(/.test(l) && l.replace(/\\'/g, "'").includes(name));
  return found ? undefined : `${proof}: no test of that name in it`;
}

/** L12 held to what is found, in its own words. */
const hostHeld = (found: ReadonlyMap<string, number>): string[] => heldTo(found, HOST_BRANCHES, 'L12').map((p) => p
  .replace('is a new gap -- make it a command, or list it with its item',
    "is a branch on the host no one decided -- list it with what the other host does there (both:, or by nature:)")
  .replace('is listed but gone -- close its item and take it off the ledger', 'is listed but gone -- take it off'));

describe("L12: every branch on the host, with what the other host does there", () => {
  it('each listed -- the other host\'s own path, or why it has none -- and none listed that is gone', () => {
    assert.deepEqual(hostHeld(hostBranches()), []);
    const unsaid = Object.entries(HOST_BRANCHES).filter(([, e]) => !e.other.startsWith(BOTH) && !e.other.startsWith(NATURE));
    assert.deepEqual(unsaid.map(([k]) => k), [], "each says `both:` (the other host's path) or `by nature:` (why none)");
  });

  it('each `both:` a way to an ability proved on each host -- two implementations held to one result -- by tests that are there', () => {
    const problems: string[] = [];
    for (const [key, e] of Object.entries(HOST_BRANCHES)) {
      if (e.other.startsWith(BOTH) && e.ability === undefined) problems.push(`${key}: a \`both:\` names the ability it is a way to`);
      if (e.other.startsWith(NATURE) && e.ability !== undefined) problems.push(`${key}: a \`by nature:\` is a way to nothing the other host has`);
    }
    const used = new Set(Object.values(HOST_BRANCHES).flatMap((e) => (e.ability ? [e.ability] : [])));
    for (const [ability, a] of Object.entries(HOST_ABILITIES)) {
      if (!used.has(ability as HostAbility)) problems.push(`${ability}: an ability no branch is a way to`);
      if (a.tab.length === 0 || a.tab.some((p) => !p.startsWith('test/'))) problems.push(`${ability}: proved on the tab by DataCube's tests`);
      if (!a.python.some((p) => p.startsWith('../python/tests/'))) problems.push(`${ability}: proved on the Python host by Python's own tests`);
      if (a.kind === 'two' && (a.parity ?? []).length === 0) problems.push(`${ability}: two implementations, and no test holds them to one result`);
      for (const p of [...a.tab, ...a.python, ...(a.parity ?? [])]) {
        const why = proved(p);
        if (why !== undefined) problems.push(`${ability}: ${why}`);
      }
    }
    assert.deepEqual(problems, []);
  });

  it('sees a branch it was not told of -- by the capability, a name holding it, a destructured one, a runner read off its rows, every one on a line -- and no plain use of one', () => {
    const added = [
      'export function offer(services: Services, rows: GridRows): void {',
      '  if (services.engine.local) open();',
      '  const made = services.engine.database.made;',
      "  const label = made ? 'Python' : 'here';",
      "  void services.engine.database.frame?.('f');",
      '  const { ingestFile } = services.engine.database;',
      "  if (!ingestFile) throw new Error('no file here');",
      '  const db = services.engine.database;',
      '  if (db.held) follow();',
      '  const run = rows.runner ? 1 : 2;',
      '  if (services.engine.local || services.engine.database.opened) twice();',
      '  const where = services.engine.database.place;',
      "  const text = 'services.engine.local ? not code' + `${services.engine.database.snap ? 'a' : 'b'}`;",
      '  plain(services.engine.local, run, where, text);',
      '}',
    ].join('\n');
    const found = hostBranchesOf([['src/x.ts', added]]);
    assert.deepEqual(Object.fromEntries([...found].sort()), {
      'src/x.ts:offer:database.frame': 1, 'src/x.ts:offer:database.opened': 1, 'src/x.ts:offer:database.snap': 1,
      'src/x.ts:offer:db.held': 1, 'src/x.ts:offer:engine.local': 2, 'src/x.ts:offer:ingestFile': 1, 'src/x.ts:offer:made': 1,
      'src/x.ts:offer:rows.runner': 1,
    }, 'a held place read plainly, a capability passed along, text in a string: no branch');
    assert.match(hostHeld(new Map([...hostBranches(), ...found])).join('\n'),
      /src\/x\.ts:offer:engine\.local \(2\) is a branch on the host no one decided -- list it with what the other host does/);
  });

  it("takes a second capability off a line it lists: K16's host half gone, the ledger fails", () => {
    const now = hostBranches();
    const k16 = 'src/app/datacube-app.ts:openDataCube:database.made';
    assert.equal(now.get(k16), 1, 'the class grid on a Python host, read on the line beside the tab\'s own engine');
    const reverted = new Map(now);
    reverted.delete(k16);
    assert.match(hostHeld(reverted).join('\n'), /openDataCube:database\.made is listed but gone/);
  });
});

// -- markers: the gaps no scan reaches, each pinned by the code that is it ------------------------------------------

/** `text` in `file` (`count` times) while its items are open: the fix takes it away, and this fails until they close.
 *  `absent`: the gap is that the text is NOT there. `within`: only the block that starts with it is read. */
interface Marker {
  readonly file: string;
  readonly text: string;
  readonly items: readonly string[];
  readonly count?: number;
  readonly absent?: true;
  readonly within?: string;
}
const MARKERS: readonly Marker[] = [
  // the grid
  // telling exactly what landed: placeholders until S1 writes each one's failing behavioural test first
  // the page
  // the board a cube alone shows its charts on is given no way to tell its page's edits
  // derived state, and what the keeper keeps
  // the browser's half of the same jobs done by different rules
];

describe('the gaps no scan reaches, pinned by their code', () => {
  for (const m of MARKERS) {
    it(`${m.items.join(' ')}: ${m.file}`, () => {
      let text = SOURCES.read(m.file);
      if (m.within !== undefined) {
        const at = text.indexOf(m.within);
        assert.ok(at >= 0, `${m.file} no longer holds the block "${m.within}": pin it again`);
        text = text.slice(at, text.indexOf('\n    });', at));
      }
      const found = text.split(m.text).length - 1;
      if (m.absent) assert.equal(found, 0, `${m.text} is in ${m.file} now: close ${m.items.join(', ')} and take this off`);
      else assert.equal(found, m.count ?? 1, `${m.file} holds "${m.text}" ${found} times, not ${m.count ?? 1}: fixed? close `
        + `${m.items.join(', ')} and take this off; moved? pin it again`);
    });
  }
  it('their number', () => pinned('MARKERS', MARKERS.length));
});

// -- a grid saved and opened again opens its rows as it was told to (closed S2: V5) --------------------------------

describe('a grid saved and opened again opens its rows as it was told to', () => {
  it('the saved tree keeps its expand level and the groups closed under it, and opens with them', () => {
    const tree = TreeState.empty(false).withExpandTo(1).setOpen(['EMEA'], false);
    const doc = writeCube({
      name: 'trades', source: { _type: 'frame', name: 't', columns: [{ name: 'region', type: 'String' }] },
      snapshot: { source: { query: { _type: 'var', name: 'x' } }, columns: [{ name: 'region', type: 'String' }], derived: [],
        rows: ['region'], pivotOn: [], sorts: [], epoch: 0 } as unknown as CubeSnapshot,
      configuration: { ...DEFAULT_CONFIGURATION, initialExpandToLevel: 1 },
      tree,
    });
    assert.equal(configurationOf(doc).initialExpandToLevel, 1);
    assert.deepEqual(doc.tree, { open: [], showTotals: false, expandTo: 1, closed: [['EMEA']] });
    // the screen opens a saved grid with the tree the document keeps (app.ts: options.tree, as given)
    const back = treeOf(readCube(JSON.parse(cubeToJson(doc))));
    assert.equal(back.key, tree.key, 'opened again: the expand level, and EMEA closed under it');
    assert.equal(back.isOpen(['AMER'], 2), true);
    assert.equal(back.isOpen(['EMEA'], 2), false);
  });
});

// -- the register, held to every ledger ------------------------------------------------------------------------------

/** The files that hold the ledgers, each naming items as string literals (a letter and a number, quoted). */
const LEDGERS = ['test/one-api-ledger.test.ts', 'test/one-api-gestures.test.ts', '../python/tests/test_one_api.py'];
// 2026-10-22 (S8): A, the step's items (one per place screen code built a command)
const ITEM = /'([GTCPYVHZBXKA]\d{1,2})'/g;
/** Every item the register has ever held: none leaves it but closed. */
const ALL = ('G1 G2 G3 G4 G5 G6 G7 G8 G9 G10 G11 G12 G13 G14 G15 T1 T2 T3 T4 C1 P1 P2 P3 P4 P5 P6 P7 P8 P9 P10 P11 P12 '
  + 'P13 P14 Y1 Y2 Y3 Y6 V1 V2 V4 V5 H1 H2 H3 Z1 Z2 Z3 Z4 Z5 Z6 Z7 Z8 Z9 Z10 Z11 Z12 Z13 Z14 Z15 Z16 Z17 Z18 Z19 B1 B2 '
  + 'B3 B4 B5 B6 B7 B8 B9 B10 B11 B12 B13 B14 X1 X2 X3 X4 X5 K1 K2 K3 K4 K5 K6 K7 K8 K9 K10 K11 K12 '
  // 2026-10-21, One API S2's, S3's and S4's audit rounds and S4's soak: what each found, added by rule 9 and closed with
  // each fix (no ledger grows)
  + 'F1 F2 F3 F4 F5 F6 F7 F8 F9 F10 F11 F12 F13 F14 F15 F16 F17 F18 F19 F20 F21 F22 F23 F24 F25 F26 F27 '
  // 2026-10-22, D10: S1's audit finding 10, registered with the user's sign-off (written with a space after it: a quoted
  // item alone is a ledger naming it, ITEM)
  + 'P15 '
  // 2026-10-22, D18: the independent review of S5, D15 and D16, added by rule 9 and closed (or declared) with each fix
  + 'F28 F29 F30 F31 F32 F33 F34 F35 F36 F37 F38 F39 F40 '
  // 2026-10-22, PR #64's CI (macOS, 821615817): a test that raced the board's lazily loaded code, added by rule 9
  + 'F41 '
  // 2026-10-22, S6 B: a page filter with a number broke Python's grid reads (the lens read it exactly), found testing K7
  + 'F42 F43 F44 '
  // 2026-10-22, S7: the final audit's findings, added by rule 9 and closed (fixed, moved to D19, or declared) with each fix
  + 'F45 F46 F47 F48 F49 F50 F51 F52 F53 F54 F55 F56 F57 F58 F59 F60 F61 F62 F63 F64 F65 F66 F67 F68 F69 F70 '
  // 2026-10-22, S7: the confirmation audit's findings and the user's question (one sort API), added by rule 9 and closed
  // (fixed, or declared) with each fix
  + 'F71 F72 F73 F74 F75 F76 F77 F78 F79 F80 F81 F82 F83 F84 F85 F86 F87 F88 F89 F90 '
  // 2026-10-22, S7: the confirmation audit's third round, added by rule 9 and closed with each fix
  + 'F91 F92 F93 F94 F95 F96 '
  // 2026-10-22, S7: the audit's fourth round, added by rule 9 and closed (fixed, or moved to D19) with each fix
  + 'F97 F98 F99 '
  // 2026-10-22, S8 (screen code builds no command): one item per place screen code built a command, and the guard, each
  // closed by the commit that moved it
  + 'A1 A2 A3 A4 A5 A6 A7 A8 A9 A10 A11 A12 A13 A14 A15 A16 A17 A18 A19 A20 A21 A22 A23 A24 A25 A26 A27 A28 A29 A30'
  + ' A31 A32 A33 A34 A35 A36 A37 A38 A39 A40 A41 A42 A43 A44 A45 A46 A47 A48 A49 '
  // 2026-10-23, S8's review and its independent audit, added by rule 9 and closed (fixed, or declared) with each fix
  + 'A50 A51 A52 A53 A54 A55 A56 A57 A58 A59 A60 A61 A62 '
  // 2026-10-23, D23: the S7 audits' four asked items (Y-6, H-9, Y-7, H-8), built and closed with each
  + 'K13 K14 K15 K16 K17 '
  // 2026-10-23, D23's independent review, added by rule 9 and closed (fixed, or declared) with each fix
  + 'R1 R2 R3 R4 R5 R6 R7 R8 ').split(' ').filter((item) => item !== '');

describe("the plan's register and the ledgers say the same", () => {
  it('every open item is in a ledger, every item a ledger names is open, and none leaves the register unclosed', () => {
    const plan = SOURCES.read(PLAN);
    const open = new Set([...plan.matchAll(/^- \[ \] \*\*([A-Z]\d{1,2})\*\*/gm)].map((m) => m[1]!));
    const closed = new Set([...plan.matchAll(/^- \[x\] \*\*([A-Z]\d{1,2})\*\*/gm)].map((m) => m[1]!));
    const named = new Set(LEDGERS.flatMap((f) => [...SOURCES.read(f).matchAll(ITEM)].map((m) => m[1]!)));
    for (const k of Object.keys(UNCHECKED_UNTIL_S6)) named.add(k);
    assert.deepEqual([...open, ...closed].sort(), [...ALL].sort(), 'the register holds every item it ever held, each open or closed');
    assert.deepEqual([...open].filter((i) => !named.has(i)).sort(), [], 'open items no ledger holds');
    assert.deepEqual([...named].filter((i) => !open.has(i)).sort(), [], 'items a ledger holds that are not open');
    assert.deepEqual([...closed].filter((i) => named.has(i)).sort(), [], 'closed items still in a ledger');
  });
});
