// THE TAB'S OWN FUNCTIONS, HEADLESS, FOR THE PYTHON TEST ALONE (python/tests/test_keeper.py OpenPage; the one API
// plan's ledger L8_UNASKED: Python's product asks none of these, so the product's bundle exports none): the two total
// diffs a tab's history is made of, its planned apply, and how it names what it makes (docs/DATACUBE_LIVE_EDITING_2026_10_24.md §3), bundled by
// //datacube:headless_tab_bundle as the global `legendLiteHeadlessTab`, which the test loads into the headless engine
// it has (Headless.execute) to stand in for a tab -- its undo and redo by the same functions the tab's are.

import { CubeDocumentError, configurationOf, readCube, treeOf } from '../src/cube-document.ts';
import { gridBetween as gridCommandsBetween } from '../src/grid-between.ts';
import type { GridDraft } from '../src/grid-edits.ts';
import { applyPlanned, namedUnder, pageBetween as between } from '../src/page-between.ts';
import { readPageCommand, type PageCommand } from '../src/page-commands.ts';
import { PageDocumentError, pageToJson } from '../src/page-document.ts';
import { CubeRefusal, type CubeSnapshot } from '../src/snapshot.ts';
import { exactly, heldPage } from '../src/headless/page.ts';
import { element, fromJson, toJson } from '../../pure-protocol/src/index.ts';

/**
 * THE COMMANDS BETWEEN TWO PAGES (page-between.ts pageBetween; docs/DATACUBE_LIVE_EDITING_2026_10_24.md §3): `from` and
 * `to`, two documents of one page (JSON, numbers exact), `scope` 'history' (the page's structure: what a step of the
 * tab's history takes back) or 'whole'. What a page open on Python's page undoes by, as the tab's history does (test_keeper.py
 * OpenPage): the same function, so both sides' undos are the same commands. JSON: `{commands}`, or `{problems}`, in
 * DataCube's words (what no command makes).
 */
export function pageBetween(from: string, to: string, scope: string): string {
  try {
    if (scope !== 'history' && scope !== 'whole') throw new CubeRefusal(`a scope is 'history' or 'whole', not ${JSON.stringify(scope)}`);
    const plan = between(heldPage(from), heldPage(to), scope);
    return `{"commands":[${plan.map((c) => toJson(c)).join(',')}]}`;
  } catch (e) {
    if (!(e instanceof CubeRefusal) && !(e instanceof CubeDocumentError) && !(e instanceof PageDocumentError)) throw e;
    return JSON.stringify({ problems: [e.message] });
  }
}

/** Page commands as JSON (numbers exact), read as the tab reads them. */
function commandsOf(commands: string): PageCommand[] {
  const exact = fromJson(commands) as unknown[];
  return (JSON.parse(commands) as unknown[]).map((c, i) => readPageCommand(exactly(c, exact[i])));
}

/**
 * A PLAN APPLIED WITH FRESH IDS (page-between.ts applyPlanned; docs/DATACUBE_LIVE_EDITING_2026_10_24.md §3): `commands`
 * as pageBetween planned them, applied to `page` (with `held`, every id it has held) as the tab's history applies an
 * undo -- a command that makes something under an id the page has held is given a fresh one, under the tab's `tag`
 * (docs/DATACUBE_LIVE_EDITING_REDESIGN_2026_10_24.md), and every later command naming the planned id follows (B12).
 * JSON: `{page, held, commands, renamed}` -- the page they make, the ids held now, the commands as applied, each planned
 * id given out afresh (planned -> given) -- or `{problems}`.
 */
export function pagePlanned(page: string, held: string, commands: string, tag: string): string {
  try {
    const doc = heldPage(page);
    const done = applyPlanned({ page: doc, held: JSON.parse(held) as string[] }, commandsOf(commands), true, tag);
    return `{"page":${JSON.stringify(pageToJson(done.draft.page))},"held":${JSON.stringify(done.draft.held)},`
      + `"commands":[${done.applied.map((c) => toJson(c)).join(',')}],"renamed":${JSON.stringify(done.renamed)}}`;
  } catch (e) {
    if (!(e instanceof CubeRefusal) && !(e instanceof CubeDocumentError) && !(e instanceof PageDocumentError)) throw e;
    return JSON.stringify({ problems: [e.message] });
  }
}

/**
 * A TAB'S COMMANDS GIVEN THEIR IDS (page-between.ts namedUnder; docs/DATACUBE_LIVE_EDITING_REDESIGN_2026_10_24.md): each
 * of `commands` that makes something and names no id given one under the tab's `tag`, after every id `page` (with
 * `held`) holds, as the tab's page names what a person makes (page-app.ts #named). JSON: `{commands}`, or `{problems}`.
 */
export function pageNamed(page: string, held: string, commands: string, tag: string): string {
  try {
    const named = namedUnder({ page: heldPage(page), held: JSON.parse(held) as string[] }, commandsOf(commands), tag);
    return `{"commands":[${named.map((c) => toJson(c)).join(',')}]}`;
  } catch (e) {
    if (!(e instanceof CubeRefusal) && !(e instanceof CubeDocumentError) && !(e instanceof PageDocumentError)) throw e;
    return JSON.stringify({ problems: [e.message] });
  }
}

/**
 * THE COMMANDS BETWEEN TWO STATES OF A GRID (grid-between.ts gridBetween): `from` and `to`, two cube documents of one grid
 * (JSON, numbers exact), over their saved forms -- the query, the configuration, the tree; what a run alone adds (column
 * types) is no command's, and the source is not read. What a page open on Python's page undoes a grid's edit by, as the
 * tab's grid does. JSON: `{commands}`, or `{problems}`.
 */
export function gridBetween(from: string, to: string): string {
  try {
    const draft = (text: string): GridDraft => {
      const cube = readCube(fromJson(text));
      // the source is no part of what the diff reads: the cube's name stands for it (grid-between.ts reads the query,
      // the configuration and the tree)
      return { snapshot: { ...cube.query, source: { query: element(cube.source.name) }, epoch: 0 } as CubeSnapshot, config: configurationOf(cube), tree: treeOf(cube) };
    };
    const plan = gridCommandsBetween(draft(from), draft(to));
    return `{"commands":[${plan.map((c) => toJson(c)).join(',')}]}`;
  } catch (e) {
    if (!(e instanceof CubeRefusal) && !(e instanceof CubeDocumentError) && !(e instanceof Error)) throw e;
    return JSON.stringify({ problems: [e.message] });
  }
}
