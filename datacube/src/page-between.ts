// WHAT TAKES A PAGE FROM ONE TO ANOTHER, AS COMMANDS (docs/DATACUBE_LIVE_EDITING_2026_10_24.md §3, W2): the page commands
// (page-commands.ts) that, applied to `from`, make `to` -- so a step of the page's history is undone and redone by
// commands like any edit (page-state.ts: a step keeps its inverse, made here when the step is made), told as them and
// applied by the keeper as them; and a screen converging on its keeper's page applies the difference. In an order every
// arm accepts, each command applied as it is planned so the plan is one the page takes: sheets added; grids added (their
// cube documents); charts added, re-pointed (`followGrid`), their spec, selection and title; grid titles; tiles moved to
// their sheets; grids removed, then charts; page filters removed and set; each sheet's layout whole, once its tiles agree;
// sheets removed, renamed, reordered; copies (`live`, `snap`); and, `whole`, the name, the saved record and the model.
//
// IDS. A command that makes something names `to`'s id for it -- so a plan replayed where that id is free (a screen
// converging) lands under it -- and `applyPlanned` gives a fresh one where it is held (an undo: a tile taken off keeps
// its id held, B12), every later command following the rename (page-rebase.ts renamedCommand), the renames returned so
// the caller pairs each new tile with the one it stands for (page-app.ts: its maker and start). An added grid's cube is
// its view's id (page-commands.ts addGrid), so a chart or the model names the grid, never the cube `to` held for it.
//
// SCOPE. `history`: the page's structure alone -- not its name, its saved record or its model, which a step of the
// history leaves as they are (F52, F74, D5), nor the cubes of the grids both have, which are the grids' own steps.
// `whole`: everything a page command can set; what none can (a model gone, a model moved, a pin taken off, a cube opened
// again over its source as it is now: `rebuilt`) is refused in words, and the screen opens the page again instead.

import { tiles } from './layout/bands.ts';
import { applyPageCommand, idsOf, nextPageId, takenIds, type PageCommand, type PageDraft } from './page-commands.ts';
import type { CubeDocument } from './cube-document.ts';
import type { ChartView, GridCopy, GridView, PageDocument, PageSheet, PageView, TextView } from './page-document.ts';
import { rearranged, renamedCommand } from './page-rebase.ts';
import { CubeRefusal, sameValue } from './snapshot.ts';

/** What of a page the commands between two states cover (above). */
export type PageScope = 'history' | 'whole';

/** The commands that make a tile or a sheet: their id is theirs to give out afresh. */
const MAKERS = new Set<PageCommand['kind']>(['addSheet', 'addGrid', 'addChart', 'addText']);

/** The kind of id a making command gives out. */
function kindMade(command: PageCommand): 'grid' | 'chart' | 'text' | 'sheet' | 'filter' {
  switch (command.kind) {
    case 'addSheet': return 'sheet';
    case 'addGrid': return 'grid';
    case 'addChart': return 'chart';
    case 'addText': return 'text';
    default: return 'filter';
  }
}

/**
 * `commands` as an editor tagged `tag` makes them (page-ids.ts nextPageId): each that makes something and names no id
 * given one under the tag, after every id `draft` holds and each given before it -- so no other editor makes the same,
 * and the keeper takes it as given (docs/DATACUBE_LIVE_EDITING_REDESIGN_2026_10_24.md, the middle version). No tag: as
 * they are (the command gives its own).
 */
export function namedUnder(draft: PageDraft, commands: readonly PageCommand[], tag: string | undefined): readonly PageCommand[] {
  if (tag === undefined) return commands;
  const taken = [...takenIds(draft)];
  return commands.map((c) => {
    if (!(MAKERS.has(c.kind) || c.kind === 'setPageFilter') || (c as { readonly id?: string }).id !== undefined) return c;
    const id = nextPageId(taken, kindMade(c), tag);
    taken.push(id);
    return { ...c, id } as PageCommand;
  });
}

/** Whether `command` makes something under an id of its own: a tile, a sheet, or a page filter the page has not. */
function makes(page: PageDocument, command: PageCommand): boolean {
  if (MAKERS.has(command.kind)) return true;
  return command.kind === 'setPageFilter' && command.id !== undefined && !(page.filters ?? []).some((f) => f.id === command.id);
}

/**
 * One planned command applied to `draft`: renamed as the ones before it were (`renamed`, which it extends), its id
 * given afresh when `fresh` and it makes something. The page after, and the command as applied.
 */
function applyOne(draft: PageDraft, planned: PageCommand, renamed: Map<string, string>, fresh: boolean, tag?: string):
  { readonly draft: PageDraft; readonly applied: PageCommand } {
  // a sheet's arrangement as the plan has it, the tiles on that sheet now that the plan did not know placed as adding
  // places one (page-rebase.ts rearranged: the keeper's rule for a writer's layout made again on top) -- an undo past
  // another editor's added tile keeps that tile on its sheet
  let command = rearranged(renamedCommand(planned, renamed), draft.page);
  const making = makes(draft.page, command);
  if (fresh && making && (command as { readonly id?: string }).id !== undefined) {
    const { id: _held, ...rest } = command as PageCommand & { id?: string };
    // given out afresh: under the making editor's tag when it has one (page-ids.ts nextPageId), else by the command
    command = (tag === undefined ? rest : { ...rest, id: nextPageId(takenIds(draft), kindMade(command), tag) }) as PageCommand;
  }
  const one = applyPageCommand(draft, command);
  const wanted = (planned as { readonly id?: string }).id;
  const given = (one.command as { readonly id?: string }).id;
  if (making && wanted !== undefined && given !== undefined && given !== wanted) renamed.set(wanted, given);
  return { draft: one.draft, applied: one.command };
}

/**
 * COMMANDS PLANNED BY `pageBetween` APPLIED to `draft`: with `fresh`, a command that makes something is given a fresh id
 * (the page has held the one planned: an undo), and every later command naming the planned id follows; without, the
 * planned ids are honoured (a page converging on the keeper's, whose ids these are), refused by the command's own rule
 * where one is held. The page after, the commands as applied, and each planned id given out afresh (planned -> given).
 */
export function applyPlanned(draft: PageDraft, commands: readonly PageCommand[], fresh: boolean, tag?: string):
  { readonly draft: PageDraft; readonly applied: PageCommand[]; readonly renamed: Readonly<Record<string, string>> } {
  let d = draft;
  const renamed = new Map<string, string>();
  const applied: PageCommand[] = [];
  for (const planned of commands) {
    const one = applyOne(d, planned, renamed, fresh, tag);
    d = one.draft;
    applied.push(one.applied);
  }
  return { draft: d, applied, renamed: Object.fromEntries(renamed) };
}

const sheetOf = (page: PageDocument, tile: string): PageSheet | undefined => page.sheets.find((s) => tiles(s.layout).includes(tile));
const gridShowing = (page: PageDocument, cube: string): PageView | undefined => page.views.find((v) => v.kind === 'grid' && v.cube === cube);

/**
 * The commands that take `from` to `to` (above), naming `to`'s ids; none when they are the same page. Each is applied as
 * it is planned, over `from` with fresh ids where `to`'s are held, so the plan is checked as it is built: a refusal
 * (CubeRefusal) is a difference no page command makes, said in words.
 */
export function pageBetween(from: PageDocument, to: PageDocument, scope: PageScope): PageCommand[] {
  const out: PageCommand[] = [];
  let sim: PageDraft = { page: from, held: idsOf(from) };
  const renamed = new Map<string, string>();
  const n = (id: string): string => renamed.get(id) ?? id;
  const cur = (): PageDocument => sim.page;
  const emit = (planned: PageCommand): void => {
    sim = applyOne(sim, planned, renamed, true).draft;
    out.push(planned);
  };
  const inFrom = (id: string): boolean => from.views.some((v) => v.id === id);
  const inTo = (id: string): boolean => to.views.some((v) => v.id === id);

  // 1. sheets `to` has
  for (const s of to.sheets) if (!from.sheets.some((x) => x.id === s.id)) emit({ kind: 'addSheet', id: s.id, name: s.name ?? null });

  // 2. grids `to` has, with their cubes, on their sheets (a band of their own: the layout follows below)
  for (const v of to.views) {
    if (v.kind !== 'grid' || inFrom(v.id)) continue;
    const sheet = sheetOf(to, v.id);
    if (sheet === undefined) throw new CubeRefusal(`'${v.id}' is on no sheet of the page: it cannot be made`);
    const document = to.cubes.find((c) => c.id === v.cube)?.cube ?? null;
    emit({ kind: 'addGrid', id: v.id, document, sheet: sheet.id, near: null, title: v.title ?? null, columns: null });
  }

  // 2b. text tiles `to` has, on their sheets (a band of their own: the layout follows below); the ones both have, their
  //     words and their title as `to` has them
  for (const v of to.views) {
    if (v.kind !== 'text') continue;
    // made when `from` has it not -- never found by its id here, which a tile made under a fresh id may hold by now
    if (!inFrom(v.id)) {
      const sheet = sheetOf(to, v.id);
      if (sheet === undefined) throw new CubeRefusal(`'${v.id}' is on no sheet of the page: it cannot be made`);
      emit({ kind: 'addText', id: v.id, sheet: sheet.id, text: v.text, title: v.title ?? null, columns: null });
      continue;
    }
    const here = cur().views.find((x): x is TextView => x.kind === 'text' && x.id === n(v.id));
    if (here === undefined) continue;
    if (here.text !== v.text) emit({ kind: 'setText', tile: v.id, text: v.text });
    if ((here.title ?? null) !== (v.title ?? null)) emit({ kind: 'renameTile', tile: v.id, title: v.title ?? null });
  }

  // 3. the charts both have: pointed at their grid in `to` (before the grid they read goes) -- or, detached in `to` (no
  //    grid shows their cube), on that kept cube as `to` holds it (`detach`, W19), unless a grid here still shows it (its
  //    removal detaches them, below: 5b) -- their spec, their title
  const keptIn = (page: PageDocument, cube: string): CubeDocument => {
    const document = page.cubes.find((c) => c.id === cube)?.cube;
    if (document === undefined) throw new CubeRefusal(`a chart reads ${cube}, which the page does not hold`);
    return document;
  };
  // the copy a kept cube reads in `to` (a detached chart's: carried by `detach`, which has no grid to snap for it)
  const copyIn = (page: PageDocument, cube: string): GridCopy | undefined => (page.copies ?? {})[cube];
  // the copy said always: `to`'s, or null for its live rows (the field absent leaves a copy as it is)
  const detachOnto = (v: ChartView): PageCommand => ({ kind: 'detach', chart: v.id, cube: v.cube, document: keptIn(to, v.cube), copy: copyIn(to, v.cube) ?? null });
  const detachNeeded = (v: ChartView): boolean => {
    const here = cur().views.find((x) => x.id === n(v.id)) as ChartView | undefined;
    if (here === undefined) return false;
    const kept = cur().cubes.find((c) => c.id === n(v.cube))?.cube;
    return here.cube !== n(v.cube) || !sameValue(kept, keptIn(to, v.cube)) || !sameValue(copyIn(cur(), n(v.cube)), copyIn(to, v.cube));
  };
  const detachedIn = (page: PageDocument, v: ChartView): boolean => gridShowing(page, v.cube) === undefined;
  // a chart's spec and title as `to` has them: set when `from`'s differ (the step changed them: an undo puts them back as
  // the step saw them, whatever the chart followed since), or when the page here still differs
  const specAndTitle = (v: ChartView): void => {
    const now = (): ChartView => cur().views.find((x) => x.id === n(v.id)) as ChartView;
    const was = from.views.find((x) => x.id === v.id) as ChartView | undefined;
    if (!sameValue(now().spec, v.spec) || (was !== undefined && !sameValue(was.spec, v.spec))) emit({ kind: 'setChart', chart: v.id, spec: v.spec });
    if (now().title !== v.title || (was !== undefined && was.title !== v.title)) emit({ kind: 'renameTile', tile: v.id, title: v.title });
  };
  // the detached charts whose kept cube a grid here still shows: pointed, and their spec and title set, after it goes (5b)
  const later = new Set<string>();
  for (const v of to.views) {
    if (v.kind !== 'chart' || !inFrom(v.id)) continue;
    const grid = gridShowing(to, v.cube);
    const here = cur().views.find((x) => x.id === n(v.id)) as ChartView | undefined;
    if (here === undefined) continue;
    if (grid === undefined) {
      if (gridShowing(cur(), n(v.cube)) !== undefined) {
        later.add(v.id);
        continue;
      }
      if (detachNeeded(v)) emit(detachOnto(v));
    } else {
      const simGrid = cur().views.find((x) => x.id === n(grid.id));
      if (here.cube !== (simGrid?.cube ?? v.cube)) emit({ kind: 'followGrid', chart: v.id, grid: grid.id });
    }
    specAndTitle(v);
  }

  // 4. grid titles
  for (const v of to.views) {
    if (v.kind !== 'grid') continue;
    const here = cur().views.find((x) => x.id === n(v.id));
    if (here !== undefined && (here.title ?? null) !== (v.title ?? null)) emit({ kind: 'renameTile', tile: v.id, title: v.title ?? null });
  }

  // 5. tiles `to` has not: grids first (a grid takes the charts following it, a frozen one stays detached), then charts
  //    -- a copy `to` keeps for a cube no grid of its shows (a detached chart's) snapped first, while the grid is here
  for (const [cube, copy] of Object.entries(to.copies ?? {})) {
    if (gridShowing(to, cube) !== undefined) continue;
    const grid = gridShowing(cur(), cube);
    if (grid !== undefined && !sameValue((cur().copies ?? {})[cube], copy)) emit({ kind: 'snap', grid: grid.id, copy });
  }
  for (const v of from.views) if (v.kind === 'grid' && !inTo(v.id)) emit({ kind: 'removeTile', tile: v.id });
  for (const v of from.views) if (v.kind === 'text' && !inTo(v.id)) emit({ kind: 'removeTile', tile: v.id });
  for (const v of from.views) {
    if (v.kind === 'chart' && !inTo(v.id) && cur().views.some((x) => x.id === v.id)) emit({ kind: 'removeTile', tile: v.id });
  }

  // 5b. the charts both have that a grid's removal left detached: on their kept cube as `to` holds it (W19), their spec
  //     and title
  for (const v of to.views) {
    if (v.kind !== 'chart' || !later.has(v.id) || !detachedIn(to, v) || !cur().views.some((x) => x.id === n(v.id))) continue;
    if (detachNeeded(v)) emit(detachOnto(v));
    specAndTitle(v);
  }

  // 6. the charts `to` has that the page has not (never had, or lost with their grid), made of their grid -- a detached
  //    one made of a grid here, any (one made for it, then taken off, when the page has none), then on its own kept
  //    cube (W19); then every chart's selection as `to` has it (after the removals: a grid taken off clears its detached
  //    charts' selections, a chart taken off has its cube's others judged, as `to` was left by the same)
  for (const v of to.views) {
    if (v.kind !== 'chart' || cur().views.some((x) => x.id === n(v.id))) continue;
    const grid = gridShowing(to, v.cube);
    if (grid !== undefined) {
      emit({ kind: 'addChart', id: v.id, near: grid.id, spec: v.spec, title: v.title, columns: null });
      continue;
    }
    const document = keptIn(to, v.cube);
    let near = cur().views.find((x) => x.kind === 'grid')?.id;
    let made: string | undefined;
    if (near === undefined) {
      const sheet = sheetOf(to, v.id) ?? to.sheets[0];
      if (sheet === undefined) throw new CubeRefusal(`'${v.id}' is on no sheet of the page: it cannot be made`);
      made = nextPageId(takenIds(sim), 'grid');
      emit({ kind: 'addGrid', id: made, document, sheet: sheet.id, near: null, title: null, columns: null });
      near = made;
    }
    emit({ kind: 'addChart', id: v.id, near, spec: v.spec, title: v.title, columns: null });
    emit(detachOnto(v));
    if (made !== undefined) emit({ kind: 'removeTile', tile: made });
    specAndTitle(v);
  }
  for (const v of to.views) {
    if (v.kind !== 'chart') continue;
    const here = cur().views.find((x) => x.id === n(v.id)) as ChartView | undefined;
    if (here !== undefined && !sameValue(here.selection ?? [], v.selection ?? [])) emit({ kind: 'select', chart: v.id, conditions: v.selection ?? [] });
  }

  // 6b. each tile on its sheet (a chart made under its grid may belong on another)
  for (const v of to.views) {
    const want = sheetOf(to, v.id);
    const have = sheetOf(cur(), n(v.id));
    if (want !== undefined && have !== undefined && n(want.id) !== have.id) emit({ kind: 'moveTile', tile: v.id, sheet: want.id, columns: null });
  }

  // 7. page filters: off, then set as `to` has them
  for (const f of from.filters ?? []) if (!(to.filters ?? []).some((x) => x.id === f.id)) emit({ kind: 'removePageFilter', filter: f.id });
  for (const f of to.filters ?? []) {
    const here = (cur().filters ?? []).find((x) => x.id === n(f.id));
    const from_ = f.from === undefined ? null : f.from;
    if (here !== undefined && here.class === f.class && sameValue(here.condition, f.condition) && (here.from ?? null) === (from_ === null ? null : n(from_))) continue;
    emit({ kind: 'setPageFilter', id: f.id, class: f.class, condition: f.condition, from: from_ });
  }

  // 8. each sheet's layout whole, its tiles agreeing now
  for (const s of to.sheets) {
    const here = cur().sheets.find((x) => x.id === n(s.id));
    if (here === undefined) continue;
    const planned: PageCommand = { kind: 'layout', sheet: s.id, layout: s.layout };
    const asHere = renamedCommand(planned, renamed) as Extract<PageCommand, { kind: 'layout' }>;
    if (!sameValue(here.layout, asHere.layout)) emit(planned);
  }

  // 9. sheets `to` has not; names; order
  for (const s of from.sheets) if (!to.sheets.some((x) => x.id === s.id)) emit({ kind: 'removeSheet', sheet: s.id });
  for (const s of to.sheets) {
    const here = cur().sheets.find((x) => x.id === n(s.id));
    if (here !== undefined && (here.name ?? null) !== (s.name ?? null)) emit({ kind: 'renameSheet', sheet: s.id, name: s.name ?? null });
  }
  for (let i = 0; i < to.sheets.length; i += 1) {
    const sheet = to.sheets[i]!;
    if (cur().sheets[i]?.id !== n(sheet.id)) emit({ kind: 'moveSheet', sheet: sheet.id, to: i });
  }

  // 10. copies: a grid live again where `to` has no copy or another, snapped where `to` has one it has not
  const cubeInTo = new Map<string, string>();
  for (const v of to.views) {
    if (v.kind !== 'grid') continue;
    const here = cur().views.find((x): x is GridView => x.kind === 'grid' && x.id === n(v.id));
    if (here !== undefined) cubeInTo.set(here.cube, v.cube);
  }
  for (const [cube, copy] of Object.entries(cur().copies ?? {})) {
    const theirs = cubeInTo.get(cube) ?? cube;
    const want = (to.copies ?? {})[theirs];
    if (want !== undefined && sameValue(want, copy)) continue;
    const grid = gridShowing(cur(), cube);
    if (grid === undefined) {
      // a kept cube's copy (a detached chart's): no grid to go live or snap for it -- its chart put on the cube again,
      // the copy as `to` has it (detach carries the copy); a cube `to` does not hold went with its chart (5)
      const chart = to.views.find((x): x is ChartView => x.kind === 'chart' && x.cube === theirs);
      if (chart !== undefined && !sameValue(copyIn(cur(), cube), copyIn(to, theirs))) emit(detachOnto(chart));
      continue;
    }
    emit({ kind: 'live', grid: grid.id });
  }
  for (const [cube, copy] of Object.entries(to.copies ?? {})) {
    const grid = gridShowing(to, cube);
    if (grid === undefined) continue;
    const here = cur().views.find((x): x is GridView => x.kind === 'grid' && x.id === n(grid.id));
    const have = here === undefined ? undefined : (cur().copies ?? {})[here.cube];
    if (!sameValue(have, copy)) emit({ kind: 'snap', grid: grid.id, copy });
  }

  if (scope === 'history') return out;

  // 11. the whole page: its name, its saved record, its model (made and kept, never unmade or moved), and no cube opened
  //     again over its source (a page is then opened again, not followed)
  if (cur().name !== to.name) emit({ kind: 'renamePage', name: to.name });
  if ((cur().record ?? null) !== (to.record ?? null)) emit({ kind: 'savedAs', record: to.record ?? null });
  const have = cur().model;
  const want = to.model;
  if (want === undefined && have !== undefined) throw new CubeRefusal("the page's model cannot be unmade: a model action's record is kept (D5)");
  if (want !== undefined) {
    if (have === undefined) emit({ kind: 'modelInto', project: want.project, workspace: want.workspace, package: want.package });
    else if (have.project !== want.project || have.workspace !== want.workspace || have.package !== want.package) {
      throw new CubeRefusal(`the page's model is ${have.project}: it cannot become ${want.project}`);
    }
    if (want.revision !== undefined && cur().model?.revision !== want.revision) emit({ kind: 'modelPin', revision: want.revision });
    else if (want.revision === undefined && have?.revision !== undefined) throw new CubeRefusal("the page's pin cannot be taken off: a model action moves it");
    const cubes = new Set([...Object.keys(want.tables), ...Object.keys(want.classes ?? {})]);
    for (const theirs of cubes) {
      const grid = gridShowing(to, theirs);
      const cube = grid === undefined ? theirs : (cur().views.find((x) => x.id === n(grid.id))?.cube ?? theirs);
      const tables = want.tables[theirs];
      const cls = want.classes?.[theirs];
      const keptTables = cur().model?.tables[cube];
      const keptClass = cur().model?.classes?.[cube];
      const sameTables = tables === undefined || sameValue(keptTables, tables);
      const sameClass = cls === undefined || keptClass === cls;
      if (sameTables && sameClass) continue;
      emit({ kind: 'modelKeep', cube: grid?.id ?? theirs, tables: sameTables ? null : tables ?? null, class: sameClass ? null : cls ?? null });
    }
    for (const cube of Object.keys(have?.tables ?? {})) {
      const theirs = cubeInTo.get(cube) ?? cube;
      if (want.tables[theirs] === undefined && cur().cubes.some((c) => c.id === cube)) {
        throw new CubeRefusal(`the page's model keeps ${cube}'s tables: they are let go with the grid, never alone`);
      }
    }
  }
  for (const c of to.cubes) {
    const here = cur().cubes.find((x) => x.id === c.id);
    if (here !== undefined && !sameValue(here.cube.source, c.cube.source)) {
      throw new CubeRefusal(`'${c.id}' was opened again over its source as it is now: the page is opened again, not followed`);
    }
  }
  return out;
}
