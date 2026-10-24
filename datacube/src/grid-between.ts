// WHAT TAKES A GRID FROM ONE STATE TO ANOTHER, AS COMMANDS (docs/DATACUBE_LIVE_EDITING_2026_10_24.md §3, W1): the grid
// commands (grid-commands.ts) that, applied to `from`, make `to` -- so an undo or a redo is produced by commands like any
// other edit (state-owner.ts toward: the screen's state after it is what the commands make, as the keeper's will be) and
// told as them, and a screen converging on its keeper's page applies the difference. Total over the SAVED form of a grid
// (its query without the types a run learned, its configuration, its tree): every difference two reachable states can
// have is some command's to make, by design -- a calculated column back in its place (`before`), a collapsed tree put
// back (`tree`), a kind or a pivot exclusion cleared (`configure`, null). In dependency order, each command applied to
// the grid as the ones before left it, so each is one the grid takes: the calculated columns `to` has first (a group, a
// sort or a filter may name one), then the Properties window's own order (grid-commands.ts draftCommands: a column made
// a dimension, the groups, the pivot, the other kinds and Aggregations, the settings, the sorts), the filter, the tree,
// and last the calculated columns `to` has not (nothing of `to` reads them). Pure: no DOM, no planner.

import { treeDocument } from './cube-document.ts';
import { applyGridCommand, draftCommands, type GridCommand } from './grid-commands.ts';
import type { GridDraft } from './grid-edits.ts';
import { defaultKind } from '../../engine-client/src/types.ts';
import { sameValue, type DerivedColumn } from './snapshot.ts';

type Level = 'row' | 'group';

const atLevel = (d: GridDraft, level: Level): readonly DerivedColumn[] =>
  (level === 'group' ? d.snapshot.groupDerived ?? [] : d.snapshot.derived);
const allCalculated = (d: GridDraft): readonly DerivedColumn[] => [...d.snapshot.derived, ...(d.snapshot.groupDerived ?? [])];
/** A calculated column as its editor declared it: its type is what a run learned, or what the editor copied, no part of
 *  the edit (cube-document.ts writeCube strips a group-level one; a row-level one the compiler types again). */
const declared = ({ type: _learned, ...column }: DerivedColumn): Omit<DerivedColumn, 'type'> => column;

/**
 * The commands that take `from` to `to`, in an order every one applies in; none when they are the same grid. Each is
 * applied as it is made, so the list is checked as it is built: a refusal (CubeRefusal) is a pair no command reaches,
 * which the tests over random edits say does not exist (test/grid-between.test.ts).
 */
export function gridBetween(from: GridDraft, to: GridDraft): GridCommand[] {
  const out: GridCommand[] = [];
  let cur: GridDraft = from;
  const emit = (command: GridCommand): void => {
    cur = applyGridCommand(cur, command);
    out.push(command);
  };
  const postponed: GridCommand[] = [];
  // 1. the calculated columns `to` has, each level in `to`'s order: one the grid has not is added before the first of
  //    `to`'s later columns the grid has (so it lands in its place); one it has, edited where its definition, its level
  //    or its place differs (an edit keeps the place when the level stays, else goes before the same), left alone otherwise
  for (const level of ['row', 'group'] as const) {
    const want = atLevel(to, level);
    for (let i = 0; i < want.length; i += 1) {
      const column = want[i]!;
      const names = atLevel(cur, level).map((d) => d.name);
      const later = want.slice(i + 1).map((d) => d.name).find((n) => names.includes(n));
      const before = later === undefined ? {} : { before: later };
      const have = allCalculated(cur).find((d) => d.name === column.name);
      if (have === undefined) {
        emit({ kind: 'calculate', level, column, ...before });
        continue;
      }
      const at = names.indexOf(column.name);
      // placed: at this level, after every earlier column of `to`'s the grid has, before the next one it has
      const placedAfter = Math.max(-1, ...want.slice(0, i).map((d) => names.indexOf(d.name)));
      const placed = at >= 0 && at > placedAfter && (later === undefined || at < names.indexOf(later));
      if (placed && sameValue(declared(have), declared(column))) continue;
      if (!placed && at >= 0 && later === undefined) {
        // the columns before it are placed in order before the next of `to`'s, which is this one: it is after them
        throw new Error(`gridBetween: ${column.name} is out of place with nothing to go before`);
      }
      const edit: GridCommand = { kind: 'editCalculated', name: column.name, level, column, ...(placed ? {} : before) };
      // one grouped or pivoted on keeps its kind and its level (grid-commands.ts editCalculated): changed in `to`, it is
      // grouped by in `to` no more (a group takes a dimension at the row level), so it is edited once the structure has
      // let it go (2026-10-24, found by the undo's random walks)
      const kind = (d: DerivedColumn): string => d.kind ?? defaultKind(d.type);
      const grouped = cur.snapshot.rows.includes(column.name) || cur.snapshot.pivotOn.includes(column.name);
      const haveLevel: Level = cur.snapshot.derived.some((d) => d.name === column.name) ? 'row' : 'group';
      if (grouped && (haveLevel !== level || kind(have) !== kind(column))) postponed.push(edit);
      else emit(edit);
    }
  }
  // 2. the structure, the settings and the sorts, as the Properties window's Apply builds them (a kind or an exclusion
  //    `to` has none for cleared by the settings); the filter whole; the tree whole, after the settings have moved it
  //    -- a top N `to` has not, or has another, taken off first (a pivot `to` has is refused while one holds), and set
  //    after the filter
  const topN = to.snapshot.topN ?? null;
  const topNMoves = !sameValue(cur.snapshot.topN ?? null, topN);
  if (topNMoves && cur.snapshot.topN !== undefined) emit({ kind: 'topN', topN: null });
  for (const command of draftCommands(cur, to)) emit(command);
  for (const command of postponed) emit(command);
  const filter = to.snapshot.filter ?? null;
  if (!sameValue(cur.snapshot.filter ?? null, filter)) emit({ kind: 'filter', filter });
  if (topNMoves && topN !== null) emit({ kind: 'topN', topN });
  if (cur.tree !== undefined && to.tree !== undefined && cur.tree.key !== to.tree.key) emit({ kind: 'tree', ...treeDocument(to.tree) });
  // 3. the calculated columns `to` has not, last: nothing `to` groups, sorts or filters by names them any more
  const kept = new Set(allCalculated(to).map((d) => d.name));
  for (const d of allCalculated(cur)) if (!kept.has(d.name)) emit({ kind: 'removeCalculated', name: d.name });
  return out;
}
