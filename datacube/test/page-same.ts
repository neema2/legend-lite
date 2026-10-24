// THE SAME PAGE, UP TO THE IDS GIVEN OUT AFRESH: two test helpers for what an undo makes (docs/DATACUBE_LIVE_EDITING_2026_10_24.md
// §3, B12: a tile a step brings back is a new tile, under a new id) -- `renamedPage`, a page with its ids renamed as a
// move said they were (planned -> given), and `canonical`, a page as content in the screen's order, in a scope.

import { inScreenOrder, type PageCommand } from '../src/page-commands.ts';
import { pageToJson, type PageDocument } from '../src/page-document.ts';
import type { PageScope } from '../src/page-between.ts';
import { renamedCommand } from '../src/page-rebase.ts';

/** `page` with its ids renamed as `given` says (planned -> given): what a move that brought a tile back makes of it. */
export function renamedPage(page: PageDocument, given: Readonly<Record<string, string>>): PageDocument {
  // a grid made again shows a cube of its own id (page-commands.ts addGrid): the cube it showed follows its view's rename
  const renamed: Record<string, string> = { ...given };
  for (const v of page.views) if (v.kind === 'grid' && given[v.id] !== undefined && v.cube !== v.id) renamed[v.cube] = given[v.id]!;
  const map = new Map(Object.entries(renamed));
  const n = (id: string): string => renamed[id] ?? id;
  const keys = <V>(o: Readonly<Record<string, V>> | undefined): Record<string, V> | undefined =>
    (o === undefined ? undefined : Object.fromEntries(Object.entries(o).map(([k, v]) => [n(k), v])));
  return {
    ...page,
    views: page.views.map((v) => (v.kind === 'text' ? { ...v, id: n(v.id) } : { ...v, id: n(v.id), cube: n(v.cube) })),
    cubes: page.cubes.map((c) => ({ id: n(c.id), cube: c.cube })),
    sheets: page.sheets.map((s) => ({
      ...s, id: n(s.id), layout: (renamedCommand({ kind: 'layout', sheet: s.id, layout: s.layout }, map) as Extract<PageCommand, { kind: 'layout' }>).layout,
    })),
    ...(page.filters ? { filters: page.filters.map((f) => ({ ...f, id: n(f.id), ...(f.from !== undefined ? { from: n(f.from) } : {}) })) } : {}),
    ...(page.copies ? { copies: keys(page.copies)! } : {}),
    ...(page.model ? { model: { ...page.model, tables: keys(page.model.tables)!, ...(page.model.classes ? { classes: keys(page.model.classes)! } : {}) } } : {}),
  };
}

/**
 * A page as content, in the screen's order -- but for the charts' and the kept cubes' order among themselves, which is
 * the order they came in (page-commands.ts inScreenOrder): a chart made again comes last, the same page. `history`:
 * without what a step of the history leaves alone (its name, which each cube carries too, its saved record, its model).
 */
export function canonical(page: PageDocument, scope: PageScope): unknown {
  const ordered = inScreenOrder(page);
  const byId = <T extends { readonly id: string }>(xs: readonly T[]): T[] => [...xs].sort((a, b) => a.id.localeCompare(b.id));
  const shown = new Set(ordered.views.filter((v) => v.kind === 'grid').map((v) => v.cube));
  const same = {
    ...ordered,
    views: [...ordered.views.filter((v) => v.kind === 'grid'), ...byId(ordered.views.filter((v) => v.kind === 'chart'))],
    cubes: [...ordered.cubes.filter((c) => shown.has(c.id)), ...byId(ordered.cubes.filter((c) => !shown.has(c.id)))],
  };
  if (scope === 'whole') return JSON.parse(pageToJson(same));
  const { name: _name, record: _record, model: _model, ...structure } = same;
  return JSON.parse(pageToJson({ ...structure, name: 'x', cubes: structure.cubes.map((c) => ({ id: c.id, cube: { ...c.cube, name: 'x' } })) }));
}

/**
 * The ids `to` gave out afresh for `from`'s (planned -> given), read off the two pages: each id of `from` that `to` has
 * not, paired with the id of `to` that `from` has not, of the same kind, in order -- what a move said (PageMoved.renamed)
 * when the test has only the pages. A grid's cube follows its view (renamedPage).
 */
export function renaming(from: PageDocument, to: PageDocument): Record<string, string> {
  const kindOf = (id: string): string => id.replace(/-?\d+$/, '');
  // its tiles, sheets and filters -- and its cubes: a kept cube follows the grid it came from, brought back under a new id
  const ids = (page: PageDocument): string[] => [...new Set([
    ...page.views.map((v) => v.id), ...page.sheets.map((s) => s.id), ...(page.filters ?? []).map((f) => f.id),
    ...page.cubes.map((c) => c.id),
  ])];
  const gone = ids(from).filter((id) => !ids(to).includes(id));
  const fresh = ids(to).filter((id) => !ids(from).includes(id));
  const renamed: Record<string, string> = {};
  for (const id of gone) {
    const i = fresh.findIndex((f) => kindOf(f) === kindOf(id));
    if (i < 0) continue;
    renamed[id] = fresh[i]!;
    fresh.splice(i, 1);
  }
  return renamed;
}

/** The two pages the same, up to the ids `to` gave out afresh (an undo's new tiles), in the history's scope. */
export function sameButIds(from: PageDocument, to: PageDocument, scope: PageScope = 'history'): { readonly same: boolean; readonly renamed: Record<string, string>; readonly a: unknown; readonly b: unknown } {
  const renamed = renaming(from, to);
  const a = canonical(renamedPage(from, renamed), scope);
  const b = canonical(to, scope);
  return { same: JSON.stringify(a) === JSON.stringify(b), renamed, a, b };
}

/** Where two JSON values differ: each path to a differing leaf (an array index as a number), at most `depth` deep. */
export function differing(a: unknown, b: unknown, path = '', depth = 9): string[] {
  if (JSON.stringify(a) === JSON.stringify(b)) return [];
  const objects = typeof a === 'object' && a !== null && typeof b === 'object' && b !== null;
  if (!objects || depth === 0) return [`${path || '(the page)'}: ${JSON.stringify(a)} vs ${JSON.stringify(b)}`];
  const keys = [...new Set([...Object.keys(a as object), ...Object.keys(b as object)])];
  return keys.flatMap((k) => differing((a as Record<string, unknown>)[k], (b as Record<string, unknown>)[k], path ? `${path}.${k}` : k, depth - 1));
}
