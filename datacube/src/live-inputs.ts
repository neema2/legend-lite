// LIVE INPUTS (docs/DATACUBE_FLOW_MODE_2026_10_24.md §3.1, FL6): a grid over another grid's rows reads that grid of its
// page as it is now. Here are the page's rules for them, with no DOM and no planner: what a source reads live, a source
// resolved against its page -- every live input the copy of what its grid is now, as the Join window copies a grid it
// joins (page-commands.ts joinInputOf) -- and the order a page's cubes open in, so a grid opens after the grids it reads.

import { isLive, withJoinInputs, joinInputs, type CubeInput, type CubeSource, type InputCopy, type InputPort, type JoinInput } from './cube-document.ts';
import type { PageDocument } from './page-document.ts';
import { boxOmitted, boxOrder, boxRenamed } from './box-columns.ts';
import { unmatchedOn } from './join-how.ts';
import { boundIn } from './page-parameters.ts';
import { CubeRefusal } from './snapshot.ts';

/** The grids `source` reads live, each once, in the order it reads them -- through its frozen inputs too. */
export function liveGrids(source: CubeSource): string[] {
  const grids: string[] = [];
  const visit = (s: CubeSource): void => {
    const input = (i: CubeInput): void => {
      if (!isLive(i)) {
        visit(i.source);
        for (const u of i.unmatched ?? []) input(u.by);
      } else if (!grids.includes(i.grid)) grids.push(i.grid);
    };
    if (s._type === 'rows') input(s.input);
    else if (s._type === 'join') joinInputs(s).forEach(input);
  };
  visit(source);
  return grids;
}

/**
 * A GRID OF THE PAGE AS AN INPUT, the one rule (the Join window's and a live input's): what it is called (its title,
 * else its source's name), its source -- resolved, so the copy holds no live input of its own -- and its query. Refused
 * in words when `grid` is no grid of the page, or reads its own rows (`through`: the grids read on the way to it).
 */
export function gridInput(page: PageDocument, grid: string, through: readonly string[] = [], port?: InputPort, copies = false): JoinInput {
  const view = page.views.find((v) => v.id === grid);
  if (view === undefined) throw new CubeRefusal(`'${grid}' is no grid of this page`);
  if (view.kind !== 'grid') throw new CubeRefusal(`'${grid}' is a ${view.kind} tile, not a grid`);
  const cube = page.cubes.find((c) => c.id === view.cube);
  if (cube === undefined) throw new CubeRefusal(`'${grid}' shows no cube of the page`);
  if (through.includes(grid)) {
    throw new CubeRefusal(`'${grid}' reads its own rows (${[...through.slice(through.indexOf(grid)), grid].join(' → ')})`);
  }
  if (port === 'left' || port === 'right') return unmatchedInput(page, grid, cube.cube.source, port, [...through, grid], copies);
  const query = port === undefined ? cube.cube.query : falseQuery(grid, cube.cube.query);
  // a flow's box hands on the columns it shows, under the names it gives them (box-columns.ts); a board's grid, all
  const onFlow = page.sheets.some((s) => s.layout.kind === 'flow' && s.layout.boxes.some((b) => b.tile === grid));
  const omit = onFlow ? boxOmitted(cube.cube.query, cube.cube.configuration) : [];
  const rename = onFlow ? boxRenamed(cube.cube.query, cube.cube.configuration) : [];
  const order = onFlow ? boxOrder(cube.cube.query, cube.cube.configuration) : undefined;
  return {
    name: view.title ?? cube.cube.source.name, source: resolvedOn(page, cube.cube.source, [...through, grid], copies), query,
    ...(omit.length > 0 ? { omit } : {}), ...(rename.length > 0 ? { rename } : {}), ...(order !== undefined ? { order } : {}),
  };
}

/**
 * A GRID'S FALSE AS ITS QUERY (FL7): the same query with its filter turned round -- the rows of its input its
 * condition does not hold for, a row whose value is empty among them: a condition holds for a value only (its compare
 * guarded, `isNotEmpty($x.c) && ...`), so its turn round holds for an empty one, and T and F are every row between them
 * (the correctness audit's 15, 2026-10-26: a Filter's, a Custom Filter's and a Date Filter's each a partition). Refused
 * in words when it has no filter: no row is left out.
 */
function falseQuery(grid: string, query: JoinInput['query']): JoinInput['query'] {
  const filter = query.filter;
  if (filter === undefined) throw new CubeRefusal(`'${grid}' has no filter: its False port leaves no row out`);
  return { ...query, filter: filter.kind === 'not' ? filter.child : { kind: 'not', child: filter } };
}

/**
 * A JOIN'S L OR R (§3.3; Alteryx's): that input itself, resolved -- its rows, its columns as it hands them on -- each of
 * its rows the other input's pair with none (`JoinInput.unmatched`: an anti-join the database runs, join-open.ts), on its
 * join's condition, turned round for its Right (join-how.ts unmatchedOn). The join's own query is its J's, and reaches
 * neither. Refused in words: a grid that is no join, a join with no rows left unmatched (a Union) or whose condition an
 * anti-join cannot say.
 */
function unmatchedInput(page: PageDocument, grid: string, source: CubeSource, side: 'left' | 'right', through: readonly string[],
  copies: boolean): JoinInput {
  if (source._type !== 'join') throw new CubeRefusal(`'${grid}' is no join: it has no Left and Right unmatched`);
  const on = unmatchedOn(source.how, side);
  const [mine, theirs] = side === 'left' ? [source.left, source.right] : [source.right, source.left];
  const rows = resolvedInput(page, mine, through, copies);
  return { ...rows, unmatched: [...(rows.unmatched ?? []), { by: resolvedInput(page, theirs, through, copies), on }] };
}

/** An input resolved (resolvedOn's): a live one the copy of its grid now, a copy with each source in it resolved. */
function resolvedInput(page: PageDocument, i: CubeInput, through: readonly string[], copies: boolean): JoinInput {
  if (!isLive(i)) return copyWith(i, (s) => resolvedOn(page, s, through, copies));
  const input = gridInput(page, i.grid, through, i.port, copies);
  // to be opened: a snapped grid read by its copy of its source's rows, its query over them (§18.8; a cached box, FL10)
  // -- but a Join's L or R, whose rows are its inputs', which no copy of its own rows holds
  const copy = copies && i.port !== 'left' && i.port !== 'right' ? copyOf(page, i.grid) : undefined;
  return copy === undefined ? input : { ...input, copy };
}

/** The copy grid `grid` reads now, as the page records it (page-document.ts copies, by its cube), or undefined. */
function copyOf(page: PageDocument, grid: string): InputCopy | undefined {
  const view = page.views.find((v) => v.id === grid);
  const copy = view?.kind === 'grid' ? page.copies?.[view.cube] : undefined;
  return copy === undefined ? undefined : { table: copy.table, label: copy.label, takenAt: copy.takenAt };
}

/** A copy with `f` made of its source and of each source of the inputs its rows are matched against. */
function copyWith(i: JoinInput, f: (s: CubeSource) => CubeSource): JoinInput {
  return { ...i, source: f(i.source), ...(i.unmatched !== undefined ? { unmatched: i.unmatched.map((u) => ({ ...u, by: copyWith(u.by, f) })) } : {}) };
}

/**
 * A SOURCE RESOLVED AGAINST ITS PAGE: every live input in it -- another grid's rows', a join's inputs' -- the copy of
 * what its grid is now (gridInput), so whatever asks what a grid reads (its leaves, its base query) asks a source with no
 * link left in it. `grid`, the grid whose source it is, if it is one of the page's: a source that reads it back is refused.
 * `copies`, resolved to be OPENED: a snapped grid's input reads its copy (JoinInput.copy) -- what a grid's rows are read
 * from now; without it, what they are a copy OF (a class maps those tables: §18.8 item 3).
 */
export function resolvedSource(page: PageDocument, source: CubeSource, grid?: string, o: { readonly copies?: boolean } = {}): CubeSource {
  // its page's parameters bound into it (page-parameters.ts): the base a box is opened over reads their values now, as
  // its own steps' queries read them through its lens
  return boundIn(resolvedOn(page, source, grid === undefined ? [] : [grid], o.copies === true), page.parameters ?? []);
}

function resolvedOn(page: PageDocument, source: CubeSource, through: readonly string[], copies = false): CubeSource {
  const input = (i: CubeInput): JoinInput => resolvedInput(page, i, through, copies);
  switch (source._type) {
    case 'rows': return { ...source, input: input(source.input) };
    case 'join': return withJoinInputs(source, input);
    default: return source;
  }
}

/** A source with every grid its links name renamed (`n`: an id to the id it is now), as the page's ids are renamed
 *  (page-rebase.ts renamedCommand). */
export function relinked(source: CubeSource, n: (id: string) => string): CubeSource {
  const input = (i: CubeInput): CubeInput => relinkedInput(i, n);
  switch (source._type) {
    case 'rows': return { ...source, input: input(source.input) };
    case 'join': return withJoinInputs(source, input);
    default: return source;
  }
}

/** An input with every grid its links name renamed (relinked's rule, a page command's input: setInput). */
export function relinkedInput(input: CubeInput, n: (id: string) => string): CubeInput {
  return isLive(input) ? { ...input, grid: n(input.grid) } : copyWith(input, (s) => relinked(s, n));
}

/**
 * A SOURCE WITH ITS LINKS TO `grid` FROZEN (FL6: a grid removed while others read it live): each live input naming it
 * replaced by `copy` of its port (none: its rows), what that grid was (gridInput) -- through its frozen inputs too; every
 * other link as it is.
 */
export function frozenFrom(source: CubeSource, grid: string, copy: (port: InputPort | undefined) => JoinInput): CubeSource {
  const input = (i: CubeInput): CubeInput => (isLive(i) ? (i.grid === grid ? copy(i.port) : i) : copyWith(i, (s) => frozenFrom(s, grid, copy)));
  switch (source._type) {
    case 'rows': return { ...source, input: input(source.input) };
    case 'join': return withJoinInputs(source, input);
    default: return source;
  }
}

/**
 * Whether a page's live inputs all hold: each names a grid of the page, and none reads its own rows -- the problem in
 * words, else undefined. A page read (page-document.ts readPage) is held to it.
 */
export function liveProblem(page: PageDocument): string | undefined {
  for (const view of page.views) {
    if (view.kind !== 'grid') continue;
    const cube = page.cubes.find((c) => c.id === view.cube);
    if (cube === undefined || liveGrids(cube.cube.source).length === 0) continue;
    try {
      resolvedSource(page, cube.cube.source, view.id);
    } catch (e) {
      if (e instanceof CubeRefusal) return `${view.id}: ${e.message}`;
      throw e;
    }
  }
  return undefined;
}

/**
 * `cubes` in an order that opens each grid after every grid it reads live (`gridOf`: the grid a cube is shown by), the
 * order they came in kept otherwise -- a page's other rules for its opening order (page-document.ts openingOrder) stand.
 * A cube that reads a grid not among them, or one in a cycle, keeps its place: what it reads is refused when it opens.
 */
export function afterWhatTheyRead<T extends { readonly cube: { readonly source: CubeSource } }>(
  cubes: readonly T[], gridOf: (cube: T) => string | undefined,
): T[] {
  const byGrid = new Map(cubes.flatMap((c) => {
    const g = gridOf(c);
    return g === undefined ? [] : [[g, c] as const];
  }));
  const placed = new Set<T>();
  const order: T[] = [];
  const place = (c: T, visiting: Set<T>): void => {
    if (placed.has(c) || visiting.has(c)) return;
    visiting.add(c);
    for (const g of liveGrids(c.cube.source)) {
      const read = byGrid.get(g);
      if (read !== undefined) place(read, visiting);
    }
    visiting.delete(c);
    if (!placed.has(c)) {
      placed.add(c);
      order.push(c);
    }
  };
  for (const c of cubes) place(c, new Set());
  return order;
}
