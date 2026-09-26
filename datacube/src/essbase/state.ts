// Essbase mode's grid: Smart View ad hoc analysis, as pure state.
//
// A grid has three axes. ROWS and COLUMNS each hold dimensions, nested
// in order, and each dimension shows an explicit list of MEMBERS -- at
// any mix of generations, the way Smart View's ad hoc grids do after a
// few zooms (2021, then its quarters, then 2022). Every other dimension
// sits on the POV, pinned to one member. Measures are a dimension too
// (user decision, docs/ESSBASE_MODE.md): placeable anywhere, subject to
// Keep Only and Remove Only like the rest.
//
// Everything here is pure: an operation takes a grid and returns a new
// one (or the SAME one when it changes nothing, so a caller can skip
// the query). Members a zoom needs -- a member's children, its
// descendants, its bottom level -- are looked up by the caller (a query
// against the source) and handed in; nothing here reads data.

/** A member: its values from the first generation down. [] is the top. */
export type MemberPath = readonly string[];

/** The Measures dimension's name, and its members' generation. */
export const MEASURES = 'Measures';

export interface OutlineDimension {
  readonly name: string;
  /**
   * The columns of its generations, coarsest first (Year, Quarter,
   * Month). For Measures: empty -- its members are the measures.
   */
  readonly generations: readonly string[];
}

export interface Outline {
  readonly dimensions: readonly OutlineDimension[];
  /** The Measures dimension's members, in their configured order. */
  readonly measures: readonly string[];
}

export interface AxisDimension {
  readonly dimension: string;
  /** Shown in this order; a member's ancestors and descendants may be here too. */
  readonly members: readonly MemberPath[];
}

export type Axis = 'rows' | 'columns';
export type ZoomLevel = 'next' | 'all' | 'bottom';

export interface EssbaseOptions {
  /** Smart View's Zoom In level. */
  readonly zoomLevel: ZoomLevel;
  /** Parents above their members (TOP, the default by the user's decision) or below. */
  readonly ancestorPosition: 'top' | 'bottom';
  readonly suppressMissingRows: boolean;
  readonly suppressZeroRows: boolean;
  readonly suppressRepeatedMembers: boolean;
  readonly suppressMissingColumns: boolean;
  readonly suppressZeroColumns: boolean;
  readonly indentation: 'subitems' | 'totals' | 'none';
  /** Move members around without querying; refresh once at the end. */
  readonly navigateWithoutData: boolean;
}

export const DEFAULT_OPTIONS: EssbaseOptions = {
  zoomLevel: 'next',
  ancestorPosition: 'top',
  // The user's decision: missing rows suppressed by default, zero and
  // repeated-member suppression off until chosen.
  suppressMissingRows: true,
  suppressZeroRows: false,
  suppressRepeatedMembers: false,
  suppressMissingColumns: false,
  suppressZeroColumns: false,
  indentation: 'subitems',
  navigateWithoutData: false,
};

export interface EssbaseGrid {
  readonly rows: readonly AxisDimension[];
  readonly columns: readonly AxisDimension[];
  /** Every dimension not on an axis, pinned to one member. */
  readonly pov: Readonly<Record<string, MemberPath>>;
  readonly options: EssbaseOptions;
}

// -- starting a grid ------------------------------------------------------

/**
 * Smart View's opening grid: the first dimension down the rows at its
 * top member, the measures across the columns, everything else on the
 * POV at its top.
 */
export function initialGrid(outline: Outline, options = DEFAULT_OPTIONS): EssbaseGrid {
  const regular = outline.dimensions.filter((d) => d.name !== MEASURES);
  const first = regular[0];
  const pov: Record<string, MemberPath> = {};
  for (const d of regular.slice(1)) pov[d.name] = [];
  return {
    rows: first ? [{ dimension: first.name, members: [[]] }] : [],
    columns: [{ dimension: MEASURES, members: outline.measures.map((m) => [m]) }],
    pov,
    options,
  };
}

// -- reading a grid -------------------------------------------------------

function axisOf(grid: EssbaseGrid, dimension: string): Axis | null {
  if (grid.rows.some((a) => a.dimension === dimension)) return 'rows';
  if (grid.columns.some((a) => a.dimension === dimension)) return 'columns';
  return null;
}

/** Whether `path` is a descendant of `ancestor` (strictly beneath it). */
export function isDescendant(path: MemberPath, ancestor: MemberPath): boolean {
  return path.length > ancestor.length && ancestor.every((v, i) => path[i] === v);
}

function same(a: MemberPath, b: MemberPath): boolean {
  return a.length === b.length && a.every((v, i) => b[i] === v);
}

/** The tuples an axis shows, outer dimension varying slowest. */
export function tuples(axis: readonly AxisDimension[]): MemberPath[][] {
  let out: MemberPath[][] = [[]];
  for (const d of axis) {
    const next: MemberPath[][] = [];
    for (const prefix of out) for (const m of d.members) next.push([...prefix, m]);
    out = next;
  }
  return axis.length === 0 ? [] : out;
}

// -- operations -----------------------------------------------------------

function withMembers(
  grid: EssbaseGrid,
  dimension: string,
  members: readonly MemberPath[],
): EssbaseGrid {
  const axis = axisOf(grid, dimension);
  if (axis === null) return grid;
  return {
    ...grid,
    [axis]: grid[axis].map((a) => (a.dimension === dimension ? { ...a, members } : a)),
  };
}

function membersOf(grid: EssbaseGrid, dimension: string): readonly MemberPath[] {
  const axis = axisOf(grid, dimension);
  return axis === null ? [] : grid[axis].find((a) => a.dimension === dimension)?.members ?? [];
}

/**
 * ZOOM IN on a member: its descendants the zoom level asks for (looked
 * up by the caller -- the direct children for Next Level, every
 * descendant in hierarchy order for All Levels, the leaves for Bottom
 * Level), placed after the member (ancestor position TOP) or before it
 * (BOTTOM). Descendants already shown are replaced, so zooming a member
 * twice does not repeat its children.
 */
export function zoomIn(
  grid: EssbaseGrid,
  dimension: string,
  member: MemberPath,
  found: readonly MemberPath[],
): EssbaseGrid {
  const list = membersOf(grid, dimension);
  const at = list.findIndex((m) => same(m, member));
  if (at < 0 || found.length === 0) return grid;
  const kept = list.filter((m) => !isDescendant(m, member));
  const where = kept.findIndex((m) => same(m, member));
  const insertAt = grid.options.ancestorPosition === 'top' ? where + 1 : where;
  return withMembers(grid, dimension,
    [...kept.slice(0, insertAt), ...found, ...kept.slice(insertAt)]);
}

/**
 * ZOOM OUT on a member: it and its siblings -- everything under its
 * parent -- collapse into the parent, where the first of them was (or
 * where the parent already is). The top member has nowhere to go.
 */
export function zoomOut(
  grid: EssbaseGrid,
  dimension: string,
  member: MemberPath,
): EssbaseGrid {
  if (member.length === 0) return grid;
  const parent = member.slice(0, -1);
  const list = membersOf(grid, dimension);
  const first = list.findIndex((m) => isDescendant(m, parent));
  if (first < 0) return grid;
  const hasParent = list.some((m) => same(m, parent));
  const out: MemberPath[] = [];
  list.forEach((m, i) => {
    if (i === first && !hasParent) out.push(parent);
    if (!isDescendant(m, parent)) out.push(m);
  });
  return withMembers(grid, dimension, out);
}

/** KEEP ONLY the selected members of a dimension. Keeping none changes nothing. */
export function keepOnly(
  grid: EssbaseGrid,
  dimension: string,
  selected: readonly MemberPath[],
): EssbaseGrid {
  const list = membersOf(grid, dimension);
  const kept = list.filter((m) => selected.some((s) => same(s, m)));
  if (kept.length === 0 || kept.length === list.length) return grid;
  return withMembers(grid, dimension, kept);
}

/** REMOVE ONLY the selected members. An axis never loses its last member. */
export function removeOnly(
  grid: EssbaseGrid,
  dimension: string,
  selected: readonly MemberPath[],
): EssbaseGrid {
  const list = membersOf(grid, dimension);
  const kept = list.filter((m) => !selected.some((s) => same(s, m)));
  if (kept.length === 0 || kept.length === list.length) return grid;
  return withMembers(grid, dimension, kept);
}

/**
 * PIVOT a dimension to the other axis, where it goes last (innermost).
 * Smart View keeps at least one dimension on the rows and one on the
 * columns, so a pivot that would empty an axis changes nothing.
 */
export function pivot(grid: EssbaseGrid, dimension: string): EssbaseGrid {
  const from = axisOf(grid, dimension);
  if (from === null || grid[from].length === 1) return grid;
  const to: Axis = from === 'rows' ? 'columns' : 'rows';
  const moving = grid[from].find((a) => a.dimension === dimension) as AxisDimension;
  return {
    ...grid,
    [from]: grid[from].filter((a) => a.dimension !== dimension),
    [to]: [...grid[to], moving],
  };
}

/**
 * PIVOT TO POV: off the grid, pinned to one member -- the one it shows
 * when it shows one, else its first. Never empties an axis.
 */
export function pivotToPov(grid: EssbaseGrid, dimension: string): EssbaseGrid {
  const from = axisOf(grid, dimension);
  if (from === null || grid[from].length === 1) return grid;
  const members = membersOf(grid, dimension);
  const pinned = members.length === 1 ? members[0] as MemberPath
    : members.find((m) => m.length === 0) ?? members[0] ?? [];
  return {
    ...grid,
    [from]: grid[from].filter((a) => a.dimension !== dimension),
    pov: { ...grid.pov, [dimension]: pinned },
  };
}

/** From the POV onto an axis, showing the member it was pinned to. */
export function povToAxis(grid: EssbaseGrid, dimension: string, axis: Axis): EssbaseGrid {
  const pinned = grid.pov[dimension];
  if (pinned === undefined) return grid;
  const { [dimension]: _gone, ...pov } = grid.pov;
  void _gone;
  return {
    ...grid,
    pov,
    [axis]: [...grid[axis], { dimension, members: [pinned] }],
  };
}

/** Pin a POV dimension to another member. */
export function setPov(grid: EssbaseGrid, dimension: string, member: MemberPath): EssbaseGrid {
  const now = grid.pov[dimension];
  if (now === undefined || same(now, member)) return grid;
  return { ...grid, pov: { ...grid.pov, [dimension]: member } };
}

/** Replace a dimension's members (Member Selection). An empty pick changes nothing. */
export function selectMembers(
  grid: EssbaseGrid,
  dimension: string,
  members: readonly MemberPath[],
): EssbaseGrid {
  if (members.length === 0 || axisOf(grid, dimension) === null) return grid;
  return withMembers(grid, dimension, members);
}

export function withOptions(grid: EssbaseGrid, patch: Partial<EssbaseOptions>): EssbaseGrid {
  return { ...grid, options: { ...grid.options, ...patch } };
}
