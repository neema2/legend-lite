// A TIMESERIES TILE CHANGED (docs/DATACUBE_TIMESERIES_DESIGN_2026_10_25.md §7; the review's A2,
// docs/TIMESERIES_REVIEW_2026_10_25.md): every change a person makes on the tile -- its toolbar, its series editor, its
// Settings, a double-click on its chart -- and every change Python's Timeseries tile makes, by ONE rule written here, in
// no screen's module: the viewer asks it (ui/timeseries-view.ts), and Python asks it headless (headless/timeseries.ts
// timeseriesEdit), as the charts' editChart is asked by both. What it returns is the tile's new spec, read by the tile's
// one reader (timeseries-spec.ts readTimeseries) before it is told as setTimeseries; what it refuses, it says.

import {
  aliasFor, firstPlot, nextSeriesId, NO_SERIES, refsOf, sourceOf, sourcesOf, timeseriesProblems, TsInlineError, wholeOf, type TsColumn,
} from './timeseries-expr.ts';
import { treeOf, writtenText } from './timeseries-text.ts';
import { ALIAS, MAX_PANES, readTimeseries, tsStateOf, type TimeseriesSpec, type TsAlign, type TsAnnotation, type TsExpr, type TsMatch, type TsPane, type TsRange,
  type TsSeries, type TsSource, type TsState, type TsView, type TsGrain } from './timeseries-spec.ts';
import type { FilterNode } from './snapshot.ts';

/** A change the tile refuses, in words. */
export class TsEditRefusal extends Error {}

/** A grid of the page as a source may read it: its cube, its title, its columns as its query types them, its rows, its
 *  filter as it is now (what a frozen tile's new source keeps). */
export interface TsGridColumns {
  readonly cube: string;
  readonly title: string;
  readonly columns: readonly TsColumn[];
  readonly rows: readonly string[];
  readonly filter: FilterNode | undefined;
}

/** A series as an editor tells it: an id it has replaces that series (kept hidden unless told), none (or a new one) adds. */
export type TsSeriesTold = Omit<TsSeries, 'id'> & { readonly id?: string };

/** The slots a tile's options tell (TsSlots), in order. */
const SLOTS = ['grain', 'align', 'range', 'view', 'panes', 'annotations'] as const;

/** The slots told whole, each replacing what the tile had, null taking it off (Python's Timeseries.options). */
export interface TsSlots {
  readonly grain?: TsGrain | null;
  readonly align?: TsAlign | null;
  readonly range?: TsRange | null;
  readonly view?: TsView | null;
  readonly panes?: readonly TsPane[] | null;
  readonly annotations?: readonly TsAnnotation[] | null;
}

/** A CHANGE TO A TIMESERIES TILE: each editor's, and each of Python's Timeseries methods'. */
export type TsChange =
  /** A series plotted: added (+ Series, Python's plot), or one it has replaced (the series editor's OK, Python's edit:
   *  `replace`, refused where it has no such series). */
  | { readonly kind: 'plot'; readonly series: TsSeriesTold; readonly replace?: boolean }
  /** A series taken off: refused while another reads it. */
  | { readonly kind: 'unplot'; readonly id: string }
  /** A line hidden or shown (its legend chip; Python's hide and show): its legend entry stays. */
  | { readonly kind: 'hide'; readonly id: string; readonly hidden: boolean }
  | { readonly kind: 'grain'; readonly grain: TsGrain | null }
  /** How two series' dates are combined (Settings' Dates): null, the dates both have. */
  | { readonly kind: 'align'; readonly align: TsAlign | null }
  | { readonly kind: 'range'; readonly range: TsRange | null }
  /** The view's settings told, each replacing its own, null taking it off (the toolbar's compare and stats, Settings). */
  | { readonly kind: 'view'; readonly view: { readonly [K in keyof TsView]?: TsView[K] | null } }
  /** A pane's height, scale or axis (the toolbar's Log: the main pane's; Settings' panes): `min`, `max` null take the
   *  hand-set end off. */
  | {
    readonly kind: 'pane'; readonly index: number; readonly height?: number; readonly log?: boolean; readonly invert?: boolean;
    readonly min?: number | null; readonly max?: number | null;
  }
  /** One instrument for another in every series that reads it (Bloomberg's change of security), in the source `alias`
   *  alone when told; a name saying it says the other. */
  | { readonly kind: 'swap'; readonly from: string; readonly to: string; readonly alias?: string }
  | { readonly kind: 'annotate'; readonly annotation: TsAnnotation }
  /** A 0/1 column shaded where it is not 0, in one step (Phase 5; FRED's recession bars): a condition series of it
   *  (`rec[US] != 0`) of source `source` (absent, the first), named after it unless told. */
  | { readonly kind: 'shade'; readonly column: string; readonly source?: string; readonly where?: readonly TsMatch[]; readonly name?: string }
  /** An annotation taken off, by its place. */
  | { readonly kind: 'unannotate'; readonly index: number }
  /** A source's alias renamed (every expression naming it following), its date or its instrument column told. */
  | { readonly kind: 'source'; readonly alias: string; readonly rename?: string; readonly date?: string; readonly key?: string | null }
  /** Another grid read: its source made by DataCube's rule over its columns (sourceOf), called after its title. */
  | { readonly kind: 'read'; readonly grid: TsGridColumns }
  /** A source no longer read: refused while a series reads it, or when it is the only one. */
  | { readonly kind: 'unread'; readonly alias: string }
  /** The tile put in a state (its state button; Python's state): FROZEN, each source keeping its grid's filter as it is
   *  now (`filters`, by the cube each reads: a removed grid's as it was); DYNAMIC, following its grids' filters again. */
  | { readonly kind: 'state'; readonly state: TsState; readonly filters: ReadonlyMap<string, FilterNode | undefined> }
  | { readonly kind: 'slots'; readonly slots: TsSlots };

/**
 * A FIRST TIMESERIES OF A GRID (Create ▸ Timeseries; Python's page.timeseries of a grid, its series untold): DataCube's
 * rule over the grid's columns as it is now (timeseries-expr.ts defaultTimeseries), its source called after the grid's
 * title -- or why the grid cannot be read as one, in words. One rule for both (the review's A3: Python made a tile of no
 * series of a grid the screen refused).
 */
export function firstTimeseries(grid: TsGridColumns): TimeseriesSpec | string {
  const source = sourceOf(grid.columns, grid.rows, grid.cube, aliasFor(grid.title, []));
  if (!source) return noPlot(grid);
  const plot = firstPlotOf(grid, source);
  return typeof plot === 'string' ? plot : { sources: [source], ...plot };
}

/** A source's first plot, told by its grid (Python's page.timeseries of grids, its series untold): firstTimeseries'
 *  rule, timeseries-expr.ts firstPlot -- or why there is none, in its words. */
export function firstPlotOf(grid: TsGridColumns, source: TsSource): Pick<TimeseriesSpec, 'series' | 'view'> | string {
  return firstPlot(grid.columns, source) ?? noPlot(grid);
}

const noPlot = (grid: TsGridColumns): string =>
  `${grid.title} has no date column with a number to plot along it: a timeseries is read along a date.`;

/** The sources a series reads, through the series it reads: of one that cannot be inlined (it reads itself), its own
 *  columns' -- its problem is said where it is drawn; what it reads directly still keeps its sources. */
function readBy(spec: TimeseriesSpec, s: TsSeries): string[] {
  try {
    return sourcesOf(spec, wholeOf(s, spec.series));
  } catch (e) {
    // one that cannot be inlined alone (the second audit's G notes: any error was taken for one)
    if (!(e instanceof TsInlineError)) throw e;
    return sourcesOf(spec, treeOf(s, spec.series));
  }
}

/** An expression with its columns of source `from` reading `to` instead (an alias renamed). */
function renameSource(e: TsExpr, from: string, to: string): TsExpr {
  switch (e.kind) {
    case 'column': return e.source === from ? { ...e, source: to } : e;
    case 'binary': return { ...e, left: renameSource(e.left, from, to), right: renameSource(e.right, from, to) };
    case 'call': return { ...e, args: e.args.map((a) => renameSource(a, from, to)) };
    default: return e;
  }
}

/** An object with each told key set, a null one taken off. */
function laid<T extends object>(was: T | undefined, change: { readonly [k: string]: unknown }): Record<string, unknown> {
  const next: Record<string, unknown> = { ...(was ?? {}) };
  for (const [k, v] of Object.entries(change)) {
    if (v === null || v === undefined) delete next[k];
    else next[k] = v;
  }
  return next;
}

/** An align as the tile keeps it: intersect, its default, as none -- one spelling whoever tells it (the Phase 1
 *  review's 9: the viewer's none and Python's 'intersect' were two, and a query asked again for nothing). */
function alignTold(align: TsAlign | null | undefined): TsAlign | null {
  return align === 'intersect' || align === undefined ? null : align;
}

/** The spec with `key` set, or taken off (null). */
function slot<K extends keyof TimeseriesSpec>(spec: TimeseriesSpec, key: K, value: TimeseriesSpec[K] | null): TimeseriesSpec {
  const { [key]: _was, ...rest } = spec;
  return (value === null || value === undefined ? rest : { ...rest, [key]: value }) as TimeseriesSpec;
}

const seriesNamed = (spec: TimeseriesSpec, id: string): TsSeries => {
  const s = spec.series.find((x) => x.id === id);
  if (!s) throw new TsEditRefusal(`there is no series ${id}`);
  return s;
};

/**
 * A TILE'S CHANGE JUDGED, by both sides alike (the second audit's A-S1: the screen kept changes Python refused, and Python
 * refused a change for a problem the tile had before it): read by the tile's one reader, then refused for a problem it
 * MAKES -- one the tile had already (a series over a column since gone) is said where the tile is drawn and blocks no
 * other change -- a tile of no series one either side may leave (it says "Add a series to plot" where it is drawn).
 * `before`: the tile as it was (null: one made new); `columnsOf`: each source's columns, as the page has its grid.
 */
export function judgedEdit(before: TimeseriesSpec | null, made: unknown,
  columnsOf: (spec: TimeseriesSpec) => ReadonlyMap<string, readonly TsColumn[] | undefined>): TimeseriesSpec {
  const spec = readTimeseries(JSON.parse(JSON.stringify(made)));
  if (typeof spec === 'string') throw new TsEditRefusal(spec);
  // what the tile had, said as it says it now: a source renamed (found by the cube it reads, one source a cube) names
  // its problems by its new alias -- else a rename "made" every problem the tile had of it (the third review's 4)
  const said = before === null ? null : spec.sources.reduce((was, now) => {
    const old = was.sources.find((x) => x.cube === now.cube);
    return old && old.alias !== now.alias && !was.sources.some((x) => x.alias === now.alias)
      ? editTimeseries(was, { kind: 'source', alias: old.alias, rename: now.alias }) : was;
  }, before);
  const had = new Set(said === null ? [] : timeseriesProblems(said, columnsOf(said)));
  const makes = timeseriesProblems(spec, columnsOf(spec)).filter((p) => p !== NO_SERIES && !had.has(p));
  if (makes.length > 0) throw new TsEditRefusal(makes.join(' '));
  return spec;
}

/** The instruments a tile's series read (`close[SPX]`: a match on its source's instrument column), each with its
 *  source's alias, in the order the series read them -- what a swap may take (its window's list). */
export function instrumentsOf(spec: TimeseriesSpec): { readonly alias: string; readonly value: string }[] {
  const first = spec.sources[0]?.alias;
  const out: { alias: string; value: string }[] = [];
  const walk = (e: TsExpr): void => {
    if (e.kind === 'column') {
      const alias = e.source ?? first;
      const key = spec.sources.find((x) => x.alias === alias)?.key;
      for (const m of e.where ?? []) {
        if (alias !== undefined && (m.column === undefined || m.column === key) && !out.some((o) => o.alias === alias && o.value === String(m.value))) {
          out.push({ alias, value: String(m.value) });
        }
      }
    } else if (e.kind === 'binary') {
      walk(e.left);
      walk(e.right);
    } else if (e.kind === 'call') {
      e.args.forEach(walk);
    }
  };
  for (const s of spec.series) walk(treeOf(s, spec.series));
  return out;
}

/**
 * ONE INSTRUMENT FOR ANOTHER across a tile (the next phases' 2, 2026-10-26: Bloomberg's change of security, TradingView's
 * symbol change): every match of `from` on its source's instrument column (`close[SPX]`) -- in source `alias` alone,
 * when told -- matched to `to` instead, each series' name saying it (a word) saying the other; refused where no series
 * reads it.
 */
function swapped(spec: TimeseriesSpec, fromTold: string, toTold: string, alias: string | undefined): TimeseriesSpec {
  // names in words, a host's JSON too (the Phase 2 review's 5: a number from Python failed as no refusal); the spaces
  // round a name not of it
  if (typeof fromTold !== 'string' || typeof toTold !== 'string') throw new TsEditRefusal('an instrument is swapped by its name, in words');
  const from = fromTold.trim();
  const to = toTold.trim();
  if (to === '') throw new TsEditRefusal('an instrument is swapped for another: name it');
  if (alias !== undefined && !spec.sources.some((x) => x.alias === alias)) throw new TsEditRefusal(`the tile reads no source ${alias}`);
  const first = spec.sources[0]?.alias;
  let swaps = 0;
  const swap = (e: TsExpr): TsExpr => {
    switch (e.kind) {
      case 'column': {
        const src = e.source ?? first;
        if (alias !== undefined && src !== alias) return e;
        const key = spec.sources.find((x) => x.alias === src)?.key;
        if (!e.where) return e;
        const where = e.where.map((m) => {
          const onKey = m.column === undefined || m.column === key;
          if (!onKey || String(m.value) !== from) return m;
          // an instrument column of numbers matched to a number, never to words
          if (typeof m.value === 'number' && !Number.isFinite(Number(to))) {
            throw new TsEditRefusal(`${src}'s instruments are numbers: ${JSON.stringify(to)} is none`);
          }
          swaps += 1;
          return { ...m, value: typeof m.value === 'number' ? Number(to) : to };
        });
        return { ...e, where };
      }
      case 'binary':
        return { ...e, left: swap(e.left), right: swap(e.right) };
      case 'call':
        return { ...e, args: e.args.map(swap) };
      case 'ref':
      case 'number':
        return e;
    }
  };
  // the word itself in a name, never a part of another: SPX in SPXW, S in S&P or in BRK.S (a & or an inner . or - joins)
  const word = from.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  const said = new RegExp(`(?<![\\p{L}\\p{N}_&]|[\\p{L}\\p{N}][.\\-])${word}(?![\\p{L}\\p{N}_&]|[.\\-][\\p{L}\\p{N}])`, 'gu');
  const exprs = new Map(spec.series.map((s) => {
    const before = swaps;
    const expr = swap(treeOf(s, spec.series));
    return [s.id, swaps === before ? undefined : expr] as const;
  }));
  // a series that read it -- itself, or through the series it reads ($A) -- plots the other: its name says so
  const reads = (id: string, seen: Set<string>): boolean => {
    if (exprs.get(id) !== undefined) return true;
    if (seen.has(id)) return false;
    seen.add(id);
    const s = spec.series.find((x) => x.id === id);
    return s !== undefined && refsOf(treeOf(s, spec.series)).some((r) => reads(r, seen));
  };
  const series = spec.series.map((s) => {
    const expr = exprs.get(s.id);
    if (expr === undefined && !reads(s.id, new Set())) return s;
    // the new name as written: never read as a replacement's pattern ($&, $')
    return { ...s, ...(expr !== undefined ? { text: writtenText(expr, spec.series) } : {}), ...(s.name !== undefined ? { name: s.name.replace(said, () => to) } : {}) };
  });
  if (swaps === 0) throw new TsEditRefusal(`no series reads ${from}${alias !== undefined ? ` of ${alias}` : ''}`);
  // and the marks of its rows, as a column's matches on the instrument column (the Phase 6 review: the other's close,
  // the first's trades on it), their label saying it as a name does
  const annotations = spec.annotations?.map((a) => {
    if (a.kind !== 'marks' || (alias !== undefined && a.source !== alias) || a.where === undefined) return a;
    const key = spec.sources.find((x) => x.alias === a.source)?.key;
    let moved = false;
    const where = a.where.map((m) => {
      if (!(m.column === undefined || m.column === key) || String(m.value) !== from) return m;
      moved = true;
      return { ...m, value: typeof m.value === 'number' ? Number(to) : to };
    });
    return moved ? { ...a, where, ...(a.label !== undefined ? { label: a.label.replace(said, () => to) } : {}) } : a;
  });
  return { ...spec, series, ...(annotations !== undefined ? { annotations } : {}) };
}

/**
 * THE TILE'S SPEC, CHANGED: `change` laid over `spec` by the tile's rules -- the one function its editors and Python ask.
 * Refused (TsEditRefusal) where a rule says no; what it returns is read by the tile's reader before it is told.
 */
export function editTimeseries(spec: TimeseriesSpec, change: TsChange): TimeseriesSpec {
  switch (change.kind) {
    case 'plot': {
      const told = change.series;
      const was = told.id !== undefined ? spec.series.find((s) => s.id === told.id) : undefined;
      if (change.replace === true && !was) throw new TsEditRefusal(`there is no series ${told.id ?? '(no id told)'}`);
      if (was) {
        const made = { ...told, id: was.id, ...(was.hidden && told.hidden === undefined ? { hidden: true } : {}) } as TsSeries;
        return { ...spec, series: spec.series.map((s) => (s.id === was.id ? made : s)) };
      }
      const id = told.id ?? nextSeriesId(spec.series);
      return { ...spec, series: [...spec.series, { ...told, id } as TsSeries] };
    }
    case 'shade': {
      if (typeof change.column !== 'string' || change.column === '') throw new TsEditRefusal('a column is shaded by its name');
      if (change.source !== undefined && !spec.sources.some((x) => x.alias === change.source)) throw new TsEditRefusal(`the tile reads no source ${change.source}`);
      // where it is not 0, read as a condition is -- shaded across every pane, its runs the database's
      const column: TsExpr = {
        kind: 'column', ...(change.source !== undefined && change.source !== spec.sources[0]?.alias ? { source: change.source } : {}), column: change.column,
        ...(change.where !== undefined && change.where.length > 0 ? { where: change.where } : {}),
      };
      const expr: TsExpr = { kind: 'binary', op: '!=', left: column, right: { kind: 'number', value: 0 } };
      // its name as told, its ends' spaces taken off by the rule, the screen's and Python's alike (the Phase 5 review's
      // notes: the screen alone trimmed it); none, the column's
      const name = change.name?.trim();
      if (name === '') throw new TsEditRefusal('a shade\'s name is no words: name it, or leave it to the column\'s');
      return { ...spec, series: [...spec.series, { id: nextSeriesId(spec.series), text: writtenText(expr, spec.series), name: name ?? change.column }] };
    }
    case 'unplot': {
      seriesNamed(spec, change.id);
      const readers = spec.series.filter((s) => s.id !== change.id && refsOf(treeOf(s, spec.series)).includes(change.id)).map((s) => s.id);
      if (readers.length > 0) {
        throw new TsEditRefusal(`series ${readers.join(', ')} ${readers.length === 1 ? 'reads' : 'read'} series ${change.id}: take ${readers.length === 1 ? 'it' : 'them'} off first, or write ${readers.length === 1 ? 'it' : 'them'} without ${change.id}`);
      }
      const plotted = { ...spec, series: spec.series.filter((s) => s.id !== change.id) };
      // its projections taken off with it (Phase 5): a projection is of its one series
      const kept = spec.annotations?.filter((a) => !(a.kind === 'projection' && a.series === change.id));
      const left = kept === undefined || kept.length === spec.annotations!.length ? plotted : slot(plotted, 'annotations', kept.length > 0 ? kept : null);
      // a view that read it reads the tile's first it can draw instead (the next phases' 3): its `of` without it
      const of = spec.view?.of?.filter((id) => id !== change.id);
      if (of === undefined || of.length === spec.view?.of?.length) return left;
      const view = laid(spec.view, { of: of.length > 0 ? of : null });
      return slot(left, 'view', Object.keys(view).length === 0 ? null : view as TsView);
    }
    case 'hide': {
      seriesNamed(spec, change.id);
      return { ...spec, series: spec.series.map((s) => (s.id === change.id ? laid(s, { hidden: change.hidden ? true : null }) as unknown as TsSeries : s)) };
    }
    case 'grain':
      return slot(spec, 'grain', change.grain);
    case 'align':
      return slot(spec, 'align', alignTold(change.align));
    case 'range':
      return slot(spec, 'range', change.range);
    case 'view': {
      const view = laid(spec.view, change.view);
      return slot(spec, 'view', Object.keys(view).length === 0 ? null : view as TsView);
    }
    case 'pane': {
      if (!Number.isInteger(change.index) || change.index < 0 || change.index >= MAX_PANES) {
        throw new TsEditRefusal(`there is no pane ${change.index} (0 to ${MAX_PANES - 1})`);
      }
      const panes = Array.from({ length: Math.max(change.index + 1, spec.panes?.length ?? 0) }, (_, i) => ({ ...(spec.panes?.[i] ?? {}) }));
      const pane = laid(panes[change.index], {
        ...(change.height !== undefined ? { height: change.height } : {}),
        ...(change.log !== undefined ? { log: change.log ? true : null } : {}),
        ...(change.invert !== undefined ? { invert: change.invert ? true : null } : {}),
        ...(change.min !== undefined ? { min: change.min } : {}),
        ...(change.max !== undefined ? { max: change.max } : {}),
      });
      panes[change.index] = pane;
      return { ...spec, panes };
    }
    case 'swap':
      return swapped(spec, change.from, change.to, change.alias);
    case 'annotate':
      return { ...spec, annotations: [...(spec.annotations ?? []), change.annotation] };
    case 'unannotate': {
      const annotations = spec.annotations ?? [];
      if (!(change.index >= 0 && change.index < annotations.length)) throw new TsEditRefusal(`there is no annotation ${change.index + 1}`);
      const kept = annotations.filter((_, i) => i !== change.index);
      return slot(spec, 'annotations', kept.length === 0 ? null : kept);
    }
    case 'source': {
      const src = spec.sources.find((s) => s.alias === change.alias);
      if (!src) throw new TsEditRefusal(`there is no source ${change.alias}`);
      const to = change.rename;
      if (to !== undefined && to !== change.alias) {
        if (!ALIAS.test(to)) throw new TsEditRefusal(`${JSON.stringify(to)} is no alias: a word, a letter first`);
        if (spec.sources.some((s) => s.alias === to)) throw new TsEditRefusal(`source ${to} is the tile's already`);
      }
      const made = laid(src, {
        ...(to !== undefined ? { alias: to } : {}),
        ...(change.date !== undefined ? { date: change.date } : {}),
        ...(change.key !== undefined ? { key: change.key } : {}),
      }) as unknown as TsSource;
      const sources = spec.sources.map((s) => (s.alias === change.alias ? made : s));
      // an alias renamed: every expression that names it follows, and every marks of its rows (the Phase 6 review's 4)
      if (to === undefined || to === change.alias) return { ...spec, sources };
      const series = spec.series.map((s) => ({ ...s, text: writtenText(renameSource(treeOf(s, spec.series), change.alias, to), spec.series) }));
      const annotations = spec.annotations?.map((a) => (a.kind === 'marks' && a.source === change.alias ? { ...a, source: to } : a));
      return { ...spec, sources, series, ...(annotations !== undefined ? { annotations } : {}) };
    }
    case 'read': {
      const g = change.grid;
      if (spec.sources.some((s) => s.cube === g.cube)) throw new TsEditRefusal(`${g.title} is read already`);
      const made = sourceOf(g.columns, g.rows, g.cube, aliasFor(g.title, spec.sources.map((s) => s.alias)));
      if (!made) throw new TsEditRefusal(`${g.title} has no date column: a timeseries is read along a date.`);
      // read by a frozen tile: kept as the rest are, its grid's filter as it is now
      const kept = tsStateOf(spec) === 'frozen' ? { ...made, kept: g.filter ? { filter: g.filter } : {} } : made;
      return { ...spec, sources: [...spec.sources, kept] };
    }
    case 'unread': {
      if (!spec.sources.some((s) => s.alias === change.alias)) throw new TsEditRefusal(`there is no source ${change.alias}`);
      if (spec.sources.length === 1) throw new TsEditRefusal(`source ${change.alias} is the only grid the tile reads`);
      // every series that reads it -- by its alias, or (the first source) by a column of no alias, which would read
      // the next one in its place (the review's A2: a series re-pointed at another grid without a word)
      const readers = spec.series.filter((s) => readBy(spec, s).includes(change.alias)).map((s) => s.id);
      if (readers.length > 0) throw new TsEditRefusal(`series ${readers.join(', ')} ${readers.length === 1 ? 'reads' : 'read'} source ${change.alias}`);
      return { ...spec, sources: spec.sources.filter((s) => s.alias !== change.alias) };
    }
    case 'state': {
      if (change.state === tsStateOf(spec)) return spec;
      const sources = spec.sources.map((src) => {
        const { kept: _was, ...following } = src;
        if (change.state === 'dynamic') return following;
        if (!change.filters.has(src.cube)) throw new TsEditRefusal(`source ${src.alias}'s grid is not open: its filter cannot be kept`);
        const filter = change.filters.get(src.cube);
        return { ...following, kept: filter ? { filter } : {} };
      });
      return { ...spec, sources };
    }
    case 'slots': {
      // a slot it has not, refused by name -- never dropped without a word (the second audit's A-S3: options(grian=...)
      // came back as it was)
      const unknown = Object.keys(change.slots).find((k) => !(SLOTS as readonly string[]).includes(k));
      if (unknown !== undefined) throw new TsEditRefusal(`there is no ${JSON.stringify(unknown)} among a tile's slots (${SLOTS.join(', ')})`);
      let next = spec;
      for (const key of SLOTS) {
        if (!Object.hasOwn(change.slots, key)) continue;
        const told = change.slots[key];
        next = key === 'align' ? slot(next, 'align', alignTold(told as TsAlign | null))
          : slot(next, key, told as TimeseriesSpec[typeof key] | null);
      }
      return next;
    }
  }
}
