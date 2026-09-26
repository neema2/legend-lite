// Ad Hoc Analysis mode, running: the grid, its history, and its queries.
//
// Each operation is a pure step (state.ts) and then, unless the user is
// navigating without data, one refresh: the grid's queries (query.ts)
// through the cube's own runner, the answers placed. An answer to a
// grid the user has since moved on from is dropped, never shown.

import type { ResultTable } from '../result.ts';
import type { LevelScope } from '../serialize.ts';
import type { CubeSnapshot } from '../snapshot.ts';
import {
  inHierarchyOrder,
  memberQuery,
  membersFrom,
  zoomDepths,
} from './outline.ts';
import { assembleGrid, planQueries, type AdHocCube, type AdHocView } from './query.ts';
import {
  initialGrid,
  keepOnly,
  MEASURES,
  pivot,
  pivotToPov,
  povToAxis,
  removeOnly,
  selectMembers,
  setPov,
  withOptions,
  zoomIn,
  zoomOut,
  type Axis,
  type AdHocGrid,
  type AdHocOptions,
  type MemberPath,
  type ZoomLevel,
} from './state.ts';

/** Pure in, rows out: the cube's own runner, whichever plane it is. */
export type Run = (
  pure: string,
  snapshot: CubeSnapshot,
  scope: LevelScope | undefined,
) => Promise<ResultTable>;

const HISTORY = 100;

export class AdHocSession {
  readonly cube: AdHocCube;
  readonly #run: Run;
  #grid: AdHocGrid;
  #past: AdHocGrid[] = [];
  #future: AdHocGrid[] = [];
  #view: AdHocView | null = null;
  /** The last answers, so a display option re-places them without asking again. */
  #last: { results: Map<string, ResultTable>; queries: ReturnType<typeof planQueries> } | null = null;
  /** Bumped per refresh; an answer for an older one is dropped. */
  #seq = 0;

  constructor(cube: AdHocCube, run: Run, grid?: AdHocGrid) {
    this.cube = cube;
    this.#run = run;
    this.#grid = grid ?? initialGrid(cube.outline);
  }

  get grid(): AdHocGrid {
    return this.#grid;
  }

  get view(): AdHocView | null {
    return this.#view;
  }

  get canUndo(): boolean {
    return this.#past.length > 0;
  }

  get canRedo(): boolean {
    return this.#future.length > 0;
  }

  /** Query the grid as it stands; null when a newer refresh overtook this one. */
  async refresh(): Promise<AdHocView | null> {
    const seq = ++this.#seq;
    const grid = this.#grid;
    const queries = planQueries(this.cube, grid);
    const results = new Map<string, ResultTable>();
    for (const q of queries) {
      results.set(q.key, await this.#run(q.pure, q.snapshot, q.scope));
      if (seq !== this.#seq) return null;
    }
    const view = assembleGrid(this.cube, grid, results, queries);
    if (seq !== this.#seq) return null;
    this.#view = view;
    this.#last = { results, queries };
    return view;
  }

  /**
   * Move to a new grid: recorded for undo, then queried -- unless the
   * user navigates without data, when nothing runs until Refresh. The
   * same grid (an operation that changed nothing) does neither.
   */
  async apply(next: AdHocGrid): Promise<AdHocView | null> {
    if (next === this.#grid) return this.#view;
    this.#past = [...this.#past, this.#grid].slice(-HISTORY);
    this.#future = [];
    this.#grid = next;
    return next.options.navigateWithoutData ? this.#view : this.refresh();
  }

  async undo(): Promise<AdHocView | null> {
    const prev = this.#past.at(-1);
    if (!prev) return this.#view;
    this.#past = this.#past.slice(0, -1);
    this.#future = [this.#grid, ...this.#future];
    this.#grid = prev;
    return this.refresh();
  }

  async redo(): Promise<AdHocView | null> {
    const next = this.#future[0];
    if (!next) return this.#view;
    this.#future = this.#future.slice(1);
    this.#past = [...this.#past, this.#grid];
    this.#grid = next;
    return this.refresh();
  }

  /** The members under `under` at generation `depth`, as the source holds them. */
  async members(dimension: string, under: MemberPath, depth: number): Promise<MemberPath[]> {
    if (dimension === MEASURES) {
      return depth === 1 && under.length === 0 ? this.cube.outline.measures.map((m) => [m]) : [];
    }
    const q = memberQuery(this.cube, dimension, under, depth);
    return membersFrom(await this.#run(q.pure, q.snapshot, q.scope), depth);
  }

  /** How many generations a dimension has (Measures: one). */
  deepest(dimension: string): number {
    if (dimension === MEASURES) return 1;
    return this.cube.outline.dimensions.find((d) => d.name === dimension)?.generations.length ?? 0;
  }

  async zoomIn(dimension: string, member: MemberPath, level?: ZoomLevel): Promise<AdHocView | null> {
    const depths = zoomDepths(level ?? this.#grid.options.zoomLevel, member.length,
      this.deepest(dimension));
    const found: MemberPath[] = [];
    for (const depth of depths) found.push(...await this.members(dimension, member, depth));
    const ordered = depths.length > 1
      ? inHierarchyOrder([member, ...found], this.#grid.options.ancestorPosition)
        .filter((m) => m !== member)
      : found;
    return this.apply(zoomIn(this.#grid, dimension, member, ordered));
  }

  zoomOut(dimension: string, member: MemberPath): Promise<AdHocView | null> {
    return this.apply(zoomOut(this.#grid, dimension, member));
  }

  keepOnly(dimension: string, members: readonly MemberPath[]): Promise<AdHocView | null> {
    return this.apply(keepOnly(this.#grid, dimension, members));
  }

  removeOnly(dimension: string, members: readonly MemberPath[]): Promise<AdHocView | null> {
    return this.apply(removeOnly(this.#grid, dimension, members));
  }

  pivot(dimension: string): Promise<AdHocView | null> {
    return this.apply(pivot(this.#grid, dimension));
  }

  pivotToPov(dimension: string): Promise<AdHocView | null> {
    return this.apply(pivotToPov(this.#grid, dimension));
  }

  povToAxis(dimension: string, axis: Axis): Promise<AdHocView | null> {
    return this.apply(povToAxis(this.#grid, dimension, axis));
  }

  setPov(dimension: string, member: MemberPath): Promise<AdHocView | null> {
    return this.apply(setPov(this.#grid, dimension, member));
  }

  selectMembers(dimension: string, members: readonly MemberPath[]): Promise<AdHocView | null> {
    return this.apply(selectMembers(this.#grid, dimension, members));
  }

  /**
   * Options only change how the answers are PLACED (suppression,
   * indentation) or how the next zoom behaves, never what is asked:
   * the last answers are placed again, no query.
   */
  async setOptions(patch: Partial<AdHocOptions>): Promise<AdHocView | null> {
    const next = withOptions(this.#grid, patch);
    this.#past = [...this.#past, this.#grid].slice(-HISTORY);
    this.#future = [];
    this.#grid = next;
    if (!this.#last) return this.refresh();
    this.#view = assembleGrid(this.cube, next, this.#last.results, this.#last.queries);
    return this.#view;
  }
}
