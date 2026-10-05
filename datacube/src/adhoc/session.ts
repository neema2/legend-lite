// Ad Hoc Analysis mode, running: the grid, its history, and its queries.
//
// Each operation is a pure step (state.ts) and then, unless the user is
// navigating without data, one query of the grid (query.ts) through the
// cube's own runner, the answers placed.
//
// THE SAME TRANSACTION RULES AS THE CUBE (Leg B / B5a): the grid has one
// owner, `StateOwner` (cube-state.ts), with Ad Hoc's rules. A step commits
// only when its answers land (a failed one leaves nothing, P2-259); an
// option that only places the answers differently re-places the answers ON
// SCREEN, never those of another grid (P2-260), and one changed while a
// step runs lands with it (P2-261); Navigate Without Data commits steps --
// undo and redo included -- without a query, and says the grid on screen
// is stale until Refresh (P2-268, P2-282); `dispose` stops the step in
// flight (P2-270).

import { STALE } from '../epoch.ts';
import { DEFAULT_HISTORY_LIMIT, StateOwner, type StateOutcome, type StateRules } from '../state-owner.ts';
import type { ResultTable } from '../../../engine-client/src/result.ts';
import type { LevelScope } from '../query.ts';
import type { CubeSnapshot } from '../snapshot.ts';
import {
  inHierarchyOrder,
  memberQuery,
  membersFrom,
  zoomDepths,
} from './outline.ts';
import { assembleGrid, planQueries, type AdHocCube, type AdHocQuery, type AdHocView } from './query.ts';
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

/** A cube level in, rows out: the cube's own runner, whichever plane it is. */
export type Run = (
  snapshot: CubeSnapshot,
  scope: LevelScope | undefined,
) => Promise<ResultTable>;


/** A grid's answers: the view placed from them, and the answers, to place again. */
interface Answers {
  readonly view: AdHocView;
  readonly results: ReadonlyMap<string, ResultTable>;
  readonly queries: readonly AdHocQuery[];
}

/** Ad Hoc's rules for the one owner: what a grid asks is its planned queries. */
function rules(cube: AdHocCube): StateRules<AdHocGrid, Answers> {
  return {
    fold: (grid) => grid,
    queryKey: (grid) => JSON.stringify(planQueries(cube, grid).map((q) => [q.key, q.snapshot, q.scope ?? null])),
    stateKey: (grid) => JSON.stringify(grid),
    land: (grid) => grid,
    represent: (grid, answers) => ({ ...answers, view: assembleGrid(cube, grid, answers.results, answers.queries) }),
    defer: (grid) => grid.options.navigateWithoutData,
  };
}

export class AdHocSession {
  readonly cube: AdHocCube;
  readonly #run: Run;
  readonly #owner: StateOwner<AdHocGrid, Answers>;
  /** Bumped per query and by a cancel: a query overtaken stops between its levels. */
  #seq = 0;

  constructor(cube: AdHocCube, run: Run, grid?: AdHocGrid, options: { historyLimit?: number } = {}) {
    this.cube = cube;
    this.#run = run;
    this.#owner = new StateOwner(grid ?? initialGrid(cube.outline), (g) => this.#query(g), rules(cube), {
      historyLimit: options.historyLimit ?? DEFAULT_HISTORY_LIMIT,
      abort: () => { this.#seq += 1; },
    });
  }

  /** The grid as the person has it: a step still running, else the one committed. */
  get grid(): AdHocGrid {
    return this.#owner.current;
  }

  /** The grid the view on screen answers (older than `grid` while navigating without data). */
  get shownGrid(): AdHocGrid {
    return this.#owner.rendered;
  }

  get view(): AdHocView | null {
    return this.#owner.view?.view ?? null;
  }

  /** The view on screen does not answer the grid: a step taken without data waits for Refresh. */
  get stale(): boolean {
    return this.#owner.stale;
  }

  get canUndo(): boolean {
    return this.#owner.canUndo;
  }

  get canRedo(): boolean {
    return this.#owner.canRedo;
  }

  get busy(): boolean {
    return this.#owner.busy;
  }

  /** Settings > Max History Stack Size. */
  setHistoryLimit(limit: number): void {
    this.#owner.setHistoryLimit(limit);
  }

  /** Stop the step in flight: nothing it answers is placed. */
  dispose(): void {
    this.#owner.cancel();
  }

  /** Query the grid as it stands; null when a newer step overtook this one. */
  refresh(): Promise<AdHocView | null> {
    return this.#settle(this.#owner.refresh());
  }

  /**
   * Move to a new grid: one step -- queried, unless the person navigates
   * without data, when it waits for Refresh. The same grid does nothing.
   */
  apply(next: AdHocGrid): Promise<AdHocView | null> {
    if (next === this.grid) return Promise.resolve(this.view);
    return this.#settle(this.#owner.change(() => next, { label: 'step' }));
  }

  undo(): Promise<AdHocView | null> {
    return this.#settle(this.#owner.undo());
  }

  redo(): Promise<AdHocView | null> {
    return this.#settle(this.#owner.redo());
  }

  /** The members under `under` at generation `depth`, as the source holds them. */
  async members(dimension: string, under: MemberPath, depth: number): Promise<MemberPath[]> {
    if (dimension === MEASURES) {
      return depth === 1 && under.length === 0 ? this.cube.outline.measures.map((m) => [m]) : [];
    }
    const q = memberQuery(this.cube, dimension, under, depth);
    return membersFrom(await this.#run(q.snapshot, q.scope), depth);
  }

  /** How many generations a dimension has (Measures: one). */
  deepest(dimension: string): number {
    if (dimension === MEASURES) return 1;
    return this.cube.outline.dimensions.find((d) => d.name === dimension)?.generations.length ?? 0;
  }

  async zoomIn(dimension: string, member: MemberPath, level?: ZoomLevel): Promise<AdHocView | null> {
    const depths = zoomDepths(level ?? this.grid.options.zoomLevel, member.length,
      this.deepest(dimension));
    const found: MemberPath[] = [];
    for (const depth of depths) found.push(...await this.members(dimension, member, depth));
    const ordered = depths.length > 1
      ? inHierarchyOrder([member, ...found], this.grid.options.ancestorPosition)
        .filter((m) => m !== member)
      : found;
    return this.apply(zoomIn(this.grid, dimension, member, ordered));
  }

  zoomOut(dimension: string, member: MemberPath): Promise<AdHocView | null> {
    return this.apply(zoomOut(this.grid, dimension, member));
  }

  keepOnly(dimension: string, members: readonly MemberPath[]): Promise<AdHocView | null> {
    return this.apply(keepOnly(this.grid, dimension, members));
  }

  removeOnly(dimension: string, members: readonly MemberPath[]): Promise<AdHocView | null> {
    return this.apply(removeOnly(this.grid, dimension, members));
  }

  pivot(dimension: string): Promise<AdHocView | null> {
    return this.apply(pivot(this.grid, dimension));
  }

  pivotToPov(dimension: string): Promise<AdHocView | null> {
    return this.apply(pivotToPov(this.grid, dimension));
  }

  povToAxis(dimension: string, axis: Axis): Promise<AdHocView | null> {
    return this.apply(povToAxis(this.grid, dimension, axis));
  }

  setPov(dimension: string, member: MemberPath): Promise<AdHocView | null> {
    return this.apply(setPov(this.grid, dimension, member));
  }

  selectMembers(dimension: string, members: readonly MemberPath[]): Promise<AdHocView | null> {
    return this.apply(selectMembers(this.grid, dimension, members));
  }

  /**
   * Options only change how the answers are PLACED (suppression,
   * indentation) or how the next zoom behaves, never what is asked: the
   * answers on screen are placed again, no query. Turning Navigate Without
   * Data off queries a grid that waited for it.
   */
  async setOptions(patch: Partial<AdHocOptions>): Promise<AdHocView | null> {
    const out = await this.#settle(this.#owner.change((g) => withOptions(g, patch), { label: 'options' }));
    if (this.view === null || (this.stale && !this.grid.options.navigateWithoutData)) return this.refresh();
    return out;
  }

  /** A grid's queries, through the cube's runner; STALE when a newer one (or a cancel) overtook it. */
  async #query(grid: AdHocGrid): Promise<Answers | typeof STALE> {
    const seq = ++this.#seq;
    const queries = planQueries(this.cube, grid);
    const results = new Map<string, ResultTable>();
    for (const q of queries) {
      results.set(q.key, await this.#run(q.snapshot, q.scope));
      if (seq !== this.#seq) return STALE;
    }
    return { view: assembleGrid(this.cube, grid, results, queries), results, queries };
  }

  /** A step's outcome as the mode reads it: the view, null when overtaken, a refusal thrown. */
  async #settle(out: Promise<StateOutcome<Answers>>): Promise<AdHocView | null> {
    const o = await out;
    if (o.kind === 'refused') throw o.error;
    if (o.kind === 'superseded' || o.kind === 'cancelled') return null;
    return this.view;
  }
}
