// The CUBE's state and its rules (docs/DATACUBE_LEG_B_STATE_OWNER_2026_09_28.md, Leg B): its
// query definition (snapshot), its settings (configuration) and which groups are open (tree),
// owned by the one owner (state-owner.ts) with the cube's rules -- what its query IS, what a
// landed answer commits.

import { applyToSnapshot, fromSnapshot, type CubeConfiguration } from './config.ts';
import type { CubeView } from './cube.ts';
import { gridBetween } from './grid-between.ts';
import { applyGridCommands, type GridCommand } from './grid-commands.ts';
import type { GridDraft } from './grid-edits.ts';
import type { CubeSnapshot } from './snapshot.ts';
import {
  StateOwner,
  type OwnerOptions,
  type Runner,
  type StateEvent,
  type StateOutcome,
  type StateRules,
} from './state-owner.ts';
import type { TreeState } from './tree.ts';

export type { ChangeOptions } from './state-owner.ts';

/** Everything that decides what the cube is and shows. */
export interface CubeState {
  readonly snapshot: CubeSnapshot;
  readonly configuration: CubeConfiguration;
  readonly tree: TreeState;
}

/** The cube's runner: the controller. */
export type StateRunner = Runner<CubeState, CubeView>;
export type Outcome = StateOutcome<CubeView>;
export type OwnerEvent = StateEvent<CubeView, GridCommand>;

/**
 * The state as the query sees it: the configuration's query-shaping settings folded into
 * the snapshot (`applyToSnapshot`), so both halves of a state always agree.
 */
export function fold(state: CubeState): CubeState {
  return { ...state, snapshot: applyToSnapshot(state.snapshot, state.configuration) };
}

/**
 * A CUBE AS THE SCREEN HOLDS ONE IT OPENS: the configuration read back from what its query carries (fromSnapshot -- a
 * column's kind or Aggregation the document holds only in its query is kept, not folded away), then folded. The screen's
 * constructor keeps this rule (app.ts: fromSnapshot, then its state owner folds); every other place a saved or rebuilt
 * cube is opened or written asks this one function -- headless opened() and gridCube, the example pages, the tests --
 * so no copy of the open path drifts (the review of the command program's stage 1a found gridCube's had).
 */
export function openedState(snapshot: CubeSnapshot, configuration: CubeConfiguration, tree: TreeState): CubeState {
  return fold({ snapshot, configuration: fromSnapshot(snapshot, configuration), tree });
}

/**
 * What the query of a state IS: the folded snapshot (its epoch aside) and the open groups.
 * Two states with the same key run the same queries; a change that keeps the key is
 * presentation.
 */
export function queryKey(state: CubeState): string {
  const { epoch: _epoch, ...query } = fold(state).snapshot;
  return JSON.stringify([query, state.tree.key]);
}

/** A state's identity for undo: everything, presentation included. */
export function stateKey(state: CubeState): string {
  const { epoch: _epoch, ...query } = state.snapshot;
  return JSON.stringify([query, state.configuration, state.tree.key]);
}

/** A state as the grid commands edit it (grid-edits.ts GridDraft), and back. */
const draftOf = (s: CubeState): GridDraft & { readonly tree: TreeState } => ({ snapshot: s.snapshot, config: s.configuration, tree: s.tree });
const stateOf = (d: GridDraft & { readonly tree: TreeState }): CubeState => fold({ snapshot: d.snapshot, configuration: d.config, tree: d.tree });

/** The cube's rules. */
export const CUBE_RULES: StateRules<CubeState, CubeView, GridCommand> = {
  fold,
  queryKey,
  stateKey,
  // As the ENGINE returned it (the compiler's types), but reading the source that was SENT:
  // the plane a query ran on (a snap's table) is the query side's, never the cube's state.
  land: (pending, view) => ({ ...pending, snapshot: { ...view.snapshot, source: pending.snapshot.source } }),
  // AN UNDO OR A REDO PRODUCED BY COMMANDS (docs/DATACUBE_LIVE_EDITING_2026_10_24.md W3): the grid commands that take one
  // state to another (grid-between.ts), and the state they make -- what the move lands and is told as, so the screen
  // after it is what the same commands make at its keeper
  between: (from, to) => gridBetween(draftOf(from), draftOf(to)),
  toward: (from, to) => stateOf(applyGridCommands(draftOf(from), gridBetween(draftOf(from), draftOf(to)))),
};

/** The owner of the cube's state: the transaction rules with the cube's own (`CUBE_RULES`). */
export class CubeStateOwner extends StateOwner<CubeState, CubeView, GridCommand> {
  constructor(initial: CubeState, run: StateRunner, options: OwnerOptions = {}) {
    super(initial, run, CUBE_RULES, options);
  }
}
