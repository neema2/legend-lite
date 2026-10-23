// The CUBE's state and its rules (docs/DATACUBE_LEG_B_STATE_OWNER_2026_09_28.md, Leg B): its
// query definition (snapshot), its settings (configuration) and which groups are open (tree),
// owned by the one owner (state-owner.ts) with the cube's rules -- what its query IS, what a
// landed answer commits.

import { applyToSnapshot, fromSnapshot, type CubeConfiguration } from './config.ts';
import type { CubeView } from './cube.ts';
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
export type OwnerEvent = StateEvent<CubeView>;

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

/** The cube's rules. */
export const CUBE_RULES: StateRules<CubeState, CubeView> = {
  fold,
  queryKey,
  stateKey,
  // As the ENGINE returned it (the compiler's types), but reading the source that was SENT:
  // the plane a query ran on (a snap's table) is the query side's, never the cube's state.
  land: (pending, view) => ({ ...pending, snapshot: { ...view.snapshot, source: pending.snapshot.source } }),
};

/** The owner of the cube's state: the transaction rules with the cube's own (`CUBE_RULES`). */
export class CubeStateOwner extends StateOwner<CubeState, CubeView> {
  constructor(initial: CubeState, run: StateRunner, options: OwnerOptions = {}) {
    super(initial, run, CUBE_RULES, options);
  }
}
