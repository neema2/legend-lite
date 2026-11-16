// DATACUBE'S PLANNER, AS AN INTERFACE (C5-T, 2026-11-15: the TypeScript split of plan and run; until then cube.ts held
// it): what a grid asks to have its queries planned -- a plan, a relation's type, a parse, a print -- answered by
// legend-lite's planner in the tab (lite-planner.ts) or a server's (planner.ts). A planner module
// (//datacube:planner): it plans and runs nothing, and reaches nothing that runs (tools/deps/ts-layers.txt). The grid
// (cube.ts, which re-exports it) runs what it answers, through its runner.

import type { SnapPull } from '../../engine-client/src/requests.ts';
import type { PlanText } from '../../engine-client/src/planned-sql.ts';
import type { Plan, PlanColumn } from '../../engine-client/src/relation-type.ts';
import type { PrintStyle } from '../../engine-client/src/pure-v1-planning.ts';
import type { Lambda } from '../../pure-protocol/src/index.ts';
import type { GridSource } from './model-writer.ts';

/** A #TS query as one statement (Show #TS, D12): the statement, or why it has none in words. */
export type TsShown = { readonly statement: string; readonly refused?: undefined } | { readonly statement?: undefined; readonly refused: string };

export interface Planner {
  /** A query (a protocol tree, query.ts) in, SQL and the compiler's result type out. */
  plan(query: Lambda, signal?: AbortSignal): Promise<Plan>;
  /**
   * The compiler's type of a query's result, compile-only (upstream
   * `lambdaRelationType`): how the cube types its source and calculated
   * columns BEFORE a level query runs.
   */
  relationType(query: Lambda, signal?: AbortSignal): Promise<PlanColumn[]>;
  /** What a person typed (a source, a calculated column) as its lambda: E1, the compiler's parse. */
  parse(text: string, signal?: AbortSignal): Promise<Lambda>;
  /** A program that writes planned as an execution plan, PlanJson's text (an Output into a table: table-output.ts);
   *  absent where the planner plans none (a host that runs a grid's rows itself). */
  executionPlan?(program: Lambda, signal?: AbortSignal): Promise<PlanText>;
  /** A query as Pure text, for a person to read: E4, the compiler's print (PRETTY unless asked). */
  print(query: Lambda, style?: PrintStyle, signal?: AbortSignal): Promise<string>;
  /** A #TS query over grids as the one statement a person would write (D12, legend-lite's own: legend-engine has no
   *  #TS, so its planner has none). */
  tsStatement?(query: Lambda, signal?: AbortSignal): Promise<TsShown>;
  /**
   * Let go of what the planner holds -- its model, compiled once and held while it plans
   * (docs/DATACUBE_MODEL_DESIGN_2026_10_10.md §9): called by its owner when nothing plans through it any more.
   */
  dispose?(): void;
  /** Where its queries are planned from: its model and the runtime they run on -- what a class made of its rows reads
   *  its source from (model-writer.ts). */
  source?(): GridSource;
  /** A query as the pull a snapshot's stream asks the planner to plan where its rows are (LitePlanner's; the user's
   *  ruling of 2026-11-12: the request carries the pull, never SQL); absent where the planner gives none. */
  pullRequest?(query: Lambda): Promise<SnapPull>;
}
