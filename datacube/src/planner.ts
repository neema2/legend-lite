// THE PLANNERS' SHARED TYPES: how a planner reads a model besides its text and runtime (ModelOptions), and a planner's
// refusal (PlanError). legend-engine's planner over its pure/v1 API (UpstreamPlanner) is DataCube's compat entry's
// (datacube/src/edge/upstream-planner.ts, //datacube:edge_entry; H11, 2026-11-17): the product's planner is
// lite-planner.ts.

import type { Lambda } from '../../pure-protocol/src/index.ts';

/** How a planner reads a model besides its text and runtime. */
export interface ModelOptions {
  /** The mapping a class source reads through: the query goes `->from(mapping, runtime)` (pure-v1.ts). */
  readonly mapping?: string;
  /** The model's enumerations: a column typed by one is said so (relation-type.ts `PlanColumn.enumeration`). */
  readonly enumerations?: readonly string[];
  /**
   * Its rows are its project's own (a saved query's version: C5), in a database of the project's own on its host -- never
   * the host's tables. A host that plans and runs where the rows are (Python's plan-and-run) reads them there, and reads
   * any other model's rows only from the tables it serves (docs/DATACUBE_ONE_API_PLAN_2026_10_21.md F28); the tab reads
   * a project's through its project databases, by the source it opens.
   */
  readonly project?: true;
  /**
   * Its model held over DataCube's Pure library from the start (docs/TS_DECISIONS_2026_11_07.md item 8b, 2026-11-07):
   * the text this planner is given is `withLibrary(model, library)`, the module holds the two apart (A5) under that
   * text, and a query calling the library plans on it -- a project's model compiled once where a timeseries tile reads
   * its grid, not again under the library (the trading model, 1.4-1.6 s in the tab). A planner that holds nothing (the
   * server's, Python's) plans over `model` alone and adds the library as before.
   */
  readonly overLibrary?: { readonly model: string; readonly library: string };
}

export class PlanError extends Error {
  /** What was asked: the query (a tree), or, for a parse, the text a person typed. */
  readonly subject: Lambda | string;
  constructor(message: string, subject: Lambda | string) {
    super(message);
    this.name = 'PlanError';
    this.subject = subject;
  }
}

