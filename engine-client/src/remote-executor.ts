// A REMOTE EXECUTOR: a host that runs a query and answers its rows (a server's plane, Query's lite rows over a server),
// the shape DataCube's RemoteRun runs through (datacube/src/runner.ts). The product's own (H11, 2026-11-17): out of
// engine-remote.ts, whose legend-engine executor (LegendEngineExecutor, over legend-engine's pure/v1) and its wire
// readers are the edge's (//engine-client:edge).

import type { ResultTable } from './result.ts';
import type { Lambda } from '../../pure-protocol/src/index.ts';
import type { PlanColumn } from './relation-type.ts';

/** What a remote engine answers with. */
export interface RemoteResult {
  readonly rows: ResultTable;
  /**
   * The SQL the engine REPORTS having run, for display only.
   *
   * It arrives as an execution activity, after the fact. Running it
   * here would need the engine's own connection, which is the thing
   * this mode exists to avoid.
   */
  readonly sql: string;
}

/** How a remote executor prints a query for a person: its host's print styles. */
export type RemotePrintStyle = 'PRETTY' | 'STANDARD';

export interface RemoteExecutor {
  execute(query: Lambda, epoch: number, signal?: AbortSignal): Promise<RemoteResult>;
  /** The compiler's type of a query's result. */
  relationType(query: Lambda, signal?: AbortSignal): Promise<PlanColumn[]>;
  /** What a person typed, as its lambda. */
  parse(text: string, signal?: AbortSignal): Promise<Lambda>;
  /** A query as Pure text for a person. */
  print(query: Lambda, style?: RemotePrintStyle, signal?: AbortSignal): Promise<string>;
}

export class RemoteExecutionError extends Error {
  /** What was asked: the query, or, for a parse, the text. */
  readonly subject: Lambda | string;
  constructor(message: string, subject: Lambda | string) {
    super(message);
    this.name = 'RemoteExecutionError';
    this.subject = subject;
  }
}
