// LIVE RESULTS, WHATEVER ENGINE KEEPS THEM (docs/DUCKDB_LIVE_2026_10_25.md §0): what a grid's reader says of the results
// its view read -- they changed, the connection ended, or they could not be followed -- the Deephaven server's
// (deephaven-engine.ts) and the tab's live DuckDB's (duckdb-live.ts) alike. DataCube's host follows a grid's view through
// it and runs the grid again when it speaks (datacube/src/app/services.ts LiveRows).

import type { QueryEngine } from './engine.ts';

/** What the live side says: a followed result changed (`since`: when the first change since the last word arrived,
 *  `performance.now()`), the live connection ended (`ended`: why) -- the server went away; `reconnect` -- or a result
 *  read since the view could not be followed (`failed`: why): it will not tick. */
export interface LiveEvent {
  readonly since: number;
  readonly ended: string | null;
  readonly failed?: string;
}

/** A grid's reader of live results: it runs the grid's plans, follows those its view read, and says when they change. */
export interface LiveReader extends QueryEngine {
  /** The view of `epoch` landed: every result read in it (or since) is followed from now, the older ones let go. */
  follow(epoch: number): Promise<void>;
  /** Told each time a followed result changes; the returned function stops telling. */
  onLive(listener: (e: LiveEvent) => void): () => void;
}
