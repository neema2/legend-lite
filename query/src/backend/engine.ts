// The engine Query runs on (engine-client's in-tab legend engine, engine-client/src/legend/engine.ts: the planner in
// the tab, a legend-lite server, or legend-engine) and the query store (query-store/src/client.ts), as Query reads them.

export * from '../../../engine-client/src/legend/engine.ts';
/** The query store, `pure/v1/query`: the one client (query-store/src/client.ts), wherever the store is. */
export type { QueryStore } from '../../../query-store/src/client.ts';
