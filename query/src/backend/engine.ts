// The engine Query runs on: legend-lite's own (engine-client's LiteEngine; the edge program's H3 and H7, 2026-11-17) --
// lite's planner in the tab with the hosts' plan runner (TabEngine), or a legend-lite server's /api/lite/v1
// (LiteServer) -- and the query store (query-store/src/client.ts), as Query reads them. legend-engine's API is the
// edge's, never asked here.

export type { LiteAnswer, LiteEngine } from '../../../engine-client/src/legend/browser-engine.ts';
export { TabEngine } from '../../../engine-client/src/legend/browser-engine.ts';
export { LiteServer } from '../../../engine-client/src/doors/lite-server.ts';
/** What reads a model's text as its protocol JSON: the engine, or the tab's planner before an engine is made. */
export type Grammar = Pick<import('../../../engine-client/src/legend/browser-engine.ts').LiteEngine, 'modelElements'>;
/** The query store: the one client (query-store/src/client.ts), wherever the store is. */
export type { QueryStore } from '../../../query-store/src/client.ts';
