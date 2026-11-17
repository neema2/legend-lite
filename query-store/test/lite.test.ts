// legend-lite's server (`//core:server --query-store`), held to the one suite in both its dialects: lite's own
// `/api/lite/v1/queries` (the store the page answers is the store a server answers) and legend-engine's
// `/api/pure/v1/query`, which the server's edge answers byte-exact (H6, 2026-11-17).

import { conformance, ENGINE, LITE } from './conformance.ts';
import { liteServer } from './lite-server.ts';

const server = liteServer();
const target = () => ({ api: server.api(), fetch: globalThis.fetch.bind(globalThis), user: 'anonymous' });

conformance("legend-lite's server", target, LITE);
conformance("legend-lite's server", target, ENGINE);
