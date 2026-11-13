// The calls an embedding host answers (host.ts says what for), and `crypto.subtle.digest` over them. A module that
// imports nothing beyond web.ts, so install-web.ts can install the digest before any module that needs it loads.

import { base64, fromBase64 } from './web.ts';

/**
 * What the host answers. Bytes cross as base64: an engine and its host share strings, not memory. What DataCube's code
 * asks of the host's database is INTENT, each planned by the host's planner and run by its runner (data-host.ts; the #6
 * bridge, the user's go of 2026-11-12): no call carries a statement or a plan of DataCube's -- until then `query` and
 * `statement` carried each statement DataCube's code had planned, both deleted.
 */
export interface HostCalls {
  /** One planner request (engine-client's planner-protocol.ts's Request, as JSON), answered as its Response (JSON). */
  planner(request: string): Promise<string>;
  /** One load (data-host.ts LoadRequest, JSON: the planner's request), planned and run by the host on its database,
   *  binding what `holds` names (LoadHolds, JSON); a mount's settings of the connection kept by the host. */
  load(request: string, holds: string): Promise<void>;
  /** Host operations (host-operations.ts HostOperation[], JSON) planned for `databaseType` and run in order by the host:
   *  each one's last statement's rows, JSON. */
  operations(databaseType: string, operations: string): Promise<string>;
  /** A table's columns as the host's catalog says them: `{"columns", "rows"}`, JSON. */
  catalog(schema: string, table: string): Promise<string>;
  /** A table Output (table-output.ts OutputRequest, JSON: the box's tables and the target as the catalog answered them,
   *  the output, its rows as a lambda), planned by the host as ONE plan (Boundary.outputPlan) and run in the plan's own
   *  transaction: its answer, lite's JSON rows (the DataCube stream's merge, 2026-11-13: until then a dialogue of a
   *  transaction's steps, a create and a write, which the one plan replaced). */
  output(request: string): Promise<string>;
  /** A digest (crypto.subtle.digest): the algorithm's name ('SHA-256', ...) and the data, base64; the digest, base64. */
  digest(algorithm: string, data: string): Promise<string>;
  /**
   * One HTTP call DataCube's code makes of a service (the model actions' SDLC and model writer: D4), answered by the
   * host -- its own by a path (`/sdlc/api/...`, `/api/lite/v1/...`: its model home and its writer, as its engine answers
   * a tab), a server's by its address: the method, the URL and the body (or null); `{status, body}` as JSON.
   */
  fetch(method: string, url: string, body: string | null): Promise<string>;
  /**
   * One call of the engine the host runs beside DataCube (docs/DATACUBE_ONE_API_PLAN_2026_10_21.md X1, D13), answered by
   * the engine's own code as the tab's call of the same path is: `/plan-and-run.json` (a grid's query planned and run
   * where its rows are) or `/warehouse.json` (a warehouse table opened under the session the engine holds). The path and
   * the body; `{status, contentType, body}` as JSON, the body base64.
   */
  engine(path: string, body: string): Promise<string>;
}

/** The host's calls, defined as `legendLiteHost` before the bundle loads; none is a bundle run where it does not belong. */
export function hostCalls(): HostCalls {
  const calls = (globalThis as { legendLiteHost?: HostCalls }).legendLiteHost;
  if (calls === undefined) {
    throw new Error("no host: this code runs inside legend-lite's Python host, which defines legendLiteHost first");
  }
  return calls;
}

/** `crypto.subtle.digest` by the host, where the engine has no crypto (cube-document.ts's content digest). */
export function installDigest(calls: HostCalls, g: Record<string, unknown> = globalThis as unknown as Record<string, unknown>): void {
  const crypto = (g['crypto'] ?? {}) as { subtle?: unknown };
  if (crypto.subtle !== undefined) return;
  const toBytes = (data: ArrayBuffer | ArrayBufferView): Uint8Array =>
    ArrayBuffer.isView(data) ? new Uint8Array(data.buffer, data.byteOffset, data.byteLength) : new Uint8Array(data);
  crypto.subtle = {
    async digest(algorithm: string | { name: string }, data: ArrayBuffer | ArrayBufferView): Promise<ArrayBuffer> {
      const name = typeof algorithm === 'string' ? algorithm : algorithm.name;
      const bytes = fromBase64(await calls.digest(name, base64(toBytes(data))));
      return bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength) as ArrayBuffer;
    },
  };
  if (g['crypto'] === undefined) {
    Object.defineProperty(g, 'crypto', { value: crypto, writable: true, configurable: true, enumerable: false });
  }
}
