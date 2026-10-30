// The calls an embedding host answers (host.ts says what for), and `crypto.subtle.digest` over them. A module that
// imports nothing beyond web.ts, so install-web.ts can install the digest before any module that needs it loads.

import { base64, fromBase64 } from './web.ts';

/** What the host answers. Bytes cross as base64: an engine and its host share strings, not memory. */
export interface HostCalls {
  /** One planner request (engine-client's planner-protocol.ts's Request, as JSON), answered as its Response (JSON). */
  planner(request: string): Promise<string>;
  /** One SQL statement run by the host's database: its result as an Arrow IPC stream, base64. */
  query(sql: string): Promise<string>;
  /** One SQL statement run with its `?`s bound (`params`: their values, a JSON list) -- a write's step (an Output into a
   *  table: table-output.ts), run in no read of the host's: its result as `query`'s. */
  statement(sql: string, params: string): Promise<string>;
  /** A file the database can read by `name` (DuckDB-WASM's registerFileBuffer): its bytes, base64. */
  registerFile(name: string, bytes: string): Promise<void>;
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
