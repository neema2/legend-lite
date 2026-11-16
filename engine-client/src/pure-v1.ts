// legend-engine's `pure/v1` API: the ONE client every server call goes
// through (docs/UPSTREAM_ENDPOINTS_DESIGN_2026_09_27.md, U4). Its planning half -- parse, print, relation types, a
// model's elements, generatePlan -- is pure-v1-planning.ts's PureV1Planning, which a planner holds (C5-T, 2026-11-15);
// this adds E8, `execute`, the SERVER's door: the server plans and runs a query in one request
// (`LegendEngineExecutor`, engine-remote.ts).

import type { Lambda } from '../../pure-protocol/src/index.ts';
import { parseExact as parseExactValues } from './values.ts';
import { PureV1Planning } from './pure-v1-planning.ts';

export { PureV1Planning, type Failure, type PrintStyle, type PureV1Options } from './pure-v1-planning.ts';

/** A server's JSON answer with its numbers exact (values.ts): a SyntaxError names the answer. */
function parseExact(raw: string): unknown {
  try {
    return parseExactValues(raw);
  } catch (e) {
    if (e instanceof SyntaxError) throw new Error(`the server's answer was not JSON: ${raw.slice(0, 200)}`);
    throw e;
  }
}

/** The first four bytes of a zstd frame (RFC 8878 §3.1.1): how an ARROW_IPC answer begins. */
const ZSTD_MAGIC = [0x28, 0xb5, 0x2f, 0xfd];

/**
 * An ARROW_IPC answer's body as its Arrow IPC stream: upstream writes the stream as one zstd frame
 * (RelationalResultToArrowIPCSerializer). A body that is not one -- the JSON an engine answers when it does not serve
 * the format -- is refused, naming what came instead; it is never read in its place. The decoder (fzstd) is loaded on
 * the first Arrow answer, so a page whose engines answer JSON never carries it (datacube's bundle budget).
 */
export async function arrowIpcStream(body: Uint8Array): Promise<Uint8Array> {
  if (!ZSTD_MAGIC.every((b, i) => body[i] === b)) {
    throw new Error(`the answer is not ARROW_IPC (a zstd frame); it begins: ${new TextDecoder().decode(body.subarray(0, 120))}`);
  }
  const { decompress } = await import('fzstd');
  try {
    return decompress(body);
  } catch (e) {
    throw new Error(`the ARROW_IPC answer does not decompress: ${String(e)}`);
  }
}

/** legend-engine's `pure/v1` client with E8, `execute`: the server's door (the planning half: PureV1Planning). */
export class PureV1Client extends PureV1Planning {
  /** E8 `execution/execute`: the rows for a query, run by the server, as the TDS JSON (an engine read as JSON). */
  async execute(query: Lambda, signal?: AbortSignal): Promise<unknown> {
    if (this.serializationFormat !== undefined) {
      throw this.fail(`this engine is read as ${this.serializationFormat}: its rows come from executeArrow`, query);
    }
    const response = await this.request('/execution/execute', this.executeInput(query), query, signal, 'json');
    return parseExact(await response.text());
  }

  /**
   * E8 in upstream's Arrow format (`?serializationFormat=ARROW_IPC`, an engine read as ARROW_IPC): the Arrow IPC
   * stream, out of its zstd frame (arrowIpcStream).
   */
  async executeArrow(query: Lambda, signal?: AbortSignal): Promise<Uint8Array> {
    if (this.serializationFormat !== 'ARROW_IPC') {
      throw this.fail('this engine is read as JSON: its rows come from execute', query);
    }
    const response = await this.request('/execution/execute?serializationFormat=ARROW_IPC', this.executeInput(query),
      query, signal, 'json');
    // the body read outside: an abort while it arrives is the caller's signal, as request keeps it
    const body = new Uint8Array(await response.arrayBuffer());
    try {
      return await arrowIpcStream(body);
    } catch (e) {
      throw this.fail(`the server at ${this.baseUrl}: ${e instanceof Error ? e.message : String(e)}`, query);
    }
  }

  /** An execute's input (E8): the query from its runtime, timed out and checked as upstream's clients send it. */
  protected executeInput(query: Lambda): object {
    return this.input(this.fromRuntime(query), {
      queryTimeOutInSeconds: 60,
      enableConstraints: true,
    });
  }
}
