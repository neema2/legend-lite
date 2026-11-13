// AN ENGINE ACROSS A WIRE (docs/DATACUBE_ENGINE_CONTRACT_2026_11_12.md §5): the contract's calls sent as JSON to an
// engine that answers them at `/contract/v1/<call>` -- its small answers JSON, a query's rows Arrow IPC with the
// contract's facts in its schema's metadata (`legend.contract`), a failure `{"error": {...}}` read back as the typed
// error it is. One client for every wire: the V8 bridge (`bridge`, Python headless: the host's `engine` call) and HTTP
// (`http`, Python's page, the Java server, the warehouse: step 5 and 6). Every call is checked against the engine's
// capabilities BEFORE anything crosses: a call or option not listed is refused by name, never sent (R6).

import { tableFromIPC } from 'apache-arrow';
import type { Lambda } from '../../pure-protocol/src/index.ts';
import type { Receipt } from './receipt.ts';
import {
  CompileError, Refused, refusalOf, requireContract, RunError, sourceKind, TransportError, writeModeName,
  type CallName, type CallOptions, type Capabilities, type Engine, type EngineError, type EngineSession,
  type Described, type ModelOver, type QueryAnswer, type ResultColumn, type ResultType, type SourceAnswer, type SourceHow, type WriteAnswer,
} from './engine-contract.ts';

/** The path every call is asked at, its name after it. */
export const CONTRACT_PATH = '/contract/v1/';

/** The metadata key a query answer's contract facts ride under. */
const FACTS = 'legend.contract';

/** One call across the wire: the call's path and its JSON body in; its status, media type and bytes out. */
export type WireSend = (path: string, body: string, signal?: AbortSignal) =>
  Promise<{ readonly status: number; readonly contentType: string; readonly body: Uint8Array }>;

/** An engine reached across `send`, by the name it is known as where a link failure is said. */
export function wireEngine(send: WireSend, where: string): Engine {
  let caps: Promise<Capabilities> | undefined;
  const capabilities = (): Promise<Capabilities> => (caps ??= ask(send, where, 'capabilities', {})
    .then((said) => requireContract(JSON.parse(text(said.body)) as Capabilities))
    .catch((e: unknown) => {
      caps = undefined;
      throw e;
    }));
  return {
    capabilities,
    async open() {
      const c = await capabilities();
      const { session } = JSON.parse(text((await ask(send, where, 'open', {})).body)) as { session: string };
      return wireSession(send, where, c, session);
    },
  };
}

function wireSession(send: WireSend, where: string, caps: Capabilities, session: string): EngineSession {
  let closed = false;
  const call = async (name: CallName, body: Record<string, unknown>, o?: CallOptions,
    option?: Parameters<typeof refusalOf>[2]): Promise<Uint8Array> => {
    if (closed) throw new Refused(name, `a call on a closed session`, caps.engine.name, 'its session was closed');
    const refused = refusalOf(caps, name, option);
    if (refused !== undefined) throw refused;
    const said = await ask(send, where, name, { session, ...body, ...(o?.request ? { request: o.request } : {}) }, o?.signal);
    return said.body;
  };
  // each answer read as the JSON it is, and typed by the call it answers (never a generic cast: brand-casts.test.ts)
  const json = async (name: CallName, body: Record<string, unknown>, o?: CallOptions,
    option?: Parameters<typeof refusalOf>[2]): Promise<unknown> => JSON.parse(text(await call(name, body, o, option)));
  return {
    // an upload's bytes cross as base64 (a wire carries JSON)
    addSource: async (source, how: SourceHow, o) => (await json('addSource', {
      source: 'upload' in source ? { upload: { name: source.upload.name, bytes: base64(source.upload.bytes) } } : source, how,
      ...(o?.name !== undefined ? { name: o.name } : {}),
    }, o, { source: sourceKind(source), how })) as SourceAnswer,
    modelOf: async (handles, o) => (await json('modelOf', { handles }, o)) as ModelOver,
    // write-only: the engine answers nothing, and this client keeps nothing of it
    bindSecret: async (name, fields, o) => { await call('bindSecret', { name, fields }, o); },
    describe: async (handle, o) => (await json('describe', { handle }, o)) as Described,
    drop: async (handle, o) => { await call('drop', { handle }, o); },
    typeOf: async (ask, o) => (await json('typeOf', { ask }, o)) as ResultType,
    query: async (ask, o) => queryAnswer(await call('query', { ask }, o), where),
    write: async (ask, o) => (await json('write', { ask }, o, { mode: writeModeName(ask.mode) })) as WriteAnswer,
    parse: async (t, o) => ((await json('parse', { text: t }, o)) as { lambda: Lambda }).lambda,
    print: async (query, style, o) => ((await json('print', { query, ...(style ? { style } : {}) }, o)) as { text: string }).text,
    elements: async (model, o) => ((await json('elements', typeof model === 'string' ? { model } : { project: model.project }, o)) as { elements: unknown[] }).elements,
    cancel: async (request) => {
      if (closed) return;
      await call('cancel', { request });
    },
    close: async () => {
      if (closed) return;
      closed = true;
      await ask(send, where, 'close', { session });
    },
  };
}

/** One call asked, its answer when it is one; its failure as the typed error it is. */
async function ask(send: WireSend, where: string, name: CallName, body: Record<string, unknown>, signal?: AbortSignal):
  Promise<{ readonly status: number; readonly contentType: string; readonly body: Uint8Array }> {
  let said: Awaited<ReturnType<WireSend>>;
  try {
    said = await send(`${CONTRACT_PATH}${name}`, JSON.stringify(body), signal);
  } catch (e) {
    if (signal?.aborted) throw e;
    throw new TransportError(`${where} did not answer: ${e instanceof Error ? e.message : String(e)}`, name, { cause: e });
  }
  if (said.status >= 200 && said.status < 300) return said;
  throw failureOf(said, name, where);
}

/** A failure's answer as its typed error: the engine's `{"error"}`, or -- anything else -- the link's. */
function failureOf(said: { readonly status: number; readonly contentType: string; readonly body: Uint8Array }, call: CallName,
  where: string): EngineError {
  let error: { kind?: string; call?: CallName; message?: string; executionId?: string; what?: string; engine?: string;
    where?: { line: number; column: number } } | undefined;
  if (said.contentType.startsWith('application/json')) {
    try {
      error = (JSON.parse(text(said.body)) as { error?: typeof error }).error;
    } catch {
      error = undefined;
    }
  }
  if (error === undefined || typeof error.message !== 'string') {
    return new TransportError(`${where} answered ${said.status}: ${text(said.body).slice(0, 500)}`, call);
  }
  const o = { ...(error.executionId !== undefined ? { executionId: error.executionId } : {}) };
  const at = error.call ?? call;
  switch (error.kind) {
    case 'compile': return new CompileError(error.message, at, { ...o, ...(error.where ? { where: error.where } : {}) });
    case 'run': return new RunError(error.message, at, o);
    case 'refused': {
      // the engine's own words, whole: what it refused and why
      const r = new Refused(at, error.what ?? at, error.engine ?? where, '', o);
      r.message = error.message;
      return r;
    }
    default: return new TransportError(`${where} answered an error of no kind the contract names: ${error.message}`, call, o);
  }
}

/** A query's answer: its Arrow rows, and the contract's facts its schema carries -- refused when they are missing. */
function queryAnswer(bytes: Uint8Array, where: string): QueryAnswer {
  let table: ReturnType<typeof tableFromIPC>;
  try {
    table = tableFromIPC(bytes);
  } catch (e) {
    throw new TransportError(`${where}'s rows could not be read: ${e instanceof Error ? e.message : String(e)}`, 'query', { cause: e });
  }
  const said = table.schema.metadata.get(FACTS);
  if (said === undefined) throw new TransportError(`${where} answered rows without the contract's facts`, 'query');
  const facts = JSON.parse(said) as { executionId: string; schema: ResultColumn[]; explain?: string; receipt?: Receipt };
  return {
    executionId: facts.executionId,
    schema: facts.schema,
    rows: (async function* batches() {
      for (const batch of table.batches) yield batch;
    })(),
    ...(facts.explain !== undefined ? { explain: facts.explain } : {}),
    ...(facts.receipt !== undefined ? { receipt: facts.receipt } : {}),
  };
}

function base64(bytes: Uint8Array): string {
  let s = '';
  for (let i = 0; i < bytes.length; i += 0x8000) s += String.fromCharCode(...bytes.subarray(i, i + 0x8000));
  return btoa(s);
}

function text(bytes: Uint8Array): string {
  return new TextDecoder().decode(bytes);
}

/**
 * THE V8 BRIDGE (Python headless): the host's `engine(path, body)` call (datacube's headless calls.ts), whose answer is
 * `{status, contentType, body}` as JSON, the body base64 -- answered by Python's engine over the read the call was made
 * in. No call carries a statement or a plan.
 */
export function bridge(engine: (path: string, body: string) => Promise<string>, fromBase64: (b: string) => Uint8Array): Engine {
  return wireEngine(async (path, body) => {
    const said = JSON.parse(await engine(path, body)) as { status: number; contentType: string; body: string };
    return { status: said.status, contentType: said.contentType, body: fromBase64(said.body) };
  }, 'the host');
}
