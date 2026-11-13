// DATACUBE'S ENGINE CONTRACT (docs/DATACUBE_ENGINE_CONTRACT_2026_11_12.md, ratified by the user 2026-11-13 with two
// amendments; the orchestrator's answers to the step-1 plan, 2026-11-13): ONE intent API, every engine behind it -- the
// tab's (in-thread), Python headless (the V8 bridge), Python's page, the Java server, the warehouse, and later a real
// legend-engine. DataCube is a pure client of intent: it sends Pure and choices, and gets typed answers back. No SQL, no
// plan, no path, no connection and no secret ever crosses this contract (R3, R4, R5).
//
// TYPES, the version, the typed errors and the capability check: no runtime dependency, so every app's bundle carries
// it for nothing (the transports are their own modules, loaded where they are used).

import type { RecordBatch } from 'apache-arrow';
import type { Lambda, ValueSpecification } from '../../pure-protocol/src/index.ts';
import type { Receipt } from './receipt.ts';

/** The contract's version, as `capabilities()` states it: a client refuses, by name, an engine it cannot speak. */
export const ENGINE_CONTRACT_VERSION = 1;

// ------------------------------------------------------------------------------------------------------------- calls

/** Every call of the contract, by name: what a capability lists and a refusal names. */
export type CallName =
  | 'capabilities' | 'open' | 'close'
  | 'addSource' | 'describe' | 'drop'
  | 'typeOf' | 'query' | 'write' | 'cancel'
  | 'parse' | 'print' | 'elements';

/** A request's id, the client's: what `cancel` names. */
export type RequestId = string;

/** What every call but `cancel` and `close` may carry. */
export interface CallOptions {
  /** The request's id, for `cancel`. */
  readonly request?: RequestId;
  /** Aborts the call on the client's side; the engine is asked to cancel as `cancel` asks (best effort). */
  readonly signal?: AbortSignal;
}

/** A source the engine holds, by the name it issued: opaque to DataCube, resolved by the engine alone (R3). */
export interface Handle {
  readonly handle: string;
}

/**
 * A QUERY, AS INTENT: Pure and choices. Never SQL, a plan, a path, a connection or a secret.
 *
 * The row cap is IN the Pure (`take(cap + 1)`, the person's `maxRows`: query.ts), as DataCube writes it today -- no
 * separate argument (the orchestrator's Q1, 2026-11-13). The model declares no connection and no runtime: the engine
 * refuses one by name; `runtime` names a runtime the ENGINE knows -- one it issued (`SourceAnswer`) or one of its own
 * project's (Q2).
 */
export interface QueryAsk {
  /** The source's model, as Pure text: its classes, stores and mappings, no connection and no runtime. */
  readonly model: string;
  /** A runtime the engine knows, by its name. */
  readonly runtime: string;
  /** The query, as pure-protocol JSON. */
  readonly query: Lambda;
  /** The query's parameters' values, by name. */
  readonly parameters?: Readonly<Record<string, ValueSpecification>>;
  /** How the query reads the model: through a mapping (`->from(mapping, runtime)`), or as a project's own. */
  readonly how?: { readonly mapping?: string; readonly project?: true };
}

/** A result column: its name and its Pure type, as the compiler typed it (never the database's wire type). */
export interface ResultColumn {
  readonly name: string;
  readonly type: string;
  /** The model's enumeration the compiler typed it with, when it is one (relation-type.ts PlanColumn). */
  readonly enumeration?: string;
}

/** What `typeOf` answers: a query's result type, compiled, nothing run. */
export interface ResultType {
  readonly columns: readonly ResultColumn[];
}

/** What `query` answers: its rows, typed by the compiler, as Arrow. */
export interface QueryAnswer {
  /** The id the engine's door minted for this run (G10: the door mints, exec binds). */
  readonly executionId: string;
  readonly schema: readonly ResultColumn[];
  /** The rows, a batch at a time: Arrow objects in-process, IPC bytes decoded by a wire transport (Q9). */
  readonly rows: AsyncIterable<RecordBatch>;
  /** What ran, as text a person reads (R4: read-only, never sent back): the query panes' SQL. */
  readonly explain?: string;
  /** Where the rows were read, as the engine says it (receipt.ts). */
  readonly receipt?: Receipt;
}

/** A source to add: bytes a person gave, a URL, a remote reference, or a grid's query (a Snap's pull, Q5). */
export type SourceAsk =
  | { readonly upload: { readonly name: string; readonly bytes: Uint8Array } }
  | { readonly url: string }
  /** A table or object the engine reaches by its own connection (a warehouse table, an S3 object): any secret it
   *  needs is bound by name inside the engine (R5). */
  | { readonly remote: { readonly kind: string; readonly ref: string } }
  /** A query's rows copied into a table of the engine's own: `how` is 'snap' alone. The pull, never SQL (B3). */
  | { readonly query: QueryAsk };

/** How a source is added: read in place (`live`, a mount) or copied into the engine's database (`snap`). */
export type SourceHow = 'live' | 'snap';

/** What `addSource` answers: the handle, what it is, and the model the ENGINE wrote over it (Q3). */
export interface SourceAnswer {
  readonly handle: Handle;
  readonly name: string;
  /** The bytes' SHA-256, lower-case hex (an upload's; a remote's as the engine read it). */
  readonly digest: string;
  readonly size: number;
  /** The model over the source, as Pure text, and the runtime (by name) it is read through: what a QueryAsk sends. */
  readonly model: string;
  readonly runtime: string;
  /** The relation a query over the source starts from. */
  readonly relation: ValueSpecification;
}

/** What `describe` answers: a source's columns, as the engine's catalog types them. */
export interface Described {
  readonly columns: readonly ResultColumn[];
}

/** A table Output's mode: DataCube's five (layout/flow.ts TABLE_MODES; Q10, 2026-11-13) -- made, made again, appended,
 *  its rows deleted then appended, merged by its keys. */
export type WriteMode =
  | 'create' | 'overwrite' | 'append' | 'deleteAppend'
  | { readonly merge: { readonly keys: readonly string[] } };

/** A box's rows written into a table of the engine's database: ONE call, the engine runs the transaction (R2). */
export interface WriteAsk {
  readonly box: QueryAsk;
  readonly target: { readonly table: string };
  readonly mode: WriteMode;
}

/** What `write` answers: the rows the database wrote. */
export interface WriteAnswer {
  readonly executionId: string;
  readonly rowsWritten: number;
}

/** How `print` writes Pure for a person. */
export type PrintStyle = 'PRETTY' | 'COMPACT';

// ------------------------------------------------------------------------------------------------------ capabilities

/** What an engine does, and the contract's version it speaks. Anything not listed is refused by name if asked. */
export interface Capabilities {
  readonly contract: number;
  readonly engine: { readonly name: string; readonly databaseType?: string };
  readonly calls: {
    readonly typeOf: boolean;
    readonly query: boolean;
    readonly cancel: boolean;
    readonly describe: boolean;
    readonly drop: boolean;
    /** The reading calls: compile-only, no database (Q4). */
    readonly parse: boolean;
    readonly print: boolean;
    readonly elements: boolean;
    readonly addSource?: { readonly sources: readonly SourceKind[]; readonly how: readonly SourceHow[] };
    readonly write?: { readonly modes: readonly WriteModeName[] };
  };
}

/** A source's kind, by name. */
export type SourceKind = 'upload' | 'url' | 'remote' | 'query';

/** A write mode, by name. */
export type WriteModeName = 'create' | 'overwrite' | 'append' | 'deleteAppend' | 'merge';

// ----------------------------------------------------------------------------------------------------------- engine

/** An engine: a planner, a runner and a database owned together. DataCube never knows which. */
export interface Engine {
  capabilities(): Promise<Capabilities>;
  open(): Promise<EngineSession>;
}

/** A session: the handles it issues live in it; `close` drops what it made. */
export interface EngineSession {
  addSource(source: SourceAsk, how: SourceHow, o?: CallOptions): Promise<SourceAnswer>;
  describe(handle: Handle, o?: CallOptions): Promise<Described>;
  drop(handle: Handle, o?: CallOptions): Promise<void>;
  typeOf(ask: QueryAsk, o?: CallOptions): Promise<ResultType>;
  query(ask: QueryAsk, o?: CallOptions): Promise<QueryAnswer>;
  write(ask: WriteAsk, o?: CallOptions): Promise<WriteAnswer>;
  /** What a person typed, as its lambda (compile-only). */
  parse(text: string, o?: CallOptions): Promise<Lambda>;
  /** A query as Pure text for a person to read (compile-only). */
  print(query: Lambda, style?: PrintStyle, o?: CallOptions): Promise<string>;
  /** A model's elements as the compiler reads them: a saved query's project (compile-only). */
  elements(model: string, o?: CallOptions): Promise<unknown[]>;
  /** Best effort; an unknown request is no error, and an answer already streamed stays valid. */
  cancel(request: RequestId): Promise<void>;
  close(): Promise<void>;
}

// ----------------------------------------------------------------------------------------------------------- errors

/** An error's kind: what DataCube shows, and never reroutes on (R6, G7). */
export type EngineErrorKind = 'compile' | 'run' | 'refused' | 'transport';

/** Every error an engine answers: its kind, the call, and the door's execution id where a door was reached. */
export abstract class EngineError extends Error {
  abstract readonly kind: EngineErrorKind;
  readonly call: CallName;
  readonly executionId: string | undefined;
  readonly request: RequestId | undefined;

  constructor(message: string, call: CallName, o: { readonly executionId?: string; readonly request?: RequestId; readonly cause?: unknown } = {}) {
    super(message, o.cause === undefined ? undefined : { cause: o.cause });
    this.call = call;
    this.executionId = o.executionId;
    this.request = o.request;
  }
}

/** The query does not type: the compiler's words and where. Nothing ran. */
export class CompileError extends EngineError {
  readonly kind = 'compile' as const;
  readonly where: { readonly line: number; readonly column: number } | undefined;

  constructor(message: string, call: CallName, o: { readonly where?: { readonly line: number; readonly column: number }; readonly executionId?: string; readonly request?: RequestId; readonly cause?: unknown } = {}) {
    super(message, call, o);
    this.name = 'CompileError';
    this.where = o.where;
  }
}

/** The database refused: its own words, any secret redacted by the engine. */
export class RunError extends EngineError {
  readonly kind = 'run' as const;

  constructor(message: string, call: CallName, o: { readonly executionId?: string; readonly request?: RequestId; readonly cause?: unknown } = {}) {
    super(message, call, o);
    this.name = 'RunError';
  }
}

/** The engine does not do that, BY NAME: a call it is not capable of, an option it does not offer, an unknown handle, a
 *  contract version it does not speak. The client never routes the call another way (G7). */
export class Refused extends EngineError {
  readonly kind = 'refused' as const;
  /** What was refused, by name ('write', "mode 'merge'", "handle 'h-…'", 'contract version 2'). */
  readonly what: string;
  /** The engine that refused, by its name. */
  readonly engine: string;

  constructor(call: CallName, what: string, engine: string, why: string, o: { readonly executionId?: string; readonly request?: RequestId } = {}) {
    super(`${engine} refuses ${what}: ${why}`, call, o);
    this.name = 'Refused';
    this.what = what;
    this.engine = engine;
  }
}

/** The link failed: no door may have been reached, so there may be no execution id. */
export class TransportError extends EngineError {
  readonly kind = 'transport' as const;

  constructor(message: string, call: CallName, o: { readonly executionId?: string; readonly request?: RequestId; readonly cause?: unknown } = {}) {
    super(message, call, o);
    this.name = 'TransportError';
  }
}

/** Whether `e` is an engine's error, by its kind (errors cross realms: V8, workers -- so never by `instanceof` alone). */
export function isEngineError(e: unknown): e is EngineError {
  if (typeof e !== 'object' || e === null) return false;
  const { kind, call } = e as { kind?: unknown; call?: unknown };
  return (kind === 'compile' || kind === 'run' || kind === 'refused' || kind === 'transport') && typeof call === 'string';
}

// ------------------------------------------------------------------------------------------------- the client's checks

/** `caps`, if the client speaks its contract's version; else refused by name -- never spoken to anyway. */
export function requireContract(caps: Capabilities): Capabilities {
  if (caps.contract !== ENGINE_CONTRACT_VERSION) {
    throw new Refused('capabilities', `contract version ${String(caps.contract)}`, caps.engine.name,
      `this client speaks contract version ${ENGINE_CONTRACT_VERSION}`);
  }
  return caps;
}

/**
 * Why `call` (with its option, if any) is refused by `caps`, or undefined: what a session checks BEFORE anything
 * crosses (R6). `addSource` names its source's kind and how; `write` its mode.
 */
export function refusalOf(caps: Capabilities, call: CallName, option?: { readonly source?: SourceKind; readonly how?: SourceHow; readonly mode?: WriteModeName }): Refused | undefined {
  const engine = caps.engine.name;
  const not = (what: string, why: string): Refused => new Refused(call, what, engine, why);
  switch (call) {
    case 'capabilities':
    case 'open':
    case 'close':
      return undefined;
    case 'typeOf':
    case 'query':
    case 'cancel':
    case 'describe':
    case 'drop':
    case 'parse':
    case 'print':
    case 'elements':
      return caps.calls[call] ? undefined : not(call, 'its capabilities do not list it');
    case 'addSource': {
      const offered = caps.calls.addSource;
      if (offered === undefined) return not(call, 'its capabilities do not list it');
      if (option?.source !== undefined && !offered.sources.includes(option.source)) return not(`a source of kind '${option.source}'`, `it adds ${offered.sources.join(', ') || 'none'}`);
      if (option?.how !== undefined && !offered.how.includes(option.how)) return not(`a source added '${option.how}'`, `it adds ${offered.how.join(', ') || 'none'}`);
      if (option?.source === 'query' && option.how !== 'snap') return not("a query's source added live", "a query's rows are added as a snap alone");
      return undefined;
    }
    case 'write': {
      const offered = caps.calls.write;
      if (offered === undefined) return not(call, 'its capabilities do not list it');
      if (option?.mode !== undefined && !offered.modes.includes(option.mode)) return not(`mode '${option.mode}'`, `it writes ${offered.modes.join(', ') || 'none'}`);
      return undefined;
    }
  }
}

/** A source ask's kind, by name. */
export function sourceKind(source: SourceAsk): SourceKind {
  if ('upload' in source) return 'upload';
  if ('url' in source) return 'url';
  if ('remote' in source) return 'remote';
  return 'query';
}

/** A write mode's name. */
export function writeModeName(mode: WriteMode): WriteModeName {
  return typeof mode === 'string' ? mode : 'merge';
}
