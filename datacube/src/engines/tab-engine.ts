// THE TAB'S ENGINE, IN-THREAD (docs/DATACUBE_ENGINE_CONTRACT_2026_11_12.md; amendment (a): no Worker): the tab's planner
// (legend-lite's module) and its database (DuckDB-WASM), owned together behind DataCube's one `Engine` contract. Built at
// step 2 (2026-11-13) so the conformance suite holds the tab to the same asks as Python; DataCube moves onto it at step 4,
// when heldDataHost goes. Until then nothing of DataCube's app calls it.
//
// What it does: typeOf, query, write (one call, the one plan's own transaction), parse, print, elements, cancel (a no-op:
// DuckDB-WASM runs a statement to its end once begun). describe, drop and addSource come with step 3. Its models obey
// R3a: a model sent to it holds no connection or runtime (checked on its PARSED elements); the tab owns its runtimes,
// keeping the Connection and Runtime of each model it wrote (served), and handing DataCube the rest by the runtime's name.

import type { HeldConnection } from '../../../engine-client/src/duckdb.ts';
import {
  CompileError, ENGINE_CONTRACT_VERSION, Refused, refusalOf, RunError, sourceKind, writeModeName,
  type CallName, type Capabilities, type Engine, type EngineSession, type QueryAsk, type ResultColumn, type WriteMode,
} from '../../../engine-client/src/engine-contract.ts';
import { runPlan } from '../../../engine-client/src/plan-runner.ts';
import type { Plan } from '../../../engine-client/src/relation-type.ts';
import { pureType } from '../../../engine-client/src/relation-type.ts';
import { recordBatchOf } from '../../../engine-client/src/engine-arrow.ts';
import { isFractional, isNumeric } from '../../../engine-client/src/types.ts';
import type { PlannedSql } from '../../../engine-client/src/planned-sql.ts';
import { toJson, type Lambda } from '../../../pure-protocol/src/index.ts';
import type { CatalogColumn, ModelTable, OutputRequest } from '../infer.ts';
import type { LitePlanner } from '../lite-planner.ts';
import { catalogColumnsOf } from '../upload.ts';

const ENGINE = 'the tab';

/** The kinds of element a model sent to an engine may not hold (R3a (a)), as the parser names them. */
const OWNED_KINDS = new Set(['connection', 'runtime']);
const SECTION_INDEX = 'sectionIndex';

/** What the tab's engine runs on: its database, held for a write alone, and its planner (legend-lite's module). */
export interface TabEngineOptions {
  readonly database: {
    readonly databaseType: string;
    execute(plan: Plan, epoch: number, signal?: AbortSignal): Promise<import('../../../engine-client/src/result.ts').ResultTable>;
    exclusive<T>(work: (held: HeldConnection) => Promise<T>): Promise<T>;
  };
  /** The tab's planner: planners over a model are made from it (withModel), and it answers the catalog's question and
   *  a write's plan. */
  readonly planner: LitePlanner;
  /** Whether a table is one of the tab's own (a file's, a copy's): no write names one (Q11 (b)). */
  readonly owns: (table: string) => boolean;
}

/** The tab's engine, and what it serves DataCube a model it wrote as (R3a (c)). */
export interface TabEngine extends Engine {
  served(model: string, runtime: string): Promise<{ readonly model: string; readonly runtime: string }>;
}

interface Element { readonly _type?: string; readonly package?: string; readonly name?: string;
  readonly schemas?: readonly { readonly name?: string; readonly tables?: readonly { readonly name: string }[] }[] }

const pathOf = (e: Element): string => (e.package ? `${e.package}::${e.name}` : String(e.name));

/** The sections of a model's text (each from its `###Name` line to the next) named `names`, in order. */
function sections(text: string, names: readonly string[]): string {
  let keep = false;
  const out: string[] = [];
  for (const line of text.split(/(?<=\n)/)) {
    if (line.startsWith('###')) keep = names.includes(line.slice(3).trim());
    if (keep) out.push(line);
  }
  return out.join('');
}

/** A random id: crypto's where the tab has it. */
function mint(): string {
  return globalThis.crypto.randomUUID().replaceAll('-', '');
}

export function tabEngine(o: TabEngineOptions): TabEngine {
  const runtimes = new Map<string, string>();
  const servedAs = new Map<string, string>();
  const caps: Capabilities = {
    contract: ENGINE_CONTRACT_VERSION,
    engine: { name: ENGINE, databaseType: o.database.databaseType },
    calls: { typeOf: true, query: true, cancel: true, describe: false, drop: false, parse: true, print: true, elements: true,
      write: { modes: ['create', 'overwrite', 'append', 'deleteAppend', 'merge'] } },
  };
  const elementsOf = async (model: string, call: CallName): Promise<Element[]> => {
    try {
      return (await o.planner.modelElements(model) as Element[]).filter((e) => e._type !== SECTION_INDEX);
    } catch (e) {
      throw new CompileError(e instanceof Error ? e.message : String(e), call, { cause: e });
    }
  };
  /** The model a query plans over (R3a): refused by name with a connection or runtime, or a runtime the tab does not
   *  own; else the model the tab served as that text, or the text with the tab's own runtime added. */
  const modelFor = async (ask: QueryAsk, call: CallName): Promise<string> => {
    const owned = (await elementsOf(ask.model, call)).filter((e) => OWNED_KINDS.has(e._type ?? ''));
    if (owned.length > 0) {
      const what = `a model declaring a ${owned[0]!._type} '${pathOf(owned[0]!)}'`;
      throw new Refused(call, what, ENGINE, 'a model DataCube sends holds classes, stores and mappings only: the engine owns its connections and runtimes (R3a)');
    }
    const runtime = runtimes.get(ask.runtime);
    if (runtime === undefined) throw new Refused(call, `the runtime '${ask.runtime}'`, ENGINE, 'it is no runtime this engine owns');
    if (ask.parameters !== undefined && Object.keys(ask.parameters).length > 0) {
      throw new Refused(call, "a query's parameters", ENGINE, 'this engine plans a query of no parameter (until step 6)');
    }
    if (ask.how !== undefined && Object.keys(ask.how).length > 0) {
      throw new Refused(call, `how a query reads its model (${JSON.stringify(ask.how)})`, ENGINE, "this engine reads a model's own tables (until step 4)");
    }
    return servedAs.get(`${ask.runtime}\n${ask.model}`) ?? `${ask.model}\n${runtime}`;
  };
  const planned = async <T>(call: CallName, work: () => Promise<T>, executionId?: string): Promise<T> => {
    try {
      return await work();
    } catch (e) {
      if (e instanceof Refused || e instanceof CompileError) throw e;
      throw new CompileError(e instanceof Error ? e.message : String(e), call, { ...(executionId ? { executionId } : {}), cause: e });
    }
  };
  const ran = async <T>(call: CallName, executionId: string, work: () => Promise<T>): Promise<T> => {
    try {
      return await work();
    } catch (e) {
      if (e instanceof Refused) throw e;
      throw new RunError(e instanceof Error ? e.message : String(e), call, { executionId, cause: e });
    }
  };
  const columnsOf = (cols: readonly { name: string; type: string; enumeration?: string }[]): ResultColumn[] =>
    cols.map((c) => ({ name: c.name, type: pureType(c.type), ...(c.enumeration !== undefined ? { enumeration: c.enumeration } : {}) }));
  const catalog = (schema: string, table: string): Promise<CatalogColumn[]> =>
    o.database.exclusive((held) => catalogColumnsOf({ run: (sql: PlannedSql) => held.table(sql) }, o.planner, schema, table));

  const session = (): EngineSession => {
    let closed = false;
    const open = (call: CallName, option?: Parameters<typeof refusalOf>[2]): void => {
      if (closed) throw new Refused(call, 'a call on a closed session', ENGINE, 'its session was closed');
      const refused = refusalOf(caps, call, option);
      if (refused !== undefined) throw refused;
    };
    return {
      async addSource(source, how) {
        open('addSource', { source: sourceKind(source), how });
        throw new Refused('addSource', 'addSource', ENGINE, 'its capabilities do not list it');
      },
      async describe() {
        open('describe');
        throw new Refused('describe', 'describe', ENGINE, 'its capabilities do not list it');
      },
      async drop() {
        open('drop');
        throw new Refused('drop', 'drop', ENGINE, 'its capabilities do not list it');
      },
      async typeOf(ask) {
        open('typeOf');
        const model = await modelFor(ask, 'typeOf');
        const p = o.planner.withModel(model, ask.runtime);
        try {
          return { columns: columnsOf(await planned('typeOf', () => p.relationType(ask.query))) };
        } finally {
          p.dispose();
        }
      },
      async query(ask, opts) {
        open('query');
        const model = await modelFor(ask, 'query');
        // THE MINT: one execution id for this plan run (Q7)
        const executionId = mint();
        const p = o.planner.withModel(model, ask.runtime);
        try {
          const plan = await planned('query', () => p.plan(ask.query, opts?.signal), executionId);
          const rows = await ran('query', executionId, () => o.database.execute(plan, 0, opts?.signal));
          // an Integer column's whole numbers as Int64: the one reader's word (types.ts), never a name compared here
          const batch = recordBatchOf(rows, plan.columns.filter((c) => isNumeric(c.type) && !isFractional(c.type)).map((c) => c.name));
          return {
            executionId,
            schema: columnsOf(plan.columns),
            rows: (async function* one() { yield batch; })(),
            explain: plan.sql,
            ...(rows.receipt !== undefined ? { receipt: rows.receipt } : {}),
          };
        } finally {
          p.dispose();
        }
      },
      async write(ask) {
        open('write', { mode: writeModeName(ask.mode) });
        const model = await modelFor(ask.box, 'write');
        const { mode, keys } = modeOf(ask.mode);
        const target = ask.target.table;
        if (o.owns(target)) {
          throw new Refused('write', `the table '${target}'`, ENGINE, "it is one of this engine's own tables (a file's, a copy's): a write names another");
        }
        // the box's tables, derived from its model's Databases (Q11 (a)), each as the catalog says it now
        const tables: ModelTable[] = [];
        for (const e of await elementsOf(model, 'write')) {
          if (e._type !== 'relational') continue;
          for (const schema of e.schemas ?? []) {
            for (const t of schema.tables ?? []) {
              const named = schema.name !== undefined && schema.name !== 'default' ? schema.name : undefined;
              tables.push({ table: t.name, pkg: e.package ?? 'local', convertible: true, ...(named ? { schema: named } : {}),
                columns: await catalog(named ?? 'main', t.name) });
            }
          }
        }
        const present = mode === 'create' || mode === 'overwrite' ? [] : await catalog('main', target);
        const request: OutputRequest = {
          databaseType: o.database.databaseType, tables,
          output: { name: target, mode, ...(mode === 'merge' ? { keys } : {}) },
          rows: toJson(ask.box.query as Lambda),
          ...(present.length > 0 ? { target: present } : {}),
        };
        // THE MINT: one execution id for this plan run (Q7)
        const executionId = mint();
        const plan = await planned('write', () => o.planner.outputPlan(request), executionId);
        const answer = await ran('write', executionId,
          () => o.database.exclusive((held) => runPlan(plan, { databaseType: o.database.databaseType, run: held.rows })));
        const read = JSON.parse(answer) as unknown;
        const n = Array.isArray(read) && read.length === 1 ? (read[0] as { value?: unknown }).value : undefined;
        if (typeof n !== 'number' || !Number.isSafeInteger(n)) throw new RunError(`the write answered no count: ${answer}`, 'write', { executionId });
        return { rowsWritten: n, executionId };
      },
      async parse(text) {
        open('parse');
        return planned('parse', () => o.planner.parse(text));
      },
      async print(query, style) {
        open('print');
        return planned('print', () => o.planner.print(query, style === 'COMPACT' ? 'STANDARD' : 'PRETTY'));
      },
      async elements(model) {
        open('elements');
        return elementsOf(model, 'elements');
      },
      async cancel() {
        // a statement DuckDB-WASM has begun runs to its end (the contract's cancel is best effort)
        open('cancel');
      },
      async close() {
        closed = true;
      },
    };
  };

  return {
    capabilities: async () => caps,
    open: async () => session(),
    async served(model, runtime) {
      const elements = await elementsOf(model, 'open');
      const owned = elements.filter((e) => OWNED_KINDS.has(e._type ?? '')).map(pathOf).sort();
      if (!owned.includes(runtime)) throw new Error(`the model declares no runtime '${runtime}' for this engine to own`);
      const own = sections(model, ['Connection', 'Runtime']);
      const mine = (await elementsOf(own, 'open')).map(pathOf).sort();
      if (JSON.stringify(mine) !== JSON.stringify(owned)) throw new Error(`the model's Connection and Runtime sections hold ${mine.join(', ')}, its elements ${owned.join(', ')}`);
      const handed = await o.planner.modelText(elements.filter((e) => !OWNED_KINDS.has(e._type ?? '')));
      runtimes.set(runtime, own);
      servedAs.set(`${runtime}\n${handed}`, model);
      return { model: handed, runtime };
    },
  };
}

function modeOf(mode: WriteMode): { readonly mode: string; readonly keys: readonly string[] } {
  if (typeof mode === 'string') return { mode, keys: [] };
  if (mode.merge.keys.length === 0) throw new Refused('write', 'a merge of no key', ENGINE, 'a merge is keyed by one column or more');
  return { mode: 'merge', keys: mode.merge.keys };
}
