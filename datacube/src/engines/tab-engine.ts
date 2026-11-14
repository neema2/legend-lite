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
import type { QueryEngine } from '../../../engine-client/src/engine.ts';
import {
  CompileError, ENGINE_CONTRACT_VERSION, Refused, refusalOf, RunError, sourceKind, writeModeName,
  type CallName, type Capabilities, type Engine, type EngineSession, type Handle, type QueryAsk, type ResultColumn,
  type ProjectAt, type ProjectCoordinates, type RevisionCoordinates, type SecretField, type WriteMode,
} from '../../../engine-client/src/engine-contract.ts';
import type { ProjectDatabases } from '../../../engine-client/src/project-data.ts';
import type { ConnectionSettings } from '../../../engine-client/src/connection-settings.ts';
import { heldDataHost } from '../data-host.ts';
import { runPlan } from '../../../engine-client/src/plan-runner.ts';
import { countQuery } from '../../../engine-client/src/snap.ts';
import { pureType } from '../../../engine-client/src/relation-type.ts';
import { recordBatchOf } from '../../../engine-client/src/engine-arrow.ts';
import { isFractional, isNumeric } from '../../../engine-client/src/types.ts';
import type { PlannedSql } from '../../../engine-client/src/planned-sql.ts';
import { lambda, toJson, type Lambda, type ValueSpecification } from '../../../pure-protocol/src/index.ts';
import type { CatalogColumn, ModelTable, OutputRequest } from '../infer.ts';
import type { LitePlanner } from '../lite-planner.ts';
import type { Planner } from '../cube.ts';
import type { ModelOptions } from '../planner.ts';
import { catalogColumnsOf } from '../upload.ts';
import { withLibrary } from '../pure-library.ts';

const ENGINE = 'the tab';

/** The kinds of element a model sent to an engine may not hold (R3a (a)), as the parser names them. */
const OWNED_KINDS = new Set(['connection', 'runtime']);
const SECTION_INDEX = 'sectionIndex';
/** What a library may declare (Q17, Q27: the orchestrator, 2026-11-13): functions and enumerations -- and profiles, their
 *  stereotypes and tags (Q27 amended by Q33, 2026-11-14: #81's library declares Profile timeseries::Words) -- nothing
 *  else: no class, association, store, mapping, connection, runtime or data. */
const LIBRARY_KINDS = new Set(['function', 'Enumeration', 'profile']);

/** What the tab's engine runs on: its database, held for a write alone, the plane's planner and its writer. */
export interface TabEngineOptions {
  readonly database: QueryEngine & {
    readonly databaseType: string;
    exclusive<T>(work: (held: HeldConnection) => Promise<T>): Promise<T>;
  };
  /** The plane the page chose (Q20): its planner -- legend-lite's module here, legend-lite on a server, or legend-engine
   *  (UpstreamPlanner, its behaviour unchanged behind this engine: a server planning rows the tab runs is an arrangement
   *  inside it, the contract's §4) -- plans and types a query over a model, parses, prints and reads elements. */
  readonly plane: {
    readonly label: string;
    readonly planner: Planner;
    another(model: string, runtime: string, how?: ModelOptions): Planner;
    elements(text: string): Promise<unknown[]>;
  };
  /** legend-lite's module, the engine's writer on every plane: a table's model, the catalog's question, a write's and a
   *  snap's plan, and the engine's own reading of a model it is sent (R3a, a library's check: Q17). */
  readonly writer: LitePlanner;
  /** Where an upload's bytes are registered for the database to read by name (DuckDB-WASM's registerFileBuffer). */
  readonly files: { registerFileBuffer(name: string, bytes: Uint8Array): Promise<void> };
  /** Where the database keeps a remote mount's settings of the connection (a bearer URL's session variable). */
  readonly settings?: ConnectionSettings;
  /** The projects the tab resolves by their coordinates: a version's model text (Q19: the page's configured projects,
   *  then its Depot) and a workspace revision's (Q23: its model home's SDLC), undefined for none; and a version's rows'
   *  databases (C5, engine-client's project-data.ts). */
  readonly projects?: {
    model(coordinates: ProjectCoordinates): Promise<string | undefined>;
    revision?(coordinates: RevisionCoordinates): Promise<string | undefined>;
    databases(): Promise<ProjectDatabases>;
  };
}

/** The tab's engine, and what it serves DataCube a model it wrote as (R3a (c)). */
export interface TabEngine extends Engine {
  served(model: string, runtime: string): Promise<{ readonly model: string; readonly runtime: string }>;
}

interface Element { readonly _type?: string; readonly package?: string; readonly name?: string;
  readonly schemas?: readonly { readonly name?: string; readonly tables?: readonly { readonly name: string;
    readonly columns?: readonly { readonly name: string; readonly type?: { readonly _type?: string } }[] }[] }[] }

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

/** How many readings of a model (its elements, a project's text) and planners over one the tab's engine keeps. */
const READINGS_KEPT = 16;
const PLANNERS_KEPT = 8;

/** `key`'s value in `kept`, made once and kept while recent (the least recent let go past `limit`); a value whose
 *  making failed is not kept, so the next asks again. */
function recent<T>(kept: Map<string, Promise<T>>, key: string, limit: number, make: () => Promise<T>): Promise<T> {
  const was = kept.get(key);
  if (was !== undefined) {
    kept.delete(key);
    kept.set(key, was);
    return was;
  }
  const made = make();
  kept.set(key, made);
  made.catch(() => { if (kept.get(key) === made) kept.delete(key); });
  while (kept.size > limit) kept.delete(kept.keys().next().value!);
  return made;
}

/** Whether a table name is one DataCube may ask (Q28): a plain identifier, never a literal or legend-lite's own. */
function plainName(name: string): boolean {
  return /^[A-Za-z_][A-Za-z0-9_]*$/.test(name) && !['true', 'false'].includes(name.toLowerCase()) && !name.startsWith('__legend_lite_');
}

/** An upload's table, named after its file (Q28): its stem (the name less its last extension), each other character
 *  `_`, led by `f_` where it does not start with a letter -- as Python's engine names it. */
function fileTable(fileName: string): string {
  const base = fileName.split(/[\\/]/).pop() ?? fileName;
  const dot = base.lastIndexOf('.');
  const stem = (dot > 0 ? base.slice(0, dot) : base).replace(/[^A-Za-z0-9_]/g, '_') || 'file';
  return /^[A-Za-z]/.test(stem) ? stem : `f_${stem}`;
}

/** Whether a project's coordinates name a version (Q19) rather than a workspace's revision (Q23). */
function isVersion(p: ProjectAt): p is ProjectCoordinates {
  return 'versionId' in p;
}

/** A random id, a UUID's canonical text: an execution's (the shape its trace comment carries) and a handle's. The tab's
 *  engine is the door of its runs: its runners are handed the id, and make none (//tools/guards:planned_sql_test). */
function mint(): string {
  return globalThis.crypto.randomUUID();
}

/** The local plane (Q20): legend-lite's module plans in the tab, the same module that writes. */
export function localPlane(planner: LitePlanner): TabEngineOptions['plane'] {
  return {
    label: 'local', planner,
    another: (model, runtime, how) => planner.withModel(model, runtime, how),
    elements: (text) => planner.modelElements(text),
  };
}

export function tabEngine(o: TabEngineOptions): TabEngine {
  const runtimes = new Map<string, string>();
  const servedAs = new Map<string, string>();
  const caps: Capabilities = {
    contract: ENGINE_CONTRACT_VERSION,
    // the plane named: its planner answers (Q20)
    engine: { name: `${ENGINE} (${o.plane.label})`, databaseType: o.database.databaseType },
    calls: { typeOf: true, query: true, cancel: true, describe: true, drop: true, parse: true, print: true, elements: true,
      bindSecret: true, modelOf: true,
      write: { modes: ['create', 'overwrite', 'append', 'deleteAppend', 'merge'] },
      // step 3 (2026-11-13): an upload, a URL, a remote file by its secret, a query's rows; the tab holds no file of its
      // own beside an upload, so `held` is not offered
      addSource: { sources: ['upload', 'url', 'remote', 'query'], how: ['live', 'snap'] } },
  };
  // the database's own data host (engine-internal: Q14's tables are those behind the handles issued here)
  const data = heldDataHost({ databaseType: o.database.databaseType, run: (sql, epoch) => o.database.run(sql, epoch),
    exclusive: (work) => o.database.exclusive(work), settings: o.settings }, o.writer);
  const issued = new Set<string>();
  /** Names being taken by a source still being read: no other is given one of them meanwhile. */
  const naming = new Set<string>();
  /** A source's table name (Q28): `stem` where it is free, else the next free `stem_2`, ... -- free meaning no table or
   *  view of the database, and none this engine issued or is reading into. */
  const named = async (stem: string, mine: string[]): Promise<string> => {
    const taken = async (name: string): Promise<boolean> => naming.has(name.toLowerCase()) || issued.has(name.toLowerCase())
      || (await data.catalog('main', name)).length > 0;
    let name = stem;
    for (let n = 2; await taken(name); n += 1) name = `${stem}_${n}`;
    naming.add(name.toLowerCase());
    mine.push(name.toLowerCase());
    return name;
  };
  /** A table of the database modelled by the tab's planner, in a package of its own (so its runtime is its own), each
   *  conversion the platform's, the table rewritten in place where it may be. */
  const modelled = async (table: string, convertible: boolean): Promise<{ model: string; runtime: string; source: ValueSpecification }> => {
    const columns = await data.catalog('main', table);
    const written = await o.writer.tableModel(columns, { table, pkg: table, convertible, databaseType: o.database.databaseType });
    if (convertible && written.conversions.length > 0) {
      await data.operations([{ op: 'relation', kind: 'table', target: { name: table }, replace: true, convert: columns, from: { relation: { name: table } } }]);
    }
    return { model: written.model, runtime: written.runtime, source: written.source };
  };
  // A MODEL READ ONCE (merging main's #81, 2026-11-14: a project's model is 4 MB, and its tile's and grid's every query
  // read it again, the trading page's first sheet 55-66 s): its elements, by its text; a project's text, by its
  // coordinates; a planner, by its model, runtime and how -- each kept while recent, a planner let go once it is not
  const readings = new Map<string, Promise<Element[]>>();
  const elementsOf = async (model: string, call: CallName): Promise<Element[]> => {
    try {
      // the engine's own reading, in lite's dialect (Q27): its checks read what the planner compiles
      return await recent(readings, model, READINGS_KEPT, async () =>
        (await o.writer.liteElements(model) as Element[]).filter((e) => e._type !== SECTION_INDEX));
    } catch (e) {
      throw new CompileError(e instanceof Error ? e.message : String(e), call, { cause: e });
    }
  };
  /** The planners kept, least recent first, each with how many calls are using it now. */
  const planners = new Map<string, { readonly planner: Planner; busy: number }>();
  /** Lets go of the least recent planners past PLANNERS_KEPT -- only those no call is using: a planner is never disposed
   *  under a running call; with every kept one busy, the cap is passed until one is free. */
  const evictIdle = (): void => {
    for (const [key, kept] of planners) {
      if (planners.size <= PLANNERS_KEPT) return;
      if (kept.busy > 0) continue;
      planners.delete(key);
      kept.planner.dispose?.();
    }
  };
  /** `use` given the plane's planner over `model` (Q20), the one kept for it while recent (its model compiled once),
   *  pinned while `use` runs. */
  const withPlanner = async <T>(model: string, runtime: string, how: ModelOptions, use: (p: Planner) => Promise<T>): Promise<T> => {
    const key = `${runtime}\u0000${JSON.stringify(how)}\u0000${model}`;
    let kept = planners.get(key);
    if (kept !== undefined) planners.delete(key);
    else kept = { planner: o.plane.another(model, runtime, how), busy: 0 };
    planners.set(key, kept);
    kept.busy++;
    evictIdle();
    try {
      return await use(kept.planner);
    } finally {
      kept.busy--;
      evictIdle();
    }
  };
  /** The model a query plans over (R3a): refused by name with a connection or runtime, or a runtime the tab does not
   *  own; else the model the tab served as that text, or the text with the tab's own runtime added, DataCube's library
   *  (Q17) after it when sent -- and how it is read: its mapping, and what its parsed elements say (Q21, Q22): the
   *  columns its stores declare BIT, its enumerations. */
  const libraries = new Set<string>();
  const resolved = async (ask: QueryAsk, call: CallName): Promise<{ readonly model: string; readonly how: ModelOptions }> => {
    const added = await elementsOf(ask.model, call);
    const owned = added.filter((e) => OWNED_KINDS.has(e._type ?? ''));
    if (owned.length > 0) {
      const what = `a model declaring a ${owned[0]!._type} '${pathOf(owned[0]!)}'`;
      throw new Refused(call, what, ENGINE, 'a model DataCube sends holds classes, stores and mappings only: the engine owns its connections and runtimes (R3a)');
    }
    if (ask.parameters !== undefined && Object.keys(ask.parameters).length > 0) {
      throw new Refused(call, "a query's parameters", ENGINE, 'this engine plans a query of no parameter (until step 6)');
    }
    const how = ask.how ?? {};
    if (Object.keys(how).some((k) => k !== 'mapping' && k !== 'project')) throw new Refused(call, `how a query reads its model (${JSON.stringify(how)})`, ENGINE, "this engine reads a model's own tables, through a mapping, over a project by its coordinates");
    const model = how.project !== undefined ? await projectModel(ask, how.project, added, call)
      : servedAs.get(`${ask.runtime}\n${ask.model}`) ?? `${ask.model}\n${ownRuntime(ask, call)}`;
    if (ask.library !== undefined) await libraryChecked(ask.library, call);
    // DataCube's library (Q17) held beside the model, as #81's planners hold it (overLibrary: its text the two together)
    const whole = ask.library === undefined ? model : withLibrary(model, ask.library);
    const read = await elementsOf(model, call);
    const bits = read.flatMap((e) => (e._type === 'relational' ? e.schemas ?? [] : []))
      .flatMap((sc) => sc.tables ?? []).flatMap((t) => t.columns ?? []).filter((c) => c.type?._type === 'Bit').map((c) => c.name);
    const enumerations = read.filter((e) => e._type === 'Enumeration').map(pathOf);
    return { model: whole, how: {
      ...(how.mapping !== undefined ? { mapping: how.mapping } : {}),
      ...(bits.length > 0 ? { bitColumns: [...new Set(bits)] } : {}),
      ...(enumerations.length > 0 ? { enumerations } : {}),
      ...(ask.library !== undefined ? { overLibrary: { model, library: ask.library } } : {}),
    } };
  };
  /** `use` given the plane's planner over a query's model (Q20) -- what types and plans it -- and that model. */
  const withPlannerFor = async <T>(ask: QueryAsk, call: CallName, use: (p: Planner, model: string) => Promise<T>): Promise<T> => {
    const r = await resolved(ask, call);
    return withPlanner(r.model, ask.runtime, r.how, (p) => use(p, r.model));
  };
  /** A runtime the tab owns, its Connection and Runtime text; refused by name where it owns none of that name. */
  const ownRuntime = (ask: QueryAsk, call: CallName): string => {
    const runtime = runtimes.get(ask.runtime);
    if (runtime === undefined) throw new Refused(call, `the runtime '${ask.runtime}'`, ENGINE, 'it is no runtime this engine owns');
    return runtime;
  };
  /** A project's text, resolved by its coordinates: a version's (Q19) or a workspace revision's (Q23); refused by name
   *  where the tab resolves none. */
  const projectTexts = new Map<string, Promise<string>>();
  const projectText = (p: ProjectAt, call: CallName): Promise<string> =>
    recent(projectTexts, JSON.stringify(p), READINGS_KEPT, () => projectTextOf(p, call));
  const projectTextOf = async (p: ProjectAt, call: CallName): Promise<string> => {
    const version = isVersion(p);
    const coords = version ? `${p.groupId}:${p.artifactId}:${p.versionId}` : `${p.project}/${p.workspace}@${p.revision}`;
    const projects = o.projects;
    if (projects === undefined || (!version && projects.revision === undefined)) {
      throw new Refused(call, `the project ${coords}`, ENGINE, `this engine resolves no ${version ? 'version' : 'workspace revision'}`);
    }
    let text: string | undefined;
    try {
      text = isVersion(p) ? await projects.model(p) : await projects.revision!(p);
    } catch (e) {
      throw new Refused(call, `the project ${coords}`, ENGINE, `its model home did not resolve it: ${e instanceof Error ? e.message : String(e)}`);
    }
    if (text === undefined) throw new Refused(call, `the project ${coords}`, ENGINE, `its model home holds no such ${version ? 'version' : 'project, workspace or revision'}`);
    return text;
  };
  /** A project's model with what DataCube adds after it (Q19): the query's runtime the project's (owned by its path),
   *  its mapping the project's or one the query adds. */
  const projectModel = async (ask: QueryAsk, p: ProjectAt, added: readonly Element[], call: CallName): Promise<string> => {
    const text = await projectText(p, call);
    const elements = await elementsOf(text, call);
    if (!elements.some((e) => e._type === 'runtime' && pathOf(e) === ask.runtime)) {
      throw new Refused(call, `the runtime '${ask.runtime}'`, ENGINE, 'it is no runtime of the project');
    }
    const mapping = ask.how?.mapping;
    if (mapping !== undefined && ![...elements, ...added].some((e) => e._type === 'mapping' && pathOf(e) === mapping)) {
      throw new Refused(call, `the mapping '${mapping}'`, ENGINE, 'it is no mapping of the project, nor one the query adds');
    }
    return ask.model.trim().length > 0 ? `${text}\n${ask.model}` : text;
  };
  /** Where a query's rows are: a version's own database (C5), else the tab's (a workspace revision's class reads the
   *  tables the page that made it opened: Q23). */
  const rowsOn = async (ask: QueryAsk, model: string) => {
    if (ask.how?.project === undefined || !isVersion(ask.how.project) || o.projects === undefined) return o.database;
    return (await (await o.projects.databases()).engineFor(model, o.database)).engine;
  };

  /** The whole model a QueryAsk plans over (R3a, its library in it): what a write's and a snap's plan is asked over. */
  const modelOfAsk = async (ask: QueryAsk, call: CallName): Promise<{ readonly model: string; readonly query: Lambda }> => {
    // the writer's: a write's and a snap's plan are its own on every plane
    const r = await resolved(ask, call);
    const p = o.writer.withModel(r.model, ask.runtime, r.how);
    try {
      // through a mapping, the query as it is planned: `->from(mapping, runtime)` outermost
      return { model: p.source().model, query: p.sent(ask.query) };
    } finally {
      p.dispose();
    }
  };
  /** DataCube's Pure library (Q17), on its PARSED elements: functions and enumerations alone (Q27), read in lite's dialect, held by its digest once checked. */
  const libraryChecked = async (library: string, call: CallName): Promise<void> => {
    const digest = await sha256(new TextEncoder().encode(library));
    if (libraries.has(digest)) return;
    const other = (await elementsOf(library, call)).find((e) => !LIBRARY_KINDS.has(e._type ?? ''));
    if (other !== undefined) throw new Refused(call, `a library declaring a ${other._type} '${pathOf(other)}'`, ENGINE, 'a library holds functions, enumerations and profiles alone (Q17, Q33)');
    libraries.add(digest);
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
    o.database.exclusive((held) => catalogColumnsOf({ run: (sql: PlannedSql) => held.table(sql) }, o.writer, schema, table));

  interface Held { readonly table: string; readonly view: boolean; readonly model: string; readonly handed: string;
    readonly runtime: string; readonly relation: ValueSpecification }
  const forget = async (h: Held): Promise<void> => {
    await data.operations([{ op: 'drop', kind: h.view ? 'view' : 'table', target: { name: h.table } }]);
    if (h.view) data.forget(h.table);
    issued.delete(h.table.toLowerCase());
    runtimes.delete(h.runtime);
    servedAs.delete(`${h.runtime}\n${h.handed}`);
  };

  const session = (): EngineSession => {
    let closed = false;
    const handles = new Map<string, Held>();
    // the models modelOf wrote for this session, let go when it closes
    const together: { readonly model: string; readonly runtime: string }[] = [];
    const secrets = new Map<string, Readonly<Partial<Record<SecretField, string | boolean>>>>();
    /** `text` with every secret this session holds taken out (Q13: nothing echoes a secret, the database's words
     *  included). */
    const redact = (text: string): string => [...secrets.values()].flatMap((f) => Object.values(f))
      .filter((v): v is string => typeof v === 'string' && v.length > 0).sort((a, b) => b.length - a.length)
      .reduce((t, v) => t.split(v).join('<redacted>'), text);
    const sourceOf = (handle: Handle, call: CallName): Held => {
      const h = handles.get(handle.handle);
      if (h === undefined) throw new Refused(call, `the handle ${JSON.stringify(handle.handle)}`, ENGINE, 'it is no handle this session issued, or it was dropped');
      return h;
    };
    const open = (call: CallName, option?: Parameters<typeof refusalOf>[2]): void => {
      if (closed) throw new Refused(call, 'a call on a closed session', ENGINE, 'its session was closed');
      const refused = refusalOf(caps, call, option);
      if (refused !== undefined) throw refused;
    };
    return {
      async addSource(source, how, opts) {
        open('addSource', { source: sourceKind(source), how });
        // THE MINT: one execution id for this source's plan run (its load, mount or copy: Q7)
        // its table's name (Q28): the one asked -- a plain identifier, never one of legend-lite's own -- or its source's own
        const asked = opts?.name;
        if (asked !== undefined && !plainName(asked)) {
          throw new Refused('addSource', `the table name ${JSON.stringify(asked)}`, ENGINE, "a table is named by a plain identifier: letters, digits and '_', led by a letter or '_'");
        }
        const executionId = mint();
        // the names this call reserves, let go once it is done (the one it read into is issued by then)
        const mine: string[] = [];
        let file: { digest: string; size: number } | undefined;
        const run = async (): Promise<{ table: string; view: boolean; model: string; runtime: string; source: ValueSpecification; rowCount?: number }> => {
          if ('upload' in source) {
            if (how !== 'snap') throw new Refused('addSource', `a file added '${how}'`, ENGINE, "a file is read into a table of this engine's own (snap)");
            const { name, bytes } = source.upload;
            const format = /\.(csv|parquet|json)$/i.exec(name)?.[1]?.toLowerCase() as 'csv' | 'parquet' | 'json' | undefined;
            if (format === undefined) throw new Refused('addSource', `the file '${name}'`, ENGINE, 'a file is read as csv, json or parquet');
            file = { digest: await sha256(bytes), size: bytes.length };
            const table = await named(asked ?? fileTable(name), mine);
            const registered = `${table}.${format}`;
            await o.files.registerFileBuffer(registered, bytes);
            await data.load({ fileLoad: { databaseType: o.database.databaseType, file: { name, ...file, format }, into: { table } } },
              { files: [{ name, ...file, handle: registered }] });
            return { table, view: false, ...(await modelled(table, true)) };
          }
          if ('url' in source || 'remote' in source) {
            if (how !== 'live') throw new Refused('addSource', `a remote file added '${how}'`, ENGINE, "a remote file is mounted live; its rows are snapped as a query's ({query})");
            const view = await named(asked ?? 'remote', mine);
            const ref = 'url' in source ? source.url : source.remote.ref;
            const format = /\.(csv|parquet)(\?|$)/i.exec(ref)?.[1]?.toLowerCase() as 'csv' | 'parquet' | undefined;
            if (format === undefined) throw new Refused('addSource', `the remote file '${ref.split('?')[0]}'`, ENGINE, 'a remote file is read as csv or parquet');
            if ('url' in source) {
              await data.load({ remoteMount: { databaseType: o.database.databaseType, view, format, location: { url: ref } } });
            } else {
              const secret = secrets.get(source.remote.secret);
              if (secret === undefined) throw new Refused('addSource', `the secret '${source.remote.secret}'`, ENGINE, 'no secret of that name is bound to this session');
              const fields = Object.keys(secret) as SecretField[];
              if (source.remote.kind === 'bearer' && (fields.length !== 1 || fields[0] !== 'URL')) {
                throw new Refused('addSource', `the secret '${source.remote.secret}' for a bearer URL`, ENGINE, 'a bearer URL is held as its field URL alone');
              }
              const request = source.remote.kind === 'bearer'
                ? { databaseType: o.database.databaseType, view, format, location: { bearer: source.remote.secret, shown: ref } }
                : { databaseType: o.database.databaseType, view, format, location: { url: ref }, credentials: { name: source.remote.secret, fields } };
              await data.load({ remoteMount: request }, { secrets: { [source.remote.secret]: secret } });
            }
            return { table: view, view: true, ...(await modelled(view, false)) };
          }
          if ('query' in source) {
            if (how !== 'snap') throw new Refused('addSource', `a query's rows added '${how}'`, ENGINE, "a query's rows are added as a snap alone");
            const { model, query: pulled } = await modelOfAsk(source.query, 'addSource');
            // its rows counted where they are, as every snap's are (snap.ts countQuery: the pull's size(), planned) -- the
            // count the answer gives (Q26)
            const counter = o.writer.withModel(model, source.query.runtime);
            let rowCount: number;
            try {
              const counting = await planned('addSource', () => counter.plan(countQuery(pulled)), executionId);
              const counted = (await o.database.execute(counting, 0)).columns[0]?.values[0];
              if (typeof counted !== 'number' && typeof counted !== 'bigint') throw new RunError(`the pull's count answered ${String(counted)}`, 'addSource', { executionId });
              rowCount = Number(counted);
            } finally {
              counter.dispose();
            }
            const table = await named(asked ?? 'snap', mine);
            const plan = await planned('addSource', () => o.writer.snapCopy({ databaseType: o.database.databaseType, model,
              runtime: source.query.runtime, lambda: toJson(pulled), table }), executionId);
            await o.database.exclusive((held) => runPlan(plan, { databaseType: o.database.databaseType, run: held.rows }, { execution: executionId }));
            return { table, view: false, rowCount, ...(await modelled(table, true)) };
          }
          throw new Refused('addSource', `a source of kind '${sourceKind(source)}'`, ENGINE, 'its capabilities do not list it');
        };
        let made: Awaited<ReturnType<typeof run>>;
        try {
          made = await run();
          issued.add(made.table.toLowerCase());
        } catch (e) {
          if (e instanceof Refused || e instanceof CompileError) throw e;
          throw new RunError(redact(e instanceof Error ? e.message : String(e)), 'addSource', { executionId });
        } finally {
          for (const n of mine) naming.delete(n);
        }
        const { model: handed, runtime } = await served(made.model, made.runtime);
        const handle = `h-${mint()}`;
        handles.set(handle, { table: made.table, view: made.view, model: made.model, handed, runtime, relation: made.source });
        return { handle: { handle }, executionId, name: made.table, model: handed, runtime, relation: made.source, ...(file ?? {}),
          // a query's rows copied: their count (Q26); no other kind's -- never a guessed 0
          ...(made.rowCount !== undefined ? { rowCount: made.rowCount } : {}) };
      },
      async modelOf(given) {
        open('modelOf');
        if (given.length === 0) throw new Refused('modelOf', 'a modelOf of no handle', ENGINE, 'modelOf is asked the handles of the sources read together');
        const held = given.map((h) => sourceOf(h, 'modelOf'));
        if (new Set(held).size !== held.length) throw new Refused('modelOf', 'a handle given twice', ENGINE, 'each source is read once');
        const tables: ModelTable[] = [];
        for (const h of held) tables.push({ table: h.table, pkg: h.table, convertible: !h.view, columns: await data.catalog('main', h.table) });
        // a connection of its own name: so the runtime is this model's alone
        const written = await planned('modelOf', () => o.writer.tablesModel(tables, o.database.databaseType, `together_${mint().slice(0, 8)}`));
        const handed = await served(written.model, written.runtime);
        together.push(handed);
        return { model: handed.model, runtime: handed.runtime, relations: written.tables.map((t) => t.source) };
      },
      async describe(handle) {
        open('describe');
        const h = sourceOf(handle, 'describe');
        return withPlanner(h.model, h.runtime, {}, async (p) =>
          ({ columns: columnsOf(await planned('describe', () => p.relationType(lambda([], h.relation)))) }));
      },
      async drop(handle) {
        open('drop');
        const h = sourceOf(handle, 'drop');
        handles.delete(handle.handle);
        await forget(h);
      },
      async bindSecret(name, fields) {
        open('bindSecret');
        if (!/^\w+$/.test(name)) throw new Refused('bindSecret', 'a secret with no plain name', ENGINE, "a secret is named by letters, digits and '_'");
        const kept: Partial<Record<SecretField, string | boolean>> = {};
        for (const [k, v] of Object.entries(fields)) {
          // the field's name alone in a refusal, never its value
          if (!SECRET_FIELDS.has(k) || (typeof v !== 'string' && typeof v !== 'boolean')) {
            throw new Refused('bindSecret', `the field '${k}' of the secret '${name}'`, ENGINE, "a secret's fields are KEY_ID, SECRET, SESSION_TOKEN, REGION, ENDPOINT, URL_STYLE, USE_SSL and URL");
          }
          kept[k as SecretField] = v;
        }
        if (Object.keys(kept).length === 0) throw new Refused('bindSecret', `the secret '${name}' with no field`, ENGINE, 'a secret is held as its fields');
        secrets.set(name, kept);
      },
      async typeOf(ask) {
        open('typeOf');
        return withPlannerFor(ask, 'typeOf', async (p) =>
          ({ columns: columnsOf(await planned('typeOf', () => p.relationType(ask.query))) }));
      },
      async query(ask, opts) {
        open('query');
        // the planner is pinned while it plans, and kept for the model's next query (withPlanner)
        const { plan, model, executionId } = await withPlannerFor(ask, 'query', async (p, m) => {
          // THE MINT: one execution id for this plan run (Q7)
          const id = mint();
          return { plan: await planned('query', () => p.plan(ask.query, opts?.signal), id), model: m, executionId: id };
        });
        const on = await rowsOn(ask, model);
        const rows = await ran('query', executionId, () => on.execute(plan, 0, opts?.signal));
        // an Integer column's whole numbers as Int64: the one reader's word (types.ts), never a name compared here
        const batch = recordBatchOf(rows, plan.columns.filter((c) => isNumeric(c.type) && !isFractional(c.type)).map((c) => c.name));
        return {
          executionId,
          schema: columnsOf(plan.columns),
          rows: (async function* one() { yield batch; })(),
          explain: plan.sql,
          ...(rows.receipt !== undefined ? { receipt: rows.receipt } : {}),
        };
      },
      async write(ask) {
        open('write', { mode: writeModeName(ask.mode) });
        const { model, query: rowsQuery } = await modelOfAsk(ask.box, 'write');
        const { mode, keys } = modeOf(ask.mode);
        const target = ask.target.table;
        // its own tables are exactly those it issued handles for (Q14, Q30)
        if (issued.has(target.toLowerCase())) {
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
          rows: toJson(rowsQuery),
          ...(present.length > 0 ? { target: present } : {}),
        };
        // THE MINT: one execution id for this plan run (Q7)
        const executionId = mint();
        const plan = await planned('write', () => o.writer.outputPlan(request), executionId);
        const answer = await ran('write', executionId,
          () => o.database.exclusive((held) => runPlan(plan, { databaseType: o.database.databaseType, run: held.rows }, { execution: executionId })));
        const read = JSON.parse(answer) as unknown;
        const n = Array.isArray(read) && read.length === 1 ? (read[0] as { value?: unknown }).value : undefined;
        if (typeof n !== 'number' || !Number.isSafeInteger(n)) throw new RunError(`the write answered no count: ${answer}`, 'write', { executionId });
        return { rowsWritten: n, executionId };
      },
      async parse(text) {
        open('parse');
        return planned('parse', () => o.plane.planner.parse(text));
      },
      async print(query, style) {
        open('print');
        return planned('print', () => o.plane.planner.print(query, style === 'COMPACT' ? 'STANDARD' : 'PRETTY'));
      },
      async elements(model) {
        open('elements');
        // the plane's reading (Q20); a project's, its connections and runtimes left out (the engine's: Q19)
        const read = async (text: string): Promise<Element[]> =>
          (await planned('elements', () => o.plane.elements(text)) as Element[]).filter((e) => e._type !== SECTION_INDEX);
        if (typeof model === 'string') return read(model);
        return (await read(await projectText(model.project, 'elements'))).filter((e) => !OWNED_KINDS.has(e._type ?? ''));
      },
      async cancel() {
        // a statement DuckDB-WASM has begun runs to its end (the contract's cancel is best effort)
        open('cancel');
      },
      async close() {
        if (closed) return;
        closed = true;
        secrets.clear();
        for (const t of together) {
          runtimes.delete(t.runtime);
          servedAs.delete(`${t.runtime}\n${t.model}`);
        }
        for (const h of handles.values()) await forget(h);
        handles.clear();
      },
    };
  };

  return {
    capabilities: async () => caps,
    open: async () => session(),
    served: (model, runtime) => served(model, runtime),
  };

  async function served(model: string, runtime: string): Promise<{ readonly model: string; readonly runtime: string }> {
    // a model this engine handed out already (a source's, a snap's copy's): served as it is
    if (servedAs.has(`${runtime}\n${model}`)) return { model, runtime };
    const elements = await elementsOf(model, 'open');
    const owned = elements.filter((e) => OWNED_KINDS.has(e._type ?? '')).map(pathOf).sort();
    if (!owned.includes(runtime)) throw new Error(`the model declares no runtime '${runtime}' for this engine to own`);
    const own = sections(model, ['Connection', 'Runtime']);
    const mine = (await elementsOf(own, 'open')).map(pathOf).sort();
    if (JSON.stringify(mine) !== JSON.stringify(owned)) throw new Error(`the model's Connection and Runtime sections hold ${mine.join(', ')}, its elements ${owned.join(', ')}`);
    const handed = await o.writer.modelText(elements.filter((e) => !OWNED_KINDS.has(e._type ?? '')));
    runtimes.set(runtime, own);
    servedAs.set(`${runtime}\n${handed}`, model);
    return { model: handed, runtime };
  }
}

const SECRET_FIELDS = new Set(['KEY_ID', 'SECRET', 'SESSION_TOKEN', 'REGION', 'ENDPOINT', 'URL_STYLE', 'USE_SSL', 'URL']);

/** Bytes' SHA-256, lower-case hex (WebCrypto's: the tab's and Node's alike). */
async function sha256(bytes: Uint8Array): Promise<string> {
  const digest = new Uint8Array(await globalThis.crypto.subtle.digest('SHA-256', new Uint8Array(bytes)));
  return [...digest].map((b) => b.toString(16).padStart(2, '0')).join('');
}

function modeOf(mode: WriteMode): { readonly mode: string; readonly keys: readonly string[] } {
  if (typeof mode === 'string') return { mode, keys: [] };
  if (mode.merge.keys.length === 0) throw new Refused('write', 'a merge of no key', ENGINE, 'a merge is keyed by one column or more');
  return { mode: 'merge', keys: mode.merge.keys };
}
