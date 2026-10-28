// WHAT A RUNTIME RUNS ON (docs/ONE_CONNECTION_DESK_2026_10_28.md, step 5): read from the model's elements, which DataCube
// reads before it asks for a grid's rows (a saved query's data space, a class's tree) -- knowledge, never a question of
// its own. A runtime binds each store to a connection, by name (or written in place); a connection says its database
// type, its datasource and its sign-in, lite's own words among them as lite's own JSON (D1: a Deephaven server's host,
// port and TLS, its key by vault reference). What a grid's rows run on is decided from this alone (`engineOf`).

/** One store a runtime binds, and the connection it binds it to. */
export interface StoreConnection {
  readonly store: string;
  /** The connection's path, or `''` for one written in place in the runtime. */
  readonly connection: string;
  /** Its database type (a relational connection's), or the connection's own kind (a model's JSON connection: its
   *  `_type`). */
  readonly databaseType: string;
  /** A Deephaven server's: where it is, and its key's vault reference -- never the key. */
  readonly deephaven?: DeephavenServerOf;
}

export interface DeephavenServerOf {
  readonly host: string;
  readonly port: number;
  readonly useTls: boolean;
  readonly keyVaultReference: string;
}

type Json = Readonly<Record<string, unknown>>;

const isObject = (v: unknown): v is Json => typeof v === 'object' && v !== null && !Array.isArray(v);
const list = (v: unknown): readonly unknown[] => (Array.isArray(v) ? v : []);
const pathOf = (e: Json): string => (typeof e['package'] === 'string' && e['package'] !== ''
  ? `${e['package']}::${String(e['name'])}` : String(e['name']));

/** Every store `runtime` binds and its connection; a runtime or a connection the elements lack is refused by name. */
export function runtimeConnections(elements: readonly unknown[], runtime: string): readonly StoreConnection[] {
  const found = elements.find((e): e is Json => isObject(e) && e['_type'] === 'runtime' && pathOf(e) === runtime);
  if (found === undefined) throw new Error(`the model declares no runtime ${runtime}`);
  const value = found['runtimeValue'];
  if (!isObject(value)) throw new Error(`the runtime ${runtime} has no value`);
  const out: StoreConnection[] = [];
  // a store's connections (`connections: [ store: [ id: connection ] ]`)
  for (const sc of list(value['connections'])) {
    if (!isObject(sc) || !isObject(sc['store'])) continue;
    const store = String(sc['store']['path']);
    for (const ic of list(sc['storeConnections'])) {
      if (isObject(ic)) out.push(connectionOf(elements, runtime, store, ic['connection']));
    }
  }
  // a connection's stores (`connectionStores: [ connection: [ store, ... ] ]`)
  for (const cs of list(value['connectionStores'])) {
    if (!isObject(cs)) continue;
    for (const sp of list(cs['storePointers'])) {
      if (isObject(sp)) out.push(connectionOf(elements, runtime, String(sp['path']), cs['connectionPointer']));
    }
  }
  return out;
}

/** The connection `value` binds `store` to: a pointer followed to the model's connection, or one written in place. */
function connectionOf(elements: readonly unknown[], runtime: string, store: string, value: unknown): StoreConnection {
  if (!isObject(value)) throw new Error(`the runtime ${runtime} binds ${store} to no connection`);
  let path = '';
  let connection: Json = value;
  if (value['_type'] === 'connectionPointer') {
    path = String(value['connection']);
    const element = elements.find((e): e is Json => isObject(e) && e['_type'] === 'connection' && pathOf(e) === path);
    if (element === undefined || !isObject(element['connectionValue'])) {
      throw new Error(`the runtime ${runtime} binds ${store} to ${path}, which the model does not declare`);
    }
    connection = element['connectionValue'];
  }
  if (connection['_type'] !== 'RelationalDatabaseConnection') {
    return { store, connection: path, databaseType: String(connection['_type']) };
  }
  const databaseType = String(connection['databaseType']);
  const spec = connection['datasourceSpecification'];
  if (!isObject(spec) || spec['_type'] !== 'deephaven') {
    if (databaseType === 'Deephaven') throw new Error(`${path || store}: a Deephaven connection names no Deephaven server`);
    return { store, connection: path, databaseType };
  }
  const auth = connection['authenticationStrategy'];
  if (!isObject(auth) || auth['_type'] !== 'preSharedKey') {
    throw new Error(`${path || store}: a Deephaven server's sign-in is a key by vault reference (PreSharedKey)`);
  }
  return {
    store, connection: path, databaseType,
    deephaven: {
      host: String(spec['host']), port: Number(spec['port']), useTls: spec['useTls'] === true,
      keyVaultReference: String(auth['keyVaultReference']),
    },
  };
}

/** Where a runtime's grids run: a Deephaven server -- every connection of it the same server's -- or the tab. A runtime
 *  binding a Deephaven server beside another database, or two servers, is refused: no query is run across two. */
export type EngineOf = { readonly kind: 'deephaven'; readonly server: DeephavenServerOf } | { readonly kind: 'tab' };

export function engineOf(runtime: string, connections: readonly StoreConnection[]): EngineOf {
  const servers = connections.filter((c) => c.deephaven !== undefined);
  if (servers.length === 0) return { kind: 'tab' };
  if (servers.length < connections.length) {
    throw new Error(`the runtime ${runtime} binds a Deephaven server beside another database: `
      + connections.map((c) => `${c.store} on ${c.databaseType}`).join(', '));
  }
  const [first] = servers;
  const key = (s: DeephavenServerOf): string => `${s.host}:${s.port}:${s.useTls}:${s.keyVaultReference}`;
  if (servers.some((s) => key(s.deephaven!) !== key(first!.deephaven!))) {
    throw new Error(`the runtime ${runtime} binds two Deephaven servers: ${[...new Set(servers.map((s) => key(s.deephaven!)))].join(', ')}`);
  }
  return { kind: 'deephaven', server: first!.deephaven! };
}

/** What a runtime runs on: where its grids run, and each store's connection. */
export interface RuntimeOn {
  readonly engine: EngineOf;
  readonly connections: readonly StoreConnection[];
}

/** What every runtime the elements declare runs on, by its path -- or why it cannot run, said when its rows are asked. */
export function runtimesOf(elements: readonly unknown[]): ReadonlyMap<string, RuntimeOn | Error> {
  const out = new Map<string, RuntimeOn | Error>();
  for (const e of elements) {
    if (!isObject(e) || e['_type'] !== 'runtime') continue;
    const runtime = pathOf(e);
    try {
      const connections = runtimeConnections(elements, runtime);
      out.set(runtime, { engine: engineOf(runtime, connections), connections });
    } catch (error) {
      out.set(runtime, error instanceof Error ? error : new Error(String(error)));
    }
  }
  return out;
}
