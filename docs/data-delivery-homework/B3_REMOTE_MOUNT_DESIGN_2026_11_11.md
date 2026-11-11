# A remote file mounted by a planned load, its secrets bound by name (B3's Q3, option c′; design only, 2026-11-11)

The B3 worker's design, asked by the orchestrator ("design (c′) to the level of the exact plan records, host steps and
tests ... no code"). Not built. It waits for the user's word on (c′): the cost read and its measurements are
`probes/DuckDbUrlVariable.py.txt` (duckdb-python 1.5.5 and DuckDB-WASM 1.33.1-dev57).

**The rule it implements (the orchestrator's framing):** authentication is a property of the connection.
- A remote file's LOCATION is not secret, and the plan names it.
- Its CREDENTIALS, and a BEARER URL (a presigned link, whose query string is the token), are held in the host's secret
  store and bound when the connection is set up.
- The plan names each of them by name and field, never by value.
- Errors are redacted by value.

## 1. What changes, in one paragraph

A remote mount stops being host operations run outside any plan (census M14's last arm). It becomes one `HOST_SESSION`
load that the planner plans and the hosts' runner runs. Its steps are:
- the extensions;
- the S3 secret, its fields bound from the host's store;
- for a bearer URL, a connection setting (`SET VARIABLE <v> = ?`, the URL bound from the store);
- the view.

The view reads the location literally (`read_parquet('s3://b/t.parquet')`), or reads a bearer URL through
`getvariable('<v>')`, so no view's text ever holds a token. A host replays a load's connection settings on every
connection it opens to that database afterwards: the tab's `connect`, and each Python read's cursor. A variable belongs
to one connection; that much is measured.

## 2. Plan records (core `executionplan`, plan version 48 — or folded into 47 if 47 has not landed; the orchestrator's call)

```java
// ExecutionPlan.LoadStep gains one variant
public sealed interface LoadStep permits LoadStep.Statement, LoadStep.Statements, LoadStep.File, LoadStep.Bound {

    /**
     * A STATEMENT WHOSE ?s THE HOST BINDS (B3 Q3): each bind either the plan's own non-secret value (a secret's scope)
     * or a value the host holds BY NAME (a credential's field, a bearer URL), never a held value itself.
     * {@code reach}: DATABASE -- run once with the load (a secret, kept by the database instance); CONNECTION -- a
     * setting of the connection (a session variable): run with the load on its connection, and replayed by the host on
     * every connection it opens to that database afterwards, until the mount is undone.
     */
    record Bound(String sql, List<Bind> binds, Reach reach) implements LoadStep { /* sql non-blank; binds = the ?s, in order */ }

    enum Reach { DATABASE, CONNECTION }

    sealed interface Bind permits Bind.Value, Bind.Held {
        /** The plan's own value, bound as text: never a credential (only the planner makes one: P2's maker rule). */
        record Value(String text) implements Bind {}
        /** A value the host holds: its store's entry {@code name}, field {@code field} (KEY_ID, SECRET, SESSION_TOKEN,
         *  REGION, ENDPOINT, URL_STYLE, USE_SSL; URL for a bearer URL). */
        record Held(String name, String field) implements Bind {}
    }
}
```

- **Where a Bound step may appear:** only in a `HOST_SESSION` load. `Load`'s constructor refuses it anywhere else by
  name, so exec never meets one. Exec already refuses a `HOST_SESSION` load: `PlanRunner` refuses any load of a cadence
  the query door does not run, naming the cadence, and `Target`'s constructor admits only ONCE, PER_ACQUISITION and
  AT_RELEASE loads.
- **The canonical form:** `Load.canonical` gains `'B' + sql + reach + per bind ('V' text | 'H' name field)`. The
  identity digests the names, never the values, so a rotated key keeps the load's identity.
- **JSON (`PlanJson`):**
  `{"_type":"bound","sql":"…","reach":"CONNECTION","binds":[{"_type":"value","text":"s3://b/"},{"_type":"held","name":"datacube_s3_t","field":"KEY_ID"}]}`,
  read back exactly, any other key refused.
- **Makers:** `setup.LoadSteps.bound(sql, binds, reach)`. ArchitectureTest's P2 rule already covers
  `ExecutionPlan$LoadStep$…`, so `Bind.Value` is made by the planner's makers alone.

## 3. Dialect host operations (core `sql/dialect`)

- `HostOperation.Source.Remote(String url, RemoteFormat)` becomes `Source.Remote(Location location, RemoteFormat format)`,
  with `sealed interface Location permits Location.Url, Location.Variable`:
  - `Url(String url)`: written as a literal. Its constructor refuses a URL with a query string by name: "a URL with a
    query string carries its token: the host holds it, and the plan names it".
  - `Variable(String name)`: written `getvariable('<name>')`.
- New `HostOperation.SetVariable(String name)`: DuckDB writes `SET VARIABLE <name> = ?`.
- New `HostOperation.ResetVariable(String name)`: DuckDB writes `RESET VARIABLE <name>`.
- `Secret(name, scope, fields)` stays as B3.1 left it (each field's ?, then SCOPE's).
- A database without these refuses them by name (`HostOperations.of`).

## 4. The planner (core `planner.LoadPlanner`) and the boundary

```java
public record RemoteMount(@Nullable String view, Location location, HostOperation.RemoteFormat format,
        @Nullable Credentials credentials) {}
public sealed interface Location permits Location.Plain, Location.Bearer {
    record Plain(String url) implements Location {}                 // no query string (refused by name otherwise)
    record Bearer(String name, String shown) implements Location {} // the URL the host holds under `name` (field URL);
                                                                     // `shown` its address without the query string
}
public record Credentials(String name, List<String> fields) {}      // an S3 secret: its name and the fields held

public static ExecutionPlan remoteMount(SqlDialect dialect, DatabaseType type, RemoteMount mount)
```

**What `remoteMount` plans:** one `HOST_SESSION` load on the target `Platform(type, null)`, with the dialect's session
setup. Its steps, in order:
1. `Statement`s: the extensions (`INSTALL/LOAD httpfs`, and iceberg for an Iceberg table).
2. With credentials: `Bound(CREATE OR REPLACE TEMPORARY SECRET "n" (TYPE S3, KEY_ID ?, …, SCOPE ?),
   [Held(n, f) for f in fields] + [Value(scope)], DATABASE)`. The scope is the location's; `credentialScope` moves
   from `remote.ts` into the planner, its one place.
3. With a bearer URL: `Bound(SET VARIABLE v = ?, [Held(name, "URL")], CONNECTION)`. The variable `v` is the planner's
   own spelling: `lite_url_` and the first 16 hex digits of the SHA-256 of `name`, so no person's name reaches an
   identifier.
4. With a view: `Statement(CREATE OR REPLACE VIEW "view" AS SELECT * FROM read_…(<'url' | getvariable('v')>))`.
   Without one (the headless `remoteAccess`, before Python's Iceberg checks), no view step.

**Refused by name:**
- a `Plain` URL with a query string;
- a `Bearer` Iceberg table (a directory has no presigned form);
- a field outside the S3 secret's;
- a bearer name that is not a store name (`[A-Za-z0-9_]+`).

**Boundary:** `Boundary.remoteMount(request)` → wasm `TabExports.remoteMountOrError`, native `lite_remote_mount`.
- The request is JSON with names and fields only:
  `{"databaseType","view"?,"location":{"url"}|{"bearer","shown"},"format","credentials"?:{"name","fields"}}`.
- Its reader refuses any other key, so a key whose name looks like a value (`secret`, `token`, `password`) is refused
  as unknown.
- **Undoing a mount** stays host operations, which carry no data and no value: `Drop(view)`, `DropSecret`,
  `ResetVariable`.
- `HostOperationsJson` loses the `remote` source's URL parsing. Its replacement is the planned load, and the census
  reads the old arm deleted (§8).

## 5. The hosts

**engine-client `plan-runner.ts`:**
- `interface HostSecrets { value(name: string, field: string): PlanValue }` refuses an unknown name or field by name.
- `runLoad(plan, run, { files?, secrets? })` binds each `Bound` step's binds (Value: its text; Held:
  `secrets.value(name, field)`).
- It returns `{ settings: KeptSetting[] }`, the load's CONNECTION-reach steps as written (sql and binds, no value).
- `replaySettings(settings, run, secrets)` runs them on a new connection.
- A `Bound` step without `secrets` is refused by name.

**engine-client `host-operations.ts`:**
- `SecretStore` implements `HostSecrets`. Its fields gain `URL`. `bound(op)` stays for the secret operation until the
  operation path is deleted.

**engine-client `duckdb-tab.ts`:**
- The tab keeps a registry of settings (`settings: { keep(name, settings), forget(name) }`) beside its store.
- `connect()` replays every kept setting after `setUpSession`: every connection set up alike, as its comment already
  demands.

**datacube `remote.ts`** (`mountRemote`, `accessRemote`):
- For each source, classify the URL: one with a query string is a bearer URL, held in the store under
  `datacube_url_<source>` with field URL; any other is a plain location.
- Hold the S3 values under `datacube_s3_<source>`.
- Ask `planner.remoteMount(request)`.
- Run `runLoad(plan, route, { secrets: store })`. `route` is B3.1's fix:
  `(sql, params) => params.length ? engine.exclusive(h => h.rows(sql, params)) : engine.run(sql, 0)`. Statements that
  bind nothing take the engine's queue, which is the measured cure for the demo's "Overflow in timestamp subtraction".
- Keep the returned settings with the tab.
- `readsRemote(shown)`: the receipt names the address without its token.
- `redactSecrets` also redacts the bearer URL, and its query string, by value. Measured: a failed read's error repeats
  the whole URL.
- `credentialScope`, `viewOperation` and `mountOperations` are deleted; the planner owns them.

**datacube `upload.ts`:**
- `mountRemoteView` calls `mountRemote`.
- `dropFileTable(view)` runs `Drop`, `DropSecret` and `ResetVariable`, and forgets the view's settings.

**Python:**
- The mount already runs in the headless bundle (`headless.call('mount')`, the TS above) on the frames' connection.
  Its answer gains `settings` (sql and binds).
- `Engine._mount` holds a store, `{name: {field: value}}`, built from the URL and the `s3` it was handed.
  `frames.keep_file(name, said, view=True, settings=…)` keeps them.
- `Frames.reading` runs each kept setting on each read's cursor after `_setup`: `cursor.execute(sql, params)`, its
  binds resolved from the store.
- Dropping the file forgets them and runs `RESET VARIABLE` on its own connection.
- `_iceberg_refusal` keeps its literal URLs. Its catalog questions run on a plain location only, since a bearer Iceberg
  table is refused at planning.

## 6. Tests

**Java (core):**
- `DuckDbRemoteMountTest` (planner):
  - the steps for a plain Parquet, a plain CSV and a plain Iceberg location;
  - a bearer URL: a CONNECTION-reach `SET VARIABLE`, the view over `getvariable`, no URL in any statement;
  - credentials: the bound secret, Held binds in field order, then the scope as a Value;
  - no view: no view step;
  - every refusal of §4 by name;
  - the JSON round trip;
  - the canonical identity equal for two mounts that differ only in what the host holds.
- `PlanJsonTest`: the `bound` step and its binds round-trip; unknown keys refused; the version.
- `ExecutionPlanTest` (or the load's own test): a `Bound` step outside a `HOST_SESSION` load refused.
- `HostOperationsTest`:
  - `SetVariable`, `ResetVariable`, and the remote's two locations rendered;
  - `Location.Url` with a query string refused;
  - a prepared JDBC run: `SET VARIABLE` bound, then the view over `getvariable` reading a local Parquet, then a second
    connection reading NULL (the measured per-connection fact, pinned).
- `ArchitectureTest` P2: unchanged, and green.
- **The census:** M14 DELETED (§8).

**TypeScript:**
- `plan-runner.test.ts`:
  - `runLoad` binds Value and Held from a store;
  - an unknown name or field refused by name;
  - a `Bound` step without secrets refused;
  - the CONNECTION settings returned;
  - `replaySettings` on a second connection of a real DuckDB-WASM: the view readable on both (the key test).
- `node-duckdb.test.ts`: the probe kept as a test. A prepared `SET VARIABLE` binds; the view's text holds only
  `getvariable`.
- `remote.test.ts`:
  - the sentinel guard extended: credentials and a presigned URL whose token is a sentinel, so no plan JSON, no
    statement's text and no receipt holds it;
  - the redaction of a failed mount's error, which repeats the URL;
  - a plain URL stays literal in the view.
- `verify_remote_test`: the demo with a query-string URL served by its local server (which ignores the query). The
  cube renders, and `duckdb_views()` holds no token.

**Python:**
- `test_engine`/`test_headless`: a remote file served locally under a query-string URL is mounted, then:
  - it is read through two reads, two cursors;
  - `duckdb_views()` holds no token;
  - dropping it resets the variable;
  - a bearer Iceberg URL is refused by name.

## 7. Order of work, and measurements first

0. Measure, in both DuckDBs, before any code:
   - `SET VARIABLE` with the planner's identifier spelling;
   - `RESET VARIABLE`;
   - `read_csv(getvariable(…), …)` with the options the dialect writes;
   - a variable set, a view created, then the view read on a NEW connection after replay;
   - whether the B3.1 regression reproduces when `CREATE VIEW` runs prepared with no parameter. The route above avoids
     it either way.
1. Java: the plan records, `PlanJson`, the maker, the host operations, the planner, the boundary (wasm and native),
   and the tests.
2. TypeScript: the runner, the store, the tab's registry and `connect`, `remote.ts`, `upload.ts`, and the tests.
3. Python: the kept settings on `Frames.reading`'s cursors, and the tests.
4. The census: M14 DELETED, with the full fast loop plus `//datacube/...` and `//python/...`.

The cost is about one slice the size of B3.4: Java about 300 lines and its tests, TypeScript about 200 and its tests,
Python about 40 and its tests.

## 8. The census's proof (M14 DELETED)

The census reads Java alone. It would list the remote source's literal URL record and its JSON reading: the exact
spellings of `Source.Remote`'s url field and of `HostOperationsJson`'s `remote` arm, fixed when written. M14's
`REMOVED_SO_FAR` folds into the deleted row: the fill (B3.4), and the seed text and file operation (B3.5).

The hosts' own deletions are outside the census's reach, so they are checked by their tests and by a grep in the
slice's report:
- `remote.ts`'s `viewOperation` and `mountOperations`;
- `model-data.ts`'s seeding (gone in B3.5);
- Python's literal `CREATE SECRET` (gone in B3.1).

## 9. What it leaves for later (not B3)

- **Short-lived scoped credentials** (STS-style: an `s3://` location with a scoped key and session token). The same
  records serve them, `Held(name, "SESSION_TOKEN")`. Only who mints them and when is new, which is the data-loading
  program's roadmap.
- **Exec's own secret store** (a server reading a remote file). Exec refuses a `Bound` step today. A server-side store
  would resolve `Held` binds the same way, by name.
