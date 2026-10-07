# DataCube app on Postgres: the plan (2026-10-02)

Follows `POSTGRES_DIALECT_HOMEWORK_2026_10_01.md`, whose legs P0 and P1 are done (commits 08d1dc916 to
9fff44529 on `feature/postgres`). Scope is **the developer path only**:

```
bazel run //datacube:app -- postgresql://bob@10.0.0.5:5432/shop
```

That command builds everything. If libpq needs a password, it asks in the terminal. It opens the
browser on `shop`'s tables, and the developer clicks one. Nothing changes in Postgres, there is no
warehouse account, and there is no second server.

Out of scope: multi-user sign-in as one's Postgres account (`serve`, TLS, listening beyond
127.0.0.1), releases and Homebrew, Python.

## Rules for every leg (code-quality bar, ruled by the user)

1. **No engine names outside the two places that own them.** Those are `sql/dialect/` (the dialect
   chosen from the Pure database type) and the warehouse's table of attachable databases (one row per
   kind, as data). TypeScript never branches on `postgres` or `duckdb`. It passes the Pure database
   type through to the model.
2. **No hooks, backdoors or test-only seams.** A capability is a real flag or API, parsed, documented
   and tested like the others.
3. **No fallbacks or guesses** (AGENTS.md invariant 4). A missing password, an unreadable column or a
   refused key is said, by name.
4. **Every leg is gated before the next starts:**
   - `//core:core_tests`, `//core:guardrails`, `//core:census`;
   - `//warehouse:tests`, `//warehouse:tests_native`, `//warehouse:postgres_live`;
   - `//datacube:tests`, `//datacube:typecheck_test`, `//wasm:differential_test`;
   - the corpus gates when shared dialect code moves.
5. **One commit per leg,** its message saying what and why.

## Legs

### A1. The warehouse runs as the app: Postgres URLs, the site, single-user (`warehouse/`)
**Postgres URLs.** A positional `postgresql://user@host:port/db[?param=value]` is a Postgres catalog.
- The catalog is named after the database; a name the catalog grammar refuses is refused by name.
- `options=-c statement_timeout=60000` is added unless the URL sets it.
- `--postgres NAME=DSN` stays, for a name that differs from the database.

**The password is libpq's own.** libpq reads `~/.pgpass` and `PGPASSWORD`. When the attach fails
with libpq's "no password supplied", the warehouse asks once on the terminal (`Console.readPassword`)
and retries. Without a terminal it stops with that message.

**`--site DIR`** serves DIR for every GET outside `/sql/`, with path traversal refused.
`/config.json` is answered as `{"warehouse": "<the request's own origin>"}`. The origin is
`http://` + Host, so `localhost` and `127.0.0.1` both work and no CORS setting is needed.

**`--single-user`** has one principal, an owner: the OS account running the server. Each Postgres
catalog still connects as its own URL's user, so several URLs with different users work. There are
no `--user` or `--owner` lists.
- A random **launch key** is made at start. `POST /sql/v1/login {"key": "<launch key>"}` issues an
  ordinary token.
- The key **stays valid while the server runs** (revised during A1). The page keeps its token in
  memory, so a single-use key would make a reload lose the sign-in, and a single-user server has no
  password to fall back on. This is the same contract as Jupyter's token. The key travels only in
  the address's fragment, which a browser never sends, and the server listens on 127.0.0.1 only.
- Combined with `--user` or `--owner`, the server refuses to start.

**The address.** With `--site` and `--single-user`, the server prints
`http://127.0.0.1:<port>/#key=<launch key>`. **`--open`** also opens it in the default browser; the
tests read the printed address instead. The key is in the fragment, which a browser never sends.
`--table schema.name` adds `&table=schema.name` to that fragment.

**No `--data`.** Single-user without `--data` uses a fresh temporary directory, removed on exit. The
app keeps nothing between runs.

**Also:**
- **Postgres 16 is checked when a catalog attaches.** An older server is refused at start, by name.
- **The site is served to a loopback Host only** (`127.0.0.1`, `localhost`, `[::1]`). A page that
  another site's name resolves here (DNS rebinding) is refused.
- **Start-up errors are one line and exit 2:** a bad command line, or a catalog that cannot attach
  (with the `~/.pgpass`/`PGPASSWORD` hint when libpq lacked a password).

**Tests:**
- URL parsing, the catalog name and the default timeout;
- `--site` traversal and `/config.json` per Host;
- the launch key (valid, reusable, wrong key refused);
- `--single-user` with `--user` refused;
- live: the version check on every attach.

### A2. `//datacube:app` (Bazel)
The `warehouse_run` rule gains an optional `site` and fixed extra args. `//datacube:app` is
`server_native` with `site = //datacube:dist` and `--single-user --open`. It is native only.

*2026-10-07 (the build rebuild's L1c):* `warehouse_run` is gone. `//datacube:app` is `warehouse_folder` (one folder: the
server beside its files, `args = ["--app", "--open"]`), and the server knows nothing of Bazel
(`docs/REBUILD_PROGRAM_2026_10_06.md` §4).

**Gate:** the command runs from a clean checkout. A Playwright test drives the native binary through
key, then list, then open, then group.

### A3. The page starts from what it was asked to open (`datacube/demo/`)
Boot works out its starting source before generating anything: a `#key`, `?remote=`, a share link,
or nothing. Sample trades are generated **only** for nothing.

With `#key`, the page:
1. logs in with the key;
2. removes the fragment from the address;
3. lists the objects;
4. opens `table` if one was given, else shows the Database section's table list, already signed in.

`page-config.ts` reads `config.json` as today. Nothing in it is secret.

**Gate:** the existing page suites (no regression for the sample, `?remote=` or share links), plus
A2's Playwright test.

**As built (A3):**
- **No cube is a state.** The cube on screen is optional. The blank page sets it to none, and a key or
  link start begins with none. Save says "there is no cube to save", and Share says so in its window.
- **A start that opens nothing** (a link that failed, a key refused, an unknown `table`) leaves the
  blank page, with the reason on its card (`.dc-blank-reason`), never an empty page.
- **No other source's Snap.** `makeApp` took `place.snapTarget ?? <the sample's>`, so any cube
  without its own Snap target snapped the sample's table. Now only a place that names what to copy
  can Snap, and the sample passes its own.
- **The picker opens on a given section** (`picker('open', 'database')`).
- **`signInWithKey`** in `warehouse.ts`.
- **`bazel run //datacube:verify_app`** (manual; needs Postgres) runs the native warehouse with
  `:dist`. It checks:
  - the table opens Live, with nothing generated;
  - grouping runs in Postgres;
  - a reload signs in again;
  - without a table, the tables are offered with no password asked;
  - a wrong key and an unknown table are said on the blank page.

### A4. Catalog kinds as data; one listing; no engine names in TypeScript
- **Warehouse:** a catalog is `Native` (DuckDB's own tables, per-object grants, the Authorizer,
  sessions) or `Attached` (passthrough). `Attached` carries a row of a closed table of attachable
  databases: Postgres today, with its extension file, attach `TYPE`, query function, cancel statement
  and Pure database type.
- **API:** `isPostgres` and the `engine` field on `/sql/v1/catalogs` go.
- **One listing:** `GET /sql/v1/objects` returns everything the caller may read in every catalog,
  each object with `catalog` and `databaseType` (the Pure name: `DuckDB`, `Postgres`). The
  per-catalog listing stays.
- **DataCube:** one listing call. `inferModel` writes `type: <databaseType>`. `CatalogEngine` and
  every `=== 'postgres'` go. Snap's rule is leg C's.

**As built (A4):**
- `Attachment` (warehouse) is the closed table, one row (`POSTGRES`). Each operation is an
  exhaustive switch: connection string (UTC pinned), passthrough, cancel and version check, plus its
  extension file, DuckDB attach type, system schemas and Pure database type.
- `Catalogs` maps a name to its `Attachment` (`attachment(c)`, `databaseType(c)`). A native
  catalog's type is `DuckDB` by definition.
- The attach alias is the neutral `attached`. `Database.attach(extension, connection, alias, type)`
  is generic.
- **The API:** `GET /sql/v1/objects` lists every catalog's objects with `catalog` and `databaseType`.
  `/sql/v1/catalogs` reports `databaseType`; its `engine` field is gone. `SqlApi.CatalogObject` and
  the binding (`allObjects`) carry both.
- **DataCube:**
  - one listing call;
  - `inferModel` requires `databaseType` (the tab's DuckDB declares its own:
    `DuckDbEngine.databaseType`);
  - Snap's interim rule is "the table's database type equals the tab engine's", until leg C;
  - saved warehouse cubes record `catalog`. A document from before reads as `main`, a rule stated
    once, at the format boundary (`readWarehouseSource`).
- **Known placeholder:** a warehouse table's model writes `specification: DuckDB { }` whatever its
  `type`. The planner reads only `type`, and Pure has no specification meaning "through a warehouse".
  A made-up `Static` would be a different placeholder, and could disturb legend-engine's planner for
  DuckDB catalogs.

### B. `timestamptz` and the column types (agent, after A1's commit; plan reviewed 2026-10-02)
**The session contract is pinned where reads run.**
- Every warehouse DuckDB connection runs the dialect's `sessionSetup()` (`SET TimeZone='UTC'`).
  It is unpinned today: measured `America/New_York`.
- The Postgres attach adds `TimeZone=UTC`. Today the zone is UTC only by the server's configuration.

**The catalog splits conversions in two:**
- *needed only for a copy* (an upload or Snap is rewritten);
- *needed to read at all* (`HUGEINT`, `UBIGINT`, `UUID`, `TIME`, `INTERVAL` stay excluded, by name).

`timestamptz` becomes readable in place, as TIMESTAMP under the UTC session.

**`bytea` is excluded by name** instead of refusing the whole table. Each Postgres type's outcome is
measured live, not read from code.

**Gate:**
- a timestamptz column's value, filter and year/month group equal `psql`'s
  `AT TIME ZONE 'UTC'`, on a Postgres and on a DuckDB catalog;
- Arrow arrives zoned and displays in UTC;
- Snap stays UTC.

### C. Live feature audit and Snap on Postgres (agent; plan pending its report)
The page's feature suites are run Live against a Postgres copy of their data. Each failure is
classified as a dialect bug, a DataCube local-engine assumption, a passthrough limit, or a data
difference, and the clear ones are fixed in their layer.

**Snap** plans the same model against the tab's database type instead of reusing the live plan's
SQL, so a Postgres table snaps like any other.

**As built (C):**
- **The audit.** `verify_features` runs Live on a warehouse table: `WAREHOUSE=<url> PORT=<port>`,
  and `WAREHOUSE_SNAP=1` to snap it first. It found one dialect bug: Postgres refuses a constant
  `GROUP BY '[ROOT]'` and reads an integer key as a position. The fix is `ConstantKeysAsExpressions`
  (a typed cast). The one check still failing is the status bar's one-word backend, which a
  warehouse cube fills with its plane sentence: a presentation choice, open.
- **Snap.**
  - `inferModel(…, { databaseType, snapDatabaseType })` writes ONE Database and two runtimes: `RT`
    (the table's type) and `SnapRT` (the tab engine's, `QueryEngine.databaseType`).
  - `SnapTarget.planner` is required: the same model against `SnapRT`. `CubeController` pulls the
    rows with the LIVE planner, because the pull runs where the rows are, and runs every query on
    the copy with the target's planner. The `snapOf` exception is gone.
- **Copy conversions are applied in the tab, after the pull.** The catalog's conversions are DuckDB
  SQL (DuckDB's catalog describes every column), and a Postgres catalog's pull is Postgres SQL. The
  pull lands in `<table>__pulled`, and the tab rewrites it into `<table>`. An upload applies the same
  rewrite at ingest.
- **Live proof** (`cube.public.trades_tz`, 5,000 rows, a `timestamptz` column):
  - the pull ran on the warehouse in Postgres SQL, and after the snap nothing went to the warehouse;
  - group, filter (including the `timestamptz` column) and pivot on the copy equal Live and psql;
  - the sweep passes 170/171 both Live and snapped.

## Order
A1 → A2 → A3 → A4, sequentially (one author, shared files). B starts after A1's commit; it touches
`duck/Database.java` and `Catalogs.java`. C's plan is reviewed before it writes code.
