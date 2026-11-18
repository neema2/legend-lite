# TYPESCRIPT: verdict MIXED

## Summary
The design of the TypeScript plan/exec split is sound and in places expert. Branded `PlannedSql`/`PlanText` types are
minted only where a plan is read. Requests and protocol kinds are closed discriminated unions with a type-level
agreement check. The planner's capabilities are small role interfaces, and the doors are tiny compose-then-run
functions. The Java boundary is one host-agnostic facade with thin TeaVM and native adapters. Most of the product
TypeScript is modern and idiomatic: no `any`, `#private` fields, `Error.cause`, careful abort and serialisation logic.
The debt sits around that core:
(1) the plan format is decoded by hand in each host, and the TS decoder is cast-driven: about 30 `as` assertions,
because its unions carry a `{ _type: string }` catch-all;
(2) the layer separation is enforced by about 1,650 lines of hand-written, regex-based TypeScript analysis. TS project
references plus a lint config would give the same, and this repo already learned that lesson once (brand-casts.ts);
(3) `Boundary.java` is a 1,004-line grab bag: inline fully qualified names, JSON requests decoded by hand, JSON
serialised and parsed back again to reach its own helpers;
(4) heavy process-history commentary and leftover re-export shims.
None of this breaks the split, but every change to the plan format, the planner protocol or the module layout pays for it.

## Strengths
- **The brands are the right TypeScript tool, and they are tested at the type level.** `engine-client/src/planned-sql.ts:24`
  declares `type PlannedSql = string & { readonly [planned]: true }`, and `:42` does the same for `PlanText`. Every send
  site takes one (`query-engine.ts:61`, `:76`, `duckdb.ts:34`). `engine-client/test/planned-sql.test.ts:20-58` proves the
  negative cases with `@ts-expect-error`: a plain string, a concatenation, a hand-written plan and a raw `AsyncDuckDB` all
  fail to compile. This is idiomatic, cheap and precise.
- **Closed protocol unions, checked by the compiler.** `legend/planner-protocol.ts:39-89` (`RequestBody` by `kind`) plus
  `:111-112` (`Same<RequestBody['kind'], RequestKind>`) keep the kind list and the union in step. `legend/planner-answer.ts:46`
  is an exhaustive `switch` that tsc checks. `requests.ts:39-63` (`HostOperation` by `op`), `tab-data.ts:30-33`
  (`TableSource`) and `snap.ts:140-142` (`PlaneState`) are proper sum types.
- **Role interfaces instead of a fat planner type.** `planner-sources.ts:15-58` gives one interface per question
  (`FileLoadSource`, `SnapStreamSource`, `HostOperationsSource`, ...), so a door asks for exactly what it uses.
- **The doors are small and readable.** All of `engine-client/src/doors/*.ts` is 284 lines.
  `doors/host-operations.ts:12-19` (ask the planner, then run inside a frame) is the whole pattern, with no framework.
  `doors/tab-duckdb.ts:34-48` overlaps the DuckDB instantiate with the planner's asks and stays testable through
  injected stand-ins.
- **One boundary, thin adapters.** `wasm/.../TabExports.java` is one-line `@JSExport` delegations through `Folded`,
  whose text framing is justified by a real constraint (strings-only hosts, `Folded.java:3-15`). The JVM differential
  compares the builds' answers instead of keeping two copies of the code.
- **Behavioural tests on real components.** `engine-client/test/plan-runner.test.ts` plans with the real WebAssembly
  planner and runs on real DuckDB-WASM. Its "unfiltered witness" (`:146-171`) records what the connection itself
  receives, and the tests assert exact statement order and transaction bracketing (`:221-235`). The runners share
  conformance fixtures with Java and Python (`test/fixtures/keyed-expansion.json`, `run-load-conformance.json`;
  `plan-runner.test.ts:568-645`). That is the right way to keep three hosts honest.
- **Careful error and concurrency handling.** A failed rollback is reported together with the original failure, which is
  kept as `cause` (`plan-runner.ts:248-253`). DuckDB's one-pending-query rule is serialised and explained
  (`duckdb.ts:543-556`). Steps are serialised so their planning cannot interleave (`tab-data.ts:98`, `:116-120`).
  Abort signals are re-checked after queueing (`duckdb.ts:469-473`).
- **Two guards fit the problem well.** `datacube/test/brand-casts.ts` reads casts with TypeScript's own checker
  (`typescript/unstable/sync`), not with regexes. `tools/deps/bundle-inputs.test.ts` (118 lines) closes a real Bazel
  sandbox hole (esbuild reading undeclared files through bazel-bin) with esbuild's own metafile.

## Findings

### TYPESCRIPT-1 [S1] The plan format is decoded by hand in every host, and the TS decoder works by casts, not by narrowing
- Where: `engine-client/src/plan-runner.ts:136-143`, `:155-161`, `:188`, `:199-200`, `:407-456`, `:530-533`, `:544`, `:757`;
  `engine-client/src/plan-runner.ts:63-134`; `python/legend_lite/_runs.py:26`, `:34-48`, `:119-160`;
  `core/src/test/java/com/legend/PlanVersionsAgreeTest.java:33`
- What:
  - **No validation at the edge.** The runner reads `JSON.parse(plan) as ReadPlan` (`:188`, and again at `:530`), so
    nothing checks the plan's shape.
  - **The unions cannot narrow.** `Step` and `LoadStep` each end in a catch-all member, `| { readonly _type: string }`
    (`:143`, `:161`). Because of it, `if (step._type === 'statements')` does not narrow, and every arm casts instead,
    for example `step as Extract<LoadStep, { _type: 'statements' }>` (`:409`), `(step as { readonly sql: string }).sql`
    (`:407`), `root as Keyed` (`:199`) and `(step as unknown as { readonly held: string }).held` (`:456`). That is about
    31 assertions on code lines in one file.
  - **The interfaces are partial and loose.** They are hand-written, with `target: unknown` (`:58`) and kinds as bare
    strings (`cadence: string` `:149`, `snapshot: string`, `keysOf: string` `:654-656`). The database type is read
    from two possible shapes (`:757-758`).
  - **Python copies the TS line for line.** `_runs.py` (`_load_of`, `run_write`, `_stream`) repeats the same checks,
    in the same order, with the same refusal strings.
  - **The version is three hand-kept constants.** `PLAN_VERSION = 51` exists in Java, TS and Python, and is held equal
    by a regex on the TS source (`^const PLAN_VERSION = (\d+);$`). The TS code bends to that regex: "the declaration
    above stays one line, as PlanVersionsAgreeTest reads it" (`:132-133`).
  - **A changelog sits in the code.** `:63-131` is a 70-line version history, and most entries end "refused by name here".
- Why it matters:
  - Every plan-format change is three hand edits, a version bump in three files, and a changelog paragraph.
  - The casts mean tsc cannot tell when Java renames or reshapes a field: the TS keeps compiling and fails at run time.
  - This is exactly the cross-language duplication the plan-as-portable-data goal (G11) should remove.
- Expert alternative: generate the plan types from the one schema that already exists. Java has typed
  `ExecutionPlan` records and `PlanJson.read`/`write` (`core/.../executionplan/`, 1,423 lines), and the repo already
  generates `generated/lite-facts.ts` by running Java (`engine-client/BUILD.bazel:321-361`). Emit
  `generated/plan.ts` (and Python `TypedDict`s) holding the closed unions and `export const PLAN_VERSION`. Decode once
  at the edge into that closed union, with `function readPlan(text: PlanText): Plan`; an unknown `_type` is refused
  there. After that, `switch (step._type) { case 'statements': step.ref ... }` narrows, ends in `default: { const _: never = step; }`,
  and needs no casts. The version history belongs in git and in PlanJson's docs.
- Confidence: verified

### TYPESCRIPT-2 [S1] Layer separation is enforced by a hand-written regex analyser of TypeScript where the build and lint could do it
- Where: `tools/deps/layer-alone.test.ts` (757 lines): `codeAlone` `:690-757`, `declaredSignatures` `:513-559`,
  `namedIn`/`origin`/`unaliased` `:465-504`, `explicitAnys` `:678-685`, `suppressions` `:450-452`,
  `literalDoorBreaches` `:397-428`; `tools/deps/app-runs.test.ts` (375 lines): `importedBy`/`reExportedBy`/`localBindings`
  `:89-146`, `RUN_CALL` `:217`, `CENSUS` `:183-210`; `tools/deps/computed-doors.mts`; `tools/deps/BUILD.bazel:186-352`
  (`_LAYERS_ALONE`, a positional 7-tuple per layer); `engine-client/BUILD.bazel:34-114`
- What:
  - **The targets do not enforce imports.** Every layer lives in one flat `src/` directory, and `js_library` does not
    check relative imports, so the file lists in `engine-client/BUILD.bazel` are declarations only.
  - **Enforcement is two typechecks per layer.** For each of 8 layers, the test copies the layer's runfiles closure to a
    temp directory, runs strict `tsc`, then plants an import and runs `tsc` again. Each layer row also pins expected
    files, a file-count floor, a planted path and planted imports.
  - **The other rules are regex parsers of TypeScript:**
    - a hand lexer for comments, strings, templates and regex literals (`codeAlone`, CCN 32);
    - regex extraction of function, class and interface signatures, with alias resolution through re-exports to a fixed
      point (`declaredSignatures` CCN 26, `answersPlanned` CCN 19);
    - three separate import and export clause parsers (`namedIn` in layer-alone; `importedBy` and `reExportedBy` in
      app-runs; a fourth in Java, `tools/guards/PlannedSqlTest.java:346-360`);
    - an `any` detector and a `@ts-ignore` detector, the latter repeated in `PlannedSqlTest.java:376`.
  - **The scanners are copied, too.** `withoutComments` appears in both `computed-doors.mts:7` and `app-runs.test.ts:232`,
    `withoutStrings` in `layer-alone.test.ts:507` and `app-runs.test.ts:238`, and argument splitting in
    `computed-doors.mts:60-72` and `layer-alone.test.ts:431-445`.
  - **The census pins call text.** `app-runs.test.ts:183-210` pins counts per file, such as `'runner.run(': 5` for cube.ts,
    by matching receiver names (`[sS]ession|[eE]ngine|...|[hH]eld`). Rename a variable to `r` and the site vanishes;
    give an unrelated `.rows(` a variable named `engine` and the census fails.
  - **The repo already found this out.** `datacube/test/brand-casts.ts:1-4` records that "a regex census read the casts'
    text, and missed ..." and moved to the checker API. These tests were written three days later the regex way.
- Why it matters:
  - About 1,650 lines of guard code (plus about 115 BUILD lines) protect 284 lines of doors.
  - Every new module means a BUILD-list assignment, possibly a floor or expected-file update, and possibly a census row.
  - Every parser gap (a TS syntax the regexes do not know) turns into either a false red or a silent pass.
  - A new engineer cannot tell which of five overlapping mechanisms owns a rule.
- Expert alternative:
  - Put each layer in its own directory (`src/records`, `src/engine-api`, `src/run`, `src/planner`, `src/server`,
    `src/doors`).
  - Give each layer a `tsconfig.<layer>.json` with `composite: true`, `include` set to its own directory and
    `references` set to the layers it may reach. Then `tsc -b` rejects any import of a file outside the project's
    references (TS6307). That is the "typechecked alone" rule with no custom code.
  - Use lint rules for the rest: `@typescript-eslint/no-explicit-any`, `@typescript-eslint/ban-ts-comment`,
    `import/no-restricted-paths`, and `no-restricted-syntax` with `ImportExpression[source.type!='Literal']` for
    computed specifiers.
  - Write the one bespoke rule (no run signature takes a planner source) against the checker API, as `brand-casts.ts`
    already does.
  - Drop the call-count census. Once the apps cannot import a run module, which the build enforces, it adds nothing.
- Confidence: verified (the line counts and the regex parsers). The TS6307 behaviour of project references is standard
  `tsc -b`; I did not run it here.

### TYPESCRIPT-3 [S2] `Boundary.java` is a grab bag: inline fully qualified names, JSON decoded by hand, JSON round trips
- Where: `wasm/src/main/java/planner/Boundary.java` (whole file), `:261-293`, `:420-492`, `:463`, `:468`, `:477`, `:484`,
  `:521-528`, `:721-784`, `:845-849`, `:906-912`, `:929-1003`
- What:
  - **No imports.** The file has 0 `import` lines, 172 `com.legend.*.Type` references and 70 `java.util.*` references, for
    example `com.legend.json.Json.Obj in = com.legend.json.Json.parseObject(request);`. Its sibling
    `HostOperationsJson.java:6-10` uses imports, so no constraint forces the style.
  - **One class does everything.** About 35 public statics cover pure/v1 routing, planning, typing, held models, file
    loads, remote mounts, snapshots, table output, authored SQL, synthesis, catalog-to-model, session setup, host
    operations, project data, and timing probes (`touchPrelude`, `hashBootSource`, `resolveBootLayer`, `zoneProbe`).
  - **Requests are decoded inline.** Each request type gets its own ad hoc `getString`/`only(...)` code.
  - **`outputPlan` serialises and parses JSON back three times to reach its own helper.** Each table node goes
    `Json.parse(Json.toCompact(t))` (`:463`, `:477`). A `Map` is serialised, parsed into an `Obj`, passed to
    `tablesModel` (which returns a String), and that String is parsed again (`:468`, `:484`).
  - **A Pure program is built from text placeholders.** `outputProgram` writes `'__lite_output_rows__'` into Pure, parses
    it, and `placed` swaps the placeholder nodes in by regex over the JSON text (`:521-528`).
  - **The same Pure connection and runtime text is written twice:** `connectionAndRuntime` (`:906-912`) and the
    `StringBuilder` in `tablesModel` (`:761-765`).
  - **The same catalog-column shape is decoded twice, with different strictness:**
    `Boundary.intOrNull` truncates silently with `(int) num.longValue()` (`:847`), while `HostOperationsJson.intOrNull`
    requires a whole number and uses `Math.toIntExact` (`:200-208`).
- Why it matters:
  - This is the planner's public surface for two hosts. Every new request lands in this file, the FQNs make it hard to
    read, and the JSON round trips and regex surgery are fragile and slow.
  - The two catalog decoders can already disagree on the same input.
- Expert alternative:
  - Use imports.
  - Give each request a record with one decoder, for example
    `record SnapCopyRequest(String databaseType, String model, String runtime, String lambda, @Nullable String schema, String table) { static SnapCopyRequest read(Json.Obj o) }`,
    all built on one strict reader shared with `HostOperationsJson`. That also gives one `CatalogColumn` decoder.
  - Have `tablesModel` return a typed `TablesModel(model, runtime, List<TableModel>)`, and serialise only on the way out.
  - Build the output program as protocol nodes, not text placeholders.
  - Split the file by concern: `PlanningBoundary`, `LoadBoundary`, `CatalogBoundary`, `Probes`. `Boundary` stays a thin
    facade if one entry class is needed.
- Confidence: verified

### TYPESCRIPT-4 [S2] `HostOperationsJson` keeps each operation's keys in two places, so a source-reading test exists to hold them equal
- Where: `wasm/src/main/java/planner/HostOperationsJson.java:36`, `:48-56`, `:57-94`, `:98`, `:102-123`, `:153-176`;
  `core/src/test/java/com/legend/PlannerSurfaceTest.java:1470-1532`
- What:
  - **The keys are written twice.** The `KEYS` map lists each operation's keys, and the `switch` arm reads them again,
    for example `case "relation" -> ... string(o, "kind") ... object(o, "target") ...`.
    `PlannerSurfaceTest.hostOperationKeys` regex-reads this Java file to check that the two agree.
  - **Removed designs stay as tombstones:** the `insert` and `secret` arms (`REFUSED`), the `file`/`remote`/`query`
    source refusals, and `SQL_KEYS = ("selectList", "replacing")`.
- Why it matters: adding an operation means editing the map and the arm and keeping a text-parsing test happy. The
  tombstones grow with every design that is removed.
- Expert alternative: give each operation one spec, `Map<String, OpSpec>` where `record OpSpec(Set<String> keys, Function<StrictObj, HostOperation> read)`.
  Or use a `StrictObj` reader that records which fields were read and refuses any unread key after `read`, so the keys
  are the reads by construction and the source test can go. A removed operation then needs no arm: the decoder already
  refuses an unknown `op` by name.
- Confidence: verified

### TYPESCRIPT-5 [S2] Two planner clients implement one protocol, and a new request kind touches about 11 places
- Where: `engine-client/src/legend/wasm-grammar.ts:227-307` vs `datacube/src/lite-planner.ts:741-838`;
  `legend/planner-protocol.ts:10-89`; `legend/planner-answer.ts:10-72`; `wasm/.../TabExports.java`; `Boundary.java`;
  `python/legend_lite/_planner.py:33`; `python/legend_lite/compiler.py:236`
- What:
  - **The same calls, written twice.** `WasmGrammar` and DataCube's `LitePlanner` each implement `fileLoad`,
    `remoteMount`, `snapStream`, `snapCopy`, `synthesize`, `sessionSetup`, `projectData` and `hostOperations` the same
    way: `planFromAnswer(unfold|decode(await port.ask({ kind, request: JSON.stringify(request) })))`. The doc comments
    are nearly identical too.
  - **A new kind is hand-edited in about 11 places:** `REQUEST_KINDS`, `RequestBody`, `PlannerModule.exports`, the
    `answer` switch, `TabExports`, `Boundary`, the native `Compiler`, Python's `_planner` table and `compiler.py`
    function, two TS clients, `requests.ts` and `planner-sources.ts`. 23 product files name `snapCopy`.
  - **Typed requests lose their types on the wire.** In the protocol they become `request: string`
    (`planner-protocol.ts:63-81`).
- Why it matters: protocol drift between the two TS clients is invisible to the compiler, and each planner feature is a
  cross-repository edit list.
- Expert alternative:
  - Write one typed `PlannerClient` over `PlannerPort` in engine-client, holding the request codecs once. `LitePlanner`
    composes it and adds held-model caching.
  - Make `RequestBody` carry the typed request (`{ kind: 'snapCopy'; request: SnapCopyRequest }`), with the client
    `JSON.stringify`ing it in one place.
  - Generate `PlannerModule['exports']` from `TabExports` by reflection, as `TypeFacts` is generated.
- Confidence: verified (both clients read); the count of 11 is from grep and reading.

### TYPESCRIPT-6 [S2] `runPlan` is one 89-line closure that mixes validation with execution and duplicates its neighbours
- Where: `engine-client/src/plan-runner.ts:527-618` (lizard: CCN 39, 89 lines), `:187-193` vs `:530-533`, `:205-211` vs
  `:556-558`, `:575-577`, `:240-258` vs `:357-395`, `:402-403`, `:476-488`, `model-data.ts:147`
- What:
  - **`read()` is re-inlined.** `runPlan` repeats it, and its local `const read` shadows the module function.
  - **The one-target check is copied** from `targetOf`.
  - **One ternary arm is unreachable.** At `:577` the nested ternary still names `'payload'`, which `:575` has already
    `continue`d past.
  - **`stream()` hand-rolls `transacted`.** It does its own begin/try/rollback/commit and composes the same "-- and
    rolling its transaction back failed" message as `:251` (`:391`).
  - **`deliver` has a hard-to-read signature.** It takes positional optionals `files?, secrets?, streams?` and fills an
    out-parameter, `landed: Landed[] = []`.
  - **The load path depends on call order.** `putFile` must compose `inLoadsTransaction(plan, run, () => runLoad(plan, run, ...))`,
    which parses the plan twice and relies on the caller pairing the two calls correctly.
- Why it matters: the runner is the most change-prone file in the unit. Its structure hides what is validated and what
  is executed, and the copies drift (one of them already has a dead arm).
- Expert alternative:
  - Split it: `validate(read: Plan): ValidatedPlan` returns the steps, the single target and the slots plan, and
    `execute(v: ValidatedPlan, session)` runs them.
  - Give `deliver` the signature `(load, session, host: HostCapabilities): Promise<{ settings, landed }>`.
  - Implement `stream` through `transacted`.
  - Make `runLoad` take `{ transaction: true }`, or expose a `runLoadInTransaction`, so callers do not pair two entry
    points.
- Confidence: verified

### TYPESCRIPT-7 [S2] The runner answers JSON text built by template strings, and its callers parse it back
- Where: `engine-client/src/plan-runner.ts:527-528`, `:800-810`; `datacube/src/table-output.ts:72-81`;
  `engine-client/test/plan-runner.test.ts` (`looseJson(await run(plan))` throughout)
- What:
  - `runPlan(): Promise<string>` hand-builds `[{"value":2}]` (`rowsOf`), and `writeTable` immediately does
    `JSON.parse(answer)` and digs out `[0].value` (`countOf`).
  - The runner is the run side's API; serialising inside it forces every consumer to parse again.
- Why it matters: it is a stringly-typed internal API. The types of the cells the runner read are thrown away at the
  module boundary, and each caller writes its own unchecked re-parse.
- Expert alternative: have `runPlan` return typed rows, `Promise<readonly Readonly<Record<string, Cell>>[]>`, where
  `Cell = string | number | boolean | null`. A `jsonRows(columns, rows)` writer then lives at the edge that needs lite's
  JSON. If byte-parity with `JsonRowsWriter` matters, test it there.
- Confidence: verified

### TYPESCRIPT-8 [S2] The run-frame witness is called by hand, opened at two levels, and blind in production
- Where: `engine-client/src/run-frame.ts:31`, `:37-39`, `:52-58`; `plan-guard.ts:10`, `:28`; `duckdb.ts:424`, `:442`, `:490`,
  `:578`, `:611`; `legend/planning-client.ts:96-140`; `doors/project-data.ts:101` with `plan-runner.ts:479`;
  `doors/tab-duckdb.ts:59`; `doors/file-load.ts:25` → `model-data.ts:147` → `plan-runner.ts:268`, `:230`, `:242`;
  `engine-client/test/unplanned.ts:16-22`, `:41-58`
- What:
  - **Calls are made by hand:** `sending()` at 6 send primitives, `planning(kind)` at 16 planner entries, and `inRun` at
    24 product sites.
  - **The frame is opened twice.** The runner functions open their own frame (`runLoad` → `inRun('a load')`), and the
    doors wrap them in another (`dataLoaded` → `inRun("a project's Data rows loaded", () => runLoad(...))`). One file put
    nests four frames.
  - **It only works under Node.** AsyncLocalStorage is not available in the browser or the headless V8, so the witness
    sees nothing there (`run-frame.ts` header, `frameKept`), and the layers' rule is the only guard in production.
  - **Tests pay a ceremony cost:** 229 `asTest(...)` and 52 `inRun('a test', ...)` sites. `unplanned()` adds every test
    statement to a module-level `Set` that only grows (`OWN`). `asTest` is a `Proxy` that wraps methods chosen by string
    name (`SENDS = ['run','execute','stream','exclusive']`).
- Why it matters: who opens a run has no single owner, the per-method calls are easy to forget on a new entry, and the
  test tax is high for a check that protects only the Node test run.
- Expert alternative:
  - Open frames in one place: the runner's exported entry points. Doors call them and do not wrap them.
  - Apply `planning()` once, as a decorator over `PlannerPort.ask` and `HttpPlanning.json`, not per method.
  - Apply `sending()` once, in a connection wrapper that `DuckDbEngine` and `WarehouseEngine` build their sends on.
  - In tests, provide one `testEngine()` fixture that is pre-framed, instead of a proxy keyed on method names, and no
    global text registry.
  - Keep the goal (G4's run-time proof), with less surface.
- Confidence: verified

### TYPESCRIPT-9 [S2] `snap.ts` checks capabilities by casting, and duplicates its state machine
- Where: `engine-client/src/snap.ts:186`, `:191`, `:194-199`, `:303-319`, `:336`, `:224-245`/`:369-395` vs `:480-562`,
  `:356-364`/`:508-517`/`:529-538`
- What:
  - **A capability is reached through a cast.** `const store = engine as unknown as Partial<StreamStore>; if (typeof store.exclusive !== 'function') throw ...`.
    The constructor takes `host?: HostOperationsSource & Partial<SnapStreamSource>` and later checks
    `typeof host.snapStream !== 'function'` and `typeof host.snapCopy !== 'function'`.
  - **The state machine is written twice.** `SnapManager` and `HostSnaps` each implement `state`, `isSnapped`,
    `sourceFor`, `release`, `detach` and `reattach` identically.
  - **`SnapInfo` is assembled three times** with the same conditional spreads.
- Why it matters: the cast hides a missing capability until run time, though the type system could require it at
  construction. The copies will drift.
- Expert alternative:
  - Type the constructor with the needs: `engine: QueryEngine & StreamStore & { databaseType }`.
  - Offer separate constructors, or a union `{ local: SnapCopySource } | { remote: RemoteSource; planner: SnapStreamSource }`,
    so an impossible mix cannot be built.
  - Extract a small `SnapState` class that both classes hold, and a `snapInfo(...)` factory.
- Confidence: verified

### TYPESCRIPT-10 [S3] Comments narrate process history (148 dates, about 193 audit and ruling references in engine-client/src)
- Where (3 of many): `engine-client/src/plan-runner.ts:63-131` (a 70-line version changelog); `run-frame.ts:54-55`
  ("the C5-T re-check's MUST 1: Windows measured the grid 14 bytes over; the tail naming a door and the plan runner cost
  31 gzipped"); `tab-data.ts:173-177`. Javadoc in `Boundary.java` reads the same way, for example `:247-259`: "B3,
  2026-11-11, the orchestrator's go ... census M14".
- What: comments make up 31% of engine-client/src (2,170 of 6,979 lines) and 272 of 810 lines in plan-runner.ts. Much of
  it says who ruled what, and when. Test names carry audit codes ("the C final audit's H8; the re-audit's S8").
- Why it matters: the comments explaining the present code (and the duckdb.ts design notes are good ones) get lost in
  the history. History in javadoc also goes stale, see TYPESCRIPT-11.
- Expert alternative: keep "what and why now" in the code and move dates, rulings and audit IDs to commit messages and
  docs. Name tests by behaviour.
- Confidence: verified

### TYPESCRIPT-11 [S3] Half-migration residue: re-export shims, a stale export, an unused variant (about 9 sites)
- Where: `engine-client/src/engine.ts:18-35` (re-exports plus a 4-line `setUpSession`, the whole module);
  `host-operations.ts:8-12` (re-exports requests.ts beside an unrelated `SecretStore`, `:21`); `duckdb.ts:68-70`;
  `model-data.ts:40`; `project-data.ts:16`; `plan-runner.ts:42`; `plan-runner.ts:235-240` (`transacted` is exported, and
  its doc names "DataCube's table writer" as a caller; no product caller exists outside this file); `requests.ts:63`
  (`{ op: 'transaction' }`, sent by no TS source since the plan carries its transaction).
- Why it matters: two import paths for one symbol, and docs that point at callers that no longer exist.
- Expert alternative: point the importers at the owning module, delete the shims and the unused variant, and make
  `transacted` module-private, or document its real users.
- Confidence: verified

### TYPESCRIPT-12 [S3] Small duplications (each a few lines, about 8 patterns)
- Where (3 representative): three SHA-256 helpers, two of them named `sha256Hex` with different signatures
  (`plan-runner.ts:299` `digestOf`, `:315` `async sha256Hex(bytes)`, `sha256.ts:19` `sha256Hex(text)` synchronous).
  The runner's blob check needs WebCrypto even though `sha256.ts` exists because "an insecure origin and some hosts do
  not give" it. The message idiom `e instanceof Error ? e.message : String(e)` is inline 16 times in engine-client/src
  and 142 in datacube/src. The inline function type `(sql: PlannedSql, params: readonly PlanValue[]) => Promise<unknown>`
  is repeated 7 times.
- Also: redaction is implemented twice (`connection-settings.ts:58-90` and `datacube/src/remote.ts:180-201`). The
  binding-kind ternary appears twice (`plan-runner.ts:577`, `plan-params.ts:47`). There are two refusal classes for one
  concept (`PlanRefusal`, `PlanParamsRefusal`). Tests define `looseJson = (text): any => JSON.parse(text)` 13 times, a
  side effect of the global `json-unknown.ts` override.
- Expert alternative: one `digest.ts` (`sha256Hex(bytes | string)`, synchronous); `messageOf(e: unknown)`;
  `type Send = ...`; one `redact`; one refusal type; a shared test helper.
- Confidence: verified

### TYPESCRIPT-13 [S3] Muddled vocabulary: "door" means two things in the same files
- Where: `tools/deps/computed-doors.mts:1-4`, `:45-49` ("each import door a module opens"); `layer-alone.test.ts:68-90`
  (`COMPUTED_DOORS` for import sites, next to `DOOR_SRCS` for the doors layer); `engine-client/BUILD.bazel:96-103`
- What: "door" names a plan-then-run module (`src/doors/`, the `:doors` target) and also any dynamic `import()`, Worker
  or URL specifier site. "run" is a layer, a frame, a method and a verb.
- Expert alternative: call the composition modules "doors" (or "use cases") and the import sites "dynamic imports"
  (`computedImports`).
- Confidence: verified

### TYPESCRIPT-14 [S3] Tests pin text where structure is available
- Where: `engine-client/test/plan-runner.test.ts:183-186` (`assert.match(plan, /"_type":"sequence"/)`), `:405-407`; the
  refusal regexes throughout (54 `assert.match`/`rejects`/`throws`); the conformance fixtures' exact cross-language
  message equality (`:575-580`, `:620-643`); `strip` written twice in one file (`:255-260`, `:273-279`)
- What: the plan's shape is checked with a regex over its JSON text, and three languages must keep byte-identical error
  prose.
- Why it matters: rewording a message is a three-language edit, and a field reorder breaks a regex.
- Expert alternative: parse the plan and assert on fields. Assert refusals by error class plus a stable code
  (`PlanRefusal { code: 'TARGET_TRANSACTION_MISSING' }`), and keep prose free to change.
- Confidence: verified

## Cross-unit observations
- **GUARDS:** `tools/guards/PlannedSqlTest.java` (1,652 lines) regex-parses TypeScript import statements (`:346-360`)
  and re-detects `@ts-ignore` and `as any` (`:366`, `:376`), overlapping `layer-alone.test.ts` and `brand-casts.ts`.
  `PlanVersionsAgreeTest.java:33` and `PlannerSurfaceTest.java:1470-1532` read TS and Java source text with regexes.
- **PYTHON:** `python/legend_lite/_runs.py` mirrors `plan-runner.ts` line for line (the stream step, the version check,
  the rollback-failure message). It also repeats its own version check twice (`:39-43`, `:126-129`), as TS does
  (`plan-runner.ts:189` vs `:531`). One generated schema would remove most of both.
- **JAVA (core):** `PlanJson`/`ExecutionPlan` are the natural source for generated host types. Java reads the plan into
  typed records; TS and Python read untyped JSON.
- **DataCube:** `lite-planner.ts` (988 lines) duplicates the engine-client planner client (TYPESCRIPT-5).
  `table-output.ts:76-81` parses the runner's text back (TYPESCRIPT-7). `remote.ts:180-201` redacts again
  (TYPESCRIPT-12). `datacube/test_imports.bzl` (11,711 lines, generated) is another sign that the build does not
  resolve TS imports itself.

## Measurements
- plan-runner.ts: 810 lines, 272 comment lines (`grep -cE "^\s*(//|\*|/\*\*)"`); about 31 type assertions on code lines
  (`grep -n " as [A-Z({]\| as unknown\| as Extract" | grep -v comment lines`).
- lizard (`python3 -m lizard -l typescript -w -C 15 -L 60`): `runPlan` body CCN 39 / 89 lines; `deliver` CCN 27 / 61 lines;
  `snap` CCN 18 / 89 lines; `codeAlone` CCN 32; `declaredSignatures` CCN 26; `literalDoorBreaches` CCN 23; `answersPlanned` CCN 19.
- jscpd (`--min-lines 5 --min-tokens 50`, engine-client/src, tools/deps, lite-planner.ts, remote.ts): 3 to 4 exact clones,
  about 0.25% of lines. The duplication in this unit is structural, not copy-paste.
- engine-client/src: 6,979 lines, 2,170 comment lines (31%); 148 ISO dates (`grep -o "20[0-9][0-9]-[0-9][0-9]-[0-9][0-9]"`);
  193 process tokens (audit|ruling|C5-T|B3|MUST n|re-audit|checkpoint|orchestrator|SHOULD-FIX|census Mn).
- TS layering guards: 1,654 lines (`layer-alone.test.ts`, `app-runs.test.ts`, `bundle-inputs.test.ts`, `computed-doors.mts`,
  `TsLayeringTest.java`, `ts-layers.txt`) plus about 115 lines of `_LAYERS_ALONE` BUILD config; versus 284 lines in `src/doors/`.
  `brand-casts.ts` + `brand-casts.test.ts` + `PlannedSqlTest.java`: another 1,951 lines.
- Boundary.java: 1,004 lines, 0 imports, 172 `com.legend.*` and 70 `java.util.*` fully qualified references.
- Test ceremony (engine-client/test, datacube/test): 229 `asTest(` sites, 52 `inRun('a test'` sites, 13 copies of `looseJson`.
- Product files naming one planner request kind (`snapCopy`), across Java, TS and Python, tests excluded: 23.
- PR #105 alone (`git diff --stat 4dfbb5b7^1 4dfbb5b7 -- engine-client tools/deps datacube wasm`): 112 files, +5,271/−2,812.
