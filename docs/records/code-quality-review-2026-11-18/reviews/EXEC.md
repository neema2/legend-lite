# EXEC: verdict WEAK

## Summary
The skeleton is right. The plan is a sealed tree that exec dispatches on with exhaustive switches. Sessions sit behind one port (`Statements`). Each database's facts live in a small connector. Results stream as typed batches to closed `Output` writers, and the two public doors really are thin. Under that skeleton, three design problems make most changes in the area expensive:
- **Stringly column types:** a column's type facts are strings with an embedded grammar. Three decoders and three writers re-parse them and match them by name, partly per cell.
- **Two runners:** "a list of steps on one session" is run by two runners (`PlanRunner.sequence`, `ProgramRunner`), with duplicated shape analysis and transaction brackets and with diverging rules.
- **Scattered session acquisition:** it is spread over five overlapping abstractions, with process-global caches that no one owns.

`PlanRunner` is a 1,209-line bag of package-private statics, called across the package. On top of this, every file carries heavy process history in its comments (327 dates and 197 ticket codes in 11.2k lines) and 578 inline fully-qualified names, which taxes every read.

Resource handling on failure paths is careful and mostly correct: try-with-resources throughout, `addSuppressed`, rollback before restore. Functions are mostly small (average CCN 3.2). The fixes are targeted redesigns, not a rewrite, so the verdict is WEAK rather than REWORK.

## Strengths
- **Compiler-checked node dispatch.** Plan nodes are handled by `switch` over the sealed `ExecutionPlan.Node` with no `default`:
  - `core/src/main/java/com/legend/exec/PlanRunner.java:100-143` (`streamRun`), `:503-514` (`steps`), `:1137-1152` (`statementsOf`);
  - `ProgramRunner.java:519-570`, `ProductRun.java:128-137`.

  Adding a node kind turns the build red at every site.
- **The connector abstraction is good.** Each database is a 50-170 line class answering a handful of facts (`DuckDbConnector.java`, `PostgresConnector.java`, `H2Connector.java`). `Connectors.of` (`Connectors.java:185-206`) is an exhaustive enum switch with no `default`, and an unbuilt type is a named refusal object (`Connectors.java:249-291`). There is no per-database `if` ladder in the runner.
- **One JDBC send site.** Every `Statement`/`ResultSet` is opened in try-with-resources at one place (`JdbcStatements.java:596-629`).
- **Careful failure paths:**
  - the snapshot bracket restores isolation and autocommit and keeps a secondary failure as suppressed (`JdbcStatements.java:473-546`);
  - `Sessions.unwind` clears the session memo before the rollback, for a stated reason (`Sessions.java:475-490`);
  - the file-load undo runs on any failure and suppresses the undo's own errors (`PlanRunner.java:679-692`);
  - `closedOnFailure` closes a half-made session (`PlanSessions.java:508-520`).
- **Thin doors.** Each `Execution`/`TestExecution` method is one planning call plus one run call (`Execution.java:43-108`, `TestExecution.java:153-197`). `ExecuteOptions` is a two-field record with withers.
- **`PlanParameters` is well typed.** It uses a sealed `Checked` (One/Many/None) and an internal sealed `Conversion` (Converted/Refused), and it reports all problems at once (`PlanParameters.java:33-100`).
- **`Output` is a closed set of formats.** A writer is made before any statement runs (`result/Output.java:20-92`). `Batcher` and `Collector` are small, guarded state machines (`Batcher.java`, `Collector.java`).
- **Low textual duplication.** jscpd finds 1.35%; the duplication is structural, not copy-paste.
- **One fixture across three languages.** Keyed expansion is held to a single conformance fixture shared by the Java, TypeScript and Python runners (`core/src/test/java/com/legend/exec/KeyedExpansionConformanceTest.java:19-56`), which is the right answer to a genuine multi-host constraint.
- **Behavioural tests on real databases.** They run against in-process databases and check the exact statements sent through a recording connection (`RaiseReadBackTest.java:100-124`), not mocks.

## Findings

### EXEC-1 [S1] Column types are strings with a grammar, re-parsed and matched by name in six classes (partly per cell)
- **Where:**
  - the string field and its writer: `core/src/main/java/com/legend/executionplan/ExecutionPlan.java:661` (`Column(String name, String type, ..., String slot, SqlDdl.ColumnType store, ...)`), `executionplan/ColumnFacts.java:182-208` (`slot(SqlType)` writes `DECIMAL(p,s)`, `ARRAY<..>`, `STRUCT<name slot, ..>`, `TAGGED<kind slot, ..>`, `MAP<k,v>`);
  - the readers: `exec/JdbcDecode.java:132-152, 246-316, 425-430, 586-666`, `result/RowDecode.java:99-241, 349-503`, `core/src/main/mongodb/com/legend/exec/MongoDbDecode.java:89-180`, `result/ArrowWriter.java:131-236`, `result/Batch.java:156-171`, `result/CellText.java:103-130`, `result/HostRows.java:81-87`.
- **What:**
  - **The slot is a typed `SqlType` in the planner and a string in the plan.** `ColumnFacts.slot` stringifies it, then exec and result parse it back with `slotWord`, `inside`, `topLevel`, `precisionAndScale`, `declaredScale` and `typeArguments`: 28 parse call sites.
  - **Every reader dispatches on string literals:**
    - the slot word `case "INTEGER", "BIGINT", "SMALLINT", "TINYINT", "HUGEINT"` (5 ladders in 4 files);
    - the Pure type, `type.equals("Integer")` … `"Decimal" || startsWith("Decimal(")` (61 matches in 8 files).
  - **Three decoders repeat the same two-stage ladder (slot word, then Pure type):** `JdbcDecode` (driver objects), `RowDecode` (JSON nodes) and `MongoDbDecode` (BSON). Each re-implements the tagged-column read (`JdbcDecode.java:169-185`, `RowDecode.java:63-85`, `MongoDbDecode.java:52-60`).
  - **Three writers repeat it again:** `ArrowWriter`, `Batch.Vector.of` and `CellText`.
  - **Some parsing happens per cell.** `JdbcDecode.struct` calls `RowDecode.fields(c)` (`JdbcDecode.java:503`), `tagged` calls `taggedSlot`/`tagOf` (`:171-176`), `map` calls `mapSlots` (`:540`), and the DECIMAL arm calls `declaredScale` (`:285`). The per-column `Kind` cache (`JdbcDecode.java:65-118`) covers the top level only.
  - **`Kind.of` has a `default -> BY_PURE_TYPE` catch-all** (`JdbcDecode.java:150`). A misspelled slot silently falls into the Pure-type ladder instead of failing.
  - **The Any arm decides structure from the text's look:** `t.startsWith("{") || t.startsWith("[")` (`JdbcDecode.java:631-634`).
  - **It is a half-migration.** The plan's `store` fact was moved from text to a typed `SqlDdl.ColumnType` ("until then its grammar spelling, which exec matched by its text", `ExecutionPlan.java:655-660`), but `slot` and `type` were not.
- **Why it matters:**
  - A new SQL type or Pure type means editing about six string ladders, with no compiler help; a missed ladder is a runtime refusal on one database lane.
  - Three ad-hoc parsers of the same grammar (`topLevel`, `typeArguments`, and the comma scan in `precisionAndScale`) can disagree.
  - Exec already imports `com.legend.sql.SqlType` (`Statements.java:76`), so the typed form is reachable. Portability (G11) constrains only the JSON form, not the Java model.
- **Expert alternative:**
  - Make `Column.slot` a `@Nullable SqlType` and add a closed `PureKind` enum (`INTEGER, FLOAT, DECIMAL, NUMBER, STRING, BOOLEAN, STRICT_DATE, DATE_TIME, DATE, STRICT_TIME, VARIANT, CLASS, ANY`) with a parameter record for `DateTime(n)` and `Decimal(p,s)`. Serialize both in `PlanJson`, where the TS and Python hosts read them.
  - Compute a per-column `Carrier` once: `sealed interface Carrier permits Integral, Floating, Decimal(int scale), Text, Temporal, Time, Bool, Json, ListOf(Carrier), StructOf(List<Field>), MapOf(Carrier, Carrier), Tagged(List<Slot>), ByKind(PureKind)`.
  - Write one decode over a small source interface: `interface CellSource<C> { Object scalar(C); String text(C); Iterable<C> elements(C); ... }`, with JDBC, JSON and BSON implementations.
  - Let the writers switch on `Carrier` exhaustively.
- **Confidence:** verified.

### EXEC-2 [S1] Two runners for "steps on one session", with duplicated orchestration, diverging rules and a lossy re-encoding of the test plan
- **Where:**
  - the two pre-passes: `exec/PlanRunner.java:378-475` (`sequence`) vs `exec/ProgramRunner.java:355-437` (`shapeOf`);
  - the brackets: `PlanRunner.java:453-473` vs `ProgramRunner.java:459-483` (`bracketed`);
  - the dispatch: `PlanRunner.java:499-516` (`steps`) vs `ProgramRunner.java:514-571` (`runStep`);
  - the re-encoding: `ProgramRunner.java:64` (`Callback(int id, ...)`), `testexec/TestRun.java:76-117`;
  - the recorded drift: `JdbcStatements.java:338-345`.
- **What:**
  - **Two near-identical shape analyses.** Each walks the steps to find the one target, whether the steps write, the strictest snapshot, the result count, and a write count named like a parameter. Both repeat the `UncheckedIOException` tunnel. `ProgramRunner` reuses `PlanRunner`'s per-step helpers but not the orchestration.
  - **The rules differ:**
    - an `Effect` is a change in `sequence` (`:417-420`) but not in `shapeOf` ("an effect commits itself", `:348`);
    - a sequence needs exactly one result step (`:449`), a program at most one (`:429`);
    - a sequence always opens a transaction, even for reads (`:462-470`, contrary to its own javadoc at `:373-374`: "where a step changes the database"), while a program opens none for reads (`:476-481`);
    - an effect is a plain `effect` in one runner and the abort-bracketed `script` in the other.
  - **One drift already had to be reconciled by a ruling:** "after the correctness audit found the two runners answering otherwise" (`JdbcStatements.java:343-345`).
  - **`TestRun.program` re-encodes the typed `TestPlan` steps lossily.** It turns them into `Node` or `Callback(int id)`; the handler then looks the step up again by index and switches on its type, with two impossible arms (`TestRun.java:105-117`).
  - **`programRun` duplicates its open-bracket-close block** for the one-session and two-session cases (`ProgramRunner.java:219-233` vs `238-249`; jscpd flags it).
- **Why it matters:** every change to transaction, snapshot or routing semantics must be made twice and kept consistent by hand. That sits against G1 ("one runner"), and a drift already happened. The `int` index is an untyped join between two step lists.
- **Expert alternative:**
  - One step runner: `StepRunner.run(List<Step> steps, Bracket bracket, Callbacks<T>)`, where `Bracket` is computed once by one `Shape.of(steps)` and the product `Sequence` is a program with no callbacks.
  - Where the rules really must differ, state them as a `TransactionPolicy` value (`ProductWrite`, `TestBody`), not as two code paths.
  - Make the callback generic: `record Callback<T>(T payload, List<Node> declared, StatementOrigin origin)` and `interface Callbacks<T> { void step(T payload, OpenProgram open); }`, so the handler receives the typed `TestPlan.Verdicts`/`Branch` directly.
- **Confidence:** the duplication and the divergences are verified; whether each divergence is intended is suspected (some are documented).

### EXEC-3 [S1] Session acquisition: five overlapping abstractions, three parallel opening ladders, process-global caches with no owner
- **Where:**
  - the sources: `exec/PlanSessions.java:27` (sealed `Source` with 8 variants, two of them pure forwarders: `Admitting` `:53-68` and `Marked` `:76-91`), `exec/SessionSource.java:25-58` (5 more variants), `Sessions.Opener` (`Sessions.java:352`), `Sessions.Opening` (`:503-518`);
  - the ladders: `PlanSessions.java:296-336` (`openShared`), `:340-357` (`openOwn`), `:386-435` (`projectDatabase`);
  - the global state: `PlanSessions.java:94, 97, 102, 367`, `SystemDatabase.java:49`, `H2Connector.java:49`;
  - the callers: `ProductRun.java:73-120`, `Execution.java:43-45`.
- **What:**
  - **A run takes two sources of sessions at once.** `SessionSource` is "where the answer's session comes from" and `PlanSessions.Source` is "where each statement's session comes from". `ProductRun` then special-cases the first by subtype (`sessions instanceof SessionSource.Given`, `:79`; `instanceof SessionSource.ProjectDatabases`, `:104`).
  - **Three switches walk the same tree** (Database {System, Platform, Declared} → connector.ownSessions → Opening {Held, Named, Url}). They differ only in policy: which `HandleStore`, which key, and whether the "make" step is `setUpOpened` or `made`.
  - **The same lookup and refusal are repeated.** `target.database()` → project is switched out three times (`:213-217, 389-393, 474-478`). Four refusal helpers overlap (`refuseUnrun :210`, `refuseLoads :359`, `refuseRefused :473`, `refuseFill :487`).
  - **Sessions are cached in static `HandleStore`s with no close or clear**, so there is no lifecycle owner for a server or a test.
  - **"Made once, broken if it failed part-way" is written twice:** with `boolean[] state = {false, false}` indexed `[0]`/`[1]` and `synchronized (state)` (`PlanSessions.java:250-269`), and with mutable `made`/`broken` fields (`SystemDatabase.java:59-60, 142-155`).
- **Why it matters:**
  - Understanding which database a statement runs on means tracing `SessionSource`, then `PlanSessions.Source` (possibly wrapped), then `openShared`/`projectDatabase`, then `Sessions.openX`, then a connector.
  - A new opening kind or keep policy has to be added to three ladders.
  - Process-global connection caches leak across tests and cannot be shut down.
- **Expert alternative:**
  - Use one `SessionProvider` interface (`open(Target)`, `openToChange(Target)`), implemented by an instance-owned `DatabasePool` (holding the three stores, `AutoCloseable`) and by `Given(Session)`. Carry the mark and the admission as plain fields of a `RunContext`, not as decorators.
  - Write one `open(Target, KeepPolicy policy)` where `KeepPolicy` supplies the store, the key and the make step.
  - Model the made/broken state machine once: `enum Made { FRESH, MAKING, MADE, BROKEN }`.
  - Give `ExecutionPlan.Database` a `project()` accessor in place of the three switches.
- **Confidence:** verified.

### EXEC-4 [S2] `PlanRunner` is a 1,209-line bag of package-private statics; out-parameters and one-element arrays
- **Where:** `exec/PlanRunner.java` (whole file); `:104, 199, 221` (one-element arrays); `:355-358` (`Held`); `:1043-1045` (`Ask`, returns `void`); repeated fragments at `:781-787` vs `:1052-1056`, `:832-839` vs `:1063-1066` and `:892-895`, and the replay loop at `:121-125, 129-133, 160-164, 345-349`.
- **What:**
  - **One class holds ten concerns:** the public run entries, the sequence orchestration, the keyed expansion, load delivery with file staging, identity verification, constant decoding, stamp binding, source admission, session opening with the session-statement memo, and the date-time text the JDBC binder uses (`dateTimeText`, `:1194`, called only from `JdbcStatements.java:705`).
  - **About 20 package-private statics** are called from `ProgramRunner`, `PlanSessions` and `SystemDatabase` (for example `systemStore` `:150`, used only by `ProgramRunner.java:398`).
  - **Results come back through out-parameters.** `Streamed[] streamed = {null}`, `long[] count = {0}` and `boolean[] rows = {false}` exist because `Ask.of` returns `void`. Steps fill a mutable `Held` out-parameter (`constant`, `result`, `heldSink`).
  - **The same fragment is pasted where a helper exists:** the null-type loop, the slot-values loop, and "feed held rows through a `Streamed`".
  - **`Streamed` duplicates `Batcher`'s started state** (`:287-310` vs `Batcher.java:255-272`).
  - **`deliver` nests five levels deep** (for, switch, switch, try-with-resources, try/catch; `:630-716`).
- **Why it matters:** the class is the centre of the runtime, so it changes in almost every task (52 commits touched it in the series). Its implicit contracts with three other classes make each change risky, and new readers cannot see where one concern ends.
- **Expert alternative:**
  - Split it into `LoadDelivery` (seed, deliver, verifyIdentity, establish), `KeyedRun` (expand, keyedOn), `SessionOpening` (`opened`), `RunValues` (admitSources, executionsBound, valuesOf, value) and a slim `PlanRunner` (entries and dispatch).
  - Make `send` generic: `<T> T send(Sql, Map<...>, Source, Ask<T>)` with `T of(Statements, PlannedSql, List<Checked>)`.
  - Have steps return their `Rows`; drop `Held`.
  - Move `dateTimeText` into `JdbcStatements`.
- **Confidence:** verified.

### EXEC-5 [S2] Two JDBC transaction implementations, and two bracket APIs whose exception contracts force tunnelling
- **Where:** `exec/Sessions.java:412-443` (`inTransaction`) and `:448-471` (`beginTurn`/`endTurn`); `exec/JdbcStatements.java:473-546` (`inSnapshot`/`restore`); `exec/Statements.java:50` (`inTransaction(Supplier<T>)`) vs `:128` (`inSnapshot(Snapshot, Work throws IOException)`); the tunnels at `PlanRunner.java:462-473` and `ProgramRunner.java:464-475`.
- **What:**
  - **Two copies of the same protocol.** `Sessions.inTransaction` and `JdbcStatements.inSnapshot` each implement "own a transaction on this connection": autocommit off, `ownTransactionBegan`, work, commit, rollback with the memo cleared, restore, `ownTransactionEnded`. They differ in small ways: the first sets autocommit to `true` rather than restoring it; only the second sets isolation; the catch sets differ.
  - **The bracket APIs disagree on checked exceptions.** `inTransaction` takes a `Supplier` (no checked exceptions) and `inSnapshot` a `Work` that throws `IOException`, so both runners wrap `IOException` in `UncheckedIOException` and unwrap it again.
- **Why it matters:** the transaction protocol is subtle (the memo is cleared before the rollback, isolation is restored after it, and errors are suppressed). With two copies, fixing a bug in one leaves the other.
- **Expert alternative:** one `static <T, X extends Exception> T inTransaction(Connection c, SessionState s, Isolation iso, ThrowingSupplier<T, X> work) throws X`, where `Isolation` is `{DATABASE_DEFAULT, REPEATABLE_READ, SERIALIZABLE}`. `Statements` exposes `transaction(Isolation, ThrowingSupplier)` and nothing else, and the catalog turn reuses its begin and end.
- **Confidence:** verified.

### EXEC-6 [S2] The `Statements` port is JDBC-shaped and fat; a decorator exists only to carry the census label, paired by hand at 17 sites
- **Where:** `exec/Statements.java:24-223`; `core/src/main/mongodb/com/legend/exec/MongoDbStatements.java:87, 98, 105, 112, 121, 128, 137, 145`; `Statements.java:145-222` (`mark()` and the `Marked` record); `exec/StatementOrigin.java:73-93`.
- **What:**
  - **The port has 12 methods, and MongoDB refuses 8 of them by name.** It includes `query(..., Rows)` (one text per row), whose only caller drops every row (`PlanRunner.java:536`), and `describe`, which only dead code reaches (EXEC-9).
  - **A decorator forwards every method just to carry a label.** `Marked` forwards all 12 methods to carry a `StatementOrigin`, which is used only to label the census.
  - **Counting is paired with sending by hand.** Every send site in `PlanRunner` and `JdbcStatements` must call `StatementOrigin.sent(st.mark(), text)` before sending (17 sites).
  - **`StatementOrigin.sent` mixes concerns:** a wall check, census counters, a stderr SQL dump, and a read of the `legend.test.current` system property (`:90`) inside a product enum.
- **Why it matters:** every new port method must be added to `Marked`, to `JdbcStatements` and to `MongoDbStatements`, the last usually as a refusal. A send site that forgets its `sent` call silently under-counts.
- **Expert alternative:**
  - Narrow the port to what every session does: `check`, `execute`, `update`, `query(stmt, slots, values, ColumnSource, Cells)` (where `ColumnSource` is `Declared(List<Column>)` or `Reported`), `transaction(...)`.
  - Put the JDBC-only capabilities (file reads, schema reads) on a separate `JdbcCapabilities` interface the runner asks for.
  - Count inside the one send choke point of each implementation, with the origin passed as an argument; delete `Marked`.
- **Confidence:** verified.

### EXEC-7 [S2] `JdbcStatements.send` trades static types for a single choke point: an enum mode, a sealed answer and four runtime downcasts
- **Where:** `exec/JdbcStatements.java:551-661` (`Send`, `Answered`, `send`, `rowsOf`, `countOf`, `executedOf`, `describedOf`), `:625-627` (two "not reached" throws); the `held` overloads at `:132, 144, 156, 164`; the duplicated bodies at `:391-437` and `:441-466`; the parameter probe at `:319-321`.
- **What:**
  - **The mode is checked at run time, not by types.** The send mode is an enum and the answer a sealed type, and each caller downcasts the answer and throws "a query's send answered …" when it mismatches. The pairing of mode and answer is enforced at run time, to keep every JDBC call in one method for an ArchUnit rule (`jdbcSendsAtOneChokePoint`).
  - **`held` has four overloads.** One, `held(Connection, RaiseReadBack, SQLException)`, builds a throwaway `new SessionState()` and is called only from tests (`RaiseReadBackTest.java:134-159`).
  - **`rows` and `reportedRows` repeat their driver-null check and row loop** (jscpd).
  - **`readsFile` decides how to bind from the driver's parameter metadata** (`getParameterCount() == 1`) rather than from the plan.
- **Why it matters:** a guard shaped the API, and the shape costs downcasts and "not reached" branches that the compiler cannot check.
- **Expert alternative:** keep one small private class as the choke point but give it typed methods:
  - `<T> T query(Connection, PlannedSql, Bind, ResultSetReader<T>)`;
  - `long update(Connection, PlannedSql, Bind)`;
  - `boolean execute(Connection, PlannedSql)`;
  - `@Nullable ResultSetMetaData describe(Connection, PlannedSql)`.

  An ArchUnit rule can still require that only this class calls `prepareStatement`/`execute*`. Beyond that:
  - give `held` one signature;
  - have the plan's `Reads` delivery say whether it binds the path.
- **Confidence:** verified.

### EXEC-8 [S2] The plan type admits states exec must refuse at run time: about 16 impossible-arm throws
- **Where:** `executionplan/ExecutionPlan.java:26, 186` (`ExecutionPlan(params, Node root, boolean writes)`, where `root` may be any of the 8 node kinds); in exec: `PlanRunner.java:135-142` (four root kinds refused), `:421, 436, 438, 513`, `ProgramRunner.java:53-55, 69-72, 306, 373, 415, 569`, `ProductRun.java:136`, `JdbcStatements.java:625-627`, `result/PureV1Writer.java:198-200`.
- **What:**
  - A root of `Update`, `Check`, `Effect` or `Load`, a `Sequence` nested in a `Sequence` or a program, and a callback that declares a non-result are all representable, and exec throws `IllegalStateException` for each.
  - `writes` is a boolean beside a root whose shape already implies it.
- **Why it matters:** each consumer of a plan (three host languages) re-implements the same refusals; the compiler cannot help.
- **Expert alternative:** split the sealed hierarchy in `executionplan`:
  - `sealed interface Answer permits Sequence, TdsResult, Keyed, Constant` for the root;
  - `sealed interface Step permits Update, Check, Effect, Load, TdsResult, Keyed, Constant` for a sequence's steps, so a sequence holds no sequence;
  - `writes()` derived from the steps.

  This is cross-unit (the plan records), but exec carries the cost.
- **Confidence:** verified.

### EXEC-9 [S2] Dead and vestigial API
- **Where:** `exec/PlanRunner.java:191-206` (`update(Update, Source)`), `:210-226` (`effect(Effect, Source)`), `:234-253` (`columns`, `describe`, `schema`); `exec/Statements.java:39-40, 71` (`query` with text rows, `describe`); `JdbcStatements.java:290-301, 567-569, 655-661`; `SessionState.java:21, 93` (`Set<Record>`).
- **What:**
  - **Four public `PlanRunner` entries have no caller** in the repository, main or test (checked with grep across the repo). Through them, `Statements.describe`, `JdbcStatements.describe`, `Send.DESCRIBE`, `Answered.Described` and `describedOf` are reachable only from dead code.
  - **`Statements.query`'s per-row text callback is vestigial:** it is left over from the removed text-rows path, and its one caller passes `row -> { }`.
  - **`SessionState` keys are `java.lang.Record`.** The only production key is a private `SeedKey(String)`, a leftover of the deleted driver's value keys.
- **Why it matters:** a reader cannot tell live paths from leftovers, and dead methods keep the port wide (EXEC-6).
- **Expert alternative:**
  - Delete the four entries and the `describe` chain.
  - Rename `query` to `check(stmt, slots, values)` (run and drain).
  - Key the memo by the load's identity `String`.
- **Confidence:** verified.

### EXEC-10 [S2] A second hand-written JSON parser
- **Where:** `exec/NonFiniteJson.java:21-136`, called from `JdbcDecode.java:297`.
- **What:** a 136-line recursive-descent JSON parser, written to accept `NaN`, `Infinity` and `-Infinity`, beside `com.legend.json.Json`. It hands each scalar and string back to `Json.parse(src.substring(...))` (`:114, 128`), so it allocates a substring and runs a second parse per token.
- **Why it matters:** two JSON readers can drift on escapes, numbers and whitespace, and the per-token re-parse is wasteful.
- **Expert alternative:** an option on the one parser, `Json.parse(text, Json.Leniency.NON_FINITE_NUMBERS)`, in the json module.
- **Confidence:** verified.

### EXEC-11 [S2] Error handling: one exception type for plan defects, user refusals and bugs; two ways to wrap `SQLException`
- **Where:**
  - the wrapping and its claim: `exec/Sessions.java:401, 419, 440, 453, 469, 497`, `JdbcStatements.java:354` vs `exec/RaisedErrors.java:153-166`;
  - the sentinel parsing: `RaisedErrors.java:55-71` vs `:123-151`;
  - the swallowed error: `RaisedErrors.java:146`.
- **What:**
  - **Exception types:** exec and result throw `IllegalStateException` 141 times, `NotImplementedException` 28, `IllegalArgumentException` 28 and `DataError` 16. `IllegalStateException` covers malformed plans ("a sequence of 2 result steps"), configuration refusals ("connection 'x' is a database reached by a URL, and the plan loads data into it") and internal bugs alike.
  - **Two ways to wrap `SQLException`.** `RaisedErrors.dataError` calls itself "the one class that constructs from a SQLException", yet `Sessions` builds `new DataError(String.valueOf(e.getMessage()), e)` six more times, which skips the raise unwrapping.
  - **The sentinel parsing is written twice** (`unwrap(String)` and `unwrapped(SQLException)`).
  - **A malformed position prefix is silently ignored** (`catch (NumberFormatException ignored)`).
- **Why it matters:** with plans arriving as data from other hosts (G11), a caller cannot tell "this plan is malformed" from "exec has a bug" without matching message text, which the tests in fact do (EXEC-17).
- **Expert alternative:**
  - Add a `PlanRejected extends LegendException` for invalid plan data, kept apart from `IllegalStateException` (bugs) and `DataError` (database).
  - Route every `SQLException` through `RaisedErrors.dataError`.
  - Use one `Optional<Raise> parse(String)` behind both unwrap methods.
- **Confidence:** verified.

### EXEC-12 [S2] `RunScope`: simple and readable, sound as a single-thread tripwire, but with a shared close token and conflated scopes
- **Where:** `base/src/main/java/com/legend/base/RunScope.java:35-45, 65-96, 141-164, 229-244`; call sites: 64 `planning(..)`, 15 `opening(..)` and 14 `statement(..)` in `core/src/main`.
- **What:**
  - **Mechanism:** thread-local counters (`int[]` depth, `int[]` judging, a name slot, an execution id). Planning seams assert "not in a run"; session seams assert "in a run".
  - **The close token is shared.** `enter()` returns one shared singleton `SCOPE`, so a double `close()` drives depth negative and silently disables the wall (`> 0` checks).
  - **Two scopes share one counter and one type.** `judging()` and `callingBack()` both bump `JUDGING` and both return the type `Judging`, so `busy()` must word its message for both.
  - **`base` hard-codes upper-layer class names** (`AnsiSqlRenderer.`, `Lowerer.`, `SpecCompiler.`) to filter stack frames.
  - **The javadoc is stale:** it says "three scopes … and four rules" (`:6`) and then lists five rules.
  - **It cannot see what it was not told about.** The wall covers only the thread and the call sites that opted in; work handed to another thread or a new seam without a `planning("…")` call is invisible. The Bazel layer split (`//core:exec` depends on no planner) carries the static half of G4.
- **Why it matters:** the guarantee is cooperative: about 93 string-labelled assertions, each a place to forget. The shared token makes a misuse corrupt the state instead of failing.
- **Expert alternative:**
  - Return a fresh token per entry, `final class Scope implements AutoCloseable { private boolean closed; }`, that refuses a second close.
  - Keep a `Deque<Phase>` with `enum Phase { PLANNING, RUNNING, JUDGING, CALLING_BACK }` per thread.
  - Pass the filter prefixes in from the caller.
  - Treat the runtime wall as a debug assertion and lean on the build-level dependency split, which already gives the compile-time proof.
- **Confidence:** verified.

### EXEC-13 [S2] `PureV1Writer` is two writers in one class; `ColumnType` admits invalid states
- **Where:** `result/PureV1Writer.java:43-58, 198-200, 216, 243, 282`; `result/Output.java:52-87`.
- **What:**
  - **Two writers share one class.** The relation writer and the graph-fetch writer branch on `shape == OBJECTS` in every method (`columns`, `batch`, `end`), and the objects writer is built with an envelope lambda that throws "not reached".
  - **`ColumnType` is three nullables validated in its constructor** (spelled, or refused).
  - **`Output.writerFor` duplicates its Engine and Arrow arms,** and calls `PureV1Writer.envelope(...)` only for the refusal it may raise, discarding the result (`:83`).
- **Why it matters:** each new answer shape adds a branch to every method, and the "validate by building and discarding" idiom hides intent.
- **Expert alternative:**
  - Make it `sealed interface PureV1Writer extends ResultSink permits RelationWriter, ObjectsWriter`.
  - Make `ColumnType` `sealed interface ColumnType permits Spelled(String type, String relational), Refused(String why)`.
  - Add a `Facts.check(columns)` that states the validation.
- **Confidence:** verified.

### EXEC-14 [S2] Session state: five concurrency mechanisms, public mutators on exec-internal state, instanceof walkers over its own subclasses
- **Where:**
  - the state: `exec/SessionState.java:21, 41, 48, 53, 59, 93-161`;
  - the subclasses and walkers: `exec/Sessions.java:47-71, 111-309` (`Jdbc`, `View`, `Lazy`, `Locked`), `:262-268` (`locked`), `:322-342` (`closed`);
  - single-threaded atomics: `ProgramRunner.java:612`, `Sessions.java:191-199`.
- **What:**
  - **Five concurrency mechanisms in one small class.** `SessionState` mixes `synchronized` methods, `AtomicBoolean`, `AtomicInteger`, `volatile` and `AtomicReference<Optional<String>>`, yet a kept session serves one request at a time (`Sessions.locked`).
  - **Atomics with no concurrency.** `AtomicReference` holds state in the single-threaded `UserBeside`, and appears inside an already `synchronized` `Lazy`.
  - **Internal state is public.** `Session.state()` and every `SessionState` mutator are public, but nothing outside the package calls them (grep).
  - **The session decorators are walked by hand.** `locked()` unwraps `View` but not `Locked`, and `closed()` unwraps both by explicit `instanceof`, so every new decorator must be added to each walker.
- **Why it matters:** readers cannot tell what is actually shared, the public mutators let any holder corrupt the memo, and the walkers are an easy place for a new subclass to break invariants.
- **Expert alternative:**
  - State the confinement: one session, one request, guarded by the session lock, and use plain fields.
  - Make `state()` and the mutators package-private.
  - Add `abstract Session root()` (the owning session) and use it in `locked` and `closed`.
- **Confidence:** verified, except the latent `locked`-of-`Locked` wrong-lock case, which is suspected and not reachable today.

### EXEC-15 [S3] Comments narrate process history instead of explaining the code (pattern)
- **Where:** across the unit, 2,539 comment lines in 11,226. Three examples:
  - `PlanRunner.java:88`: `// the runtime wall (the deep audit, 2026-11-05): ...`;
  - `JdbcStatements.java:200` (and 9 more): `act();   // THE WALL's rule 3, per act (2026-11-06)`;
  - `H2Connector.java:33-43`: a "HISTORY:" paragraph about a removed constant.
- **What:**
  - **Counts:**
    - 327 dates, 197 ticket or audit codes (`J6`, `MUST-FIX 2`, `D70`, `S181` …), 103 "audit" mentions, 63 "until then …" clauses, 70 `docs/…` paths and 32 "ruling" mentions;
    - tombstones for deleted code (`DatabaseConnector.java:54, 66`, `StatementOrigin.java:50, 63, 67`).
  - **Stale or wrong comments:**
    - `PlanRunner.java:35-37`: "checks it is the database and server version …, writing a value's type where the statement leaves it to the value, H2's". No version check exists (`Sessions.java:385-391`), and type writing was deleted on 2026-11-09 per AGENTS.md.
    - `Statements.java:42`: an orphaned javadoc, "Where a query's rows go.", directly above `update`'s javadoc.
    - `RunScope.java:6`: "four rules" (five are listed).
    - `SessionState.java:11`: "What a body driver ESTABLISHED" (the body driver is deleted).
    - `JdbcStatements.java:37-40`: "never re-derived from the driver's product name", while `takesTransactions` does exactly that when `declared` is null (`:350-361`).
- **Why it matters:** a reader must filter history to find the contract, and the history goes stale (five verified instances).
- **Expert alternative:** say in javadoc what the code does and why, and leave history in commits and the design docs; delete the tombstones.
- **Confidence:** verified.

### EXEC-16 [S3] Inline fully-qualified names instead of imports (pattern)
- **Where:** 578 occurrences in the unit's code lines (top: `JdbcDecode` 66, `Sessions` 55, `JdbcStatements` 44, `PureV1Writer` 41). Examples:
  - `Sessions.java:555, 562, 573`: `com.legend.executionplan.ExecutionPlan.@com.legend.base.Nullable DatabaseSettings`;
  - `PlanRunner.java:646, 659`: `com.legend.error.DataError` and `java.util.Objects`, both already imported (`:7`, `:28`);
  - `PureV1Writer.java:67-73`: `java.util.Map`, `java.util.Collections`, `java.util.LinkedHashMap` inline.
- **Why it matters:** signatures double in length and wrap, which hides the real types.
- **Expert alternative:** imports; a static import for `Nullable`-heavy files.
- **Confidence:** verified.

### EXEC-17 [S3] Opaque names, vocabulary sprawl, positional booleans (pattern)
- **Where:**
  - participle method names: `JdbcStatements.held/heldBy/naming/failed/act` (`:62-196`), `PlanRunner.opened` (`:1091`), `Output.told` (`:110`), `PureV1Writer.ran` (`:188`);
  - vocabulary for loads: seed, load, establish, register, made, setUp (`PlanRunner.java:584`, `PlanSessions.java:463, 497`);
  - vocabulary for session sources: `SessionSource`, `PlanSessions.Source`, `Sessions.Opener`, `Sessions.Opening`;
  - positional booleans: `DatabaseConnector.Decoding(boolean x5)` built positionally (`DuckDbConnector.java:75`: `new Decoding(true, false, false, true, true)`; also `SnowflakeConnector.java:58`, `TrinoConnector.java:97`).
- **Why it matters:** names do not say what a method does (`held` reads a raise back and resets it; `naming` re-wraps an error under a payload label), and positional booleans invite silent swaps.
- **Expert alternative:**
  - Use verb names: `readBackRaise`, `labelPayloadFailure`, `prepareSession`, `requireExecution`.
  - Keep one term per concept.
  - Give `Decoding` a builder or `EnumSet<DriverQuirk>`.
- **Confidence:** verified.

### EXEC-18 [S3] Tests: behavioural and real, but coupled to internals and to message fragments (pattern)
- **Where:**
  - internals: `core/src/test/java/com/legend/exec/DataLoadsTest.java:102, 362` (call `PlanRunner.seed`/`opened` directly, inside a hand-entered `RunScope`);
  - message fragments: `ProgramRunnerTest.java:83` (`getMessage().contains("ONCE")`);
  - serialized text: `RaiseReadBackTest.java:87` (`json.contains("\"raiseReadBack\":{...}")`);
  - the 9 `Planted*.java` files (507 lines).
- **What:**
  - 47 `assertTrue(x.contains(...))` message-fragment assertions in the exec tests.
  - 32 hand-built `new ExecutionPlan.Target(db, List.of(), List.of(), null, null, null)` calls with six positional arguments.
  - Tests reach package-private statics, so the EXEC-4 split forces test rewrites.
  - The `Planted*` classes exist only to show ArchUnit rules fire (guard tests, not exec tests).
- **Why it matters:** refactors and message rewording break tests that do not test behaviour, and the repeated six-argument target construction obscures what each test sets up.
- **Expert alternative:**
  - A `Targets.platform()` / `Targets.on(type)` test builder.
  - Assertions on exception types (EXEC-11) rather than text.
  - Tests through the public `stream` and `establish` entries.
- **Confidence:** verified.

## Cross-unit observations
- **`ExecutionPlan` record design (planner/plan unit)** is the root cause of EXEC-1 and EXEC-8. `Column.slot` and `Column.type` are strings while `Column.store` is typed. `Node root` admits non-answers. `Target` has six positional fields, three of them nullable.
- **`testexec/TestRun.java:76-117`** re-encodes `TestPlan` steps into `ProgramRunner` steps with `int` ids (EXEC-2).
- **Four walkers each extract "the statements of a node"** on their own: `ExecutionPlan.stamps` (`ExecutionPlan.java:147`), `PlanRunner.statementsOf`, `ProgramRunner.shapeOf` and `ProductRun.home`. A `Node.statements()` on the sealed type would serve all of them.
- **The keyed expansion is implemented three times** (Java `PlanRunner.expand`, TS `engine-client/src/plan-runner.ts:667` `expandKeyed`, Python `python/legend_lite/_planned.py`). The shared conformance fixture is the right mitigation.
- **Guards shaped exec's API in places.** The single JDBC choke point (EXEC-7), the "one statement per door method" rule, and the planted fixtures. A narrower port plus package-private visibility would enforce most of this without contorting types.
- **`MongoDbDecode` and `MongoDbStatements`** (`core/src/main/mongodb/...`) live in package `com.legend.exec` but outside the listed sources. They are a third decoder and a port implementation that refuses 8 of 12 methods.

## Measurements
- **Unit size:** 11,226 lines of main code (exec, result, doors, RunScope) and 8,361 lines of exec tests in 52 files; the MongoDB exec sources add 844 lines. `cat … | wc -l`.
- **lizard** (`python3 -m lizard -l java core/src/main/java/com/legend/exec core/src/main/java/com/legend/result`): 663 functions, average NLOC 9.2, average CCN 3.2, 14 warnings. Hot spots:

  | Function | CCN | NLOC |
  |---|---|---|
  | `RowDecode.cell` | 58 | 130 |
  | `JdbcDecode.byPureType` | 47 | 78 |
  | `JdbcStatements.bind` | 27 | 64 |
  | `JdbcDecode.decode` | 27 | 60 |
  | `PlanRunner.sequence` | 26 | 88 |
  | `PlanRunner.keyedOn` | 22 | 80 |
  | `ProgramRunner.shapeOf` | 22 | 79 |
  | `ArrowWriter.encoder` | 21 | 44 |
  | `PlanRunner.deliver` | 18 | 67 |
  | `NonFiniteJson.value` | 18 | 70 |
- **jscpd** (`--min-lines 6 --min-tokens 60`, exec and result): 10 clones, 125 duplicated lines (1.35%). Among them: `ProgramRunner` 221-229/241-250, `JdbcStatements` 392-402/442-452, `Sessions` View/Locked, `DuckDb`/`Sqlite` connectors.
- **Stringly type dispatch** (grep over exec, result and the MongoDB exec):
  - slot-word ladders (`case "BIGINT"|"INTEGER"`): 5 in 4 files;
  - Pure-type name matches (`equals("Integer")`, `case "Float"`, …): 61 in 8 files;
  - `"TAGGED"` checks: 10;
  - slot-grammar parse calls (`slotWord`, `topLevel`, `typeArguments`, `precisionAndScale`, `declaredScale`): 28.
- **Comment history markers** (grep over the unit): 327 dates, 197 ticket codes, 103 "audit", 63 "until then", 70 `docs/` paths, 32 "ruling".
- **Inline fully-qualified names on code lines:** 578.
- **Exceptions thrown** in exec and result: `IllegalStateException` 141, `NotImplementedException` 28, `IllegalArgumentException` 28, `DataError` 16, `UncheckedIOException` 3.
- **`RunScope` call sites in core main:** `planning` 64, `opening` 15, `statement` 14, `enter` 7, `plans` 6.
- **`StatementOrigin.sent` manual pairings:** 17 (`PlanRunner` 13, `JdbcStatements` 4).
- **Dead `PlanRunner` public entries** (repo-wide grep, main and test): `columns`, `describe`, `effect(Effect, Source)` and `update(Update, Source)` all have 0 callers.
- **Commits touching each file in the series range** (`git log 770acc55^1..4dfbb5b7`): `PlanRunner` 52, `JdbcStatements` 42, `PlanSessions` 28, `ProgramRunner` 18, `Sessions` 11.
