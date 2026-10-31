# M3 brief draft (B12) -- from the planning agent, 2026-10-31 (read at d9dc669a). Condensed by the main session.

## The ten items
1. The door (B6, N1): PlanMaker.plan = the QUERY door for every product caller; a new public TEST-door entry plans any body
   through program (the 2.B "gate" row no step built). The query door scans the typed body once (before query.writes()
   routes it, PlanMaker.java:58) and refuses by name at the first statement holding: an effect; a from()'s inline seeds or
   a named runtime's; an eager run; a verdict; an assertError. Reuse StatementEffects.containsEffect/callsVerdict
   (compiler/spec/StatementEffects.java:31,:121), ExecutionContext.reader() (as Compiler.programFacts, Compiler.java:729-760),
   LetPlanner.kind. ArchUnit: only com.legend.driver calls the test door (no caller at M3).
2. One program state in PlanMaker.program (ProductStatements.prepare makes a fresh PlanEnv per statement, :59): planRows,
   queryLets across statements, frames, the minter, the door, the last Setup key, pending pre-steps.
3. Effects as Effect steps (R11, D34, decision 14; test door). Split Prepared.Effect by catalog row (NativeFn.java:920-979):
   DB effects -> statements by EffectPlanner.stringArgs/statements, each String arg folded by Literals.fold, computed arg
   refused by name (T9, M9; M0: 0 of 124,975); setUpDataSQLs -> a constant of its texts; print/println -> no step, a null
   constant when last, both doors (R18); connectionByElement -> a handle; EffectfulMap refused (T9, M9; 84/pass); an effect
   let -> Effect step with today's guard (BodyCompiler.java:296-302). Consecutive effect statements one script (segment rule
   BodyCompiler.java:120-125,:323-335; text BodyDriver.java:1402-1411). ExecutionPlan.Effect gains the statements, the
   bracketed form (SqlDialect.script), the abort (scriptAbort); the runner sends the bracket only where the session owns its
   transaction and the program opened none; on failure the abort first (BodyDriver.sendScript :1407-1416). The effect writer
   gets the dialect only after the runtime is decided (M2's road names none; EffectPlanner calls env.writtenIn()).
4. The transaction rule (B2, D42, MV10) -- needs a ruling (C1). Recommended: program opens one transaction only when a step
   is an Update (today's test road BodyDriver.java:68-80; upstream's autocommit executeInDb); an Effect inside an open
   bracket sends bare text; after each Effect, SessionState.markWriting() (as BodyCompiler.java:290) so a later Setup reseeds.
5. F11: the dialect's abort-on-error fact + savepoint/rollback-to/release texts; Verdicts gains an optional savepoint;
   PlanRunner.verdicts (:357-412) sets it before the fused statement when in a transaction, rolls back before the wrappers
   on rejection (:376-383), releases otherwise; VerdictBatch fills it. M0: 0 fused groups inside a body transaction. The
   planner emitting it waits for verdict steps on the program road (M6/M7).
6. Setup steps (D35, audit G2): new node ExecutionPlan.Setup(key, steps); emitted before a statement whose seed sources
   (BodyDriver.establishContexts :740-773: the from()s, their named runtimes, the program's runtime) name test data, only
   when the key differs from the last emitted or an Effect came since; steps CsvSeed.planned (:79-101); the runner skips a
   Setup whose key the session holds clean (SessionState :38-51), else runs under SEED and records the key. The test door's
   target carries no setup (JudgePlans.target :56-73) -- C6.
7. Frames (D15, B10): planner/ProgramFrames implements ResultEnvelopeSplice.Frames (:111-144): frame(name) -> the View;
   inlineExecute(ec, eager) -> FramePlanner.chain + activity rows, eager -> a Check step queued before the statement;
   relationalActivitySql -> PlanAllocations.activitySql. ProductStatements.prepare inlines with ResultEnvelopeSplice.hook.
   PlanMaker.program's let loop (:128-152) dispatches on LetPlanner.kind: FRAME -> frames, never lets; test door: an eager
   Check at the let (decision 1); ALIAS; STAGED as today; STAGED_GRID refused (T12); VALUE -> lets. A value-position
   execute(...) is a Check before the last statement plus the result step over its run chain. BodyDriver.frameReplaceEnv
   (:403-438) moves to the planner; the driver delegates.
8. The frame CTE (D31, MV9): a frame FramePlanner.cte plans (:170-226) lowered once at its let with the PRODUCT's
   configuration and the program's minter (not BodyPlanner.bare: identity lane, and writtenIn()); readers read frame_<let>,
   the definition attached by sql/FrameCtes.attach, aliases under AliasPrefix.frameBody (as VerdictBatch :196-204). Where:
   C3.
9. Activity SQL and the trace stamp (D47, D49): the activity SQL already plan data (PlanAllocations :280-295); the comment
   (:316) becomes plan data, the stamp the eager Check is sent under (C4). Today: the eager run's real stamp (host), a stamp
   reserved for the next statement (database) (BodyDriver :496-502).
10. Table renames as plan data (M2 carry-over): BodyPlanner.prepare never reads the from() context's
    postProcessors().tableReplace(); lowerAndPrepare applies only env.tableReplace() (an execute frame's). Merge the from()'s
    renames, loud on conflict, on both roads -- its own sub-step, measured and posted (C9).
Not in M3: verdict steps (M6); T8-T10 (M9); system database (M4: refused by name on the test door); D30.
Movement: product SQL for inline execute(...) reads (D15 refusals become plans); new named refusals; PlanJson 23 -> 24 and
the TS pin (it refuses setup by name; already refuses effect); test road unchanged except item 10.

## Sub-steps
M3a measure only (C_M3_MEASUREMENTS.md: transaction-scope changes per C1 option and rollbacks a later failure would cause;
seed sends inside an effect run; frames read once/twice per statement and program; activity comments read / "" recorded;
inline eager executes; system-store frames; staged grids; effectful runtime args; from() tableReplace counts; door refusals
in core tests and what the product answers an assert today; DuckDB/H2 abort-on-error + SAVEPOINT support).
M3b records + runner (Effect.script, Setup, Verdicts.savepoint, stamp field; PlanJson 24; TS refusals; program's
transaction rule, bracket, Setup memo, markWriting, F11 rung; VerdictBatch fills savepoint) -- no movement.
M3c the door + test-door entry + program state + ArchUnit -- refusal texts change (C8).
M3d effects on the test door -- census: every corpus setup program (BeforePackage, assert-free) planned whole, its Effect
texts vs the test road's, byte-identical expected.
M3e Setup steps -- census per statement.
M3f frames without CTE + activity stamp + the query door's inline lazy execute(...).values -- product SQL moves.
M3g the frame CTE in C3's form -- product SQL; MV9's would-be movement counted.
M3h the from() renames -- TEST-ROAD SQL CHANGES; posted (C9).
M3i census re-run, audit, records, checkpoint.

## Choices no ruling covers (C1-C12)
C1 transaction rule: B2's "a program that changes the database runs in one transaction, effect scripts inside it with the
   dialect's bracket" cannot hold literally (DuckDB/Postgres scripts open BEGIN..COMMIT, cannot nest in the program's JDBC
   transaction) and upstream runs executeInDb with autocommit (legend-pure ExecuteInDb.java:169-173; RelationalGen.java:111,
   :220, :382). A (recommended): today's rule, only Updates bracket, effects commit themselves, a script carries its own
   bracket where the session owns its transaction (MV10 = 0). B: every change brackets, scripts bare (moves failure state;
   H2/MySQL/MariaDB DDL commits implicitly; ClickHouse/BigQuery no general transactions).
C2 Setup key + generation memo (process cache keyed by the sources' value, never model identity; key spelling).
C3 MV9's form: CTE wherever FramePlanner.cte plans one (both modes; host text moves) vs CTE only where read more than once
   (B10's words; database single reads move to pasted); "more than once" per statement or per program (M7 fusion changes it).
C4 the trace stamp: S1 (rec.) planner mints the comment, the eager Check carries it, runner sends via the trace's reserve
   (plans non-deterministic, census normalizes); S2 a slot bound at run time like COUNT; S3 a deterministic per-plan UUID
   (diverges from upstream; register row). Also a frame never run: host "" vs database reserved stamp -- one rule needed.
C5 savepoints: dialect texts sent as effects (no JDBC census move) vs JDBC setSavepoint; a dialect that aborts on error with
   no savepoint: no fused statement in a transaction, or refuse.
C6 the test door's target: empty setup (judges' rule) vs the product's declared setup (double seeding on a shared session).
C7 where the test-only script facts live (ledger texts, message decoration, failing-statement index): plan fields vs the
   test door's sink in the driver; and B4's library does not exist until M6: land in planner/ (ships into wasm/native via
   //core:plan_side) and move at M6, or pull B4 forward.
C8 the new door refusal texts: output spellings under R12, posted case by case?
C9 M3h's test-road movement: no approved MV class; land with a posting, or defer the test-road half to M5.
C10 loadCsvToDbTable's CSV: a test resource read at planning: hand the resolver to the planner, or refuse at M3.
C11 an effect as the program's last statement: Effect + constant (true for dropAndCreate*, null otherwise, as today's host
    rows), or refused like a handle.
C12 the eager run on the query door: refuse a let-bound execute by name (rec.; D15 then serves the product only through
    inline lazy reads) vs plan it with no eager step (drops Pure's strict let evaluation, breaks NO FALLBACKS). Still to
    read: engine executionPlan_generation.pure on whether a let r = execute(...) inside a planned lambda is evaluated.

## Mis-scoped as written
1 B2's transaction sentence (C1). 2 MV9/MV10 tagged M3 but the test road switches at M5: countable only at M3.
3 2.B says effects/seeds are planned by the TEST library (B4, built at M6) while decision 14 calls consecutive effects a
product feature; M3 has no library (C7). 4 B6 refuses the eager run on the product door while 2.B lists D15 as a product
feature; on the query door D15 reduces to inline lazy reads (C12). 5 T12 staged raw grids has no step in §7; refuse at M3
and give it a step (a plan cannot express "drop however the body ends"; a JVM-wide table counter). 6 "proved by the product
planning what the test road plans" needs a per-statement entry (B1, M5) and step origins (B5, M6): only assert-free
programs (setup programs) pair whole at M3. 7 the planner cannot emit F11 until verdict steps reach the program road.
8 §5's guard table still names PlanJson.VERSION (22).

## Upstream read
executeInDb autocommit on (legend-pure ExecuteInDb.java:169-173; compiled RelationalGen.java:111,:220,:382); the activity
comment minted per execution (core_relational relationalMappingExecution.pure:475-478,:491-492; recorded :521,:575; only
regex-matched by tests functions/tests/testSimple.pure:68); a connection's post-processors apply to any query on it
(postprocessor/postProcessor.pure:66-80). Still to read: executionPlan_generation.pure for C12.

## Risks
PlanRunner.program brackets on any Effect (:178-181, :294-306) unlike the test road (BodyDriver :68-80): naive wiring moves
MV10 everywhere. Seeds re-run inside an effect run before that run's script (establishContexts before collectEffect;
collectEffect marks dirty): Setup steps must precede the script step. Eager-run ordering (after Setup and any pending
script; an inline eager Check before its statement; at M5/M10 each eager Check closes a fused group; MV1 ran without them;
159 frame lets after a deferred verdict per pass). The dialect-free product road: FramePlanner.cte -> bare ->
writtenIn(), EffectPlanner too, throw. One env per statement loses frame plan rows. H2 effect DDL commits the
transaction. Multi-statement scripts per JDBC execute depend on the driver (MySQL allowMultiQueries, ClickHouse, Trino).
System store on the test door gives a second target: refuse until M4. Stale javadoc UserCallInliner.java:1489-1491.

## Guards that move
PlanJson.VERSION + plan-runner.ts PLAN_VERSION; JavaEvalLedgerTest pins (PlanMaker, ProductStatements, FramePlanner,
BodyPlanner, PlanRunner, new ProgramFrames); NativeDispatchTest pin (planner's effect dispatch = catalog EFFECT rows); a new
ArchUnit test-door rule; execReachesNoPlanning / theRunnerIsModelFree stay green; HarnessDisciplineTest if driver sites move.
