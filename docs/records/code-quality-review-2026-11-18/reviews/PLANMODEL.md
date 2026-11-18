# PLANMODEL: verdict MIXED

## Summary
The core of the plan model is sound. Plans are immutable records under sealed roots (`Node`, `Binding`, `LoadStep`, `Database`, `TestStep`, `Side`, `HostJudgment`). Constructors check invariants and copy their inputs defensively. `PlannedSql` turns "exec sends only plan text" into a compile-time property. A reflective guard proves the plan holds only data. `TestPlan` reuses the product's nodes by composition, so it is not a parallel hierarchy.

The debt is around that core:
- **Serialization.** Five hand-maintained codecs (about 4,540 lines) use three tag conventions and three null conventions, and they rewrite untyped JSON by sniffing keys. One of them is generator output whose generator was thrown away.
- **Addressing.** A full `Target` is embedded in every statement, and each `Load` points back to its holder's target. Hoisting machinery and six-field equality checks exist only to manage that.
- **Strings inside a typed plan.** Several typed facts are spelled as strings and parsed back at run.
- **Dead cargo.** Every statement carries the planner's whole MIR tree, and no runtime reads it.
- **Package cohesion.** The test-plan library holds judging behaviour and compatibility grammar beside its data.
- **Comments.** Javadoc is saturated with dates, audit labels and doc paths (236 dates and 68 doc paths in 36 files), and some of it is stale or wrong.

A new engineer can learn what a plan is from the sealed types. Learning which combinations are valid means reading constructor checks, prose and runtime refusals.

MIXED, at the weaker end of MIXED: the type core would pass a demanding review, but the serialization layer and the comments would not.

## Strengths
- **Closed sum types throughout, with exhaustive switches.** `ExecutionPlan.Node` permits eight node kinds (core/src/main/java/com/legend/executionplan/ExecutionPlan.java:186); `Binding` (:870), `LoadStep` (:1153) and `Database` (:1105) are sealed too, as are `TestPlan.TestStep` (core/src/main/java/com/legend/testexecutionplan/TestPlan.java:74), `TestPlan.Side` (:388) and the 23-variant `HostJudgment` (core/src/main/java/com/legend/testexecutionplan/HostJudgment.java:25). Every writer switches over them exhaustively, so a new variant fails to compile until it is encoded (PlanJson.java:513-599, MirJson.java:281-483).
- **Invariants enforced at construction, with defensive copies and messages that name the problem.**
  - `Keyed` (ExecutionPlan.java:543-592), `Region.check` (:833-857), the bundle consistency of `Load` (:402-438) and the payload content of `Sql` (:728-748).
  - `DataRef` refuses machine paths (core/src/main/java/com/legend/executionplan/DataRef.java:148-162).
  - `Bound`, `Held` and `Attach` names are pattern-checked, so no secret can sit in a plan (ExecutionPlan.java:1321, :1344).
  - Maps are copied into `TreeMap`s, so equality and serialization are deterministic.
- **`PlannedSql` is a small, well-aimed type** (core/src/main/java/com/legend/executionplan/PlannedSql.java:24-46). Its constructor is private and `fromPlan` is the single way to mint one, so session methods that take `PlannedSql` cannot be handed ad-hoc text. It is the cleanest piece of the unit.
- **A planning failure travels as data.** `Failure` is a record with a closed `Kind` list (core/src/main/java/com/legend/testexecutionplan/Failure.java:42-94). `Failures.of` refuses an unlisted exception class rather than substituting a stand-in (Failures.java:24-42).
- **`MirJson` is uniform and careful.** It has one enum tag family per sealed root and exhaustive readers (MirJson.java:42-65, 935-1062). Doubles and decimals travel as text, so the round trip is lossless.
- **The guards are proportionate.**
  - `PlanJson.requireRoundTrip` (PlanJson.java:186-205) states G11 in a few lines.
  - `PlanTypesAreDataTest` (core/src/test/java/com/legend/PlanTypesAreDataTest.java:71-166) walks the record graph and rejects any non-data type. One test, a real invariant, cheap to keep.
  - `TestPlanJsonRoundTripTest` derives its coverage from `getPermittedSubclasses()` instead of a hand-kept list.
- **Load identity is content-addressed with a length-prefixed canonical form**, so two loads that run differently cannot share an identity (ExecutionPlan.java:327-392).
- **`PlanJsonTest` tests behaviour.** It round-trips every node kind, refuses tampered or missing blobs (core/src/test/java/com/legend/executionplan/PlanJsonTest.java:107-119), refuses unknown target keys, and checks that duplicate or dangling loads are refused (:141-171).

## Findings

### PLANMODEL-1 [S1] Five hand-maintained codecs, three conventions, untyped post-processing
- Where:
  - core/src/main/java/com/legend/executionplan/PlanJson.java:170-181, 304-442, 961-1024, 1385-1422
  - core/src/main/java/com/legend/executionplan/MirJson.java:772-783
  - core/src/main/java/com/legend/testexecutionplan/HostJson.java:16-23, 136-172, 1037-1206
  - core/src/main/java/com/legend/testexecutionplan/TestPlanJson.java:56-64, 170-173, 231-240, 282-287, 371-376
  - core/src/main/java/com/legend/executionplan/ConnectionWire.java:23-63
  - core/src/main/java/com/legend/executionplan/ExecutionPlan.java:327-392
- What: the plan's JSON is spread over five codecs: `PlanJson` (1,423 lines), `MirJson` (1,146), `HostJson` (1,295), `TestPlanJson` (405) and `ConnectionWire` (271). They do not agree on conventions.
  - **Tags.** There are three tag conventions:
    - lowerCamel enum tags: `tdsResult`, `keyed`;
    - Java simple names as string literals with a `default ->` refusal: `"AllRows"` in HostJson.java:1182-1205, `InMemory` in ConnectionWire;
    - ad-hoc strings: `"appeal"`, `"lite"`/`"compiledPureCompat"` (TestPlanJson.java:171-172), `"csv"`, `"reads"`, `"plain"` (PlanJson.java:464-497, 821-829, 880).
  - **Enum lookup.** The enum-lookup helper is written four times: `PlanJson.named`, `MirJson.named`, `ConnectionWire.named` and `TestPlanJson.tag`. A fifth inline loop reads the judge (TestPlanJson.java:231-240).
  - **Nulls.** There are three null conventions:
    - omit the key: `count`, `stamp`, `upper`, `fraction`;
    - write an explicit null: `maxColumns`, `slot`, `tree`, `abort`, `empty`;
    - require presence of every key: HostJson's `field()`.
  - **Generated output kept as source.** `HostJson` is generator output committed without its generator. Commit dc655af87 calls it "generated once from the record list and kept as source". It has 239 `v_` locals, 26 identity ternaries (`x.family() == null ? null : x.family()`, HostJson.java:139) and a 142-line tag switch. It is now maintained by hand.
  - **Untyped rewriting after the typed pass.** `PlanJson` builds an untyped `Map` tree and then rewrites it with six key-sniffing walkers: `hoist`, `inline`, `collectBundles`, `collectLoads`, `collectStatements` and `namedDigests`. A target is "an object holding `database`, `session` and `loads`" (:381), and a load is any object with `identity`, `cadence` and `steps` (:325).
  - **Same encoding twice.** `PlanJson.store`/`readStore` (:456-497) re-implements `MirJson.columnType`/`readColumnType` (:748-768, 1138-1145) with the same shape. Multiplicity is coded in both `PlanJson` and `HostJson` (PlanJson.java:499-506; HostJson.java:49-60).
  - **A second serialization for identity.** Load identity has its own hand-written canonical serializer (`Load.canonical`) beside the codec. Every new `LoadStep` variant needs both.
  - **A vestigial interface.** `ConnectionWire.Fields` has one implementation, an anonymous adapter in `PlanJson` (:1385-1422). It is left over from when the codec lived in the model package, which has no JSON dependency.
- Why it matters: this is the area that changes most. Since 2026-11-01 there were 70 commits to ExecutionPlan.java and 56 to PlanJson.java. Every new field or variant costs a writer half, a reader half, possibly a sniffer, a convention choice, and the TypeScript and Python copies. The write and read halves are not tied together, so an asymmetry (a field written but not read, a different null rule) surfaces only if a round-trip test happens to cover it. Key-sniffing walkers can mutate any future object that happens to carry the same keys, including an edge extension's JSON, because `hoistLoads` runs over the whole test plan.
- Constraint check: the hand-written shape really is forced. Reflection is banned in production (ArchitectureTest.java:2591-2604, F1.11), and the planner compiles to WebAssembly, where there is no `MessageDigest` (PlanJson.java:846). That rules out runtime-reflective codecs and Jackson. It does not force five unconnected styles, and it does not force throwing away a generator.
- Expert alternative: one small codec kit that is safe under TeaVM and the reflection ban, built from method references only. Each record is declared once for both directions:
  ```java
  interface Codec<T> { Object write(T t, WriteCtx w); T read(Json.Node n, ReadCtx r); }
  static final Codec<Failure> FAILURE = Codecs.record(Failure::new,
      req("kind", Failure::kind, Codecs.enumOf(Failure.Kind.class)),
      opt("message", Failure::message, STRING), opt("phase", Failure::phase, enumOf(Phase.class)), ...);
  static final Codec<HostJudgment> HOST_JUDGMENT = Codecs.sealed(HostJudgment.class,
      variant(HostJudgment.AllRows.class, ALL_ROWS), ...);   // a test checks every permitted subclass has a case
  ```
  - One tag rule, one null rule and one unknown-key rule, enforced in the kit.
  - Load and target hoisting and blob detachment done in the typed pass through `WriteCtx` and `ReadCtx`, which collect loads and targets by identity. All six sniffing walkers go.
  - Load identity is the hash of the codec's JSON of `(cadence, steps)`, so `Load.canonical` goes.
  - Alternatively, keep generated code but generate it in the build (an annotation processor or a genrule over the sealed roots) instead of committing one-off output.
- Confidence: verified.

### PLANMODEL-2 [S1] Every statement embeds its full Target; each Load points back to its holder's target
- Where:
  - core/src/main/java/com/legend/executionplan/ExecutionPlan.java:309-310, 564-566, 725, 956-997
  - core/src/main/java/com/legend/executionplan/PlanJson.java:711-727, 371-442
- What:
  - `Sql` carries a whole `Target`: database, connection description, session statements, loads, raise read-back, settings and transaction (:725). Every statement in a plan repeats it.
  - A `Load` itself carries a `target`. A target's loads must re-state that same target minus its loads, so `Target`'s constructor compares six fields one by one (:974-980).
  - `bare()` (:989-991) and `PlanJson.writeHeldDatabase` (:725-727) rebuild partial `Target` copies.
  - `Keyed` must check that `keys.target().equals(body.target())` (:564).
  - The JSON writer then hoists loads out of every embedded target by sniffing keys and inlines them back on read (PlanJson.java:371-442). Targets themselves are still written once per statement.
- Why it matters:
  - Adding a field to `Target` means editing the equality check, `bare()`, `writeHeldDatabase`, the reader's `only(...)` key list (PlanJson.java:1244) and the sniffer's identifying keys.
  - The cycle (Target → Load → Target) exists only because loads are addressed by containment rather than by reference.
  - A reader cannot tell from the types that "all statements of one plan usually share one target". The format makes them repeat it.
- Expert alternative: address targets and loads by id from plan-level tables, and validate references once in the plan's constructor.
  ```java
  record ExecutionPlan(List<Parameter> parameters, Map<String, Target> targets, Map<String, Load> loads,
                       Node root, boolean writes)            // ctor: every TargetRef/LoadRef resolves
  record Sql(String statement, List<Slot> slots, TargetRef target, ...)
  record Target(Database database, List<String> session, List<LoadRef> loads, ...)  // a Load holds no target
  ```
  `hoistLoads`, `inlineLoads`, `bare()`, the six-field check and the `Keyed` same-target check reduce to id equality. A plan's JSON is then a direct image of the record.
- Confidence: verified (structure); suspected (the JSON-size cost, which I did not measure).

### PLANMODEL-3 [S2] Typed facts spelled as strings inside the typed plan, parsed back at run
- Where:
  - core/src/main/java/com/legend/executionplan/ExecutionPlan.java:661 (`Column.slot`), :143, :237, :763
  - core/src/main/java/com/legend/executionplan/ColumnFacts.java:182-208
  - core/src/main/java/com/legend/result/RowDecode.java:373-500 and core/src/main/java/com/legend/exec/JdbcDecode.java:428
  - core/src/main/java/com/legend/testexecutionplan/HostJudgment.java:113, :186-188
  - core/src/main/java/com/legend/testexecutionplan/BodyFacts.java:20
  - core/src/main/java/com/legend/testexecutionplan/SideShape.java:32
  - core/src/main/java/com/legend/testexecutionplan/Appeal.java:161, :168
- What:
  - **`Column.slot`.** It is a `String`. `ColumnFacts.slot(SqlType)` spells a typed `SqlType` as `"ARRAY<MAP<K,V>>"`, `"STRUCT<name type, ...>"` or `"TAGGED<kind slot, ...>"`. The runtime then parses it back (`inside(c, "ARRAY")`, `topLevel(...)`, `slot.startsWith("DECIMAL(")`). There are 44 `.slot()` and `slotWord` reads in exec, result and testexec. The plan already has a typed `SqlType` codec (`MirJson.type`), and the same move from string to typed object was already made for `store` (ExecutionPlan.java:655-657).
  - **`Column.type`.** Parsed with `kind.startsWith("DateTime(")` and substring arithmetic (RowDecode.java:390-394).
  - **`HostJudgment.Rendered.form`.** An encoded sum: `"CSVJOIN:" + separator` or `"CSVTEXT"` (testplan/AssertVerdicts.java:1981-1987), decoded with `startsWith` and `substring` (testexec/TdsCompare.java:204-205). Its `eForm` and `aForm` fields are read at run only for nullness (testexec/HostJudgments.java:824).
  - **`RowArg.Literal.carrier`.** A Java class name matched with `case "Double"` (testexec/HostJudgments.java:679-688).
  - **Magic slot parameters.** Stamps and counts are parameter names with a magic prefix (`#stamp`, `#count`, detected by `startsWith(STAMP)`). Other non-parameter slots are `Binding` variants with an ignored `parameter`; the test passes `""` (PlanJsonTest.java:102).
  - **Other strings.**
    - `BodyFacts.shape` is a string of letters (`"FLAXEO"`).
    - `SideShape.multiplicity` is a `String` beside `long lower`, although `ExecutionPlan.Multiplicity` and its `.text()` exist.
    - `TempTable.kind` and `InListSeed.kind` are free strings.
    - `Verdict.arm` is a `String` that nothing in production reads; it is only copied between constructors and serialized.
- Why it matters: these facts are typed when the planner makes them and untyped in the plan, so every reader re-parses them and every spelling change is a silent cross-module break. This is the stringly-typed dispatch the rubric names, in the one place that claims to be "typed data".
- Expert alternative:
  - `Column.slot` becomes a `@Nullable SqlType`, coded by `MirJson.type`.
  - `sealed interface RenderForm permits CsvJoin(String separator), CsvText, ToString` plus `enum RenderedSide { EXPECTED, ACTUAL }`.
  - `sealed interface Literal permits Str, Bool, Dbl, Lng, Int, BigInt`.
  - `Binding.Stamp(String name)` and `Binding.Count(String name)`, with `Slot(Binding)` and the parameter name only on the parameter-binding variants.
  - `BodyFacts(List<StatementKind> shape)`.
  - `SideShape` holds an `ExecutionPlan.Multiplicity`.
  - Delete `Verdict.arm`, or make it an enum the judges switch on.
- Confidence: verified.

### PLANMODEL-4 [S2] Sum types flattened into nullable fields, sentinel ints, prose rules and `default` arms
- Where:
  - core/src/main/java/com/legend/testexecutionplan/Appeal.java:130-146
  - core/src/main/java/com/legend/testexecutionplan/RootLoop.java:29-32, 73-79, 100-108
  - core/src/main/java/com/legend/testexecutionplan/Failure.java:25-38
  - core/src/main/java/com/legend/executionplan/ExecutionPlan.java:47-112, 416-421, 441-474, 540-612, 968-973, 1027-1034
  - core/src/main/java/com/legend/testexecutionplan/Agreement.java:19-24
- What:
  - **`Appeal.Read`.** Four nullables guarded by `(side == null) != (shape == null) || side != null && (failure != null || unlisted != null) || failure != null && unlisted != null`.
  - **`RootLoop.Message` and `RootLoop.Row`.** Each is "a side or a refusal, one of the two", checked at run. `Row` uses `NO_VERDICT = -1`. Appeals use `NO_APPEAL = 0`, and `Agreement.raisingVerdict` uses the same sentinel.
  - **`Failure`.** A flattened per-kind record: `phase` is allowed only for MODEL, `element` only for MODEL or MAPPING_RESOLUTION, and `unjudgedReason` only for ASSERT_FAILED.
  - **`DatabaseSettings`.** `required.isEmpty() == (readBack == null)`.
  - **`Keyed`.** `keysOf` (ROWS or COLUMNS) is coupled to `admitted` and to the slot rules by constructor checks.
  - **`Load.Cadence`.** Where each cadence may stand lives in prose and runtime refusals: a target takes only ONCE and PER_ACQUISITION, `Bound`/`Stream`/`Attach` steps only HOST_SESSION, "a program node of this cadence is refused", and "exec refuses it by name".
  - **`ExecutionPlan.answer()`.** It returns `Node`, so `answerShape`, `answerSql` and `answerColumns` each need a `default -> throw ... "not reached"` arm (:52, :65, :89). The "is an answer" predicate is repeated three times (:73, :100, TestPlan.java:168).
  - **`Sequence`.** It may hold zero or several answering steps; that is refused only when someone asks for its answer.
- Why it matters: a reader has to assemble the valid combinations from constructor bodies and javadoc. Each new state is one more boolean clause, and a sentinel number is easy to compare the wrong way (`appeal != NO_APPEAL && verdict != NO_VERDICT`, PlannedAppeals.java:59-60).
- Expert alternative:
  - `sealed interface Read permits Planned(Side, SideShape), Failed(Failure), Unlisted(String, String), Absent`.
  - `sealed interface Message permits Planned(Side, boolean carried), Refused(Failure)`.
  - A `Row` with a `sealed RowSource permits Spelled(int verdict), Refused(Failure)`, and `@Nullable Integer appeal` instead of 0.
  - Failure kinds that carry data as their own records: `Model(Phase, msg, element)`, `MappingResolution(msg, element)`, `AssertFailed(msg, @Nullable unjudged)`, `Plain(Kind, msg)`.
  - `sealed interface Answer extends Node permits TdsResult, Keyed, Constant` returned by `answer()`, so no `default` arms are needed.
  - Loads split by where they stand: `ProgramLoad` as a `Node` (BEFORE_STEP, AT_STEP, PER_TRANSACTION), `TargetLoad` on a target (ONCE, PER_ACQUISITION), and `HostLoadPlan` as its own root for HOST_SESSION with its `Bound`/`Stream`/`Attach` steps. Exec then cannot be handed one.
- Confidence: verified.

### PLANMODEL-5 [S2] The plan carries the planner's MIR tree, which nothing runs from
- Where:
  - core/src/main/java/com/legend/executionplan/ExecutionPlan.java:713-714, 725-726
  - core/src/main/java/com/legend/executionplan/PlanJson.java:671-672, 1236-1237
  - core/src/main/java/com/legend/executionplan/MirJson.java (whole file)
  - core/src/test/java/com/legend/testcases/PlanRoundTrip.java:99-120
- What: every `Sql` holds `SqlQuery tree`, and `PlanJson` writes and reads the whole tree through MirJson's 1,146 lines. A search for `.tree()` on a plan statement in main sources finds no reader in exec, testexec, result, server or the hosts. The only consumer is a test that re-renders it (PlanRoundTrip.java:99-120). The javadoc says the opposite of the code: "it is not written to the lite JSON yet (a later step), so a plan read back from JSON has none" (ExecutionPlan.java:713-714). A test is named `theTypedTreeIsNotWrittenYet`, but it asserts only the version prefix (PlanJsonTest.java:181-213).
- Why it matters:
  - It is the largest codec in the unit, kept in lockstep with every MIR change, for data no runtime uses.
  - It makes `:execution_plan` depend on `:sql`'s whole MIR.
  - It inflates every plan's JSON.
  - It sits uneasily with G4's "the plan is SQL and data only … no planning objects": the MIR is the lowering's output.
- Expert alternative: drop `tree` from `ExecutionPlan.Sql`. Keep it on the planner's side (`record Planned(Sql sql, SqlQuery tree)`) for lineage or explain, or emit it as a separate explain artifact on request. If a host truly needs it, name that consumer in the javadoc and test it.
- Confidence: verified (no production reader found by grep; the doc contradiction is verified).

### PLANMODEL-6 [S2] `testexecutionplan` is "data alone" in name only; edge data and grammar live inside it
- Where:
  - core/BUILD.bazel:81-84
  - core/src/main/java/com/legend/testexecutionplan/Verdicts.java:9-210
  - core/src/main/java/com/legend/testexecutionplan/JudgeJson.java:16-46
  - core/src/main/java/com/legend/testexecutionplan/TypedSide.java, Row.java:5, AssertListener.java, Edge.java, Failures.java:45-79
  - core/src/main/java/com/legend/testexecutionplan/Appeal.java:32-33
  - core/src/main/java/com/legend/testexecutionplan/TestPlanJson.java:282-287, 313
- What: the BUILD comment says the library is "a test body's plan as data … Data alone". It also holds a good deal of behaviour:
  - `Verdicts`: a bag of judge statics (`firstTextDiff`, `repr`, `constantValue` switching on type names, `fail()`, which is declared to return `TypedSide` but always throws, and the engine-golden recognizers `stripChainedPlanWarning` and `isPopulationGolden`).
  - `JudgeJson`: a 300-line port of json-simple 1.1.1's parser for `COMPILED_PURE_COMPAT`. In LITE mode it parses the text twice with two JSON libraries (`com.legend.json.Json`, then `com.legend.sql.Json`, :40-45).
  - Runtime carriers holding `Object` cells ("cells are raw JDBC objects", Row.java:5), the listener interface and the edge's run-time callback interface (`Edge`).
  - `Failures.raised`, which constructs 23 exception types.

  Two pieces are compatibility code. `JudgeJson` is labelled "Compatibility code: the edge's when the edge package exists (P2)"; `com.legend.edge` now exists, and it was not moved. `Appeal` is the SQL-text appeal, which G5 §2 lists as edge. It implements the edge's `Extension` point but lives in the product package, and `TestPlanJson` special-cases it inline with `e instanceof Appeal` instead of going through `ExtensionCodec`.
- Why it matters:
  - The library's stated contract (data that both sides read) does not match what is in it.
  - Its dependencies grew to `:error` and `:values` to support the behaviour.
  - G5's one-direction edge rule is weakened where the edge's own records and grammar sit in a product package.
  - The extension point is bypassed by its main tenant.
- Expert alternative:
  - Keep `test_plan` to records and codecs.
  - Move the shared judging semantics (`Verdicts`, `TypedSide`, `Row`, `Column`, `AssertListener`, `Failures.raised`) to a `judging` library below both `testplan` and `testexec`.
  - Move `JudgeJson`'s compatibility grammar and `Appeal`, with its codec, to `com.legend.edge` behind `ExtensionCodec`. `TestPlanJson` then has no special case.
- Confidence: verified.

### PLANMODEL-7 [S2] The same concept implemented several times across the pair and against the model
- Where:
  - core/src/main/java/com/legend/testexecutionplan/TestPlan.java:96-121, 167-169 vs core/src/main/java/com/legend/executionplan/ExecutionPlan.java:47-112
  - ExecutionPlan.java:197-214 vs TestPlan.java:414-431
  - core/src/main/java/com/legend/testexecutionplan/Column.java:8 vs core/src/main/java/com/legend/testexecutionplan/SideShape.java:47 vs ExecutionPlan.java:661
  - ExecutionPlan.java:1004, 280 vs TestPlan.java:240
  - core/src/main/java/com/legend/executionplan/PlanJson.java:186-205 vs core/src/main/java/com/legend/testexecutionplan/TestPlanJson.java:67-85
  - core/src/main/java/com/legend/executionplan/AuthenticationSpec.java vs core/src/main/java/com/legend/model/AuthenticationSpec.java; core/src/main/java/com/legend/planner/ConnectionPlans.java
  - core/src/main/java/com/legend/executionplan/DataStores.java:66-74 vs PlanJson.java:854
- What:
  - **Answer logic.** `TestPlan` re-implements `answers()`, `answer()`, `sendsTheAnswer()` and the "answering" predicate. Its `answerShape()` builds a throwaway `ExecutionPlan.reading(parameters, answer())` to reach the product's rule (TestPlan.java:115).
  - **Constants.** `ExecutionPlan.Constant` and `TestPlan.Side.Constant` are the same text-cells-with-nulls record with a copied constructor. The product javadoc links `{@link Side.Constant}`, which cannot resolve from `executionplan` (ExecutionPlan.java:193).
  - **Columns.** There are three column records. `testexecutionplan.Column(name, pureType, multiplicity)` and `SideShape.Column(name, type, multiplicity)` are identical in shape.
  - **Transaction statements.** The dialect's transaction-control statements come in three shapes: `Transaction(begin, commit, rollback)`, `Effect.Script(bracketed, abort)` and `Verdicts.Savepoint(set, rollbackTo, release)`, each with its own `plannedX()` accessors.
  - **Round-trip guard.** The guard is written twice, nearly verbatim (PlanJson.java:186-205 and TestPlanJson.java:67-85).
  - **Connection description.** It is a faithful copy of the model's. The body of `executionplan.AuthenticationSpec` from `public sealed` on is byte-identical to `model.AuthenticationSpec` (`diff` reports 0 lines), and a field-by-field copier (ConnectionPlans.java, 87 lines) and a codec stand beside it. One new authentication flavour costs four edits.
  - **SHA-256.** Two implementations sit in one package: `DataStores.sha256` (`MessageDigest`) and `cache.Hash` (portable).
- Why it matters: each copy must evolve with the other, and nothing ties them together except discipline.
- Expert alternative:
  - Move the answer rule to a static helper on `Node`, `Answer answerOf(List<Node> steps)`, used by both plans.
  - Have one `TextCells(List<Column> columns, List<List<String>> cells)` record that both constants hold.
  - Have one `Column(name, pureType, @Nullable Multiplicity)` for judged sides.
  - Have one `TransactionStatements` record that the three uses share.
  - Have one generic `RoundTrip.require(T plan, Codec<T>)`.
  - Put the connection description in a small `:connection` records library that both `:model` and `:execution_plan` depend on. That keeps "the model depends on no plan" and "exec reads no model" with no copy and no copier.
- Confidence: verified.

### PLANMODEL-8 [S2] The reader's strictness and errors are inconsistent; a misspelled optional key becomes a default
- Where:
  - core/src/main/java/com/legend/executionplan/PlanJson.java:1374-1381 (`only`) used at :784, :1244, :1256, :1262, :1268, :1338, :1344, :1348, :1357, :1366, but not in `readNode` (:1065-1151), `readSql` (:1215-1239), `readRelationColumns` (:1186-1197) or `readParameter` (:1051-1054)
  - PlanJson.java:1039-1040
  - raw `valueOf`: PlanJson.java:490, 1126, 1194, 1318, 1325, 1353, 1368; HostJson.java:165, 167, 191
  - unchecked casts: 18 `(Json.X)` casts in PlanJson, 20 in MirJson
- What:
  - **Strictness.** Ten object kinds (target, read-back, settings, setting, transaction, bound, its two bind kinds, stream, attach) refuse unknown keys. Nodes, statements, columns and parameters ignore them. A misspelled `"stmap"`, `"regoins"`, `"script"`, `"count"` or `"fraction"` is therefore read as absent, which is exactly the silent default G7 forbids. The target only became strict after an audit (PlanJson.java:1242-1243).
  - **Errors.** Malformed input raises `ClassCastException` from bare casts. Other enum fields go through `named` with a message, but `valueOf` raises `IllegalArgumentException: No enum constant …` with no context. HostJson checks every node type properly (`obj`/`str`/`bool`), so the three codecs disagree here too.
  - **Stale message.** The format reader still says "a data reference's format is csv or rows" after the `rows` format was deleted.
- Why it matters: `PlanJson.contentOf` describes the input as possibly untrusted ("where an untrusted plan is read", :846). Some refusals are by name and some by `ClassCastException`, and some are not refusals at all.
- Expert alternative: one reader context that does all of this for every object, as part of the PLANMODEL-1 kit: `obj.required("x", codec)`, `obj.optional("x", codec)`, and `obj.done()`, which rejects any unread key. Use typed accessors that throw a `PlanFormatException(path, expected, found)`.
- Confidence: verified.

### PLANMODEL-9 [S2] Versioning is a lockstep fingerprint with a prose changelog in three places
- Where:
  - core/src/main/java/com/legend/executionplan/PlanJson.java:31-151
  - core/src/main/java/com/legend/testexecutionplan/TestPlanJson.java:25-46, 219-225
  - core/src/test/java/com/legend/executionplan/PlanJsonTest.java:185-212
  - core/src/test/java/com/legend/PlanVersionsAgreeTest.java:27-38
  - python/legend_lite/_runs.py:26
- What:
  - **Exact match.** `VERSION = 51` (and `TestPlanJson.VERSION = 21` with an embedded `productVersion`) is checked by exact equality. No older version is ever read.
  - **Changelog in javadoc.** The constant's javadoc is a 120-line changelog of every bump since version 2, and the same history is repeated in a test comment.
  - **Branch bookkeeping.** The number tracks branches as well as formats: "Still 48 (not on main), 2026-11-12" (:134), "Still 49 (not on main)" (:143).
  - **Partial pinning.** `PlanVersionsAgreeTest` ties only the TypeScript runner's constant to Java, by a regular expression over the `.ts` source. Python's `PLAN_VERSION = 51` is not checked.
- Why it matters: every change to the format edits three prose histories. The number gives no compatibility. All it says is "same build", which is fine in a monorepo, but then the changelog and manual bumps are noise. And the one host it does not pin can drift silently.
- Expert alternative: say what it is. Use a format fingerprint derived mechanically (a hash of the codec's schema description, or the sealed record graph that `PlanTypesAreDataTest` already walks), emitted as a generated constant for Java, TypeScript and Python and checked by one test. Keep the changelog in git and in the design doc. If compatibility across releases is ever wanted, switch to `major.minor` with additive fields readable by older readers.
- Confidence: verified.

### PLANMODEL-10 [S3] Javadoc reads as process history, and some of it is stale or wrong
- Where: pattern across the unit.
- What: 236 ISO dates and 68 `docs/*.md` paths in 36 files. PlanJson.java has 64 dates and ExecutionPlan.java 59. Audit labels appear throughout ("the C checkpoint's SHOULD-FIX 3", "the slice 2 invariant audit's D2", "hole 7", "census M18"), as do "until then …" clauses.

  Three representative examples:
  - The `writes` parameter, ExecutionPlan.java:21-24: "(the plan-as-data design's slice 5, 2026-11-09: … -- until then the door read it off the typed query …)".
  - HostJudgment.java:190-194: "(2026-11-04: the wrappers hid those from the gate: 61 DuckDB corpus bodies…)".
  - The comment that records a deletion in place of the deleted code, TestPlan.java:55-56.

  Stale or wrong statements, all verified:
  - The `Sql.tree` comment is false (ExecutionPlan.java:713-714; see PLANMODEL-5).
  - The `Sequence` javadoc still says "A verdict group may be a step (S1b) … run by PlanRunner.program" (ExecutionPlan.java:223-224). Verdict groups left the product plan (:184-185).
  - `ConnectionSpecification` says "Sealed root for the three … flavors" (there are ten) and "Mirrors engine's `com.gs.legend.model.def…`", the deleted module (ConnectionSpecification.java:3-8).
  - `ConnectionWire` says "The model depends on no JSON" in a package that depends on `//json` (ConnectionWire.java:14-15).
  - The `{@link Side.Constant}` link cannot resolve (ExecutionPlan.java:193).
  - About ten passages describe semantics as "the walk's", though the walk was deleted on 2026-11-06 (HostJudgment.java:40, 127-128, 158; RootLoop.java:18; TestPlan.java:88; Verdicts.java:119).
  - Orphan stacked javadoc blocks (PlanJson.java:615-616; Verdicts.java:118-119).
  - engine-client/src/plan-runner.ts:2 says "version 22".

  Signal to noise is low. For example, `Keyed`'s javadoc (ExecutionPlan.java:521-539) is 19 lines, about a third of them provenance.
- Why it matters: a maintainer has to separate what the code does now from what it did on 2026-10-27. The stale lines actively mislead: the `tree` comment would lead a reader to believe plans read back have no MIR.
- Expert alternative: javadoc states the present contract in a sentence or two, plus the reason where the reason is not obvious. Provenance moves to commit messages and the design doc. Remove the "the walk" references or restate them as the rule itself.
- Confidence: verified.

### PLANMODEL-11 [S3] Name collisions, residue names and forwarding wrappers
- Where: pattern.
- What: one package reuses simple names:
  - `Verdicts` (class) vs `TestPlan.Verdicts` (record); inside TestPlan the record shadows the class.
  - `Row` (class) vs `RootLoop.Row`.
  - `Decided`: `HostJudgment.Decided` vs `RootLoop.Decided`.
  - `Rows`: `HostJudgment.Rows`, `TestPlan.Side.Rows`, `Appeal.Rows`.
  - `Json`: `HostJudgment.Json` vs `com.legend.json.Json`.
  - `Statements`: `LoadStep.Statements` vs `DataRef.Statements`, whose only constant is named `JSON` (DataRef.java:95).

  Residue from renames: `SetupTag` and `setupStep` for load steps, and the error text "setup step" (PlanJson.java:162, 861, 1314). The switch variable `case Load u`, copied from `Update` (PlanJson.java:577; ExecutionPlan.java:110).

  Ten public one-line forwarders follow no naming pattern (`nodeJson`→`node`, `nodeOf`→`readNode`, `columnsJson`/`columnsOf`, `targetJson`/`targetOf`, `loadStepJson`/`loadStepOf`, `parameterOf`; PlanJson.java:509, 617, 730, 735, 836, 841, 1027, 1061, 1158). `TestPlanJson.codecs()` forwards to `Codecs.ALL` (:257-259).
- Why it matters: collisions force qualified names and confuse search. The forwarders double the API surface for no behaviour.
- Expert alternative: give each nested type a distinct name (for example `VerdictGroup`, `VerdictRow`, `PlannedDecision`, `JsonEquality`), make the codec methods public under one `write*`/`read*` pair per type, and finish the renames.
- Confidence: verified.

### PLANMODEL-12 [S3] God files, telescoping constructors and positional nulls
- Where:
  - core/src/main/java/com/legend/executionplan/ExecutionPlan.java (1,352 lines, 54 nested types), :769-786
  - core/src/main/java/com/legend/testexecutionplan/TestPlan.java:203-228, 349-368
  - core/src/test/java/com/legend/executionplan/PlanJsonTest.java:34-35, 97-98
- What:
  - **One file.** The whole product plan, from nodes to statements, targets, loads, settings and connection steps, is one file.
  - **Constructor overloads.** `Sql` has four constructors; `Verdicts` has four plus two `with*` copiers; `Verdict` has four plus `withHost`.
  - **Positional nulls.** Callers write `new Target(db, List.of(), List.of(), null, null, null)`. The data-layer tests reach up into `planner.LoadPlanner`, `planner.ConnectionPlans`, `setup.LoadSteps` and `model.*` only to build fixtures.
- Why it matters: a new engineer navigates one 1,350-line file. Positional nulls hide which field is which, and adding a component means touching every overload.
- Expert alternative:
  - Split the sealed hierarchy across files in the package (`Node.java`, `Sql.java`, `Target.java`, `Load.java`, `LoadStep.java`, `Column.java`); sealed types can span files.
  - Replace overloads with named factories (`Target.of(database)`, `.withSession(...)`) or a small builder.
  - Give the data layer its own test fixtures.
- Confidence: verified.

### PLANMODEL-13 [S3] Inline fully qualified names instead of imports
- Where: pattern. Code lines (excluding imports and comments) hold 229 inline `java.util.*`, `java.math.*` or `com.legend.*` qualified names; `@com.legend.base.Nullable` appears 110 times.
- What: three examples:
  - `java.util.Collections.unmodifiableList(new java.util.ArrayList<>(row))` (ExecutionPlan.java:210).
  - `com.legend.error.LegendCompileException.@Nullable Phase v_phase = … com.legend.error.LegendCompileException.Phase.valueOf(…)` (HostJson.java:167).
  - `com.legend.testexecutionplan.TestPlan.JudgingRules.COMPILED_PURE_COMPAT` (seen from testexec/HostJudgments.java:822).
- Why it matters: it adds noise to already dense code and hides the dependency list at the top of each file.
- Expert alternative: imports (including `import com.legend.base.Nullable`), and nested-type imports for `ExecutionPlan.*`.
- Confidence: verified.

## Cross-unit observations
- **TypeScript.** engine-client/src/plan-runner.ts:137-171 hand-declares the plan's JSON shapes, a third copy of the schema (Java records, Java codec, TypeScript types). Its header comment says "version 22" (:2). A schema generated from the Java records (or the fingerprint in PLANMODEL-9) would keep the hosts honest.
- **Python.** python/legend_lite/_runs.py:26 pins `PLAN_VERSION = 51`, and no test ties it to `PlanJson.VERSION`.
- **Slot parsing at run.** core/src/main/java/com/legend/result/RowDecode.java:373-500 and core/src/main/java/com/legend/exec/JdbcDecode.java:428 parse the plan's `slot` strings (PLANMODEL-3). They would simplify the moment `slot` is a `SqlType`.
- **Testexec.** core/src/main/java/com/legend/testexec/HostJudgments.java:679-688 and 811-830 use string switches on `carrier` and `form`. Verdicts are signalled by `Verdicts.fail()` throwing from a method typed to return a `TypedSide`; that is control flow by exception in the judges.
- **Round trip on every test body.** core/src/main/java/com/legend/testplan/TestPlanner.java:167 runs `TestPlanJson.requireRoundTrip` on every test body in production: a full write, parse and deep-equals as a guard on each body. The product plan does it only behind `-Dlegend.plan.roundtrip` (core/src/main/java/com/legend/TypedQuery.java:249-251). The cost is suspected, not measured.
- **Compatibility in the product planner.** core/src/main/java/com/legend/testplan/AssertVerdicts.java:1023-1030 builds `eForm`/`aForm` strings whose only use at run is their nullness. Appeals (edge data) are constructed by the product's test planner (`SqlTextVerdicts`), which is compatibility code in the product (G5).

## Measurements
| Measure | Value | How |
|---|---|---|
| Codec lines | PlanJson 1,423; MirJson 1,146; HostJson 1,295; TestPlanJson 405; ConnectionWire 271 (about 4,540 total) | `wc -l core/src/main/java/com/legend/{executionplan,testexecutionplan}/*.java` |
| Records in the two packages | 147, with about 94 `@Nullable` components, 52 `boolean` and 166 `String` | a regex count over the record headers (approximate) |
| Process-history density | 236 ISO dates and 68 `docs/*.md` paths in 36 files (PlanJson 64 dates, ExecutionPlan 59) | `grep -rhoE "20[0-9]{2}-[0-9]{2}-[0-9]{2}"` and `grep -rhoE "docs/[A-Za-z0-9_./-]+\.md"` over both packages |
| Inline qualified names in code lines | 229 (110 `com.legend.base.Nullable`) | grep excluding import and comment lines |
| Generated-code artifacts in HostJson | 239 `v_` locals, 26 identity ternaries, 93 `new LinkedHashMap<>()` | grep |
| Unchecked JSON casts | PlanJson 18, MirJson 20 | `grep -c "(Json\.\(Obj\|Str\|Arr\|Num\)) "` |
| Run-side reads of the `slot` string | 44 in exec, result and testexec | `grep -rn "slotWord(\|\.slot()"` |
| Churn since 2026-11-01 | ExecutionPlan.java 70 commits, PlanJson.java 56, TestPlanJson.java 24, TestPlan.java 18, HostJson.java 9 | `git log --oneline --since=2026-11-01 -- <file> \| wc -l` |
| Most complex functions | MirJson::readExpr CCN 58 (128 NLOC); MirJson::expr CCN 51 (203 NLOC); HostJson::wHostJudgment 142 NLOC; PlanJson::readNode CCN 30 (87 NLOC); PlanJson::node 86 NLOC; ExecutionPlan Load::canonical CCN 23 | `python3 -m lizard -l java -L 60 -C 15 -w` |
| Copy-paste | 0 clones at 6 lines / 60 tokens; 4 trivial clones in weak mode at 5 / 40. The duplication is structural (per-record codecs, parallel records), not textual. | jscpd |
| Model vs plan `AuthenticationSpec` | 0 differing lines from the `public sealed` declaration on | `diff` of the two files from the declaration on |
| Format versions | product `VERSION = 51`, test plan `VERSION = 21` (plus embedded `productVersion`); the TypeScript constant is checked, the Python one is not | `grep -rn PLAN_VERSION` |
| Nested types in the big files | ExecutionPlan.java 54, HostJudgment.java 34, TestPlan.java 14 | grep for record/enum/interface declarations |
