# M6: Pure-mode parity (channel A, legend-pure's route) vs the reference (legend-pure, `core-compiled`; `core-interpreted` noted)

Measured 2026-11-25 in `/home/user/s2` at `da231244` (nothing committed; the probe reverted).

## Which targets exist

Channel A's legend-pure route is the default audience (no `-Dpct.audience`, no `-Dpct.channela.rules`; see M4's record
for the code). Targets on the three databases: `//pct:pct_duckdb_<5 suites>`, `//pct:pct_sqlite_<5 suites>` (manual) and
`//pct:pct_h2` (**Relation only**: there is no H2 legend-pure-route target for Essential, Grammar, Standard or
Unclassified). Each is judged by its expected-failure ledger class, so a green run says only "the ledger holds". To get
each test's RAW outcome I ran a temporary probe: a copy of the ENGINE lane's class with the audience left at its
default (legend-pure's), no expected failures, per-test outcomes by full name -- the same lanes and backends
(`LEGENDLITE_PCT_BACKEND` unset / `sqlite` / `h2`), 11 runs.

Probe sources (both reverted, `git status --short` empty afterwards):
`/tmp/claude-0/-home-user-legend-lite/e82d575c-3b18-52af-b239-5bdd496a53f4/scratchpad/r1/probes/Test_LegendLite_PureRawProbe_PCT.java.txt` and `/tmp/claude-0/-home-user-legend-lite/e82d575c-3b18-52af-b239-5bdd496a53f4/scratchpad/r1/probes/m6-pure-route-probe.BUILD.txt`.

## The reference

- `core-compiled` (the reference): legend-engine's `pct-manifests/core-compiled/` -- Relation 3 rows, Standard 0,
  Unclassified 0 (Variant 0, ScenarioQuant 0) -- and, for the platform suites, legend-pure's own manifests: Essential
  `platform/pure/essential/pct_essential_native.json` (0 rows) and Grammar
  `legend-pure-runtime-java-engine-compiled/.../pct_grammar_compile_native.json` (**3 rows**: `testLargeMinus`,
  `testLargePlus`, `testLargeTimes`, "Assert failure"). So the compiled reference fails 6 tests in lite's five suites
  (the task's "core-compiled: 3" is the Relation file alone).
- `core-interpreted`: Relation 5 rows, Standard 2, the rest 0 (legend-pure's interpreted Grammar manifest
  `pct_grammar_native.json`: 0). Seven tests.
- The reference passes every test it does not list.

## Commands

```
cd /home/user/s2
# probe applied: the class above added under pct/src/test/java/org/finos/legend/lite/pct/, the stanza appended to pct/BUILD.bazel
bazel build --lockfile_mode=error --keep_going --jobs=3 --noremote_upload_local_results \
  //pct:probe_pure_duckdb_{essential,grammar,relation,standard,unclassified} \
  //pct:probe_pure_sqlite_{essential,grammar,relation,standard,unclassified} //pct:probe_pure_h2_relation
git checkout -- pct/BUILD.bazel && rm pct/src/test/java/org/finos/legend/lite/pct/Test_LegendLite_PureRawProbe_PCT.java
python3 /tmp/claude-0/-home-user-legend-lite/e82d575c-3b18-52af-b239-5bdd496a53f4/scratchpad/r1/probes/m6.py <legend-engine src> <legend-pure src> /tmp/claude-0/-home-user-legend-lite/e82d575c-3b18-52af-b239-5bdd496a53f4/scratchpad/r1/data/probe_pure /home/user/s2 /tmp/claude-0/-home-user-legend-lite/e82d575c-3b18-52af-b239-5bdd496a53f4/scratchpad/r1/m6.json
python3 /tmp/claude-0/-home-user-legend-lite/e82d575c-3b18-52af-b239-5bdd496a53f4/scratchpad/r1/probes/m6_report.py /tmp/claude-0/-home-user-legend-lite/e82d575c-3b18-52af-b239-5bdd496a53f4/scratchpad/r1/m6.json /home/user/s2
```

Raw outcomes kept in `/tmp/claude-0/-home-user-legend-lite/e82d575c-3b18-52af-b239-5bdd496a53f4/scratchpad/r1/data/probe_pure/<db>_<suite>/outcomes.tsv`.

**Check:** each lane's raw failure count equals its ledger's row count exactly (DuckDB 18/5/1/0/0; SQLite 20/8/2/0/1;
H2 relation 21), and every failing test has its ledger row -- the committed ledgers are exact at `da231244`.

## Buckets, per database and scope

#### duckdb

| scope | lite ran | core-compiled rows | core-interpreted rows | both pass | reference passes, lite fails | reference fails, lite passes | both fail | vs core-interpreted: lite fails where it passes |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| Essential | 345 | 0 | 0 | 327 | 18 | 0 | 0 | 18 |
| Grammar | 137 | 3 | 0 | 129 | 5 | 3 | 0 | 5 |
| Relation | 469 | 3 | 5 | 465 | 1 | 3 | 0 | 1 |
| Standard | 205 | 0 | 2 | 205 | 0 | 0 | 0 | 0 |
| Unclassified | 93 | 0 | 0 | 93 | 0 | 0 | 0 | 0 |
| **total** | **1249** | | | **1219** | **24** | **6** | **0** | **24** |

#### sqlite

| scope | lite ran | core-compiled rows | core-interpreted rows | both pass | reference passes, lite fails | reference fails, lite passes | both fail | vs core-interpreted: lite fails where it passes |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| Essential | 345 | 0 | 0 | 325 | 20 | 0 | 0 | 20 |
| Grammar | 137 | 3 | 0 | 129 | 5 | 0 | 3 | 8 |
| Relation | 469 | 3 | 5 | 464 | 2 | 3 | 0 | 2 |
| Standard | 205 | 0 | 2 | 205 | 0 | 0 | 0 | 0 |
| Unclassified | 93 | 0 | 0 | 92 | 1 | 0 | 0 | 1 |
| **total** | **1249** | | | **1215** | **28** | **3** | **3** | **31** |

#### h2

| scope | lite ran | core-compiled rows | core-interpreted rows | both pass | reference passes, lite fails | reference fails, lite passes | both fail | vs core-interpreted: lite fails where it passes |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| Relation | 469 | 3 | 5 | 446 | 20 | 2 | 1 | 19 |

## Every "reference passes, lite fails" test, with lite's message and its ledger row

Every one has a ledger row (0 without). Where the row's own comment block is not directly above it, the reason is the
list's opening comment -- for DuckDB's and SQLite's six "column mismatch" rows (`testAtError`, `testDayOfMonthError`,
`testHourError`, `testMinuteError`, `testNewDateError`, `testSecondError`) that is: "COLUMN-precision limit: message and
LINE now match real pure exactly (SQL-raised guards + call-stack source info), but the failing call executes in DuckDB
-- its column within the test expression cannot be recovered without source-span plumbing through the wire. The
reference adapter excludes these too." (`Test_LegendLite_EssentialFunctions_PCT.java:37-42`).

#### duckdb: the reference passes, lite fails

1. Essential `meta::pure::functions::collection::tests::at::testAtError`
   - lite: `Assert failure at (resource:/platform/pure/essential/tests/assert.pure line:21 column:5), "Execution error column mismatch. Actual: 23 where expected: 37"`
   - ledger: `pct/src/test/java/org/finos/legend/lite/pct/Test_LegendLite_EssentialFunctions_PCT.java:107` -- no comment above the row's group
2. Essential `meta::pure::functions::collection::tests::find::testFindInstance`
   - lite: `Assert failure at (resource:/platform/pure/essential/tests/assert.pure line:21 column:5), "\nexpected: <@_0030heh instanceOf meta::pure::functions::collection::tests::model::CO_Person>\nactual:   <@_0031aaq instanceOf meta::pure::functions::collection::tests::model::CO_Person>"`
   - ledger: `pct/src/test/java/org/finos/legend/lite/pct/Test_LegendLite_EssentialFunctions_PCT.java:97` -- its comment: "INSTANCE IDENTITY through the wire (assertIs against the original instance — addresses are nondeterministic, so the STABLE fragment pins; the reference adapter excludes these too):"
3. Essential `meta::pure::functions::collection::tests::find::testFindUsingVarForFunction`
   - lite: `Assert failure at (resource:/platform/pure/essential/tests/assert.pure line:21 column:5), "\nexpected: <@_00328jg instanceOf meta::pure::functions::collection::tests::model::CO_Person>\nactual:   <@_0033024 instanceOf meta::pure::functions::collection::tests::model::CO_Person>"`
   - ledger: `pct/src/test/java/org/finos/legend/lite/pct/Test_LegendLite_EssentialFunctions_PCT.java:98` -- its comment: "INSTANCE IDENTITY through the wire (assertIs against the original instance — addresses are nondeterministic, so the STABLE fragment pins; the reference adapter excludes these too):"
4. Essential `meta::pure::functions::collection::tests::head::testHeadComplex`
   - lite: `Assert failure at (resource:/platform/pure/essential/tests/assert.pure line:21 column:5), "\nexpected: <@_003a8dg instanceOf meta::pure::functions::collection::tests::model::CO_Person>\nactual:   <@_003amgm instanceOf meta::pure::functions::collection::tests::model::CO_Person>"`
   - ledger: `pct/src/test/java/org/finos/legend/lite/pct/Test_LegendLite_EssentialFunctions_PCT.java:104` -- its comment: "testHeadComplex: nested-struct reconstruction now REBUILDS the CO_Person correctly (classPropertyTypeOf); the assert then requires the interpreter's ORIGINAL instance back — reference identity, unfixable over the wire (the reference adapter excludes it too, failing even earlier)"
5. Essential `meta::pure::functions::date::tests::testAdjustByDaysBigNumber`
   - lite: `Invalid Input Error: Type INT64 with value 12345678912 can't be cast because the value is out of range for the destination type INT32`
   - ledger: `pct/src/test/java/org/finos/legend/lite/pct/Test_LegendLite_EssentialFunctions_PCT.java:125` -- its comment: "TIMESTAMP-DOMAIN limit: these expected results live at years 1.4M-800M — beyond DuckDB's TIMESTAMP range entirely (290309 BC to 294246 AD). Representing them would mean a string-domain calendar reimplementation; ledgered as a carrier-domain limit (the in-domain BigNumber adjusts — MINUTES, MICROSECONDS — pass). B7 (RaisedErrors): NATIVE database errors now surface with their honest envelope — the class-blind strip is dead. The four BigNumber pins carry the SAME spellings upstream's own DuckDB ma"
6. Essential `meta::pure::functions::date::tests::testAdjustByHoursBigNumber`
   - lite: `Out of Range Error: Interval value 12345678912 hours out of range`
   - ledger: `pct/src/test/java/org/finos/legend/lite/pct/Test_LegendLite_EssentialFunctions_PCT.java:126` -- its comment: "TIMESTAMP-DOMAIN limit: these expected results live at years 1.4M-800M — beyond DuckDB's TIMESTAMP range entirely (290309 BC to 294246 AD). Representing them would mean a string-domain calendar reimplementation; ledgered as a carrier-domain limit (the in-domain BigNumber adjusts — MINUTES, MICROSECONDS — pass). B7 (RaisedErrors): NATIVE database errors now surface with their honest envelope — the class-blind strip is dead. The four BigNumber pins carry the SAME spellings upstream's own DuckDB ma"
7. Essential `meta::pure::functions::date::tests::testAdjustByMonthsBigNumber`
   - lite: `Invalid Input Error: Type INT64 with value 9600000000 can't be cast because the value is out of range for the destination type INT32`
   - ledger: `pct/src/test/java/org/finos/legend/lite/pct/Test_LegendLite_EssentialFunctions_PCT.java:127` -- its comment: "TIMESTAMP-DOMAIN limit: these expected results live at years 1.4M-800M — beyond DuckDB's TIMESTAMP range entirely (290309 BC to 294246 AD). Representing them would mean a string-domain calendar reimplementation; ledgered as a carrier-domain limit (the in-domain BigNumber adjusts — MINUTES, MICROSECONDS — pass). B7 (RaisedErrors): NATIVE database errors now surface with their honest envelope — the class-blind strip is dead. The four BigNumber pins carry the SAME spellings upstream's own DuckDB ma"
8. Essential `meta::pure::functions::date::tests::testAdjustByWeeksBigNumber`
   - lite: `Invalid Input Error: Type INT64 with value 12345678912 can't be cast because the value is out of range for the destination type INT32`
   - ledger: `pct/src/test/java/org/finos/legend/lite/pct/Test_LegendLite_EssentialFunctions_PCT.java:128` -- its comment: "TIMESTAMP-DOMAIN limit: these expected results live at years 1.4M-800M — beyond DuckDB's TIMESTAMP range entirely (290309 BC to 294246 AD). Representing them would mean a string-domain calendar reimplementation; ledgered as a carrier-domain limit (the in-domain BigNumber adjusts — MINUTES, MICROSECONDS — pass). B7 (RaisedErrors): NATIVE database errors now surface with their honest envelope — the class-blind strip is dead. The four BigNumber pins carry the SAME spellings upstream's own DuckDB ma"
9. Essential `meta::pure::functions::date::tests::testDayOfMonthError`
   - lite: `Assert failure at (resource:/platform/pure/essential/tests/assert.pure line:21 column:5), "Execution error column mismatch. Actual: 23 where expected: 36"`
   - ledger: `pct/src/test/java/org/finos/legend/lite/pct/Test_LegendLite_EssentialFunctions_PCT.java:108` -- no comment above the row's group
10. Essential `meta::pure::functions::date::tests::testHourError`
   - lite: `Assert failure at (resource:/platform/pure/essential/tests/assert.pure line:21 column:5), "Execution error column mismatch. Actual: 23 where expected: 36"`
   - ledger: `pct/src/test/java/org/finos/legend/lite/pct/Test_LegendLite_EssentialFunctions_PCT.java:109` -- no comment above the row's group
11. Essential `meta::pure::functions::date::tests::testMinuteError`
   - lite: `Assert failure at (resource:/platform/pure/essential/tests/assert.pure line:21 column:5), "Execution error column mismatch. Actual: 23 where expected: 36"`
   - ledger: `pct/src/test/java/org/finos/legend/lite/pct/Test_LegendLite_EssentialFunctions_PCT.java:110` -- no comment above the row's group
12. Essential `meta::pure::functions::date::tests::testNewDateError`
   - lite: `Assert failure at (resource:/platform/pure/essential/tests/assert.pure line:21 column:5), "Execution error column mismatch. Actual: 23 where expected: 29"`
   - ledger: `pct/src/test/java/org/finos/legend/lite/pct/Test_LegendLite_EssentialFunctions_PCT.java:111` -- no comment above the row's group
13. Essential `meta::pure::functions::date::tests::testSecondError`
   - lite: `Assert failure at (resource:/platform/pure/essential/tests/assert.pure line:21 column:5), "Execution error column mismatch. Actual: 23 where expected: 36"`
   - ledger: `pct/src/test/java/org/finos/legend/lite/pct/Test_LegendLite_EssentialFunctions_PCT.java:112` -- no comment above the row's group
14. Essential `meta::pure::functions::lang::tests::match::testMatchWithMixedReturnType`
   - lite: `scalar lowering not yet implemented for TypedDeactivate`
   - ledger: `pct/src/test/java/org/finos/legend/lite/pct/Test_LegendLite_EssentialFunctions_PCT.java:138` -- its comment: "deactivate() reflects the EXPRESSION (a ValueSpecification metamodel object) — legend-lite compiles to SQL and holds no expression tree at run time; metamodel reflection is out of vocabulary. 2026-08-27 leg 3b: the B lane PASSES this row (deactivate folds at TYPE time and the verdict reads the TypedTypeRef — B-FIXES-A); the A lane's composition EXECUTES the let eagerly, so the un-lowerable reflection carrier walls loudly at the Lowerer (correct: deactivate is compile-time and must never reach MI"
15. Essential `meta::pure::functions::math::tests::abs::testBigFloatAbs`
   - lite: `Assert failure at (resource:/platform/pure/essential/tests/assert.pure line:21 column:5), "\nexpected: 123456789123456789.99\nactual:   123456789123456780.0"`
   - ledger: `pct/src/test/java/org/finos/legend/lite/pct/Test_LegendLite_EssentialFunctions_PCT.java:55` -- its comment: "COLUMN-precision limit: message and LINE now match real pure exactly (SQL-raised guards + call-stack source info), but the failing call executes in DuckDB — its column within the test expression cannot be recovered without source-span plumbing through the wire. The reference adapter excludes these too. HARNESS SERIALIZATION LOSS: the wire serializes ^$x(prop = expr) as copy('', prop) — the override VALUE never reaches the text we compile. The ^$var(...) copy-with-update feature itself works (Typ"
16. Essential `meta::pure::functions::math::tests::trigonometry::testArcCosineError`
   - lite: `Assert failure at (resource:/platform/pure/essential/tests/assert.pure line:21 column:5), "Execution error message mismatch.\nThe actual message was "the database answered NaN for a Float, which has no NaN in Pure"\nwhere the expected message was:"Unable to compute acos of 2.0""`
   - ledger: `pct/src/test/java/org/finos/legend/lite/pct/Test_LegendLite_EssentialFunctions_PCT.java:152` -- its comment: "RELATIONAL DOMAIN SEMANTICS (batch 61, 2026-09-04): the Pure interpreter raises "Unable to compute acos of 2.0"; the engine's relational spec cell is the bare acos(%s), whose out-of-domain answer on H2 is NaN (the relational corpus asserts the row DROPS — testFilterUsingArcCosFunction). Every engine relational PCT adapter ledgers these two tests as expected failures (relational-h2 EssentialFunctions_manifest: "No error was thrown"; duckdb/postgres: the database's own error). Ours: the NaN cell c"
17. Essential `meta::pure::functions::math::tests::trigonometry::testArcSineError`
   - lite: `Assert failure at (resource:/platform/pure/essential/tests/assert.pure line:21 column:5), "Execution error message mismatch.\nThe actual message was "the database answered NaN for a Float, which has no NaN in Pure"\nwhere the expected message was:"Unable to compute asin of 2.0""`
   - ledger: `pct/src/test/java/org/finos/legend/lite/pct/Test_LegendLite_EssentialFunctions_PCT.java:153` -- its comment: "RELATIONAL DOMAIN SEMANTICS (batch 61, 2026-09-04): the Pure interpreter raises "Unable to compute acos of 2.0"; the engine's relational spec cell is the bare acos(%s), whose out-of-domain answer on H2 is NaN (the relational corpus asserts the row DROPS — testFilterUsingArcCosFunction). Every engine relational PCT adapter ledgers these two tests as expected failures (relational-h2 EssentialFunctions_manifest: "No error was thrown"; duckdb/postgres: the database's own error). Ours: the NaN cell c"
18. Essential `meta::pure::functions::string::tests::toString::testComplexClassToString`
   - lite: `Assert failure at (resource:/platform/pure/essential/tests/assert.pure line:21 column:5), "\nexpected: '// Warning: Good for gin -- Sad times no tonic'\nactual:   '@_o1'"`
   - ledger: `pct/src/test/java/org/finos/legend/lite/pct/Test_LegendLite_EssentialFunctions_PCT.java:85` -- its comment: "RELATIONAL substring is the engine's VERBATIM 1-based SQL passthrough (testFilterUsingParseIntegerFunction golden + rows); platform pure is 0-based — the reference DuckDB adapter LEDGERS the same divergence for these exact tests (its exclusions pin the identical off-by-one). The two key-sorts fail through the SAME cause: their sort keys ARE substrings (the comparator machinery itself is pinned green in ValueSortComparatorTest). G5 toString wall: a class instance's toString cannot lower to SQL (J"
19. Grammar `meta::pure::functions::collection::tests::filter::testFilterInstance`
   - lite: `Assert failure at (resource:/platform/pure/essential/tests/assert.pure line:21 column:5), "\nexpected: [<@_004ebqb instanceOf meta::pure::functions::collection::tests::model::CO_Person>, <@_004ebr2 instanceOf meta::pure::functions::collection::tests::model::CO_Person>]\nactual:   [<@_004f4e2 instanceOf meta::pure::functions::collection::tests::model::CO_Person>, <@_004f4e4 instanceOf meta::pure::f`
   - ledger: `pct/src/test/java/org/finos/legend/lite/pct/Test_LegendLite_GrammarFunctions_PCT.java:44` -- its comment: "NOTE 2026-10-20 (object identity, docs/OBJECT_IDENTITY_2026_10_20.md): testEqNonPrimitive passes -- every object carries its identity, and the adapter binds each captured object once by a let (capturedObjects, objectLets), so eq(, ) compares two objects and eq(, ) one. NOTE 2026-10-20 (the user's rule: as many tests as possible without hacks): testEqualNonPrimitive, testEqPrimitiveExtension and testEqualPrimitiveExtension pass -- the model the adapter hands over keeps <<equality.Key>> and the te"
20. Grammar `meta::pure::functions::collection::tests::first::testFirstComplex`
   - lite: `Assert failure at (resource:/platform/pure/essential/tests/assert.pure line:21 column:5), "\nexpected: <@_004gc0a instanceOf meta::pure::functions::collection::tests::model::CO_Person>\nactual:   <@_004gq34 instanceOf meta::pure::functions::collection::tests::model::CO_Person>"`
   - ledger: `pct/src/test/java/org/finos/legend/lite/pct/Test_LegendLite_GrammarFunctions_PCT.java:50` -- its comment: "testFirstComplex: nested-struct reconstruction now REBUILDS the CO_Person correctly (classPropertyTypeOf); the assert then requires the interpreter's ORIGINAL instance back — reference identity, unfixable over the wire (the reference adapter excludes it too, failing even earlier)"
21. Grammar `meta::pure::functions::collection::tests::map::testMapRelationshipFromManyToMany`
   - lite: `Assert failure at (resource:/platform/pure/essential/tests/assert.pure line:21 column:5), "\nexpected: [<@_004idqv instanceOf meta::pure::functions::collection::tests::map::model::M_Location>, <@_004idrq instanceOf meta::pure::functions::collection::tests::map::model::M_Location>, <@_004idsd instanceOf meta::pure::functions::collection::tests::map::model::M_Location>]\nactual:   [<@_004ikj3 instan`
   - ledger: `pct/src/test/java/org/finos/legend/lite/pct/Test_LegendLite_GrammarFunctions_PCT.java:60` -- its comment: "(getAll::testBasic FIXED 2026-08-28, metamodel-store leg: Class.all() is a mapped-class query over the seeded metamodel.classes table — the B1-era wall entry removed, the ledger failed loudly on the fix by design) INSTANCE IDENTITY through the wire: these asserts require the ORIGINAL let-bound instances back (assertIs / address-equal) — reference identity cannot cross a value serialization boundary; the reference adapter excludes this family too."
22. Grammar `meta::pure::functions::collection::tests::map::testMapRelationshipFromManyToOne`
   - lite: `Assert failure at (resource:/platform/pure/essential/tests/assert.pure line:21 column:5), "\nexpected: [<@_004ikrp instanceOf meta::pure::functions::collection::tests::map::model::M_Address>, <@_004iksk instanceOf meta::pure::functions::collection::tests::map::model::M_Address>]\nactual:   [<@_004ir13 instanceOf meta::pure::functions::collection::tests::map::model::M_Address>, <@_004ir15 instanceO`
   - ledger: `pct/src/test/java/org/finos/legend/lite/pct/Test_LegendLite_GrammarFunctions_PCT.java:61` -- its comment: "(getAll::testBasic FIXED 2026-08-28, metamodel-store leg: Class.all() is a mapped-class query over the seeded metamodel.classes table — the B1-era wall entry removed, the ledger failed loudly on the fix by design) INSTANCE IDENTITY through the wire: these asserts require the ORIGINAL let-bound instances back (assertIs / address-equal) — reference identity cannot cross a value serialization boundary; the reference adapter excludes this family too."
23. Grammar `meta::pure::functions::collection::tests::map::testMapRelationshipFromOneToOne`
   - lite: `Assert failure at (resource:/platform/pure/essential/tests/assert.pure line:21 column:5), "\nexpected: <@_004ir81 instanceOf meta::pure::functions::collection::tests::map::model::M_Address>\nactual:   <@_004j265 instanceOf meta::pure::functions::collection::tests::map::model::M_Address>"`
   - ledger: `pct/src/test/java/org/finos/legend/lite/pct/Test_LegendLite_GrammarFunctions_PCT.java:64` -- its comment: "OneToOne: the captured $address reaches the text since 2026-10-20 (the adapter binds captured objects by lets, object identity); its assertIs then needs the interpreter's own object back, as the two above"
24. Relation `meta::pure::functions::relation::tests::composition::testVariantArrayColumn_joinStrings`
   - lite: `Assert failure at (resource:/platform/pure/essential/tests/assert.pure line:21 column:5), "\nexpected: '#TDS\n   id,payload,joined\n   1,\'[1,2,3]\',1,2,3\n   2,\'[4,5,6]\',4,5,6\n   3,\'[7,8,9]\',7,8,9\n   4,\'null\',\n#'\nactual:   '#TDS\n   id,payload,joined\n   1,\'[1,2,3]\',1,2,3\n   2,\'[4,5,6]\',4,5,6\n   3,\'[7,8,9]\',7,8,9\n   4,\'null\',null\n#'"`
   - ledger: `pct/src/test/java/org/finos/legend/lite/pct/Test_LegendLite_RelationFunctions_PCT.java:45` -- its comment: "The result wire (TDS parser, null literals ["", "null"]) cannot represent an EMPTY STRING cell; joinStrings over the empty collection is '' — right in SQL, unrepresentable on the wire. Pin is the FULL expected+actual text, verbatim the official legend-engine DuckDB PCT's pin for the SAME failure — a loose fragment would keep matching if the test regressed elsewhere. 2026-09-10 (upstream boundary batch 1): variant cells spell \'…\' since the 5.92.0 TDS reader quotes with the Pure string form (Ren"

#### sqlite: the reference passes, lite fails

1. Essential `meta::pure::functions::collection::tests::at::testAtError`
   - lite: `Assert failure at (resource:/platform/pure/essential/tests/assert.pure line:21 column:5), "Execution error column mismatch. Actual: 23 where expected: 37"`
   - ledger: `pct/src/test/java/org/finos/legend/lite/pct/Test_LegendLite_SQLite_EssentialFunctions_PCT.java:42` -- its comment: "THE PLATFORM'S OWN (each pinned on the DuckDB lane too, by the same cause): name resolution, lowering and harness limits, error positions and messages"
2. Essential `meta::pure::functions::collection::tests::find::testFindInstance`
   - lite: `Assert failure at (resource:/platform/pure/essential/tests/assert.pure line:21 column:5), "\nexpected: <@_0030heh instanceOf meta::pure::functions::collection::tests::model::CO_Person>\nactual:   <@_0031aaq instanceOf meta::pure::functions::collection::tests::model::CO_Person>"`
   - ledger: `pct/src/test/java/org/finos/legend/lite/pct/Test_LegendLite_SQLite_EssentialFunctions_PCT.java:37` -- its comment: "INSTANCE IDENTITY (as on DuckDB): the asserts want the interpreter's ORIGINAL instances back, or eq/== by reference; the harness inlines captured instances BY VALUE, so identity is erased before the wire"
3. Essential `meta::pure::functions::collection::tests::find::testFindUsingVarForFunction`
   - lite: `Assert failure at (resource:/platform/pure/essential/tests/assert.pure line:21 column:5), "\nexpected: <@_00328jg instanceOf meta::pure::functions::collection::tests::model::CO_Person>\nactual:   <@_0033024 instanceOf meta::pure::functions::collection::tests::model::CO_Person>"`
   - ledger: `pct/src/test/java/org/finos/legend/lite/pct/Test_LegendLite_SQLite_EssentialFunctions_PCT.java:38` -- its comment: "INSTANCE IDENTITY (as on DuckDB): the asserts want the interpreter's ORIGINAL instances back, or eq/== by reference; the harness inlines captured instances BY VALUE, so identity is erased before the wire"
4. Essential `meta::pure::functions::collection::tests::head::testHeadComplex`
   - lite: `Assert failure at (resource:/platform/pure/essential/tests/assert.pure line:21 column:5), "\nexpected: <@_003a8dg instanceOf meta::pure::functions::collection::tests::model::CO_Person>\nactual:   <@_003amgm instanceOf meta::pure::functions::collection::tests::model::CO_Person>"`
   - ledger: `pct/src/test/java/org/finos/legend/lite/pct/Test_LegendLite_SQLite_EssentialFunctions_PCT.java:39` -- its comment: "INSTANCE IDENTITY (as on DuckDB): the asserts want the interpreter's ORIGINAL instances back, or eq/== by reference; the harness inlines captured instances BY VALUE, so identity is erased before the wire"
5. Essential `meta::pure::functions::date::tests::testAdjustByDaysBigNumber`
   - lite: `a date past the years 0000-9999 has no SQLite value`
   - ledger: `pct/src/test/java/org/finos/legend/lite/pct/Test_LegendLite_SQLite_EssentialFunctions_PCT.java:48` -- its comment: "2026-10-20, plan-route parity (b0d5e435, measured by the step-9 lane session): testFold, testFoldToMany and testFoldFiltering pass -- the adapter prints a copy with its updated values (CopyWithUpdate) 2026-11-19, A3 of the compatibility program, piece 2 (the user's word, 2026-11-19: "approved: push both A3 branches and make piece 2's moves"): indexOf is Pure's in the core, 0-based, -1 on a miss (S134), so indexOf::testSimple, testFromIndex and indexof::testIndexOfOneElement pass and leave (measu"
6. Essential `meta::pure::functions::date::tests::testAdjustByHoursBigNumber`
   - lite: `a date past the years 0000-9999 has no SQLite value`
   - ledger: `pct/src/test/java/org/finos/legend/lite/pct/Test_LegendLite_SQLite_EssentialFunctions_PCT.java:49` -- its comment: "2026-10-20, plan-route parity (b0d5e435, measured by the step-9 lane session): testFold, testFoldToMany and testFoldFiltering pass -- the adapter prints a copy with its updated values (CopyWithUpdate) 2026-11-19, A3 of the compatibility program, piece 2 (the user's word, 2026-11-19: "approved: push both A3 branches and make piece 2's moves"): indexOf is Pure's in the core, 0-based, -1 on a miss (S134), so indexOf::testSimple, testFromIndex and indexof::testIndexOfOneElement pass and leave (measu"
7. Essential `meta::pure::functions::date::tests::testAdjustByMinutesBigNumber`
   - lite: `a date past the years 0000-9999 has no SQLite value`
   - ledger: `pct/src/test/java/org/finos/legend/lite/pct/Test_LegendLite_SQLite_EssentialFunctions_PCT.java:73` -- its comment: "SQLITE'S OWN LIMIT: its dates are texts of the years 0000-9999 (a date function answers NULL past them, raised here; a five-digit year compares as text)"
8. Essential `meta::pure::functions::date::tests::testAdjustByMonthsBigNumber`
   - lite: `a date past the years 0000-9999 has no SQLite value`
   - ledger: `pct/src/test/java/org/finos/legend/lite/pct/Test_LegendLite_SQLite_EssentialFunctions_PCT.java:50` -- its comment: "2026-10-20, plan-route parity (b0d5e435, measured by the step-9 lane session): testFold, testFoldToMany and testFoldFiltering pass -- the adapter prints a copy with its updated values (CopyWithUpdate) 2026-11-19, A3 of the compatibility program, piece 2 (the user's word, 2026-11-19: "approved: push both A3 branches and make piece 2's moves"): indexOf is Pure's in the core, 0-based, -1 on a miss (S134), so indexOf::testSimple, testFromIndex and indexof::testIndexOfOneElement pass and leave (measu"
9. Essential `meta::pure::functions::date::tests::testAdjustByWeeksBigNumber`
   - lite: `a date past the years 0000-9999 has no SQLite value`
   - ledger: `pct/src/test/java/org/finos/legend/lite/pct/Test_LegendLite_SQLite_EssentialFunctions_PCT.java:51` -- its comment: "2026-10-20, plan-route parity (b0d5e435, measured by the step-9 lane session): testFold, testFoldToMany and testFoldFiltering pass -- the adapter prints a copy with its updated values (CopyWithUpdate) 2026-11-19, A3 of the compatibility program, piece 2 (the user's word, 2026-11-19: "approved: push both A3 branches and make piece 2's moves"): indexOf is Pure's in the core, 0-based, -1 on a miss (S134), so indexOf::testSimple, testFromIndex and indexof::testIndexOfOneElement pass and leave (measu"
10. Essential `meta::pure::functions::date::tests::testDayOfMonthError`
   - lite: `Assert failure at (resource:/platform/pure/essential/tests/assert.pure line:21 column:5), "Execution error column mismatch. Actual: 23 where expected: 36"`
   - ledger: `pct/src/test/java/org/finos/legend/lite/pct/Test_LegendLite_SQLite_EssentialFunctions_PCT.java:52` -- its comment: "2026-10-20, plan-route parity (b0d5e435, measured by the step-9 lane session): testFold, testFoldToMany and testFoldFiltering pass -- the adapter prints a copy with its updated values (CopyWithUpdate) 2026-11-19, A3 of the compatibility program, piece 2 (the user's word, 2026-11-19: "approved: push both A3 branches and make piece 2's moves"): indexOf is Pure's in the core, 0-based, -1 on a miss (S134), so indexOf::testSimple, testFromIndex and indexof::testIndexOfOneElement pass and leave (measu"
11. Essential `meta::pure::functions::date::tests::testHourError`
   - lite: `Assert failure at (resource:/platform/pure/essential/tests/assert.pure line:21 column:5), "Execution error column mismatch. Actual: 23 where expected: 36"`
   - ledger: `pct/src/test/java/org/finos/legend/lite/pct/Test_LegendLite_SQLite_EssentialFunctions_PCT.java:53` -- its comment: "2026-10-20, plan-route parity (b0d5e435, measured by the step-9 lane session): testFold, testFoldToMany and testFoldFiltering pass -- the adapter prints a copy with its updated values (CopyWithUpdate) 2026-11-19, A3 of the compatibility program, piece 2 (the user's word, 2026-11-19: "approved: push both A3 branches and make piece 2's moves"): indexOf is Pure's in the core, 0-based, -1 on a miss (S134), so indexOf::testSimple, testFromIndex and indexof::testIndexOfOneElement pass and leave (measu"
12. Essential `meta::pure::functions::date::tests::testMinuteError`
   - lite: `Assert failure at (resource:/platform/pure/essential/tests/assert.pure line:21 column:5), "Execution error column mismatch. Actual: 23 where expected: 36"`
   - ledger: `pct/src/test/java/org/finos/legend/lite/pct/Test_LegendLite_SQLite_EssentialFunctions_PCT.java:54` -- its comment: "2026-10-20, plan-route parity (b0d5e435, measured by the step-9 lane session): testFold, testFoldToMany and testFoldFiltering pass -- the adapter prints a copy with its updated values (CopyWithUpdate) 2026-11-19, A3 of the compatibility program, piece 2 (the user's word, 2026-11-19: "approved: push both A3 branches and make piece 2's moves"): indexOf is Pure's in the core, 0-based, -1 on a miss (S134), so indexOf::testSimple, testFromIndex and indexof::testIndexOfOneElement pass and leave (measu"
13. Essential `meta::pure::functions::date::tests::testNewDateError`
   - lite: `Assert failure at (resource:/platform/pure/essential/tests/assert.pure line:21 column:5), "Execution error column mismatch. Actual: 23 where expected: 29"`
   - ledger: `pct/src/test/java/org/finos/legend/lite/pct/Test_LegendLite_SQLite_EssentialFunctions_PCT.java:55` -- its comment: "2026-10-20, plan-route parity (b0d5e435, measured by the step-9 lane session): testFold, testFoldToMany and testFoldFiltering pass -- the adapter prints a copy with its updated values (CopyWithUpdate) 2026-11-19, A3 of the compatibility program, piece 2 (the user's word, 2026-11-19: "approved: push both A3 branches and make piece 2's moves"): indexOf is Pure's in the core, 0-based, -1 on a miss (S134), so indexOf::testSimple, testFromIndex and indexof::testIndexOfOneElement pass and leave (measu"
14. Essential `meta::pure::functions::date::tests::testSecondError`
   - lite: `Assert failure at (resource:/platform/pure/essential/tests/assert.pure line:21 column:5), "Execution error column mismatch. Actual: 23 where expected: 36"`
   - ledger: `pct/src/test/java/org/finos/legend/lite/pct/Test_LegendLite_SQLite_EssentialFunctions_PCT.java:56` -- its comment: "2026-10-20, plan-route parity (b0d5e435, measured by the step-9 lane session): testFold, testFoldToMany and testFoldFiltering pass -- the adapter prints a copy with its updated values (CopyWithUpdate) 2026-11-19, A3 of the compatibility program, piece 2 (the user's word, 2026-11-19: "approved: push both A3 branches and make piece 2's moves"): indexOf is Pure's in the core, 0-based, -1 on a miss (S134), so indexOf::testSimple, testFromIndex and indexof::testIndexOfOneElement pass and leave (measu"
15. Essential `meta::pure::functions::lang::tests::match::testMatchWithMixedReturnType`
   - lite: `scalar lowering not yet implemented for TypedDeactivate`
   - ledger: `pct/src/test/java/org/finos/legend/lite/pct/Test_LegendLite_SQLite_EssentialFunctions_PCT.java:57` -- its comment: "2026-10-20, plan-route parity (b0d5e435, measured by the step-9 lane session): testFold, testFoldToMany and testFoldFiltering pass -- the adapter prints a copy with its updated values (CopyWithUpdate) 2026-11-19, A3 of the compatibility program, piece 2 (the user's word, 2026-11-19: "approved: push both A3 branches and make piece 2's moves"): indexOf is Pure's in the core, 0-based, -1 on a miss (S134), so indexOf::testSimple, testFromIndex and indexof::testIndexOfOneElement pass and leave (measu"
16. Essential `meta::pure::functions::math::tests::abs::testBigFloatAbs`
   - lite: `Assert failure at (resource:/platform/pure/essential/tests/assert.pure line:21 column:5), "\nexpected: 123456789123456789.99\nactual:   123456789123456780.0"`
   - ledger: `pct/src/test/java/org/finos/legend/lite/pct/Test_LegendLite_SQLite_EssentialFunctions_PCT.java:65` -- its comment: "2026-10-20 (the parity phase): a Float root computed from exact-digit literals alone keeps its DECIMAL value on every other lane (LiteralSpelling.exactFloatConstant); SQLite has no exact decimal -- it reads the literal 123456789123456789.99 as a REAL, 15 to 17 digits -- so the digits are the database's limit 2026-10-20: the Float is the database's double, as for the same expression over a column (the special case that kept a literal-only Float a DECIMAL removed, as on the DuckDB lane) 2026-10-27"
17. Essential `meta::pure::functions::math::tests::trigonometry::testArcCosineError`
   - lite: `Assert failure at (resource:/platform/pure/essential/tests/assert.pure line:21 column:5), "Execution error message mismatch.\nThe actual message was "acos of a value outside [-1, 1] is NaN, which this database cannot hold"\nwhere the expected message was:"Unable to compute acos of 2.0""`
   - ledger: `pct/src/test/java/org/finos/legend/lite/pct/Test_LegendLite_SQLite_EssentialFunctions_PCT.java:66` -- its comment: "2026-10-20 (the parity phase): a Float root computed from exact-digit literals alone keeps its DECIMAL value on every other lane (LiteralSpelling.exactFloatConstant); SQLite has no exact decimal -- it reads the literal 123456789123456789.99 as a REAL, 15 to 17 digits -- so the digits are the database's limit 2026-10-20: the Float is the database's double, as for the same expression over a column (the special case that kept a literal-only Float a DECIMAL removed, as on the DuckDB lane) 2026-10-27"
18. Essential `meta::pure::functions::math::tests::trigonometry::testArcSineError`
   - lite: `Assert failure at (resource:/platform/pure/essential/tests/assert.pure line:21 column:5), "Execution error message mismatch.\nThe actual message was "asin of a value outside [-1, 1] is NaN, which this database cannot hold"\nwhere the expected message was:"Unable to compute asin of 2.0""`
   - ledger: `pct/src/test/java/org/finos/legend/lite/pct/Test_LegendLite_SQLite_EssentialFunctions_PCT.java:67` -- its comment: "2026-10-20 (the parity phase): a Float root computed from exact-digit literals alone keeps its DECIMAL value on every other lane (LiteralSpelling.exactFloatConstant); SQLite has no exact decimal -- it reads the literal 123456789123456789.99 as a REAL, 15 to 17 digits -- so the digits are the database's limit 2026-10-20: the Float is the database's double, as for the same expression over a column (the special case that kept a literal-only Float a DECIMAL removed, as on the DuckDB lane) 2026-10-27"
19. Essential `meta::pure::functions::string::tests::format::testFormatDate`
   - lite: `TIMEZONE reached SQLite: SQLite has no time zones but UTC`
   - ledger: `pct/src/test/java/org/finos/legend/lite/pct/Test_LegendLite_SQLite_EssentialFunctions_PCT.java:75` -- its comment: "SQLITE'S OWN LIMIT: no time zones but UTC"
20. Essential `meta::pure::functions::string::tests::toString::testComplexClassToString`
   - lite: `Assert failure at (resource:/platform/pure/essential/tests/assert.pure line:21 column:5), "\nexpected: '// Warning: Good for gin -- Sad times no tonic'\nactual:   '@_o1'"`
   - ledger: `pct/src/test/java/org/finos/legend/lite/pct/Test_LegendLite_SQLite_EssentialFunctions_PCT.java:70` -- its comment: "2026-10-20 (object identity): every object carries its __id now, so the default arm prints it (@_o1); the class's own toString() qualifier is still not in the model the adapter hands over, as on the DuckDB lane"
21. Grammar `meta::pure::functions::collection::tests::filter::testFilterInstance`
   - lite: `Assert failure at (resource:/platform/pure/essential/tests/assert.pure line:21 column:5), "\nexpected: [<@_004ebqb instanceOf meta::pure::functions::collection::tests::model::CO_Person>, <@_004ebr2 instanceOf meta::pure::functions::collection::tests::model::CO_Person>]\nactual:   [<@_004f4e2 instanceOf meta::pure::functions::collection::tests::model::CO_Person>, <@_004f4e4 instanceOf meta::pure::f`
   - ledger: `pct/src/test/java/org/finos/legend/lite/pct/Test_LegendLite_SQLite_GrammarFunctions_PCT.java:36` -- its comment: "INSTANCE IDENTITY (as on DuckDB): the asserts want the interpreter's ORIGINAL instances back, or eq/== by reference; the harness inlines captured instances BY VALUE, so identity is erased before the wire"
22. Grammar `meta::pure::functions::collection::tests::first::testFirstComplex`
   - lite: `Assert failure at (resource:/platform/pure/essential/tests/assert.pure line:21 column:5), "\nexpected: <@_004gc0a instanceOf meta::pure::functions::collection::tests::model::CO_Person>\nactual:   <@_004gq34 instanceOf meta::pure::functions::collection::tests::model::CO_Person>"`
   - ledger: `pct/src/test/java/org/finos/legend/lite/pct/Test_LegendLite_SQLite_GrammarFunctions_PCT.java:37` -- its comment: "INSTANCE IDENTITY (as on DuckDB): the asserts want the interpreter's ORIGINAL instances back, or eq/== by reference; the harness inlines captured instances BY VALUE, so identity is erased before the wire"
23. Grammar `meta::pure::functions::collection::tests::map::testMapRelationshipFromManyToMany`
   - lite: `Assert failure at (resource:/platform/pure/essential/tests/assert.pure line:21 column:5), "\nexpected: [<@_004idqv instanceOf meta::pure::functions::collection::tests::map::model::M_Location>, <@_004idrq instanceOf meta::pure::functions::collection::tests::map::model::M_Location>, <@_004idsd instanceOf meta::pure::functions::collection::tests::map::model::M_Location>]\nactual:   [<@_004ikj3 instan`
   - ledger: `pct/src/test/java/org/finos/legend/lite/pct/Test_LegendLite_SQLite_GrammarFunctions_PCT.java:38` -- its comment: "INSTANCE IDENTITY (as on DuckDB): the asserts want the interpreter's ORIGINAL instances back, or eq/== by reference; the harness inlines captured instances BY VALUE, so identity is erased before the wire"
24. Grammar `meta::pure::functions::collection::tests::map::testMapRelationshipFromManyToOne`
   - lite: `Assert failure at (resource:/platform/pure/essential/tests/assert.pure line:21 column:5), "\nexpected: [<@_004ikrp instanceOf meta::pure::functions::collection::tests::map::model::M_Address>, <@_004iksk instanceOf meta::pure::functions::collection::tests::map::model::M_Address>]\nactual:   [<@_004ir13 instanceOf meta::pure::functions::collection::tests::map::model::M_Address>, <@_004ir15 instanceO`
   - ledger: `pct/src/test/java/org/finos/legend/lite/pct/Test_LegendLite_SQLite_GrammarFunctions_PCT.java:39` -- its comment: "INSTANCE IDENTITY (as on DuckDB): the asserts want the interpreter's ORIGINAL instances back, or eq/== by reference; the harness inlines captured instances BY VALUE, so identity is erased before the wire"
25. Grammar `meta::pure::functions::collection::tests::map::testMapRelationshipFromOneToOne`
   - lite: `Assert failure at (resource:/platform/pure/essential/tests/assert.pure line:21 column:5), "\nexpected: <@_004ir81 instanceOf meta::pure::functions::collection::tests::map::model::M_Address>\nactual:   <@_004j265 instanceOf meta::pure::functions::collection::tests::map::model::M_Address>"`
   - ledger: `pct/src/test/java/org/finos/legend/lite/pct/Test_LegendLite_SQLite_GrammarFunctions_PCT.java:48` -- its comment: "THE PLATFORM'S OWN (each pinned on the DuckDB lane too, by the same cause): name resolution, lowering and harness limits, the 0-based indexOf, error positions and messages 2026-10-20, object identity (docs/OBJECT_IDENTITY_2026_10_20.md, b0d5e435, measured by the step-9 lane session): eq is identity and equal a keyed class's keys, and the adapter binds each captured object once by a let -- testEqNonPrimitive, testEqualNonPrimitive and the two primitive-extension tests pass OneToOne: the captured "
26. Relation `meta::pure::functions::relation::tests::composition::TestJoin_CurrentUserId`
   - lite: `CURRENT_USER_FN reached SQLite: SQLite has no users`
   - ledger: `pct/src/test/java/org/finos/legend/lite/pct/Test_LegendLite_SQLite_RelationFunctions_PCT.java:38` -- its comment: "SQLITE'S OWN LIMIT: no users (an embedded database has no session user)"
27. Relation `meta::pure::functions::relation::tests::composition::testVariantArrayColumn_joinStrings`
   - lite: `Assert failure at (resource:/platform/pure/essential/tests/assert.pure line:21 column:5), "\nexpected: '#TDS\n   id,payload,joined\n   1,\'[1,2,3]\',1,2,3\n   2,\'[4,5,6]\',4,5,6\n   3,\'[7,8,9]\',7,8,9\n   4,\'null\',\n#'\nactual:   '#TDS\n   id,payload,joined\n   1,\'[1,2,3]\',1,2,3\n   2,\'[4,5,6]\',4,5,6\n   3,\'[7,8,9]\',7,8,9\n   4,\'null\',null\n#'"`
   - ledger: `pct/src/test/java/org/finos/legend/lite/pct/Test_LegendLite_SQLite_RelationFunctions_PCT.java:36` -- its comment: "THE PLATFORM'S OWN (each pinned on the DuckDB lane too, by the same cause): name resolution, lowering and harness limits, the 0-based indexOf, error positions and messages"
28. Unclassified `meta::pure::functions::runtime::currentUserId::testCurrentUserId`
   - lite: `CURRENT_USER_FN reached SQLite: SQLite has no users`
   - ledger: `pct/src/test/java/org/finos/legend/lite/pct/Test_LegendLite_SQLite_UnclassifiedFunctions_PCT.java:35` -- its comment: "SQLITE'S OWN LIMIT: no users (an embedded database has no session user)"

#### h2: the reference passes, lite fails

1. Relation `meta::pure::functions::relation::tests::composition::testVariantArrayColumn_joinStrings`
   - lite: `collection reduction 'STRING_AGG' reached a dialect without a list encoding`
   - ledger: `pct/src/test/java/org/finos/legend/lite/pct/Test_LegendLite_H2_RelationFunctions_PCT.java:60` -- its comment: "NOTE 2026-10-20 (the parity phase): testCoalesceInPreFilter unpinned -- a literal list's filtered length is the sum of each element's predicate as a count (CarrierStrategies.filteredLiteralLength)"
2. Relation `meta::pure::functions::relation::tests::composition::testVariantArrayColumn_reverse`
   - lite: `LIST_REVERSE reached a dialect without a list encoding`
   - ledger: `pct/src/test/java/org/finos/legend/lite/pct/Test_LegendLite_H2_RelationFunctions_PCT.java:57` -- its comment: "NOTE 2026-10-20 (the parity phase): testCoalesceInPreFilter unpinned -- a literal list's filtered length is the sum of each element's predicate as a count (CarrierStrategies.filteredLiteralLength)"
3. Relation `meta::pure::functions::relation::tests::composition::testVariantArrayColumn_sort`
   - lite: `LIST_SORT reached a dialect without a list encoding`
   - ledger: `pct/src/test/java/org/finos/legend/lite/pct/Test_LegendLite_H2_RelationFunctions_PCT.java:54` -- its comment: "NOTE 2026-10-20 (the parity phase): testCoalesceInPreFilter unpinned -- a literal list's filtered length is the sum of each element's predicate as a count (CarrierStrategies.filteredLiteralLength)"
4. Relation `meta::pure::functions::relation::tests::composition::testVariantColumn_contains`
   - lite: `collection membership reached a dialect without a list encoding [collection: Cast]`
   - ledger: `pct/src/test/java/org/finos/legend/lite/pct/Test_LegendLite_H2_RelationFunctions_PCT.java:59` -- its comment: "NOTE 2026-10-20 (the parity phase): testCoalesceInPreFilter unpinned -- a literal list's filtered length is the sum of each element's predicate as a count (CarrierStrategies.filteredLiteralLength)"
5. Relation `meta::pure::functions::relation::tests::composition::testVariantColumn_distinct_removeDuplicates`
   - lite: `LIST_SORT reached a dialect without a list encoding`
   - ledger: `pct/src/test/java/org/finos/legend/lite/pct/Test_LegendLite_H2_RelationFunctions_PCT.java:53` -- its comment: "NOTE 2026-10-20 (the parity phase): testCoalesceInPreFilter unpinned -- a literal list's filtered length is the sum of each element's predicate as a count (CarrierStrategies.filteredLiteralLength)"
6. Relation `meta::pure::functions::relation::tests::composition::testVariantColumn_functionComposition`
   - lite: `LIST_FILTER reached a dialect without a list encoding`
   - ledger: `pct/src/test/java/org/finos/legend/lite/pct/Test_LegendLite_H2_RelationFunctions_PCT.java:50` -- its comment: "H2 has no list type: the list functions have no encoding on this dialect"
7. Relation `meta::pure::functions::relation::tests::composition::testVariantColumn_indexOf`
   - lite: `LIST_POSITION reached a dialect without a list encoding`
   - ledger: `pct/src/test/java/org/finos/legend/lite/pct/Test_LegendLite_H2_RelationFunctions_PCT.java:58` -- its comment: "NOTE 2026-10-20 (the parity phase): testCoalesceInPreFilter unpinned -- a literal list's filtered length is the sum of each element's predicate as a count (CarrierStrategies.filteredLiteralLength)"
8. Relation `meta::pure::functions::relation::tests::composition::testVariantMapColumn_keys_LateralFlatten`
   - lite: `UNNEST reached a dialect without an unnest placement`
   - ledger: `pct/src/test/java/org/finos/legend/lite/pct/Test_LegendLite_H2_RelationFunctions_PCT.java:65` -- its comment: "H2 has no UNNEST placement for a flatten"
9. Relation `meta::pure::functions::relation::tests::composition::testVariantMapColumn_values_LateralFlatten`
   - lite: `UNNEST reached a dialect without an unnest placement`
   - ledger: `pct/src/test/java/org/finos/legend/lite/pct/Test_LegendLite_H2_RelationFunctions_PCT.java:66` -- its comment: "H2 has no UNNEST placement for a flatten"
10. Relation `meta::pure::functions::relation::tests::extend::testVariantColumn_filter`
   - lite: `LIST_FILTER reached a dialect without a list encoding`
   - ledger: `pct/src/test/java/org/finos/legend/lite/pct/Test_LegendLite_H2_RelationFunctions_PCT.java:48` -- its comment: "H2 has no list type: the list functions have no encoding on this dialect"
11. Relation `meta::pure::functions::relation::tests::extend::testVariantColumn_fold`
   - lite: `fold reached a dialect without a fold encoding`
   - ledger: `pct/src/test/java/org/finos/legend/lite/pct/Test_LegendLite_H2_RelationFunctions_PCT.java:61` -- its comment: "NOTE 2026-10-20 (the parity phase): testCoalesceInPreFilter unpinned -- a literal list's filtered length is the sum of each element's predicate as a count (CarrierStrategies.filteredLiteralLength)"
12. Relation `meta::pure::functions::relation::tests::extend::testVariantColumn_map`
   - lite: `LIST_TRANSFORM reached a dialect without a list encoding`
   - ledger: `pct/src/test/java/org/finos/legend/lite/pct/Test_LegendLite_H2_RelationFunctions_PCT.java:56` -- its comment: "NOTE 2026-10-20 (the parity phase): testCoalesceInPreFilter unpinned -- a literal list's filtered length is the sum of each element's predicate as a count (CarrierStrategies.filteredLiteralLength)"
13. Relation `meta::pure::functions::relation::tests::filter::testVariantColumn_filterOutputFromLambda`
   - lite: `LIST_FILTER reached a dialect without a list encoding`
   - ledger: `pct/src/test/java/org/finos/legend/lite/pct/Test_LegendLite_H2_RelationFunctions_PCT.java:49` -- its comment: "H2 has no list type: the list functions have no encoding on this dialect"
14. Relation `meta::pure::functions::relation::tests::lateral::testLateralJoin`
   - lite: `Function "LATERAL" not found; SQL statement:\nSELECT "_tds0"."departmentId" AS "departmentId", "_tds0"."departmentName" AS "departmentName", "t0"."employeeId" AS "employeeId", "t0"."employeeName" AS "employeeName", "t0"."department" AS "department"\nFROM (VALUES (1, 'Engineering'), (2, 'Support'), (3, 'Sales')) AS "_tds0"("departmentId", "departmentName")\nCROSS JOIN LATERAL (\n  SELECT *\n  FROM `
   - ledger: `pct/src/test/java/org/finos/legend/lite/pct/Test_LegendLite_H2_RelationFunctions_PCT.java:71` -- its comment: "NOTE 2026-10-20 (the parity phase): testSimpleProject, testSimpleProjectList, testSimpleProjectWithEmpty and testFilterPostProject unpinned -- a lateral over a constant list is that list's rows, each struct field a column (ConstantLateralsAsRows) H2 has no LATERAL: the SQL lite renders is refused by the database"
15. Relation `meta::pure::functions::relation::tests::lateral::testLateralJoinAreInnerJoins`
   - lite: `Function "LATERAL" not found; SQL statement:\nSELECT "_tds0"."employeeId" AS "employeeId", "_tds0"."employeeName" AS "employeeName", "_tds0"."department" AS "department", "t0"."departmentId" AS "departmentId", "t0"."departmentName" AS "departmentName"\nFROM (VALUES (101, 'Rafael', 1), (102, 'Pierre', 1), (103, 'Jake', 1), (104, 'John', 2), (105, 'Jane', 2), (106, 'Alice', 3), (107, 'Bob', 3), (108`
   - ledger: `pct/src/test/java/org/finos/legend/lite/pct/Test_LegendLite_H2_RelationFunctions_PCT.java:73` -- its comment: "NOTE 2026-10-20 (the parity phase): testSimpleProject, testSimpleProjectList, testSimpleProjectWithEmpty and testFilterPostProject unpinned -- a lateral over a constant list is that list's rows, each struct field a column (ConstantLateralsAsRows) H2 has no LATERAL: the SQL lite renders is refused by the database"
16. Relation `meta::pure::functions::relation::tests::lateral::testLateralJoin_Chained`
   - lite: `Function "LATERAL" not found; SQL statement:\nSELECT "_tds0"."departmentId" AS "departmentId", "_tds0"."departmentName" AS "departmentName", "t0"."did" AS "did", "t0"."Budget" AS "Budget", "t0"."Program" AS "Program", "t1"."employeeId" AS "employeeId", "t1"."employeeName" AS "employeeName", "t1"."department" AS "department"\nFROM (VALUES (1, 'Engineering'), (2, 'Support'), (3, 'Sales')) AS "_tds0"`
   - ledger: `pct/src/test/java/org/finos/legend/lite/pct/Test_LegendLite_H2_RelationFunctions_PCT.java:72` -- its comment: "NOTE 2026-10-20 (the parity phase): testSimpleProject, testSimpleProjectList, testSimpleProjectWithEmpty and testFilterPostProject unpinned -- a lateral over a constant list is that list's rows, each struct field a column (ConstantLateralsAsRows) H2 has no LATERAL: the SQL lite renders is refused by the database"
17. Relation `meta::pure::functions::relation::tests::lateral::testLateralJoin_CorrelatedColumnNameAlsoOnInner`
   - lite: `Function "LATERAL" not found; SQL statement:\nSELECT "_tds0"."personId" AS "personId", "_tds0"."ref" AS "ref", "t0"."itemId" AS "itemId"\nFROM (VALUES (1, 10), (2, 20)) AS "_tds0"("personId", "ref")\nCROSS JOIN LATERAL (\n  SELECT "_tds1"."itemId" AS "itemId"\n  FROM (VALUES (10, 99), (30, 99)) AS "_tds1"("itemId", "ref")\n  WHERE "_tds1"."itemId" = "_tds0"."ref"\n) AS "t0" [90022-252]`
   - ledger: `pct/src/test/java/org/finos/legend/lite/pct/Test_LegendLite_H2_RelationFunctions_PCT.java:74` -- its comment: "NOTE 2026-10-20 (the parity phase): testSimpleProject, testSimpleProjectList, testSimpleProjectWithEmpty and testFilterPostProject unpinned -- a lateral over a constant list is that list's rows, each struct field a column (ConstantLateralsAsRows) H2 has no LATERAL: the SQL lite renders is refused by the database"
18. Relation `meta::pure::functions::relation::variant::tests::flatten::testFlatten_LateralJoin`
   - lite: `UNNEST reached a dialect without an unnest placement`
   - ledger: `pct/src/test/java/org/finos/legend/lite/pct/Test_LegendLite_H2_RelationFunctions_PCT.java:63` -- its comment: "H2 has no UNNEST placement for a flatten"
19. Relation `meta::pure::functions::relation::variant::tests::flatten::testFlatten_LateralJoin_Nested`
   - lite: `UNNEST reached a dialect without an unnest placement`
   - ledger: `pct/src/test/java/org/finos/legend/lite/pct/Test_LegendLite_H2_RelationFunctions_PCT.java:64` -- its comment: "H2 has no UNNEST placement for a flatten"
20. Relation `meta::pure::functions::relation::variant::tests::flatten::testFlatten_LateralJoin_Nested_Extend`
   - lite: `LIST_FILTER reached a dialect without a list encoding`
   - ledger: `pct/src/test/java/org/finos/legend/lite/pct/Test_LegendLite_H2_RelationFunctions_PCT.java:47` -- its comment: "H2 has no list type: the list functions have no encoding on this dialect"

#### the reference fails (core-compiled), lite passes; and both fail

- duckdb_grammar (reference fails, lite passes) `meta::pure::functions::math::tests::minus::testLargeMinus`: reference `Assert failure`
- duckdb_grammar (reference fails, lite passes) `meta::pure::functions::math::tests::plus::testLargePlus`: reference `Assert failure`
- duckdb_grammar (reference fails, lite passes) `meta::pure::functions::math::tests::times::testLargeTimes`: reference `Assert failure`
- duckdb_relation (reference fails, lite passes) `meta::pure::functions::relation::tests::composition::testExtendAddOnNull`: reference `\nexpected: '#TDS\n   id,grp,newCol\n   null,0,null\n   8,1,8\n   null,1,8\n   null,1,8\n   1,2,6\n   5,2,6\n   3,3,10\n   7,3,10\n   4,4,4\n   9,5,9\n#'\nactua`
- duckdb_relation (reference fails, lite passes) `meta::pure::functions::relation::tests::composition::testExtendFilterOutNull`: reference `\nexpected: '#TDS\n   p,o,i,newCol\n   300,2,20,30\n   300,1,10,30\n   200,3,30,80\n   200,3,30,80\n   200,1,10,80\n   200,1,10,80\n   100,3,30,60\n   100,2,20,`
- duckdb_relation (reference fails, lite passes) `meta::pure::functions::relation::tests::composition::testVariantColumn_slice`: reference `\nexpected: '#TDS\n   id,payload,fromRow,toRow,sliced\n   0,\'null\',1,2,\'null\'\n   1,\'[1,2,3,4,5,6]\',2,3,\'[3]\'\n   2,\'[1,2,3,4,5,6]\',0,4,\'[1,2,3,4]\'\`
- h2_relation (reference fails, lite passes) `meta::pure::functions::relation::tests::composition::testExtendAddOnNull`: reference `\nexpected: '#TDS\n   id,grp,newCol\n   null,0,null\n   8,1,8\n   null,1,8\n   null,1,8\n   1,2,6\n   5,2,6\n   3,3,10\n   7,3,10\n   4,4,4\n   9,5,9\n#'\nactua`
- h2_relation (reference fails, lite passes) `meta::pure::functions::relation::tests::composition::testExtendFilterOutNull`: reference `\nexpected: '#TDS\n   p,o,i,newCol\n   300,2,20,30\n   300,1,10,30\n   200,3,30,80\n   200,3,30,80\n   200,1,10,80\n   200,1,10,80\n   100,3,30,60\n   100,2,20,`
- h2_relation (both fail) `meta::pure::functions::relation::tests::composition::testVariantColumn_slice`: reference `\nexpected: '#TDS\n   id,payload,fromRow,toRow,sliced\n   0,\'null\',1,2,\'null\'\n   1,\'[1,2,3,4,5,6]\',2,3,\'[3]\'\n   2,\'[1,2,3,4,5,6]\',0,4,\'[1,2,3,4]\'\`; lite `LIST_SLICE reached a dialect without a list encoding`
- sqlite_grammar (both fail) `meta::pure::functions::math::tests::minus::testLargeMinus`: reference `Assert failure`; lite `an integer past 64 bits has no SQLite value`
- sqlite_grammar (both fail) `meta::pure::functions::math::tests::plus::testLargePlus`: reference `Assert failure`; lite `an integer past 64 bits has no SQLite value`
- sqlite_grammar (both fail) `meta::pure::functions::math::tests::times::testLargeTimes`: reference `Assert failure`; lite `an integer past 64 bits has no SQLite value`
- sqlite_relation (reference fails, lite passes) `meta::pure::functions::relation::tests::composition::testExtendAddOnNull`: reference `\nexpected: '#TDS\n   id,grp,newCol\n   null,0,null\n   8,1,8\n   null,1,8\n   null,1,8\n   1,2,6\n   5,2,6\n   3,3,10\n   7,3,10\n   4,4,4\n   9,5,9\n#'\nactua`
- sqlite_relation (reference fails, lite passes) `meta::pure::functions::relation::tests::composition::testExtendFilterOutNull`: reference `\nexpected: '#TDS\n   p,o,i,newCol\n   300,2,20,30\n   300,1,10,30\n   200,3,30,80\n   200,3,30,80\n   200,1,10,80\n   200,1,10,80\n   100,3,30,60\n   100,2,20,`
- sqlite_relation (reference fails, lite passes) `meta::pure::functions::relation::tests::composition::testVariantColumn_slice`: reference `\nexpected: '#TDS\n   id,payload,fromRow,toRow,sliced\n   0,\'null\',1,2,\'null\'\n   1,\'[1,2,3,4,5,6]\',2,3,\'[3]\'\n   2,\'[1,2,3,4,5,6]\',0,4,\'[1,2,3,4]\'\`

## In short

| database | tests | both pass | reference passes, lite fails | reference fails, lite passes | both fail |
|---|---:|---:|---:|---:|---:|
| DuckDB (5 suites) | 1,249 | 1,219 | 24 | 6 | 0 |
| SQLite (5 suites) | 1,249 | 1,215 | 28 | 3 | 3 |
| H2 (Relation) | 469 | 446 | 20 | 2 | 1 |

Against `core-interpreted` instead: DuckDB 24 lite failures where the interpreter passes, SQLite 31, H2 19. Lite passes
all 7 of core-interpreted's failures on DuckDB and on SQLite; on H2 it passes 3 of the 5 relation rows (the two it fails,
`testVariantColumn_slice` and `testVariantColumn_filter`, are H2 list-encoding refusals).

What the lite failures are, by kind (DuckDB's 24): instance identity across the wire (5 grammar + 3 essential:
`testFindInstance`, `testFindUsingVarForFunction`, `testHeadComplex`, `testFilterInstance`, `testFirstComplex`, the three
`testMapRelationship*`), the error column (6), the database's own error for a date beyond its range (4
`testAdjustBy*BigNumber`), NaN for acos/asin (2), `testBigFloatAbs` (S120), `testComplexClassToString`,
`testMatchWithMixedReturnType` (refused: `scalar lowering not yet implemented for TypedDeactivate`),
`testVariantArrayColumn_joinStrings` (a null array element joined). SQLite adds its date range (5), 64-bit integers
(3, which the compiled reference also fails), time zones (`testFormatDate`) and `currentUserId` (2). H2's 21 failures are 17
list/unnest/fold encodings H2's dialect refuses (`... reached a dialect without a list encoding`) and 4 `LATERAL`
statements H2 rejects (`Function "LATERAL" not found`); `testVariantColumn_slice` is one the compiled reference fails too,
so 20 land in "reference passes, lite fails".

## Channel B vs channel A (same database), from the committed recordings

Channel B's per-test outcomes are committed by the edge golden net
(`pct/src/test/resources/edge/channel_b_<db>_<suite>_outcomes.tsv`, every outcome; no channel-B run was made here).
Compared test by test with the raw channel-A outcomes above:

| lane | both pass | A fails, B passes | A passes, B fails | both fail |
|---|---:|---:|---:|---:|
| DuckDB essential | 327 | 12 | 0 | 6 |
| DuckDB grammar | 132 | 5 | 0 | 0 |
| DuckDB relation | 468 | 1 | 0 | 0 |
| DuckDB standard | 205 | 0 | 0 | 0 |
| DuckDB unclassified | 93 | 0 | 0 | 0 |
| SQLite essential | 324 | 12 | 1 | 8 |
| SQLite grammar | 129 | 5 | 0 | 3 |
| SQLite relation | 457 | 1 | 10 | 1 |
| SQLite standard | 205 | 0 | 0 | 0 |
| SQLite unclassified | 92 | 0 | 0 | 1 |
| H2 relation | 436 | 0 | 12 | 21 |

They do not agree everywhere. Where channel A fails and B passes (36), the failure is channel A's boundary, not the
value: instance identity across the wire, the error's column, `testBigFloatAbs` (B compares the two doubles in the
database), `testComplexClassToString`, `testMatchWithMixedReturnType`, `testVariantArrayColumn_joinStrings`. Where A
passes and B fails (23): on H2 and SQLite, B plans the whole body and meets dialect refusals A never reaches because the
interpreter computes that part (`STRING_AGG`/`LIST_GET` without a list encoding on H2 -- the 9 `testSimpleGroupBy_*`,
`testCoalesceInPreFilter`, `testExtendJoinStringOnNull`, `testOLAPAggStringWithPartitionAndUnboundedWindow`,
`testJoinStrings_GroupBy_Unordered`; SQLite's non-literal `regexp_extract_all` and its `testDateDiffWithDifferentTimeZones`
"expected: [0], actual: []"). Channel B's ledger is `pct/src/test/resources/org/finos/legend/lite/pct/channelb/ledger.tsv`.
