# The assert, wall and lean-SQL audit (2026-11-04)

The user's request (2026-11-04), two things in parallel: (1) a deep audit of the PCT and core work -- that everything
really goes through the product path, that no `assert*` function is special-cased, that "assert* functions really are
like any other SQL lowering function on the database lane", the bar being "anyone can write a NEW test function using
any of the assert* constructs and they should just do what they say they do on the tin"; and the plan/exec split --
"the plan and compiler pipeline really can only plan ... and same on the flip side that the execute cannot reach back
into the compiler", and that "the driver" is not a back door; (2) lean SQL on the product path before the corpus.

Two independent read-only audits (one per question, briefed with code pointers, not conclusions), every headline claim
spot-checked in the code, and the assert audit's probes RUN (`core/src/test/java/com/legend/driver/AssertProbeTest.java`,
measurement only: it prints, asserts nothing).

## 1. The assert family: the bar is not met

**Measured** (`AssertProbeTest`, DuckDB, each probe under the host judge, the database judge alone and BOTH; the
expected outcome is upstream's): of 37 probes, **10** behave as upstream under every judge; **14 are refused or crash**;
**4 decide the wrong verdict**; **9 fail with the wrong words**.

| Outcome | Probes |
|---|---|
| Right under every judge | `assertEquals` mixed list, empty vs filtered, Integer vs Float, two asserts the second failing, a class value's property, `assertEmpty`, `assertSameElements`, `assertEqWithinTolerance`, `assertInstanceOf(at(0))`, a two-statement helper |
| Refused ("the walk is closed") or crashing | `assertNotEq`, `assertNotSize`, `assertNotContains` (lite's own prelude: each reduces to `assert`); a one-statement helper `function t::check(x){ assertEquals(2, $x) }`; an assert in an `if`; an assert bound by a `let`; an assert then a query; `range(1,4)->forAll(...)`; `assertInstanceOf(1, Integer, 'msg')`; `assertIs(1, 1)` (host: "no scalar lowering registered ... implemented by [Verdict]"); a zip `forAll` with computed arguments (refused by name); `assertError` with a `let` message (host: refused; database: `NullPointerException`); `assertError` over a failing assert |
| Wrong verdict | `assertEquals([3, 1, 2], [1, 2]->concatenate([3]))` and `->union([3])` PASS (upstream fails: the multiset leniency, S131, reaches collection `concatenate`/`union`); `assertIs(t::Employee, [^Employee, ^Person]->at(0)->type())` FAILS (decided at planning from the static type); `assertEquals('x=1.0000000000000002', 'x=1.0')` the host PASSES, the database fails, BOTH reports the judges disagree (S132's float tokens applied to any String on the host only) |
| Wrong words | a custom message is dropped under every judge: `assert(c, 'm')`, `assertFalse(c, 'm')`, a computed message, `assertEquals(1, 2, 'custom')`, the format overload, a zip `forAll(p | assert(..., 'm'))` -- each "Assert failed" or the default text; the database judge alone words three otherwise: `assertSize` (`\nexpected: 3\nactual: 2`, the host `expected size: 3, actual size: 2`), `assertContains`, a Decimal (`3.0` where the host prints `3.0D`) -- BOTH prints the host's, so it hides them |

**Why (code facts, each checked):** the Lowerer refuses every assert native -- "no scalar lowering registered ...
implemented by [Verdict], not as a scalar" (`lowering/NoRule.java:38-43`); an assert is a statement-position form that
Java dispatches by family and argument shape (`driver/AssertVerdicts.java` 2,548 lines, `SqlTextVerdicts` 2,151,
`HostJudge` 1,699, `DatabaseJudge` 917), admitted only as a statement on its own (`testplan/BodyClass.java`); both
judges hard-code the condition family's text (`HostJudge.conditionOf`: `fail("Assert failed")`; `DatabaseJudge`'s
condition passes `"Assert failed"`); no other family reads its message argument; the database judge plans only what the
host planner plans (`databaseGate` calls `planHost` first); a one-statement helper is reduced only when it "asserts a
class value or reads a string entry" (`BodyDriver.java:313-365`, a rule shaped by two corpus witnesses). Meanwhile
`fail` HAS a real SQL lowering (`Scalars.java:246-254`, `PureSql.raise`) and `LambdaBodies` already lowers
`assert(c, m); v` as `if(c, |v, |fail(m))` -- the generic machinery exists; statement asserts do not use it.

**Other findings** (the audit's, ranked medium): recognizers keyed on the engine corpus's helper names or golden text
(`assertSameSQL`, TDG `assertSqlEquals`, `assertEqualsH2Compatible`, the lineage tree print's `startsWith("root\n")`
rewrite, the `'TDSNull'` sentinel equal to a null by value, the `forAll`-contains subset rewrite, `distinct == [true]`),
none a register row; the multiset leniency keyed on `args.get(1)` only (swapping the sides changes the outcome); core's
CI runs the host judge only (`core/BUILD.bazel` sets no `legend.test.judge`) and PCT channel B defaults to BOTH, where
the host absorbs the database judge's declines -- the 2026-11-04 "database judge decides every assert" result was a
manual run. Nothing anywhere decides a verdict by test name.

**Direction the audit proposes:** give the assert family real lowerings -- upstream's own Pure bodies over `assert`, and
`assert(c, m)` as `if(c, |true, |fail(m))` with the message carried -- so an assert is an ordinary function: in an
`if`, a `let`, a helper, a lambda, a `forAll` over any source, with its own message, decided by the database; the
shape recognizers become deletable.

## 2. The plan/exec wall: real between libraries, not at run time

**What the driver is** (`com.legend.driver`): the code that turns a TEST BODY (lets, effects, asserts, a last answer)
into database work and decides its asserts -- the product query road (`Execution.run` -> `TypedQuery` -> `PlanRunner`)
needs none of it. It holds the program road (`BodyRouter`: plan the asserts, plan the body, run it once, judge each
verdict step's rows in a sink) and the walk (`BodyDriver`: plan one statement, send it, read it, plan the next). Its
Bazel target depends on the planner, compiler, lowering and dialects AND on `exec` -- it sits on both sides by design.

**Leaks found (call chains in the audit; L1 checked in the code):**

1. **L1 (critical) -- the database judge plans and sends SQL inside the running program.** `BodyRouter`'s sink ->
   `DatabaseAsserts.received` -> `VerdictBatch.Planned.judge` -> `executeOne`: `runner.plan(vq, ...)`
   (`judge/VerdictBatch.java:702`, a render through the dialect) then `PlanRunner.program(...)` (`:712`) on the same
   session, from inside the outer program's sink. Triggered by the split rung (a rejected fused statement), a member the
   dialect cannot spell, the appeals, and the `orElse` fallbacks added 2026-11-04 (the rendered-text rows fallback, this
   session's own work).
2. **L2 (critical) -- the SQL-text appeal recompiles, plans and executes mid-judgment**, and through the corpus
   harness's `provideStore` can run a whole other program (`test/PureTestRunner.java:512` -> `Execution.runProgram`).
3. **L3 (high) -- the walk is open by default** (`WalkDoor`: no property, allowed); the product's test runners and the
   corpus walk freely.
4. **L4 (high) -- "planning sends nothing" is a mode flag, not a structure**: the assert planners hold the live session
   and hooks that can execute (system-store seeding reachable from side planning; the splice hook's eager inline
   execute; `evalSide`/`runVerdict`/`buildFrame` choosing plan-or-send by flags); `JudgePlans.target` reads a live
   session's version.
5. **L5 (medium)** -- a compiler rewrite (`ResultEnvelopeSplice`) calls back through an interface whose driver
   implementation executes. **L6** -- the test-data generator plans from rows it read (D30's named residue). **L7** --
   the compiled model holds live system sessions; `TypedQuery.specs()`/`resolved()` are public.

**What the guards can and cannot see:** Bazel strict deps and the ArchUnit rules stop planning classes from naming
`exec` and `exec` from naming planning classes; JDBC is confined. They cannot see the bridge libraries (`driver`,
`judge`, `testdatagen`) that may name both, calls through interfaces (`Runner.plan`, `Appeal`, `ProgramSink`,
`AssertListener`), or runtime flags; `PlanRunner`'s public one-statement methods are called from 18 sites in 8 files
outside `exec` (`BodyDriver` 4, `TestDataGenerator` 4, `JudgePlans` 3, `VerdictBatch` 2, `Execution` 2, `HostSides`,
`BodyRouter`, `ConnectionResolver`). No runtime check asserts "not while planning" or "not while running".

**The proposed wall:** planning takes target FACTS, never a session, and yields a sealed plan; the runner's sinks are
leaves (never plan, never run); the judge is a pure function of rows; every fallback is a planned step or a column of
the same statement; the walk closed by default, then deleted. Guards: method-level call-graph rules from every sink and
every planner entry (interfaces resolved to their implementations), registers of the crossing interfaces and the
runner's outside call sites (shrink-only), a runtime phase seal (`sent` and `Sessions.open` throw while planning; the
planner, Lowerer and SQL writer throw while running; a nested run throws), and census counters
(`SENT_WHILE_PLANNING`, `PLANNED_WHILE_RUNNING`, `NESTED_RUNS`).

## 3. Lean SQL on the product path (measured)

`-Dlegend.diagnostics=dump-sql`, a structural census per statement (SELECT count, the depth at which a SELECT opens,
repeated parenthesized sub-expressions of 40+ characters).

| Product SQL | Statements | Chars median / p99 / max | SELECTs median / max | Depth median / p99 / max | Repeating a sub-expression |
|---|---|---|---|---|---|
| PCT channel A, DuckDB (the product door, `Execution.run`) | 2,492 | 83 / 5,539 / 9,027 | 1 / 9 | 0 / 2 / 5 | 220 (9%) |
| Core, queries run through `Execution.run` | 4,808 | 98 / 4,054 / 28,816 | 1 / 87 | 0 / 7 / 25 | 419 (9%) |
| Core, the metamodel queries (a body's non-assert statements) | 221 | 328 / 83,329 / 83,330 | 3 / 35 | 2 / 7 / 8 | 26 |

**Reading it:** a PCT-like query is flat (one SELECT, depth 0 at the median); the waste is REPETITION and a few
families: (1) an instance literal spelled again for every property read (`struct_extract({...whole struct...}, 'name')`
per field: about 58 statements, 237K repeated characters); (2) a variant/JSON coercion repeating its argument
(`CASE WHEN json_type(X) = 'ARRAY' THEN ... X ... WHEN X IS NULL ...`), multiplying when chained; (3) small wrappers --
`> ALL (SELECT ... FROM (SELECT ... HAVING ...) WHERE x IS NOT NULL)` (a level for a NULL filter the HAVING can hold),
`sort()` of a mixed-number list computing its key twice. **Depth is not bloat by itself:** core's 20-deep statement is
twenty window columns each reading the last (`UserFunctionIntegrationTest#aChainOfCallsIsTypedOnceEach`) -- minimal.
**The metamodel query's bloat is a generic lowering shape:** a class hierarchy mapped onto one table with a
discriminator becomes one `UNION ALL` branch per concrete subtype over the SAME table, differing only in
`WHERE kind = '...'` (five branches for the relational elements, about twenty for the data types), each carrying a NULL
column for every other subtype's properties -- where one `SELECT ... WHERE kind IN (...)` would do (the "41 levels" of
`CORE_SQL_SIZE_2026_11_04.md` were the alias counter, not nesting: the real depth is 5-8). SQLite's instance
collections nest a JSON level per list operation (25 deep at worst).

**Existing guards:** 69 SELECT-count pins in 8 core test files (`LowerRelationDuckDbTest` 28, `ResolveSimpleClassTest`
11, `ResolveNavigationTest` 9, `JoinTortureTest` 8, ...), none loosened in the local history -- relation pipelines and
simple class queries. **Not covered:** collection functions, instance literals, variants, subtype hierarchies, the
metamodel, repeated expressions anywhere.
