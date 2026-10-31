# C. M0: the measurements (the test-is-the-product plan's step M0, 2026-10-31)

The plan's M0 (`docs/TEST_IS_THE_PRODUCT_PLAN_2026_10_31.md` §7) measures everything and changes nothing in the
product. Every count below comes from one research patch (never committed). It is saved here as text:
`probes/M0Census.java.txt` (the collector), `probes/m0-hooks.diff.txt` (the hooks in the driver, the planner, the
judges and `ScanOrder`), and `probes/m0-agg.py.txt` (the aggregator). The patch ran on the S4a code (8a842acb).

The runs:

| Run | What | Outcomes |
|---|---|---|
| **base** | `//spec:corpus_duckdb`, `//spec:corpus_h2` (host and database passes), the five `//pct:pct_channel_b_*` suites, every hook on | identical to the baseline: DuckDB host 2471/108, database 2473/108; H2 host 2289/295, database 2233/353; channel B 5/5; the census lines (`statement-origins`) identical, `probe=0` |
| **early** | the same corpora with the MV1 experiment (`M0Census.CLOSE_EARLY`: a verdict group closes before every non-verdict step) | identical pass/fail on both corpora; see §3 |

Two artefacts of the probe itself, not movements:
- The product road's extra renders count as firings of the DuckDB engine-compatibility order key (the census
  attributes `StableScanOrder.firings` per test). The two DuckDB engine-order registers therefore moved in the
  instrumented runs (`update_rcorpus_duckdb_3/4`), and the `StableScanOrder` rows of §6's R8 table are inflated.
- In the early run, the outside-body register moved by construction (§3).

Columns below: `chB` is the five channel B suites summed (each suite runs both judges). The corpus columns are one
pass each.

## 0. The answers in plain words

1. **Where both roads plan a statement, the product's SQL is the test's SQL.** Of 34,463 side or value plans that both
   roads made, 30,448 are byte-identical and 3,250 more differ only in alias numbering. All 765 that really differ do so
   for a known lowering setting:
   - the identity lane's layout (test-only, the canon);
   - the streaming graph root (D7);
   - the post-processors (D10);
   - the time zone (D4);
   - declared egress (D3).

   None differs in the front half (inlining, resolution, staging). The product road's other refusals are all product
   features the test road has and the product road lacks, chiefly `execute(...)` read as a value (D15): 18,008 of the
   19,569 refusals.
2. **MV1 moves no test.** Closing a verdict group before every non-verdict step changes no outcome and no message on
   either corpus (only the random trace ids differ). It adds 24 fused groups on DuckDB and 9 on H2. It touches 42
   bodies (44 value statements) per corpus.
3. **MV8 is at most one test.** On DuckDB, all 108 failing tests fail with the same kind of error in both passes.
   The database pass plans every side of a segment before it judges, so it is the closest thing today to a
   whole-body compile. On H2 one test changes kind (`DataError` to `DialectCapability`).
4. **The four lowering-time `ModelException` guards are run-time errors by Pure's meaning.** Tests expect each of them
   through `assertError`: duration unit, window frame, and twice "type not supported yet". Under R14 they become
   planned raises. Every other planning-time failure is an unsupported or invalid program, so a compile error.
5. **Several of the plan's risks are empty in this corpus:**
   - T8: a generator fold never changes a statement's type (41 folds).
   - R13: no effect string argument is computed (124,975 literal, 0 computed).
   - D18: no effect let reaches the cross-store gap.
   - F11: no fused group is sent inside a body transaction.
   - D30: no `toOne()` reader fires.
   - MV2: the two "AssertVerdicts.side" texts appear in no output.
   - The grid-pair Java arm never runs in the database judge.

## 1. The parity census (H1-H3: each statement or side planned both ways)

**How the census was taken.**

*The pairs.* Every lowering the test road performs is paired with the same source planned the PRODUCT way:
- the program's lets as written (frames and effect lets included, the enclosing bodies' first), then the statement or
  side;
- `UserCallInliner` with no hook, the plain `StoreResolver` and `CrossStoreGuard`;
- `TypedQuery.lowerer`'s configuration: object layouts, instance keys, one `ObjectSites` per program (B9), the
  exists-join form unless temporal, the streaming root, the features;
- then `EnumValueTables` and the dialect's render.

*The classes.* Each pair falls into one class:
- **same**;
- **numbering-only** (equal after ids and alias numbers are normalized);
- **diff lowering**: the test body re-lowered with the product's configuration equals the product's SQL, attributed to
  each test-road factor that alone changes it;
- **diff front**: a difference that is not the lowering's;
- **product-refuses**.

The three roads: `value` is a value statement, `host-side` a host judge's side, `db-side` a database judge's side
(before the canon wrap).

| | chB | duckdb/database | duckdb/host | h2/database | h2/host |
|---|---|---|---|---|---|
| db-side planned | 10,050 | 7,911 | -- | 8,055 | -- |
| ... same | 8,552 | 3,930 | -- | 4,030 | -- |
| ... numbering-only | 1,244 | 188 | -- | 185 | -- |
| ... diff lowering | 254 | 99 | -- | 11 | -- |
| ... product refuses | 0 | 3,694 | -- | 3,829 | -- |
| host-side planned | 9,868 | 1,368 | 7,742 | 1,352 | 7,516 |
| ... same | 8,370 | 51 | 2,756 | 51 | 2,676 |
| ... numbering-only | 1,244 | 4 | 191 | 4 | 188 |
| ... diff lowering | 254 | 9 | 109 | 9 | 20 |
| ... product refuses | 0 | 1,304 | 4,686 | 1,288 | 4,632 |
| value planned | 34 | 34 | 34 | 34 | 34 |
| ... same / numbering-only | 32 / 2 | 0 | 0 | 0 | 0 |
| ... product refuses | 0 | 34 | 34 | 34 | 34 |
| **diff front** | **0** | **0** | **0** | **0** | **0** |
| side folded to a constant (no plan) | 44 | 3,446 | 4,688 | 3,446 | 4,613 |
| answered with no plan (effects: seeds, scripts) | 0 | 124,329 | 124,329 | 124,328 | 124,329 |

Lowerings the census does not pair:
- the database mode's frame chains (`BodyPlanner.bare`, 1,467 per pass);
- the host mode's eager frame runs (1,975 DuckDB, 1,960 H2: decision 1's eager run at a let, MV7).

Frames are D15's product feature, which the product road does not have, so the census cannot compare them yet.

**The differences, by the factor that causes them** (one example each, at the first differing character):

| Factor | Count (all columns) | What it is | Example |
|---|---|---|---|
| LAYOUT (+LANE 32, +SEED 40) | 682 | the identity lane's object layout: each object under a verdict carries its canon (T-construct, R10) | TEST `list_extract(list_transform([{'parts': ['firmTable'], '__id': ..., '__type': '...QualifiedName'}], __s -> struct_insert(__s, "__canon" := ...)), 1)`; PRODUCT `{'parts': ['firmTable'], '__id': ..., '__type': '...QualifiedName'}` |
| STREAM | 64 | D7: the test road aggregates a graph fetch into one JSON array cell; the product streams one object per row | TEST `SELECT CAST(coalesce(to_json(list(json_object('name', t0.name) ORDER BY t0.id ...)), '[]') AS VARCHAR)`; PRODUCT `SELECT CAST(json_object('name', t0.name) AS VARCHAR) ... ORDER BY t0.id` |
| TZ+STREAM | 4 | D4 (the connection's time zone) with D7 | a graph fetch with a `settlementDateTime` column |
| POST | 12 | D10: a frame's `tableReplace` post-processor | TEST `FROM otherFirmTable ... differentPersonTable`; PRODUCT `FROM firmTable ... personTable` |
| EGRESS | 3 | D3: declared egress on the value lane | TEST `to_json(CASE WHEN {'__id': 'i14', ...} IS NULL THEN NULL ELSE {...} END)`; PRODUCT `to_json({'__id': 'o14', ...})` |
| SEED | 0 after numbering | D9: the seedable let prefix; channel B's 1,242 SEED differences were alias numbers only (`t8_a` against `t0_a`, `_tds2` against `_tds0`) | -- |

**Why the product road refuses** (the 19,569 refusals over all columns, grouped by the product feature the test road
has):

| Group | Count | Plan rows |
|---|---|---|
| `execute(...)` read as a value: `$result.values`, `execute(...).values->...` ("class query under TypedPropertyAccess / TypedMap is not resolvable yet") | 18,008 | D15, D32 |
| the execute call's mapping (`runtime ... has N mappings binding class`), `getAll` with no mapping in scope, store resolution leaving a getAll | about 870 | D15 |
| let-bound values the test road seeds or binds (dynamic slice bounds, a filter over a whole variable, `UnfoldableRef`, a join reading `$var`, a struct literal or `UNNEST` H2 cannot place) | about 350 | D9, D13 |
| `$plan` and metamodel values (`no SQL type for Pure class ...TableAlias`) | 116 | D13 |
| test-data generator carriers (`TypedTestDataGen`, `TypedCsvCensus`) | 100 | T8 |
| helper calls the test road inlines through its hooks (`TypedUserCall`, `TypedSerialize`) | about 100 | D13, D14 |
| a keyed pivot asked for as text (an artefact of the probe's render) | 15 | -- |

## 2. Value statements and effects

- Value statements the test road plans: 34 per corpus pass. Every one reads an execute frame (`$_sN_result.values`), so
  the product road refuses all 34 (D15). In channel B, 34 of 34 are the same SQL (2 differ only in numbering).
- Effect statements: about 124,300 per corpus pass, every one answered with no plan (the seeds and scripts).
  - Their string arguments: 124,975 literal, **0 computed** (R13: a computed effect string never occurs).
  - Effectful maps (`$sqls->map(s|executeInDb(...))`, T9): 84 per corpus pass.
- Bodies that send no statement (W1, constant-only programs):

  | chB | duckdb/database | duckdb/host | h2/database | h2/host |
  |---|---|---|---|---|
  | 26 of 2,498 | 47 of 3,187 | 155 | 159 | 217 |

## 3. MV1: a verdict group closes before every non-verdict step (the experiment)

**The base run.** Database mode defers verdicts and runs a segment's value statements first.

| Count (each corpus's database pass) | DuckDB | H2 |
|---|---|---|
| bodies with a step after a deferred verdict | 42 | 42 |
| value statements after a deferred verdict | 11 | 11 |
| ... of which the body's last statement | 33 | 33 |
| frame lets after a deferred verdict (they send nothing in database mode) | 159 | 159 |

The other step kinds never occur after a deferred verdict:
- staged rows and staged grids;
- `assertError`;
- a forced frame;
- seeds and effects, which already close the group.

Channel B has none of these cases.

**The experiment** (`CLOSE_EARLY`):

| | DuckDB | H2 |
|---|---|---|
| Pass / fail | 2473/108 (unchanged) | 2233/353 (unchanged) |
| Failure messages | identical (only the random `executionTraceID`s differ) | identical |
| Fused groups | 2,521 → 2,545 | 2,520 → 2,529 |

The outside-body register moved by construction: 14 DuckDB and 7 H2 pure bodies became more than one send (for
example `toPostgresModel::tests::testConvertAlias` and `tds::tests::extensions::rowValueDifferenceTest`). At M10 that
register's rule for a pure body (one send) changes with 2.D's rule. That change is MV1's only movement, and it moves
statement counts, not tests.

## 4. R14 / MV8: compile errors and early-detected run-time errors

**How the failing bodies fail** (counted at the top-level body; "before-last" means the failure came before the body's
last statement):

| Failure | chB | duckdb/database | duckdb/host | h2/database | h2/host |
|---|---|---|---|---|---|
| a verdict (`AssertFailed`), last statement | 4 | 43 | 29 | 59 | 42 |
| a verdict, before the last | 6 | 0 | 14 | 0 | 20 |
| the database raised (`DataError`), last | 0 | 0 | 0 | 4 | 0 |
| the database raised, before the last | 8 | 0 | 0 | 0 | 5 |
| planning (compile-time kinds), last | 0 | 31 | 33 | 250 | 140 |
| planning, before the last | 0 | 14 | 14 | 15 | 65 |

**The planning-time failures, by where they were raised** (DuckDB / H2, same in both passes unless shown):

| Layer and kind | DuckDB | H2 | R14 class |
|---|---|---|---|
| `compiler` `TypeInferenceException` (a helper body that does not type, an unknown function) | 12 | 12 | compile error (invalid program) |
| `resolver` `MappingResolutionException` / `NotImplementedException` | 16 | 15 | compile error (unsupported or invalid mapping use) |
| `lowering.MatchFold`, `plan.PlanText`, `planner.EnginePlans`, `lineage`, `testdatagen` `NotImplementedException` | 15 | 15 | compile error (unsupported) |
| `sql` `DialectCapability` (`LIST_FILTER`, `encodeBaseN`, ... on a database without it) | 2 | 223 (database) / 161 (host) | compile error for that target |
| `sql.Json` `IllegalStateException` "golden JSON does not parse" (host pass only) | 2 | 2 | **ambiguous, posted**: the golden is a literal (knowable at compile time), but the engine parses it when the assert runs |

**The lowering-time guards that are run-time errors by Pure's meaning.** A census of the code finds four
`ModelException` sites in `lowering/`. Channel B's `assertError` catches three of them:

| Guard | Where | Caught by `assertError` (chB) | R14 class |
|---|---|---|---|
| "Unsupported duration unit for StrictDate. Units can only be: [YEARS, DAYS, MONTHS, WEEKS]" | `Scalars.java:721` | 6 | run-time: a planned raise |
| "Invalid window frame boundary - lower bound of window frame cannot be greater than the upper bound!" | `Windows.java:228` | 8 | run-time: a planned raise |
| "The type X is not supported yet!" | `CastPolicy.java:135` | 2 | run-time: a planned raise |
| "The type X is not supported yet!" (its twin) | `Lowerer.java:3366` | 0 | run-time, as its twin |

`assertError` also caught 28 database raises (`DataError`), and in 4 cases no error at all ("No error was thrown").

**Planning failures after a body's first failing statement (MV8).**
- *The database pass as the proxy.* The database pass plans every side of a segment before it judges, so a later
  statement's planning failure would replace an earlier assert failure there.
  - DuckDB: all 108 failing tests fail with the same kind in both passes.
  - H2: one test changes kind, `projection::filter::in::H2Test` (`DataError` on the host pass, `DialectCapability` on
    the database pass).
- *The product road as the proxy.* The same statements, planned the product way (an approximation, since the product
  lacks D15), are refused only for D15's reasons, plus `TypeInferenceException`s in helper bodies of tests that already
  fail at planning.
- *What remains unmeasured.* A statement in a branch that never runs needs the one-plan compiler of M10 to measure.

## 5. The database judge self-contained (MV2, MV3)

- **MV2:** neither "AssertVerdicts.side" text appears in any failure message, census key or log line of the corpora or
  channel B.
- **MV3, the Java arms inside the database judge:**
  - The grid-pair arm (`TdsCompare.grids`) ran 0 times.
  - Class-instance `assertEq` sent to `HostJudge.eq`: 2 (channel B).
  - The engine-text asserts that the SQL judge does not attempt (per corpus pass, from the census lines):
    - `assertEquals` over SQL text, 1,006;
    - rendered text, 258;
    - JSON bytes, 176;
    - `assertSameElements` over rendered text, 19.
- **Unjudged** in the database pass:
  - H2: 5 `assertJsonStringsEqual` ("json: not byte-equal (keys sorted, compact)");
  - channel B: 2 `assertTdsEquivalent` (a variant cell kind the canon does not claim).

## 6. The other items M0 names

| Item | Count | Reading |
|---|---|---|
| T8: does a statement's compiled type depend on the generator's rows | 41 folds (39 per corpus, 2 chB), **0** changing the statement's type | the generator can be a Keyed step without a re-typed tail |
| R8: the order key on a capped host side (`JudgePlans`) | 88 capped + 1 cap wrapper per corpus host pass; 459-460 uncapped; database pass 16 uncapped, `CanonicalRenderSql` 10 uncapped | the 89 capped reads are the rows the kept exception chooses (audit W7) |
| T13: the referee (database pass) | DuckDB: MATCH 1,065 + 49 + 23 + 7, DIVERGED 9, DECLINED 6; H2: MATCH 1,043 + 49 + 23 + 6, DIVERGED 8, DECLINED 14; about 1,258 golden replays per DuckDB pass | unchanged from the plan's figures |
| D7: graph answers streamed to a caller's sink | 0 in the corpora and channel B (the service tests use it) | -- |
| D18: an effect let the cross-store guard would refuse | 0 (no effect let reaches that path) | the missing guard costs nothing to add |
| D27: value-lane timestamp cells trimmed | chB 244 of 244; corpus host 2 of 7 | e.g. `1973-11-13T23:09:11.000000000+0000` → `1973-11-13T23:09:11+0000` |
| D30: a `toOne()` reader checked | 0 | -- |
| F11 / MV10: fused groups sent inside a body transaction | 0 (2,521 DuckDB and 2,520 H2 outside one) | the savepoint has no corpus case |
| MV5: plans that differ only in id literals | 0 alone; ids differ only together with the identity lane's layout (LAYOUT+LANE, 16) | -- |
| Frame CTEs (database pass) | cte=1,401, pasted=510, class=1,686 (H2 1,823), class-cte=53 | unchanged |
| Census baseline | DuckDB host body=3972 let=1974 other=6219 system=121 tdg=890; database body=6553; H2 host let=1950 other=5793 system=120; database body=6430 fallback=18; `probe=0` | identical to the plan's §8.3 |

## 7. What this changes in the plan

- **D15 is the product road's one big gap.** Of every refusal, 92% are `execute(...)` read as a value, and 4% more are
  that execute call's mapping. M3's frames brief (B10) and M5 carry it. Nothing else the test road plans is out of the
  product's reach by more than the D9/D13 let bindings and the T8 carriers the plan already lists.
- **The lowering differences are exactly the plan's list:**
  - the identity lane (R10's canon, test-only);
  - D7;
  - D10;
  - D4;
  - D3.

  No front-half difference exists where both roads plan. The parity guard's permanent form (M10) can be byte-identical
  after the numbering normalization, with those five factors as its named exceptions.
- **MV1 needs no ruling on outcomes.** It changes statement counts and the outside-body register's rule, and nothing
  else.
- **MV8 is one H2 test at most.** R14's split of the guards is mechanical: the four `ModelException` guards become
  planned raises, and every other planning failure is a compile error. One case is posted, the golden-JSON parse.
- **Empty risks:** T8 (no re-typing), R13 (no computed strings), D18, F11 and D30 have no corpus or channel B case.
  The designs stay as written, smaller in practice.
