# The debt registry: what we pinned, and what we never came back for

An audit of the **pins themselves** — every ratchet, ledger, census,
allowlist and ceiling in the repo. Not "is the guard working" but *what
is in the pile, has it ever shrunk, and is zero even the target*.

Audited at `9bcabe77`. Baseline `mvn -o -pl core test` → **4221 tests,
0 failures**. Evidence: code and git history only.

---

# PART 0 — THE REGISTER TYPES

## The registry in one box

```
170 pins total
     78  DEBT-CEILING     caps a count of bad things   ← the debt
     41  QUALITY-FLOOR    asserts a minimum of good
     29  EXACT-PIN        an exact set or count
     22  INVARIANT        pinned at zero  ✓

641  items across all violator allowlists
 52  of the 78 ceilings state NO target
  6  pins name a real destination
```

The ten biggest piles:

| # | pile | count |
|---|---|---|
| 1 | `MAX_PLATFORM_CATALOG` (parser-equivalence) | **1559** |
| 2 | corpus `softRescued` — passes rescued by text-match, not row equality | **613** |
| 3 | corpus `maxAdvisorySqlDiffs` — divergent golden SQL on *passing* tests | **309** |
| 4 | corpus `softAdv` | **303** |
| 5 | corpus `softDiff` | **257** |
| 6 | `MAX_MSG_GENUINE_MISMATCH` — real error-voice divergence | **254** |
| 7 | `MAX_M3_ONLY_PLATFORM_GAPS` | **226** |
| 8 | `SqlFn.LIST_` carrier occurrences | **127** |
| 9 | `JdbcSurfaceCensusTest.TEST_REGISTER` | **123** |
| 10 | `STRING_DISPATCH_SITES` | **87** |

## The ten themes — what each register actually polices

| # | Theme | Registers | Scale |
|---|---|---|---|
| **1** | **Foreign-system access** — *who may touch the database, raw SQL, the parser* | `JdbcSurfaceCensusTest`, `RawSqlLedgerTest`, `ParserBoundaryArchTest`, `NoEagerTypeReferencesTest` | 14 prod + 123 test JDBC files; 13 dialect classes |
| **2** | **Java-vs-database ownership** — *how much Pure semantics is in Java* | `JavaEvalLedgerTest`, `TenetRatchetTest`, `VerdictWorld2ConsistencyTest` | 13 files / 6,545 pinned lines; 12 accessor sites |
| **3** | **Carrier purity** — *how values are represented before the dialect* | `CarrierPurityRatchetTest`, `SqlTextRatchetTest`, `TypeSpellingParityTest` | 175 carrier occurrences; 37 SQL-text sites |
| **4** | **Code shape** — *the structural tax on future edits* | `CodeShapeGuardrailTest` | file ≤3500, method ≤250; 9 files + 20 methods in the danger band |
| **5** | **Error discipline** | `ErrorShapeGuardrailTest` | 28 broad catches, 13 catch-returns-value, 5 `default -> "literal"` |
| **6** | **Ambient surface** | `ObservabilityGuardrailTest` | 19 env flags, 34 stderr prints, 87 string-dispatch sites |
| **7** | **Evidence integrity** — *can our tests still see a failure?* | `SkipCensusTest`, `HarnessDisciplineTest`, `PctDisciplineTest`, `DuckWorkspaces` | 15 `@Disabled`; 36 sort sites; `LEAK_CEILING = 8` |
| **8** | **Conformance coverage** | corpus scoreboard, `ChannelB*`, PCT, oracle manifests | 591 excused rows |
| **9** | **Grammar/parse parity** | all of `parser-equivalence` | the most sophisticated set in the repo |
| **10** | **Oracle staleness** | version-pinned snapshots + `version-skew-claims.tsv` | 1.4 MB fixture pinned to engine 4.138.2 |

### Theme 9 deserves its own box — least known, best run

```
MAX_LENIENT              = 17     we accept what the engine rejects
MAX_UNJUSTIFIED_LENIENCY =  0  ✓  burned 127 → 39 → 51 → 52 → 5 → 0
MAX_MSG_GENUINE_MISMATCH = 254    error messages that genuinely differ
MSG_VERBATIM_FLOOR       = 863    messages matching the engine word-for-word
MIN_COLUMN_EXACT         = 337    error positions exact to the column
MIN_DOCS_MATCHED         = 6489
MAX_DIVERGENCES          =  0  ✓  a generative dual-parse fuzzer, at zero
MAX_SEAM_ORACLE_ASYMMETRY=  0  ✓  burned from 18
MAX_STRICT_ORACLE_ASYMMETRY = 2   burned from 181
```

The only theme with **floors as well as ceilings** — and, not a
coincidence, the only one with several registers driven to zero.
Measuring both directions is what makes a burn finishable.

## The axis every register is missing: SIZE vs AGREEMENT

**Every pin measures *size*. Not one measures *agreement*.** Each answers
"how many copies exist?", none answers "do the copies agree?" — which is
where the defects live. A concept-ownership sweep found nine executed
defects from duplicated concepts, and **every one sits inside an
already-green register**:

- `GridCompare.epochSeconds` — the `LocalDateTime` arm uses
  `ZoneOffset.UTC`, the `java.sql.Timestamp` arm uses the **JVM default
  zone**. Same instant, **18,000 seconds apart** off UTC. The comment
  three lines up asserts the two "carry the same value", and notes the
  `java.sql.Date` sibling *was* fixed to UTC — the `Timestamp` one wasn't.
- `EngineStyleH2.timestampLit` emits `TIMESTAMP'2020-01-01 13:00.0000'` —
  seconds dropped by `LocalDateTime.toString()`. Exactly the bug
  `DynamicPivot.tsText` was written to fix, and its comment names it. One
  printer was hardened; its twin in another package was never told.
- `SignatureMangle`'s grammar spells type names `[A-Za-z][A-Za-z0-9]*` —
  **no underscore** — while `Protocol.mangleType` emits raw simple names.
  `f_CO_Person_1__String_1_` reads back as **arity 0** with the wrong
  return type. The corpus is full of `CO_Person`, `M_Person`, `LA_Person`.
  Zero tests.
- Two constant folders disagree on `1 == 1.0`: `LiteralFolds` uses
  `BigDecimal.compareTo` (true), `StaticFold` uses `.equals()` (false).
  Both fold Pure `equal` in the same compile. Neither is tested.

## The fifth state: CERTIFIED

Worse than a frozen pile is a pin that **certifies the divergence as
permanent**:

- `TypeSpellingParityTest:44-46` compares the two SQL-type spellers after
  `.replace(", ", ",")` — it **normalizes the disagreement away** and
  calls it "the documented decimal-spacing delta."
- `EqualityWorldsConformanceTest:98,103,109` holds three `diverge(...)`
  rows asserting host and database give **opposite answers** for
  `1 == 1.0`, `[] == []`, `0.1+0.2 == 0.3`.

A test that passes *because* two implementations disagree is the terminal
state of a size-only registry.

## The repo's own answer: the F3.7 "exact accounting" wave

One corner of the repo already solved this, and it is the pattern to copy.
On 2026-08-16 a wave converted several bare ceilings into **two-directional
EXACT-PINs** — `FixtureAdjudicationTest`, `OwnDialectCensusTest`,
`OwnCorpusConformanceTest`, `MutationFuzzTest`, and the `CorpusSweepTest`
ledger sizes.

The stated reason each time: **a bare ceiling leaves silent headroom for
new unreviewed rows under the same count.** The evidence was blunt —
`LENIENCY_KINDS`' old ceiling of 21 *"was stale by ELEVEN"*, and
`OVER_STRICT_PINS`' ceiling of 6 was stale by one.

An exact pin fails in *both* directions, so:
- growth demands adjudication in the same commit, and
- **shrinkage forces the pin down** — you cannot bank an improvement
  silently and spend it later.

That second property is exactly what the 78 `<=` ceilings lack, and it is
why `EVICT_SIZE` could absorb 22 loosenings against 11 tightenings while
still reading green.

Two of the module's tests deliberately assert **nothing** and say so:
`MigrationSizingTest` (*"does not assert… so 'finish the migration' can be
costed instead of guessed"*) and `CorpusCensusTest` (*"its job is to make
the number visible, not to freeze it"*). Both measure large debt surfaces
that can therefore grow in silence — an honest design choice, but the two
biggest uncapped numbers in the module.

And one ledger is **silently** empty: `parser-surface-exclusions.tsv` has
no rows, but nothing asserts it stays empty — unlike `c12-walls.tsv` and
`c12-known-diffs.tsv`, which are mechanically fenced at zero.

## The five states

| State | Meaning | Exemplar |
|---|---|---|
| **BURNING** | pile + a working burn engine | `CarrierPurityRatchetTest` — dated corpus-measured pin moves, self-nagging when a count drops |
| **COMPLETED** | reached zero, became an invariant | `METHOD_ALLOWLIST` (0, stable 1,587 commits); `DEAD_PRIVATE_METHODS` (0); `PhaseHCensusTest` (ratchet → `assertEquals`); `REGEX_WHITELIST` (target stated *and* reached) |
| **FROZEN** | pile, no movement, no owner | `STRING_DISPATCH_SITES = 87`; `METHOD_LIMIT`/`FILE_LIMIT` (1,590 commits, never moved) |
| **MISLABELLED** | permanent facts dressed as debt | `HarnessDisciplineTest`: only **11 of 36** rows are comparison leniency |
| **CERTIFIED** | the pin asserts the divergence | `TypeSpellingParityTest`, `EqualityWorldsConformanceTest` |

## Three storage media — only one is enforced

| Medium | Count | Enforced? |
|---|---|---|
| Assertions in test code | **37 files** | yes — but **20 of 49 core pins don't run in CI** |
| Data ledgers on disk | **15 files** (5 already at zero) | partly |
| Prose in comments and docs | **122 markers** in `core/src/main` | **no** |

## The structural finding

```
registers saying "shrink-only" / "only shrinks" : 11   (a direction)
registers naming a target or end state          :  2   (a destination)
```

…and one of those two says *"the target is NOT zero."* **No register
defines done.** A ceiling with no target can never be declared paid, so
it becomes furniture.

Compounding it: the README's guardrail index has **8 rows** against a real
registry of **37 files**, and one of those 8 points at the `engine`
module, deleted in August.

---

# PART 1 — HAS ANYTHING EVER BEEN BURNED DOWN?

**49 core pins. 26 have ever moved in the tightening direction; 23 never
have.** Strip out the 7 achievement floors (corpus/Channel-B PASS counts)
and it is **19 of 42**. For guard **coverage floors**: **0 of 9 ever
raised, 5 lowered**.

## Pins that only ever went up

| pin | tighten | loosen | growth |
|---|---|---|---|
| `BROAD_CATCH_COUNTS` | **0** | **7** | 15 → 30, verified monotonic: 15→17→26→27→30 |
| `maxAdvisorySqlDiffs` | **0** | 3 | 297 → 309 |
| `MUTABLE_FIELD_ALLOWLIST` | 3 | **24** | 20 → 48 (**+140%**) |
| `TEST_REGISTER` (JDBC) | 1 | **17** | 105 → 123 (**+17% in 3 days**) |
| `EVICT_SIZE` | 11 | 22 | 5,385 → 7,495 (**+39%**) |
| declared-gap registry | 1 | 3 | 17 → 74 (**4.4×**) |

Five of the seven broad-catch loosenings are **new broad catches written
after the guard existed**, one justified as *"broad by design, reviewed."*
Self-certification is what the guard exists to make expensive.

## Two effects that flatter the numbers

**Re-pins that repaid nothing.** `ab704457` moved `com.legend.harness`
from `src/main` to `src/test` — **7,564 lines out of scanned scope in one
commit** — and simultaneously re-pinned four constants (`20→13`, `23→18`,
`43→32`, `111→87`). Four apparent tightenings, **zero debt repaid**.

**Shrinkage is deletion, not repayment.** `EVICT_SIZE`'s downward moves
are dominated by rows *vanishing* (`DbMetaData`, `HostEval`, `GridReads`,
`ResultNav`). The rows that stayed and grew — `AssertVerdicts` 4-up/0-down,
`PureAsserts` 4-up/0-down, `StatementExecutor` 2-up/0-down — have **never
once** been tightened.

## The burns that did happen share a shape

`METHOD_ALLOWLIST` 6→**0** in a single day (stable 1,587 commits);
`DEAD_PRIVATE_METHODS` 9→**0**; `EVICT_NAMES` 65→**40**;
`MAX_UNJUSTIFIED_LENIENCY` 127→**0**; `MAX_SEAM_ORACLE_ASYMMETRY` 18→**0**;
corpus PASS 0→2,332.

**Every one was cleared in a single push, not decremented gradually.** In
this repo gradual erosion has never worked — the piles left to erode grew
instead. That is the argument for scheduling burns as discrete work with
a defined end state.

## 20 of 49 pins do not run in CI

`gate.yml` runs `-pl core` only; the corpus runner `assumeTrue`-skips. The
sharpest instance: the commit that *deleted* `CorpusSoftCeilingTest` —
explicitly because *"it read the COMMITTED markdown while the corpus never
runs in CI"* — then put **four new ceilings in the very file that does not
run in CI**.

**Caveat:** 21 of the 49 pins were seeded within the last three days.
"Never tightened" is damning for `METHOD_LIMIT` (1,590 commits) and
`BROAD_CATCH_COUNTS` (900); it is uninformative for the soft ceilings (27).

---

# PART 2 — THE PILES

## Theme 1 — the JDBC surface (the worked example)

**The pin says 14 production files. Only 8 contain a JDBC call.** The rest
are on the register by *contagion* — a `throws SQLException` clause or a
`case java.sql.Date` arm. `SeedSqlForms.java` has **zero JDBC** and is on
the register for two `throws` clauses.

- **`StatementExecutor` (2,887 lines) has ZERO statement-execution calls.**
  Its whole JDBC surface is one `Connection` field and 16 `throws`. Its
  real problem is that it does plan-text, `toSQLString`, assert handling
  *and* sequencing — but that is not JDBC, and coupling them is how this
  stalls.
- **`TestDataGenerator` is a second complete kernel** — 1,679 lines, 13
  JDBC sites — with **zero production callers** (the one `src/main`
  reference is a comment). `git mv` to `src/test` is the largest single
  register reduction available.
- **`SQLException` is the Pure assertion-failure channel.**
  `AssertVerdicts.fail()` throws `new java.sql.SQLException(message)` for
  an *assert* failure. Blocker #1.

**Cheapest win:** `Executor.fetch` normalizes `Timestamp` but not
`java.sql.Date`; that one omission funds ~12 duplicate arms in
`PureAsserts`/`GridCompare`. Add one arm, delete twelve.

**And the pin should change shape.** Today it is a *register* — you comply
by adding your file. It should be an **outside-in rule with no register**:
no file outside `com.legend.exec.jdbc` may match the JDBC pattern. Then a
new file cannot be added by paperwork; the only legal move is to call the
kernel. **A register lets debt grow by paperwork; a rule does not.**

## Theme 4 — the pile you cannot see

```
lowering/Scalars.java              3498 lines   headroom  2
parser/MappingProtocolParser.java  3481         headroom 19
lowering/Lowerer.java              3472         headroom 28
resolver/StoreResolver.java        3464         headroom 36
```

**`Scalars.java` has two lines of headroom** — a three-line bug fix fails
the build. Nine files and twenty methods are in the band; two methods sit
*exactly* on the 250-line ceiling. Invisible to the registry, because
everything under the limit reads green.

**A dead carve-out hides the largest file:** `ArchitectureTest:588-589`
exempts `Scalars` from the dialect-blindness rule, but `javap` shows **0**
`sql/dialect` refs. The javadoc four lines up already says *"the frozen
carve-out is retired."*

## Theme 5 — a register that conceals a bug

`FunctionCompiler:150` and `Typer:2281` are pinned as broad catches, but
both self-describe as **silently re-dispatching a call to a different
function**, and `FunctionCompiler` names its ticket (#56). Correctness
debt filed in the wrong ledger.

**Six phantom pin slots** enforce nothing; two silently license a new
undocumented catch. All five `default -> "literal"` sites violate the
rule's own doctrine — worst is `MetamodelWalk:496` `default -> "LEFT"`,
where an unrecognised join kind silently becomes a LEFT join.

## Theme 7 — 3:1 noise

`HarnessDisciplineTest.ALLOWED` has 36 rows; only **11** are comparison
leniency. 25 are deterministic output ordering, 3 are regex false
positives. A reviewer cannot tell which 11 matter.

## Theme 8 — the parity split, computed at last

**591 excused rows. 255 (43%) are ours; 328 (55%) are verified parity or
by-design.** The oracle is honest: our snapshots are **byte-identical** to
the engine's own manifests — nothing curated.

**PCT is a finished burn.** TRUE-WIRE-BUG **0** in all five suites,
DECLINED **0**, our 36 `expectedFailures` have **100% overlap** with the
engine's own 249 exclusions — not one row is ours alone. We pass **213
rows the reference DuckDB adapter cannot**. Peak 94 → 36.

**The corpus axis is not finished, and the exposure is in the PASS column:**

```
advisory sql diffs : 309/309   sqldiff-pass : 257/257   adv-pass : 303/303
0-assert passes    :  27/27    text-rescued : 613/613
```

**Five of five at 100% of budget.** And **PASS has been flat at 2332 since
2026-08-18 while `sqldiff` and `adv` each grew +10** — ten exact passes
converted to soft passes, ceilings raised to absorb it, headline unmoved.
**755–1147 of 2332 passes carry a softness marker**; only **320 (13.7%)**
are byte-verified against the engine's golden SQL.

The declared-gap registry covers **74 of 557 declines (13%)**; 483 are
uncapped, including one 450-row bucket. It has gone 17 → 73 → 74 and **not
one row has ever been retired.**

**Two mechanisms silence the corpus entirely:** `-Drcorpus.only=x` or
`-Drcorpus.test=x` reduces the suite to **zero assertions**; under
`-Drcorpus.backend=h2` the lane returns early before every quality gate.

**The best adjudication work in the repo is dead code.**
`diagnoses.csv` — 283 rows, each with verdict, effort, confidence and a
falsifier — is read by **nothing**. With its siblings: **~962 ledger rows
nothing executes against.** And **all 15 `@Disabled` tests have EMPTY
BODIES** — enabling one yields a vacuous green.

## Theme 8b — `nlq`, the inverse register set

Zero debt-ceilings, zero named constants, zero skip registries. 38 pins,
almost all `>=` floors on **model size**, not quality. Two are **vacuous**
(`recall() >= 0.0`, `latencyMs() >= 0`); one **contradicts its own
comment** (comment says 50%, code asserts `0.40`). **9 of 38 never execute
in CI.** A hidden allowlist lives in the eval JSON: `acceptableRootClasses`
widens correct routing for **21 of 54** cases, and **21 of 25 holdout
cases have an empty `mustExclude`**, making half the pass predicate
vacuous.

## Theme 10 — a clock nobody wound

`engine-grammar-fixtures-4.138.2.jsonl` (1.4 MB) is pinned to engine
**4.138.2**; the reference checkout is **4.137.1-SNAPSHOT** — *older* than
the fixture. `version-skew-claims.tsv` (25 rows) adjudicates each
divergence; three say **"re-adjudicate at re-pin"**, an event with no
owner and no date.

## Guards whose scope can rot to zero

`GuardCoverage.assertFloor` is called by **7 of 37** registers.
`NoEagerTypeReferencesTest` asserts only `Files.isDirectory` — **a
zero-file walk passes**. `ParserBoundaryArchTest` adds four sibling
modules only `if (Files.isDirectory(...))`.

Three stale rule messages describe a state the code does not hold:

| site | says | actually |
|---|---|---|
| `ArchitectureTest:588` | *"the frozen carve-out is retired"* | the `Scalars` exemption is live |
| `ArchitectureTest:896` | *"the pinned exceptions only shrink"* | there is no exception list |
| `ArchitectureTest:720` | *"the two sanctioned exceptions"* | the code lists **five** |

---

---

# PART 2b — THE DEBT NO CHECK CAN SEE

**53 distinct pieces of debt are invisible to every automated check**: 19
measured numbers with no pin, 24 chartered guards never built, 10 named
owners or tracking ids that resolve to nothing.

## `nlq` is RED at HEAD, and the cause is an over-correction

```
mvn -o -pl nlq -am test  ->  208 tests, 1 error, BUILD FAILURE
NlqCdmModelTest.setup: Found duplicated value 'provision'
                       in enumeration 'observable::PriceTypeEnum'
```

The chain: the frontend audit found `Enum m::E { A, A }` silently accepted
→ commit `e5984b02` ("seven eager rejections") made it a **hard
rejection** → the real CDM model contains `provision,` twice
(`cdm-model.pure:2574-2575`) → the module has been red since.

**And the rejection is stricter than the reference.** legend-engine's
`EnumerationValidator.java:47` emits a **`Warning`**, not an error, and its
own test expects `null` errors alongside
`"COMPILATION warning at [3:4-6]: Found duplicated value 'TEA'"`. The
model compiles there.

Two lessons, both on the audit rather than the fix:
- **A leniency finding must carry the reference's *severity*, not just its
  existence.** "The engine rejects this" and "the engine warns about this"
  are different fixes.
- **A module outside every gate will absorb a regression silently.** The
  prior audit named this exact risk — *"adding `nlq` to `allgates.sh` is
  the cheapest coverage win in the repo"* — four commits before the break.
  It was not done.

## Numbers that drifted while nobody re-derived them

| source | written | actual today |
|---|---|---|
| `AUDIT_23` censused scope (says **"Complete"**) | ~37k LOC | **80,282** (+87%) |
| — its resolver slice | 14 files / 12.1k | **31 files / 26,067** (+107%) |
| `AUDIT_23` tolerance census (**"task #75 closed"**) | ~25 sites | **38 sites / 28 tests** |
| order-leniency (measured yesterday) | 393 / 2434 | **403 / 2434**, 495 sites |
| `DESIGN_DEBT` M6 "Lowerer is ~1100 lines" | 1,186 | **3,472** (+193%, ledger untouched 46 days) |
| `STATE_AUDIT` voids / records | 306 / 476 | **555 / 878** (+81% / +85%; "Start at S0" never started) |
| `GATES.md` chain budget ("re-pin pending") | 398s | **780s** |

## Owners that don't exist

**There is no task registry anywhere in this repo**, yet `task #NN` is
cited **44 times** across 15 production files. The sharpest instance:
`FunctionCompiler.java:157` hands a live silent-wrong-dispatch bug — a
signature-broken overload silently re-dispatching to a healthy sibling —
to **`task #56`**, which appears in zero docs, zero source, zero commits.
`StoreResolver.java:2241` cites "audit 13", a document that does not exist.
`MissProbe.java:20-23` says a gap is "filed in FOUNDATIONS_PLAN §9"; §9
contains no such row.

## A ledger generator aimed at the wrong repository

`scripts/outstanding.py:14` sets `REPO = "/Users/neema/legend/legend-lite"`
— **a different user's separate checkout** — and opens
`docs/OUTSTANDING.md` with mode `"w"`. That file's 15 hand-appended
`@Disabled("GAP:")` rows are the only record of 15 declared platform gaps,
and they sit below the generated body. One run destroys them. The script
is invoked by no gate, which is the only reason it hasn't happened.

## Chartered and never built — 24 of 43

Dominated by charters older than a week, while the *built* column is
almost entirely the last 48 hours. Notable still-open ones: the union
arm-factory leg (6 `TRUST_ONE` shims live in `UnionSynthesis`); an H2
`RAISE_ERROR` mechanism the docs describe that has **0 hits** in source;
compile-through equality and grid compile-through; an exhaustiveness
ratchet (the durable fix for the `default -> false` class); the
`lowering ↛ sql.dialect` rule (`ArchitectureTest:479` still allows it);
and the `static final` cross-layer constant ban — both violations
(`EngineStyleH2:179`, `SnapshotEnvelope:133,139`) still inlined past
ArchUnit.

**Credit where the burn is real:** `VerdictWorld2ConsistencyTest` built;
`parser-equivalence` now green (47/0/0) with GATE8's roster widened 20 → 27;
soft-pass ceilings binding live; `ConnectionResolver` content-keyed;
`endsWith("::toOne")` gone; `Stamps.java`'s fictional "PCT lane" owner
replaced with a real closure. Three of this sweep's own rows went stale
*while it was measuring them*.

---

# PART 2c — THE NUMBER NOBODY HAD COMPUTED

The Java-eval ledger pins **7,024 stripped lines** as host-side debt.
Classified line by line, for the first time:

| bucket | lines | % |
|---|---:|---:|
| **PERMANENT BY NATURE** | **5,248** | **74.7%** |
| Misplaced compiler work — leaves the *executor*, not Java | 698 | 9.9% |
| SQL-burnable, product path | **590** | 8.4% |
| SQL-burnable, test-scope only | 415 | 5.9% |
| Pure semantics in Java, not SQL-burnable | 73 | 1.0% |

**Three-quarters of the pile is permanent and mislabelled as debt.** Only
**590 burnable lines actually ship in the product.** `PlanText` is 746 of
750 lines pure compile-time text composition; `AggAwareActivities` is
225/225 (its `equal` arm *composes the string `" == "`*); `JsonCompare`
64/64; `GridProbe` 26/26. All sit on a shrink-only eviction register for
no reason — which makes the pile look four times more damning than it is
*and* hides the 1,000 lines that genuinely burn.

## The 434-line "win" that burned nothing

`StatementExecutor` is pinned 2728 and measures 2294 — 434 lines of
apparent slack. Reconstructed per commit: `8610762e` moved −401 lines out
and created `ResultEnvelopeSplice` (+365); `e88521b5`/`3410bcd0` moved
−272 into `ExecuteChainAssembly` (+179) and `SeedableLets` (+30). **Net
repo change: +75. Zero lines burned.**

The ledger is a *per-file* register with **no cross-file conservation
check**, so an Invariant-7 relocation reads as a burn and leaves 434 lines
of silent-regrowth headroom on the one file whose own row says it "absorbs
by design." The watch is blind by 19% on its largest surface.

## The unregistered pile is bigger than the folds

| pile | stripped | on a size register? |
|---|---:|---|
| Corpus harness (`EngineTestExecutor` 2397, `H2Verify` 437) | **2,834** | no |
| `exec` with no size pin — **`Executor.java` 503**, `Ddl` 331, … | **1,255** | no |
| Relocated out of ledgered files (2026-08-21) | **714** | no |
| Compile-time folds (`StaticFold` 496, +4 others) | **677** | no |

**`Executor.java` — 503 lines, the egress decode cluster, named in the
ledger's own prose and adjudicated in an audit — has no size pin at all.**

## The folds disagree with the SQL lowering — and they ship

`StaticFold` is reachable from an **ordinary product query**: any query
containing `<relation>.columns->map(...)` routes arbitrary Pure
expressions through the Java interpreter and reifies the result into SQL
as a literal (`Typer.java:618-625`, widened at `:1360-1369`). **No test
anywhere exercises its arithmetic or equality vocabulary.** Running the
same expression bare and wrapped gives two answers from one compiler:

| expression | Java fold | SQL lane |
|---|---|---|
| `[9223372036854775807,1]->plus()` | **−9223372036854775808** | `9223372036854775808` |
| `1 == 1.0` | `StaticFold` **false** / `LiteralFolds` **true** | true |
| `1->in([1.0])` | false | true |
| `1e10->toString()` | `"1.0E10"` | `"10000000000.0"` |
| `hasSubsecond` literal vs column | `false` (Boolean) | `1` (Integer) |

The first is a **sign flip on a real arithmetic answer** —
`StaticFold.java:254-256` is `args.stream().mapToLong(a -> (Long) a).sum()`,
an unchecked `long` sum. The second means **the two compile-time folders
sit on opposite sides of a divergence the tree has ratified** at
`EqualityWorldsConformanceTest:98-99`. The fifth flips the answer *and*
the wire kind depending on whether the operand is a literal or a column.

Two further confirmed defects on the same surface: `'a'->instanceOf(String)`
**aborts the compile** (`Scalars.java:2530-2543` can only emit
`BoolLit(true)`; anything else throws), and `limit(0-1)` yields `[]` on
the list lane while the relation lane emits `LIMIT -1` and raises
(`ConstBounds.java:40-55` validates nothing).

**And a test is defending one of these.** `ExtendCheckerTest:2022`
comments *"DuckDB returns integers for has\* functions (1 = true, 0 =
false)"* and asserts `intValue() != 0`. DuckDB returns nothing —
`Scalars.java:808` emits the constant `1`.

## Two ledger claims that do not survive contact with code

- `JavaEvalLedgerTest:36` cites `wrapH2Boolean` as an exemplar. **That
  symbol does not exist anywhere in the tree** — grep hits only that
  javadoc line. `EVICT_NAMES` drift detection does not cover prose.
- `ArchitectureTest.theInterpreterPerformsNoJdbc` does **not** establish
  "no database value can enter the channel" — it bans JDBC *types*, and a
  decoded value is a `String`/`Long`/`LocalDateTime` with zero JDBC
  dependency. Its regex is a full match, so all 17 nested records
  (`$NodeH`, `$JtnH`, …) are uncovered; the `(\$.*)?` idiom that fixes it
  is used twice elsewhere in the same file. The claim is true today by the
  shape of `planWalk`, and nothing guards it.

# PART 3 — WHAT TO DO

## Today, no design decisions

1. Delete the dead `Scalars` carve-out (`ArchitectureTest:588-589`).
2. Delete the 6 phantom broad-catch rows — two are live holes.
3. Shrink `ENDS_WITH_FQN` 18→13, `STDERR_PRINTS` 34→32.
4. Retire `LL_STAMP_COUNT` — the census flipped; the flag now only
   disables an invariant.
5. `git mv TestDataGenerator` to `src/test` — **and register it in
   `TEST_REGISTER` in the same commit**; the census's own docstring
   records that a previous such move "silently walked 7,564 lines out of
   seven guards."
6. Normalize `java.sql.Date` in `Executor.fetch` — deletes 12 duplicate
   arms; two files leave the JDBC register.

## This week

7. Burn the **17 write-once rows** in `ArchitectureTest`'s
   static-collection register with `Map.copyOf` — cheapest 17 items here.
8. Move `FunctionCompiler:150` / `Typer:2281` to the correctness backlog.
9. Split `HarnessDisciplineTest.ALLOWED` into `LENIENCY` (11) and
   `DETERMINISTIC_OUTPUT` (25).
10. Burn the 5 `default -> "literal"` sites, starting `MetamodelWalk:496`.
11. Fix the `@`-blind `MUTABLE_FIELD` regex — five mutable fields entered
    production unaudited.
12. Fix or relabel `noStaticMutableState` — it advertises "banned
    outright, NO allowlist" over 11 invisible `ThreadLocal` holders.

## Structural — the changes that stop this recurring

13. **Give every register a target, not just a direction.** Add an
    `END_STATE` field: zero, a number with a date, or PERMANENT.
14. **Split PERMANENT out of every register.** Themes 5 and 7 are ~⅔
    permanent facts; mislabelling is what makes a registry unreadable.
15. **Add AGREEMENT pins, not just SIZE pins.** Every executed defect
    above sits inside a green size-register. Start with
    `instanceof java.sql.*` / `java.time.*` / `BigDecimal` outside
    `com.legend.values` = 0.
16. **Retire the two CERTIFIED pins** — a test that passes because two
    implementations disagree should be a bug, not a fixture.
17. **Convert registers to rules where the target is zero** (the JDBC
    pattern). A register grows by paperwork; a rule cannot.
18. **Raise the coverage floors** — all nine are calibrated to a repo that
    no longer exists.
19. **Add a 90%-headroom warning to the size guards** so theme 4's pile is
    visible before it blocks an edit.
20. **Maintain the index** — the README lists 8 of 37 and one row points at
    a deleted module.

## The single highest-value collapse

Themes 2, 3 and the marshalling smear are **one concept counted three
ways** — the same four files (`PureAsserts` 482, `GridCompare` 379,
`Executor` 792, `AssertVerdicts` 508) carry all of them, with **22
coercion ladders across 9 files** and no owner.

Meanwhile `com/legend/values/` — a package that exists, is
ArchUnit-enforced JDK-only, and already owns `PureDateLiteral` — holds
755 lines of date records **and nothing else. The owner is already built
and empty.**

One `WireValue.Kind of(Object)` classifier plus five converters is
**≈ −490 / +250 LOC** and kills the 18,000-second timestamp bug, the
`java.sql.Date` misclassification, the two inverted total orders, and the
reason `AssertVerdicts` exists.
