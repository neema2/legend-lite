# 09 — Test quality: is it bulletproof?

**Scope read end to end:** `MappingNormalizerTest.java` (5,041), `ModelNormalizerTest.java`,
`MainTableInferenceTest.java`, `ValidationLineTest.java`, `ShadowWalkerCensusTest.java`,
`OneIndexTest.java`, plus a survey of every other test touching the normalizer.

**Standing principle for this audit:** *a green check that only proves well-formedness will pass a
placeholder.* Tests asserting "something was produced", "it parses", or "no exception" are near-worthless.

---

## Verdict

**No, it is not bulletproof — but it is a long way from a placeholder.** The unit-level verification
is genuinely substance-pinning (77% of `MappingNormalizerTest` asserts exact emitted identifiers).
The holes are specific and nameable: three of the six load-bearing rules have **no in-repo test at
all**, the set-level poison channel is provably dead code, order-independence is asserted in a form
designed not to notice, and the one gate that judges real SQL and real rows does not run in
`mvn test` and does not run on this machine at all.

---

## Does the suite pass at HEAD?

| lane | command | result |
|---|---|---|
| `core` (all normalizer verification lives here) | `mvn -pl core test` | **GREEN** — 4,431 tests, 0 failures, 0 errors, 16 skipped |
| `heavy` group in core | `-Dgroups=heavy` | no-op; only 2 classes tagged heavy, neither normalizer-related |
| full reactor | `mvn test` | **RED** — 5 failures, all in `spec/.../com.legend.generators.*` (upstream-checkout parity), none touching the normalizer |
| the corpus gate | `mvn -pl spec test -Dtest=MinimalCorpusTest -Dsurefire.excludedGroups=` | **RED before running a single corpus test**: `MinimalCorpusTest.java:518` — `expected: Census[declared=2761, excluded=148, discovered=2613] but was: Census[declared=2721, excluded=146, discovered=2575]` |

The 16 skips are 15 `@Disabled("GAP: …")` rows in `RelationalMappingIntegrationTest` (pinned in
`SkipCensusTest`) plus `CorpusDifferentialTest`.

---

## Q1 — Substance vs well-formedness in `MappingNormalizerTest`

103 `@Test` methods. Classification (a = exact emitted identifiers/values/messages; b = node kinds,
arities, key lists; c = non-failure only):

| class | count | % |
|---|---|---|
| **(a) pins exact substance** | **79** | **76.7%** |
| (b) structure only | 18 | 17.5% |
| (c) non-failure only | 6 | 5.8% |

*Corroborated independently by the lead auditor: 471 `assertEquals` against 53
`assertTrue`/`assertFalse` across the file.*

**A calibration caveat:** this file asserts **zero SQL text and zero row values**. Its "substance" is
AST-exact — the emitted alias (`Person_Firm__Firm_Org`), the exact column (`ORG_ID`), the exact bound
variable (`$s` vs `$t`), the exact literal (`CString("A")`). For a normalizer whose *output is the
AST* that is the right level, and these assertions would kill a plausible-but-wrong rewrite. But
**nothing here proves the AST lowers to correct SQL**; that lives entirely in the corpus gate that
doesn't run.

### The (c) six — the near-worthless assertions

| test | line | what it actually asserts |
|---|---|---|
| `viewWithOnlyJoinBackedColumns_infersRootFromJoin` | `:4274` | `assertTrue(exMsg.isEmpty())` — never asserts the inferred root is `T_PERSON` |
| `groupByPerRowFormulaOutsideKeyRejected` | `:3822` | `assertTrue(exMsg.isEmpty())` — never asserts the orphan PM was withheld |
| `m2mSetRoute_soleOrRootSet_isBenign` | `:4998` | `poisons.isEmpty()` |
| `normalizeIsIdempotent` / `relationalNormalizeIsIdempotent` | `:2131`, `:2105` | two runs equal each other — self-comparison, no absolute pin |
| `noInlineRealizationSurvivesPhaseE` | `:4340` | `assertFalse(fqn.isEmpty())` |

### Important behaviours with only (b)/(c) coverage — the soft spots

- `~distinct` column narrowing (`:1376-1385`) — asserts `select` exists, never *which* columns.
- View filter/distinct/groupBy layering (`:4364`, `:4409`, `:4453`) — relative spine order only (HIGH-2).
- View `~groupBy` (`:4496`) — the emitted keys are never asserted.
- `chainAliasCollision` (`:4558`) — asserts the two slots *differ*, never what they are.
- `localProperty_wrappingExpression` (`:1510`) — asserts arity 3, no values.
- `~primaryKey` not lowered (`:4111`) — structural negative only.
- Multi-hop OE fallback (`:1664`) — the join condition is never pinned.

---

## Q2 — Would a placeholder pass? Per-rule verdict

| # | rule | covering tests | would a plausible-but-wrong body go red? |
|---|---|---|---|
| **i** | **main-table inference** | `MainTableInferenceTest.java:48, 68` (2 tests) | **YES, narrowly.** Both assert the exact table (`"T_PERSON"`). An impl counting a join terminal, or skipping Otherwise-Embedded blocks, goes red. But only 2 unit tests guard the whole rule, and every other relational test declares `~mainTable` explicitly, bypassing inference. The **view** root sub-rule `joinOnlyViewRoot` (`ViewRelation.java:486-520`) is covered only by `:4274`'s `isEmpty()` — **a wrong-but-non-throwing root would pass.** |
| **ii** | **view-as-frame** | `:4176, 4280, 4332, 4376, 4421, 4478` + `AssociationViewJoinTest` | **YES, for the core rule.** A revert to flattening fails `:4208` (`assertEquals("project", frame.function())`) and `:4220` (`assertEquals("pname", nameVal.property())`) hard. **Partially, for layering** — the three re-pinned tests use a *searching* helper, so mis-positioned or extra pipeline steps are invisible. |
| **iii** | **union routes + member ordinals** | *zero* in `MappingNormalizerTest`. Facts: `OneIndexTest.java:188` (`assertEquals(List.of("w::Car","w::Bike"), unionMembers())`). Routes: judged by **rows** — `StackShapeWitnessTest.java:196,204`, `RoutedNavigateTest.java:144,169`, `ResolveUnionTest.java:215` | **YES for the covered branches** — `StackShapeWitnessTest:198` pins `List.of("10\|Q1-a","11\|null","20\|null","21\|null")`, exactly the dead-route `continue` at `UnionSynthesis.java:302`; `:206` pins the `PINNED_SINGLE` branch. Member *declaration order* is pinned exactly. **NO for `unionKeyThreads`**: the `<col>_<ordinal>` thread naming has **no in-repo assertion of any kind** — the name appears in test sources only inside a comment (`ArchitectureTest.java:1003`). Swapping ordinals goes undetected locally. |
| **iv** | **`~groupBy` stage 2** (`MappingNormalizer.java:1848-1870`, `GroupBySynthesis.renameGroupedNavCond`) | **NONE** | **NO TEST WOULD GO RED.** Every one of the five `~groupBy` tests is on a flat single table with no `Join` PM. The near-miss, `groupByKeyWithJoinNavigation` (`:3608`), uses a `JoinTerminalColumn`, which the stage-2 loop *skips* at `:1855`. The three diagnostics — `"multi-hop Join PM"` (`:1860`), `"not a ~groupBy key"` (`GroupBySynthesis.java:115`), `"did not emit a single navigate step"` (`:96`) — exist only in production source. **You could delete the entire stage-2 block and `mvn -pl core test` stays green.** |
| **v** | **INNER-join `~filter` row explosion** (`JoinChainEmission.innerFilteredSource:932`) | **NONE semantic** | **NO TEST WOULD GO RED.** The only `(INNER)` `~filter` anywhere in the repo's tests is `parser-equivalence/.../engine-grammar-fixtures-4.145.0.jsonl:1541`, `"kind":"roundtrip"` — grammar text parity, never reaching the normalizer. Both loud-decline branches (`"NULL-TOLERANT condition"` `:970`, `"null-tolerance cannot be classified"` `:1046`) are unasserted. **This is the path whose own comment says the alternative "keeps NULL-extended parents the engine's INNER join drops"** — a silent-wrong-rows hazard with zero local guard. |
| **vi** | **poison fault isolation** | per-class: `ValidationLineTest.java:62,74,77`, `OneIndexTest.java:159-161`, `MappingNormalizerTest.java:1622` + 16 `poisonReasons()` sites. per-set: **NONE** | **YES for per-class** — `ValidationLineTest` is the model: both sides tested, exact reason asserted, the healthy sibling asserted still bound. **NO for per-set** — see HIGH-1. |

---

## Q3 — Negative / error-path coverage

Both sides *are* tested, but only for 2 conditions out of ~15.

- 15 tests are named `*Throws` / `*Rejected`; only **3** call `normalizeStrict`
  (`:3929`, `:4134`, `:4839`). The other ~12 assert a **poison string on the tolerant path only** and
  never verify that a strict build throws. **Deleting `ledger.strictErrors.add(e)` from a branch
  would not go red.**
- **Reasons are asserted, not just existence** — the suite's strongest habit. 16 `poisonReasons()`
  sites, all asserting message content (`"unmapped class"`, `"multi-table"`, `"Ambiguous"`,
  `"set-routed"`, `"explosion"`, `"no_such_set"`).
- The one exception is the module half of `routedPropertyUnderTwoOwnersIsLoud` (`:3935`):
  `assertDoesNotThrow(() -> normalizeViaPipeline(parsed))` — the comment claims *"the mapping is
  walled with the reason"*, but no reason is asserted.

---

## Findings

### HIGH-1 — Set-level poisons are write-only; no test covers them, and no reader can

`MappingNormalizer.java:312`, `:348`:

```java
ledger.poisons.putIfAbsent(cm.className() + "[" + ResolvedMapping.idOf(cm) + "]", invalidSet);
```

`grep -rn "\.poisons()" core/src/main/java` returns **exactly one** production reader —
`PureModelContext.java:366-369`, a plain-FQN `get`. No site anywhere in `core/src/main` constructs the
`class + "[" + setId + "]"` key for a lookup. The intent is documented — `MappingLedger.java:32-33`
says the key is *"'class', 'class[setId]' or an association FQN"* — so **the design believes these
surface. They cannot.**

**Why no test caught it.** Every test reads poisons one of two ways, and both are blind to the key:
`ValidationLineTest.java:62,74` and `OneIndexTest.java:161` go through
`mappingPoison(mapping, plainClassFqn)`; `MappingNormalizerTest.poisonReasons` (`:2453`) joins *all*
map **values** with `" ;; "` and never inspects a key. The single test that does inspect a key
(`:1622`) uses the plain form `"my::M::model::Person"`.

**Why it matters.** Line 312's branch (`pp.invalid()` for a non-root set) also does **not** add to
`ledger.strictErrors` — so that reason is lost in **both** build modes.

### HIGH-2 — The view-frame commit converted three exact-nesting assertions into a fuzzy spine search, and dropped one substance assertion

`MappingNormalizerTest.java:2505-2526` (new helpers), applied at `:4364-4373`, `:4409-4418`,
`:4453-4459`. Introduced by `7da6acaa8`.

`spineIndex(spine, "filter", from)` walks *forward until it finds* the named function. **Before** the
commit, `viewFilterAndMappingFilterAndDistinct_allThreeSurvive` asserted the literal nesting
`map(filter(distinct(select(filter(tableReference)))))`, each step pinned as the immediate
parameter-0 child — **including the `select` narrowing step**. **After**, it asserts
`mappingFilter == 1`, `distinct < viewFilter`, `tableRef == spine.size()-1`. Any number of unasserted
operations may now be interleaved between the pinned points, and the `select` assertion is gone.

The commit message calls this *"three normalizer tests re-pinned to the frame spine"*. It is a re-pin,
but **a strictly weaker one**. (The `AGE` → `page` change in the same hunk is a legitimate behaviour
change under the frame model, not a weakening.) **This is precisely the pattern the 2026-09-07 harness
audit at `7ec35923` was written about.**

### HIGH-3 — Three load-bearing rules have zero in-repo tests

- **`~groupBy` stage 2** — `MappingNormalizer.java:1848-1870`, `GroupBySynthesis.java:43-129`.
- **`(INNER)` `~filter` row explosion** — `JoinChainEmission.java:932-1020`.
- **`unionKeyThreads` ordinal naming** — `UnionSynthesis.recordKeyThreads`.

All three are corpus-witnessed by name
(`spec/src/test/resources/rcorpus/duckdb-unordered-register.txt:229-246, 372, 377` list the relevant
engine test families, none in any fail roster). **But the corpus gate does not run in `mvn test` and
does not run at all on this checkout** (HIGH-4). **Locally, these three rules are unverified.**

### HIGH-4 — The gate that judges substance is opt-in, environment-gated, and currently inert

`spec/src/test/java/com/legend/rcorpus/MinimalCorpusTest.java:43` (`@Tag("heavy")`), `:109`
(`Assumptions.assumeTrue(Corpus.available(), …)`).

**The gate's design is excellent and worth saying plainly:** 2,613 discovered corpus tests, the fail
roster pinned as an **exact set** per lane, both floor and ceiling — `:30-40` explains that it moved
off a count precisely because *"a count let a red flip hide behind a green one"*. Plus exact
`skipped` / `accepted-divergence` / order-leniency registers.

Two problems:

1. It is **excluded from the default build** (`spec/pom.xml:32`), so nothing in `mvn test` exercises
   real SQL or real rows for the normalizer.
2. On this checkout it **fails before running a single corpus test** (`:518`, the census pin). The
   local legend-engine checkout is behind the 4.145.0 pin.

Credit where due: it walls **loudly** rather than skipping silently, and `SkipCensusTest` pins the
exact set of files permitted to carry `Assumptions.assume*`. But the consequence stands: **no
"DuckDB 108 / H2 444 EXACT" claim in the arc's commit messages could be independently verified.**
(Note also that 108 and 444 are *fail*-roster sizes, not pass counts — easy to misread as a score.)

### MED-1 — Order independence is asserted in a form designed not to notice it

`MappedInClosureTest.java:100-118`, `compiledMappingsIgnoreElementOrder`. The claim under test is
`MappingNormalizer.java:170-173`. One test exists. It:

- swaps exactly **two** whole mapping blocks (`:104` vs `:106`) that share no classes — the easiest
  possible permutation, one permutation only. `INCLUDER`, the only mapping in the fixture with an
  include, is **excluded**;
- compares `classBindings().stream().map(cb -> cb.classFqn()).sorted().toList()` (`:110-111`) — the
  `.sorted()` **explicitly discards binding order**, which is observable output (it drives union arm
  order and lifted-element order), and keeps only class FQN strings: not set ids, not root flags, not
  function FQNs;
- compares `ma.facts()` to `mb.facts()` (`:112`) — this part is real and does cover `unionMembers` ordering;
- **never compares the synthesized `FunctionDefinition` bodies between the two orders.**

No test anywhere uses `Collections.shuffle`, `Collections.reverse`, or a permutation over a model's
element list. `ResolveUnionTest.java:228 routeOrderIrrelevant` varies PM order *within one class
mapping* and compares **sorted** row lists — adjacent, not this.

*(See `02-stamped-facts.md` MED-10/Q5: the claim is nonetheless **true**, for structural reasons the
test does not check.)*

### MED-2 — 12 of 15 error tests never exercise the strict build

15 methods named `*Throws`/`*Rejected`; only `:3929`, `:4134`, `:4839` call `normalizeStrict`. Both a
name/behaviour mismatch and a coverage gap.

### MED-3 — `inferViewMainTable`'s hardest case is guarded only by a census that counts call sites

`ShadowWalkerCensusTest.java:73` pins it at 5 — but that census counts **regex matches on raw lines**
(no comment stripping) and asserts *nothing about behaviour*. Behavioural coverage is `:4176`,
`:4280`, `:4478` (single-root, exact table), `:4225` (multi-root, exact message), and `:4252` — the
join-only case (`joinOnlyViewRoot`), which asserts only `exMsg.isEmpty()`.

### LOW-1 — Test smells

- **Name/behaviour mismatch, self-admitted:** `joinPmMultiHopMissingPreludeEntryDiagnostic` (`:3171`)
  never triggers the diagnostic its name promises; the comment at `:3174-3182` says so.
- **Name contradicts assertion:** `groupByPerRowFormulaOutsideKeyRejected` (`:3801`) — method says
  "Rejected", `@DisplayName` says "Withheld", assertion is `exMsg.isEmpty()`.
- **~10 `*Throws` names on tests that assert a poison, not a throw.**
- **Clean on the rest:** no `@Disabled` in the normalizer package, no assertion-free tests, no
  `toString()` assertions (the one hit, `IncludeRulesTest.java:103`, is a failure *message* argument),
  no catch-and-ignore (the only `catch` at `:5036` rethrows).
- Near-duplicate fixtures across the `joinPm*` predicate-rewriter family (`:2772`-`:3006`) — six tests
  share a `T_SRC`/`T_TGT` skeleton. Each asserts something distinct; parameterizable, not a defect.

### LOW-2 — A stale ceiling that has gone inert

`CodeShapeGuardrailTest.java:47` pins `"MappingNormalizer.java", 3510`. **The file is 2,910 lines.**
The header says "SHRINK only", but nothing enforces shrink — the rewrite shrank the file by ~600 lines
and the ceiling was never ratcheted down, so **the guard is currently 600 lines of slack on the exact
file it exists to watch.**

### LOW-3 — Six allowlist entries added naming a batch but not a date

`CodeShapeGuardrailTest.java:65` (`UserCallInliner.spent`, "batch 149"), `:124-132`
(`Lowerer.verbatimEquality`, `Lowerer.engineText`, `Lowerer.features`,
`Lowerer.legacyNullUnsafeEquals`, `ExecutionTrace.last`, "Phase 2a (batch 136)" / "Phase 2b (batch 137)").
`AGENTS.md:13` requires *"a **dated** justification comment naming the task/incident"*. These name the
task, not the date. Marginal, and the justifications are substantive.

---

## Q5 — Ratchets: no violation found

Every ratchet/census/allowlist file was diffed over the full 210-commit window (`7ec35923..HEAD`).
**Every numeric pin moved DOWN or stayed flat. Not one was raised.**

| pin | move | justification |
|---|---|---|
| `ShadowWalkerCensusTest` `inferViewMainTable` | 6 → 5 | `7da6acaa8`, dated, names the leg |
| `ShadowWalkerCensusTest` store family (4 rows) | 9/7/2/2 → 0 | `6048acec`, "T4.1 step 3d, 2026-09-13" |
| `ShadowWalkerCensusTest` property family (6 rows) | 45/53/3/1/3/2 → 0 | `52b7205a7`, "step 3b, 2026-09-13" |
| `ShadowWalkerCensusTest` stereotype family | 2/3 → 0 | `f829f9df3`, "step 3c, 2026-09-13" |
| `CarrierPurityRatchetTest` `SqlFn\.LIST_` | 142 → 141 | dated 2026-09-12 |
| `LegacyReachbackCensusTest` `PureModelContext.java` | 6 → 4 | "T4.1 step 4b, 2026-09-13" |
| `LegacyReachbackCensusTest` normalizer rows | 5+3+1 → consolidated 2 | "T4.1 step 4a, 2026-09-13" |
| `HarnessDisciplineTest` `MinimalCorpus.java` | 4 → 2 | "batch 7a, 2026-09-11" |
| `SqlTextRatchetTest` `StatementExecutor.java` | 2 → row deleted | "batch 137" |
| `ArchitectureTest` reflection pardons | removed entirely | `4b38bde7a` "no pardons" |

`ShadowWalkerCensusTest` is worth noting as *stronger* than a ratchet: `assertEquals(REGISTER, actual)`
at `:100` is exact equality, so **shrinkage fails too** and forces the same-commit re-pin the header demands.

Allowlist **additions** (all with dated reasons): `JdbcSurfaceCensusTest` (+9, each dated),
`SkipCensusTest` (+6, dated), `HarnessDisciplineTest` (+2, dated), `CodeShapeGuardrailTest` (+6,
batch-named but undated — LOW-3).

> **Cross-reference.** `12-git-history-and-ratchets.md` F1 finds the one genuine AGENTS.md violation:
> two census *rows* were **deleted** rather than ratcheted, one without justification. That is
> compatible with this section — no *number* was raised.

---

## Other test files touching the normalizer

**In `core/src/test/java/com/legend/normalizer/`:**

- `AssociationViewJoinTest.java` (93L, 2 tests) — an association whose join condition is spelled
  through a view; the resolved-vs-raw operation bug from remediation T1.10, plus the backing-view exemption.
- `AssocSimpleNameProbeTest.java` (96L, 1 test) — simple-name `AssociationMapping` in a class-mapping
  header binds by FQN through the mapping's import scope. *(Note: its title says "same-package
  fallback" but the fixture exercises a **wildcard** match — see `06-associations-and-xstore.md` Q1.)*
- `IncludeRulesTest.java` (114L, 3 tests) — the engine's include rules R1 and R5. Pins
  `memberSetIds()` exactly at `:87`. Notes the corpus never reaches these, so these witnesses are the
  only judge.
- `LegacyCleanSheetConvergenceTest.java` (210L) — M6: a legacy mapping and its hand-written
  clean-sheet equivalent must yield the same binding table. **This is what makes "the legacy DSL
  desugars to clean-sheet" testable rather than a doc sentence.**
- `MappedInClosureTest.java` (119L, 2 tests) — "mapped" is a per-closure question (R1); and the
  order-independence test (MED-1).
- `MappingClosuresTest.java` (110L, 3 tests) — include-order facts as one memoized computation;
  later-include-wins + store substitution; roots and enum mappings composing through includes.

**Elsewhere:** `compiler/OneIndexTest.java` (4 tests — one index built before Phase E, read by it,
written by nothing; `:97` asserts identity-equality of everything F1 knows before and after);
`compiler/KnowledgeLayerTest.java:132`; `compiler/ModelBuilderTest.java`;
`compiler/spec/PhaseHCensusTest.java`; `compiler/spec/TypeCheckerTest.java`; `ArchitectureTest.java`
(notably `normalizerNeverWritesIntoTheModelIndex`, added in this window);
`LegacyReachbackCensusTest.java`, `JdbcSurfaceCensusTest.java`, `ShadowWalkerCensusTest.java`,
`CodeShapeGuardrailTest.java`, `CarrierPurityRatchetTest.java`;
`builtin/NativeCatalogGovernanceTest.java`; `parser/ElementParserTest.java`;
`resolver/ClassSourceTest.java`; `integration/StressTest10K.java`, `StressTestDense.java`,
`AssociationIntegrationTest.java`; `testing/Phases.java` (the shared `normalize`/`context` helper
keeping unit fixtures on the production phase shape).

---

## Bottom line

The normalizer's verification is **good where it looks, and blind in four named places**. What's there
is real: three quarters of the flagship suite pins exact emitted identifiers, error paths assert
*reasons* rather than existence, the ratchet discipline is clean across 210 commits with every number
moving down, and the corpus gate's set-equality fail roster is a genuinely well-designed judge.

But: stage-2 `~groupBy` navigation and the `(INNER)` `~filter` row-explosion path could both be
replaced with `return null;` and `mvn -pl core test` would stay green. The set-level poison channel is
provably dead and no test could have caught it. The order-independence claim is guarded by a test that
sorts away the thing it would need to see. And the one gate that would have caught the first two is
`@Tag("heavy")`, absent from `mvn test`, and red-before-it-starts on this checkout.

**Cheapest high-value fixes, in order:** (1) make the bracketed poison key readable or delete the two
writes; (2) add one test each for stage-2 groupBy and the INNER `~filter` — both ~40-line fixtures in
the existing style; (3) make `compiledMappingsIgnoreElementOrder` compare unsorted bindings *and*
lifted function bodies; (4) reconcile the corpus census pin with the checkout so the gate can run.
