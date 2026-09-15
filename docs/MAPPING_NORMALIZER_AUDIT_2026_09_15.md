# The mapping-normalizer rewrite, audited (2026-09-15)

**Subject:** the ~209-commit arc `d3cd6efc9..ae16e5c46` — T4.1 steps 2–6, Clean-sheet B1–B3.3,
and "Legacy routes as composition" legs 1–6g.
**Audited at:** `ae16e5c46` (main HEAD).
**Method:** sixteen parallel audits, each reading whole files end to end rather than sampling.
Docs and commit messages were treated as *claims to be checked*, never as evidence.
**Detail:** `docs/mapping-normalizer-audit-2026-09-15/` — one file per dimension, plus `FIXLIST.md`.

> **For the session that picks this up:** start at
> [`findings/FIXLIST.md`](mapping-normalizer-audit-2026-09-15/findings/FIXLIST.md). It is the ordered,
> actionable list with `file:line` for every item. This file is the argument; the FIXLIST is the work.

---

## 0. Evidence grades

Findings carry a grade because the grades differ and it matters which is which.

| grade | meaning |
|---|---|
| **PROVEN** | demonstrated by executing the code (a probe was written and run) |
| **VERIFIED** | re-checked in the source by the lead auditor, independently of the agent that found it |
| **REPORTED** | one audit's finding, not independently re-checked |

Where two audits disagreed (the embedded cycle guard), the call sites decided it — see
[`findings/01-mapping-normalizer-core.md`](mapping-normalizer-audit-2026-09-15/findings/01-mapping-normalizer-core.md)
and [`findings/15-m2m-json-enum.md`](mapping-normalizer-audit-2026-09-15/findings/15-m2m-json-enum.md).

---

## 1. Verdicts on the questions asked

| question | verdict | one-line answer |
|---|---|---|
| Mappings driven by stamped facts? | **half** | Typed per-binding facts are stamped once and read verbatim. Every *untyped string-map* fact has one reader or none; three are written and never read. |
| No rediscovery downstream? | **half** | The resolver never touches the legacy parse surface (0 code refs in 35,517 lines, mechanically guarded). It freely re-derives from the *compiled* artifact, in copies that disagree. |
| Bulletproof? | **no** | 77% of normalizer tests pin exact values — good. But three load-bearing rules have zero tests, and the gate that judges real rows is opt-in and currently cannot run. |
| Correct? | **no** | Six correctness bugs, four demonstrated by execution. Both cycle guards broken in mirror-image ways. Silent wrong rows on `~distinct`. |
| SQL lean? | **yes** | 85% of 344 corpus queries are one flat SELECT. Leaner than the reference engine on 5 of 10 shapes, parity on 4, behind on 1. |
| Code lean? | **no** | ~550 dead production lines, two dead `Pipeline` fields, five callerless methods, 101 unused imports across three files, 12 orphaned javadoc blocks. |
| One owner per decision? | **no** | Of 18 enumerable decisions: 6 clean, 3 split, **9 divergent**. The most-copied rule has ~14 implementations in a method whose javadoc calls it "The one rule." |
| No fallbacks? | **no** | 36 null-returning censused defaults remain. Four live name-resolution fallbacks inside Phase E, contradicting the package's own contract. |
| No hacks / shortcuts / deferrals? | **mostly** | Zero TODO/FIXME. 47 `NotImplementedException` walls, almost all loud and well-argued — but ~20 reject shapes the engine handles, and none fails a strict build. |
| Typed, or string hacking? | **split** | Dispatch is properly typed (exact enum lookup, no simple-name matching). *Identity* is not: set ids, poison keys and slot names are strings with grammar. |
| Duplication? | **heavy** | Five include-closure walkers with four shadowing rules. XStore synthesis implemented twice verbatim. The binding-construction ternary written twice, 60 lines apart. |
| Architecture / design? | **mixed** | The compiled artifact is well-designed. The package was cut to fit a line-count guardrail, not at joints — and the guardrail still carries 600 lines of slack. |

---

## 2. Build state at HEAD (measured, not claimed)

- `mvn -pl core test -Denforcer.skip=true` → **4,431 tests, 0 failures, 16 skipped.**
- CI at `ae16e5c46`: **all nine gates green** on Linux, macOS and Windows.
- Full reactor `mvn test` → **5 failures**, all in `spec/.../com.legend.generators.*` — upstream-checkout
  parity, none touching the normalizer.

Everything in this audit is a latent hazard or dead weight, **not** a broken build.

### The blocking operational problem

Both oracle checkouts have drifted off the SHAs pinned in `tools/oracle-pins.env`
(bumped to 4.145.0 on 2026-09-11, commit `0a4a928c6`):

| | pinned | actually on disk |
|---|---|---|
| `legend-engine` | 4.145.0 (`230c1591`) | **4.137.1-SNAPSHOT** (`943d38b3`, pulled 2026-08-06) |
| `legend-pure` | 5.99.0 (`7fbc7d6e`) | **5.92.0-3** (`d00cfd5b`, pulled 2026-08-05) |

**Four independent instruments are blinded by this one stale tree:**
the corpus gate (`MinimalCorpusTest`), engine-citation verification, generator parity
(`NativeSignatureGeneratorTest`, `PreludeGeneratorTest`, …), and the dyna registry
(`DynaFnRegistryTest.registryMatchesTheCheckout`).

Credit where due: the repo's own guard caught the lead auditor bypassing `oracle_roots_check` by
running maven by hand — the exact trap `docs/GATES.md` documents. `MinimalCorpusTest.pinCensus`
failed with *"the corpus denominator moved"* (`declared=2721, discovered=2575` vs pinned
`2761/2613`) before running a single row. That is defence-in-depth working as designed.

**Consequence for this audit:** every "DuckDB 108 / H2 444 EXACT" claim in the arc's commit
messages is currently unreproducible locally, and citation line numbers cannot be checked.
See [`findings/10-engine-citations.md`](mapping-normalizer-audit-2026-09-15/findings/10-engine-citations.md)
for why the citations are nonetheless judged *honest*.

---

## 3. The through-line

> **Every typed, per-binding fact is alive and singly-owned.
> Every untyped string-map fact has exactly one consumer, or none.**

That correlation explains most of what follows. Where the design stamped a *typed* fact onto the
binding — `RelationalSource` (sealed, non-null, total), `DeclaredKeys`, `Operation.memberSetIds`,
`primaryKeyColumns` — the fact is consumed, singly-owned, and correct. Where it stamped a
`Map<String, ?>` into `NormalizationFacts`, the fact has one reader (thin) or zero (dead), and the
rule it encodes is recomputed at each point of use, divergently.

The clean owners are all *stamped typed facts*. The divergent owners are all *rules recomputed
from the surface*.

---

## 4. Correctness bugs

Full detail and probe output in the per-dimension files. Ordered by severity.

| # | bug | grade | where |
|---|---|---|---|
| C1 | A validation verdict is silently discarded — an INVALID class set is bound anyway | **PROVEN** | `MappingPrePass.java:67`, `MappingNormalizer.java:310,354` |
| C2 | `~distinct` silently returns too many rows (raw-row fallback) | **PROVEN** | `MappingNormalizer.java:1916`, `collectMappedColumns:1952` |
| C3 | Both cycle guards broken, in mirror-image ways | **VERIFIED** | `MappingNormalizer.java:1245` (M2M), `:2165` (embedded) |
| C4 | `~groupBy`'s "withhold the property" rule is not implemented | **PROVEN** | `GroupBySynthesis.java:181-188` vs `MappingNormalizer.java:1926-1933` |
| C5 | Per-hop `(INNER)` join types silently discarded at emission | **VERIFIED** | `JoinChainEmission` never reads `hop.joinType()` |
| C6 | Two slot minters share one namespace; only one checks collisions | **VERIFIED** | `JoinChainEmission.java:643-650` vs `:654-664` |
| C7 | Strict-build error selection is non-deterministic across JVM runs | **VERIFIED** | `MappingValidation.java:53`, `MappingPrePass.java:65` |
| C8 | The two coercion lanes contradict each other | REPORTED | `DeclaredCoercions.java:52-66` vs `:175-195` |
| C9 | Store substitution never reaches association mappings pulled through an include | REPORTED | `MappingClosures.java:200`, `StoreSubstitutionRewrite.applyAssoc` |
| C10 | We reject XStore shapes that sit in our own corpus manifest | **VERIFIED** | `MappingNormalizer.java:1039`, `XStorePureEnds.java:229` |
| C11 | Inline-embedded resolution missing the engine's ambiguity wall and subtype check | REPORTED | `MappingNormalizer.java:2229-2240` |
| C12 | Relational association path has no direction-agreement wall (its XStore sibling does) | REPORTED | `AssociationSynthesis.java:428-432` |
| C13 | `toString` dyna is a silent wrong translation (ISO form vs engine's `CAST AS VARCHAR`) | REPORTED | `DynaFn` `TO_STRING` is `Resolution.PURE` with no arm |

**C1 and C3 share a root cause worth naming:** one `IdentityHashMap` in `MappingValidation`
produces both the dropped-verdict bug (C1) and the non-determinism (C7).

---

## 5. Facts stamped and never read

| fact | shape | readers | state |
|---|---|---|---|
| `ClassBinding.Relational.source` | sealed `Table \| Json`, non-null | many | alive |
| `declared` (`DeclaredKeys`) | typed record | 20 | alive |
| `Operation.memberSetIds` | `List<String>` on its variant | 7 | alive |
| `primaryKeyColumns` | typed component | 11 | alive |
| `facts().poisons` — class key | `Map<String,String>` | 3 | thin |
| `facts().poisons` — `class[setId]` key | same map, 2nd grammar | **0** | **write-only** |
| `facts().mixedUnions` | `Map<String,List<String>>` | 1 | thin |
| `facts().unionMembers` | `Map<String,List<String>>` | 1 | thin |
| `facts().routedTargetClasses` | nested string maps | 1 | thin |
| `facts().nullableCensus` | `Map<String,Set<String>>` | **0** | **dead** |
| `routedTargetSets` | `Map<String,String>` | **0** | **dead** |

Two whole producers run on every compile for nothing: `SetDispatch` (99 lines) walks the full
include closure of every mapping to build `routedTargetSets`; `RequiredNullableCensus` (130 lines)
feeds an accessor nothing calls, while its javadoc claims *"the corpus harness AGGREGATES across
its models and pins"* — no such harness exists.

The `class[setId]` poison key is the one that bites: it is the **per-set fault-isolation arm**, and
the sole reader looks up a plain class FQN. A non-root set that fails to synthesize records its
reason where nothing can address it, and the user gets a bare *"class X is not mapped in mapping M"*
— three lines below a comment promising the opposite.

Detail: [`findings/02-stamped-facts.md`](mapping-normalizer-audit-2026-09-15/findings/02-stamped-facts.md).

---

## 6. One owner per decision

18 enumerable decisions: **6 clean, 3 split, 9 divergent.**

| decision | sites | verdict |
|---|---|---|
| How a set id is spelled | ~14 | **divergent** — `ResolvedMapping.idOf` calls itself "The one rule"; `SetKeyFacts.setKey` omits the `::`→`_` substitution. `ClassSources:432` inlines it 82 lines above its own helper. |
| Find a class's binding in the closure | 5 | **divergent** — four shadowing rules: own-first-last-include-wins, BFS-shallowest-wins, no-walk-at-all, includes-before-own |
| Which set is root or sole | 6 | **divergent** — each counts over a different scope |
| How the include closure orders | ~10 | **divergent** — `MappingClosures` admits four orders internally |
| How an include path resolves | 5 | **divergent** — Phase D owns this; four more owners, three rules |
| Which simple name an association denotes | 2 | **divergent** — same tiers, opposite ambiguity policy (D throws, E takes first silently) |
| What poisons a set, which reason wins | 7 | **divergent** — one map, three key grammars, three collision policies |
| Which dyna function a name denotes | 2 | **divergent** — `DynaFn.of` exact vs `equalsIgnoreCase` literals gating INNER vs LEFT+WHERE |
| Union member ordering | 1 | **clean** — decided at E, stamped, read verbatim. The model for the rest. |
| A set's physical source | 1 | **clean** — sealed, non-null; a door that forgets to stamp does not compile |
| When a cast is needed | 1 | **clean** — single owner, class named for the decision |

Full table: [`findings/16-architecture-review.md`](mapping-normalizer-audit-2026-09-15/findings/16-architecture-review.md).

### The biggest single duplication

`lineage/ScanRelations.java` — **2,797 lines** re-reading the raw parse surface with its own
include walker, shadowing rule and main-table inference, reading **zero** stamped facts.

It is **not** an analysis sidecar. `rootImpl` feeds `plan/PlanText.java:70,188`,
`StatementExecutor` at four sites, and `PlanAllocations`; `relTree` drives `TestDataGenerator`.
So it decides what the user *sees* in the execution plan while `ClassSources.findBinding` decides
what actually *runs* — and the two disagree on `root()` handling, class identity (tail-match vs
`equals`), include precedence (first vs last wins), and missing-include policy (silent skip vs throw).

Coverage: **zero unit tests**; 49 corpus goldens of which `docs/OUTSTANDING.md` records 9 as
"sql-only: advisory golden-SQL assert, no row verification"; and the engine's own
`scanRelations.pure` spec deliberately excluded from the run per `docs/GATES.md:422`.

Detail: [`findings/08-lineage-rediscovery.md`](mapping-normalizer-audit-2026-09-15/findings/08-lineage-rediscovery.md).

---

## 7. Measure scope — the pattern behind the gaps

The arc's headline properties are each defined by a grep over a single file or a single syntactic
construct. **At HEAD, each holds inside its grep and fails just outside it.**

| claim | inside the measure | outside it |
|---|---|---|
| "no first-wins on the union path" (`ae16e5c46`) | true — `putIfAbsent` → throwing `recordOwner()`, with a witness test | `JoinChainEmission.java:622` carries the structurally identical silent first-wins, in a method whose javadoc says it exists to distinguish exactly that collision class. `StackBuilder` has four more plus a *last*-wins at `:1169` discarding n−1 routes. |
| "B5 every guard loud" (`12e36db45`) | the `orElse(null)` census in `normalizer/` reached zero | 36 null-returning censused defaults remain; the sorting criterion was "never fired on one corpus run", which **exempts precisely the sites whose silence has consequences** |
| "UnionSynthesis 3,287 → 976 lines" | true | production code across the arc is net **+129 lines** — a new 1,972-line `StackBuilder` was created inside it; the published measure counts only the normalizer side of the seam |

Related: `CodeShapeGuardrailTest.java:47` pins `MappingNormalizer.java` at **3510**; the file is
**2910**. The header says "SHRINK only" but nothing enforces shrink, so the rewrite's ~600-line
reduction was never ratcheted down. The guard watching the god-class carries 600 lines of slack.

### The one clean AGENTS.md violation

Across the whole arc **no numeric ratchet was ever raised** — every pin moved down or stayed flat,
with dated justifications. That is a genuinely clean record.

The violation is narrower: commit `6048acec2` **deleted** two `ShadowWalkerCensusTest` rows rather
than driving them to zero. The inline justification covers one (`pureKindOf`). The other,
`declaredPlatformKind`, is still defined at `DeclaredCoercions.java:74` and called at `:54, :102,
:137`, with `RelationalKinds.pureKindOf` at `:115, :145` — **six live call sites inside the very
directory the census scans**, now unpinned and free to grow.

AGENTS.md: *"A ceiling, pin, or allowlist entry moves ONLY with a dated justification comment
naming the task/incident."*

Detail: [`findings/12-git-history-and-ratchets.md`](mapping-normalizer-audit-2026-09-15/findings/12-git-history-and-ratchets.md).

---

## 8. Verification — is it bulletproof?

**No, but it is a long way from a placeholder**, and the distinction matters.

The house style is good: **79 of 103** `MappingNormalizerTest` tests (77%) pin exact emitted
identifiers — the exact alias, exact column, exact bound variable — not shapes. Error tests assert
*reasons*, not merely that a poison exists. Corroborated independently: 471 `assertEquals` against
53 `assertTrue`/`assertFalse`.

The holes are specific:

| load-bearing rule | would a wrong implementation go red? |
|---|---|
| Main-table inference | yes — narrowly; 2 unit tests, both pinning the exact table |
| View-as-frame | yes — a revert to flattening fails hard on exact assertions |
| Union routes & member ordinals | partly — routes judged by real rows; `unionKeyThreads` ordinals have **zero** assertions |
| `~groupBy` stage 2 | **no** — delete the whole block and `mvn -pl core test` stays green |
| `(INNER)` `~filter` row explosion | **no** — the only `(INNER)` filter in tests is a grammar round-trip that never reaches the normalizer |
| Per-set poison isolation | **no** — provably unreadable; no test could have caught it |

Two further gaps:

- **The order-independence test sorts away what it needs to see.**
  `MappedInClosureTest.java:110` compares `classBindings().map(cb -> cb.classFqn()).sorted()` —
  discarding binding order, set ids, root flags and function FQNs — and never compares the
  synthesized function bodies. It swaps exactly two mappings that share no classes. Nothing
  anywhere shuffles an element list. *(The claim it guards is nonetheless **true** — for structural
  reasons the test does not check. See `findings/02-stamped-facts.md` §Q5.)*
- **The arc weakened three tests.** `7da6acaa8` converted three view tests from exact nesting
  (`map(filter(distinct(select(filter(tableReference)))))`, each pinned as the immediate
  parameter-0 child) to a `spineIndex(...)` forward search accepting any number of unasserted
  operations between pinned points, and dropped the `assertEquals("select", ...)` narrowing
  assertion outright. The commit calls this "re-pinned to the frame spine" — it is a re-pin, but a
  strictly weaker one. Same pattern as `7ec35923`'s own harness audit.

Detail: [`findings/09-test-quality.md`](mapping-normalizer-audit-2026-09-15/findings/09-test-quality.md).

---

## 9. SQL leanness — the clearest win

Measured on the live corpus with SQL dumping: **400 emitted statements, 344 distinct SELECTs**,
plus ~45 targeted probes and DuckDB `EXPLAIN` plans.

| census | count |
|---|---|
| single flat SELECT | **292 of 344 (85%)** |
| repeated identical subselect subtree | **0** |
| unnecessary `DISTINCT` | **0** |
| `CAST`/`COALESCE` outside the JSON envelope | 0 of 29 / 0 of 23 |
| `SELECT *` subselect | 11 (3.2%) |

Against the reference engine: **leaner on 5 of 10 shapes, parity on 4, behind on 1.**

| shape | engine | ours | verdict |
|---|---|---|---|
| view-backed | inlines all view columns + all view joins, then wraps | 2-column frame, no extra joins | **we win** |
| `~filter (INNER)` | subselect dumping all 7 physical columns | flat, demanded columns only | **we win** |
| Operation union | 5 SELECTs, pk null-padding, OR join | 3 SELECTs, one merged key, equi-join | **we win big** |
| `~groupBy` + agg filter | wraps | HAVING fold | **we win** |
| `~groupBy` projected | flat, clean | 2 SELECTs + 2 dead columns | **we lose** |

Four gaps, in payback order: (1) union double-materialization, **measured 2.0×** (2.26 ms vs
1.12 ms, 4 scans vs 2, on 200k rows) — no CSE/CTE pass exists; (2) the `~groupBy` wrapper and dead
columns, the one parity regression; (3) `AssociationMapping` + `~filter` root isolation into
`SELECT *`; (4) `SELECT *` frames and per-hop chain nesting.

The structural root of 2–4 is one place: `SubselectPrune` is the *only* post-lowering SQL
optimizer, and it prunes columns without ever collapsing a wrapper or touching a star.

Detail: [`findings/11-sql-leanness.md`](mapping-normalizer-audit-2026-09-15/findings/11-sql-leanness.md).

---

## 10. What is genuinely good

Stated plainly, because a findings list distorts without it.

- **The compiled binding table.** `ClassBinding` sealed by kind; `RelationalSource` sealed, non-null
  and **total** (`Table | Json`, no `Unknown` variant) — a door that forgets to stamp does not
  compile. Its explicit refusal of convenience constructors, with the incident that taught it
  ("one silently dropped `primaryKeyColumns` — the AssocJoin disease"), is the right lesson from
  the right bug.
- **The resolver really is clean of the parse surface** — 0 code refs across 35,517 lines (the six
  grep hits are comments), and *guarded*: `LegacyReachbackCensusTest` pins exact per-file
  occurrence counts of `findLegacyMapping` with a 250-file coverage floor. Count-exact beats
  package-shaped.
- **Order independence is engineered, not lucky.** All pre-passes complete before any synthesis;
  `MappingClosures` reads only raw index surfaces; `ModelBuilder.from(adopted)` shares element
  identity so the memo cannot fork; `resolveAllStores` is Kahn-ordered. `HashMap` is imported into
  `UnionSynthesis` and never instantiated.
- **The shadow-walker retirement.** 13 of 14 walkers driven to zero with an exact-equality ratchet
  that fails on shrinkage too, forcing the same-commit re-pin. The 14th is openly carried as OWED.
- **The citations are honest** even though unverifiable locally — offsets grow monotonically
  (+207 → +332 → +337) while *internal* gaps are preserved to the line. A clerical error cannot do
  that. Every citation into legend-pure is exact.
- **`ResolvedMapping`** is the model for the whole package: one object, constructed once, answering
  every closure question. Where the package follows it, one-owner holds; where it does not, it does not.
- **Loud-over-silent where it was done.** `dynaFnName` refuses to pass through an unhandled
  translated function and names the engine dialects registering it. `seedAliasScope` records
  ambiguity rather than picking a sub-row. `withElement` keeps genuine bugs raw so they fail the
  build rather than walling an element away.
- **The guard culture.** `GuardCoverage.assertFloor` exists because *"six of the audit's nine
  compliance-theater instances were guards whose scope rotted, not guards whose logic broke."*
  AGENTS.md marks every invariant `[ENFORCED]` or `[CONVENTION]` and says plainly which will not
  turn anything red. That candour is what made this audit possible.

---

## 11. Corrections made during the audit

Recorded because a fresh session should not re-derive them.

1. **"Build is green" was claimed twice before it was true.** `mvn` is not on `PATH`; both runs
   exited 127. Real invocation:
   `JAVA_HOME=/Users/neemsandv/jdk/jdk-21.0.11+10/Contents/Home /Users/neemsandv/jdk/apache-maven-3.9.9/bin/mvn -pl core test -Denforcer.skip=true`
   (`-o` offline fails on a missing `maven-enforcer-plugin`).
2. **The legacy-surface invariant was called unguarded.** It is guarded, by
   `LegacyReachbackCensusTest` — count-exact per file, not ArchUnit-shaped.
3. **Two audits disagreed on the embedded cycle guard.** One called it sound, one called it dead.
   The call sites settled it: dead (all three pass a fresh set).
4. **The "citations are wrong by ~330 lines" reading is unsound** as stated — it compares against a
   checkout 8 minor versions behind the pin. The citations are judged honest; see §2 and
   `findings/10-engine-citations.md`.
5. **"A ratchet was loosened" is too strong.** No number was raised across 210 commits. Two census
   *rows* were deleted; one without justification. §7.

---

## 12. Index of detail files

| file | covers |
|---|---|
| `findings/FIXLIST.md` | **start here** — ordered actionable fixes with `file:line` |
| `findings/01-mapping-normalizer-core.md` | `MappingNormalizer`, `Pipeline`, `ModelNormalizer` — incl. the four executed probes |
| `findings/02-stamped-facts.md` | the stamped-facts pipeline; fact census; order-independence adjudication |
| `findings/03-join-chain-emission.md` | `JoinChainEmission`, slot minting, `innerFilteredSource` |
| `findings/04-union-and-routes.md` | `UnionSynthesis`, route classification, the leg-6g verdict |
| `findings/05-views-and-groupby.md` | `ViewRelation`, `GroupBySynthesis`; is the flattening fallback really gone |
| `findings/06-associations-and-xstore.md` | `AssociationSynthesis`, `XStorePureEnds`, the Phase-D/E layering violation |
| `findings/07-downstream-consumers.md` | the resolver: what it consumes vs re-derives |
| `findings/08-lineage-rediscovery.md` | `ScanRelations` — the 2,797-line duplicate on a shipped surface |
| `findings/09-test-quality.md` | test substance census; which rules would go red |
| `findings/10-engine-citations.md` | citation census; the stale-checkout adjudication |
| `findings/11-sql-leanness.md` | emitted SQL, probes, engine comparison, DuckDB plans |
| `findings/12-git-history-and-ratchets.md` | arc map, deletion-claim verdicts, ratchet table |
| `findings/13-dyna-and-coercions.md` | `RelOpTranslator`, `DeclaredCoercions`, enum decode |
| `findings/14-guards-fallbacks-census.md` | exhaustive census of silent defaults, first-wins, swallows |
| `findings/15-m2m-json-enum.md` | M2M, JSON-source, embedded, clean-sheet bypass |
| `findings/16-architecture-review.md` | package decomposition, noun test, one-owner table, proposed guards |
