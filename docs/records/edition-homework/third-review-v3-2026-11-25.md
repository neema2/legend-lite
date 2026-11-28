# R3: independent review of edition design v3 (2026-11-25)

Reviewed: `scratchpad/edition-design-v3-2026-11-25.md`, against the amendments (items 1-21, the scratchpad copy), the
second review, r1/r2, the census, AGENTS.md and the rules of record. Code read in `/home/user/s2` (da231244). `L/` =
`/home/user/s2/core/src/main/java/com/legend/`; `ENG/` = the pinned legend-engine source; `RoR` =
`docs/records/compat-rules-of-record-2026-11-19.md`.

## Verdict

**Not ready for the go.** The resolver's exit is a good place for the pass, but the design does not yet say how the pass
knows the part. A role per body, plus "reads a store", plus labels from the resolver alone cannot reproduce legend-engine's
placement for host-side lets, spliced `execute()` results or PCT's storeless queries. On top of that, folds before the
pass and lowering choices after it still decide meanings that differ by edition.

---

## MUST-FIX before the user's go

**1. The seam is not "one place every planned statement passes" (§3.2, §10.4).**
- The resolver is built in more than one place, not only at `BodyPlanner.java:68-70`:
  - `L/Compiler.java:954`, inside `lowerResolved`, which `PlanAllocations.java:398` and `tools/census/RenderCensus.java:85` call;
  - nested, inside a resolution: `L/resolver/ClassSources.java:907`, `SubQueryLift.java:240,277,321`.

  Those nested resolvers are built bare: no rule set, no role, no graph-key text. A pass "at `resolve`'s exit" would run
  inside them.
- Seeded lets skip the resolver altogether. `ProductStatements.seeded` (`:358-366`) and `BodyPlanner.java:128-129` lower
  `env.queryLets()`, which are the inliner's let values and never resolved, as a prefix to the statement
  (`L/lowering/SeedableLets.java:27-50`). So a governed call inside a seeded let is lowered without passing the pass.
- **Fix:**
  - run the pass once, at the outermost exit, called by the planner after the top-level `resolve`. The call sites are
    `ProductStatements:150`, `BodyPlanner:213`, `PlanAllocations:204`, `EnginePlans:162` and `Compiler:954`;
  - add a guard that nested resolvers never run it;
  - route seeded let values through the pass, or prove that they hold no governed call.
- §3.1's plumbing list is stale. The rule set now has to reach `PlanEnv` and the resolver's callers, not `SpecCompiler`.
  The text still lists five sites and says "four" (the second review's SHOULD-FIX, unfixed).

**2. Let boundaries are gone before the pass, and legend-engine places code by them (§3.2, §3.7, risk 1).**
- A3 inlines each store-bound root on its own *because* "inlining a whole body reduces its lets into the statements that
  read them, which would make a value legend-engine computes on the host (with Pure's meaning) store-bound code"
  (`L/edge/PureMeaningRewrite.java:36-39`).
- The planner's G½ does exactly that: it dissolves lets into their readers (`ProductStatements.java:126-140`).
- So the pass sees `let x = 'abcdef'->substring(1,3); T->filter(r|$r.NAME == $x)` as a substring inside a store-reading
  statement. It would write the database's `substring`, while legend-engine and A3 give Pure's.
- A3's pinned cases would fail: `PureMeaningRewriteTest.java:93-130` (host-side lets, and a function called on both
  sides) and `:173-203` (a let shared by the store and the host).
- This disagreement is known now, not a shadow-run surprise. "A3 in shadow, then delete" is unsafe until the pass has a
  let rule.
- **Fix:** the inliner stamps let provenance (a host-side scalar let, or a store-bound let). Then state the rule:
  materialized versus inlined into its readers.

**3. A role per body cannot carry the parts (§3.2, §9 M2).**
- **(a) The corpus mixes the two roles in one body.** A harness read of `$r.values` is the frame's chain, "unresolved —
  downstream reads compose over it and resolve as a whole" (`L/compiler/spec/ResultEnvelopeSplice.java:62-63`).
  `View.spliced()` returns the raw chain for every class-rooted or unplanned frame (`:89-94`). So one resolved statement
  holds both the `execute()` query (`engine`) and harness code (`pure`).
  - M7 counts what rides on this: substring in 3 harness tests, indexOf in 7, and 77 that take a Float's or a date's
    text.
  - The boundary must be a node label the splice writes, not a role the caller gives.
- **(b) PCT engine mode is storeless.** Channel A compiles every case with `PctBackend.withStorelessRuntime`
  (`pct/.../PctPlanRoute.java:134`). By §3.2's rule ("whether a statement reads a store") every case is then
  legend-engine's Java part, with Pure's `substring`, `indexOf` and raises. That contradicts §2.1 ("the PCT engine run is
  all database part") and ruling 20.2.
  - It is not all database part upstream either. legend-engine's adapter pre-evaluates multi-expression functions: "the
    latter preeval, invalidating many PCT test as preeval will evaluate constant values" (`ENG/.../pct_relational.pure:337-340`).
  - A role is needed for the PCT adapter, with its own exception for preeval.
- **(c) Verdict queries the judges mint go through the same exit.** `AssertVerdicts` builds them (for example
  `VerdictQueries.assertEqualsOf` and `predicateVector`, `L/testplan/AssertVerdicts.java:187,1500`), and they are planned
  through `BodyDriver.planValue`/`planHostSide` → `BodyPlanner.stagedSide` → the resolver (`BodyDriver.java:749-759`). So
  the harness rules would also rewrite the judges' own calls.
  - §9 M3's answer ("they are the judges', not the program's") needs a mechanism: a label, or a separate planning road.

**4. Provenance has more sources than the resolver, and v3 does not say how a label is represented (§3.2).**
- Labels are needed from:
  - the inliner (lets: item 2);
  - the splice (item 3a);
  - the typer: normalized bodies are inlined at typing (`L/compiler/spec/Overloads.java:129-131`). legend-engine
    *pre-evaluates* them in Pure (`ENG/.../router_routing.pure:202-245`), which makes them its Java part. After the fold
    cut, those nodes reach the pass unlabelled and are read as database part;
  - the resolver (mapping code, M2M, constraints, pushed properties).
- The representation decides whether R2 can be byte-identical:
  - **wrapper nodes** change every `instanceof` site downstream;
  - **identity side tables** are lost when later steps rebuild nodes (`onFormPass` `StoreResolver.java:257-275`,
    `ObjectReferenceDecode`, `ChainNormalizer`, nested resolvers).
- `ClassSources` deliberately composes M2M bindings so that "NOTHING downstream knows M2M" (`ClassSources.java:925`), and
  caches sources per route (`:315`). The graph-fetch versus TDS distinction has to be stamped against that design.
- Identity-keyed tables downstream of the pass must survive an empty pass: `testplan/BodyClass.java:227`,
  `lowering/ObjectSites.java:24`, `Lowerer.java:1436`.

**5. Folds still decide edition meanings before the pass, and the allowlist keeps those while dropping what is needed (§3.5, §6.1 item 4).**
- **(a) Folds run inside the crossing marker.**
  - At G½, `LiteralUnroll.fold` runs on every rewritten node (`UserCallInliner.java:1347-1361`), so on the call inside a
    `TypedCrossTypeComparison` too:
    - `E1.A == E2.A` folds to `false` through `neverEqual` (`LiteralUnroll.java:389-438`);
    - `1->in([1.0])` folds to `false` (`:596-600`).
  - The pass then sees `marker(false)`. That breaks ruling 20.4 (an enumeration against another enumeration's value
    compares by name) and ruling 7 (comparisons follow the database) for spelled operands. The inventory's T-2 already
    shows `false` at the engine door today.
  - The allowlist's "`==`/`in`/`contains` over … enumerations" keeps exactly this fold.
  - `resolver/LiteralFolds.java:81-87` chooses a class-typed `if` branch *inside* the resolver with SQL's numeric
    equality (`StoreResolver.java:335-359`), so `if(1 == 1.0, …)` is decided before the pass, which is wrong for `pure`.
- **(b) Tension with the product.** Cutting `neverEqual` and the cross-kind `in` makes lite's function bodies answer the
  database's `true` for spelled enumeration pairs, against S169 (ruled 2026-10-30, cited at `LiteralUnroll.java:426-432`).
  Keeping them breaks `engine`. With `LiteRules` empty, v3 has no place to put S169. This needs a decision (D3).
- **(c) The allowlist is the wrong list.** It is the fold audit's StaticFold "schema subset" (fold-audit §6, List 3 item
  3), applied to all of about 70 fold sites. It leaves out:
  - **what the restored rule itself allows:** `not`, `and` and `or` (Boolean), and `makeString`/`joinStrings` over
    Strings;
  - **what the audit's List 1 keeps:** `toOne`/`at`, `size`, `concatenate`, LiteralUnroll's structural folds,
    `NormalizeFolds`, `ColumnsMetaFold`, `MatchFold`;
  - **folds that other steps need:**
    - the inliner's literal arms (`UserCallInliner.java:1093`ff, "the recursion wall otherwise");
    - the `execute()`-query peel (`ExecuteChainAssembly.java:83-110`: `if(<literal>)`, `preval`, `withFeatureFlags`),
      without which the corpus's queries are not found;
    - RL-1, without which "class query under if() with a runtime condition is not resolvable yet"
      (`StoreResolver.java:351-354`);
  - **folds no list mentions:** substring's position arithmetic (`lowering/FeatureRules.java:60-70`), `KeyedCounts`
    (`L/lowering/KeyedCounts.java`, absent from the fold audit) and `hugeWiden` (kept by ruling 20.5).

  "`ConstBounds` is the only exception" is false.
- **(d) Conflicts with the record.** The restored rule is narrower than the ruled 11-19 reading: "Integer constants
  (including `substring`'s position arithmetic), a list's size fixed by its type" (RoR `:48-49`; AGENTS.md, "Folds").
  §6.1 item 8 (SQLite `chunk`: a literal pattern built from an Integer) breaks §3.5's "number … text goes to the database".
- **Fix:**
  - classify every fold site T, S, K or V, using the audit's List 1 and List 2;
  - exclude marker interiors and cross-kind pairs from every fold;
  - reconcile with the 11-19 reading;
  - have the user rule the list before it lands.

**6. Meanings decided below the HIR have no carrier (§3.4, §5.3, §9 M3).**

The pass rewrites typed HIR. These rows are decided later:
- **Raises:** chosen per plan by `LoweringForms.raise` (`L/edge/EdgeLoweringForms.java:24-38`).
- **Implicit `toOne` guards:** they have no node at all (`Coercions.java:63-107`, `ToOneGuard.java`).
- **Keyed counts:** a fold over the SQL tree (`KeyedCounts`; the edge's form, `EdgeLoweringForms.java:101-106`).
- **SQLite's Decimal arithmetic:** a refusal in the dialect (`sql/dialect/SQLite.java:1655-1681`; the edge's override,
  `PureText.java:420-427`).
- **H2's settings:** carried on the dialect instance (`H2.java:238-239`).

"Plain in the database part, lite's checks in the Java part" (S-1, S-5, S-22) needs a per-node carrier that the lowering
honours. "The `toOne` guards are routed in R3" is not a design.
- **Fix:** for each row, name the node or marker the pass writes and the lowering arm that reads it. Otherwise state that
  the part is decided per statement, and accept what that costs.

**7. The rules of record contradict v3, and §8 does not amend them.**
- RoR `:80-91` (the edition, 2026-11-25) puts the reader in "the typer's edition package (`compiler.spec.edition`) …
  nothing after the typer can see it", and has flags read "by the typer". v3 reads at the resolver's exit.
- The 11-22 rule (RoR `:173`): "legend-engine's departures from Pure, belong to the edge: a door rewrite before the
  planner or a presenter after it". v3 has rule sets inside the planner, with engine forms in the product's dialects.
- The 11-19 folds reading (5d).
- §8 must list each one for the user to rule.

**8. §6.1 ("independent of the mechanism") is not all independent.**
- **Item 4 (folds):** depends on the mechanism (5a, 5b). It is not small: it changes product answers and SQL. It goes to
  the user with its measurement, not under amendment 19's standing word.
- **Item 5 (S-13):** lands through `LoweringForms`, which R7 deletes. It has no carrier after that (item 6).
- **Item 7 (`acos`/`asin` raise):**
  - unless the new kind joins `PureText.ENGINE_UNCHECKED`, pure/v1 on H2 raises where legend-engine answers NaN, written
    as `null`;
  - lite's product SQL grows by a guard.
- **Item 8 (`chunk`):** contradicts item 4 (5d).
- Items 1, 2, 3 and 6 are independent.

---

## SHOULD-FIX

- **Flags are per `execute()`, not per query.** Second review M8, half answered.
  - The door has to peel `let fn = {…}->withFeatureFlags(…); execute($fn, …)` as `ExecuteChainAssembly` does.
  - Flags come from the frame's context argument (`ContextReading.java:354-380`) and are merged per frame
    (`BodyPlanner.java:78-90`).
  - One `EngineRules` per test body misapplies a flag when the body has several `execute()` calls.
- **Guard 3 cannot be written as stated.**
  - The product itself mints `sqlSubstring` and `sqlPosition` for the relational DSL and the islands
    (`normalizer/RelOpTranslator.java:447-476`; `protocol/sql/SqlToPure.java:5945-5949`; `iq/IqFunctions.java:667,910`),
    so the run-time witness fails on those tests.
  - A3 and `EdgeLoweringForms` mint non-lite forms until R3 and R7, so guard 3 is red in R2.
- **Guard 7 (the SQL-difference guard) has no mechanism.**
  - Lite refuses many corpus queries (the T rows), so "the product's SQL for the same query" often does not exist.
  - The P rows stay in SQL until R4, and the edge dialects until R7.
  - Diffing SQL text and mapping each hunk to an S row is not practical. Compare the pass's log of rewritten nodes instead,
    plus a lowering of the same tree under `LiteRules`.
- **Guard 6** is declarative only for `StaticFold.FoldOp`. For the other sites it needs a class allowlist of fold sites,
  and `LiteralUnrollLedgerTest` contradicts the code today.
- **Invariant 4.** "Asks … or leaves the call unchanged" is a default. Give an exhaustive answer per governed native.
- **The second review's SHOULD-FIXes that v3 does not answer:**
  - the compile route's strict-typing errors;
  - product forms versus compositions of lite natives (11-22);
  - the fates of `HostForms`, `LiteHostForms` and `EdgeHearing`;
  - A4's judge choice, which reads `PlanStyle.PRODUCT` (`testplan/TestPlanner.java:73-79`) and loses its signal in R7;
  - the fates of `UpstreamPrints` (Java's print used as a value), `Styled`, `PlanEnv`, `WhereZones` and
    `RefusedExecutedValues`.

  Census 0 (R7) cannot be reached without these.
- **Databases where legend-engine's answer is not checked:** A3 refuses them (`PureMeaningRewrite.java:101`,
  `PureText.engineAnswers`). v3 says nothing about whether `EngineRules` refuses there or applies.
- **Per-project editions** (ruling 1; RoR: "a property of code"). §3.2 says "function boundaries are gone, which the rules
  don't need", but an edition per project needs exactly those boundaries, or a label per inlined body.
- **§3.2 cites `StatementEffects` for "reads a store"; it has no such fact** (`StatementEffects.java:40-302` covers
  effects, verdicts and writes). Use `SystemRoutes.stores` or `BodyPlanner.collectStores`.
- **Islands "stay outside the pass" has no mechanism.**
  - Islands are translated to Pure (`SqlToPure`) and inlined, and they carry no label.
  - It is harmless only because islands always read a store.
- **§3.3's exception covers text only.** `executeValues`' shape and other serialized JSON read further inside a program
  are meaning too (census hard case 7).
  - Error words that a program reads (`assertError`, through `HostForms.caught`) are meaning as well, not "error wording".
- **Gaps in §5:**
  - T-5 (`PureOnlyTexts`: `%0wd`, `%r`, `@id`);
  - T-8, T-11 and T-13, left undecided;
  - T-10 (SQLite's Decimal arithmetic under `pure`, which PCT SQLite relies on today through `UPSTREAM`);
  - P-13 (MongoDB's date format);
  - P-16 (a product inconsistency, not an audience difference: it needs a product fix or a register row);
  - `TestDataPlanner.Printing`;
  - the scalar lift (B2b S6a, held): R4 needs it for texts on an assert's side, and v3 never mentions it;
  - S-6 and S-8 "under `pure`: Pure's" is wrong, since legend-pure has neither function.
- **After R7:**
  - the carrier of H2's settings (`H2.java:238-239`);
  - keeping the two kinds of session apart (`EdgeH2SessionTest`; project databases are keyed by dialect class name,
    `PlanMaker.java:1315`).

---

## §9: does v3 answer the second review's must-fixes?

| # | Answered? |
|---|---|
| M1 placement | **Partly.** The pass moved to after inlining, but lets (item 2), PCT's storeless queries (3b) and normalized bodies (4) are not placed |
| M2 the corpus region | **No.** Roles are per body; the splice mixes the two (3a). Other natives that take a query (`executionPlan`, `executeLegendQuery`) are unassigned |
| M3 sites outside the call | **Partly.** Casts and slices are now seen. Raises, implicit guards, keyed counts and SQLite have no carrier (6); the homogeneous-list text (P-16) is not mentioned |
| M4 folds | **No.** Order and S169 unresolved; wrong list (5) |
| M5 Java-run code | **Plausible** for constraints (they are children of `TypedSerializeGraph`, `TypedSerializeGraph.java:261-268`). M2M needs stamping against `ClassSources`' design (4) |
| M6 relational DSL and islands | **Mostly moot.** The normalizer already maps dynaFunctions to SQL-meaning natives (`RelOpTranslator.java:447-476`). The "islands outside" claim has no mechanism |
| M7 `TDSNull` | Ruled |
| M8 flags | **Half.** Read from the typed tree, but the per-`execute()` scope is not addressed |
| M9 run-time kinds | Ruled; the mechanism is "MEDIUM", unspecified |
| M10 per-part answers | Open (§10), honestly |
| M11 §8 honesty | **Partly.** Tenets 3 and 6 are amended; the rules of record's edition paragraph, 11-22 and the folds reading are not (7) |

---

## Decisions for the user (not yet asked)

1. **Host-side lets under `engine`.** Reproduce legend-engine (Pure's meaning, which needs labels from the inliner), or
   take the database's answer for any let folded into a store statement (recorded, and A3's behaviour lost).
2. **PCT engine-mode placement.** Everything in SQL, or legend-engine's preeval for multi-expression functions.
3. **S169 versus T-2 and S-11.** Where lite's "spelled enumeration pair never equal" lives once the folds stop answering
   cross-kind pairs.
4. **The fold rule.**
   - v3's narrow restoration, or the 11-19 reading (Integer constants, list size).
   - Whether a Boolean root over non-Boolean operands (`|2 > 1`, `|1 == 1`) is a Java constant or the database's (fold
     audit, List 3 item 1).
   - Whether `KeyedCounts` (S147) is a fact fixed by the model.
5. **The allowlist itself,** ruled before item 4 of §6.1 lands. Is item 4 inside or outside the standing word (amendment
   19)?
6. **`EngineRules` on databases where legend-engine's answer is not checked:** refuse, or apply.
7. **Feature flags:** scoped per `execute()` or per query. Refusing them in `lite` is a product move.
8. **The scalar lift for assert-side texts in PCT and the corpus,** or accept the moves.
9. **SQLite's Decimal arithmetic (T-10) under `pure`.**
10. **Per-project editions:** label inlined code with its project's edition, or rule one edition per query.
11. **The three amendments to the rules of record** in item 7.

---

## What gets worse (by phase and audience)

| Phase | Lite product | pure/v1 | Corpus | PCT, Pure mode | PCT, engine mode (DuckDB) |
|---|---|---|---|---|---|
| §6.1 | Fold cut: DataCube's relation-generic bodies may refuse (schema arithmetic SF-2/3, SF-7, SF-11, SF-12, SF-19a); S169 regresses in function bodies if LU-1d goes; Boolean roots go to SQL; `acos`/`asin` raise (an extra guard) | DECIMAL `0.1+0.2` moves away (ruled); H2 `acos`/`asin` raise where engine answers `null` unless added to `ENGINE_UNCHECKED` | H2 +2 (DECIMAL). At risk if the allowlist is applied as written: the `execute()` peel, RL-1, recursion over literal lists (UC-1), `firstNotNull` (LU-1b). Keyed counts: SQL grows | Better: the nine Pure disagreements, `acos`/`asin`, `testBigFloatAbs`, lateral, `chunk`. Switching to compiled legend-pure moves other tests, unmeasured | `acos`/`asin`: better |
| R2 | None, if labels are side data and the empty pass keeps node identity | None | None | None | None |
| R3 | A flag in a `lite` program is refused (a product move) | Host-side lets get the database's `substring`/`indexOf` (worse than A3 today); storeless statements switch from plain to checked raises (unmeasured); unchecked databases answered or refused, unspecified | Spliced harness and query mislabeled (3a): about 10 substring/indexOf tests and up to 77 text tests at risk; flags misapplied across several `execute()` calls | About nothing (PCT has 0 crossings) | Storeless queries become the Java part, so ruling 20.2 is unmet: substring/indexOf stay in bucket 3, not 2. Wherever the pass writes the database's form instead, multi-expression preeval risks bucket 5 |
| R4 | None | Java-part Float and DateTime text unmeasured | `PurePrintForms` passes deleted: the harness's Float/DateTime texts become lite's in SQL; up to 77 tests unless the lift lands; MongoDB's date format unassigned | 6 text cases move without the scalar lift; SQLite Decimal arithmetic refused if `UPSTREAM`'s REAL arithmetic leaves with `pure` (unmeasured) | Float text re-spelled per database in Java (medium) |
| R7 | None | H2 settings and session separation need a new carrier | Same; the judge choice (A4) loses its signal | — | — |

---

## Not verified

- Whether every governed call keeps its identity and shape through `ChainNormalizer`, `SubQueryLift` and
  `NavMaterializer`. Only spot-checked.
- How many corpus frames are class-rooted or unplanned, and so composed into harness statements.
- legend-engine's let materialization beyond A3's probe-backed tests. Whether a `#TDS` literal in pure/v1 is its database
  part.
- What the fold cut-back actually refuses and moves (no probe was run). The moves on PCT SQLite from T-10. The moves from
  switching channel A to compiled legend-pure.
- Whether one cached `ClassSource` can serve both a graph-fetch consumer and a TDS consumer in one query, which would make
  one M2M label wrong for one of them.

---

## Addendum: v3 §5.4, the classification of legend-engine's known wrong answers (the same reviewer, on the committed v3 at faa1a2bc; transcribed by B3)

The verdict (not ready) and must-fixes 1-8 stand.

**Are the delegations really the database's own function, and cheap?** Mostly yes, on DuckDB and H2 only. Each is a one-line map to the database's function in legend-engine:
- DuckDB: `round` `duckdbExtension.pure:865-871`, `datediff('week')` `:213`/`:478-483`, `split_part` `:276`, `cast(%s as decimal)` `:282`, `cast(%s as varchar)` `:284`.
- H2, through `toPostgresModel.pure`: round `:410`, datediff `:397`, toDecimal `:406`, toString `:404`, rem→mod `:426`, ascii `:345`.

The "cheap" claims fail on these points:
- **A. Placement.** PCT's channel-A queries are storeless (`PctPlanRoute.java:134`), so v3 §3.2's part rule would make them legend-engine's Java part, and the delegations would not apply to the run they are measured on. legend-engine's Java answers for `round`, `dateDiff`, `toDecimal`, `rem`, `splitPart` and `toString` have not been asked.
- **B. Forms are per database.** `SqlFn.ROUND_HALF_UP` is the database's `ROUND` only on DuckDB, H2 and Databricks; elsewhere it is composed (`sql/dialect/Postgres.java:1129-1141`), and Postgres's own `round` on a double is half-even. `dateDiff` weeks differs per database, and Postgres has none. Each row needs a verbatim form per dialect, plus a refusal where legend-engine's answer was not read (as A3 refuses, `PureMeaningRewrite.java:101`).
- **C. Enumeration by name is MEDIUM, not cheap.** `LiteralUnroll.neverEqual` folds it at G½. Its mechanism (legend-engine writing enumerations as names) is inferred.
- **D. `toString` as the database's cast belongs in the presenter at a last step, per rule zero.** SQL is needed only where the text is used further (S-15/S-16).
  - **Bucket-5 risk:** DuckDB passes the Float `toString` pair because its Float literal is a DECIMAL. With the DECIMAL literal deleted (ruling 11) and S-15, lite on DuckDB would print `1.3421e-08` where legend-engine passes.
  - M1's "engine lanes: 0 moves" was measured while `PurePrintForms` still wrote legend-pure's print, which R4 deletes. Measure it.
- **E. The other rows:**
  - **H2 `rem`:** lite already writes plain `MOD` (`H2.java:657`). The probe's `3` may come from a decimal-typed literal, which would make it S-10 (ruled K), not a call substitution. Unverified.
  - **H2 `toDecimal` (3.8 → 4):** inferred as a default that changed with the H2 2 upgrade, the same reasoning that made `mod` a regression. Probed on 2.1.214 only; lite runs 2.5.252.
  - **`splitPart`:** dropping the guard also drops Pure's out-of-range raise. legend-engine's index conversion (`pureToSQLQuery.pure:3199-3200`) is part of the form.
  - **H2 `ascii('')`:** lite's product writes plain `ascii` (`H2.java:767`), so lite and `pure` answer NULL where Pure answers 0. A product gap, to fix or record.

**Is anything classified as presentation actually meaning?**
1. **"A number's type":** DuckDB promoting a mixed list (`greatest([1.23, 2])` → `2.0` against Pure's `2`) changes the value's type. That's meaning, and a missing row (S-11 covers comparisons only).
2. **DateTime precision:** presentation at a last step (pure/v1's JSON already prints nine digits). In legend-engine's Java part, precision affects Pure's equality and text, so it is meaning there, unmeasured.
3. **The Decimal scale lost in reading (c2):** genuinely presentation on DuckDB.
4. **H2's `testDecimalPlus` returning `6`:** untraced; possibly a value difference.
5. **"No new work":** not quite. S-11 is MEDIUM meaning with no designed mechanism.

**Added decisions:**
- delegations on databases other than DuckDB and H2: refuse, or apply;
- the DECIMAL-literal deletion against DuckDB's Float `toString` pair (bucket-5 risk);
- whether H2 `rem`'s `3` needs a decimal literal.
