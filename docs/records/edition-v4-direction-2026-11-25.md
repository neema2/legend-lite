# The editions: the v4 direction, and the decision that remains (2026-11-25, late night)

*For a session with no context. Read `edition-program-START-HERE-2026-11-25.md` §0 first, then this file, then `edition-design-v3-2026-11-25.md` (whose mechanism this replaces; everything else in v3 carries over).*

**Status.** The third independent review (`edition-homework/third-review-v3-2026-11-25.md`) found v3 not ready. This file records the direction that came out of it, what is verified and what is not, the homework that would prove it, and the user's fallback. **Nothing here is built.** The user is short on time: "we have to decide something soon running out of time - if we cant figure out we will just fail those substring tests probably and move on".

---

## 1. Why three designs failed, in one paragraph

Every design tried to decide legend-engine's meaning *before* lowering, and to infer *where legend-engine would run each piece of code*: in the database, or in its own Java.
- **v1** put it in the typer, which types a call from its callee's signature, never its body (`SpecCompiler.java:30-33`), so it can't see placement.
- **v3** put it in a pass at the store resolver's exit. But:
  - the inliner (G½) has already merged `let`s into their readers there;
  - the resolver is also built in other places (`Compiler.java:954`, nested in `ClassSources.java:907`, `SubQueryLift.java:240,277,321`);
  - seeded `let` values skip it;
  - the corpus can splice its `execute()` query into the harness (`ResultEnvelopeSplice.java:62-94`);
  - PCT's engine run is storeless;
  - and meanings decided *in* lowering (checked versus plain raises, the implicit `toOne` guards, keyed counts) have no node before it.

## 2. The direction: an edition is a set of overrides on the lowering's one rule selection

**Verified in the code:**
- **One rule selection for scalar natives.** Every scalar native call is lowered through `lowering/Scalars.lower` (`Scalars.java:2816-2829`), which picks the rule in one place: `FeatureRules.select(key, features)`, then the plain table `RULES.get(key)`. That covers `substring`, `indexOf`, `round`, `dateDiff`, `isNumeric`, the parses, `toString`, `divide`, `rem`, `splitPart` and `toDecimal`. Its own comment: "a flagged rule … wins over the plain one for its key".
- **The override set is already per query.** `features` (`Set<Feature>`, legend-engine's feature flags) is handed in for each query.
- **The one authority.** `platform/ImplementationTable` calls itself "what executes every declaration … the one authority", and already supports feature-keyed overrides (`RegistryKeys.featureOverrides`, `ImplementationTable.java:~60`).
- **Raises are chosen in one function, by kind.** `LoweringForms.raise(kind, checked, plain)`; the edge's choice is `EdgeLoweringForms.raise`, plain for `PureText.ENGINE_UNCHECKED`.
- **Every tree that becomes SQL goes through `Lowerer.lower`.** That includes the seeded `let` values the resolver misses (`PlanMaker:599,637`, `ProductStatements:350,397`, `BodyPlanner:128`).

**The design:**
- **The product's rule table holds lite's rules.**
- **`pure` and `engine` are override sets:** data, built at the edge, handed in by the door per query in place of today's feature set. Under `engine`, for example: `substring` → as given; `indexOf` → `sqlPosition`; `round` → the database's `ROUND`; `dateDiff` → the database's own, per dialect; the cross-type marker → the database's comparison; and raise kinds → plain.
- **The lowerer doesn't change.** It already asks "which rule for this key?". There are no `if edition` branches and no new hook sites.
- **Per-database spelling is the dialect's.** An override names a rule; each dialect writes it. Where legend-engine's answer was not read for a database, the override refuses by name.
- **Feature flags become overrides,** which is what they already are.
- **The guards:**
  - no product class depends on an override set's implementation;
  - overrides name only native keys and raise kinds (an allowlist);
  - no edition is in the plan;
  - each mode's SQL equals the product's except where an override says so.

**Not verified (the homework, §5):**
- whether *every* edition-dependent construct goes through that selection. Equality and the cross-type marker, the text forms, aggregates and the implicit `toOne` guards (`Coercions.java:63-90`, `ToOneGuard.java:68-72`, which bypass the raise choice today) may be lowered elsewhere;
- whether the per-query feature set can carry a whole override set;
- that it works end to end.

## 3. Placement: two neutral facts stamped at G½, while `let`s still exist

**How the code decides "database or not" today (A3).** A3 is `edge/PureMeaningCheck`, `PureMeaningRewrite`, `EngineSubstitutes` (about 900 lines). Step A3 of the compatibility program's stage A, ruled by the user on 2026-11-19. It runs at pure/v1's doors only (`PureV1Routes.checkMeaning`, `HostAnswers`, `TestDataRoutes`), on the typed query, before inlining (`PureMeaningCheck.java:214-260, 322-330`):
1. **Per top-level statement,** it asks `compiler/spec/StatementEffects.reaches(stmt, isAStoreRead, specs, memo)`. A product function: does the statement reach `TypedGetAll`, `TypedTableReference`, `TypedFrom`, `TypedMilestonedAccess` or `TypedViewRelation`? Transitively, following user calls into their bodies, typed on demand.
2. **It sorts each statement:**
   - **store-bound:** legend-engine sends it to the database, so the database's meaning;
   - **host-side:** legend-engine computes it in Java before the query, so Pure's meaning.
3. **A fixpoint correction:** a host-side `let` whose value *is* a lambda named by store-bound code becomes store-bound. A `let` whose value is merely computed with a lambda stays host-side (asked of legend-engine 4.145.0).
4. **A refusal:** a function value it can't follow is refused by name.
5. **Then the rewrite:** `PureMeaningRewrite` inlines each store-bound statement on its own and rewrites `substring`, `indexOf` and `toOne` of empty there.

A3 also refuses mapping code (`~func`, M2M), a graph fetch's derived properties, databases other than H2 and DuckDB, and a lambda shared between store-bound and host-side code. It does not run in the corpus or PCT's engine run.

**The proposal (the user's question: "can we have some data on let node that tells us?").** The inliner (G½) is where `let`s are merged into their readers, so it still sees each `let` whole. There it stamps two neutral facts as data on the value, a thin transparent marker that travels with the value when it is substituted:
1. **"computed without reading a store":** the same `StatementEffects.reaches` call A3 makes, with A3's lambda-`let` fixpoint and refusal;
2. **"the query of an `execute()` call":** including `let query = {|...}; execute($query, ...)`, since the `let` is resolved there.

The lowering's rule selection reads the marker:

| Door | `execute()` query / database part | "computed without a store" | everything else |
|---|---|---|---|
| pure/v1 | `engine` | the plain rule (legend-engine's Java part: Pure's) | `engine` |
| corpus | `engine` | Pure's | Pure's (the harness) |
| PCT engine run | `engine` everywhere (this door ignores the markers) | | |
| PCT Pure run, product | the markers change nothing | | |

The product only states facts; the door's override set decides what they mean. With this, A3 can be deleted, and `substring`/`indexOf` reach `~func` and relational mapping code too, because the resolver inlines them before lowering. That is the gain over A3.

**Not verified:**
- that the inliner can compute "reads no store" for a `let`'s value at that point;
- that a transparent marker survives the passes between G½ and lowering (resolver, folds) and is ignored under `lite`, keeping product SQL byte-identical. The third review warned that wrapper nodes can break byte-identity;
- that it reproduces A3's decisions.

## 4. What carries over from v3 unchanged

- the editions (`lite`, `pure` = compiled legend-pure, `engine`);
- rule zero and tenet 1 as amended;
- presentation at the door's presenter;
- the classified and ruled list of legend-engine's known wrong answers (v3 §5.4: delegations matched, bugs not reproduced), with the third review's corrections: a form per dialect, enum-by-name MEDIUM, mixed-list promotion is meaning, H2 `ascii('')` a product gap;
- the folds cut back (§3.5), with the open edge in §6.3;
- the gates (§4) and guards (§7), adapted to overrides;
- the phases (§6), with R2 now "override sets per query + G½ markers + guards";
- what lands now (§6.1);
- every ruling in `edition-homework/v2-amendments-ruled-2026-11-25.md` (1-22).

## 5. The homework that would prove it (about two hours, temporary probes, nothing committed)

1. **Trace every edition-dependent construct in v3 §5 to where it is lowered:** through `Scalars.lower`'s selection, `LoweringForms.raise`, or elsewhere. List the elsewheres.
2. **Plumbing:** can the per-query `Set<Feature>` be replaced by an override set (key → rule, raise kind → plain/checked) without touching the lowerer's logic?
3. **A probe of two overrides:** `substring` as legend-engine has it, and `round` as the database's `ROUND`. Run them under `engine` on the engine lanes and the corpus, including a `~func` mapping that uses `substring`.
4. **A probe of the G½ marker:** "computed without reading a store", via `StatementEffects.reaches`. Run it in shadow beside A3 on A3's tests (`PureMeaningRewriteTest`, `PureMeaningEngineTest`), pure/v1's goldens and the corpus, and list every disagreement.

## 6. Decisions for the user

1. **The direction:** override sets on the lowering's rule selection, plus two G½ markers, instead of v3's pass. Recommended, subject to the homework.
2. **pure/v1's host-side `let`s:** the G½ marker (recommended if the probe agrees with A3); keep A3 as is; or **the fallback**.
3. **The fold rule's edge:** whether `|1 == 1` and `|2 > 1` stay compile-time constants, and whether a keyed count is a model fact. Recommended: Booleans over constants fold, numbers don't, and the keyed count is a model fact.

**The fallback (the user, 2026-11-25):** "if we cant figure out we will just fail those substring tests probably and move on". Concretely: `engine` takes the database's `substring`/`indexOf` everywhere the override applies, without host-side placement. The pure/v1 tests that pin Pure's meaning in a host-side `let` (`PureMeaningRewriteTest:93-130`, `:173-203`; `PureMeaningEngineTest:389-398`) are recorded as known differences, a register row, and A3 is deleted. Cost: a pure/v1 query that computes `substring` in a `let` reading no store gets the database's answer where legend-engine gives Pure's. That is rare in real queries.
