# Audit: d9c16617..d585e5b8 (core, pct, spec) -- step 3 (raises by the table) and step 2b (comparison setting in the typer)

Read only. Audited commit d585e5b8 (HEAD 593aca57 has no code change under core/pct/spec against it). The working tree
has further UNCOMMITTED edits (PureText.ENGINE_TESTS, EngineTable.TESTS, Edition.stampsTestBodies, ProductStatements,
MinimalCorpus, LeanSqlCorpusTest...). They are out of scope. Every file I quote was checked at d585e5b8.

## What I read

- In full: the whole diff; at d585e5b8, EngineTable, EditionTable, LiteEditionTable, Styled, EdgeLoweringForms,
  CrossTypeRule, LiteCrossTypeRule, EngineComparisons, PureComparisons, CrossTypeComparisons, EqualityClasses,
  TypedCrossTypeComparison, TypedQuery, CrossTypeRuleTest, PureComparisonsTest, the PCT Postgres pin.
- In part: Typer (constructor, inFunctionScope, the scope, compared, holdsAQueryAnswer, synth's head), Overloads
  (applyGeneric's mint, inlineNormalized, the rollback loop at 715-745, candidatesOf), SpecCompiler (compile, the
  marked typing, islands), PureText (UPSTREAM, PURE_V1, ENGINE_UNCHECKED), PlanEnv.styled/editionTable,
  ProductStatements.prepare, Scalars (shift, rem, sqrt, quotient, pureSubstring, guarded), Coercions, DecimalKindRules,
  ListRules, ListEncodings.slice, CastPolicy, Lowerer's four call sites, PureModelContext.isSubtype, NavigateChecker's
  rebuilds, StaticFold.relationColumns, NormalizeFolds, PctPlanRoute.execute, ChannelB.typed, Audience,
  PureTestRunner, MinimalCorpus, LeanSqlCorpusTest, QueryCaptureListener, the guard tests' edited entries.
- Searches: every old `forms.raise`/4-arg `guarded` site; every `Styled` construction; every `EditionTable`
  implementation; every door's `Compiler.query`/`queryResolved` under edge/ and test/; every catch in compiler/spec;
  every typing-time pattern match on `TypedCBoolean`/`TypedCInteger`/the comparison calls; every rebuild
  (`withInfo`/`withChildren`/`mapChildren`) in compiler/spec.

## Verdicts on the claims

**Claim 1 (product road untouched).** Change A: no counterexample. LiteEditionTable.checks is always true;
ProductLoweringForms.raise always took `checked`; the lazy supplier changed nothing observable; every old site
(the 4 `forms.raise` sites and the 7 four-argument `guarded` calls) now asks `checks`, and the 3-argument `guarded`
sites asked nothing before and ask nothing now. Change B: the typed tree is the same for every query I could
construct (QUERY gives the marker, BODY and UNREAD give the call, the held-argument rule picks the same arguments).
One narrow counterexample on refusals: finding 2 (a body that compares two classes, one with a broken super, now
throws where it typed before).

**Claim 2 (change A at legend-engine's door).** No counterexample. For an unstamped call, or a null call,
`checks` is `!ENGINE_UNCHECKED.contains(kind)`, which is exactly the old `PURE_V1` form's `plain.contains(kind)`,
negated. Every `n` passed is the rule's own call (toOne in Coercions, divide in DecimalKindRules, the range call,
rem, sqrt, quotient, substring, the shift). Cast and slice pass null, as documented. At d585e5b8, PCT and the corpus
use `UPSTREAM`. Its `twoForm(Edition)` returns the product form (LiteEdition), so LiteEditionTable is used and every
check is written, as under the old `EdgeLoweringForms.PCT`. The uncommitted ENGINE_TESTS work changes this for the
corpus (finding 9). The one gap is the record, which understates which answers move (finding 4).

**Claim 3 (change B, no answer changes at any door).** Mostly holds. One real difference at legend-pure's door:
its refusal is now thrown during typing (finding 1). Points (a), (b), (d), (e) and (f) of the brief: no
counterexample. Details are in findings 2, 7, 8 and 10.

## Findings, by severity

No BLOCKER. No MAJOR.

### 1. MINOR: legend-pure's refusal now fires inside the typer, where trial typing does not catch it
`core/src/main/java/com/legend/edge/PureComparisons.java:57,81`; `core/src/main/java/com/legend/compiler/spec/Overloads.java:733-741`.
- Before: legend-pure's door refused only the markers that survived into the final typed body (`asPures` walked
  `query.body()` after typing).
- Now: `inQuery` throws `NotImplementedException` at the moment the comparison is typed.
- The overload search with rollback (`for (TypedFunction cand : ranked) { try { bindDeferredAndBuild ... } catch
  (TypeInferenceException e) ...}`) catches only `TypeInferenceException`. So a refusal raised while a candidate
  that will be rejected is being tried aborts the whole query. Before, that trial's marker was simply thrown away.
- Scenario (legend-pure's door, channel B or PctPlanRoute's LEGEND_PURE):
  - two overloads that tie on their value arguments: `f(p: Function<{Integer[0..1]->Boolean[1]}>[1], s: String[1])`
    and `f(p: Function<{Float[0..1]->Any[*]}>[1], s: String[1])`;
  - with `z: Float[0..1]`, the query is `f(y|if($y == $z, |1, |2), 'a')`;
  - trying candidate 1 types `$y == $z` as Integer against Float, both sides may be empty, so the refusal is thrown.
    Before, candidate 1 failed on its Boolean return, candidate 2 typed with no crossing, and the query was answered.
- Second effect: a body with such a comparison followed by an ordinary type error used to report the type error
  (typing finished first). Now it reports the refusal, with a different exception type and message.
- I found no PCT or corpus case that does this. Rollback across lambda parameter types is rare, so this is
  theoretical.
- Fixes: (a) answer with a marker-like refusal node and refuse after typing, as lite's planner does; or (b) collect
  the refusals and raise them when the query's typing ends. Or say in the record that the refusal now fires during
  typing.

### 2. MINOR: the product now runs the crossing rule in every function body, and it can throw there
`core/src/main/java/com/legend/compiler/spec/Typer.java:159,218`; `EqualityClasses.java:101`; `PureModelContext.java:244`.
- `inFunctionScope` and the body typed into its call are now `BODY`, so `EqualityClasses.crossing` runs for every
  comparison in every body. Before, `marking` was false there and nothing ran.
- For two class-typed sides it calls `ctx.isSubtype`, which rethrows a poisoned super's failure (`throw r.failure()`).
- Scenario:
  - a tolerant module (`buildModule`) with `Class x::A {}`, `Class x::B extends x::Missing {}` and
    `function x::f(a: x::A[1], b: x::B[1]): Boolean[1] { $a == $b }`;
  - `compileAllBodies` now returns a wall for `x::f`. Before, `x::f` typed as the call.
  - The same happens to a product query whose inlined body holds such a comparison.
- The error it reports is true (the model is broken), but it is a new refusal on the product road, in an already
  broken model.
- Lesser effects:
  - Cost: a record object and a lookup per comparison in every body, for an answer every setting
    (`LiteCrossTypeRule`, `EngineComparisons`, `PureComparisons`) discards (`inBody` returns `comparison()` in all
    three).
  - Loading: `subtree`/`isSubtype` now run for user classes in bodies. Both go through ModelContext, so invariant 5's
    rule (always load through the context) is respected, but it is new work on a hot path.
- Fix: skip the crossing in BODY until a setting's `inBody` needs it, or catch nothing and accept it, but record it.

### 3. MINOR: `TypedQuery#comparing` is listed as kind OPTION, but CrossTypeRule is an open interface of code
`core/src/test/java/com/legend/PlannerSurfaceTest.java:1078` (and 1212).
- OPTION is "one of a closed set of words or a flag, refused by name when unknown".
- `CrossTypeRule` is a public, unsealed interface. Any caller can pass an implementation; CrossTypeRuleTest's
  `Recording` does. That implementation's returned `TypedSpec` is spliced into the typed tree and planned.
- The dated note calls the parameter "one of the settings the doors hold (lite's, legend-engine's, legend-pure's)",
  but nothing makes the set closed.
- Fix: list it as CALLBACK (a caller's function called back with a planning-side value), or close the set. Note that
  a sealed interface in the unnamed module needs its permitted classes in the same package, so closing it means
  moving the three classes or adding a guard. The entry is dated, so this is about accuracy, not a guard moved
  without a note.

### 4. MINOR: the record understates which answers move at legend-engine's door (change A)
`core/src/main/java/com/legend/edge/EngineTable.java:131` (`checks`); `planner/Edition.java:26-28`.
- At d585e5b8 the stamp covers four places:
  - a query-level `let` that reads no store;
  - a last statement that reads no store;
  - a model-to-model mapping's transforms under a graph fetch or a model connection;
  - a checked graph fetch's constraints.
- Every check of class A in those places is now written. So `|5->rem(0)`, `|-1.0->sqrt()` and `|[1,2]->toOne()` as
  a whole pure/v1 query now raise lite's (Pure's) error, as does a `rem(0)` in an M2M transform under a graph fetch.
  Before, all of these were the plain operation, the database's own answer.
- The progress log (§6a, "Step 3") names only "a host-side `let`" as the answer to confirm with the user.
- This is consistent with the stated design, but AGENTS.md's fidelity rule requires each moved answer to be posted
  and recorded. List the other three places (and a SEMANTICS_REGISTER row) when this goes to the user.

### 5. MINOR: the tests leave parts of the claims unpinned
- `CrossTypeRuleTest`'s `Recording` answers the call, as legend-engine's setting does. Nothing tests:
  - legend-pure's literal answers (`false`, `-1`) keeping a query-written argument as typed. For example,
    `|#TDS..#->x::keep(1 == 1.0)` under `asPures` should still answer `false`, not be re-typed in the body as the call.
  - legend-pure's refusal being raised during typing, with its type and message (PureComparisonsTest has no refusal
    case).
- `EngineTablePlacementTest.aCheckLegendEnginesSqlDoesNotWriteIsWrittenOutsideAStoreQuery` pins only `rem`. Five
  sites now pass `n` with no stamped-call test, so a wrong `n` there would go unseen: the shift, sqrt, divide
  (DecimalKindRules), toOne (Coercions) and range (ListRules). Nothing pins the three non-`let` stamped places of
  finding 4 either.
- `HookRulingTest.plainOf` asks `checks(k, null)` only. It proves the unstamped column, never the stamped one.
- Lite's held-argument marker is pinned (`CrossTypeComparisonsTest`, "an argument of an inlined call keeps its
  marker"). Good.

### 6. MINOR: stale comments and dead code left by the diff
- `core/src/main/java/com/legend/lowering/Styled.java` (javadoc `@param forms`, lines 18-20 of the file, and the
  `PRODUCT` javadoc): still says `forms` carries "the set of raises the style wrote plain" and "the edge's pure/v1
  style legend-engine's census class A plain", and "no plain raise, ProductLoweringForms". It is the table now.
- `core/src/main/java/com/legend/edge/EdgeLoweringForms.java:10-17`:
  - the class javadoc still documents `@param plain`, a record component that no longer exists;
  - "legend-engine's choices" is no longer accurate for PCT;
  - two blank lines are left where the imports went.
- `core/src/main/java/com/legend/edge/PureText.java:114`: "legend-pure's guarded words" now describes nothing; the
  form no longer holds the raises.
- `core/src/main/java/com/legend/compiler/spec/CrossTypeComparisons.java:51`: `holds` is now unused (its one caller
  was `Overloads.inlineNormalized`). Delete it.
- `CrossTypeComparisons.java:41`: `in`'s javadoc ("what a door that rewrites them loosened") describes the removed
  rewrite; `in` is now test-only.
- `core/src/test/java/com/legend/compiler/spec/CrossTypeComparisonsTest.java:93,102`: the display name ("a door's
  rewrite replaces each marker") and the helper's javadoc ("as legend-engine's door does (edge.EngineComparisons)")
  describe the removed rewrite.
- `SpecCompiler.typeQueryBodyMarked` (`SpecCompiler.java:331`): the name now says "marked" for a typing that marks
  nothing under the doors' settings. A rename is a NOTE.

### 7. NOTE: the held-argument rule by identity is robust in what I found, but fragile for lite's marker
- `Typer.holdsAQueryAnswer` matches by identity; the old `CrossTypeComparisons.holds` matched by `instanceof`.
- An answer loses its identity only if the node itself is rebuilt between the mint and the enclosing
  `inlineNormalized`:
  - legend-pure's literal answers are leaves, so rebuilds return them unchanged;
  - legend-engine's (and legend-pure's ORDERING) answer is the call, and re-typing it in BODY gives the same call;
  - only lite's marker would change its answer if it lost its identity: it would be re-typed as BODY, become the call,
    and the planner would not refuse it.
- The only typing-time rebuild I found over such a subtree is NavigateChecker's `repointReads`/`renameVar`
  (legacyNavigate). `TypedNavigate.children()` still holds each route's original condition, so the original marker is
  still found.
- No counterexample. A comment naming the requirement ("an answer node is never rebuilt before its call's
  inlining"), or a test, would keep it honest.

### 8. NOTE: `comparing` makes a second TypedQuery and SpecCompiler per door query
- Callers: I checked every door path. All set their setting before the body is read: PureV1Routes' three routes and
  HostAnswers now call `asLegendEngine` before `runtimeOf`, so the body is typed once instead of twice; loweredSeed,
  ChannelB, PctPlanRoute (options kept), Audience/PureTestRunner, MinimalCorpus, LeanSqlCorpusTest. No caller hands
  `comparing` a `withBody` query; that case is refused by name.
- Unchanged and pre-existing: `ServiceTestRunner:259,286` and `PureV1Routes:216,253` (resultType only) apply no
  setting, as before.
- Two side effects:
  - QueryCaptureListener (`Compiler.java:754`) keys captured queries by identity, so edge-door queries are no longer
    captured. The author noted this. Arguably right, since those are not product queries.
  - One wasted `SpecCompiler`/`Typer` construction per door query. Cheap.

### 9. NOTE: claim 2's "the corpus hands lite's edition" holds at d585e5b8 only
The uncommitted working tree switches the corpus to `PureText.ENGINE_TESTS` / `EngineTable.TESTS`. With that,
`checks` is asked for the corpus's statements, and class A becomes plain inside its queries. Audit that step
separately.

### 10. NOTE: tenet 6 and the edition exception
`CrossTypeRule` is a second door setting, read by `compiler.spec.Typer`. AGENTS.md's edition exception says the
typer reads the edition "in one package (`compiler.spec.edition`)". The design record (§6a, F5) already says these
are to be reconciled. The default (`SpecCompiler(ctx)` gives lite's setting) is safe: a door that forgets its setting
gets a marker, which lite's planner refuses loudly.

### 11. NOTE: guard edits (claim 4), each checked
- `CodeShapeGuardrailTest` MUTABLE_FIELD_ALLOWLIST: `Typer.marking` renamed to `Typer.compared`. Dated 2026-11-26,
  count unchanged, needed (the field was renamed). The new `queryAnswers` and `crossTypes` fields are final.
- `EdgeOnlyMembersTest`: adds `TypedQuery#comparing`. Dated. Its only main-code callers are the edge's two settings.
- `PlannerSurfaceTest`: two entries for `comparing`. Dated. The kind is questionable (finding 3).
- `JavaEvalLedgerTest`: the comment of the existing EngineComparisons/PureComparisons entry is reworded. No pin
  moved.
- `HookRulingTest` and `CastNoReWrapTest`: moved to the new API; the expected sets are unchanged.
- `JudgeJsonTest`: `assertSame` (no rewrite) is weakened to `assertEquals` on the bodies. Justified in a dated
  comment, and still pins "nothing cleaned".
- PCT Postgres expected failure (`Test_LegendLite_Postgres_StandardFunctions_PCT.java:53`): the message is respelled
  to channel A's text. The row has a dated note from the same day; the respelling itself has none (only the commit
  message). It is needed to match, and no ceiling moved.
