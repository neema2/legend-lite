# ARCH: verdict SOUND-WITH-DEBT

## Summary (at most 12 lines; answer your brief's questions first)

- **Right design?** Mostly yes. The skeleton is right: one compiler and one runner (G1), a style handed to the planner at the door, the edge in its own Bazel library that the product cannot name (G5, enforced by the build), and a `ServiceLoader` opt-in that is looked up only with `--engine`. The question is what goes through that skeleton. 12 of the 21 hook kinds, and 5 of `PlanStyle`'s 10 methods, carry *presentation* into the plan at planning time; 8 of those kinds build the text in SQL. Only about 9 kinds are essential: real differences in computation or judging. The rest of the complexity is accidental: three lookup shapes inside one interface, closed lists checked at run time instead of by javac, a stateful style that records words, and about 101 pinned lists across 18 guards.
- **Concepts per new behaviour:** today about 7 extension mechanisms plus about 15 ruling names. The writer path (`Presenter` / `PureV1Writer` plus one register row) takes 2, and it is exactly the next program's model.
- **Distance from "one plan, two presentations":** medium, and the PR points the right way. Query answers are already Java writers over typed rows. Hosts are already split into a plan half and an answer half. `generatePlan` already exports a lite plan into legend-engine's plan format. Raise sites are typed.
  - What has to be undone is internal Java: the roughly 2,000 lines of SQL text builders, the hook shapes that return `SqlExpr` for presentation, and the database judge's words in SQL (both lite's and upstream's).
  - One premise of the next program is not in the code. No raise carries its operand *values* back from the database, so a sentence like "Cannot divide 5 by zero" cannot be worded in Java except from literal operands (ARCH-6).
- **The narrow BLOCKER test (an expensive-to-undo commitment that conflicts with the next program):** nothing passes it.
  - The plan format is exact-version only, every reader is in this repository, and no plan is stored.
  - `PlanStyle` is in-repo API; no Java artifact is published.
  - The shipped roots already start at the product, with the edge as an opt-in.
- **Fold the presentation move into this PR?** No: make it the very next PR. Its open decisions need the user's rulings (kind-B rows, raise operands), it changes no product answer, and folding it in recreates the long-lived branch. Its first commit should remove the edge net's SQL pin (ARCH-5).
- **Fit to merge as the foundation?** Yes, after one narrow fix. The edge host routes (TypeScript tab and Python) lost legend-pure's words for raises in the final train's K10 change (ARCH-1). The fix is about 20 lines, or the user can rule it as a register row instead.

## Blockers (if any; each one also appears in Findings)

- **ARCH-1** [WRONG-ANSWER] [BLOCKER]: the edge host routes answer lite's sentence where the server answers legend-pure's words for a planned raise site. `siteWords` is written into the host plan half, and neither host reads it. Introduced by K10 (v58) in the final train; no test covers it.

## Strengths

- **S1. One road, edge on or off (G1).**
  - Edge off: `ConnectionResolver.liteRunner` calls `Execution.runOnProjectDatabases(query, runtime, sources, values, output)`, which calls `TypedQuery.projectPlan(runtime)` with `PlanStyle.PRODUCT`, and then `ProductRun` (`core/src/main/java/com/legend/server/ConnectionResolver.java:48-51`, `core/src/main/java/com/legend/TypedQuery.java:239-240`).
  - Edge on: pure/v1 makes the same call with only the style changed (`core/src/main/java/com/legend/edge/EdgeServer.java:38-42`). PCT does the same (`pct/src/test/java/org/finos/legend/lite/pct/extension/PctPlanRoute.java:50-58`).
  - No second planner, runner or evaluator exists. Verified.
- **S2. The direction rule (G5) is held by the build, not by convention.** The edge is its own `legend_java_library`. The shipped server's main is the product's (`core/BUILD.bazel:565-579`; the edge module `:510-517`). A module is looked up only when `--engine` names one (`core/src/main/java/com/legend/server/LegendHttpServer.java:612-628`, `core/src/main/edge_server/META-INF/services/com.legend.server.MountProvider`). Closure tests use `genquery` (`tools/deps/BUILD.bazel:499-524`). Verified.
- **S3. Result presentation is already Java over typed rows, at the boundary.**
  - The edge writes pure/v1's JSON and Arrow answers after the run: `EngineAnswers.engine` / `.arrow`, built on `PureV1Writer` and `Presenter` (`core/src/main/java/com/legend/edge/EngineAnswers.java:29-69`, `core/src/main/java/com/legend/edge/Presenter.java`).
  - PCT writes legend-engine's JSON from the rows it collected, after the run (`PctPlanRoute.java:61-75`).
  - This is the next program's target design, already working for query answers.
- **S4. The host split is the import/export seam's shape.** `HostAnswers.jsonPlan(body, models, style)` returns the statement, columns and envelope, and `jsonAnswer(plan, rows)` writes upstream's bytes from the host's typed rows (`core/src/main/java/com/legend/edge/HostAnswers.java:234-300`, `:310-`). That is one plan with the presentation at the answer half.
- **S5. Typed facts reach the plan; upstream words left it.**
  - Raise sites carry kind and where (v56). Decimal digits are plan data (v57).
  - K10 removed the `upstream` field from the product's sites (`core/src/main/java/com/legend/executionplan/ExecutionPlan.java`, records `Site` and `DecimalDigits`; `core/src/main/java/com/legend/executionplan/PlanJson.java:171`, version 58).
- **S6. Export already half exists.** `PureV1Routes.generatePlan` turns lite's `QueryPlan` into legend-engine's `SingleExecutionPlan` JSON (`core/src/main/java/com/legend/edge/PureV1Routes.java:262-288`, `:363-`). legend-engine's statement-shape printer (`EdgeLoweringForms.EngineShape`, `EnginePlans`) is used only for upstream's plan text, not for the SQL that runs (`core/src/main/java/com/legend/edge/EdgeLoweringForms.java:180-`).
- **S7. The host judge is already a two-form Java presenter over typed values.**
  - `HostWords` has 28 methods, implemented by `LiteHostWords` and `EdgeHostWords`.
  - Its rules come through `HostForms` (`LiteHostForms` and `CompiledPureHostForms`), handed in by the run hook (`core/src/main/java/com/legend/testexec/HostWords.java`, `core/src/main/java/com/legend/edge/Appeals.java:200-201`).
  - This is the pattern the database judge should converge on (ARCH-3).
- **S8. The product answers its own form, explicitly, everywhere.** `PlanStyle.PRODUCT` passes each product form by name, with no null and no default branch, and every facet is refused by name (`core/src/main/java/com/legend/database/PlanStyle.java:94-153`).

## Findings

### ARCH-1 [WRONG-ANSWER] [BLOCKER] The edge host routes drop legend-pure's raise words: `siteWords` is written and never read
- **Where:**
  - written: `core/src/main/java/com/legend/edge/HostAnswers.java:212` (arrowPlan) and `:279` (jsonPlan), after `PureText.recording(style)` and `recording.read()` (`:185-187`, `:239-242`)
  - read on the server only: `core/src/main/java/com/legend/edge/RaisedWords.java:22-53`
  - the hosts' failure paths: `engine-client/src/raised.ts:67` (`new PlanRaise(site.kind, site.message, ...)`) and `python/legend_lite/_raised.py:95` (`return sites[n - 1]['message']`), reached from `engine-client/src/legend/edge-engine.ts:58-63` and `python/legend_lite/edge/_pure_v1.py:200-238`
- **What:** K10 (plan v58, the final train) took legend-pure's words out of the plan's sites. It moved them into a planning-call recorder that two places read:
  - The server's pure/v1 route reads it in-process through `RaisedWords` and rewords the `DataError`.
  - The host plan half (`jsonPlan` and `arrowPlan`) copies it into a `siteWords` field.
  - Neither the TypeScript host nor the Python host reads `siteWords`: `grep -rn siteWords --include=*.ts --include=*.py` finds nothing. The answer half (`jsonAnswer`) presents rows only and has no failure path.
  - Before K10 the tab used `site.upstream ?? site.message` (commit `b241648e2`, which changed `raised.ts:67`). On `main`, raises had no site tokens, and the dialect wrote legend-pure's text in SQL.
  - So on the shipped engine artifacts (`//wasm:engine_planner`, `legend-lite[engine]`), a raise of a site whose operands are literals now answers lite's sentence. Examples: DATE_PART_ABSENT, INDEX_OUT_OF_BOUNDS and LIST_LENGTHS_DIFFER, which are kinds pure/v1 still checks.
  - For the same query, the server answers legend-pure's words.
  - Raises whose operands are computed still answer legend-pure's words in the tab, because the edge dialect writes those in SQL. The same route therefore answers inconsistently.
- **Why it matters:**
  - It breaks done-criterion 2 of the program ("a legend-engine client gets exactly upstream's bytes"), whose §7.1 requires edge-on routes byte-exact.
  - It breaks server-versus-host parity.
  - No golden covers a checked pure/v1 raise: `core/src/test/resources/edge/pure_v1_answers.tsv` has only unchecked kinds (divideByZero, sqrtOfNegative, parseInteger), plus two compile refusals.
- **Instead:** pick one, before merge:
  - In the failure path, each host words a raise from `siteWords` (statement text and `n`) when the answer it read carries them: about 10 lines in each of `raised.ts` and `_raised.py`. Add one test per host with a literal-operand raise, for example `#>{...}#->extend(~h: x | %2024->hour())` under pure/v1.
  - Or give the answer half a failure entry (`errorAnswer(plan, raisedSite)`), so the edge, not the host, presents it. This is the next program's shape.
  - Or have the user rule the divergence into a `docs/SEMANTICS_REGISTER.md` row.
- **Confidence:** the code path is verified: the field is written, both hosts' failure paths return `site.message`, and the history shows `upstream` was read before K10. The end-to-end input is suspected. I did not run the tab or Python, and I could not prove a particular query reaches a literal-operand checked site through `jsonPlan`'s gates (no bound values, no loads, a TDS root).

### ARCH-2 [WRONG-DESIGN] [FIX-AFTER] Most of the hook surface carries presentation into the plan at planning, much of it as SQL
- **Where:** `core/src/main/java/com/legend/testplan/PlanStyleFacets.java:24-26` (5 `KINDS`) and `:32-77` (16 `TWO_FORMS`); `core/src/main/java/com/legend/database/PlanStyle.java:24-90` (10 methods).
- **What:** I classified each kind by what it means (my classification, from the signatures):
  - **Presentation, 12 of 21.**
    - 8 build text in SQL: `DateTimeTexts`, `ListTexts`, `TestDataPlanner.Printing`, `ExecutedValues`, `VerdictWords`, `RelationTexts`, `PureOnlyTexts`, `ResultEnvelopes`. Each returns `SqlExpr` or `TypedSpec`.
    - 4 compose text in Java at planning: `DateLiteralTexts`, `RoutedQueries`, `ActivityTexts`, `UpstreamPlans`.
  - **Computation or judging, 9 of 21:** `LoweringForms` (mostly), `DatabaseSettingsForm`, `VerdictNumbers`, `CrossKindNumbers`, `RenderedTextRule`, `GoldenNullWord`, `JsonGrammar`, `RelationCounting`, `EngineTestText`.
  - **`PlanStyle` methods:** `traceCell` (composed in SQL by `PlanEnv.traceText`, `core/src/main/java/com/legend/planner/PlanEnv.java:173-177`), `errorWords`, `siteWords`, `graphKeyDate` and `graphKeyString` are presentation.
  - **The recorded SQL text builders:** 1,992 lines at the head, against the handoff's 1,978 at `4e8702016`. The +14 is refactoring, not new SQL-text logic: `RelationTextLowering` +1, `PureMessages` +2, `UpstreamVerdictWords` +7, `EngineResultEnvelopes` +4.
  - **Not in that count:**
    - `TestDataRoutes.seedJson` builds legend-engine's seed answer JSON in SQL (`core/src/main/java/com/legend/edge/TestDataRoutes.java:238-262`, about 25 lines).
    - `EdgeLoweringForms.oneBare` builds legend-engine's JsonBuilder cardinality envelope in SQL (`core/src/main/java/com/legend/edge/EdgeLoweringForms.java:71-95`).
    - The trace comment is composed in SQL (`PlanEnv.java:173-177`).
- **Why it matters:** it is the plan of record's own §2.2 / §4 item 1 drift, now quantified as hook surface. Under "one plan, two presentations" these 12 kinds become writer choices that the plan names as data. The planner's hook surface shrinks to the 9 computation kinds plus the dialect.
- **Instead:** this is the next program's work, as written. Its guard (step 5) should count SQL-building presentation kinds as well as lines, and include the three uncounted sites above.
- **Confidence:** verified (signatures and line counts read). The classification is a judgment.

### ARCH-3 [WRONG-DESIGN] [FIX-AFTER] Under the compatibility rules, the database judge computes legend-pure's failure words in SQL on every run, and then discards them
- **Where:**
  - `core/src/main/java/com/legend/testexec/IsolatedRun.java:97-106`: the database run answers nobody.
  - `IsolatedRun.java:112-130`: only the host's reports reach callers.
  - `IsolatedRun.java:147-180`: with `messages=false`, verdicts alone are compared.
  - `core/src/main/java/com/legend/edge/AppealRun.java:36-39` (`wordsAlike()` is false); `pct/src/test/java/org/finos/legend/lite/pct/channelb/ChannelB.java:399` (BOTH is the default); `core/src/main/java/com/legend/edge/UpstreamVerdictWords.java` (198 lines, `SqlExpr` builders).
- **What:** In BOTH mode under the edge, the database judge's plan carries `UpstreamVerdictWords`' CONCATs, plus `UpstreamDateLiteralTexts` and the `PureOnlyTexts` used by `VerdictSql`. The database computes them. The words are used only in a `JudgesDisagree` diagnostic and when the judge mode is DATABASE alone (`spec/src/test/java/com/legend/rcorpus/LeanSqlCorpusTest.java:50`).
  - Each failure sentence exists in up to four implementations: lite and upstream, each in Java (`HostWords`, 28 methods) and in SQL (`VerdictWords`, 24 methods).
  - Under lite's rules, the two lite forms must stay equal byte for byte, which the run checks.
- **Why it matters:** it is presentation in SQL with almost no reader. It inflates the pinned judge-plan sizes (`channel_b.essential.judge.plan`, 28.68M characters) and doubles the work of every wording change.
- **Instead:** the database judge returns typed verdict operands (pass, expected, actual, index), and `HostWords` words both judges. Retire `VerdictWords`, `LiteVerdictWords` and `UpstreamVerdictWords` (portability program, step 2). Interim: under the edge, plan the database judge with lite's words, since nobody reads them.
- **Confidence:** verified.

### ARCH-4 [WRONG-DESIGN] [FIX-AFTER] `PlanStyle` is a typed service locator: three lookup shapes, refusals at run time instead of javac exhaustiveness, and hand delegation
- **Where:**
  - `core/src/main/java/com/legend/database/PlanStyle.java:47` (`twoForm(Class<F>, F)`), `:79` and `:90` (`facet(Class<F>)` and `facet(Class<F>, Supplier)`), plus 6 single-purpose methods.
  - `core/src/main/java/com/legend/edge/PureText.java:110-178`: a 16-arm `if (kind == X.class)` chain ending in `throw ... none of the declared kinds`.
  - `PureText.java:218-283`: `PURE_V1` delegates 9 of 10 methods to `UPSTREAM`.
  - `core/src/main/java/com/legend/edge/SiteWordsRecorder.java:80-123`: delegates 9 of 10.
- **What:** One style object lives in `com.legend.database`, below every layer that owns a kind, so it cannot name its kinds. Kinds are therefore looked up by class token at run time. The lists are then held in place by guards (`PlanStyleFacetsTest`, `HookRulingTest`'s exact-set pins, `PlanStyleSurfaceTest`). AGENTS.md invariant 3 rejects exactly this pattern for render methods ("javac enforces exhaustiveness").
  - A per-layer typed record already exists: `lowering.Styled` (`core/src/main/java/com/legend/lowering/Styled.java`), assembled in `PlanEnv.styled()` (`PlanEnv.java:181-186`).
- **Why it matters:** every new kind touches four places: the interface, the closed list, the edge's if-chain, and sometimes `PURE_V1` and the recorder. Each of those touches a pin. That ceremony is the accidental complexity the handoff's §4.8 describes.
- **Instead:**
  - Give each layer a typed record of its own kinds: `lowering.Styled` (exists); a judge record for `VerdictNumbers`, `CrossKindNumbers`, `RenderedTextRule`, `GoldenNullWord`, `JsonGrammar` and `RelationCounting`; a planner record for `ActivityTexts`, `RoutedQueries`, `ExecutedValues` and `UpstreamPlans`.
  - The door builds them: the product from constants, the edge from its own forms. `PlanStyle` keeps the dialect alone.
  - `PURE_V1` becomes `UPSTREAM` with one component replaced. A missing form then fails at compile time, and the closed-list guards collapse.
  - Do it as part of the next program, since about 12 of these kinds change shape anyway.
- **Confidence:** verified.

### ARCH-5 [WRONG-DESIGN] [FIX-AFTER] The edge golden net pins the edge's SQL text through `activities`, which no PCT verdict reads
- **Where:** `pct/src/test/java/org/finos/legend/lite/pct/extension/PctCaseRecorder.java:108-114` records `activities` (each holding the executed SQL) "so a move of the edge's plan text is a visible line too". `pct/src/test/java/org/finos/legend/lite/pct/extension/ValueBridge.java:550-578`: `engineTDS` reads only `builder.columns` and `result.rows`. `tools/edgenet/EdgeNormalize.java` normalizes rows only.
- **What:** 1,490 golden lines in `pct/src/test/resources/edge/*_answers.tsv` carry SQL that legend-pure's PCT adapter never reads. Any redesign of the edge's SQL, which is the next program's whole point, re-records them. This is the mechanism behind the handoff's §4.3 ("the safety net rewarded copying").
  - pure/v1's goldens differ: there the activity SQL *is* answer bytes a client reads (`core/src/test/resources/edge/pure_v1_answers.tsv`).
- **Why it matters:** it is the first obstacle to the next program, and it costs one line to remove.
- **Instead:** as the next PR's first commit, `EdgeNormalize` drops `activities[].sql` from PCT answer lines (keeping it for pure/v1). With that, the net compares outcomes and answer bytes, as the handoff recommends. Separately, give the net a per-push CI lane (F33/D6).
- **Confidence:** verified.

### ARCH-6 [WRONG-DESIGN] [NOTE] Raise values never return from the database, so "word the raise in Java after the run" holds only for literal operands
- **Where:**
  - The token carries only `LG#<n>`: `core/src/main/java/com/legend/sql/RaiseKind.java:61-66`.
  - The site holds kind, where and message: `ExecutionPlan.Site`.
  - The edge words at planning from literal operands only (`core/src/main/java/com/legend/edge/PureMessages.java:132-140`); computed operands go to a SQL CONCAT (`PureText.java:389-391`).
  - The stateful recorder is keyed by statement text: `SiteWordsRecorder.java:27-78`.
- **What:** The portability program's §7.1 says "the product's raise already carries its kind and typed operands... the edge writes legend-pure's sentence in Java from them, after the error comes back".
  - The operands are typed only in the MIR (`SqlExpr.RaiseText.operands`). At run the database returns the token, never the operands' values. That follows the user's 2026-11-10 rule ("a fixed text known at planning, never the value").
  - So a sentence like "Cannot divide 5 by zero" over a column cannot be worded in Java from what comes back.
  - Today the edge papers over this with two mechanisms: a planning-time recorder (a side-effecting `PlanStyle`, sealed after one read) and SQL CONCAT.
  - Related: SQL that `generatePlan` exports under `PURE_V1` can contain `LG#` tokens with no site table beside it (by construction: `PureText.java:389-391`, `PureV1Routes.java:280-288`). An imported or exported plan would need a translation rule for those tokens.
- **Why it matters:** this is the one premise of the next program the code does not support. It needs a design and a user ruling: an edge-only raise payload that carries operand values as typed data, or kind-B rows.
- **Instead:** decide it at the start of the next program. Keep the product's rule, and let the edge's style add operand slots to its sites (an additive plan-format field), so the recorder and `PureMessages`' SQL half both go.
- **Confidence:** verified for the code. The impact on the next program is my reading.

### ARCH-7 [WRONG-DESIGN] [NOTE] Plan v58 carries lite's raise presentation (`message`, top-level `raises`), derived and checked again on read
- **Where:** `core/src/main/java/com/legend/executionplan/PlanJson.java`: `raises(plan)` maps each kind to `ErrorKind.sentence()`; `readSites` rejects a `message` that is not its kind's sentence.
- **What:** Both fields are fully derivable from kind and where. They are lite's own form, so nothing upstream is in the product. They are still presentation in the plan, which the next program wants presentation-free.
- **Why it matters:** low. The format is exact-version (`PlanJson.java:1043`), every reader is in this repository (Java, TypeScript, Python), and no plan is stored, so dropping them later is one version bump. It is not an expensive commitment, so not a blocker.
- **Instead:** drop both fields when the plan gains its output-form field (next program, §5).
- **Confidence:** verified.

### ARCH-8 [WRONG-DESIGN] [NOTE] The output form is the caller's choice of writer, not data in the plan
- **Where:** `core/src/main/java/com/legend/edge/PureV1Routes.java:302-322` (`EngineAnswers.engine` handed in as the sink), `EdgeServer.java:40` (`RaisedWords` wraps the run).
- **What:** The portability program's §5 puts "which output form applies" into the plan as data. Here the door picks the writer and the error rewording. The plan cannot say how it is to be presented, so a plan exported or replayed elsewhere loses that.
- **Why it matters:** this is an additive change, so no conflict.
- **Instead:** add a presentation field to the plan (or to the answer-half contract) in the next program.
- **Confidence:** verified.

### ARCH-9 [BROKEN-RULE] [FIX-AFTER] A JVM system property chooses edge SQL at render, outside the style
- **Where:** `core/src/main/java/com/legend/edge/PureText.java:368` (`Boolean.getBoolean("legend.exec.engineScanOrder")` in `DuckDb.scanOrderPasses`).
- **What:** A process-global flag changes the planned SQL (the `StableScanOrder` pass), and neither the style nor the plan-cache key sees it. It was moved from the product's dialect (W6), not introduced. But it is now part of the edge's design, beside a `PlanStyle` built to carry exactly such choices.
- **Why it matters:** G10 (determinism), and the plan caches' rule that a memo keys on everything that wrote it.
- **Instead:** make it a style of the corpus (`PureText.CORPUS`, or a `LoweringForms` / `DatabaseSettingsForm` component), chosen by the corpus door.
- **Confidence:** verified.

### ARCH-10 [COSMETIC] [FIX-AFTER] Guard weight and vocabulary
- **Where:** `core/src/test/java/com/legend/*` (18 edge guard tests); `tools/guards/shrink_only.py` (980 lines).
- **What:** About 101 pinned `static final` Map/Set/List fields across 18 guard tests. Examples: `PlannerSurfaceTest` has 31, `EdgeSeparationTest` 10, `HookLambdasTest` 8.
  - The ruling vocabulary includes `READ_API`, `HOOK_ENTRIES`, `HOOKS_RULED` (22 HOOK-RULED census entries), `TWO_FORMS`, `KINDS`, `TWO_FORM_EXCEPTIONS`, `OVERRIDERS`, `IN_MEANING`, `RULED_IN_MEANING`, `REACH_OPEN`, `ROOTS_OPEN`, `COMPAT_CLASSES`, `FACADES` and `@OpenLine`.
  - The GUARDS unit is +17,228 lines, against an edge of 21,961.
  - A count of 0 includes 22 entries ruled away rather than moved. 7 `@OpenLine` classes still stand in product packages as ruled facets.
- **Why it matters:** it is the main cost multiplier on every change in the next program. The handoff's §4.8 says the same.
- **Instead:** once ARCH-4's typed records land, collapse the closed-list guards into compile-time checks. Keep the build closure tests and a few meaning guards.
- **Confidence:** verified (counts).

### ARCH-11 [COSMETIC] [NOTE] `generatePlan` plans the query twice
- **Where:** `core/src/main/java/com/legend/edge/PureV1Routes.java:275` and `:280`.
- **What:** `executionPlan(runtime, Set.of(), PURE_V1)` runs only to test for a `Constant` root, then `plan(runtime, PURE_V1)` plans again.
- **Instead:** plan once and branch on the root.
- **Confidence:** verified.

## Cross-area observations

- **HOSTS:** ARCH-1 belongs to that area too (`engine-client/src/raised.ts:67`, `python/legend_lite/_raised.py:95`). Both hosts also receive `siteWords` from `arrowPlan` (`HostAnswers.java:212`, Python's `_execute`) and ignore it.
- **LOWERSQL (suspected):** `PURE_V1`'s plain-raise set is applied only to raises routed through `LoweringForms.raise` (`core/src/main/java/com/legend/lowering/Scalars.java:3023-3025`).
  - `toOne`'s `CheckedOne` nodes are built without consulting the forms (`core/src/main/java/com/legend/lowering/Coercions.java:75`, `:83`, `:165`; `core/src/main/java/com/legend/lowering/Lowerer.java:1094`). They are rendered as raises by `AnsiSqlRenderer.checkedOne` (`core/src/main/java/com/legend/sql/dialect/AnsiSqlRenderer.java:804-818`).
  - So pure/v1 may still check SIZE_NOT_MULTIPLICITY, which `ENGINE_UNCHECKED` (`PureText.java:207`) lists as unchecked ("toOne's processNoOp"), where legend-engine writes no check.
  - The golden `raise.toOneOverMany` is an H2 refusal, so DuckDB is not covered.
- **PRODUCT / SAFETYNET:** follow-up F32 ("refuse internal natives by name to user code", marked "before landing (Phase 9)") is still open in `docs/records/edge-follow-ups-2026-11-18.md`. `typeRaise` and `cellText` are new in this PR (0 files on `main`, 1 at the head). Worth a check by whoever covers the product's API surface.
- **SAFETYNET:** the edge net is still not in CI (F33/D6). ARCH-5 is the change that lets it guard the next program instead of resisting it.
- **JUDGES:** ARCH-3. With BOTH and the edge, the database judge's upstream words reach nobody.

## Measurements (numbers with the command that produced them)

- **Edge size at the head:** 21,961 lines in `core/src/main/java/com/legend/edge` (93 files), plus 927 in the host edges.
  - Commands: `wc -l core/src/main/java/com/legend/edge/*.java | tail -1`; `find . -path '*/edge/*' -name '*.java' -not -path './core/src/main/java/com/legend/edge/*' | xargs wc -l | tail -1`.
  - On `main`, the edge package was 4 files and 302 lines: `git ls-tree -r --name-only main -- core/src/main/java/com/legend/edge/ | xargs -I{} git show main:{} | wc -l`.
- **SQL text builders:** 1,992 lines at the head, against 1,978 at `4e8702016`. Command: `for f in RelationTextLowering PureMessages UpstreamVerdictWords PurePrintForms EdgePureOnlyTexts EngineResultEnvelopes PureListTexts PureDateTimePrint EdgeDateTimeTexts; do git show <rev>:core/src/main/java/com/legend/edge/$f.java | wc -l; done`.

  | File | at `4e8702016` | at head |
  |---|---|---|
  | `RelationTextLowering` | 780 | 781 |
  | `PureMessages` | 479 | 481 |
  | `UpstreamVerdictWords` | 191 | 198 |
  | `EngineResultEnvelopes` | 88 | 92 |
  | the other five | unchanged | unchanged |

- **SQL string-literal and CONCAT sites per edge file:** `RelationTextLowering` 47, `PureMessages` 44, `EngineResultEnvelopes` 22, `EdgePureOnlyTexts` 14, `TestDataRoutes` 11 (not counted in D1). Command: `grep -c "SqlFn.CONCAT\|new SqlExpr.StringLit\|StringLit(" <file>`.
- **Hook surface:** `PlanStyle` has 10 methods, `TWO_FORMS` 16 kinds and `KINDS` 5 (`PlanStyle.java`, `PlanStyleFacets.java:24-77`). In product code, 43 files reference `PlanStyle`, with 16 `twoForm` call sites and 21 `facet` call sites. Command: `grep -rln PlanStyle core/src/main/java/com/legend | grep -v /edge/ | wc -l`; `grep -rn '\.twoForm(' ... | grep -v ^./edge/ | wc -l`; the same for `\.facet(`.
- **Edge-net golden lines carrying SQL that the PCT adapter never reads:** 1,490 (`duckdb_relation` 460, `h2_relation` 443, `sqlite_relation` 459, `duckdb_standard` 64, `sqlite_standard` 64). Command: `grep -c "SELECT\|select " pct/src/test/resources/edge/*.tsv`.
- **Pinned lists in the 18 edge guard tests:** 101 in total. Command: `grep -cE "static final (java\.util\.)?(Map|Set|List)<" <test>.java`. HOOK-RULED census entries: 22 (`grep -c HOOK-RULED EdgeSeparationTest.java` over its `Map.entry` lines). `@OpenLine` classes in product packages: 7.
- **PR lines added by unit:** guards +17,228, the safety net (goldens) +23,222, tests +35,650, edge +10,925 (net of moves), product +6,064 / -5,224. Command: an `awk` sum over `audit/units/*.txt`.
- **Plan format:** version 51 on `main`, 58 at the head (`grep VERSION core/src/main/java/com/legend/executionplan/PlanJson.java`). It is exact-version only (`PlanJson.java:1043`). No Java artifact is published: `grep -rn "maven_export\|java_export" --include=BUILD.bazel` finds nothing.
- **Host readers of `siteWords`:** 0. Command: `grep -rn "siteWords\|site_words" --include=*.ts --include=*.py --include=*.mjs .`, which finds nothing outside `node_modules`.
