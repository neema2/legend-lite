# A3: the `engine` rewrite, the design note (2026-11-19; revision 2, the reviewer's word, built)

B1-R (track O), for the independent reviewer before any build, on the compat owner's brief of ~20:06 UTC.

**The user's rulings this carries:**
- ~20:05: the `engine` opt-in **yes**, built in this batch, "let's not take shortcuts on the rewrite part just bc I
  asked, let's do the right design for the rewrite part";
- ~20:35: the door-only switch rule, with `LEGEND_LITE_PURE_MEANING` as its first entry.

The ~20:05 ruling amends D1 ("no translator") and the final plan's §2 part 2 for requests a server opts in, as H16 §5
set out.

**Evidence:**
- H16 and its addendum, on this branch;
- legend-engine 4.145.0's pinned sources, read here (each citation below checked);
- a throwaway test run in this session through the public API and never committed (section 3).

## Revision 2: the reviewer's word (record on claude/compat-review, "Design review: the `engine` rewrite")

APPROVED TO BUILD, with 1 must-fix to the design and 4 should-fixes. Each one is taken as follows.

**D-M1, how the sites are matched: by identity, from one walk, with each store-bound root inlined on its own.**
"Matched by position" is withdrawn. Under `engine` the door (`PureMeaningRewrite`, built):
1. runs the detector's walk on the query, which names its store-bound ROOTS: each statement that reads a store, and
   each let-bound lambda that store-bound code names;
2. inlines each of those roots ON ITS OWN, with the product planner's `UserCallInliner`: a statement, or a let's value
   with the let kept;
3. runs the detector's walk again on that body. The walk records each flagged call by identity (`Site.node`), only
   where it met the call in the query's own tree. A call it met in a body it followed (a function the inliner kept as
   a call, a mapping) has no node;
4. rewrites exactly those nodes through `TypedSpec.mapChildren`, within those roots alone;
5. refuses when the number of distinct nodes rewritten is not the number flagged.

**Why each root is inlined on its own, not the whole body (measured).** `inlineBody` beta-reduces a query's lets into
the statements that read them ("a let lives in the database or nowhere"). Inlining the whole body would therefore
splice a host-side `let`'s value into the store-bound statement. There the walk would flag it, and the rewrite would
give it SQL's meaning, where legend-engine computes it on the host with Pure's (H16's probe, `where region = 'bc'`).
Inlining each root on its own leaves a host-side let as written, and the planner's G½ reduces it later, unrewritten.

The plant R1 (the whole body inlined at once) turns four tests red:
- the D-M1 plant (`aFunctionCalledHostSideAndStoreSideIsRewrittenAtTheStoreSideUseAlone`);
- `aHostSideLetsCallStaysAsWritten_besideAStoreSideCallRewritten`;
- the let-bound lambda test;
- the store-bound let test.

One walk still decides both what is flagged and what is rewritten, so they agree by construction.

**D-S1, settled (the reviewer's addendum ca8ea4a5c, and the user's ~21:10 "Yes to all three for substring").** This
replaces the paragraph that pointed at `EdgeOnlyDataTest`.
- `meta::legend::lite::substringAsGiven` is lite-internal: not user-resolvable, in `Pure.COMPILER_OWN`, so a user who
  writes it is refused by name. The rewrite writes typed nodes after name resolution, so it is unaffected.
- `EdgeOnlyDataTest` cannot hold it, because `Scalars` names its `SqlFn`, a real product use. The honest record is
  where producers are counted:
  - its `native-claims.tsv` row, whose claimers are `Scalars` and the door's `EngineSubstitutes`, held by
    ClaimRegistryTest's pin;
  - `INTERNAL_DESUGAR` +1;
  - `SqlFnSortedTest` PRODUCT +1.
- Lite's own SQL substring (piece 1's `sqlSubstring`, clamped) and this native stay two natives, each javadoc saying
  which meaning it carries.

**D-S2, the substitute is typed as the typer would.** The replacement `TypedNativeCall`'s callee is the registry's
function for the native's `FunctionId`, with its declared signature. It is never a hand-assembled signature or
`ExprType`. B1 builds that construction once, in `com.legend.edge.EngineSubstitutes` (named `SubstringAsGivenCall` in
earlier messages, 1e6d9c68c's among them): `substring(ctx, call)` and `indexOf(ctx, call)`. The callee is resolved the
typer's way, and the shared `checked` refuses any callee or info that is not exactly the registry's (its test plants
four wrong callees). The rewrite calls it through one seam, `PureMeaningRewrite.Substitute`, and builds no
call of its own.

**D-S3, H2's route.** legend-engine's H2 SELECTs go through its dialect translation
(`relationalMappingExecution.pure:440-443`): `'substring'` is `functionCall('substring')` (`toPostgresModel.pure:418`)
and `'indexOf'` is `functionCall('strpos')` (`:381`). The H2 extension's `:262`/`:231` are not on that route. The
conclusion does not change: it is the raw call with the arguments as given, and the batteries measured 46 of 46.

**D-S4.** The edge's three engine-style printers are dropped from the native's part, since A2 deletes them.

**Built beyond the note:**
- the per-database facts are one record in `PureText`, `MEASURED` (`zeroStartAsPure`, `engineAnswers`), so
  `DialectBoundaryTest`'s pin stays at 2. H2 answers under `engine`. DuckDB stays refused until its batteries are
  measured with the native;
- the three test-data routes re-type their seed's query from the request. Under `engine` that query, the one whose
  meaning chooses the keys, is rewritten before it is lowered, so the routes answer and are not refused
  (`PureMeaningEngineTest`).

**The rewrite's plants (each a sabotage of `PureMeaningRewrite.java`, run, then restored; 12 tests):**

| plant | the tests it turns red |
|---|---|
| R1 the whole body inlined at once | the D-M1 plant, the host-side-let-beside test, the let-bound lambda test, the store-bound let test |
| R2 a store-bound let root not rewritten | `aStoreBoundLetIsRewrittenInsideTheLet_andEveryRootIsCounted` (refused by the count check) |
| R3 R2 with the count check gone | the same test (a half-rewritten answer) |
| R4 an unmeasured database let through | `aDatabaseNotMeasuredUnderEngineIsRefused` |
| R5 an unverified call let through | `anUnverifiedCallIsRefused` |
| R6 a call outside the query's tree let through | `anM2MChainsTransformsAreRefused_theStoreResolverReachesThem` |

**Measured at build** (2026-11-19, on a local merge of the detector branch with B1's `claude/compat-a3-raw-substring`
897db4bbe; the batteries' `engine` half, `//pct:battery_compare_engine_{h2,duckdb}`, pinned at
`pct/batteries/agreement_engine_{h2,duckdb}.tsv`):

| database | under `engine` | the rest |
|---|---|---|
| H2 | AGREE 46: every legend-engine answer | 2 DIFFER, the 3-argument `indexOf` (legend-engine refuses, lite answers: E-R2); 1 BOTH_FAIL |
| DuckDB | AGREE 47: every legend-engine answer, as section 2 predicted | 2 DIFFER, the 3-argument `indexOf` |

So DuckDB is enabled (`PureText.MEASURED`), by the design's own rule. The `warn` pins did not move.

**On the stage-A batch's tree** (2026-11-20, `claude/compat-a4-host-only` ced5708ec merged into
`claude/compat-a3-engine`, piece 2 under it, both halves regenerated there):

| database | `warn` (legend-pure's meaning, piece 2's) | `engine` (the wiring) |
|---|---|---|
| H2 | AGREE 11, BOTH_FAIL 1, DIFFER 37 (was AGREE 46: piece 2 moved it) | AGREE 46, BOTH_FAIL 1, DIFFER 2: the same counts as before piece 2 |
| DuckDB | AGREE 8, DIFFER 41 (was AGREE 31, DIFFER 18) | AGREE 47, DIFFER 2 |

The `engine` half's counts did not move with piece 2. Its two DIFFER rows (the 3-argument `indexOf`, `col.i04` and
`lit.i04`) changed only lite's side: the detector does not flag the 3-argument form, so it answers legend-pure's
meaning there now (`["Credit",2]` where it answered 3); legend-engine refuses it either way (E-R2).

**The wiring, through every route the detector covers** (`PureMeaningEngineTest`, run on H2 with the connection's
declared rows): execute, execute as Arrow, generatePlan, generateSeedData, generateTestData_WithDefaultSeed and
generateTestData_WithSeed each answer 200 under `engine`, with the header `engine <codes>`. A recording runner shows
that execute and Arrow are handed the rewritten query: `substringAsGiven` and `sqlPosition` by FunctionId, and no
String `substring` or `indexOf` left. So nothing downstream re-resolves the native by name.

| plant | the tests it turns red |
|---|---|
| W1 the engine branch plans the query unrewritten | the execute and Arrow tests, the unmeasured-database test; on the stage-A batch's tree 8 of the 13: also the three H2 class shapes, the DuckDB class-shape plan and the graph fetch's refusal |
| W2 the seed lowered unrewritten | before piece 2, the DuckDB seed's SQL alone (E-S2's test; on H2 lite's own `substring` was already the database's, so the seed's keys were the same); on the stage-A batch's tree (planted and run 2026-11-20) 3 of the 13: `generateSeedDataChoosesLegendEnginesKeysUnderEngine` and `generateTestDataWithTheDefaultSeedRunsUnderEngine` (the H2 seed), and `theSeedIsRewritten_whereAStartOfZeroDiffers` |

On the stage-A batch's tree the tests run both ways: each route that has an answer to read asserts legend-engine's
under `engine` and legend-pure's under `warn` (`'abcdef'->substring(1, 3)` is 'abc' and 'bc'; `'xoxo'->indexOf('o')`
is 2 and 1; the seed's row 3 is chosen, and is not). Two warn answers are failures, as legend-pure's are:
`'ab'->substring(1, 3)` runs past the string's end, so a query over the whole table raises under `warn` and answers
under `engine`; and a seed of no row is no filter (upstream's `orFilters([])`), so the default-seed route answers every
row under `warn` and row 3 alone under `engine`.

**A gap found:** the test-data scan (`ScanRelations`) cannot serve a function applied to a class's property inside a
filter, on any setting. It reads `$p.name->substring(...)` as a property hop ("scalar 'name' in MID position"),
because only its `OPERATORS` end a chain. The engine test seeds on a literal's `substring` for that reason.

**The notes for the build:**
- register rows S130, S134 and S383 name the `engine` value and what it does;
- the differences page's item 1 names the operator's option beside the hand rewrite;
- the guard moves, each with a dated note: `INTERNAL_DESUGAR` +1, native-claims and `SqlFnSortedTest` PRODUCT +1
  (B1's, with `EngineSubstitutes`' funnel row); the funnel register +1 for `PureMeaningRewrite.java` and the two new
  pins `agreement_engine_{h2,duckdb}.tsv` (B1-R's, ruled by the user ~22:20, "Yes to all three"). No `EdgeOnlyDataTest`
  entry (D-S1).

## The decisions, on one page

| question | decision | the reason, in one line |
|---|---|---|
| 1. the operation | **one new native, "the database's own SUBSTRING, over the arguments as given"**: a new MIR function, `SqlFn.SUBSTRING_AS_GIVEN`, which every dialect renders as its SUBSTRING and which `SubstringClamp` does not touch. `indexOf` uses piece 1's `sqlPosition` | legend-engine's answer IS the database's own SUBSTRING over Pure's arguments, so the database computes it (tenet 1). Nothing is imitated in Pure or Java |
| 2. per database | `engine` is enabled on **H2 and DuckDB**; every other database is refused by name | both extensions write the plain call (read below); the batteries are measured on both. Another database joins only when its extension is read and its batteries are measured |
| 3. helpers | **rewrite after the inliner (G½), through the public API**: `new UserCallInliner(query.specs()).inlineBody(body)`, then the flagged calls replaced, then `query.withBody(...)` | measured: inlining reaches user functions and qualified properties, and the inlined body plans to byte-identical SQL. No clones, no rebuilt model, no core hook |
| 4. the reach | rewritten: the query, let-bound lambdas, user functions, qualified properties, a `validate` constraint inline. **Refused by name**: an M2M chain's transforms, a `~func` mapping, an unverified site | the refused ones run in phase H (the store resolver), after the body exists, so no rewrite of the body reaches them |
| 5. errors and the plan | the rewrite runs **after typing, on typed nodes**, so no error can name the substitute; the plan is lowered from the rewritten body and carries nothing else | it is the same typed program with one callee changed per site |
| 6. tests | the batteries pinned under `engine` per enabled database; each setting (`refuse`, `warn`, `engine`) tested; one plant per rule | section 6 |
| 7. size | **about 2.5 days, as two parts**: B1 builds the core native (about 1 day) while B1-R builds the edge rewrite (about 1.5 days) | section 7 |

## 1. The operation

**What legend-engine computes, read in its pinned sources:**
- `pureToSQLQuery.pure:4658-4664` (`processSubstring`): without `CORRECT_SQL_SUBSTRING_INDEXING` it is
  `processDynaFunction('substring', ...)`, with Pure's arguments as given. Beside it: "Correcting substring
  unconditionally would change the results of existing queries, hence the flag".
- Each database extension writes that dynaFunction as the database's own call:

  | extension | `substring` | `indexOf` |
  |---|---|---|
  | H2: the dialect translation its SELECTs take (`relationalMappingExecution.pure:440-443`; D-S3) | `functionCall('substring')` (`toPostgresModel.pure:418`) | `functionCall('strpos')` (`:381`) |
  | `duckdbExtension.pure:277`, `:237` | `substring%s` | `instr(%s, %s)` |
  | `postgresExtension.pure:248`, `:220` | `substring%s` | `strpos(%s, %s)` |
  | `snowflakeExtension.pure:379`, `:331` | `substring%s` | `CHARINDEX(%s)`, the needle first |

So **legend-engine's answer is, by definition, what the target database's own SUBSTRING returns for Pure's arguments
as given**, quirks and all: H2's start-0-as-1, DuckDB's window for a start of 0 and its backward negative length, and
DuckDB's constant folder on a literal. That confirms the compat owner's reading.

**The alternatives, against the rules:**

| design | what computes legend-engine's answer | verdict |
|---|---|---|
| H5's Pure library (85 lines, H16) | lite's Pure, imitating each database's SUBSTRING in `min`, `max` and `if` | **no.** It re-implements each database's behavior, needs one library per database, and breaks if a database version changes its SUBSTRING. Tenet 4: "upstream is the spec of the answer, never the design". It also needs an extended model, built and held |
| the swap to piece 1's `sqlSubstring` (H16 addendum) | the database, but through lite's `SubstringClamp`, which raises a start below 1 to 1 on nine dialects | **no.** It is exact on H2 alone; on DuckDB it misses 16 of legend-engine's 47 answers, measured |
| **one new native, the database's SUBSTRING as given** | **the database itself, unclamped** | **yes.** It is exact by construction wherever legend-engine writes the plain call, its meaning is honest, and the database computes |

**So the raw SUBSTRING is the right thing to do**, in the user's words "only if it's the right thing": it is the
operation legend-engine performs. Each alternative either imitates that operation or alters it.

**The native:**
- `meta::legend::lite::substringAsGiven(s: String[1], start: Integer[1]): String[1]` and
  `(s, start, length)`. Lite-internal, signature-verified as piece 1's natives are.
- Its meaning: "the database's own SUBSTRING over these arguments, unchanged" (1-based, with a length).
- It lowers through one `Scalars` rule to `SqlFn.SUBSTRING_AS_GIVEN`.
- Each dialect renders it as its SUBSTRING, one label on the existing case: `case SUBSTRING, SUBSTRING_AS_GIVEN ->`.
- `SubstringClamp` matches `SUBSTRING` alone, so it passes through.
- A dialect where a type pass must still apply (Postgres's `StringCountsAsInt`) adds it there when that database is
  enabled. Postgres is not in this batch.

**`indexOf` needs no new native.** Every extension writes the database's own find function, and lite's
`sqlPosition` (piece 1, `SqlFn.STRPOS`) is that function with the same meaning: 1-based, 0 on a miss. The batteries
agree on every `indexOf` request on both databases (H16).

## 2. Per database

`engine` is enabled per database by a fact in the edge's registry, `PureText`, the owner of the edge's database
names (the ruling of ~20:10). The literal-0 table already lives there, so the two facts become one line:
`ENGINE_FACTS = Map.of(DatabaseType.H2, ..., DatabaseType.DuckDB, ...)`, a record of the literal-0 arities and
whether `engine` is enabled. So `DialectBoundaryTest`'s pin for `PureText` stays at 2.

| database | the extension read | the batteries | `engine` |
|---|---|---|---|
| H2 | yes (above) | the swap today: all 46 of legend-engine's answers. The new native writes the same SQL on H2, which has no clamp | **enabled**; pinned at build |
| DuckDB | yes (above) | predicted 47 of 47: the 16 swap misses are each the clamp's (H16 addendum), and the new native removes it | **enabled once measured at build**; if any miss remains, refused by name until it is understood |
| every other database | not this batch | not measured | refused under `engine` by name ("no measured engine answer for <type>") |

## 3. Helpers: the rewrite after the inliner

**The question:** a store-bound call inside a user function that is also called host-side. Clones (H16) would rebuild
and hold a model per clone set. The alternative is to rewrite the query after the inliner.

**Measured in this session** (a throwaway test, through the public API only: `UserCallInliner`'s public constructor
and `inlineBody`, `TypedQuery.withBody`, `TypedQuery.plan`). On `PureMeaningCheckTest`'s model:

| query | user calls before, after inlining | the `substring` call in the inlined body | the inlined body planned |
|---|---|---|---|
| a user function in a column | 1, 0 | present | the same SQL, byte for byte |
| a qualified property read | 1, 0 | present | the same SQL |
| a let-bound lambda | 0, 0 | present | the same SQL |
| a host-side let | 0, 0 | present | the same SQL |

**So the design is:**
1. Under `engine`, after the detector has typed and checked the query, the edge inlines its body.
2. It replaces each flagged native call with a call of the new native (or `sqlPosition`). Same arguments, same type,
   same position. (Superseded by revision 2's D-M1: the calls are matched by identity, from one walk over each
   store-bound root inlined on its own, never by position.)
3. It plans `query.withBody(rewritten)`.

**What this gives:**
- A host-side caller of the same function keeps Pure's meaning, because each use is inlined separately.
- No clone, no extended model, no rebuild.
- The planner's own G½ finds nothing left to inline. The measurement shows the inlined body plans to the same SQL.

**Not a core hook:** `UserCallInliner` is public and used by the product's planner (`BodyPlanner`), so
`EdgeOnlyMembersTest` does not count it, and `ArchitectureTest`'s planner-entries rule covers `planner`, `testplan`
and `lowering`, not `compiler.spec`. **Checked in the first hour of the build, by the fast loop:** that no guard
refuses the edge's `main` code reaching `UserCallInliner` or building a `TypedNativeCall`. If one does, the build
stops and says so.

**Where inlining stops** (recursion, a body it cannot inline): a flagged site left inside a `TypedUserCall` after
inlining is refused under `engine` by name. The detector counted it, so it is never answered silently.

## 4. The reach

| reached | under `engine` | why |
|---|---|---|
| the query's store-bound statements, let-bound lambdas | rewritten | in the body |
| user functions, qualified properties | rewritten, after inlining | measured above |
| a constraint `validate` turns into a filter | rewritten | inline in the body |
| an M2M chain's transforms under a TDS query | **refused by name** | they are lifted class-mapping functions the store resolver (phase H) inlines after the body exists, so a body rewrite cannot reach them. Rewriting the mapping would mean a second model. Lite's M2M chain support is partial today (its own stress suite: "the test runtime losing the ModelChainConnection's mapping") |
| a `~func` relation-function mapping | **refused by name** | the same: reached in phase H |
| an unverified site (a function value, an untypeable body) | **refused** | not followed, so not rewritable |
| a host-side let | not rewritten | legend-engine computes it with Pure's meaning (H16's probe) |

## 5. Errors and the plan

- **Errors:** the rewrite runs after typing, on typed nodes, and changes a callee whose signature types the same
  arguments to the same type. So no type error can arise from it, and none can name the substitute (H5's (c) is
  gone). A database error words SQL, as any query's does.
- **The plan:** it is lowered from the rewritten body. It holds the SQL the database runs, and nothing about the
  rewrite: no switch, no flag, no second program. The header says what happened, `x-legend-lite-pure-meaning:
  engine substring/3=1` (the codes and counts, the detector's form).

## 6. Tests

**`PureMeaningRewriteTest`, in the edge:**
- `engine` answers a flagged query with the new native's SQL. On DuckDB the SQL holds no `GREATEST(..., 1)` clamp.
- `refuse` and `warn` stay as today.
- A setting other than the three stops the server's start.

**A plant per rule:**
- a host-side let is not rewritten;
- the corrected option keeps `substring` Pure's;
- a user function called both host-side and store-side is rewritten at the store-side use only;
- a qualified property is rewritten;
- an M2M chain, a `~func` mapping and an unverified site are refused under `engine`;
- an unmeasured database is refused under `engine`;
- a site left inside a call after inlining is refused.

**The batteries under `engine`:** a third per-push half (`battery_engine_<db>`), pinned at
`pct/batteries/agreement_engine_{h2,duckdb}.tsv`. The expected rows: H2, all 46 legend-engine answers; DuckDB, 47.
The 3-argument `indexOf` stays legend-engine's refusal against lite's answer, as E-R2 rules.

**The core native's own tests (B1):**
- its SQL on each dialect: the plain call, no clamp;
- its answer on H2 and DuckDB for the battery's `substring` rows, against the database's own;
- the native-catalog census rows.

## 7. Size, the split and the guard moves

| part | who | what | days |
|---|---|---|---|
| the core native | B1, in parallel, from the reviewer's approval | the two signatures (`Pure.java`, verified as piece 1's are); one `Scalars` rule; `SqlFn.SUBSTRING_AS_GIVEN`; one label in each dialect's SUBSTRING case (about 13 files; no engine-style printer, A2 deletes them: D-S4); `SubstringAsGivenCall` (D-S2); the native's tests | about 1 |
| the edge rewrite | B1-R | `PureMeaningRewrite` (the inline of each root, the replacement by identity, `withBody`, about 150 lines); `engine` in the setting's switch; `ENGINE_FACTS` in `PureText`; the refusals; `PureMeaningRewriteTest`; the batteries under `engine` and their two pins | about 1.5 |

The two parts meet at the native's name alone, so B1-R builds against it from day one and rebases onto B1's commit.
Both land in the batch after the core switch (piece 2), with piece 1 under them.

**Guard moves to expect** (each named exactly by the fast loop at build, each to the user with a dated note):

| guard | the move | whose |
|---|---|---|
| `NativeCatalogGovernanceTest`'s `INTERNAL_DESUGAR` ceiling | +1 (the new native's name; piece 1 moves it 38 to 40, so 41 after both) | B1 |
| `native-claims.tsv` | regenerated | B1, and B1-R for the rewrite |
| `SqlFnSortedTest` | none expected: the new value goes in its sorted place | B1 |
| `EdgeOnlyDataTest`, the edge-only `SqlFn` census | **none**: it cannot hold a function `Scalars` names (the reviewer's addendum ca8ea4a5c). The producers are counted in `native-claims.tsv` (`Scalars` and `EngineSubstitutes`), `INTERNAL_DESUGAR` +1 and `SqlFnSortedTest` PRODUCT +1 | B1 |
| `JavaEvalLedgerTest`'s funnel register | +1 row (`PureMeaningRewrite.java`) | B1-R |
| `DialectBoundaryTest` | none: `PureText`'s line becomes the `ENGINE_FACTS` line, so the pin stays at 2 | B1-R |
| new pins | `agreement_engine_{h2,duckdb}.tsv` (new files) | B1-R |

## 8. The rules of record

| rule | holds | how |
|---|---|---|
| 1: the database computes everything | yes | legend-engine's answer is computed by the database's own SUBSTRING; nothing is imitated |
| 2 and 3: presentation | yes | untouched |
| tenet 4: upstream is the spec of the answer, never the design | yes | the design is the one compiler with one more native, not a copy of legend-engine's code |
| tenet 6: no modes in the core | yes | the native is a function with its own meaning, not a mode; the switch is read once, at the door (the switch rule) |
| no core hook | yes | the native is ordinary product vocabulary; the rewrite uses public API only, checked in the first hour |
| D1 and the final plan's §2 part 2 | amended | by the user's ~20:05 ruling, for requests a server opts in to with `engine` |

## 9. substr, the corrected substring and a literal-0 start (2026-11-20; the compat owner's go on the part common to A and B)

Asked of legend-engine 4.145.0 on H2 and DuckDB (docs/records/compat-homework/a3-engine-probes/substr/), not assumed.
legend-engine's processSubstr sends `substr`, and `substring` under `CORRECT_SQL_SUBSTRING_INDEXING`, as the database's
SUBSTRING over `start + 1` and `end - start`. It folds literal indexes and adds no range check. So in range it agrees with
legend-pure. Out of range it answers the database's cut-short value, per database (`'FX'->substr(1, 3)` is 'X' on both;
`substr(2, 1)` is '' on H2 and 'r' on DuckDB). Lite raises there, as legend-pure does (S383, the translator's 97d4c78b2).

| part | what |
|---|---|
| the detector | four codes, `substr/2`, `substr/3`, `substring(corrected)/2`, `substring(corrected)/3` (`Code.outOfRangeOnly`); the sentence says they differ out of range alone |
| the walk under `engine` (the reviewer's M-a1) | `PureMeaningCheck.forEngine`: the measured literal-0 exemption is off, so `$r.x->substring(0, n)` is rewritten too ('ab'->substring(0, 3) answers 'ab', legend-engine's; lite raises); `refuse` and `warn` keep the exemption |
| the substitute | `EngineSubstitutes.substr`: `substringAsGiven` over legend-engine's folded literal indexes. Each folded index is an Integer literal typed as the typer typed the literal it stands for, through a `substitute()` overload that takes the built arguments and `checked()` |
| a computed index | refused by name under `engine`. Its `start + 1` would be a typed call the door would have to assemble (D-S2). The user's word is pending: (1) a native `substrAsGiven`, or (3') answered as lite answers it, with the header |
| the REFUSE policy | pending the user's A or B |

**Measured at build** (the batteries' ten new rows on a value shorter than its end, `col.`/`lit.` `s19`, `r01`, `r02`,
`c01`, `c02`; legend-engine's oracle re-run in the same pass, +10 rows per database, no row moved):

| half | H2 | DuckDB |
|---|---|---|
| `engine` | AGREE on all ten: {AGREE=56, BOTH_FAIL=1, DIFFER=2} (was 46) | AGREE on all ten: {AGREE=57, DIFFER=2} (was 47) |
| `warn` | DIFFER on all ten (lite raises): {AGREE=11, BOTH_FAIL=1, DIFFER=47} | DIFFER on all ten: {AGREE=8, DIFFER=51} |

| plant | the tests it turns red |
|---|---|
| s1 the detector skips `substr` | 6: the detector's two, the rewrite's, and the engine test's three `substr` tests |
| s2 the corrected `substring` quiet again | 3: one each in the detector, rewrite and engine tests |
| s3 `engine` keeps the literal-0 exemption | 2: M-a1's rewrite and engine tests |
| s4 the substitute sends Pure's indexes unfolded | 3: the engine test's past-the-end pair and the folded plan |

The guard moves: `native-claims.tsv` regenerated, `substr`'s two rows naming `EngineSubstitutes` and `PureMeaningCheck`
beside `Scalars` (ClaimRegistryTest's pin reads `substringAsGiven`'s rows alone, which do not move); the four battery pins
and legend-engine's two answer files as above; no ceiling, ledger or allowlist.
