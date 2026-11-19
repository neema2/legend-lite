# A3: the `engine` rewrite, the design note (2026-11-19)

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
  | `h2Extension2_1_214.pure:262`, `:231` | `substring%s` (the arguments joined) | `LOCATE(%s)`, the needle first |
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
2. It replaces each flagged native call, matched by position among the detector's sites, with a call of the new
   native (or `sqlPosition`). Same arguments, same type, same position.
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
| the core native | B1, in parallel, from the reviewer's approval | the two signatures (`Pure.java`, verified as piece 1's are); one `Scalars` rule; `SqlFn.SUBSTRING_AS_GIVEN`; one label in each dialect's SUBSTRING case (about 13 files) and the edge's three engine-style printers; the native's tests | about 1 |
| the edge rewrite | B1-R | `PureMeaningRewrite` (the inline, the replacement by position, `withBody`, about 150 lines); `engine` in the setting's switch; `ENGINE_FACTS` in `PureText`; the refusals; `PureMeaningRewriteTest`; the batteries under `engine` and their two pins | about 1.5 |

The two parts meet at the native's name alone, so B1-R builds against it from day one and rebases onto B1's commit.
Both land in the batch after the core switch (piece 2), with piece 1 under them.

**Guard moves to expect** (each named exactly by the fast loop at build, each to the user with a dated note):

| guard | the move | whose |
|---|---|---|
| `NativeCatalogGovernanceTest`'s `INTERNAL_DESUGAR` ceiling | +1 (the new native's name; piece 1 moves it 38 to 40, so 41 after both) | B1 |
| `native-claims.tsv` | regenerated | B1, and B1-R for the rewrite |
| `SqlFnSortedTest` | none expected: the new value goes in its sorted place | B1 |
| `EdgeOnlyDataTest`, the edge-only `SqlFn` census | **checked at build**. The native and its rule are product code, so its `SqlFn` is made by product code. If the census counts a function only edge-rewritten programs reach, that is a stop-and-say | B1 |
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
