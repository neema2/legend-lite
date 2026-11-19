# A3, B1's part: the core native `substringAsGiven` (2026-11-19)

The database's own SUBSTRING over Pure's arguments as given: the operation of the `engine` rewrite's design
(`docs/records/compat-homework/A3-engine-rewrite-design.md` on `claude/compat-a3-detector`, approved with D-M1 and
D-S1..S4 in `docs/records/compat-pr-reviews-2026-11-19.md` on `claude/compat-review`, D-S1 corrected at `ca8ea4a5`).
B1-R builds the edge rewrite, which calls this native.

**The code:** branch `claude/compat-a3-raw-substring`, head `897db4bbe6`. Its base is A2 (`claude/compat-a2-printers`, `6551c20362`)
plus piece 1's patch (the translator's record branch, `claude/compat-translator-spike`, `48801791`,
`docs/records/compat-homework/a3/piece1/0001-a3-piece1-frontends.patch`). It rebases onto `claude/compat-a3-switch`
once that is pushed.

## What it is

| piece | where | what |
|---|---|---|
| the native | `Pure.java` | `meta::legend::lite::substringAsGiven(s, start)` and `(s, start, length)`. Lite-internal: in `INTERNAL_DESUGAR`, not on `LITE_SURFACE`. In `COMPILER_OWN` (the owner's yes, ~21:12), so a user's call by its full name, by an overload's id or under `eval` is refused by name. Without it, the full name compiled, as every lite-internal native's does. Its javadoc and `sqlSubstring`'s each say which is which |
| the MIR function | `SqlFn.SUBSTRING_AS_GIVEN` | the plain call; `SubstringClamp` matches `SUBSTRING` alone |
| the rule | `Scalars` | one rule: the native's two ids to `SqlFn.SUBSTRING_AS_GIVEN` |
| the dialects | 10 arms, MySQL's list, MongoDB's refusal | `case SUBSTRING, SUBSTRING_AS_GIVEN ->` in BigQuery, ClickHouse, Databricks, DuckDb, H2, MariaDb, Postgres, SQLite, Snowflake, Trino; MySQL's pass-through list. MongoDB refuses it by name (the owner's ruling, ~21:12; below). None on the engine-style printers, which A2 deletes. `SqlTyping` types it VARCHAR as SUBSTRING |
| D-S2's check | `edge/EngineSubstitutes` | package-private (the owner's yes). `substring(ctx, call)` builds substringAsGiven; `indexOf(ctx, call)` builds piece 1's sqlPosition for the 2-argument String indexOf (B1-R asked for one construction path). One `checked(ctx, call, nativeIds)` refuses a callee that is not the registry's function for one of the native's FunctionIds, as the typer's `InferenceKernel.resolveOverload` picks it, or a type that is not its output. Each entry refuses a call that is not the Pure String overload it replaces. It sits in the edge because a core member only the edge calls would be an `EdgeOnlyMembersTest` line. B1-R's rewrite calls it |

## Measured (`EngineSubstitutesTest`, in `//core:core_edge_tests`)

| claim | result |
|---|---|
| each substitute is the registry's function for its native's id, for each arity; its arguments, type and position are the original's | pass (substring, both arities; indexOf, 2 arguments) |
| D-S2's plants: the other arity's overload, piece 1's `sqlSubstring`, a hand-made signature with the right id and the wrong return type, Pure's own `substring`, and the right callee with a hand-made `ExprType` | each refused |
| a call no substitute stands for: the 3-argument indexOf, and each entry given the other's call | each refused |
| the 10 relational dialects with an arm, and MySQL, write no clamp for it. Where nothing rewrites `SUBSTRING` (a start of 2), the two write the same text, except on Postgres, where `StringCountsAsInt` casts SUBSTRING's counts alone | pass |
| the partner: `SUBSTRING` with the same arguments is clamped on 10 dialects (SubstringClamp's nine and MySQL), and a literal start of 0 is raised to 1 there; H2 is not clamped | pass |
| MongoDB refuses it by name | pass |
| H2 and DuckDB: each of the batteries' 18 substring rows, over the column and over the literal (`col.s01`-`s18`, `lit.s01`-`s18`), answers as the database's own `substring(...)` run directly, as legend-engine writes it. The desks are the connection's declared test data, with no fixture write | 72 of 72 equal |
| a user cannot name it: by its full name, by an overload's signature id, or under eval it is refused as the compiler's own; its bare name is an unknown function | pass |

## MongoDB refuses it (the owner's ruling, ~21:12)

MongoDB has no SUBSTRING of its own: its SUBSTRING arm is lite's `$substrCP` translation, which clamps the start in the
pipeline (`{"$max": [0, {"$subtract": [start, 1]}]}`). A label named "as given" there would answer lite's reading
under a false name, a fallback (AGENTS.md invariants 3 and 4). So its arm throws `DialectCapability` for
`SUBSTRING_AS_GIVEN` by name. That is the dialect's own refusal type for a function it cannot express, as for
TRANSLATE and `CURRENT_USER_FN`; it is an `IllegalStateException`, not the `UnsupportedOperationException` the ruling
named. The test pins the refusal.

## Postgres, for when it is enabled

`StringCountsAsInt` casts SUBSTRING's start and length to INTEGER on Postgres and is untouched here, as the brief says.
So `SUBSTRING_AS_GIVEN` is written without the cast. Where a start or a length is a BIGINT (an Integer column, or a
parameter bound as BIGINT), Postgres has no `substr(text, bigint, ...)`, so that statement would fail. Postgres is not
enabled under `engine` in this batch. When it is, `StringCountsAsInt` should take the new function too.

## The guard moves, for the user's word (none made)

**Made**, the user's ruling ~21:10 "Yes to all three for substring" (relayed by the compat owner), each with a dated
note:

| guard | the move |
|---|---|
| `native-claims.tsv` | regenerated. substringAsGiven's two rows name `EngineSubstitutes;Scalars`. EngineSubstitutes also joins the `also` column of sqlPosition, indexOf/2 and substring/2 and /3, the calls it reads |
| `NativeCatalogGovernanceTest`'s `INTERNAL_DESUGAR` ceiling | `<= 38` to `<= 41`. Piece 1's 38 to 40 is carried in a line its own dated note replaces where piece 1 lands; this one is 40 to 41 |
| `SqlFnSortedTest` PRODUCT | + `SUBSTRING_AS_GIVEN`, with the note |
| the new pin | `ClaimRegistryTest.substringAsGivensOneProductClaimerIsScalars` (spec): the rows' `also` column is exactly Scalars and EngineSubstitutes, and a planted product claimer is named. Its named limit: the column does not see a use through the `Pure.Lite` name constant alone |

**Not made: outside the ruling, for the user's word.** The design did not foresee these:

| guard | the move | why |
|---|---|---|
| `spec/.../ratchets.tsv` `implementation.kinds.Intrinsic` | 738 to 740 (piece 1: 735 to 738) | the native's two overloads |
| `CatalogUpstreamDiffTest`'s `NOT_UPSTREAM_MAX` | 76 to 78 (piece 1: 73 to 76) | the two overloads upstream does not declare |
| `JavaEvalLedgerTest`'s edge funnel register | + `EngineSubstitutes.java`, with its tenet line: "the engine rewrite's substitutes, typed by the typer's own overload resolution over the registry; builds typed nodes, plans and runs nothing" | a new class in the edge package |

**Piece 1's own, not B1's:** `FUNCTION_FQN_LITERALS_MAX` 346 to 347, the 9 text goldens, and its share of the three
counts above.

## The checks

| target | result at `897db4bbe6` |
|---|---|
| `//core:core_edge_tests`, `//core:core_tests_builtin`, `//core:core_tests_sql`, `//core:guardrails_edge` | green |
| `//core:census`, `//core:guardrails`, `//spec:spec_tests` | red on exactly the four above: the three unmade moves, and piece 1's FQN-literal count |
| `//core:core_tests_{root,planner,lowering,compiler}` | green at `f3553c634b` |
