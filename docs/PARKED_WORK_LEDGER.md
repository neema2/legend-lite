# The parked-work ledger

Work we decided NOT to do yet, recorded so it cannot be forgotten and cannot drift
silently. Every row is enforced by `core/src/test/java/com/legend/ParkedWorkLedgerTest.java`,
which runs in gate 1 of every chain: each row names an ANCHOR — a mechanical fact about
today's code that holds only while the item is still parked. Change the situation and the
anchor goes red, so whoever touches the area must close the row or restate it.

**The rules.**

1. A row leaves this ledger by being FIXED. Never by being loosened, never by being
   deleted because it is inconvenient.
2. Every row carries the date it was parked, WHO decided, WHY, the acceptance test that
   closes it, and the cost of leaving it parked.
3. The anchor must be mechanical. "We should remember to…" is not a row.
4. A green anchor is not approval. Each row is a debt with a stated price.

---

## PARK-1 — Cross-store associations: one shared predicate, not per-end

**Parked** 2026-09-15 by the user ("we can park xstore for now"), during the
mapping-normalizer audit burndown (FIXLIST P3-1).

**What we do today.** A cross-store (XStore) association's two ends must resolve to a
single shared predicate: both directions are canonicalized and compared, and a difference
is walled with *"has direction-specific conditions; a single shared predicate is required
for now"*.

**What the engine does.** The model is direction-SCOPED: `XStoreAssociationImplementation`
is an empty class and all semantics live per end in each `XStorePropertyMapping`'s
`crossExpression` (`mapping.pure:174-180`). None of the engine's eight XStore validations
compares end A to end B.

**Cost while parked.** Four engine fixtures sit in our own corpus manifest, parse and
round-trip, and cannot be normalized:

| fixture | shape |
|---|---|
| `testModelJoinsToRelationalJoins.pure:399-400` | four ordering comparisons, operands flipped per side |
| `relationMappingSetup.pure:638-639` | genuinely asymmetric bodies, not inverses |
| `testMappingCrossStore.pure:239-242` | four property mappings over two set pairs; we read set ids from the first only |
| `executionPlanTestSnowflake.pure:493-499` | a one-ended XStore, which the engine accepts |

The rule is also implemented TWICE verbatim (the column-space and property-space paths),
and neither copy has a test.

**Acceptance (what closes this row).** Per-end predicates carried to the resolver, set ids
read per property mapping rather than from the first, ONE implementation, and the four
fixtures above normalizing with rows matching the engine.

**Anchor.** The wall text `has direction-specific conditions` appears in exactly two
product files: `MappingNormalizer.java` and `XStorePureEnds.java`. Implementing per-end
predicates removes or moves it; unifying the duplicate changes the count.

---

## PARK-3 — `toString` emits pure's ISO form, not the database's cast

**Parked** 2026-09-15, during the audit burndown (FIXLIST P3-4), after building the fix and
letting the corpus judge it.

**What happens today.** The relational `toString` dynafunction resolves as PURE with no
translator arm, so the name passes through to pure's own `toString`. The engine renders the
dynafunction as the DATABASE's text: `cast(%s as varchar)` in both our lanes
(`duckdbExtension.pure:284`, `h2Extension2_1_214.pure:266`). For a date that is the
difference between the database's format and pure's ISO spelling. The codebase already knew:
the `concat` arm casts for exactly this reason.

**Why it is parked rather than fixed.** The obvious arm — emit `cast(v, @String)`, the same
`strCast` the concat arm uses — was written and run. It COLLAPSES MULTIPLICITY: a
multi-valued argument comes back as one value.

| lane | verdict with the arm |
|---|---|
| DuckDB | LOST 1: `testGraphFetchMultiPrimitiveOnInlineChild` — `$.authors[0].authorId` expected `[5001]`, got `5001` |
| H2 | LOST 1, same test |

Bisected: with the arm removed and the rest of the leg kept, both lanes are EXACT again. A
correct arm has to cast WITHOUT flattening (map the cast over the collection, or decide the
cast by the argument's multiplicity, which the dyna lane does not carry here).

**Cost while parked.** A `toString` written in a mapping expression over a date yields
pure's ISO text where the engine yields the database's. No corpus row currently exercises
it, which is why this is a latent divergence rather than a failing row.

**Acceptance (what closes this row).** A multiplicity-preserving cast arm, with
`testGraphFetchMultiPrimitiveOnInlineChild` still EXACT and a witness for the date shape.

**Anchor.** `DynaFn.TO_STRING` appears in exactly one product file, `DynaFnDecisions.java`, as
its PURE decision (restated 2026-10-06, when the platform's decisions left the generated
registry); nothing dispatches on it. Adding the arm references it in another file and turns
this row red.

---

## PARK-4 — The `~groupBy` wrapper projects columns nothing reads

**Parked** 2026-09-15 (FIXLIST P4-2), after building the audit's stated fix and letting the
corpus refute its root cause.

**What happens today.** A `~groupBy` class mapping emits two SELECTs where the engine's
golden for the identical shape (`testGroupBy.pure:74-79`) is one flat `SELECT … GROUP BY`,
and the inner one projects columns the outer never reads. Rows are correct; the cost is text
and parity, and the audit measured ZERO runtime cost on DuckDB.

**The audit's stated root cause is WRONG.** It named `SubselectPrune`'s refusal to prune
grouped selects, calling the refusal "correct for DISTINCT, unnecessary for GROUP BY".
Semantically that reasoning holds — what a grouped select returns per group is the GROUP BY
clause's business, not the projection list's — but the ENGINE KEEPS THOSE PROJECTIONS, so
pruning them diverges from the golden text:

```
left outer join (select "root".ENTITY_ID as ENTITY_ID, "root".name as name,
                 "root".value as value
                 from Entity.LegalEntity as "root" group by "root".ENTITY_ID)
```

`ENTITY_ID` is the group key and the outer query never reads it; the engine projects it
anyway. Lifting the refusal dropped it and LOST
`testJoinWithInequalities` on both lanes (sql-text verdict). Reverted.

**What the real fix is.** Collapsing the wrapper — a conservative select-merge pass that
folds a single-source subselect into its parent — which is the same missing machinery as
PARK-2 and FIXLIST P4-3/P4-4. `SubselectPrune` prunes columns and never collapses a wrapper.

**Acceptance (what closes this row).** The `~groupBy` shape emits one flat select matching
the engine's golden, with `testJoinWithInequalities` and the group-by family still EXACT.

**Anchor.** `SubselectPrune`'s prune guard still lists `groupBy` beside `distinct` — the
exact clause `projections().isEmpty() || sel.distinct() || !sel.groupBy()` sits in
`SubselectPrune.java` and nowhere else. A select-merge pass, or anyone lifting the refusal
again, moves it.

---

## PARK-2 — A union read twice is built twice (no common-subexpression pass)

**Parked** 2026-09-15 during the same burndown (FIXLIST P4-1). *Anchor note 2026-09-18
(judging leg 3.1): `VerdictSql` now also constructs a `SqlWith` — the database-mode verdict
statement (each assert side as a CTE, one verdict row) — which is not a common-subexpression
pass and does not touch the union lowering; the construction anchor names both files, and
this item stays parked. Anchor note 2026-09-20 (leg 3.4 step 2): `SqlWith.prepend` hoists a
statement's frame CTEs to its head — a construction helper the anchor now also names; still
not a common-subexpression pass.*

**What happens today.** A union-mapped class is one whose rows come from several tables
stacked together. When one query both filters on a related collection and aggregates over
it, we emit the stacked union TWICE — the filter builds its own deduplicated copy instead
of reusing the grouped relation that is already there — so the database scans the
underlying tables twice for one question. (Two aggregates over one union already share a
single copy; the duplication is specific to the filter.)

**Measured** by the audit on 2,000 firms against 200,000 people (NOT re-run since):

| | engine-shaped | ours |
|---|---|---|
| table scans | 2 | 4 |
| hash joins | 1 | 2 |
| best of 7 runs | 1.12 ms | 2.26 ms |

Rows are CORRECT. This is a leanness and speed defect, not a correctness one. The same
emission also carries a dead always-true condition and a left join plus a not-null test
that together are an inner join written the long way.

**Why it survives.** Nothing in the normal lowering path looks for a repeated subtree.
`SubselectPrune` is the only post-lowering pass and it prunes columns without ever
collapsing a wrapper; the CTE builder exists but only an opt-in parity post-processor
(`extractSubqueriesAsCtes`, behind the `extractCtes` flag) ever calls it.

**Acceptance (what closes this row).** A pass in the normal path that names a repeated
subtree once and points both readers at the name, with the measurement above closing.

**Anchor.** `extractSubqueriesAsCtes` is called from exactly one product file
(`SqlPostProcessors.java`, the opt-in path) and `new SqlWith(` is constructed in exactly
one (`SqlRewriter.java`, its structural copy-on-write). A real common-subexpression pass
moves at least one of those. *Anchor note 2026-10-11 (BigQuery's burn-down): a `SqlWith`
carries a `recursive` flag (a fold hoisted as a top-level recursive CTE,
`FoldsAsRecursiveCtes`), and every rebuild of one goes through `SqlWith.with`, which keeps
it; so `SqlRewriter`'s copy-on-write constructs through `SqlWith.java`, and the anchor names
`SqlWith.java` and `VerdictSql.java`. The recursive CTE is not a common-subexpression pass;
this item stays parked.*

---

# The build rebuild's debts (parked 2026-10-07)

**Parked** 2026-10-07 by the user: "we need a ledger of hacks that we need to come back and fix ... a detailed list
of all of these to fix correctly after we land the bump and ... stick to actually fixing them correctly instead of
hacks on hacks". Each row below is fixed after the build rebuild lands (`docs/REBUILD_PROGRAM_2026_10_06.md`, plan
branch), correctly, with its own design agreed first, unless the row names the phase of the program that closes it;
none is worked around in the meantime. Sources: the Phase 3 audit (2026-10-07) and the Phase 3b census. A row is
closed only by deleting it here and its anchors in `ParkedWorkLedgerTest`, in the commit that does the fix; where a
row allows keeping a behavior, keeping it means a `docs/SEMANTICS_REGISTER.md` row replaces this one.

---

## PARK-6 — Arguments typed more than once

**What we do today.** The typer often looks at an argument's type to choose a route, then rewrites the call and types
the rewritten call from scratch, typing that argument again: a dot call's receiver (`Typer`'s qualified-property
branch, then the route it takes), the generic path's auto-map probe (`CallShapes.autoMapReceiver`, before the
arguments are typed), the derived-property shadow, the `map` rewrite, the legacy-TDS desugars, the receiver-owned
function check, and functions that must be inlined (an argument typed once per use in the inlined body).

**What legend-pure does.** It types each argument once and matches the function on the typed arguments.

**Why parked.** The correct fix is legend-pure's typing order, a rework of the typer's call path; today it changes
no result.

**Cost while parked.** Time only: the typer records nothing per typing (checked 2026-10-07), so a second typing
changes no result. In a chain of such calls the work doubles at each level.

**Acceptance.** Arguments typed first, once; the route chosen on the typed arguments; the checkers take typed
arguments. Same results on every lane; typing time measured against main.

**Anchors** (one per place; the `map` rewrites sit inside the first two): a receiver typed only to choose a route
(`recv = synth(af.parameters().get(0), env)`, and `grecv` in the TDS receiver checks) in exactly `Typer.java`,
`CallShapes.java` and `TdsDesugars.java`; the property's body call re-applied after typing
(`applyGeneric(Calls.exact(d.bodyFunctionFqn(), qargs), env)`; the spelling since L7 step 3, 2026-10-10, when every built call moved into `Calls`) in exactly `Overloads.java` and `Typer.java`;
the receiver-owned check's receiver typing (`Type rt = t.synth(recv, env)`) in exactly `ReceiverOwnedFunctions.java`;
the must-inline substitution of untyped arguments (`subst.put(chosen.parameters().get(i).name(),
af.parameters().get(i))`) in exactly `Overloads.java`.

**Research.** `docs/build-inventory/program/DEBTS_RESOLVE_AND_TYPE_ONCE.md` (plan branch): the seven places, the facts
that bound the fix, legend-pure's order.

---

## PARK-7 — `Any` ranked with the type parameters (our typing order is not legend-pure's)

**What we do today.** The overload ranking puts an `Any` parameter, and a value typed `Any`, with the type parameters,
and applies legend-pure's literal order (`Any` a concrete class) only to break a tie that is left, in
`resolveOverload` only (`anyConcrete`).

**Why.** legend-pure matches the calls inside a lambda before their arguments are typed; this compiler types them
first. On the finished types legend-pure's literal rule picks `collection::in` where legend-pure itself picks
`relation::in` (12 reference-lane calls). The adjustment imitates legend-pure's order instead of having it.

**Why parked.** It goes with PARK-6's typing-order rework; until then the adjustment keeps the reference lane's calls
right.

**Cost while parked.** A wrong-version pick where the two orders differ and the adjustment does not cover it. Not
checked against legend-pure's types (Phase 3's recorded soft spot).

**Acceptance.** legend-pure's typing order (with PARK-6's rework); `Any` ranked as the concrete class m3 makes it, and
the flag gone; the reference lane's OVERLOAD count not up.

**Anchor.** `anyConcrete` appears in exactly one product file, `InferenceKernel.java`.

---

## PARK-8 — Tie-breaks legend-pure does not have

**What we do today.** After the ranking, `resolveOverload` keeps four older tie-breaks: a duplicate signature (the
first wins), a built-in over a program's function (`nativeWinners`), the most specific signature, and `Nil`
narrowing. The lenient pass (lambda arguments not typed yet) leaves a tie to declaration order. legend-pure reports a
tie as "too many matches".

**Why parked.** Each tie-break needs legend-pure run on the calls that reach it; no lane result depends on them today.

**Cost while parked.** A call legend-pure refuses as ambiguous may compile here, picked by an order a user cannot see.

**Acceptance.** Each tie-break checked against legend-pure on the calls that reach it (legend-pure run on them, as the
Phase 3 probe did); those legend-pure does not have removed, the rest recorded in `SEMANTICS_REGISTER.md`. Either way
this row and its anchor are deleted (the section's closing rule).

**Anchor.** `nativeWinners` appears in exactly one product file, `InferenceKernel.java`.

---

## PARK-9 — The acceptance test admits what legend-pure rejects (the platform-rule rank)

**What we do today.** Some argument and parameter pairs pass this compiler's acceptance test that legend-pure
rejects: a relation into `TabularDataSet`; a bare function type against a `Function<…>` parameter, or a function value
against a bare function-type parameter; a property value where a `FunctionDefinition` is expected; a lambda whose
parameter is narrower than the declared one. The ranking gives each the one fixed rank `PLATFORM_RULE_DISTANCE`
(after every real parent class, before a type parameter).

**Why parked.** Matching legend-pure's acceptance changes which programs compile (the TDS erasure among them): a
design of its own.

**Cost while parked.** Programs legend-pure rejects compile here, and their ranking is ours, not legend-pure's. One
known consequence (the fixes' audit, 2026-10-07): for a value typed as a bare function type, which legend-pure matches
to no carrier parameter, a carrier parameter ranks at the platform-rule distance, so it beats a type parameter and a
bare function-type parameter whose type is not identical, where legend-pure would pick one of those.

**Acceptance.** The acceptance test matches legend-pure's (its `GenericTypeMatch` with legend-pure's parameter
behaviors), the TDS erasure decided on its own; `PLATFORM_RULE_DISTANCE` deleted.

**Anchor.** `PLATFORM_RULE_DISTANCE` appears in exactly one product file, `InferenceKernel.java`.

---

## PARK-10 — Parts of legend-pure's ranking not ported

**What we do today.** Three parts of legend-pure's match are simplified (`FunctionMatch`'s javadoc lists them): a
relation-type parameter ranks without comparing columns (legend-pure's `RelationTypeMatch` compares column types and
multiplicities); a type-operation parameter (`T+V`) ranks as untyped (legend-pure: non-concrete); type arguments are
compared by position when their counts agree (legend-pure first maps them through the class hierarchy).

**Why parked.** No lane result depends on these parts; each needs a port with tests against legend-pure.

**Cost while parked.** A different pick where two candidates differ only there; none seen on the lanes.

**Acceptance.** All three ported, each with a kernel test against legend-pure's answer.

**Anchors** (one per part, all in exactly one product file, `InferenceKernel.java`): `Type.SchemaAlgebra ignored ->
FunctionMatch.TypeFit.NULL` (type operations); `Type.RelationType ignored ->
FunctionMatch.TypeFit.of(FunctionMatch.Kind.RELATION)` (relation columns); `if (actualArgs.size() ==
fg.arguments().size())` (type arguments by position).

---

## PARK-11 — Legacy TDS functions and `agg` recognized by name, not by implementation rows (closes in Phase 4)

**What we do today.** Phase 3's plan gave `TdsLegacy`'s functions (18) rows by function id and had `groupBy`'s `agg`
recognized by resolved id. Built instead: recognition by the names a call resolves to, falling back to the spelling
when nothing resolves (`TdsLegacy.matches`, `GroupByChecker.isAgg`); the spelling rule is older (2026-09-11).

**Why it waits for Phase 4.** A row is keyed by a function id, and an id needs a declaration. The platform's own world
declares none of these functions (neither the catalog nor the prelude): in a user program `restrict(...)` resolves to
nothing, so only its spelling is left. Adding them to the catalog would grow what Phase 5 deletes. Phase 4's default
world, generated from upstream, declares the 14 that are upstream query handlers. Of the other 4 (checked
2026-10-07): `columnByName` is a qualified property of `TabularDataSet` (upstream `tds.pure:21`), not a function, so it
comes with the class; `columnValues`, `renameColumn` and `window` are upstream functions no handler registers and only
upstream's own tests call, which legend-engine does not let a user query call; whether they stay out of the default
world is the user's decision (the plan's Phase 4).

**Cost while parked.** These functions dispatch by name.

**Acceptance.** The default world declares the 14 query handlers, each id with an implementation-table row (the
platform's desugar); `columnByName` read as the class member; the other three as the user decides; recognition reads
the row; the spelling fallback and `isAgg`'s name test deleted (the plan's Phase 4).

**Anchor.** The spelling fallback `candidateFqns().isEmpty() ? name.equals(bare())` appears in exactly one product
file, `TdsLegacy.java`.

---

## PARK-15 — The legacy plan picks an enumeration mapping without the place it is used

**Parked** 2026-10-08 by the user's step 2 decisions (docs/EXECUTION_PLAN_BOUNDARY_2026_10_05.md §9), found while
reading the legacy plan's parameter code for the one parameter list; widened by that landing's audit.

**What happens today.** One mapping may map an enum twice — two columns storing it two ways. The legacy,
legend-engine-shaped plan (`executionPlan(...)`'s text and node model) then picks an enumeration mapping without
knowing where it is used:
- an enum PARAMETER's translation is one template function per parameter: `PlanText.enumMapFnOf` asks
  `enumMappingOf(ctx, mappingFqn, enumFqn)`, the first enumeration mapping over that enum (the mapping's own, then its
  includes', an exact path before a simple name), and `PlanAllocations.planTemplateFunctions` emits that one
  `enumMap_` function the same way;
- a RESULT column's enumeration-mapping id (`PlanText.enumMappingIdFor`) is chosen by the physical column when the
  property mapping names it, and otherwise falls back to the first one declared.
legend-engine chooses at the place of use, from the property mapping there (`pureToSQLQuery.pure:8511`:
`'enumMap_' + fetchEnumFullPath($propertyMapping.currentPropertyMapping->at(0)->getEnumPropMappingTransformer() ...)`).
So for an enum mapped twice the legacy plan names the wrong translation at one of the two places, and a plan
executed from that text would compare or decode against the other column's codes.

**Who it touches.** Only the legacy plan (Pure code's `executionPlan`, the corpus and PCT plan-text checks). The lite
plan does not choose in Java at all: the database translates a parameter at each place it is compared, from a value
table written into that place's SQL (the user, 2026-10-07).

**Acceptance (what closes this row).** The legacy plan's enumeration mapping is chosen at each place from that place's
property mapping — parameter template functions and result-column ids alike — and a test with one enum mapped twice
in one mapping shows each place naming its own, as legend-engine's plan does.

**Anchor.** The three choices with no place of use: `enumMapFnOf`'s call `enumMappingOf(ctx, mappingFqn, enumFqn)` in
`PlanText.java`; `planTemplateFunctions`' call `PlanText.enumMappingOf(env.ctx(), pmr.fullPath(), et.fqn())` in
`PlanAllocations.java`; and `enumMappingIdFor`'s `candidates.get(0)` fallback in `PlanText.java`. Choosing per place
changes all three.

---

## PARK-16 — CLOSED 2026-10-16: the test-data generator's statements name tables as the session's dialect does

**Closed** by the dialect program's Phase 3 (`docs/DIALECT_LEAK_CENSUS_AND_PLAN_2026_10_12.md`). Every statement the
test-data generator runs is IR its session's dialect writes (the dialect threaded from `BodyCompiler` and
`SqlTextVerdicts` through `TestDataGenerationNatives`, the agreed fix), so a table or schema is named as that
session's queries name it. The acceptance, `ReservedNamesGenerationTest`: a generation over a default-schema table
`order` and over a table in a schema `select`, on DuckDB and H2. (The product half closed 2026-10-09, with E.)

---

## PARK-18 — the legacy printer cannot write a query's explicit null placement

**Parked** 2026-10-09 by the Plan Gen / Exec Split session with E-4b, PROPOSED to the user for confirmation (the
user's rule is "for backwards compatibility/legacy mode we need to be fully exact"; this is the one measured spelling E-4b
could not reach in the printer alone, because the IR does not carry it). Found by measuring legend-engine 4.145.0
(`docs/execution-plan-boundary-2026-10-05/legacy-text/`).

**What the engine does.** It writes `nulls first`/`nulls last` in a window's order and in an ordered aggregate exactly
where the query says `emptyFirst()`/`emptyLast()` (`engine-l2.sql`, `engine-l4.sql`, `engine-l8.sql`,
`engine-l13.sql`), and nothing for Pure's own null-is-largest order.

**What lite does today.** The lowering stamps every sort key with a null order (`Sorts.nullsOf`, `Fold.sortNulls`,
`Lowerer.lowerOver`): the query's explicit one when it has one, Pure's null-is-largest otherwise. The execution dialects
need that stamp; the legacy printer cannot tell the two apart in `SqlSelect.SortKey`, so it writes none
(`EngineStyleH2.sortKey`, `aggOrderNullPlacement`) — right for every golden, wrong for an explicit placement.

**The fix.** `SqlSelect.SortKey` carries whether its placement is the query's own (`TypedSortKey.nullOrder() != null`
at the three lowering sites); the legacy printer writes `nulls first`/`nulls last` for exactly those keys.

**Acceptance.** `LegacyTextTest` gains the l2 and l4 shapes, the engine's `nulls first`/`nulls last` text; the census
shows no other legacy statement changing.

**When.** With the compatibility mode (docs/EXECUTION_PLAN_BOUNDARY_2026_10_05.md §8, phase 4), or sooner if a golden
needs it. Cost of leaving it parked: a legacy text over an explicit `emptyFirst()`/`emptyLast()` omits the clause.

**Anchor.** `aggOrderNullPlacement` in `EngineStyleH2.java` returns `""`.

---

## PARK-19 — on H2, a parameter whose literal has no one type is not bound

**Parked** 2026-10-09 by the Plan Gen / Exec Split session with step 2's landing 2 slice (b), PROPOSED to the user for a
decision between the two exact ways out. Found by measuring the pinned drivers
(`docs/execution-plan-boundary-2026-10-05/probes/LiteralProbe.java` → `literal-results.txt`).

**What happens today.** A plan binds each parameter as a value where it is written; the database must give the
placeholder the type the literal of today's `let` path has. DuckDB and Postgres type a bare `?` by the bound value, and
every type answers as its literal. H2 types a parameter when it prepares the statement, by its neighbour (`ID * ?` with
1.5 answers `[2, 6]`, the literal `[1.5, 4.5]`) or not at all, so H2 writes each placeholder typed (`CAST(? AS T)`):
exact for Integer, String, Boolean, StrictDate and DateTime. A Float's or a Decimal's literal is typed by its own digits
(`2.50` is `NUMERIC(3,2)`), and no type a statement names keeps a value's own scale on H2: `NUMERIC` rounds to none,
`NUMERIC(38,2)` pads (`1.10`), `DECFLOAT` keeps the value but drops trailing zeros (`3.75` where the literal answers
`3.7500`). A Date's or a Number's value decides its kind. So `H2.placeholder` refuses such a parameter by name.

**Not a version's.** The H2 the PCT lane pins, 2.4.240 (2025-09-22), answers the same in every case (`literal-results.txt`,
its last section): H2 types a parameter when it prepares the statement, before it has a value, in 2.1 and 2.4 alike. And
the product's H2 is 2.1.214 because it is legend-engine 4.145.0's (the engine-parity target), not by default.

**The choice.** Not `DECFLOAT`: it keeps every value but not its spelling, and a value returned must be exactly what
Pure's Java returns (the user, 2026-10-09: "we need to return from the data exactly what pure returns from it's java";
Pure's `BigDecimal` keeps the scale, 1.50 × 2.50 = 3.7500). The exact ways: (B) keep refusing on H2 (the recommendation,
until an H2 user needs it), or (C) on H2 alone, write a decimal value into its statement as its literal — legend-engine's
own way (FreeMarker), exact by construction, but against "nothing edits SQL text at run time" (§9), so it needs a clean
form of its own, not a splice.

**Acceptance.** (B): the row stays, its anchor holding; (C): `PlanMakerTest`'s Float, Decimal, Date and Number cases run
on H2, every answer the literal's, text for text.

**When.** Before step 4 switches the server's `execute` to plans: until then today's path serves H2 decimal parameters.
Cost of leaving it parked past step 4: a query with a Float, Decimal, Date or Number parameter is refused on H2.

**Anchor.** `H2.placeholder`'s refusal, "has no one type a statement names", in `H2.java` (gone with the fix).

**Fixed 2026-10-09 (step 3, `docs/EXECUTION_PLAN_BOUNDARY_2026_10_05.md` §9), neither (B) nor (C).** The user: "we own
this whole pipeline" — the plan says how each placeholder is bound, and where the type is the value's, H2's plan casts
the placeholder to a TYPE HOLE (`CAST(? AS )`) that the runner fills, when the value is known, with the type H2 gives
that value's literal: `NUMERIC(precision,scale)` of its own digits, `DECFLOAT(precision)` at an extreme magnitude,
`BIGINT`, `DATE`, `TIMESTAMP(9)`. The value is still bound, never written into the text. Measured, every value against
its literal, alone, in arithmetic, compared and in the answer's JSON (`probes/ValueTypedCastProbe.java` →
`value-typed-cast-results.txt`): the same type and text, a small whole number's type apart (BIGINT for the literal's
INTEGER: the same text). `PlanMakerTest` runs every scalar case on H2.

---

## PARK-20 — a list of decimals, Dates or Numbers is not bound as a parameter

**Parked** 2026-10-09 by the Plan Gen / Exec Split session with step 2's landing 2 slice (e). Found by measuring the
pinned drivers (`docs/execution-plan-boundary-2026-10-05/probes/ListProbe.java` → `list-results.txt`).

**What happens today.** A list parameter is bound as ONE array made by the driver (`createArrayOf`) under its element
type's name, and `col = ANY(?)` answers as the literal `col IN (...)` of today's `let` path on DuckDB, H2 and Postgres for
integers, strings, booleans, dates and timestamps, the empty list included. An array of decimals does not on DuckDB: its
driver makes a `DECIMAL` array of the default scale, three places, so `0.1234` is read as `0.123` and matches the wrong
row (`[2]`, the literal `[1, 3]`); H2 and Postgres keep the values. A Date's or a Number's value decides its kind, so a
list of them has no one element type. So `QueryParameters.Declared.slot` refuses a Float, Decimal, Date or Number list
by name.

**The fix.** A decimal list bound so each database keeps its values' own scale: on DuckDB as an array of a stated
precision and scale (the plan cannot know the values' scale), as text cast per element, or as the values written into
the statement as literals; measured against today's literal list before it lands. A Date or Number list binds each value
by its kind once the runner (step 3) converts values.

**Acceptance.** `PlanMakerTest`'s list cases gain a Float, a Decimal (a value of more than three places), a Date and a
Number list, answering as their literal lists on DuckDB, H2 and Postgres.

**When.** Before step 4 switches the server's `execute` to plans: until then today's path serves them. Cost of leaving
it past step 4: a query with such a list parameter is refused.

**Anchor.** The refusal in `QueryParameters.java`: "a list of decimals, Dates or Numbers has no one element type".

**Also (2026-10-09, step 3).** A list of DateTimes is bound, as one array of timestamps through the driver, and the
drivers do not pass digits finer than a microsecond alike (DuckDB's cuts them, Postgres's rounds them:
`probes/timestamp-results.txt`), where one DateTime parameter is passed as its text into the cast of its literal's
type. A DateTime list with finer digits than a microsecond can answer otherwise than its literal list on DuckDB and
Postgres; the fix is this row's (a list's elements each typed as its literal), and its acceptance gains such a list.

---

## PARK-21 — a plan does not bind an optional enumeration, a class instance, or a Byte, LatestDate, StrictTime or Variant value

**Parked** 2026-10-09 by the Plan Gen / Exec Split session with step 2's landing 2, after its audit (blocker B1 and
finding S6).

**What happens today.** `QueryParameters.Declared.slot` refuses, by name, when the plan is made:
- **an optional enumeration parameter** (`st: E[0..1]`). Its ABSENCE has three candidate answers and none is measured:
  legend-engine's plan writes `${optionalVarPlaceHolderOperationSelector(st![], ..., '0 = 1')}` (the legacy golden of
  `testOptionalEnumParameterEqualsClassProp`: no row for `==`, every row for `!=`); Pure's own equality holds for
  `[] == []` (the rows whose status is empty); and today's let path compares a NULL (no row for `==`, the non-empty
  rows for `!=`). The value table answers as the engine's text reads for both, the decoded comparison as the let path,
  so neither is bound until the engine's answer is measured. A present value would be exact; the plan cannot know.
- **a class instance** (`i: C[1]`): Pure takes one, and the legacy printer writes its properties (`${i.name}`); a lite
  plan binds plain values only (§9, step 2's decisions), so its properties are not slots yet.
- **a Byte, LatestDate or StrictTime value**: no measured binding. A Variant, being a class to the planner, is refused
  as a class instance.

The runner (step 3, `PlanParameters`), should a plan carry one, refuses a value of any of these types by name ("a
Byte value is not bound by a plan (PARK-21)").

**The fix.** For the optional enumeration: run the three shapes (`==`, `!=`, absent and present) through legend-engine
4.145.0's `execute` and bind what it answers — the value table's `NOT IN` beside the null arms already gives the
engine's text's answer if that is the engine's; else a null arm. For a class instance: each property read a slot of its
own, named `i.name` as the legacy printer names it. For the other types: measured bindings.

**Acceptance.** `PlanMakerTest` binds each: an optional enumeration absent and present, `==` and `!=`, answering as
legend-engine does; a class parameter's properties; the three types — on DuckDB, H2 and Postgres.

**When.** Before step 4 switches the server's `execute` to plans: until then today's path serves them. Cost of leaving
it past step 4: such a query is refused.

**Anchors.** The three refusals in `QueryParameters.java`: "an optional enumeration's absence is not bound", "a class
instance is not bound as a plan's parameter", "a Byte, LatestDate or StrictTime value is not bound"; and the runner's,
"is not bound by a plan (PARK-21)" in `PlanParameters.java`.

---

## PARK-22 — engine JSON with spans for a path literal across lines is refused by lite's reader

**Parked** 2026-10-09 by the Studio / SDLC / Depot line, with the protocol program's leg 5 (the user: "can wait for
the server phase").

**What happens today.** A path literal (`#/Person/firm/name#`) reaches several lines only where the engine's PRETTY
printer breaks a list argument inside it (4 texts of the corpus). Read from TEXT, lite writes such a literal's spans
exactly as the engine does (leg 5). Read from engine JSON that CARRIES spans, lite refuses it by name: the engine
writes a literal's spans shifted by its start column plus its whole length on the literal's first line only, so from a
one-line span lite works back the literal's column and length and writes them back exactly; across lines the spans do
not say where the literal's lines break, and lite cannot rebuild a position it would write back the same.

**The fix (to design).** A record read from JSON keeps the spans it was given and the emitter writes those back,
where today it rebuilds them from a position; for the path literal (and the other island forms whose spans the engine
shifts) the record then carries its spans as read. That is a change to what lite's records carry, so it is designed
with the protocol program's leg 8 (the server reads models as records), which decides what a record read from JSON
holds.

**Acceptance.** Engine JSON with spans for a multi-line path literal (the 4 texts of
`legend-pure-m2-dsl-path-grammar`'s `TestDSLCompilation`) reads, and writes back byte for byte
(`ModelReaderParityTest`'s exact-inverse rule).

**Cost of leaving it.** Such JSON is refused by name. The JSON an SDLC stores and Studio sends normally carries no
spans, and text lite reads itself is unaffected.

**When.** With leg 8.

**Anchors.** The refusal in `SpecIslandReader.java`: "a multi-line path literal span".

---

## PARK-23 — the tab writes some doubles and every float exactly, but by the slow route

**Parked** 2026-10-09 by the Studio / SDLC / Depot line, with the protocol program's leg 5 (the user: "park the exact").
**Restated** 2026-10-10, when its exactness was fixed: TeaVM's class library now converts every double and float both
ways exactly as the JDK does (`third_party/teavm_classlib`, `ExactDecimal`; the user, 2026-10-10: the tab stays on
TeaVM, held to the JDK by tests, `docs/WEB_IMAGE_SPIKE_2026_10_10.md`). `//wasm:conformance_test` holds every number
family to the JDK with no difference; `Json`'s writer and reader needed no change. What is left of the row is its speed
clause ("no slower than today's"), which one route does not meet.

**What happens today.** `ExactDecimal.shortest` takes a fast route for a normal double whose shortest decimal has at
most 15 digits and whose scale is within 10^±22 (about 1e-7 to 1e37: what people write), at TeaVM's old speed (40,000
such values in 74 ms in the module, against 72 before). Everything else takes the exact route over big integers: a
double of 16 or 17 digits, a subnormal, a double outside that range, and every float. Measured on random doubles (all
17 digits): 40,000 in 261 ms against TeaVM's old (inexact) 74, about 3.5 times, 6.5 microseconds a value; random floats,
40,000 in 53 ms. Reading is at the old speed (Clinger's fast case, else big integers: 40,000 random decimals in 88 ms
against 82).

**The fix.** A shortest-digits algorithm for the remaining case, written from its paper (Schubfach, Giulietti 2020; or
Ryu, Adams 2018) under the clean-room rule, held by the same tests.

**Acceptance.** `//third_party/teavm_classlib:tests` and `//wasm:conformance_test` unchanged; the module writes 40,000
random doubles within a small factor of TeaVM's old 74 ms.

**Cost of leaving it.** Only the tab pays (the server writes with the JDK), and only for the values the exact route
takes: about 6.5 microseconds a double. The tab prints few doubles (literals, plan answers).

**When.** When the tab writes doubles in bulk, or with the offer of these fixes to TeaVM, whose users would want the
old speed.

**Anchors.** `ExactDecimal.FAST_DIGITS` is 15, and the fast route's limit is derived from it (`FAST_LIMIT =
POW10[FAST_DIGITS]`); `third_party/teavm_classlib`'s `ExactDecimalTest.theFastRouteStopsAtFifteenDigits_park23` holds
it: a faster route for the rest changes it, and the row closes.

---

## PARK-24 — lite's answers differ from Pure's values in four measured places (the output layer)

**Recorded** 2026-10-10 by the Plan Gen / Exec Split session, with the user's ruling the same day: an answer is Pure's
value — its kind, its digits, a decimal's scale — everywhere; its text follows the API served (`pure/v1`:
legend-engine's JSON conventions, the 2026-09-27 ruling; lite's own outputs: Pure's own text). **Restated the same
day:** the first statement measured Pure through legend-engine's server, which runs Pure COMPILED (a Float a `double`);
the reference is Pure INTERPRETED (the numeric charter's, and PCT's), where a Float keeps its digits. Two of the four
first gaps — Float arithmetic in decimal (`3 * 1.1` = `3.3`) and a Decimal literal's scale (`2.50D` = `2.50`) — are what
interpreted Pure answers, so they are not gaps. Evidence: `docs/execution-plan-boundary-2026-10-05/probes/engine-reference/`
(`pure-interpreted-results.txt` from `PureEval.java`; `results.txt`, legend-engine's execute and compiled Pure).

**What happens today** — today's path and a plan alike (a plan reproduces today's path), against interpreted Pure:
1. A whole Float prints without its `.0`: `$r.ID * 1.5` answers `3` where Pure and the engine answer `3.0`. (On
   Postgres a plan's Number-typed column answered `3.0` and today's `let` path's Float column `3`: `PlanCases`' decimal
   Number case compares its value in a filter until this is fixed.)
2. A Float at an extreme magnitude prints in exponent form in lite's own outputs: `1.5E15`, `2.5E-7`, where Pure prints
   `1500000000000000.0`, `0.00000025` (on `pure/v1`, legend-engine's JSON writes `1.5E15` too).
3. A decimal result loses its scale: `$r.ID * 2.50D` answers `2.5`, `5`, `7.5` where Pure's are `2.50`, `5.00`, `7.50`.
4. A DateTime prints `2024-01-02T10:30:00`, where Pure prints `2024-01-02T10:30:00+0000` and the engine's execute
   `2024-01-02T10:30:00.000000000+0000`.

**The fix.** `docs/OUTPUT_LAYER_DESIGN_2026_10_10.md`: one text owner per kind, per API, both paths at once; each
deliberate difference from legend-engine (its compiled Float arithmetic, `3.3000000000000003`; its 16-place decimal
arithmetic, `cast(x as Decimal(32,16))`) a row of `docs/SEMANTICS_REGISTER.md`.

**Acceptance.** The design's output matrix as a test: each answer Pure's value, in the API's text; `PlanCases`' decimal
Number case projects its value again.

**When.** Next, after step 3 lands (the user's order, 2026-10-10).

**Anchor.** Pinned at the first statement's gap 1: `plainFloat(` in `AnsiSqlRenderer.java`, the Float literal written
bare — which the restatement keeps (Rule 1 is interpreted Pure's). The anchor moves with the design's first code
commit, to the whole Float's text (`floatPrint`).

## PARK-25 — the shared element namespace misses services, data, connections, runtimes and clean-sheet mappings

**Recorded** 2026-10-11 by the Bazel program session, from the DataCube line's report: a model holding two mappings of
one name crashed the normalizer's include ordering (the cycle check counted the parsed mappings against the ordered
names and named an empty cycle: an index error, not a refusal). Fixed the same day (GATES "A duplicate mapping is
refused"): the check counts the names, and a parsed mapping takes its slot in the shared packageable-element namespace
(`ModelBuilder.internElement`), so a mapping defined twice, or under a class's name, is `ModelIntegrity`'s "Duplicated
element", as a class's is (the D6b batch, engine parity).

**What happens today.** Services, `###Data` elements, connections (standalone and a runtime's inline ones) and runtimes
still slot last-wins by plain `intern`: a second definition of one of those names in one text replaces the first
silently. A clean-sheet (function-form) mapping is never ingested at parse (`ModelBuilder.indexed` answers true for
it: the index holds nothing for the pre-E surface) and its product registers by plain `intern`, so one defined twice,
under a class's name, or beside a DSL mapping of its name, is last-wins too. The module parse (`Compiler.parseSources`)
drops a same-kind duplicate itself and reports it, so the corpora never see the same-kind case; a cross-kind collision
(a runtime under a service's name, a clean-sheet mapping under a DSL mapping's) reaches the index and is not refused.
legend-engine refuses every duplicated element. The same namespace holds the PACKAGES: an element whose path is also
a package (`trades::domain` beside `trades::domain::Trade`) is refused by legend-engine 4.145.0 with "An element named
'domain' already exists in the package 'trades'" (the DataCube line's audit, 2026-10-11, against the engine); lite's
index registers no package, so nothing collides (not proven by a run). And the parsed model's side maps (`ParsedModel`'s
offsets, imports, sources, spans) are keyed by path alone (`ParsedModel.keyOf`), so a cross-kind pair of one name shares
one entry: the refusal's position is whichever element was recorded under the key (the engine points at the second),
and parser-equivalence's section planner (`Sectionize.plan`) declines such a snippet as not sectionizable (2026-10-11,
found by the conformance ratchet on the refusal's own test).

**The fix.** Each of those kinds registers through `internElement` (a clean-sheet mapping's name takes its slot at
parse, the surface itself staying out of the index), and every element's package prefixes take theirs as packages,
so a path that is also a package is refused as the engine refuses it; the side maps key by kind and path, as the module
parse's dedupe does, so a refusal points at the second element. A runtime's inline connections are named `rt$store$id` (the
reserved `$` keeps them from a user's names); two inline connections of one runtime with the same store and id would
then collide with each other, which the fix decides (the engine's answer). The six corpus passes and the render census
judge whether any corpus model relied on the last win.

**Acceptance.** A test per kind in `IncludeRulesTest`'s shape (the second definition refused by name, the module parse
reporting the drop), one for a path that is also a package; the corpus passes identical.

**Why parked.** The report was the mapping's, fixed in its landing; the other kinds change what a model with a
collision does, so they take a judged landing of their own. The cost while parked: a cross-kind collision among those
kinds compiles with the last definition and no word.

**When.** Not scheduled: a small landing of its own, the user's call.

**Anchor.** `putAtId(services, intern(sd.qualifiedName()), sd)` in `ModelBuilder.java`: the first of the plain-intern
slots.

## PARK-26 — a mapping included under two spellings is refused as an include cycle

**Recorded** 2026-10-11 by the Bazel program session, from the audit of the duplicate-mapping fix (GATES "A
duplicate mapping is refused"). `StoreSubstitutionRewrite.resolveAllStores` resolves each include path to a mapping
name (the path as spelled, else under the includer's package) and counts the resolved names as the mapping's pending
includes: `include w::A` and `include A` written in package `w` resolve to one name, counted twice and satisfied once,
so the mapping is never ready and is refused as "mapping include cycle". The listed-twice check just before it
("Duplicated mapping include") compares the paths as spelled, so it does not catch the pair.

**What happens today.** A model including one mapping under both spellings is refused with the wrong reason. Not
measured against the corpora.

**The fix.** Establish legend-engine's rule for the pair (its "Duplicated mapping include" check: by the resolved
mapping, or by the spelling), then either count a resolved name once or refuse the pair as a duplicated include.

**Acceptance.** A test in `IncludeRulesTest` with both spellings, asserting the engine's answer.

**Why parked.** The engine's rule for the pair is not in the repository's upstream notes, and the fix is a choice
between accepting and refusing; the cost while parked is one wrong refusal reason on a shape no corpus is known to use.

**When.** Not scheduled; with PARK-25 or on its own.

**Anchor.** `if (fqn != null) {` followed by `incs.add(fqn);` in `StoreSubstitutionRewrite.java`: the resolved names
counted as listed.

## PARK-28 — a `~func` descriptor's signature is not matched against the function's

**Recorded** 2026-10-11 by the Bazel program session, from the audit of the descriptor-type refusal (GATES "A `~func`
descriptor spelling a type by its package path is refused"). legend-engine resolves a relation class mapping's `~func`
descriptor by its full id (`f__Relation_1_`: the name, the parameter types, the return type and multiplicity, legend-pure's
`functionDescriptorToId`), so `~func f():Any[*]` against `f():Relation<Any>[1]` is "Can't find the packageable element
'f__Any_MANY_'" there (inferred from the id format and the engine fixture for a mismatched descriptor in
`parser-equivalence/src/test/resources/engine-grammar-fixtures.jsonl`; not run).

**What happens today.** `MappingFromProtocol.relationFunction` cuts the descriptor at `(` and resolves the name alone
(`MappingNormalizer.relationFunctionPipeline`: by name, then by signature id), so a signature naming another return type
or multiplicity, or none at all (`~func f()`, which the parser accepts), compiles against whichever overload the name
finds. The `~src f():R[1]` form keeps the name only (`MappingProtocolParser`, the `~src` arm: `fd.nameEnd()`), so a
signature tail there is accepted and dropped; the engine parses `~src` as an expression (a tail is likely a parse error
there; not verified). A function activator's `function:` descriptor (`FunctionActivatorSectionGrammar`) is parsed only,
never resolved, so a package path or a wrong signature there is never checked. The descriptor-type refusal
(`refuseDescriptorTypes`) reads the types' spelling, not their match.

**The fix.** Convert the descriptor to the engine's id (`SignatureMangle`'s format: head names, multiplicities) and
resolve by it when a signature is written, refusing a mismatch as the engine does; the `~src` form's tail refused or
matched the same way, after the engine's answer for it is read from its grammar; the activator's descriptor resolved
when activators compile.

**Acceptance.** `RelationFunctionDescriptorTest`: a descriptor naming another return type or multiplicity refused with
the engine's message; `~func f()` without a signature answered as the engine answers it (read from its grammar first);
the corpus passes identical.

**Why parked.** The report was the package path's, fixed in its landing; matching the signature changes what resolves
for every `~func` with a signature, so it takes a judged landing of its own, after the engine's answers for the
no-signature and `~src` forms are read. The cost while parked: a descriptor whose signature does not match compiles
here and fails in the engine, with no word.

**When.** Not scheduled; a small landing of its own, the user's call.

**Anchor.** `sig < 0 ? funcRef : funcRef.substring(0, sig)` in `MappingFromProtocol.java`: the signature cut away.

## PARK-29 — an Iceberg table on Python is checked once, before its view, not on every read

**Recorded** 2026-10-13 by the DataCube + Python line, with G5 (`docs/ONE_PLATFORM_PROGRAM_2026_10_12.md`: Iceberg files
on Python, every file remote). An Iceberg table's metadata names its files by path, so a remote table could make
Python's DuckDB read this machine's files (R4).

**What happens today.** Before a page's Iceberg table is mounted, `Engine._iceberg_refusal` (`python/legend_lite/
engine.py`) reads the manifest lists its snapshots name, the manifests those name, and the files its current snapshot
reads, each level only once the level naming it is checked, and refuses the table unless every one is a remote URL
(`remote_url`). Two things remain:
1. **The check holds when it runs, not afterwards.** A server that answers differently later, or a table written again
   after the page opened it, can name a file on this machine at the view's next read. Pinning the snapshot would not
   close it: the snapshot's metadata is read again on every scan, and DuckDB finds the current metadata file by its
   own rule (`version-hint.text`, `version_name_format`), which Python would have to copy.
2. **DuckDB 1.5.5's `iceberg_scan` ends the process when a manifest cannot be read** (an uncaught exception: measured
   2026-10-13 with a manifest answering 404, and with a missing local one), where `iceberg_metadata` and `read_avro`
   report the same failure. The check reads every manifest through those first, so a table that fails them is
   refused; one whose manifest stops answering between the check and the view still ends the process. The tab's
   DuckDB (WebAssembly) is likely the same; not measured.
3. **A refused table keeps its keys.** The page's S3 keys are put on the shared connection before the check (the
   mount's access step), scoped to the table's own URL, and are not taken off when it is refused, as a mount that fails
   leaves them (the audit's S5).

**The fix.** DuckDB itself refusing this machine's paths on the connection legend-lite owns, for every read
(`enable_external_access = false` with `allowed_directories` naming the remote schemes and the headless engine's own
folder, once DuckDB's rules for remote prefixes there are measured), so no check is needed; and the abort reported to
DuckDB's iceberg extension, the pin moved when it is fixed.

**Acceptance.** `python/tests/test_headless.py`'s `Remote` cases, with a table that names a file on this machine only
in a snapshot written after the mount, refused at the read; a manifest answering 404 at the view's read is an error,
not the process's end.

**Why parked.** Turning external access off changes what every query on Python's shared connection may read (a
person's own use of it included) and cannot be turned back on for that connection; it takes a design of its own. The
cost while parked: a table that changes between the check and a read, or a server that answers so on purpose.

**When.** With G6 (a connection per page on Python), which gives the place to turn it off for a page's reads alone.

**Anchor.** `def _iceberg_refusal(` in `python/legend_lite/engine.py` (ParkedWorkLedgerTest reads core only; this row's
anchor is checked by hand).

## PARK-32 — a page's charts are not drawn headless, so Python exports a grid, not a board

**Recorded** 2026-10-13 by the DataCube + Python line, with D (`docs/ONE_PLATFORM_PROGRAM_2026_10_12.md`: headless
pages). A grid's Export is one core (`datacube/src/export-render.ts`), the screen's menu and Python's `grid.export()`
alike; on the screen, a page with charts exports its whole board, each chart as its picture where the board puts it.

**What happens today.** `grid.export(kind)` (headless `gridExport`) writes the grid as its Export menu does --
formats, appearance, heatmaps -- but carries no chart: DataCube draws a chart's picture through the page's own canvas
(echarts on the board), which V8 inside Python has none of. Python has no `page.export()`.

**The fix.** A chart drawn headless (echarts' server-side SVG rendering, which needs no canvas), the board laid out as
the screen lays it out (`page.exportPage()`'s tiles), and `page.export(kind)` over it.

**Acceptance.** `python/tests/test_headless.py`: a page of a grid and a chart exported as HTML carries the chart as an
SVG where the board puts it, and as PDF and Excel as the screen's export does.

**Why parked.** Drawing charts headless is a dependency and a design of its own (fonts, sizes); the grid's export is
what stands alone.

**When.** With F1 (dashboards on classes), which reworks the board.

**Anchor.** `PARK-32` in `datacube/src/headless/grid.ts` (`gridExport`) (ParkedWorkLedgerTest reads core only; checked
by hand).

## PARK-34 — an execute over a column whose relational spelling is unprobed is refused

**Recorded** 2026-10-13 by the DataCube + Python line, with C1 (`docs/ONE_PLATFORM_PROGRAM_2026_10_12.md`: the tab's
execute answered by the Java the server and Python use; its audit). Execute's TDS answer names each column's relational
type (`builder.columns[].relationalType`, `UpstreamRelationType.relationalSpelling`), which knows the spellings measured
against legend-engine 4.145.0 -- Integer, Float, Decimal, Number, String, Boolean, StrictDate, DateTime, Date and
enumerations -- and refuses any other ("the relational spelling of ... is unprobed").

**What happens today.** An execute whose result has a column of another type -- a Variant (a `SEMISTRUCTURED` store
column) is the one a page meets -- is refused 500, on legend-lite's server since E8 and in the tab since C1: before C1
the tab typed its rows itself and never asked the spelling, so such a query ran there. Python's ARROW_IPC answer names
the same builder, so it refuses alike. Planning (DataCube's grids, `planJson`) is not affected.

**The fix.** Ask legend-engine what its TDS builder spells a Variant column (and each other type a store column can
have that is not measured), and add it to `relationalSpelling` beside the others, with the probe's date.

**Acceptance.** `PureV1ApiTest`: an execute over a Variant column answers the engine's builder, its relationalType the
probed spelling; `//datacube:python_execute_test` and the tab agree on it.

**Why parked.** It needs legend-engine run (the L line's), and refusing an unmeasured spelling is the rule (do not
guess); C1 moved the tab onto the server's answer, which already refused it.

**When.** With L1 (the differential against legend-engine), which runs the engine.

**Anchor.** `is unprobed -- probe legend-engine, do not guess` in `core/src/main/java/com/legend/plan/UpstreamRelationType.java`
(ParkedWorkLedgerTest).

## PARK-36 — a page on Python reads a query's rows whole, never as they come

**Recorded** 2026-10-13 by the DataCube + Python line, at the user's ruling on the program's G2
(`docs/ONE_PLATFORM_PROGRAM_2026_10_12.md` §5 G2): "is this chunking thing worth it - seems like we've spent too much
time and ended up in bit of hack solution instead of true streaming"; "drop it".

**What happens today.** Python's plan-and-run reads a result whole into memory and answers it whole, and the page's
`pythonRunner` hands it to the cube as one chunk. The tab's own DuckDB streams its batches.

**Why the first attempt was dropped.** Built and dropped the same day:
- it still read a frame's result whole under the frames' lock, then sent it in parts, so it saved no memory;
- only a warehouse table's chunks were passed through as they came;
- a stream cut short looked complete (an HTTP/1.0 body ends at the connection's close), and was fixed only by counting
  rows on the page: a patch over a transport that says no end.

**The fix, a design of its own:**
- read from DuckDB as it goes (a record batch reader), on a connection of the query's own (G6: Python's one shared
  connection would hold every other query for the duration);
- a transport that says its end (HTTP/1.1 chunked, its last chunk), so a cut answer fails as a cut answer;
- the grid drawing rows as they come, without which the gain is not felt.

**Acceptance.** A result larger than Python's memory reaches the page, its first rows shown before its last are read;
a stream cut in Python fails on the page in words.

**Cost while parked.** A large result's first row waits for its last on Python's pages; memory holds the whole result.

**When.** When a result too large for memory, or a slow first row from a large warehouse table, makes it worth it.

**Anchor.** `the whole result as one chunk: streamed back in chunks is a follow-up` in
`datacube/src/services/python-host.ts` (ParkedWorkLedgerTest reads core only; checked by hand).

## PARK-37 — a spelled integer at or above 2^63 read as an index, an offset or a precision wraps

**Recorded** 2026-10-14 by the Compiler Rewrite line, at the audit of the three planner fixes the tenth line asked for
(`docs/build-inventory/program/evidence/iq-finds/AUDIT.md`, question 1). The two folds that compare or equate two
spelled integers now read the exact value (`TypedCInteger.big()`: the parser keeps a literal beyond long a
`BigInteger`); the other reads of a spelled integer's `longValue()` in the compiler and the lowering are the index,
offset and precision kind, and the audit lists them.

**What happens today.** A literal at or above 2^63 wraps when such a read takes its `longValue()`:
`at(18446744073709551616)` over a spelled collection picks element 0 (`ExecuteChainAssembly`), a zero test reads the
wrapped value (`ResultEnvelopeSplice`, `CastChecker`), and `StaticFold`'s evaluator adds and subtracts in long. Nothing
a query writes on purpose; a wrong answer rather than a refusal when one does.

**The fix.** Each such read takes `big()` and refuses where the value does not fit (an index beyond the collection, a
precision beyond int), with a test per read.

**Acceptance.** `at` with an index at 2^64 over a spelled collection is refused, not answered with element 0; the zero
tests and `StaticFold`'s arithmetic agree with Pure's for a literal beyond long.

**Why parked.** No query reaches these reads with such a literal today, and no wrong answer was reported; the fix
touches a dozen sites across the compiler and the lowering.

**When.** With the next change to any of those sites, or a reported wrong answer.

**Anchor.** `k.value().longValue() < coll.elements().size()` in
`core/src/main/java/com/legend/compiler/spec/ExecuteChainAssembly.java` (ParkedWorkLedgerTest).
