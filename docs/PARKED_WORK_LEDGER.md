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
moves at least one of those.

---

## PARK-15 — The legacy plan picks an enumeration mapping without the place it is used

(Numbered 15: the build rebuild's Phase 3 branch holds PARK-5 to PARK-14.)

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

## PARK-16 — the test-data generator's hand-built SQL spells a table or schema name raw

**Parked** 2026-10-08 by the user, to keep the execution plan line on its path ("make plans the only way to run"), with
the instruction to "record the ddl fix so that we actually do it (either now or later)". Found by the Studio line's
landing audit; confirmed in the code the same day. **Restated** 2026-10-09: the product half is FIXED (below); the user
chose "product now, generator later" for the rest.

**Fixed 2026-10-09 (with E, `docs/EXECUTION_PLAN_BOUNDARY_2026_10_05.md` §10).** The statements that create, drop, fill
and empty a table (`AnsiSqlRenderer.render(SqlDdl)` and `render(SqlDml)`), and `CREATE`/`DROP SCHEMA`, spelled schema
and table names RAW, where queries spell them through `physicalName` (a reserved word or a name that is not plain is
quoted): a default-schema table `order` got `Drop table if exists order;`, refused by DuckDB and H2, on the server and in
the Studio tab (`CsvSeed.sqls`). Now one rule for the dialects that execute: DDL and DML spell each name through
`physicalName`, as queries do, and as legend-engine's own H2 DDL does (`translateCreateTableStatementForH2` spells the
table through `tableToString`, its query spelling); Postgres's two overrides are gone. (The legacy engine-text printer's
lexicon has no reserved words, so its setup text spells such a name bare, as before.) `ReservedNamesSeedTest` seeds a default-schema table `order` and a table in
a schema `select` and answers a query over each on DuckDB and H2, `PostgresArmTest` on Postgres; the render census shows
nothing else changing.

**What remains.** The test-data generator's hand-built SQL (`TestDataGenerator.qualify`, `select ... from
<schema.table>`) spells the names raw, outside the dialects: it runs on whatever test database it is handed, with no
dialect, so a reserved table name breaks a test-data generation over it.

**The fix (agreed).** The generator's names spell through the session's dialect's `physicalName`: the dialect is threaded
from the test-body executor (`StatementExecutor`, `BodyCompiler`, `SqlTextVerdicts`) through
`TestDataGenerationNatives` to the generator.

**Not in scope (reviewed 2026-10-08, the audit of E-1).** `StatementExecutor.ddlStatementString` also writes a schema
name raw (`Drop schema if exists <s> cascade;`, `Create Schema if not exists <s>;`): that is the Pure natives
`dropSchemaStatement`/`createSchemaStatement` returning legend-engine's own text as a value (`toDDL.pure`), parity by
design, not a statement lite spells for a database.

**Acceptance (what closes this row).** A test-data generation over a default-schema table named `order`, and over a
table in a schema named `select`, runs on DuckDB and H2.

**When.** In phase 3 of the execution plan program (the runner for Pure test bodies), when that executor code moves out
of `exec`; sooner if a user meets it. Cost of leaving it parked: test-data generation over a reserved or unusual table
name fails loudly (a SQL syntax error); no product path is affected.

**Anchor.** The raw spelling `|| "default".equals(schema) ? table : schema + "." + table;` in `TestDataGenerator.java`.

## PARK-17 — Python's refusal kind is mixed until the protocol program's leg 6

**Parked** 2026-10-09 by the DataCube + Python line, on the protocol program's leg 4
(`docs/PROTOCOL_PROGRAM_2026_10_05.md` §4, step 4b), which moved Python's grammar (`parse`, `print_tree`,
`model_elements`) onto legend-engine's `pure/v1` through `lite_pure_v1`.

**What happens today.** A Python `LegendError`'s `kind` is the engine's refusal kind (`errorType`, `PARSER`, else the
answer's status) for those three, as `pure/v1` answers it, and still the compiler's Java exception class for the rest
of Python's compiler calls (`relation_type`, `plan`, `plan_text`, `database_from_catalog`, `table_model`, ...): two
vocabularies for one field.

**The fix (agreed).** Leg 6: the boundary's operations that duplicate a `pure/v1` endpoint go from both adapters,
Python's included (`lite_plan_json`, `lite_relation_type_json`, where they are true twins of E9 and E5), and Python asks
`pure/v1` for them as the tab does; for each call that remains lite's own, the kind it reports is decided with the
DataCube + Python line, so that no Python user sees a Java class name where `pure/v1` would give an `errorType`.

**Acceptance (what closes this row).** Every Python compiler call that is a `pure/v1` endpoint asks `lite_pure_v1`,
and `python/legend_lite/compiler.py`'s `LegendError` documents one rule for `kind`.

**Cost of leaving it.** A Python caller that branches on `kind` must know which call it made.

**When.** With leg 6 — whose one whole-model compile replaces the copy in `PureV1Api.compile` that the anchor names, so
the anchor goes red there and the row must be closed or restated.

**Anchors.** Two, because the parked behaviour lives in `python/`, which `ParkedWorkLedgerTest` cannot read (it scans
`core/src/main/java`): (1) in the ledger test, a PROXY for leg 6's start -- `PureV1Api.compile` stringing "compile a
whole model" together itself (`com.legend.Compiler.compileAllBodies(` then `com.legend.Compiler.compileModel(`), in
`PureV1Api.java` alone, which leg 6's one whole-model compile replaces; (2) on the behaviour itself,
`python/tests/test_compiler.py`'s `test_refusal_kinds_are_mixed_until_leg_6` (`//python:bindings_test`), which pins the
grammar's refusal kind as the engine's and `relation_type`'s, `plan`'s and `plan_text`'s as a Java class name, so a
call moving to `pure/v1` without this row being closed or restated turns it red (found by leg 4's audit, 2026-10-09).

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

**Anchor.** `H2.placeholder`'s refusal, "has no one type a statement names", in `H2.java`.
