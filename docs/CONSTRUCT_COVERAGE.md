# Construct coverage taxonomy — real legend-engine core_relational

Generated from the sweep scoreboard (denominator 2489) plus the eager-G census
(`Compiler.compileAllBodies`: 6741 corpus functions, 4909 walls). Classification
is judgment-tagged from normalized failure reasons — `BUG?` means *suspected*
defect in legend-lite (should work, doesn't); `GAP-*` means a construct we have
not built; `DIFF` means we execute and produce different rows.

## Headline

| class | tests | meaning |
|---|---|---|
| PASS | 902 | row-verified against the engine corpus |
| DIFF | 30 | executes; rows differ (the 30-entry burn ledger) |
| GAP-vocab | 833 | runner/feature vocabulary the harness does not recognize yet |
| GAP-H | 160 | StoreResolver (H) vocabulary — object-space nodes, slots, multi-hop |
| GAP-M | 131 | mapping constructs — union keys, inheritance dispatch, ~groupBy PMs, views |
| GAP-G | 33 | type-checker gaps — higher-order lambda positions |
| GAP-I | 22 | lowering vocabulary |
| GAP-other | 134 | unclassified tail (small clusters) |
| BUG? | 244 | SUSPECTED legend-lite defects: resolution, multiplicity, SQL emission |
| DARK | ~146 | parse-walled files (#50) — tests never discovered, outside the denominator |

## The no-execute bucket (739) by feature family

Tests whose bodies never call `execute(|...)` — they exercise OTHER engine
entry points; each maps to a feature track:

| family | tests | track |
|---|---|---|
| graphFetch/tests | 117 | graph-fetch execution |
| executionPlan/tests | 93 | plan generation |
| tests/mapping+tests root | 113 | #43 runner vocabulary (executeReflectively, asserts) |
| testDataGeneration/tests | 68 | #46 test-data generation |
| transform/fromPure | 50 | #47 pure printer/round-trip |
| lineage/scanRelations | 49 | #44 lineage |
| tds+functions/tests | 67 | #43 runner vocabulary |
| aggregationAware | 28 | aggregation-aware rewrite |
| validation | 31 | #45 constraints |
| sqlDialectTranslation+sqlQueryToString | 30 | dialect translation |
| router/tests | 18 | routing |
| others | 75 | long tail |

## Ranked clusters (>=5 tests)

| n | class | status | normalized reason | top families | track |
|---|---|---|---|---|---|
| 739 | GAP-vocab | SHAPE | no execute(\|...) call | graphFetch/tests(109), executionPlan/tests(93), testDataGeneration/tests(68) | #43/#44-47 feature families |
| 69 | GAP-vocab | SHAPE | sql-only: N advisory golden-SQL assert(s), no row verification | milestoning/tests(22), postprocessor/tests(18), functions/tests/projection(6) | #43 golden-SQL-only asserts |
| 59 | BUG? | ERROR | '_' is not a known class, mapping, runtime, connection, or database — user elements in a query need a fully qu | tests/mapping/modelJoin(26), tests/mapping/extends(17), tests/mapping/extends/union(8) | resolution: modelJoin/extends imports |
| 36 | GAP-H | ERROR | object-space expression node TypedFilter is not substitutable yet (H2 vocabulary) | tests/advanced(13), functions/tests/projection(11), postprocessor/tests(4) | resolver vocabulary (H) |
| 36 | BUG? | ERROR | unknown function '_' | tds/tests(11), functions/tests/projection(9), tests/mapping/enumeration(5) | resolution/registration |
| 33 | GAP-G | ERROR | a bare lambda has no type outside a call position (lambdas type against their call's signature) | executionPlan/tests(10), tds/tests(7), graphFetch/tests(4) | higher-order lambdas (let-bound/returned) |
| 24 | GAP-H | ERROR | class query under TypedPropertyAccess is not resolvable yet (H2 vocabulary) | functions/tests(11), tests/mapping/classMappingFilterWithInnerJoin(4), tests/mapping/inheritance(4) | resolver vocabulary (H) |
| 20 | BUG? | ERROR | '_' is not a known class, mapping, runtime, connection, or database | tests/mapping/modelJoin(9), tests/mapping/classMappingFilterWithInnerJoin(6), functions/tests/projection(3) | resolution: modelJoin/extends imports |
| 18 | GAP-H | ERROR | navigation through class-typed slot property '_' is not supported yet | tests/advanced(14), functions/tests(3), tests/query(1) | resolver vocabulary (H) |
| 17 | BUG? | SHAPE | statement '_' failed through the pipeline: unknown function '_' | tds/tests(11), tests/mapping/union(3), functions/tests(2) | resolution/registration |
| 17 | GAP-M | ERROR | property '_' of class '_' is not mapped in mapping '_' | tests/mapping/union(12), milestoning/tests(3), functions/tests/projection(1) | mapping constructs (union/inheritance/groupBy/views) |
| 17 | GAP-M | ERROR | class '_' is not mapped in mapping '_' (mapping ~filter with an explicit (INNER) join type row-explodes throug | tests/mapping/classMappingFilterWithInnerJoin(17) | mapping constructs (union/inheritance/groupBy/views) |
| 15 | GAP-M | ERROR | filter predicate references column '_', unresolvable even after isolation | milestoning/tests(8), tests/mapping/tree(7) | milestoning/tree filter isolation |
| 14 | GAP-M | ERROR | class '_' is not mapped in mapping '_' (Join '_' navigates to a CLASS mapped over view '_'; class navigation o | functions/tests(4), functions/tests/projection(4), milestoning/tests(3) | mapping constructs (union/inheritance/groupBy/views) |
| 14 | GAP-M | ERROR | a navigation join over this union demands key column '_', which union member rows do not all carry; heterogene | tests/mapping/inheritance(14) | mapping constructs (union/inheritance/groupBy/views) |
| 13 | GAP-other | ERROR | in function '_': property '_' of '_': expected _ got _ (value: AppliedFunction[function=toOne, parameters=[App | tds/tests(6), functions/tests/projection(3), tests/mapping(3) | unclassified tail |
| 12 | BUG? | ERROR | Binder Error: No function matches the given name and argument types '_'. You might need to add explicit type c | functions/tests(5), tds/tests(5), tests/mapping/sqlFunction(2) | SQL emission |
| 12 | BUG? | ERROR | expected _ most one value, got _ ([*]) | functions/tests(9), functions/tests/projection(1), tests/mapping/association(1) | multiplicity typing |
| 11 | GAP-H | ERROR | class-typed property '_' used as a whole value is graph output (Phase H4) | functions/tests(3), tests/mapping/embedded(2), tests/mapping/inheritance(2) | resolver vocabulary (H) |
| 11 | GAP-vocab | SHAPE | execute() whose query argument is not a lambda | functions/tests/projection(4), milestoning/tests(4), functions/tests(2) | #43 runner vocabulary |
| 11 | GAP-H | ERROR | object-space expression node TypedSortBy is not substitutable yet (H2 vocabulary) | tests/mapping/union(10), functions/tests/projection(1) | resolver vocabulary (H) |
| 11 | GAP-M | ERROR | class '_' is not mapped in mapping '_' (PropertyMapping '_' for property '_' is not supported under ~groupBy ( | tests/mapping/groupBy(10), postprocessor/tests(1) | mapping constructs (union/inheritance/groupBy/views) |
| 10 | GAP-H | ERROR | class query under TypedMap is not resolvable yet (H2 vocabulary) | functions/tests(8), functions/tests/projection(1), tests/mapping/union(1) | resolver vocabulary (H) |
| 10 | BUG? | ERROR | relation has no column '_' | tds/tests(9), tests/mapping/union(1) | SQL emission |
| 10 | GAP-M | ERROR | class '_' is not mapped in mapping '_' | tests/mapping/association(7), tests/mapping/union(3) | mapping constructs (union/inheritance/groupBy/views) |
| 9 | GAP-I | ERROR | lowering not yet implemented for TypedNativeCall | tds/tests(4), tests/advanced(2), tests/mapping(2) | lowering vocabulary |
| 9 | GAP-vocab | SHAPE | assert form '_' is not supported yet | graphFetch/tests(5), functions/tests(1), functions/tests/projection(1) | #43 runner vocabulary |
| 9 | BUG? | ERROR | in call to '_', argument N: expected _ most one value, got _ ([*]) | functions/tests/projection(5), milestoning/tests(2), router/tests(1) | multiplicity typing |
| 8 | GAP-H | ERROR | object-space expression node TypedMap is not substitutable yet (H2 vocabulary) | functions/tests(2), functions/tests/projection(2), tests/injection(1) | resolver vocabulary (H) |
| 8 | BUG? | ERROR | no overload of '_' matches N argument(s) of these shapes | tds/tests(8) | typing/signature |
| 7 | GAP-H | ERROR | multi-hop navigation firm.address.name through an embedded/slot head is not supported yet | functions/tests(7) | resolver vocabulary (H) |
| 7 | GAP-M | ERROR | class '_' is not mapped in mapping '_' (Join '_' not found in db '_'; PM='_', mapping=meta::relational::graphF | graphFetch/tests/union(7) | mapping constructs (union/inheritance/groupBy/views) |
| 7 | BUG? | ERROR | ~name_length: mapped/aggregate column specifications need an enclosing call to type against | tds/tests(7) | typing/signature |
| 7 | GAP-M | ERROR | association '_' is not mapped in mapping '_' (property '_' routes to NON-root mapping set '_' — multi-set disp | tests/mapping/inheritance(7) | mapping constructs (union/inheritance/groupBy/views) |
| 6 | BUG? | ERROR | class meta::relational::tests::model::simple::Address has no property '_' | functions/tests(6) | typing/signature |
| 6 | DIFF | FAIL | assertSize: expected _ got _ | functions/tests/projection(2), tests/query(2), functions/tests(1) | wrong rows (f5 ledger) |
| 6 | GAP-other | ERROR | in function '_': unknown table '_' in database '_' | tests/query(3), functions/tests(1), milestoning/tests(1) | unclassified tail |
| 6 | GAP-I | ERROR | scalar lowering not yet implemented for TypedCLatestDate | milestoning/tests(6) | lowering vocabulary |
| 6 | BUG? | ERROR | tableReference expects (database, '_'); got _=meta::relational::tests::db], CString[value=default], CString[va | tds/tests(6) | typing/signature |
| 6 | GAP-H | ERROR | object-space use of the instance variable '_' other than property access is not supported yet | tests/mapping/inheritance(3), tests/mapping/union(3) | resolver vocabulary (H) |
| 6 | BUG? | ERROR | in function '_': unknown function '_' | tests/mapping/propertyfunc(6) | resolution/registration |
| 5 | GAP-other | ERROR | unknown type '_' in @TabularDataSet | functions/tests(5) | unclassified tail |
| 5 | GAP-other | SHAPE | harness wrapper '_' carries no zero-arg lambda body | functions/tests(5) | unclassified tail |
| 5 | GAP-other | ERROR | no SQL type for generic Class<meta::pure::metamodel::type::Any> at the lowering boundary | tds/tests(5) | unclassified tail |
| 5 | GAP-other | ERROR | in function '_': property '_' of '_': expected _ got _(N,N) | tests/datatype(5) | unclassified tail |
| 5 | GAP-M | ERROR | class '_' is not mapped in mapping '_' (Join '_' not found in db 'meta::relational::tests::mapping::classMappi | tests/mapping/classMappingFilterWithInnerJoin(5) | mapping constructs (union/inheritance/groupBy/views) |
| 5 | GAP-other | ERROR | graph child '_' of class '_' is mapped as an embedded/join-slot/otherwise/M2M binding — only association child | tests/mapping/embedded(5) | unclassified tail |
| 5 | GAP-other | ERROR | milestoning column '_' is not on the pipeline row of '_' | tests/mapping/relation(5) | unclassified tail |

## Burn order (recommendation)

1. **BUG? clusters first** (~195 tests): resolution failures in modelJoin/extends
   (79), unknown-function (53), multiplicity typing (21), SQL emission (22) —
   suspected defects beat feature gaps; each is likely a small fix with a big cluster.
2. **#50 parse walls**: ~146 dark tests + 18 files excluded from the setup universe.
3. **GAP-H resolver vocabulary** (~120): TypedFilter/TypedSortBy/TypedMap substitution,
   slot navigation, multi-hop — one coherent H-layer arc.
4. **GAP-M mapping constructs** (~110): INNER-join mapping filters (17, one family),
   heterogeneous union keys (14), ~groupBy PMs (11), view navigation (14).
5. **GAP-vocab #43** (~200 of the no-execute bucket are harness vocabulary, not
   feature families — executeReflectively and golden-SQL-only asserts).
6. Feature tracks (#44-47) by user priority — each is a self-contained corpus family.

Regenerate: rerun the sweep, then the clustering script (this file's tables are
derived artifacts; the scoreboard is the source of truth).
