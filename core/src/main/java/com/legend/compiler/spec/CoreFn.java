package com.legend.compiler.spec;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * The closed vocabulary of <strong>core structural constructs</strong> &mdash; the
 * functions the type checker treats as language forms rather than library calls
 * (GHC's sealed {@code PrimOp}; PHASE_G_SPEC_COMPILER.md §6). Strings die at this
 * boundary: a parse-time function name is resolved to a {@code CoreFn} exactly
 * once ({@link #of}), and every downstream dispatch is an exhaustive
 * {@code switch} over this enum &mdash; the compiler refuses to add a construct
 * without a rule, and the whole structural core is visible here.
 *
 * <p><strong>What belongs here:</strong> constructs with binding/scoping effects
 * ({@code let}, lambda-thunk branches of {@code if}), non-value syntactic
 * arguments (colspecs, store refs, {@code ^Class(&hellip;)} payloads), or a
 * distinct HIR node that lowering must switch on ({@code filter}, {@code sort},
 * {@code rename}, &hellip;). <strong>What does not:</strong> the ~440 scalar and
 * collection natives ({@code +}, {@code size}, {@code toUpper}) &mdash; they ride
 * the generic signature-driven path and lower via the native-function table,
 * exactly GHC's library-functions-vs-primops split.
 *
 * <p>A {@code CoreFn} owns <em>every</em> overload of its name: an arm that only
 * treats certain shapes specially (e.g. relation {@code sort} vs collection
 * {@code sort}) delegates the rest to the generic path explicitly &mdash; the
 * decision is visible in the arm, never in a fall-through.
 */
public enum CoreFn {

    /** {@code let name = value} &mdash; a binding with scope effects ({@code letFunction}). */
    LET("letFunction"),
    /** {@code deactivate(e)} &mdash; the compile-time reflection carrier (leg 3b): folds to the expression's DECLARED type. */
    DEACTIVATE("deactivate"),
    /** {@code getRelationalCSVDataFromQuery(query, mapping)} &mdash; TDG lane S1: compile-time reflection, folds to instance literals ({@code CsvCensusChecker}). */
    GET_RELATIONAL_CSV_DATA("getRelationalCSVDataFromQuery"),
    /** {@code generateTestData(...)} &mdash; TDG lane S2: RUNTIME data extraction; the checker emits a protocol-capturing carrier ({@code GenerateTestDataChecker}). */
    GENERATE_TEST_DATA("generateTestData"),
    /** {@code generateSeedDataString(...)} &mdash; TDG lane S3 tail: runtime seed-text extraction, same carrier. */
    GENERATE_SEED_DATA_STRING("generateSeedDataString"),
    /** {@code planTestDataGeneration(...)} &mdash; the TDG PLAN: the same
     * protocol-capturing carrier (flavor {@code plan}); {@code planToString}
     * over it prints the engine's MultiResultSequence text. */
    PLAN_TEST_DATA_GENERATION("planTestDataGeneration"),
    /** {@code mayExecuteAlloyTest(serverThunk, |fallback)} &mdash; the engine
     * test-harness BRANCH native (core_functions_unclassified/test.pure:15):
     * no Alloy server exists on this platform, so the call IS its fallback
     * thunk &mdash; the branch the engine's own serverless CI takes
     * ({@code MayExecuteChecker}; walk parity: {@code alloyFallback}). */
    MAY_EXECUTE_ALLOY_TEST("mayExecuteAlloyTest"),
    /** {@code mayExecuteLegendTest(serverThunk, |fallback)} &mdash; the same
     * branch native for a live Legend server (test.pure:18). */
    MAY_EXECUTE_LEGEND_TEST("mayExecuteLegendTest"),
    /** {@code if(cond, |then, |else)} &mdash; thunk branches + branch-type join. */
    IF("if"),
    /** {@code ^Class(prop=value, &hellip;)} &mdash; instance construction ({@code new}). */
    NEW("new"),
    /** {@code #>{db.TABLE}#} &mdash; a physical table reference resolved through the store. */
    TABLE_REFERENCE("tableReference"),
    /** {@code tableToTDS(tableReference(...))} &mdash; the engine's Table&rarr;TDS wrapper; identity over the relation carrier. */
    TABLE_TO_TDS("tableToTDS"),
    /** {@code project} &mdash; builds a relation schema from projection lambdas / colspecs. */
    PROJECT("project"),
    /** Relation {@code sort(asc(~col), &hellip;)}; collection sort delegates to the generic path. */
    SORT("sort"),
    /** Relation {@code rename(~old, ~new)} &mdash; schema surgery {@code T-Z+V}. */
    RENAME("rename"),
    /** {@code filter(pred)} &mdash; relation + collection overloads; checked generically, own HIR node. */
    FILTER("filter"),
    /** {@code map(fn)} &mdash; collection map / relation row-map; checked generically, own HIR node. */
    MAP("map"),
    /** {@code asc(~col)} / {@code ascending(~col)} &mdash; a sort key; checked generically, own HIR node. */
    ASC("ascending", "asc"),
    /** {@code desc(~col)} / {@code descending(~col)} &mdash; a sort key; checked generically, own HIR node. */
    DESC("descending", "desc"),
    /** {@code sortInfo->emptyFirst()} &mdash; the sort key with nulls placed FIRST
     *  (upstream's {@code ^SortInfo(nullOrder = NullOrder.FIRST)}, 4.145.0). */
    EMPTY_FIRST("emptyFirst"),
    /** {@code sortInfo->emptyLast()} &mdash; the sort key with nulls placed LAST. */
    EMPTY_LAST("emptyLast"),
    /** Relation {@code select(~cols)} &mdash; column projection by name; {@code newTDSRelationAccessor} is its legacy alias. */
    SELECT("select", "newTDSRelationAccessor"),
    /** Relation {@code distinct} / {@code distinct(~[cols])} &mdash; row dedup, optionally narrowing. */
    DISTINCT("distinct"),
    /** Relation {@code concatenate} &mdash; UNION ALL of two same-schema relations. */
    CONCATENATE("concatenate"),
    /** {@code limit(n)} / {@code take(n)} &mdash; SQL LIMIT; relation + collection overloads. */
    LIMIT("limit"),
    /** Alias of {@link #LIMIT} with its own parse name. */
    TAKE("take"),
    /** {@code drop(n)} &mdash; SQL OFFSET; relation + collection overloads. */
    DROP("drop"),
    /** {@code slice(start, stop)} &mdash; SQL LIMIT+OFFSET over {@code [start, stop)}. */
    SLICE("slice"),
    /** Relation {@code extend(~newCol:x|…)} &mdash; adds computed columns ({@code T+Z}). */
    EXTEND("extend"),
    /** {@code groupBy(~keys, ~agg:map:reduce)} &mdash; grouped aggregation ({@code Z+R}); relation + class source. */
    GROUP_BY("groupBy", "groupByOverInstances", "groupByComputedKeys"),
    /** Legacy TDS {@code groupByWithWindowSubset(set, functions, aggValues,
     * ids, subSelectIds, subAggIds)} (tds.pure:867) &mdash; the store's rule
     * (pureToSQLQuery processObjectGroupByWithWindowSubSet) subsets the
     * functions and aggregates by id, then groups: a desugar to the 4-arg
     * legacy groupBy. */
    GROUP_BY_WITH_WINDOW_SUBSET("groupByWithWindowSubset"),
    /** Relation {@code aggregate(~agg:map:reduce)} &mdash; whole-relation aggregation ({@code Relation<R>}). */
    AGGREGATE("aggregate"),
    /** Relation {@code join(other, JoinKind.INNER, {t,v|cond})} &mdash; schema union {@code T+V}. */
    JOIN("join", "joinWithPrefix", "joinSlot"),
    /** Relation {@code asOfJoin(other, {t,v|match} [, {t,v|cond}])} &mdash; temporal join, {@code T+V}. */
    AS_OF_JOIN("asOfJoin", "asOfJoinWithPrefix"),
    /** {@code cast(@T)} &mdash; type conversion at the source's multiplicity ({@code T[m]}). */
    CAST("cast"),
    TYPE_AS_DECLARED("typeAsDeclared"),
    /** Mapping-side WIRE coercion — SQL cast at execution, bare in engine text. */
    CAST_AS_DECLARED("castAsDeclared"),
    /** {@code to(@T)} &mdash; nullable conversion ({@code T[0..1]}). */
    TO("to"),
    /** {@code toMany(@T)} &mdash; widening conversion ({@code T[*]}). */
    TO_MANY("toMany"),
    /** {@code match(value, [t:Type|…])} &mdash; compile-time static dispatch; result = the matched body's type. */
    MATCH("match"),
    /** {@code eval} &mdash; &beta;-reduction of a lambda / funcRef / ~col / function-typed variable. */
    EVAL("eval"),
    /** {@code #TDS …#} inline grid &mdash; a relation source with a header-parsed schema. */
    TDS("tds"),
    /** {@code sourceUrl('…')} &mdash; a semi-structured source, {@code (data:Variant)[1]}. */
    SOURCE_URL("sourceUrl"),
    /** Relation {@code flatten(~col)} &mdash; unnest; source schema with the column widened to Variant. */
    FLATTEN("flatten"),
    /** Relation {@code pivot(~cols, ~agg)} &mdash; static group columns + data-dependent pivoted columns. */
    PIVOT("pivot"),
    /** Relation {@code columns()} &mdash; static column METADATA folded at compile time. */
    COLUMNS("columns"),
    /** {@code toJSON(tds)} &mdash; a TDS serialized as the engine's TDS JSON
     * ({@code {"columns":[{name,type,metaType}],"rows":[{"values":[..]}]}}),
     * emitted by the database over the chain; any other argument rides the
     * generic native. */
    TO_JSON("toJSON"),
    /** {@code tdsToJSONKeyValueObjectString(tds)} (toJSON.pure:231) &mdash; the
     * TDS as a JSON array of row objects keyed by column name, emitted by
     * the database. */
    TDS_TO_JSON_KV("tdsToJSONKeyValueObjectString"),
    /** Collection {@code sortBy(key)} &mdash; ascending sort by a key lambda. */
    SORT_BY("sortBy"),
    /** Collection {@code sortByReversed(key)} &mdash; descending sort by a key lambda. */
    SORT_BY_REVERSED("sortByReversed"),
    /** {@code Class.all()} &mdash; the object-graph source anchor ({@code getAll<T>(Class<T>):T[*]}). */
    GET_ALL("getAll"),
    /** {@code Class.allVersions()} — the milestoned VERSION-sweep extent. */
    GET_ALL_FOR_EACH_DATE("getAllForEachDate"),

    GET_ALL_VERSIONS("getAllVersions"),
    /** {@code Class.allVersionsInRange(start, end)} — versions overlapping the range. */
    GET_ALL_VERSIONS_IN_RANGE("getAllVersionsInRange"),
    /** {@code from(mapping?, runtime)} &mdash; execution-context binding (type passthrough). */
    FROM("from"),
    /** Relation {@code write([target])} &mdash; persist; {@code Integer[1]} row count. */
    WRITE("write"),
    /** {@code fold({e,acc|…}, init)} &mdash; reduction, classified into a lowering strategy. */
    FOLD("fold"),
    /** {@code navigate} &mdash; the clean-sheet graph-traversal primitive (pre-map/post-map/inline). */
    NAVIGATE("navigate"),
    LEGACY_NAVIGATE("legacyNavigate"),
    /** {@code route(target, rows, cond)} &mdash; one route of a several-route legacyNavigate; loud on its own. */
    ROUTE("route"),
    /** {@code isDistinct(collection, #{Class{a, b}}#)} &mdash; no duplicates comparing by the tree's leaves (the 1-arg collection form types generically). */
    IS_DISTINCT("isDistinct"),
    /** {@code graphFetch(#{Class{…}}#)} &mdash; object-graph projection; result = source type. */
    GRAPH_FETCH("graphFetch"),
    /** {@code graphFetchChecked(#{Class{…}}#)} &mdash; checked projection; {@code Checked<T>[*]}. */
    GRAPH_FETCH_CHECKED("graphFetchChecked"),
    /** {@code serialize(#{Class{…}}#)} &mdash; graph serialization; {@code String[1]}. */
    SERIALIZE("serialize"),
    /** {@code over(~partition [, asc(~key)…])} &mdash; a window definition ({@code _Window<T>[1]}). */
    OVER("over");

    private static final Map<String, CoreFn> BY_NAME = new HashMap<>();

    /** Every parse-time name (aliases included) with its construct — the
     *  CoreFn CLAIMS ({@link com.legend.builtin.Claims}, kind CORE_FN): every
     *  catalog overload whose bare name is one of these is dispatched here. */
    public static Map<String, CoreFn> parseNames() {
        return java.util.Collections.unmodifiableMap(BY_NAME);
    }

    static {
        for (CoreFn fn : values()) {
            for (String name : fn.parseNames) {
                BY_NAME.put(name, fn);
            }
        }
    }

    private final String[] parseNames;

    CoreFn(String... parseNames) {
        this.parseNames = parseNames;
    }

    /**
     * THE FUNCTIONS EACH FORM OWNS, by exact FQN: every overload declared at
     * one of these FQNs is the form's (the implementation table's Form row).
     * Written out here, never derived from a bare name — the {@link #of}
     * bare-name and FQN-tail rules that still dispatch today are what this
     * list replaces (platform architecture untangle, step 2). {@code NEW} owns
     * no function: {@code ^Class(...)} is syntax.
     */
    private static final Map<CoreFn, java.util.Set<String>> OWNS = owns();

    private static Map<CoreFn, java.util.Set<String>> owns() {
        Map<CoreFn, java.util.Set<String>> m = new java.util.EnumMap<>(CoreFn.class);
        m.put(LET, java.util.Set.of(
                "meta::pure::functions::lang::letFunction"));
        m.put(DEACTIVATE, java.util.Set.of(
                "meta::pure::functions::meta::deactivate"));
        m.put(GET_RELATIONAL_CSV_DATA, java.util.Set.of(
                "meta::relational::testDataGeneration::getRelationalCSVDataFromQuery"));
        m.put(GENERATE_TEST_DATA, java.util.Set.of(
                "meta::relational::testDataGeneration::generateTestData"));
        m.put(GENERATE_SEED_DATA_STRING, java.util.Set.of(
                "meta::relational::testDataGeneration::generateSeedDataString"));
        m.put(PLAN_TEST_DATA_GENERATION, java.util.Set.of(
                "meta::relational::testDataGeneration::executionPlan::planTestDataGeneration"));
        m.put(MAY_EXECUTE_ALLOY_TEST, java.util.Set.of(
                "meta::alloy::test::mayExecuteAlloyTest"));
        m.put(MAY_EXECUTE_LEGEND_TEST, java.util.Set.of(
                "meta::legend::test::mayExecuteLegendTest"));
        m.put(IF, java.util.Set.of(
                "meta::pure::functions::lang::if"));
        m.put(TABLE_REFERENCE, java.util.Set.of(
                "meta::relational::functions::database::tableReference"));
        m.put(TABLE_TO_TDS, java.util.Set.of(
                "meta::pure::tds::tableToTDS"));
        m.put(PROJECT, java.util.Set.of(
                "meta::pure::functions::relation::project",
                "meta::pure::tds::project"));
        m.put(SORT, java.util.Set.of(
                "meta::pure::functions::collection::sort",
                "meta::pure::functions::relation::sort",
                "meta::pure::tds::sort"));
        m.put(RENAME, java.util.Set.of(
                "meta::pure::functions::relation::rename"));
        m.put(FILTER, java.util.Set.of(
                "meta::pure::functions::collection::filter",
                "meta::pure::functions::relation::filter",
                "meta::pure::tds::filter"));
        m.put(MAP, java.util.Set.of(
                "meta::pure::functions::collection::map",
                "meta::pure::functions::relation::map"));
        m.put(ASC, java.util.Set.of(
                "meta::pure::functions::relation::ascending",
                "meta::pure::tds::asc"));
        m.put(DESC, java.util.Set.of(
                "meta::pure::functions::relation::descending",
                "meta::pure::tds::desc"));
        m.put(EMPTY_FIRST, java.util.Set.of(
                "meta::pure::functions::relation::emptyFirst"));
        m.put(EMPTY_LAST, java.util.Set.of(
                "meta::pure::functions::relation::emptyLast"));
        m.put(SELECT, java.util.Set.of(
                "meta::pure::functions::relation::select",
                "meta::pure::metamodel::relation::newTDSRelationAccessor"));
        m.put(DISTINCT, java.util.Set.of(
                "meta::pure::functions::collection::distinct",
                "meta::pure::functions::relation::distinct",
                "meta::pure::tds::distinct"));
        m.put(CONCATENATE, java.util.Set.of(
                "meta::pure::functions::collection::concatenate",
                "meta::pure::functions::relation::concatenate"));
        m.put(LIMIT, java.util.Set.of(
                "meta::pure::functions::collection::limit",
                "meta::pure::functions::relation::limit",
                "meta::pure::tds::limit"));
        m.put(TAKE, java.util.Set.of(
                "meta::pure::functions::collection::take"));
        m.put(DROP, java.util.Set.of(
                "meta::pure::functions::collection::drop",
                "meta::pure::functions::relation::drop"));
        m.put(SLICE, java.util.Set.of(
                "meta::pure::functions::collection::slice",
                "meta::pure::functions::relation::slice"));
        m.put(EXTEND, java.util.Set.of(
                "meta::pure::functions::relation::extend",
                "meta::pure::tds::extend"));
        m.put(GROUP_BY, java.util.Set.of(
                "meta::legend::lite::groupByComputedKeys",
                "meta::legend::lite::groupByOverInstances",
                "meta::pure::functions::collection::groupBy",
                "meta::pure::functions::relation::groupBy",
                "meta::pure::tds::groupBy"));
        m.put(GROUP_BY_WITH_WINDOW_SUBSET, java.util.Set.of(
                "meta::pure::tds::groupByWithWindowSubset"));
        m.put(AGGREGATE, java.util.Set.of(
                "meta::pure::functions::relation::aggregate"));
        m.put(JOIN, java.util.Set.of(
                "meta::legend::lite::joinSlot",
                "meta::legend::lite::joinWithPrefix",
                "meta::pure::functions::relation::join"));
        m.put(AS_OF_JOIN, java.util.Set.of(
                "meta::legend::lite::asOfJoinWithPrefix",
                "meta::pure::functions::relation::asOfJoin"));
        m.put(CAST, java.util.Set.of(
                "meta::pure::functions::lang::cast"));
        m.put(TYPE_AS_DECLARED, java.util.Set.of(
                "meta::legend::lite::typeAsDeclared"));
        m.put(CAST_AS_DECLARED, java.util.Set.of(
                "meta::legend::lite::castAsDeclared"));
        m.put(TO, java.util.Set.of(
                "meta::pure::functions::variant::convert::to"));
        m.put(TO_MANY, java.util.Set.of(
                "meta::pure::functions::variant::convert::toMany"));
        m.put(MATCH, java.util.Set.of(
                "meta::pure::functions::lang::match"));
        m.put(EVAL, java.util.Set.of(
                "meta::pure::functions::lang::eval",
                "meta::pure::functions::relation::eval"));
        m.put(TDS, java.util.Set.of(
                "meta::legend::lite::tds"));
        m.put(SOURCE_URL, java.util.Set.of(
                "meta::legend::lite::sourceUrl"));
        m.put(FLATTEN, java.util.Set.of(
                "meta::pure::functions::relation::variant::flatten"));
        m.put(PIVOT, java.util.Set.of(
                "meta::pure::functions::relation::pivot"));
        m.put(COLUMNS, java.util.Set.of(
                "meta::pure::functions::relation::columns"));
        m.put(TO_JSON, java.util.Set.of(
                "meta::json::toJSON"));
        m.put(TDS_TO_JSON_KV, java.util.Set.of(
                "meta::json::tdsToJSONKeyValueObjectString"));
        m.put(SORT_BY, java.util.Set.of(
                "meta::pure::functions::collection::sortBy"));
        m.put(SORT_BY_REVERSED, java.util.Set.of(
                "meta::pure::functions::collection::sortByReversed"));
        m.put(GET_ALL, java.util.Set.of(
                "meta::pure::functions::collection::getAll"));
        m.put(GET_ALL_FOR_EACH_DATE, java.util.Set.of(
                "meta::pure::functions::collection::getAllForEachDate"));
        m.put(GET_ALL_VERSIONS, java.util.Set.of(
                "meta::pure::functions::collection::getAllVersions"));
        m.put(GET_ALL_VERSIONS_IN_RANGE, java.util.Set.of(
                "meta::pure::functions::collection::getAllVersionsInRange"));
        m.put(FROM, java.util.Set.of(
                "meta::pure::mapping::from"));
        m.put(WRITE, java.util.Set.of(
                "meta::pure::functions::relation::write"));
        m.put(FOLD, java.util.Set.of(
                "meta::pure::functions::collection::fold"));
        m.put(NAVIGATE, java.util.Set.of(
                "meta::legend::lite::navigate"));
        m.put(LEGACY_NAVIGATE, java.util.Set.of(
                "meta::legend::lite::legacyNavigate"));
        m.put(ROUTE, java.util.Set.of(
                "meta::legend::lite::route"));
        m.put(IS_DISTINCT, java.util.Set.of(
                "meta::pure::functions::collection::isDistinct"));
        m.put(GRAPH_FETCH, java.util.Set.of(
                "meta::pure::graphFetch::execution::graphFetch"));
        m.put(GRAPH_FETCH_CHECKED, java.util.Set.of(
                "meta::pure::graphFetch::execution::graphFetchChecked"));
        m.put(SERIALIZE, java.util.Set.of(
                "meta::pure::graphFetch::execution::serialize"));
        m.put(OVER, java.util.Set.of(
                "meta::pure::functions::relation::over"));
        return Map.copyOf(m);
    }

    /** The FQNs whose every overload this form owns (empty for {@code NEW}). */
    public java.util.Set<String> ownedFqns() {
        return OWNS.getOrDefault(this, java.util.Set.of());
    }

    /** The construct's canonical parse-time name (aliases like {@code ascending} resolve here too). */
    public String parseName() {
        return parseNames[0];
    }

    /**
     * The single string&rarr;construct resolution point: the parse-time name of an
     * applied function, to its core construct &mdash; or empty for a library call
     * (which rides the generic path).
     */
    public static Optional<CoreFn> of(String parseName) {
        CoreFn direct = BY_NAME.get(parseName);
        if (direct != null) {
            // INTERNAL-ONLY constructs (the lite desugar IR) dispatch
            // solely on their exact FQN spelling — a user-written bare
            // 'tds'/'otherwise'/... must fall through to generic
            // resolution, where the partitioned bare-name index refuses
            // it as an unknown function.
            if (com.legend.builtin.Pure.INTERNAL_DESUGAR.contains(parseName)) {
                return Optional.empty();
            }
            return Optional.of(direct);
        }
        // FQN-keyed catalog era (FQN_MIGRATION step 1): a platform-qualified
        // call dispatches to the same core checker as its bare spelling —
        // ONLY when the FQN is an actual CATALOG NATIVE (a USER function
        // living under meta::pure::* — perfectly legal — must not hijack a
        // checker; the pin that caught this: meta::pure::custom::map).
        if (parseName.contains("::")
                && !com.legend.builtin.Pure.nativeFunctionsAt(parseName).isEmpty()) {
            int sep = parseName.lastIndexOf("::");
            return Optional.ofNullable(BY_NAME.get(parseName.substring(sep + 2)));
        }
        // relation::eval(~col, $row) — the ColSpec accessor (real relation
        // eval.pure) — routes to the EVAL checker whose ColSpec shape-arm
        // desugars it to $row.col. A CURATED alias, not a registration: its
        // ⊆-colspec signature would pollute the shared bare-name 'eval'
        // overload set that variableEval resolves against (audit: five
        // higher-order corpus tests broke).
        // meta::pure::tds::distinct (engine tds.pure:471, TabularDataSet
        // surface) — CURATED alias for the same reason as relation::eval:
        // registering the FQN as a native TIES with relation::distinct in
        // the shared bare-name overload set (ambiguous-overload on every
        // plain ->distinct()).
        // meta::pure::tds::extend (engine tds.pure, the TabularDataSet
        // surface a TDS-typed receiver resolves the bare spelling to) —
        // CURATED alias for the same reason: the ExtendChecker owns the
        // legacy col() normalization; the generic path would type the
        // col() collection standalone (the TDG applied-functions witness).
        if (parseName.equals("meta::pure::tds::extend")) {
            return Optional.of(EXTEND);
        }
        if (parseName.equals("meta::pure::tds::distinct")) {
            return Optional.of(DISTINCT);
        }
        if (parseName.equals("meta::pure::functions::relation::eval")) {
            return Optional.of(EVAL);
        }
        return Optional.empty();
    }
}
