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
    /** {@code if(cond, |then, |else)} &mdash; thunk branches + branch-type join. */
    IF("if"),
    /** {@code ^Class(prop=value, &hellip;)} &mdash; instance construction ({@code new}). */
    NEW("new"),
    /** {@code #>{db.TABLE}#} &mdash; a physical table reference resolved through the store. */
    TABLE_REFERENCE("tableReference"),
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
    ASC("asc", "ascending"),
    /** {@code desc(~col)} / {@code descending(~col)} &mdash; a sort key; checked generically, own HIR node. */
    DESC("desc", "descending"),
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
    GROUP_BY("groupBy"),
    /** Relation {@code aggregate(~agg:map:reduce)} &mdash; whole-relation aggregation ({@code Relation<R>}). */
    AGGREGATE("aggregate"),
    /** Relation {@code join(other, JoinKind.INNER, {t,v|cond})} &mdash; schema union {@code T+V}. */
    JOIN("join"),
    /** Relation {@code asOfJoin(other, {t,v|match} [, {t,v|cond}])} &mdash; temporal join, {@code T+V}. */
    AS_OF_JOIN("asOfJoin"),
    /** {@code cast(@T)} &mdash; type conversion at the source's multiplicity ({@code T[m]}). */
    CAST("cast"),
    TYPE_AS_DECLARED("typeAsDeclared"),
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
    /** Collection {@code sortBy(key)} &mdash; ascending sort by a key lambda. */
    SORT_BY("sortBy"),
    /** Collection {@code sortByReversed(key)} &mdash; descending sort by a key lambda. */
    SORT_BY_REVERSED("sortByReversed"),
    /** {@code Class.all()} &mdash; the object-graph source anchor ({@code getAll<T>(Class<T>):T[*]}). */
    GET_ALL("getAll"),
    /** {@code Class.allVersions()} — the milestoned VERSION-sweep extent. */
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
    /** {@code graphFetch(#{Class{…}}#)} &mdash; object-graph projection; result = source type. */
    GRAPH_FETCH("graphFetch"),
    /** {@code serialize(#{Class{…}}#)} &mdash; graph serialization; {@code String[1]}. */
    SERIALIZE("serialize"),
    /** {@code over(~partition [, asc(~key)…])} &mdash; a window definition ({@code _Window<T>[1]}). */
    OVER("over");

    private static final Map<String, CoreFn> BY_NAME = new HashMap<>();

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
        if (parseName.equals("meta::pure::functions::relation::eval")) {
            return Optional.of(EVAL);
        }
        return Optional.empty();
    }
}
