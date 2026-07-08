// Ported from engine/com.gs.legend.compiler.Pure (auto-generated there from
// BuiltinRegistry.registerSignature calls). Kept verbatim except for:
//   - package + imports retargeted to core
//   - NativeFunctionDef -> NativeFunctionDefinition (core's parser record)
//   - signature(...) routes through ElementParser instead of engine's
//     hand-rolled PureNativeSignatureParser, eliminating the second parser
//     and giving us a single parse pipeline for user source AND stdlib.
//
// Naming scheme: <NAME>__<ARG1TYPE>_<ARG1MULT>__<ARG2TYPE>_<ARG2MULT>__...
// Multiplicity: [1]->1, [N]->N, [*]->MANY, [0..1]->0_1, [1..*]->1_MANY, [N..M]->N_M.
// Return type omitted (Pure overloads on args only).
//
// HAND-CURATED (the old "do not edit" header predated the port): signatures
// are kept verbatim to real legend-pure/legend-engine .pure sources where
// they exist; deliberately WEAKENED signatures carry LITE-WEAKENED markers
// (blocked on named kernel capabilities — no shape divergences remain); invented natives (tableReference, tds, legacyNavigate, ...) are
// commented individually. To add a native: add the signature, re-run tests
// (the golden catalog file will show the diff).
package com.legend.builtin;

import com.legend.parser.ElementParser;
import com.legend.parser.element.ClassDefinition;
import com.legend.parser.element.EnumDefinition;
import com.legend.parser.element.NativeFunctionDefinition;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Typed identifiers for every Pure native overload &mdash; the single source
 * of truth for Pure-name strings in the system. Every consumer (type checker,
 * checkers, binding tables, lowering) should reference natives by these
 * constants, not by string lookups.
 *
 * <p>Each constant is a {@link NativeFunctionDefinition} produced by routing
 * the signature through {@link ElementParser} at class-load time. Class init
 * fails loudly if any signature stops parsing &mdash; that is the
 * parse-coverage guarantee.
 *
 * <p>Constants are populated in declaration order; {@link #all()} returns the
 * full list for downstream consumers to ingest at bootstrap.
 */
public final class Pure {
    private Pure() {}

    // ================================================================
    // Built-in type FQNs.
    //
    // Single source of truth for the names of stdlib types Pure code
    // refers to without an explicit import. Same role as the
    // {@link NativeFunctionDefinition} constants below, restricted to
    // strings until {@code NativeClassDefinition} (planned follow-up)
    // lands and these get promoted to structured records.
    //
    // Consumers (NameResolver, TypeChecker, tests, etc.) should
    // reference these constants instead of hard-coding the FQN string.
    // ================================================================

    /** {@code meta::pure::metamodel::type::} &mdash; package for core
     *  primitives and {@code Any}, {@code Type}, etc. */
    public static final String TYPE_PKG = "meta::pure::metamodel::type";

    /** {@code meta::pure::metamodel::relation::} &mdash; package for
     *  {@link #RELATION}, {@link #COL_SPEC}, etc. */
    public static final String RELATION_PKG = "meta::pure::metamodel::relation";

    /** {@code meta::pure::metamodel::function::} &mdash; package for
     *  {@link #FUNCTION}. */
    public static final String FUNCTION_PKG = "meta::pure::metamodel::function";

    /** {@code meta::pure::functions::relation::} &mdash; package for
     *  relation-algebra helpers ({@link #WINDOW}, {@link #SORT_INFO}, ...). */
    public static final String RELATION_FUNCTIONS_PKG = "meta::pure::functions::relation";

    /** {@code meta::pure::functions::date::} &mdash; package for date-related
     *  enums ({@link #DURATION_UNIT}, {@link #MONTH}, ...) and helpers. */
    public static final String DATE_FUNCTIONS_PKG = "meta::pure::functions::date";

    /** {@code meta::pure::functions::hash::} &mdash; package for hash-related
     *  enums ({@link #HASH_TYPE}). */
    public static final String HASH_FUNCTIONS_PKG = "meta::pure::functions::hash";

    /** {@code meta::pure::functions::collection::} &mdash; package for collection
     *  helper carriers ({@link #LIST}, {@link #PAIR}). */
    public static final String COLLECTION_PKG = "meta::pure::functions::collection";

    /** {@code meta::pure::functions::math::mathUtility::} &mdash; package for math
     *  helper carriers ({@link #ROW_MAPPER}). */
    public static final String MATH_UTILITY_PKG = "meta::pure::functions::math::mathUtility";

    /** {@code meta::pure::metamodel::variant::} &mdash; package for {@link #VARIANT}. */
    public static final String VARIANT_PKG = "meta::pure::metamodel::variant";

    /** {@code meta::pure::graphFetch::} &mdash; package for graph-fetch
     *  tree carriers ({@link #ROOT_GRAPH_FETCH_TREE}). */
    public static final String GRAPH_FETCH_PKG = "meta::pure::graphFetch";

    /** {@code meta::relational::metamodel::} &mdash; package for relational-store
     *  built-ins ({@link #SORT_DIRECTION}). Distinct from
     *  {@link #RELATION_FUNCTIONS_PKG} which carries Pure-level relation
     *  algebra: this one lives under {@code meta::relational::} and is owned
     *  by the relational DSL. */
    public static final String RELATIONAL_PKG = "meta::relational::metamodel";

    // ================================================================
    // Native class catalog.
    //
    // Built-in types declared as parsed {@link ClassDefinition} records
    // (with {@code isNative=true}) so consumers can treat them uniformly
    // with user classes: same record type, same access patterns, same
    // {@link com.legend.context.ModelContext} lookups. Bodies are empty
    // for now &mdash; we only carry name + type parameters + superclass
    // hierarchy. Property bodies will land in a follow-up when the
    // type-checker needs them.
    //
    // Hierarchy mirrors the engine's M3 platform Pure declarations.
    //
    // Naming: the constants below are the records themselves
    // (e.g. {@link #INTEGER} is a {@link ClassDefinition}, not a string).
    // For the FQN string, call {@code .qualifiedName()}.
    // ================================================================

    /** Native classes in declaration order. Populated by {@link #nativeClass(String)}. */
    private static final List<ClassDefinition> ALL_CLASSES = new ArrayList<>();

    /** Snapshot of every native class declared by {@link Pure}, declaration order. */
    public static List<ClassDefinition> allNativeClasses() {
        return Collections.unmodifiableList(ALL_CLASSES);
    }

    /**
     * Parse one {@code native Class ...} declaration through
     * {@link ElementParser} and stash the resulting record.
     *
     * <p>Call sites contain real Pure source verbatim &mdash; the same
     * text that would appear in an engine {@code .pure} file. This keeps
     * the catalog visually identical to engine declarations and means
     * any copy-paste from engine sources just works.
     *
     * <p>Class-load fails loudly if {@code pureSource} is malformed, parses
     * to something other than a {@link ClassDefinition}, or comes back
     * with {@code isNative=false}.
     */
    private static ClassDefinition nativeClass(String pureSource) {
        var parsed = ElementParser.parse(pureSource);
        if (parsed.elements().size() != 1) {
            throw new IllegalStateException(
                    "expected exactly one element parsed from: " + pureSource
                            + " (got " + parsed.elements().size() + ")");
        }
        var el = parsed.elements().get(0);
        if (!(el instanceof ClassDefinition cls)) {
            throw new IllegalStateException(
                    "expected ClassDefinition but got " + el.getClass().getSimpleName()
                            + " from: " + pureSource);
        }
        if (!cls.isNative()) {
            throw new IllegalStateException(
                    "expected native class but parsed isNative=false from: " + pureSource);
        }
        ALL_CLASSES.add(cls);
        return cls;
    }

    // ---- Top of the hierarchy ----
    public static final ClassDefinition ANY  = nativeClass("native Class meta::pure::metamodel::type::Any {}");
    public static final ClassDefinition NIL  = nativeClass("native Class meta::pure::metamodel::type::Nil  extends meta::pure::metamodel::type::Any {}");
    public static final ClassDefinition TYPE = nativeClass("native Class meta::pure::metamodel::type::Type extends meta::pure::metamodel::type::Any {}");
    /** Real M3's element root (meta::pure::metamodel::ModelElement) — corpus fixtures pass these around. */
    public static final ClassDefinition MODEL_ELEMENT = nativeClass("native Class meta::pure::metamodel::ModelElement extends meta::pure::metamodel::type::Any {}");

    // ---- Numeric tower ----
    public static final ClassDefinition NUMBER  = nativeClass("native Class meta::pure::metamodel::type::Number  extends meta::pure::metamodel::type::Any {}");
    public static final ClassDefinition INTEGER = nativeClass("native Class meta::pure::metamodel::type::Integer extends meta::pure::metamodel::type::Number {}");
    public static final ClassDefinition FLOAT   = nativeClass("native Class meta::pure::metamodel::type::Float   extends meta::pure::metamodel::type::Number {}");
    public static final ClassDefinition DECIMAL = nativeClass("native Class meta::pure::metamodel::type::Decimal extends meta::pure::metamodel::type::Number {}");

    // ---- Other primitives ----
    public static final ClassDefinition STRING  = nativeClass("native Class meta::pure::metamodel::type::String  extends meta::pure::metamodel::type::Any {}");
    public static final ClassDefinition BOOLEAN = nativeClass("native Class meta::pure::metamodel::type::Boolean extends meta::pure::metamodel::type::Any {}");
    public static final ClassDefinition BYTE    = nativeClass("native Class meta::pure::metamodel::type::Byte    extends meta::pure::metamodel::type::Any {}");

    // ---- Date hierarchy ----
    public static final ClassDefinition DATE        = nativeClass("native Class meta::pure::metamodel::type::Date        extends meta::pure::metamodel::type::Any {}");
    public static final ClassDefinition STRICT_DATE = nativeClass("native Class meta::pure::metamodel::type::StrictDate  extends meta::pure::metamodel::type::Date {}");
    public static final ClassDefinition DATE_TIME   = nativeClass("native Class meta::pure::metamodel::type::DateTime    extends meta::pure::metamodel::type::Date {}");
    public static final ClassDefinition LATEST_DATE = nativeClass("native Class meta::pure::metamodel::type::LatestDate  extends meta::pure::metamodel::type::Date {}");
    public static final ClassDefinition STRICT_TIME = nativeClass("native Class meta::pure::metamodel::type::StrictTime  extends meta::pure::metamodel::type::Any {}");

    // ---- Relation algebra (parameterized) ----
    public static final ClassDefinition RELATION             = nativeClass("native Class meta::pure::metamodel::relation::Relation<T>         extends meta::pure::metamodel::type::Any {}");
    public static final ClassDefinition COL_SPEC             = nativeClass("native Class meta::pure::metamodel::relation::ColSpec<T>          extends meta::pure::metamodel::type::Any {}");
    public static final ClassDefinition COL_SPEC_ARRAY       = nativeClass("native Class meta::pure::metamodel::relation::ColSpecArray<T>     extends meta::pure::metamodel::type::Any {}");
    public static final ClassDefinition FUNC_COL_SPEC        = nativeClass("native Class meta::pure::metamodel::relation::FuncColSpec<F, R>   extends meta::pure::metamodel::type::Any {}");
    public static final ClassDefinition FUNC_COL_SPEC_ARRAY  = nativeClass("native Class meta::pure::metamodel::relation::FuncColSpecArray<F, R> extends meta::pure::metamodel::type::Any {}");
    public static final ClassDefinition AGG_COL_SPEC         = nativeClass("native Class meta::pure::metamodel::relation::AggColSpec<F, U, R> extends meta::pure::metamodel::type::Any {}");
    public static final ClassDefinition AGG_COL_SPEC_ARRAY   = nativeClass("native Class meta::pure::metamodel::relation::AggColSpecArray<F, U, R> extends meta::pure::metamodel::type::Any {}");

    // ---- Function carrier (parameterized over a function-type token) ----
    public static final ClassDefinition FUNCTION = nativeClass("native Class meta::pure::metamodel::function::Function<F> extends meta::pure::metamodel::type::Any {}");

    // ---- Metaclass ----
    // Pure exposes the metaclass as `Class<T>` (parameterized over the
    // class it describes); used by signatures like `getAll(Class<T>):T[*]`.
    public static final ClassDefinition CLASS = nativeClass("native Class meta::pure::metamodel::type::Class<T> extends meta::pure::metamodel::type::Type {}");

    // ---- Variant (semi-structured value carrier) ----
    public static final ClassDefinition VARIANT = nativeClass("native Class meta::pure::metamodel::variant::Variant extends meta::pure::metamodel::type::Any {}");

    // ---- Collection carriers ----
    public static final ClassDefinition LIST = nativeClass("native Class meta::pure::functions::collection::List<T>    extends meta::pure::metamodel::type::Any {}");
    public static final ClassDefinition PAIR = nativeClass("native Class meta::pure::functions::collection::Pair<U, V> extends meta::pure::metamodel::type::Any {}");

    // ---- Math helper carrier (rowwise correlation/covariance inputs) ----
    public static final ClassDefinition ROW_MAPPER = nativeClass("native Class meta::pure::functions::math::mathUtility::RowMapper<T, U> extends meta::pure::metamodel::type::Any {}");

    // ---- Graph-fetch tree carrier ----
    public static final ClassDefinition ROOT_GRAPH_FETCH_TREE =
            nativeClass("native Class meta::pure::graphFetch::RootGraphFetchTree<T> extends meta::pure::metamodel::type::Any {}");

    // ---- Relation-functions helpers ----
    public static final ClassDefinition WINDOW    = nativeClass("native Class meta::pure::functions::relation::_Window<T>   extends meta::pure::metamodel::type::Any {}");
    public static final ClassDefinition TRAVERSAL = nativeClass("native Class meta::pure::functions::relation::_Traversal   extends meta::pure::metamodel::type::Any {}");
    public static final ClassDefinition SORT_INFO = nativeClass("native Class meta::pure::functions::relation::SortInfo<T>  extends meta::pure::metamodel::type::Any {}");

    // ---- Window-frame hierarchy (mirrors engine's frame.pure / range.pure / rows.pure) ----
    public static final ClassDefinition FRAME                 = nativeClass("native Class meta::pure::functions::relation::Frame                extends meta::pure::metamodel::type::Any {}");
    public static final ClassDefinition FRAME_VALUE           = nativeClass("native Class meta::pure::functions::relation::FrameValue           extends meta::pure::metamodel::type::Any {}");
    public static final ClassDefinition UNBOUNDED_FRAME_VALUE = nativeClass("native Class meta::pure::functions::relation::UnboundedFrameValue  extends meta::pure::functions::relation::FrameValue {}");
    public static final ClassDefinition _RANGE                = nativeClass("native Class meta::pure::functions::relation::_Range               extends meta::pure::functions::relation::Frame {}");
    public static final ClassDefinition ROWS                  = nativeClass("native Class meta::pure::functions::relation::Rows                 extends meta::pure::functions::relation::Frame {}");

    // ================================================================
    // Native enum catalog.
    //
    // Engine declares several stdlib types as {@code Enum} rather than
    // {@code Class} (e.g. {@link #DURATION_UNIT}, {@link #JOIN_KIND}).
    // Modelled as parsed {@link EnumDefinition} records so they round-trip
    // through {@link ElementParser} the same way native classes do.
    //
    // Same naming convention as the class catalog: the constant is the
    // record itself (e.g. {@link #JOIN_KIND} is an {@link EnumDefinition}).
    // ================================================================

    /** Native enums in declaration order. Populated by {@link #nativeEnum(String)}. */
    private static final List<EnumDefinition> ALL_ENUMS = new ArrayList<>();

    /** Snapshot of every native enum declared by {@link Pure}, declaration order. */
    public static List<EnumDefinition> allNativeEnums() {
        return Collections.unmodifiableList(ALL_ENUMS);
    }

    /**
     * Parse one {@code Enum ...} declaration through {@link ElementParser}
     * and stash the resulting record.
     *
     * <p>Like {@link #nativeClass(String)}, call sites contain real Pure
     * source verbatim. Class-load fails loudly on any malformed declaration.
     */
    private static EnumDefinition nativeEnum(String pureSource) {
        var parsed = ElementParser.parse(pureSource);
        if (parsed.elements().size() != 1) {
            throw new IllegalStateException(
                    "expected exactly one element parsed from: " + pureSource
                            + " (got " + parsed.elements().size() + ")");
        }
        var el = parsed.elements().get(0);
        if (!(el instanceof EnumDefinition def)) {
            throw new IllegalStateException(
                    "expected EnumDefinition but got " + el.getClass().getSimpleName()
                            + " from: " + pureSource);
        }
        ALL_ENUMS.add(def);
        return def;
    }

    // ---- Date enums ----
    public static final EnumDefinition DURATION_UNIT = nativeEnum("""
            Enum meta::pure::functions::date::DurationUnit
            {
                YEARS, MONTHS, WEEKS, DAYS, HOURS, MINUTES,
                SECONDS, MILLISECONDS, MICROSECONDS, NANOSECONDS
            }
            """);

    public static final EnumDefinition MONTH = nativeEnum("""
            Enum meta::pure::functions::date::Month
            {
                January, February, March, April, May, June,
                July, August, September, October, November, December
            }
            """);

    public static final EnumDefinition QUARTER = nativeEnum("""
            Enum meta::pure::functions::date::Quarter
            {
                Q1, Q2, Q3, Q4
            }
            """);

    public static final EnumDefinition DAY_OF_WEEK = nativeEnum("""
            Enum meta::pure::functions::date::DayOfWeek
            {
                Monday, Tuesday, Wednesday, Thursday, Friday, Saturday, Sunday
            }
            """);

    // ---- Relation enums ----
    public static final EnumDefinition SORT_TYPE = nativeEnum(
            "Enum meta::pure::functions::relation::SortType { ASC, DESC }");

    public static final EnumDefinition JOIN_KIND = nativeEnum("""
            Enum meta::pure::functions::relation::JoinKind
            {
                LEFT, RIGHT, FULL, INNER
            }
            """);

    // ---- Hash enum ----
    public static final EnumDefinition HASH_TYPE = nativeEnum(
            "Enum meta::pure::functions::hash::HashType { MD5, SHA1, SHA256 }");

    // ---- Relational-store enum (lives under meta::relational, not meta::pure) ----
    public static final EnumDefinition SORT_DIRECTION = nativeEnum(
            "Enum meta::relational::metamodel::SortDirection { ASC, DESC }");

    // ================================================================
    // Native function catalog.
    // ================================================================

    /**
     * Definitions in declaration order &mdash; which is LOAD-BEARING (overload
     * selection keeps the FIRST best-scoring candidate on ties, so reordering
     * can change tie-breaks; the golden catalog file pins it) and NOT
     * constant name. Populated by {@link #signature(String)}.
     */
    private static final List<NativeFunctionDefinition> ALL = new ArrayList<>();

    /** Snapshot of every Pure native def, in (load-bearing) declaration order. */
    public static List<NativeFunctionDefinition> all() {
        return Collections.unmodifiableList(ALL);
    }

    // ====================================================================
    // Indexed lookup surface — the bootstrap catalog's query API.
    //
    // The catalog is fixed at class-load; the FQN indexes are built once,
    // lazily (the holder idiom guarantees every constant is registered
    // first). Consumers in BOTH phases — NameResolver's prelude (D) and
    // element compilation (F) — read these instead of building private
    // indexes of the same data.
    // ====================================================================

    private static final class Index {
        static final java.util.Map<String, ClassDefinition> CLASS_BY_FQN = new java.util.HashMap<>();
        static final java.util.Map<String, EnumDefinition> ENUM_BY_FQN = new java.util.HashMap<>();
        static final java.util.Map<String, List<NativeFunctionDefinition>> FN_BY_FQN = new java.util.HashMap<>();
        /** name -> overload signature keys; nativeNamed's O(1) surface (re-audit M5). */
        static final java.util.Map<String, java.util.Set<String>> KEYS_BY_NAME = new java.util.HashMap<>();

        static {
            for (ClassDefinition cd : ALL_CLASSES) {
                CLASS_BY_FQN.put(cd.qualifiedName(), cd);
            }
            for (EnumDefinition ed : ALL_ENUMS) {
                ENUM_BY_FQN.put(ed.qualifiedName(), ed);
            }
            for (NativeFunctionDefinition nfd : ALL) {
                FN_BY_FQN.computeIfAbsent(nfd.qualifiedName(), k -> new ArrayList<>()).add(nfd);
                KEYS_BY_NAME.computeIfAbsent(nfd.qualifiedName(), k -> new java.util.HashSet<>())
                        .add(nfd.signatureKey());
            }
        }
    }

    /** The native class registered at {@code fqn}, if any. */
    public static java.util.Optional<ClassDefinition> findNativeClass(String fqn) {
        return java.util.Optional.ofNullable(Index.CLASS_BY_FQN.get(fqn));
    }

    /** The native enumeration registered at {@code fqn}, if any. */
    public static java.util.Optional<EnumDefinition> findNativeEnum(String fqn) {
        return java.util.Optional.ofNullable(Index.ENUM_BY_FQN.get(fqn));
    }

    /** Every native overload registered at {@code fqn} (empty when none). */
    /**
     * Whether {@code signatureKey} identifies one of the native overloads
     * registered at {@code name} — the parser-node-free membership test for
     * identity-keyed consumers (AUDIT_2026_07 §1c).
     */
    /**
     * The signature KEYS of every native overload registered at {@code name}
     * — the parser-node-free registration surface for the lowering's rule
     * tables (AUDIT_2026_07 §1c: dispatch identity crosses as STRINGS).
     */
    public static List<String> nativeKeysAt(String name) {
        List<String> keys = new ArrayList<>();
        for (var f : nativeFunctionsAt(name)) {
            keys.add(f.signatureKey());
        }
        return keys;
    }

    /**
     * Signature keys of specific overloads the lowering must single out
     * (string CONCAT-plus; IN) — parser records stay behind this wall.
     */
    public static String keyPlusString() {
        return PLUS__STRING_1__STRING_1.signatureKey();
    }

    public static String keyIn() {
        return IN__ANY_1__ANY_MANY.signatureKey();
    }

    public static boolean nativeNamed(String name, String signatureKey) {
        return Index.KEYS_BY_NAME
                .getOrDefault(name, java.util.Set.of())
                .contains(signatureKey);
    }

    public static List<NativeFunctionDefinition> nativeFunctionsAt(String fqn) {
        return Index.FN_BY_FQN.getOrDefault(fqn, List.of());
    }

    /** All native class FQNs — the resolver's prelude / known-FQN universe. */
    public static java.util.Set<String> nativeClassFqns() {
        return Collections.unmodifiableSet(Index.CLASS_BY_FQN.keySet());
    }

    /** All native enumeration FQNs — the resolver's prelude / known-FQN universe. */
    public static java.util.Set<String> nativeEnumFqns() {
        return Collections.unmodifiableSet(Index.ENUM_BY_FQN.keySet());
    }

    /**
     * Parse a Pure native signature through {@link ElementParser} and stash
     * the resulting record. Class-load fails if the signature is malformed
     * or if {@code ElementParser} refuses any grammar form &mdash; that is
     * the comprehensive parse-coverage guarantee.
     */
    private static NativeFunctionDefinition signature(String pureSignature) {
        var parsed = ElementParser.parse(pureSignature);
        if (parsed.elements().size() != 1) {
            throw new IllegalStateException(
                    "expected exactly one element parsed from: " + pureSignature
                            + " (got " + parsed.elements().size() + ")");
        }
        var el = parsed.elements().get(0);
        if (!(el instanceof NativeFunctionDefinition def)) {
            throw new IllegalStateException(
                    "expected NativeFunctionDefinition but got " + el.getClass().getSimpleName()
                            + " from: " + pureSignature);
        }
        ALL.add(def);
        return def;
    }

    public static final NativeFunctionDefinition ABS__T_1 = signature("native function abs<T>(number:T[1]):T[1];");
    public static final NativeFunctionDefinition ACOS__NUMBER_1 = signature("native function acos(number:meta::pure::metamodel::type::Number[1]):meta::pure::metamodel::type::Float[1];");
    public static final NativeFunctionDefinition ADD__T_MANY__INTEGER_1__T_1 = signature("native function add<T>(set:T[*], index:meta::pure::metamodel::type::Integer[1], val:T[1]):T[*];");
    public static final NativeFunctionDefinition ADD__T_MANY__T_1 = signature("native function add<T>(set:T[*], val:T[1]):T[*];");
    public static final NativeFunctionDefinition ADJUST__DATE_1__INTEGER_1__DURATION_UNIT_1 = signature("native function adjust(d:meta::pure::metamodel::type::Date[1], amount:meta::pure::metamodel::type::Integer[1], unit:meta::pure::functions::date::DurationUnit[1]):meta::pure::metamodel::type::Date[1];");
    public static final NativeFunctionDefinition AGGREGATE__RELATION_1__AGG_COL_SPEC_1 = signature("native function aggregate<T,K,V,R>(r:meta::pure::metamodel::relation::Relation<T>[1], agg:meta::pure::metamodel::relation::AggColSpec<{T[1]->K[0..1]},{K[*]->V[0..1]},R>[1]):meta::pure::metamodel::relation::Relation<R>[1];");
    public static final NativeFunctionDefinition AGGREGATE__RELATION_1__AGG_COL_SPEC_ARRAY_1 = signature("native function aggregate<T,K,V,R>(r:meta::pure::metamodel::relation::Relation<T>[1], agg:meta::pure::metamodel::relation::AggColSpecArray<{T[1]->K[0..1]},{K[*]->V[0..1]},R>[1]):meta::pure::metamodel::relation::Relation<R>[1];");
    public static final NativeFunctionDefinition AND__BOOLEAN_1__BOOLEAN_1 = signature("native function and(left:meta::pure::metamodel::type::Boolean[1], right:meta::pure::metamodel::type::Boolean[1]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition AND__BOOLEAN_MANY = signature("native function and(bools:meta::pure::metamodel::type::Boolean[*]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition ASCENDING__COL_SPEC_1 = signature("native function ascending<T>(column:meta::pure::metamodel::relation::ColSpec<T>[1]):meta::pure::functions::relation::SortInfo<T>[1];");
    public static final NativeFunctionDefinition ASCII__STRING_1 = signature("native function ascii(str:meta::pure::metamodel::type::String[1]):meta::pure::metamodel::type::Integer[1];");
    public static final NativeFunctionDefinition ASC__COL_SPEC_1 = signature("native function asc<T>(column:meta::pure::metamodel::relation::ColSpec<T>[1]):meta::pure::functions::relation::SortInfo<T>[1];");
    public static final NativeFunctionDefinition ASIN__NUMBER_1 = signature("native function asin(number:meta::pure::metamodel::type::Number[1]):meta::pure::metamodel::type::Float[1];");
    public static final NativeFunctionDefinition AS_OF_JOIN__RELATION_1__RELATION_1__FUNCTION_1 = signature("native function asOfJoin<T,V>(rel1:meta::pure::metamodel::relation::Relation<T>[1], rel2:meta::pure::metamodel::relation::Relation<V>[1], match:meta::pure::metamodel::function::Function<{T[1],V[1]->meta::pure::metamodel::type::Boolean[1]}>[1]):meta::pure::metamodel::relation::Relation<T+V>[1];");
    public static final NativeFunctionDefinition AS_OF_JOIN__RELATION_1__RELATION_1__FUNCTION_1__FUNCTION_1 = signature("native function asOfJoin<T,V>(rel1:meta::pure::metamodel::relation::Relation<T>[1], rel2:meta::pure::metamodel::relation::Relation<V>[1], match:meta::pure::metamodel::function::Function<{T[1],V[1]->meta::pure::metamodel::type::Boolean[1]}>[1], join:meta::pure::metamodel::function::Function<{T[1],V[1]->meta::pure::metamodel::type::Boolean[1]}>[1]):meta::pure::metamodel::relation::Relation<T+V>[1];");
    public static final NativeFunctionDefinition AS_OF_JOIN__RELATION_1__RELATION_1__FUNCTION_1__FUNCTION_1__STRING_1 = signature("native function asOfJoin<T,V>(rel1:meta::pure::metamodel::relation::Relation<T>[1], rel2:meta::pure::metamodel::relation::Relation<V>[1], match:meta::pure::metamodel::function::Function<{T[1],V[1]->meta::pure::metamodel::type::Boolean[1]}>[1], join:meta::pure::metamodel::function::Function<{T[1],V[1]->meta::pure::metamodel::type::Boolean[1]}>[1], prefix:meta::pure::metamodel::type::String[1]):meta::pure::metamodel::relation::Relation<T+V>[1];");
    public static final NativeFunctionDefinition ATAN2__NUMBER_1__NUMBER_1 = signature("native function atan2(y:meta::pure::metamodel::type::Number[1], x:meta::pure::metamodel::type::Number[1]):meta::pure::metamodel::type::Float[1];");
    public static final NativeFunctionDefinition ATAN__NUMBER_1 = signature("native function atan(number:meta::pure::metamodel::type::Number[1]):meta::pure::metamodel::type::Float[1];");
    public static final NativeFunctionDefinition AT__T_MANY__INTEGER_1 = signature("native function at<T>(set:T[*], index:meta::pure::metamodel::type::Integer[1]):T[1];");
    public static final NativeFunctionDefinition AVERAGE_RANK = signature("native function averageRank():meta::pure::metamodel::type::Number[1];");
    public static final NativeFunctionDefinition AVERAGE__NUMBER_MANY = signature("native function average(numbers:meta::pure::metamodel::type::Number[*]):meta::pure::metamodel::type::Float[1];");
    // WINDOW FAMILY — VERIFIED per function 2026-07-08 against real checkouts;
    // now FULLY FAITHFUL (the LITE-SHAPED category is EMPTY): ranking and
    // slice are verbatim (core_functions_relation/relation/functions/
    // {ranking,slice}); the 4-arg colToAgg aggregates below (average,
    // stdDevPopulation — the ONLY aggregate window functions real pure has;
    // everything else windows via the agg-col spelling
    // ~c:{p,w,r|$r.col}:y|$y->sum()) are verbatim core_functions_standard/
    // math/aggregator. The old engine-lite 3-arg row-returning aggregate
    // forms were MADE UP (never in real pure, unlowerable, exercised only by
    // engine-lite-authored tests since rewritten) and are DELETED.
    // over(): verify the ⊆-constrained args + the String[*] overload in 4c.
    public static final NativeFunctionDefinition AVERAGE__RELATION_1__WINDOW_1__T_1__COL_SPEC_1 = signature("native function average<T>(partition:meta::pure::metamodel::relation::Relation<T>[1], window:meta::pure::functions::relation::_Window<T>[1], row:T[1], colToAgg:meta::pure::metamodel::relation::ColSpec<(?:meta::pure::metamodel::type::Number)⊆T>[1]):meta::pure::metamodel::type::Float[1];");
    public static final NativeFunctionDefinition AVG__NUMBER_MANY = signature("native function avg(numbers:meta::pure::metamodel::type::Number[*]):meta::pure::metamodel::type::Float[1];");
    public static final NativeFunctionDefinition BETWEEN__NUMBER_1__NUMBER_1__NUMBER_1 = signature("native function between(value:meta::pure::metamodel::type::Number[1], low:meta::pure::metamodel::type::Number[1], high:meta::pure::metamodel::type::Number[1]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition BIT_AND__INTEGER_1__INTEGER_1 = signature("native function bitAnd(left:meta::pure::metamodel::type::Integer[1], right:meta::pure::metamodel::type::Integer[1]):meta::pure::metamodel::type::Integer[1];");
    public static final NativeFunctionDefinition BIT_OR__INTEGER_1__INTEGER_1 = signature("native function bitOr(left:meta::pure::metamodel::type::Integer[1], right:meta::pure::metamodel::type::Integer[1]):meta::pure::metamodel::type::Integer[1];");
    public static final NativeFunctionDefinition BIT_SHIFT_LEFT__INTEGER_1__INTEGER_1 = signature("native function bitShiftLeft(value:meta::pure::metamodel::type::Integer[1], bits:meta::pure::metamodel::type::Integer[1]):meta::pure::metamodel::type::Integer[1];");
    public static final NativeFunctionDefinition BIT_SHIFT_RIGHT__INTEGER_1__INTEGER_1 = signature("native function bitShiftRight(value:meta::pure::metamodel::type::Integer[1], bits:meta::pure::metamodel::type::Integer[1]):meta::pure::metamodel::type::Integer[1];");
    public static final NativeFunctionDefinition BIT_XOR__INTEGER_1__INTEGER_1 = signature("native function bitXor(left:meta::pure::metamodel::type::Integer[1], right:meta::pure::metamodel::type::Integer[1]):meta::pure::metamodel::type::Integer[1];");
    public static final NativeFunctionDefinition CAST__ANY_m__T_1 = signature("native function cast<T|m>(source:meta::pure::metamodel::type::Any[m], type:T[1]):T[m];");
    public static final NativeFunctionDefinition CBRT__NUMBER_1 = signature("native function cbrt(number:meta::pure::metamodel::type::Number[1]):meta::pure::metamodel::type::Float[1];");
    public static final NativeFunctionDefinition CEILING__NUMBER_1 = signature("native function ceiling(number:meta::pure::metamodel::type::Number[1]):meta::pure::metamodel::type::Integer[1];");
    public static final NativeFunctionDefinition CHAR__INTEGER_1 = signature("native function char(code:meta::pure::metamodel::type::Integer[1]):meta::pure::metamodel::type::String[1];");
    public static final NativeFunctionDefinition COALESCE__ANY_0_1__ANY_0_1__ANY_1 = signature("native function coalesce(value:meta::pure::metamodel::type::Any[0..1], value2:meta::pure::metamodel::type::Any[0..1], defaultValue:meta::pure::metamodel::type::Any[1]):meta::pure::metamodel::type::Any[1];");
    public static final NativeFunctionDefinition COALESCE__ANY_0_1__ANY_1 = signature("native function coalesce(value:meta::pure::metamodel::type::Any[0..1], defaultValue:meta::pure::metamodel::type::Any[1]):meta::pure::metamodel::type::Any[1];");
    public static final NativeFunctionDefinition COALESCE__ANY_MANY = signature("native function coalesce(values:meta::pure::metamodel::type::Any[*]):meta::pure::metamodel::type::Any[0..1];");
    public static final NativeFunctionDefinition COMPARE__ANY_1__ANY_1 = signature("native function compare(left:meta::pure::metamodel::type::Any[1], right:meta::pure::metamodel::type::Any[1]):meta::pure::metamodel::type::Integer[1];");
    public static final NativeFunctionDefinition CONCATENATE__RELATION_1__RELATION_1 = signature("native function concatenate<T>(rel1:meta::pure::metamodel::relation::Relation<T>[1], rel2:meta::pure::metamodel::relation::Relation<T>[1]):meta::pure::metamodel::relation::Relation<T>[1];");
    public static final NativeFunctionDefinition CONCAT__STRING_MANY = signature("native function concat(strs:meta::pure::metamodel::type::String[*]):meta::pure::metamodel::type::String[1];");
    public static final NativeFunctionDefinition CONTAINS__ANY_MANY__ANY_1 = signature("native function contains(collection:meta::pure::metamodel::type::Any[*], val:meta::pure::metamodel::type::Any[1]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition CONTAINS__STRING_1__STRING_1 = signature("native function contains(source:meta::pure::metamodel::type::String[1], val:meta::pure::metamodel::type::String[1]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition CONTAINS__T_MANY__T_1__FUNCTION_1 = signature("native function contains<T>(collection:T[*], val:T[1], comparator:meta::pure::metamodel::function::Function<{T[1],T[1]->meta::pure::metamodel::type::Boolean[1]}>[1]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition CORR__NUMBER_MANY__NUMBER_MANY = signature("native function corr(x:meta::pure::metamodel::type::Number[*], y:meta::pure::metamodel::type::Number[*]):meta::pure::metamodel::type::Number[1];");
    public static final NativeFunctionDefinition CORR__ROW_MAPPER_MANY = signature("native function corr<T,U>(values:meta::pure::functions::math::mathUtility::RowMapper<T,U>[*]):meta::pure::metamodel::type::Number[0..1];");
    public static final NativeFunctionDefinition COSH__NUMBER_1 = signature("native function cosh(number:meta::pure::metamodel::type::Number[1]):meta::pure::metamodel::type::Float[1];");
    public static final NativeFunctionDefinition COS__NUMBER_1 = signature("native function cos(number:meta::pure::metamodel::type::Number[1]):meta::pure::metamodel::type::Float[1];");
    public static final NativeFunctionDefinition COT__NUMBER_1 = signature("native function cot(number:meta::pure::metamodel::type::Number[1]):meta::pure::metamodel::type::Float[1];");
    public static final NativeFunctionDefinition COUNT__T_MANY = signature("native function count<T>(values:T[*]):meta::pure::metamodel::type::Integer[1];");
    public static final NativeFunctionDefinition COVAR_POPULATION__NUMBER_MANY__NUMBER_MANY = signature("native function covarPopulation(x:meta::pure::metamodel::type::Number[*], y:meta::pure::metamodel::type::Number[*]):meta::pure::metamodel::type::Number[1];");
    public static final NativeFunctionDefinition COVAR_POPULATION__ROW_MAPPER_MANY = signature("native function covarPopulation<T,U>(values:meta::pure::functions::math::mathUtility::RowMapper<T,U>[*]):meta::pure::metamodel::type::Number[0..1];");
    public static final NativeFunctionDefinition COVAR_SAMPLE__NUMBER_MANY__NUMBER_MANY = signature("native function covarSample(x:meta::pure::metamodel::type::Number[*], y:meta::pure::metamodel::type::Number[*]):meta::pure::metamodel::type::Number[1];");
    public static final NativeFunctionDefinition COVAR_SAMPLE__ROW_MAPPER_MANY = signature("native function covarSample<T,U>(values:meta::pure::functions::math::mathUtility::RowMapper<T,U>[*]):meta::pure::metamodel::type::Number[0..1];");
    public static final NativeFunctionDefinition CUMULATIVE_DISTRIBUTION__RELATION_1__WINDOW_1__T_1 = signature("native function cumulativeDistribution<T>(rel:meta::pure::metamodel::relation::Relation<T>[1], w:meta::pure::functions::relation::_Window<T>[1], row:T[1]):meta::pure::metamodel::type::Float[1];");
    public static final NativeFunctionDefinition CURRENT_USER_ID = signature("native function currentUserId():meta::pure::metamodel::type::String[1];");
    public static final NativeFunctionDefinition DATE_DIFF__DATE_1__DATE_1__DURATION_UNIT_1 = signature("native function dateDiff(d1:meta::pure::metamodel::type::Date[1], d2:meta::pure::metamodel::type::Date[1], du:meta::pure::functions::date::DurationUnit[1]):meta::pure::metamodel::type::Integer[1];");
    public static final NativeFunctionDefinition DATE_PART__DATE_1 = signature("native function datePart(d:meta::pure::metamodel::type::Date[1]):meta::pure::metamodel::type::StrictDate[1];");
    public static final NativeFunctionDefinition DATE__INTEGER_1 = signature("native function date(year:meta::pure::metamodel::type::Integer[1]):meta::pure::metamodel::type::Date[1];");
    public static final NativeFunctionDefinition DATE__INTEGER_1__INTEGER_1 = signature("native function date(year:meta::pure::metamodel::type::Integer[1], month:meta::pure::metamodel::type::Integer[1]):meta::pure::metamodel::type::Date[1];");
    public static final NativeFunctionDefinition DATE__INTEGER_1__INTEGER_1__INTEGER_1 = signature("native function date(year:meta::pure::metamodel::type::Integer[1], month:meta::pure::metamodel::type::Integer[1], day:meta::pure::metamodel::type::Integer[1]):meta::pure::metamodel::type::StrictDate[1];");
    public static final NativeFunctionDefinition DATE__INTEGER_1__INTEGER_1__INTEGER_1__INTEGER_1 = signature("native function date(year:meta::pure::metamodel::type::Integer[1], month:meta::pure::metamodel::type::Integer[1], day:meta::pure::metamodel::type::Integer[1], hour:meta::pure::metamodel::type::Integer[1]):meta::pure::metamodel::type::DateTime[1];");
    public static final NativeFunctionDefinition DATE__INTEGER_1__INTEGER_1__INTEGER_1__INTEGER_1__INTEGER_1 = signature("native function date(year:meta::pure::metamodel::type::Integer[1], month:meta::pure::metamodel::type::Integer[1], day:meta::pure::metamodel::type::Integer[1], hour:meta::pure::metamodel::type::Integer[1], minute:meta::pure::metamodel::type::Integer[1]):meta::pure::metamodel::type::DateTime[1];");
    public static final NativeFunctionDefinition DATE__INTEGER_1__INTEGER_1__INTEGER_1__INTEGER_1__INTEGER_1__NUMBER_1 = signature("native function date(year:meta::pure::metamodel::type::Integer[1], month:meta::pure::metamodel::type::Integer[1], day:meta::pure::metamodel::type::Integer[1], hour:meta::pure::metamodel::type::Integer[1], minute:meta::pure::metamodel::type::Integer[1], second:meta::pure::metamodel::type::Number[1]):meta::pure::metamodel::type::DateTime[1];");
    public static final NativeFunctionDefinition DAY_OF_MONTH__DATE_1 = signature("native function dayOfMonth(d:meta::pure::metamodel::type::Date[1]):meta::pure::metamodel::type::Integer[1];");
    public static final NativeFunctionDefinition DAY_OF_WEEK_NUMBER__DATE_1 = signature("native function dayOfWeekNumber(d:meta::pure::metamodel::type::Date[1]):meta::pure::metamodel::type::Integer[1];");
    public static final NativeFunctionDefinition DAY_OF_WEEK__DATE_1 = signature("native function dayOfWeek(d:meta::pure::metamodel::type::Date[1]):meta::pure::functions::date::DayOfWeek[1];");
    public static final NativeFunctionDefinition DAY_OF_YEAR__DATE_1 = signature("native function dayOfYear(d:meta::pure::metamodel::type::Date[1]):meta::pure::metamodel::type::Integer[1];");
    public static final NativeFunctionDefinition DECODE_BASE64__STRING_1 = signature("native function decodeBase64(str:meta::pure::metamodel::type::String[1]):meta::pure::metamodel::type::String[1];");
    public static final NativeFunctionDefinition DENSE_RANK__RELATION_1__WINDOW_1__T_1 = signature("native function denseRank<T>(rel:meta::pure::metamodel::relation::Relation<T>[1], w:meta::pure::functions::relation::_Window<T>[1], row:T[1]):meta::pure::metamodel::type::Integer[1];");
    public static final NativeFunctionDefinition DESCENDING__COL_SPEC_1 = signature("native function descending<T>(column:meta::pure::metamodel::relation::ColSpec<T>[1]):meta::pure::functions::relation::SortInfo<T>[1];");
    public static final NativeFunctionDefinition DESC__COL_SPEC_1 = signature("native function desc<T>(column:meta::pure::metamodel::relation::ColSpec<T>[1]):meta::pure::functions::relation::SortInfo<T>[1];");
    public static final NativeFunctionDefinition DISTINCT__RELATION_1 = signature("native function distinct<T>(rel:meta::pure::metamodel::relation::Relation<T>[1]):meta::pure::metamodel::relation::Relation<T>[1];");
    public static final NativeFunctionDefinition DISTINCT__RELATION_1__COL_SPEC_ARRAY_1 = signature("native function distinct<X,T>(rel:meta::pure::metamodel::relation::Relation<T>[1], columns:meta::pure::metamodel::relation::ColSpecArray<X⊆T>[1]):meta::pure::metamodel::relation::Relation<X>[1];");
    public static final NativeFunctionDefinition DIVIDE_ROUND__NUMBER_1__NUMBER_1__INTEGER_1 = signature("native function divideRound(left:meta::pure::metamodel::type::Number[1], right:meta::pure::metamodel::type::Number[1], scale:meta::pure::metamodel::type::Integer[1]):meta::pure::metamodel::type::Float[1];");
    public static final NativeFunctionDefinition DIVIDE__NUMBER_1__NUMBER_1 = signature("native function divide(dividend:meta::pure::metamodel::type::Number[1], divisor:meta::pure::metamodel::type::Number[1]):meta::pure::metamodel::type::Float[1];");
    public static final NativeFunctionDefinition DIVIDE__NUMBER_1__NUMBER_1__INTEGER_1 = signature("native function divide(dividend:meta::pure::metamodel::type::Number[1], divisor:meta::pure::metamodel::type::Number[1], scale:meta::pure::metamodel::type::Integer[1]):meta::pure::metamodel::type::Decimal[1];");
    public static final NativeFunctionDefinition DROP__RELATION_1__INTEGER_1 = signature("native function drop<T>(rel:meta::pure::metamodel::relation::Relation<T>[1], size:meta::pure::metamodel::type::Integer[1]):meta::pure::metamodel::relation::Relation<T>[1];");
    public static final NativeFunctionDefinition DROP__T_MANY__INTEGER_1 = signature("native function drop<T>(set:T[*], count:meta::pure::metamodel::type::Integer[1]):T[*];");
    public static final NativeFunctionDefinition ENCODE_BASE64__STRING_1 = signature("native function encodeBase64(str:meta::pure::metamodel::type::String[1]):meta::pure::metamodel::type::String[1];");
    public static final NativeFunctionDefinition ENDS_WITH__STRING_1__STRING_1 = signature("native function endsWith(source:meta::pure::metamodel::type::String[1], val:meta::pure::metamodel::type::String[1]):meta::pure::metamodel::type::Boolean[1];");
    // VERIFIED vs real legend-pure grammar/functions/boolean/equality/equal.pure:
    // equal(left:Any[*], right:Any[*]):Boolean[1] — collection equality is part
    // of the contract (identity/primitive/collection/model-defined equality).
    public static final NativeFunctionDefinition EQUAL__ANY_MANY__ANY_MANY = signature("native function equal(left:meta::pure::metamodel::type::Any[*], right:meta::pure::metamodel::type::Any[*]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition EQ__ANY_1__ANY_1 = signature("native function eq(left:meta::pure::metamodel::type::Any[1], right:meta::pure::metamodel::type::Any[1]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition EVAL__FUNCTION_1 = signature("native function eval(func:meta::pure::metamodel::function::Function<meta::pure::metamodel::type::Any>[1]):meta::pure::metamodel::type::Any[*];");
    // LITE-WEAKENED vs real pure eval<T,V|m,n>(Function<{T[m]->V[n]}>[1], T[m]):V[n]
    // — returns Any[*] here; EvalChecker computes the real type bespoke.
    public static final NativeFunctionDefinition EVAL__FUNCTION_1__T_MANY = signature("native function eval<T>(func:meta::pure::metamodel::function::Function<meta::pure::metamodel::type::Any>[1], param:T[*]):meta::pure::metamodel::type::Any[*];");
    public static final NativeFunctionDefinition EXISTS__T_MANY__FUNCTION_1 = signature("native function exists<T>(value:T[*], func:meta::pure::metamodel::function::Function<{T[1]->meta::pure::metamodel::type::Boolean[1]}>[1]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition EXP__NUMBER_1 = signature("native function exp(exponent:meta::pure::metamodel::type::Number[1]):meta::pure::metamodel::type::Float[1];");
    public static final NativeFunctionDefinition EXTEND__C_MANY__FUNC_COL_SPEC_1 = signature("native function extend<C,Z>(cl:C[*], f:meta::pure::metamodel::relation::FuncColSpec<{C[1]->meta::pure::metamodel::type::Any[0..1]},Z>[1]):C[*];");
    public static final NativeFunctionDefinition EXTEND__C_MANY__FUNC_COL_SPEC_ARRAY_1 = signature("native function extend<C,Z>(cl:C[*], fs:meta::pure::metamodel::relation::FuncColSpecArray<{C[1]->meta::pure::metamodel::type::Any[*]},Z>[1]):C[*];");
    public static final NativeFunctionDefinition EXTEND__RELATION_1__AGG_COL_SPEC_1 = signature("native function extend<T,K,V,R>(r:meta::pure::metamodel::relation::Relation<T>[1], agg:meta::pure::metamodel::relation::AggColSpec<{T[1]->K[0..1]},{K[*]->V[0..1]},R>[1]):meta::pure::metamodel::relation::Relation<T+R>[1];");
    public static final NativeFunctionDefinition EXTEND__RELATION_1__AGG_COL_SPEC_ARRAY_1 = signature("native function extend<T,K,V,R>(r:meta::pure::metamodel::relation::Relation<T>[1], agg:meta::pure::metamodel::relation::AggColSpecArray<{T[1]->K[0..1]},{K[*]->V[0..1]},R>[1]):meta::pure::metamodel::relation::Relation<T+R>[1];");
    public static final NativeFunctionDefinition EXTEND__RELATION_1__FUNC_COL_SPEC_1 = signature("native function extend<T,Z>(r:meta::pure::metamodel::relation::Relation<T>[1], f:meta::pure::metamodel::relation::FuncColSpec<{T[1]->meta::pure::metamodel::type::Any[0..1]},Z>[1]):meta::pure::metamodel::relation::Relation<T+Z>[1];");
    public static final NativeFunctionDefinition EXTEND__RELATION_1__FUNC_COL_SPEC_ARRAY_1 = signature("native function extend<T,Z>(r:meta::pure::metamodel::relation::Relation<T>[1], fs:meta::pure::metamodel::relation::FuncColSpecArray<{T[1]->meta::pure::metamodel::type::Any[*]},Z>[1]):meta::pure::metamodel::relation::Relation<T+Z>[1];");
    public static final NativeFunctionDefinition EXTEND__RELATION_1__TRAVERSAL_1__FUNC_COL_SPEC_1 = signature("native function extend<S,T,Z>(r:meta::pure::metamodel::relation::Relation<S>[1], path:meta::pure::functions::relation::_Traversal[1], f:meta::pure::metamodel::relation::FuncColSpec<{S[1],T[1]->meta::pure::metamodel::type::Any[0..1]},Z>[1]):meta::pure::metamodel::relation::Relation<S+Z>[1];");
    public static final NativeFunctionDefinition EXTEND__RELATION_1__TRAVERSAL_1__FUNC_COL_SPEC_ARRAY_1 = signature("native function extend<S,T,Z>(r:meta::pure::metamodel::relation::Relation<S>[1], path:meta::pure::functions::relation::_Traversal[1], fs:meta::pure::metamodel::relation::FuncColSpecArray<{S[1],T[1]->meta::pure::metamodel::type::Any[*]},Z>[1]):meta::pure::metamodel::relation::Relation<S+Z>[1];");
    public static final NativeFunctionDefinition EXTEND__RELATION_1__WINDOW_1__AGG_COL_SPEC_1 = signature("native function extend<T,K,V,R>(r:meta::pure::metamodel::relation::Relation<T>[1], window:meta::pure::functions::relation::_Window<T>[1], agg:meta::pure::metamodel::relation::AggColSpec<{meta::pure::metamodel::relation::Relation<T>[1],meta::pure::functions::relation::_Window<T>[1],T[1]->K[0..1]},{K[*]->V[0..1]},R>[1]):meta::pure::metamodel::relation::Relation<T+R>[1];");
    public static final NativeFunctionDefinition EXTEND__RELATION_1__WINDOW_1__AGG_COL_SPEC_ARRAY_1 = signature("native function extend<T,K,V,R>(r:meta::pure::metamodel::relation::Relation<T>[1], window:meta::pure::functions::relation::_Window<T>[1], agg:meta::pure::metamodel::relation::AggColSpecArray<{meta::pure::metamodel::relation::Relation<T>[1],meta::pure::functions::relation::_Window<T>[1],T[1]->K[0..1]},{K[*]->V[0..1]},R>[1]):meta::pure::metamodel::relation::Relation<T+R>[1];");
    public static final NativeFunctionDefinition EXTEND__RELATION_1__WINDOW_1__FUNC_COL_SPEC_1 = signature("native function extend<T,Z,W,R>(r:meta::pure::metamodel::relation::Relation<T>[1], window:meta::pure::functions::relation::_Window<T>[1], f:meta::pure::metamodel::relation::FuncColSpec<{meta::pure::metamodel::relation::Relation<T>[1],meta::pure::functions::relation::_Window<T>[1],T[1]->meta::pure::metamodel::type::Any[0..1]},R>[1]):meta::pure::metamodel::relation::Relation<T+R>[1];");
    public static final NativeFunctionDefinition EXTEND__RELATION_1__WINDOW_1__FUNC_COL_SPEC_ARRAY_1 = signature("native function extend<T,Z,W,R>(r:meta::pure::metamodel::relation::Relation<T>[1], window:meta::pure::functions::relation::_Window<T>[1], f:meta::pure::metamodel::relation::FuncColSpecArray<{meta::pure::metamodel::relation::Relation<T>[1],meta::pure::functions::relation::_Window<T>[1],T[1]->meta::pure::metamodel::type::Any[*]},R>[1]):meta::pure::metamodel::relation::Relation<T+R>[1];");
    public static final NativeFunctionDefinition FILTER__RELATION_1__FUNCTION_1 = signature("native function filter<T>(rel:meta::pure::metamodel::relation::Relation<T>[1], f:meta::pure::metamodel::function::Function<{T[1]->meta::pure::metamodel::type::Boolean[1]}>[1]):meta::pure::metamodel::relation::Relation<T>[1];");
    public static final NativeFunctionDefinition FILTER__T_MANY__FUNCTION_1 = signature("native function filter<T>(value:T[*], func:meta::pure::metamodel::function::Function<{T[1]->meta::pure::metamodel::type::Boolean[1]}>[1]):T[*];");
    public static final NativeFunctionDefinition FIND__T_MANY__FUNCTION_1 = signature("native function find<T>(value:T[*], func:meta::pure::metamodel::function::Function<{T[1]->meta::pure::metamodel::type::Boolean[1]}>[1]):T[0..1];");
    public static final NativeFunctionDefinition FIRST_DAY_OF_MONTH__DATE_1 = signature("native function firstDayOfMonth(d:meta::pure::metamodel::type::Date[1]):meta::pure::metamodel::type::Date[1];");
    public static final NativeFunctionDefinition FIRST_DAY_OF_QUARTER__DATE_1 = signature("native function firstDayOfQuarter(d:meta::pure::metamodel::type::Date[1]):meta::pure::metamodel::type::StrictDate[1];");
    public static final NativeFunctionDefinition FIRST_DAY_OF_YEAR__DATE_1 = signature("native function firstDayOfYear(d:meta::pure::metamodel::type::Date[1]):meta::pure::metamodel::type::Date[1];");
    public static final NativeFunctionDefinition FIRST_HOUR_OF_DAY__DATE_1 = signature("native function firstHourOfDay(d:meta::pure::metamodel::type::Date[1]):meta::pure::metamodel::type::DateTime[1];");
    public static final NativeFunctionDefinition FIRST_MILLISECOND_OF_SECOND__DATE_1 = signature("native function firstMillisecondOfSecond(d:meta::pure::metamodel::type::Date[1]):meta::pure::metamodel::type::DateTime[1];");
    public static final NativeFunctionDefinition FIRST_MINUTE_OF_HOUR__DATE_1 = signature("native function firstMinuteOfHour(d:meta::pure::metamodel::type::Date[1]):meta::pure::metamodel::type::DateTime[1];");
    public static final NativeFunctionDefinition FIRST_SECOND_OF_MINUTE__DATE_1 = signature("native function firstSecondOfMinute(d:meta::pure::metamodel::type::Date[1]):meta::pure::metamodel::type::DateTime[1];");
    // Param names w:Relation, f:_Window are VERBATIM real pure
    // (core_functions_relation/relation/functions/slice/first.pure) — kept
    // faithful even though they read swapped.
    public static final NativeFunctionDefinition FIRST__RELATION_1__WINDOW_1__T_1 = signature("native function first<T>(w:meta::pure::metamodel::relation::Relation<T>[1], f:meta::pure::functions::relation::_Window<T>[1], r:T[1]):T[0..1];");
    public static final NativeFunctionDefinition FIRST__T_MANY = signature("native function first<T>(set:T[*]):T[0..1];");
    public static final NativeFunctionDefinition FIRST__T_MANY__INTEGER_1 = signature("native function first<T>(set:T[*], count:meta::pure::metamodel::type::Integer[1]):T[*];");
    public static final NativeFunctionDefinition FLATTEN__T_MANY__COL_SPEC_1 = signature("native function flatten<T,Z>(valueToFlatten:T[*], columnWithFlattenedValue:meta::pure::metamodel::relation::ColSpec<Z=(?:T)>[1]):meta::pure::metamodel::relation::Relation<Z>[1];");
    public static final NativeFunctionDefinition FLOOR__NUMBER_1 = signature("native function floor(number:meta::pure::metamodel::type::Number[1]):meta::pure::metamodel::type::Integer[1];");
    public static final NativeFunctionDefinition FOLD__T_MANY__FUNCTION_1__V_m = signature("native function fold<T,V|m>(source:T[*], lambda:meta::pure::metamodel::function::Function<{T[1],V[m]->V[m]}>[1], init:V[m]):V[m];");
    public static final NativeFunctionDefinition FORMAT__STRING_1__ANY_MANY = signature("native function format(format:meta::pure::metamodel::type::String[1], args:meta::pure::metamodel::type::Any[*]):meta::pure::metamodel::type::String[1];");
    public static final NativeFunctionDefinition FOR_ALL__T_MANY__FUNCTION_1 = signature("native function forAll<T>(value:T[*], func:meta::pure::metamodel::function::Function<{T[1]->meta::pure::metamodel::type::Boolean[1]}>[1]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition FROM_EPOCH_VALUE__INTEGER_1 = signature("native function fromEpochValue(epoch:meta::pure::metamodel::type::Integer[1]):meta::pure::metamodel::type::Date[1];");
    public static final NativeFunctionDefinition FROM_EPOCH_VALUE__INTEGER_1__DURATION_UNIT_1 = signature("native function fromEpochValue(epoch:meta::pure::metamodel::type::Integer[1], unit:meta::pure::functions::date::DurationUnit[1]):meta::pure::metamodel::type::Date[1];");
    public static final NativeFunctionDefinition FROM__RELATION_1 = signature("native function from<T>(source:meta::pure::metamodel::relation::Relation<T>[1]):meta::pure::metamodel::relation::Relation<T>[1];");
    public static final NativeFunctionDefinition FROM__RELATION_1__ANY_1 = signature("native function from<T>(source:meta::pure::metamodel::relation::Relation<T>[1], runtime:meta::pure::metamodel::type::Any[1]):meta::pure::metamodel::relation::Relation<T>[1];");
    public static final NativeFunctionDefinition FROM__T_MANY__ANY_1__ANY_1 = signature("native function from<T>(source:T[*], mapping:meta::pure::metamodel::type::Any[1], runtime:meta::pure::metamodel::type::Any[1]):T[*];");
    public static final NativeFunctionDefinition GENERATE_GUID = signature("native function generateGuid():meta::pure::metamodel::type::String[1];");
    public static final NativeFunctionDefinition GET_ALL__CLASS_1 = signature("native function getAll<T>(class:meta::pure::metamodel::type::Class<T>[1]):T[*];");
    public static final NativeFunctionDefinition GET_ALL__CLASS_1__DATE_1 = signature("native function getAll<T>(class:meta::pure::metamodel::type::Class<T>[1], date:meta::pure::metamodel::type::Date[1]):T[*];");
    public static final NativeFunctionDefinition GET_ALL__CLASS_1__DATE_1__DATE_1 = signature("native function getAll<T>(class:meta::pure::metamodel::type::Class<T>[1], from:meta::pure::metamodel::type::Date[1], to:meta::pure::metamodel::type::Date[1]):T[*];");
    public static final NativeFunctionDefinition GET__VARIANT_1__ANY_1 = signature("native function get(source:meta::pure::metamodel::variant::Variant[1], key:meta::pure::metamodel::type::Any[1]):meta::pure::metamodel::variant::Variant[0..1];");
    public static final NativeFunctionDefinition GRAPH_FETCH__T_MANY__COL_SPEC_1 = signature("native function graphFetch<T>(source:T[*], col:meta::pure::metamodel::relation::ColSpec<T>[1]):T[*];");
    public static final NativeFunctionDefinition GRAPH_FETCH__T_MANY__COL_SPEC_ARRAY_1 = signature("native function graphFetch<T>(source:T[*], cols:meta::pure::metamodel::relation::ColSpecArray<T>[1]):T[*];");
    public static final NativeFunctionDefinition GRAPH_FETCH__T_MANY__ROOT_GRAPH_FETCH_TREE_1 = signature("native function graphFetch<T>(source:T[*], tree:meta::pure::graphFetch::RootGraphFetchTree<T>[1]):T[*];");
    public static final NativeFunctionDefinition GRAPH_FETCH__T_MANY__ROOT_GRAPH_FETCH_TREE_1__INTEGER_1 = signature("native function graphFetch<T>(source:T[*], tree:meta::pure::graphFetch::RootGraphFetchTree<T>[1], batchSize:meta::pure::metamodel::type::Integer[1]):T[*];");
    public static final NativeFunctionDefinition GREATER_THAN_EQUAL__DATE_0_1__DATE_0_1 = signature("native function greaterThanEqual(left:meta::pure::metamodel::type::Date[0..1], right:meta::pure::metamodel::type::Date[0..1]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition GREATER_THAN_EQUAL__DATE_0_1__DATE_1 = signature("native function greaterThanEqual(left:meta::pure::metamodel::type::Date[0..1], right:meta::pure::metamodel::type::Date[1]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition GREATER_THAN_EQUAL__DATE_1__DATE_0_1 = signature("native function greaterThanEqual(left:meta::pure::metamodel::type::Date[1], right:meta::pure::metamodel::type::Date[0..1]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition GREATER_THAN_EQUAL__DATE_1__DATE_1 = signature("native function greaterThanEqual(left:meta::pure::metamodel::type::Date[1], right:meta::pure::metamodel::type::Date[1]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition GREATER_THAN_EQUAL__NUMBER_0_1__NUMBER_0_1 = signature("native function greaterThanEqual(left:meta::pure::metamodel::type::Number[0..1], right:meta::pure::metamodel::type::Number[0..1]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition GREATER_THAN_EQUAL__NUMBER_0_1__NUMBER_1 = signature("native function greaterThanEqual(left:meta::pure::metamodel::type::Number[0..1], right:meta::pure::metamodel::type::Number[1]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition GREATER_THAN_EQUAL__NUMBER_1__NUMBER_0_1 = signature("native function greaterThanEqual(left:meta::pure::metamodel::type::Number[1], right:meta::pure::metamodel::type::Number[0..1]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition GREATER_THAN_EQUAL__NUMBER_1__NUMBER_1 = signature("native function greaterThanEqual(left:meta::pure::metamodel::type::Number[1], right:meta::pure::metamodel::type::Number[1]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition GREATER_THAN_EQUAL__STRING_0_1__STRING_0_1 = signature("native function greaterThanEqual(left:meta::pure::metamodel::type::String[0..1], right:meta::pure::metamodel::type::String[0..1]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition GREATER_THAN_EQUAL__STRING_0_1__STRING_1 = signature("native function greaterThanEqual(left:meta::pure::metamodel::type::String[0..1], right:meta::pure::metamodel::type::String[1]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition GREATER_THAN_EQUAL__STRING_1__STRING_0_1 = signature("native function greaterThanEqual(left:meta::pure::metamodel::type::String[1], right:meta::pure::metamodel::type::String[0..1]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition GREATER_THAN_EQUAL__STRING_1__STRING_1 = signature("native function greaterThanEqual(left:meta::pure::metamodel::type::String[1], right:meta::pure::metamodel::type::String[1]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition GREATER_THAN__DATE_0_1__DATE_0_1 = signature("native function greaterThan(left:meta::pure::metamodel::type::Date[0..1], right:meta::pure::metamodel::type::Date[0..1]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition GREATER_THAN__DATE_0_1__DATE_1 = signature("native function greaterThan(left:meta::pure::metamodel::type::Date[0..1], right:meta::pure::metamodel::type::Date[1]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition GREATER_THAN__DATE_1__DATE_0_1 = signature("native function greaterThan(left:meta::pure::metamodel::type::Date[1], right:meta::pure::metamodel::type::Date[0..1]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition GREATER_THAN__DATE_1__DATE_1 = signature("native function greaterThan(left:meta::pure::metamodel::type::Date[1], right:meta::pure::metamodel::type::Date[1]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition GREATER_THAN__NUMBER_0_1__NUMBER_0_1 = signature("native function greaterThan(left:meta::pure::metamodel::type::Number[0..1], right:meta::pure::metamodel::type::Number[0..1]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition GREATER_THAN__NUMBER_0_1__NUMBER_1 = signature("native function greaterThan(left:meta::pure::metamodel::type::Number[0..1], right:meta::pure::metamodel::type::Number[1]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition GREATER_THAN__NUMBER_1__NUMBER_0_1 = signature("native function greaterThan(left:meta::pure::metamodel::type::Number[1], right:meta::pure::metamodel::type::Number[0..1]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition GREATER_THAN__NUMBER_1__NUMBER_1 = signature("native function greaterThan(left:meta::pure::metamodel::type::Number[1], right:meta::pure::metamodel::type::Number[1]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition GREATER_THAN__STRING_0_1__STRING_0_1 = signature("native function greaterThan(left:meta::pure::metamodel::type::String[0..1], right:meta::pure::metamodel::type::String[0..1]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition GREATER_THAN__STRING_0_1__STRING_1 = signature("native function greaterThan(left:meta::pure::metamodel::type::String[0..1], right:meta::pure::metamodel::type::String[1]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition GREATER_THAN__STRING_1__STRING_0_1 = signature("native function greaterThan(left:meta::pure::metamodel::type::String[1], right:meta::pure::metamodel::type::String[0..1]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition GREATER_THAN__STRING_1__STRING_1 = signature("native function greaterThan(left:meta::pure::metamodel::type::String[1], right:meta::pure::metamodel::type::String[1]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition GREATEST__ANY_MANY = signature("native function greatest(values:meta::pure::metamodel::type::Any[*]):meta::pure::metamodel::type::Any[0..1];");
    public static final NativeFunctionDefinition GROUP_BY__C_MANY__FUNC_COL_SPEC_ARRAY_1__AGG_COL_SPEC_1 = signature("native function groupBy<C,Z,K,V,R>(cl:C[*], keys:meta::pure::metamodel::relation::FuncColSpecArray<{C[1]->meta::pure::metamodel::type::Any[*]},Z>[1], aggs:meta::pure::metamodel::relation::AggColSpec<{C[1]->K[0..1]},{K[*]->V[0..1]},R>[1]):meta::pure::metamodel::relation::Relation<Z+R>[1];");
    public static final NativeFunctionDefinition GROUP_BY__C_MANY__FUNC_COL_SPEC_ARRAY_1__AGG_COL_SPEC_ARRAY_1 = signature("native function groupBy<C,Z,K,V,R>(cl:C[*], keys:meta::pure::metamodel::relation::FuncColSpecArray<{C[1]->meta::pure::metamodel::type::Any[*]},Z>[1], aggs:meta::pure::metamodel::relation::AggColSpecArray<{C[1]->K[0..1]},{K[*]->V[0..1]},R>[1]):meta::pure::metamodel::relation::Relation<Z+R>[1];");
    public static final NativeFunctionDefinition GROUP_BY__K_MANY__FUNCTION_MANY__ANY_MANY__STRING_MANY = signature("native function groupBy<K,V,U>(set:K[*], fns:meta::pure::metamodel::function::Function<{K[1]->meta::pure::metamodel::type::Any[*]}>[*], aggs:meta::pure::metamodel::type::Any[*], ids:meta::pure::metamodel::type::String[*]):meta::pure::metamodel::relation::Relation<K>[1];");
    public static final NativeFunctionDefinition GROUP_BY__RELATION_1__COL_SPEC_1__AGG_COL_SPEC_1 = signature("native function groupBy<T,Z,K,V,R>(r:meta::pure::metamodel::relation::Relation<T>[1], cols:meta::pure::metamodel::relation::ColSpec<Z⊆T>[1], agg:meta::pure::metamodel::relation::AggColSpec<{T[1]->K[0..1]},{K[*]->V[0..1]},R>[1]):meta::pure::metamodel::relation::Relation<Z+R>[1];");
    public static final NativeFunctionDefinition GROUP_BY__RELATION_1__COL_SPEC_1__AGG_COL_SPEC_ARRAY_1 = signature("native function groupBy<T,Z,K,V,R>(r:meta::pure::metamodel::relation::Relation<T>[1], cols:meta::pure::metamodel::relation::ColSpec<Z⊆T>[1], agg:meta::pure::metamodel::relation::AggColSpecArray<{T[1]->K[0..1]},{K[*]->V[0..1]},R>[1]):meta::pure::metamodel::relation::Relation<Z+R>[1];");
    public static final NativeFunctionDefinition GROUP_BY__RELATION_1__COL_SPEC_ARRAY_1__AGG_COL_SPEC_1 = signature("native function groupBy<T,Z,K,V,R>(r:meta::pure::metamodel::relation::Relation<T>[1], cols:meta::pure::metamodel::relation::ColSpecArray<Z⊆T>[1], agg:meta::pure::metamodel::relation::AggColSpec<{T[1]->K[0..1]},{K[*]->V[0..1]},R>[1]):meta::pure::metamodel::relation::Relation<Z+R>[1];");
    public static final NativeFunctionDefinition GROUP_BY__RELATION_1__COL_SPEC_ARRAY_1__AGG_COL_SPEC_ARRAY_1 = signature("native function groupBy<T,Z,K,V,R>(r:meta::pure::metamodel::relation::Relation<T>[1], cols:meta::pure::metamodel::relation::ColSpecArray<Z⊆T>[1], agg:meta::pure::metamodel::relation::AggColSpecArray<{T[1]->K[0..1]},{K[*]->V[0..1]},R>[1]):meta::pure::metamodel::relation::Relation<Z+R>[1];");
    public static final NativeFunctionDefinition HASH_CODE__ANY_MANY = signature("native function hashCode(val:meta::pure::metamodel::type::Any[*]):meta::pure::metamodel::type::Integer[1];");
    public static final NativeFunctionDefinition HASH__STRING_1 = signature("native function hash(str:meta::pure::metamodel::type::String[1]):meta::pure::metamodel::type::String[1];");
    public static final NativeFunctionDefinition HASH__STRING_1__HASH_TYPE_1 = signature("native function hash(str:meta::pure::metamodel::type::String[1], algorithm:meta::pure::functions::hash::HashType[1]):meta::pure::metamodel::type::String[1];");
    public static final NativeFunctionDefinition HAS_DAY__DATE_1 = signature("native function hasDay(d:meta::pure::metamodel::type::Date[1]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition HAS_HOUR__DATE_1 = signature("native function hasHour(d:meta::pure::metamodel::type::Date[1]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition HAS_MINUTE__DATE_1 = signature("native function hasMinute(d:meta::pure::metamodel::type::Date[1]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition HAS_MONTH__DATE_1 = signature("native function hasMonth(d:meta::pure::metamodel::type::Date[1]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition HAS_SECOND__DATE_1 = signature("native function hasSecond(d:meta::pure::metamodel::type::Date[1]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition HAS_SUBSECOND_WITH_AT_LEAST_PRECISION__DATE_1__INTEGER_1 = signature("native function hasSubsecondWithAtLeastPrecision(d:meta::pure::metamodel::type::Date[1], precision:meta::pure::metamodel::type::Integer[1]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition HAS_SUBSECOND__DATE_1 = signature("native function hasSubsecond(d:meta::pure::metamodel::type::Date[1]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition HEAD__T_MANY = signature("native function head<T>(set:T[*]):T[0..1];");
    public static final NativeFunctionDefinition HOUR__DATE_1 = signature("native function hour(d:meta::pure::metamodel::type::Date[1]):meta::pure::metamodel::type::Integer[1];");
    // Real legend-pure (essential/lang/flow/if.pure): if<T|m>(Boolean[1], {->T[m]}, {->T[m]}):T[m].
    // The multiplicity VARIABLE m is shared by both branches and the result, so the result multiplicity
    // is the branches' (engine-lite dropped m and returned [*]/forced [1] — the bug flagged in §4.2).
    public static final NativeFunctionDefinition IF__BOOLEAN_1__FUNCTION_1__FUNCTION_1 = signature("native function if<T|m>(test:meta::pure::metamodel::type::Boolean[1], then:meta::pure::metamodel::function::Function<{->T[m]}>[1], else:meta::pure::metamodel::function::Function<{->T[m]}>[1]):T[m];");
    public static final NativeFunctionDefinition INDEX_OF__STRING_1__STRING_1 = signature("native function indexOf(str:meta::pure::metamodel::type::String[1], toFind:meta::pure::metamodel::type::String[1]):meta::pure::metamodel::type::Integer[1];");
    public static final NativeFunctionDefinition INDEX_OF__STRING_1__STRING_1__INTEGER_1 = signature("native function indexOf(str:meta::pure::metamodel::type::String[1], toFind:meta::pure::metamodel::type::String[1], fromIndex:meta::pure::metamodel::type::Integer[1]):meta::pure::metamodel::type::Integer[1];");
    public static final NativeFunctionDefinition INDEX_OF__T_MANY__T_1 = signature("native function indexOf<T>(set:T[*], value:T[1]):meta::pure::metamodel::type::Integer[1];");
    public static final NativeFunctionDefinition INIT__T_MANY = signature("native function init<T>(set:T[*]):T[*];");
    public static final NativeFunctionDefinition INSTANCE_OF__ANY_1__TYPE_1 = signature("native function instanceOf(instance:meta::pure::metamodel::type::Any[1], type:meta::pure::metamodel::type::Type[1]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition IN__ANY_1__ANY_MANY = signature("native function in(value:meta::pure::metamodel::type::Any[1], collection:meta::pure::metamodel::type::Any[*]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition IS_AFTER_DAY__DATE_1__DATE_1 = signature("native function isAfterDay(d1:meta::pure::metamodel::type::Date[1], d2:meta::pure::metamodel::type::Date[1]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition IS_BEFORE_DAY__DATE_1__DATE_1 = signature("native function isBeforeDay(d1:meta::pure::metamodel::type::Date[1], d2:meta::pure::metamodel::type::Date[1]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition IS_DISTINCT__ANY_1__ANY_1 = signature("native function isDistinct(left:meta::pure::metamodel::type::Any[1], right:meta::pure::metamodel::type::Any[1]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition IS_EMPTY__T_MANY = signature("native function isEmpty<T>(value:T[*]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition IS_NOT_EMPTY__T_MANY = signature("native function isNotEmpty<T>(value:T[*]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition IS_NOT_NULL__T_1 = signature("native function isNotNull<T>(val:T[1]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition IS_NULL__T_1 = signature("native function isNull<T>(val:T[1]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition IS_ON_DAY__DATE_1__DATE_1 = signature("native function isOnDay(d1:meta::pure::metamodel::type::Date[1], d2:meta::pure::metamodel::type::Date[1]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition IS_ON_OR_AFTER_DAY__DATE_1__DATE_1 = signature("native function isOnOrAfterDay(d1:meta::pure::metamodel::type::Date[1], d2:meta::pure::metamodel::type::Date[1]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition IS_ON_OR_BEFORE_DAY__DATE_1__DATE_1 = signature("native function isOnOrBeforeDay(d1:meta::pure::metamodel::type::Date[1], d2:meta::pure::metamodel::type::Date[1]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition JARO_WINKLER_SIMILARITY__STRING_1__STRING_1 = signature("native function jaroWinklerSimilarity(s1:meta::pure::metamodel::type::String[1], s2:meta::pure::metamodel::type::String[1]):meta::pure::metamodel::type::Float[1];");
    public static final NativeFunctionDefinition JOIN_STRINGS__STRING_MANY__STRING_1 = signature("native function joinStrings(strings:meta::pure::metamodel::type::String[*], separator:meta::pure::metamodel::type::String[1]):meta::pure::metamodel::type::String[1];");
    public static final NativeFunctionDefinition JOIN_STRINGS__STRING_MANY__STRING_1__STRING_1__STRING_1 = signature("native function joinStrings(strings:meta::pure::metamodel::type::String[*], prefix:meta::pure::metamodel::type::String[1], separator:meta::pure::metamodel::type::String[1], suffix:meta::pure::metamodel::type::String[1]):meta::pure::metamodel::type::String[1];");
    public static final NativeFunctionDefinition JOIN__RELATION_1__RELATION_1__JOIN_KIND_1__FUNCTION_1 = signature("native function join<T,V>(rel1:meta::pure::metamodel::relation::Relation<T>[1], rel2:meta::pure::metamodel::relation::Relation<V>[1], joinKind:meta::pure::functions::relation::JoinKind[1], f:meta::pure::metamodel::function::Function<{T[1],V[1]->meta::pure::metamodel::type::Boolean[1]}>[1]):meta::pure::metamodel::relation::Relation<T+V>[1];");
    // join (ColSpec form, Relation<S> -> Relation<S+A>): chain-join used by
    // MappingNormalizer's relational synth. The ColSpec binds a sub-row alias
    // (~firm:) to a tableReference in its function1 body; the trailing lambda
    // is the join condition over (source-row, target-row). Defaults to LEFT.
    // This is the relational, same-store widening primitive; cross-class
    // widening uses `associate` on Class[*] above.
    public static final NativeFunctionDefinition JOIN__RELATION_1__COL_SPEC_1__FUNCTION_1 = signature("native function join<S,T,A>(rel:meta::pure::metamodel::relation::Relation<S>[1], binding:meta::pure::metamodel::relation::ColSpec<A>[1], cond:meta::pure::metamodel::function::Function<{S[1],T[1]->meta::pure::metamodel::type::Boolean[1]}>[1]):meta::pure::metamodel::relation::Relation<S+A>[1];");
    public static final NativeFunctionDefinition JOIN__RELATION_1__RELATION_1__JOIN_KIND_1__FUNCTION_1__STRING_1 = signature("native function join<T,V>(rel1:meta::pure::metamodel::relation::Relation<T>[1], rel2:meta::pure::metamodel::relation::Relation<V>[1], joinKind:meta::pure::functions::relation::JoinKind[1], f:meta::pure::metamodel::function::Function<{T[1],V[1]->meta::pure::metamodel::type::Boolean[1]}>[1], prefix:meta::pure::metamodel::type::String[1]):meta::pure::metamodel::relation::Relation<T+V>[1];");
    public static final NativeFunctionDefinition LAG__RELATION_1__T_1 = signature("native function lag<T>(w:meta::pure::metamodel::relation::Relation<T>[1], r:T[1]):T[0..1];");
    public static final NativeFunctionDefinition LAG__RELATION_1__T_1__INTEGER_1 = signature("native function lag<T>(w:meta::pure::metamodel::relation::Relation<T>[1], r:T[1], offset:meta::pure::metamodel::type::Integer[1]):T[0..1];");
    public static final NativeFunctionDefinition LAST__RELATION_1__WINDOW_1__T_1 = signature("native function last<T>(w:meta::pure::metamodel::relation::Relation<T>[1], f:meta::pure::functions::relation::_Window<T>[1], row:T[1]):T[0..1];");
    public static final NativeFunctionDefinition LAST__T_MANY = signature("native function last<T>(set:T[*]):T[0..1];");
    public static final NativeFunctionDefinition LEAD__RELATION_1__T_1 = signature("native function lead<T>(w:meta::pure::metamodel::relation::Relation<T>[1], r:T[1]):T[0..1];");
    public static final NativeFunctionDefinition LEAD__RELATION_1__T_1__INTEGER_1 = signature("native function lead<T>(w:meta::pure::metamodel::relation::Relation<T>[1], r:T[1], offset:meta::pure::metamodel::type::Integer[1]):T[0..1];");
    public static final NativeFunctionDefinition LEAST__ANY_MANY = signature("native function least(values:meta::pure::metamodel::type::Any[*]):meta::pure::metamodel::type::Any[0..1];");
    public static final NativeFunctionDefinition LEFT__STRING_1__INTEGER_1 = signature("native function left(str:meta::pure::metamodel::type::String[1], len:meta::pure::metamodel::type::Integer[1]):meta::pure::metamodel::type::String[1];");
    // legacyNavigate (pipeline step): structurally symmetric to clean-
    // sheet `navigate`. Widens the current row scope by adding a named
    // slot bound to an instance of the target class, materialized
    // through the target's mapping. The lambda takes two ROW references
    // (source-row from the current scope, target-main-table-row of the
    // slot class); using physical-column access in the lambda is what
    // makes the call "legacy" rather than clean. Emitted exclusively by
    // MappingNormalizer for class-typed Join PMs (single-hop final hop,
    // multi-hop final hop, OtherwiseEmbedded fallback). See
    // docs/MAPPING_LEGACY_TO_FUNCTION.md §2.1.
    // navigate — THE clean-sheet graph-traversal primitive (MAPPING_CLEAN_SHEET.md §3):
    // pre-map widens a Relation with a named class-typed sub-row (row-multiplying,
    // like join; the sub-row column itself is [1] per output row, §3.4); post-map
    // fills a DECLARED class property via an instance-space predicate; inline is the
    // constructor-slot form. The target extent rides the colspec as a zero-param thunk.
    public static final NativeFunctionDefinition NAVIGATE__RELATION_1__FUNC_COL_SPEC_1__FUNCTION_1 = signature("native function navigate<S,T,Z>(rel:meta::pure::metamodel::relation::Relation<S>[1], target:meta::pure::metamodel::relation::FuncColSpec<{->T[*]},Z>[1], pred:meta::pure::metamodel::function::Function<{S[1],T[1]->meta::pure::metamodel::type::Boolean[1]}>[1]):meta::pure::metamodel::relation::Relation<S+Z>[1];");
    public static final NativeFunctionDefinition NAVIGATE__C_MANY__FUNC_COL_SPEC_1__FUNCTION_1 = signature("native function navigate<C,T,Z>(cl:C[*], target:meta::pure::metamodel::relation::FuncColSpec<{->T[*]},Z>[1], pred:meta::pure::metamodel::function::Function<{C[1],T[1]->meta::pure::metamodel::type::Boolean[1]}>[1]):C[*];");
    public static final NativeFunctionDefinition NAVIGATE__T_MANY__FUNCTION_1 = signature("native function navigate<T>(target:T[*], pred:meta::pure::metamodel::function::Function<{T[1]->meta::pure::metamodel::type::Boolean[1]}>[1]):T[*];");
    public static final NativeFunctionDefinition LEGACY_NAVIGATE__RELATION_1__COL_SPEC_1__FUNCTION_1 = signature("native function legacyNavigate<S,T,A>(rel:meta::pure::metamodel::relation::Relation<S>[1], binding:meta::pure::metamodel::relation::ColSpec<A>[1], cond:meta::pure::metamodel::function::Function<{S[1],T[1]->meta::pure::metamodel::type::Boolean[1]}>[1]):meta::pure::metamodel::relation::Relation<S+A>[1];");
    // legacyAssocPredicate: row-extraction adapter for AssociationMapping
    // predicate function bodies. The outer function signature is
    // (A[1], B[1]) -> Boolean[1] (matching a clean AssociationMapping
    // predicate function); the adapter extracts the underlying main-
    // table rows of $a and $b and binds them to the lambda's two Row
    // parameters so the body can speak physical-column predicates.
    // Emitted exclusively by MappingNormalizer for Relational
    // AssociationMapping bodies. See docs/MAPPING_LEGACY_TO_FUNCTION.md §2.2.
    public static final NativeFunctionDefinition LEGACY_ASSOC_PREDICATE__A_1__B_1__FUNCTION_1 = signature("native function legacyAssocPredicate<A,B>(a:A[1], b:B[1], cond:meta::pure::metamodel::function::Function<{meta::pure::metamodel::type::Any[1],meta::pure::metamodel::type::Any[1]->meta::pure::metamodel::type::Boolean[1]}>[1]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition LENGTH__STRING_1 = signature("native function length(str:meta::pure::metamodel::type::String[1]):meta::pure::metamodel::type::Integer[1];");
    public static final NativeFunctionDefinition LESS_THAN_EQUAL__DATE_0_1__DATE_0_1 = signature("native function lessThanEqual(left:meta::pure::metamodel::type::Date[0..1], right:meta::pure::metamodel::type::Date[0..1]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition LESS_THAN_EQUAL__DATE_0_1__DATE_1 = signature("native function lessThanEqual(left:meta::pure::metamodel::type::Date[0..1], right:meta::pure::metamodel::type::Date[1]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition LESS_THAN_EQUAL__DATE_1__DATE_0_1 = signature("native function lessThanEqual(left:meta::pure::metamodel::type::Date[1], right:meta::pure::metamodel::type::Date[0..1]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition LESS_THAN_EQUAL__DATE_1__DATE_1 = signature("native function lessThanEqual(left:meta::pure::metamodel::type::Date[1], right:meta::pure::metamodel::type::Date[1]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition LESS_THAN_EQUAL__NUMBER_0_1__NUMBER_0_1 = signature("native function lessThanEqual(left:meta::pure::metamodel::type::Number[0..1], right:meta::pure::metamodel::type::Number[0..1]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition LESS_THAN_EQUAL__NUMBER_0_1__NUMBER_1 = signature("native function lessThanEqual(left:meta::pure::metamodel::type::Number[0..1], right:meta::pure::metamodel::type::Number[1]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition LESS_THAN_EQUAL__NUMBER_1__NUMBER_0_1 = signature("native function lessThanEqual(left:meta::pure::metamodel::type::Number[1], right:meta::pure::metamodel::type::Number[0..1]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition LESS_THAN_EQUAL__NUMBER_1__NUMBER_1 = signature("native function lessThanEqual(left:meta::pure::metamodel::type::Number[1], right:meta::pure::metamodel::type::Number[1]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition LESS_THAN_EQUAL__STRING_0_1__STRING_0_1 = signature("native function lessThanEqual(left:meta::pure::metamodel::type::String[0..1], right:meta::pure::metamodel::type::String[0..1]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition LESS_THAN_EQUAL__STRING_0_1__STRING_1 = signature("native function lessThanEqual(left:meta::pure::metamodel::type::String[0..1], right:meta::pure::metamodel::type::String[1]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition LESS_THAN_EQUAL__STRING_1__STRING_0_1 = signature("native function lessThanEqual(left:meta::pure::metamodel::type::String[1], right:meta::pure::metamodel::type::String[0..1]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition LESS_THAN_EQUAL__STRING_1__STRING_1 = signature("native function lessThanEqual(left:meta::pure::metamodel::type::String[1], right:meta::pure::metamodel::type::String[1]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition LESS_THAN__DATE_0_1__DATE_0_1 = signature("native function lessThan(left:meta::pure::metamodel::type::Date[0..1], right:meta::pure::metamodel::type::Date[0..1]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition LESS_THAN__DATE_0_1__DATE_1 = signature("native function lessThan(left:meta::pure::metamodel::type::Date[0..1], right:meta::pure::metamodel::type::Date[1]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition LESS_THAN__DATE_1__DATE_0_1 = signature("native function lessThan(left:meta::pure::metamodel::type::Date[1], right:meta::pure::metamodel::type::Date[0..1]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition LESS_THAN__DATE_1__DATE_1 = signature("native function lessThan(left:meta::pure::metamodel::type::Date[1], right:meta::pure::metamodel::type::Date[1]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition LESS_THAN__NUMBER_0_1__NUMBER_0_1 = signature("native function lessThan(left:meta::pure::metamodel::type::Number[0..1], right:meta::pure::metamodel::type::Number[0..1]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition LESS_THAN__NUMBER_0_1__NUMBER_1 = signature("native function lessThan(left:meta::pure::metamodel::type::Number[0..1], right:meta::pure::metamodel::type::Number[1]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition LESS_THAN__NUMBER_1__NUMBER_0_1 = signature("native function lessThan(left:meta::pure::metamodel::type::Number[1], right:meta::pure::metamodel::type::Number[0..1]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition LESS_THAN__NUMBER_1__NUMBER_1 = signature("native function lessThan(left:meta::pure::metamodel::type::Number[1], right:meta::pure::metamodel::type::Number[1]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition LESS_THAN__STRING_0_1__STRING_0_1 = signature("native function lessThan(left:meta::pure::metamodel::type::String[0..1], right:meta::pure::metamodel::type::String[0..1]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition LESS_THAN__STRING_0_1__STRING_1 = signature("native function lessThan(left:meta::pure::metamodel::type::String[0..1], right:meta::pure::metamodel::type::String[1]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition LESS_THAN__STRING_1__STRING_0_1 = signature("native function lessThan(left:meta::pure::metamodel::type::String[1], right:meta::pure::metamodel::type::String[0..1]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition LESS_THAN__STRING_1__STRING_1 = signature("native function lessThan(left:meta::pure::metamodel::type::String[1], right:meta::pure::metamodel::type::String[1]):meta::pure::metamodel::type::Boolean[1];");
    // Real legend-pure: letFunction(String[1], T[m]):T[m] (mangled letFunction_String_1__T_m__T_m_) —
    // the multiplicity VARIABLE m is what makes a binding preserve its value's multiplicity through the
    // standard resolve→unify→resolveOutput pipeline (multi-valued let, `let xs = [1,2,3]`). engine-lite
    // flattened m→[1], which broke that and forced a bespoke checker; the mult var restores correctness.
    public static final NativeFunctionDefinition LET_FUNCTION__STRING_1__T_m = signature("native function letFunction<T|m>(name:meta::pure::metamodel::type::String[1], value:T[m]):T[m];");
    public static final NativeFunctionDefinition LEVENSHTEIN_DISTANCE__STRING_1__STRING_1 = signature("native function levenshteinDistance(s1:meta::pure::metamodel::type::String[1], s2:meta::pure::metamodel::type::String[1]):meta::pure::metamodel::type::Integer[1];");
    public static final NativeFunctionDefinition LIMIT__RELATION_1__INTEGER_1 = signature("native function limit<T>(rel:meta::pure::metamodel::relation::Relation<T>[1], size:meta::pure::metamodel::type::Integer[1]):meta::pure::metamodel::relation::Relation<T>[1];");
    public static final NativeFunctionDefinition LIMIT__T_MANY__INTEGER_1 = signature("native function limit<T>(set:T[*], size:meta::pure::metamodel::type::Integer[1]):T[*];");
    public static final NativeFunctionDefinition LIST__T_MANY = signature("native function list<T>(values:T[*]):meta::pure::functions::collection::List<T>[1];");
    public static final NativeFunctionDefinition LOG10__NUMBER_1 = signature("native function log10(value:meta::pure::metamodel::type::Number[1]):meta::pure::metamodel::type::Float[1];");
    public static final NativeFunctionDefinition LOG__NUMBER_1 = signature("native function log(value:meta::pure::metamodel::type::Number[1]):meta::pure::metamodel::type::Float[1];");
    public static final NativeFunctionDefinition LPAD__STRING_1__INTEGER_1 = signature("native function lpad(str:meta::pure::metamodel::type::String[1], len:meta::pure::metamodel::type::Integer[1]):meta::pure::metamodel::type::String[1];");
    public static final NativeFunctionDefinition LPAD__STRING_1__INTEGER_1__STRING_1 = signature("native function lpad(str:meta::pure::metamodel::type::String[1], len:meta::pure::metamodel::type::Integer[1], pad:meta::pure::metamodel::type::String[1]):meta::pure::metamodel::type::String[1];");
    public static final NativeFunctionDefinition LTRIM__STRING_1 = signature("native function ltrim(str:meta::pure::metamodel::type::String[1]):meta::pure::metamodel::type::String[1];");
    public static final NativeFunctionDefinition MAP__T_0_1__FUNCTION_1 = signature("native function map<T,V>(value:T[0..1], func:meta::pure::metamodel::function::Function<{T[1]->V[0..1]}>[1]):V[0..1];");
    public static final NativeFunctionDefinition MAP__T_MANY__FUNCTION_1 = signature("native function map<T,V>(value:T[*], func:meta::pure::metamodel::function::Function<{T[1]->V[*]}>[1]):V[*];");
    public static final NativeFunctionDefinition MATCHES__STRING_1__STRING_1 = signature("native function matches(str:meta::pure::metamodel::type::String[1], regex:meta::pure::metamodel::type::String[1]):meta::pure::metamodel::type::Boolean[1];");
    // LITE-WEAKENED vs real pure match<T,R>(...):R[*] — returns Any[*] here;
    // MatchChecker computes the union type bespoke.
    public static final NativeFunctionDefinition MATCH__ANY_MANY__FUNCTION_1_MANY = signature("native function match(value:meta::pure::metamodel::type::Any[*], branches:meta::pure::metamodel::function::Function<meta::pure::metamodel::type::Any>[1..*]):meta::pure::metamodel::type::Any[*];");
    public static final NativeFunctionDefinition MATCH__ANY_MANY__FUNCTION_1_MANY__P_MANY = signature("native function match<P>(value:meta::pure::metamodel::type::Any[*], branches:meta::pure::metamodel::function::Function<meta::pure::metamodel::type::Any>[1..*], extra:P[*]):meta::pure::metamodel::type::Any[*];");
    public static final NativeFunctionDefinition MAX_BY__ROW_MAPPER_MANY = signature("native function maxBy<T,U>(values:meta::pure::functions::math::mathUtility::RowMapper<T,U>[*]):T[0..1];");
    public static final NativeFunctionDefinition MAX_BY__T_MANY__FUNCTION_1 = signature("native function maxBy<T>(values:T[*], key:meta::pure::metamodel::function::Function<{T[1]->meta::pure::metamodel::type::Any[1]}>[1]):T[0..1];");
    public static final NativeFunctionDefinition MAX_BY__T_MANY__FUNCTION_1__INTEGER_1 = signature("native function maxBy<T>(values:T[*], key:meta::pure::metamodel::function::Function<{T[1]->meta::pure::metamodel::type::Any[1]}>[1], count:meta::pure::metamodel::type::Integer[1]):T[*];");
    public static final NativeFunctionDefinition MAX_BY__T_MANY__T_MANY = signature("native function maxBy<T>(values:T[*], keys:T[*]):T[0..1];");
    public static final NativeFunctionDefinition MAX_BY__T_MANY__T_MANY__INTEGER_1 = signature("native function maxBy<T>(values:T[*], keys:T[*], count:meta::pure::metamodel::type::Integer[1]):T[*];");
    public static final NativeFunctionDefinition MAX_DATE__DATE_1__DATE_1 = signature("native function maxDate(d1:meta::pure::metamodel::type::Date[1], d2:meta::pure::metamodel::type::Date[1]):meta::pure::metamodel::type::Date[1];");
    public static final NativeFunctionDefinition MAX__DATE_1__DATE_1 = signature("native function max(left:meta::pure::metamodel::type::Date[1], right:meta::pure::metamodel::type::Date[1]):meta::pure::metamodel::type::Date[1];");
    public static final NativeFunctionDefinition MAX__DATE_MANY = signature("native function max(dates:meta::pure::metamodel::type::Date[*]):meta::pure::metamodel::type::Date[0..1];");
    public static final NativeFunctionDefinition MAX__DATE_TIME_1__DATE_TIME_1 = signature("native function max(left:meta::pure::metamodel::type::DateTime[1], right:meta::pure::metamodel::type::DateTime[1]):meta::pure::metamodel::type::DateTime[1];");
    public static final NativeFunctionDefinition MAX__DATE_TIME_MANY = signature("native function max(dates:meta::pure::metamodel::type::DateTime[*]):meta::pure::metamodel::type::DateTime[0..1];");
    public static final NativeFunctionDefinition MAX__FLOAT_1__FLOAT_1 = signature("native function max(left:meta::pure::metamodel::type::Float[1], right:meta::pure::metamodel::type::Float[1]):meta::pure::metamodel::type::Float[1];");
    public static final NativeFunctionDefinition MAX__FLOAT_MANY = signature("native function max(values:meta::pure::metamodel::type::Float[*]):meta::pure::metamodel::type::Float[0..1];");
    public static final NativeFunctionDefinition MAX__INTEGER_1__INTEGER_1 = signature("native function max(left:meta::pure::metamodel::type::Integer[1], right:meta::pure::metamodel::type::Integer[1]):meta::pure::metamodel::type::Integer[1];");
    public static final NativeFunctionDefinition MAX__INTEGER_MANY = signature("native function max(values:meta::pure::metamodel::type::Integer[*]):meta::pure::metamodel::type::Integer[0..1];");
    public static final NativeFunctionDefinition MAX__NUMBER_1__NUMBER_1 = signature("native function max(left:meta::pure::metamodel::type::Number[1], right:meta::pure::metamodel::type::Number[1]):meta::pure::metamodel::type::Number[1];");
    public static final NativeFunctionDefinition MAX__NUMBER_MANY = signature("native function max(numbers:meta::pure::metamodel::type::Number[*]):meta::pure::metamodel::type::Number[0..1];");
    public static final NativeFunctionDefinition MAX__STRICT_DATE_1__STRICT_DATE_1 = signature("native function max(left:meta::pure::metamodel::type::StrictDate[1], right:meta::pure::metamodel::type::StrictDate[1]):meta::pure::metamodel::type::StrictDate[1];");
    public static final NativeFunctionDefinition MAX__STRICT_DATE_MANY = signature("native function max(dates:meta::pure::metamodel::type::StrictDate[*]):meta::pure::metamodel::type::StrictDate[0..1];");
    public static final NativeFunctionDefinition MD5__STRING_1 = signature("native function md5(str:meta::pure::metamodel::type::String[1]):meta::pure::metamodel::type::String[1];");
    public static final NativeFunctionDefinition MEAN__NUMBER_MANY = signature("native function mean(numbers:meta::pure::metamodel::type::Number[*]):meta::pure::metamodel::type::Float[1];");
    public static final NativeFunctionDefinition MEDIAN__NUMBER_MANY = signature("native function median(numbers:meta::pure::metamodel::type::Number[*]):meta::pure::metamodel::type::Number[1];");
    public static final NativeFunctionDefinition MINUS__DECIMAL_1__DECIMAL_1 = signature("native function minus(left:meta::pure::metamodel::type::Decimal[1], right:meta::pure::metamodel::type::Decimal[1]):meta::pure::metamodel::type::Decimal[1];");
    public static final NativeFunctionDefinition MINUS__FLOAT_1__FLOAT_1 = signature("native function minus(left:meta::pure::metamodel::type::Float[1], right:meta::pure::metamodel::type::Float[1]):meta::pure::metamodel::type::Float[1];");
    public static final NativeFunctionDefinition MINUS__INTEGER_1__INTEGER_1 = signature("native function minus(left:meta::pure::metamodel::type::Integer[1], right:meta::pure::metamodel::type::Integer[1]):meta::pure::metamodel::type::Integer[1];");
    public static final NativeFunctionDefinition MINUS__NUMBER_1__NUMBER_1 = signature("native function minus(left:meta::pure::metamodel::type::Number[1], right:meta::pure::metamodel::type::Number[1]):meta::pure::metamodel::type::Number[1];");
    public static final NativeFunctionDefinition MINUS__T_MANY = signature("native function minus<T>(values:T[*]):T[1];");
    public static final NativeFunctionDefinition MINUTE__DATE_1 = signature("native function minute(d:meta::pure::metamodel::type::Date[1]):meta::pure::metamodel::type::Integer[1];");
    public static final NativeFunctionDefinition MIN_BY__ROW_MAPPER_MANY = signature("native function minBy<T,U>(values:meta::pure::functions::math::mathUtility::RowMapper<T,U>[*]):T[0..1];");
    public static final NativeFunctionDefinition MIN_BY__T_MANY__FUNCTION_1 = signature("native function minBy<T>(values:T[*], key:meta::pure::metamodel::function::Function<{T[1]->meta::pure::metamodel::type::Any[1]}>[1]):T[0..1];");
    public static final NativeFunctionDefinition MIN_BY__T_MANY__FUNCTION_1__INTEGER_1 = signature("native function minBy<T>(values:T[*], key:meta::pure::metamodel::function::Function<{T[1]->meta::pure::metamodel::type::Any[1]}>[1], count:meta::pure::metamodel::type::Integer[1]):T[*];");
    public static final NativeFunctionDefinition MIN_BY__T_MANY__T_MANY = signature("native function minBy<T>(values:T[*], keys:T[*]):T[0..1];");
    public static final NativeFunctionDefinition MIN_BY__T_MANY__T_MANY__INTEGER_1 = signature("native function minBy<T>(values:T[*], keys:T[*], count:meta::pure::metamodel::type::Integer[1]):T[*];");
    public static final NativeFunctionDefinition MIN_DATE__DATE_1__DATE_1 = signature("native function minDate(d1:meta::pure::metamodel::type::Date[1], d2:meta::pure::metamodel::type::Date[1]):meta::pure::metamodel::type::Date[1];");
    public static final NativeFunctionDefinition MIN__DATE_1__DATE_1 = signature("native function min(left:meta::pure::metamodel::type::Date[1], right:meta::pure::metamodel::type::Date[1]):meta::pure::metamodel::type::Date[1];");
    public static final NativeFunctionDefinition MIN__DATE_MANY = signature("native function min(dates:meta::pure::metamodel::type::Date[*]):meta::pure::metamodel::type::Date[0..1];");
    public static final NativeFunctionDefinition MIN__DATE_TIME_1__DATE_TIME_1 = signature("native function min(left:meta::pure::metamodel::type::DateTime[1], right:meta::pure::metamodel::type::DateTime[1]):meta::pure::metamodel::type::DateTime[1];");
    public static final NativeFunctionDefinition MIN__DATE_TIME_MANY = signature("native function min(dates:meta::pure::metamodel::type::DateTime[*]):meta::pure::metamodel::type::DateTime[0..1];");
    public static final NativeFunctionDefinition MIN__FLOAT_1__FLOAT_1 = signature("native function min(left:meta::pure::metamodel::type::Float[1], right:meta::pure::metamodel::type::Float[1]):meta::pure::metamodel::type::Float[1];");
    public static final NativeFunctionDefinition MIN__FLOAT_MANY = signature("native function min(values:meta::pure::metamodel::type::Float[*]):meta::pure::metamodel::type::Float[0..1];");
    public static final NativeFunctionDefinition MIN__INTEGER_1__INTEGER_1 = signature("native function min(left:meta::pure::metamodel::type::Integer[1], right:meta::pure::metamodel::type::Integer[1]):meta::pure::metamodel::type::Integer[1];");
    public static final NativeFunctionDefinition MIN__INTEGER_MANY = signature("native function min(values:meta::pure::metamodel::type::Integer[*]):meta::pure::metamodel::type::Integer[0..1];");
    public static final NativeFunctionDefinition MIN__NUMBER_1__NUMBER_1 = signature("native function min(left:meta::pure::metamodel::type::Number[1], right:meta::pure::metamodel::type::Number[1]):meta::pure::metamodel::type::Number[1];");
    public static final NativeFunctionDefinition MIN__NUMBER_MANY = signature("native function min(numbers:meta::pure::metamodel::type::Number[*]):meta::pure::metamodel::type::Number[0..1];");
    public static final NativeFunctionDefinition MIN__STRICT_DATE_1__STRICT_DATE_1 = signature("native function min(left:meta::pure::metamodel::type::StrictDate[1], right:meta::pure::metamodel::type::StrictDate[1]):meta::pure::metamodel::type::StrictDate[1];");
    public static final NativeFunctionDefinition MIN__STRICT_DATE_MANY = signature("native function min(dates:meta::pure::metamodel::type::StrictDate[*]):meta::pure::metamodel::type::StrictDate[0..1];");
    public static final NativeFunctionDefinition MODE__ANY_MANY = signature("native function mode(values:meta::pure::metamodel::type::Any[*]):meta::pure::metamodel::type::Any[0..1];");
    public static final NativeFunctionDefinition MOD__INTEGER_1__INTEGER_1 = signature("native function mod(dividend:meta::pure::metamodel::type::Integer[1], divisor:meta::pure::metamodel::type::Integer[1]):meta::pure::metamodel::type::Integer[1];");
    public static final NativeFunctionDefinition MONTH_NUMBER__DATE_1 = signature("native function monthNumber(d:meta::pure::metamodel::type::Date[1]):meta::pure::metamodel::type::Integer[1];");
    public static final NativeFunctionDefinition MONTH__DATE_1 = signature("native function month(d:meta::pure::metamodel::type::Date[1]):meta::pure::functions::date::Month[1];");
    public static final NativeFunctionDefinition NEW_TDS_RELATION_ACCESSOR__RELATION_1 = signature("native function newTDSRelationAccessor<T>(tds:meta::pure::metamodel::relation::Relation<T>[1]):meta::pure::metamodel::relation::Relation<T>[1];");
    public static final NativeFunctionDefinition NOT_EQUAL_ANSI__ANY_1__ANY_1 = signature("native function notEqualAnsi(left:meta::pure::metamodel::type::Any[1], right:meta::pure::metamodel::type::Any[1]):meta::pure::metamodel::type::Boolean[1];");
    // VERIFIED vs real legend-pure (notEqual == !equal, same params): Any[*], Any[*].
    // The constant name already said MANY while the signature said [1] — fixed.
    public static final NativeFunctionDefinition NOT_EQUAL__ANY_MANY__ANY_MANY = signature("native function notEqual(left:meta::pure::metamodel::type::Any[*], right:meta::pure::metamodel::type::Any[*]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition NOT__BOOLEAN_1 = signature("native function not(value:meta::pure::metamodel::type::Boolean[1]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition NOW = signature("native function now():meta::pure::metamodel::type::DateTime[1];");
    public static final NativeFunctionDefinition NTH__RELATION_1__WINDOW_1__T_1__INTEGER_1 = signature("native function nth<T>(w:meta::pure::metamodel::relation::Relation<T>[1], f:meta::pure::functions::relation::_Window<T>[1], r:T[1], offset:meta::pure::metamodel::type::Integer[1]):T[0..1];");
    public static final NativeFunctionDefinition NTILE__RELATION_1__T_1__INTEGER_1 = signature("native function ntile<T>(rel:meta::pure::metamodel::relation::Relation<T>[1], row:T[1], tileCount:meta::pure::metamodel::type::Integer[1]):meta::pure::metamodel::type::Integer[1];");
    public static final NativeFunctionDefinition OBJECT_REFERENCE_IN__ANY_1__ANY_MANY = signature("native function objectReferenceIn(col:meta::pure::metamodel::type::Any[1], values:meta::pure::metamodel::type::Any[*]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition OFFSET__RELATION_1__T_1__INTEGER_1 = signature("native function offset<T>(w:meta::pure::metamodel::relation::Relation<T>[1], r:T[1], offset:meta::pure::metamodel::type::Integer[1]):T[0..1];");
    public static final NativeFunctionDefinition OR__BOOLEAN_1__BOOLEAN_1 = signature("native function or(left:meta::pure::metamodel::type::Boolean[1], right:meta::pure::metamodel::type::Boolean[1]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition OR__BOOLEAN_MANY = signature("native function or(values:meta::pure::metamodel::type::Boolean[*]):meta::pure::metamodel::type::Boolean[1];");
    // otherwise (generic class-level structural merge): takes a partial
    // instance and a fallback instance of the same class and returns a
    // complete instance — partial's set fields win, fallback fills the
    // rest (docs/MAPPING_CLEAN_SHEET.md §4.3). Emitted by MappingNormalizer
    // for OtherwiseEmbedded PMs as otherwise(^Inner(<inline subs>),
    // $row.<slot>) where the fallback slot is a legacyNavigate'd
    // instance. partial is always constructed (T[1]); the fallback slot
    // may be optional (T[0..1]); the merge yields a complete T[1].
    public static final NativeFunctionDefinition OTHERWISE__T_1__T_0_1 = signature("native function otherwise<T>(partial:T[1], fallback:T[0..1]):T[1];");
    public static final NativeFunctionDefinition OVER__COL_SPEC_1 = signature("native function over<T>(cols:meta::pure::metamodel::relation::ColSpec<T>[1]):meta::pure::functions::relation::_Window<T>[1];");
    public static final NativeFunctionDefinition OVER__COL_SPEC_1__SORT_INFO_MANY = signature("native function over<T>(cols:meta::pure::metamodel::relation::ColSpec<T>[1], sortInfo:meta::pure::functions::relation::SortInfo<T>[*]):meta::pure::functions::relation::_Window<T>[1];");
    public static final NativeFunctionDefinition OVER__COL_SPEC_1__SORT_INFO_MANY__RANGE_1 = signature("native function over<T>(cols:meta::pure::metamodel::relation::ColSpec<T>[1], sortInfo:meta::pure::functions::relation::SortInfo<T>[*], range:meta::pure::functions::relation::_Range[1]):meta::pure::functions::relation::_Window<T>[1];");
    public static final NativeFunctionDefinition OVER__COL_SPEC_1__SORT_INFO_MANY__ROWS_1 = signature("native function over<T>(cols:meta::pure::metamodel::relation::ColSpec<T>[1], sortInfo:meta::pure::functions::relation::SortInfo<T>[*], rows:meta::pure::functions::relation::Rows[1]):meta::pure::functions::relation::_Window<T>[1];");
    public static final NativeFunctionDefinition OVER__COL_SPEC_ARRAY_1 = signature("native function over<T>(cols:meta::pure::metamodel::relation::ColSpecArray<T>[1]):meta::pure::functions::relation::_Window<T>[1];");
    public static final NativeFunctionDefinition OVER__COL_SPEC_ARRAY_1__SORT_INFO_MANY = signature("native function over<T>(cols:meta::pure::metamodel::relation::ColSpecArray<T>[1], sortInfo:meta::pure::functions::relation::SortInfo<T>[*]):meta::pure::functions::relation::_Window<T>[1];");
    public static final NativeFunctionDefinition OVER__SORT_INFO_MANY = signature("native function over<T>(sortInfo:meta::pure::functions::relation::SortInfo<T>[*]):meta::pure::functions::relation::_Window<T>[1];");
    public static final NativeFunctionDefinition PAIR__T_1__U_1 = signature("native function pair<T,U>(first:T[1], second:U[1]):meta::pure::functions::collection::Pair<T,U>[1];");
    public static final NativeFunctionDefinition PARSE_BOOLEAN__STRING_1 = signature("native function parseBoolean(string:meta::pure::metamodel::type::String[1]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition PARSE_DATE__STRING_1 = signature("native function parseDate(string:meta::pure::metamodel::type::String[1]):meta::pure::metamodel::type::Date[1];");
    public static final NativeFunctionDefinition PARSE_DECIMAL__STRING_1 = signature("native function parseDecimal(string:meta::pure::metamodel::type::String[1]):meta::pure::metamodel::type::Decimal[1];");
    public static final NativeFunctionDefinition PARSE_FLOAT__STRING_1 = signature("native function parseFloat(string:meta::pure::metamodel::type::String[1]):meta::pure::metamodel::type::Float[1];");
    public static final NativeFunctionDefinition PARSE_INTEGER__STRING_1 = signature("native function parseInteger(string:meta::pure::metamodel::type::String[1]):meta::pure::metamodel::type::Integer[1];");
    public static final NativeFunctionDefinition PERCENTILE_CONT__NUMBER_MANY__NUMBER_1 = signature("native function percentileCont(numbers:meta::pure::metamodel::type::Number[*], p:meta::pure::metamodel::type::Number[1]):meta::pure::metamodel::type::Number[1];");
    public static final NativeFunctionDefinition PERCENTILE_DISC__NUMBER_MANY__NUMBER_1 = signature("native function percentileDisc(numbers:meta::pure::metamodel::type::Number[*], p:meta::pure::metamodel::type::Number[1]):meta::pure::metamodel::type::Number[1];");
    public static final NativeFunctionDefinition PERCENTILE__NUMBER_MANY__NUMBER_1 = signature("native function percentile(numbers:meta::pure::metamodel::type::Number[*], p:meta::pure::metamodel::type::Number[1]):meta::pure::metamodel::type::Number[1];");
    public static final NativeFunctionDefinition PERCENTILE__NUMBER_MANY__NUMBER_1__BOOLEAN_1__BOOLEAN_1 = signature("native function percentile(numbers:meta::pure::metamodel::type::Number[*], p:meta::pure::metamodel::type::Number[1], ascending:meta::pure::metamodel::type::Boolean[1], continuous:meta::pure::metamodel::type::Boolean[1]):meta::pure::metamodel::type::Number[0..1];");
    public static final NativeFunctionDefinition PERCENT_RANK__RELATION_1__WINDOW_1__T_1 = signature("native function percentRank<T>(rel:meta::pure::metamodel::relation::Relation<T>[1], w:meta::pure::functions::relation::_Window<T>[1], row:T[1]):meta::pure::metamodel::type::Float[1];");
    public static final NativeFunctionDefinition PI = signature("native function pi():meta::pure::metamodel::type::Float[1];");
    public static final NativeFunctionDefinition PIVOT__RELATION_1__COL_SPEC_1__AGG_COL_SPEC_1 = signature("native function pivot<T,Z,K,V,R>(r:meta::pure::metamodel::relation::Relation<T>[1], cols:meta::pure::metamodel::relation::ColSpec<Z⊆T>[1], agg:meta::pure::metamodel::relation::AggColSpec<{T[1]->K[0..1]},{K[*]->V[0..1]},R>[1]):meta::pure::metamodel::relation::Relation<meta::pure::metamodel::type::Any>[1];");
    public static final NativeFunctionDefinition PIVOT__RELATION_1__COL_SPEC_1__AGG_COL_SPEC_ARRAY_1 = signature("native function pivot<T,Z,K,V,R>(r:meta::pure::metamodel::relation::Relation<T>[1], cols:meta::pure::metamodel::relation::ColSpec<Z⊆T>[1], agg:meta::pure::metamodel::relation::AggColSpecArray<{T[1]->K[0..1]},{K[*]->V[0..1]},R>[1]):meta::pure::metamodel::relation::Relation<meta::pure::metamodel::type::Any>[1];");
    public static final NativeFunctionDefinition PIVOT__RELATION_1__COL_SPEC_1__ANY_1_MANY__AGG_COL_SPEC_1 = signature("native function pivot<T,Z,K,V,R>(r:meta::pure::metamodel::relation::Relation<T>[1], cols:meta::pure::metamodel::relation::ColSpec<Z⊆T>[1], values:meta::pure::metamodel::type::Any[1..*], agg:meta::pure::metamodel::relation::AggColSpec<{T[1]->K[0..1]},{K[*]->V[0..1]},R>[1]):meta::pure::metamodel::relation::Relation<meta::pure::metamodel::type::Any>[1];");
    public static final NativeFunctionDefinition PIVOT__RELATION_1__COL_SPEC_ARRAY_1__AGG_COL_SPEC_1 = signature("native function pivot<T,Z,K,V,R>(r:meta::pure::metamodel::relation::Relation<T>[1], cols:meta::pure::metamodel::relation::ColSpecArray<Z⊆T>[1], agg:meta::pure::metamodel::relation::AggColSpec<{T[1]->K[0..1]},{K[*]->V[0..1]},R>[1]):meta::pure::metamodel::relation::Relation<meta::pure::metamodel::type::Any>[1];");
    public static final NativeFunctionDefinition PIVOT__RELATION_1__COL_SPEC_ARRAY_1__AGG_COL_SPEC_ARRAY_1 = signature("native function pivot<T,Z,K,V,R>(r:meta::pure::metamodel::relation::Relation<T>[1], cols:meta::pure::metamodel::relation::ColSpecArray<Z⊆T>[1], agg:meta::pure::metamodel::relation::AggColSpecArray<{T[1]->K[0..1]},{K[*]->V[0..1]},R>[1]):meta::pure::metamodel::relation::Relation<meta::pure::metamodel::type::Any>[1];");
    public static final NativeFunctionDefinition PLUS__DECIMAL_1__DECIMAL_1 = signature("native function plus(left:meta::pure::metamodel::type::Decimal[1], right:meta::pure::metamodel::type::Decimal[1]):meta::pure::metamodel::type::Decimal[1];");
    public static final NativeFunctionDefinition PLUS__FLOAT_1__FLOAT_1 = signature("native function plus(left:meta::pure::metamodel::type::Float[1], right:meta::pure::metamodel::type::Float[1]):meta::pure::metamodel::type::Float[1];");
    public static final NativeFunctionDefinition PLUS__INTEGER_1__INTEGER_1 = signature("native function plus(left:meta::pure::metamodel::type::Integer[1], right:meta::pure::metamodel::type::Integer[1]):meta::pure::metamodel::type::Integer[1];");
    public static final NativeFunctionDefinition PLUS__NUMBER_1__NUMBER_1 = signature("native function plus(left:meta::pure::metamodel::type::Number[1], right:meta::pure::metamodel::type::Number[1]):meta::pure::metamodel::type::Number[1];");
    public static final NativeFunctionDefinition PLUS__STRING_1__STRING_1 = signature("native function plus(left:meta::pure::metamodel::type::String[1], right:meta::pure::metamodel::type::String[1]):meta::pure::metamodel::type::String[1];");
    public static final NativeFunctionDefinition PLUS__T_MANY = signature("native function plus<T>(values:T[*]):T[1];");
    public static final NativeFunctionDefinition POW__NUMBER_1__NUMBER_1 = signature("native function pow(base:meta::pure::metamodel::type::Number[1], exponent:meta::pure::metamodel::type::Number[1]):meta::pure::metamodel::type::Number[1];");
    public static final NativeFunctionDefinition PROJECT__C_MANY__FUNC_COL_SPEC_ARRAY_1 = signature("native function project<C,T>(cl:C[*], x:meta::pure::metamodel::relation::FuncColSpecArray<{C[1]->meta::pure::metamodel::type::Any[*]},T>[1]):meta::pure::metamodel::relation::Relation<T>[1];");
    public static final NativeFunctionDefinition PROJECT__K_MANY__FUNCTION_MANY__STRING_MANY = signature("native function project<K>(set:K[*], fns:meta::pure::metamodel::function::Function<{K[1]->meta::pure::metamodel::type::Any[*]}>[*], ids:meta::pure::metamodel::type::String[*]):meta::pure::metamodel::relation::Relation<K>[1];");
    public static final NativeFunctionDefinition PROJECT__RELATION_1__FUNC_COL_SPEC_ARRAY_1 = signature("native function project<T,Z>(r:meta::pure::metamodel::relation::Relation<T>[1], fs:meta::pure::metamodel::relation::FuncColSpecArray<{T[1]->meta::pure::metamodel::type::Any[*]},Z>[1]):meta::pure::metamodel::relation::Relation<Z>[1];");
    public static final NativeFunctionDefinition QUARTER_NUMBER__DATE_1 = signature("native function quarterNumber(d:meta::pure::metamodel::type::Date[1]):meta::pure::metamodel::type::Integer[1];");
    public static final NativeFunctionDefinition QUARTER__DATE_1 = signature("native function quarter(d:meta::pure::metamodel::type::Date[1]):meta::pure::functions::date::Quarter[1];");
    public static final NativeFunctionDefinition RANGE__INTEGER_1 = signature("native function range(stop:meta::pure::metamodel::type::Integer[1]):meta::pure::metamodel::type::Integer[*];");
    public static final NativeFunctionDefinition RANGE__INTEGER_1__INTEGER_1 = signature("native function range(start:meta::pure::metamodel::type::Integer[1], stop:meta::pure::metamodel::type::Integer[1]):meta::pure::metamodel::type::Integer[*];");
    public static final NativeFunctionDefinition RANGE__INTEGER_1__INTEGER_1__INTEGER_1 = signature("native function range(start:meta::pure::metamodel::type::Integer[1], stop:meta::pure::metamodel::type::Integer[1], step:meta::pure::metamodel::type::Integer[1]):meta::pure::metamodel::type::Integer[*];");
    public static final NativeFunctionDefinition RANK__RELATION_1__WINDOW_1__T_1 = signature("native function rank<T>(rel:meta::pure::metamodel::relation::Relation<T>[1], w:meta::pure::functions::relation::_Window<T>[1], row:T[1]):meta::pure::metamodel::type::Integer[1];");
    public static final NativeFunctionDefinition REMOVE_ALL_OPTIMIZED__T_MANY__T_MANY = signature("native function removeAllOptimized<T>(set:T[*], other:T[*]):T[*];");
    public static final NativeFunctionDefinition REMOVE_DUPLICATES_BY__T_MANY__FUNCTION_1 = signature("native function removeDuplicatesBy<T,V>(col:T[*], key:meta::pure::metamodel::function::Function<{T[1]->V[1]}>[1]):T[*];");
    public static final NativeFunctionDefinition REMOVE_DUPLICATES__T_MANY = signature("native function removeDuplicates<T>(col:T[*]):T[*];");
    public static final NativeFunctionDefinition REMOVE_DUPLICATES__T_MANY__FUNCTION_0_1__FUNCTION_0_1 = signature("native function removeDuplicates<T,V>(col:T[*], key:meta::pure::metamodel::function::Function<{T[1]->V[1]}>[0..1], eql:meta::pure::metamodel::function::Function<{V[1],V[1]->meta::pure::metamodel::type::Boolean[1]}>[0..1]):T[*];");
    public static final NativeFunctionDefinition REMOVE_DUPLICATES__T_MANY__FUNCTION_1 = signature("native function removeDuplicates<T>(col:T[*], eql:meta::pure::metamodel::function::Function<{T[1],T[1]->meta::pure::metamodel::type::Boolean[1]}>[1]):T[*];");
    public static final NativeFunctionDefinition REM__NUMBER_1__NUMBER_1 = signature("native function rem(dividend:meta::pure::metamodel::type::Number[1], divisor:meta::pure::metamodel::type::Number[1]):meta::pure::metamodel::type::Number[1];");
    public static final NativeFunctionDefinition RENAME__RELATION_1__COL_SPEC_1__COL_SPEC_1 = signature("native function rename<T,Z,K,V>(r:meta::pure::metamodel::relation::Relation<T>[1], old:meta::pure::metamodel::relation::ColSpec<Z=(?:K)⊆T>[1], new:meta::pure::metamodel::relation::ColSpec<V=(?:K)>[1]):meta::pure::metamodel::relation::Relation<T-Z+V>[1];");
    public static final NativeFunctionDefinition RENAME__RELATION_1__COL_SPEC_ARRAY_1__COL_SPEC_ARRAY_1 = signature("native function rename<T,Z,V>(r:meta::pure::metamodel::relation::Relation<T>[1], oldCols:meta::pure::metamodel::relation::ColSpecArray<Z⊆T>[1], newCols:meta::pure::metamodel::relation::ColSpecArray<V>[1]):meta::pure::metamodel::relation::Relation<T-Z+V>[1];");
    public static final NativeFunctionDefinition REPEAT_STRING__STRING_1__INTEGER_1 = signature("native function repeatString(str:meta::pure::metamodel::type::String[1], count:meta::pure::metamodel::type::Integer[1]):meta::pure::metamodel::type::String[1];");
    public static final NativeFunctionDefinition REPLACE__STRING_1__STRING_1__STRING_1 = signature("native function replace(str:meta::pure::metamodel::type::String[1], toFind:meta::pure::metamodel::type::String[1], replacement:meta::pure::metamodel::type::String[1]):meta::pure::metamodel::type::String[1];");
    public static final NativeFunctionDefinition REVERSE_STRING__STRING_1 = signature("native function reverseString(str:meta::pure::metamodel::type::String[1]):meta::pure::metamodel::type::String[1];");
    public static final NativeFunctionDefinition REVERSE__T_m = signature("native function reverse<T|m>(set:T[m]):T[m];");
    public static final NativeFunctionDefinition RIGHT__STRING_1__INTEGER_1 = signature("native function right(str:meta::pure::metamodel::type::String[1], len:meta::pure::metamodel::type::Integer[1]):meta::pure::metamodel::type::String[1];");
    public static final NativeFunctionDefinition ROUND__DECIMAL_1__INTEGER_1 = signature("native function round(decimal:meta::pure::metamodel::type::Decimal[1], scale:meta::pure::metamodel::type::Integer[1]):meta::pure::metamodel::type::Decimal[1];");
    public static final NativeFunctionDefinition ROUND__FLOAT_1__INTEGER_1 = signature("native function round(float:meta::pure::metamodel::type::Float[1], scale:meta::pure::metamodel::type::Integer[1]):meta::pure::metamodel::type::Float[1];");
    public static final NativeFunctionDefinition ROUND__NUMBER_1 = signature("native function round(number:meta::pure::metamodel::type::Number[1]):meta::pure::metamodel::type::Integer[1];");
    public static final NativeFunctionDefinition ROWS__INTEGER_1__INTEGER_1 = signature("native function rows(offsetFrom:meta::pure::metamodel::type::Integer[1], offsetTo:meta::pure::metamodel::type::Integer[1]):meta::pure::functions::relation::Rows[1];");
    public static final NativeFunctionDefinition ROWS__UNBOUNDED_1__UNBOUNDED_1 = signature("native function rows(offsetFrom:meta::pure::functions::relation::UnboundedFrameValue[1], offsetTo:meta::pure::functions::relation::UnboundedFrameValue[1]):meta::pure::functions::relation::Rows[1];");
    public static final NativeFunctionDefinition ROWS__UNBOUNDED_1__INTEGER_1 = signature("native function rows(offsetFrom:meta::pure::functions::relation::UnboundedFrameValue[1], offsetTo:meta::pure::metamodel::type::Integer[1]):meta::pure::functions::relation::Rows[1];");
    public static final NativeFunctionDefinition ROWS__INTEGER_1__UNBOUNDED_1 = signature("native function rows(offsetFrom:meta::pure::metamodel::type::Integer[1], offsetTo:meta::pure::functions::relation::UnboundedFrameValue[1]):meta::pure::functions::relation::Rows[1];");
    public static final NativeFunctionDefinition ROW_MAPPER__T_0_1__U_0_1 = signature("native function rowMapper<T,U>(value:T[0..1], key:U[0..1]):meta::pure::functions::math::mathUtility::RowMapper<T,U>[1];");
    public static final NativeFunctionDefinition ROW_NUMBER__RELATION_1__T_1 = signature("native function rowNumber<T>(rel:meta::pure::metamodel::relation::Relation<T>[1], row:T[1]):meta::pure::metamodel::type::Integer[1];");
    public static final NativeFunctionDefinition RPAD__STRING_1__INTEGER_1 = signature("native function rpad(str:meta::pure::metamodel::type::String[1], len:meta::pure::metamodel::type::Integer[1]):meta::pure::metamodel::type::String[1];");
    public static final NativeFunctionDefinition RPAD__STRING_1__INTEGER_1__STRING_1 = signature("native function rpad(str:meta::pure::metamodel::type::String[1], len:meta::pure::metamodel::type::Integer[1], pad:meta::pure::metamodel::type::String[1]):meta::pure::metamodel::type::String[1];");
    public static final NativeFunctionDefinition RTRIM__STRING_1 = signature("native function rtrim(str:meta::pure::metamodel::type::String[1]):meta::pure::metamodel::type::String[1];");
    public static final NativeFunctionDefinition SECOND__DATE_1 = signature("native function second(d:meta::pure::metamodel::type::Date[1]):meta::pure::metamodel::type::Integer[1];");
    public static final NativeFunctionDefinition SELECT__RELATION_1 = signature("native function select<T>(r:meta::pure::metamodel::relation::Relation<T>[1]):meta::pure::metamodel::relation::Relation<T>[1];");
    public static final NativeFunctionDefinition SELECT__RELATION_1__COL_SPEC_1 = signature("native function select<T,Z>(r:meta::pure::metamodel::relation::Relation<T>[1], cols:meta::pure::metamodel::relation::ColSpec<Z⊆T>[1]):meta::pure::metamodel::relation::Relation<Z>[1];");
    public static final NativeFunctionDefinition SELECT__RELATION_1__COL_SPEC_ARRAY_1 = signature("native function select<T,Z>(r:meta::pure::metamodel::relation::Relation<T>[1], cols:meta::pure::metamodel::relation::ColSpecArray<Z⊆T>[1]):meta::pure::metamodel::relation::Relation<Z>[1];");
    public static final NativeFunctionDefinition SERIALIZE__T_MANY__ROOT_GRAPH_FETCH_TREE_1 = signature("native function serialize<T>(source:T[*], tree:meta::pure::graphFetch::RootGraphFetchTree<T>[1]):meta::pure::metamodel::type::String[1];");
    public static final NativeFunctionDefinition SERIALIZE__T_MANY__ROOT_GRAPH_FETCH_TREE_1__ANY_1 = signature("native function serialize<T>(source:T[*], tree:meta::pure::graphFetch::RootGraphFetchTree<T>[1], config:meta::pure::metamodel::type::Any[1]):meta::pure::metamodel::type::String[1];");
    public static final NativeFunctionDefinition SHA1__STRING_1 = signature("native function sha1(str:meta::pure::metamodel::type::String[1]):meta::pure::metamodel::type::String[1];");
    public static final NativeFunctionDefinition SHA256__STRING_1 = signature("native function sha256(str:meta::pure::metamodel::type::String[1]):meta::pure::metamodel::type::String[1];");
    public static final NativeFunctionDefinition SIGN__NUMBER_1 = signature("native function sign(number:meta::pure::metamodel::type::Number[1]):meta::pure::metamodel::type::Integer[1];");
    public static final NativeFunctionDefinition SINH__NUMBER_1 = signature("native function sinh(number:meta::pure::metamodel::type::Number[1]):meta::pure::metamodel::type::Float[1];");
    public static final NativeFunctionDefinition SIN__NUMBER_1 = signature("native function sin(number:meta::pure::metamodel::type::Number[1]):meta::pure::metamodel::type::Float[1];");
    public static final NativeFunctionDefinition SIZE__RELATION_1 = signature("native function size<T>(rel:meta::pure::metamodel::relation::Relation<T>[1]):meta::pure::metamodel::type::Integer[1];");
    public static final NativeFunctionDefinition SIZE__T_MANY = signature("native function size<T>(col:T[*]):meta::pure::metamodel::type::Integer[1];");
    public static final NativeFunctionDefinition SLICE__RELATION_1__INTEGER_1__INTEGER_1 = signature("native function slice<T>(rel:meta::pure::metamodel::relation::Relation<T>[1], start:meta::pure::metamodel::type::Integer[1], stop:meta::pure::metamodel::type::Integer[1]):meta::pure::metamodel::relation::Relation<T>[1];");
    public static final NativeFunctionDefinition SLICE__T_MANY__INTEGER_1__INTEGER_1 = signature("native function slice<T>(set:T[*], start:meta::pure::metamodel::type::Integer[1], end:meta::pure::metamodel::type::Integer[1]):T[*];");
    public static final NativeFunctionDefinition SORT_BY_REVERSED__T_m__FUNCTION_0_1 = signature("native function sortByReversed<T,U|m>(col:T[m], key:meta::pure::metamodel::function::Function<{T[1]->U[1]}>[0..1]):T[m];");
    public static final NativeFunctionDefinition SORT_BY__T_m__FUNCTION_0_1 = signature("native function sortBy<T,U|m>(col:T[m], key:meta::pure::metamodel::function::Function<{T[1]->U[1]}>[0..1]):T[m];");
    public static final NativeFunctionDefinition SORT__RELATION_1__SORT_INFO_MANY = signature("native function sort<X,T>(rel:meta::pure::metamodel::relation::Relation<T>[1], sortInfo:meta::pure::functions::relation::SortInfo<X⊆T>[*]):meta::pure::metamodel::relation::Relation<T>[1];");
    public static final NativeFunctionDefinition SORT__RELATION_1__STRING_1__SORT_DIRECTION_1 = signature("native function sort<T>(rel:meta::pure::metamodel::relation::Relation<T>[1], col:meta::pure::metamodel::type::String[1], direction:meta::relational::metamodel::SortDirection[1]):meta::pure::metamodel::relation::Relation<T>[1];");
    public static final NativeFunctionDefinition SORT__RELATION_1__STRING_MANY = signature("native function sort<T>(rel:meta::pure::metamodel::relation::Relation<T>[1], cols:meta::pure::metamodel::type::String[*]):meta::pure::metamodel::relation::Relation<T>[1];");
    public static final NativeFunctionDefinition SORT__T_m = signature("native function sort<T|m>(col:T[m]):T[m];");
    public static final NativeFunctionDefinition SORT__T_m__FUNCTION_0_1 = signature("native function sort<T|m>(col:T[m], comp:meta::pure::metamodel::function::Function<{T[1],T[1]->meta::pure::metamodel::type::Integer[1]}>[0..1]):T[m];");
    public static final NativeFunctionDefinition SORT__T_m__FUNCTION_0_1__FUNCTION_0_1 = signature("native function sort<T,U|m>(col:T[m], key:meta::pure::metamodel::function::Function<{T[1]->U[1]}>[0..1], comp:meta::pure::metamodel::function::Function<{U[1],U[1]->meta::pure::metamodel::type::Integer[1]}>[0..1]):T[m];");
    public static final NativeFunctionDefinition SOURCE_URL__STRING_1 = signature("native function sourceUrl(url:meta::pure::metamodel::type::String[1]):meta::pure::metamodel::relation::Relation<meta::pure::metamodel::type::Any>[1];");
    public static final NativeFunctionDefinition SPLIT_PART__STRING_0_1__STRING_1__INTEGER_1 = signature("native function splitPart(str:meta::pure::metamodel::type::String[0..1], delimiter:meta::pure::metamodel::type::String[1], index:meta::pure::metamodel::type::Integer[1]):meta::pure::metamodel::type::String[0..1];");
    public static final NativeFunctionDefinition SPLIT__STRING_1__STRING_1 = signature("native function split(str:meta::pure::metamodel::type::String[1], delimiter:meta::pure::metamodel::type::String[1]):meta::pure::metamodel::type::String[*];");
    public static final NativeFunctionDefinition SQL_FALSE = signature("native function sqlFalse():meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition SQL_NULL = signature("native function sqlNull():meta::pure::metamodel::type::Nil[0];");
    public static final NativeFunctionDefinition SQL_TRUE = signature("native function sqlTrue():meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition SQRT__NUMBER_1 = signature("native function sqrt(number:meta::pure::metamodel::type::Number[1]):meta::pure::metamodel::type::Float[1];");
    public static final NativeFunctionDefinition STARTS_WITH__STRING_1__STRING_1 = signature("native function startsWith(source:meta::pure::metamodel::type::String[1], val:meta::pure::metamodel::type::String[1]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition STD_DEV_POPULATION__NUMBER_MANY = signature("native function stdDevPopulation(numbers:meta::pure::metamodel::type::Number[*]):meta::pure::metamodel::type::Number[1];");
    public static final NativeFunctionDefinition STD_DEV_POPULATION__RELATION_1__WINDOW_1__T_1__COL_SPEC_1 = signature("native function stdDevPopulation<T>(partition:meta::pure::metamodel::relation::Relation<T>[1], window:meta::pure::functions::relation::_Window<T>[1], row:T[1], colToAgg:meta::pure::metamodel::relation::ColSpec<(?:meta::pure::metamodel::type::Number)⊆T>[1]):meta::pure::metamodel::type::Number[1];");
    public static final NativeFunctionDefinition STD_DEV_SAMPLE__NUMBER_MANY = signature("native function stdDevSample(numbers:meta::pure::metamodel::type::Number[*]):meta::pure::metamodel::type::Number[1];");
    public static final NativeFunctionDefinition STD_DEV__NUMBER_MANY = signature("native function stdDev(numbers:meta::pure::metamodel::type::Number[*]):meta::pure::metamodel::type::Number[1];");
    public static final NativeFunctionDefinition SUBSTRING__STRING_1__INTEGER_1 = signature("native function substring(str:meta::pure::metamodel::type::String[1], start:meta::pure::metamodel::type::Integer[1]):meta::pure::metamodel::type::String[1];");
    public static final NativeFunctionDefinition SUBSTRING__STRING_1__INTEGER_1__INTEGER_1 = signature("native function substring(str:meta::pure::metamodel::type::String[1], start:meta::pure::metamodel::type::Integer[1], end:meta::pure::metamodel::type::Integer[1]):meta::pure::metamodel::type::String[1];");
    public static final NativeFunctionDefinition SUB__DECIMAL_1__DECIMAL_1 = signature("native function sub(left:meta::pure::metamodel::type::Decimal[1], right:meta::pure::metamodel::type::Decimal[1]):meta::pure::metamodel::type::Decimal[1];");
    public static final NativeFunctionDefinition SUB__FLOAT_1__FLOAT_1 = signature("native function sub(left:meta::pure::metamodel::type::Float[1], right:meta::pure::metamodel::type::Float[1]):meta::pure::metamodel::type::Float[1];");
    public static final NativeFunctionDefinition SUB__INTEGER_1__INTEGER_1 = signature("native function sub(left:meta::pure::metamodel::type::Integer[1], right:meta::pure::metamodel::type::Integer[1]):meta::pure::metamodel::type::Integer[1];");
    public static final NativeFunctionDefinition SUB__NUMBER_1__NUMBER_1 = signature("native function sub(left:meta::pure::metamodel::type::Number[1], right:meta::pure::metamodel::type::Number[1]):meta::pure::metamodel::type::Number[1];");
    public static final NativeFunctionDefinition SUM__NUMBER_MANY = signature("native function sum(numbers:meta::pure::metamodel::type::Number[*]):meta::pure::metamodel::type::Number[1];");
    public static final NativeFunctionDefinition TABLE_REFERENCE__STRING_1__STRING_1 = signature("native function tableReference(db:meta::pure::metamodel::type::String[1], name:meta::pure::metamodel::type::String[1]):meta::pure::metamodel::relation::Relation<meta::pure::metamodel::type::Any>[1];");
    public static final NativeFunctionDefinition TAIL__T_MANY = signature("native function tail<T>(set:T[*]):T[*];");
    public static final NativeFunctionDefinition TAKE__RELATION_1__INTEGER_1 = signature("native function take<T>(rel:meta::pure::metamodel::relation::Relation<T>[1], size:meta::pure::metamodel::type::Integer[1]):meta::pure::metamodel::relation::Relation<T>[1];");
    public static final NativeFunctionDefinition TAKE__T_MANY__INTEGER_1 = signature("native function take<T>(set:T[*], size:meta::pure::metamodel::type::Integer[1]):T[*];");
    public static final NativeFunctionDefinition TANH__NUMBER_1 = signature("native function tanh(number:meta::pure::metamodel::type::Number[1]):meta::pure::metamodel::type::Float[1];");
    public static final NativeFunctionDefinition TAN__NUMBER_1 = signature("native function tan(number:meta::pure::metamodel::type::Number[1]):meta::pure::metamodel::type::Float[1];");
    public static final NativeFunctionDefinition TDS__STRING_1__STRING_1 = signature("native function tds(tag:meta::pure::metamodel::type::String[1], raw:meta::pure::metamodel::type::String[1]):meta::pure::metamodel::relation::Relation<meta::pure::metamodel::type::Any>[1];");
    public static final NativeFunctionDefinition TIMES__DECIMAL_1__DECIMAL_1 = signature("native function times(left:meta::pure::metamodel::type::Decimal[1], right:meta::pure::metamodel::type::Decimal[1]):meta::pure::metamodel::type::Decimal[1];");
    public static final NativeFunctionDefinition TIMES__FLOAT_1__FLOAT_1 = signature("native function times(left:meta::pure::metamodel::type::Float[1], right:meta::pure::metamodel::type::Float[1]):meta::pure::metamodel::type::Float[1];");
    public static final NativeFunctionDefinition TIMES__INTEGER_1__INTEGER_1 = signature("native function times(left:meta::pure::metamodel::type::Integer[1], right:meta::pure::metamodel::type::Integer[1]):meta::pure::metamodel::type::Integer[1];");
    public static final NativeFunctionDefinition TIMES__NUMBER_1__NUMBER_1 = signature("native function times(left:meta::pure::metamodel::type::Number[1], right:meta::pure::metamodel::type::Number[1]):meta::pure::metamodel::type::Number[1];");
    public static final NativeFunctionDefinition TIMES__T_MANY = signature("native function times<T>(values:T[*]):T[1];");
    public static final NativeFunctionDefinition TIME_BUCKET__DATE_1__INTEGER_1__DURATION_UNIT_1 = signature("native function timeBucket(d:meta::pure::metamodel::type::Date[1], amount:meta::pure::metamodel::type::Integer[1], unit:meta::pure::functions::date::DurationUnit[1]):meta::pure::metamodel::type::Date[1];");
    public static final NativeFunctionDefinition TODAY = signature("native function today():meta::pure::metamodel::type::StrictDate[1];");
    public static final NativeFunctionDefinition TO_DECIMAL__NUMBER_1 = signature("native function toDecimal(number:meta::pure::metamodel::type::Number[1]):meta::pure::metamodel::type::Decimal[1];");
    public static final NativeFunctionDefinition TO_DEGREES__NUMBER_1 = signature("native function toDegrees(radians:meta::pure::metamodel::type::Number[1]):meta::pure::metamodel::type::Float[1];");
    public static final NativeFunctionDefinition TO_EPOCH_VALUE__DATE_1 = signature("native function toEpochValue(d:meta::pure::metamodel::type::Date[1]):meta::pure::metamodel::type::Integer[1];");
    public static final NativeFunctionDefinition TO_EPOCH_VALUE__DATE_1__DURATION_UNIT_1 = signature("native function toEpochValue(d:meta::pure::metamodel::type::Date[1], unit:meta::pure::functions::date::DurationUnit[1]):meta::pure::metamodel::type::Integer[1];");
    public static final NativeFunctionDefinition TO_FLOAT__NUMBER_1 = signature("native function toFloat(number:meta::pure::metamodel::type::Number[1]):meta::pure::metamodel::type::Float[1];");
    public static final NativeFunctionDefinition TO_LOWER_FIRST_CHARACTER__STRING_1 = signature("native function toLowerFirstCharacter(str:meta::pure::metamodel::type::String[1]):meta::pure::metamodel::type::String[1];");
    public static final NativeFunctionDefinition TO_LOWER__STRING_1 = signature("native function toLower(source:meta::pure::metamodel::type::String[1]):meta::pure::metamodel::type::String[1];");
    public static final NativeFunctionDefinition TO_MANY__T_0_1__V_0_1 = signature("native function toMany<T,V>(source:T[0..1], type:V[0..1]):V[*];");
    public static final NativeFunctionDefinition TO_ONE_MANY__T_MANY__STRING_1 = signature("native function toOneMany<T>(values:T[*], message:meta::pure::metamodel::type::String[1]):T[1..*];");
    public static final NativeFunctionDefinition TO_ONE__T_MANY = signature("native function toOne<T>(values:T[*]):T[1];");
    public static final NativeFunctionDefinition TO_ONE__T_MANY__STRING_1 = signature("native function toOne<T>(values:T[*], message:meta::pure::metamodel::type::String[1]):T[1];");
    public static final NativeFunctionDefinition TO_RADIANS__NUMBER_1 = signature("native function toRadians(degrees:meta::pure::metamodel::type::Number[1]):meta::pure::metamodel::type::Float[1];");
    public static final NativeFunctionDefinition TO_STRING__ANY_1 = signature("native function toString(any:meta::pure::metamodel::type::Any[1]):meta::pure::metamodel::type::String[1];");
    public static final NativeFunctionDefinition TO_UPPER_FIRST_CHARACTER__STRING_1 = signature("native function toUpperFirstCharacter(str:meta::pure::metamodel::type::String[1]):meta::pure::metamodel::type::String[1];");
    public static final NativeFunctionDefinition TO_UPPER__STRING_1 = signature("native function toUpper(source:meta::pure::metamodel::type::String[1]):meta::pure::metamodel::type::String[1];");
    public static final NativeFunctionDefinition TO_VARIANT__ANY_MANY = signature("native function toVariant(source:meta::pure::metamodel::type::Any[*]):meta::pure::metamodel::variant::Variant[1];");
    public static final NativeFunctionDefinition TO__T_0_1__V_0_1 = signature("native function to<T,V>(source:T[0..1], type:V[0..1]):V[0..1];");
    public static final NativeFunctionDefinition TRAVERSE__RELATION_1__FUNCTION_1 = signature("native function traverse<T,V>(target:meta::pure::metamodel::relation::Relation<V>[1], cond:meta::pure::metamodel::function::Function<{T[1],V[1]->meta::pure::metamodel::type::Boolean[1]}>[1]):meta::pure::functions::relation::_Traversal[1];");
    public static final NativeFunctionDefinition TRAVERSE__TRAVERSAL_1__RELATION_1__FUNCTION_1 = signature("native function traverse<T,V>(prev:meta::pure::functions::relation::_Traversal[1], target:meta::pure::metamodel::relation::Relation<V>[1], cond:meta::pure::metamodel::function::Function<{T[1],V[1]->meta::pure::metamodel::type::Boolean[1]}>[1]):meta::pure::functions::relation::_Traversal[1];");
    public static final NativeFunctionDefinition TRIM__STRING_1 = signature("native function trim(str:meta::pure::metamodel::type::String[1]):meta::pure::metamodel::type::String[1];");
    public static final NativeFunctionDefinition TYPE__ANY_1 = signature("native function type(any:meta::pure::metamodel::type::Any[1]):meta::pure::metamodel::type::Type[1];");
    public static final NativeFunctionDefinition UNBOUNDED = signature("native function unbounded():meta::pure::functions::relation::UnboundedFrameValue[1];");
    public static final NativeFunctionDefinition VARIANCE_POPULATION__NUMBER_MANY = signature("native function variancePopulation(numbers:meta::pure::metamodel::type::Number[*]):meta::pure::metamodel::type::Number[1];");
    public static final NativeFunctionDefinition VARIANCE_SAMPLE__NUMBER_MANY = signature("native function varianceSample(numbers:meta::pure::metamodel::type::Number[*]):meta::pure::metamodel::type::Number[1];");
    public static final NativeFunctionDefinition VARIANCE__NUMBER_MANY = signature("native function variance(numbers:meta::pure::metamodel::type::Number[*]):meta::pure::metamodel::type::Number[1];");
    public static final NativeFunctionDefinition VARIANCE__NUMBER_MANY__BOOLEAN_1 = signature("native function variance(numbers:meta::pure::metamodel::type::Number[*], isSample:meta::pure::metamodel::type::Boolean[1]):meta::pure::metamodel::type::Number[1];");
    public static final NativeFunctionDefinition VARIANT_TO__ANY_1__T_1 = signature("native function variantTo<T>(val:meta::pure::metamodel::type::Any[1], type:T[1]):T[1];");
    public static final NativeFunctionDefinition WAVG__ROW_MAPPER_MANY = signature("native function wavg<T,U>(values:meta::pure::functions::math::mathUtility::RowMapper<T,U>[*]):meta::pure::metamodel::type::Float[1];");
    public static final NativeFunctionDefinition WEEK_OF_YEAR__DATE_1 = signature("native function weekOfYear(d:meta::pure::metamodel::type::Date[1]):meta::pure::metamodel::type::Integer[1];");
    public static final NativeFunctionDefinition WRITE__RELATION_1 = signature("native function write<T>(source:meta::pure::metamodel::relation::Relation<T>[1]):meta::pure::metamodel::type::Integer[1];");
    public static final NativeFunctionDefinition WRITE__RELATION_1__ANY_1 = signature("native function write<T>(source:meta::pure::metamodel::relation::Relation<T>[1], target:meta::pure::metamodel::type::Any[1]):meta::pure::metamodel::type::Integer[1];");
    public static final NativeFunctionDefinition XOR__BOOLEAN_1__BOOLEAN_1 = signature("native function xor(left:meta::pure::metamodel::type::Boolean[1], right:meta::pure::metamodel::type::Boolean[1]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition YEAR__DATE_1 = signature("native function year(d:meta::pure::metamodel::type::Date[1]):meta::pure::metamodel::type::Integer[1];");
    public static final NativeFunctionDefinition ZIP__T_MANY__U_MANY = signature("native function zip<T,U>(set1:T[*], set2:U[*]):meta::pure::functions::collection::Pair<T,U>[*];");
    public static final NativeFunctionDefinition _RANGE__NUMBER_1__NUMBER_1 = signature("native function _range(offsetFrom:meta::pure::metamodel::type::Number[1], offsetTo:meta::pure::metamodel::type::Number[1]):meta::pure::functions::relation::_Range[1];");
}
