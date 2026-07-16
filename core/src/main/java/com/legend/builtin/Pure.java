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
// HAND-CURATED port of the real legend-pure/legend-engine native catalog.
// Every signature is VERBATIM to its real .pure source (verified per
// function; NO divergence categories remain as of 2026-07-08) — except the
// individually-commented INVENTED pipeline natives (tableReference, tds,
// legacyNavigate, ...), which are internal plumbing, not stdlib claims.
// To add a native: add the verbatim signature citing its .pure path,
// re-run tests (the golden catalog file shows the diff).
package com.legend.builtin;

import com.legend.parser.ElementParser;
import com.legend.model.ClassDefinition;
import com.legend.model.EnumDefinition;
import com.legend.model.NativeFunctionDefinition;

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
    // real m3: Type extends PackageableElement extends ... ModelElement — the
    // chain contracts to the link we model (a Class value conforms to
    // ModelElement; letFn's removeDuplicates over classes needs it)
    public static final ClassDefinition TYPE = nativeClass("native Class meta::pure::metamodel::type::Type extends meta::pure::metamodel::ModelElement {}");
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

    // ===== engine RELATIONAL-RUNTIME surface (K-phase natives) =====
    // The corpus's own executeInDb WRAPPER functions
    // (relationalExtension.pure) compile against these classes and inline
    // to the native leaf below; the CONNECTION VALUE at run time is the
    // execution context's one ambient JDBC connection (the K dispatch
    // never evaluates connection expressions).
    // the real platform_dsl_store/grammar/runtime.pure trio — properties
    // as REAL pure declares them (the connection VALUES never evaluate;
    // these exist so connection-resolution chains TYPE-check)
    public static final ClassDefinition RUNTIME_CONNECTION = nativeClass("native Class meta::core::runtime::Connection {}");
    public static final ClassDefinition CONNECTION_STORE = nativeClass("native Class meta::core::runtime::ConnectionStore { connection: meta::core::runtime::Connection[1]; element: meta::pure::metamodel::type::Any[1]; }");
    public static final ClassDefinition RUNTIME = nativeClass("native Class meta::core::runtime::Runtime { connectionStores: meta::core::runtime::ConnectionStore[*]; }");
    // scalar properties as REAL relationalRuntime.pure declares them (the
    // Function-typed post-processor properties are omitted until demanded);
    // the corpus's testDatabaseConnection(...) constructs these
    public static final ClassDefinition DATABASE_CONNECTION = nativeClass("native Class meta::external::store::relational::runtime::DatabaseConnection extends meta::core::runtime::Connection { type: meta::relational::runtime::DatabaseType[1]; debug: meta::pure::metamodel::type::Boolean[0..1]; timeZone: meta::pure::metamodel::type::String[0..1]; quoteIdentifiers: meta::pure::metamodel::type::Boolean[0..1]; queryTimeOutInSeconds: meta::pure::metamodel::type::Integer[0..1]; }");
    public static final ClassDefinition TEST_DATABASE_CONNECTION = nativeClass("native Class meta::external::store::relational::runtime::TestDatabaseConnection extends meta::external::store::relational::runtime::DatabaseConnection { testDataSetupCsv: meta::pure::metamodel::type::String[0..1]; }");
    // the store METACLASS (real: extends meta::pure::store::Store) — a
    // database REFERENCE is a value of this type (classReference), so the
    // corpus's testRuntime(db:Database[1]) overload family type-checks
    public static final ClassDefinition DATABASE_METACLASS = nativeClass("native Class meta::relational::metamodel::Database {}");
    // real platform_store_relational/functions.pure:50-65 (dataSource and
    // Row's value(name) qualified property omitted until demanded) — setup
    // functions INTROSPECT results (println(executeInDb(...).rows.values))
    public static final ClassDefinition RESULT_SET = nativeClass("native Class meta::relational::metamodel::execute::ResultSet extends meta::pure::metamodel::type::Any { executionTimeInNanoSecond: meta::pure::metamodel::type::Integer[1]; connectionAcquisitionTimeInNanoSecond: meta::pure::metamodel::type::Integer[1]; executionPlanInformation: meta::pure::metamodel::type::String[0..1]; columnNames: meta::pure::metamodel::type::String[*]; rows: meta::relational::metamodel::execute::Row[*]; }");
    public static final ClassDefinition RESULT_SET_ROW = nativeClass("native Class meta::relational::metamodel::execute::Row extends meta::pure::metamodel::type::Any { values: meta::pure::metamodel::type::Any[*]; parent: meta::relational::metamodel::execute::ResultSet[1]; }");

    // ---- Function carrier (parameterized over a function-type token) ----
    public static final ClassDefinition FUNCTION = nativeClass("native Class meta::pure::metamodel::function::Function<F> extends meta::pure::metamodel::type::Any {}");

    // ---- Metaclass ----
    // Pure exposes the metaclass as `Class<T>` (parameterized over the
    // class it describes); used by signatures like `getAll(Class<T>):T[*]`.
    public static final ClassDefinition CLASS = nativeClass("native Class meta::pure::metamodel::type::Class<T> extends meta::pure::metamodel::type::Type {}");
    // The enumeration metaclass (real m3: Class Enumeration<T> extends Type) —
    // a bare enumeration reference (STR_GeographicEntityType->toString()) is a
    // value of this type.
    public static final ClassDefinition ENUMERATION = nativeClass("native Class meta::pure::metamodel::type::Enumeration<T> extends meta::pure::metamodel::type::Type {}");

    // ---- Variant (semi-structured value carrier) ----
    public static final ClassDefinition VARIANT = nativeClass("native Class meta::pure::metamodel::variant::Variant extends meta::pure::metamodel::type::Any {}");

    // ---- Collection carriers ----
    // REAL pure declares values (legend-pure platform/pure/anonymousCollections.pure:33-35,
    // <<equality.Key>>) — property access and ^List(values=...) construction validate against it.
    public static final ClassDefinition LIST = nativeClass("native Class meta::pure::functions::collection::List<T>    extends meta::pure::metamodel::type::Any { values: T[*]; }");
    // REAL pure declares first/second (legend-pure platform/pure/anonymousCollections.pure:17-25,
    // both <<equality.Key>>) — property access and instance construction validate against THEM.
    public static final ClassDefinition PAIR = nativeClass("native Class meta::pure::functions::collection::Pair<U, V> extends meta::pure::metamodel::type::Any { first: U[1]; second: V[1]; }");
    public static final ClassDefinition MAP = nativeClass("native Class meta::pure::functions::collection::Map<U, V> extends meta::pure::metamodel::type::Any {}");

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
    public static final ClassDefinition _RANGE_INTERVAL       = nativeClass("native Class meta::pure::functions::relation::_RangeInterval       extends meta::pure::functions::relation::Frame {}");
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

    // ---- Relational runtime enums ----
    // real relationalRuntime.pure:21 — the corpus's testDatabaseConnection
    // constructs ^TestDatabaseConnection(type=DatabaseType.H2, ...)
    public static final EnumDefinition DATABASE_TYPE = nativeEnum("""
            Enum meta::relational::runtime::DatabaseType
            {
                DB2, H2, MemSQL, Sybase, SybaseIQ, Composite, Postgres, SqlServer,
                Hive, Snowflake, Presto, Trino, BigQuery, Redshift, Databricks,
                Spanner, Athena, Aurora, SparkSQL, DuckDB, Oracle, ClickHouse,
                DebugPrint
            }
            """);

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

    // ---- String/date enums (real regexpParameter.pure / formatDate.pure) ----
    public static final EnumDefinition REGEXP_PARAMETER = nativeEnum("""
            Enum meta::pure::functions::string::RegexpParameter
            {
                CASE_SENSITIVE, CASE_INSENSITIVE, MULTILINE, NON_NEWLINE_SENSITIVE
            }
            """);
    public static final EnumDefinition STRICT_DATE_FORMAT = nativeEnum(
            "Enum meta::pure::functions::date::StrictDateFormat { ISO8601 }");
    public static final EnumDefinition DATE_TIME_FORMAT = nativeEnum(
            "Enum meta::pure::functions::date::DateTimeFormat { ISO8601_NanoSecondPrecision }");

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

    /** The legacy TDS join kind (join(tds, JoinType.INNER, ...)). */
    public static final EnumDefinition JOIN_TYPE = nativeEnum(
            "Enum meta::relational::metamodel::join::JoinType"
            + " { INNER, LEFT_OUTER, RIGHT_OUTER, FULL_OUTER }");

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
        /** bare name -> ALL overloads across packages (filter ∈ collection+relation, ...). */
        static final java.util.Map<String, List<NativeFunctionDefinition>> FN_BY_BARE = new java.util.HashMap<>();
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
                String bare = nfd.qualifiedName().contains("::")
                        ? nfd.qualifiedName().substring(nfd.qualifiedName().lastIndexOf("::") + 2)
                        : nfd.qualifiedName();
                FN_BY_BARE.computeIfAbsent(bare, k -> new ArrayList<>()).add(nfd);
                // keys index serves BOTH spellings (registration tables use bare).
                KEYS_BY_NAME.computeIfAbsent(nfd.qualifiedName(), k -> new java.util.HashSet<>())
                        .add(nfd.signatureKey());
                KEYS_BY_NAME.computeIfAbsent(bare, k -> new java.util.HashSet<>())
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
     * Signature keys of the overloads at {@code name} that take a parameter
     * whose type is the EXACT class {@code paramClassFqn} (audit 15:
     * replaces the lowering's {@code contains("_Window")} key probe —
     * identification is by full FQN, never substring).
     */
    public static List<String> nativeKeysAt(String name, String paramClassFqn) {
        List<String> keys = new ArrayList<>();
        for (var f : nativeFunctionsAt(name)) {
            for (var prm : f.parameters()) {
                String head = switch (prm.type()) {
                    case com.legend.model.TypeExpression.NameRef nr -> nr.name();
                    case com.legend.model.TypeExpression.Generic g -> g.name();
                    default -> null;
                };
                if (paramClassFqn.equals(head)) {
                    keys.add(f.signatureKey());
                    break;
                }
            }
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

    /** The real second overload: in(value:Any[0..1], ...) — an empty needle is FALSE. */
    public static String keyInOptional() {
        return IN__ANY_0_1__ANY_MANY.signatureKey();
    }

    public static boolean nativeNamed(String name, String signatureKey) {
        return Index.KEYS_BY_NAME
                .getOrDefault(name, java.util.Set.of())
                .contains(signatureKey);
    }

    public static List<NativeFunctionDefinition> nativeFunctionsAt(String name) {
        // FQN-keyed catalog with a BARE-NAME secondary index: a qualified
        // lookup resolves its exact package; a bare lookup returns the union
        // of overloads across packages (overload resolution picks by shape).
        if (name.contains("::")) {
            return Index.FN_BY_FQN.getOrDefault(name, List.of());
        }
        return Index.FN_BY_BARE.getOrDefault(name, List.of());
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

    public static final NativeFunctionDefinition ABS__T_1 = signature("native function meta::pure::functions::math::abs<T>(number:T[1]):T[1];");
    public static final NativeFunctionDefinition ACOS__NUMBER_1 = signature("native function meta::pure::functions::math::acos(number:meta::pure::metamodel::type::Number[1]):meta::pure::metamodel::type::Float[1];");
    public static final NativeFunctionDefinition ADD__T_MANY__INTEGER_1__T_1 = signature("native function meta::pure::functions::collection::add<T>(set:T[*], index:meta::pure::metamodel::type::Integer[1], val:T[1]):T[*];");
    public static final NativeFunctionDefinition ADD__T_MANY__T_1 = signature("native function meta::pure::functions::collection::add<T>(set:T[*], val:T[1]):T[*];");

    // CALENDAR-AGGREGATION natives (engine calendarFunctions.pure —
    // 32 fns, one shape; lowered as CASE-conditioned aggregates over
    // the LegendCalendarSchema calendar table, task G1):
    public static final NativeFunctionDefinition CAL_ANNUALIZED = signature("native function meta::pure::functions::date::calendar::annualized(date:meta::pure::metamodel::type::Date[0..1], calendarType:meta::pure::metamodel::type::String[1], endDate:meta::pure::metamodel::type::Date[1], value:meta::pure::metamodel::type::Number[0..1]):meta::pure::metamodel::type::Number[0..1];");
    public static final NativeFunctionDefinition CAL_CME = signature("native function meta::pure::functions::date::calendar::cme(date:meta::pure::metamodel::type::Date[0..1], calendarType:meta::pure::metamodel::type::String[1], endDate:meta::pure::metamodel::type::Date[1], value:meta::pure::metamodel::type::Number[0..1]):meta::pure::metamodel::type::Number[0..1];");
    public static final NativeFunctionDefinition CAL_CW = signature("native function meta::pure::functions::date::calendar::cw(date:meta::pure::metamodel::type::Date[0..1], calendarType:meta::pure::metamodel::type::String[1], endDate:meta::pure::metamodel::type::Date[1], value:meta::pure::metamodel::type::Number[0..1]):meta::pure::metamodel::type::Number[0..1];");
    public static final NativeFunctionDefinition CAL_CW_FM = signature("native function meta::pure::functions::date::calendar::cw_fm(date:meta::pure::metamodel::type::Date[0..1], calendarType:meta::pure::metamodel::type::String[1], endDate:meta::pure::metamodel::type::Date[1], value:meta::pure::metamodel::type::Number[0..1]):meta::pure::metamodel::type::Number[0..1];");
    public static final NativeFunctionDefinition CAL_C_Y_MINUS2 = signature("native function meta::pure::functions::date::calendar::CYMinus2(date:meta::pure::metamodel::type::Date[0..1], calendarType:meta::pure::metamodel::type::String[1], endDate:meta::pure::metamodel::type::Date[1], value:meta::pure::metamodel::type::Number[0..1]):meta::pure::metamodel::type::Number[0..1];");
    public static final NativeFunctionDefinition CAL_C_Y_MINUS3 = signature("native function meta::pure::functions::date::calendar::CYMinus3(date:meta::pure::metamodel::type::Date[0..1], calendarType:meta::pure::metamodel::type::String[1], endDate:meta::pure::metamodel::type::Date[1], value:meta::pure::metamodel::type::Number[0..1]):meta::pure::metamodel::type::Number[0..1];");
    public static final NativeFunctionDefinition CAL_MTD = signature("native function meta::pure::functions::date::calendar::mtd(date:meta::pure::metamodel::type::Date[0..1], calendarType:meta::pure::metamodel::type::String[1], endDate:meta::pure::metamodel::type::Date[1], value:meta::pure::metamodel::type::Number[0..1]):meta::pure::metamodel::type::Number[0..1];");
    public static final NativeFunctionDefinition CAL_P12WA = signature("native function meta::pure::functions::date::calendar::p12wa(date:meta::pure::metamodel::type::Date[0..1], calendarType:meta::pure::metamodel::type::String[1], endDate:meta::pure::metamodel::type::Date[1], value:meta::pure::metamodel::type::Number[0..1]):meta::pure::metamodel::type::Number[0..1];");
    public static final NativeFunctionDefinition CAL_P12WTD = signature("native function meta::pure::functions::date::calendar::p12wtd(date:meta::pure::metamodel::type::Date[0..1], calendarType:meta::pure::metamodel::type::String[1], endDate:meta::pure::metamodel::type::Date[1], value:meta::pure::metamodel::type::Number[0..1]):meta::pure::metamodel::type::Number[0..1];");
    public static final NativeFunctionDefinition CAL_P4WA = signature("native function meta::pure::functions::date::calendar::p4wa(date:meta::pure::metamodel::type::Date[0..1], calendarType:meta::pure::metamodel::type::String[1], endDate:meta::pure::metamodel::type::Date[1], value:meta::pure::metamodel::type::Number[0..1]):meta::pure::metamodel::type::Number[0..1];");
    public static final NativeFunctionDefinition CAL_P4WTD = signature("native function meta::pure::functions::date::calendar::p4wtd(date:meta::pure::metamodel::type::Date[0..1], calendarType:meta::pure::metamodel::type::String[1], endDate:meta::pure::metamodel::type::Date[1], value:meta::pure::metamodel::type::Number[0..1]):meta::pure::metamodel::type::Number[0..1];");
    public static final NativeFunctionDefinition CAL_P52WTD = signature("native function meta::pure::functions::date::calendar::p52wtd(date:meta::pure::metamodel::type::Date[0..1], calendarType:meta::pure::metamodel::type::String[1], endDate:meta::pure::metamodel::type::Date[1], value:meta::pure::metamodel::type::Number[0..1]):meta::pure::metamodel::type::Number[0..1];");
    public static final NativeFunctionDefinition CAL_P52WA = signature("native function meta::pure::functions::date::calendar::p52wa(date:meta::pure::metamodel::type::Date[0..1], calendarType:meta::pure::metamodel::type::String[1], endDate:meta::pure::metamodel::type::Date[1], value:meta::pure::metamodel::type::Number[0..1]):meta::pure::metamodel::type::Number[0..1];");
    public static final NativeFunctionDefinition CAL_P12MTD = signature("native function meta::pure::functions::date::calendar::p12mtd(date:meta::pure::metamodel::type::Date[0..1], calendarType:meta::pure::metamodel::type::String[1], endDate:meta::pure::metamodel::type::Date[1], value:meta::pure::metamodel::type::Number[0..1]):meta::pure::metamodel::type::Number[0..1];");
    public static final NativeFunctionDefinition CAL_PMA = signature("native function meta::pure::functions::date::calendar::pma(date:meta::pure::metamodel::type::Date[0..1], calendarType:meta::pure::metamodel::type::String[1], endDate:meta::pure::metamodel::type::Date[1], value:meta::pure::metamodel::type::Number[0..1]):meta::pure::metamodel::type::Number[0..1];");
    public static final NativeFunctionDefinition CAL_PMTD = signature("native function meta::pure::functions::date::calendar::pmtd(date:meta::pure::metamodel::type::Date[0..1], calendarType:meta::pure::metamodel::type::String[1], endDate:meta::pure::metamodel::type::Date[1], value:meta::pure::metamodel::type::Number[0..1]):meta::pure::metamodel::type::Number[0..1];");
    public static final NativeFunctionDefinition CAL_PQTD = signature("native function meta::pure::functions::date::calendar::pqtd(date:meta::pure::metamodel::type::Date[0..1], calendarType:meta::pure::metamodel::type::String[1], endDate:meta::pure::metamodel::type::Date[1], value:meta::pure::metamodel::type::Number[0..1]):meta::pure::metamodel::type::Number[0..1];");
    public static final NativeFunctionDefinition CAL_PRIOR_DAY = signature("native function meta::pure::functions::date::calendar::priorDay(date:meta::pure::metamodel::type::Date[0..1], calendarType:meta::pure::metamodel::type::String[1], endDate:meta::pure::metamodel::type::Date[1], value:meta::pure::metamodel::type::Number[0..1]):meta::pure::metamodel::type::Number[0..1];");
    public static final NativeFunctionDefinition CAL_PRIOR_YEAR = signature("native function meta::pure::functions::date::calendar::priorYear(date:meta::pure::metamodel::type::Date[0..1], calendarType:meta::pure::metamodel::type::String[1], endDate:meta::pure::metamodel::type::Date[1], value:meta::pure::metamodel::type::Number[0..1]):meta::pure::metamodel::type::Number[0..1];");
    public static final NativeFunctionDefinition CAL_PW = signature("native function meta::pure::functions::date::calendar::pw(date:meta::pure::metamodel::type::Date[0..1], calendarType:meta::pure::metamodel::type::String[1], endDate:meta::pure::metamodel::type::Date[1], value:meta::pure::metamodel::type::Number[0..1]):meta::pure::metamodel::type::Number[0..1];");
    public static final NativeFunctionDefinition CAL_PW_FM = signature("native function meta::pure::functions::date::calendar::pw_fm(date:meta::pure::metamodel::type::Date[0..1], calendarType:meta::pure::metamodel::type::String[1], endDate:meta::pure::metamodel::type::Date[1], value:meta::pure::metamodel::type::Number[0..1]):meta::pure::metamodel::type::Number[0..1];");
    public static final NativeFunctionDefinition CAL_PWA = signature("native function meta::pure::functions::date::calendar::pwa(date:meta::pure::metamodel::type::Date[0..1], calendarType:meta::pure::metamodel::type::String[1], endDate:meta::pure::metamodel::type::Date[1], value:meta::pure::metamodel::type::Number[0..1]):meta::pure::metamodel::type::Number[0..1];");
    public static final NativeFunctionDefinition CAL_PWTD = signature("native function meta::pure::functions::date::calendar::pwtd(date:meta::pure::metamodel::type::Date[0..1], calendarType:meta::pure::metamodel::type::String[1], endDate:meta::pure::metamodel::type::Date[1], value:meta::pure::metamodel::type::Number[0..1]):meta::pure::metamodel::type::Number[0..1];");
    public static final NativeFunctionDefinition CAL_PYMTD = signature("native function meta::pure::functions::date::calendar::pymtd(date:meta::pure::metamodel::type::Date[0..1], calendarType:meta::pure::metamodel::type::String[1], endDate:meta::pure::metamodel::type::Date[1], value:meta::pure::metamodel::type::Number[0..1]):meta::pure::metamodel::type::Number[0..1];");
    public static final NativeFunctionDefinition CAL_PYQTD = signature("native function meta::pure::functions::date::calendar::pyqtd(date:meta::pure::metamodel::type::Date[0..1], calendarType:meta::pure::metamodel::type::String[1], endDate:meta::pure::metamodel::type::Date[1], value:meta::pure::metamodel::type::Number[0..1]):meta::pure::metamodel::type::Number[0..1];");
    public static final NativeFunctionDefinition CAL_PYTD = signature("native function meta::pure::functions::date::calendar::pytd(date:meta::pure::metamodel::type::Date[0..1], calendarType:meta::pure::metamodel::type::String[1], endDate:meta::pure::metamodel::type::Date[1], value:meta::pure::metamodel::type::Number[0..1]):meta::pure::metamodel::type::Number[0..1];");
    public static final NativeFunctionDefinition CAL_PYWA = signature("native function meta::pure::functions::date::calendar::pywa(date:meta::pure::metamodel::type::Date[0..1], calendarType:meta::pure::metamodel::type::String[1], endDate:meta::pure::metamodel::type::Date[1], value:meta::pure::metamodel::type::Number[0..1]):meta::pure::metamodel::type::Number[0..1];");
    public static final NativeFunctionDefinition CAL_PYWTD = signature("native function meta::pure::functions::date::calendar::pywtd(date:meta::pure::metamodel::type::Date[0..1], calendarType:meta::pure::metamodel::type::String[1], endDate:meta::pure::metamodel::type::Date[1], value:meta::pure::metamodel::type::Number[0..1]):meta::pure::metamodel::type::Number[0..1];");
    public static final NativeFunctionDefinition CAL_QTD = signature("native function meta::pure::functions::date::calendar::qtd(date:meta::pure::metamodel::type::Date[0..1], calendarType:meta::pure::metamodel::type::String[1], endDate:meta::pure::metamodel::type::Date[1], value:meta::pure::metamodel::type::Number[0..1]):meta::pure::metamodel::type::Number[0..1];");
    public static final NativeFunctionDefinition CAL_REPORT_END_DAY = signature("native function meta::pure::functions::date::calendar::reportEndDay(date:meta::pure::metamodel::type::Date[0..1], calendarType:meta::pure::metamodel::type::String[1], endDate:meta::pure::metamodel::type::Date[1], value:meta::pure::metamodel::type::Number[0..1]):meta::pure::metamodel::type::Number[0..1];");
    public static final NativeFunctionDefinition CAL_WTD = signature("native function meta::pure::functions::date::calendar::wtd(date:meta::pure::metamodel::type::Date[0..1], calendarType:meta::pure::metamodel::type::String[1], endDate:meta::pure::metamodel::type::Date[1], value:meta::pure::metamodel::type::Number[0..1]):meta::pure::metamodel::type::Number[0..1];");
    public static final NativeFunctionDefinition CAL_YTD = signature("native function meta::pure::functions::date::calendar::ytd(date:meta::pure::metamodel::type::Date[0..1], calendarType:meta::pure::metamodel::type::String[1], endDate:meta::pure::metamodel::type::Date[1], value:meta::pure::metamodel::type::Number[0..1]):meta::pure::metamodel::type::Number[0..1];");
    public static final NativeFunctionDefinition ADJUST__DATE_1__INTEGER_1__DURATION_UNIT_1 = signature("native function meta::pure::functions::date::adjust(d:meta::pure::metamodel::type::Date[1], amount:meta::pure::metamodel::type::Integer[1], unit:meta::pure::functions::date::DurationUnit[1]):meta::pure::metamodel::type::Date[1];");
    public static final NativeFunctionDefinition AGGREGATE__RELATION_1__AGG_COL_SPEC_1 = signature("native function meta::pure::functions::relation::aggregate<T,K,V,R>(r:meta::pure::metamodel::relation::Relation<T>[1], agg:meta::pure::metamodel::relation::AggColSpec<{T[1]->K[0..1]},{K[*]->V[0..1]},R>[1]):meta::pure::metamodel::relation::Relation<R>[1];");
    public static final NativeFunctionDefinition AGGREGATE__RELATION_1__AGG_COL_SPEC_ARRAY_1 = signature("native function meta::pure::functions::relation::aggregate<T,K,V,R>(r:meta::pure::metamodel::relation::Relation<T>[1], agg:meta::pure::metamodel::relation::AggColSpecArray<{T[1]->K[0..1]},{K[*]->V[0..1]},R>[1]):meta::pure::metamodel::relation::Relation<R>[1];");
    public static final NativeFunctionDefinition AND__BOOLEAN_1__BOOLEAN_1 = signature("native function meta::pure::functions::boolean::and(left:meta::pure::metamodel::type::Boolean[1], right:meta::pure::metamodel::type::Boolean[1]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition AND__BOOLEAN_MANY = signature("native function meta::pure::functions::collection::and(bools:meta::pure::metamodel::type::Boolean[*]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition ASCENDING__COL_SPEC_1 = signature("native function meta::pure::functions::relation::ascending<T>(column:meta::pure::metamodel::relation::ColSpec<T>[1]):meta::pure::functions::relation::SortInfo<T>[1];");
    public static final NativeFunctionDefinition ASCII__STRING_1 = signature("native function meta::pure::functions::string::ascii(str:meta::pure::metamodel::type::String[1]):meta::pure::metamodel::type::Integer[1];");
    public static final NativeFunctionDefinition ASC__COL_SPEC_1 = signature("native function meta::pure::tds::asc<T>(column:meta::pure::metamodel::relation::ColSpec<T>[1]):meta::pure::functions::relation::SortInfo<T>[1];");
    public static final NativeFunctionDefinition ASIN__NUMBER_1 = signature("native function meta::pure::functions::math::asin(number:meta::pure::metamodel::type::Number[1]):meta::pure::metamodel::type::Float[1];");
    public static final NativeFunctionDefinition AS_OF_JOIN__RELATION_1__RELATION_1__FUNCTION_1 = signature("native function meta::pure::functions::relation::asOfJoin<T,V>(rel1:meta::pure::metamodel::relation::Relation<T>[1], rel2:meta::pure::metamodel::relation::Relation<V>[1], match:meta::pure::metamodel::function::Function<{T[1],V[1]->meta::pure::metamodel::type::Boolean[1]}>[1]):meta::pure::metamodel::relation::Relation<T+V>[1];");
    public static final NativeFunctionDefinition AS_OF_JOIN__RELATION_1__RELATION_1__FUNCTION_1__FUNCTION_1 = signature("native function meta::pure::functions::relation::asOfJoin<T,V>(rel1:meta::pure::metamodel::relation::Relation<T>[1], rel2:meta::pure::metamodel::relation::Relation<V>[1], match:meta::pure::metamodel::function::Function<{T[1],V[1]->meta::pure::metamodel::type::Boolean[1]}>[1], join:meta::pure::metamodel::function::Function<{T[1],V[1]->meta::pure::metamodel::type::Boolean[1]}>[1]):meta::pure::metamodel::relation::Relation<T+V>[1];");
    public static final NativeFunctionDefinition AS_OF_JOIN__RELATION_1__RELATION_1__FUNCTION_1__FUNCTION_1__STRING_1 = signature("native function meta::pure::functions::relation::asOfJoin<T,V>(rel1:meta::pure::metamodel::relation::Relation<T>[1], rel2:meta::pure::metamodel::relation::Relation<V>[1], match:meta::pure::metamodel::function::Function<{T[1],V[1]->meta::pure::metamodel::type::Boolean[1]}>[1], join:meta::pure::metamodel::function::Function<{T[1],V[1]->meta::pure::metamodel::type::Boolean[1]}>[1], prefix:meta::pure::metamodel::type::String[1]):meta::pure::metamodel::relation::Relation<T+V>[1];");
    public static final NativeFunctionDefinition ATAN2__NUMBER_1__NUMBER_1 = signature("native function meta::pure::functions::math::atan2(y:meta::pure::metamodel::type::Number[1], x:meta::pure::metamodel::type::Number[1]):meta::pure::metamodel::type::Float[1];");
    public static final NativeFunctionDefinition ATAN__NUMBER_1 = signature("native function meta::pure::functions::math::atan(number:meta::pure::metamodel::type::Number[1]):meta::pure::metamodel::type::Float[1];");
    public static final NativeFunctionDefinition AT__T_MANY__INTEGER_1 = signature("native function meta::pure::functions::collection::at<T>(set:T[*], index:meta::pure::metamodel::type::Integer[1]):T[1];");
    public static final NativeFunctionDefinition AVERAGE_RANK = signature("native function meta::pure::functions::math::olap::averageRank():meta::pure::metamodel::type::Number[1];");
    public static final NativeFunctionDefinition AVERAGE__NUMBER_MANY = signature("native function meta::pure::functions::math::average(numbers:meta::pure::metamodel::type::Number[*]):meta::pure::metamodel::type::Float[1];");
    // WINDOW FAMILY — VERIFIED per function 2026-07-08 against real checkouts;
    // now FULLY FAITHFUL: ranking and
    // slice are verbatim (core_functions_relation/relation/functions/
    // {ranking,slice}); the 4-arg colToAgg aggregates below (average,
    // stdDevPopulation — the ONLY aggregate window functions real pure has;
    // everything else windows via the agg-col spelling
    // ~c:{p,w,r|$r.col}:y|$y->sum()) are verbatim core_functions_standard/
    // math/aggregator. The old engine-lite 3-arg row-returning aggregate
    // forms were MADE UP (never in real pure, unlowerable, exercised only by
    // engine-lite-authored tests since rewritten) and are DELETED.
    // over(): verify the ⊆-constrained args + the String[*] overload in 4c.
    public static final NativeFunctionDefinition AVERAGE__RELATION_1__WINDOW_1__T_1__COL_SPEC_1 = signature("native function meta::pure::functions::math::average<T>(partition:meta::pure::metamodel::relation::Relation<T>[1], window:meta::pure::functions::relation::_Window<T>[1], row:T[1], colToAgg:meta::pure::metamodel::relation::ColSpec<(?:meta::pure::metamodel::type::Number)⊆T>[1]):meta::pure::metamodel::type::Float[1];");
    public static final NativeFunctionDefinition AVG__NUMBER_MANY = signature("native function meta::legend::lite::avg(numbers:meta::pure::metamodel::type::Number[*]):meta::pure::metamodel::type::Float[1];");
    public static final NativeFunctionDefinition BETWEEN__NUMBER = signature("native function meta::pure::functions::boolean::between(value:meta::pure::metamodel::type::Number[0..1], lower:meta::pure::metamodel::type::Number[0..1], upper:meta::pure::metamodel::type::Number[0..1]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition BETWEEN__STRING = signature("native function meta::pure::functions::boolean::between(value:meta::pure::metamodel::type::String[0..1], lower:meta::pure::metamodel::type::String[0..1], upper:meta::pure::metamodel::type::String[0..1]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition BETWEEN__STRICT_DATE = signature("native function meta::pure::functions::boolean::between(value:meta::pure::metamodel::type::StrictDate[0..1], lower:meta::pure::metamodel::type::StrictDate[0..1], upper:meta::pure::metamodel::type::StrictDate[0..1]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition BETWEEN__DATE_TIME = signature("native function meta::pure::functions::boolean::between(value:meta::pure::metamodel::type::DateTime[0..1], lower:meta::pure::metamodel::type::DateTime[0..1], upper:meta::pure::metamodel::type::DateTime[0..1]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition BIT_AND__INTEGER_1__INTEGER_1 = signature("native function meta::pure::functions::math::bitAnd(left:meta::pure::metamodel::type::Integer[1], right:meta::pure::metamodel::type::Integer[1]):meta::pure::metamodel::type::Integer[1];");
    public static final NativeFunctionDefinition BIT_OR__INTEGER_1__INTEGER_1 = signature("native function meta::pure::functions::math::bitOr(left:meta::pure::metamodel::type::Integer[1], right:meta::pure::metamodel::type::Integer[1]):meta::pure::metamodel::type::Integer[1];");
    public static final NativeFunctionDefinition BIT_SHIFT_LEFT__INTEGER_1__INTEGER_1 = signature("native function meta::pure::functions::math::bitShiftLeft(value:meta::pure::metamodel::type::Integer[1], bits:meta::pure::metamodel::type::Integer[1]):meta::pure::metamodel::type::Integer[1];");
    public static final NativeFunctionDefinition BIT_SHIFT_RIGHT__INTEGER_1__INTEGER_1 = signature("native function meta::pure::functions::math::bitShiftRight(value:meta::pure::metamodel::type::Integer[1], bits:meta::pure::metamodel::type::Integer[1]):meta::pure::metamodel::type::Integer[1];");
    public static final NativeFunctionDefinition BIT_XOR__INTEGER_1__INTEGER_1 = signature("native function meta::pure::functions::math::bitXor(left:meta::pure::metamodel::type::Integer[1], right:meta::pure::metamodel::type::Integer[1]):meta::pure::metamodel::type::Integer[1];");
    public static final NativeFunctionDefinition CAST__ANY_m__T_1 = signature("native function meta::pure::functions::lang::cast<T|m>(source:meta::pure::metamodel::type::Any[m], type:T[1]):T[m];");
    public static final NativeFunctionDefinition SUB_TYPE__ANY_m__T_1 = signature("native function meta::pure::functions::lang::subType<T|m>(source:meta::pure::metamodel::type::Any[m], object:T[1]):T[m];");
    public static final NativeFunctionDefinition CBRT__NUMBER_1 = signature("native function meta::pure::functions::math::cbrt(number:meta::pure::metamodel::type::Number[1]):meta::pure::metamodel::type::Float[1];");
    public static final NativeFunctionDefinition CEILING__NUMBER_1 = signature("native function meta::pure::functions::math::ceiling(number:meta::pure::metamodel::type::Number[1]):meta::pure::metamodel::type::Integer[1];");
    public static final NativeFunctionDefinition CHAR__INTEGER_1 = signature("native function meta::pure::functions::string::char(code:meta::pure::metamodel::type::Integer[1]):meta::pure::metamodel::type::String[1];");
    // coalesce: REAL pure is GENERIC (legend-engine core_functions_unclassified/flow/coalesce.pure)
    // — six overloads: 1-3 optional values, ifEmpty either [1] (result [1]) or [0..1] (result [0..1]).
    public static final NativeFunctionDefinition COALESCE__T_0_1__T_1 = signature("native function meta::pure::functions::flow::coalesce<T>(value:T[0..1], ifEmpty:T[1]):T[1];");
    public static final NativeFunctionDefinition COALESCE__T_0_1__T_0_1__T_1 = signature("native function meta::pure::functions::flow::coalesce<T>(value1:T[0..1], value2:T[0..1], ifEmpty:T[1]):T[1];");
    public static final NativeFunctionDefinition COALESCE__T_0_1__T_0_1__T_0_1__T_1 = signature("native function meta::pure::functions::flow::coalesce<T>(value1:T[0..1], value2:T[0..1], value3:T[0..1], ifEmpty:T[1]):T[1];");
    public static final NativeFunctionDefinition COALESCE__T_0_1__T_0_1 = signature("native function meta::pure::functions::flow::coalesce<T>(value:T[0..1], ifEmpty:T[0..1]):T[0..1];");
    public static final NativeFunctionDefinition COALESCE__T_0_1__T_0_1__T_0_1 = signature("native function meta::pure::functions::flow::coalesce<T>(value1:T[0..1], value2:T[0..1], ifEmpty:T[0..1]):T[0..1];");
    public static final NativeFunctionDefinition COALESCE__T_0_1__T_0_1__T_0_1__T_0_1 = signature("native function meta::pure::functions::flow::coalesce<T>(value1:T[0..1], value2:T[0..1], value3:T[0..1], ifEmpty:T[0..1]):T[0..1];");
    public static final NativeFunctionDefinition COMPARE__ANY_1__ANY_1 = signature("native function meta::pure::functions::lang::compare(left:meta::pure::metamodel::type::Any[1], right:meta::pure::metamodel::type::Any[1]):meta::pure::metamodel::type::Integer[1];");
    public static final NativeFunctionDefinition CONCATENATE__T_MANY__T_MANY = signature("native function meta::pure::functions::collection::concatenate<T>(set1:T[*], set2:T[*]):T[*];");
    public static final NativeFunctionDefinition CONCATENATE__RELATION_1__RELATION_1 = signature("native function meta::pure::functions::relation::concatenate<T>(rel1:meta::pure::metamodel::relation::Relation<T>[1], rel2:meta::pure::metamodel::relation::Relation<T>[1]):meta::pure::metamodel::relation::Relation<T>[1];");
    public static final NativeFunctionDefinition CONTAINS__ANY_MANY__ANY_1 = signature("native function meta::pure::functions::collection::contains(collection:meta::pure::metamodel::type::Any[*], val:meta::pure::metamodel::type::Any[1]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition CONTAINS__STRING_1__STRING_1 = signature("native function meta::pure::functions::string::contains(source:meta::pure::metamodel::type::String[1], val:meta::pure::metamodel::type::String[1]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition CONTAINS__T_MANY__T_1__FUNCTION_1 = signature("native function meta::pure::functions::collection::contains<T>(collection:T[*], val:T[1], comparator:meta::pure::metamodel::function::Function<{T[1],T[1]->meta::pure::metamodel::type::Boolean[1]}>[1]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition CORR__NUMBER_MANY__NUMBER_MANY = signature("native function meta::pure::functions::math::corr(x:meta::pure::metamodel::type::Number[*], y:meta::pure::metamodel::type::Number[*]):meta::pure::metamodel::type::Number[1];");
    public static final NativeFunctionDefinition CORR__ROW_MAPPER_MANY = signature("native function meta::pure::functions::math::corr<T,U>(values:meta::pure::functions::math::mathUtility::RowMapper<T,U>[*]):meta::pure::metamodel::type::Number[0..1];");
    public static final NativeFunctionDefinition COSH__NUMBER_1 = signature("native function meta::pure::functions::math::cosh(number:meta::pure::metamodel::type::Number[1]):meta::pure::metamodel::type::Float[1];");
    public static final NativeFunctionDefinition COS__NUMBER_1 = signature("native function meta::pure::functions::math::cos(number:meta::pure::metamodel::type::Number[1]):meta::pure::metamodel::type::Float[1];");
    public static final NativeFunctionDefinition COT__NUMBER_1 = signature("native function meta::pure::functions::math::cot(number:meta::pure::metamodel::type::Number[1]):meta::pure::metamodel::type::Float[1];");
    public static final NativeFunctionDefinition COUNT__T_MANY = signature("native function meta::pure::functions::collection::count<T>(values:T[*]):meta::pure::metamodel::type::Integer[1];");
    public static final NativeFunctionDefinition COVAR_POPULATION__NUMBER_MANY__NUMBER_MANY = signature("native function meta::pure::functions::math::covarPopulation(x:meta::pure::metamodel::type::Number[*], y:meta::pure::metamodel::type::Number[*]):meta::pure::metamodel::type::Number[1];");
    public static final NativeFunctionDefinition COVAR_POPULATION__ROW_MAPPER_MANY = signature("native function meta::pure::functions::math::covarPopulation<T,U>(values:meta::pure::functions::math::mathUtility::RowMapper<T,U>[*]):meta::pure::metamodel::type::Number[0..1];");
    public static final NativeFunctionDefinition COVAR_SAMPLE__NUMBER_MANY__NUMBER_MANY = signature("native function meta::pure::functions::math::covarSample(x:meta::pure::metamodel::type::Number[*], y:meta::pure::metamodel::type::Number[*]):meta::pure::metamodel::type::Number[1];");
    public static final NativeFunctionDefinition COVAR_SAMPLE__ROW_MAPPER_MANY = signature("native function meta::pure::functions::math::covarSample<T,U>(values:meta::pure::functions::math::mathUtility::RowMapper<T,U>[*]):meta::pure::metamodel::type::Number[0..1];");
    public static final NativeFunctionDefinition CUMULATIVE_DISTRIBUTION__RELATION_1__WINDOW_1__T_1 = signature("native function meta::pure::functions::relation::cumulativeDistribution<T>(rel:meta::pure::metamodel::relation::Relation<T>[1], w:meta::pure::functions::relation::_Window<T>[1], row:T[1]):meta::pure::metamodel::type::Float[1];");
    public static final NativeFunctionDefinition CURRENT_USER_ID = signature("native function meta::pure::functions::runtime::currentUserId():meta::pure::metamodel::type::String[1];");
    public static final NativeFunctionDefinition DATE_DIFF__DATE_1__DATE_1__DURATION_UNIT_1 = signature("native function meta::pure::functions::date::dateDiff(d1:meta::pure::metamodel::type::Date[1], d2:meta::pure::metamodel::type::Date[1], du:meta::pure::functions::date::DurationUnit[1]):meta::pure::metamodel::type::Integer[1];");
    public static final NativeFunctionDefinition DATE_PART__DATE_1 = signature("native function meta::pure::functions::date::datePart(d:meta::pure::metamodel::type::Date[1]):meta::pure::metamodel::type::StrictDate[1];");
    public static final NativeFunctionDefinition DATE__INTEGER_1 = signature("native function meta::pure::functions::date::date(year:meta::pure::metamodel::type::Integer[1]):meta::pure::metamodel::type::Date[1];");
    public static final NativeFunctionDefinition DATE__INTEGER_1__INTEGER_1 = signature("native function meta::pure::functions::date::date(year:meta::pure::metamodel::type::Integer[1], month:meta::pure::metamodel::type::Integer[1]):meta::pure::metamodel::type::Date[1];");
    public static final NativeFunctionDefinition DATE__INTEGER_1__INTEGER_1__INTEGER_1 = signature("native function meta::pure::functions::date::date(year:meta::pure::metamodel::type::Integer[1], month:meta::pure::metamodel::type::Integer[1], day:meta::pure::metamodel::type::Integer[1]):meta::pure::metamodel::type::StrictDate[1];");
    public static final NativeFunctionDefinition DATE__INTEGER_1__INTEGER_1__INTEGER_1__INTEGER_1 = signature("native function meta::pure::functions::date::date(year:meta::pure::metamodel::type::Integer[1], month:meta::pure::metamodel::type::Integer[1], day:meta::pure::metamodel::type::Integer[1], hour:meta::pure::metamodel::type::Integer[1]):meta::pure::metamodel::type::DateTime[1];");
    public static final NativeFunctionDefinition DATE__INTEGER_1__INTEGER_1__INTEGER_1__INTEGER_1__INTEGER_1 = signature("native function meta::pure::functions::date::date(year:meta::pure::metamodel::type::Integer[1], month:meta::pure::metamodel::type::Integer[1], day:meta::pure::metamodel::type::Integer[1], hour:meta::pure::metamodel::type::Integer[1], minute:meta::pure::metamodel::type::Integer[1]):meta::pure::metamodel::type::DateTime[1];");
    public static final NativeFunctionDefinition DATE__INTEGER_1__INTEGER_1__INTEGER_1__INTEGER_1__INTEGER_1__NUMBER_1 = signature("native function meta::pure::functions::date::date(year:meta::pure::metamodel::type::Integer[1], month:meta::pure::metamodel::type::Integer[1], day:meta::pure::metamodel::type::Integer[1], hour:meta::pure::metamodel::type::Integer[1], minute:meta::pure::metamodel::type::Integer[1], second:meta::pure::metamodel::type::Number[1]):meta::pure::metamodel::type::DateTime[1];");
    public static final NativeFunctionDefinition DAY_OF_MONTH__DATE_1 = signature("native function meta::pure::functions::date::dayOfMonth(d:meta::pure::metamodel::type::Date[1]):meta::pure::metamodel::type::Integer[1];");
    public static final NativeFunctionDefinition DAY_OF_WEEK_NUMBER__DATE_1 = signature("native function meta::pure::functions::date::dayOfWeekNumber(d:meta::pure::metamodel::type::Date[1]):meta::pure::metamodel::type::Integer[1];");
    public static final NativeFunctionDefinition DAY_OF_WEEK__DATE_1 = signature("native function meta::pure::functions::date::dayOfWeek(d:meta::pure::metamodel::type::Date[1]):meta::pure::functions::date::DayOfWeek[1];");
    public static final NativeFunctionDefinition DAY_OF_YEAR__DATE_1 = signature("native function meta::pure::functions::date::dayOfYear(d:meta::pure::metamodel::type::Date[1]):meta::pure::metamodel::type::Integer[1];");
    public static final NativeFunctionDefinition DECODE_BASE64__STRING_1 = signature("native function meta::pure::functions::string::decodeBase64(str:meta::pure::metamodel::type::String[1]):meta::pure::metamodel::type::String[1];");
    public static final NativeFunctionDefinition DENSE_RANK__RELATION_1__WINDOW_1__T_1 = signature("native function meta::pure::functions::relation::denseRank<T>(rel:meta::pure::metamodel::relation::Relation<T>[1], w:meta::pure::functions::relation::_Window<T>[1], row:T[1]):meta::pure::metamodel::type::Integer[1];");
    public static final NativeFunctionDefinition DESCENDING__COL_SPEC_1 = signature("native function meta::pure::functions::relation::descending<T>(column:meta::pure::metamodel::relation::ColSpec<T>[1]):meta::pure::functions::relation::SortInfo<T>[1];");
    public static final NativeFunctionDefinition DESC__COL_SPEC_1 = signature("native function meta::pure::tds::desc<T>(column:meta::pure::metamodel::relation::ColSpec<T>[1]):meta::pure::functions::relation::SortInfo<T>[1];");
    public static final NativeFunctionDefinition DISTINCT__RELATION_1 = signature("native function meta::pure::functions::relation::distinct<T>(rel:meta::pure::metamodel::relation::Relation<T>[1]):meta::pure::metamodel::relation::Relation<T>[1];");
    public static final NativeFunctionDefinition DISTINCT__RELATION_1__COL_SPEC_ARRAY_1 = signature("native function meta::pure::functions::relation::distinct<X,T>(rel:meta::pure::metamodel::relation::Relation<T>[1], columns:meta::pure::metamodel::relation::ColSpecArray<X⊆T>[1]):meta::pure::metamodel::relation::Relation<X>[1];");
    public static final NativeFunctionDefinition IS_NUMERIC__STRING_0_1 = signature("native function meta::legend::lite::isNumeric(str:meta::pure::metamodel::type::String[0..1]):meta::pure::metamodel::type::Boolean[0..1];");
    public static final NativeFunctionDefinition CONVERT_TIME_ZONE_FORMAT__DATE_0_1__STRING_1__STRING_1 = signature("native function meta::legend::lite::convertTimeZoneFormat(d:meta::pure::metamodel::type::DateTime[0..1], tz:meta::pure::metamodel::type::String[1], fmt:meta::pure::metamodel::type::String[1]):meta::pure::metamodel::type::String[0..1];");

    /** Relational FORMAT dynafunctions (convertDate('MMMyyyy') et al) — lite natives. */
    public static final NativeFunctionDefinition CONVERT_DATE_FORMAT__STRING_0_1__STRING_1 = signature("native function meta::legend::lite::convertDateFormat(str:meta::pure::metamodel::type::String[0..1], fmt:meta::pure::metamodel::type::String[1]):meta::pure::metamodel::type::StrictDate[0..1];");
    public static final NativeFunctionDefinition CONVERT_DATE_TIME_FORMAT__STRING_0_1__STRING_1 = signature("native function meta::legend::lite::convertDateTimeFormat(str:meta::pure::metamodel::type::String[0..1], fmt:meta::pure::metamodel::type::String[1]):meta::pure::metamodel::type::DateTime[0..1];");
    public static final NativeFunctionDefinition PARSE_DATE_FORMAT__STRING_0_1__STRING_1 = signature("native function meta::legend::lite::parseDateFormat(str:meta::pure::metamodel::type::String[0..1], fmt:meta::pure::metamodel::type::String[1]):meta::pure::metamodel::type::DateTime[0..1];");
    public static final NativeFunctionDefinition DIVIDE_ROUND__NUMBER_1__NUMBER_1__INTEGER_1 = signature("native function meta::legend::lite::divideRound(left:meta::pure::metamodel::type::Number[1], right:meta::pure::metamodel::type::Number[1], scale:meta::pure::metamodel::type::Integer[1]):meta::pure::metamodel::type::Float[1];");
    public static final NativeFunctionDefinition DIVIDE__NUMBER_1__NUMBER_1 = signature("native function meta::pure::functions::math::divide(dividend:meta::pure::metamodel::type::Number[1], divisor:meta::pure::metamodel::type::Number[1]):meta::pure::metamodel::type::Float[1];");
    public static final NativeFunctionDefinition DIVIDE__NUMBER_1__NUMBER_1__INTEGER_1 = signature("native function meta::pure::functions::math::divide(dividend:meta::pure::metamodel::type::Number[1], divisor:meta::pure::metamodel::type::Number[1], scale:meta::pure::metamodel::type::Integer[1]):meta::pure::metamodel::type::Decimal[1];");
    public static final NativeFunctionDefinition DROP__RELATION_1__INTEGER_1 = signature("native function meta::pure::functions::relation::drop<T>(rel:meta::pure::metamodel::relation::Relation<T>[1], size:meta::pure::metamodel::type::Integer[1]):meta::pure::metamodel::relation::Relation<T>[1];");
    public static final NativeFunctionDefinition DROP__T_MANY__INTEGER_1 = signature("native function meta::pure::functions::collection::drop<T>(set:T[*], count:meta::pure::metamodel::type::Integer[1]):T[*];");
    public static final NativeFunctionDefinition ENCODE_BASE64__STRING_1 = signature("native function meta::pure::functions::string::encodeBase64(str:meta::pure::metamodel::type::String[1]):meta::pure::metamodel::type::String[1];");
    public static final NativeFunctionDefinition ENDS_WITH__STRING_1__STRING_1 = signature("native function meta::pure::functions::string::endsWith(source:meta::pure::metamodel::type::String[1], val:meta::pure::metamodel::type::String[1]):meta::pure::metamodel::type::Boolean[1];");
    // VERIFIED vs real legend-pure grammar/functions/boolean/equality/equal.pure:
    // equal(left:Any[*], right:Any[*]):Boolean[1] — collection equality is part
    // of the contract (identity/primitive/collection/model-defined equality).
    public static final NativeFunctionDefinition EQUAL__ANY_MANY__ANY_MANY = signature("native function meta::pure::functions::boolean::equal(left:meta::pure::metamodel::type::Any[*], right:meta::pure::metamodel::type::Any[*]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition EQ__ANY_1__ANY_1 = signature("native function meta::pure::functions::boolean::eq(left:meta::pure::metamodel::type::Any[1], right:meta::pure::metamodel::type::Any[1]):meta::pure::metamodel::type::Boolean[1];");
    // VERBATIM real pure (platform/pure/essential/lang/eval/eval.pure),
    // arities 1-3 (real pure goes to 8; add on demand). Typed via the
    // kernel's FunctionType unification for function VALUES; lambda-literal
    // and colspec sources short-circuit in EvalChecker.
    public static final NativeFunctionDefinition EVAL__FUNCTION_1 = signature("native function meta::pure::functions::lang::eval<V|m>(func:meta::pure::metamodel::function::Function<{->V[m]}>[1]):V[m];");
    public static final NativeFunctionDefinition EVAL__FUNCTION_1__T_n = signature("native function meta::pure::functions::lang::eval<T,V|m,n>(func:meta::pure::metamodel::function::Function<{T[n]->V[m]}>[1], param:T[n]):V[m];");
    public static final NativeFunctionDefinition EVAL__FUNCTION_1__T_n__U_p = signature("native function meta::pure::functions::lang::eval<T,U,V|m,n,p>(func:meta::pure::metamodel::function::Function<{T[n],U[p]->V[m]}>[1], param1:T[n], param2:U[p]):V[m];");
    public static final NativeFunctionDefinition EXISTS__T_MANY__FUNCTION_1 = signature("native function meta::pure::functions::collection::exists<T>(value:T[*], func:meta::pure::metamodel::function::Function<{T[1]->meta::pure::metamodel::type::Boolean[1]}>[1]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition EXP__NUMBER_1 = signature("native function meta::pure::functions::math::exp(exponent:meta::pure::metamodel::type::Number[1]):meta::pure::metamodel::type::Float[1];");
    public static final NativeFunctionDefinition EXTEND__C_MANY__FUNC_COL_SPEC_1 = signature("native function meta::pure::functions::relation::extend<C,Z>(cl:C[*], f:meta::pure::metamodel::relation::FuncColSpec<{C[1]->meta::pure::metamodel::type::Any[0..1]},Z>[1]):C[*];");
    public static final NativeFunctionDefinition EXTEND__C_MANY__FUNC_COL_SPEC_ARRAY_1 = signature("native function meta::pure::functions::relation::extend<C,Z>(cl:C[*], fs:meta::pure::metamodel::relation::FuncColSpecArray<{C[1]->meta::pure::metamodel::type::Any[*]},Z>[1]):C[*];");
    public static final NativeFunctionDefinition EXTEND__RELATION_1__AGG_COL_SPEC_1 = signature("native function meta::pure::functions::relation::extend<T,K,V,R>(r:meta::pure::metamodel::relation::Relation<T>[1], agg:meta::pure::metamodel::relation::AggColSpec<{T[1]->K[0..1]},{K[*]->V[0..1]},R>[1]):meta::pure::metamodel::relation::Relation<T+R>[1];");
    public static final NativeFunctionDefinition EXTEND__RELATION_1__AGG_COL_SPEC_ARRAY_1 = signature("native function meta::pure::functions::relation::extend<T,K,V,R>(r:meta::pure::metamodel::relation::Relation<T>[1], agg:meta::pure::metamodel::relation::AggColSpecArray<{T[1]->K[0..1]},{K[*]->V[0..1]},R>[1]):meta::pure::metamodel::relation::Relation<T+R>[1];");
    public static final NativeFunctionDefinition EXTEND__RELATION_1__FUNC_COL_SPEC_1 = signature("native function meta::pure::functions::relation::extend<T,Z>(r:meta::pure::metamodel::relation::Relation<T>[1], f:meta::pure::metamodel::relation::FuncColSpec<{T[1]->meta::pure::metamodel::type::Any[0..1]},Z>[1]):meta::pure::metamodel::relation::Relation<T+Z>[1];");
    public static final NativeFunctionDefinition EXTEND__RELATION_1__FUNC_COL_SPEC_ARRAY_1 = signature("native function meta::pure::functions::relation::extend<T,Z>(r:meta::pure::metamodel::relation::Relation<T>[1], fs:meta::pure::metamodel::relation::FuncColSpecArray<{T[1]->meta::pure::metamodel::type::Any[*]},Z>[1]):meta::pure::metamodel::relation::Relation<T+Z>[1];");
    public static final NativeFunctionDefinition EXTEND__RELATION_1__TRAVERSAL_1__FUNC_COL_SPEC_1 = signature("native function meta::pure::functions::relation::extend<S,T,Z>(r:meta::pure::metamodel::relation::Relation<S>[1], path:meta::pure::functions::relation::_Traversal[1], f:meta::pure::metamodel::relation::FuncColSpec<{S[1],T[1]->meta::pure::metamodel::type::Any[0..1]},Z>[1]):meta::pure::metamodel::relation::Relation<S+Z>[1];");
    public static final NativeFunctionDefinition EXTEND__RELATION_1__TRAVERSAL_1__FUNC_COL_SPEC_ARRAY_1 = signature("native function meta::pure::functions::relation::extend<S,T,Z>(r:meta::pure::metamodel::relation::Relation<S>[1], path:meta::pure::functions::relation::_Traversal[1], fs:meta::pure::metamodel::relation::FuncColSpecArray<{S[1],T[1]->meta::pure::metamodel::type::Any[*]},Z>[1]):meta::pure::metamodel::relation::Relation<S+Z>[1];");
    public static final NativeFunctionDefinition EXTEND__RELATION_1__WINDOW_1__AGG_COL_SPEC_1 = signature("native function meta::pure::functions::relation::extend<T,K,V,R>(r:meta::pure::metamodel::relation::Relation<T>[1], window:meta::pure::functions::relation::_Window<T>[1], agg:meta::pure::metamodel::relation::AggColSpec<{meta::pure::metamodel::relation::Relation<T>[1],meta::pure::functions::relation::_Window<T>[1],T[1]->K[0..1]},{K[*]->V[0..1]},R>[1]):meta::pure::metamodel::relation::Relation<T+R>[1];");
    public static final NativeFunctionDefinition EXTEND__RELATION_1__WINDOW_1__AGG_COL_SPEC_ARRAY_1 = signature("native function meta::pure::functions::relation::extend<T,K,V,R>(r:meta::pure::metamodel::relation::Relation<T>[1], window:meta::pure::functions::relation::_Window<T>[1], agg:meta::pure::metamodel::relation::AggColSpecArray<{meta::pure::metamodel::relation::Relation<T>[1],meta::pure::functions::relation::_Window<T>[1],T[1]->K[0..1]},{K[*]->V[0..1]},R>[1]):meta::pure::metamodel::relation::Relation<T+R>[1];");
    public static final NativeFunctionDefinition EXTEND__RELATION_1__WINDOW_1__FUNC_COL_SPEC_1 = signature("native function meta::pure::functions::relation::extend<T,Z,W,R>(r:meta::pure::metamodel::relation::Relation<T>[1], window:meta::pure::functions::relation::_Window<T>[1], f:meta::pure::metamodel::relation::FuncColSpec<{meta::pure::metamodel::relation::Relation<T>[1],meta::pure::functions::relation::_Window<T>[1],T[1]->meta::pure::metamodel::type::Any[0..1]},R>[1]):meta::pure::metamodel::relation::Relation<T+R>[1];");
    public static final NativeFunctionDefinition EXTEND__RELATION_1__WINDOW_1__FUNC_COL_SPEC_ARRAY_1 = signature("native function meta::pure::functions::relation::extend<T,Z,W,R>(r:meta::pure::metamodel::relation::Relation<T>[1], window:meta::pure::functions::relation::_Window<T>[1], f:meta::pure::metamodel::relation::FuncColSpecArray<{meta::pure::metamodel::relation::Relation<T>[1],meta::pure::functions::relation::_Window<T>[1],T[1]->meta::pure::metamodel::type::Any[*]},R>[1]):meta::pure::metamodel::relation::Relation<T+R>[1];");
    public static final NativeFunctionDefinition FILTER__RELATION_1__FUNCTION_1 = signature("native function meta::pure::functions::relation::filter<T>(rel:meta::pure::metamodel::relation::Relation<T>[1], f:meta::pure::metamodel::function::Function<{T[1]->meta::pure::metamodel::type::Boolean[1]}>[1]):meta::pure::metamodel::relation::Relation<T>[1];");
    public static final NativeFunctionDefinition FILTER__T_MANY__FUNCTION_1 = signature("native function meta::pure::functions::collection::filter<T>(value:T[*], func:meta::pure::metamodel::function::Function<{T[1]->meta::pure::metamodel::type::Boolean[1]}>[1]):T[*];");
    public static final NativeFunctionDefinition FIND__T_MANY__FUNCTION_1 = signature("native function meta::pure::functions::collection::find<T>(value:T[*], func:meta::pure::metamodel::function::Function<{T[1]->meta::pure::metamodel::type::Boolean[1]}>[1]):T[0..1];");
    public static final NativeFunctionDefinition FIRST_DAY_OF_MONTH__DATE_1 = signature("native function meta::pure::functions::date::firstDayOfMonth(d:meta::pure::metamodel::type::Date[1]):meta::pure::metamodel::type::Date[1];");
    public static final NativeFunctionDefinition FIRST_DAY_OF_QUARTER__DATE_1 = signature("native function meta::pure::functions::date::firstDayOfQuarter(d:meta::pure::metamodel::type::Date[1]):meta::pure::metamodel::type::StrictDate[1];");
    public static final NativeFunctionDefinition FIRST_DAY_OF_YEAR__DATE_1 = signature("native function meta::pure::functions::date::firstDayOfYear(d:meta::pure::metamodel::type::Date[1]):meta::pure::metamodel::type::Date[1];");
    public static final NativeFunctionDefinition FIRST_HOUR_OF_DAY__DATE_1 = signature("native function meta::pure::functions::date::firstHourOfDay(d:meta::pure::metamodel::type::Date[1]):meta::pure::metamodel::type::DateTime[1];");
    public static final NativeFunctionDefinition FIRST_MILLISECOND_OF_SECOND__DATE_1 = signature("native function meta::pure::functions::date::firstMillisecondOfSecond(d:meta::pure::metamodel::type::Date[1]):meta::pure::metamodel::type::DateTime[1];");
    public static final NativeFunctionDefinition FIRST_MINUTE_OF_HOUR__DATE_1 = signature("native function meta::pure::functions::date::firstMinuteOfHour(d:meta::pure::metamodel::type::Date[1]):meta::pure::metamodel::type::DateTime[1];");
    public static final NativeFunctionDefinition FIRST_SECOND_OF_MINUTE__DATE_1 = signature("native function meta::pure::functions::date::firstSecondOfMinute(d:meta::pure::metamodel::type::Date[1]):meta::pure::metamodel::type::DateTime[1];");
    // Param names w:Relation, f:_Window are VERBATIM real pure
    // (core_functions_relation/relation/functions/slice/first.pure) — kept
    // faithful even though they read swapped.
    public static final NativeFunctionDefinition FIRST__RELATION_1__WINDOW_1__T_1 = signature("native function meta::pure::functions::relation::first<T>(w:meta::pure::metamodel::relation::Relation<T>[1], f:meta::pure::functions::relation::_Window<T>[1], r:T[1]):T[0..1];");
    public static final NativeFunctionDefinition FIRST__T_MANY = signature("native function meta::pure::functions::collection::first<T>(set:T[*]):T[0..1];");
    public static final NativeFunctionDefinition FIRST__T_MANY__INTEGER_1 = signature("native function meta::pure::functions::collection::first<T>(set:T[*], count:meta::pure::metamodel::type::Integer[1]):T[*];");
    public static final NativeFunctionDefinition FLATTEN__T_MANY__COL_SPEC_1 = signature("native function meta::pure::functions::relation::variant::flatten<T,Z>(valueToFlatten:T[*], columnWithFlattenedValue:meta::pure::metamodel::relation::ColSpec<Z=(?:T)>[1]):meta::pure::metamodel::relation::Relation<Z>[1];");
    public static final NativeFunctionDefinition FLOOR__NUMBER_1 = signature("native function meta::pure::functions::math::floor(number:meta::pure::metamodel::type::Number[1]):meta::pure::metamodel::type::Integer[1];");
    public static final NativeFunctionDefinition FOLD__T_MANY__FUNCTION_1__V_m = signature("native function meta::pure::functions::collection::fold<T,V|m>(source:T[*], lambda:meta::pure::metamodel::function::Function<{T[1],V[m]->V[m]}>[1], init:V[m]):V[m];");
    public static final NativeFunctionDefinition FORMAT__STRING_1__ANY_MANY = signature("native function meta::pure::functions::string::format(format:meta::pure::metamodel::type::String[1], args:meta::pure::metamodel::type::Any[*]):meta::pure::metamodel::type::String[1];");
    public static final NativeFunctionDefinition FOR_ALL__T_MANY__FUNCTION_1 = signature("native function meta::pure::functions::collection::forAll<T>(value:T[*], func:meta::pure::metamodel::function::Function<{T[1]->meta::pure::metamodel::type::Boolean[1]}>[1]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition FROM_EPOCH_VALUE__INTEGER_1 = signature("native function meta::pure::functions::date::fromEpochValue(epoch:meta::pure::metamodel::type::Integer[1]):meta::pure::metamodel::type::Date[1];");
    public static final NativeFunctionDefinition FROM_EPOCH_VALUE__INTEGER_1__DURATION_UNIT_1 = signature("native function meta::pure::functions::date::fromEpochValue(epoch:meta::pure::metamodel::type::Integer[1], unit:meta::pure::functions::date::DurationUnit[1]):meta::pure::metamodel::type::Date[1];");
    public static final NativeFunctionDefinition FROM__RELATION_1 = signature("native function meta::pure::mapping::from<T>(source:meta::pure::metamodel::relation::Relation<T>[1]):meta::pure::metamodel::relation::Relation<T>[1];");
    public static final NativeFunctionDefinition FROM__RELATION_1__ANY_1 = signature("native function meta::pure::mapping::from<T>(source:meta::pure::metamodel::relation::Relation<T>[1], runtime:meta::pure::metamodel::type::Any[1]):meta::pure::metamodel::relation::Relation<T>[1];");
    public static final NativeFunctionDefinition FROM__T_MANY__ANY_1__ANY_1 = signature("native function meta::pure::mapping::from<T>(source:T[*], mapping:meta::pure::metamodel::type::Any[1], runtime:meta::pure::metamodel::type::Any[1]):T[*];");
    public static final NativeFunctionDefinition GENERATE_GUID = signature("native function meta::pure::functions::string::generation::generateGuid():meta::pure::metamodel::type::String[1];");
    public static final NativeFunctionDefinition GET_ALL__CLASS_1 = signature("native function meta::pure::functions::collection::getAll<T>(class:meta::pure::metamodel::type::Class<T>[1]):T[*];");
    public static final NativeFunctionDefinition GET_ALL__CLASS_1__DATE_1 = signature("native function meta::pure::functions::collection::getAll<T>(class:meta::pure::metamodel::type::Class<T>[1], date:meta::pure::metamodel::type::Date[1]):T[*];");
    public static final NativeFunctionDefinition GET_ALL__CLASS_1__DATE_1__DATE_1 = signature("native function meta::pure::functions::collection::getAll<T>(class:meta::pure::metamodel::type::Class<T>[1], from:meta::pure::metamodel::type::Date[1], to:meta::pure::metamodel::type::Date[1]):T[*];");
    public static final NativeFunctionDefinition GET_ALL_VERSIONS__CLASS_1 = signature("native function meta::pure::functions::collection::getAllVersions<T>(class:meta::pure::metamodel::type::Class<T>[1]):T[*];");
    public static final NativeFunctionDefinition GET_ALL_VERSIONS_IN_RANGE__CLASS_1__DATE_1__DATE_1 = signature("native function meta::pure::functions::collection::getAllVersionsInRange<T>(class:meta::pure::metamodel::type::Class<T>[1], start:meta::pure::metamodel::type::Date[1], end:meta::pure::metamodel::type::Date[1]):T[*];");
    public static final NativeFunctionDefinition GET__VARIANT_1__ANY_1 = signature("native function meta::pure::functions::variant::navigation::get(source:meta::pure::metamodel::variant::Variant[1], key:meta::pure::metamodel::type::Any[1]):meta::pure::metamodel::variant::Variant[0..1];");
    public static final NativeFunctionDefinition GRAPH_FETCH__T_MANY__COL_SPEC_1 = signature("native function meta::pure::graphFetch::execution::graphFetch<T>(source:T[*], col:meta::pure::metamodel::relation::ColSpec<T>[1]):T[*];");
    public static final NativeFunctionDefinition GRAPH_FETCH__T_MANY__COL_SPEC_ARRAY_1 = signature("native function meta::pure::graphFetch::execution::graphFetch<T>(source:T[*], cols:meta::pure::metamodel::relation::ColSpecArray<T>[1]):T[*];");
    public static final NativeFunctionDefinition GRAPH_FETCH__T_MANY__ROOT_GRAPH_FETCH_TREE_1 = signature("native function meta::pure::graphFetch::execution::graphFetch<T>(source:T[*], tree:meta::pure::graphFetch::RootGraphFetchTree<T>[1]):T[*];");
    public static final NativeFunctionDefinition GRAPH_FETCH__T_MANY__ROOT_GRAPH_FETCH_TREE_1__INTEGER_1 = signature("native function meta::pure::graphFetch::execution::graphFetch<T>(source:T[*], tree:meta::pure::graphFetch::RootGraphFetchTree<T>[1], batchSize:meta::pure::metamodel::type::Integer[1]):T[*];");
    public static final NativeFunctionDefinition GREATER_THAN_EQUAL__DATE_0_1__DATE_0_1 = signature("native function meta::pure::functions::boolean::greaterThanEqual(left:meta::pure::metamodel::type::Date[0..1], right:meta::pure::metamodel::type::Date[0..1]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition GREATER_THAN_EQUAL__DATE_0_1__DATE_1 = signature("native function meta::pure::functions::boolean::greaterThanEqual(left:meta::pure::metamodel::type::Date[0..1], right:meta::pure::metamodel::type::Date[1]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition GREATER_THAN_EQUAL__DATE_1__DATE_0_1 = signature("native function meta::pure::functions::boolean::greaterThanEqual(left:meta::pure::metamodel::type::Date[1], right:meta::pure::metamodel::type::Date[0..1]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition GREATER_THAN_EQUAL__DATE_1__DATE_1 = signature("native function meta::pure::functions::boolean::greaterThanEqual(left:meta::pure::metamodel::type::Date[1], right:meta::pure::metamodel::type::Date[1]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition GREATER_THAN_EQUAL__NUMBER_0_1__NUMBER_0_1 = signature("native function meta::pure::functions::boolean::greaterThanEqual(left:meta::pure::metamodel::type::Number[0..1], right:meta::pure::metamodel::type::Number[0..1]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition GREATER_THAN_EQUAL__NUMBER_0_1__NUMBER_1 = signature("native function meta::pure::functions::boolean::greaterThanEqual(left:meta::pure::metamodel::type::Number[0..1], right:meta::pure::metamodel::type::Number[1]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition GREATER_THAN_EQUAL__NUMBER_1__NUMBER_0_1 = signature("native function meta::pure::functions::boolean::greaterThanEqual(left:meta::pure::metamodel::type::Number[1], right:meta::pure::metamodel::type::Number[0..1]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition GREATER_THAN_EQUAL__NUMBER_1__NUMBER_1 = signature("native function meta::pure::functions::boolean::greaterThanEqual(left:meta::pure::metamodel::type::Number[1], right:meta::pure::metamodel::type::Number[1]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition GREATER_THAN_EQUAL__STRING_0_1__STRING_0_1 = signature("native function meta::pure::functions::boolean::greaterThanEqual(left:meta::pure::metamodel::type::String[0..1], right:meta::pure::metamodel::type::String[0..1]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition GREATER_THAN_EQUAL__STRING_0_1__STRING_1 = signature("native function meta::pure::functions::boolean::greaterThanEqual(left:meta::pure::metamodel::type::String[0..1], right:meta::pure::metamodel::type::String[1]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition GREATER_THAN_EQUAL__STRING_1__STRING_0_1 = signature("native function meta::pure::functions::boolean::greaterThanEqual(left:meta::pure::metamodel::type::String[1], right:meta::pure::metamodel::type::String[0..1]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition GREATER_THAN_EQUAL__STRING_1__STRING_1 = signature("native function meta::pure::functions::boolean::greaterThanEqual(left:meta::pure::metamodel::type::String[1], right:meta::pure::metamodel::type::String[1]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition GREATER_THAN__DATE_0_1__DATE_0_1 = signature("native function meta::pure::functions::boolean::greaterThan(left:meta::pure::metamodel::type::Date[0..1], right:meta::pure::metamodel::type::Date[0..1]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition GREATER_THAN__DATE_0_1__DATE_1 = signature("native function meta::pure::functions::boolean::greaterThan(left:meta::pure::metamodel::type::Date[0..1], right:meta::pure::metamodel::type::Date[1]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition GREATER_THAN__DATE_1__DATE_0_1 = signature("native function meta::pure::functions::boolean::greaterThan(left:meta::pure::metamodel::type::Date[1], right:meta::pure::metamodel::type::Date[0..1]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition GREATER_THAN__DATE_1__DATE_1 = signature("native function meta::pure::functions::boolean::greaterThan(left:meta::pure::metamodel::type::Date[1], right:meta::pure::metamodel::type::Date[1]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition GREATER_THAN__NUMBER_0_1__NUMBER_0_1 = signature("native function meta::pure::functions::boolean::greaterThan(left:meta::pure::metamodel::type::Number[0..1], right:meta::pure::metamodel::type::Number[0..1]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition GREATER_THAN__NUMBER_0_1__NUMBER_1 = signature("native function meta::pure::functions::boolean::greaterThan(left:meta::pure::metamodel::type::Number[0..1], right:meta::pure::metamodel::type::Number[1]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition GREATER_THAN__NUMBER_1__NUMBER_0_1 = signature("native function meta::pure::functions::boolean::greaterThan(left:meta::pure::metamodel::type::Number[1], right:meta::pure::metamodel::type::Number[0..1]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition GREATER_THAN__NUMBER_1__NUMBER_1 = signature("native function meta::pure::functions::boolean::greaterThan(left:meta::pure::metamodel::type::Number[1], right:meta::pure::metamodel::type::Number[1]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition GREATER_THAN__STRING_0_1__STRING_0_1 = signature("native function meta::pure::functions::boolean::greaterThan(left:meta::pure::metamodel::type::String[0..1], right:meta::pure::metamodel::type::String[0..1]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition GREATER_THAN__STRING_0_1__STRING_1 = signature("native function meta::pure::functions::boolean::greaterThan(left:meta::pure::metamodel::type::String[0..1], right:meta::pure::metamodel::type::String[1]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition GREATER_THAN__STRING_1__STRING_0_1 = signature("native function meta::pure::functions::boolean::greaterThan(left:meta::pure::metamodel::type::String[1], right:meta::pure::metamodel::type::String[0..1]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition GREATER_THAN__STRING_1__STRING_1 = signature("native function meta::pure::functions::boolean::greaterThan(left:meta::pure::metamodel::type::String[1], right:meta::pure::metamodel::type::String[1]):meta::pure::metamodel::type::Boolean[1];");
    // greatest/least: REAL pure is GENERIC (legend-engine core_functions_standard/collection/{greatest,least}.pure).
    public static final NativeFunctionDefinition GREATEST__X_MANY = signature("native function meta::pure::functions::collection::greatest<X>(values:X[*]):X[0..1];");
    public static final NativeFunctionDefinition GREATEST__X_1_MANY = signature("native function meta::pure::functions::collection::greatest<X>(values:X[1..*]):X[1];");
    public static final NativeFunctionDefinition GROUP_BY__C_MANY__FUNC_COL_SPEC_ARRAY_1__AGG_COL_SPEC_1 = signature("native function meta::pure::tds::groupBy<C,Z,K,V,R>(cl:C[*], keys:meta::pure::metamodel::relation::FuncColSpecArray<{C[1]->meta::pure::metamodel::type::Any[*]},Z>[1], aggs:meta::pure::metamodel::relation::AggColSpec<{C[1]->K[0..1]},{K[*]->V[0..1]},R>[1]):meta::pure::metamodel::relation::Relation<Z+R>[1];");
    public static final NativeFunctionDefinition GROUP_BY__C_MANY__FUNC_COL_SPEC_ARRAY_1__AGG_COL_SPEC_ARRAY_1 = signature("native function meta::pure::tds::groupBy<C,Z,K,V,R>(cl:C[*], keys:meta::pure::metamodel::relation::FuncColSpecArray<{C[1]->meta::pure::metamodel::type::Any[*]},Z>[1], aggs:meta::pure::metamodel::relation::AggColSpecArray<{C[1]->K[0..1]},{K[*]->V[0..1]},R>[1]):meta::pure::metamodel::relation::Relation<Z+R>[1];");
    public static final NativeFunctionDefinition GROUP_BY__K_MANY__FUNCTION_MANY__ANY_MANY__STRING_MANY = signature("native function meta::pure::functions::collection::groupBy<K,V,U>(set:K[*], fns:meta::pure::metamodel::function::Function<{K[1]->meta::pure::metamodel::type::Any[*]}>[*], aggs:meta::pure::metamodel::type::Any[*], ids:meta::pure::metamodel::type::String[*]):meta::pure::metamodel::relation::Relation<K>[1];");
    public static final NativeFunctionDefinition GROUP_BY__RELATION_1__COL_SPEC_1__AGG_COL_SPEC_1 = signature("native function meta::pure::functions::relation::groupBy<T,Z,K,V,R>(r:meta::pure::metamodel::relation::Relation<T>[1], cols:meta::pure::metamodel::relation::ColSpec<Z⊆T>[1], agg:meta::pure::metamodel::relation::AggColSpec<{T[1]->K[0..1]},{K[*]->V[0..1]},R>[1]):meta::pure::metamodel::relation::Relation<Z+R>[1];");
    public static final NativeFunctionDefinition GROUP_BY__RELATION_1__COL_SPEC_1__AGG_COL_SPEC_ARRAY_1 = signature("native function meta::pure::functions::relation::groupBy<T,Z,K,V,R>(r:meta::pure::metamodel::relation::Relation<T>[1], cols:meta::pure::metamodel::relation::ColSpec<Z⊆T>[1], agg:meta::pure::metamodel::relation::AggColSpecArray<{T[1]->K[0..1]},{K[*]->V[0..1]},R>[1]):meta::pure::metamodel::relation::Relation<Z+R>[1];");
    public static final NativeFunctionDefinition GROUP_BY__RELATION_1__COL_SPEC_ARRAY_1__AGG_COL_SPEC_1 = signature("native function meta::pure::functions::relation::groupBy<T,Z,K,V,R>(r:meta::pure::metamodel::relation::Relation<T>[1], cols:meta::pure::metamodel::relation::ColSpecArray<Z⊆T>[1], agg:meta::pure::metamodel::relation::AggColSpec<{T[1]->K[0..1]},{K[*]->V[0..1]},R>[1]):meta::pure::metamodel::relation::Relation<Z+R>[1];");
    public static final NativeFunctionDefinition GROUP_BY__RELATION_1__COL_SPEC_ARRAY_1__AGG_COL_SPEC_ARRAY_1 = signature("native function meta::pure::functions::relation::groupBy<T,Z,K,V,R>(r:meta::pure::metamodel::relation::Relation<T>[1], cols:meta::pure::metamodel::relation::ColSpecArray<Z⊆T>[1], agg:meta::pure::metamodel::relation::AggColSpecArray<{T[1]->K[0..1]},{K[*]->V[0..1]},R>[1]):meta::pure::metamodel::relation::Relation<Z+R>[1];");
    public static final NativeFunctionDefinition HASH_CODE__ANY_MANY = signature("native function meta::pure::functions::hash::hashCode(val:meta::pure::metamodel::type::Any[*]):meta::pure::metamodel::type::Integer[1];");
    // lite convenience; REAL pure hashing is hash(text, HashType) below.
    public static final NativeFunctionDefinition HASH__STRING_1 = signature("native function meta::legend::lite::hash(str:meta::pure::metamodel::type::String[1]):meta::pure::metamodel::type::String[1];");
    public static final NativeFunctionDefinition HASH__STRING_1__HASH_TYPE_1 = signature("native function meta::pure::functions::hash::hash(str:meta::pure::metamodel::type::String[1], algorithm:meta::pure::functions::hash::HashType[1]):meta::pure::metamodel::type::String[1];");
    public static final NativeFunctionDefinition HAS_DAY__DATE_1 = signature("native function meta::pure::functions::date::hasDay(d:meta::pure::metamodel::type::Date[1]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition HAS_HOUR__DATE_1 = signature("native function meta::pure::functions::date::hasHour(d:meta::pure::metamodel::type::Date[1]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition HAS_MINUTE__DATE_1 = signature("native function meta::pure::functions::date::hasMinute(d:meta::pure::metamodel::type::Date[1]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition HAS_MONTH__DATE_1 = signature("native function meta::pure::functions::date::hasMonth(d:meta::pure::metamodel::type::Date[1]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition HAS_SECOND__DATE_1 = signature("native function meta::pure::functions::date::hasSecond(d:meta::pure::metamodel::type::Date[1]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition HAS_SUBSECOND_WITH_AT_LEAST_PRECISION__DATE_1__INTEGER_1 = signature("native function meta::pure::functions::date::hasSubsecondWithAtLeastPrecision(d:meta::pure::metamodel::type::Date[1], precision:meta::pure::metamodel::type::Integer[1]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition HAS_SUBSECOND__DATE_1 = signature("native function meta::pure::functions::date::hasSubsecond(d:meta::pure::metamodel::type::Date[1]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition HEAD__T_MANY = signature("native function meta::pure::functions::collection::head<T>(set:T[*]):T[0..1];");
    public static final NativeFunctionDefinition HOUR__DATE_1 = signature("native function meta::pure::functions::date::hour(d:meta::pure::metamodel::type::Date[1]):meta::pure::metamodel::type::Integer[1];");
    // Real legend-pure (essential/lang/flow/if.pure): if<T|m>(Boolean[1], {->T[m]}, {->T[m]}):T[m].
    // The multiplicity VARIABLE m is shared by both branches and the result, so the result multiplicity
    // is the branches' (engine-lite dropped m and returned [*]/forced [1] — the bug flagged in §4.2).
    public static final NativeFunctionDefinition IF__BOOLEAN_1__FUNCTION_1__FUNCTION_1 = signature("native function meta::pure::functions::lang::if<T|m>(test:meta::pure::metamodel::type::Boolean[1], then:meta::pure::metamodel::function::Function<{->T[m]}>[1], else:meta::pure::metamodel::function::Function<{->T[m]}>[1]):T[m];");
    public static final NativeFunctionDefinition IF__PAIR_MANY__FUNCTION_1 = signature("native function meta::pure::functions::lang::if<T|m>(condList:meta::pure::functions::collection::Pair<meta::pure::metamodel::function::Function<{->meta::pure::metamodel::type::Boolean[1]}>,meta::pure::metamodel::function::Function<{->T[m]}>>[*], last:meta::pure::metamodel::function::Function<{->T[m]}>[1]):T[m];");
    public static final NativeFunctionDefinition INDEX_OF__STRING_1__STRING_1 = signature("native function meta::pure::functions::string::indexOf(str:meta::pure::metamodel::type::String[1], toFind:meta::pure::metamodel::type::String[1]):meta::pure::metamodel::type::Integer[1];");
    public static final NativeFunctionDefinition INDEX_OF__STRING_1__STRING_1__INTEGER_1 = signature("native function meta::pure::functions::string::indexOf(str:meta::pure::metamodel::type::String[1], toFind:meta::pure::metamodel::type::String[1], fromIndex:meta::pure::metamodel::type::Integer[1]):meta::pure::metamodel::type::Integer[1];");
    public static final NativeFunctionDefinition INDEX_OF__T_MANY__T_1 = signature("native function meta::pure::functions::collection::indexOf<T>(set:T[*], value:T[1]):meta::pure::metamodel::type::Integer[1];");
    public static final NativeFunctionDefinition INIT__T_MANY = signature("native function meta::pure::functions::collection::init<T>(set:T[*]):T[*];");
    public static final NativeFunctionDefinition INSTANCE_OF__ANY_1__TYPE_1 = signature("native function meta::pure::functions::meta::instanceOf(instance:meta::pure::metamodel::type::Any[1], type:meta::pure::metamodel::type::Type[1]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition IN__ANY_1__ANY_MANY = signature("native function meta::pure::functions::collection::in(value:meta::pure::metamodel::type::Any[1], collection:meta::pure::metamodel::type::Any[*]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition IN__ANY_0_1__ANY_MANY = signature("native function meta::pure::functions::collection::in(value:meta::pure::metamodel::type::Any[0..1], collection:meta::pure::metamodel::type::Any[*]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition IS_AFTER_DAY__DATE_1__DATE_1 = signature("native function meta::pure::functions::date::isAfterDay(d1:meta::pure::metamodel::type::Date[1], d2:meta::pure::metamodel::type::Date[1]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition IS_BEFORE_DAY__DATE_1__DATE_1 = signature("native function meta::pure::functions::date::isBeforeDay(d1:meta::pure::metamodel::type::Date[1], d2:meta::pure::metamodel::type::Date[1]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition IS_DISTINCT__ANY_1__ANY_1 = signature("native function meta::pure::functions::collection::isDistinct(left:meta::pure::metamodel::type::Any[1], right:meta::pure::metamodel::type::Any[1]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition IS_EMPTY__T_MANY = signature("native function meta::pure::functions::collection::isEmpty<T>(value:T[*]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition IS_NOT_EMPTY__T_MANY = signature("native function meta::pure::functions::collection::isNotEmpty<T>(value:T[*]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition IS_ON_DAY__DATE_1__DATE_1 = signature("native function meta::pure::functions::date::isOnDay(d1:meta::pure::metamodel::type::Date[1], d2:meta::pure::metamodel::type::Date[1]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition IS_ON_OR_AFTER_DAY__DATE_1__DATE_1 = signature("native function meta::pure::functions::date::isOnOrAfterDay(d1:meta::pure::metamodel::type::Date[1], d2:meta::pure::metamodel::type::Date[1]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition IS_ON_OR_BEFORE_DAY__DATE_1__DATE_1 = signature("native function meta::pure::functions::date::isOnOrBeforeDay(d1:meta::pure::metamodel::type::Date[1], d2:meta::pure::metamodel::type::Date[1]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition JARO_WINKLER_SIMILARITY__STRING_1__STRING_1 = signature("native function meta::pure::functions::string::jaroWinklerSimilarity(s1:meta::pure::metamodel::type::String[1], s2:meta::pure::metamodel::type::String[1]):meta::pure::metamodel::type::Float[1];");
    public static final NativeFunctionDefinition MAKE_STRING__ANY_MANY = signature("native function meta::pure::functions::string::makeString(any:meta::pure::metamodel::type::Any[*]):meta::pure::metamodel::type::String[1];");
    public static final NativeFunctionDefinition MAKE_STRING__ANY_MANY__STRING_1 = signature("native function meta::pure::functions::string::makeString(any:meta::pure::metamodel::type::Any[*], separator:meta::pure::metamodel::type::String[1]):meta::pure::metamodel::type::String[1];");
    public static final NativeFunctionDefinition MAKE_STRING__ANY_MANY__STRING_1__STRING_1__STRING_1 = signature("native function meta::pure::functions::string::makeString(any:meta::pure::metamodel::type::Any[*], prefix:meta::pure::metamodel::type::String[1], separator:meta::pure::metamodel::type::String[1], suffix:meta::pure::metamodel::type::String[1]):meta::pure::metamodel::type::String[1];");
    public static final NativeFunctionDefinition JOIN_STRINGS__STRING_MANY__STRING_1 = signature("native function meta::pure::functions::string::joinStrings(strings:meta::pure::metamodel::type::String[*], separator:meta::pure::metamodel::type::String[1]):meta::pure::metamodel::type::String[1];");
    public static final NativeFunctionDefinition JOIN_STRINGS__STRING_MANY__STRING_1__STRING_1__STRING_1 = signature("native function meta::pure::functions::string::joinStrings(strings:meta::pure::metamodel::type::String[*], prefix:meta::pure::metamodel::type::String[1], separator:meta::pure::metamodel::type::String[1], suffix:meta::pure::metamodel::type::String[1]):meta::pure::metamodel::type::String[1];");
    public static final NativeFunctionDefinition JOIN__RELATION_1__RELATION_1__JOIN_KIND_1__FUNCTION_1 = signature("native function meta::pure::functions::relation::join<T,V>(rel1:meta::pure::metamodel::relation::Relation<T>[1], rel2:meta::pure::metamodel::relation::Relation<V>[1], joinKind:meta::pure::functions::relation::JoinKind[1], f:meta::pure::metamodel::function::Function<{T[1],V[1]->meta::pure::metamodel::type::Boolean[1]}>[1]):meta::pure::metamodel::relation::Relation<T+V>[1];");
    // join (ColSpec form, Relation<S> -> Relation<S+A>): chain-join used by
    // MappingNormalizer's relational synth. The ColSpec binds a sub-row alias
    // (~firm:) to a tableReference in its function1 body; the trailing lambda
    // is the join condition over (source-row, target-row). Defaults to LEFT.
    // This is the relational, same-store widening primitive; cross-class
    // widening uses `associate` on Class[*] above.
    public static final NativeFunctionDefinition JOIN__RELATION_1__FUNC_COL_SPEC_1__FUNCTION_1 = signature("native function meta::legend::lite::join<S,T,Z>(rel:meta::pure::metamodel::relation::Relation<S>[1], slot:meta::pure::metamodel::relation::FuncColSpec<{->meta::pure::metamodel::relation::Relation<T>[1]},Z>[1], cond:meta::pure::metamodel::function::Function<{S[1],T[1]->meta::pure::metamodel::type::Boolean[1]}>[1]):meta::pure::metamodel::relation::Relation<S+Z>[1];");
    public static final NativeFunctionDefinition JOIN__RELATION_1__RELATION_1__JOIN_KIND_1__FUNCTION_1__STRING_1 = signature("native function meta::pure::functions::relation::join<T,V>(rel1:meta::pure::metamodel::relation::Relation<T>[1], rel2:meta::pure::metamodel::relation::Relation<V>[1], joinKind:meta::pure::functions::relation::JoinKind[1], f:meta::pure::metamodel::function::Function<{T[1],V[1]->meta::pure::metamodel::type::Boolean[1]}>[1], prefix:meta::pure::metamodel::type::String[1]):meta::pure::metamodel::relation::Relation<T+V>[1];");
    public static final NativeFunctionDefinition LAG__RELATION_1__T_1 = signature("native function meta::pure::functions::relation::lag<T>(w:meta::pure::metamodel::relation::Relation<T>[1], r:T[1]):T[0..1];");
    public static final NativeFunctionDefinition LAG__RELATION_1__T_1__INTEGER_1 = signature("native function meta::pure::functions::relation::lag<T>(w:meta::pure::metamodel::relation::Relation<T>[1], r:T[1], offset:meta::pure::metamodel::type::Integer[1]):T[0..1];");
    public static final NativeFunctionDefinition LAST__RELATION_1__WINDOW_1__T_1 = signature("native function meta::pure::functions::relation::last<T>(w:meta::pure::metamodel::relation::Relation<T>[1], f:meta::pure::functions::relation::_Window<T>[1], row:T[1]):T[0..1];");
    public static final NativeFunctionDefinition LAST__T_MANY = signature("native function meta::pure::functions::collection::last<T>(set:T[*]):T[0..1];");
    public static final NativeFunctionDefinition LEAD__RELATION_1__T_1 = signature("native function meta::pure::functions::relation::lead<T>(w:meta::pure::metamodel::relation::Relation<T>[1], r:T[1]):T[0..1];");
    public static final NativeFunctionDefinition LEAD__RELATION_1__T_1__INTEGER_1 = signature("native function meta::pure::functions::relation::lead<T>(w:meta::pure::metamodel::relation::Relation<T>[1], r:T[1], offset:meta::pure::metamodel::type::Integer[1]):T[0..1];");
    public static final NativeFunctionDefinition LEAST__X_MANY = signature("native function meta::pure::functions::collection::least<X>(values:X[*]):X[0..1];");
    public static final NativeFunctionDefinition LEAST__X_1_MANY = signature("native function meta::pure::functions::collection::least<X>(values:X[1..*]):X[1];");
    public static final NativeFunctionDefinition LEFT__STRING_1__INTEGER_1 = signature("native function meta::pure::functions::string::left(str:meta::pure::metamodel::type::String[1], len:meta::pure::metamodel::type::Integer[1]):meta::pure::metamodel::type::String[1];");
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
    public static final NativeFunctionDefinition NAVIGATE__RELATION_1__FUNC_COL_SPEC_1__FUNCTION_1 = signature("native function meta::legend::lite::navigate<S,T,Z>(rel:meta::pure::metamodel::relation::Relation<S>[1], target:meta::pure::metamodel::relation::FuncColSpec<{->T[*]},Z>[1], pred:meta::pure::metamodel::function::Function<{S[1],T[1]->meta::pure::metamodel::type::Boolean[1]}>[1]):meta::pure::metamodel::relation::Relation<S+Z>[1];");
    public static final NativeFunctionDefinition NAVIGATE__C_MANY__FUNC_COL_SPEC_1__FUNCTION_1 = signature("native function meta::legend::lite::navigate<C,T,Z>(cl:C[*], target:meta::pure::metamodel::relation::FuncColSpec<{->T[*]},Z>[1], pred:meta::pure::metamodel::function::Function<{C[1],T[1]->meta::pure::metamodel::type::Boolean[1]}>[1]):C[*];");
    public static final NativeFunctionDefinition NAVIGATE__T_MANY__FUNCTION_1 = signature("native function meta::legend::lite::navigate<T>(target:T[*], pred:meta::pure::metamodel::function::Function<{T[1]->meta::pure::metamodel::type::Boolean[1]}>[1]):T[*];");
    public static final NativeFunctionDefinition LEGACY_NAVIGATE__RELATION_1__FUNC_COL_SPEC_1__RELATION_1__FUNCTION_1 = signature("native function meta::legend::lite::legacyNavigate<S,C,T,Z>(rel:meta::pure::metamodel::relation::Relation<S>[1], target:meta::pure::metamodel::relation::FuncColSpec<{->C[*]},Z>[1], tgtRows:meta::pure::metamodel::relation::Relation<T>[1], cond:meta::pure::metamodel::function::Function<{S[1],T[1]->meta::pure::metamodel::type::Boolean[1]}>[1]):meta::pure::metamodel::relation::Relation<S+Z>[1];");
    // legacyAssocPredicate: row-extraction adapter for AssociationMapping
    // predicate function bodies. The outer function signature is
    // (A[1], B[1]) -> Boolean[1] (matching a clean AssociationMapping
    // predicate function); the adapter extracts the underlying main-
    // table rows of $a and $b and binds them to the lambda's two Row
    // parameters so the body can speak physical-column predicates.
    // Emitted exclusively by MappingNormalizer for Relational
    // AssociationMapping bodies. See docs/MAPPING_LEGACY_TO_FUNCTION.md §2.2.
    // typeAsDeclared: the mapping-side TYPE ASSERTION (a binding read
    // types as the DECLARED property; NO SQL is emitted — engine parity
    // for e.g. an Integer property over a DOUBLE column, calendar family)
    public static final NativeFunctionDefinition TYPE_AS_DECLARED__ANY_01__T_1 = signature("native function meta::legend::lite::typeAsDeclared<T>(value:meta::pure::metamodel::type::Any[0..1], type:T[1]):T[0..1];");
    public static final NativeFunctionDefinition ID__ANY_1 = signature("native function meta::pure::functions::meta::id(instance:meta::pure::metamodel::type::Any[1]):meta::pure::metamodel::type::String[1];");
    // K-phase natives: the engine's JDBC boundary (executed host-side at
    // the EXECUTE phase, never lowered to SQL). executeInDb is the 4-arg
    // leaf every corpus wrapper bottoms out at; testRuntime and
    // connectionByElement type the connection-resolution chains
    // (execution-context elements are Any[1] — the from() convention).
    public static final NativeFunctionDefinition EXECUTE_IN_DB__STRING_1__CONN_1__INTEGER_1__INTEGER_1 = signature("native function meta::relational::metamodel::execute::executeInDb(sql:meta::pure::metamodel::type::String[1], databaseConnection:meta::external::store::relational::runtime::DatabaseConnection[1], timeOutInSeconds:meta::pure::metamodel::type::Integer[1], fetchSize:meta::pure::metamodel::type::Integer[1]):meta::relational::metamodel::execute::ResultSet[1];");
    public static final NativeFunctionDefinition CONNECTION_BY_ELEMENT__ANY_1__ANY_1 = signature("native function meta::core::runtime::connectionByElement(runtime:meta::core::runtime::Runtime[1], store:meta::pure::metamodel::type::Any[1]):meta::core::runtime::Connection[1];");
    // dropAndCreateTableInDb: ordinary pure in the real engine (toDDL.pure
    // walks the Database metamodel to spell DDL) — a K-native here, DDL
    // rendered from the compiled store model (com.legend.exec.Ddl). The
    // database argument types as the store METACLASS, exactly like real
    // pure (audit 17: Any[1] let string literals type-check).
    public static final NativeFunctionDefinition DROP_AND_CREATE_TABLE_IN_DB__ANY_1__STRING_1__CONN_1 = signature("native function meta::relational::functions::toDDL::dropAndCreateTableInDb(database:meta::relational::metamodel::Database[1], tableName:meta::pure::metamodel::type::String[1], c:meta::external::store::relational::runtime::DatabaseConnection[1]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition DROP_AND_CREATE_TABLE_IN_DB__ANY_1__STRING_1__STRING_1__CONN_1 = signature("native function meta::relational::functions::toDDL::dropAndCreateTableInDb(database:meta::relational::metamodel::Database[1], schema:meta::pure::metamodel::type::String[1], tableName:meta::pure::metamodel::type::String[1], c:meta::external::store::relational::runtime::DatabaseConnection[1]):meta::pure::metamodel::type::Boolean[1];");
    // relationalExtension.pure's wrappers (2-arg AND the 3-arg debug
    // variant) are the corpus's OWN pure code — shared module sources in
    // the harness — and inline to the 4-arg native leaf. No natives here
    // (audit 17: a same-signature native would TIE with the corpus's own
    // function the day it compiles).
    public static final NativeFunctionDefinition DROP_AND_CREATE_SCHEMA_IN_DB__STRING_1__CONN_1 = signature("native function meta::relational::functions::toDDL::dropAndCreateSchemaInDb(schema:meta::pure::metamodel::type::String[1], c:meta::external::store::relational::runtime::DatabaseConnection[1]):meta::pure::metamodel::type::Boolean[1];");
    // real essential/io print surface — K-dispatched as NO-OPS: debug
    // output whose ARGUMENTS are never evaluated (they may introspect
    // ResultSets, which never materialize host-side)
    public static final NativeFunctionDefinition PRINT__ANY_M__INTEGER_1 = signature("native function meta::pure::functions::io::print(param:meta::pure::metamodel::type::Any[*], max:meta::pure::metamodel::type::Integer[1]):meta::pure::metamodel::type::Nil[0];");
    public static final NativeFunctionDefinition PRINT__ANY_M = signature("native function meta::pure::functions::io::print(param:meta::pure::metamodel::type::Any[*]):meta::pure::metamodel::type::Nil[0];");
    public static final NativeFunctionDefinition PRINTLN__ANY_M__INTEGER_1 = signature("native function meta::pure::functions::io::println(param:meta::pure::metamodel::type::Any[*], max:meta::pure::metamodel::type::Integer[1]):meta::pure::metamodel::type::Nil[0];");
    public static final NativeFunctionDefinition PRINTLN__ANY_M = signature("native function meta::pure::functions::io::println(param:meta::pure::metamodel::type::Any[*]):meta::pure::metamodel::type::Nil[0];");
    public static final NativeFunctionDefinition DROP_AND_CREATE_SCHEMA_IN_DB__STRING_1__CONN_1__BOOLEAN_1 = signature("native function meta::relational::functions::toDDL::dropAndCreateSchemaInDb(schema:meta::pure::metamodel::type::String[1], c:meta::external::store::relational::runtime::DatabaseConnection[1], debug:meta::pure::metamodel::type::Boolean[1]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition LEGACY_ASSOC_PREDICATE__A_1__B_1__RELATION_1__RELATION_1__FUNCTION_1 = signature("native function meta::legend::lite::legacyAssocPredicate<A,B,S,T>(a:A[1], b:B[1], src:meta::pure::metamodel::relation::Relation<S>[1], tgt:meta::pure::metamodel::relation::Relation<T>[1], cond:meta::pure::metamodel::function::Function<{S[1],T[1]->meta::pure::metamodel::type::Boolean[1]}>[1]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition LENGTH__STRING_1 = signature("native function meta::pure::functions::string::length(str:meta::pure::metamodel::type::String[1]):meta::pure::metamodel::type::Integer[1];");
    public static final NativeFunctionDefinition LESS_THAN_EQUAL__DATE_0_1__DATE_0_1 = signature("native function meta::pure::functions::boolean::lessThanEqual(left:meta::pure::metamodel::type::Date[0..1], right:meta::pure::metamodel::type::Date[0..1]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition LESS_THAN_EQUAL__DATE_0_1__DATE_1 = signature("native function meta::pure::functions::boolean::lessThanEqual(left:meta::pure::metamodel::type::Date[0..1], right:meta::pure::metamodel::type::Date[1]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition LESS_THAN_EQUAL__DATE_1__DATE_0_1 = signature("native function meta::pure::functions::boolean::lessThanEqual(left:meta::pure::metamodel::type::Date[1], right:meta::pure::metamodel::type::Date[0..1]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition LESS_THAN_EQUAL__DATE_1__DATE_1 = signature("native function meta::pure::functions::boolean::lessThanEqual(left:meta::pure::metamodel::type::Date[1], right:meta::pure::metamodel::type::Date[1]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition LESS_THAN_EQUAL__NUMBER_0_1__NUMBER_0_1 = signature("native function meta::pure::functions::boolean::lessThanEqual(left:meta::pure::metamodel::type::Number[0..1], right:meta::pure::metamodel::type::Number[0..1]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition LESS_THAN_EQUAL__NUMBER_0_1__NUMBER_1 = signature("native function meta::pure::functions::boolean::lessThanEqual(left:meta::pure::metamodel::type::Number[0..1], right:meta::pure::metamodel::type::Number[1]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition LESS_THAN_EQUAL__NUMBER_1__NUMBER_0_1 = signature("native function meta::pure::functions::boolean::lessThanEqual(left:meta::pure::metamodel::type::Number[1], right:meta::pure::metamodel::type::Number[0..1]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition LESS_THAN_EQUAL__NUMBER_1__NUMBER_1 = signature("native function meta::pure::functions::boolean::lessThanEqual(left:meta::pure::metamodel::type::Number[1], right:meta::pure::metamodel::type::Number[1]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition LESS_THAN_EQUAL__STRING_0_1__STRING_0_1 = signature("native function meta::pure::functions::boolean::lessThanEqual(left:meta::pure::metamodel::type::String[0..1], right:meta::pure::metamodel::type::String[0..1]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition LESS_THAN_EQUAL__STRING_0_1__STRING_1 = signature("native function meta::pure::functions::boolean::lessThanEqual(left:meta::pure::metamodel::type::String[0..1], right:meta::pure::metamodel::type::String[1]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition LESS_THAN_EQUAL__STRING_1__STRING_0_1 = signature("native function meta::pure::functions::boolean::lessThanEqual(left:meta::pure::metamodel::type::String[1], right:meta::pure::metamodel::type::String[0..1]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition LESS_THAN_EQUAL__STRING_1__STRING_1 = signature("native function meta::pure::functions::boolean::lessThanEqual(left:meta::pure::metamodel::type::String[1], right:meta::pure::metamodel::type::String[1]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition LESS_THAN__DATE_0_1__DATE_0_1 = signature("native function meta::pure::functions::boolean::lessThan(left:meta::pure::metamodel::type::Date[0..1], right:meta::pure::metamodel::type::Date[0..1]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition LESS_THAN__BOOLEAN_0_1__BOOLEAN_0_1 = signature("native function meta::pure::functions::boolean::lessThan(left:meta::pure::metamodel::type::Boolean[0..1], right:meta::pure::metamodel::type::Boolean[0..1]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition LESS_THAN_EQUAL__BOOLEAN_0_1__BOOLEAN_0_1 = signature("native function meta::pure::functions::boolean::lessThanEqual(left:meta::pure::metamodel::type::Boolean[0..1], right:meta::pure::metamodel::type::Boolean[0..1]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition GREATER_THAN__BOOLEAN_0_1__BOOLEAN_0_1 = signature("native function meta::pure::functions::boolean::greaterThan(left:meta::pure::metamodel::type::Boolean[0..1], right:meta::pure::metamodel::type::Boolean[0..1]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition GREATER_THAN_EQUAL__BOOLEAN_0_1__BOOLEAN_0_1 = signature("native function meta::pure::functions::boolean::greaterThanEqual(left:meta::pure::metamodel::type::Boolean[0..1], right:meta::pure::metamodel::type::Boolean[0..1]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition LESS_THAN__DATE_0_1__DATE_1 = signature("native function meta::pure::functions::boolean::lessThan(left:meta::pure::metamodel::type::Date[0..1], right:meta::pure::metamodel::type::Date[1]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition LESS_THAN__DATE_1__DATE_0_1 = signature("native function meta::pure::functions::boolean::lessThan(left:meta::pure::metamodel::type::Date[1], right:meta::pure::metamodel::type::Date[0..1]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition LESS_THAN__DATE_1__DATE_1 = signature("native function meta::pure::functions::boolean::lessThan(left:meta::pure::metamodel::type::Date[1], right:meta::pure::metamodel::type::Date[1]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition LESS_THAN__NUMBER_0_1__NUMBER_0_1 = signature("native function meta::pure::functions::boolean::lessThan(left:meta::pure::metamodel::type::Number[0..1], right:meta::pure::metamodel::type::Number[0..1]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition LESS_THAN__NUMBER_0_1__NUMBER_1 = signature("native function meta::pure::functions::boolean::lessThan(left:meta::pure::metamodel::type::Number[0..1], right:meta::pure::metamodel::type::Number[1]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition LESS_THAN__NUMBER_1__NUMBER_0_1 = signature("native function meta::pure::functions::boolean::lessThan(left:meta::pure::metamodel::type::Number[1], right:meta::pure::metamodel::type::Number[0..1]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition LESS_THAN__NUMBER_1__NUMBER_1 = signature("native function meta::pure::functions::boolean::lessThan(left:meta::pure::metamodel::type::Number[1], right:meta::pure::metamodel::type::Number[1]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition LESS_THAN__STRING_0_1__STRING_0_1 = signature("native function meta::pure::functions::boolean::lessThan(left:meta::pure::metamodel::type::String[0..1], right:meta::pure::metamodel::type::String[0..1]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition LESS_THAN__STRING_0_1__STRING_1 = signature("native function meta::pure::functions::boolean::lessThan(left:meta::pure::metamodel::type::String[0..1], right:meta::pure::metamodel::type::String[1]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition LESS_THAN__STRING_1__STRING_0_1 = signature("native function meta::pure::functions::boolean::lessThan(left:meta::pure::metamodel::type::String[1], right:meta::pure::metamodel::type::String[0..1]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition LESS_THAN__STRING_1__STRING_1 = signature("native function meta::pure::functions::boolean::lessThan(left:meta::pure::metamodel::type::String[1], right:meta::pure::metamodel::type::String[1]):meta::pure::metamodel::type::Boolean[1];");
    // Real legend-pure: letFunction(String[1], T[m]):T[m] (mangled letFunction_String_1__T_m__T_m_) —
    // the multiplicity VARIABLE m is what makes a binding preserve its value's multiplicity through the
    // standard resolve→unify→resolveOutput pipeline (multi-valued let, `let xs = [1,2,3]`). engine-lite
    // flattened m→[1], which broke that and forced a bespoke checker; the mult var restores correctness.
    public static final NativeFunctionDefinition LET_FUNCTION__STRING_1__T_m = signature("native function meta::pure::functions::lang::letFunction<T|m>(name:meta::pure::metamodel::type::String[1], value:T[m]):T[m];");
    public static final NativeFunctionDefinition LEVENSHTEIN_DISTANCE__STRING_1__STRING_1 = signature("native function meta::pure::functions::string::levenshteinDistance(s1:meta::pure::metamodel::type::String[1], s2:meta::pure::metamodel::type::String[1]):meta::pure::metamodel::type::Integer[1];");
    public static final NativeFunctionDefinition LIMIT__RELATION_1__INTEGER_1 = signature("native function meta::pure::functions::relation::limit<T>(rel:meta::pure::metamodel::relation::Relation<T>[1], size:meta::pure::metamodel::type::Integer[1]):meta::pure::metamodel::relation::Relation<T>[1];");
    public static final NativeFunctionDefinition LIMIT__T_MANY__INTEGER_1 = signature("native function meta::pure::functions::collection::limit<T>(set:T[*], size:meta::pure::metamodel::type::Integer[1]):T[*];");
    public static final NativeFunctionDefinition LIST__T_MANY = signature("native function meta::pure::functions::collection::list<T>(values:T[*]):meta::pure::functions::collection::List<T>[1];");
    public static final NativeFunctionDefinition LOG10__NUMBER_1 = signature("native function meta::pure::functions::math::log10(value:meta::pure::metamodel::type::Number[1]):meta::pure::metamodel::type::Float[1];");
    public static final NativeFunctionDefinition LOG__NUMBER_1 = signature("native function meta::pure::functions::math::log(value:meta::pure::metamodel::type::Number[1]):meta::pure::metamodel::type::Float[1];");
    public static final NativeFunctionDefinition LPAD__STRING_1__INTEGER_1 = signature("native function meta::pure::functions::string::lpad(str:meta::pure::metamodel::type::String[1], len:meta::pure::metamodel::type::Integer[1]):meta::pure::metamodel::type::String[1];");
    public static final NativeFunctionDefinition LPAD__STRING_1__INTEGER_1__STRING_1 = signature("native function meta::pure::functions::string::lpad(str:meta::pure::metamodel::type::String[1], len:meta::pure::metamodel::type::Integer[1], pad:meta::pure::metamodel::type::String[1]):meta::pure::metamodel::type::String[1];");
    public static final NativeFunctionDefinition LTRIM__STRING_1 = signature("native function meta::pure::functions::string::ltrim(str:meta::pure::metamodel::type::String[1]):meta::pure::metamodel::type::String[1];");
    public static final NativeFunctionDefinition MAP__T_0_1__FUNCTION_1 = signature("native function meta::pure::functions::collection::map<T,V>(value:T[0..1], func:meta::pure::metamodel::function::Function<{T[1]->V[0..1]}>[1]):V[0..1];");
    public static final NativeFunctionDefinition MAP__T_MANY__FUNCTION_1 = signature("native function meta::pure::functions::collection::map<T,V>(value:T[*], func:meta::pure::metamodel::function::Function<{T[1]->V[*]}>[1]):V[*];");
    public static final NativeFunctionDefinition MATCHES__STRING_1__STRING_1 = signature("native function meta::pure::functions::string::matches(str:meta::pure::metamodel::type::String[1], regex:meta::pure::metamodel::type::String[1]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition REGEXP_LIKE__2 = signature("native function meta::pure::functions::string::regexpLike(string:meta::pure::metamodel::type::String[1], regexp:meta::pure::metamodel::type::String[1]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition REGEXP_LIKE__3 = signature("native function meta::pure::functions::string::regexpLike(string:meta::pure::metamodel::type::String[1], regexp:meta::pure::metamodel::type::String[1], regexpParameters:meta::pure::functions::string::RegexpParameter[1..*]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition REGEXP_COUNT__2 = signature("native function meta::pure::functions::string::regexpCount(string:meta::pure::metamodel::type::String[1], regexp:meta::pure::metamodel::type::String[1]):meta::pure::metamodel::type::Integer[1];");
    public static final NativeFunctionDefinition REGEXP_COUNT__3 = signature("native function meta::pure::functions::string::regexpCount(string:meta::pure::metamodel::type::String[1], regexp:meta::pure::metamodel::type::String[1], regexpParameters:meta::pure::functions::string::RegexpParameter[1..*]):meta::pure::metamodel::type::Integer[1];");
    public static final NativeFunctionDefinition REGEXP_EXTRACT__3 = signature("native function meta::pure::functions::string::regexpExtract(string:meta::pure::metamodel::type::String[1], regexp:meta::pure::metamodel::type::String[1], extractAll:meta::pure::metamodel::type::Boolean[1]):meta::pure::metamodel::type::String[*];");
    public static final NativeFunctionDefinition REGEXP_EXTRACT__4 = signature("native function meta::pure::functions::string::regexpExtract(string:meta::pure::metamodel::type::String[1], regexp:meta::pure::metamodel::type::String[1], extractAll:meta::pure::metamodel::type::Boolean[1], groupNumber:meta::pure::metamodel::type::Integer[1]):meta::pure::metamodel::type::String[*];");
    public static final NativeFunctionDefinition REGEXP_EXTRACT__4P = signature("native function meta::pure::functions::string::regexpExtract(string:meta::pure::metamodel::type::String[1], regexp:meta::pure::metamodel::type::String[1], extractAll:meta::pure::metamodel::type::Boolean[1], regexpParameters:meta::pure::functions::string::RegexpParameter[1..*]):meta::pure::metamodel::type::String[*];");
    public static final NativeFunctionDefinition REGEXP_EXTRACT__5 = signature("native function meta::pure::functions::string::regexpExtract(string:meta::pure::metamodel::type::String[1], regexp:meta::pure::metamodel::type::String[1], extractAll:meta::pure::metamodel::type::Boolean[1], groupNumber:meta::pure::metamodel::type::Integer[1], regexpParameters:meta::pure::functions::string::RegexpParameter[1..*]):meta::pure::metamodel::type::String[*];");
    public static final NativeFunctionDefinition REGEXP_INDEX_OF__2 = signature("native function meta::pure::functions::string::regexpIndexOf(string:meta::pure::metamodel::type::String[1], regexp:meta::pure::metamodel::type::String[1]):meta::pure::metamodel::type::Integer[1];");
    public static final NativeFunctionDefinition REGEXP_INDEX_OF__3 = signature("native function meta::pure::functions::string::regexpIndexOf(string:meta::pure::metamodel::type::String[1], regexp:meta::pure::metamodel::type::String[1], groupNumber:meta::pure::metamodel::type::Integer[1]):meta::pure::metamodel::type::Integer[1];");
    public static final NativeFunctionDefinition REGEXP_INDEX_OF__3P = signature("native function meta::pure::functions::string::regexpIndexOf(string:meta::pure::metamodel::type::String[1], regexp:meta::pure::metamodel::type::String[1], regexpParameters:meta::pure::functions::string::RegexpParameter[1..*]):meta::pure::metamodel::type::Integer[1];");
    public static final NativeFunctionDefinition REGEXP_INDEX_OF__4 = signature("native function meta::pure::functions::string::regexpIndexOf(string:meta::pure::metamodel::type::String[1], regexp:meta::pure::metamodel::type::String[1], groupNumber:meta::pure::metamodel::type::Integer[1], regexpParameters:meta::pure::functions::string::RegexpParameter[1..*]):meta::pure::metamodel::type::Integer[1];");
    public static final NativeFunctionDefinition REGEXP_REPLACE__4 = signature("native function meta::pure::functions::string::regexpReplace(string:meta::pure::metamodel::type::String[1], regexp:meta::pure::metamodel::type::String[1], replacement:meta::pure::metamodel::type::String[1], replaceAll:meta::pure::metamodel::type::Boolean[1]):meta::pure::metamodel::type::String[1];");
    public static final NativeFunctionDefinition REGEXP_REPLACE__5 = signature("native function meta::pure::functions::string::regexpReplace(string:meta::pure::metamodel::type::String[1], regexp:meta::pure::metamodel::type::String[1], replacement:meta::pure::metamodel::type::String[1], replaceAll:meta::pure::metamodel::type::Boolean[1], regexpParameters:meta::pure::functions::string::RegexpParameter[1..*]):meta::pure::metamodel::type::String[1];");
    public static final NativeFunctionDefinition BIT_NOT__INTEGER_1 = signature("native function meta::pure::functions::math::bitNot(arg:meta::pure::metamodel::type::Integer[1]):meta::pure::metamodel::type::Integer[1];");
    public static final NativeFunctionDefinition Z_SCORE__WINDOW = signature("native function meta::pure::functions::math::zScore<T>(partition:meta::pure::metamodel::relation::Relation<T>[1], window:meta::pure::functions::relation::_Window<T>[1], row:T[1], colToZScore:meta::pure::metamodel::relation::ColSpec<(?:meta::pure::metamodel::type::Number)⊆T>[1]):meta::pure::metamodel::type::Float[1];");
    public static final NativeFunctionDefinition FORMAT_DATE__STRICT_DATE = signature("native function meta::pure::functions::date::formatDate(date:meta::pure::metamodel::type::StrictDate[1], dateFormat:meta::pure::functions::date::StrictDateFormat[1]):meta::pure::metamodel::type::String[1];");
    public static final NativeFunctionDefinition FORMAT_DATE__DATE_TIME = signature("native function meta::pure::functions::date::formatDate(dateTime:meta::pure::metamodel::type::DateTime[1], dateTimeFormat:meta::pure::functions::date::DateTimeFormat[1]):meta::pure::metamodel::type::String[1];");
    // VERBATIM real pure (platform/pure/essential/lang/flow/match.pure):
    // Nil branch params (bottom — the kernel's FunctionType arm skips them),
    // T[m] = the branch result; MatchChecker REFINES to the statically
    // selected branch's type (sound: a subtype of the signature's T[m]).
    public static final NativeFunctionDefinition MATCH__ANY_MANY__FUNCTION_1_MANY = signature("native function meta::pure::functions::lang::match<T|m,n>(var:meta::pure::metamodel::type::Any[*], functions:meta::pure::metamodel::function::Function<{meta::pure::metamodel::type::Nil[n]->T[m]}>[1..*]):T[m];");
    public static final NativeFunctionDefinition MATCH__ANY_MANY__FUNCTION_1_MANY__P_o = signature("native function meta::pure::functions::lang::match<T,P|m,n,o>(var:meta::pure::metamodel::type::Any[*], functions:meta::pure::metamodel::function::Function<{meta::pure::metamodel::type::Nil[n],P[o]->T[m]}>[1..*], with:P[o]):T[m];");
    public static final NativeFunctionDefinition MAX_BY__ROW_MAPPER_MANY = signature("native function meta::pure::functions::math::maxBy<T,U>(values:meta::pure::functions::math::mathUtility::RowMapper<T,U>[*]):T[0..1];");
    public static final NativeFunctionDefinition MAX_BY__T_MANY__FUNCTION_1 = signature("native function meta::pure::functions::math::maxBy<T>(values:T[*], key:meta::pure::metamodel::function::Function<{T[1]->meta::pure::metamodel::type::Any[1]}>[1]):T[0..1];");
    public static final NativeFunctionDefinition MAX_BY__T_MANY__FUNCTION_1__INTEGER_1 = signature("native function meta::pure::functions::math::maxBy<T>(values:T[*], key:meta::pure::metamodel::function::Function<{T[1]->meta::pure::metamodel::type::Any[1]}>[1], count:meta::pure::metamodel::type::Integer[1]):T[*];");
    public static final NativeFunctionDefinition MAX_BY__T_MANY__T_MANY = signature("native function meta::pure::functions::math::maxBy<T>(values:T[*], keys:T[*]):T[0..1];");
    public static final NativeFunctionDefinition MAX_BY__T_MANY__T_MANY__INTEGER_1 = signature("native function meta::pure::functions::math::maxBy<T>(values:T[*], keys:T[*], count:meta::pure::metamodel::type::Integer[1]):T[*];");
    public static final NativeFunctionDefinition MAX_DATE__DATE_1__DATE_1 = signature("native function meta::legend::lite::maxDate(d1:meta::pure::metamodel::type::Date[1], d2:meta::pure::metamodel::type::Date[1]):meta::pure::metamodel::type::Date[1];");
    public static final NativeFunctionDefinition MAX__DATE_1__DATE_1 = signature("native function meta::pure::functions::date::max(left:meta::pure::metamodel::type::Date[1], right:meta::pure::metamodel::type::Date[1]):meta::pure::metamodel::type::Date[1];");
    public static final NativeFunctionDefinition MAX__DATE_MANY = signature("native function meta::pure::functions::date::max(dates:meta::pure::metamodel::type::Date[*]):meta::pure::metamodel::type::Date[0..1];");
    public static final NativeFunctionDefinition MAX__DATE_TIME_1__DATE_TIME_1 = signature("native function meta::pure::functions::date::max(left:meta::pure::metamodel::type::DateTime[1], right:meta::pure::metamodel::type::DateTime[1]):meta::pure::metamodel::type::DateTime[1];");
    public static final NativeFunctionDefinition MAX__DATE_TIME_MANY = signature("native function meta::pure::functions::date::max(dates:meta::pure::metamodel::type::DateTime[*]):meta::pure::metamodel::type::DateTime[0..1];");
    public static final NativeFunctionDefinition MAX__FLOAT_1__FLOAT_1 = signature("native function meta::pure::functions::math::max(left:meta::pure::metamodel::type::Float[1], right:meta::pure::metamodel::type::Float[1]):meta::pure::metamodel::type::Float[1];");
    public static final NativeFunctionDefinition MAX__FLOAT_MANY = signature("native function meta::pure::functions::math::max(values:meta::pure::metamodel::type::Float[*]):meta::pure::metamodel::type::Float[0..1];");
    public static final NativeFunctionDefinition MAX__INTEGER_1__INTEGER_1 = signature("native function meta::pure::functions::math::max(left:meta::pure::metamodel::type::Integer[1], right:meta::pure::metamodel::type::Integer[1]):meta::pure::metamodel::type::Integer[1];");
    public static final NativeFunctionDefinition MAX__INTEGER_MANY = signature("native function meta::pure::functions::math::max(values:meta::pure::metamodel::type::Integer[*]):meta::pure::metamodel::type::Integer[0..1];");
    public static final NativeFunctionDefinition MAX__NUMBER_1__NUMBER_1 = signature("native function meta::pure::functions::math::max(left:meta::pure::metamodel::type::Number[1], right:meta::pure::metamodel::type::Number[1]):meta::pure::metamodel::type::Number[1];");
    public static final NativeFunctionDefinition MAX__NUMBER_MANY = signature("native function meta::pure::functions::math::max(numbers:meta::pure::metamodel::type::Number[*]):meta::pure::metamodel::type::Number[0..1];");
    public static final NativeFunctionDefinition MAX__STRICT_DATE_1__STRICT_DATE_1 = signature("native function meta::pure::functions::date::max(left:meta::pure::metamodel::type::StrictDate[1], right:meta::pure::metamodel::type::StrictDate[1]):meta::pure::metamodel::type::StrictDate[1];");
    public static final NativeFunctionDefinition MAX__STRICT_DATE_MANY = signature("native function meta::pure::functions::date::max(dates:meta::pure::metamodel::type::StrictDate[*]):meta::pure::metamodel::type::StrictDate[0..1];");
    public static final NativeFunctionDefinition MEAN__NUMBER_MANY = signature("native function meta::pure::functions::math::mean(numbers:meta::pure::metamodel::type::Number[*]):meta::pure::metamodel::type::Float[1];");
    public static final NativeFunctionDefinition MEDIAN__NUMBER_MANY = signature("native function meta::pure::functions::math::median(numbers:meta::pure::metamodel::type::Number[*]):meta::pure::metamodel::type::Number[1];");
    public static final NativeFunctionDefinition MINUS__DECIMAL_1__DECIMAL_1 = signature("native function meta::pure::functions::math::minus(left:meta::pure::metamodel::type::Decimal[1], right:meta::pure::metamodel::type::Decimal[1]):meta::pure::metamodel::type::Decimal[1];");
    public static final NativeFunctionDefinition MINUS__FLOAT_1__FLOAT_1 = signature("native function meta::pure::functions::math::minus(left:meta::pure::metamodel::type::Float[1], right:meta::pure::metamodel::type::Float[1]):meta::pure::metamodel::type::Float[1];");
    public static final NativeFunctionDefinition MINUS__INTEGER_1__INTEGER_1 = signature("native function meta::pure::functions::math::minus(left:meta::pure::metamodel::type::Integer[1], right:meta::pure::metamodel::type::Integer[1]):meta::pure::metamodel::type::Integer[1];");
    public static final NativeFunctionDefinition MINUS__NUMBER_1__NUMBER_1 = signature("native function meta::pure::functions::math::minus(left:meta::pure::metamodel::type::Number[1], right:meta::pure::metamodel::type::Number[1]):meta::pure::metamodel::type::Number[1];");
    public static final NativeFunctionDefinition MINUS__T_MANY = signature("native function meta::pure::functions::math::minus<T>(values:T[*]):T[1];");
    public static final NativeFunctionDefinition MINUTE__DATE_1 = signature("native function meta::pure::functions::date::minute(d:meta::pure::metamodel::type::Date[1]):meta::pure::metamodel::type::Integer[1];");
    public static final NativeFunctionDefinition MIN_BY__ROW_MAPPER_MANY = signature("native function meta::pure::functions::math::minBy<T,U>(values:meta::pure::functions::math::mathUtility::RowMapper<T,U>[*]):T[0..1];");
    public static final NativeFunctionDefinition MIN_BY__T_MANY__FUNCTION_1 = signature("native function meta::pure::functions::math::minBy<T>(values:T[*], key:meta::pure::metamodel::function::Function<{T[1]->meta::pure::metamodel::type::Any[1]}>[1]):T[0..1];");
    public static final NativeFunctionDefinition MIN_BY__T_MANY__FUNCTION_1__INTEGER_1 = signature("native function meta::pure::functions::math::minBy<T>(values:T[*], key:meta::pure::metamodel::function::Function<{T[1]->meta::pure::metamodel::type::Any[1]}>[1], count:meta::pure::metamodel::type::Integer[1]):T[*];");
    public static final NativeFunctionDefinition MIN_BY__T_MANY__T_MANY = signature("native function meta::pure::functions::math::minBy<T>(values:T[*], keys:T[*]):T[0..1];");
    public static final NativeFunctionDefinition MIN_BY__T_MANY__T_MANY__INTEGER_1 = signature("native function meta::pure::functions::math::minBy<T>(values:T[*], keys:T[*], count:meta::pure::metamodel::type::Integer[1]):T[*];");
    public static final NativeFunctionDefinition MIN_DATE__DATE_1__DATE_1 = signature("native function meta::legend::lite::minDate(d1:meta::pure::metamodel::type::Date[1], d2:meta::pure::metamodel::type::Date[1]):meta::pure::metamodel::type::Date[1];");
    public static final NativeFunctionDefinition MIN__DATE_1__DATE_1 = signature("native function meta::pure::functions::date::min(left:meta::pure::metamodel::type::Date[1], right:meta::pure::metamodel::type::Date[1]):meta::pure::metamodel::type::Date[1];");
    public static final NativeFunctionDefinition MIN__DATE_MANY = signature("native function meta::pure::functions::date::min(dates:meta::pure::metamodel::type::Date[*]):meta::pure::metamodel::type::Date[0..1];");
    public static final NativeFunctionDefinition MIN__DATE_TIME_1__DATE_TIME_1 = signature("native function meta::pure::functions::date::min(left:meta::pure::metamodel::type::DateTime[1], right:meta::pure::metamodel::type::DateTime[1]):meta::pure::metamodel::type::DateTime[1];");
    public static final NativeFunctionDefinition MIN__DATE_TIME_MANY = signature("native function meta::pure::functions::date::min(dates:meta::pure::metamodel::type::DateTime[*]):meta::pure::metamodel::type::DateTime[0..1];");
    public static final NativeFunctionDefinition MIN__FLOAT_1__FLOAT_1 = signature("native function meta::pure::functions::math::min(left:meta::pure::metamodel::type::Float[1], right:meta::pure::metamodel::type::Float[1]):meta::pure::metamodel::type::Float[1];");
    public static final NativeFunctionDefinition MIN__FLOAT_MANY = signature("native function meta::pure::functions::math::min(values:meta::pure::metamodel::type::Float[*]):meta::pure::metamodel::type::Float[0..1];");
    public static final NativeFunctionDefinition MIN__INTEGER_1__INTEGER_1 = signature("native function meta::pure::functions::math::min(left:meta::pure::metamodel::type::Integer[1], right:meta::pure::metamodel::type::Integer[1]):meta::pure::metamodel::type::Integer[1];");
    public static final NativeFunctionDefinition MIN__INTEGER_MANY = signature("native function meta::pure::functions::math::min(values:meta::pure::metamodel::type::Integer[*]):meta::pure::metamodel::type::Integer[0..1];");
    public static final NativeFunctionDefinition MIN__NUMBER_1__NUMBER_1 = signature("native function meta::pure::functions::math::min(left:meta::pure::metamodel::type::Number[1], right:meta::pure::metamodel::type::Number[1]):meta::pure::metamodel::type::Number[1];");
    public static final NativeFunctionDefinition MIN__NUMBER_MANY = signature("native function meta::pure::functions::math::min(numbers:meta::pure::metamodel::type::Number[*]):meta::pure::metamodel::type::Number[0..1];");
    public static final NativeFunctionDefinition MIN__STRICT_DATE_1__STRICT_DATE_1 = signature("native function meta::pure::functions::date::min(left:meta::pure::metamodel::type::StrictDate[1], right:meta::pure::metamodel::type::StrictDate[1]):meta::pure::metamodel::type::StrictDate[1];");
    public static final NativeFunctionDefinition MIN__STRICT_DATE_MANY = signature("native function meta::pure::functions::date::min(dates:meta::pure::metamodel::type::StrictDate[*]):meta::pure::metamodel::type::StrictDate[0..1];");
    // mode: REAL pure has CONCRETE numeric overloads, result [1] (core_functions_standard/math/aggregator/mode.pure).
    public static final NativeFunctionDefinition MODE__INTEGER_MANY = signature("native function meta::pure::functions::math::mode(numbers:meta::pure::metamodel::type::Integer[*]):meta::pure::metamodel::type::Integer[1];");
    public static final NativeFunctionDefinition MODE__FLOAT_MANY = signature("native function meta::pure::functions::math::mode(numbers:meta::pure::metamodel::type::Float[*]):meta::pure::metamodel::type::Float[1];");
    public static final NativeFunctionDefinition MODE__NUMBER_MANY = signature("native function meta::pure::functions::math::mode(numbers:meta::pure::metamodel::type::Number[*]):meta::pure::metamodel::type::Number[1];");
    public static final NativeFunctionDefinition MOD__INTEGER_1__INTEGER_1 = signature("native function meta::pure::functions::math::mod(dividend:meta::pure::metamodel::type::Integer[1], divisor:meta::pure::metamodel::type::Integer[1]):meta::pure::metamodel::type::Integer[1];");
    public static final NativeFunctionDefinition MONTH_NUMBER__DATE_1 = signature("native function meta::pure::functions::date::monthNumber(d:meta::pure::metamodel::type::Date[1]):meta::pure::metamodel::type::Integer[1];");
    public static final NativeFunctionDefinition MONTH__DATE_1 = signature("native function meta::pure::functions::date::month(d:meta::pure::metamodel::type::Date[1]):meta::pure::functions::date::Month[1];");
    public static final NativeFunctionDefinition NEW_TDS_RELATION_ACCESSOR__RELATION_1 = signature("native function meta::pure::metamodel::relation::newTDSRelationAccessor<T>(tds:meta::pure::metamodel::relation::Relation<T>[1]):meta::pure::metamodel::relation::Relation<T>[1];");
    public static final NativeFunctionDefinition NOT_EQUAL_ANSI__ANY_1__ANY_1 = signature("native function meta::legend::lite::notEqualAnsi(left:meta::pure::metamodel::type::Any[1], right:meta::pure::metamodel::type::Any[1]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition NOT__BOOLEAN_1 = signature("native function meta::pure::functions::boolean::not(value:meta::pure::metamodel::type::Boolean[1]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition NOW = signature("native function meta::pure::functions::date::now():meta::pure::metamodel::type::DateTime[1];");
    public static final NativeFunctionDefinition NTH__RELATION_1__WINDOW_1__T_1__INTEGER_1 = signature("native function meta::pure::functions::relation::nth<T>(w:meta::pure::metamodel::relation::Relation<T>[1], f:meta::pure::functions::relation::_Window<T>[1], r:T[1], offset:meta::pure::metamodel::type::Integer[1]):T[0..1];");
    public static final NativeFunctionDefinition NTILE__RELATION_1__T_1__INTEGER_1 = signature("native function meta::pure::functions::relation::ntile<T>(rel:meta::pure::metamodel::relation::Relation<T>[1], row:T[1], tileCount:meta::pure::metamodel::type::Integer[1]):meta::pure::metamodel::type::Integer[1];");
    public static final NativeFunctionDefinition OBJECT_REFERENCE_IN__ANY_1__ANY_MANY = signature("native function meta::pure::functions::collection::objectReferenceIn(col:meta::pure::metamodel::type::Any[1], values:meta::pure::metamodel::type::Any[*]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition OFFSET__RELATION_1__T_1__INTEGER_1 = signature("native function meta::pure::functions::relation::offset<T>(w:meta::pure::metamodel::relation::Relation<T>[1], r:T[1], offset:meta::pure::metamodel::type::Integer[1]):T[0..1];");
    public static final NativeFunctionDefinition OR__BOOLEAN_1__BOOLEAN_1 = signature("native function meta::pure::functions::boolean::or(left:meta::pure::metamodel::type::Boolean[1], right:meta::pure::metamodel::type::Boolean[1]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition OR__BOOLEAN_MANY = signature("native function meta::pure::functions::collection::or(values:meta::pure::metamodel::type::Boolean[*]):meta::pure::metamodel::type::Boolean[1];");
    // otherwise (generic class-level structural merge): takes a partial
    // instance and a fallback instance of the same class and returns a
    // complete instance — partial's set fields win, fallback fills the
    // rest (docs/MAPPING_CLEAN_SHEET.md §4.3). Emitted by MappingNormalizer
    // for OtherwiseEmbedded PMs as otherwise(^Inner(<inline subs>),
    // $row.<slot>) where the fallback slot is a legacyNavigate'd
    // instance. partial is always constructed (T[1]); the fallback slot
    // may be optional (T[0..1]); the merge yields a complete T[1].
    public static final NativeFunctionDefinition OTHERWISE__T_1__T_0_1 = signature("native function meta::legend::lite::otherwise<T>(partial:T[1], fallback:T[0..1]):T[1];");
    public static final NativeFunctionDefinition OVER__COL_SPEC_1 = signature("native function meta::pure::functions::relation::over<T>(cols:meta::pure::metamodel::relation::ColSpec<T>[1]):meta::pure::functions::relation::_Window<T>[1];");
    public static final NativeFunctionDefinition OVER__COL_SPEC_1__SORT_INFO_MANY = signature("native function meta::pure::functions::relation::over<T>(cols:meta::pure::metamodel::relation::ColSpec<T>[1], sortInfo:meta::pure::functions::relation::SortInfo<T>[*]):meta::pure::functions::relation::_Window<T>[1];");
    public static final NativeFunctionDefinition OVER__COL_SPEC_1__SORT_INFO_1__RANGE_1 = signature("native function meta::pure::functions::relation::over<T>(cols:meta::pure::metamodel::relation::ColSpec<T>[1], sortInfo:meta::pure::functions::relation::SortInfo<T>[1], range:meta::pure::functions::relation::_Range[1]):meta::pure::functions::relation::_Window<T>[1];");
    public static final NativeFunctionDefinition OVER__COL_SPEC_1__SORT_INFO_MANY__ROWS_1 = signature("native function meta::pure::functions::relation::over<T>(cols:meta::pure::metamodel::relation::ColSpec<T>[1], sortInfo:meta::pure::functions::relation::SortInfo<T>[*], rows:meta::pure::functions::relation::Rows[1]):meta::pure::functions::relation::_Window<T>[1];");
    public static final NativeFunctionDefinition OVER__COL_SPEC_ARRAY_1 = signature("native function meta::pure::functions::relation::over<T>(cols:meta::pure::metamodel::relation::ColSpecArray<T>[1]):meta::pure::functions::relation::_Window<T>[1];");
    public static final NativeFunctionDefinition OVER__COL_SPEC_ARRAY_1__SORT_INFO_MANY = signature("native function meta::pure::functions::relation::over<T>(cols:meta::pure::metamodel::relation::ColSpecArray<T>[1], sortInfo:meta::pure::functions::relation::SortInfo<T>[*]):meta::pure::functions::relation::_Window<T>[1];");
    public static final NativeFunctionDefinition OVER__SORT_INFO_MANY = signature("native function meta::pure::functions::relation::over<T>(sortInfo:meta::pure::functions::relation::SortInfo<T>[*]):meta::pure::functions::relation::_Window<T>[1];");
    public static final NativeFunctionDefinition OVER__SORT_INFO_1__RANGE_1 = signature("native function meta::pure::functions::relation::over<T>(sortInfo:meta::pure::functions::relation::SortInfo<T>[1], range:meta::pure::functions::relation::_Range[1]):meta::pure::functions::relation::_Window<T>[1];");
    public static final NativeFunctionDefinition OVER__SORT_INFO_1__RANGE_INTERVAL_1 = signature("native function meta::pure::functions::relation::over<T>(sortInfo:meta::pure::functions::relation::SortInfo<T>[1], rangeInterval:meta::pure::functions::relation::_RangeInterval[1]):meta::pure::functions::relation::_Window<T>[1];");
    public static final NativeFunctionDefinition OVER__COL_SPEC_1__SORT_INFO_1__RANGE_INTERVAL_1 = signature("native function meta::pure::functions::relation::over<T>(cols:meta::pure::metamodel::relation::ColSpec<T>[1], sortInfo:meta::pure::functions::relation::SortInfo<T>[1], rangeInterval:meta::pure::functions::relation::_RangeInterval[1]):meta::pure::functions::relation::_Window<T>[1];");
    public static final NativeFunctionDefinition MAX_GENERIC__X_MANY = signature("native function meta::pure::functions::collection::max<X>(values:X[*]):X[0..1];");
    public static final NativeFunctionDefinition MAX_GENERIC__X_1_MANY = signature("native function meta::pure::functions::collection::max<X>(values:X[1..*]):X[1];");
    public static final NativeFunctionDefinition MIN_GENERIC__X_MANY = signature("native function meta::pure::functions::collection::min<X>(values:X[*]):X[0..1];");
    public static final NativeFunctionDefinition MIN_GENERIC__X_1_MANY = signature("native function meta::pure::functions::collection::min<X>(values:X[1..*]):X[1];");
    public static final NativeFunctionDefinition MAX_CMP__T_MANY = signature("native function meta::pure::functions::collection::max<T>(col:T[*], comp:meta::pure::metamodel::function::Function<{T[1],T[1]->meta::pure::metamodel::type::Integer[1]}>[1]):T[0..1];");
    public static final NativeFunctionDefinition MAX_CMP__T_1_MANY = signature("native function meta::pure::functions::collection::max<T>(col:T[1..*], comp:meta::pure::metamodel::function::Function<{T[1],T[1]->meta::pure::metamodel::type::Integer[1]}>[1]):T[1];");
    public static final NativeFunctionDefinition MIN_CMP__T_MANY = signature("native function meta::pure::functions::collection::min<T>(col:T[*], comp:meta::pure::metamodel::function::Function<{T[1],T[1]->meta::pure::metamodel::type::Integer[1]}>[1]):T[0..1];");
    public static final NativeFunctionDefinition MIN_CMP__T_1_MANY = signature("native function meta::pure::functions::collection::min<T>(col:T[1..*], comp:meta::pure::metamodel::function::Function<{T[1],T[1]->meta::pure::metamodel::type::Integer[1]}>[1]):T[1];");
    public static final NativeFunctionDefinition PAIR__T_1__U_1 = signature("native function meta::pure::functions::collection::pair<T,U>(first:T[1], second:U[1]):meta::pure::functions::collection::Pair<T,U>[1];");
    public static final NativeFunctionDefinition NEW_MAP__PAIRS = signature("native function meta::pure::functions::collection::newMap<U,V>(pairs:meta::pure::functions::collection::Pair<U,V>[*]):meta::pure::functions::collection::Map<U,V>[1];");
    public static final NativeFunctionDefinition MAP_GET__MAP_1__U_1 = signature("native function meta::pure::functions::collection::get<U,V>(m:meta::pure::functions::collection::Map<U,V>[1], key:U[1]):V[0..1];");
    public static final NativeFunctionDefinition MAP_PUT__MAP_1__U_1__V_1 = signature("native function meta::pure::functions::collection::put<U,V>(m:meta::pure::functions::collection::Map<U,V>[1], key:U[1], value:V[1]):meta::pure::functions::collection::Map<U,V>[1];");
    public static final NativeFunctionDefinition MAP_PUT_ALL__MAP_1__PAIRS = signature("native function meta::pure::functions::collection::putAll<U,V>(m:meta::pure::functions::collection::Map<U,V>[1], pairs:meta::pure::functions::collection::Pair<U,V>[*]):meta::pure::functions::collection::Map<U,V>[1];");
    public static final NativeFunctionDefinition MAP_PUT_ALL__MAP_1__MAP_1 = signature("native function meta::pure::functions::collection::putAll<U,V>(m:meta::pure::functions::collection::Map<U,V>[1], o:meta::pure::functions::collection::Map<U,V>[1]):meta::pure::functions::collection::Map<U,V>[1];");
    public static final NativeFunctionDefinition MAP_KEYS__MAP_1 = signature("native function meta::pure::functions::collection::keys<U,V>(m:meta::pure::functions::collection::Map<U,V>[1]):U[*];");
    public static final NativeFunctionDefinition MAP_VALUES__MAP_1 = signature("native function meta::pure::functions::collection::values<U,V>(m:meta::pure::functions::collection::Map<U,V>[1]):V[*];");
    public static final NativeFunctionDefinition PARSE_BOOLEAN__STRING_1 = signature("native function meta::pure::functions::string::parseBoolean(string:meta::pure::metamodel::type::String[1]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition PARSE_DATE__STRING_1 = signature("native function meta::pure::functions::string::parseDate(string:meta::pure::metamodel::type::String[1]):meta::pure::metamodel::type::Date[1];");
    public static final NativeFunctionDefinition PARSE_DECIMAL__STRING_1 = signature("native function meta::pure::functions::string::parseDecimal(string:meta::pure::metamodel::type::String[1]):meta::pure::metamodel::type::Decimal[1];");
    public static final NativeFunctionDefinition PARSE_DECIMAL__STRING_1__INTEGER_1__INTEGER_1 = signature("native function meta::pure::functions::string::parseDecimal(string:meta::pure::metamodel::type::String[1], precision:meta::pure::metamodel::type::Integer[1], scale:meta::pure::metamodel::type::Integer[1]):meta::pure::metamodel::type::Decimal[1];");
    public static final NativeFunctionDefinition PARSE_FLOAT__STRING_1 = signature("native function meta::pure::functions::string::parseFloat(string:meta::pure::metamodel::type::String[1]):meta::pure::metamodel::type::Float[1];");
    public static final NativeFunctionDefinition PARSE_INTEGER__STRING_1 = signature("native function meta::pure::functions::string::parseInteger(string:meta::pure::metamodel::type::String[1]):meta::pure::metamodel::type::Integer[1];");
    public static final NativeFunctionDefinition PERCENTILE_CONT__NUMBER_MANY__NUMBER_1 = signature("native function meta::legend::lite::percentileCont(numbers:meta::pure::metamodel::type::Number[*], p:meta::pure::metamodel::type::Number[1]):meta::pure::metamodel::type::Number[1];");
    public static final NativeFunctionDefinition PERCENTILE_DISC__NUMBER_MANY__NUMBER_1 = signature("native function meta::legend::lite::percentileDisc(numbers:meta::pure::metamodel::type::Number[*], p:meta::pure::metamodel::type::Number[1]):meta::pure::metamodel::type::Number[1];");
    public static final NativeFunctionDefinition PERCENTILE__NUMBER_MANY__NUMBER_1 = signature("native function meta::pure::functions::math::percentile(numbers:meta::pure::metamodel::type::Number[*], p:meta::pure::metamodel::type::Number[1]):meta::pure::metamodel::type::Number[1];");
    public static final NativeFunctionDefinition PERCENTILE__NUMBER_MANY__NUMBER_1__BOOLEAN_1__BOOLEAN_1 = signature("native function meta::pure::functions::math::percentile(numbers:meta::pure::metamodel::type::Number[*], p:meta::pure::metamodel::type::Number[1], ascending:meta::pure::metamodel::type::Boolean[1], continuous:meta::pure::metamodel::type::Boolean[1]):meta::pure::metamodel::type::Number[0..1];");
    public static final NativeFunctionDefinition PERCENT_RANK__RELATION_1__WINDOW_1__T_1 = signature("native function meta::pure::functions::relation::percentRank<T>(rel:meta::pure::metamodel::relation::Relation<T>[1], w:meta::pure::functions::relation::_Window<T>[1], row:T[1]):meta::pure::metamodel::type::Float[1];");
    public static final NativeFunctionDefinition PI = signature("native function meta::pure::functions::math::pi():meta::pure::metamodel::type::Float[1];");
    public static final NativeFunctionDefinition PIVOT__RELATION_1__COL_SPEC_1__AGG_COL_SPEC_1 = signature("native function meta::pure::functions::relation::pivot<T,Z,K,V,R>(r:meta::pure::metamodel::relation::Relation<T>[1], cols:meta::pure::metamodel::relation::ColSpec<Z⊆T>[1], agg:meta::pure::metamodel::relation::AggColSpec<{T[1]->K[0..1]},{K[*]->V[0..1]},R>[1]):meta::pure::metamodel::relation::Relation<meta::pure::metamodel::type::Any>[1];");
    public static final NativeFunctionDefinition PIVOT__RELATION_1__COL_SPEC_1__AGG_COL_SPEC_ARRAY_1 = signature("native function meta::pure::functions::relation::pivot<T,Z,K,V,R>(r:meta::pure::metamodel::relation::Relation<T>[1], cols:meta::pure::metamodel::relation::ColSpec<Z⊆T>[1], agg:meta::pure::metamodel::relation::AggColSpecArray<{T[1]->K[0..1]},{K[*]->V[0..1]},R>[1]):meta::pure::metamodel::relation::Relation<meta::pure::metamodel::type::Any>[1];");
    public static final NativeFunctionDefinition PIVOT__RELATION_1__COL_SPEC_1__ANY_1_MANY__AGG_COL_SPEC_1 = signature("native function meta::pure::functions::relation::pivot<T,Z,K,V,R>(r:meta::pure::metamodel::relation::Relation<T>[1], cols:meta::pure::metamodel::relation::ColSpec<Z⊆T>[1], values:meta::pure::metamodel::type::Any[1..*], agg:meta::pure::metamodel::relation::AggColSpec<{T[1]->K[0..1]},{K[*]->V[0..1]},R>[1]):meta::pure::metamodel::relation::Relation<meta::pure::metamodel::type::Any>[1];");
    public static final NativeFunctionDefinition PIVOT__RELATION_1__COL_SPEC_ARRAY_1__AGG_COL_SPEC_1 = signature("native function meta::pure::functions::relation::pivot<T,Z,K,V,R>(r:meta::pure::metamodel::relation::Relation<T>[1], cols:meta::pure::metamodel::relation::ColSpecArray<Z⊆T>[1], agg:meta::pure::metamodel::relation::AggColSpec<{T[1]->K[0..1]},{K[*]->V[0..1]},R>[1]):meta::pure::metamodel::relation::Relation<meta::pure::metamodel::type::Any>[1];");
    public static final NativeFunctionDefinition PIVOT__RELATION_1__COL_SPEC_ARRAY_1__AGG_COL_SPEC_ARRAY_1 = signature("native function meta::pure::functions::relation::pivot<T,Z,K,V,R>(r:meta::pure::metamodel::relation::Relation<T>[1], cols:meta::pure::metamodel::relation::ColSpecArray<Z⊆T>[1], agg:meta::pure::metamodel::relation::AggColSpecArray<{T[1]->K[0..1]},{K[*]->V[0..1]},R>[1]):meta::pure::metamodel::relation::Relation<meta::pure::metamodel::type::Any>[1];");
    public static final NativeFunctionDefinition PLUS__DECIMAL_1__DECIMAL_1 = signature("native function meta::pure::functions::math::plus(left:meta::pure::metamodel::type::Decimal[1], right:meta::pure::metamodel::type::Decimal[1]):meta::pure::metamodel::type::Decimal[1];");
    public static final NativeFunctionDefinition PLUS__FLOAT_1__FLOAT_1 = signature("native function meta::pure::functions::math::plus(left:meta::pure::metamodel::type::Float[1], right:meta::pure::metamodel::type::Float[1]):meta::pure::metamodel::type::Float[1];");
    public static final NativeFunctionDefinition PLUS__INTEGER_1__INTEGER_1 = signature("native function meta::pure::functions::math::plus(left:meta::pure::metamodel::type::Integer[1], right:meta::pure::metamodel::type::Integer[1]):meta::pure::metamodel::type::Integer[1];");
    public static final NativeFunctionDefinition PLUS__NUMBER_1__NUMBER_1 = signature("native function meta::pure::functions::math::plus(left:meta::pure::metamodel::type::Number[1], right:meta::pure::metamodel::type::Number[1]):meta::pure::metamodel::type::Number[1];");
    public static final NativeFunctionDefinition PLUS__STRING_1__STRING_1 = signature("native function meta::pure::functions::math::plus(left:meta::pure::metamodel::type::String[1], right:meta::pure::metamodel::type::String[1]):meta::pure::metamodel::type::String[1];");
    public static final NativeFunctionDefinition PLUS__T_MANY = signature("native function meta::pure::functions::math::plus<T>(values:T[*]):T[1];");
    public static final NativeFunctionDefinition POW__NUMBER_1__NUMBER_1 = signature("native function meta::pure::functions::math::pow(base:meta::pure::metamodel::type::Number[1], exponent:meta::pure::metamodel::type::Number[1]):meta::pure::metamodel::type::Number[1];");
    public static final NativeFunctionDefinition PROJECT__C_MANY__FUNC_COL_SPEC_ARRAY_1 = signature("native function meta::pure::functions::relation::project<C,T>(cl:C[*], x:meta::pure::metamodel::relation::FuncColSpecArray<{C[1]->meta::pure::metamodel::type::Any[*]},T>[1]):meta::pure::metamodel::relation::Relation<T>[1];");
    public static final NativeFunctionDefinition PROJECT__K_MANY__FUNCTION_MANY__STRING_MANY = signature("native function meta::pure::tds::project<K>(set:K[*], fns:meta::pure::metamodel::function::Function<{K[1]->meta::pure::metamodel::type::Any[*]}>[*], ids:meta::pure::metamodel::type::String[*]):meta::pure::metamodel::relation::Relation<K>[1];");
    public static final NativeFunctionDefinition PROJECT__RELATION_1__FUNC_COL_SPEC_ARRAY_1 = signature("native function meta::pure::functions::relation::project<T,Z>(r:meta::pure::metamodel::relation::Relation<T>[1], fs:meta::pure::metamodel::relation::FuncColSpecArray<{T[1]->meta::pure::metamodel::type::Any[*]},Z>[1]):meta::pure::metamodel::relation::Relation<Z>[1];");
    public static final NativeFunctionDefinition QUARTER_NUMBER__DATE_1 = signature("native function meta::pure::functions::date::quarterNumber(d:meta::pure::metamodel::type::Date[1]):meta::pure::metamodel::type::Integer[1];");
    public static final NativeFunctionDefinition QUARTER__DATE_1 = signature("native function meta::pure::functions::date::quarter(d:meta::pure::metamodel::type::Date[1]):meta::pure::functions::date::Quarter[1];");
    public static final NativeFunctionDefinition RANGE__INTEGER_1 = signature("native function meta::pure::functions::collection::range(stop:meta::pure::metamodel::type::Integer[1]):meta::pure::metamodel::type::Integer[*];");
    public static final NativeFunctionDefinition RANGE__INTEGER_1__INTEGER_1 = signature("native function meta::pure::functions::collection::range(start:meta::pure::metamodel::type::Integer[1], stop:meta::pure::metamodel::type::Integer[1]):meta::pure::metamodel::type::Integer[*];");
    public static final NativeFunctionDefinition RANGE__INTEGER_1__INTEGER_1__INTEGER_1 = signature("native function meta::pure::functions::collection::range(start:meta::pure::metamodel::type::Integer[1], stop:meta::pure::metamodel::type::Integer[1], step:meta::pure::metamodel::type::Integer[1]):meta::pure::metamodel::type::Integer[*];");
    public static final NativeFunctionDefinition RANK__RELATION_1__WINDOW_1__T_1 = signature("native function meta::pure::functions::relation::rank<T>(rel:meta::pure::metamodel::relation::Relation<T>[1], w:meta::pure::functions::relation::_Window<T>[1], row:T[1]):meta::pure::metamodel::type::Integer[1];");
    public static final NativeFunctionDefinition REMOVE_ALL_OPTIMIZED__T_MANY__T_MANY = signature("native function meta::pure::functions::collection::removeAllOptimized<T>(set:T[*], other:T[*]):T[*];");
    public static final NativeFunctionDefinition REMOVE_DUPLICATES_BY__T_MANY__FUNCTION_1 = signature("native function meta::pure::functions::collection::removeDuplicatesBy<T,V>(col:T[*], key:meta::pure::metamodel::function::Function<{T[1]->V[1]}>[1]):T[*];");
    public static final NativeFunctionDefinition REMOVE_DUPLICATES__T_MANY = signature("native function meta::pure::functions::collection::removeDuplicates<T>(col:T[*]):T[*];");
    public static final NativeFunctionDefinition DISTINCT__T_MANY = signature("native function meta::pure::functions::collection::distinct<T>(s:T[*]):T[*];");
    /** The collection overload's key, exported so LOWERING (parser-free) can rule on it. */
    public static final String DISTINCT_COLLECTION_KEY = DISTINCT__T_MANY.signatureKey();
    /** pair()'s key, exported for the STRUCT-carrier lowering rule. */
    public static final String PAIR_KEY = PAIR__T_1__U_1.signatureKey();
    /** Map get()'s key — the bare name is shared with variant get. */
    public static final String MAP_GET_KEY = MAP_GET__MAP_1__U_1.signatureKey();
    public static final NativeFunctionDefinition REMOVE_DUPLICATES__T_MANY__FUNCTION_0_1__FUNCTION_0_1 = signature("native function meta::pure::functions::collection::removeDuplicates<T,V>(col:T[*], key:meta::pure::metamodel::function::Function<{T[1]->V[1]}>[0..1], eql:meta::pure::metamodel::function::Function<{V[1],V[1]->meta::pure::metamodel::type::Boolean[1]}>[0..1]):T[*];");
    public static final NativeFunctionDefinition REMOVE_DUPLICATES__T_MANY__FUNCTION_1 = signature("native function meta::pure::functions::collection::removeDuplicates<T>(col:T[*], eql:meta::pure::metamodel::function::Function<{T[1],T[1]->meta::pure::metamodel::type::Boolean[1]}>[1]):T[*];");
    public static final NativeFunctionDefinition REM__NUMBER_1__NUMBER_1 = signature("native function meta::pure::functions::math::rem(dividend:meta::pure::metamodel::type::Number[1], divisor:meta::pure::metamodel::type::Number[1]):meta::pure::metamodel::type::Number[1];");
    public static final NativeFunctionDefinition RENAME__RELATION_1__COL_SPEC_1__COL_SPEC_1 = signature("native function meta::pure::functions::relation::rename<T,Z,K,V>(r:meta::pure::metamodel::relation::Relation<T>[1], old:meta::pure::metamodel::relation::ColSpec<Z=(?:K)⊆T>[1], new:meta::pure::metamodel::relation::ColSpec<V=(?:K)>[1]):meta::pure::metamodel::relation::Relation<T-Z+V>[1];");
    public static final NativeFunctionDefinition RENAME__RELATION_1__COL_SPEC_ARRAY_1__COL_SPEC_ARRAY_1 = signature("native function meta::pure::functions::relation::rename<T,Z,V>(r:meta::pure::metamodel::relation::Relation<T>[1], oldCols:meta::pure::metamodel::relation::ColSpecArray<Z⊆T>[1], newCols:meta::pure::metamodel::relation::ColSpecArray<V>[1]):meta::pure::metamodel::relation::Relation<T-Z+V>[1];");
    public static final NativeFunctionDefinition REPEAT_STRING__STRING_0_1__INTEGER_1 = signature("native function meta::pure::functions::string::repeatString(str:meta::pure::metamodel::type::String[0..1], count:meta::pure::metamodel::type::Integer[1]):meta::pure::metamodel::type::String[0..1];");
    public static final NativeFunctionDefinition REPLACE__STRING_1__STRING_1__STRING_1 = signature("native function meta::pure::functions::string::replace(str:meta::pure::metamodel::type::String[1], toFind:meta::pure::metamodel::type::String[1], replacement:meta::pure::metamodel::type::String[1]):meta::pure::metamodel::type::String[1];");
    public static final NativeFunctionDefinition REVERSE_STRING__STRING_1 = signature("native function meta::pure::functions::string::reverseString(str:meta::pure::metamodel::type::String[1]):meta::pure::metamodel::type::String[1];");
    public static final NativeFunctionDefinition REVERSE__T_m = signature("native function meta::pure::functions::collection::reverse<T|m>(set:T[m]):T[m];");
    public static final NativeFunctionDefinition RIGHT__STRING_1__INTEGER_1 = signature("native function meta::pure::functions::string::right(str:meta::pure::metamodel::type::String[1], len:meta::pure::metamodel::type::Integer[1]):meta::pure::metamodel::type::String[1];");
    public static final NativeFunctionDefinition ROUND__DECIMAL_1__INTEGER_1 = signature("native function meta::pure::functions::math::round(decimal:meta::pure::metamodel::type::Decimal[1], scale:meta::pure::metamodel::type::Integer[1]):meta::pure::metamodel::type::Decimal[1];");
    public static final NativeFunctionDefinition ROUND__FLOAT_1__INTEGER_1 = signature("native function meta::pure::functions::math::round(float:meta::pure::metamodel::type::Float[1], scale:meta::pure::metamodel::type::Integer[1]):meta::pure::metamodel::type::Float[1];");
    public static final NativeFunctionDefinition ROUND__NUMBER_1 = signature("native function meta::pure::functions::math::round(number:meta::pure::metamodel::type::Number[1]):meta::pure::metamodel::type::Integer[1];");
    public static final NativeFunctionDefinition ROWS__INTEGER_1__INTEGER_1 = signature("native function meta::pure::functions::relation::rows(offsetFrom:meta::pure::metamodel::type::Integer[1], offsetTo:meta::pure::metamodel::type::Integer[1]):meta::pure::functions::relation::Rows[1];");
    // over(frame) — real over.pure line 21's (cols:String[*], sortInfo:[*],
    // frame:Frame[0..1]) admits the frame-only call through empty varargs;
    // our arity-exact matching registers the collapsed form.
    public static final NativeFunctionDefinition OVER__COL_SPEC_1__ROWS_1 = signature("native function meta::pure::functions::relation::over<T>(cols:meta::pure::metamodel::relation::ColSpec<T>[1], rows:meta::pure::functions::relation::Rows[1]):meta::pure::functions::relation::_Window<T>[1];");
    public static final NativeFunctionDefinition OVER__COL_SPEC_ARRAY_1__ROWS_1 = signature("native function meta::pure::functions::relation::over<T>(cols:meta::pure::metamodel::relation::ColSpecArray<T>[1], rows:meta::pure::functions::relation::Rows[1]):meta::pure::functions::relation::_Window<T>[1];");
    public static final NativeFunctionDefinition OVER__COL_SPEC_ARRAY_1__SORT_INFO_MANY__ROWS_1 = signature("native function meta::pure::functions::relation::over<T>(cols:meta::pure::metamodel::relation::ColSpecArray<T>[1], sortInfo:meta::pure::functions::relation::SortInfo<T>[*], rows:meta::pure::functions::relation::Rows[1]):meta::pure::functions::relation::_Window<T>[1];");
    public static final NativeFunctionDefinition OVER__COL_SPEC_ARRAY_1__SORT_INFO_1__RANGE_1 = signature("native function meta::pure::functions::relation::over<T>(cols:meta::pure::metamodel::relation::ColSpecArray<T>[1], sortInfo:meta::pure::functions::relation::SortInfo<T>[1], range:meta::pure::functions::relation::_Range[1]):meta::pure::functions::relation::_Window<T>[1];");
    public static final NativeFunctionDefinition OVER__COL_SPEC_ARRAY_1__SORT_INFO_1__RANGE_INTERVAL_1 = signature("native function meta::pure::functions::relation::over<T>(cols:meta::pure::metamodel::relation::ColSpecArray<T>[1], sortInfo:meta::pure::functions::relation::SortInfo<T>[1], rangeInterval:meta::pure::functions::relation::_RangeInterval[1]):meta::pure::functions::relation::_Window<T>[1];");
    public static final NativeFunctionDefinition REDUCE__RELATION_1__WINDOW_1__T_1__FUNCTION_1__FUNCTION_1 = signature("native function meta::pure::functions::relation::reduce<T,V,U|m>(rel:meta::pure::metamodel::relation::Relation<T>[1], w:meta::pure::functions::relation::_Window<T>[1], row:T[1], map:meta::pure::metamodel::function::Function<{T[1]->V[*]}>[1], agg:meta::pure::metamodel::function::Function<{V[*]->U[m]}>[1]):U[m];");
    public static final NativeFunctionDefinition FROM_JSON__STRING_1 = signature("native function meta::pure::functions::variant::convert::fromJson(json:meta::pure::metamodel::type::String[1]):meta::pure::metamodel::variant::Variant[1];");
    public static final NativeFunctionDefinition VARIANT_FLATTEN__T_MANY__COL_SPEC_1 = signature("native function meta::pure::functions::relation::variant::flatten<T>(valueToFlatten:T[*], columnWithFlattenedValue:meta::pure::metamodel::relation::ColSpec<meta::pure::metamodel::type::Any>[1]):meta::pure::metamodel::relation::Relation<meta::pure::metamodel::type::Any>[1];");
    public static final NativeFunctionDefinition LATERAL__RELATION_1__FUNCTION_1 = signature("native function meta::pure::functions::relation::lateral<T,V>(rel:meta::pure::metamodel::relation::Relation<T>[1], f:meta::pure::metamodel::function::Function<{T[1]->meta::pure::metamodel::relation::Relation<V>[1]}>[1]):meta::pure::metamodel::relation::Relation<T+V>[1];");
    public static final NativeFunctionDefinition ROWS__UNBOUNDED_1__UNBOUNDED_1 = signature("native function meta::pure::functions::relation::rows(offsetFrom:meta::pure::functions::relation::UnboundedFrameValue[1], offsetTo:meta::pure::functions::relation::UnboundedFrameValue[1]):meta::pure::functions::relation::Rows[1];");
    public static final NativeFunctionDefinition ROWS__UNBOUNDED_1__INTEGER_1 = signature("native function meta::pure::functions::relation::rows(offsetFrom:meta::pure::functions::relation::UnboundedFrameValue[1], offsetTo:meta::pure::metamodel::type::Integer[1]):meta::pure::functions::relation::Rows[1];");
    public static final NativeFunctionDefinition ROWS__INTEGER_1__UNBOUNDED_1 = signature("native function meta::pure::functions::relation::rows(offsetFrom:meta::pure::metamodel::type::Integer[1], offsetTo:meta::pure::functions::relation::UnboundedFrameValue[1]):meta::pure::functions::relation::Rows[1];");
    public static final NativeFunctionDefinition ROW_MAPPER__T_0_1__U_0_1 = signature("native function meta::pure::functions::math::mathUtility::rowMapper<T,U>(value:T[0..1], key:U[0..1]):meta::pure::functions::math::mathUtility::RowMapper<T,U>[1];");
    public static final NativeFunctionDefinition ROW_NUMBER__RELATION_1__T_1 = signature("native function meta::pure::functions::relation::rowNumber<T>(rel:meta::pure::metamodel::relation::Relation<T>[1], row:T[1]):meta::pure::metamodel::type::Integer[1];");
    public static final NativeFunctionDefinition RPAD__STRING_1__INTEGER_1 = signature("native function meta::pure::functions::string::rpad(str:meta::pure::metamodel::type::String[1], len:meta::pure::metamodel::type::Integer[1]):meta::pure::metamodel::type::String[1];");
    public static final NativeFunctionDefinition RPAD__STRING_1__INTEGER_1__STRING_1 = signature("native function meta::pure::functions::string::rpad(str:meta::pure::metamodel::type::String[1], len:meta::pure::metamodel::type::Integer[1], pad:meta::pure::metamodel::type::String[1]):meta::pure::metamodel::type::String[1];");
    public static final NativeFunctionDefinition RTRIM__STRING_1 = signature("native function meta::pure::functions::string::rtrim(str:meta::pure::metamodel::type::String[1]):meta::pure::metamodel::type::String[1];");
    public static final NativeFunctionDefinition SECOND__DATE_1 = signature("native function meta::pure::functions::date::second(d:meta::pure::metamodel::type::Date[1]):meta::pure::metamodel::type::Integer[1];");
    public static final NativeFunctionDefinition SELECT__RELATION_1 = signature("native function meta::pure::functions::relation::select<T>(r:meta::pure::metamodel::relation::Relation<T>[1]):meta::pure::metamodel::relation::Relation<T>[1];");
    public static final NativeFunctionDefinition SELECT__RELATION_1__COL_SPEC_1 = signature("native function meta::pure::functions::relation::select<T,Z>(r:meta::pure::metamodel::relation::Relation<T>[1], cols:meta::pure::metamodel::relation::ColSpec<Z⊆T>[1]):meta::pure::metamodel::relation::Relation<Z>[1];");
    public static final NativeFunctionDefinition SELECT__RELATION_1__COL_SPEC_ARRAY_1 = signature("native function meta::pure::functions::relation::select<T,Z>(r:meta::pure::metamodel::relation::Relation<T>[1], cols:meta::pure::metamodel::relation::ColSpecArray<Z⊆T>[1]):meta::pure::metamodel::relation::Relation<Z>[1];");
    public static final NativeFunctionDefinition SERIALIZE__T_MANY__ROOT_GRAPH_FETCH_TREE_1 = signature("native function meta::pure::graphFetch::execution::serialize<T>(source:T[*], tree:meta::pure::graphFetch::RootGraphFetchTree<T>[1]):meta::pure::metamodel::type::String[1];");
    public static final NativeFunctionDefinition SERIALIZE__T_MANY__ROOT_GRAPH_FETCH_TREE_1__ANY_1 = signature("native function meta::pure::graphFetch::execution::serialize<T>(source:T[*], tree:meta::pure::graphFetch::RootGraphFetchTree<T>[1], config:meta::pure::metamodel::type::Any[1]):meta::pure::metamodel::type::String[1];");
    public static final NativeFunctionDefinition SIGN__NUMBER_1 = signature("native function meta::pure::functions::math::sign(number:meta::pure::metamodel::type::Number[1]):meta::pure::metamodel::type::Integer[1];");
    public static final NativeFunctionDefinition SINH__NUMBER_1 = signature("native function meta::pure::functions::math::sinh(number:meta::pure::metamodel::type::Number[1]):meta::pure::metamodel::type::Float[1];");
    public static final NativeFunctionDefinition SIN__NUMBER_1 = signature("native function meta::pure::functions::math::sin(number:meta::pure::metamodel::type::Number[1]):meta::pure::metamodel::type::Float[1];");
    public static final NativeFunctionDefinition SIZE__RELATION_1 = signature("native function meta::pure::functions::relation::size<T>(rel:meta::pure::metamodel::relation::Relation<T>[1]):meta::pure::metamodel::type::Integer[1];");
    public static final NativeFunctionDefinition SIZE__T_MANY = signature("native function meta::pure::functions::collection::size<T>(col:T[*]):meta::pure::metamodel::type::Integer[1];");
    public static final NativeFunctionDefinition SLICE__RELATION_1__INTEGER_1__INTEGER_1 = signature("native function meta::pure::functions::relation::slice<T>(rel:meta::pure::metamodel::relation::Relation<T>[1], start:meta::pure::metamodel::type::Integer[1], stop:meta::pure::metamodel::type::Integer[1]):meta::pure::metamodel::relation::Relation<T>[1];");
    public static final NativeFunctionDefinition SLICE__T_MANY__INTEGER_1__INTEGER_1 = signature("native function meta::pure::functions::collection::slice<T>(set:T[*], start:meta::pure::metamodel::type::Integer[1], end:meta::pure::metamodel::type::Integer[1]):T[*];");
    public static final NativeFunctionDefinition SORT_BY_REVERSED__T_m__FUNCTION_0_1 = signature("native function meta::pure::functions::collection::sortByReversed<T,U|m>(col:T[m], key:meta::pure::metamodel::function::Function<{T[1]->U[1]}>[0..1]):T[m];");
    public static final NativeFunctionDefinition SORT_BY__T_m__FUNCTION_0_1 = signature("native function meta::pure::functions::collection::sortBy<T,U|m>(col:T[m], key:meta::pure::metamodel::function::Function<{T[1]->U[1]}>[0..1]):T[m];");
    public static final NativeFunctionDefinition SORT__RELATION_1__SORT_INFO_MANY = signature("native function meta::pure::functions::relation::sort<X,T>(rel:meta::pure::metamodel::relation::Relation<T>[1], sortInfo:meta::pure::functions::relation::SortInfo<X⊆T>[*]):meta::pure::metamodel::relation::Relation<T>[1];");
    public static final NativeFunctionDefinition SORT__RELATION_1__STRING_1__SORT_DIRECTION_1 = signature("native function meta::pure::tds::sort<T>(rel:meta::pure::metamodel::relation::Relation<T>[1], col:meta::pure::metamodel::type::String[1], direction:meta::relational::metamodel::SortDirection[1]):meta::pure::metamodel::relation::Relation<T>[1];");
    public static final NativeFunctionDefinition SORT__RELATION_1__STRING_MANY = signature("native function meta::pure::functions::relation::sort<T>(rel:meta::pure::metamodel::relation::Relation<T>[1], cols:meta::pure::metamodel::type::String[*]):meta::pure::metamodel::relation::Relation<T>[1];");
    public static final NativeFunctionDefinition SORT__T_m = signature("native function meta::pure::functions::collection::sort<T|m>(col:T[m]):T[m];");
    public static final NativeFunctionDefinition SORT__T_m__FUNCTION_0_1 = signature("native function meta::pure::functions::collection::sort<T|m>(col:T[m], comp:meta::pure::metamodel::function::Function<{T[1],T[1]->meta::pure::metamodel::type::Integer[1]}>[0..1]):T[m];");
    public static final NativeFunctionDefinition SORT__T_m__FUNCTION_0_1__FUNCTION_0_1 = signature("native function meta::pure::functions::collection::sort<T,U|m>(col:T[m], key:meta::pure::metamodel::function::Function<{T[1]->U[1]}>[0..1], comp:meta::pure::metamodel::function::Function<{U[1],U[1]->meta::pure::metamodel::type::Integer[1]}>[0..1]):T[m];");
    public static final NativeFunctionDefinition SOURCE_URL__STRING_1 = signature("native function meta::legend::lite::sourceUrl(url:meta::pure::metamodel::type::String[1]):meta::pure::metamodel::relation::Relation<meta::pure::metamodel::type::Any>[1];");
    public static final NativeFunctionDefinition SPLIT_PART__STRING_0_1__STRING_1__INTEGER_1 = signature("native function meta::pure::functions::string::splitPart(str:meta::pure::metamodel::type::String[0..1], delimiter:meta::pure::metamodel::type::String[1], index:meta::pure::metamodel::type::Integer[1]):meta::pure::metamodel::type::String[0..1];");
    public static final NativeFunctionDefinition SPLIT__STRING_1__STRING_1 = signature("native function meta::pure::functions::string::split(str:meta::pure::metamodel::type::String[1], delimiter:meta::pure::metamodel::type::String[1]):meta::pure::metamodel::type::String[*];");
    public static final NativeFunctionDefinition SQL_FALSE = signature("native function meta::relational::functions::sqlQueryToString::sqlFalse():meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition SQL_NULL = signature("native function meta::relational::functions::sqlQueryToString::sqlNull():meta::pure::metamodel::type::Nil[0];");
    public static final NativeFunctionDefinition SQL_TRUE = signature("native function meta::relational::functions::sqlQueryToString::sqlTrue():meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition SQRT__NUMBER_1 = signature("native function meta::pure::functions::math::sqrt(number:meta::pure::metamodel::type::Number[1]):meta::pure::metamodel::type::Float[1];");
    public static final NativeFunctionDefinition STARTS_WITH__STRING_1__STRING_1 = signature("native function meta::pure::functions::string::startsWith(source:meta::pure::metamodel::type::String[1], val:meta::pure::metamodel::type::String[1]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition STD_DEV_POPULATION__NUMBER_MANY = signature("native function meta::pure::functions::math::stdDevPopulation(numbers:meta::pure::metamodel::type::Number[*]):meta::pure::metamodel::type::Number[1];");
    public static final NativeFunctionDefinition STD_DEV_POPULATION__RELATION_1__WINDOW_1__T_1__COL_SPEC_1 = signature("native function meta::pure::functions::math::stdDevPopulation<T>(partition:meta::pure::metamodel::relation::Relation<T>[1], window:meta::pure::functions::relation::_Window<T>[1], row:T[1], colToAgg:meta::pure::metamodel::relation::ColSpec<(?:meta::pure::metamodel::type::Number)⊆T>[1]):meta::pure::metamodel::type::Number[1];");
    public static final NativeFunctionDefinition STD_DEV_SAMPLE__NUMBER_MANY = signature("native function meta::pure::functions::math::stdDevSample(numbers:meta::pure::metamodel::type::Number[*]):meta::pure::metamodel::type::Number[1];");
    public static final NativeFunctionDefinition STD_DEV__NUMBER_MANY = signature("native function meta::pure::functions::math::stdDev(numbers:meta::pure::metamodel::type::Number[*]):meta::pure::metamodel::type::Number[1];");
    // CORPUS-SHAPE window overload — see VARIANCE__RELATION_1__WINDOW_1__T_1.
    public static final NativeFunctionDefinition STD_DEV__RELATION_1__WINDOW_1__T_1 = signature("native function meta::pure::functions::math::stdDev<T>(w:meta::pure::metamodel::relation::Relation<T>[1], f:meta::pure::functions::relation::_Window<T>[1], r:T[1]):T[0..1];");
    public static final NativeFunctionDefinition SUBSTRING__STRING_1__INTEGER_1 = signature("native function meta::pure::functions::string::substring(str:meta::pure::metamodel::type::String[1], start:meta::pure::metamodel::type::Integer[1]):meta::pure::metamodel::type::String[1];");
    public static final NativeFunctionDefinition SUBSTRING__STRING_1__INTEGER_1__INTEGER_1 = signature("native function meta::pure::functions::string::substring(str:meta::pure::metamodel::type::String[1], start:meta::pure::metamodel::type::Integer[1], end:meta::pure::metamodel::type::Integer[1]):meta::pure::metamodel::type::String[1];");
    public static final NativeFunctionDefinition SUB__DECIMAL_1__DECIMAL_1 = signature("native function meta::legend::lite::sub(left:meta::pure::metamodel::type::Decimal[1], right:meta::pure::metamodel::type::Decimal[1]):meta::pure::metamodel::type::Decimal[1];");
    public static final NativeFunctionDefinition SUB__FLOAT_1__FLOAT_1 = signature("native function meta::legend::lite::sub(left:meta::pure::metamodel::type::Float[1], right:meta::pure::metamodel::type::Float[1]):meta::pure::metamodel::type::Float[1];");
    public static final NativeFunctionDefinition SUB__INTEGER_1__INTEGER_1 = signature("native function meta::legend::lite::sub(left:meta::pure::metamodel::type::Integer[1], right:meta::pure::metamodel::type::Integer[1]):meta::pure::metamodel::type::Integer[1];");
    public static final NativeFunctionDefinition SUB__NUMBER_1__NUMBER_1 = signature("native function meta::legend::lite::sub(left:meta::pure::metamodel::type::Number[1], right:meta::pure::metamodel::type::Number[1]):meta::pure::metamodel::type::Number[1];");
    public static final NativeFunctionDefinition SUM__FLOAT_MANY = signature("native function meta::pure::functions::math::sum(numbers:meta::pure::metamodel::type::Float[*]):meta::pure::metamodel::type::Float[1];");
    public static final NativeFunctionDefinition SUM__INTEGER_MANY = signature("native function meta::pure::functions::math::sum(numbers:meta::pure::metamodel::type::Integer[*]):meta::pure::metamodel::type::Integer[1];");
    public static final NativeFunctionDefinition SUM__NUMBER_MANY = signature("native function meta::pure::functions::math::sum(numbers:meta::pure::metamodel::type::Number[*]):meta::pure::metamodel::type::Number[1];");
    public static final NativeFunctionDefinition TABLE_REFERENCE__STRING_1__STRING_1 = signature("native function meta::relational::functions::database::tableReference(db:meta::pure::metamodel::type::String[1], name:meta::pure::metamodel::type::String[1]):meta::pure::metamodel::relation::Relation<meta::pure::metamodel::type::Any>[1];");
    public static final NativeFunctionDefinition TAIL__T_MANY = signature("native function meta::pure::functions::collection::tail<T>(set:T[*]):T[*];");
    public static final NativeFunctionDefinition TAKE__RELATION_1__INTEGER_1 = signature("native function meta::pure::functions::collection::take<T>(rel:meta::pure::metamodel::relation::Relation<T>[1], size:meta::pure::metamodel::type::Integer[1]):meta::pure::metamodel::relation::Relation<T>[1];");
    public static final NativeFunctionDefinition TAKE__T_MANY__INTEGER_1 = signature("native function meta::pure::functions::collection::take<T>(set:T[*], size:meta::pure::metamodel::type::Integer[1]):T[*];");
    public static final NativeFunctionDefinition TANH__NUMBER_1 = signature("native function meta::pure::functions::math::tanh(number:meta::pure::metamodel::type::Number[1]):meta::pure::metamodel::type::Float[1];");
    public static final NativeFunctionDefinition TAN__NUMBER_1 = signature("native function meta::pure::functions::math::tan(number:meta::pure::metamodel::type::Number[1]):meta::pure::metamodel::type::Float[1];");
    public static final NativeFunctionDefinition TDS__STRING_1__STRING_1 = signature("native function meta::legend::lite::tds(tag:meta::pure::metamodel::type::String[1], raw:meta::pure::metamodel::type::String[1]):meta::pure::metamodel::relation::Relation<meta::pure::metamodel::type::Any>[1];");
    public static final NativeFunctionDefinition TIMES__DECIMAL_1__DECIMAL_1 = signature("native function meta::pure::functions::math::times(left:meta::pure::metamodel::type::Decimal[1], right:meta::pure::metamodel::type::Decimal[1]):meta::pure::metamodel::type::Decimal[1];");
    public static final NativeFunctionDefinition TIMES__FLOAT_1__FLOAT_1 = signature("native function meta::pure::functions::math::times(left:meta::pure::metamodel::type::Float[1], right:meta::pure::metamodel::type::Float[1]):meta::pure::metamodel::type::Float[1];");
    public static final NativeFunctionDefinition TIMES__INTEGER_1__INTEGER_1 = signature("native function meta::pure::functions::math::times(left:meta::pure::metamodel::type::Integer[1], right:meta::pure::metamodel::type::Integer[1]):meta::pure::metamodel::type::Integer[1];");
    public static final NativeFunctionDefinition TIMES__NUMBER_1__NUMBER_1 = signature("native function meta::pure::functions::math::times(left:meta::pure::metamodel::type::Number[1], right:meta::pure::metamodel::type::Number[1]):meta::pure::metamodel::type::Number[1];");
    public static final NativeFunctionDefinition TIMES__T_MANY = signature("native function meta::pure::functions::math::times<T>(values:T[*]):T[1];");
    // timeBucket: REAL pure has CONCRETE overloads (core_functions_standard/date/operation/timeBucket.pure)
    // — the abstract Date form was ours, and it broke lattice-kind recovery (midnight buckets).
    public static final NativeFunctionDefinition TIME_BUCKET__DATETIME_1__INTEGER_1__DURATION_UNIT_1 = signature("native function meta::pure::functions::date::timeBucket(date:meta::pure::metamodel::type::DateTime[1], quantity:meta::pure::metamodel::type::Integer[1], unit:meta::pure::functions::date::DurationUnit[1]):meta::pure::metamodel::type::DateTime[1];");
    public static final NativeFunctionDefinition TIME_BUCKET__STRICTDATE_1__INTEGER_1__DURATION_UNIT_1 = signature("native function meta::pure::functions::date::timeBucket(date:meta::pure::metamodel::type::StrictDate[1], quantity:meta::pure::metamodel::type::Integer[1], unit:meta::pure::functions::date::DurationUnit[1]):meta::pure::metamodel::type::StrictDate[1];");
    public static final NativeFunctionDefinition TODAY = signature("native function meta::pure::functions::date::today():meta::pure::metamodel::type::StrictDate[1];");
    public static final NativeFunctionDefinition TO_DECIMAL__NUMBER_1 = signature("native function meta::pure::functions::math::toDecimal(number:meta::pure::metamodel::type::Number[1]):meta::pure::metamodel::type::Decimal[1];");
    public static final NativeFunctionDefinition TO_DEGREES__NUMBER_1 = signature("native function meta::pure::functions::math::toDegrees(radians:meta::pure::metamodel::type::Number[1]):meta::pure::metamodel::type::Float[1];");
    public static final NativeFunctionDefinition TO_EPOCH_VALUE__DATE_1 = signature("native function meta::pure::functions::date::toEpochValue(d:meta::pure::metamodel::type::Date[1]):meta::pure::metamodel::type::Integer[1];");
    public static final NativeFunctionDefinition TO_EPOCH_VALUE__DATE_1__DURATION_UNIT_1 = signature("native function meta::pure::functions::date::toEpochValue(d:meta::pure::metamodel::type::Date[1], unit:meta::pure::functions::date::DurationUnit[1]):meta::pure::metamodel::type::Integer[1];");
    public static final NativeFunctionDefinition TO_FLOAT__NUMBER_1 = signature("native function meta::pure::functions::math::toFloat(number:meta::pure::metamodel::type::Number[1]):meta::pure::metamodel::type::Float[1];");
    public static final NativeFunctionDefinition TO_LOWER_FIRST_CHARACTER__STRING_1 = signature("native function meta::pure::functions::string::toLowerFirstCharacter(str:meta::pure::metamodel::type::String[1]):meta::pure::metamodel::type::String[1];");
    public static final NativeFunctionDefinition TO_LOWER__STRING_1 = signature("native function meta::pure::functions::string::toLower(source:meta::pure::metamodel::type::String[1]):meta::pure::metamodel::type::String[1];");
    public static final NativeFunctionDefinition TO_MANY__T_0_1__V_0_1 = signature("native function meta::pure::functions::variant::convert::toMany<T,V>(source:T[0..1], type:V[0..1]):V[*];");
    public static final NativeFunctionDefinition TO_ONE_MANY__T_MANY__STRING_1 = signature("native function meta::pure::functions::multiplicity::toOneMany<T>(values:T[*], message:meta::pure::metamodel::type::String[1]):T[1..*];");
    public static final NativeFunctionDefinition TO_ONE__T_MANY = signature("native function meta::pure::functions::multiplicity::toOne<T>(values:T[*]):T[1];");
    public static final NativeFunctionDefinition TO_ONE__T_MANY__STRING_1 = signature("native function meta::pure::functions::multiplicity::toOne<T>(values:T[*], message:meta::pure::metamodel::type::String[1]):T[1];");
    public static final NativeFunctionDefinition TO_RADIANS__NUMBER_1 = signature("native function meta::pure::functions::math::toRadians(degrees:meta::pure::metamodel::type::Number[1]):meta::pure::metamodel::type::Float[1];");
    public static final NativeFunctionDefinition TO_STRING__ANY_1 = signature("native function meta::pure::functions::string::toString(any:meta::pure::metamodel::type::Any[1]):meta::pure::metamodel::type::String[1];");
    public static final NativeFunctionDefinition TO_UPPER_FIRST_CHARACTER__STRING_1 = signature("native function meta::pure::functions::string::toUpperFirstCharacter(str:meta::pure::metamodel::type::String[1]):meta::pure::metamodel::type::String[1];");
    public static final NativeFunctionDefinition TO_UPPER__STRING_1 = signature("native function meta::pure::functions::string::toUpper(source:meta::pure::metamodel::type::String[1]):meta::pure::metamodel::type::String[1];");
    public static final NativeFunctionDefinition TO_VARIANT__ANY_MANY = signature("native function meta::pure::functions::variant::convert::toVariant(source:meta::pure::metamodel::type::Any[*]):meta::pure::metamodel::variant::Variant[1];");
    public static final NativeFunctionDefinition TO__T_0_1__V_0_1 = signature("native function meta::pure::functions::variant::convert::to<T,V>(source:T[0..1], type:V[0..1]):V[0..1];");
    public static final NativeFunctionDefinition TRAVERSE__RELATION_1__FUNCTION_1 = signature("native function meta::legend::lite::traverse<T,V>(target:meta::pure::metamodel::relation::Relation<V>[1], cond:meta::pure::metamodel::function::Function<{T[1],V[1]->meta::pure::metamodel::type::Boolean[1]}>[1]):meta::pure::functions::relation::_Traversal[1];");
    public static final NativeFunctionDefinition TRAVERSE__TRAVERSAL_1__RELATION_1__FUNCTION_1 = signature("native function meta::legend::lite::traverse<T,V>(prev:meta::pure::functions::relation::_Traversal[1], target:meta::pure::metamodel::relation::Relation<V>[1], cond:meta::pure::metamodel::function::Function<{T[1],V[1]->meta::pure::metamodel::type::Boolean[1]}>[1]):meta::pure::functions::relation::_Traversal[1];");
    public static final NativeFunctionDefinition TRIM__STRING_1 = signature("native function meta::pure::functions::string::trim(str:meta::pure::metamodel::type::String[1]):meta::pure::metamodel::type::String[1];");
    public static final NativeFunctionDefinition TYPE__ANY_1 = signature("native function meta::pure::functions::meta::type(any:meta::pure::metamodel::type::Any[1]):meta::pure::metamodel::type::Type[1];");
    public static final NativeFunctionDefinition UNBOUNDED = signature("native function meta::pure::functions::relation::unbounded():meta::pure::functions::relation::UnboundedFrameValue[1];");
    public static final NativeFunctionDefinition VARIANCE_POPULATION__NUMBER_MANY = signature("native function meta::pure::functions::math::variancePopulation(numbers:meta::pure::metamodel::type::Number[*]):meta::pure::metamodel::type::Number[1];");
    public static final NativeFunctionDefinition VARIANCE_SAMPLE__NUMBER_MANY = signature("native function meta::pure::functions::math::varianceSample(numbers:meta::pure::metamodel::type::Number[*]):meta::pure::metamodel::type::Number[1];");
    public static final NativeFunctionDefinition VARIANCE__NUMBER_MANY = signature("native function meta::pure::functions::math::variance(numbers:meta::pure::metamodel::type::Number[*]):meta::pure::metamodel::type::Number[1];");
    // CORPUS-SHAPE window overload (no real-pure counterpart): the
    // first/last 3-arg spelling with the column named by the wrapping
    // property access — conform-by-emission, VARIANCE(col) OVER (...).
    public static final NativeFunctionDefinition VARIANCE__RELATION_1__WINDOW_1__T_1 = signature("native function meta::pure::functions::math::variance<T>(w:meta::pure::metamodel::relation::Relation<T>[1], f:meta::pure::functions::relation::_Window<T>[1], r:T[1]):T[0..1];");
    public static final NativeFunctionDefinition VARIANCE__NUMBER_MANY__BOOLEAN_1 = signature("native function meta::pure::functions::math::variance(numbers:meta::pure::metamodel::type::Number[*], isSample:meta::pure::metamodel::type::Boolean[1]):meta::pure::metamodel::type::Number[1];");
    // lite spelling of REAL pure meta::pure::functions::variant::convert::to/toMany.
    public static final NativeFunctionDefinition VARIANT_TO__ANY_1__T_1 = signature("native function meta::legend::lite::variantTo<T>(val:meta::pure::metamodel::type::Any[1], type:T[1]):T[1];");
    public static final NativeFunctionDefinition WAVG__ROW_MAPPER_MANY = signature("native function meta::pure::functions::math::wavg<T,U>(values:meta::pure::functions::math::mathUtility::RowMapper<T,U>[*]):meta::pure::metamodel::type::Float[1];");
    public static final NativeFunctionDefinition WEEK_OF_YEAR__DATE_1 = signature("native function meta::pure::functions::date::weekOfYear(d:meta::pure::metamodel::type::Date[1]):meta::pure::metamodel::type::Integer[1];");
    public static final NativeFunctionDefinition WRITE__RELATION_1 = signature("native function meta::pure::functions::relation::write<T>(source:meta::pure::metamodel::relation::Relation<T>[1]):meta::pure::metamodel::type::Integer[1];");
    public static final NativeFunctionDefinition WRITE__RELATION_1__ANY_1 = signature("native function meta::pure::functions::relation::write<T>(source:meta::pure::metamodel::relation::Relation<T>[1], target:meta::pure::metamodel::type::Any[1]):meta::pure::metamodel::type::Integer[1];");
    public static final NativeFunctionDefinition XOR__BOOLEAN_1__BOOLEAN_1 = signature("native function meta::pure::functions::boolean::xor(left:meta::pure::metamodel::type::Boolean[1], right:meta::pure::metamodel::type::Boolean[1]):meta::pure::metamodel::type::Boolean[1];");
    public static final NativeFunctionDefinition YEAR__DATE_1 = signature("native function meta::pure::functions::date::year(d:meta::pure::metamodel::type::Date[1]):meta::pure::metamodel::type::Integer[1];");
    public static final NativeFunctionDefinition ZIP__T_MANY__U_MANY = signature("native function meta::pure::functions::collection::zip<T,U>(set1:T[*], set2:U[*]):meta::pure::functions::collection::Pair<T,U>[*];");
    public static final NativeFunctionDefinition _RANGE__NUMBER_1__NUMBER_1 = signature("native function meta::pure::functions::relation::_range(offsetFrom:meta::pure::metamodel::type::Number[1], offsetTo:meta::pure::metamodel::type::Number[1]):meta::pure::functions::relation::_Range[1];");
    public static final NativeFunctionDefinition _RANGE__UNBOUNDED_1__NUMBER_1 = signature("native function meta::pure::functions::relation::_range(offsetFrom:meta::pure::functions::relation::UnboundedFrameValue[1], offsetTo:meta::pure::metamodel::type::Number[1]):meta::pure::functions::relation::_Range[1];");
    public static final NativeFunctionDefinition _RANGE__NUMBER_1__UNBOUNDED_1 = signature("native function meta::pure::functions::relation::_range(offsetFrom:meta::pure::metamodel::type::Number[1], offsetTo:meta::pure::functions::relation::UnboundedFrameValue[1]):meta::pure::functions::relation::_Range[1];");
    public static final NativeFunctionDefinition _RANGE__INT_1__DU_1__INT_1__DU_1 = signature("native function meta::pure::functions::relation::_range(offsetFrom:meta::pure::metamodel::type::Integer[1], offsetFromDurationUnit:meta::pure::functions::date::DurationUnit[1], offsetTo:meta::pure::metamodel::type::Integer[1], offsetToDurationUnit:meta::pure::functions::date::DurationUnit[1]):meta::pure::functions::relation::_RangeInterval[1];");
    public static final NativeFunctionDefinition _RANGE__UNBOUNDED_1__INT_1__DU_1 = signature("native function meta::pure::functions::relation::_range(offsetFrom:meta::pure::functions::relation::UnboundedFrameValue[1], offsetTo:meta::pure::metamodel::type::Integer[1], offsetToDurationUnit:meta::pure::functions::date::DurationUnit[1]):meta::pure::functions::relation::_RangeInterval[1];");
    public static final NativeFunctionDefinition _RANGE__INT_1__DU_1__UNBOUNDED_1 = signature("native function meta::pure::functions::relation::_range(offsetFrom:meta::pure::metamodel::type::Integer[1], offsetFromDurationUnit:meta::pure::functions::date::DurationUnit[1], offsetTo:meta::pure::functions::relation::UnboundedFrameValue[1]):meta::pure::functions::relation::_RangeInterval[1];");
    public static final NativeFunctionDefinition _RANGE__UNBOUNDED_1__UNBOUNDED_1 = signature("native function meta::pure::functions::relation::_range(offsetFrom:meta::pure::functions::relation::UnboundedFrameValue[1], offsetTo:meta::pure::functions::relation::UnboundedFrameValue[1]):meta::pure::functions::relation::_Range[1];");
}
