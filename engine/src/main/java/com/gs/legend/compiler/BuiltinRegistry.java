package com.gs.legend.compiler;
import com.gs.legend.model.m3.Type;

import com.gs.legend.model.def.EnumDefinition;
import com.gs.legend.model.m3.Primitive;

import java.util.*;

/**
 * Registry of built-in function signatures parsed from Pure native function
 * declarations.
 *
 * <p>
 * This is the single source of truth for function type-checking. Every built-in
 * function (relation functions from legend-engine, scalar/aggregate/window
 * functions
 * from legend-pure) must be registered here. If a function isn't registered,
 * the
 * TypeChecker rejects it — <b>no passthrough</b>.
 *
 * <h3>Architecture</h3>
 * 
 * <pre>
 * Pure signature string  →  PureParser.parseNativeFunction()  →  NativeFunctionDef
 *                                                                       ↓
 *                                                              BuiltinRegistry
 *                                                                       ↓
 *                                                              TypeChecker.resolve()
 * </pre>
 *
 * <h3>Two tiers</h3>
 * <ul>
 * <li><b>Tier 1 — Relation functions</b> (~50 overloads): transform data shape
 * (rich type vars, schema algebra). Custom compile methods in TypeChecker.</li>
 * <li><b>Tie r 2 — DynaFunctions</b> (~225): transform data values
 * (scalars, aggregates, windows). Generic compilation via registry.</li>
 * </ul>
 *
 * @see NativeFunctionDef
 * @see Type
 * @see Mult
 */
public class BuiltinRegistry {

    /** All overloads keyed by simple function name. */
    private final Map<String, List<NativeFunctionDef>> functions = new LinkedHashMap<>();

    /** Total count of registered overloads (for reporting). */
    private int totalOverloads = 0;

    /**
     * Register a native function definition.
     * Multiple overloads of the same function name are allowed.
     */
    public void register(NativeFunctionDef def) {
        functions.computeIfAbsent(def.name(), k -> new ArrayList<>()).add(def);
        totalOverloads++;
    }

    /**
     * Register a native function by name and Pure signature string.
     * Parses the signature via PureParser into a structured NativeFunctionDef.
     *
     * Pre-processes the signature to strip Pure-specific constraint syntax
     * that the grammar doesn't model directly:
     * - Z⊆T → Z (subset constraint, preserved in rawSignature)
     * - Z=(?:K)⊆T → Z (type-match constraint, preserved in rawSignature)
     * - Relation<T+V> → Relation<T_plus_V> (schema union)
     * - Relation<T-Z+V> → Relation<T_minus_Z_plus_V> (schema remove+add)
     *
     * @param name          Simple function name (e.g., "filter", "plus", "toLower")
     * @param pureSignature Full Pure native function declaration string
     */
    public void registerSignature(String name, String pureSignature) {
        String rawSignature = pureSignature.trim();
        var def = com.gs.legend.parser.PureNativeSignatureParser.parse(rawSignature);
        register(normalizeConstraints(def));
    }

    /**
     * Post-parse normalization: strips SchemaAlgebra constraints from the Type tree,
     * leaving bare type variables for overload resolution and structural matching.
     *
     * <p>Constraints are metadata (e.g., Z⊆T means "Z must be a column subset of T").
     * For type matching, only the type variable (Z) matters. The constraint is preserved
     * in {@code rawSignature} for schema inference.
     *
     * <p>Examples:
     * <ul>
     *   <li>{@code ColSpecArray<Z⊆T>} → {@code ColSpecArray<Z>}</li>
     *   <li>{@code Relation<T+V>} → {@code Relation<T>} (algebra stripped, rawSignature preserved)</li>
     *   <li>{@code ColSpec<Z=(?:K)⊆T>} → {@code ColSpec<Z>}</li>
     * </ul>
     */
    private static NativeFunctionDef normalizeConstraints(NativeFunctionDef def) {
        var normalizedParams = def.params().stream()
                .map(p -> new Type.Parameter(p.name(), normalizeType(p.type()), p.multiplicity()))
                .toList();
        Type normalizedReturn = normalizeType(def.returnType());
        return new NativeFunctionDef(
                def.name(), def.typeParams(), def.multParams(),
                normalizedParams, normalizedReturn, def.returnMult(),
                def.rawSignature());
    }

    /**
     * Recursively normalizes a Type by stripping SchemaAlgebra to its left operand.
     * Only strips constraint operations (Subset, Equal) — these are validation metadata.
     * Preserves structural operations (Union, Difference) — these describe actual schema shape
     * needed for join/rename return types like {@code Relation<T+V>} and {@code Relation<T-Z+V>}.
     *
     * <p>Exhaustive over the {@link Type} sealed hierarchy: structural variants
     * recurse into their children; leaves (anything that can't contain a SchemaAlgebra)
     * return as-is.
     */
    private static Type normalizeType(Type type) {
        return switch (type) {
            case Type.SchemaAlgebra sa -> switch (sa.op()) {
                // Constraints: strip to left operand (the type variable)
                case SUBSET, EQUAL -> normalizeType(sa.left());
                // Structural: keep but normalize children
                case UNION, DIFFERENCE -> new Type.SchemaAlgebra(
                        normalizeType(sa.left()), sa.op(), normalizeType(sa.right()));
            };
            case Type.Parameterized p -> new Type.Parameterized(
                    p.rawType(),
                    p.typeArgs().stream().map(BuiltinRegistry::normalizeType).toList());
            case Type.FunctionType ft -> new Type.FunctionType(
                    ft.params().stream()
                            .map(p -> new Type.Parameter(p.name(), normalizeType(p.type()), p.multiplicity()))
                            .toList(),
                    normalizeType(ft.returnType()),
                    ft.returnMult());
            // Leaves — no nested Type children that could contain a SchemaAlgebra.
            case Primitive p -> p;
            case Type.NameRef nr -> nr;
            case Type.ClassType c -> c;
            case Type.EnumType e -> e;
            case Type.PrecisionDecimal pd -> pd;
            case Type.TypeVar tv -> tv;
            case Type.Relation r -> r;
            case Type.Tuple t -> t;
            case Type.FunctionReference fr -> fr;
            case Type.RelationTypeVar rtv -> rtv;
        };
    }

    /**
     * Look up all overloads of a function by simple name.
     * Returns empty list if not registered.
     */
    public List<NativeFunctionDef> resolve(String name) {
        return functions.getOrDefault(name, List.of());
    }

    /**
     * Resolve a function signature by name, arity, and source type.
     *
     * <p>Strict matching — no silent fallbacks:
     * <ol>
     *   <li>Filter by arity — throw if no overload matches</li>
     *   <li>If exactly one match — return it</li>
     *   <li>If multiple — disambiguate by relational/scalar context</li>
     *   <li>If still ambiguous — throw</li>
     * </ol>
     *
     * @param fn     Simple function name
     * @param arity  Number of actual parameters
     * @param source TypeInfo of the source (first param), or null for no-source calls
     * @return The single matching NativeFunctionDef
     * @throws PureCompileException if not registered, no arity match, or ambiguous
     */
    public NativeFunctionDef resolveSignature(String fn, int arity, TypeInfo source) {
        var defs = resolve(fn);
        if (defs.isEmpty()) {
            throw new PureCompileException("Unknown function: '" + fn + "'");
        }

        // Step 1: filter by exact arity first; fall back to variadic [*] matches
        var arityMatches = defs.stream().filter(d -> d.arity() == arity).toList();
        if (arityMatches.isEmpty()) {
            arityMatches = defs.stream().filter(d -> d.matchesArity(arity)).toList();
        }
        if (arityMatches.isEmpty()) {
            throw new PureCompileException(
                    "No overload of '" + fn + "' accepts " + arity + " arguments "
                    + "(registered arities: " + defs.stream().map(d -> String.valueOf(d.arity())).distinct().toList() + ")");
        }
        if (arityMatches.size() == 1) {
            return arityMatches.get(0);
        }

        // Step 2: disambiguate by relational/scalar
        boolean srcRelational = source != null && source.isRelational();
        var contextMatches = arityMatches.stream()
                .filter(d -> isRelationalOverload(d) == srcRelational)
                .toList();
        if (contextMatches.size() == 1) {
            return contextMatches.get(0);
        }
        if (contextMatches.isEmpty()) {
            throw new PureCompileException(
                    "No " + (srcRelational ? "relational" : "scalar") + " overload of '"
                    + fn + "' with arity " + arity);
        }

        // Multiple matches even after disambiguation — ambiguous
        throw new PureCompileException(
                "Ambiguous overload: " + contextMatches.size() + " overloads of '"
                + fn + "' match arity " + arity + " in " + (srcRelational ? "relational" : "scalar") + " context");
    }

    /**
     * Whether a function def's first parameter expects a Relation type.
     */
    private static boolean isRelationalOverload(NativeFunctionDef def) {
        return !def.params().isEmpty()
                && def.params().get(0).type() instanceof Type.Parameterized p
                && "Relation".equals(p.rawType());
    }

    /**
     * Check if a function is registered (any overload).
     */
    public boolean isRegistered(String name) {
        return functions.containsKey(name);
    }

    /**
     * All registered function names (unordered).
     */
    public Set<String> allFunctionNames() {
        return Collections.unmodifiableSet(functions.keySet());
    }

    /**
     * All registered functions with their overloads.
     */
    public Map<String, List<NativeFunctionDef>> allRegistered() {
        return Collections.unmodifiableMap(functions);
    }

    /**
     * Total number of registered function names (not counting overloads).
     */
    public int functionCount() {
        return functions.size();
    }

    /**
     * Total number of registered overloads across all functions.
     */
    public int overloadCount() {
        return totalOverloads;
    }

    // ===== Aggregate convenience accessors =====
    // Cached at init for identity-based dispatch (==) in PlanGenerator.

    private NativeFunctionDef wavgDef, rowMapperDef, hashCodeDef;
    private NativeFunctionDef corrDef, covarSampleDef, covarPopulationDef;
    private NativeFunctionDef maxByDef, minByDef;

    private void cacheConvenienceDefs() {
        wavgDef = resolve("wavg").get(0);
        rowMapperDef = resolve("rowMapper").get(0);
        hashCodeDef = resolve("hashCode").get(0);
        corrDef = resolve("corr").get(0);
        covarSampleDef = resolve("covarSample").get(0);
        covarPopulationDef = resolve("covarPopulation").get(0);
        maxByDef = resolve("maxBy").get(0);
        minByDef = resolve("minBy").get(0);
    }

    public NativeFunctionDef wavg()              { return wavgDef; }
    public NativeFunctionDef rowMapper()         { return rowMapperDef; }
    public NativeFunctionDef hashCodeAgg()       { return hashCodeDef; }
    public NativeFunctionDef corr()              { return corrDef; }
    public NativeFunctionDef covarSample()       { return covarSampleDef; }
    public NativeFunctionDef covarPopulation()   { return covarPopulationDef; }
    public NativeFunctionDef maxBy()             { return maxByDef; }
    public NativeFunctionDef minBy()             { return minByDef; }

    // ===== Platform enums =====
    //
    // Platform-defined enums referenced by built-in function signatures (JoinKind, DurationUnit,
    // Month, etc.). Previously registered per-PureModelBuilder via registerPlatformEnums();
    // now registered once at JVM init here as the source of truth. PureModelBuilder pulls them
    // via PLATFORM_ENUMS and registers in its local symbol table at construction.

    public static final List<EnumDefinition> PLATFORM_ENUMS = List.of(
            // Date enums (legend-pure: essential/date/_structures.pure)
            EnumDefinition.of("meta::pure::functions::date::Month",
                    "January", "February", "March", "April", "May", "June",
                    "July", "August", "September", "October", "November", "December"),
            EnumDefinition.of("meta::pure::functions::date::Quarter",
                    "Q1", "Q2", "Q3", "Q4"),
            EnumDefinition.of("meta::pure::functions::date::DayOfWeek",
                    "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"),
            EnumDefinition.of("meta::pure::functions::date::DurationUnit",
                    "YEARS", "MONTHS", "WEEKS", "DAYS", "HOURS", "MINUTES",
                    "SECONDS", "MILLISECONDS", "MICROSECONDS", "NANOSECONDS"),
            // Relation enums (legend-engine: relation/functions/transformation/)
            EnumDefinition.of("meta::pure::functions::relation::JoinKind",
                    "INNER", "LEFT", "RIGHT", "FULL"),
            EnumDefinition.of("meta::pure::functions::relation::SortType",
                    "ASC", "DESC"),
            // Legacy relational TDS sort direction (legend-pure: platform_store_relational/grammar/relational.pure).
            // Distinct from SortType above: SortDirection is the enum parameter to the
            // legacy 3-arg sort(rel, 'col', direction); SortType is the modern
            // ascending/descending / SortInfo-driven relation sort.
            EnumDefinition.of("meta::relational::metamodel::SortDirection",
                    "ASC", "DESC"),
            // Hash enum (legend-engine: hash/hash.pure)
            EnumDefinition.of("meta::pure::functions::hash::HashType",
                    "MD5", "SHA1", "SHA256"));

    /** FQN → EnumDefinition lookup for platform enums (immutable). */
    private static final Map<String, EnumDefinition> PLATFORM_ENUMS_BY_FQN;
    static {
        Map<String, EnumDefinition> m = new LinkedHashMap<>();
        for (EnumDefinition e : PLATFORM_ENUMS) m.put(e.qualifiedName(), e);
        PLATFORM_ENUMS_BY_FQN = Map.copyOf(m);
    }

    /** Looks up a platform enum by fully qualified name. */
    public static Optional<EnumDefinition> findPlatformEnum(String fqn) {
        return Optional.ofNullable(PLATFORM_ENUMS_BY_FQN.get(fqn));
    }

    /**
     * Fully qualified names of every built-in type (15 primitives + all platform enums) that
     * should be auto-imported into every {@code PureModelBuilder}'s initial {@code ImportScope}.
     *
     * <p>{@code PureModelBuilder} calls {@code imports.addImport(fqn)} for each entry at
     * construction so that user Pure source referencing {@code Integer}, {@code DurationUnit},
     * {@code JoinKind} etc. by simple name resolves to the correct FQN via the standard import
     * mechanism.
     *
     * <p>Name-by-name (not wildcard) — unambiguous, O(1) HashMap hit in {@code typeImports},
     * no future-addition magic. Exposed as immutable data (not a void-setter method) so callers
     * own the application step and the registry stays a pure catalog.
     */
    public static final List<String> BUILTIN_IMPORTS;
    static {
        var imports = new ArrayList<String>(Primitive.ALL.size() + PLATFORM_ENUMS.size());
        for (Primitive p : Primitive.ALL) imports.add(p.qualifiedName());
        for (EnumDefinition e : PLATFORM_ENUMS) imports.add(e.qualifiedName());
        BUILTIN_IMPORTS = List.copyOf(imports);
    }

    // ===== Singleton =====

    private static final BuiltinRegistry INSTANCE = createDefault();

    /**
     * Returns the singleton registry with all built-in functions registered.
     */
    public static BuiltinRegistry instance() {
        return INSTANCE;
    }

    // ===== Registration =====

    private static BuiltinRegistry createDefault() {
        var reg = new BuiltinRegistry();
        registerRelationFunctions(reg);
        registerScalarFunctions(reg);
        reg.cacheConvenienceDefs();
        return reg;
    }

    /**
     * Tier 1: Relation functions from legend-engine.
     * ~50 overloads across ~20 functions that transform data shape.
     */
    private static void registerRelationFunctions(BuiltinRegistry reg) {
        // Shape-preserving
        reg.registerSignature("filter",
                "native function filter<T>(rel:Relation<T>[1], f:Function<{T[1]->meta::pure::metamodel::type::Boolean[1]}>[1]):Relation<T>[1];");
        reg.registerSignature("sort",
                "native function sort<X,T>(rel:Relation<T>[1], sortInfo:SortInfo<X⊆T>[*]):Relation<T>[1];");
        // Legacy TDS: sort(rel, colName, direction) — desugared in SortChecker
        reg.registerSignature("sort",
                "native function sort<T>(rel:Relation<T>[1], col:meta::pure::metamodel::type::String[1], direction:meta::relational::metamodel::SortDirection[1]):Relation<T>[1];");
        // Legacy TDS: sort(rel, colNames) — defaults to ascending (Handlers.java line 1986)
        reg.registerSignature("sort",
                "native function sort<T>(rel:Relation<T>[1], cols:meta::pure::metamodel::type::String[*]):Relation<T>[1];");
        // limit, drop, slice registered in registerScalarFunctions (relation + scalar overloads)
        reg.registerSignature("concatenate",
                "native function concatenate<T>(rel1:Relation<T>[1], rel2:Relation<T>[1]):Relation<T>[1];");
        reg.registerSignature("size", "native function size<T>(rel:Relation<T>[1]):meta::pure::metamodel::type::Integer[1];");

        // Distinct
        reg.registerSignature("distinct", "native function distinct<T>(rel:Relation<T>[1]):Relation<T>[1];");
        reg.registerSignature("distinct",
                "native function distinct<X,T>(rel:Relation<T>[1], columns:ColSpecArray<X⊆T>[1]):Relation<X>[1];");

        // Select
        reg.registerSignature("select", "native function select<T>(r:Relation<T>[1]):Relation<T>[1];");
        reg.registerSignature("select",
                "native function select<T,Z>(r:Relation<T>[1], cols:ColSpec<Z⊆T>[1]):Relation<Z>[1];");
        reg.registerSignature("select",
                "native function select<T,Z>(r:Relation<T>[1], cols:ColSpecArray<Z⊆T>[1]):Relation<Z>[1];");

        // Rename
        reg.registerSignature("rename",
                "native function rename<T,Z,K,V>(r:Relation<T>[1], old:ColSpec<Z=(?:K)⊆T>[1], new:ColSpec<V=(?:K)>[1]):Relation<T-Z+V>[1];");
        reg.registerSignature("rename",
                "native function rename<T,Z,V>(r:Relation<T>[1], oldCols:ColSpecArray<Z⊆T>[1], newCols:ColSpecArray<V>[1]):Relation<T-Z+V>[1];");

        // Extend — scalar (Relation source)
        reg.registerSignature("extend",
                "native function extend<T,Z>(r:Relation<T>[1], f:FuncColSpec<{T[1]->meta::pure::metamodel::type::Any[0..1]},Z>[1]):Relation<T+Z>[1];");
        reg.registerSignature("extend",
                "native function extend<T,Z>(r:Relation<T>[1], fs:FuncColSpecArray<{T[1]->meta::pure::metamodel::type::Any[*]},Z>[1]):Relation<T+Z>[1];");
        // Extend — scalar (Class source): stays in object space (C[*] -> C[*])
        reg.registerSignature("extend",
                "native function extend<C,Z>(cl:C[*], f:FuncColSpec<{C[1]->meta::pure::metamodel::type::Any[0..1]},Z>[1]):C[*];");
        reg.registerSignature("extend",
                "native function extend<C,Z>(cl:C[*], fs:FuncColSpecArray<{C[1]->meta::pure::metamodel::type::Any[*]},Z>[1]):C[*];");
        // Extend — aggregate
        reg.registerSignature("extend",
                "native function extend<T,K,V,R>(r:Relation<T>[1], agg:AggColSpec<{T[1]->K[0..1]},{K[*]->V[0..1]},R>[1]):Relation<T+R>[1];");
        reg.registerSignature("extend",
                "native function extend<T,K,V,R>(r:Relation<T>[1], agg:AggColSpecArray<{T[1]->K[0..1]},{K[*]->V[0..1]},R>[1]):Relation<T+R>[1];");
        // Extend — window scalar
        reg.registerSignature("extend",
                "native function extend<T,Z,W,R>(r:Relation<T>[1], window:_Window<T>[1], f:FuncColSpec<{Relation<T>[1],_Window<T>[1],T[1]->meta::pure::metamodel::type::Any[0..1]},R>[1]):Relation<T+R>[1];");
        reg.registerSignature("extend",
                "native function extend<T,Z,W,R>(r:Relation<T>[1], window:_Window<T>[1], f:FuncColSpecArray<{Relation<T>[1],_Window<T>[1],T[1]->meta::pure::metamodel::type::Any[*]},R>[1]):Relation<T+R>[1];");
        // Extend — window aggregate (3-arity fn1: {p,w,r|...})
        reg.registerSignature("extend",
                "native function extend<T,K,V,R>(r:Relation<T>[1], window:_Window<T>[1], agg:AggColSpec<{Relation<T>[1],_Window<T>[1],T[1]->K[0..1]},{K[*]->V[0..1]},R>[1]):Relation<T+R>[1];");
        reg.registerSignature("extend",
                "native function extend<T,K,V,R>(r:Relation<T>[1], window:_Window<T>[1], agg:AggColSpecArray<{Relation<T>[1],_Window<T>[1],T[1]->K[0..1]},{K[*]->V[0..1]},R>[1]):Relation<T+R>[1];");
        // Extend — traverse (FK path traversal): 2-param lambda {src,tgt|...}
        // S = source relation schema, T = terminal (joined) table schema
        reg.registerSignature("extend",
                "native function extend<S,T,Z>(r:Relation<S>[1], path:_Traversal[1], f:FuncColSpec<{S[1],T[1]->meta::pure::metamodel::type::Any[0..1]},Z>[1]):Relation<S+Z>[1];");
        reg.registerSignature("extend",
                "native function extend<S,T,Z>(r:Relation<S>[1], path:_Traversal[1], fs:FuncColSpecArray<{S[1],T[1]->meta::pure::metamodel::type::Any[*]},Z>[1]):Relation<S+Z>[1];");

        // ========== Traverse — chainable FK path hops ==========
        reg.registerSignature("traverse",
                "native function traverse<T,V>(target:Relation<V>[1], cond:Function<{T[1],V[1]->meta::pure::metamodel::type::Boolean[1]}>[1]):_Traversal[1];");
        reg.registerSignature("traverse",
                "native function traverse<T,V>(prev:_Traversal[1], target:Relation<V>[1], cond:Function<{T[1],V[1]->meta::pure::metamodel::type::Boolean[1]}>[1]):_Traversal[1];");

        // ========== Sort direction constructors ==========
        reg.registerSignature("ascending",
                "native function ascending<T>(column:ColSpec<T>[1]):SortInfo<T>[1];");
        reg.registerSignature("descending",
                "native function descending<T>(column:ColSpec<T>[1]):SortInfo<T>[1];");
        reg.registerSignature("asc",
                "native function asc<T>(column:ColSpec<T>[1]):SortInfo<T>[1];");
        reg.registerSignature("desc",
                "native function desc<T>(column:ColSpec<T>[1]):SortInfo<T>[1];");

        // ========== Window specification constructors ==========
        // over() — constructs _Window<T> from partition cols, sort info, and optional frame
        reg.registerSignature("over",
                "native function over<T>(cols:ColSpec<T>[1]):_Window<T>[1];");
        reg.registerSignature("over",
                "native function over<T>(cols:ColSpecArray<T>[1]):_Window<T>[1];");
        reg.registerSignature("over",
                "native function over<T>(cols:ColSpec<T>[1], sortInfo:SortInfo<T>[*]):_Window<T>[1];");
        reg.registerSignature("over",
                "native function over<T>(cols:ColSpecArray<T>[1], sortInfo:SortInfo<T>[*]):_Window<T>[1];");
        reg.registerSignature("over",
                "native function over<T>(cols:ColSpec<T>[1], sortInfo:SortInfo<T>[*], rows:Rows[1]):_Window<T>[1];");
        reg.registerSignature("over",
                "native function over<T>(cols:ColSpec<T>[1], sortInfo:SortInfo<T>[*], range:_Range[1]):_Window<T>[1];");
        reg.registerSignature("over",
                "native function over<T>(sortInfo:SortInfo<T>[*]):_Window<T>[1];");

        // ========== Frame constructors ==========
        // rows() — physical row offsets
        reg.registerSignature("rows",
                "native function rows(offsetFrom:meta::pure::metamodel::type::Integer[1], offsetTo:meta::pure::metamodel::type::Integer[1]):Rows[1];");
        // unbounded() — unbounded frame bound
        reg.registerSignature("unbounded",
                "native function unbounded():UnboundedFrameValue[1];");
        // _range() — logical range offsets
        reg.registerSignature("_range",
                "native function _range(offsetFrom:meta::pure::metamodel::type::Number[1], offsetTo:meta::pure::metamodel::type::Number[1]):_Range[1];");

        // GroupBy
        reg.registerSignature("groupBy",
                "native function groupBy<T,Z,K,V,R>(r:Relation<T>[1], cols:ColSpecArray<Z⊆T>[1], agg:AggColSpec<{T[1]->K[0..1]},{K[*]->V[0..1]},R>[1]):Relation<Z+R>[1];");
        reg.registerSignature("groupBy",
                "native function groupBy<T,Z,K,V,R>(r:Relation<T>[1], cols:ColSpec<Z⊆T>[1], agg:AggColSpec<{T[1]->K[0..1]},{K[*]->V[0..1]},R>[1]):Relation<Z+R>[1];");
        reg.registerSignature("groupBy",
                "native function groupBy<T,Z,K,V,R>(r:Relation<T>[1], cols:ColSpecArray<Z⊆T>[1], agg:AggColSpecArray<{T[1]->K[0..1]},{K[*]->V[0..1]},R>[1]):Relation<Z+R>[1];");
        reg.registerSignature("groupBy",
                "native function groupBy<T,Z,K,V,R>(r:Relation<T>[1], cols:ColSpec<Z⊆T>[1], agg:AggColSpecArray<{T[1]->K[0..1]},{K[*]->V[0..1]},R>[1]):Relation<Z+R>[1];");
        // Class source: groupBy(cl:C[*], keys:FuncColSpecArray, aggs:AggColSpecArray) — like project(C[*], ...)
        reg.registerSignature("groupBy",
                "native function groupBy<C,Z,K,V,R>(cl:C[*], keys:FuncColSpecArray<{C[1]->meta::pure::metamodel::type::Any[*]},Z>[1], aggs:AggColSpecArray<{C[1]->K[0..1]},{K[*]->V[0..1]},R>[1]):Relation<Z+R>[1];");
        reg.registerSignature("groupBy",
                "native function groupBy<C,Z,K,V,R>(cl:C[*], keys:FuncColSpecArray<{C[1]->meta::pure::metamodel::type::Any[*]},Z>[1], aggs:AggColSpec<{C[1]->K[0..1]},{K[*]->V[0..1]},R>[1]):Relation<Z+R>[1];");
        // Legacy TDS: groupBy(set, keyFns, aggValues, ids) — desugared to arity-3 in GroupByChecker
        reg.registerSignature("groupBy",
                "native function groupBy<K,V,U>(set:K[*], fns:Function<{K[1]->meta::pure::metamodel::type::Any[*]}>[*], aggs:meta::pure::metamodel::type::Any[*], ids:meta::pure::metamodel::type::String[*]):Relation<K>[1];");

        // Aggregate
        reg.registerSignature("aggregate",
                "native function aggregate<T,K,V,R>(r:Relation<T>[1], agg:AggColSpec<{T[1]->K[0..1]},{K[*]->V[0..1]},R>[1]):Relation<R>[1];");
        reg.registerSignature("aggregate",
                "native function aggregate<T,K,V,R>(r:Relation<T>[1], agg:AggColSpecArray<{T[1]->K[0..1]},{K[*]->V[0..1]},R>[1]):Relation<R>[1];");

        // Join
        reg.registerSignature("join",
                "native function join<T,V>(rel1:Relation<T>[1], rel2:Relation<V>[1], joinKind:meta::pure::functions::relation::JoinKind[1], f:Function<{T[1],V[1]->meta::pure::metamodel::type::Boolean[1]}>[1]):Relation<T+V>[1];");
        // Join with right-side prefix (syntactic sugar for rename on right source)
        reg.registerSignature("join",
                "native function join<T,V>(rel1:Relation<T>[1], rel2:Relation<V>[1], joinKind:meta::pure::functions::relation::JoinKind[1], f:Function<{T[1],V[1]->meta::pure::metamodel::type::Boolean[1]}>[1], prefix:meta::pure::metamodel::type::String[1]):Relation<T+V>[1];");
        reg.registerSignature("asOfJoin",
                "native function asOfJoin<T,V>(rel1:Relation<T>[1], rel2:Relation<V>[1], match:Function<{T[1],V[1]->meta::pure::metamodel::type::Boolean[1]}>[1]):Relation<T+V>[1];");
        reg.registerSignature("asOfJoin",
                "native function asOfJoin<T,V>(rel1:Relation<T>[1], rel2:Relation<V>[1], match:Function<{T[1],V[1]->meta::pure::metamodel::type::Boolean[1]}>[1], join:Function<{T[1],V[1]->meta::pure::metamodel::type::Boolean[1]}>[1]):Relation<T+V>[1];");
        // asOfJoin with prefix (requires both match + join lambdas)
        reg.registerSignature("asOfJoin",
                "native function asOfJoin<T,V>(rel1:Relation<T>[1], rel2:Relation<V>[1], match:Function<{T[1],V[1]->meta::pure::metamodel::type::Boolean[1]}>[1], join:Function<{T[1],V[1]->meta::pure::metamodel::type::Boolean[1]}>[1], prefix:meta::pure::metamodel::type::String[1]):Relation<T+V>[1];");

        // Pivot
        reg.registerSignature("pivot",
                "native function pivot<T,Z,K,V,R>(r:Relation<T>[1], cols:ColSpec<Z⊆T>[1], agg:AggColSpec<{T[1]->K[0..1]},{K[*]->V[0..1]},R>[1]):Relation<meta::pure::metamodel::type::Any>[1];");
        reg.registerSignature("pivot",
                "native function pivot<T,Z,K,V,R>(r:Relation<T>[1], cols:ColSpecArray<Z⊆T>[1], agg:AggColSpec<{T[1]->K[0..1]},{K[*]->V[0..1]},R>[1]):Relation<meta::pure::metamodel::type::Any>[1];");
        reg.registerSignature("pivot",
                "native function pivot<T,Z,K,V,R>(r:Relation<T>[1], cols:ColSpec<Z⊆T>[1], values:meta::pure::metamodel::type::Any[1..*], agg:AggColSpec<{T[1]->K[0..1]},{K[*]->V[0..1]},R>[1]):Relation<meta::pure::metamodel::type::Any>[1];");
        reg.registerSignature("pivot",
                "native function pivot<T,Z,K,V,R>(r:Relation<T>[1], cols:ColSpecArray<Z⊆T>[1], agg:AggColSpecArray<{T[1]->K[0..1]},{K[*]->V[0..1]},R>[1]):Relation<meta::pure::metamodel::type::Any>[1];");
        reg.registerSignature("pivot",
                "native function pivot<T,Z,K,V,R>(r:Relation<T>[1], cols:ColSpec<Z⊆T>[1], agg:AggColSpecArray<{T[1]->K[0..1]},{K[*]->V[0..1]},R>[1]):Relation<meta::pure::metamodel::type::Any>[1];");

        // Project
        reg.registerSignature("project",
                "native function project<C,T>(cl:C[*], x:FuncColSpecArray<{C[1]->meta::pure::metamodel::type::Any[*]},T>[1]):Relation<T>[1];");
        reg.registerSignature("project",
                "native function project<T,Z>(r:Relation<T>[1], fs:FuncColSpecArray<{T[1]->meta::pure::metamodel::type::Any[*]},Z>[1]):Relation<Z>[1];");
        // Legacy TDS: project(set, functions, ids) — desugared to arity-2 in ProjectChecker
        reg.registerSignature("project",
                "native function project<K>(set:K[*], fns:Function<{K[1]->meta::pure::metamodel::type::Any[*]}>[*], ids:meta::pure::metamodel::type::String[*]):Relation<K>[1];");

        // Flatten
        reg.registerSignature("flatten",
                "native function flatten<T,Z>(valueToFlatten:T[*], columnWithFlattenedValue:ColSpec<Z=(?:T)>[1]):Relation<Z>[1];");

        // Window functions (native)
        reg.registerSignature("first", "native function first<T>(w:Relation<T>[1], f:_Window<T>[1], r:T[1]):T[0..1];");
        reg.registerSignature("last", "native function last<T>(w:Relation<T>[1], f:_Window<T>[1], row:T[1]):T[0..1];");
        reg.registerSignature("nth",
                "native function nth<T>(w:Relation<T>[1], f:_Window<T>[1], r:T[1], offset:meta::pure::metamodel::type::Integer[1]):T[0..1];");
        reg.registerSignature("offset",
                "native function offset<T>(w:Relation<T>[1], r:T[1], offset:meta::pure::metamodel::type::Integer[1]):T[0..1];");
        reg.registerSignature("rowNumber", "native function rowNumber<T>(rel:Relation<T>[1], row:T[1]):meta::pure::metamodel::type::Integer[1];");
        reg.registerSignature("ntile",
                "native function ntile<T>(rel:Relation<T>[1], row:T[1], tileCount:meta::pure::metamodel::type::Integer[1]):meta::pure::metamodel::type::Integer[1];");
        reg.registerSignature("rank",
                "native function rank<T>(rel:Relation<T>[1], w:_Window<T>[1], row:T[1]):meta::pure::metamodel::type::Integer[1];");
        reg.registerSignature("denseRank",
                "native function denseRank<T>(rel:Relation<T>[1], w:_Window<T>[1], row:T[1]):meta::pure::metamodel::type::Integer[1];");
        reg.registerSignature("percentRank",
                "native function percentRank<T>(rel:Relation<T>[1], w:_Window<T>[1], row:T[1]):meta::pure::metamodel::type::Float[1];");
        reg.registerSignature("cumulativeDistribution",
                "native function cumulativeDistribution<T>(rel:Relation<T>[1], w:_Window<T>[1], row:T[1]):meta::pure::metamodel::type::Float[1];");

        // Window functions (delegate to offset)
        reg.registerSignature("lag", "native function lag<T>(w:Relation<T>[1], r:T[1]):T[0..1];");
        reg.registerSignature("lag", "native function lag<T>(w:Relation<T>[1], r:T[1], offset:meta::pure::metamodel::type::Integer[1]):T[0..1];");
        reg.registerSignature("lead", "native function lead<T>(w:Relation<T>[1], r:T[1]):T[0..1];");
        reg.registerSignature("lead", "native function lead<T>(w:Relation<T>[1], r:T[1], offset:meta::pure::metamodel::type::Integer[1]):T[0..1];");
    }

    /**
     * Tier 2: Scalar/aggregate/window functions.
     * Organized by category from DynaFunctionRegistry.
     */
    private static void registerScalarFunctions(BuiltinRegistry reg) {
        // ===== DynaFunction-only (SQL relational operations, not standard Pure functions) =====
        // These are used in relational property mapping syntax: e.g., fullName: concat([DB] T.FIRST, ' ', [DB] T.LAST)
        reg.registerSignature("concat",
                "native function concat(strs:meta::pure::metamodel::type::String[*]):meta::pure::metamodel::type::String[1];");
        reg.registerSignature("sqlNull", "native function sqlNull():meta::pure::metamodel::type::Nil[0];");
        reg.registerSignature("sqlTrue", "native function sqlTrue():meta::pure::metamodel::type::Boolean[1];");
        reg.registerSignature("sqlFalse", "native function sqlFalse():meta::pure::metamodel::type::Boolean[1];");
        reg.registerSignature("isNull", "native function isNull<T>(val:T[1]):meta::pure::metamodel::type::Boolean[1];");
        reg.registerSignature("isNotNull", "native function isNotNull<T>(val:T[1]):meta::pure::metamodel::type::Boolean[1];");
        reg.registerSignature("sub", "native function sub(left:meta::pure::metamodel::type::Number[1], right:meta::pure::metamodel::type::Number[1]):meta::pure::metamodel::type::Number[1];");
        reg.registerSignature("notEqualAnsi",
                "native function notEqualAnsi(left:meta::pure::metamodel::type::Any[1], right:meta::pure::metamodel::type::Any[1]):meta::pure::metamodel::type::Boolean[1];");
        reg.registerSignature("group", "native function group<T>(val:T[1]):T[1];");
        reg.registerSignature("divideRound",
                "native function divideRound(left:meta::pure::metamodel::type::Number[1], right:meta::pure::metamodel::type::Number[1], scale:meta::pure::metamodel::type::Integer[1]):meta::pure::metamodel::type::Number[1];");
        reg.registerSignature("isDistinct",
                "native function isDistinct(left:meta::pure::metamodel::type::Any[1], right:meta::pure::metamodel::type::Any[1]):meta::pure::metamodel::type::Boolean[1];");
        reg.registerSignature("currentUserId", "native function currentUserId():meta::pure::metamodel::type::String[1];");
        reg.registerSignature("md5", "native function md5(str:meta::pure::metamodel::type::String[1]):meta::pure::metamodel::type::String[1];");
        reg.registerSignature("sha1", "native function sha1(str:meta::pure::metamodel::type::String[1]):meta::pure::metamodel::type::String[1];");
        reg.registerSignature("sha256", "native function sha256(str:meta::pure::metamodel::type::String[1]):meta::pure::metamodel::type::String[1];");
        reg.registerSignature("repeatString",
                "native function repeatString(str:meta::pure::metamodel::type::String[1], count:meta::pure::metamodel::type::Integer[1]):meta::pure::metamodel::type::String[1];");
        reg.registerSignature("objectReferenceIn",
                "native function objectReferenceIn(col:meta::pure::metamodel::type::Any[1], values:meta::pure::metamodel::type::Any[*]):meta::pure::metamodel::type::Boolean[1];");
        reg.registerSignature("averageRank",
                "native function averageRank():meta::pure::metamodel::type::Number[1];");
        reg.registerSignature("variantTo",
                "native function variantTo<T>(val:meta::pure::metamodel::type::Any[1], type:T[1]):T[1];");

        // ===== String =====
        reg.registerSignature("toLower", "native function toLower(source:meta::pure::metamodel::type::String[1]):meta::pure::metamodel::type::String[1];");
        reg.registerSignature("toUpper", "native function toUpper(source:meta::pure::metamodel::type::String[1]):meta::pure::metamodel::type::String[1];");
        reg.registerSignature("trim", "native function trim(str:meta::pure::metamodel::type::String[1]):meta::pure::metamodel::type::String[1];");
        reg.registerSignature("ltrim", "native function ltrim(str:meta::pure::metamodel::type::String[1]):meta::pure::metamodel::type::String[1];");
        reg.registerSignature("rtrim", "native function rtrim(str:meta::pure::metamodel::type::String[1]):meta::pure::metamodel::type::String[1];");
        reg.registerSignature("substring",
                "native function substring(str:meta::pure::metamodel::type::String[1], start:meta::pure::metamodel::type::Integer[1], end:meta::pure::metamodel::type::Integer[1]):meta::pure::metamodel::type::String[1];");
        reg.registerSignature("substring",
                "native function substring(str:meta::pure::metamodel::type::String[1], start:meta::pure::metamodel::type::Integer[1]):meta::pure::metamodel::type::String[1];");
        reg.registerSignature("indexOf", "native function indexOf(str:meta::pure::metamodel::type::String[1], toFind:meta::pure::metamodel::type::String[1]):meta::pure::metamodel::type::Integer[1];");
        reg.registerSignature("indexOf",
                "native function indexOf(str:meta::pure::metamodel::type::String[1], toFind:meta::pure::metamodel::type::String[1], fromIndex:meta::pure::metamodel::type::Integer[1]):meta::pure::metamodel::type::Integer[1];");
        reg.registerSignature("startsWith", "native function startsWith(source:meta::pure::metamodel::type::String[1], val:meta::pure::metamodel::type::String[1]):meta::pure::metamodel::type::Boolean[1];");
        reg.registerSignature("endsWith", "native function endsWith(source:meta::pure::metamodel::type::String[1], val:meta::pure::metamodel::type::String[1]):meta::pure::metamodel::type::Boolean[1];");
        reg.registerSignature("contains", "native function contains(source:meta::pure::metamodel::type::String[1], val:meta::pure::metamodel::type::String[1]):meta::pure::metamodel::type::Boolean[1];");
        // Collection contains: [1,2,3]->contains(2) — uses Any, not T (per legend-pure)
        reg.registerSignature("contains",
                "native function contains(collection:meta::pure::metamodel::type::Any[*], val:meta::pure::metamodel::type::Any[1]):meta::pure::metamodel::type::Boolean[1];");
        // Collection contains with comparator: [...]->contains(elem, {a,b | $a == $b})
        reg.registerSignature("contains",
                "native function contains<T>(collection:T[*], val:T[1], comparator:Function<{T[1],T[1]->meta::pure::metamodel::type::Boolean[1]}>[1]):meta::pure::metamodel::type::Boolean[1];");
        reg.registerSignature("reverseString", "native function reverseString(str:meta::pure::metamodel::type::String[1]):meta::pure::metamodel::type::String[1];");
        reg.registerSignature("replace",
                "native function replace(str:meta::pure::metamodel::type::String[1], toFind:meta::pure::metamodel::type::String[1], replacement:meta::pure::metamodel::type::String[1]):meta::pure::metamodel::type::String[1];");
        reg.registerSignature("length", "native function length(str:meta::pure::metamodel::type::String[1]):meta::pure::metamodel::type::Integer[1];");
        reg.registerSignature("toString", "native function toString(any:meta::pure::metamodel::type::Any[1]):meta::pure::metamodel::type::String[1];");
        reg.registerSignature("format", "native function format(format:meta::pure::metamodel::type::String[1], args:meta::pure::metamodel::type::Any[*]):meta::pure::metamodel::type::String[1];");
        reg.registerSignature("joinStrings",
                "native function joinStrings(strings:meta::pure::metamodel::type::String[*], separator:meta::pure::metamodel::type::String[1]):meta::pure::metamodel::type::String[1];");
        // 4-arg form: joinStrings(prefix, separator, suffix)
        reg.registerSignature("joinStrings",
                "native function joinStrings(strings:meta::pure::metamodel::type::String[*], prefix:meta::pure::metamodel::type::String[1], separator:meta::pure::metamodel::type::String[1], suffix:meta::pure::metamodel::type::String[1]):meta::pure::metamodel::type::String[1];");
        reg.registerSignature("split", "native function split(str:meta::pure::metamodel::type::String[1], delimiter:meta::pure::metamodel::type::String[1]):meta::pure::metamodel::type::String[*];");
        reg.registerSignature("matches", "native function matches(str:meta::pure::metamodel::type::String[1], regex:meta::pure::metamodel::type::String[1]):meta::pure::metamodel::type::Boolean[1];");
        reg.registerSignature("ascii", "native function ascii(str:meta::pure::metamodel::type::String[1]):meta::pure::metamodel::type::Integer[1];");
        reg.registerSignature("char", "native function char(code:meta::pure::metamodel::type::Integer[1]):meta::pure::metamodel::type::String[1];");
        reg.registerSignature("lpad", "native function lpad(str:meta::pure::metamodel::type::String[1], len:meta::pure::metamodel::type::Integer[1], pad:meta::pure::metamodel::type::String[1]):meta::pure::metamodel::type::String[1];");
        reg.registerSignature("lpad", "native function lpad(str:meta::pure::metamodel::type::String[1], len:meta::pure::metamodel::type::Integer[1]):meta::pure::metamodel::type::String[1];");
        reg.registerSignature("rpad", "native function rpad(str:meta::pure::metamodel::type::String[1], len:meta::pure::metamodel::type::Integer[1], pad:meta::pure::metamodel::type::String[1]):meta::pure::metamodel::type::String[1];");
        reg.registerSignature("rpad", "native function rpad(str:meta::pure::metamodel::type::String[1], len:meta::pure::metamodel::type::Integer[1]):meta::pure::metamodel::type::String[1];");
        reg.registerSignature("splitPart",
                "native function splitPart(str:meta::pure::metamodel::type::String[0..1], delimiter:meta::pure::metamodel::type::String[1], index:meta::pure::metamodel::type::Integer[1]):meta::pure::metamodel::type::String[0..1];");
        reg.registerSignature("left", "native function left(str:meta::pure::metamodel::type::String[1], len:meta::pure::metamodel::type::Integer[1]):meta::pure::metamodel::type::String[1];");
        reg.registerSignature("right", "native function right(str:meta::pure::metamodel::type::String[1], len:meta::pure::metamodel::type::Integer[1]):meta::pure::metamodel::type::String[1];");
        reg.registerSignature("toUpperFirstCharacter",
                "native function toUpperFirstCharacter(str:meta::pure::metamodel::type::String[1]):meta::pure::metamodel::type::String[1];");
        reg.registerSignature("toLowerFirstCharacter",
                "native function toLowerFirstCharacter(str:meta::pure::metamodel::type::String[1]):meta::pure::metamodel::type::String[1];");
        reg.registerSignature("encodeBase64", "native function encodeBase64(str:meta::pure::metamodel::type::String[1]):meta::pure::metamodel::type::String[1];");
        reg.registerSignature("decodeBase64", "native function decodeBase64(str:meta::pure::metamodel::type::String[1]):meta::pure::metamodel::type::String[1];");
        reg.registerSignature("hash", "native function hash(str:meta::pure::metamodel::type::String[1], algorithm:meta::pure::functions::hash::HashType[1]):meta::pure::metamodel::type::String[1];");
        reg.registerSignature("hash", "native function hash(str:meta::pure::metamodel::type::String[1]):meta::pure::metamodel::type::String[1];");
        reg.registerSignature("levenshteinDistance",
                "native function levenshteinDistance(s1:meta::pure::metamodel::type::String[1], s2:meta::pure::metamodel::type::String[1]):meta::pure::metamodel::type::Integer[1];");
        reg.registerSignature("jaroWinklerSimilarity",
                "native function jaroWinklerSimilarity(s1:meta::pure::metamodel::type::String[1], s2:meta::pure::metamodel::type::String[1]):meta::pure::metamodel::type::Float[1];");
        reg.registerSignature("hashCode", "native function hashCode(val:meta::pure::metamodel::type::Any[*]):meta::pure::metamodel::type::Integer[1];");

        // ===== Math (basic) =====
        reg.registerSignature("abs", "native function abs<T>(number:T[1]):T[1];");
        reg.registerSignature("ceiling", "native function ceiling(number:meta::pure::metamodel::type::Number[1]):meta::pure::metamodel::type::Integer[1];");
        reg.registerSignature("floor", "native function floor(number:meta::pure::metamodel::type::Number[1]):meta::pure::metamodel::type::Integer[1];");
        reg.registerSignature("round", "native function round(number:meta::pure::metamodel::type::Number[1]):meta::pure::metamodel::type::Integer[1];");
        reg.registerSignature("round", "native function round(decimal:meta::pure::metamodel::type::Decimal[1], scale:meta::pure::metamodel::type::Integer[1]):meta::pure::metamodel::type::Decimal[1];");
        reg.registerSignature("round", "native function round(float:meta::pure::metamodel::type::Float[1], scale:meta::pure::metamodel::type::Integer[1]):meta::pure::metamodel::type::Float[1];");
        reg.registerSignature("sqrt", "native function sqrt(number:meta::pure::metamodel::type::Number[1]):meta::pure::metamodel::type::Float[1];");
        reg.registerSignature("cbrt", "native function cbrt(number:meta::pure::metamodel::type::Number[1]):meta::pure::metamodel::type::Float[1];");
        reg.registerSignature("pow", "native function pow(base:meta::pure::metamodel::type::Number[1], exponent:meta::pure::metamodel::type::Number[1]):meta::pure::metamodel::type::Number[1];");
        reg.registerSignature("exp", "native function exp(exponent:meta::pure::metamodel::type::Number[1]):meta::pure::metamodel::type::Float[1];");
        reg.registerSignature("log", "native function log(value:meta::pure::metamodel::type::Number[1]):meta::pure::metamodel::type::Float[1];");
        reg.registerSignature("log10", "native function log10(value:meta::pure::metamodel::type::Number[1]):meta::pure::metamodel::type::Float[1];");
        reg.registerSignature("sign", "native function sign(number:meta::pure::metamodel::type::Number[1]):meta::pure::metamodel::type::Integer[1];");
        reg.registerSignature("mod", "native function mod(dividend:meta::pure::metamodel::type::Integer[1], divisor:meta::pure::metamodel::type::Integer[1]):meta::pure::metamodel::type::Integer[1];");
        reg.registerSignature("rem", "native function rem(dividend:meta::pure::metamodel::type::Number[1], divisor:meta::pure::metamodel::type::Number[1]):meta::pure::metamodel::type::Number[1];");

        // ===== Trigonometry =====
        reg.registerSignature("sin", "native function sin(number:meta::pure::metamodel::type::Number[1]):meta::pure::metamodel::type::Float[1];");
        reg.registerSignature("cos", "native function cos(number:meta::pure::metamodel::type::Number[1]):meta::pure::metamodel::type::Float[1];");
        reg.registerSignature("tan", "native function tan(number:meta::pure::metamodel::type::Number[1]):meta::pure::metamodel::type::Float[1];");
        reg.registerSignature("asin", "native function asin(number:meta::pure::metamodel::type::Number[1]):meta::pure::metamodel::type::Float[1];");
        reg.registerSignature("acos", "native function acos(number:meta::pure::metamodel::type::Number[1]):meta::pure::metamodel::type::Float[1];");
        reg.registerSignature("atan", "native function atan(number:meta::pure::metamodel::type::Number[1]):meta::pure::metamodel::type::Float[1];");
        reg.registerSignature("atan2", "native function atan2(y:meta::pure::metamodel::type::Number[1], x:meta::pure::metamodel::type::Number[1]):meta::pure::metamodel::type::Float[1];");
        reg.registerSignature("sinh", "native function sinh(number:meta::pure::metamodel::type::Number[1]):meta::pure::metamodel::type::Float[1];");
        reg.registerSignature("cosh", "native function cosh(number:meta::pure::metamodel::type::Number[1]):meta::pure::metamodel::type::Float[1];");
        reg.registerSignature("tanh", "native function tanh(number:meta::pure::metamodel::type::Number[1]):meta::pure::metamodel::type::Float[1];");
        reg.registerSignature("cot", "native function cot(number:meta::pure::metamodel::type::Number[1]):meta::pure::metamodel::type::Float[1];");
        reg.registerSignature("toDegrees", "native function toDegrees(radians:meta::pure::metamodel::type::Number[1]):meta::pure::metamodel::type::Float[1];");
        reg.registerSignature("toRadians", "native function toRadians(degrees:meta::pure::metamodel::type::Number[1]):meta::pure::metamodel::type::Float[1];");
        reg.registerSignature("pi", "native function pi():meta::pure::metamodel::type::Float[1];");

        // ===== Arithmetic =====
        // legend-pure: fold form (values:T[*]):T[1] — parser wraps $a+$b → plus([$a,$b])
        reg.registerSignature("plus", "native function plus<T>(values:T[*]):T[1];");
        reg.registerSignature("minus", "native function minus<T>(values:T[*]):T[1];");
        reg.registerSignature("times", "native function times<T>(values:T[*]):T[1];");
        // legend-lite extension: concrete binary forms (our parser emits plus(a,b) not plus([a,b]))
        // Concrete overloads carry the correct return type (Integer+Integer→Integer).
        // With compile-then-resolve, the resolver scores ALL params:
        // - Homogeneous (Integer+Integer) → picks Integer×Integer (score 4 > Number×Number score 2)
        // - Mixed (Integer+Float) → Integer×Integer fails param[1], Number×Number wins
        reg.registerSignature("plus", "native function plus(left:meta::pure::metamodel::type::Integer[1], right:meta::pure::metamodel::type::Integer[1]):meta::pure::metamodel::type::Integer[1];");
        reg.registerSignature("plus", "native function plus(left:meta::pure::metamodel::type::Float[1], right:meta::pure::metamodel::type::Float[1]):meta::pure::metamodel::type::Float[1];");
        reg.registerSignature("plus", "native function plus(left:meta::pure::metamodel::type::Decimal[1], right:meta::pure::metamodel::type::Decimal[1]):meta::pure::metamodel::type::Decimal[1];");
        reg.registerSignature("plus", "native function plus(left:meta::pure::metamodel::type::Number[1], right:meta::pure::metamodel::type::Number[1]):meta::pure::metamodel::type::Number[1];");
        reg.registerSignature("plus", "native function plus(left:meta::pure::metamodel::type::String[1], right:meta::pure::metamodel::type::String[1]):meta::pure::metamodel::type::String[1];");
        reg.registerSignature("minus", "native function minus(left:meta::pure::metamodel::type::Integer[1], right:meta::pure::metamodel::type::Integer[1]):meta::pure::metamodel::type::Integer[1];");
        reg.registerSignature("minus", "native function minus(left:meta::pure::metamodel::type::Float[1], right:meta::pure::metamodel::type::Float[1]):meta::pure::metamodel::type::Float[1];");
        reg.registerSignature("minus", "native function minus(left:meta::pure::metamodel::type::Decimal[1], right:meta::pure::metamodel::type::Decimal[1]):meta::pure::metamodel::type::Decimal[1];");
        reg.registerSignature("minus", "native function minus(left:meta::pure::metamodel::type::Number[1], right:meta::pure::metamodel::type::Number[1]):meta::pure::metamodel::type::Number[1];");
        reg.registerSignature("times", "native function times(left:meta::pure::metamodel::type::Integer[1], right:meta::pure::metamodel::type::Integer[1]):meta::pure::metamodel::type::Integer[1];");
        reg.registerSignature("times", "native function times(left:meta::pure::metamodel::type::Float[1], right:meta::pure::metamodel::type::Float[1]):meta::pure::metamodel::type::Float[1];");
        reg.registerSignature("times", "native function times(left:meta::pure::metamodel::type::Decimal[1], right:meta::pure::metamodel::type::Decimal[1]):meta::pure::metamodel::type::Decimal[1];");
        reg.registerSignature("times", "native function times(left:meta::pure::metamodel::type::Number[1], right:meta::pure::metamodel::type::Number[1]):meta::pure::metamodel::type::Number[1];");
        reg.registerSignature("divide", "native function divide(dividend:meta::pure::metamodel::type::Number[1], divisor:meta::pure::metamodel::type::Number[1]):meta::pure::metamodel::type::Float[1];");
        // 3-arg: divide with explicit scale
        reg.registerSignature("divide",
                "native function divide(dividend:meta::pure::metamodel::type::Number[1], divisor:meta::pure::metamodel::type::Number[1], scale:meta::pure::metamodel::type::Integer[1]):meta::pure::metamodel::type::Decimal[1];");

        // ===== Comparison =====
        reg.registerSignature("equal", "native function equal(left:meta::pure::metamodel::type::Any[1], right:meta::pure::metamodel::type::Any[1]):meta::pure::metamodel::type::Boolean[1];");
        reg.registerSignature("notEqual", "native function notEqual(left:meta::pure::metamodel::type::Any[1], right:meta::pure::metamodel::type::Any[1]):meta::pure::metamodel::type::Boolean[1];");
        reg.registerSignature("eq", "native function eq(left:meta::pure::metamodel::type::Any[1], right:meta::pure::metamodel::type::Any[1]):meta::pure::metamodel::type::Boolean[1];");
        reg.registerSignature("greaterThan",
                "native function greaterThan(left:meta::pure::metamodel::type::Number[1], right:meta::pure::metamodel::type::Number[1]):meta::pure::metamodel::type::Boolean[1];");
        reg.registerSignature("greaterThan",
                "native function greaterThan(left:meta::pure::metamodel::type::Date[1], right:meta::pure::metamodel::type::Date[1]):meta::pure::metamodel::type::Boolean[1];");
        reg.registerSignature("greaterThan",
                "native function greaterThan(left:meta::pure::metamodel::type::String[1], right:meta::pure::metamodel::type::String[1]):meta::pure::metamodel::type::Boolean[1];");
        // Nullable overloads for greaterThan (Pure has [0..1] variants for all types)
        reg.registerSignature("greaterThan", "native function greaterThan(left:meta::pure::metamodel::type::Number[0..1], right:meta::pure::metamodel::type::Number[0..1]):meta::pure::metamodel::type::Boolean[1];");
        reg.registerSignature("greaterThan", "native function greaterThan(left:meta::pure::metamodel::type::Number[1], right:meta::pure::metamodel::type::Number[0..1]):meta::pure::metamodel::type::Boolean[1];");
        reg.registerSignature("greaterThan", "native function greaterThan(left:meta::pure::metamodel::type::Number[0..1], right:meta::pure::metamodel::type::Number[1]):meta::pure::metamodel::type::Boolean[1];");
        reg.registerSignature("greaterThan", "native function greaterThan(left:meta::pure::metamodel::type::Date[0..1], right:meta::pure::metamodel::type::Date[0..1]):meta::pure::metamodel::type::Boolean[1];");
        reg.registerSignature("greaterThan", "native function greaterThan(left:meta::pure::metamodel::type::Date[1], right:meta::pure::metamodel::type::Date[0..1]):meta::pure::metamodel::type::Boolean[1];");
        reg.registerSignature("greaterThan", "native function greaterThan(left:meta::pure::metamodel::type::Date[0..1], right:meta::pure::metamodel::type::Date[1]):meta::pure::metamodel::type::Boolean[1];");
        reg.registerSignature("greaterThan", "native function greaterThan(left:meta::pure::metamodel::type::String[0..1], right:meta::pure::metamodel::type::String[0..1]):meta::pure::metamodel::type::Boolean[1];");
        reg.registerSignature("greaterThan", "native function greaterThan(left:meta::pure::metamodel::type::String[1], right:meta::pure::metamodel::type::String[0..1]):meta::pure::metamodel::type::Boolean[1];");
        reg.registerSignature("greaterThan", "native function greaterThan(left:meta::pure::metamodel::type::String[0..1], right:meta::pure::metamodel::type::String[1]):meta::pure::metamodel::type::Boolean[1];");
        reg.registerSignature("lessThan", "native function lessThan(left:meta::pure::metamodel::type::Number[1], right:meta::pure::metamodel::type::Number[1]):meta::pure::metamodel::type::Boolean[1];");
        reg.registerSignature("lessThan", "native function lessThan(left:meta::pure::metamodel::type::Date[1], right:meta::pure::metamodel::type::Date[1]):meta::pure::metamodel::type::Boolean[1];");
        reg.registerSignature("lessThan",
                "native function lessThan(left:meta::pure::metamodel::type::String[1], right:meta::pure::metamodel::type::String[1]):meta::pure::metamodel::type::Boolean[1];");
        // Nullable overloads for lessThan
        reg.registerSignature("lessThan", "native function lessThan(left:meta::pure::metamodel::type::Number[0..1], right:meta::pure::metamodel::type::Number[0..1]):meta::pure::metamodel::type::Boolean[1];");
        reg.registerSignature("lessThan", "native function lessThan(left:meta::pure::metamodel::type::Number[1], right:meta::pure::metamodel::type::Number[0..1]):meta::pure::metamodel::type::Boolean[1];");
        reg.registerSignature("lessThan", "native function lessThan(left:meta::pure::metamodel::type::Number[0..1], right:meta::pure::metamodel::type::Number[1]):meta::pure::metamodel::type::Boolean[1];");
        reg.registerSignature("lessThan", "native function lessThan(left:meta::pure::metamodel::type::Date[0..1], right:meta::pure::metamodel::type::Date[0..1]):meta::pure::metamodel::type::Boolean[1];");
        reg.registerSignature("lessThan", "native function lessThan(left:meta::pure::metamodel::type::Date[1], right:meta::pure::metamodel::type::Date[0..1]):meta::pure::metamodel::type::Boolean[1];");
        reg.registerSignature("lessThan", "native function lessThan(left:meta::pure::metamodel::type::Date[0..1], right:meta::pure::metamodel::type::Date[1]):meta::pure::metamodel::type::Boolean[1];");
        reg.registerSignature("lessThan", "native function lessThan(left:meta::pure::metamodel::type::String[0..1], right:meta::pure::metamodel::type::String[0..1]):meta::pure::metamodel::type::Boolean[1];");
        reg.registerSignature("lessThan", "native function lessThan(left:meta::pure::metamodel::type::String[1], right:meta::pure::metamodel::type::String[0..1]):meta::pure::metamodel::type::Boolean[1];");
        reg.registerSignature("lessThan", "native function lessThan(left:meta::pure::metamodel::type::String[0..1], right:meta::pure::metamodel::type::String[1]):meta::pure::metamodel::type::Boolean[1];");
        reg.registerSignature("greaterThanEqual",
                "native function greaterThanEqual(left:meta::pure::metamodel::type::Number[1], right:meta::pure::metamodel::type::Number[1]):meta::pure::metamodel::type::Boolean[1];");
        reg.registerSignature("greaterThanEqual",
                "native function greaterThanEqual(left:meta::pure::metamodel::type::Date[1], right:meta::pure::metamodel::type::Date[1]):meta::pure::metamodel::type::Boolean[1];");
        reg.registerSignature("greaterThanEqual",
                "native function greaterThanEqual(left:meta::pure::metamodel::type::String[1], right:meta::pure::metamodel::type::String[1]):meta::pure::metamodel::type::Boolean[1];");
        // Nullable overloads for greaterThanEqual
        reg.registerSignature("greaterThanEqual", "native function greaterThanEqual(left:meta::pure::metamodel::type::Number[0..1], right:meta::pure::metamodel::type::Number[0..1]):meta::pure::metamodel::type::Boolean[1];");
        reg.registerSignature("greaterThanEqual", "native function greaterThanEqual(left:meta::pure::metamodel::type::Number[1], right:meta::pure::metamodel::type::Number[0..1]):meta::pure::metamodel::type::Boolean[1];");
        reg.registerSignature("greaterThanEqual", "native function greaterThanEqual(left:meta::pure::metamodel::type::Number[0..1], right:meta::pure::metamodel::type::Number[1]):meta::pure::metamodel::type::Boolean[1];");
        reg.registerSignature("greaterThanEqual", "native function greaterThanEqual(left:meta::pure::metamodel::type::Date[0..1], right:meta::pure::metamodel::type::Date[0..1]):meta::pure::metamodel::type::Boolean[1];");
        reg.registerSignature("greaterThanEqual", "native function greaterThanEqual(left:meta::pure::metamodel::type::Date[1], right:meta::pure::metamodel::type::Date[0..1]):meta::pure::metamodel::type::Boolean[1];");
        reg.registerSignature("greaterThanEqual", "native function greaterThanEqual(left:meta::pure::metamodel::type::Date[0..1], right:meta::pure::metamodel::type::Date[1]):meta::pure::metamodel::type::Boolean[1];");
        reg.registerSignature("greaterThanEqual", "native function greaterThanEqual(left:meta::pure::metamodel::type::String[0..1], right:meta::pure::metamodel::type::String[0..1]):meta::pure::metamodel::type::Boolean[1];");
        reg.registerSignature("greaterThanEqual", "native function greaterThanEqual(left:meta::pure::metamodel::type::String[1], right:meta::pure::metamodel::type::String[0..1]):meta::pure::metamodel::type::Boolean[1];");
        reg.registerSignature("greaterThanEqual", "native function greaterThanEqual(left:meta::pure::metamodel::type::String[0..1], right:meta::pure::metamodel::type::String[1]):meta::pure::metamodel::type::Boolean[1];");
        reg.registerSignature("lessThanEqual",
                "native function lessThanEqual(left:meta::pure::metamodel::type::Number[1], right:meta::pure::metamodel::type::Number[1]):meta::pure::metamodel::type::Boolean[1];");
        reg.registerSignature("lessThanEqual",
                "native function lessThanEqual(left:meta::pure::metamodel::type::Date[1], right:meta::pure::metamodel::type::Date[1]):meta::pure::metamodel::type::Boolean[1];");
        reg.registerSignature("lessThanEqual",
                "native function lessThanEqual(left:meta::pure::metamodel::type::String[1], right:meta::pure::metamodel::type::String[1]):meta::pure::metamodel::type::Boolean[1];");
        // Nullable overloads for lessThanEqual
        reg.registerSignature("lessThanEqual", "native function lessThanEqual(left:meta::pure::metamodel::type::Number[0..1], right:meta::pure::metamodel::type::Number[0..1]):meta::pure::metamodel::type::Boolean[1];");
        reg.registerSignature("lessThanEqual", "native function lessThanEqual(left:meta::pure::metamodel::type::Number[1], right:meta::pure::metamodel::type::Number[0..1]):meta::pure::metamodel::type::Boolean[1];");
        reg.registerSignature("lessThanEqual", "native function lessThanEqual(left:meta::pure::metamodel::type::Number[0..1], right:meta::pure::metamodel::type::Number[1]):meta::pure::metamodel::type::Boolean[1];");
        reg.registerSignature("lessThanEqual", "native function lessThanEqual(left:meta::pure::metamodel::type::Date[0..1], right:meta::pure::metamodel::type::Date[0..1]):meta::pure::metamodel::type::Boolean[1];");
        reg.registerSignature("lessThanEqual", "native function lessThanEqual(left:meta::pure::metamodel::type::Date[1], right:meta::pure::metamodel::type::Date[0..1]):meta::pure::metamodel::type::Boolean[1];");
        reg.registerSignature("lessThanEqual", "native function lessThanEqual(left:meta::pure::metamodel::type::Date[0..1], right:meta::pure::metamodel::type::Date[1]):meta::pure::metamodel::type::Boolean[1];");
        reg.registerSignature("lessThanEqual", "native function lessThanEqual(left:meta::pure::metamodel::type::String[0..1], right:meta::pure::metamodel::type::String[0..1]):meta::pure::metamodel::type::Boolean[1];");
        reg.registerSignature("lessThanEqual", "native function lessThanEqual(left:meta::pure::metamodel::type::String[1], right:meta::pure::metamodel::type::String[0..1]):meta::pure::metamodel::type::Boolean[1];");
        reg.registerSignature("lessThanEqual", "native function lessThanEqual(left:meta::pure::metamodel::type::String[0..1], right:meta::pure::metamodel::type::String[1]):meta::pure::metamodel::type::Boolean[1];");
        reg.registerSignature("between",
                "native function between(value:meta::pure::metamodel::type::Number[1], low:meta::pure::metamodel::type::Number[1], high:meta::pure::metamodel::type::Number[1]):meta::pure::metamodel::type::Boolean[1];");
        reg.registerSignature("compare", "native function compare(left:meta::pure::metamodel::type::Any[1], right:meta::pure::metamodel::type::Any[1]):meta::pure::metamodel::type::Integer[1];");
        reg.registerSignature("greatest", "native function greatest(values:meta::pure::metamodel::type::Any[*]):meta::pure::metamodel::type::Any[0..1];");
        reg.registerSignature("least", "native function least(values:meta::pure::metamodel::type::Any[*]):meta::pure::metamodel::type::Any[0..1];");
        reg.registerSignature("coalesce", "native function coalesce(values:meta::pure::metamodel::type::Any[*]):meta::pure::metamodel::type::Any[0..1];");
        reg.registerSignature("coalesce", "native function coalesce(value:meta::pure::metamodel::type::Any[0..1], defaultValue:meta::pure::metamodel::type::Any[1]):meta::pure::metamodel::type::Any[1];");
        // 3-arg: coalesce(value, value2, defaultValue)
        reg.registerSignature("coalesce",
                "native function coalesce(value:meta::pure::metamodel::type::Any[0..1], value2:meta::pure::metamodel::type::Any[0..1], defaultValue:meta::pure::metamodel::type::Any[1]):meta::pure::metamodel::type::Any[1];");
        reg.registerSignature("in", "native function in(value:meta::pure::metamodel::type::Any[1], collection:meta::pure::metamodel::type::Any[*]):meta::pure::metamodel::type::Boolean[1];");

        // ===== Boolean =====
        reg.registerSignature("and", "native function and(left:meta::pure::metamodel::type::Boolean[1], right:meta::pure::metamodel::type::Boolean[1]):meta::pure::metamodel::type::Boolean[1];");
        reg.registerSignature("and", "native function and(bools:meta::pure::metamodel::type::Boolean[*]):meta::pure::metamodel::type::Boolean[1];");
        reg.registerSignature("or", "native function or(left:meta::pure::metamodel::type::Boolean[1], right:meta::pure::metamodel::type::Boolean[1]):meta::pure::metamodel::type::Boolean[1];");
        // Fold form: or(values:Boolean[*]) — parser wraps $a || $b → or([$a,$b])
        reg.registerSignature("or", "native function or(values:meta::pure::metamodel::type::Boolean[*]):meta::pure::metamodel::type::Boolean[1];");
        reg.registerSignature("not", "native function not(value:meta::pure::metamodel::type::Boolean[1]):meta::pure::metamodel::type::Boolean[1];");
        reg.registerSignature("xor", "native function xor(left:meta::pure::metamodel::type::Boolean[1], right:meta::pure::metamodel::type::Boolean[1]):meta::pure::metamodel::type::Boolean[1];");
        reg.registerSignature("isEmpty", "native function isEmpty<T>(value:T[*]):meta::pure::metamodel::type::Boolean[1];");
        reg.registerSignature("isNotEmpty", "native function isNotEmpty<T>(value:T[*]):meta::pure::metamodel::type::Boolean[1];");

        // ===== Bitwise =====
        reg.registerSignature("bitAnd", "native function bitAnd(left:meta::pure::metamodel::type::Integer[1], right:meta::pure::metamodel::type::Integer[1]):meta::pure::metamodel::type::Integer[1];");
        reg.registerSignature("bitOr", "native function bitOr(left:meta::pure::metamodel::type::Integer[1], right:meta::pure::metamodel::type::Integer[1]):meta::pure::metamodel::type::Integer[1];");
        reg.registerSignature("bitXor", "native function bitXor(left:meta::pure::metamodel::type::Integer[1], right:meta::pure::metamodel::type::Integer[1]):meta::pure::metamodel::type::Integer[1];");
        reg.registerSignature("bitShiftLeft",
                "native function bitShiftLeft(value:meta::pure::metamodel::type::Integer[1], bits:meta::pure::metamodel::type::Integer[1]):meta::pure::metamodel::type::Integer[1];");
        reg.registerSignature("bitShiftRight",
                "native function bitShiftRight(value:meta::pure::metamodel::type::Integer[1], bits:meta::pure::metamodel::type::Integer[1]):meta::pure::metamodel::type::Integer[1];");

        // ===== Date/Time =====
        reg.registerSignature("dateDiff",
                "native function dateDiff(d1:meta::pure::metamodel::type::Date[1], d2:meta::pure::metamodel::type::Date[1], du:meta::pure::functions::date::DurationUnit[1]):meta::pure::metamodel::type::Integer[1];");
        reg.registerSignature("datePart", "native function datePart(d:meta::pure::metamodel::type::Date[1]):meta::pure::metamodel::type::StrictDate[1];");
        // date(year) → Date, date(year,month) → Date, date(y,m,d) → StrictDate, etc.
        reg.registerSignature("date", "native function date(year:meta::pure::metamodel::type::Integer[1]):meta::pure::metamodel::type::Date[1];");
        reg.registerSignature("date",
                "native function date(year:meta::pure::metamodel::type::Integer[1], month:meta::pure::metamodel::type::Integer[1]):meta::pure::metamodel::type::Date[1];");
        reg.registerSignature("date",
                "native function date(year:meta::pure::metamodel::type::Integer[1], month:meta::pure::metamodel::type::Integer[1], day:meta::pure::metamodel::type::Integer[1]):meta::pure::metamodel::type::StrictDate[1];");
        // 4-arg: date(year, month, day, hour)
        reg.registerSignature("date",
                "native function date(year:meta::pure::metamodel::type::Integer[1], month:meta::pure::metamodel::type::Integer[1], day:meta::pure::metamodel::type::Integer[1], hour:meta::pure::metamodel::type::Integer[1]):meta::pure::metamodel::type::DateTime[1];");
        // 5-arg: date(year, month, day, hour, minute)
        reg.registerSignature("date",
                "native function date(year:meta::pure::metamodel::type::Integer[1], month:meta::pure::metamodel::type::Integer[1], day:meta::pure::metamodel::type::Integer[1], hour:meta::pure::metamodel::type::Integer[1], minute:meta::pure::metamodel::type::Integer[1]):meta::pure::metamodel::type::DateTime[1];");
        // 6-arg: date(year, month, day, hour, minute, second)
        reg.registerSignature("date",
                "native function date(year:meta::pure::metamodel::type::Integer[1], month:meta::pure::metamodel::type::Integer[1], day:meta::pure::metamodel::type::Integer[1], hour:meta::pure::metamodel::type::Integer[1], minute:meta::pure::metamodel::type::Integer[1], second:meta::pure::metamodel::type::Number[1]):meta::pure::metamodel::type::DateTime[1];");
        reg.registerSignature("adjust",
                "native function adjust(d:meta::pure::metamodel::type::Date[1], amount:meta::pure::metamodel::type::Integer[1], unit:meta::pure::functions::date::DurationUnit[1]):meta::pure::metamodel::type::Date[1];");
        reg.registerSignature("timeBucket",
                "native function timeBucket(d:meta::pure::metamodel::type::Date[1], amount:meta::pure::metamodel::type::Integer[1], unit:meta::pure::functions::date::DurationUnit[1]):meta::pure::metamodel::type::Date[1];");
        reg.registerSignature("year", "native function year(d:meta::pure::metamodel::type::Date[1]):meta::pure::metamodel::type::Integer[1];");
        reg.registerSignature("monthNumber", "native function monthNumber(d:meta::pure::metamodel::type::Date[1]):meta::pure::metamodel::type::Integer[1];");
        reg.registerSignature("dayOfMonth", "native function dayOfMonth(d:meta::pure::metamodel::type::Date[1]):meta::pure::metamodel::type::Integer[1];");
        reg.registerSignature("hour", "native function hour(d:meta::pure::metamodel::type::Date[1]):meta::pure::metamodel::type::Integer[1];");
        reg.registerSignature("minute", "native function minute(d:meta::pure::metamodel::type::Date[1]):meta::pure::metamodel::type::Integer[1];");
        reg.registerSignature("second", "native function second(d:meta::pure::metamodel::type::Date[1]):meta::pure::metamodel::type::Integer[1];");
        reg.registerSignature("now", "native function now():meta::pure::metamodel::type::DateTime[1];");
        reg.registerSignature("today", "native function today():meta::pure::metamodel::type::StrictDate[1];");
        reg.registerSignature("hasHour", "native function hasHour(d:meta::pure::metamodel::type::Date[1]):meta::pure::metamodel::type::Boolean[1];");
        reg.registerSignature("hasMinute", "native function hasMinute(d:meta::pure::metamodel::type::Date[1]):meta::pure::metamodel::type::Boolean[1];");
        reg.registerSignature("hasMonth", "native function hasMonth(d:meta::pure::metamodel::type::Date[1]):meta::pure::metamodel::type::Boolean[1];");
        reg.registerSignature("hasDay", "native function hasDay(d:meta::pure::metamodel::type::Date[1]):meta::pure::metamodel::type::Boolean[1];");
        reg.registerSignature("hasSecond", "native function hasSecond(d:meta::pure::metamodel::type::Date[1]):meta::pure::metamodel::type::Boolean[1];");
        reg.registerSignature("hasSubsecond", "native function hasSubsecond(d:meta::pure::metamodel::type::Date[1]):meta::pure::metamodel::type::Boolean[1];");
        reg.registerSignature("hasSubsecondWithAtLeastPrecision",
                "native function hasSubsecondWithAtLeastPrecision(d:meta::pure::metamodel::type::Date[1], precision:meta::pure::metamodel::type::Integer[1]):meta::pure::metamodel::type::Boolean[1];");
        // Date extraction functions
        reg.registerSignature("month", "native function month(d:meta::pure::metamodel::type::Date[1]):meta::pure::functions::date::Month[1];");
        reg.registerSignature("quarter", "native function quarter(d:meta::pure::metamodel::type::Date[1]):meta::pure::functions::date::Quarter[1];");
        reg.registerSignature("quarterNumber", "native function quarterNumber(d:meta::pure::metamodel::type::Date[1]):meta::pure::metamodel::type::Integer[1];");
        reg.registerSignature("weekOfYear", "native function weekOfYear(d:meta::pure::metamodel::type::Date[1]):meta::pure::metamodel::type::Integer[1];");
        reg.registerSignature("dayOfWeek", "native function dayOfWeek(d:meta::pure::metamodel::type::Date[1]):meta::pure::functions::date::DayOfWeek[1];");
        reg.registerSignature("dayOfWeekNumber", "native function dayOfWeekNumber(d:meta::pure::metamodel::type::Date[1]):meta::pure::metamodel::type::Integer[1];");
        reg.registerSignature("dayOfYear", "native function dayOfYear(d:meta::pure::metamodel::type::Date[1]):meta::pure::metamodel::type::Integer[1];");
        // Date navigation
        reg.registerSignature("firstDayOfMonth", "native function firstDayOfMonth(d:meta::pure::metamodel::type::Date[1]):meta::pure::metamodel::type::Date[1];");
        reg.registerSignature("firstDayOfYear", "native function firstDayOfYear(d:meta::pure::metamodel::type::Date[1]):meta::pure::metamodel::type::Date[1];");
        reg.registerSignature("firstDayOfQuarter", "native function firstDayOfQuarter(d:meta::pure::metamodel::type::Date[1]):meta::pure::metamodel::type::StrictDate[1];");
        reg.registerSignature("firstHourOfDay", "native function firstHourOfDay(d:meta::pure::metamodel::type::Date[1]):meta::pure::metamodel::type::DateTime[1];");
        reg.registerSignature("firstMinuteOfHour", "native function firstMinuteOfHour(d:meta::pure::metamodel::type::Date[1]):meta::pure::metamodel::type::DateTime[1];");
        reg.registerSignature("firstSecondOfMinute", "native function firstSecondOfMinute(d:meta::pure::metamodel::type::Date[1]):meta::pure::metamodel::type::DateTime[1];");
        reg.registerSignature("firstMillisecondOfSecond", "native function firstMillisecondOfSecond(d:meta::pure::metamodel::type::Date[1]):meta::pure::metamodel::type::DateTime[1];");
        // Epoch conversion
        reg.registerSignature("toEpochValue", "native function toEpochValue(d:meta::pure::metamodel::type::Date[1], unit:meta::pure::functions::date::DurationUnit[1]):meta::pure::metamodel::type::Integer[1];");
        // 1-arg: defaults to seconds
        reg.registerSignature("toEpochValue", "native function toEpochValue(d:meta::pure::metamodel::type::Date[1]):meta::pure::metamodel::type::Integer[1];");
        reg.registerSignature("fromEpochValue", "native function fromEpochValue(epoch:meta::pure::metamodel::type::Integer[1], unit:meta::pure::functions::date::DurationUnit[1]):meta::pure::metamodel::type::Date[1];");
        // 1-arg: defaults to seconds
        reg.registerSignature("fromEpochValue", "native function fromEpochValue(epoch:meta::pure::metamodel::type::Integer[1]):meta::pure::metamodel::type::Date[1];");
        // Date comparison predicates
        reg.registerSignature("isAfterDay", "native function isAfterDay(d1:meta::pure::metamodel::type::Date[1], d2:meta::pure::metamodel::type::Date[1]):meta::pure::metamodel::type::Boolean[1];");
        reg.registerSignature("isBeforeDay", "native function isBeforeDay(d1:meta::pure::metamodel::type::Date[1], d2:meta::pure::metamodel::type::Date[1]):meta::pure::metamodel::type::Boolean[1];");
        reg.registerSignature("isOnDay", "native function isOnDay(d1:meta::pure::metamodel::type::Date[1], d2:meta::pure::metamodel::type::Date[1]):meta::pure::metamodel::type::Boolean[1];");
        reg.registerSignature("isOnOrAfterDay", "native function isOnOrAfterDay(d1:meta::pure::metamodel::type::Date[1], d2:meta::pure::metamodel::type::Date[1]):meta::pure::metamodel::type::Boolean[1];");
        reg.registerSignature("isOnOrBeforeDay", "native function isOnOrBeforeDay(d1:meta::pure::metamodel::type::Date[1], d2:meta::pure::metamodel::type::Date[1]):meta::pure::metamodel::type::Boolean[1];");
        // Min/max dates
        reg.registerSignature("minDate", "native function minDate(d1:meta::pure::metamodel::type::Date[1], d2:meta::pure::metamodel::type::Date[1]):meta::pure::metamodel::type::Date[1];");
        reg.registerSignature("maxDate", "native function maxDate(d1:meta::pure::metamodel::type::Date[1], d2:meta::pure::metamodel::type::Date[1]):meta::pure::metamodel::type::Date[1];");

        // ===== Conversion =====
        reg.registerSignature("parseInteger", "native function parseInteger(string:meta::pure::metamodel::type::String[1]):meta::pure::metamodel::type::Integer[1];");
        reg.registerSignature("parseFloat", "native function parseFloat(string:meta::pure::metamodel::type::String[1]):meta::pure::metamodel::type::Float[1];");
        reg.registerSignature("parseDecimal", "native function parseDecimal(string:meta::pure::metamodel::type::String[1]):meta::pure::metamodel::type::Decimal[1];");
        reg.registerSignature("parseDate", "native function parseDate(string:meta::pure::metamodel::type::String[1]):meta::pure::metamodel::type::Date[1];");
        reg.registerSignature("parseBoolean", "native function parseBoolean(string:meta::pure::metamodel::type::String[1]):meta::pure::metamodel::type::Boolean[1];");
        reg.registerSignature("toDecimal", "native function toDecimal(number:meta::pure::metamodel::type::Number[1]):meta::pure::metamodel::type::Decimal[1];");
        reg.registerSignature("toFloat", "native function toFloat(number:meta::pure::metamodel::type::Number[1]):meta::pure::metamodel::type::Float[1];");

        // ===== Aggregation =====
        reg.registerSignature("count", "native function count<T>(values:T[*]):meta::pure::metamodel::type::Integer[1];");
        reg.registerSignature("sum", "native function sum(numbers:meta::pure::metamodel::type::Number[*]):meta::pure::metamodel::type::Number[1];");
        reg.registerSignature("average", "native function average(numbers:meta::pure::metamodel::type::Number[*]):meta::pure::metamodel::type::Float[1];");
        reg.registerSignature("avg", "native function avg(numbers:meta::pure::metamodel::type::Number[*]):meta::pure::metamodel::type::Float[1];");
        reg.registerSignature("mean", "native function mean(numbers:meta::pure::metamodel::type::Number[*]):meta::pure::metamodel::type::Float[1];");
        reg.registerSignature("median", "native function median(numbers:meta::pure::metamodel::type::Number[*]):meta::pure::metamodel::type::Number[1];");
        reg.registerSignature("mode", "native function mode(values:meta::pure::metamodel::type::Any[*]):meta::pure::metamodel::type::Any[0..1];");
        reg.registerSignature("min", "native function min(numbers:meta::pure::metamodel::type::Number[*]):meta::pure::metamodel::type::Number[0..1];");
        reg.registerSignature("min", "native function min(left:meta::pure::metamodel::type::Number[1], right:meta::pure::metamodel::type::Number[1]):meta::pure::metamodel::type::Number[1];");
        // Typed numeric overloads (Handlers.java lines 2434-2451)
        reg.registerSignature("min", "native function min(left:meta::pure::metamodel::type::Integer[1], right:meta::pure::metamodel::type::Integer[1]):meta::pure::metamodel::type::Integer[1];");
        reg.registerSignature("min", "native function min(left:meta::pure::metamodel::type::Float[1], right:meta::pure::metamodel::type::Float[1]):meta::pure::metamodel::type::Float[1];");
        reg.registerSignature("min", "native function min(values:meta::pure::metamodel::type::Integer[*]):meta::pure::metamodel::type::Integer[0..1];");
        reg.registerSignature("min", "native function min(values:meta::pure::metamodel::type::Float[*]):meta::pure::metamodel::type::Float[0..1];");
        // Temporal overloads (Handlers.java)
        reg.registerSignature("min", "native function min(left:meta::pure::metamodel::type::DateTime[1], right:meta::pure::metamodel::type::DateTime[1]):meta::pure::metamodel::type::DateTime[1];");
        reg.registerSignature("min", "native function min(left:meta::pure::metamodel::type::StrictDate[1], right:meta::pure::metamodel::type::StrictDate[1]):meta::pure::metamodel::type::StrictDate[1];");
        reg.registerSignature("min", "native function min(left:meta::pure::metamodel::type::Date[1], right:meta::pure::metamodel::type::Date[1]):meta::pure::metamodel::type::Date[1];");
        reg.registerSignature("min", "native function min(dates:meta::pure::metamodel::type::DateTime[*]):meta::pure::metamodel::type::DateTime[0..1];");
        reg.registerSignature("min", "native function min(dates:meta::pure::metamodel::type::StrictDate[*]):meta::pure::metamodel::type::StrictDate[0..1];");
        reg.registerSignature("min", "native function min(dates:meta::pure::metamodel::type::Date[*]):meta::pure::metamodel::type::Date[0..1];");
        reg.registerSignature("max", "native function max(numbers:meta::pure::metamodel::type::Number[*]):meta::pure::metamodel::type::Number[0..1];");
        reg.registerSignature("max", "native function max(left:meta::pure::metamodel::type::Number[1], right:meta::pure::metamodel::type::Number[1]):meta::pure::metamodel::type::Number[1];");
        // Typed numeric overloads
        reg.registerSignature("max", "native function max(left:meta::pure::metamodel::type::Integer[1], right:meta::pure::metamodel::type::Integer[1]):meta::pure::metamodel::type::Integer[1];");
        reg.registerSignature("max", "native function max(left:meta::pure::metamodel::type::Float[1], right:meta::pure::metamodel::type::Float[1]):meta::pure::metamodel::type::Float[1];");
        reg.registerSignature("max", "native function max(values:meta::pure::metamodel::type::Integer[*]):meta::pure::metamodel::type::Integer[0..1];");
        reg.registerSignature("max", "native function max(values:meta::pure::metamodel::type::Float[*]):meta::pure::metamodel::type::Float[0..1];");
        // Temporal overloads
        reg.registerSignature("max", "native function max(left:meta::pure::metamodel::type::DateTime[1], right:meta::pure::metamodel::type::DateTime[1]):meta::pure::metamodel::type::DateTime[1];");
        reg.registerSignature("max", "native function max(left:meta::pure::metamodel::type::StrictDate[1], right:meta::pure::metamodel::type::StrictDate[1]):meta::pure::metamodel::type::StrictDate[1];");
        reg.registerSignature("max", "native function max(left:meta::pure::metamodel::type::Date[1], right:meta::pure::metamodel::type::Date[1]):meta::pure::metamodel::type::Date[1];");
        reg.registerSignature("max", "native function max(dates:meta::pure::metamodel::type::DateTime[*]):meta::pure::metamodel::type::DateTime[0..1];");
        reg.registerSignature("max", "native function max(dates:meta::pure::metamodel::type::StrictDate[*]):meta::pure::metamodel::type::StrictDate[0..1];");
        reg.registerSignature("max", "native function max(dates:meta::pure::metamodel::type::Date[*]):meta::pure::metamodel::type::Date[0..1];");
        reg.registerSignature("maxBy",
                "native function maxBy<T>(values:T[*], key:Function<{T[1]->meta::pure::metamodel::type::Any[1]}>[1]):T[0..1];");
        reg.registerSignature("maxBy",
                "native function maxBy<T>(values:T[*], key:Function<{T[1]->meta::pure::metamodel::type::Any[1]}>[1], count:meta::pure::metamodel::type::Integer[1]):T[*];");
        // Aggregate spec overload: $y->maxBy() where $y is RowMapper<value,key> from fn1
        // legend-engine: maxBy_RowMapper_MANY__T_$0_1$ — returns first type arg
        reg.registerSignature("maxBy",
                "native function maxBy<T,U>(values:RowMapper<T,U>[*]):T[0..1];");
        // List-keyed: [1,2]->maxBy([10,20]) — keys parallel to values
        reg.registerSignature("maxBy",
                "native function maxBy<T>(values:T[*], keys:T[*]):T[0..1];");
        reg.registerSignature("maxBy",
                "native function maxBy<T>(values:T[*], keys:T[*], count:meta::pure::metamodel::type::Integer[1]):T[*];");
        reg.registerSignature("minBy",
                "native function minBy<T>(values:T[*], key:Function<{T[1]->meta::pure::metamodel::type::Any[1]}>[1]):T[0..1];");
        reg.registerSignature("minBy",
                "native function minBy<T>(values:T[*], key:Function<{T[1]->meta::pure::metamodel::type::Any[1]}>[1], count:meta::pure::metamodel::type::Integer[1]):T[*];");
        // Aggregate spec overload: $y->minBy() where $y is RowMapper<value,key> from fn1
        // legend-engine: minBy_RowMapper_MANY__T_$0_1$ — returns first type arg
        reg.registerSignature("minBy",
                "native function minBy<T,U>(values:RowMapper<T,U>[*]):T[0..1];");
        // List-keyed: [1,2]->minBy([10,20]) — keys parallel to values
        reg.registerSignature("minBy",
                "native function minBy<T>(values:T[*], keys:T[*]):T[0..1];");
        reg.registerSignature("minBy",
                "native function minBy<T>(values:T[*], keys:T[*], count:meta::pure::metamodel::type::Integer[1]):T[*];");
        reg.registerSignature("stdDev", "native function stdDev(numbers:meta::pure::metamodel::type::Number[*]):meta::pure::metamodel::type::Number[1];");
        reg.registerSignature("stdDevSample", "native function stdDevSample(numbers:meta::pure::metamodel::type::Number[*]):meta::pure::metamodel::type::Number[1];");
        reg.registerSignature("variance", "native function variance(numbers:meta::pure::metamodel::type::Number[*]):meta::pure::metamodel::type::Number[1];");
        // 2-arg form: variance(numbers, isSample) — PCT uses variance([1,2], false) for population
        reg.registerSignature("variance",
                "native function variance(numbers:meta::pure::metamodel::type::Number[*], isSample:meta::pure::metamodel::type::Boolean[1]):meta::pure::metamodel::type::Number[1];");
        reg.registerSignature("varianceSample", "native function varianceSample(numbers:meta::pure::metamodel::type::Number[*]):meta::pure::metamodel::type::Number[1];");
        reg.registerSignature("variancePopulation", "native function variancePopulation(numbers:meta::pure::metamodel::type::Number[*]):meta::pure::metamodel::type::Number[1];");
        reg.registerSignature("covarPopulation",
                "native function covarPopulation(x:meta::pure::metamodel::type::Number[*], y:meta::pure::metamodel::type::Number[*]):meta::pure::metamodel::type::Number[1];");
        // Aggregate spec overload: $y->covarPopulation() where $y is RowMapper from fn1
        // legend-engine: covarPopulation_RowMapper_MANY__Number_$0_1$
        reg.registerSignature("covarPopulation",
                "native function covarPopulation<T,U>(values:RowMapper<T,U>[*]):meta::pure::metamodel::type::Number[0..1];");
        reg.registerSignature("corr", "native function corr(x:meta::pure::metamodel::type::Number[*], y:meta::pure::metamodel::type::Number[*]):meta::pure::metamodel::type::Number[1];");
        // Aggregate spec overload: $y->corr() where $y is RowMapper from fn1
        // legend-engine: corr_RowMapper_MANY__Number_$0_1$
        reg.registerSignature("corr",
                "native function corr<T,U>(values:RowMapper<T,U>[*]):meta::pure::metamodel::type::Number[0..1];");
        reg.registerSignature("percentile", "native function percentile(numbers:meta::pure::metamodel::type::Number[*], p:meta::pure::metamodel::type::Number[1]):meta::pure::metamodel::type::Number[1];");
        // Official: percentile(Number[*], Float[1], Boolean[1], Boolean[1]):Number[0..1]
        reg.registerSignature("percentile", "native function percentile(numbers:meta::pure::metamodel::type::Number[*], p:meta::pure::metamodel::type::Number[1], ascending:meta::pure::metamodel::type::Boolean[1], continuous:meta::pure::metamodel::type::Boolean[1]):meta::pure::metamodel::type::Number[0..1];");
        // legend-engine: wavg_RowMapper_MANY__Float_1_
        reg.registerSignature("wavg",
                "native function wavg<T,U>(values:RowMapper<T,U>[*]):meta::pure::metamodel::type::Float[1];");
        reg.registerSignature("covarSample", "native function covarSample(x:meta::pure::metamodel::type::Number[*], y:meta::pure::metamodel::type::Number[*]):meta::pure::metamodel::type::Number[1];");
        // Aggregate spec overload: $y->covarSample() where $y is RowMapper from fn1
        // legend-engine: covarSample_RowMapper_MANY__Number_$0_1$
        reg.registerSignature("covarSample",
                "native function covarSample<T,U>(values:RowMapper<T,U>[*]):meta::pure::metamodel::type::Number[0..1];");
        reg.registerSignature("stdDevPopulation", "native function stdDevPopulation(numbers:meta::pure::metamodel::type::Number[*]):meta::pure::metamodel::type::Number[1];");
        reg.registerSignature("percentileCont",
                "native function percentileCont(numbers:meta::pure::metamodel::type::Number[*], p:meta::pure::metamodel::type::Number[1]):meta::pure::metamodel::type::Number[1];");
        reg.registerSignature("percentileDisc",
                "native function percentileDisc(numbers:meta::pure::metamodel::type::Number[*], p:meta::pure::metamodel::type::Number[1]):meta::pure::metamodel::type::Number[1];");

        // ===== Window aggregate overloads =====
        // These model the 3-param window call pattern: (Relation<T>, _Window<T>, T) → T
        // The TypeVar return type causes compileTypePropagating to propagate
        // relationType in window context (e.g., $p->sum($w,$r).salary).
        reg.registerSignature("sum", "native function sum<T>(rel:Relation<T>[1], w:_Window<T>[1], r:T[1]):T[0..1];");
        reg.registerSignature("avg", "native function avg<T>(rel:Relation<T>[1], w:_Window<T>[1], r:T[1]):T[0..1];");
        reg.registerSignature("average", "native function average<T>(rel:Relation<T>[1], w:_Window<T>[1], r:T[1]):T[0..1];");
        reg.registerSignature("mean", "native function mean<T>(rel:Relation<T>[1], w:_Window<T>[1], r:T[1]):T[0..1];");
        reg.registerSignature("min", "native function min<T>(rel:Relation<T>[1], w:_Window<T>[1], r:T[1]):T[0..1];");
        reg.registerSignature("max", "native function max<T>(rel:Relation<T>[1], w:_Window<T>[1], r:T[1]):T[0..1];");
        reg.registerSignature("count", "native function count<T>(rel:Relation<T>[1], w:_Window<T>[1], r:T[1]):meta::pure::metamodel::type::Integer[1];");
        reg.registerSignature("median", "native function median<T>(rel:Relation<T>[1], w:_Window<T>[1], r:T[1]):T[0..1];");
        reg.registerSignature("mode", "native function mode<T>(rel:Relation<T>[1], w:_Window<T>[1], r:T[1]):T[0..1];");
        reg.registerSignature("percentile", "native function percentile<T>(rel:Relation<T>[1], w:_Window<T>[1], r:T[1]):T[0..1];");
        reg.registerSignature("stdDev", "native function stdDev<T>(rel:Relation<T>[1], w:_Window<T>[1], r:T[1]):T[0..1];");
        reg.registerSignature("stdDevSample", "native function stdDevSample<T>(rel:Relation<T>[1], w:_Window<T>[1], r:T[1]):T[0..1];");
        reg.registerSignature("stdDevPopulation", "native function stdDevPopulation<T>(rel:Relation<T>[1], w:_Window<T>[1], r:T[1]):T[0..1];");
        reg.registerSignature("variance", "native function variance<T>(rel:Relation<T>[1], w:_Window<T>[1], r:T[1]):T[0..1];");
        reg.registerSignature("varianceSample", "native function varianceSample<T>(rel:Relation<T>[1], w:_Window<T>[1], r:T[1]):T[0..1];");
        reg.registerSignature("variancePopulation", "native function variancePopulation<T>(rel:Relation<T>[1], w:_Window<T>[1], r:T[1]):T[0..1];");
        reg.registerSignature("corr", "native function corr<T>(rel:Relation<T>[1], w:_Window<T>[1], r:T[1]):T[0..1];");
        reg.registerSignature("covarPopulation", "native function covarPopulation<T>(rel:Relation<T>[1], w:_Window<T>[1], r:T[1]):T[0..1];");
        reg.registerSignature("covarSample", "native function covarSample<T>(rel:Relation<T>[1], w:_Window<T>[1], r:T[1]):T[0..1];");

        // ===== Collection =====
        reg.registerSignature("toOne", "native function toOne<T>(values:T[*]):T[1];");
        reg.registerSignature("toOne", "native function toOne<T>(values:T[*], message:meta::pure::metamodel::type::String[1]):T[1];");
        reg.registerSignature("toOneMany", "native function toOneMany<T>(values:T[*], message:meta::pure::metamodel::type::String[1]):T[1..*];");
        reg.registerSignature("last", "native function last<T>(set:T[*]):T[0..1];");
        reg.registerSignature("head", "native function head<T>(set:T[*]):T[0..1];");
        reg.registerSignature("at", "native function at<T>(set:T[*], index:meta::pure::metamodel::type::Integer[1]):T[1];");
        reg.registerSignature("reverse", "native function reverse<T|m>(set:T[m]):T[m];");
        // slice, take, drop registered below alongside relation overloads
        reg.registerSignature("exists",
                "native function exists<T>(value:T[*], func:Function<{T[1]->meta::pure::metamodel::type::Boolean[1]}>[1]):meta::pure::metamodel::type::Boolean[1];");
        reg.registerSignature("forAll",
                "native function forAll<T>(value:T[*], func:Function<{T[1]->meta::pure::metamodel::type::Boolean[1]}>[1]):meta::pure::metamodel::type::Boolean[1];");
        reg.registerSignature("find",
                "native function find<T>(value:T[*], func:Function<{T[1]->meta::pure::metamodel::type::Boolean[1]}>[1]):T[0..1];");
        reg.registerSignature("filter",
                "native function filter<T>(value:T[*], func:Function<{T[1]->meta::pure::metamodel::type::Boolean[1]}>[1]):T[*];");
        reg.registerSignature("map", "native function map<T,V>(value:T[*], func:Function<{T[1]->V[*]}>[1]):V[*];");
        reg.registerSignature("map",
                "native function map<T,V>(value:T[0..1], func:Function<{T[1]->V[0..1]}>[1]):V[0..1];");
        reg.registerSignature("fold",
                "native function fold<T,V>(source:T[*], lambda:Function<{T[1],V[1]->V[1]}>[1], init:V[1]):V[1];");
        reg.registerSignature("zip", "native function zip<T,U>(set1:T[*], set2:U[*]):Pair<T,U>[*];");
        reg.registerSignature("indexOf", "native function indexOf<T>(set:T[*], value:T[1]):meta::pure::metamodel::type::Integer[1];");
        reg.registerSignature("removeAllOptimized",
                "native function removeAllOptimized<T>(set:T[*], other:T[*]):T[*];");
        reg.registerSignature("size", "native function size<T>(col:T[*]):meta::pure::metamodel::type::Integer[1];");
        reg.registerSignature("range", "native function range(start:meta::pure::metamodel::type::Integer[1], stop:meta::pure::metamodel::type::Integer[1], step:meta::pure::metamodel::type::Integer[1]):meta::pure::metamodel::type::Integer[*];");
        reg.registerSignature("range", "native function range(start:meta::pure::metamodel::type::Integer[1], stop:meta::pure::metamodel::type::Integer[1]):meta::pure::metamodel::type::Integer[*];");
        reg.registerSignature("range", "native function range(stop:meta::pure::metamodel::type::Integer[1]):meta::pure::metamodel::type::Integer[*];");
        reg.registerSignature("init", "native function init<T>(set:T[*]):T[*];");
        reg.registerSignature("tail", "native function tail<T>(set:T[*]):T[*];");
        reg.registerSignature("add", "native function add<T>(set:T[*], val:T[1]):T[*];");
        reg.registerSignature("add", "native function add<T>(set:T[*], index:meta::pure::metamodel::type::Integer[1], val:T[1]):T[*];");
        reg.registerSignature("removeDuplicates", "native function removeDuplicates<T>(col:T[*]):T[*];");
        // 2-arg convenience form (removeDuplicates.pure line 25): delegates to 3-arg native with [] key
        reg.registerSignature("removeDuplicates",
                "native function removeDuplicates<T>(col:T[*], eql:Function<{T[1],T[1]->meta::pure::metamodel::type::Boolean[1]}>[1]):T[*];");
        // 3-arg native form (removeDuplicates.pure line 23)
        reg.registerSignature("removeDuplicates",
                "native function removeDuplicates<T,V>(col:T[*], key:Function<{T[1]->V[1]}>[0..1], eql:Function<{V[1],V[1]->meta::pure::metamodel::type::Boolean[1]}>[0..1]):T[*];");
        reg.registerSignature("removeDuplicatesBy",
                "native function removeDuplicatesBy<T,V>(col:T[*], key:Function<{T[1]->V[1]}>[1]):T[*];");
        reg.registerSignature("first", "native function first<T>(set:T[*]):T[0..1];");
        // 2-arg: first(set, count) — take first N elements
        reg.registerSignature("first", "native function first<T>(set:T[*], count:meta::pure::metamodel::type::Integer[1]):T[*];");

        // ===== Relation overloads for limit/take/drop/slice =====
        reg.registerSignature("limit",
                "native function limit<T>(rel:Relation<T>[1], size:meta::pure::metamodel::type::Integer[1]):Relation<T>[1];");
        reg.registerSignature("take",
                "native function take<T>(rel:Relation<T>[1], size:meta::pure::metamodel::type::Integer[1]):Relation<T>[1];");
        reg.registerSignature("drop",
                "native function drop<T>(rel:Relation<T>[1], size:meta::pure::metamodel::type::Integer[1]):Relation<T>[1];");
        reg.registerSignature("slice",
                "native function slice<T>(rel:Relation<T>[1], start:meta::pure::metamodel::type::Integer[1], stop:meta::pure::metamodel::type::Integer[1]):Relation<T>[1];");
        // Scalar overloads for drop/slice (array operations)
        reg.registerSignature("drop",
                "native function drop<T>(set:T[*], count:meta::pure::metamodel::type::Integer[1]):T[*];");
        reg.registerSignature("slice",
                "native function slice<T>(set:T[*], start:meta::pure::metamodel::type::Integer[1], end:meta::pure::metamodel::type::Integer[1]):T[*];");
        reg.registerSignature("limit",
                "native function limit<T>(set:T[*], size:meta::pure::metamodel::type::Integer[1]):T[*];");
        reg.registerSignature("take",
                "native function take<T>(set:T[*], size:meta::pure::metamodel::type::Integer[1]):T[*];");

        // ===== Boolean / Meta =====
        reg.registerSignature("instanceOf", "native function instanceOf(instance:meta::pure::metamodel::type::Any[1], type:Type[1]):meta::pure::metamodel::type::Boolean[1];");

        // ===== Collection sort (legend-pure) =====
        reg.registerSignature("sort",
                "native function sort<T,U|m>(col:T[m], key:Function<{T[1]->U[1]}>[0..1], comp:Function<{U[1],U[1]->meta::pure::metamodel::type::Integer[1]}>[0..1]):T[m];");
        reg.registerSignature("sort", "native function sort<T|m>(col:T[m]):T[m];");
        reg.registerSignature("sort",
                "native function sort<T|m>(col:T[m], comp:Function<{T[1],T[1]->meta::pure::metamodel::type::Integer[1]}>[0..1]):T[m];");

        // ===== Collection sortBy / sortByReversed (legend-engine) =====
        reg.registerSignature("sortBy",
                "native function sortBy<T,U|m>(col:T[m], key:Function<{T[1]->U[1]}>[0..1]):T[m];");
        reg.registerSignature("sortByReversed",
                "native function sortByReversed<T,U|m>(col:T[m], key:Function<{T[1]->U[1]}>[0..1]):T[m];");

        // ===== letFunction =====
        // Standard form: letFunction('varName', valueExpr) → propagates value type
        reg.registerSignature("letFunction",
                "native function letFunction<T>(name:meta::pure::metamodel::type::String[1], value:T[1]):T[1];");
        // Single-param form (expression-only let, no variable binding)
        reg.registerSignature("letFunction",
                "native function letFunction<T>(value:T[*]):T[*];");

        // ===== from (runtime binding) =====
        // Relational: from(source:Relation<T>, runtime) — passthrough, just binds runtime
        reg.registerSignature("from",
                "native function from<T>(source:Relation<T>[1], runtime:meta::pure::metamodel::type::Any[1]):Relation<T>[1];");
        // Single-arg: from(source:Relation<T>) — identity passthrough
        reg.registerSignature("from",
                "native function from<T>(source:Relation<T>[1]):Relation<T>[1];");
        // Scalar from: from(source:T, mapping, runtime) — M2M / non-relational
        reg.registerSignature("from",
                "native function from<T>(source:T[*], mapping:meta::pure::metamodel::type::Any[1], runtime:meta::pure::metamodel::type::Any[1]):T[*];");

        // ===== write =====
        // write(source:Relation<T>, target) → Integer[1] (count of rows written)
        reg.registerSignature("write",
                "native function write<T>(source:Relation<T>[1], target:meta::pure::metamodel::type::Any[1]):meta::pure::metamodel::type::Integer[1];");
        // write(source:Relation<T>) — single-arg form
        reg.registerSignature("write",
                "native function write<T>(source:Relation<T>[1]):meta::pure::metamodel::type::Integer[1];");
        // TDS relation accessor — wraps a TDS literal into a target for write()
        reg.registerSignature("newTDSRelationAccessor",
                "native function newTDSRelationAccessor<T>(tds:Relation<T>[1]):Relation<T>[1];");

        // ===== if (conditional) =====
        // if(condition, thenLambda, elseLambda) → T
        reg.registerSignature("if",
                "native function if<T>(test:meta::pure::metamodel::type::Boolean[1], then:Function<{->T[*]}>[1], else:Function<{->T[*]}>[1]):T[*];");

        // ===== get (variant navigation) =====
        // get(variant, key) → Variant[0..1] — untyped navigation on a JSON-backed source.
        // Key is Any[1] to accept String (field access) or Integer (array index); the
        // PlanGenerator branches on key type to emit SQL. Return type is always Variant
        // so chained {@code ->get(...)->get(...)} compose naturally and to(@Type) handles
        // final typed extraction.
        reg.registerSignature("get",
                "native function get(source:meta::pure::metamodel::variant::Variant[1], key:meta::pure::metamodel::type::Any[1]):meta::pure::metamodel::variant::Variant[0..1];");

        // ===== Variant conversion (legend-engine aligned) =====
        // to(@Type) → T[0..1] — scalar conversion
        reg.registerSignature("to",
                "native function to<T,V>(source:T[0..1], type:V[0..1]):V[0..1];");
        // toMany(@Type) → T[*] — array conversion
        reg.registerSignature("toMany",
                "native function toMany<T,V>(source:T[0..1], type:V[0..1]):V[*];");

        // ===== Type Conversion =====
        // cast: preserves source multiplicity via mult var |m
        reg.registerSignature("cast",
                "native function cast<T|m>(source:meta::pure::metamodel::type::Any[m], type:T[1]):T[m];");
        // toVariant: lossy conversion to untyped Variant (returns Variant, NOT input type)
        reg.registerSignature("toVariant",
                "native function toVariant(source:meta::pure::metamodel::type::Any[*]):meta::pure::metamodel::variant::Variant[1];");

        // ===== Data Access =====
        // tableReference: synthetic — emitted by MappingNormalizer and #>{db.TABLE}# DSL
        // Custom checker: TableReferenceChecker (resolves table, builds schema)
        reg.registerSignature("tableReference",
                "native function tableReference(db:meta::pure::metamodel::type::String[1], name:meta::pure::metamodel::type::String[1]):Relation<meta::pure::metamodel::type::Any>[1];");
        // tds: synthetic — emitted by parser for TDS literals
        // Custom checker: TdsChecker (parses inline TDS, builds schema)
        reg.registerSignature("tds",
                "native function tds(tag:meta::pure::metamodel::type::String[1], raw:meta::pure::metamodel::type::String[1]):Relation<meta::pure::metamodel::type::Any>[1];");
        // getAll: class-based data query — T from Class type argument
        reg.registerSignature("getAll",
                "native function getAll<T>(class:Class<T>[1]):T[*];");
        reg.registerSignature("getAll",
                "native function getAll<T>(class:Class<T>[1], date:meta::pure::metamodel::type::Date[1]):T[*];");
        reg.registerSignature("getAll",
                "native function getAll<T>(class:Class<T>[1], from:meta::pure::metamodel::type::Date[1], to:meta::pure::metamodel::type::Date[1]):T[*];");

        // ===== Meta Functions =====
        // eval: function application — return type from compiled body, not signature
        reg.registerSignature("eval",
                "native function eval(func:Function<meta::pure::metamodel::type::Any>[1]):meta::pure::metamodel::type::Any[*];");
        reg.registerSignature("eval",
                "native function eval<T>(func:Function<meta::pure::metamodel::type::Any>[1], param:T[*]):meta::pure::metamodel::type::Any[*];");
        // match: type dispatch — return type from matching branch body
        reg.registerSignature("match",
                "native function match(value:meta::pure::metamodel::type::Any[*], branches:Function<meta::pure::metamodel::type::Any>[1..*]):meta::pure::metamodel::type::Any[*];");
        reg.registerSignature("match",
                "native function match<P>(value:meta::pure::metamodel::type::Any[*], branches:Function<meta::pure::metamodel::type::Any>[1..*], extra:P[*]):meta::pure::metamodel::type::Any[*];");

        // ===== Graph Fetch =====
        reg.registerSignature("graphFetch",
                "native function graphFetch<T>(source:T[*], tree:RootGraphFetchTree<T>[1]):T[*];");
        reg.registerSignature("graphFetch",
                "native function graphFetch<T>(source:T[*], tree:RootGraphFetchTree<T>[1], batchSize:meta::pure::metamodel::type::Integer[1]):T[*];");
        // graphFetch with ColSpec/ColSpecArray — desugared form of #{...}#
        reg.registerSignature("graphFetch",
                "native function graphFetch<T>(source:T[*], col:ColSpec<T>[1]):meta::pure::metamodel::type::String[1];");
        reg.registerSignature("graphFetch",
                "native function graphFetch<T>(source:T[*], cols:ColSpecArray<T>[1]):meta::pure::metamodel::type::String[1];");
        reg.registerSignature("serialize",
                "native function serialize<T>(source:T[*], tree:RootGraphFetchTree<T>[1]):meta::pure::metamodel::type::String[1];");
        reg.registerSignature("serialize",
                "native function serialize<T>(source:T[*], tree:RootGraphFetchTree<T>[1], config:meta::pure::metamodel::type::Any[1]):meta::pure::metamodel::type::String[1];");

        // ===== Misc =====
        reg.registerSignature("pair", "native function pair<T,U>(first:T[1], second:U[1]):Pair<T,U>[1];");
        reg.registerSignature("list", "native function list<T>(values:T[*]):List<T>[1];");
        reg.registerSignature("type", "native function type(any:meta::pure::metamodel::type::Any[1]):Type[1];");
        reg.registerSignature("generateGuid", "native function generateGuid():meta::pure::metamodel::type::String[1];");
        // rowMapper: matches legend-engine rowMapper_T_$0_1$__U_$0_1$__RowMapper_1_
        reg.registerSignature("rowMapper",
                "native function rowMapper<T,U>(value:T[0..1], key:U[0..1]):RowMapper<T,U>[1];");
    }
}
