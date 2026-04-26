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
    public NativeFunctionDef registerSignature(String name, String pureSignature) {
        NativeFunctionDef normalized = parseAndNormalize(pureSignature);
        register(normalized);
        return normalized;
    }

    /**
     * Parse a Pure native function signature into a {@link NativeFunctionDef}
     * without registering it. Used by {@link Pure} to build typed constants
     * at class-load time before {@link #INSTANCE} ingests them.
     */
    public static NativeFunctionDef parseAndNormalize(String pureSignature) {
        String rawSignature = pureSignature.trim();
        var def = com.gs.legend.parser.PureNativeSignatureParser.parse(rawSignature);
        return normalizeConstraints(def);
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
            case Type.GenericType gt -> new Type.GenericType(
                    gt.rawType(),
                    gt.typeArgs().stream().map(BuiltinRegistry::normalizeType).toList());
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
            case com.gs.legend.model.m3.LClass lc -> lc;
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
                && Type.asLClass(def.params().get(0).type()) == com.gs.legend.model.m3.LClass.RELATION;
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

    // ========== Platform profiles ==========

    // Profiles shipped by the platform, always in scope. Currently hosts the `equality`
    // profile whose `Key` stereotype is applied on properties of builtin classes like
    // `Pair<U,V>` and `List<T>` — legend-pure authors use {@code <<equality.Key>>}
    // there to declare membership in the structural-equality key. Registering empty
    // (stereotype-name-only) bodies is sufficient: legend-lite doesn't compile
    // stereotype bodies, it only needs the profile FQN to be resolvable so the
    // stereotype-application records canonicalize cleanly.

    /** Platform profiles registered at JVM init; mirrors {@link #PLATFORM_ENUMS}. */
    public static final List<com.gs.legend.model.def.ProfileDefinition> PLATFORM_PROFILES = List.of(
            new com.gs.legend.model.def.ProfileDefinition(
                    "meta::pure::profiles::equality",
                    List.of("Key"),
                    List.of()));

    /** FQN → ProfileDefinition lookup for platform profiles (immutable). */
    private static final Map<String, com.gs.legend.model.def.ProfileDefinition> PLATFORM_PROFILES_BY_FQN;
    static {
        Map<String, com.gs.legend.model.def.ProfileDefinition> m = new LinkedHashMap<>();
        for (var p : PLATFORM_PROFILES) m.put(p.qualifiedName(), p);
        PLATFORM_PROFILES_BY_FQN = Map.copyOf(m);
    }

    /** Looks up a platform profile by fully qualified name. */
    public static Optional<com.gs.legend.model.def.ProfileDefinition> findPlatformProfile(String fqn) {
        return Optional.ofNullable(PLATFORM_PROFILES_BY_FQN.get(fqn));
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
        var imports = new ArrayList<String>(
                Primitive.ALL.size() + PLATFORM_ENUMS.size() + PLATFORM_PROFILES.size()
                        + BuiltinClassRegistry.BUILTIN_CLASS_FQNS.size());
        for (Primitive p : Primitive.ALL) imports.add(p.qualifiedName());
        for (EnumDefinition e : PLATFORM_ENUMS) imports.add(e.qualifiedName());
        for (var prof : PLATFORM_PROFILES) imports.add(prof.qualifiedName());
        // Phase 2.5e: platform classes (Pair, Relation, SortInfo, ...) auto-imported so
        // user source can reference them by simple name just like primitives and enums.
        imports.addAll(BuiltinClassRegistry.BUILTIN_CLASS_FQNS);
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

    /**
     * Bootstraps the registry from {@link Pure} — the single source of truth
     * for native declarations. Each {@code Pure.X} constant is a typed
     * {@link NativeFunctionDef} parsed at class-load time; we ingest the
     * snapshot here. No string-keyed registration calls remain.
     */
    private static BuiltinRegistry createDefault() {
        var reg = new BuiltinRegistry();
        for (NativeFunctionDef def : Pure.all()) {
            reg.register(def);
        }
        reg.cacheConvenienceDefs();
        return reg;
    }

}
