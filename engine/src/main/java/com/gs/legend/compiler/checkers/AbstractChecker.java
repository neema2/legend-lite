package com.gs.legend.compiler.checkers;

import com.gs.legend.ast.*;
import com.gs.legend.compiler.*;
import com.gs.legend.model.m3.Multiplicity;
import com.gs.legend.plan.GenericType;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Base class for function type-checkers.
 *
 * <p>Provides signature-driven validation infrastructure:
 * <ul>
 *   <li>{@link #unify} — validate + bind type variables from signature against actuals</li>
 *   <li>{@link #resolve} — resolve a PType to GenericType using bindings</li>
 *   <li>{@link #resolveMult} — resolve a Mult to Multiplicity</li>
 *   <li>{@link #bindLambdaParam} — bind lambda param in CompilationContext</li>
 *   <li>{@link #resolveOutput} — construct output ExpressionType from signature return type</li>
 * </ul>
 *
 * <p>String comparisons on {@code PType.Parameterized.rawType()} are centralized
 * in {@link #unifyType} and {@link #resolve} — the bridge between parse-time
 * PTypes and compile-time GenericTypes. TODO: replace with proper type algebra.
 */
public abstract class AbstractChecker implements FunctionChecker {

    protected final TypeCheckEnv env;

    protected AbstractChecker(TypeCheckEnv env) {
        this.env = env;
    }

    // ========== Bindings ==========

    /**
     * Unified container for type-variable and multiplicity-variable bindings.
     *
     * <p>Populated by {@link #unify} in one pass — captures both type vars
     * (T, V, etc.) and mult vars (m) from the source expression. Used by
     * {@link #resolve} and {@link #resolveOutput} to reconstruct output types
     * with correct types AND multiplicities.
     *
     * <p>Delegate methods ({@code get}, {@code put}, {@code containsKey},
     * {@code getOrDefault}) forward to the type-var map for backward
     * compatibility — most callers only interact with type vars directly.
     */
    protected static class Bindings {
        private final Map<String, GenericType> types = new LinkedHashMap<>();
        private final Map<String, Multiplicity> mults = new LinkedHashMap<>();

        public Map<String, GenericType> types() { return types; }
        public Map<String, Multiplicity> mults() { return mults; }

        // Delegates to type-var map for zero-churn backward compat
        public GenericType get(String key) { return types.get(key); }
        public void put(String key, GenericType value) { types.put(key, value); }
        public boolean containsKey(String key) { return types.containsKey(key); }
        public GenericType getOrDefault(String key, GenericType defaultValue) {
            return types.getOrDefault(key, defaultValue);
        }
    }

    // ========== Overload resolution ==========

    /**
     * Resolves the correct function overload by matching the AST param structure
     * against registered signature param types.
     *
     * <p>Backward-compatible entry point — delegates to the compile-then-resolve
     * overload with an empty compiled-types map. Used by ExtendChecker, FilterChecker,
     * and other checkers that don't pre-compile params.
     */
    protected NativeFunctionDef resolveOverload(String funcName,
                                                 List<ValueSpecification> params,
                                                 TypeInfo source) {
        return resolveOverload(funcName, params, source, Map.of());
    }

    /**
     * Compile-then-resolve overload resolution.
     *
     * <p>Resolution strategy (JLS §15.12-style, no silent fallbacks):
     * <ol>
     *   <li>Filter by arity — throw if none match</li>
     *   <li>If exactly one — return it</li>
     *   <li>Structural match: use compiled param types when available,
     *       fall back to AST-shape matching otherwise</li>
     *   <li>If still ambiguous — score by type specificity (exact &gt; subtype &gt; TypeVar)</li>
     *   <li>Exactly one must win — throw if zero or ambiguous</li>
     * </ol>
     *
     * @param funcName       Simple function name
     * @param params         AST parameters
     * @param source         Compiled TypeInfo for param[0], or null
     * @param compiledTypes  Pre-compiled expression types keyed by param index (may be empty)
     * @return The single matching NativeFunctionDef
     * @throws PureCompileException on no match or ambiguity
     */
    protected NativeFunctionDef resolveOverload(String funcName,
                                                 List<ValueSpecification> params,
                                                 TypeInfo source,
                                                 Map<Integer, ExpressionType> compiledTypes) {
        var defs = BuiltinFunctionRegistry.instance().resolve(funcName);
        if (defs.isEmpty()) {
            throw new PureCompileException("Unknown function: '" + funcName + "'");
        }

        // Step 1: filter by exact arity first; fall back to variadic [*] matches
        var arityMatches = defs.stream()
                .filter(d -> d.arity() == params.size())
                .toList();
        if (arityMatches.isEmpty()) {
            arityMatches = defs.stream()
                    .filter(d -> d.matchesArity(params.size()))
                    .toList();
        }
        if (arityMatches.isEmpty()) {
            throw new PureCompileException(
                    "No overload of '" + funcName + "' accepts " + params.size() + " arguments"
                    + " (registered arities: " + defs.stream()
                            .map(d -> String.valueOf(d.arity())).distinct().toList() + ")");
        }
        if (arityMatches.size() == 1) {
            return arityMatches.get(0);
        }

        // Step 2: structural matching — uses compiled types (type + multiplicity) when available
        var matches = arityMatches.stream()
                .filter(d -> {
                    boolean m = matchesStructurally(d, params, source, compiledTypes);
                    return m;
                })
                .toList();
        if (matches.size() == 1) {
            return matches.get(0);
        }
        if (matches.isEmpty()) {
            throw new PureCompileException(
                    "No overload of '" + funcName + "' matches the given argument types"
                    + " (tried " + arityMatches.size() + " candidates with arity " + params.size() + ")");
        }

        // Step 3: type-based scoring across ALL compiled params.
        // Pick the most specific overload (JLS §15.12.2.5: prefer Integer over Number).
        if (!compiledTypes.isEmpty()) {
            var scored = scoreOverloads(matches, compiledTypes);
            if (scored != null) {
                return scored;
            }
        }

        // Ambiguous — multiple candidates matched
        throw new PureCompileException(
                "Ambiguous overload: " + matches.size() + " overloads of '"
                + funcName + "' match the given argument types");
    }

    /**
     * Scores overload candidates against ALL compiled expression types.
     * Returns the most specific candidate, or null if no unique winner.
     *
     * <p>Per candidate: sum score across all params with compiled types.
     * Type: exact = 2, subtype = 1, TypeVar = 0, incompatible = -1.
     * Multiplicity: exact = 5, graduated subsumption 1–4, Var = 0, incompatible = -1.
     * A single -1 on either axis eliminates the candidate.
     *
     * <p>Mirrors JLS §15.12.2.5: among applicable methods, prefer the one
     * with more specific parameter types and multiplicities.
     */
    private NativeFunctionDef scoreOverloads(List<NativeFunctionDef> candidates,
                                              Map<Integer, ExpressionType> compiledTypes) {
        NativeFunctionDef best = null;
        int bestScore = -1;
        boolean ambiguous = false;

        for (var def : candidates) {
            int score = scoreCandidate(def, compiledTypes);
            if (score < 0) continue; // not applicable
            if (score > bestScore) {
                best = def;
                bestScore = score;
                ambiguous = false;
            } else if (score == bestScore) {
                ambiguous = true;
            }
        }

        if (ambiguous || best == null) return null;
        return best;
    }

    /**
     * Scores a single candidate against all compiled expression types.
     * Returns -1 if any param is incompatible, otherwise sum of per-param scores.
     *
     * <p>Each param contributes: {@code typeScore * 10 + multScore}.
     * Type specificity always dominates (exact=2, subtype=1, TypeVar=0).
     * Multiplicity matching compares declared vs actual:
     * exact = 5, graduated subsumption 1–4, Var = 0, incompatible = -1.
     *
     * <p>This correctly prefers {@code map(T[0..1],...)} for {@code JSON[1]}
     * sources over {@code map(T[*],...)} because {@code [0..1]} is a tighter
     * fit than {@code [*]} for a {@code [1]} actual.
     */
    private int scoreCandidate(NativeFunctionDef def, Map<Integer, ExpressionType> compiledTypes) {
        int totalScore = 0;
        for (var entry : compiledTypes.entrySet()) {
            int paramIdx = entry.getKey();
            ExpressionType actual = entry.getValue();
            if (paramIdx >= def.params().size()) continue;

            PType.Param sigParam = def.params().get(paramIdx);
            int typeScore = scoreParamType(sigParam.type(), actual.type());
            if (typeScore < 0) return -1; // incompatible type — eliminate
            int multScore = scoreParamMult(sigParam.mult(), actual.multiplicity());
            if (multScore < 0) return -1; // incompatible multiplicity — eliminate
            totalScore += typeScore * 10 + multScore;
        }
        return totalScore;
    }

    /**
     * Scores a single param: declared type vs actual compiled type.
     * Returns 2 for exact, 1 for subtype, 0 for TypeVar/Any, -1 for incompatible.
     */
    private int scoreParamType(PType declaredType, GenericType actualType) {
        if (declaredType instanceof PType.Concrete c) {
            // Normalize PrecisionDecimal → DECIMAL for scoring
            if (actualType instanceof GenericType.PrecisionDecimal) {
                actualType = GenericType.Primitive.DECIMAL;
            }
            // EnumType: exact match if names equal, else incompatible
            if (actualType instanceof GenericType.EnumType et) {
                return et.typeName().equals(c.name()) ? 2 : -1;
            }
            if ("Any".equals(c.name())) return 0; // Any accepts everything, lowest priority
            if (!(actualType instanceof GenericType.Primitive actualPrim)) {
                return -1;
            }
            try {
                GenericType.Primitive declared = GenericType.Primitive.fromTypeName(c.name());
                if (actualPrim == declared) return 2;           // exact match
                if (actualPrim.isSubtypeOf(declared)) return 1; // subtype match
                return -1; // incompatible
            } catch (IllegalArgumentException e) {
                return -1;
            }
        }
        // TypeVar or Parameterized — always applicable but lowest priority
        return 0;
    }

    /**
     * Scores declared multiplicity vs actual source multiplicity.
     *
     * <p>Scoring uses graduated tightness: tighter declared multiplicities
     * score higher within subsumption, so the most specific applicable
     * overload wins.
     *
     * <p>Score table:
     * <ul>
     *   <li>Exact match (declared == actual) → 5</li>
     *   <li>Declared subsumes actual with tightness ranking:
     *       {@code [1]}=4, {@code [0..1]}=3, {@code [1..*]}=2, {@code [*]}=1</li>
     *   <li>Mult.Var (unresolved variable) → 0 (always applicable, lowest priority)</li>
     *   <li>Incompatible (actual doesn't fit declared) → -1 (eliminates candidate)</li>
     * </ul>
     *
     * <p>Example: actual={@code [1]}, both {@code map(T[*],...)} (score 1) and
     * {@code map(T[0..1],...)} (score 3) subsume it, but {@code [0..1]} wins.
     */
    private int scoreParamMult(Mult declared, Multiplicity actual) {
        if (declared instanceof Mult.Var) {
            return 0; // multiplicity variable — always applicable, lowest priority
        }
        if (declared instanceof Mult.Fixed f) {
            Multiplicity decl = f.value();
            if (decl.equals(actual)) return 5; // exact match — highest
            if (subsumes(decl, actual)) return multiplicityTightness(decl); // graduated
            return -1; // incompatible
        }
        return 0;
    }

    /**
     * Ranks how tight a declared multiplicity is.
     * Tighter = higher score = preferred when both subsume the actual.
     *
     * <p>Ranking: {@code [1]} (4) > {@code [0..1]} (3) > {@code [1..*]} (2) > {@code [*]} (1).
     */
    private static int multiplicityTightness(Multiplicity m) {
        if (m.isSingular() && m.isRequired()) return 4;  // [1]
        if (m.isSingular())                    return 3;  // [0..1]
        if (m.isRequired())                    return 2;  // [1..*]
        return 1;                                         // [*]
    }

    /**
     * True if {@code declared} multiplicity can accept values with {@code actual} multiplicity.
     * {@code [*]} subsumes everything. {@code [0..1]} subsumes {@code [1]}.
     * {@code [1..*]} subsumes {@code [1]}. {@code [0..1]} does NOT subsume {@code [*]} or {@code [1..*]}.
     */
    private static boolean subsumes(Multiplicity declared, Multiplicity actual) {
        return declared.lowerBound() <= actual.lowerBound()
                && (declared.upperBound() == null
                    || (actual.upperBound() != null && declared.upperBound() >= actual.upperBound()));
    }



    /**
     * Checks if a def's param types AND multiplicities structurally match the AST param nodes.
     * Uses compiled types for Concrete matching when available, falls back
     * to AST-shape matching otherwise.
     * Multiplicity: [0..1] actual never matches [1] declared.
     */
    private boolean matchesStructurally(NativeFunctionDef def,
                                        List<ValueSpecification> params,
                                        TypeInfo source,
                                        Map<Integer, ExpressionType> compiledTypes) {
        for (int i = 0; i < def.params().size() && i < params.size(); i++) {
            PType.Param sigParam = def.params().get(i);
            PType expected = sigParam.type();
            ValueSpecification actual = params.get(i);
            ExpressionType compiledExpr = compiledTypes.get(i);
            GenericType compiledType = compiledExpr != null ? compiledExpr.type() : null;
            boolean sm = structuralMatch(expected, actual, source, i == 0, compiledType);
            if (!sm) {
                return false;
            }
            // Multiplicity check: if we have compiled multiplicity, verify it
            // fits the declared multiplicity. [0..1] must NOT match [1].
            if (compiledExpr != null && sigParam.mult() instanceof Mult.Fixed f) {
                if (!subsumes(f.value(), compiledExpr.multiplicity())) {
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * Matches a single PType against an AST node + optional compiled type.
     *
     * <p>For Concrete types: if a compiled type is available, uses subtype
     * checking for precise matching. Otherwise falls back to AST-shape
     * filtering (reject lambdas and class instances).
     */
    private boolean structuralMatch(PType expected, ValueSpecification actual,
                                    TypeInfo source, boolean isSource,
                                    GenericType compiledType) {
        // PureCollection wraps multiple elements (e.g., [ascending(~id), ascending(~name)])
        // For Concrete types with a compiled collection type, use that directly —
        // recursing into elements loses the compiled type context.
        if (actual instanceof PureCollection(java.util.List<ValueSpecification> elements)
                && !elements.isEmpty()) {
            if (compiledType != null && expected instanceof PType.Concrete) {
                // Compiled type is definitive — use subtype checking
                return isConcreteCompatible(((PType.Concrete) expected).name(), compiledType);
            }
            return elements.stream().allMatch(e -> structuralMatch(expected, e, source, false, null));
        }
        if (expected instanceof PType.Parameterized p) {
            return switch (p.rawType()) {
                case "Relation" -> isSource
                        ? (source != null && source.isRelational())
                        : (compiledType instanceof GenericType.Relation)
                          || (actual instanceof ClassInstance ci && "relation".equals(ci.type()));
                case "ColSpec"
                        -> actual instanceof ClassInstance ci && "colSpec".equals(ci.type());
                case "FuncColSpec"
                        -> actual instanceof ClassInstance ci && "colSpec".equals(ci.type())
                            && ci.value() instanceof com.gs.legend.ast.ColSpec cs && cs.function2() == null;
                case "AggColSpec"
                        -> actual instanceof ClassInstance ci && "colSpec".equals(ci.type())
                            && ci.value() instanceof com.gs.legend.ast.ColSpec cs && cs.function2() != null;
                case "ColSpecArray"
                        -> actual instanceof ClassInstance ci && "colSpecArray".equals(ci.type());
                case "FuncColSpecArray"
                        -> actual instanceof ClassInstance ci && "colSpecArray".equals(ci.type())
                            && ci.value() instanceof com.gs.legend.ast.ColSpecArray csa
                            && csa.colSpecs().stream().allMatch(s -> s.function2() == null);
                case "AggColSpecArray"
                        -> actual instanceof ClassInstance ci && "colSpecArray".equals(ci.type())
                            && ci.value() instanceof com.gs.legend.ast.ColSpecArray csa
                            && csa.colSpecs().stream().allMatch(s -> s.function2() != null);
                case "Function" -> actual instanceof LambdaFunction
                    || actual instanceof PackageableElementPtr; // function reference (e.g., eq_Any_1__Any_1__Boolean_1_)
                case "SortInfo" -> actual instanceof AppliedFunction;
                case "_Window" -> actual instanceof AppliedFunction af
                    && !"traverse".equals(simpleName(af.function()));
                case "Rows", "_Range" -> actual instanceof AppliedFunction;
                default -> isSource && source != null
                        && source.type() instanceof GenericType.Parameterized gp
                        && p.rawType().equals(gp.rawType());
            };
        }
        if (expected instanceof PType.TypeVar) {
            // TypeVar (T, V, etc.) matches anything EXCEPT Relation sources
            return !isSource || source == null || !source.isRelational();
        }
        if (expected instanceof PType.Concrete c) {
            // Any is the top type — accepts everything
            if ("Any".equals(c.name())) {
                return true;
            }
            // _Traversal: matches traverse() AppliedFunction (no type params → Concrete)
            if ("_Traversal".equals(c.name())) {
                return actual instanceof AppliedFunction af
                        && "traverse".equals(simpleName(af.function()));
            }
            // Reject lambdas and class instances for specific Concrete types
            // (e.g., Number, String) — they are structural, not scalar values
            if (actual instanceof LambdaFunction || actual instanceof ClassInstance) {
                return false;
            }

            // If we have a compiled type for this param, use precise subtype checking
            if (compiledType != null) {
                return isConcreteCompatible(c.name(), compiledType);
            }
            // No compiled type → can't match Concrete types.
            // All callers that need Concrete matching go through ScalarChecker
            // which always provides compiled types. 3-arg callers resolve via
            // arity or Parameterized matching before reaching this point.
            return false;
        }
        return false;
    }


    /**
     * Checks if a compiled type is compatible with a declared Concrete type name.
     * Compatible means: exact match or the actual type is a subtype of the declared type.
     */
    private boolean isConcreteCompatible(String declaredName, GenericType actualType) {
        // EnumType: match if the enum's simple name equals the declared name
        if (actualType instanceof GenericType.EnumType et) {
            return et.typeName().equals(declaredName);
        }
        // PrecisionDecimal → normalize to Primitive.DECIMAL for subtype checks
        if (actualType instanceof GenericType.PrecisionDecimal) {
            actualType = GenericType.Primitive.DECIMAL;
        }
        if (!(actualType instanceof GenericType.Primitive actualPrim)) {
            // Non-primitive (ClassType, Relation, etc.) — only "Any" accepts these
            return "Any".equals(declaredName);
        }
        try {
            GenericType.Primitive declared = GenericType.Primitive.fromTypeName(declaredName);
            // Any (from empty collections []) is covariant with all types
            if (actualPrim == GenericType.Primitive.ANY) {
                return true;
            }
            return actualPrim == declared || actualPrim.isSubtypeOf(declared);
        } catch (IllegalArgumentException e) {
            return false;
        }
    }



    /**
     * Validates the source parameter AND binds type + multiplicity variables
     * in one pass. Returns {@link Bindings} containing both.
     *
     * <p>This replaces the need for a separate validate() call on the source
     * parameter — unify does both validation and binding.
     *
     * <p>Unifies only the source parameter (param[0]). Sufficient for
     * single-TypeVar functions (filter, sort, exists, map, etc.).
     * For multi-TypeVar functions (join, asOfJoin), use
     * {@link #unify(NativeFunctionDef, List)}.
     */
    protected Bindings unify(NativeFunctionDef def, ExpressionType source) {
        var bindings = new Bindings();
        if (def.params().isEmpty()) {
            throw new PureCompileException(
                    def.name() + "(): signature has no parameters");
        }
        PType.Param param0 = def.params().get(0);

        // Validate + bind type vars
        unifyType(param0.type(), source.type(), bindings, def.name() + "() source");

        // Bind mult vars (e.g., m in cast<T|m>, sort<T|m>)
        if (param0.mult() instanceof Mult.Var v) {
            bindings.mults().put(v.name(), source.multiplicity());
        }

        // Validate multiplicity — skip for Relation types since our system
        // uses [*] for table()/typed() sources, but signatures say [1].
        // The [1] means "one relation container", not "one row".
        // TODO: Fix table()/typed() to return Relation<T>[1], then enable this check.
        if (!(source.type() instanceof GenericType.Relation)) {
            validateMult(param0.mult(), source.multiplicity(), def.name() + "() source");
        }

        return bindings;
    }

    /**
    /**
     * Multi-parameter unification: validates + binds type variables from
     * multiple actual arguments against the signature.
     *
     * <p>Required for multi-TypeVar functions like:
     * <pre>
     * join&lt;T,V&gt;(rel1:Relation&lt;T&gt;, rel2:Relation&lt;V&gt;, ...)
     * </pre>
     * where T must be bound from param[0] and V from param[1].
     *
     * <p>Null entries in {@code actuals} are skipped — callers pass null
     * for params that are not yet compiled (lambdas, enum literals, etc.)
     * and handle those params separately.
     *
     * <p>Also binds multiplicity variables from any parameter that uses
     * a {@code Mult.Var}.
     *
     * @param def     The resolved function signature
     * @param actuals Compiled ExpressionTypes, indexed by param position.
     *                Null entries are skipped.
     * @return Bindings containing both type and multiplicity variable bindings
     */
    protected Bindings unify(NativeFunctionDef def,
                              List<ExpressionType> actuals) {
        var bindings = new Bindings();
        if (def.params().isEmpty()) {
            throw new PureCompileException(
                    def.name() + "(): signature has no parameters");
        }
        for (int i = 0; i < def.params().size() && i < actuals.size(); i++) {
            if (actuals.get(i) == null) continue;
            PType.Param param = def.params().get(i);
            unifyType(param.type(), actuals.get(i).type(), bindings,
                      def.name() + "() param[" + i + "]");
            // Bind mult vars from this param
            if (param.mult() instanceof Mult.Var v) {
                bindings.mults().put(v.name(), actuals.get(i).multiplicity());
            }
        }
        return bindings;
    }

    /**
     * Unifies a compiled parameter's type against its signature type, binding type variables.
     * Called by subclasses (e.g., ScalarChecker) for source-less functions like rowMapper(val, key)
     * where type vars must be bound from the argument types rather than from a source expression.
     */
    protected void unifyParam(PType.Param sigParam, GenericType actualType,
                               Bindings bindings, String context) {
        unifyType(sigParam.type(), actualType, bindings, context);
    }

    /**
     * Recursively matches a PType against a GenericType, populating type variable bindings.
     * No silent skips — every case either binds, validates, or throws.
     */
    private void unifyType(PType expected, GenericType actual,
                           Bindings bindings, String context) {
        switch (expected) {
            case PType.TypeVar v -> {
                // Normalize: PrecisionDecimal → DECIMAL
                GenericType normalized = actual;
                if (normalized instanceof GenericType.PrecisionDecimal) {
                    normalized = GenericType.Primitive.DECIMAL;
                }
                GenericType existing = bindings.get(v.name());
                if (existing != null) {
                    // Check compatibility: normalize existing for comparison too
                    GenericType existNorm = existing;
                    if (existNorm instanceof GenericType.PrecisionDecimal) {
                        existNorm = GenericType.Primitive.DECIMAL;
                    }
                    if (!existNorm.typeName().equals(normalized.typeName())
                            && !"Any".equals(existing.typeName())
                            && !"Any".equals(normalized.typeName())) {
                        throw new PureCompileException(
                                context + ": type variable " + v.name() + " bound to "
                                        + existing.typeName() + " but got " + normalized.typeName());
                    }
                } else {
                    bindings.put(v.name(), normalized);
                }
            }
            case PType.Parameterized p -> {
                if ("Relation".equals(p.rawType())) {
                    if (!(actual instanceof GenericType.Relation rel)) {
                        throw new PureCompileException(
                                context + ": expected Relation, got " + actual.typeName());
                    }
                    // Bind T to Tuple (row schema), NOT to the Relation container.
                    // In Pure/relational algebra, T in Relation<T> is a single tuple's shape,
                    // so lead<T>() returning T[0..1] gives Tuple[0..1] (a row), not Relation[0..1].
                    for (var typeArg : p.typeArgs()) {
                        unifyType(typeArg, new GenericType.Tuple(rel.schema()), bindings, context);
                    }
                } else if ("Function".equals(p.rawType())
                        && actual instanceof GenericType.FunctionReference) {
                    // FunctionReference (bare function name like eq_Any_1__...) is a
                    // Function value — accept without deep type-arg unification.
                    // Analogous to legend-engine's isPackageableElementSubtypeOfFunction.
                } else if (actual instanceof GenericType.Parameterized gp
                        && p.rawType().equals(gp.rawType())) {
                    // Generic parameterized: RowMapper<T,U>, Pair<T,U>, List<T>, etc.
                    // Unify type arguments pairwise
                    for (int i = 0; i < p.typeArgs().size() && i < gp.typeArgs().size(); i++) {
                        unifyType(p.typeArgs().get(i), gp.typeArgs().get(i), bindings, context);
                    }
                } else {
                    throw new PureCompileException(
                            context + ": expected " + p.rawType() + ", got " + actual.typeName());
                }
            }
            case PType.Concrete c -> {
                GenericType g = c.toGenericType();
                if (g == null) {
                    // Non-primitive signature type (DurationUnit, JoinKind, etc.)
                    // — validate that the actual is an EnumType with matching FQN
                    if (actual instanceof GenericType.EnumType et) {
                        if (!et.qualifiedName().equals(c.name())) {
                            throw new PureCompileException(
                                    context + ": expected " + c.name() + ", got " + et.qualifiedName());
                        }
                    } else {
                        throw new PureCompileException(
                                context + ": expected " + c.name() + ", got " + actual.typeName());
                    }
                    return;
                }
                // Use subtype hierarchy: Integer is subtype of Number, Number of Any, etc.
                // Any is the top type — accepts everything
                if (g == GenericType.Primitive.ANY) {
                    return; // Any accepts all types
                }
                // Normalize actual AND signature type:
                //   PrecisionDecimal → DECIMAL (for both)
                GenericType gNorm = g;
                if (gNorm instanceof GenericType.PrecisionDecimal) {
                    gNorm = GenericType.Primitive.DECIMAL;
                }
                GenericType norm = actual;
                if (norm instanceof GenericType.PrecisionDecimal) {
                    norm = GenericType.Primitive.DECIMAL;
                }
                if (gNorm instanceof GenericType.Primitive expectedPrim
                        && norm instanceof GenericType.Primitive actualPrim) {
                    // Any (from empty collections []) is covariant with all types
                    if (actualPrim == GenericType.Primitive.ANY) {
                        return;
                    }
                    if (!actualPrim.isSubtypeOf(expectedPrim)) {
                        throw new PureCompileException(
                                context + ": expected " + c.name() + ", got " + actual.typeName());
                    }
                } else if (!gNorm.typeName().equals(norm.typeName())) {
                    throw new PureCompileException(
                            context + ": expected " + c.name() + ", got " + actual.typeName());
                }
            }
            case PType.SchemaAlgebra sa -> {
                // Schema algebra (T+V, T-Z) doesn't appear during source unification.
                // If encountered (e.g., in a FunctionType param), skip — the algebra
                // only matters during return-type resolution via resolve().
            }
            case PType.FunctionType ft -> throw new PureCompileException(
                    context + ": FunctionType should not appear in source unification");
            case PType.RelationTypeVar rtv -> throw new PureCompileException(
                    context + ": RelationTypeVar should not appear in source unification");
        }
    }

    // ========== Type resolution ==========

    /**
     * Resolves a PType to a GenericType using type variable bindings.
     * Every case either resolves or throws — no null returns.
     */
    protected GenericType resolve(PType type, Bindings bindings, String context) {
        return switch (type) {
            case PType.TypeVar v -> {
                GenericType resolved = bindings.get(v.name());
                if (resolved == null) {
                    throw new PureCompileException(
                            context + ": unbound type variable " + v.name());
                }
                yield resolved;
            }
            case PType.Concrete c -> {
                GenericType g = c.toGenericType();
                if (g == null) {
                    // Non-primitive: look up against model as enum or class
                    var modelCtx = env.modelContext();
                    if (modelCtx != null && modelCtx.findEnum(c.name()).isPresent()) {
                        yield new GenericType.EnumType(c.name());
                    }
                    if (modelCtx != null) {
                        var classOpt = modelCtx.findClass(c.name());
                        if (classOpt.isPresent()) {
                            yield new GenericType.ClassType(classOpt.get().qualifiedName());
                        }
                    }
                    // Meta-model types that map to String in SQL context
                    if ("Type".equals(c.name())) {
                        yield GenericType.Primitive.STRING;
                    }
                    throw new PureCompileException(
                            context + ": unresolvable concrete type: " + c.name());
                }
                yield g;
            }
            case PType.Parameterized p -> {
                if ("Relation".equals(p.rawType()) && !p.typeArgs().isEmpty()) {
                    GenericType inner = resolve(p.typeArgs().get(0), bindings, context);
                    // Reconstruct Relation from Tuple: Relation<T> where T=Tuple(schema)
                    // → GenericType.Relation(schema). Functions like filter() return Relation<T>[1].
                    if (inner instanceof GenericType.Tuple t) {
                        yield new GenericType.Relation(t.schema());
                    }
                    yield inner; // fallback: already a Relation from direct binding
                }
                // Generic parameterized: RowMapper<T,U>, Pair<T,U>, List<T>, etc.
                List<GenericType> resolvedArgs = p.typeArgs().stream()
                        .map(a -> resolve(a, bindings, context)).toList();
                yield new GenericType.Parameterized(p.rawType(), resolvedArgs);
            }
            case PType.SchemaAlgebra sa -> {
                GenericType left = resolve(sa.left(), bindings, context);
                GenericType right = resolve(sa.right(), bindings, context);
                yield resolveSchemaAlgebra(left, right, sa.op(), context);
            }
            case PType.FunctionType ft -> throw new PureCompileException(
                    context + ": cannot resolve FunctionType to GenericType");
            case PType.RelationTypeVar rtv -> throw new PureCompileException(
                    context + ": cannot resolve RelationTypeVar to GenericType");
        };
    }

    /**
     * Resolves a SchemaAlgebra node to a {@link GenericType.Tuple} by performing
     * the schema operation (Union, Difference) on the resolved left/right Tuples.
     *
     * <p>Operations supported:
     * <ul>
     *   <li>{@code Union (T+V)} — merges all columns from both schemas</li>
     *   <li>{@code Difference (T-Z)} — removes Z's column names from T's schema</li>
     * </ul>
     *
     * <p>The result is always a {@code Tuple} — callers like the {@code Relation<T+V>}
     * case in {@link #resolve} unwrap it back into a {@code Relation}.
     */
    private GenericType resolveSchemaAlgebra(GenericType left, GenericType right,
                                            PType.SchemaAlgebra.OpType op, String context) {
        GenericType.Relation.Schema leftSchema = extractSchema(left, context + " (left)");
        GenericType.Relation.Schema rightSchema = extractSchema(right, context + " (right)");

        return switch (op) {
            case Union -> new GenericType.Tuple(leftSchema.merge(rightSchema));
            case Difference -> new GenericType.Tuple(leftSchema.withoutColumns(
                    rightSchema.columns().keySet()));
            case Subset, Equal -> throw new PureCompileException(
                    context + ": schema algebra op " + op + " not yet supported in resolution");
        };
    }

    /**
     * Extracts the column schema from a resolved GenericType.
     * Handles both Tuple (row type from type-var binding) and Relation.
     */
    private GenericType.Relation.Schema extractSchema(GenericType type, String context) {
        return switch (type) {
            case GenericType.Tuple t -> t.schema();
            case GenericType.Relation r -> r.schema();
            default -> throw new PureCompileException(
                    context + ": expected Tuple or Relation schema, got " + type.typeName());
        };
    }

    /**
     * Resolves a Mult to a Multiplicity.
     * Fixed multiplicities resolve directly. Var multiplicities default to MANY.
     */
    protected Multiplicity resolveMult(Mult mult, String context) {
        return resolveMult(mult, Map.of(), context);
    }

    /**
     * Resolves a Mult to a Multiplicity with mult-var bindings.
     * Fixed multiplicities resolve directly. Var multiplicities are looked up
     * in the bindings map; if unbound, defaults to MANY.
     *
     * <p>Used by {@code cast<T|m>(source:Any[m]):T[m]} where {@code m} must
     * preserve the source's actual multiplicity.
     */
    protected Multiplicity resolveMult(Mult mult, Map<String, Multiplicity> multBindings,
                                       String context) {
        return switch (mult) {
            case Mult.Fixed f -> f.value();
            case Mult.Var v -> {
                Multiplicity bound = multBindings.get(v.name());
                if (bound == null) {
                    throw new PureCompileException(
                            context + ": unbound multiplicity variable '" + v.name()
                            + "' — caller must bind via unifyMultVars()");
                }
                yield bound;
            }
        };
    }

    /**
     * Constructs the output ExpressionType from the signature's return type
     * + bindings (both type-var and mult-var).
     *
     * <p>Mult-var resolution is automatic — if the return type uses a
     * multiplicity variable (e.g., {@code T[m]}), the binding populated
     * by {@link #unify} is used. No caller action needed.
     */
    protected ExpressionType resolveOutput(NativeFunctionDef def,
                                           Bindings bindings, String context) {
        GenericType returnType = resolve(def.returnType(), bindings, context + " return type");
        Multiplicity returnMult = resolveMult(def.returnMult(), bindings.mults(),
                context + " return mult");
        return new ExpressionType(returnType, returnMult);
    }

    /**
     * Binds a lambda parameter in the CompilationContext based on the resolved type.
     * Dispatches between:
     * - Relation: row binding (withRelationType + mapping)
     * - ClassType: class instance binding (withLambdaParam + mapping)
     * - Scalar: simple value binding (withLambdaParam)
     */
    protected TypeChecker.CompilationContext bindLambdaParam(
            TypeChecker.CompilationContext ctx, String paramName,
            GenericType resolvedType, TypeInfo source) {
        if (resolvedType == null) {
            throw new PureCompileException(
                    "Cannot bind lambda param '" + paramName + "': resolved type is null");
        }
        if (resolvedType instanceof GenericType.Relation rel) {
            // Relation row: bind schema columns for property access
            return ctx.withRelationType(paramName, rel.schema());
        } else if (resolvedType instanceof GenericType.Tuple) {
            // Tuple = T in Relation<T> = row schema type (our RelationType).
            // Bind as lambdaParam so compileVariable returns Tuple type,
            // preserving type identity for unification (e.g., rowNumber<T>(rel, row:T)).
            // Column property access ($r.id) is handled by compileProperty.
            return ctx.withLambdaParam(paramName, resolvedType);
        } else if (resolvedType instanceof GenericType.ClassType) {
            // Class instance: bind type for property resolution via modelContext
            return ctx.withLambdaParam(paramName, resolvedType);
        } else {
            return ctx.withLambdaParam(paramName, resolvedType);
        }
    }

    // ========== Multiplicity validation ==========

    /**
     * Validates actual multiplicity against expected.
     * Only rejects [*] → [1] (can't squeeze many into single).
     * Accepts [1] → [*] (a single value is a valid collection).
     */
    private void validateMult(Mult expected, Multiplicity actual, String context) {
        switch (expected) {
            case Mult.Fixed f -> {
                Multiplicity exp = f.value();
                // Only reject: expected [1] but got [*]
                if (exp.equals(Multiplicity.ONE) && actual.isMany()) {
                    throw new PureCompileException(
                            context + ": expected multiplicity [1], got [*]");
                }
                // [*] accepts [1] — a single value is a valid collection
            }
            // Multiplicity variable — skip validation (can't check against unknown)
            case Mult.Var v -> { /* pass */ }
        }
    }

    // ========== Lambda helpers ==========

    /**
     * Extracts the FunctionType from a lambda parameter definition.
     * Handles both {@code Function<{...}>} (filter, map, etc.) and
     * {@code FuncColSpecArray<{...},T>} (project).
     * Must always succeed — malformed signature throws.
     */
    protected PType.FunctionType extractFunctionType(PType.Param lambdaDef) {
        if (lambdaDef.type() instanceof PType.Parameterized fp
                && !fp.typeArgs().isEmpty()
                && fp.typeArgs().get(0) instanceof PType.FunctionType ft) {
            // Both Function<{T[1]->Boolean[1]}> and FuncColSpecArray<{C[1]->Any[*]},T>
            // have the FunctionType as their first type argument
            if ("Function".equals(fp.rawType())
                    || "FuncColSpec".equals(fp.rawType())
                    || "AggColSpec".equals(fp.rawType())
                    || "FuncColSpecArray".equals(fp.rawType())
                    || "AggColSpecArray".equals(fp.rawType())) {
                return ft;
            }
        }
        throw new PureCompileException(
                "Signature malformed: expected Function<{...}> or FuncColSpecArray<{...}>, got "
                        + lambdaDef.type());
    }

    /**
     * Compiles all lambda body statements; returns the TypeInfo of the last.
     */
    protected TypeInfo compileLambdaBody(LambdaFunction lambda, TypeChecker.CompilationContext ctx) {
        TypeInfo last = null;
        for (var stmt : lambda.body()) {
            last = env.compileExpr(stmt, ctx);
        }
        if (last == null) {
            throw new PureCompileException("Lambda body produced no type");
        }
        return last;
    }

    /**
     * Validates lambda return type AND multiplicity against a FunctionType.
     * Uses unifyType internally for type checking and validateMult for multiplicity.
     */
    protected void validateLambdaReturn(TypeInfo bodyType, PType.FunctionType ft,
                                        Bindings bindings, String funcName) {
        String context = funcName + "() predicate return";

        // Validate return type — use resolve to get expected GenericType from bindings
        GenericType expectedType = resolve(ft.returnType(), bindings, context);
        if (!expectedType.typeName().equals(bodyType.type().typeName())) {
            throw new PureCompileException(
                    context + ": expected " + expectedType.typeName()
                            + ", got " + bodyType.type().typeName());
        }

        // Validate return multiplicity
        validateMult(ft.returnMult(), bodyType.expressionType().multiplicity(), context);
    }

    /**
     * Returns true if a signature param expects a lambda ({@code Function<{...}>}).
     * Useful for generic checkers that iterate params and need to distinguish
     * lambda args from scalar args.
     */
    protected static boolean isLambdaParam(PType.Param sigParam) {
        return sigParam.type() instanceof PType.Parameterized p
                && "Function".equals(p.rawType());
    }

    /**
     * Full lambda argument compilation: extract FunctionType from signature,
     * bind lambda param, compile body, optionally validate return type.
     *
     * <p>Skips return validation when the return type is an unbound TypeVar
     * (e.g., V in {@code map<T,V>}) since the return type is resolved
     * FROM the body, not checked against it.
     *
     * @return The TypeInfo of the lambda body (last statement)
     */
    protected TypeInfo compileLambdaArg(LambdaFunction lambda, PType.Param sigParam,
                                        Bindings bindings, TypeInfo source,
                                        TypeChecker.CompilationContext ctx, String funcName) {
        PType.FunctionType ft = extractFunctionType(sigParam);

        // Bind lambda params (all of them, not just the first)
        TypeChecker.CompilationContext lambdaCtx = ctx;
        int paramCount = Math.min(lambda.parameters().size(), ft.paramTypes().size());
        for (int pi = 0; pi < paramCount; pi++) {
            String paramName = lambda.parameters().get(pi).name();
            GenericType resolvedParamType = resolve(ft.paramTypes().get(pi).type(), bindings,
                    funcName + "() lambda param " + pi);
            lambdaCtx = bindLambdaParam(lambdaCtx, paramName, resolvedParamType, source);
        }

        // Compile body
        if (lambda.body().isEmpty()) {
            return null;
        }
        TypeInfo bodyResult = compileLambdaBody(lambda, lambdaCtx);

        // Bind unbound return TypeVar from body result (e.g., V in map<T,V>)
        // This lets resolveOutput work without special-case logic downstream.
        if (ft.returnType() instanceof PType.TypeVar tv && !bindings.containsKey(tv.name())) {
            if (bodyResult.type() != null) {
                bindings.put(tv.name(), bodyResult.type());
            }
        }

        // Validate return type — skip for TypeVars we just bound from the body
        // (they ARE the body result, so validation is tautological)
        if (!(ft.returnType() instanceof PType.TypeVar tv2) || !bindings.containsKey(tv2.name())
                || bindings.get(tv2.name()) != bodyResult.type()) {
            validateLambdaReturn(bodyResult, ft, bindings, funcName);
        }

        return bodyResult;
    }

    // ========== Shared utilities ==========

    /**
     * Unwraps a cast() wrapper to get the inner AppliedFunction.
     * E.g., cast($y→plus(), @Integer) → the plus() AppliedFunction.
     * Returns the body as-is if no cast wrapper.
     * Shared by GroupByChecker and ExtendChecker for fn2 body processing.
     */
    protected AppliedFunction unwrapCast(ValueSpecification body, String funcContext) {
        if (body instanceof AppliedFunction af) {
            if ("cast".equals(simpleName(af.function()))) {
                if (!af.parameters().isEmpty()
                        && af.parameters().get(0) instanceof AppliedFunction inner) {
                    return inner;
                }
                return af;
            }
            return af;
        }
        throw new PureCompileException(
                funcContext + ": fn2 body must be a function call, got: "
                        + body.getClass().getSimpleName());
    }

    /**
     * Extracts cast target as GenericType from a cast() wrapper.
     * Returns null if body is not wrapped in cast().
     * Shared by GroupByChecker and ExtendChecker.
     */
    protected GenericType extractCastGenericType(ValueSpecification body) {
        if (body instanceof AppliedFunction af
                && "cast".equals(simpleName(af.function()))) {
            for (var p : af.parameters()) {
                if (p instanceof GenericTypeInstance gti) {
                    return gti.resolvedType();
                }
            }
        }
        return null;
    }

    /** Extracts simple function name from qualified name (e.g. "meta::pure::...::sort" → "sort"). */
    protected static String simpleName(String qualifiedName) {
        return com.gs.legend.model.SymbolTable.extractSimpleName(qualifiedName);
    }

    /**
     * Extracts a column name from a ValueSpecification.
     * Handles ColSpec (from ~col syntax) and AppliedProperty (from $p.col syntax).
     */
    protected static String extractColumnName(ValueSpecification vs) {
        if (vs instanceof com.gs.legend.ast.ClassInstance(String type, Object value)
                && value instanceof com.gs.legend.ast.ColSpec cs) {
            return cs.name();
        }
        if (vs instanceof com.gs.legend.ast.AppliedProperty ap) {
            return ap.property();
        }
        throw new PureCompileException(
                "Cannot extract column name from " + vs.getClass().getSimpleName());
    }

    /**
     * Extracts column names from a ValueSpecification.
     * Handles both single ColSpec (~col) and ColSpecArray (~[col1, col2]).
     */
    protected static java.util.List<String> extractColumnNames(ValueSpecification vs) {
        if (vs instanceof com.gs.legend.ast.ClassInstance(String type, Object value)
                && value instanceof com.gs.legend.ast.ColSpecArray(java.util.List<com.gs.legend.ast.ColSpec> specs)) {
            return specs.stream().map(com.gs.legend.ast.ColSpec::name).toList();
        }
        if (vs instanceof com.gs.legend.ast.PureCollection(java.util.List<ValueSpecification> values)) {
            return values.stream().map(AbstractChecker::extractColumnName).toList();
        }
        return java.util.List.of(extractColumnName(vs));
    }

    // ========== Class hierarchy utilities ==========

    /**
     * Convenience: find a class in the model context.
     * Returns empty if modelContext is null or class not found.
     */
    protected java.util.Optional<com.gs.legend.model.m3.PureClass> findClass(String className) {
        var mc = env.modelContext();
        if (mc == null) return java.util.Optional.empty();
        return mc.findClass(className);
    }

    /**
     * Convenience: LCA via this checker's model context.
     * Delegates to {@link com.gs.legend.model.ModelContext#findLowestCommonAncestor}.
     */
    protected java.util.Optional<com.gs.legend.model.m3.PureClass> findLowestCommonAncestor(
            String className1, String className2) {
        var mc = env.modelContext();
        if (mc == null) return java.util.Optional.empty();
        return mc.findLowestCommonAncestor(className1, className2);
    }

    /**
     * Resolves the LCA of two class-typed sources into a TypeInfo.
     *
     * <p>Given left and right TypeInfos with ClassType element types, finds their
     * lowest common ancestor and builds a relational schema from its properties.
     * Returns null if the element types are not ClassTypes or no LCA exists.
     *
     * <p>Reusable by any checker that combines two class sources (concatenate, join, etc.).
     */
    protected TypeInfo resolveClassLCA(TypeInfo left, TypeInfo right) {
        com.gs.legend.plan.GenericType leftElem = left.type();
        com.gs.legend.plan.GenericType rightElem = right.type();

        if (leftElem instanceof com.gs.legend.plan.GenericType.ClassType(String leftClass)
                && rightElem instanceof com.gs.legend.plan.GenericType.ClassType(String rightClass)) {
            var lcaOpt = findLowestCommonAncestor(leftClass, rightClass);
            if (lcaOpt.isPresent()) {
                var lcaClass = lcaOpt.get();
                var lcaCols = new java.util.LinkedHashMap<String, com.gs.legend.plan.GenericType>();
                for (var prop : lcaClass.allProperties(env.modelContext())) {
                    lcaCols.put(prop.name(),
                            com.gs.legend.plan.GenericType.fromTypeRef(prop.typeRef()));
                }
                var lcaRelType = com.gs.legend.plan.GenericType.Relation.Schema.withoutPivot(lcaCols);
                return TypeInfo.builder()
                        .expressionType(ExpressionType.many(
                                new com.gs.legend.plan.GenericType.Relation(lcaRelType)))
                        .build();
            }
        }
        return null;
    }
}
