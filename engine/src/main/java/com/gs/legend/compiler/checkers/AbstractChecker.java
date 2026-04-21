package com.gs.legend.compiler.checkers;

import com.gs.legend.ast.*;
import com.gs.legend.compiler.*;
import com.gs.legend.compiler.typed.TypedSpec;
import com.gs.legend.model.m3.Multiplicity;
import com.gs.legend.model.m3.Primitive;
import com.gs.legend.model.m3.Type;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Base class for function type-checkers.
 *
 * <p>Provides signature-driven validation infrastructure built on the sealed
 * {@link Type} hierarchy:
 * <ul>
 *   <li>{@link #unify} — validate + bind type variables from signature against actuals</li>
 *   <li>{@link #resolve} — resolve a signature {@link Type} using bindings</li>
 *   <li>{@link #resolveMult} — resolve a {@link Multiplicity} using mult-var bindings</li>
 *   <li>{@link #bindLambdaParam} — bind lambda param in CompilationContext</li>
 *   <li>{@link #resolveOutput} — construct output {@link ExpressionType} from a signature return type</li>
 * </ul>
 *
 * <p>Dispatch on {@link Type.Parameterized#rawType()} is centralized in
 * {@link #structuralMatch}, {@link #unifyType}, and {@link #resolve}: these are
 * the only sites that know about signature-layer pseudo-types ({@code Relation},
 * {@code ColSpec}, {@code _Window}, etc.) by name. A future refactor may replace
 * the string dispatch with a typed {@code rawType} enum once the FQN migration
 * (item 5 in the audit) lands.
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
        private final Map<String, Type> types = new LinkedHashMap<>();
        private final Map<String, Multiplicity> mults = new LinkedHashMap<>();

        public Map<String, Type> types() { return types; }
        public Map<String, Multiplicity> mults() { return mults; }

        // Delegates to type-var map for zero-churn backward compat
        public Type get(String key) { return types.get(key); }
        public void put(String key, Type value) { types.put(key, value); }
        public boolean containsKey(String key) { return types.containsKey(key); }
        public Type getOrDefault(String key, Type defaultValue) {
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
        var defs = BuiltinRegistry.instance().resolve(funcName);
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
                .filter(d -> matchesStructurally(d, params, source, compiledTypes))
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

            Type.Parameter sigParam = def.params().get(paramIdx);
            int typeScore = scoreParamType(sigParam.type(), actual.type());
            if (typeScore < 0) return -1; // incompatible type — eliminate
            int multScore = scoreParamMult(sigParam.multiplicity(), actual.multiplicity());
            if (multScore < 0) return -1; // incompatible multiplicity — eliminate
            totalScore += typeScore * 10 + multScore;
        }
        return totalScore;
    }

    /**
     * Scores a single param: declared type vs actual compiled type.
     * Returns 2 for exact, 1 for subtype, 0 for TypeVar/Any, -1 for incompatible.
     *
     * <p>Native signatures produce fully resolved {@link Type} values at parse time
     * (primitives as {@link Primitive}, enums as {@link Type.EnumType}, type params
     * as {@link Type.TypeVar}); this method dispatches on those variants directly.
     * PrecisionDecimal normalization is done on both sides so that a
     * {@code PrecisionDecimal(38,2)} actual scores as exact against a declared
     * {@code Decimal}.
     */
    private int scoreParamType(Type declaredType, Type actualType) {
        // Normalize PrecisionDecimal → Primitive.DECIMAL on both sides.
        if (actualType instanceof Type.PrecisionDecimal) {
            actualType = Primitive.DECIMAL;
        }
        if (declaredType instanceof Type.PrecisionDecimal) {
            declaredType = Primitive.DECIMAL;
        }
        if (declaredType instanceof Primitive declaredPrim) {
            if (declaredPrim == Primitive.ANY) return 0;     // Any accepts everything, lowest priority
            if (!(actualType instanceof Primitive actualPrim)) return -1;
            if (actualPrim == declaredPrim) return 2;        // exact match
            if (actualPrim.isSubtypeOf(declaredPrim)) return 1;   // subtype match
            return -1;
        }
        if (declaredType instanceof Type.EnumType declaredEnum) {
            return actualType instanceof Type.EnumType actualEnum
                    && actualEnum.qualifiedName().equals(declaredEnum.qualifiedName())
                    ? 2 : -1;
        }
        // TypeVar, Parameterized, FunctionType, SchemaAlgebra, etc. —
        // always applicable (structural matching handled elsewhere), lowest priority
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
    private int scoreParamMult(Multiplicity declared, Multiplicity actual) {
        if (declared instanceof Multiplicity.Var) {
            return 0; // multiplicity variable — always applicable, lowest priority
        }
        if (declared instanceof Multiplicity.Bounded decl) {
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
     * Uses compiled types for Primitive/EnumType matching when available, falls back
     * to AST-shape matching otherwise.
     * Multiplicity: [0..1] actual never matches [1] declared.
     */
    private boolean matchesStructurally(NativeFunctionDef def,
                                        List<ValueSpecification> params,
                                        TypeInfo source,
                                        Map<Integer, ExpressionType> compiledTypes) {
        for (int i = 0; i < def.params().size() && i < params.size(); i++) {
            Type.Parameter sigParam = def.params().get(i);
            Type expected = sigParam.type();
            ValueSpecification actual = params.get(i);
            ExpressionType compiledExpr = compiledTypes.get(i);
            Type compiledType = compiledExpr != null ? compiledExpr.type() : null;
            boolean sm = structuralMatch(expected, actual, source, i == 0, compiledType);
            if (!sm) {
                return false;
            }
            // Multiplicity check: if we have compiled multiplicity, verify it
            // fits the declared multiplicity. [0..1] must NOT match [1].
            if (compiledExpr != null && sigParam.multiplicity() instanceof Multiplicity.Bounded b) {
                if (!subsumes(b, compiledExpr.multiplicity())) {
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * Matches a single Type against an AST node + optional compiled type.
     *
     * <p>For Primitive/EnumType declared types: if a compiled type is available,
     * uses subtype checking for precise matching. Otherwise falls back to AST-shape
     * filtering (reject lambdas and class instances).
     */
    private boolean structuralMatch(Type expected, ValueSpecification actual,
                                    TypeInfo source, boolean isSource,
                                    Type compiledType) {
        // PureCollection wraps multiple elements (e.g., [ascending(~id), ascending(~name)])
        // For Primitive/EnumType declared types with a compiled collection type, use that directly —
        // recursing into elements loses the compiled type context.
        if (actual instanceof PureCollection(java.util.List<ValueSpecification> elements)
                && !elements.isEmpty()) {
            if (compiledType != null && (expected instanceof Primitive || expected instanceof Type.EnumType)) {
                // Compiled type is definitive — use subtype checking
                return primitiveOrEnumCompatible(expected, compiledType);
            }
            return elements.stream().allMatch(e -> structuralMatch(expected, e, source, false, null));
        }
        com.gs.legend.model.m3.LClass expectedLc = Type.asLClass(expected);
        if (expectedLc != null) {
            return switch (expectedLc) {
                case RELATION -> isSource
                        ? (source != null && source.isRelational())
                        : (compiledType instanceof Type.Relation);
                case COL_SPEC
                        -> actual instanceof com.gs.legend.ast.ColSpec;
                case FUNC_COL_SPEC
                        -> actual instanceof com.gs.legend.ast.ColSpec cs && cs.function2() == null;
                case AGG_COL_SPEC
                        -> actual instanceof com.gs.legend.ast.ColSpec cs && cs.function2() != null;
                case COL_SPEC_ARRAY
                        -> actual instanceof com.gs.legend.ast.ColSpecArray;
                case FUNC_COL_SPEC_ARRAY
                        -> actual instanceof com.gs.legend.ast.ColSpecArray csa
                            && csa.colSpecs().stream().allMatch(s -> s.function2() == null);
                case AGG_COL_SPEC_ARRAY
                        -> actual instanceof com.gs.legend.ast.ColSpecArray csa
                            && csa.colSpecs().stream().allMatch(s -> s.function2() != null);
                case FUNCTION -> actual instanceof LambdaFunction
                    || actual instanceof PackageableElementPtr; // function reference (e.g., eq_Any_1__Any_1__Boolean_1_)
                case SORT_INFO -> actual instanceof AppliedFunction;
                case WINDOW -> actual instanceof AppliedFunction af
                    && !"traverse".equals(simpleName(af.function()));
                case ROWS, RANGE -> actual instanceof AppliedFunction;
                case TRAVERSAL -> actual instanceof AppliedFunction af
                    && "traverse".equals(simpleName(af.function()));
                // Other catalog classes (Pair, List, Map, Frame variants, RowMapper, TreeNode,
                // ValueHolder, Type, Class, FunctionDefinition, RootGraphFetchTree) — fall through
                // to user-parameterized-matching logic below.
                default -> isSource && source != null
                        && Type.asLClass(source.type()) == expectedLc;
            };
        }
        if (expected instanceof Type.TypeVar) {
            // TypeVar (T, V, etc.) matches anything EXCEPT Relation sources
            return !isSource || source == null || !source.isRelational();
        }
        if (expected instanceof Primitive p) {
            // Any is the top type — accepts everything
            if (p == Primitive.ANY) return true;
            // Reject lambdas, column instances, and struct literals for specific primitives —
            // they're structural, not scalar values.
            if (actual instanceof LambdaFunction
                    || actual instanceof com.gs.legend.ast.ColumnInstance
                    || actual instanceof com.gs.legend.ast.NewInstance) {
                return false;
            }
            // Compiled type is needed to subtype-check against a specific primitive. All callers
            // that need primitive matching go through ScalarChecker which provides compiled types.
            return compiledType != null && primitiveOrEnumCompatible(p, compiledType);
        }
        if (expected instanceof Type.PrecisionDecimal) {
            // Mirrors the "bare Decimal = DEFAULT_DECIMAL" convention applied by
            // {@link PureNativeSignatureParser}: signatures written as {@code Decimal[1]}
            // land here as PrecisionDecimal(38,18). Treat the declared precision as
            // {@link Primitive#DECIMAL} for matching — any Decimal actual fits.
            if (actual instanceof LambdaFunction
                    || actual instanceof com.gs.legend.ast.ColumnInstance
                    || actual instanceof com.gs.legend.ast.NewInstance) return false;
            return compiledType != null && primitiveOrEnumCompatible(Primitive.DECIMAL, compiledType);
        }
        if (expected instanceof Type.EnumType e) {
            if (actual instanceof LambdaFunction
                    || actual instanceof com.gs.legend.ast.ColumnInstance
                    || actual instanceof com.gs.legend.ast.NewInstance) return false;
            return compiledType != null && primitiveOrEnumCompatible(e, compiledType);
        }
        return false;
    }


    /**
     * Checks whether a compiled actual type fits a declared {@link Primitive} or
     * {@link Type.EnumType}. Fits means exact match or subtype in the primitive
     * lattice (enums require strict FQN equality; enums don't subtype).
     *
     * <p>Mirrors the normalization in {@link #scoreParamType}: any {@link Type.PrecisionDecimal}
     * (on either side) flattens to {@link Primitive#DECIMAL} so precision differences don't
     * spuriously reject a {@code PrecisionDecimal(38,1)} actual against a declared {@code Decimal}
     * parameter.
     */
    private boolean primitiveOrEnumCompatible(Type declared, Type actualType) {
        if (actualType instanceof Type.PrecisionDecimal) {
            actualType = Primitive.DECIMAL;
        }
        if (declared instanceof Type.PrecisionDecimal) {
            declared = Primitive.DECIMAL;
        }
        // EnumType: exact FQN match required
        if (declared instanceof Type.EnumType de) {
            return actualType instanceof Type.EnumType ae
                    && ae.qualifiedName().equals(de.qualifiedName());
        }
        if (declared == Primitive.ANY) return true;
        // Any (from empty collections []) is covariant with all types.
        if (actualType == Primitive.ANY) return true;
        return actualType.equals(declared) || actualType.isSubtypeOf(declared);
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
        Type.Parameter param0 = def.params().get(0);

        // Validate + bind type vars
        unifyType(param0.type(), source.type(), bindings, def.name() + "() source");

        // Bind mult vars (e.g., m in cast<T|m>, sort<T|m>)
        if (param0.multiplicity() instanceof Multiplicity.Var v) {
            bindings.mults().put(v.name(), source.multiplicity());
        }

        // Validate multiplicity — skip for Relation types since our system
        // uses [*] for table()/typed() sources, but signatures say [1].
        // The [1] means "one relation container", not "one row".
        // TODO: Fix table()/typed() to return Relation<T>[1], then enable this check.
        if (!(source.type() instanceof Type.Relation)) {
            validateMult(param0.multiplicity(), source.multiplicity(), def.name() + "() source");
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
            Type.Parameter param = def.params().get(i);
            unifyType(param.type(), actuals.get(i).type(), bindings,
                      def.name() + "() param[" + i + "]");
            // Bind mult vars from this param
            if (param.multiplicity() instanceof Multiplicity.Var v) {
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
    protected void unifyParam(Type.Parameter sigParam, Type actualType,
                               Bindings bindings, String context) {
        unifyType(sigParam.type(), actualType, bindings, context);
    }

    /**
     * Recursively matches a Type against a Type, populating type variable bindings.
     * No silent skips — every case either binds, validates, or throws.
     */
    private void unifyType(Type expected, Type actual,
                           Bindings bindings, String context) {
        switch (expected) {
            case Type.TypeVar v -> {
                // Bind the actual type unchanged so precision-carrying types
                // (PrecisionDecimal(p, s)) flow through to the return type —
                // e.g., plus<T>(values:T[*]):T[1] applied to PrecisionDecimal(38,18)
                // values returns PrecisionDecimal(38,18), not a bare DECIMAL.
                //
                // Compatibility check between multiple bindings of the same T
                // normalizes PrecisionDecimal → DECIMAL so two different-precision
                // decimals aren't considered a conflict.
                Type existing = bindings.get(v.name());
                if (existing != null) {
                    Type existNorm = existing instanceof Type.PrecisionDecimal ? Primitive.DECIMAL : existing;
                    Type actualNorm = actual instanceof Type.PrecisionDecimal ? Primitive.DECIMAL : actual;
                    if (!existNorm.equals(actualNorm)
                            && existing != Primitive.ANY
                            && actual != Primitive.ANY) {
                        throw new PureCompileException(
                                context + ": type variable " + v.name() + " bound to "
                                        + existing.typeName() + " but got " + actual.typeName());
                    }
                } else {
                    bindings.put(v.name(), actual);
                }
            }
            case Primitive expectedPrim -> {
                if (expectedPrim == Primitive.ANY) return; // Any accepts all types
                // Normalize PrecisionDecimal → DECIMAL on actual.
                Type norm = actual instanceof Type.PrecisionDecimal ? Primitive.DECIMAL : actual;
                if (norm instanceof Primitive actualPrim) {
                    if (actualPrim == Primitive.ANY) return; // empty-collection covariance
                    if (!actualPrim.isSubtypeOf(expectedPrim)) {
                        throw new PureCompileException(
                                context + ": expected " + expectedPrim.typeName() + ", got " + actual.typeName());
                    }
                } else if (norm != expectedPrim) {
                    throw new PureCompileException(
                            context + ": expected " + expectedPrim.typeName() + ", got " + actual.typeName());
                }
            }
            case Type.EnumType expectedEnum -> {
                if (!(actual instanceof Type.EnumType actualEnum)
                        || !actualEnum.qualifiedName().equals(expectedEnum.qualifiedName())) {
                    throw new PureCompileException(
                            context + ": expected " + expectedEnum.qualifiedName() + ", got " + actual.typeName());
                }
            }
            case Type.ClassType ct -> {
                // Class-typed signature parameter — accept any class (model-level match done elsewhere)
                if (!(actual instanceof Type.ClassType)) {
                    throw new PureCompileException(
                            context + ": expected class " + ct.qualifiedName() + ", got " + actual.typeName());
                }
            }
            case Type.NameRef nr -> throw new PureCompileException(
                    context + ": unresolved NameRef '" + nr.qualifiedName() + "' in signature — native signatures should be fully resolved");
            case Type.PrecisionDecimal pd -> {
                // Declared PrecisionDecimal in a signature — treat as DECIMAL for unification
                Type norm = actual instanceof Type.PrecisionDecimal ? Primitive.DECIMAL : actual;
                if (!(norm instanceof Primitive p) || !p.isSubtypeOf(Primitive.DECIMAL)) {
                    throw new PureCompileException(
                            context + ": expected Decimal, got " + actual.typeName());
                }
            }
            case Type.SchemaAlgebra sa -> {
                // Schema algebra (T+V, T-Z) doesn't appear during source unification.
                // If encountered (e.g., in a FunctionType param), skip — the algebra
                // only matters during return-type resolution via resolve().
            }
            case Type.FunctionType ft -> throw new PureCompileException(
                    context + ": FunctionType should not appear in source unification");
            case Type.RelationTypeVar rtv -> throw new PureCompileException(
                    context + ": RelationTypeVar should not appear in source unification");
            case Type.Relation r -> {
                // Already-resolved Relation in a signature — just verify actual is Relation
                if (!(actual instanceof Type.Relation)) {
                    throw new PureCompileException(
                            context + ": expected Relation, got " + actual.typeName());
                }
            }
            case Type.Tuple t -> {
                // Already-resolved Tuple — verify actual shape
                if (!(actual instanceof Type.Tuple)) {
                    throw new PureCompileException(
                            context + ": expected Tuple, got " + actual.typeName());
                }
            }
            case Type.FunctionReference fr -> throw new PureCompileException(
                    context + ": FunctionReference should not appear in source unification");
            case Type.GenericType gt -> unifyGenericType(gt, actual, bindings, context);
            case com.gs.legend.model.m3.LClass lc -> unifyLClass(lc, List.of(), actual, bindings, context);
        }
    }

    /**
     * Unifies an expected {@code GenericType(LClass.X, args)} or {@code GenericType(ClassType, args)}
     * against an actual value. Delegates to {@link #unifyLClass} for platform-class rawTypes;
     * for user class rawTypes, asserts the actual is a {@code ClassType} with the same FQN —
     * deeper structural matching (property types, type-arg compatibility) is handled by
     * source-spec unification at the caller level.
     */
    private void unifyGenericType(Type.GenericType gt, Type actual,
                                   Bindings bindings, String context) {
        if (gt.rawType() instanceof com.gs.legend.model.m3.LClass lc) {
            unifyLClass(lc, gt.typeArgs(), actual, bindings, context);
            return;
        }
        // User-class generic: verify the actual is a ClassType with the same FQN. Deep structural
        // matching (property types, type-arg compatibility) happens via source-spec unification
        // at the caller level; here we only assert the class identity matches.
        if (gt.rawType() instanceof Type.ClassType expectedCt
                && actual instanceof Type.ClassType actualCt
                && expectedCt.qualifiedName().equals(actualCt.qualifiedName())) {
            return;
        }
        throw new PureCompileException(
                context + ": expected " + gt.typeName() + ", got " + actual.typeName());
    }

    /**
     * Unifies an expected platform-class (optionally with type args) against an actual value.
     * Handles the class-specific unification rules keyed on the {@link com.gs.legend.model.m3.LClass}
     * identity (Relation, Function, and the general "same rawType — unify args pairwise" case).
     */
    private void unifyLClass(com.gs.legend.model.m3.LClass expectedLc, List<Type> expectedArgs,
                              Type actual, Bindings bindings, String context) {
        if (expectedLc == com.gs.legend.model.m3.LClass.RELATION) {
            if (!(actual instanceof Type.Relation rel)) {
                throw new PureCompileException(
                        context + ": expected Relation, got " + actual.typeName());
            }
            // Bind T to Tuple (row schema), NOT to the Relation container.
            // In Pure/relational algebra, T in Relation<T> is a single tuple's shape,
            // so lead<T>() returning T[0..1] gives Tuple[0..1] (a row), not Relation[0..1].
            for (Type typeArg : expectedArgs) {
                unifyType(typeArg, new Type.Tuple(rel.schema()), bindings, context);
            }
            return;
        }
        if (expectedLc == com.gs.legend.model.m3.LClass.FUNCTION
                && actual instanceof Type.FunctionReference) {
            // FunctionReference (bare function name like eq_Any_1__...) is a
            // Function value — accept without deep type-arg unification.
            // Analogous to legend-engine's isPackageableElementSubtypeOfFunction.
            return;
        }
        // Generic platform class with same rawType on actual: RowMapper<T,U>, Pair<T,U>, etc.
        com.gs.legend.model.m3.LClass actualLc = Type.asLClass(actual);
        if (actualLc == expectedLc) {
            List<Type> actualArgs = actual instanceof Type.GenericType gt ? gt.typeArgs() : List.of();
            // Bare LClass leaf on the actual side (no GenericType wrapper) is a valid zero-arg
            // reference — identity match alone is sufficient. Otherwise, arities must agree;
            // silent truncation would mask signature/argument mismatches (AGENTS.md #4).
            if (!actualArgs.isEmpty() && actualArgs.size() != expectedArgs.size()) {
                throw new PureCompileException(
                        context + ": " + expectedLc.typeName() + " arity mismatch — expected "
                                + expectedArgs.size() + " type argument(s), got " + actualArgs.size());
            }
            for (int i = 0; i < actualArgs.size(); i++) {
                unifyType(expectedArgs.get(i), actualArgs.get(i), bindings, context);
            }
            return;
        }
        throw new PureCompileException(
                context + ": expected " + expectedLc.qualifiedName() + ", got " + actual.typeName());
    }

    // ========== Type resolution ==========

    /**
     * Resolves a Type to a Type using type variable bindings.
     * Every case either resolves or throws — no null returns.
     */
    protected Type resolve(Type type, Bindings bindings, String context) {
        return switch (type) {
            case Type.TypeVar v -> {
                Type resolved = bindings.get(v.name());
                if (resolved == null) {
                    throw new PureCompileException(
                            context + ": unbound type variable " + v.name());
                }
                yield resolved;
            }
            case Primitive p -> p;
            case Type.EnumType e -> e;
            case Type.ClassType c -> c;
            case com.gs.legend.model.m3.LClass lc -> lc;
            case Type.PrecisionDecimal pd -> pd;
            case Type.NameRef nr -> throw new PureCompileException(
                    context + ": unresolved NameRef '" + nr.qualifiedName() + "' during resolution — native signatures should be fully resolved at parse time");
            case Type.GenericType gt -> {
                if (gt.rawType() == com.gs.legend.model.m3.LClass.RELATION && !gt.typeArgs().isEmpty()) {
                    Type inner = resolve(gt.typeArgs().get(0), bindings, context);
                    // Reconstruct Relation from Tuple: Relation<T> where T=Tuple(schema)
                    // → Type.Relation(schema). Functions like filter() return Relation<T>[1].
                    if (inner instanceof Type.Tuple t) {
                        yield new Type.Relation(t.schema());
                    }
                    yield inner; // fallback: already a Relation from direct binding
                }
                // Generic parameterized: RowMapper<T,U>, Pair<T,U>, List<T>, etc. — resolve args.
                List<Type> resolvedArgs = gt.typeArgs().stream()
                        .map(a -> resolve(a, bindings, context)).toList();
                yield new Type.GenericType(gt.rawType(), resolvedArgs);
            }
            case Type.SchemaAlgebra sa -> {
                Type left = resolve(sa.left(), bindings, context);
                Type right = resolve(sa.right(), bindings, context);
                yield resolveSchemaAlgebra(left, right, sa.op(), context);
            }
            case Type.FunctionType ft -> throw new PureCompileException(
                    context + ": cannot resolve FunctionType to Type");
            case Type.RelationTypeVar rtv -> throw new PureCompileException(
                    context + ": cannot resolve RelationTypeVar to Type");
            case Type.Relation r -> r;
            case Type.Tuple t -> t;
            case Type.FunctionReference fr -> fr;
        };
    }

    /**
     * Resolves a SchemaAlgebra node to a {@link Type.Tuple} by performing
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
    private Type resolveSchemaAlgebra(Type left, Type right,
                                            Type.SchemaAlgebra.Op op, String context) {
        Type.Schema leftSchema = extractSchema(left, context + " (left)");
        Type.Schema rightSchema = extractSchema(right, context + " (right)");

        return switch (op) {
            case UNION -> new Type.Tuple(leftSchema.merge(rightSchema));
            case DIFFERENCE -> new Type.Tuple(leftSchema.withoutColumns(
                    rightSchema.columns().keySet()));
            case SUBSET, EQUAL -> throw new PureCompileException(
                    context + ": schema algebra op " + op + " not yet supported in resolution");
        };
    }

    /**
     * Extracts the column schema from a resolved Type.
     * Handles both Tuple (row type from type-var binding) and Relation.
     */
    private Type.Schema extractSchema(Type type, String context) {
        return switch (type) {
            case Type.Tuple t -> t.schema();
            case Type.Relation r -> r.schema();
            default -> throw new PureCompileException(
                    context + ": expected Tuple or Relation schema, got " + type.typeName());
        };
    }

    /**
     * Resolves a Mult to a Multiplicity.
     * Fixed multiplicities resolve directly. Var multiplicities default to MANY.
     */
    protected Multiplicity resolveMult(Multiplicity mult, String context) {
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
    protected Multiplicity resolveMult(Multiplicity mult, Map<String, Multiplicity> multBindings,
                                       String context) {
        return switch (mult) {
            case Multiplicity.Bounded b -> b;
            case Multiplicity.Var v -> {
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
        Type returnType = resolve(def.returnType(), bindings, context + " return type");
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
            Type resolvedType, TypedSpec source) {
        if (resolvedType == null) {
            throw new PureCompileException(
                    "Cannot bind lambda param '" + paramName + "': resolved type is null");
        }
        if (resolvedType instanceof Type.Relation rel) {
            // Relation row: bind schema columns for property access
            return ctx.withRelationType(paramName, rel.schema());
        } else if (resolvedType instanceof Type.Tuple) {
            // Tuple = T in Relation<T> = row schema type (our RelationType).
            // Bind as lambdaParam so compileVariable returns Tuple type,
            // preserving type identity for unification (e.g., rowNumber<T>(rel, row:T)).
            // Column property access ($r.id) is handled by compileProperty.
            return ctx.withLambdaParam(paramName, resolvedType);
        } else if (resolvedType instanceof Type.ClassType) {
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
    private void validateMult(Multiplicity expected, Multiplicity actual, String context) {
        switch (expected) {
            case Multiplicity.Bounded exp -> {
                // Only reject: expected [1] but got [*]
                if (exp.equals(Multiplicity.ONE) && actual.isMany()) {
                    throw new PureCompileException(
                            context + ": expected multiplicity [1], got [*]");
                }
                // [*] accepts [1] — a single value is a valid collection
            }
            // Multiplicity variable — skip validation (can't check against unknown)
            case Multiplicity.Var v -> { /* pass */ }
        }
    }

    // ========== Lambda helpers ==========

    /**
     * Extracts the FunctionType from a lambda parameter definition.
     * Handles both {@code Function<{...}>} (filter, map, etc.) and
     * {@code FuncColSpecArray<{...},T>} (project).
     * Must always succeed — malformed signature throws.
     */
    protected Type.FunctionType extractFunctionType(Type.Parameter lambdaDef) {
        if (lambdaDef.type() instanceof Type.GenericType fp
                && !fp.typeArgs().isEmpty()
                && fp.typeArgs().get(0) instanceof Type.FunctionType ft) {
            // Both Function<{T[1]->Boolean[1]}> and FuncColSpecArray<{C[1]->Any[*]},T>
            // have the FunctionType as their first type argument
            com.gs.legend.model.m3.LClass lc = fp.rawType() instanceof com.gs.legend.model.m3.LClass l ? l : null;
            if (lc == com.gs.legend.model.m3.LClass.FUNCTION
                    || lc == com.gs.legend.model.m3.LClass.FUNC_COL_SPEC
                    || lc == com.gs.legend.model.m3.LClass.AGG_COL_SPEC
                    || lc == com.gs.legend.model.m3.LClass.FUNC_COL_SPEC_ARRAY
                    || lc == com.gs.legend.model.m3.LClass.AGG_COL_SPEC_ARRAY) {
                return ft;
            }
        }
        throw new PureCompileException(
                "Signature malformed: expected Function<{...}> or FuncColSpecArray<{...}>, got "
                        + lambdaDef.type());
    }

    /**
     * Compiles all lambda body statements; returns the TypedSpec of the last.
     */
    protected TypedSpec compileLambdaBody(LambdaFunction lambda, TypeChecker.CompilationContext ctx) {
        TypedSpec last = null;
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
    protected void validateLambdaReturn(TypeInfo bodyType, Type.FunctionType ft,
                                        Bindings bindings, String funcName) {
        String context = funcName + "() predicate return";

        // Validate return type — use resolve to get expected Type from bindings.
        // Normalize PrecisionDecimal → DECIMAL so two different-precision decimals
        // aren't treated as different types (matches unifyTypeVar convention).
        Type expectedType = resolve(ft.returnType(), bindings, context);
        Type expectedNorm = expectedType instanceof Type.PrecisionDecimal ? Primitive.DECIMAL : expectedType;
        Type actualNorm = bodyType.type() instanceof Type.PrecisionDecimal ? Primitive.DECIMAL : bodyType.type();
        if (!expectedNorm.equals(actualNorm)) {
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
    protected static boolean isLambdaParam(Type.Parameter sigParam) {
        return sigParam.type() instanceof Type.GenericType gt
                && gt.rawType() == com.gs.legend.model.m3.LClass.FUNCTION;
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
    protected TypedSpec compileLambdaArg(LambdaFunction lambda, Type.Parameter sigParam,
                                        Bindings bindings, TypedSpec source,
                                        TypeChecker.CompilationContext ctx, String funcName) {
        Type.FunctionType ft = extractFunctionType(sigParam);

        // Bind lambda params (all of them, not just the first)
        TypeChecker.CompilationContext lambdaCtx = ctx;
        int paramCount = Math.min(lambda.parameters().size(), ft.params().size());
        for (int pi = 0; pi < paramCount; pi++) {
            String paramName = lambda.parameters().get(pi).name();
            Type resolvedParamType = resolve(ft.params().get(pi).type(), bindings,
                    funcName + "() lambda param " + pi);
            lambdaCtx = bindLambdaParam(lambdaCtx, paramName, resolvedParamType, source);
        }

        // Compile body
        if (lambda.body().isEmpty()) {
            return null;
        }
        TypedSpec bodyResult = compileLambdaBody(lambda, lambdaCtx);

        // Bind unbound return TypeVar from body result (e.g., V in map<T,V>)
        // This lets resolveOutput work without special-case logic downstream.
        if (ft.returnType() instanceof Type.TypeVar tv && !bindings.containsKey(tv.name())) {
            if (bodyResult.type() != null) {
                bindings.put(tv.name(), bodyResult.type());
            }
        }

        // Validate return type — skip for TypeVars we just bound from the body
        // (they ARE the body result, so validation is tautological)
        if (!(ft.returnType() instanceof Type.TypeVar tv2) || !bindings.containsKey(tv2.name())
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
     * Extracts cast target as Type from a cast() wrapper.
     * Returns null if body is not wrapped in cast().
     * Shared by GroupByChecker and ExtendChecker.
     */
    protected Type extractCastGenericType(ValueSpecification body) {
        if (body instanceof AppliedFunction af
                && "cast".equals(simpleName(af.function()))) {
            for (var p : af.parameters()) {
                if (p instanceof TypeAnnotation ta) {
                    return ta.resolve(env.modelContext());
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
        if (vs instanceof com.gs.legend.ast.ColSpec cs) {
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
        if (vs instanceof com.gs.legend.ast.ColSpecArray(java.util.List<com.gs.legend.ast.ColSpec> specs)) {
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
        com.gs.legend.model.m3.Type leftElem = left.type();
        com.gs.legend.model.m3.Type rightElem = right.type();

        if (leftElem instanceof com.gs.legend.model.m3.Type.ClassType(String leftClass)
                && rightElem instanceof com.gs.legend.model.m3.Type.ClassType(String rightClass)) {
            var lcaOpt = findLowestCommonAncestor(leftClass, rightClass);
            if (lcaOpt.isPresent()) {
                var lcaClass = lcaOpt.get();
                var lcaCols = new java.util.LinkedHashMap<String, com.gs.legend.model.m3.Type>();
                for (var prop : lcaClass.allProperties(env.modelContext())) {
                    lcaCols.put(prop.name(),
                            prop.type());
                }
                var lcaRelType = com.gs.legend.model.m3.Type.Schema.withoutPivot(lcaCols);
                return TypeInfo.builder()
                        .expressionType(ExpressionType.many(
                                new com.gs.legend.model.m3.Type.Relation(lcaRelType)))
                        .build();
            }
        }
        return null;
    }
}
