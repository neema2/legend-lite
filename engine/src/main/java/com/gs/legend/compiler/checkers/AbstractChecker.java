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

    // ========== Overload resolution ==========

    /**
     * Resolves the correct function overload by matching the AST param structure
     * against registered signature param types.
     *
     * <p>Resolution strategy (no silent fallbacks):
     * <ol>
     *   <li>Get all overloads by name</li>
     *   <li>Filter by arity — throw if none match</li>
     *   <li>If exactly one — return it</li>
     *   <li>Structurally match each candidate's PType params against AST nodes</li>
     *   <li>Exactly one must match — throw if zero or ambiguous</li>
     * </ol>
     *
     * @param funcName   Simple function name
     * @param params     AST parameters (not yet compiled, except source)
     * @param source     Compiled TypeInfo for param[0], or null
     * @return The single matching NativeFunctionDef
     * @throws PureCompileException on no match or ambiguity
     */
    protected NativeFunctionDef resolveOverload(String funcName,
                                                 List<ValueSpecification> params,
                                                 TypeInfo source) {
        var defs = BuiltinFunctionRegistry.instance().resolve(funcName);
        if (defs.isEmpty()) {
            throw new PureCompileException("Unknown function: '" + funcName + "'");
        }

        // Step 1: filter by arity
        var arityMatches = defs.stream()
                .filter(d -> d.arity() == params.size())
                .toList();
        if (arityMatches.isEmpty()) {
            throw new PureCompileException(
                    "No overload of '" + funcName + "' accepts " + params.size() + " arguments"
                    + " (registered arities: " + defs.stream()
                            .map(d -> String.valueOf(d.arity())).distinct().toList() + ")");
        }
        if (arityMatches.size() == 1) {
            return arityMatches.get(0);
        }

        // Step 2: structural matching against AST nodes
        var matches = arityMatches.stream()
                .filter(d -> matchesStructurally(d, params, source))
                .toList();
        if (matches.size() == 1) {
            return matches.get(0);
        }
        if (matches.isEmpty()) {
            throw new PureCompileException(
                    "No overload of '" + funcName + "' matches the given argument types"
                    + " (tried " + arityMatches.size() + " candidates with arity " + params.size() + ")");
        }

        // Ambiguous — multiple candidates matched
        throw new PureCompileException(
                "Ambiguous overload: " + matches.size() + " overloads of '"
                + funcName + "' match the given argument types");
    }

    /**
     * Checks if a def's param types structurally match the AST param nodes.
     * No compilation needed — only inspects AST node types.
     */
    private boolean matchesStructurally(NativeFunctionDef def,
                                        List<ValueSpecification> params,
                                        TypeInfo source) {
        for (int i = 0; i < def.params().size() && i < params.size(); i++) {
            PType expected = def.params().get(i).type();
            ValueSpecification actual = params.get(i);
            if (!structuralMatch(expected, actual, source, i == 0)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Matches a single PType against an AST node.
     * Returns false (reject) for unknown types — no silent pass-through.
     */
    private boolean structuralMatch(PType expected, ValueSpecification actual,
                                    TypeInfo source, boolean isSource) {
        // PureCollection wraps multiple elements (e.g., [ascending(~id), ascending(~name)])
        // Check if elements match the expected type
        if (actual instanceof PureCollection(java.util.List<ValueSpecification> elements)
                && !elements.isEmpty()) {
            return elements.stream().allMatch(e -> structuralMatch(expected, e, source, false));
        }
        if (expected instanceof PType.Parameterized p) {
            return switch (p.rawType()) {
                case "Relation" -> isSource
                        ? (source != null && source.isRelational())
                        : actual instanceof ClassInstance ci && "relation".equals(ci.type());
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
                case "Function" -> actual instanceof LambdaFunction;
                case "SortInfo" -> actual instanceof AppliedFunction;
                case "_Window" -> actual instanceof AppliedFunction;
                case "Rows", "_Range" -> actual instanceof AppliedFunction;
                default -> false;
            };
        }
        if (expected instanceof PType.TypeVar) {
            // TypeVar (T, V, etc.) matches anything EXCEPT Relation sources
            return !isSource || source == null || !source.isRelational();
        }
        if (expected instanceof PType.Concrete c) {
            return switch (c.name()) {
                case "Integer" -> actual instanceof CInteger;
                case "String" -> actual instanceof CString;
                case "Float" -> actual instanceof CFloat;
                case "Decimal" -> actual instanceof CDecimal;
                case "Boolean" -> actual instanceof CBoolean;
                default -> false;
            };
        }
        return false;
    }

    // ========== Type variable unification ==========

    /**
     * Validates the source parameter AND binds type variables in one pass.
     * Returns a map of type variable name → resolved GenericType.
     *
     * <p>This replaces the need for a separate validate() call on the source
     * parameter — unify does both validation and binding.
     *
     * <p>Currently unifies only the source parameter (param[0]). Sufficient
     * for single-TypeVar functions (filter, sort, exists, map, etc.).
     * Multi-param unification needed for join<T,V,K,R> — extend when needed.
     */
    protected Map<String, GenericType> unify(NativeFunctionDef def, ExpressionType source) {
        var bindings = new LinkedHashMap<String, GenericType>();
        if (def.params().isEmpty()) {
            throw new PureCompileException(
                    def.name() + "(): signature has no parameters");
        }
        PType.Param param0 = def.params().get(0);

        // TODO: Currently unifies only param[0] (the source). This works for
        //   single-TypeVar functions (filter, sort, exists, map, forAll) where T
        //   is fully determined by the source. For multi-TypeVar functions like
        //   join<T,V,K,R>, we need to iterate all params and unify each against
        //   its corresponding actual argument. When we hit join, extend this to:
        //     for (int i = 0; i < def.params().size(); i++)
        //       unifyType(def.params().get(i).type(), actuals[i].type(), bindings, ctx)

        // Validate + bind type
        unifyType(param0.type(), source.type(), bindings, def.name() + "() source");

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
     * Recursively matches a PType against a GenericType, populating type variable bindings.
     * No silent skips — every case either binds, validates, or throws.
     */
    private void unifyType(PType expected, GenericType actual,
                           Map<String, GenericType> bindings, String context) {
        switch (expected) {
            case PType.TypeVar v -> {
                GenericType existing = bindings.get(v.name());
                if (existing != null) {
                    if (!existing.typeName().equals(actual.typeName())) {
                        throw new PureCompileException(
                                context + ": type variable " + v.name() + " bound to "
                                        + existing.typeName() + " but got " + actual.typeName());
                    }
                } else {
                    bindings.put(v.name(), actual);
                }
            }
            case PType.Parameterized p -> {
                if ("Relation".equals(p.rawType())) {
                    if (!(actual instanceof GenericType.Relation)) {
                        throw new PureCompileException(
                                context + ": expected Relation, got " + actual.typeName());
                    }
                    for (var typeArg : p.typeArgs()) {
                        unifyType(typeArg, actual, bindings, context);
                    }
                } else {
                    throw new PureCompileException(
                            context + ": unexpected parameterized type: " + p.rawType());
                }
            }
            case PType.Concrete c -> {
                GenericType g = c.toGenericType();
                if (g == null) {
                    throw new PureCompileException(
                            context + ": unresolvable concrete type in signature: " + c.name());
                }
                if (!g.typeName().equals(actual.typeName())) {
                    throw new PureCompileException(
                            context + ": expected " + c.name() + ", got " + actual.typeName());
                }
            }
            case PType.SchemaAlgebra sa -> throw new PureCompileException(
                    context + ": schema algebra unification not yet supported");
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
    protected GenericType resolve(PType type, Map<String, GenericType> bindings, String context) {
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
                    throw new PureCompileException(
                            context + ": unresolvable concrete type: " + c.name());
                }
                yield g;
            }
            case PType.Parameterized p -> {
                if ("Relation".equals(p.rawType()) && !p.typeArgs().isEmpty()) {
                    yield resolve(p.typeArgs().get(0), bindings, context);
                }
                throw new PureCompileException(
                        context + ": cannot resolve parameterized type: " + p);
            }
            case PType.SchemaAlgebra sa -> throw new PureCompileException(
                    context + ": schema algebra resolution not yet supported");
            case PType.FunctionType ft -> throw new PureCompileException(
                    context + ": cannot resolve FunctionType to GenericType");
            case PType.RelationTypeVar rtv -> throw new PureCompileException(
                    context + ": cannot resolve RelationTypeVar to GenericType");
        };
    }

    /**
     * Resolves a Mult to a Multiplicity.
     * Fixed multiplicities resolve directly. Var multiplicities are not yet supported.
     */
    protected Multiplicity resolveMult(Mult mult, String context) {
        return switch (mult) {
            case Mult.Fixed f -> f.value();
            // Multiplicity variable (e.g. 'm' in T[m]) — default to MANY.
            // Sort, filter, etc. preserve cardinality so MANY is always safe.
            case Mult.Var v -> Multiplicity.MANY;
        };
    }

    /**
     * Constructs the output ExpressionType from the signature's return type + bindings.
     * This is the generic way to compute return types — no hardcoding.
     */
    protected ExpressionType resolveOutput(NativeFunctionDef def,
                                           Map<String, GenericType> bindings, String context) {
        GenericType returnType = resolve(def.returnType(), bindings, context + " return type");
        Multiplicity returnMult = resolveMult(def.returnMult(), context + " return mult");
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
            TypeChecker.CompilationContext lambdaCtx = ctx.withRelationType(paramName, rel.schema());
            if (source.mapping() != null) {
                lambdaCtx = lambdaCtx.withMapping(paramName, source.mapping());
            }
            return lambdaCtx;
        } else if (resolvedType instanceof GenericType.ClassType) {
            // Class instance: bind type for property resolution via modelContext
            // + mapping for property→column resolution in SQL generation
            TypeChecker.CompilationContext lambdaCtx = ctx.withLambdaParam(paramName, resolvedType);
            if (source.mapping() != null) {
                lambdaCtx = lambdaCtx.withMapping(paramName, source.mapping());
            }
            return lambdaCtx;
        } else {
            GenericType elemType = resolvedType;
            if (elemType.isList()) {
                elemType = elemType.elementType();
            }
            return ctx.withLambdaParam(paramName, elemType);
        }
    }

    // ========== Multiplicity validation ==========

    /**
     * Validates actual multiplicity against expected.
     */
    private void validateMult(Mult expected, Multiplicity actual, String context) {
        switch (expected) {
            case Mult.Fixed f -> {
                Multiplicity exp = f.value();
                if (exp.equals(Multiplicity.ONE) && actual.isMany()) {
                    throw new PureCompileException(
                            context + ": expected multiplicity [1], got [*]");
                }
                if (exp.equals(Multiplicity.MANY) && !actual.isMany()) {
                    throw new PureCompileException(
                            context + ": expected multiplicity [*], got " + actual);
                }
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
            if ("Function".equals(fp.rawType()) || "FuncColSpecArray".equals(fp.rawType())) {
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
                                        Map<String, GenericType> bindings, String funcName) {
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

    // ========== Shared utilities ==========

    /** Extracts simple function name from qualified name (e.g. "meta::pure::...::sort" → "sort"). */
    protected static String simpleName(String qualifiedName) {
        int idx = qualifiedName.lastIndexOf("::");
        return idx >= 0 ? qualifiedName.substring(idx + 2) : qualifiedName;
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
        com.gs.legend.plan.GenericType leftElem = left.type() != null ? left.type().elementType() : null;
        com.gs.legend.plan.GenericType rightElem = right.type() != null ? right.type().elementType() : null;

        if (leftElem instanceof com.gs.legend.plan.GenericType.ClassType(String leftClass)
                && rightElem instanceof com.gs.legend.plan.GenericType.ClassType(String rightClass)) {
            var lcaOpt = findLowestCommonAncestor(leftClass, rightClass);
            if (lcaOpt.isPresent()) {
                var lcaClass = lcaOpt.get();
                var lcaCols = new java.util.LinkedHashMap<String, com.gs.legend.plan.GenericType>();
                for (var prop : lcaClass.allProperties()) {
                    lcaCols.put(prop.name(), com.gs.legend.plan.GenericType.fromType(prop.genericType()));
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
