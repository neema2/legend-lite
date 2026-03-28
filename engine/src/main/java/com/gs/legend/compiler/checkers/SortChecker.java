package com.gs.legend.compiler.checkers;

import com.gs.legend.ast.*;
import com.gs.legend.compiler.*;
import com.gs.legend.plan.GenericType;

import java.util.*;

/**
 * Signature-driven type checker for {@code sort()}, {@code sortBy()},
 * and {@code sortByReversed()}.
 *
 * <p>Canonical Pure signatures:
 * <ul>
 *   <li>Relation: {@code sort<X,T>(rel:Relation<T>[1], sortInfo:SortInfo<X⊆T>[*]):Relation<T>[1]}</li>
 *   <li>Collection: {@code sort<T,U|m>(col:T[m], key:{T→U}[0..1], comp:{U,U→Int}[0..1]):T[m]}</li>
 *   <li>Collection: {@code sort<T|m>(col:T[m]):T[m]}</li>
 *   <li>Collection: {@code sort<T|m>(col:T[m], comp:{T,T→Int}[0..1]):T[m]}</li>
 *   <li>sortBy: {@code sortBy<T,U|m>(col:T[m], key:{T→U}[0..1]):T[m]}</li>
 *   <li>sortByReversed: {@code sortByReversed<T,U|m>(col:T[m], key:{T→U}[0..1]):T[m]}</li>
 * </ul>
 *
 * <p>Dispatch:
 * <ul>
 *   <li>{@code sort} on Relation → {@link #checkRelationSort}</li>
 *   <li>{@code sort} on Collection → {@link #checkCollectionSort} (detects direction from compare lambda)</li>
 *   <li>{@code sortBy} → {@link #checkCollectionSortBy} with ASC</li>
 *   <li>{@code sortByReversed} → {@link #checkCollectionSortBy} with DESC</li>
 * </ul>
 *
 * <p>Both paths use {@link #unify} for type variable binding and
 * {@link #resolveOutput} for return type — fully signature-driven.
 */
public class SortChecker extends AbstractChecker {

    public SortChecker(TypeCheckEnv env) {
        super(env);
    }

    public TypeInfo check(AppliedFunction af, TypeInfo source,
                          TypeChecker.CompilationContext ctx) {
        String funcName = simpleName(af.function());
        var params = af.parameters();

        if (source.isRelational()) {
            // Compile literal params (CString, EnumValue, etc.) before resolve
            // so structural matching has real types for disambiguation.
            // Skip AST-shape params (AppliedFunction, ClassInstance, LambdaFunction)
            // which are matched structurally without needing compiled types.
            var compiledTypes = new java.util.HashMap<Integer, ExpressionType>();
            for (int i = 1; i < params.size(); i++) {
                ValueSpecification p = params.get(i);
                if (!(p instanceof AppliedFunction) && !(p instanceof ClassInstance)
                        && !(p instanceof LambdaFunction) && !(p instanceof PureCollection)) {
                    TypeInfo ti = env.compileExpr(p, ctx);
                    compiledTypes.put(i, ti.expressionType());
                }
            }
            NativeFunctionDef def = resolveOverload("sort", params, source, compiledTypes);
            // Legacy TDS: sort(Relation, String, SortDirection) — arity 3
            // Rewrite to sort(ascending(~col)) and recompile through modern path
            if (def.arity() == 3) {
                return checkLegacySort(af, source, ctx);
            }
            return checkRelationSort(af, source, def);
        }

        // Collection sort — dispatch by function name
        return switch (funcName) {
            case "sort" -> {
                NativeFunctionDef def = resolveOverload("sort", params, source);
                yield checkCollectionSort(af, source, ctx, def);
            }
            case "sortBy" -> {
                NativeFunctionDef def = resolveOverload("sortBy", params, source);
                yield checkCollectionSortBy(af, source, ctx, def, TypeInfo.SortDirection.ASC);
            }
            case "sortByReversed" -> {
                NativeFunctionDef def = resolveOverload("sortByReversed", params, source);
                yield checkCollectionSortBy(af, source, ctx, def, TypeInfo.SortDirection.DESC);
            }
            default -> throw new PureCompileException(
                    "SortChecker: unexpected function '" + funcName + "'");
        };
    }



    // ========== Relation Sort ==========

    /**
     * Relation sort: sort<X,T>(rel:Relation<T>[1], sortInfo:SortInfo<X⊆T>[*]):Relation<T>[1]
     * Extracts SortSpecs from ascending(~col) / descending(~col) sort info params.
     */
    private TypeInfo checkRelationSort(AppliedFunction af, TypeInfo source, NativeFunctionDef def) {
        List<ValueSpecification> params = af.parameters();

        // 1. Bind type variables from signature
        var bindings = unify(def, source.expressionType());

        // 2. Extract sort specs from SortInfo params
        List<TypeInfo.SortSpec> sortSpecs = List.of();
        if (params.size() > 1) {
            sortSpecs = resolveSortInfoParams(params.get(1), source.schema());
        }

        // 3. Output type from signature (Relation<T>[1] → same as input)
        ExpressionType outputType = resolveOutput(def, bindings, "sort()");
        return TypeInfo.builder()
                .mapping(source.mapping())
                .sortSpecs(sortSpecs)
                .expressionType(outputType)
                .build();
    }

    /**
     * Legacy TDS sort: sort(Relation, String, SortDirection)
     *
     * <pre>
     * sort('col', SortDirection.ASC)  → sort(ascending(~col))
     * sort('col', SortDirection.DESC) → sort(descending(~col))
     * </pre>
     *
     * <p>Pure AST→AST rewrite, then recompile through the modern path.
     * Same pattern as ProjectChecker.rewriteLegacyProject.
     */
    private TypeInfo checkLegacySort(AppliedFunction af, TypeInfo source,
                                      TypeChecker.CompilationContext ctx) {
        List<ValueSpecification> params = af.parameters();

        // Extract column name from param[1] (CString)
        if (!(params.get(1) instanceof CString(String colName))) {
            throw new PureCompileException("sort(): legacy syntax requires column name as String");
        }

        // Extract direction from param[2] (EnumValue) — required, no defaulting
        if (!(params.get(2) instanceof EnumValue ev)) {
            throw new PureCompileException(
                    "sort(): legacy syntax requires SortDirection enum (e.g., SortDirection.ASC), got "
                    + params.get(2).getClass().getSimpleName());
        }
        String dirFn = switch (ev.value().toUpperCase()) {
            case "ASC", "ASCENDING" -> "ascending";
            case "DESC", "DESCENDING" -> "descending";
            default -> throw new PureCompileException(
                    "sort(): unknown SortDirection value '" + ev.value()
                    + "', expected ASC or DESC");
        };

        // Build: ascending(~col) or descending(~col)
        var colSpec = new ColSpec(colName, null);
        var colSpecInstance = new ClassInstance("colSpec", colSpec);
        var sortInfo = new AppliedFunction(dirFn, List.of(colSpecInstance));

        // Build new arity-2 AF: sort(source, sortInfo)
        var rewritten = new AppliedFunction(
                af.function(),
                List.of(params.get(0), sortInfo),
                af.hasReceiver(),
                af.sourceText(),
                af.argTexts());

        TypeInfo result = env.compileExpr(rewritten, ctx);
        return TypeInfo.from(result).inlinedBody(rewritten).build();
    }

    /**
     * Resolves sort info parameters into SortSpecs.
     * Handles single: sort(ascending(~col))
     * Handles array:  sort([ascending(~col), descending(~col2)])
     */
    private List<TypeInfo.SortSpec> resolveSortInfoParams(ValueSpecification vs,
                                                          GenericType.Relation.Schema schema) {
        // Array of sort specs: [ascending(~id), descending(~name)]
        if (vs instanceof PureCollection(List<ValueSpecification> values)) {
            return values.stream()
                    .map(elem -> resolveSingleSortInfo(elem, schema))
                    .toList();
        }
        // Single sort spec: ascending(~id)
        return List.of(resolveSingleSortInfo(vs, schema));
    }

    /**
     * Resolves a single ascending(~col) or descending(~col) into a SortSpec.
     * Also handles ClassInstance("sortInfo", ColSpec) wrapper for ~col→ascending() syntax.
     */
    private TypeInfo.SortSpec resolveSingleSortInfo(ValueSpecification vs,
                                                    GenericType.Relation.Schema schema) {
        // ascending(~col) / descending(~col) → AppliedFunction
        if (vs instanceof AppliedFunction af) {
            String funcName = simpleName(af.function());
            if ("ascending".equals(funcName) || "asc".equals(funcName)) {
                String col = extractColumnName(af.parameters().get(0));
                validateColumn(col, schema);
                return new TypeInfo.SortSpec(col, TypeInfo.SortDirection.ASC);
            }
            if ("descending".equals(funcName) || "desc".equals(funcName)) {
                String col = extractColumnName(af.parameters().get(0));
                validateColumn(col, schema);
                return new TypeInfo.SortSpec(col, TypeInfo.SortDirection.DESC);
            }
        }

        // ClassInstance("sortInfo", ColSpec) — bare ~col, default ASC
        if (vs instanceof ClassInstance(String type, Object value)
                && "sortInfo".equals(type) && value instanceof ColSpec cs) {
            validateColumn(cs.name(), schema);
            return new TypeInfo.SortSpec(cs.name(), TypeInfo.SortDirection.ASC);
        }

        throw new PureCompileException(
                "sort(): expected ascending(~col) or descending(~col), got "
                        + vs.getClass().getSimpleName());
    }

    // ========== Collection Sort ==========

    /**
     * Collection sort: fully signature-driven.
     * Uses extractFunctionType → resolve → bindLambdaParam → compileLambdaBody
     * for each lambda parameter, same as FilterChecker.
     *
     * <p>Signatures:
     * <ul>
     *   <li>{@code sort<T,U|m>(col:T[m], key:{T[1]→U[1]}[0..1], comp:{U[1],U[1]→Int[1]}[0..1]):T[m]}</li>
     *   <li>{@code sort<T|m>(col:T[m]):T[m]}</li>
     *   <li>{@code sort<T|m>(col:T[m], comp:{T[1],T[1]→Int[1]}[0..1]):T[m]}</li>
     * </ul>
     */
    private TypeInfo checkCollectionSort(AppliedFunction af, TypeInfo source,
                                         TypeChecker.CompilationContext ctx, NativeFunctionDef def) {
        List<ValueSpecification> params = af.parameters();

        // 1. Bind type variables from signature
        var bindings = unify(def, source.expressionType());

        // 2. Process lambdas through the signature — key lambda first to bind U
        for (int i = 1; i < params.size() && i < def.params().size(); i++) {
            if (!(params.get(i) instanceof LambdaFunction lambda)) {
                env.compileExpr(params.get(i), ctx);
                continue;
            }

            // Extract FunctionType from signature param
            PType.FunctionType ft = extractFunctionType(def.params().get(i));

            // Bind lambda params using resolved types from signature
            TypeChecker.CompilationContext lambdaCtx = ctx;
            for (int p = 0; p < lambda.parameters().size() && p < ft.paramTypes().size(); p++) {
                String paramName = lambda.parameters().get(p).name();
                GenericType resolvedParamType = resolve(ft.paramTypes().get(p).type(), bindings,
                        "sort() lambda param '" + paramName + "'");
                lambdaCtx = bindLambdaParam(lambdaCtx, paramName, resolvedParamType, source);
            }

            // Compile lambda body with bound params
            TypeInfo bodyType = compileLambdaBody(lambda, lambdaCtx);

            // Bind return type variable (e.g. U from key:{T→U}) into bindings
            // so the next lambda (comp:{U,U→Int}) can resolve U
            if (bodyType != null && bodyType.expressionType() != null
                    && ft.returnType() instanceof PType.TypeVar tv
                    && !bindings.containsKey(tv.name())) {
                bindings.put(tv.name(), bodyType.expressionType().type());
            }
        }

        // 3. Detect direction from compare lambda
        TypeInfo.SortDirection direction = detectCompareDirection(params);

        // 4. Direction-only SortSpec (PlanGen reads key lambda from AST)
        List<TypeInfo.SortSpec> sortSpecs = List.of(
                new TypeInfo.SortSpec(null, direction));

        // 5. Resolve associations from key lambda (for association sort support)
        Map<String, TypeInfo.AssociationTarget> associations =
                resolveAssociationsFromParams(params, source);

        // 6. Output type from signature (T[m] → same as input)
        ExpressionType outputType = resolveOutput(def, bindings, "sort()");
        return TypeInfo.builder()
                .mapping(source.mapping())
                .associations(associations)
                .sortSpecs(sortSpecs)
                .expressionType(outputType)
                .build();
    }

    /**
     * sortBy / sortByReversed: key lambda only, fixed direction.
     * Uses compileLambdaArg — the full pipeline from AbstractChecker.
     */
    private TypeInfo checkCollectionSortBy(AppliedFunction af, TypeInfo source,
                                            TypeChecker.CompilationContext ctx,
                                            NativeFunctionDef def,
                                            TypeInfo.SortDirection direction) {
        var bindings = unify(def, source.expressionType());

        // compileLambdaArg does: extractFunctionType → resolve → bind → compile → bind return TypeVar
        LambdaFunction lambda = (LambdaFunction) af.parameters().get(1);
        compileLambdaArg(lambda, def.params().get(1), bindings, source, ctx, "sortBy");

        // Resolve associations (sortBy on class: {p|$p.primaryAddress.city})
        Map<String, TypeInfo.AssociationTarget> associations =
                resolveAssociationsFromParams(af.parameters(), source);

        ExpressionType outputType = resolveOutput(def, bindings, "sortBy()");
        return TypeInfo.builder()
                .mapping(source.mapping())
                .associations(associations)
                .sortSpecs(List.of(new TypeInfo.SortSpec(null, direction)))
                .expressionType(outputType)
                .build();
    }

    /**
     * Detects sort direction from compare lambda argument order.
     * {x,y|$x→compare($y)} → ASC (natural)
     * {x,y|$y→compare($x)} → DESC (reversed — second param is receiver)
     */
    private TypeInfo.SortDirection detectCompareDirection(List<ValueSpecification> params) {
        for (int i = 1; i < params.size(); i++) {
            if (params.get(i) instanceof LambdaFunction(List<Variable> parameters, List<ValueSpecification> body)
                    && parameters.size() == 2 && !body.isEmpty()) {
                var firstBody = body.get(0);
                if (firstBody instanceof AppliedFunction af2
                        && simpleName(af2.function()).equals("compare")
                        && !af2.parameters().isEmpty()) {
                    String secondParam = parameters.get(1).name();
                    if (af2.parameters().get(0) instanceof Variable v
                            && v.name().equals(secondParam)) {
                        return TypeInfo.SortDirection.DESC;
                    }
                }
            }
        }
        return TypeInfo.SortDirection.ASC;
    }


    // ========== Helpers ==========

    private void validateColumn(String col, GenericType.Relation.Schema schema) {
        if (schema != null && !schema.columns().isEmpty()) {
            schema.requireColumn(col);
        }
    }
}
