package com.gs.legend.compiler.checkers;

import com.gs.legend.ast.*;
import com.gs.legend.compiler.*;
import com.gs.legend.plan.GenericType;

import java.util.*;

/**
 * Signature-driven type checker for {@code sort()} and {@code sortBy()}.
 *
 * <p>Canonical Pure signatures only:
 * <ul>
 *   <li>Relation: {@code sort<X,T>(rel:Relation<T>[1], sortInfo:SortInfo<X⊆T>[*]):Relation<T>[1]}</li>
 *   <li>Collection: {@code sort<T,U|m>(col:T[m], key:{T→U}[0..1], comp:{U,U→Int}[0..1]):T[m]}</li>
 *   <li>Collection: {@code sort<T|m>(col:T[m]):T[m]}</li>
 *   <li>Collection: {@code sort<T|m>(col:T[m], comp:{T,T→Int}[0..1]):T[m]}</li>
 *   <li>sortBy: {@code sortBy<T,U|m>(col:T[m], key:{T→U}[0..1]):T[m]}</li>
 * </ul>
 *
 * <p>Relation sort uses {@code ascending(~col)} / {@code descending(~col)} which produce
 * SortInfo values. Collection sort uses key/comparator lambdas.
 *
 * <p>Both paths use {@link #unify} for type variable binding and
 * {@link #resolveOutput} for return type — fully signature-driven.
 */
public class SortChecker extends AbstractChecker {

    public SortChecker(TypeCheckEnv env) {
        super(env);
    }

    public TypeInfo check(AppliedFunction af, TypeInfo source,
                          TypeChecker.CompilationContext ctx, NativeFunctionDef def) {
        // Branch on the SIGNATURE's first param type, not the source.
        // Relation sort is strictly for Relation<T> (TDS).
        // Class-based mapped sources use Collection sort.
        if (isRelationSignature(def)) {
            return checkRelationSort(af, source, def);
        }
        return checkCollectionSort(af, source, ctx, def);
    }

    /** Check if the resolved signature's first param is Relation<T>. */
    private boolean isRelationSignature(NativeFunctionDef def) {
        if (def.params().isEmpty()) return false;
        return def.params().get(0).type() instanceof PType.Parameterized p
                && "Relation".equals(p.rawType());
    }

    // ========== Relation Sort ==========

    /**
     * Relation sort: sort<X,T>(rel:Relation<T>[1], sortInfo:SortInfo<X⊆T>[*]):Relation<T>[1]
     * Extracts SortSpecs from ascending(~col) / descending(~col) sort info params.
     */
    private TypeInfo checkRelationSort(AppliedFunction af, TypeInfo source, NativeFunctionDef def) {
        List<ValueSpecification> params = af.parameters();

        // 1. Bind type variables from signature
        Map<String, GenericType> bindings = unify(def, source.expressionType());

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

    /**
     * Collection sort / sortBy: fully signature-driven.
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
        Map<String, GenericType> bindings = unify(def, source.expressionType());

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

        // 4. Build SortSpec from key lambda (store body AST for PlanGenerator)
        List<TypeInfo.SortSpec> sortSpecs;
        if (params.size() > 1 && params.get(1) instanceof LambdaFunction lf
                && lf.parameters().size() == 1 && !lf.body().isEmpty()) {
            String paramName = lf.parameters().get(0).name();
            sortSpecs = List.of(TypeInfo.SortSpec.fromLambda(
                    lf.body().get(0), paramName, direction));
        } else {
            sortSpecs = List.of(new TypeInfo.SortSpec(null, direction));
        }

        // 5. Resolve associations from key lambda (for association sort support)
        Map<String, TypeInfo.AssociationTarget> associations = Map.of();
        if (source.mapping() != null && params.size() > 1
                && params.get(1) instanceof LambdaFunction lf) {
            associations = env.resolveAssociations(lf.body(), source.mapping());
        }

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
