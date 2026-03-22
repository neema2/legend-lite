package com.gs.legend.compiler.checkers;

import com.gs.legend.ast.*;
import com.gs.legend.compiler.*;
import com.gs.legend.model.mapping.ClassMapping;
import com.gs.legend.plan.GenericType;

import java.util.*;

/**
 * Signature-driven type checker for {@code project()}.
 *
 * <p>Two overloads (from project.pure):
 * <ul>
 *   <li>Class:    {@code project<C,T>(cl:C[*], x:FuncColSpecArray<{C[1]->Any[*]},T>[1]):Relation<T>[1]}</li>
 *   <li>Relation: {@code project<T,Z>(r:Relation<T>[1], fs:FuncColSpecArray<{T[1]->Any[*]},Z>[1]):Relation<Z>[1]}</li>
 * </ul>
 *
 * <p>Both take a single {@code FuncColSpecArray} — the {@code ~[alias:x|$x.prop, ...]} syntax.
 * Each ColSpec lambda is compiled via {@link #compileLambdaBody} for type safety.
 * Only type-level info is produced; PlanGenerator walks the AST directly for SQL
 * code generation (Pattern A).
 *
 * <p>TODO: Add old TDS overloads for backward compat:
 * <ul>
 *   <li>{@code project<K>(set:K[*], functions:Function<{K[1]->Any[*]}>[*], ids:String[*]):TabularDataSet[1]}</li>
 *   <li>{@code project<T>(set:T[*], columnSpecifications:ColumnSpecification<T>[*]):TabularDataSet[1]}</li>
 * </ul>
 */
public class ProjectChecker extends AbstractChecker {

    public ProjectChecker(TypeCheckEnv env) {
        super(env);
    }

    public TypeInfo check(AppliedFunction af, TypeInfo source,
                          TypeChecker.CompilationContext ctx) {
        List<ValueSpecification> params = af.parameters();
        NativeFunctionDef def = resolveOverload("project", params, source);
        ClassMapping mapping = source.mapping();

        // 1. Bind type variables from signature
        Map<String, GenericType> bindings = unify(def, source.expressionType());

        // 2. Extract ColSpecs from FuncColSpecArray param
        List<ColSpec> colSpecs = extractColSpecs(params.get(1));

        // 3. Resolve lambda param type from signature
        PType.FunctionType ft = extractFunctionType(def.params().get(1));
        GenericType resolvedParamType = resolve(ft.paramTypes().get(0).type(), bindings,
                "project() lambda param");

        // 4. Type-check each ColSpec lambda → build output schema
        Map<String, GenericType> projectedColumns = new LinkedHashMap<>();
        List<TypeInfo.ProjectionSpec> projectionSpecs = new ArrayList<>();

        for (ColSpec cs : colSpecs) {
            String alias = cs.name();
            LambdaFunction lambda = cs.function1();

            if (lambda == null) {
                // Simple column reference: ~prop → synthesize identity lambda
                lambda = new LambdaFunction(
                        List.of(new Variable("x")),
                        List.of(new AppliedProperty(alias, List.of(new Variable("x")))));
            }

            // Bind lambda param
            String paramName = lambda.parameters().isEmpty() ? "x"
                    : lambda.parameters().get(0).name();
            TypeChecker.CompilationContext lambdaCtx = bindLambdaParam(
                    ctx, paramName, resolvedParamType, source);

            // Compile body → type comes from the type system
            TypeInfo bodyType = compileLambdaBody(lambda, lambdaCtx);

            // Extract property path for association detection
            List<String> propertyPath = extractPropertyPath(lambda);

            projectedColumns.put(alias, bodyType.type());
            projectionSpecs.add(new TypeInfo.ProjectionSpec(propertyPath, alias));
        }

        // 5. Resolve associations (multi-hop property paths)
        Map<String, TypeInfo.AssociationTarget> associations = Map.of();
        if (mapping != null) {
            List<ValueSpecification> bodies = new ArrayList<>();
            for (var cs : colSpecs) {
                if (cs.function1() != null) bodies.addAll(cs.function1().body());
            }
            associations = env.resolveAssociations(bodies, mapping);
        }
        if (source.hasAssociations()) {
            var merged = new LinkedHashMap<>(source.associations());
            associations.forEach(merged::putIfAbsent);
            associations = Map.copyOf(merged);
        }

        // 6. Build output Relation<Schema>
        GenericType.Relation.Schema resultSchema =
                GenericType.Relation.Schema.withoutPivot(projectedColumns);

        return TypeInfo.builder()
                .mapping(mapping)
                .associations(associations)
                .projections(projectionSpecs)
                .expressionType(ExpressionType.many(new GenericType.Relation(resultSchema)))
                .build();
    }

    // ========== Helpers ==========

    /** Extracts ColSpec list from a FuncColSpecArray parameter. */
    private static List<ColSpec> extractColSpecs(ValueSpecification param) {
        if (param instanceof ClassInstance ci && ci.value() instanceof ColSpecArray(List<ColSpec> specs)) {
            return specs;
        }
        throw new PureCompileException(
                "project() param 2 must be a FuncColSpecArray (~[...]), got "
                        + param.getClass().getSimpleName());
    }

    /**
     * Extracts the property path from a lambda body.
     * Returns multi-element list for association navigation (e.g. ["items", "productName"]),
     * single-element for simple property access.
     */
    private static List<String> extractPropertyPath(LambdaFunction lf) {
        if (lf.body().isEmpty()) return List.of();
        return extractPathFromExpr(lf.body().get(0));
    }

    private static List<String> extractPathFromExpr(ValueSpecification vs) {
        if (vs instanceof AppliedProperty ap) {
            // Check for chained property: $p.items.productName
            if (!ap.parameters().isEmpty() && ap.parameters().get(0) instanceof AppliedProperty parent) {
                var path = new ArrayList<>(extractPathFromExpr(parent));
                path.add(ap.property());
                return path;
            }
            return List.of(ap.property());
        }
        // Function wrapping: $p.date->monthNumber() → extract bottommost property
        if (vs instanceof AppliedFunction af && !af.parameters().isEmpty()) {
            return extractPathFromExpr(af.parameters().get(0));
        }
        return List.of();
    }
}
