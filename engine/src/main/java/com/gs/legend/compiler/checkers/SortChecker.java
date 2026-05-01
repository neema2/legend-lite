package com.gs.legend.compiler.checkers;

import com.gs.legend.ast.*;
import com.gs.legend.compiler.*;
import com.gs.legend.compiler.typed.SortDirection;
import com.gs.legend.compiler.typed.TypedColumnSortKey;
import com.gs.legend.compiler.typed.TypedExpressionSortKey;
import com.gs.legend.compiler.typed.TypedLambda;
import com.gs.legend.compiler.typed.TypedSort;
import com.gs.legend.compiler.typed.TypedSortKey;
import com.gs.legend.compiler.typed.TypedSpec;
import com.gs.legend.model.m3.Type;

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

    public TypedSort check(AppliedFunction af, TypedSpec source,
                          TypeChecker.CompilationContext ctx) {
        String funcName = simpleName(af.function());
        var params = af.parameters();

        if (source.isRelation()) {
            // Pre-compile literal params before resolve so structural matching
            // sees real types. Skip AST-shape params (AF / ColSpec / Lambda /
            // PureCollection) which are matched structurally.
            var compiledTypes = new HashMap<Integer, ExpressionType>();
            for (int i = 1; i < params.size(); i++) {
                ValueSpecification p = params.get(i);
                if (!(p instanceof AppliedFunction) && !(p instanceof ColumnInstance)
                        && !(p instanceof LambdaFunction) && !(p instanceof PureCollection)) {
                    TypedSpec t = env.compileExpr(p, ctx);
                    compiledTypes.put(i, t.expressionType());
                }
            }
            NativeFunctionDef def = resolveOverload("sort", params, source, compiledTypes);
            // Legacy TDS: sort(Relation, String[, SortDirection]) — rewrite to
            // sort(ascending(~col)) / sort(descending(~col)) and recompile.
            if (params.size() >= 2 && params.get(1) instanceof CString) {
                return checkLegacySort(af, ctx);
            }
            return checkRelationSort(af, source, def);
        }

        return switch (funcName) {
            case "sort" -> {
                NativeFunctionDef def = resolveOverload("sort", params, source);
                yield checkCollectionSort(af, source, ctx, def);
            }
            case "sortBy" -> {
                NativeFunctionDef def = resolveOverload("sortBy", params, source);
                yield checkCollectionSortBy(af, source, ctx, def, SortDirection.ASC);
            }
            case "sortByReversed" -> {
                NativeFunctionDef def = resolveOverload("sortByReversed", params, source);
                yield checkCollectionSortBy(af, source, ctx, def, SortDirection.DESC);
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
    private TypedSort checkRelationSort(AppliedFunction af, TypedSpec source,
                                        NativeFunctionDef def) {
        List<ValueSpecification> params = af.parameters();
        var bindings = unify(def, source.expressionType());

        List<TypedSortKey> keys = params.size() > 1
                ? resolveSortInfoParams(params.get(1), source.schema())
                : List.of();

        ExpressionType outputType = resolveOutput(def, bindings, "sort()");
        return new TypedSort(source, keys, def, outputType);
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
    private TypedSort checkLegacySort(AppliedFunction af,
                                      TypeChecker.CompilationContext ctx) {
        List<ValueSpecification> params = af.parameters();

        if (!(params.get(1) instanceof CString(String colName))) {
            throw new PureCompileException(
                    "sort(): legacy syntax requires column name as String");
        }

        String dirFn = "ascending"; // 2-arg form defaults to ASC
        if (params.size() >= 3) {
            if (!(params.get(2) instanceof EnumValue ev)) {
                throw new PureCompileException(
                        "sort(): legacy syntax requires SortDirection enum "
                                + "(e.g., SortDirection.ASC), got "
                                + params.get(2).getClass().getSimpleName());
            }
            dirFn = switch (ev.value().toUpperCase()) {
                case "ASC", "ASCENDING" -> "ascending";
                case "DESC", "DESCENDING" -> "descending";
                default -> throw new PureCompileException(
                        "sort(): unknown SortDirection value '" + ev.value()
                                + "', expected ASC or DESC");
            };
        }

        // Rewrite {@code sort(source, 'col'[, dir])} → {@code sort(source, dir(~col))}
        // and recompile through the modern relation-sort path.
        var colSpec = new ColSpec(colName, null);
        var sortInfo = new AppliedFunction(dirFn, List.of(colSpec));
        var rewritten = new AppliedFunction(
                af.function(),
                List.of(params.get(0), sortInfo),
                af.hasReceiver(),
                af.sourceText(),
                af.argTexts());
        // Recompile lands in checkRelationSort via the modern arity-2 path.
        return (TypedSort) env.compileExpr(rewritten, ctx);
    }

    /**
     * Resolves sort info parameters into SortSpecs.
     * Handles single: sort(ascending(~col))
     * Handles array:  sort([ascending(~col), descending(~col2)])
     */
    private List<TypedSortKey> resolveSortInfoParams(ValueSpecification vs,
                                                     Type.Schema schema) {
        if (vs instanceof PureCollection(List<ValueSpecification> values)) {
            return values.stream()
                    .map(elem -> resolveSingleSortInfo(elem, schema))
                    .toList();
        }
        return List.of(resolveSingleSortInfo(vs, schema));
    }

    /**
     * Resolves {@code ascending(~col)} / {@code descending(~col)} / bare
     * {@code ~col} into a {@link TypedColumnSortKey}. Bare ColSpec defaults ASC.
     */
    private TypedSortKey resolveSingleSortInfo(ValueSpecification vs, Type.Schema schema) {
        if (vs instanceof AppliedFunction af) {
            String funcName = simpleName(af.function());
            if ("ascending".equals(funcName) || "asc".equals(funcName)) {
                String col = extractColumnName(af.parameters().get(0));
                validateColumn(col, schema);
                return new TypedColumnSortKey(col, SortDirection.ASC);
            }
            if ("descending".equals(funcName) || "desc".equals(funcName)) {
                String col = extractColumnName(af.parameters().get(0));
                validateColumn(col, schema);
                return new TypedColumnSortKey(col, SortDirection.DESC);
            }
        }
        if (vs instanceof ColSpec cs) {
            validateColumn(cs.name(), schema);
            return new TypedColumnSortKey(cs.name(), SortDirection.ASC);
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
    private TypedSort checkCollectionSort(AppliedFunction af, TypedSpec source,
                                          TypeChecker.CompilationContext ctx,
                                          NativeFunctionDef def) {
        List<ValueSpecification> params = af.parameters();
        var bindings = unify(def, source.expressionType());

        // Compile each lambda through the signature so U binds from the key
        // lambda before the comp lambda compiles. Capture the first (key)
        // lambda's TypedLambda for the output's sort key.
        TypedLambda keyLambda = null;
        for (int i = 1; i < params.size() && i < def.params().size(); i++) {
            if (!(params.get(i) instanceof LambdaFunction lambda)) {
                env.compileExpr(params.get(i), ctx);
                continue;
            }
            Type.Parameter sigParam = def.params().get(i);
            TypedLambda compiled = compileLambdaArg(
                    lambda, sigParam, bindings, ctx, "sort", source);
            if (keyLambda == null) keyLambda = compiled;
        }

        SortDirection direction = detectCompareDirection(params);
        // If a key lambda was supplied, emit an expression-based sort key.
        // With no key lambda (bare {@code sort(col)}) or only a comp lambda, the
        // sort is over identity — represented by an empty keys list.
        List<TypedSortKey> keys = keyLambda != null
                ? List.of(new TypedExpressionSortKey(keyLambda, direction))
                : List.of();

        ExpressionType outputType = resolveOutput(def, bindings, "sort()");
        return new TypedSort(source, keys, def, outputType);
    }

    /**
     * sortBy / sortByReversed: key lambda only, fixed direction. Uses
     * {@link #compileLambdaArg} — the full pipeline from {@code AbstractChecker}.
     */
    private TypedSort checkCollectionSortBy(AppliedFunction af, TypedSpec source,
                                            TypeChecker.CompilationContext ctx,
                                            NativeFunctionDef def,
                                            SortDirection direction) {
        var bindings = unify(def, source.expressionType());
        LambdaFunction lambda = (LambdaFunction) af.parameters().get(1);
        TypedLambda keyLambda = compileLambdaArg(
                lambda, def.params().get(1), bindings, ctx, "sortBy", source);

        ExpressionType outputType = resolveOutput(def, bindings, "sortBy()");
        return new TypedSort(source,
                List.of(new TypedExpressionSortKey(keyLambda, direction)),
                def, outputType);
    }

    /**
     * Detects sort direction from compare lambda argument order.
     * {@code {x,y|$x->compare($y)}} → ASC (natural);
     * {@code {x,y|$y->compare($x)}} → DESC (reversed — second param is receiver).
     */
    private SortDirection detectCompareDirection(List<ValueSpecification> params) {
        for (int i = 1; i < params.size(); i++) {
            if (params.get(i) instanceof LambdaFunction(
                        List<Variable> parameters, List<ValueSpecification> body)
                    && parameters.size() == 2 && !body.isEmpty()) {
                var firstBody = body.get(0);
                if (firstBody instanceof AppliedFunction af2
                        && simpleName(af2.function()).equals("compare")
                        && !af2.parameters().isEmpty()) {
                    String secondParam = parameters.get(1).name();
                    if (af2.parameters().get(0) instanceof Variable v
                            && v.name().equals(secondParam)) {
                        return SortDirection.DESC;
                    }
                }
            }
        }
        return SortDirection.ASC;
    }


    // ========== Helpers ==========

    private void validateColumn(String col, Type.Schema schema) {
        if (schema != null && !schema.columns().isEmpty()) {
            schema.requireColumn(col);
        }
    }
}
