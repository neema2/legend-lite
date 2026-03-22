package com.gs.legend.compiler.checkers;

import com.gs.legend.ast.*;
import com.gs.legend.compiler.*;
import com.gs.legend.plan.GenericType;

import java.util.*;

/**
 * Type checker for {@code groupBy()} — Relation API only.
 *
 * <p>Supports the 4 official overloads from legend-engine:
 * <pre>
 * groupBy(r:Relation<T>, cols:ColSpec<Z⊆T>,       agg:AggColSpec<...>)      → Relation<Z+R>
 * groupBy(r:Relation<T>, cols:ColSpecArray<Z⊆T>,  agg:AggColSpec<...>)      → Relation<Z+R>
 * groupBy(r:Relation<T>, cols:ColSpec<Z⊆T>,       agg:AggColSpecArray<...>) → Relation<Z+R>
 * groupBy(r:Relation<T>, cols:ColSpecArray<Z⊆T>,  agg:AggColSpecArray<...>) → Relation<Z+R>
 * </pre>
 *
 * <p>Signature-driven: compiles fn1/fn2 lambda bodies for type validation.
 * Produces {@link TypeInfo.AggColumnSpec} with resolved function + types only —
 * PlanGenerator reads fn1/fn2 from the AST directly.
 *
 * @see AggregateChecker
 */
public class GroupByChecker extends AbstractChecker {

    public GroupByChecker(TypeCheckEnv env) {
        super(env);
    }

    public TypeInfo check(AppliedFunction af, List<ValueSpecification> params,
                          TypeInfo source, TypeChecker.CompilationContext ctx) {
        if (params.size() < 3) {
            throw new PureCompileException(
                    "groupBy() requires source, group columns, and aggregate specs");
        }

        GenericType.Relation.Schema sourceSchema = source.schema();
        if (sourceSchema == null) {
            throw new PureCompileException(
                    "groupBy() requires a Relation source with a known schema");
        }

        Map<String, GenericType> resultColumns = new LinkedHashMap<>();
        List<TypeInfo.ColumnSpec> groupCols = new ArrayList<>();
        List<TypeInfo.AggColumnSpec> aggCols = new ArrayList<>();

        // --- Group columns (param 1): ~col or ~[col1, col2] ---
        List<String> groupColNames = extractColumnNames(params.get(1));
        for (String col : groupColNames) {
            if (!sourceSchema.columns().containsKey(col)) {
                throw new PureCompileException(
                        "groupBy(): group column '" + col + "' not found in source. Available: "
                                + sourceSchema.columns().keySet());
            }
            resultColumns.put(col, sourceSchema.columns().get(col));
            groupCols.add(TypeInfo.ColumnSpec.col(col));
        }

        // --- Aggregate columns (param 2): AggColSpec or AggColSpecArray ---
        List<ColSpec> aggSpecs = extractAggColSpecs(params.get(2));
        for (ColSpec cs : aggSpecs) {
            TypeInfo.AggColumnSpec acs = compileAggColSpec(cs, sourceSchema, source, ctx);
            resultColumns.put(acs.alias(), acs.castType() != null ? acs.castType() : acs.returnType());
            aggCols.add(acs);
        }

        if (resultColumns.isEmpty()) {
            throw new PureCompileException("groupBy() produced no output columns");
        }

        var schema = GenericType.Relation.Schema.withoutPivot(resultColumns);
        return TypeInfo.builder()
                .mapping(source.mapping())
                .columnSpecs(groupCols)
                .aggColumnSpecs(aggCols)
                .expressionType(ExpressionType.many(new GenericType.Relation(schema)))
                .build();
    }

    // ========== Aggregate column compilation ==========

    /**
     * Compiles a single aggregate ColSpec into an AggColumnSpec.
     *
     * <p>PCT pattern: {@code ~alias : x | $x.sourceCol : y | $y->aggFunc(args)}
     * <br>Parser produces: {@code ColSpec("alias", fn1={x→$x.sourceCol}, fn2={y→$y->aggFunc(args)})}
     *
     * <p>fn1 body can be:
     * <ul>
     *   <li>Simple property access: {@code $x.col}</li>
     *   <li>Expression: {@code $x.price * $x.quantity}</li>
     *   <li>rowMapper: {@code rowMapper($x.col1, $x.col2)} (for wavg, corr, etc.)</li>
     * </ul>
     */
    TypeInfo.AggColumnSpec compileAggColSpec(ColSpec cs,
                                             GenericType.Relation.Schema sourceSchema,
                                             TypeInfo source,
                                             TypeChecker.CompilationContext ctx) {
        String alias = cs.name();

        // --- fn1: compile map lambda ---
        if (cs.function1() == null || cs.function1().body().isEmpty()) {
            throw new PureCompileException(
                    "groupBy(): aggregate spec '" + alias + "' is missing value extraction lambda (fn1)");
        }
        LambdaFunction fn1 = cs.function1();
        String fn1Param = fn1.parameters().isEmpty() ? null : fn1.parameters().get(0).name();
        GenericType sourceRelType = new GenericType.Relation(sourceSchema);
        TypeChecker.CompilationContext fn1Ctx = bindLambdaParam(ctx, fn1Param, sourceRelType, source);
        TypeInfo fn1Result = compileLambdaBody(fn1, fn1Ctx);

        // --- fn2: compile reduce lambda ---
        if (cs.function2() == null || cs.function2().body().isEmpty()) {
            throw new PureCompileException(
                    "groupBy(): aggregate spec '" + alias + "' is missing aggregate lambda (fn2)");
        }
        LambdaFunction fn2 = cs.function2();
        String fn2Param = fn2.parameters().isEmpty() ? null : fn2.parameters().get(0).name();
        // fn2 param has type K[*] — the collection of mapped values
        TypeChecker.CompilationContext fn2Ctx = ctx.withLambdaParam(fn2Param, fn1Result.type());
        TypeInfo fn2Result = compileLambdaBody(fn2, fn2Ctx);

        // --- Resolve aggregate function from fn2 body ---
        var fn2Body = fn2.body().get(0);
        AppliedFunction fn2Af = unwrapCast(fn2Body);
        String funcName = simpleName(fn2Af.function());
        var registry = BuiltinFunctionRegistry.instance();
        var defs = registry.resolve(funcName);
        if (defs.isEmpty()) {
            throw new PureCompileException(
                    "groupBy(): unknown aggregate function '" + funcName + "' in spec '" + alias + "'");
        }
        NativeFunctionDef resolved = defs.get(0);

        // --- Extract cast type (if fn2 is wrapped in cast()) ---
        GenericType castType = extractCastGenericType(fn2Body);

        // --- Return type: use fn2 result, refined by cast if present ---
        GenericType returnType = fn2Result.type();
        // For type-variable aggregates (min, max, sum, etc.), refine to source column type
        if (returnType == GenericType.Primitive.NUMBER && fn1Result.type() != null
                && fn1Result.type().isNumeric()) {
            returnType = fn1Result.type();
        }

        return new TypeInfo.AggColumnSpec(alias, resolved, returnType, castType);
    }

    // ========== Helpers ==========

    /**
     * Extracts ColSpec list from AggColSpec or AggColSpecArray parameter.
     */
    public static List<ColSpec> extractAggColSpecs(ValueSpecification vs) {
        if (vs instanceof ClassInstance ci) {
            if (ci.value() instanceof ColSpecArray(List<ColSpec> specs)) {
                return specs;
            }
            if (ci.value() instanceof ColSpec cs) {
                return List.of(cs);
            }
        }
        throw new PureCompileException(
                "groupBy() aggregate parameter must be ~col:... or ~[...], got: "
                        + vs.getClass().getSimpleName());
    }

    /**
     * Unwraps a cast() wrapper to get the inner AppliedFunction.
     * E.g., cast($y->plus(), @Integer) → the plus() AppliedFunction.
     * Returns the body as-is if no cast wrapper.
     */
    private AppliedFunction unwrapCast(ValueSpecification body) {
        if (body instanceof AppliedFunction af) {
            if ("cast".equals(simpleName(af.function()))) {
                // cast(innerFunc, @Type) — unwrap to inner
                if (!af.parameters().isEmpty()
                        && af.parameters().get(0) instanceof AppliedFunction inner) {
                    return inner;
                }
                // cast(Variable, @Type) — parser quirk, default to plus()
                return af;
            }
            return af;
        }
        throw new PureCompileException(
                "groupBy(): fn2 body must be a function call, got: "
                        + body.getClass().getSimpleName());
    }

    /**
     * Extracts cast target as GenericType from a cast() wrapper.
     * Returns null if body is not wrapped in cast().
     */
    private GenericType extractCastGenericType(ValueSpecification body) {
        if (body instanceof AppliedFunction af
                && "cast".equals(simpleName(af.function()))) {
            for (var p : af.parameters()) {
                if (p instanceof GenericTypeInstance(String fullPath)) {
                    String typeName = simpleName(fullPath);
                    return GenericType.fromTypeName(typeName);
                }
            }
        }
        return null;
    }
}
