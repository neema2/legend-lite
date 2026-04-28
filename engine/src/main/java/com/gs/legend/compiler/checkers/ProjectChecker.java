package com.gs.legend.compiler.checkers;

import com.gs.legend.ast.*;
import com.gs.legend.compiler.*;
import com.gs.legend.compiler.typed.TypedLambda;
import com.gs.legend.compiler.typed.TypedProject;
import com.gs.legend.compiler.typed.TypedProjectionCol;
import com.gs.legend.compiler.typed.TypedPropertyAccess;
import com.gs.legend.compiler.typed.TypedSpec;
import com.gs.legend.model.m3.Type;

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

    public TypedProject check(AppliedFunction af, TypedSpec source,
                          TypeChecker.CompilationContext ctx) {
        List<ValueSpecification> params = af.parameters();
        NativeFunctionDef def = resolveOverload("project", params, source);

        // Legacy TDS desugar: rewrite arity-3 form to arity-2 and recompile
        // through the modern signature-driven path.
        if (def.arity() == 3) {
            AppliedFunction rewritten = rewriteLegacyProject(af,
                    (PureCollection) params.get(1), (PureCollection) params.get(2));
            // Recompile lands back in this checker's modern arity-2 branch \u2192 TypedProject.
            return (TypedProject) env.compileExpr(rewritten, ctx);
        }

        var bindings = unify(def, source.expressionType());
        List<ColSpec> colSpecs = extractColSpecs(params.get(1));
        Type.FunctionType ft = extractFunctionType(def.params().get(1));
        Type resolvedParamType = resolve(ft.params().get(0).type(), bindings,
                "project() lambda param");

        Map<String, Type> projectedColumns = new LinkedHashMap<>();
        List<TypedProjectionCol> projections = new ArrayList<>(colSpecs.size());

        for (ColSpec cs : colSpecs) {
            String alias = cs.name();
            LambdaFunction lambda = cs.function1();
            if (lambda == null) {
                // Bare ~prop — synthesize identity lambda {x | $x.prop}.
                lambda = new LambdaFunction(
                        List.of(new Variable("x")),
                        List.of(new AppliedProperty(alias, List.of(new Variable("x")))));
            }

            String paramName = lambda.parameters().isEmpty() ? "x"
                    : lambda.parameters().get(0).name();
            TypeChecker.CompilationContext lambdaCtx = bindLambdaParam(
                    ctx, paramName, resolvedParamType, source);
            TypedSpec body = compileLambdaBody(lambda, lambdaCtx);

            // Association path is now a field on {@link TypedPropertyAccess};
            // previously required a sidecar lookup.
            Optional<List<String>> associationPath = Optional.empty();
            if (body instanceof TypedPropertyAccess tpa) {
                associationPath = tpa.associationPath();
            }
            TypedLambda expr = GroupByChecker.buildTypedLambda(
                    lambda, resolvedParamType, body);

            projectedColumns.put(alias, body.type());
            projections.add(new TypedProjectionCol(alias, expr, associationPath));
        }

        Type.Schema resultSchema = Type.Schema.withoutPivot(projectedColumns);
        return new TypedProject(source, projections, def,
                ExpressionType.one(new Type.Relation(resultSchema)));
    }

    // ========== Legacy TDS Desugaring ==========

    /**
     * Rewrites legacy TDS arity-3 project() to Relation DSL arity-2.
     *
     * <pre>
     * project([{e|$e.dept}, {e|$e.sal}], ['dept', 'sal'])
     *   → project(~[dept:e|$e.dept, sal:e|$e.sal])
     * </pre>
     *
     * <p>This is a pure AST→AST transform. The rewritten node flows through
     * the same signature-driven type checking as the Relation DSL syntax.
     */
    private static AppliedFunction rewriteLegacyProject(
            AppliedFunction af, PureCollection lambdas, PureCollection aliases) {
        List<ValueSpecification> fns = lambdas.values();
        List<ValueSpecification> ids = aliases.values();

        if (fns.size() != ids.size()) {
            throw new PureCompileException(
                    "project() legacy syntax: lambda count (" + fns.size()
                            + ") must match alias count (" + ids.size() + ")");
        }

        // Zip lambdas + aliases → ColSpec nodes
        List<ColSpec> colSpecs = new ArrayList<>();
        for (int i = 0; i < fns.size(); i++) {
            if (!(ids.get(i) instanceof CString cs))
                throw new PureCompileException(
                        "project() legacy syntax: alias[" + i + "] must be a String literal");
            if (!(fns.get(i) instanceof LambdaFunction lf))
                throw new PureCompileException(
                        "project() legacy syntax: function[" + i + "] must be a lambda");
            colSpecs.add(new ColSpec(cs.value(), lf));
        }

        var colSpecArray = new ColSpecArray(colSpecs);

        // Build new arity-2 AF: project(source, colSpecArray)
        return new AppliedFunction(
                af.function(),
                List.of(af.parameters().get(0), colSpecArray),
                af.hasReceiver(),
                af.sourceText(),
                af.argTexts());
    }

    // ========== Helpers ==========

    /** Extracts ColSpec list from a FuncColSpecArray parameter. */
    private static List<ColSpec> extractColSpecs(ValueSpecification param) {
        if (param instanceof ColSpecArray(List<ColSpec> specs)) {
            return specs;
        }
        throw new PureCompileException(
                "project() param 2 must be a FuncColSpecArray (~[...]), got "
                        + param.getClass().getSimpleName());
    }

}
