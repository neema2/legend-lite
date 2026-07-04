package com.legend.compiler.spec;

import com.legend.compiler.spec.typed.TypedProject;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.parser.spec.AppliedFunction;
import com.legend.parser.spec.AppliedProperty;
import com.legend.parser.spec.CString;
import com.legend.parser.spec.ColSpec;
import com.legend.parser.spec.ColSpecArray;
import com.legend.parser.spec.LambdaFunction;
import com.legend.parser.spec.PureCollection;
import com.legend.parser.spec.ValueSpecification;
import com.legend.parser.spec.Variable;

import java.util.ArrayList;
import java.util.List;

/**
 * {@code project} (engine {@code ProjectChecker}): the legacy TDS form
 * {@code project([lambdas], ['names'])} and bare {@code ~prop} columns DESUGAR
 * into the modern {@code ~[alias:lambda]} form (engine's own rewrite), then ONE
 * generic check types relation- and class-source alike ({@code Relation<Z>} /
 * {@code FuncColSpecArray<{C[1]->Any[*]},T>}); {@code Z} binds from the checked
 * lambda bodies. This class only desugars and emits.
 */
final class ProjectChecker {

    private ProjectChecker() {
    }

    static TypedSpec check(Typer t, AppliedFunction af, Env env) {
        AppliedFunction modern = af.parameters().size() == 3 ? legacyToModern(af) : af;
        Application a = t.checkGeneric(withMappedColumns(modern), env);
        return new TypedProject(a.args().get(0), Args.funcCols(a.args().get(1)), a.out());
    }

    /**
     * Desugar the legacy TDS {@code project(src, [p|expr, …], ['name', …])} into the
     * modern {@code project(src, ~[name:p|expr, …])} &mdash; a pure AST&rarr;AST
     * rewrite (engine {@code ProjectChecker.rewriteLegacyProject}).
     */
    private static AppliedFunction legacyToModern(AppliedFunction af) {
        List<ValueSpecification> ps = af.parameters();
        if (!(ps.get(1) instanceof PureCollection lambdas) || !(ps.get(2) instanceof PureCollection names)) {
            throw new TypeInferenceException(
                    "project(source, [lambdas], [names]) expects two collection literals");
        }
        if (lambdas.values().size() != names.values().size()) {
            throw new TypeInferenceException("project has " + lambdas.values().size()
                    + " column expression(s) but " + names.values().size() + " name(s)");
        }
        List<ColSpec> specs = new ArrayList<>(names.values().size());
        for (int i = 0; i < names.values().size(); i++) {
            if (!(names.values().get(i) instanceof CString name)) {
                throw new TypeInferenceException("expected a string-literal column name");
            }
            if (!(lambdas.values().get(i) instanceof LambdaFunction lf)) {
                throw new TypeInferenceException(
                        "a project column must be a single-parameter, single-expression lambda");
            }
            specs.add(new ColSpec(name.value(), lf));
        }
        return new AppliedFunction(af.function(), List.of(ps.get(0), new ColSpecArray(specs)));
    }

    /**
     * Normalize the column argument: a single {@code ~col} wraps into an array, and
     * a bare {@code ~prop} becomes the identity lambda {@code prop:x|$x.prop}
     * (engine's bare-reference desugar) &mdash; so the checked form is always a
     * mapped colspec array.
     */
    private static AppliedFunction withMappedColumns(AppliedFunction af) {
        if (af.parameters().size() != 2) {
            throw new TypeInferenceException("project expects (source, ~[columns])");
        }
        List<ColSpec> specs = switch (af.parameters().get(1)) {
            case ColSpec cs -> List.of(cs);
            case ColSpecArray arr -> arr.colSpecs();
            default -> throw new TypeInferenceException("project expects ~[…] column specifications");
        };
        ColSpecArray mapped = new ColSpecArray(specs.stream().map(ProjectChecker::identityIfBare).toList());
        return new AppliedFunction(af.function(), List.of(af.parameters().get(0), mapped));
    }

    /** {@code ~prop} &rarr; {@code prop:x|$x.prop} &mdash; a pass-through column is an identity mapping. */
    private static ColSpec identityIfBare(ColSpec cs) {
        if (cs.function1() != null) {
            return cs;
        }
        return new ColSpec(cs.name(), new LambdaFunction(
                List.of(new Variable("x")),
                List.of(new AppliedProperty(new Variable("x"), cs.name()))));
    }
}
