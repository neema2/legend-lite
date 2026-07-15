package com.legend.compiler.spec;

import com.legend.compiler.element.type.ExprType;
import com.legend.compiler.spec.typed.TypedLet;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.model.spec.AppliedFunction;
import com.legend.model.spec.CString;
import com.legend.model.spec.ValueSpecification;

import java.util.List;

/**
 * {@code let name = value} (engine {@code LetChecker}): bind {@code name} to
 * {@code value}. <strong>Fully signature-driven</strong> against the real-legend-pure
 * {@code letFunction(name:String[1], value:T[m]):T[m]} &mdash; the multiplicity
 * variable {@code m} binds the value's multiplicity, so {@code resolveOutput}
 * yields the value's own type+multiplicity (multi-valued let included).
 */
final class LetChecker {

    private LetChecker() {
    }

    static TypedLet check(Typer t, AppliedFunction af, Env env) {
        List<ValueSpecification> params = af.parameters();
        if (params.size() != 2) {
            // Guard BEFORE indexing (audit: params.get(1) could IOOBE). The
            // registered 1-arg letFunction form is not wired to this checker.
            throw new TypeInferenceException(
                    "malformed let: expected (name, value), got " + params.size() + " argument(s)");
        }
        if (!(params.get(0) instanceof CString nameLit)) {
            throw new TypeInferenceException("malformed let: the variable name must be a string literal");
        }
        ExprType nameInfo = t.synth(params.get(0), env).info();   // the literal name, String[1]
        TypedSpec value = t.synth(params.get(1), env);
        InferenceKernel.Resolution r = t.kernel().resolveOverload(
                t.model().findFunction(CoreFn.LET.parseName()), List.of(nameInfo, value.info()));
        return new TypedLet(nameLit.value(), value, r.output());
    }
}
