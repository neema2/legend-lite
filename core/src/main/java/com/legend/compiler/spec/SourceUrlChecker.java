package com.legend.compiler.spec;

import com.legend.compiler.element.type.ExprType;
import com.legend.builtin.Pure;
import com.legend.compiler.element.type.Multiplicity;
import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.typed.TypedSourceUrl;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.parser.spec.AppliedFunction;
import com.legend.parser.spec.CString;

import java.util.List;

/**
 * {@code sourceUrl('…')} (engine {@code SourceUrlChecker}) &mdash; validated
 * against the registered {@code sourceUrl(String[1])} signature, then typed as
 * the fixed one-column semi-structured relation {@code (data:Variant)[1]}
 * (engine's rule), replacing the signature's {@code Relation<Any>} placeholder.
 */
final class SourceUrlChecker {

    private SourceUrlChecker() {
    }

    static TypedSpec check(Typer t, AppliedFunction af, Env env) {
        if (af.parameters().size() != 1 || !(af.parameters().get(0) instanceof CString url)) {
            throw new TypeInferenceException("sourceUrl expects a string-literal URL");
        }
        // Validate against the registered native signature — never ignored.
        InferenceKernel.Resolution sig = t.kernel().resolveOverload(
                t.model().findFunction(CoreFn.SOURCE_URL.parseName()),
                List.of(ExprType.one(Type.Primitive.STRING)));

        Type.RelationType schema = new Type.RelationType(List.of(new Type.Column(
                "data", new Type.ClassType(Pure.VARIANT.qualifiedName()), Multiplicity.Bounded.ONE)));
        return new TypedSourceUrl(url.value(), new ExprType(schema, sig.output().multiplicity()));
    }
}
