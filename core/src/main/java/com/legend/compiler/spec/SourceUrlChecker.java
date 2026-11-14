package com.legend.compiler.spec;

import com.legend.error.TypeInferenceException;

import com.legend.compiler.element.type.ExprType;
import com.legend.builtin.Pure;
import com.legend.compiler.element.type.Multiplicity;
import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.typed.TypedSourceUrl;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.protocol.spec.AppliedFunction;
import com.legend.protocol.spec.CString;

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
        // (lite-INTERNAL vocabulary: resolve by exact identity, the bare
        // name is not in the user namespace)
        InferenceKernel.Resolution sig = t.kernel().resolveOverload(
                t.model().findFunction(com.legend.builtin.Pure.Lite.SOURCE_URL),
                List.of(ExprType.one(Type.Primitive.STRING)));

        Type.RelationType schema = new Type.RelationType(List.of(new Type.Column(
                "data", new Type.ClassType(com.legend.compiler.element.type.PlatformTypes.VARIANT), Multiplicity.Bounded.ONE)));
        return new TypedSourceUrl(url.value(),
                new ExprType(Type.relation(schema), sig.output().multiplicity()));
    }
}
