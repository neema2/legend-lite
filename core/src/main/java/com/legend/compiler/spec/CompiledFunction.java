package com.legend.compiler.spec;

import com.legend.compiler.element.TypedFunction;
import com.legend.compiler.spec.typed.TypedSpec;

import java.util.List;

/**
 * A type-checked function: its (Phase-F) {@code signature} plus the type-checked
 * {@code body} statements (engine {@code CompiledFunction}). The whole point of
 * the inversion is that <em>every</em> body site &mdash; derived properties,
 * constraints, service queries, mapping transforms, user functions &mdash; is a
 * function, so this one bundle is the uniform Phase-G output.
 *
 * @param signature the function whose body was checked
 * @param body      the type-checked statements, in source order (non-empty)
 */
public record CompiledFunction(TypedFunction signature, List<TypedSpec> body) {

    public CompiledFunction {
        body = List.copyOf(body);
        if (body.isEmpty()) {
            throw new IllegalArgumentException("a compiled function body must have at least one statement");
        }
    }

    /** The body's result &mdash; the last statement, checked against the declared return. */
    public TypedSpec result() {
        return body.get(body.size() - 1);
    }
}
