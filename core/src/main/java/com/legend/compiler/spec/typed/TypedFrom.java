package com.legend.compiler.spec.typed;

import com.legend.compiler.spec.ExprType;

import java.util.List;
import java.util.Optional;

/**
 * An execution-context binding {@code ->from(runtime)} / {@code ->from(mapping,
 * runtime)} (engine {@code TypedFrom}) &mdash; a type passthrough
 * ({@code Relation<T>[1]} / {@code T[*]}) that slots the referenced mapping and
 * runtime onto the node for the back-end.
 *
 * @param source  the value being bound to an execution context
 * @param mapping the mapping reference (the M2M three-argument form), if present
 * @param runtime the runtime reference, if present
 * @param info    the source type unchanged
 */
public record TypedFrom(TypedSpec source, Optional<TypedPackageableRef> mapping,
                        Optional<TypedPackageableRef> runtime, ExprType info) implements TypedSpec {
    @Override
    public List<TypedSpec> children() {
        List<TypedSpec> out = new java.util.ArrayList<>();
        out.add(source);
        mapping.ifPresent(out::add);
        runtime.ifPresent(out::add);
        return out;
    }
}
