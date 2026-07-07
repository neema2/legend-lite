package com.legend.compiler.spec.typed;

import com.legend.compiler.element.type.ExprType;

import java.util.List;

/**
 * A type-checked {@code map} (engine {@code TypedMap}) &mdash; one node for the
 * collection overloads ({@code map<T,V|m>(T[m], {T[1]->V[m]}):V[m]} family) and
 * the relation row-map; lowering disambiguates by the source's type. The result
 * type is the mapper body's (bound generically via the signature's {@code V};
 * this node is <em>emission</em>, not a bespoke rule).
 *
 * @param source the collection or relation being mapped
 * @param mapper the per-element / per-row transform
 * @param info   the result type &mdash; the mapper's body type at the signature's result multiplicity
 */
public record TypedMap(TypedSpec source, TypedLambda mapper, ExprType info) implements TypedSpec {
    @Override
    public List<TypedSpec> children() {
        return List.of(source, mapper);
    }
}
