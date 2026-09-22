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

    @Override
    public TypedSpec withChildren(java.util.List<TypedSpec> kids) {
        TypedSpec.expectChildren(kids, 2, "TypedMap");
        return new TypedMap(kids.get(0), (TypedLambda) kids.get(1), info);
    }

    /** The property name of a SINGLE-HOP auto-map node
     * ({@code map(src, v|$v.prop)} — pure's dot-rule as a node, whether
     * user-written or Typer-emitted), or null for any other shape. THE
     * canonical link-reader (D3): a navigation path is a chain of
     * these; the resolver's ingress adapter (Pipelines.chainForm) and
     * the lowering's path readers consume hops ONLY through it. */
    public static @com.legend.base.Nullable String singleHopProperty(TypedSpec spec) {
        if (spec instanceof TypedMap m
                && m.mapper() instanceof TypedLambda ml
                && ml.body().size() == 1
                && ml.parameters().size() == 1
                && ml.body().get(0) instanceof TypedPropertyAccess pa
                && pa.source() instanceof TypedVariable v
                && v.name().equals(ml.parameters().get(0))) {
            return pa.property();
        }
        return null;
    }
    @Override
    public TypedSpec withInfo(ExprType info) {
        return new TypedMap(source, mapper, info);
    }
}
