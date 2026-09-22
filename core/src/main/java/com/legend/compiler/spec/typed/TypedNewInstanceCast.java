package com.legend.compiler.spec.typed;

import com.legend.compiler.element.type.ExprType;

import java.util.List;
import java.util.Objects;

/**
 * A checked {@code ^Class($srcExpr)} MAPPING CAST &mdash; feeds
 * {@code srcExpr} (an upstream class value) through {@code classFqn}'s
 * active mapping to produce {@code classFqn} instances. The normalizer
 * emits it for class-typed model-to-model bindings
 * ({@code department: $src.department} where the property's type is
 * itself M2M-mapped); the resolver composes it during class-source
 * extraction (H5). It never reaches the lowerer.
 *
 * @param classFqn the TARGET class the source value casts into
 * @param source   the upstream class value being fed through the mapping
 * @param info     {@code classFqn} at the source's multiplicity
 */
public record TypedNewInstanceCast(String classFqn, TypedSpec source,
                                   ExprType info,
                                   @com.legend.base.Nullable String targetSetId)
        implements TypedSpec {

    public TypedNewInstanceCast {
        Objects.requireNonNull(classFqn, "classFqn");
        Objects.requireNonNull(source, "source");
    }

    /** No declared set route on the originating mapping line. */
    public TypedNewInstanceCast(String classFqn, TypedSpec source,
            ExprType info) {
        this(classFqn, source, info, null);
    }

    @Override
    public List<TypedSpec> children() {
        return List.of(source);
    }

    @Override
    public TypedSpec withChildren(java.util.List<TypedSpec> kids) {
        TypedSpec.expectChildren(kids, 1, "TypedNewInstanceCast");
        return new TypedNewInstanceCast(classFqn, kids.get(0), info, targetSetId);
    }
    @Override
    public TypedSpec withInfo(ExprType info) {
        return new TypedNewInstanceCast(classFqn, source, info, targetSetId);
    }
}
