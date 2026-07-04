package com.legend.compiler.spec.typed;

import com.legend.compiler.spec.ExprType;

import java.util.List;

/**
 * A type-checked object-graph property access {@code $source.property} (engine
 * {@code TypedPropertyAccess}). The result {@link #info()} carries the property's
 * type with its multiplicity <em>composed</em> along the path (e.g. a
 * {@code [*]} receiver makes a {@code [1]} property {@code [*]}).
 *
 * <p>Currently nested for multi-hop ({@code $x.a.b} = access(access($x,a),b)).
 * The §5 flat {@code associationPath} representation is a lowering-time
 * refinement and is deferred until Phase I exists to consume it.
 */
public record TypedPropertyAccess(TypedSpec source, String property, ExprType info) implements TypedSpec {
    @Override
    public List<TypedSpec> children() {
        return List.of(source);
    }
}
