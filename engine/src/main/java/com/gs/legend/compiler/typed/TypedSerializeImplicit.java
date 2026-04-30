package com.gs.legend.compiler.typed;

import com.gs.legend.compiler.ExpressionType;

import java.util.List;

/**
 * Implicit form of {@link TypedSerialize}: marks a bare class-typed query
 * root that needs an automatic JSON-envelope wrap at lowering time.
 *
 * <p>Synthesized at the tail of {@link com.gs.legend.compiler.MappingResolver}
 * for queries like {@code P.all()->filter(...)} whose root has type
 * {@code Class[*]} but does not end in an explicit {@code ->serialize()} or
 * {@code ->graphFetch()}. Legend-lite's invariant is that the database
 * assembles JSON, so a bare class collection at the query root carries
 * implicit-serialize semantics; this marker makes that explicit in the IR
 * so the lowerer can route through the same JSON-envelope mechanism as
 * explicit {@link TypedSerialize} without any post-hoc root special-casing.
 *
 * <p>Why a marker rather than synthesizing a real {@code TypedSerialize}:
 * a real {@code TypedSerialize} is the type-checker's output and carries a
 * resolved {@code NativeFunctionDef} plus a String[1] {@code ExpressionType}.
 * Reconstructing both outside the type checker is rebuilding mini-compiler
 * machinery for no semantic gain. The marker carries only what lowering
 * actually consumes — source plus the synthesized leaf-only fetch tree —
 * and delegates {@link #info()} to its source so type/multiplicity continue
 * to read as {@code Class[*]} (which is what they actually are; the
 * serialize-to-JSON conversion is a lowering-time concern, not a type one).
 *
 * <p>{@code children} is the legend-lite-default tree: one
 * {@link TypedGraphTree} leaf per entry in the resolved store's
 * {@code propertyToColumn} map. Mirrors the legacy {@code Lowerer.wrapBareClass}
 * tree synthesis, just lifted from lowering into resolution where the data
 * naturally lives.
 */
public record TypedSerializeImplicit(
        TypedSpec source,
        List<TypedGraphTree> children
) implements TypedSpec {
    public TypedSerializeImplicit {
        children = List.copyOf(children);
    }

    /**
     * Type/multiplicity passes through from the source: the marker is a
     * lowering-time tag, not a type-changing operator. The runtime result
     * format ({@code Variant} JSON) is decided by {@code ResultFormat.from()}
     * via the marker's node identity, not by inspecting {@code info().type()}.
     */
    @Override
    public ExpressionType info() {
        return source.info();
    }
}
