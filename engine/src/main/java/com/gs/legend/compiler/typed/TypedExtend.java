package com.gs.legend.compiler.typed;

import com.gs.legend.compiler.ExpressionType;
import com.gs.legend.compiler.NativeFunctionDef;

import java.util.List;

/**
 * Relational extend: {@code source->extend(~[col1:expr, col2:window, col3:traverse, ...])}.
 *
 * <p>Each extension column is a sealed {@link TypedExtendCol} subvariant — scalar,
 * window, or traverse. PlanGenerator pattern-matches on the subvariant; no flags.
 *
 * <p>When a top-level {@code traverse()} clause appears as an extend parameter
 * (not wrapping a ColSpec), the resolved hops are recorded as a {@link TraversalSpec}
 * in {@link #traversalSpecs}. A {@link com.gs.legend.ast.PureCollection} of
 * traverses (multi-join DynaFunction mappings) yields one spec per element;
 * each association-extend ColSpec contributes its own spec. Each spec's hops
 * apply <em>before</em> the column extensions are evaluated, exposing the
 * terminal tables' columns to every scalar/window column in {@code extensions}.
 *
 * <p>Spec boundaries matter to lowering: hops <em>within</em> one spec chain
 * ({@code source -> hop0 -> hop1 -> ...}) but each spec re-roots at the
 * source alias, matching legacy plangen semantics.
 */
public record TypedExtend(
        TypedSpec source,
        List<TraversalSpec> traversalSpecs,
        List<TypedExtendCol> extensions,
        NativeFunctionDef def,
        ExpressionType info
) implements TypedNative {
    public TypedExtend {
        traversalSpecs = List.copyOf(traversalSpecs);
        extensions = List.copyOf(extensions);
    }
}
