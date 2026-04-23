package com.gs.legend.compiler.typed;

import com.gs.legend.compiled.CompiledFunction;
import com.gs.legend.compiler.ExpressionType;

/**
 * {@code ClassName.all()} — the root relation for a mapped class, with the
 * class's resolved compiled mapping function attached.
 *
 * <p>{@code mappingFn} is <strong>name-resolution output</strong>: the class
 * → mapping-function binding is computed by {@code GetAllChecker} at HIR
 * construction time (via {@code env.compileMappingFunctionFor}) and attached
 * directly on the node. Matches Rust's HIR idiom where resolved refs live on
 * nodes, not in sidecar tables.
 *
 * @param className The class being materialized.
 * @param mappingFn Compiled synthetic function that produces rows for this class.
 * @param info      Type + multiplicity of the resulting relation.
 */
public record TypedGetAll(
        String className,
        CompiledFunction mappingFn,
        ExpressionType info) implements TypedSpec {}
