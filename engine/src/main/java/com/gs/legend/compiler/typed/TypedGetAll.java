package com.gs.legend.compiler.typed;

import com.gs.legend.compiled.CompiledFunction;
import com.gs.legend.compiler.ExpressionType;

/**
 * {@code ClassName.all()} — the root relation for a mapped class, with the
 * class's resolved compiled mapping function attached when present.
 *
 * <p>{@code mappingFn} is <strong>name-resolution output</strong>: the class
 * → mapping-function binding is computed by {@code GetAllChecker} at HIR
 * construction time (via {@code env.tryCompileMappingFunctionFor}) and attached
 * directly on the node. Matches Rust's HIR idiom where resolved refs live on
 * nodes, not in sidecar tables.
 *
 * <p><strong>Nullable:</strong> {@code mappingFn} is {@code null} when the
 * class has no mapping in the active scope. Type-check is a front-end concern
 * — the type of {@code Class.all()} is {@code Class[*]} regardless of mapping.
 * Mapping is a back-end / link-time concern: the precise "no mapping for class X"
 * error fires at the use site in {@code SourceLowering}, not at type-check.
 *
 * @param className The class being materialized.
 * @param mappingFn Compiled synthetic function that produces rows for this class,
 *                  or {@code null} if no mapping is registered for the class.
 * @param info      Type + multiplicity of the resulting relation.
 */
public record TypedGetAll(
        String className,
        CompiledFunction mappingFn,
        ExpressionType info) implements TypedSpec {}
