package com.gs.legend.compiled;

import com.gs.legend.model.m3.Multiplicity;
import com.gs.legend.model.m3.Type;

import java.util.List;

/**
 * Compiled-state representation of a Pure user function.
 *
 * <p>The {@code body} is standalone-compiled against the declared signature
 * (not against any particular call site). Query-time call sites
 * monomorphize on top of this standalone body — transient, not stored.
 * Recursive functions are unsupported under the current macro-expansion
 * strategy and surface as compile errors.
 */
public record CompiledFunction(
        String qualifiedName,
        List<CompiledParameter> parameters,
        Type returnType,
        Multiplicity returnMultiplicity,
        CompiledExpression body,
        SourceLocation sourceLocation) implements CompiledElement {
}
