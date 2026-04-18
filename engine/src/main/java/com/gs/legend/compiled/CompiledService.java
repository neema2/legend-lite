package com.gs.legend.compiled;

import java.util.List;

/**
 * Compiled-state representation of a Pure service.
 *
 * <p>A service exposes a typed query body parameterised by {@code parameters}.
 * The body is a compiled {@link CompiledExpression} over the service's
 * declared mapping/runtime context.
 */
public record CompiledService(
        String qualifiedName,
        List<CompiledParameter> parameters,
        CompiledExpression queryBody,
        SourceLocation sourceLocation) implements CompiledElement {
}
