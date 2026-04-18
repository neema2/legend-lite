package com.gs.legend.compiled;

import java.util.Map;

/**
 * Compiled-state representation of a connection — how to reach a store.
 *
 * <p>{@code properties} holds connection-specific settings (JDBC URL,
 * credentials handle, model-chain target runtime, etc.) as a pure-data
 * map. Structured subtypes can land later if needed; the flat-map shape
 * keeps the sealed hierarchy small for now.
 */
public record CompiledConnection(
        String qualifiedName,
        String storeFqn,
        ConnectionKind kind,
        Map<String, String> properties,
        SourceLocation sourceLocation) implements CompiledElement {
}
