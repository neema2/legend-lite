package com.gs.legend.compiled;

import java.util.List;

/**
 * Compiled-state representation of a runtime — binds mappings to the
 * connections that serve their stores.
 *
 * @param mappingFqns FQNs of the mappings this runtime serves.
 * @param connections Per-store connection bindings (store FQN → connection FQN).
 */
public record CompiledRuntime(
        String qualifiedName,
        List<String> mappingFqns,
        List<CompiledRuntimeConnection> connections,
        SourceLocation sourceLocation) implements CompiledElement {
}
