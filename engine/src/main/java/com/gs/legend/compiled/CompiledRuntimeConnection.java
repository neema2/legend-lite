package com.gs.legend.compiled;

/**
 * One store→connection binding inside a {@link CompiledRuntime}.
 *
 * @param storeFqn      FQN of the store this connection serves.
 * @param connectionFqn FQN of the {@link CompiledConnection} to use for that store.
 */
public record CompiledRuntimeConnection(
        String storeFqn,
        String connectionFqn) {
}
