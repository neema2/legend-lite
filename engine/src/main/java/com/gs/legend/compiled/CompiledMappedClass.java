package com.gs.legend.compiled;

import java.util.Optional;

/**
 * One class→source binding inside a {@link CompiledMapping}.
 *
 * <p>The {@code sourceSpec} is the compiled {@code ValueSpecification}
 * expression emitted by {@code MappingNormalizer}: a {@code Relation} chain
 * for relational mappings, a projection of another class's properties for
 * M2M mappings.
 *
 * @param classFqn       FQN of the class being mapped.
 * @param kind           {@link MappingKind#RELATIONAL} or {@link MappingKind#M2M}.
 * @param sourceSpec     Compiled source expression for this class.
 * @param rootTableFqn   Root table FQN for relational mappings; empty for M2M.
 * @param sourceClassFqn Source class FQN for M2M mappings; empty for relational.
 */
public record CompiledMappedClass(
        String classFqn,
        MappingKind kind,
        CompiledExpression sourceSpec,
        Optional<String> rootTableFqn,
        Optional<String> sourceClassFqn) {
}
