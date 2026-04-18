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
 * @param classFqn   FQN of the class being mapped.
 * @param kind       {@link MappingKind#RELATIONAL} or {@link MappingKind#M2M}.
 * @param sourceSpec Compiled source expression for this class.
 * @param sourceName The source this class is mapped from, interpreted per
 *                   {@code kind}: root table name for {@link MappingKind#RELATIONAL},
 *                   source class FQN for {@link MappingKind#M2M}. Empty when the
 *                   def record doesn't carry one (e.g., a relational class mapping
 *                   with no explicit main table).
 */
public record CompiledMappedClass(
        String classFqn,
        MappingKind kind,
        CompiledExpression sourceSpec,
        Optional<String> sourceName) {
}
