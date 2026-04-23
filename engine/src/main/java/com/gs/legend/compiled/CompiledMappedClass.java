package com.gs.legend.compiled;

import java.util.Optional;

/**
 * One class→source binding inside a {@link CompiledMapping}.
 *
 * <p>The {@code mappingFunction} is the compiled synthetic function whose body
 * materializes source rows for this class — a {@code tableReference → filter →
 * joins → extend} chain for relational mappings, a {@code getAll(src) → extend}
 * chain for M2M mappings. Emitted by {@code MappingNormalizer} and compiled by
 * {@code TypeChecker.check(PureFunction)}.
 *
 * @param classFqn         FQN of the class being mapped.
 * @param kind             {@link MappingKind#RELATIONAL} or {@link MappingKind#M2M}.
 * @param mappingFunction  Compiled synthetic mapping function for this class.
 * @param sourceName       The source this class is mapped from, interpreted per
 *                         {@code kind}: root table name for {@link MappingKind#RELATIONAL},
 *                         source class FQN for {@link MappingKind#M2M}. Empty when the
 *                         def record doesn't carry one (e.g., a relational class mapping
 *                         with no explicit main table).
 */
public record CompiledMappedClass(
        String classFqn,
        MappingKind kind,
        CompiledFunction mappingFunction,
        Optional<String> sourceName) {
}
