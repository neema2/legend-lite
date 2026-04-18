package com.gs.legend.compiled;

import java.util.List;

/**
 * Compiled-state representation of a Pure mapping.
 *
 * <p>Consists of one {@link CompiledMappedClass} per class the mapping
 * covers. Each mapped class carries its compiled source-spec
 * ({@link CompiledExpression}) produced by {@code MappingNormalizer}
 * then type-checked by {@code TypeChecker}.
 */
public record CompiledMapping(
        String qualifiedName,
        List<CompiledMappedClass> mappedClasses,
        SourceLocation sourceLocation) implements CompiledElement {
}
