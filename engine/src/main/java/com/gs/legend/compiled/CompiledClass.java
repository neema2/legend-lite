package com.gs.legend.compiled;

import java.util.List;
import java.util.Map;

/**
 * Compiled-state representation of a Pure class.
 *
 * <p>Holds the class's own properties, compiled derived properties,
 * compiled constraints, superclass FQNs, and annotations. Injected
 * properties from associations / Extensions live in separate
 * {@link CompiledBackRefFragment}s and are merged at lookup time — this
 * record is never mutated after construction.
 */
public record CompiledClass(
        String qualifiedName,
        List<String> superClassFqns,
        List<CompiledProperty> properties,
        List<CompiledDerivedProperty> derivedProperties,
        List<CompiledConstraint> constraints,
        List<StereotypeRef> stereotypes,
        Map<String, String> taggedValues,
        SourceLocation sourceLocation) implements CompiledElement {
}
