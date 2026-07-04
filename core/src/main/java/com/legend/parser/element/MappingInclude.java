package com.legend.parser.element;

import java.util.List;
import java.util.Objects;

/**
 * A mapping {@code include} clause &mdash; a structural concept shared by both
 * the legacy ({@link LegacyMappingDefinition}) and canonical
 * ({@link MappingDefinition}) mapping forms (it survives the legacy&rarr;canonical
 * rewrite unchanged), so it lives as one top-level record rather than a nested
 * copy on each.
 *
 * <pre>
 *   include other::BaseMapping
 *   include other::BaseMapping [oldStore -&gt; newStore, oldStore2 -&gt; newStore2]
 * </pre>
 *
 * @param mappingPath    fully-qualified path of the included mapping
 * @param substitutions  store substitutions applied during inclusion;
 *                       empty list when none were written
 */
public record MappingInclude(String mappingPath, List<StoreSubstitution> substitutions) {
    public MappingInclude {
        Objects.requireNonNull(mappingPath, "Mapping path cannot be null");
        substitutions = substitutions == null ? List.of() : List.copyOf(substitutions);
    }

    /** A single {@code oldStore -> newStore} pair inside an include's bracket list. */
    public record StoreSubstitution(String originalStore, String replacementStore) {
        public StoreSubstitution {
            Objects.requireNonNull(originalStore, "Original store cannot be null");
            Objects.requireNonNull(replacementStore, "Replacement store cannot be null");
        }
    }
}
