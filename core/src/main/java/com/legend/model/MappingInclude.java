package com.legend.model;

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
 * @param mappingPath    fully-qualified path of the included mapping; for an
 *                       {@code include dataspace X} marker, the data space's path
 * @param substitutions  store substitutions applied during inclusion;
 *                       empty list when none were written
 * @param dataSpace      an {@code include dataspace X} MARKER, as the model reader
 *                       leaves it: name resolution rewrites it, once, to the data
 *                       space's default execution context's mapping (engine
 *                       {@code DataSpaceIncludedMappingHandler}), so no walker of a
 *                       mapping's includes past name resolution ever meets one
 *                       ({@code NameResolver.resolveDataSpaceIncludes}; 2026-11-14,
 *                       until then the reader dropped the include)
 */
public record MappingInclude(String mappingPath, List<StoreSubstitution> substitutions, boolean dataSpace) {
    public MappingInclude {
        Objects.requireNonNull(mappingPath, "Mapping path cannot be null");
        substitutions = substitutions == null ? List.of() : List.copyOf(substitutions);
        if (dataSpace && !substitutions.isEmpty()) {
            throw new IllegalArgumentException("an include dataspace takes no store substitutions");
        }
    }

    /** A mapping include ({@code include M}, {@code include M [a -> b]}). */
    public MappingInclude(String mappingPath, List<StoreSubstitution> substitutions) {
        this(mappingPath, substitutions, false);
    }

    /** A single {@code oldStore -> newStore} pair inside an include's bracket list. */
    public record StoreSubstitution(String originalStore, String replacementStore) {
        public StoreSubstitution {
            Objects.requireNonNull(originalStore, "Original store cannot be null");
            Objects.requireNonNull(replacementStore, "Replacement store cannot be null");
        }
    }
}
