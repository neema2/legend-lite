package com.gs.legend.model.def;

import java.util.List;
import java.util.Objects;

/**
 * Represents a mapping include directive within a {@code ###Mapping} block.
 * 
 * Pure syntax:
 * <pre>
 * Mapping model::MyMapping
 * (
 *     include model::BaseMapping
 *     include model::OtherMapping[store::DB1 -> store::DB2]
 * )
 * </pre>
 * 
 * @param includedMappingPath  The fully qualified path of the included mapping
 * @param storeSubstitutions   Store substitutions (original → substitute pairs)
 */
public record MappingInclude(
        String includedMappingPath,
        List<StoreSubstitution> storeSubstitutions
) {
    public MappingInclude {
        Objects.requireNonNull(includedMappingPath, "Included mapping path cannot be null");
        storeSubstitutions = storeSubstitutions != null ? List.copyOf(storeSubstitutions) : List.of();
    }

    /**
     * Convenience constructor for includes without store substitutions.
     */
    public MappingInclude(String includedMappingPath) {
        this(includedMappingPath, List.of());
    }

    /**
     * A store substitution: replaces one store with another in the included mapping.
     * 
     * Pure syntax: {@code [store::Original -> store::Substitute]}
     * 
     * @param originalStore   The original store path
     * @param substituteStore The substitute store path
     */
    public record StoreSubstitution(
            String originalStore,
            String substituteStore
    ) {
        public StoreSubstitution {
            Objects.requireNonNull(originalStore, "Original store cannot be null");
            Objects.requireNonNull(substituteStore, "Substitute store cannot be null");
        }
    }
}
