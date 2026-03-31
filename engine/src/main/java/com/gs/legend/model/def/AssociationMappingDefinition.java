package com.gs.legend.model.def;

import java.util.List;
import java.util.Objects;

/**
 * Represents an association mapping within a {@code ###Mapping} block.
 * 
 * Pure syntax:
 * <pre>
 * model::PersonFirm: Relational
 * {
 *     AssociationMapping
 *     (
 *         employees[firm, person]: [DB]@FirmPerson,
 *         firm[person, firm]: [DB]@FirmPerson
 *     )
 * }
 * </pre>
 * 
 * Also supports XStore association mappings:
 * <pre>
 * model::PersonFirm: XStore
 * {
 *     employees: $this.firmId == $that.id,
 *     firm: $this.id == $that.firmId
 * }
 * </pre>
 * 
 * @param associationName The fully qualified association name
 * @param mappingType     "Relational" or "XStore"
 * @param properties      The property mappings within the association
 */
public record AssociationMappingDefinition(
        String associationName,
        String mappingType,
        List<AssociationPropertyMapping> properties
) {
    public AssociationMappingDefinition {
        Objects.requireNonNull(associationName, "Association name cannot be null");
        Objects.requireNonNull(mappingType, "Mapping type cannot be null");
        Objects.requireNonNull(properties, "Properties cannot be null");
        properties = List.copyOf(properties);
    }

    /**
     * A single property mapping within an association mapping.
     * 
     * @param propertyName    The property name
     * @param sourceSetId     The source set ID (from {@code [source, target]}, nullable)
     * @param targetSetId     The target set ID (nullable)
     * @param joinChain       For Relational: the join chain (nullable for XStore)
     * @param crossExpression For XStore: the cross expression string (nullable for Relational)
     */
    public record AssociationPropertyMapping(
            String propertyName,
            String sourceSetId,
            String targetSetId,
            List<JoinChainElement> joinChain,
            String crossExpression
    ) {
        public AssociationPropertyMapping {
            Objects.requireNonNull(propertyName, "Property name cannot be null");
            joinChain = joinChain != null ? List.copyOf(joinChain) : List.of();
        }
    }
}
