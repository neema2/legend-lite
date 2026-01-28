package org.finos.legend.pure.dsl.m2m;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Represents a Model-to-Model class mapping.
 * 
 * Pure syntax:
 * <pre>
 * TargetClass[optionalTag]: Pure
 * {
 *     ~src SourceClass
 *     ~filter $src.isActive == true
 *     
 *     targetProp1: $src.sourceProp1,
 *     targetProp2: $src.first + ' ' + $src.last
 * }
 * </pre>
 * 
 * @param targetClassName The target class being mapped to
 * @param tag Optional mapping tag (e.g., for disambiguation)
 * @param sourceClassName The source class being mapped from (~src)
 * @param filter Optional filter expression (~filter)
 * @param propertyMappings The property mappings
 */
public record M2MClassMapping(
        String targetClassName,
        String tag,
        String sourceClassName,
        M2MExpression filter,
        List<M2MPropertyMapping> propertyMappings
) {
    public M2MClassMapping {
        Objects.requireNonNull(targetClassName, "Target class name cannot be null");
        Objects.requireNonNull(sourceClassName, "Source class name cannot be null");
        Objects.requireNonNull(propertyMappings, "Property mappings cannot be null");
        propertyMappings = List.copyOf(propertyMappings);
    }
    
    /**
     * Creates an M2M class mapping without tag or filter.
     */
    public static M2MClassMapping of(
            String targetClassName,
            String sourceClassName,
            List<M2MPropertyMapping> propertyMappings) {
        return new M2MClassMapping(targetClassName, null, sourceClassName, null, propertyMappings);
    }
    
    /**
     * @return The optional tag for this mapping
     */
    public Optional<String> optionalTag() {
        return Optional.ofNullable(tag);
    }
    
    /**
     * @return The optional filter expression
     */
    public Optional<M2MExpression> optionalFilter() {
        return Optional.ofNullable(filter);
    }
    
    /**
     * Finds a property mapping by target property name.
     */
    public Optional<M2MPropertyMapping> findPropertyMapping(String propertyName) {
        return propertyMappings.stream()
                .filter(pm -> pm.propertyName().equals(propertyName))
                .findFirst();
    }
    
    @Override
    public String toString() {
        var sb = new StringBuilder();
        sb.append(targetClassName);
        if (tag != null) {
            sb.append("[").append(tag).append("]");
        }
        sb.append(": Pure\n{\n");
        sb.append("    ~src ").append(sourceClassName).append("\n");
        if (filter != null) {
            sb.append("    ~filter ").append(filter).append("\n");
        }
        for (var pm : propertyMappings) {
            sb.append("    ").append(pm).append(",\n");
        }
        sb.append("}");
        return sb.toString();
    }
}
