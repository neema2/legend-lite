package org.finos.legend.pure.dsl.definition;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Represents a Pure M2M (Model-to-Model) mapping definition.
 * 
 * Pure syntax:
 * 
 * <pre>
 * Mapping package::MappingName
 * (
 *     TargetClass: Pure
 *     {
 *         ~src SourceClass
 *         ~filter $src.isActive == true
 *         
 *         targetProp1: $src.sourceProp1,
 *         targetProp2: $src.first + ' ' + $src.last,
 *         derivedProp: $src.value->toUpper()
 *     }
 * )
 * </pre>
 * 
 * @param qualifiedName The fully qualified mapping name
 * @param classMappings The list of M2M class mappings
 */
public record M2MMappingDefinition(
        String qualifiedName,
        List<M2MClassMappingDefinition> classMappings) implements PureDefinition {

    public M2MMappingDefinition {
        Objects.requireNonNull(qualifiedName, "Qualified name cannot be null");
        Objects.requireNonNull(classMappings, "Class mappings cannot be null");
        classMappings = List.copyOf(classMappings);
    }

    /**
     * @return The simple mapping name (without package)
     */
    public String simpleName() {
        int idx = qualifiedName.lastIndexOf("::");
        return idx >= 0 ? qualifiedName.substring(idx + 2) : qualifiedName;
    }

    /**
     * Finds a class mapping by target class name.
     */
    public Optional<M2MClassMappingDefinition> findClassMapping(String targetClassName) {
        return classMappings.stream()
                .filter(cm -> cm.targetClassName().equals(targetClassName))
                .findFirst();
    }

    /**
     * Represents an M2M class mapping within a mapping.
     * 
     * @param targetClassName  The target class being mapped to
     * @param sourceClassName  The source class being mapped from (~src)
     * @param filterExpression Optional filter expression string (~filter)
     * @param propertyMappings The property mappings with expressions
     */
    public record M2MClassMappingDefinition(
            String targetClassName,
            String sourceClassName,
            String filterExpression,
            List<M2MPropertyMappingDefinition> propertyMappings) {
        public M2MClassMappingDefinition {
            Objects.requireNonNull(targetClassName, "Target class name cannot be null");
            Objects.requireNonNull(sourceClassName, "Source class name cannot be null");
            Objects.requireNonNull(propertyMappings, "Property mappings cannot be null");
            propertyMappings = List.copyOf(propertyMappings);
        }

        /**
         * @return The optional filter expression
         */
        public Optional<String> optionalFilterExpression() {
            return Optional.ofNullable(filterExpression);
        }

        /**
         * Finds a property mapping by property name.
         */
        public Optional<M2MPropertyMappingDefinition> findPropertyMapping(String propertyName) {
            return propertyMappings.stream()
                    .filter(pm -> pm.propertyName().equals(propertyName))
                    .findFirst();
        }
    }

    /**
     * Represents an M2M property mapping with an expression.
     * 
     * @param propertyName     The target property name
     * @param expressionString The M2M expression string (e.g., "$src.firstName + '
     *                         ' + $src.lastName")
     */
    public record M2MPropertyMappingDefinition(
            String propertyName,
            String expressionString) {
        public M2MPropertyMappingDefinition {
            Objects.requireNonNull(propertyName, "Property name cannot be null");
            Objects.requireNonNull(expressionString, "Expression string cannot be null");
        }
    }
}
