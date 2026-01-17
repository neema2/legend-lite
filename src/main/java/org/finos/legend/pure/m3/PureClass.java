package org.finos.legend.pure.m3;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Represents a user-defined Class in the Pure type system.
 * A class is a composite type consisting of named properties.
 * 
 * This is named PureClass to avoid collision with java.lang.Class.
 * 
 * @param packagePath The package path (e.g., "model::domain")
 * @param name The class name (e.g., "Person")
 * @param properties Immutable list of properties belonging to this class
 */
public record PureClass(
        String packagePath,
        String name,
        List<Property> properties
) implements Type {
    
    public PureClass {
        Objects.requireNonNull(packagePath, "Package path cannot be null");
        Objects.requireNonNull(name, "Class name cannot be null");
        Objects.requireNonNull(properties, "Properties cannot be null");
        
        if (name.isBlank()) {
            throw new IllegalArgumentException("Class name cannot be blank");
        }
        
        // Ensure immutability
        properties = List.copyOf(properties);
    }
    
    /**
     * Convenience constructor for classes in the root package.
     */
    public PureClass(String name, List<Property> properties) {
        this("", name, properties);
    }
    
    /**
     * @return The class name (implements Type interface)
     */
    @Override
    public String typeName() {
        return name;
    }
    
    /**
     * @return The fully qualified name (package::ClassName)
     */
    public String qualifiedName() {
        return packagePath.isEmpty() ? name : packagePath + "::" + name;
    }
    
    /**
     * Finds a property by name.
     * 
     * @param propertyName The name to search for
     * @return Optional containing the property if found
     */
    public Optional<Property> findProperty(String propertyName) {
        return properties.stream()
                .filter(p -> p.name().equals(propertyName))
                .findFirst();
    }
    
    /**
     * @param propertyName The property name to look up
     * @return The property
     * @throws IllegalArgumentException if property not found
     */
    public Property getProperty(String propertyName) {
        return findProperty(propertyName)
                .orElseThrow(() -> new IllegalArgumentException(
                        "Property '" + propertyName + "' not found in class " + qualifiedName()));
    }
    
    @Override
    public String toString() {
        var sb = new StringBuilder();
        sb.append("Class ").append(qualifiedName()).append(" {\n");
        for (Property prop : properties) {
            sb.append("    ").append(prop).append(";\n");
        }
        sb.append("}");
        return sb.toString();
    }
}
