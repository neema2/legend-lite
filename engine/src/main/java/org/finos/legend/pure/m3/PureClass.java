package org.finos.legend.pure.m3;

import org.finos.legend.pure.dsl.definition.StereotypeApplication;
import org.finos.legend.pure.dsl.definition.TaggedValue;

import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Represents a user-defined Class in the Pure type system.
 * A class is a composite type consisting of named properties.
 * 
 * This is named PureClass to avoid collision with java.lang.Class.
 * 
 * @param packagePath  The package path (e.g., "model::domain")
 * @param name         The class name (e.g., "Person")
 * @param superClasses List of superclasses (resolved references)
 * @param properties   Immutable list of properties belonging to this class
 * @param stereotypes  Stereotype annotations on this class (e.g., nlq.rootEntity)
 * @param taggedValues Tagged value annotations on this class (e.g., nlq.description)
 */
public record PureClass(
        String packagePath,
        String name,
        List<PureClass> superClasses,
        List<Property> properties,
        List<StereotypeApplication> stereotypes,
        List<TaggedValue> taggedValues) implements Type {

    public PureClass {
        Objects.requireNonNull(packagePath, "Package path cannot be null");
        Objects.requireNonNull(name, "Class name cannot be null");
        Objects.requireNonNull(properties, "Properties cannot be null");

        if (name.isBlank()) {
            throw new IllegalArgumentException("Class name cannot be blank");
        }

        // Ensure immutability
        superClasses = superClasses == null ? List.of() : List.copyOf(superClasses);
        properties = List.copyOf(properties);
        stereotypes = stereotypes == null ? List.of() : List.copyOf(stereotypes);
        taggedValues = taggedValues == null ? List.of() : List.copyOf(taggedValues);
    }

    /**
     * Constructor for backwards compatibility (no annotations).
     */
    public PureClass(String packagePath, String name, List<PureClass> superClasses, List<Property> properties) {
        this(packagePath, name, superClasses, properties, List.of(), List.of());
    }

    /**
     * Convenience constructor for classes with no superclasses.
     */
    public PureClass(String packagePath, String name, List<Property> properties) {
        this(packagePath, name, List.of(), properties);
    }

    /**
     * Convenience constructor for classes in the root package with no superclasses.
     */
    public PureClass(String name, List<Property> properties) {
        this("", name, List.of(), properties);
    }

    /**
     * Gets the value of a tagged value by profile and tag name.
     */
    public String getTagValue(String profileName, String tagName) {
        for (TaggedValue tv : taggedValues) {
            if (tv.profileName().equals(profileName) && tv.tagName().equals(tagName)) {
                return tv.value();
            }
        }
        return null;
    }

    /**
     * Checks if a stereotype is applied to this class.
     */
    public boolean hasStereotype(String profileName, String stereotypeName) {
        for (StereotypeApplication sa : stereotypes) {
            if (sa.profileName().equals(profileName) && sa.stereotypeName().equals(stereotypeName)) {
                return true;
            }
        }
        return false;
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
     * Finds a property by name, searching this class and then all superclasses.
     * Uses depth-first traversal of the inheritance hierarchy.
     * 
     * @param propertyName The name to search for
     * @return Optional containing the property if found
     */
    public Optional<Property> findProperty(String propertyName) {
        // First search local properties
        Optional<Property> local = properties.stream()
                .filter(p -> p.name().equals(propertyName))
                .findFirst();
        if (local.isPresent()) {
            return local;
        }

        // Then search superclasses (depth-first)
        Set<String> visited = new HashSet<>();
        return findPropertyInSuperclasses(propertyName, visited);
    }

    /**
     * Helper method for recursive property search through inheritance chain.
     * Tracks visited classes to avoid infinite loops with diamond inheritance.
     */
    private Optional<Property> findPropertyInSuperclasses(String propertyName, Set<String> visited) {
        for (PureClass superClass : superClasses) {
            String superQualifiedName = superClass.qualifiedName();
            if (visited.contains(superQualifiedName)) {
                continue; // Already visited, skip to prevent cycles
            }
            visited.add(superQualifiedName);

            // Check super's local properties first
            Optional<Property> found = superClass.properties().stream()
                    .filter(p -> p.name().equals(propertyName))
                    .findFirst();
            if (found.isPresent()) {
                return found;
            }

            // Recursively search super's superclasses
            found = superClass.findPropertyInSuperclasses(propertyName, visited);
            if (found.isPresent()) {
                return found;
            }
        }
        return Optional.empty();
    }

    /**
     * Returns all properties including inherited ones.
     * Local properties have precedence over inherited ones if there are name
     * conflicts.
     * 
     * @return List of all properties (local + inherited)
     */
    public List<Property> allProperties() {
        if (superClasses.isEmpty()) {
            return properties;
        }

        // Collect inherited properties, giving precedence to local ones
        Set<String> localPropertyNames = new HashSet<>();
        for (Property p : properties) {
            localPropertyNames.add(p.name());
        }

        java.util.List<Property> result = new java.util.ArrayList<>(properties);
        Set<String> visited = new HashSet<>();
        collectInheritedProperties(result, localPropertyNames, visited);

        return List.copyOf(result);
    }

    private void collectInheritedProperties(java.util.List<Property> result,
            Set<String> collectedNames, Set<String> visitedClasses) {
        for (PureClass superClass : superClasses) {
            String superQualifiedName = superClass.qualifiedName();
            if (visitedClasses.contains(superQualifiedName)) {
                continue;
            }
            visitedClasses.add(superQualifiedName);

            for (Property prop : superClass.properties()) {
                if (!collectedNames.contains(prop.name())) {
                    result.add(prop);
                    collectedNames.add(prop.name());
                }
            }

            // Recursively collect from super's superclasses
            superClass.collectInheritedProperties(result, collectedNames, visitedClasses);
        }
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
        sb.append("Class ").append(qualifiedName());
        if (!superClasses.isEmpty()) {
            sb.append(" extends ");
            sb.append(String.join(", ", superClasses.stream()
                    .map(PureClass::qualifiedName)
                    .toList()));
        }
        sb.append(" {\n");
        for (Property prop : properties) {
            sb.append("    ").append(prop).append(";\n");
        }
        sb.append("}");
        return sb.toString();
    }
}
