package com.gs.legend.model.m3;

import com.gs.legend.model.def.StereotypeApplication;
import com.gs.legend.model.def.TaggedValue;

import java.util.*;

/**
 * Represents a user-defined Class in the Pure type system.
 * A class is a composite type consisting of named properties.
 *
 * <p>This is named PureClass to avoid collision with java.lang.Class.
 *
 * <p>Phase A of the Bazel cross-project dependency work replaced the resolved
 * {@code List<PureClass> superClasses} field with {@code List<String> superClassFqns}
 * so cross-project superclass references don't force their targets to load eagerly.
 * See {@code docs/BAZEL_IMPLEMENTATION_PLAN.md} §2. Inheritance-aware property lookup
 * ({@link #findProperty}, {@link #allProperties}) takes a {@link com.gs.legend.model.ModelContext}
 * so the superclass chain can be walked via {@code ctx.findClass} lazily.
 * {@link #findLocalProperty} remains as a context-free local-only primitive.
 *
 * @param packagePath     The package path (e.g., "model::domain")
 * @param name            The class name (e.g., "Person")
 * @param superClassFqns  Fully qualified names of superclasses (canonical form)
 * @param properties      Immutable list of properties belonging to this class
 * @param stereotypes     Stereotype annotations on this class (e.g., nlq.rootEntity)
 * @param taggedValues    Tagged value annotations on this class (e.g., nlq.description)
 */
public record PureClass(
        String packagePath,
        String name,
        List<String> superClassFqns,
        List<Property> properties,
        List<StereotypeApplication> stereotypes,
        List<TaggedValue> taggedValues) implements TypeDecl {

    public PureClass {
        Objects.requireNonNull(packagePath, "Package path cannot be null");
        Objects.requireNonNull(name, "Class name cannot be null");
        Objects.requireNonNull(properties, "Properties cannot be null");

        if (name.isBlank()) {
            throw new IllegalArgumentException("Class name cannot be blank");
        }

        // Ensure immutability
        superClassFqns = superClassFqns == null ? List.of() : List.copyOf(superClassFqns);
        properties = List.copyOf(properties);
        stereotypes = stereotypes == null ? List.of() : List.copyOf(stereotypes);
        taggedValues = taggedValues == null ? List.of() : List.copyOf(taggedValues);
    }

    /**
     * Convenience constructor — no annotations (stereotypes/taggedValues default to empty).
     */
    public PureClass(String packagePath, String name, List<String> superClassFqns, List<Property> properties) {
        this(packagePath, name, superClassFqns, properties, List.of(), List.of());
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
     * @return The class name (implements {@link TypeDecl} interface)
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
     * O(n) lookup of a LOCAL property by name. Does not walk the superclass chain.
     *
     * <p>For inheritance-aware lookup, callers should use
     * {@code ModelContext.findProperty(PureClass, String)} which walks the chain via
     * {@code findClass}. This method exists as a pure-data primitive used by that walker
     * internally and by code that genuinely wants local-only semantics.
     *
     * <p>Scan is a simple loop — typical class has 5-30 properties, making this
     * effectively free. If profiling ever shows it as a hotspot, swap the implementation;
     * the method signature stays the same.
     */
    public Optional<Property> findLocalProperty(String propertyName) {
        for (Property p : properties) {
            if (p.name().equals(propertyName)) {
                return Optional.of(p);
            }
        }
        return Optional.empty();
    }

    /**
     * Inheritance-aware property lookup. Walks the superclass chain via
     * {@code ctx.findClass(fqn)} with cycle detection. Local properties take precedence
     * over inherited ones.
     *
     * <p>This is the canonical way to look up properties; use {@link #findLocalProperty}
     * only when explicitly skipping the inheritance walk.
     */
    public Optional<Property> findProperty(String propertyName, com.gs.legend.model.ModelContext ctx) {
        Optional<Property> local = findLocalProperty(propertyName);
        if (local.isPresent()) return local;
        return findPropertyInSuperclasses(propertyName, ctx, new HashSet<>());
    }

    private Optional<Property> findPropertyInSuperclasses(String propertyName,
            com.gs.legend.model.ModelContext ctx, Set<String> visited) {
        for (String fqn : superClassFqns) {
            if (!visited.add(fqn)) continue; // cycle guard
            Optional<PureClass> superOpt = ctx.findClass(fqn);
            if (superOpt.isEmpty()) continue;
            PureClass superClass = superOpt.get();
            Optional<Property> found = superClass.findLocalProperty(propertyName);
            if (found.isPresent()) return found;
            found = superClass.findPropertyInSuperclasses(propertyName, ctx, visited);
            if (found.isPresent()) return found;
        }
        return Optional.empty();
    }

    /**
     * Inheritance-aware getter that throws if the property is not found. Equivalent to
     * {@code findProperty(name, ctx).orElseThrow(...)}.
     */
    public Property getProperty(String propertyName, com.gs.legend.model.ModelContext ctx) {
        return findProperty(propertyName, ctx)
                .orElseThrow(() -> new IllegalArgumentException(
                        "Property '" + propertyName + "' not found in class " + qualifiedName()));
    }

    /**
     * All properties of this class including inherited ones, walking the superclass chain
     * via {@code ctx.findClass}. Local properties take precedence over inherited properties
     * with the same name. Preserves declaration order: local properties first, then inherited.
     */
    public List<Property> allProperties(com.gs.legend.model.ModelContext ctx) {
        if (superClassFqns.isEmpty()) {
            return properties;
        }
        Set<String> collectedNames = new HashSet<>();
        for (Property p : properties) {
            collectedNames.add(p.name());
        }
        List<Property> result = new java.util.ArrayList<>(properties);
        collectInheritedProperties(ctx, result, collectedNames, new HashSet<>());
        return List.copyOf(result);
    }

    private void collectInheritedProperties(com.gs.legend.model.ModelContext ctx,
            List<Property> result, Set<String> collectedNames, Set<String> visited) {
        for (String fqn : superClassFqns) {
            if (!visited.add(fqn)) continue; // cycle guard
            Optional<PureClass> superOpt = ctx.findClass(fqn);
            if (superOpt.isEmpty()) continue;
            PureClass superClass = superOpt.get();
            for (Property prop : superClass.properties()) {
                if (collectedNames.add(prop.name())) {
                    result.add(prop);
                }
            }
            superClass.collectInheritedProperties(ctx, result, collectedNames, visited);
        }
    }

    @Override
    public String toString() {
        var sb = new StringBuilder();
        sb.append("Class ").append(qualifiedName());
        if (!superClassFqns.isEmpty()) {
            sb.append(" extends ");
            sb.append(String.join(", ", superClassFqns));
        }
        sb.append(" {\n");
        for (Property prop : properties) {
            sb.append("    ").append(prop).append(";\n");
        }
        sb.append("}");
        return sb.toString();
    }
}
