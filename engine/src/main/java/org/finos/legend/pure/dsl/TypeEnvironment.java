package org.finos.legend.pure.dsl;

import org.finos.legend.pure.m3.PureClass;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Type environment for compilation — provides class definitions with
 * property types and multiplicities.
 * 
 * Populated from:
 * - PureModelBuilder (QueryService path — user-defined models)
 * - ProcessorSupport (PCT path — Pure interpreter metadata)
 * 
 * Phase 1: Class definitions only.
 * Future: Table/column types, lambda parameter types, etc.
 */
public final class TypeEnvironment {

    private static final TypeEnvironment EMPTY = new TypeEnvironment(Map.of());

    private final Map<String, PureClass> classes;

    private TypeEnvironment(Map<String, PureClass> classes) {
        this.classes = Collections.unmodifiableMap(classes);
    }

    /**
     * Returns an empty type environment (no class metadata available).
     */
    public static TypeEnvironment empty() {
        return EMPTY;
    }

    /**
     * Creates a type environment from a map of class definitions.
     * 
     * @param classes Map of qualified class name to PureClass
     */
    public static TypeEnvironment of(Map<String, PureClass> classes) {
        if (classes == null || classes.isEmpty()) {
            return EMPTY;
        }
        return new TypeEnvironment(new HashMap<>(classes));
    }

    /**
     * Finds a class by qualified or simple name.
     * Tries exact match first, then falls back to simple name matching.
     */
    public Optional<PureClass> findClass(String className) {
        if (className == null) return Optional.empty();

        // Exact match
        PureClass cls = classes.get(className);
        if (cls != null) return Optional.of(cls);

        // Try simple name match (last segment after ::)
        String simpleName = className.contains("::") 
                ? className.substring(className.lastIndexOf("::") + 2)
                : className;
        for (var entry : classes.entrySet()) {
            if (entry.getKey().endsWith("::" + simpleName) || entry.getValue().name().equals(simpleName)) {
                return Optional.of(entry.getValue());
            }
        }

        return Optional.empty();
    }

    /**
     * @return Unmodifiable map of qualified class names to PureClass definitions
     */
    public Map<String, PureClass> classes() {
        return classes;
    }

    /**
     * @return true if this environment has any class definitions
     */
    public boolean hasClasses() {
        return !classes.isEmpty();
    }

    /**
     * @return The number of classes in this environment
     */
    public int classCount() {
        return classes.size();
    }

    @Override
    public String toString() {
        return "TypeEnvironment[classes=" + classes.size() + "]";
    }
}
