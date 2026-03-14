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
     * Finds a class by fully qualified name (exact match only).
     */
    public Optional<PureClass> findClass(String className) {
        if (className == null) return Optional.empty();
        PureClass cls = classes.get(className);
        return cls != null ? Optional.of(cls) : Optional.empty();
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

    /**
     * Serializes all classes to Pure DSL text (Class definitions).
     * Produces output like:
     *   Class meta::pkg::Person { firstName: String[1]; address: meta::pkg::Address[0..1]; }
     */
    public String toPureDsl() {
        if (classes.isEmpty()) return "";
        var sb = new StringBuilder();
        for (PureClass cls : classes.values()) {
            sb.append("Class ").append(cls.qualifiedName()).append(" {\n");
            for (var prop : cls.properties()) {
                sb.append("  ").append(prop.name()).append(": ");
                // Use qualified name for class types, simple name for primitives
                if (prop.genericType() instanceof PureClass pc) {
                    sb.append(pc.qualifiedName());
                } else {
                    sb.append(prop.genericType().typeName());
                }
                sb.append(prop.multiplicity()).append(";\n");
            }
            sb.append("}\n");
        }
        return sb.toString();
    }

    @Override
    public String toString() {
        return "TypeEnvironment[classes=" + classes.size() + "]";
    }
}
