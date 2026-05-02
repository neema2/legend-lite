package com.gs.legend.model;

import com.gs.legend.model.m3.Primitive;
import com.gs.legend.model.m3.PureClass;
import com.gs.legend.model.m3.Type;
import com.gs.legend.model.store.Table;
import java.util.Map;
import java.util.Optional;

/**
 * Context for model lookups during Pure compilation.
 * 
 * Provides access to:
 * - Class definitions and mappings
 * - Association definitions (for navigation through relationships)
 * - Enum definitions
 * 
 * This interface allows the compiler to resolve property navigation
 * through associations and type-check mapping expressions.
 */
public interface ModelContext {

    /**
     * Finds a PureClass by name.
     * 
     * @param className Simple or qualified class name
     * @return The PureClass, if found
     */
    Optional<PureClass> findClass(String className);

    /**
     * Finds an association by the property name used to navigate it.
     * 
     * For example, if Person has a property 'addresses' that navigates
     * to Address via an association, this method finds that association.
     * 
     * @param fromClassName The source class name
     * @param propertyName  The property name used for navigation
     * @return The association and navigation details, if found
     */
    Optional<AssociationNavigation> findAssociationByProperty(String fromClassName, String propertyName);

    /**
     * Finds a table by database FQN and table name.
     * 
     * @param db   The database FQN (e.g., "store::TestDB")
     * @param name The table name within that database (e.g., "T_PERSON")
     * @return The table, if found
     */
    Optional<Table> findTable(String db, String name);

    /**
     * Finds an enum definition by name.
     * 
     * @param enumName Simple or qualified enum name
     * @return The EnumDefinition, if found
     */
    default Optional<com.gs.legend.model.def.EnumDefinition> findEnum(String enumName) {
        return Optional.empty();
    }

    /**
     * Canonical name → {@link Type} resolver. Single entry point for all kind-agnostic
     * type lookup. Returns the first match in order:
     *
     * <ol>
     *   <li>Built-in primitive via {@link BuiltinRegistry#findPrimitive} (FQN-keyed)</li>
     *   <li>User-defined class (wrapped in {@link Type.ClassType} during phase 2.5c overlap)</li>
     *   <li>User-defined enum (wrapped in {@link Type.EnumType} during phase 2.5c overlap)</li>
     * </ol>
     *
     * <p>The "primitives first" order matches {@link Type#resolve}'s historical fast path —
     * primitives dominate property-type lookups (e.g., 100K classes × 50 properties where
     * most property types are Integer / String / Boolean). Checking primitives first avoids
     * two HashMap misses per primitive lookup.
     *
     * <p>FQN-only by contract. Simple-name resolution is upstream's problem via
     * {@code NameResolver} + {@code ImportScope}; every {@code PureModelBuilder} seeds its
     * initial {@code ImportScope} with all built-in FQNs so simple names get rewritten before
     * they reach here.
     *
     * @param name Fully qualified type name
     * @return The resolved {@link Type}, or empty if not a known primitive, class, or enum
     */
    default Optional<Type> findType(String name) {
        return Primitive.findByFqn(name).map(p -> (Type) p)
                .or(() -> findClass(name).map(c -> (Type) new Type.ClassType(c.qualifiedName())))
                .or(() -> findEnum(name).map(e -> new Type.EnumType(e.qualifiedName())));
    }

    /**
     * Checks if a value is valid for a given enum type.
     * 
     * @param enumName  The enum type name
     * @param valueName The enum value to check
     * @return true if the value is valid for the enum
     */
    default boolean hasEnumValue(String enumName, String valueName) {
        return findEnum(enumName).map(e -> e.hasValue(valueName)).orElse(false);
    }

    /**
     * Returns all association navigations for a class (lightweight, no physical join info).
     * Used by GetAllChecker for recursive compilation of target class expressions.
     *
     * @param className The class to look up associations for
     * @return Map of property name → AssociationNavigation
     */
    default Map<String, AssociationNavigation> findAllAssociationNavigations(String className) {
        return Map.of();
    }

    /**
     * Finds user-defined function definitions by name (FQN or simple name).
     *
     * @param name Function name (qualified or simple)
     * @return List of matching {@link com.gs.legend.model.m3.PureFunction}s (empty if none found;
     *         multiple for overloads). Parse-layer {@link com.gs.legend.model.def.FunctionDefinition}
     *         is internal to {@link com.gs.legend.model.PureModelBuilder}; consumers receive the
     *         typed downstream form.
     */
    default java.util.List<com.gs.legend.model.m3.PureFunction> findFunction(String name) {
        return java.util.List.of();
    }

    /**
     * Returns the FQN of the synthetic mapping function that materializes
     * rows for the given class in the active mapping scope.
     *
     * <p>Overlay provided by {@code MappingNormalizer.modelContext()} so
     * {@link com.gs.legend.compiler.TypeChecker} can resolve per-class
     * mapping function bodies without a direct {@code NormalizedMapping}
     * dependency. Base implementation returns empty — only the per-query
     * overlay knows about active mapping scope.
     *
     * @param className Simple or qualified class name
     * @return Synthetic mapping function FQN, if this class has a mapping
     */
    default Optional<String> findMappingFunctionFqn(String className) {
        return Optional.empty();
    }

    /**
     * Inverse of {@link #findMappingFunctionFqn}: function FQN → class FQN.
     *
     * <p>Used by {@link com.gs.legend.compiler.TypeChecker} when compiling
     * a synthetic mapping function, to recover the materialized class FQN
     * for binding-multiplicity validation in {@code ExtendChecker}. Base
     * implementation returns empty — only the per-query overlay supplied
     * by {@code MappingNormalizer.modelContext()} knows the binding.
     *
     * @param fnFqn Function FQN
     * @return Materialized class FQN, if {@code fnFqn} is a synthetic
     *         mapping function in the active scope
     */
    default Optional<String> findClassForMappingFunction(String fnFqn) {
        return Optional.empty();
    }

    /**
     * Compiler-visible association navigation info.
     * TypeChecker uses this to resolve association-contributed property types.
     *
     * @param targetClassName The class being navigated TO
     * @param isToMany        Whether this is a to-many navigation
     */
    record AssociationNavigation(
            String targetClassName,
            boolean isToMany) {}

    /**
     * {@code true} if the class at {@code childFqn} equals {@code parentFqn} or transitively
     * extends it. Walks {@link com.gs.legend.model.m3.PureClass#superClassFqns()} recursively
     * via {@link #findClass}, so cross-project lazy-loaded classes resolve through the same
     * lookup path as property navigation.
     *
     * <p>Returns {@code false} if either class is not found — callers that need to
     * distinguish "unknown class" from "not a subtype" should call {@link #findClass}
     * explicitly first.
     */
    default boolean isClassSubtype(String childFqn, String parentFqn) {
        if (childFqn.equals(parentFqn)) return true;
        return findClass(childFqn)
                .map(c -> c.superClassFqns().stream().anyMatch(s -> isClassSubtype(s, parentFqn)))
                .orElse(false);
    }

    /**
     * Finds the lowest common ancestor (LCA) of two classes using BFS on the
     * superclass hierarchy. Returns empty if no common ancestor is found.
     *
     * <p>Uses BFS for now (sufficient for single-inheritance and simple diamonds).
     * TODO: Upgrade to C3 linearization for proper MRO in complex hierarchies.
     */
    default Optional<PureClass> findLowestCommonAncestor(String className1, String className2) {
        var class1Opt = findClass(className1);
        var class2Opt = findClass(className2);
        if (class1Opt.isEmpty() || class2Opt.isEmpty())
            return Optional.empty();

        // Collect all ancestors of class1 (including itself). Walks superclasses via FQN lookup
        // so lazy-loaded classes resolve through findClass() without requiring superClasses to
        // have been eagerly populated.
        var ancestors1 = new java.util.LinkedHashSet<String>();
        var queue = new java.util.ArrayDeque<PureClass>();
        queue.add(class1Opt.get());
        while (!queue.isEmpty()) {
            var cls = queue.poll();
            if (ancestors1.add(cls.qualifiedName())) {
                for (String superFqn : cls.superClassFqns()) {
                    findClass(superFqn).ifPresent(queue::add);
                }
            }
        }

        // BFS class2's ancestor chain, return first match
        queue.add(class2Opt.get());
        var visited = new java.util.HashSet<String>();
        while (!queue.isEmpty()) {
            var cls = queue.poll();
            if (!visited.add(cls.qualifiedName()))
                continue;
            if (ancestors1.contains(cls.qualifiedName())) {
                return Optional.of(cls);
            }
            for (String superFqn : cls.superClassFqns()) {
                findClass(superFqn).ifPresent(queue::add);
            }
        }
        return Optional.empty();
    }
}
