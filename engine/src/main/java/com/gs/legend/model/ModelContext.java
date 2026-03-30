package com.gs.legend.model;

import com.gs.legend.model.m3.Association;
import com.gs.legend.model.m3.PureClass;
import com.gs.legend.model.mapping.ClassMapping;
import com.gs.legend.model.mapping.MappingRegistry;
import com.gs.legend.model.store.Join;
import com.gs.legend.model.store.Table;

import java.util.Optional;

/**
 * Context for model lookups during Pure compilation.
 * 
 * Provides access to:
 * - Class definitions and mappings
 * - Association definitions (for navigation through relationships)
 * - Join definitions (for SQL generation)
 * 
 * This interface allows the compiler to resolve property navigation
 * through associations and generate appropriate SQL (EXISTS for to-many,
 * JOIN for explicit relational queries).
 */
public interface ModelContext {

    /**
     * Returns the mapping registry for table/class lookups.
     * Default is an empty registry (never null).
     */
    default MappingRegistry getMappingRegistry() {
        return new MappingRegistry();
    }

    /**
     * Finds a mapping for a class by name.
     * 
     * @param className Simple or qualified class name
     * @return The class mapping, if found
     */
    Optional<ClassMapping> findMapping(String className);

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
     * Returns all association navigations for a given class.
     * Each entry maps a property name to its AssociationNavigation.
     *
     * @param className The class to find association navigations for
     * @return Map of property name → AssociationNavigation (empty if none)
     */
    default java.util.Map<String, AssociationNavigation> findAllAssociationNavigations(String className) {
        return java.util.Map.of();
    }

    /**
     * Finds a join by name.
     * 
     * @param joinName The join name
     * @return The join, if found
     */
    Optional<Join> findJoin(String joinName);

    /**
     * Finds a table by name.
     * 
     * @param tableName The table name
     * @return The table, if found
     */
    Optional<Table> findTable(String tableName);

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
     * Represents navigation through an association.
     * 
     * @param association The association being navigated
     * @param sourceEnd   The end we're navigating FROM
     * @param targetEnd   The end we're navigating TO
     * @param isToMany    Whether this is a to-many navigation
     * @param join        The relational join for this association (if available)
     */
    record AssociationNavigation(
            Association association,
            Association.AssociationEnd sourceEnd,
            Association.AssociationEnd targetEnd,
            boolean isToMany,
            Join join) {
        /**
         * @return The target class name
         */
        public String targetClassName() {
            return targetEnd.targetClass();
        }
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

        // Collect all ancestors of class1 (including itself)
        var ancestors1 = new java.util.LinkedHashSet<String>();
        var queue = new java.util.ArrayDeque<PureClass>();
        queue.add(class1Opt.get());
        while (!queue.isEmpty()) {
            var cls = queue.poll();
            if (ancestors1.add(cls.qualifiedName())) {
                queue.addAll(cls.superClasses());
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
            queue.addAll(cls.superClasses());
        }
        return Optional.empty();
    }
}
