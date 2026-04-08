package com.gs.legend.model;

import com.gs.legend.ast.ValueSpecification;
import com.gs.legend.model.m3.PureClass;
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
     * Compiler-visible view of a mapping — expressions only, no routing info.
     * Sealed interface with two variants: M2M (Pure expression-based) and
     * Relational (column-based with optional filter).
     *
     * <p>TypeChecker sees this via {@link #findMappingExpression(String)}.
     * MappingResolver never sees this — it reads from NormalizedMapping accessors.
     */
    sealed interface MappingExpression {

        /**
         * M2M (Pure) mapping: sourceSpec is the single source of truth.
         * The sourceSpec is a ValueSpecification chain synthesized by MappingNormalizer:
         * {@code getAll("SrcClass") -> filter({src|cond}) -> extend(~[prop:{src|expr}, ...])}
         *
         * @param sourceClassName  The source class being mapped from (~src)
         * @param sourceSpec       Synthesized class-space chain: getAll → filter → extend
         */
        record M2M(
                String sourceClassName,
                ValueSpecification sourceSpec) implements MappingExpression {}

        /**
         * Relational mapping: source relation is the single source of truth.
         * The sourceSpec is a ValueSpecification chain synthesized by MappingNormalizer:
         * {@code tableReference("db.TABLE") -> filter(...) -> join(...) -> extend(traverse()) -> distinct()}
         * Association traversals are embedded as extend() nodes with fn1=traverse.
         *
         * @param className        The class being mapped
         * @param sourceSpec   Synthesized Relation ValueSpec (tableRef + filter + joins + extends + distinct)
         */
        record Relational(
                String className,
                ValueSpecification sourceSpec) implements MappingExpression {}
    }

    /**
     * Finds the compiler-visible mapping expression for a class.
     * Returns only expressions — no routing info (tables, columns, joins).
     *
     * @param className Simple or qualified class name
     * @return The mapping expression, if this class has a mapping
     */
    default Optional<MappingExpression> findMappingExpression(String className) {
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
