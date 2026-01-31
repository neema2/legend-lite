package org.finos.legend.pure.dsl;

import org.finos.legend.engine.store.Join;
import org.finos.legend.engine.store.RelationalMapping;
import org.finos.legend.engine.store.Table;
import org.finos.legend.pure.m3.Association;
import org.finos.legend.pure.m3.PureClass;

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
     * Finds a mapping for a class by name.
     * 
     * @param className Simple or qualified class name
     * @return The relational mapping, if found
     */
    Optional<RelationalMapping> findMapping(String className);

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
    default Optional<org.finos.legend.pure.dsl.definition.EnumDefinition> findEnum(String enumName) {
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
}
