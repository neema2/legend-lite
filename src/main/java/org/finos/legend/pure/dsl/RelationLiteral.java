package org.finos.legend.pure.dsl;

/**
 * A Relation literal starting from a database table reference.
 * 
 * Syntax: #>{store::DatabaseRef.TABLE_NAME}
 * 
 * Example:
 * 
 * <pre>
 * #>{store::PersonDB.T_PERSON}
 *     ->select(~firstName, ~lastName)
 *     ->filter(x | $x.age > 30)
 * </pre>
 */
public record RelationLiteral(
        String databaseRef, // e.g., "store::PersonDB"
        String tableName // e.g., "T_PERSON"
) implements RelationExpression {
}
