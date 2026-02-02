package org.finos.legend.pure.dsl.legend;

/**
 * A Relation literal: #>{store::DB.TABLE}#
 * 
 * Example: #>{store::PersonDB.T_PERSON}#
 */
public record RelationLiteral(
        String databaseRef, // e.g., "store::PersonDB"
        String tableName // e.g., "T_PERSON"
) implements Expression {
}
