package org.finos.legend.pure.dsl;

import java.util.List;

/**
 * Select specific columns from a Relation.
 * 
 * Syntax: relation->select(~col1, ~col2, ...)
 * 
 * Example:
 * 
 * <pre>
 * #>{store::DB.T_PERSON}->select(~firstName, ~lastName, ~age)
 * </pre>
 */
public record RelationSelectExpression(
                PureExpression source,
                List<String> columns) implements RelationExpression {
}
