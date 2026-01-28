package org.finos.legend.pure.dsl;

/**
 * First expression: source->first()
 * 
 * Returns the first element of a collection, or null if empty.
 * Translates to LIMIT 1 in SQL.
 * 
 * Pure syntax:
 * Person.all()->first()
 * Person.all()->filter(...)->first()
 */
public record FirstExpression(PureExpression source) implements PureExpression {
}
