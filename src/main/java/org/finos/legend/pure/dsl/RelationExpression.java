package org.finos.legend.pure.dsl;

/**
 * Sealed interface for Relation-typed expressions in the Pure language.
 * 
 * Relation expressions operate on typed column references (Relation<T>).
 * Created by project() from ClassExpression.
 * 
 * Type flow:
 * Class.all()->project() → RelationExpression
 * ->filter() → RelationExpression (stays Relation)
 * ->groupBy() → RelationExpression
 */
public sealed interface RelationExpression extends PureExpression
        permits ProjectExpression, GroupByExpression, RelationFilterExpression {
}
