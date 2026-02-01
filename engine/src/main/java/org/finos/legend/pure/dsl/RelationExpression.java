package org.finos.legend.pure.dsl;

/**
 * Sealed interface for Relation-typed expressions in the Pure language.
 * 
 * Relation expressions operate on typed column references (Relation<T>).
 * Can be created from:
 * - Class.all()->project()
 * - #>{database.table} literal
 * 
 * Type flow:
 * Class.all()->project() → RelationExpression
 * #>{db.table} → RelationExpression
 * ->filter() → RelationExpression (stays Relation)
 * ->select() → RelationExpression
 * ->extend() → RelationExpression
 * ->groupBy() → RelationExpression
 */
public sealed interface RelationExpression extends PureExpression
        permits ProjectExpression, GroupByExpression, AggregateExpression, RelationFilterExpression,
        RelationSortExpression, RelationLimitExpression, RelationLiteral, RelationSelectExpression,
        RelationExtendExpression, JoinExpression, AsOfJoinExpression, FlattenExpression, DistinctExpression,
        RenameExpression, ConcatenateExpression, TdsLiteral {
}
