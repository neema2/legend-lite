package org.finos.legend.pure.dsl;

/**
 * Sealed interface for Class-typed expressions in the Pure language.
 * 
 * Class expressions operate on Class instances (not yet converted to Relation).
 * To convert to Relation, use project().
 * 
 * Type flow:
 * Class.all() → ClassExpression
 * ->filter() → ClassExpression (stays Class)
 * ->project() → RelationExpression (converts to Relation)
 */
public sealed interface ClassExpression extends PureExpression
                permits ClassAllExpression, ClassFilterExpression, ClassSortByExpression, ClassLimitExpression {
}
