package org.finos.legend.pure.dsl;

/**
 * AST node for limit()/take() on a RelationExpression.
 * 
 * Pure syntax: ...->project([...])->limit(10)
 * 
 * @param source The RelationExpression to limit
 * @param limit  Maximum number of results (null = no limit)
 * @param offset Number of results to skip (0 = no offset)
 */
public record RelationLimitExpression(
        RelationExpression source,
        Integer limit,
        int offset) implements RelationExpression {

    public static RelationLimitExpression limit(RelationExpression source, int limit) {
        return new RelationLimitExpression(source, limit, 0);
    }

    public static RelationLimitExpression offset(RelationExpression source, int offset) {
        return new RelationLimitExpression(source, null, offset);
    }

    public static RelationLimitExpression slice(RelationExpression source, int start, int stop) {
        return new RelationLimitExpression(source, stop - start, start);
    }
}
