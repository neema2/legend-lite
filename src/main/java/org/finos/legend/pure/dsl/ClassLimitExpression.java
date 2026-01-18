package org.finos.legend.pure.dsl;

/**
 * AST node for limit()/take() on a ClassExpression.
 * 
 * Pure syntax: Person.all()->limit(10)
 * 
 * @param source The ClassExpression to limit
 * @param limit  Maximum number of results (null = no limit)
 * @param offset Number of results to skip (0 = no offset)
 */
public record ClassLimitExpression(
        ClassExpression source,
        Integer limit,
        int offset) implements ClassExpression {

    public static ClassLimitExpression limit(ClassExpression source, int limit) {
        return new ClassLimitExpression(source, limit, 0);
    }

    public static ClassLimitExpression offset(ClassExpression source, int offset) {
        return new ClassLimitExpression(source, null, offset);
    }

    public static ClassLimitExpression slice(ClassExpression source, int start, int stop) {
        return new ClassLimitExpression(source, stop - start, start);
    }
}
