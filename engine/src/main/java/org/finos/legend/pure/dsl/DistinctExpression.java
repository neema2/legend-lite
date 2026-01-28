package org.finos.legend.pure.dsl;

import java.util.List;

/**
 * AST for distinct operation: relation->distinct() or
 * relation->distinct(~[cols])
 */
public record DistinctExpression(
        PureExpression source,
        List<String> columns // empty = all columns
) implements RelationExpression {

    public static DistinctExpression all(PureExpression source) {
        return new DistinctExpression(source, List.of());
    }

    public static DistinctExpression columns(PureExpression source, List<String> columns) {
        return new DistinctExpression(source, columns);
    }
}
