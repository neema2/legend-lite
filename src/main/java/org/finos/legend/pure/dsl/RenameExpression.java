package org.finos.legend.pure.dsl;

/**
 * AST for rename operation: relation->rename(~oldCol, ~newCol)
 */
public record RenameExpression(
        PureExpression source,
        String oldColumnName,
        String newColumnName) implements RelationExpression {
}
