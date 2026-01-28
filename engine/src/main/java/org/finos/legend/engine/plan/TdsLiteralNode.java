package org.finos.legend.engine.plan;

import java.util.List;

/**
 * IR node for inline TDS (Tabular Data Set) literals.
 * 
 * Generates SQL using VALUES clause:
 * {@code
 * SELECT * FROM (VALUES
 *   (val1, val2, val3),
 *   (val4, val5, val6)
 * ) AS _tds(col1, col2, col3)
 * }
 *
 * @param columnNames Column names for the inline table
 * @param rows        Data rows, each row is a list of values
 */
public record TdsLiteralNode(
        List<String> columnNames,
        List<List<Object>> rows) implements RelationNode {

    @Override
    public <T> T accept(RelationNodeVisitor<T> visitor) {
        return visitor.visit(this);
    }
}
