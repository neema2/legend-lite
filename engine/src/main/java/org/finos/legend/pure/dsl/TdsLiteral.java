package org.finos.legend.pure.dsl;

import java.util.List;

/**
 * AST node for inline TDS (Tabular Data Set) literals.
 * 
 * Syntax:
 * {@code
 * #TDS
 *   col1, col2, col3
 *   val1, val2, val3
 *   val4, val5, val6
 * #
 * }
 *
 * @param columnNames List of column names from the header row
 * @param rows        List of data rows, each row is a list of values
 */
public record TdsLiteral(
        List<String> columnNames,
        List<List<Object>> rows) implements RelationExpression {
}
