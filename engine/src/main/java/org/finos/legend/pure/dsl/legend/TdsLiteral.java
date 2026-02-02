package org.finos.legend.pure.dsl.legend;

import java.util.List;

/**
 * Represents a TDS (Tabular Data Set) literal in the Pure AST.
 * 
 * Example syntax:
 * #TDS
 * col1, col2
 * val1, val2
 * val3, val4
 * #
 */
public record TdsLiteral(
        List<String> columnNames,
        List<List<Object>> rows) implements Expression {

    public TdsLiteral {
        columnNames = List.copyOf(columnNames);
        rows = rows.stream()
                .map(List::copyOf)
                .toList();
    }

    public static TdsLiteral of(List<String> columnNames, List<List<Object>> rows) {
        return new TdsLiteral(columnNames, rows);
    }
}
