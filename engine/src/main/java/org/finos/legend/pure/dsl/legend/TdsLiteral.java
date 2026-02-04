package org.finos.legend.pure.dsl.legend;

import java.util.List;

/**
 * Represents a TDS (Tabular Data Set) literal in the Pure AST.
 * 
 * Example syntax:
 * #TDS
 * col1:Type1, col2
 * val1, val2
 * val3, val4
 * #
 */
public record TdsLiteral(
        List<TdsColumn> columns,
        List<List<Object>> rows) implements Expression {

    /**
     * Column definition with name and optional type.
     */
    public record TdsColumn(String name, String type) {
        public static TdsColumn of(String name) {
            return new TdsColumn(name, null);
        }

        public static TdsColumn of(String name, String type) {
            return new TdsColumn(name, type);
        }

        public boolean isVariant() {
            return type != null &&
                    (type.contains("Variant") || type.equalsIgnoreCase("Variant"));
        }
    }

    public TdsLiteral {
        columns = List.copyOf(columns);
        rows = rows.stream()
                .map(List::copyOf)
                .toList();
    }

    /**
     * Returns just the column names for backward compatibility.
     */
    public List<String> columnNames() {
        return columns.stream().map(TdsColumn::name).toList();
    }

    public static TdsLiteral of(List<TdsColumn> columns, List<List<Object>> rows) {
        return new TdsLiteral(columns, rows);
    }
}
