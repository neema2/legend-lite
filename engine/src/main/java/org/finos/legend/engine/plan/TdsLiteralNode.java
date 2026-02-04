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
 * @param columns Columns with names and optional types
 * @param rows    Data rows, each row is a list of values
 */
public record TdsLiteralNode(
        List<TdsColumn> columns,
        List<List<Object>> rows) implements RelationNode {

    /**
     * Column definition with name and optional type for TDS literals.
     * Type is used for proper SQL generation (e.g., casting Variant columns to
     * JSON).
     */
    public record TdsColumn(String name, String type) {
        public static TdsColumn of(String name) {
            return new TdsColumn(name, null);
        }

        public static TdsColumn of(String name, String type) {
            return new TdsColumn(name, type);
        }

        /**
         * Returns true if this column has a Variant type
         */
        public boolean isVariant() {
            return type != null &&
                    (type.contains("Variant") || type.equalsIgnoreCase("Variant"));
        }
    }

    /**
     * Returns just the column names for backward compatibility.
     */
    public List<String> columnNames() {
        return columns.stream().map(TdsColumn::name).toList();
    }

    @Override
    public <T> T accept(RelationNodeVisitor<T> visitor) {
        return visitor.visit(this);
    }
}
