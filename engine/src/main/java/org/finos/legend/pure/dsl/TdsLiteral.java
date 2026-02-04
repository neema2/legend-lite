package org.finos.legend.pure.dsl;

import java.util.List;

/**
 * AST node for inline TDS (Tabular Data Set) literals.
 * 
 * Syntax:
 * {@code
 * #TDS
 *   col1:Type1, col2, col3:Type3
 *   val1, val2, val3
 *   val4, val5, val6
 * #
 * }
 *
 * @param columns List of column definitions with names and optional types
 * @param rows    List of data rows, each row is a list of values
 */
public record TdsLiteral(
                List<TdsColumn> columns,
                List<List<Object>> rows) implements RelationExpression {

        /**
         * Column definition with name and optional type.
         * If type is null, it's inferred from the data.
         */
        public record TdsColumn(String name, String type) {
                /**
                 * Creates a column with inferred type (type = null)
                 */
                public static TdsColumn of(String name) {
                        return new TdsColumn(name, null);
                }

                /**
                 * Creates a column with explicit type
                 */
                public static TdsColumn of(String name, String type) {
                        return new TdsColumn(name, type);
                }

                /**
                 * Returns true if this column has an explicit Variant type
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
}
