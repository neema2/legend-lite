package org.finos.legend.engine.sql.ast;

/**
 * Represents an item in the SELECT list.
 * 
 * Can be:
 * - A column or expression with optional alias
 * - A * (all columns)
 * - A qualified * (table.*)
 */
public sealed interface SelectItem extends SQLNode
        permits SelectItem.AllColumns, SelectItem.ExpressionItem {

    /**
     * SELECT * or SELECT table.*
     * 
     * @param tableQualifier Optional table name for qualified star (null for
     *                       unqualified *)
     */
    record AllColumns(String tableQualifier) implements SelectItem {
        public static AllColumns unqualified() {
            return new AllColumns(null);
        }

        public static AllColumns qualified(String table) {
            return new AllColumns(table);
        }

        public boolean isQualified() {
            return tableQualifier != null;
        }
    }

    /**
     * An expression in the SELECT list with optional alias.
     * e.g., firstName, UPPER(name) AS upperName, a + b AS sum
     */
    record ExpressionItem(Expression expression, String alias) implements SelectItem {
        public static ExpressionItem of(Expression expr) {
            return new ExpressionItem(expr, null);
        }

        public static ExpressionItem of(Expression expr, String alias) {
            return new ExpressionItem(expr, alias);
        }

        public boolean hasAlias() {
            return alias != null;
        }
    }
}
