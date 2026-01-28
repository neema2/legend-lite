package org.finos.legend.engine.sql.ast;

import java.util.List;

/**
 * Represents items in the FROM clause.
 * 
 * Supports:
 * - Simple table references: FROM table, FROM schema.table
 * - Table aliases: FROM table AS t
 * - Table functions: FROM service('/path'), FROM table('store', 'name'), FROM
 * class('pkg::Class')
 * - Subqueries: FROM (SELECT ...) AS sub
 * - JOINs: FROM a JOIN b ON condition
 */
public sealed interface FromItem extends SQLNode
        permits FromItem.TableRef, FromItem.TableFunction, FromItem.SubQuery, FromItem.JoinedTable {

    /**
     * Reference to a table: schema.table AS alias
     */
    record TableRef(String schema, String table, String alias) implements FromItem {
        public static TableRef of(String table) {
            return new TableRef(null, table, null);
        }

        public static TableRef of(String table, String alias) {
            return new TableRef(null, table, alias);
        }

        public static TableRef qualified(String schema, String table) {
            return new TableRef(schema, table, null);
        }

        public static TableRef qualified(String schema, String table, String alias) {
            return new TableRef(schema, table, alias);
        }

        public boolean hasSchema() {
            return schema != null;
        }

        public boolean hasAlias() {
            return alias != null;
        }

        /**
         * Returns the effective name to use in queries (alias if present, otherwise
         * table name).
         */
        public String effectiveName() {
            return alias != null ? alias : table;
        }
    }

    /**
     * Table function call: service('/path'), table('store', 'name'),
     * class('pkg::Class')
     * 
     * This is the key Legend-Engine extension to standard SQL.
     */
    record TableFunction(String functionName, List<Expression> arguments, String alias) implements FromItem {
        public static TableFunction service(String path, String alias) {
            return new TableFunction("service", List.of(Expression.stringLiteral(path)), alias);
        }

        public static TableFunction table(String store, String tableName, String alias) {
            return new TableFunction("table", List.of(
                    Expression.stringLiteral(store),
                    Expression.stringLiteral(tableName)), alias);
        }

        public static TableFunction tableShort(String qualifiedName, String alias) {
            return new TableFunction("table", List.of(Expression.identifier(qualifiedName)), alias);
        }

        public static TableFunction clazz(String qualifiedClassName, String alias) {
            return new TableFunction("class", List.of(Expression.identifier(qualifiedClassName)), alias);
        }

        public boolean hasAlias() {
            return alias != null;
        }

        public boolean isServiceFunction() {
            return "service".equalsIgnoreCase(functionName);
        }

        public boolean isTableFunction() {
            return "table".equalsIgnoreCase(functionName);
        }

        public boolean isClassFunction() {
            return "class".equalsIgnoreCase(functionName);
        }
    }

    /**
     * Subquery in FROM: (SELECT ...) AS alias
     */
    record SubQuery(SelectStatement query, String alias) implements FromItem {
        public SubQuery {
            if (alias == null || alias.isBlank()) {
                throw new IllegalArgumentException("Subquery must have an alias");
            }
        }
    }

    /**
     * Joined tables: a JOIN b ON condition
     */
    record JoinedTable(FromItem left, JoinType joinType, FromItem right, Expression condition) implements FromItem {

        public enum JoinType {
            INNER("INNER JOIN"),
            LEFT_OUTER("LEFT OUTER JOIN"),
            RIGHT_OUTER("RIGHT OUTER JOIN"),
            FULL_OUTER("FULL OUTER JOIN"),
            CROSS("CROSS JOIN");

            private final String sql;

            JoinType(String sql) {
                this.sql = sql;
            }

            public String toSql() {
                return sql;
            }
        }
    }
}
