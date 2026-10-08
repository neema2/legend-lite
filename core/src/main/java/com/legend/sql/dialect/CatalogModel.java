// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import java.util.ArrayList;
import java.util.List;

/**
 * A Pure Database built from a database's own CATALOG (docs/DATACUBE_TYPES_TO_SERVER_2026_09_27.md,
 * T2) -- the shape upstream's {@code pure/v1/utilities/database/schemaExploration} builds from
 * JDBC metadata, here from the STRUCTURED catalog rows a caller read (DuckDB's
 * {@code duckdb_columns()} joined to {@code duckdb_types()}: no type string is parsed; the user,
 * 2026-10-01). Each column's type is read by the database's own dialect
 * ({@link SqlDialect#catalogType}); a column whose value must be converted at the source
 * comes back with its conversion, for the caller to apply where the source allows it. The
 * browser's copy of this (a TypeScript type table) is deleted: the compiler decides.
 */
public final class CatalogModel {

    private CatalogModel() {
    }

    /**
     * One catalog column, as the database's catalog describes it.
     *
     * @param dataType    the column's own type name, as the catalog writes it ({@code DECIMAL(18,3)},
     *                    {@code JSON}, a user type's name): for an ALIAS and for messages, never parsed
     * @param logicalType its canonical type ({@code duckdb_types().logical_type}), or null when the
     *                    catalog names none
     * @param precision   a DECIMAL's precision, as a number; null for any other type
     * @param scale       a DECIMAL's scale, as a number; null for any other type
     * @param notNull     the catalog says the column holds no NULL: it is declared {@code NOT NULL}, so the
     *                    compiler types it {@code [1]} (as legend-engine does a NOT NULL column)
     */
    public record Column(String name, String dataType, @com.legend.base.Nullable String logicalType,
            @com.legend.base.Nullable Integer precision, @com.legend.base.Nullable Integer scale, boolean notNull) {
    }

    /** A column's conversion at the source: SQL over the column, e.g. {@code to_json("items")}. */
    public record Conversion(String column, String sql) {
    }

    /**
     * The Database element's text; the relation accessor that reads the table
     * ({@code #>{db.schema.table}#}); the conversions a COPY of the table applies so it holds
     * the declared types (an upload's rewrite at ingest, a Snap into the tab); and the columns
     * left out, because their source cannot convert them or no Database type holds them.
     */
    public record Database(String text, String accessor, List<Conversion> conversions, List<String> excluded) {

        /**
         * The select list a COPY of the table applies so that it holds the declared types:
         * {@code * REPLACE (<conversion> AS "<column>", ...)}, or {@code *} when no column converts. In
         * DuckDB's spelling (its star modifier), because every copy is a DuckDB table: an upload's
         * rewrite, a Snap into the tab, a Python frame. The one place this list is written.
         */
        public String copySelectList() {
            if (conversions.isEmpty()) {
                return "*";
            }
            List<String> items = new ArrayList<>();
            for (Conversion c : conversions) {
                items.add(c.sql() + " AS " + sqlIdent(c.column()));
            }
            return "* REPLACE (" + String.join(", ", items) + ")";
        }
    }

    /**
     * {@code ###Relational Database <path> ( [Schema s (] Table t ( col TYPE, ... ) [)] )}.
     * A column of a type the dialect cannot declare is refused, naming the column; so are two
     * columns one name apart only by case (the database would not tell them apart either).
     *
     * @param convertible whether the source can apply a conversion (an upload rewritten at
     *                    ingest can; a read-only table cannot). When it cannot, a column that
     *                    needs one to be read at all ({@link CatalogType.Read#CONVERTED}) is left
     *                    out of the Database and named in {@code excluded}; one that needs it only
     *                    in a copy ({@link CatalogType.Read#COPY_CONVERTED}: a zoned timestamp,
     *                    read under the UTC session) is declared and read as stored in place, its
     *                    conversion still listed for a copy of the table to apply. A column no
     *                    Database type holds ({@link CatalogType.Read#LEFT_OUT}) is left out and
     *                    named on every source.
     */
    public static Database database(String path, @com.legend.base.Nullable String schema, String table,
            List<Column> columns, SqlDialect dialect, boolean convertible) {
        if (columns.isEmpty()) {
            throw new IllegalArgumentException("the table '" + table + "' has no columns");
        }
        accessorName("schema", schema);
        accessorName("table", table);
        List<String> lines = new ArrayList<>();
        List<Conversion> conversions = new ArrayList<>();
        List<String> excluded = new ArrayList<>();
        java.util.Set<String> seen = new java.util.HashSet<>();
        for (Column c : columns) {
            if (!seen.add(c.name().toLowerCase(java.util.Locale.ROOT))) {
                throw new IllegalArgumentException("the table '" + table + "' has two columns named '"
                        + c.name() + "'");
            }
            CatalogType t;
            try {
                t = dialect.catalogType(c);
            } catch (DialectCapability e) {
                throw new DialectCapability("column '" + c.name() + "': " + e.getMessage());
            }
            boolean read = switch (t.read()) {
                // a COPY_CONVERTED column (a zoned timestamp) reads as stored in place: the UTC session
                case AS_STORED, COPY_CONVERTED -> true;
                case CONVERTED -> convertible;
                case LEFT_OUT -> false;
            };
            if (!read) {
                excluded.add(c.name());
                continue;
            }
            lines.add(ident(c.name()) + " " + t.declared() + (c.notNull() ? " NOT NULL" : ""));
            // what a copy applies: every declared column's conversion (on a read-only source only the
            // COPY_CONVERTED ones are declared; the source itself reads them as stored)
            if (t.conversion() != null) {
                conversions.add(new Conversion(c.name(), t.conversion().replace("%s", sqlIdent(c.name()))));
            }
        }
        if (lines.isEmpty()) {
            throw new IllegalArgumentException("no column of '" + table
                    + "' can be read from its source: " + String.join(", ", excluded));
        }
        String tableBlock = "Table " + ident(table) + "\n    (\n        "
                + String.join(",\n        ", lines) + "\n    )";
        String body = schema == null ? "    " + tableBlock
                : "    Schema " + ident(schema) + "\n    (\n        "
                        + tableBlock.replace("\n", "\n    ") + "\n    )";
        String accessor = "#>{" + path + "." + (schema == null ? "" : ident(schema) + ".") + ident(table) + "}#";
        return new Database("###Relational\nDatabase " + path + "\n(\n" + body + "\n)\n", accessor,
                List.copyOf(conversions), List.copyOf(excluded));
    }

    /**
     * A schema or table name the accessor can carry, quoted when it is not a plain identifier.
     * Upstream reads {@code #>{db.schema.table}#} by splitting on {@code .}, and the accessor's
     * grammar refuses {@code ( ) { } | ; =} and a line break: a name with any of those is refused.
     */
    private static void accessorName(String what, @com.legend.base.Nullable String name) {
        if (name != null && name.chars().anyMatch(ch -> ".(){}|;=\n".indexOf(ch) >= 0)) {
            throw new IllegalArgumentException("the " + what + " name '" + name
                    + "' cannot be read through #>{db." + what + "}#: a '.', '(', ')', '{', '}', '|', ';',"
                    + " '=' or line break cannot be carried there");
        }
    }

    /** A name as a Pure Database identifier: bare when it is one, else quoted with the
     *  lexer's backslash escapes (a doubled quote would end the token). */
    static String ident(String name) {
        if (name.matches("[A-Za-z_][A-Za-z0-9_]*")) {
            return name;
        }
        return "\"" + name.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
    }

    /** A name as a SQL identifier, always quoted, for a conversion's column reference. */
    private static String sqlIdent(String name) {
        return "\"" + name.replace("\"", "\"\"") + "\"";
    }
}
