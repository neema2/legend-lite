// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql;

import java.util.List;

/**
 * DDL as SQL IR (2026-09-16): a store's declared shape as statements the
 * dialect renders, exactly as it renders a query — the dialect owns the
 * identifier rule and the type spelling; nothing outside it decides a
 * target. The column facts are the STORE's declarations (type, nullability,
 * key membership): they ride the node, never a caller's flag.
 *
 * <p>Column types are the DECLARED vocabulary ({@link ColumnType}: the
 * relational grammar's types, sized and scaled as written) — a declaration,
 * not a computed value's {@link SqlType}. The exec layer maps the store
 * model's type onto it; the SQL layer never sees the store model.
 */
public sealed interface SqlDdl
        permits SqlDdl.CreateTable, SqlDdl.DropTable, SqlDdl.CreateSchema, SqlDdl.DropSchema {

    /** A declared column type, as the relational grammar spells it. */
    sealed interface ColumnType permits ColumnType.Plain, ColumnType.Sized, ColumnType.Scaled {
        /** The unsized kinds. {@code VARCHAR}: text of any length &mdash; no store
         *  declares it (the grammar sizes VARCHAR); a bulk load's staging
         *  columns hold each cell as text before the database casts it. */
        /** {@code TIMESTAMP_NS}: a date-time of nanosecond precision, lite's column type (2026-10-17); a dialect
         *  whose timestamps keep microseconds refuses it by name. */
        enum Kind { BIGINT, SMALLINT, TINYINT, INTEGER, FLOAT, DOUBLE, REAL, BIT, TIMESTAMP, DATE, TIME,
            TIMESTAMP_NS, JSON, VARCHAR, OTHER, DISTINCT, ARRAY, OBJECT }
        record Plain(Kind kind) implements ColumnType {
        }
        /** {@code VARCHAR(n)}, {@code CHAR(n)}, {@code BINARY(n)}, {@code VARBINARY(n)}. */
        record Sized(String kind, int size) implements ColumnType {
        }
        /** {@code DECIMAL(p, s)}, {@code NUMERIC(p, s)}. */
        record Scaled(String kind, int precision, int scale) implements ColumnType {
        }
    }

    /** One declared column. {@code declaredQuoted}: the store wrote the
     *  name in quotes (the engine keeps them in the metamodel and its
     *  corpus references the column quoted). */
    record Column(String name, boolean declaredQuoted, ColumnType type,
                  boolean notNull, boolean primaryKey) {
    }

    /** {@code CREATE [TEMPORARY] TABLE [schema.]table (columns..., PRIMARY KEY(...))}. The names are their text;
     *  {@code quoting} says which the store declared quoted ({@link com.legend.sql.NameQuoting}). */
    record CreateTable(@com.legend.base.Nullable String schema, String table, com.legend.sql.NameQuoting quoting,
                       List<Column> columns, boolean temporary) implements SqlDdl {
        public CreateTable {
            columns = List.copyOf(columns);
        }

        public CreateTable(@com.legend.base.Nullable String schema, String table, com.legend.sql.NameQuoting quoting,
                List<Column> columns) {
            this(schema, table, quoting, columns, false);
        }
    }

    /** {@code DROP TABLE IF EXISTS [schema.]table}, its names as {@link CreateTable}'s. */
    record DropTable(@com.legend.base.Nullable String schema, String table, com.legend.sql.NameQuoting quoting)
            implements SqlDdl {
    }

    /** {@code CREATE SCHEMA IF NOT EXISTS schema}; {@code quoted}: the store declared the name quoted. */
    record CreateSchema(String schema, boolean quoted) implements SqlDdl {
    }

    /** {@code DROP SCHEMA IF EXISTS schema CASCADE}; {@code quoted} as {@link CreateSchema}'s. */
    record DropSchema(String schema, boolean quoted) implements SqlDdl {
    }
}
