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
        /** The unsized kinds. */
        enum Kind { BIGINT, SMALLINT, TINYINT, INTEGER, FLOAT, DOUBLE, REAL, BIT, TIMESTAMP, DATE,
            JSON, OTHER, DISTINCT, ARRAY, OBJECT }
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

    /** {@code CREATE [TEMPORARY] TABLE [schema.]table (columns..., PRIMARY KEY(...))}. */
    record CreateTable(@com.legend.base.Nullable String schema, String table,
                       List<Column> columns, boolean temporary) implements SqlDdl {
        public CreateTable {
            columns = List.copyOf(columns);
        }

        public CreateTable(@com.legend.base.Nullable String schema, String table, List<Column> columns) {
            this(schema, table, columns, false);
        }
    }

    /** {@code DROP TABLE IF EXISTS [schema.]table}. */
    record DropTable(@com.legend.base.Nullable String schema, String table) implements SqlDdl {
    }

    /** {@code CREATE SCHEMA IF NOT EXISTS schema}. */
    record CreateSchema(String schema) implements SqlDdl {
    }

    /** {@code DROP SCHEMA IF EXISTS schema CASCADE}. */
    record DropSchema(String schema) implements SqlDdl {
    }
}
