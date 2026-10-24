// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql;

import java.util.List;

/**
 * Row-writing statements as SQL IR (2026-09-23): the dialect renders them as
 * it renders a query and a {@link SqlDdl} &mdash; its identifier rule, its
 * literal spelling. A table is named as DDL names it ({@code schema} null or
 * {@code default}: bare): its names' text, and which its store declared quoted ({@link NameQuoting}), spelled once,
 * by the dialect. So is a column: {@code columns} holds their text, {@code quotedColumns} the ones the store declared
 * quoted ({@code "Amt"}), and the dialect spells each as its queries reference it ({@code SqlDialect.storedName}).
 */
public sealed interface SqlDml permits SqlDml.InsertValues, SqlDml.InsertFromTable, SqlDml.DeleteAll,
        SqlDml.CreateTempTableAs, SqlDml.InsertSelect, SqlDml.Update, SqlDml.DeleteWhere, SqlDml.Merge {

    /** {@code INSERT INTO [schema.]table [(columns)] VALUES (row), ...};
     *  {@code columns} empty: every column in declared order. */
    record InsertValues(@com.legend.base.Nullable String schema, String table, NameQuoting quoting, List<String> columns,
                        java.util.Set<String> quotedColumns, List<List<SqlExpr>> rows) implements SqlDml {
        public InsertValues {
            columns = List.copyOf(columns);
            quotedColumns = java.util.Collections.unmodifiableSet(new java.util.LinkedHashSet<>(quotedColumns));
            rows = List.copyOf(rows);
        }
    }

    /** {@code INSERT INTO [schema.]table [(columns)] SELECT * FROM source}: a
     *  staged table's rows, cast by the database into the target's columns. */
    record InsertFromTable(@com.legend.base.Nullable String schema, String table, NameQuoting quoting,
                           List<String> columns, java.util.Set<String> quotedColumns, String source) implements SqlDml {
        public InsertFromTable {
            columns = List.copyOf(columns);
            quotedColumns = java.util.Collections.unmodifiableSet(new java.util.LinkedHashSet<>(quotedColumns));
        }
    }

    /** {@code DELETE FROM [schema.]table}: every row. */
    record DeleteAll(@com.legend.base.Nullable String schema, String table, NameQuoting quoting) implements SqlDml {
    }

    /** {@code CREATE TEMPORARY TABLE table AS query}: a table of the session's own, holding a query's rows, its
     *  columns the query's (the test-data generator's fetches, 2026-10-16). The name is one the platform makes: bare.
     *  {@code columns}, when given, names the table's columns in the query's order, each as its relation names it (a
     *  program's staged rows, read after by those names: the writes design's leg 4, 2026-10-20), for a dialect whose
     *  database would otherwise fold a stored column's name; empty for the query's own. {@code staged}: a program's
     *  staged rows, made within the program's transaction, which the statement must not end, and gone when the
     *  program has run ({@link com.legend.sql.dialect.SqlDialect#dropsStagedTablesWithTheirTransaction}: dropped by
     *  the program's last step, or by the database as the transaction ends; PR #67's review, 2026-10-22). */
    record CreateTempTableAs(String table, SqlQuery query, List<String> columns, boolean staged) implements SqlDml {
        public CreateTempTableAs {
            columns = List.copyOf(columns);
        }

        public CreateTempTableAs(String table, SqlQuery query) {
            this(table, query, List.of(), false);
        }

        /** A program's staged rows ({@code meta::legend::lite::stage}), {@code columns} as the staged relation names
         *  them, in {@code query}'s order. */
        public static CreateTempTableAs staged(String table, SqlQuery query, List<String> columns) {
            return new CreateTempTableAs(table, query, columns, true);
        }
    }

    // ------------------------------------------------------------------ the store writes (docs/WRITES_DESIGN_2026_10_18.md §4, L1)

    /** {@code INSERT INTO [schema.]table (columns) query}: a query's rows into the named columns, the query's outputs
     *  in the columns' order, each value cast by the database into its column (the general form of
     *  {@link InsertFromTable}, whose source is a staged table's name; Pure's {@code write}, 2026-10-18). */
    record InsertSelect(@com.legend.base.Nullable String schema, String table, NameQuoting quoting, List<String> columns,
                        java.util.Set<String> quotedColumns, SqlQuery query) implements SqlDml {
        public InsertSelect {
            columns = List.copyOf(columns);
            quotedColumns = java.util.Collections.unmodifiableSet(new java.util.LinkedHashSet<>(quotedColumns));
            if (columns.isEmpty()) {
                throw new IllegalArgumentException("an INSERT of a query names its columns");
            }
        }
    }

    /** {@code UPDATE [schema.]table AS alias SET column = value, ... [WHERE where]}: each value and the condition read
     *  the row through {@code alias}; no condition, every row (lite's {@code update}, 2026-10-18). {@code storedTypes}:
     *  the table's columns' declared store types, as a scanned {@link SqlSource.Table} carries them, so a column the
     *  dialect reads other than as held is read so here too. */
    record Update(@com.legend.base.Nullable String schema, String table, NameQuoting quoting, String alias,
                  java.util.Map<String, SqlDdl.ColumnType> storedTypes, List<Assignment> assignments,
                  @com.legend.base.Nullable SqlExpr where) implements SqlDml {
        public Update {
            storedTypes = java.util.Collections.unmodifiableMap(new java.util.LinkedHashMap<>(storedTypes));
            assignments = List.copyOf(assignments);
            if (assignments.isEmpty()) {
                throw new IllegalArgumentException("an UPDATE assigns a column");
            }
        }

        /** {@code column = value}: the column as its store declared it ({@code quoted}: delimited, its case kept). */
        public record Assignment(String column, boolean quoted, SqlExpr value) {
        }
    }

    /** {@code DELETE FROM [schema.]table AS alias [WHERE where]}: the rows the condition holds for, read through
     *  {@code alias}; no condition, every row (lite's {@code delete}, 2026-10-18; {@link DeleteAll} is the seed's
     *  unaliased form). {@code storedTypes} as {@link Update}'s. */
    record DeleteWhere(@com.legend.base.Nullable String schema, String table, NameQuoting quoting, String alias,
                       java.util.Map<String, SqlDdl.ColumnType> storedTypes, @com.legend.base.Nullable SqlExpr where)
            implements SqlDml {
        public DeleteWhere {
            storedTypes = java.util.Collections.unmodifiableMap(new java.util.LinkedHashMap<>(storedTypes));
        }
    }

    /** An upsert by key: {@code MERGE INTO [schema.]table AS alias USING (query) AS source ON alias.k = source.k AND
     *  ... WHEN MATCHED THEN UPDATE SET c = source.c, ... WHEN NOT MATCHED THEN INSERT (columns) VALUES (source.c,
     *  ...)}. The query's outputs are named as the table's {@code columns}, in their order; {@code keys} are among
     *  them; a column not a key is updated where a row matches (none when every column is a key: no WHEN MATCHED
     *  arm). Lite's {@code merge}, 2026-10-18. */
    record Merge(@com.legend.base.Nullable String schema, String table, NameQuoting quoting, List<String> columns,
                 java.util.Set<String> quotedColumns, List<String> keys, SqlQuery query, String alias,
                 String sourceAlias) implements SqlDml {
        public Merge {
            columns = List.copyOf(columns);
            quotedColumns = java.util.Collections.unmodifiableSet(new java.util.LinkedHashSet<>(quotedColumns));
            keys = List.copyOf(keys);
            if (keys.isEmpty() || !columns.containsAll(keys)) {
                throw new IllegalArgumentException("a MERGE's keys " + keys + " are among its columns " + columns);
            }
        }
    }
}
