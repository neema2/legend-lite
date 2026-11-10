// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.planner;

import com.legend.executionplan.DataRef;
import com.legend.executionplan.ExecutionPlan;
import com.legend.setup.RowLoad;
import com.legend.sql.NameQuoting;
import com.legend.sql.SqlDdl;
import com.legend.sql.dialect.SqlDialect;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * THE MODEL'S ROWS AS A LOAD'S STEPS, INLINE OR IN THE BUNDLE (docs/DATA_IN_THE_PLAN_2026_11_09.md L6c, 2026-11-10,
 * decision 1 and its clarification: the threshold is where the bytes travel, never what a statement holds). Rows whose
 * text is at most {@link #INLINE_LIMIT_BYTES} ride in the plan as the one insert the database runs; above it they go to
 * the load's bundle as {@code DataRef.Rows} text, named by digest, and the steps stage them: a temporary staging table
 * of text columns created, the bundle's rows streamed into it by one row's insert (each cell bound as text), copied
 * into the table (the database casts every cell, as it casts the inline insert's literals), the staging table dropped.
 * A load that casts its cells, or a database with no temporary table, keeps its rows inline.
 *
 * @param steps   the load's steps for the rows
 * @param content the bundle they read, by digest: none where they are inline
 */
record ContentRows(List<ExecutionPlan.LoadStep> steps, Map<String, String> content) {

    /** The most text the model's rows carry inline: 16 KB, near a query's own SQL at its 99th percentile (decision 1's
     *  starting proposal, the demo's markets fill measured at 1.87 MB). */
    static final int INLINE_LIMIT_BYTES = 16 * 1024;

    /** {@code load}'s rows as steps on {@code dialect}'s database: none for a load of no row. */
    static ContentRows of(RowLoad load, SqlDialect dialect) {
        if (load.rows().isEmpty()) {
            return new ContentRows(List.of(), Map.of());
        }
        String json = json(load.rows());
        byte[] bytes = json.getBytes(StandardCharsets.UTF_8);
        if (bytes.length <= INLINE_LIMIT_BYTES || !load.casts().isEmpty() || !dialect.temporaryTables()) {
            return new ContentRows(List.of(new ExecutionPlan.LoadStep.Statement(dialect.render(load.values()))),
                    Map.of());
        }
        String digest = com.legend.cache.Hash.of(bytes).hex();
        DataRef ref = new DataRef(DataRef.Namespace.CONTENT, "rows/" + digest + ".json", digest, bytes.length,
                DataRef.Rows.JSON);
        List<SqlDdl.Column> text = new ArrayList<>(load.width());
        List<String> stageColumns = new ArrayList<>(load.width());
        for (int c = 0; c < load.width(); c++) {
            text.add(new SqlDdl.Column("c" + c, false, new SqlDdl.ColumnType.Plain(SqlDdl.ColumnType.Kind.VARCHAR),
                    false, false));
            stageColumns.add("c" + c);
        }
        String insert = dialect.rowsInsert(new com.legend.sql.SqlDml.LoadCsv(null, RowLoad.STAGE, NameQuoting.BARE,
                stageColumns, java.util.Set.of(), Collections.nCopies(load.width(),
                        new SqlDdl.ColumnType.Plain(SqlDdl.ColumnType.Kind.VARCHAR)), ',', '"'));
        List<ExecutionPlan.LoadStep> steps = List.of(
                new ExecutionPlan.LoadStep.Statement(dialect.render(new SqlDdl.CreateTable(null, RowLoad.STAGE,
                        NameQuoting.BARE, text, true))),
                new ExecutionPlan.LoadStep.File(ref, new ExecutionPlan.LoadStep.Delivery.Streamed(insert,
                        load.width()), Math.max(1, (int) Math.ceil(bytes.length / (1024.0 * 1024.0)))),
                new ExecutionPlan.LoadStep.Statement(dialect.render(load.fromStage(RowLoad.STAGE))),
                new ExecutionPlan.LoadStep.Statement(dialect.render(new SqlDdl.DropTable(null, RowLoad.STAGE,
                        NameQuoting.BARE))));
        return new ContentRows(steps, Map.of(digest, json));
    }

    /** {@code rows} as {@code DataRef.Rows} text: a JSON array of arrays of strings, a null cell {@code null}. */
    static String json(List<List<String>> rows) {
        StringBuilder sb = new StringBuilder("[");
        for (int r = 0; r < rows.size(); r++) {
            sb.append(r == 0 ? "[" : ",[");
            List<String> row = rows.get(r);
            for (int c = 0; c < row.size(); c++) {
                if (c > 0) {
                    sb.append(',');
                }
                String cell = row.get(c);
                if (cell == null) {
                    sb.append("null");
                } else {
                    quote(sb, cell);
                }
            }
            sb.append(']');
        }
        return sb.append(']').toString();
    }

    private static void quote(StringBuilder sb, String s) {
        sb.append('"');
        for (int i = 0; i < s.length(); i++) {
            char ch = s.charAt(i);
            switch (ch) {
                case '"' -> sb.append("\\\"");
                case '\\' -> sb.append("\\\\");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                case '\t' -> sb.append("\\t");
                case '\b' -> sb.append("\\b");
                case '\f' -> sb.append("\\f");
                default -> {
                    if (ch < 0x20) {
                        sb.append(String.format(java.util.Locale.ROOT, "\\u%04x", (int) ch));
                    } else {
                        sb.append(ch);
                    }
                }
            }
        }
        sb.append('"');
    }
}
