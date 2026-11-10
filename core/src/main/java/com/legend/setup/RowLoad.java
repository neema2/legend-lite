package com.legend.setup;

import com.legend.sql.SqlDml;
import com.legend.sql.SqlExpr;

import java.util.ArrayList;
import java.util.List;

/**
 * Rows for ONE table, every cell the TEXT the database casts to its column's
 * type ({@code null} is SQL NULL) &mdash; the shape of the data legend-lite
 * holds as values: the system metamodel seed, test data. The DATABASE does the
 * typing on every load path: the text path writes each cell as a string
 * literal; a {@code exec.BulkLoad} stages the cells as text and casts in one
 * {@code INSERT ... SELECT}. Nothing here converts a value, and nothing here
 * spells SQL: the dialect renders the {@link SqlDml} this becomes.
 *
 * @param schema  the table's schema (null or {@code default}: the default one), its text
 * @param table   the table, its text
 * @param quoting which of the two names the store declared quoted (the dialect spells both, once)
 * @param columns the target columns, their text, or empty for every column in declared order
 * @param quotedColumns the ones of {@code columns} the store declared quoted (the dialect spells each column, once)
 * @param width   the number of cells in every row
 * @param rows    the rows; each exactly {@code width} cells
 * @param casts   per cell position, the type each cell's text is CAST to in the database (null: none, the column
 *                takes the text as it is), or empty for no casts at all: for a database that takes no text into a
 *                column of another type (MariaDB and MySQL take no {@code 'true'} into a BOOLEAN; BigQuery takes no
 *                string into an INT64 or a BOOL; measured 2026-10-11). The DATABASE still does the typing: the
 *                dialect spells its own conversion.
 * @param types   per cell position, its column's declared type (null: the store declares none), or empty for none
 *                known: what each bound cell's placeholder is typed by ({@link #steps})
 */
public record RowLoad(@com.legend.base.Nullable String schema, String table, com.legend.sql.NameQuoting quoting,
                      List<String> columns, java.util.Set<String> quotedColumns, int width, List<List<String>> rows,
                      List<com.legend.sql.@com.legend.base.Nullable SqlType> casts,
                      List<com.legend.sql.SqlDdl.@com.legend.base.Nullable ColumnType> types) {

    /** A load whose cells the database takes as text. */
    public RowLoad(@com.legend.base.Nullable String schema, String table, com.legend.sql.NameQuoting quoting,
            List<String> columns, java.util.Set<String> quotedColumns, int width, List<List<String>> rows) {
        this(schema, table, quoting, columns, quotedColumns, width, rows, List.of(), List.of());
    }

    /** A load whose cells are CAST in the database ({@link #casts}): a server session table's rows. */
    public RowLoad(@com.legend.base.Nullable String schema, String table, com.legend.sql.NameQuoting quoting,
            List<String> columns, java.util.Set<String> quotedColumns, int width, List<List<String>> rows,
            List<com.legend.sql.@com.legend.base.Nullable SqlType> casts) {
        this(schema, table, quoting, columns, quotedColumns, width, rows, casts, List.of());
    }

    /** This load with its columns' declared types ({@link #types}). */
    public RowLoad typed(List<com.legend.sql.SqlDdl.@com.legend.base.Nullable ColumnType> declared) {
        return new RowLoad(schema, table, quoting, columns, quotedColumns, width, rows, casts, declared);
    }

    public RowLoad {
        columns = List.copyOf(columns);
        quotedColumns = java.util.Collections.unmodifiableSet(new java.util.LinkedHashSet<>(quotedColumns));
        rows = List.copyOf(rows);
        casts = java.util.Collections.unmodifiableList(new ArrayList<>(casts));
        types = java.util.Collections.unmodifiableList(new ArrayList<>(types));
        if (!types.isEmpty() && types.size() != width) {
            throw new IllegalArgumentException("row load into " + table + ": " + types.size() + " types for rows of "
                    + width);
        }
        if (!casts.isEmpty() && casts.size() != width) {
            throw new IllegalArgumentException("row load into " + table + ": "
                    + casts.size() + " casts for rows of " + width);
        }
        if (!columns.isEmpty() && columns.size() != width) {
            throw new IllegalArgumentException("row load into " + table + ": "
                    + columns.size() + " columns named for rows of " + width);
        }
        for (List<String> row : rows) {
            if (row.size() != width) {
                throw new IllegalArgumentException("row load into " + table + ": a row of "
                        + row.size() + " cells where every row has " + width);
            }
        }
    }

    /** The load as ONE multi-row insert of string literals &mdash; the path
     *  every engine without a {@code exec.BulkLoad} takes. */
    public SqlDml.InsertValues values() {
        List<List<SqlExpr>> out = new ArrayList<>(rows.size());
        for (List<String> row : rows) {
            List<SqlExpr> cells = new ArrayList<>(row.size());
            for (int c = 0; c < row.size(); c++) {
                String cell = row.get(c);
                com.legend.sql.SqlType cast = casts.isEmpty() ? null : casts.get(c);
                cells.add(cell == null ? new SqlExpr.NullLit()
                        : cast == null ? new SqlExpr.StringLit(cell)
                        : new SqlExpr.Cast(new SqlExpr.StringLit(cell), cast));
            }
            out.add(cells);
        }
        return new SqlDml.InsertValues(schema, table, quoting, columns, quotedColumns, out);
    }

    /** The staged path's copy into this table from {@code stage}. */
    public SqlDml.InsertFromTable fromStage(String stage) {
        return new SqlDml.InsertFromTable(schema, table, quoting, columns, quotedColumns, stage);
    }

    /** The staging table a bulk load appends its cells to: temporary, so one per session, dropped first and after. */
    public static final String STAGE = "legend_row_load";

    /** A load's steps and the bundle they read. */
    public record Steps(List<com.legend.executionplan.ExecutionPlan.LoadStep> steps,
            java.util.Map<String, String> content) {
    }

    /**
     * THE MODEL'S ROWS AS A LOAD'S STEP (the checkpoint audits of 2026-11-10: tenets M-5, S-1, S-7; correctness M2 and
     * S8): the rows a {@code DataRef.Rows} text in the load's bundle, by digest, and one file step that STREAMS them --
     * each row's cells bound as text to its column's placeholder (the dialect's, typed by the column's declared type),
     * in chunks of the dialect's size, never a literal at any size. Where the database has a bulk loader the step names
     * its staged form (a temporary staging table of text columns, dropped first and after, copied into the table). Where
     * the bundle travels -- inline in the plan's JSON or a blob beside it -- is the plan writer's, by its size over the
     * whole plan ({@code PlanJson.writeBundle}). The one maker of the model's rows' steps: a project's Data, a
     * payload's elements, the catalog's rows, a connection's test data. None for a load of no row.
     */
    public Steps steps(com.legend.sql.dialect.SqlDialect dialect) {
        if (!casts.isEmpty()) {
            throw new IllegalStateException("row load into " + table + ": a load with casts is a server session"
                    + " table's rows (Ddl.metamodelTemporary), never a load's step");
        }
        if (rows.isEmpty()) {
            return new Steps(List.of(), java.util.Map.of());
        }
        if (columns.isEmpty()) {
            throw new IllegalStateException("row load into " + table + ": a load's step names its columns");
        }
        String json = json(rows);
        String digest = com.legend.cache.Hash.ofUtf8(json).hex();
        long size = json.getBytes(java.nio.charset.StandardCharsets.UTF_8).length;
        var ref = new com.legend.executionplan.DataRef(com.legend.executionplan.DataRef.Namespace.CONTENT,
                "rows/" + digest + ".json", digest, size, com.legend.executionplan.DataRef.Rows.JSON);
        List<com.legend.sql.SqlDdl.@com.legend.base.Nullable ColumnType> declared = types.isEmpty()
                ? java.util.Collections.nCopies(width, null) : types;
        SqlDml.LoadCsv into = new SqlDml.LoadCsv(schema, table, quoting, columns, quotedColumns, declared, ',', '"');
        int chunk = dialect.rowsPerInsert(width);
        com.legend.executionplan.ExecutionPlan.LoadStep.Delivery.@com.legend.base.Nullable Staged staged = null;
        if (dialect.loadsRowsInBulk()) {
            List<com.legend.sql.SqlDdl.Column> text = new ArrayList<>(width);
            for (int c = 0; c < width; c++) {
                text.add(new com.legend.sql.SqlDdl.Column("c" + c, false,
                        new com.legend.sql.SqlDdl.ColumnType.Plain(com.legend.sql.SqlDdl.ColumnType.Kind.VARCHAR),
                        false, false));
            }
            staged = new com.legend.executionplan.ExecutionPlan.LoadStep.Delivery.Staged(STAGE,
                    dialect.render(new com.legend.sql.SqlDdl.CreateTable(null, STAGE, com.legend.sql.NameQuoting.BARE,
                            text, true)),
                    dialect.render(fromStage(STAGE)), dialect.dropTemporary(STAGE));
        }
        var step = new com.legend.executionplan.ExecutionPlan.LoadStep.File(ref,
                new com.legend.executionplan.ExecutionPlan.LoadStep.Delivery.Streamed(dialect.rowsInsert(into, 1), width,
                        dialect.rowsInsert(into, chunk), chunk, staged),
                Math.max(1, (int) Math.ceil(size / (1024.0 * 1024.0))));
        return new Steps(List.of(step), java.util.Map.of(digest, json));
    }

    /** {@code rows} as {@code DataRef.Rows} text: a JSON array of arrays of strings, a null cell {@code null}. */
    public static String json(List<List<String>> rows) {
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
                        sb.append("\\u").append(String.format(java.util.Locale.ROOT, "%04x", (int) ch));
                    } else {
                        sb.append(ch);
                    }
                }
            }
        }
        sb.append('"');
    }
}
