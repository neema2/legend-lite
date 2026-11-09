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
 */
public record RowLoad(@com.legend.base.Nullable String schema, String table, com.legend.sql.NameQuoting quoting,
                      List<String> columns, java.util.Set<String> quotedColumns, int width, List<List<String>> rows,
                      List<com.legend.sql.@com.legend.base.Nullable SqlType> casts) {

    /** A load whose cells the database takes as text. */
    public RowLoad(@com.legend.base.Nullable String schema, String table, com.legend.sql.NameQuoting quoting,
            List<String> columns, java.util.Set<String> quotedColumns, int width, List<List<String>> rows) {
        this(schema, table, quoting, columns, quotedColumns, width, rows, List.of());
    }

    public RowLoad {
        columns = List.copyOf(columns);
        quotedColumns = java.util.Collections.unmodifiableSet(new java.util.LinkedHashSet<>(quotedColumns));
        rows = List.copyOf(rows);
        casts = java.util.Collections.unmodifiableList(new ArrayList<>(casts));
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

    /** The staging table a bulk load appends its cells to: temporary, so one per session, created and dropped inside
     *  the one load. */
    public static final String STAGE = "legend_row_load";

    /** This load for a bulk loader, every statement rendered by {@code dialect}: its rows, and the staging table they
     *  are appended to as text, created (temporary, one text column per cell), copied into the target table (the
     *  database casts each cell) and dropped — a plan's setup step ({@code ExecutionPlan.LoadStep.Rows}), which the
     *  execution side's loader runs. Its rows are not empty. */
    public com.legend.executionplan.ExecutionPlan.LoadStep.Rows staged(com.legend.sql.dialect.SqlDialect dialect) {
        if (!casts.isEmpty()) {
            // the stage's copy is one INSERT ... SELECT *: it casts nothing, so a cast would be dropped
            throw new IllegalStateException("row load into " + table + ": a load with casts has no staged form");
        }
        List<com.legend.sql.SqlDdl.Column> text = new ArrayList<>(width);
        for (int c = 0; c < width; c++) {
            text.add(new com.legend.sql.SqlDdl.Column("c" + c, false,
                    new com.legend.sql.SqlDdl.ColumnType.Plain(com.legend.sql.SqlDdl.ColumnType.Kind.VARCHAR),
                    false, false));
        }
        return new com.legend.executionplan.ExecutionPlan.LoadStep.Rows(STAGE,
                dialect.render(new com.legend.sql.SqlDdl.CreateTable(null, STAGE, com.legend.sql.NameQuoting.BARE, text,
                        true)),
                dialect.render(fromStage(STAGE)),
                dialect.render(new com.legend.sql.SqlDdl.DropTable(null, STAGE, com.legend.sql.NameQuoting.BARE)), rows);
    }
}
