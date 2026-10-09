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
 * @param schema  the table's schema (null or {@code default}: the default one)
 * @param table   the table
 * @param columns the target columns, or empty for every column in declared order
 * @param width   the number of cells in every row
 * @param rows    the rows; each exactly {@code width} cells
 */
public record RowLoad(@com.legend.base.Nullable String schema, String table, List<String> columns,
                      int width, List<List<String>> rows) {

    public RowLoad {
        columns = List.copyOf(columns);
        rows = List.copyOf(rows);
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
            for (String cell : row) {
                cells.add(cell == null ? new SqlExpr.NullLit() : new SqlExpr.StringLit(cell));
            }
            out.add(cells);
        }
        return new SqlDml.InsertValues(schema, table, columns, out);
    }

    /** The staged path's copy into this table from {@code stage}. */
    public SqlDml.InsertFromTable fromStage(String stage) {
        return new SqlDml.InsertFromTable(schema, table, columns, stage);
    }

    /** The staging table a bulk load appends its cells to: temporary, so one per session, created and dropped inside
     *  the one load. */
    public static final String STAGE = "legend_row_load";

    /** This load for a bulk loader, every statement rendered by {@code dialect}: its rows, and the staging table they
     *  are appended to as text, created (temporary, one text column per cell), copied into the target table (the
     *  database casts each cell) and dropped — a plan's setup step ({@code ExecutionPlan.SetupStep.Rows}), which the
     *  execution side's loader runs. Its rows are not empty. */
    public com.legend.executionplan.ExecutionPlan.SetupStep.Rows staged(com.legend.sql.dialect.SqlDialect dialect) {
        List<com.legend.sql.SqlDdl.Column> text = new ArrayList<>(width);
        for (int c = 0; c < width; c++) {
            text.add(new com.legend.sql.SqlDdl.Column("c" + c, false,
                    new com.legend.sql.SqlDdl.ColumnType.Plain(com.legend.sql.SqlDdl.ColumnType.Kind.VARCHAR),
                    false, false));
        }
        return new com.legend.executionplan.ExecutionPlan.SetupStep.Rows(STAGE,
                dialect.render(new com.legend.sql.SqlDdl.CreateTable(null, STAGE, text, true)),
                dialect.render(fromStage(STAGE)),
                dialect.render(new com.legend.sql.SqlDdl.DropTable(null, STAGE)), rows);
    }
}