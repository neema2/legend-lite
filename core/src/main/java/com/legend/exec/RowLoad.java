package com.legend.exec;

import com.legend.sql.SqlDml;
import com.legend.sql.SqlExpr;

import java.util.ArrayList;
import java.util.List;

/**
 * Rows for ONE table, every cell the TEXT the database casts to its column's
 * type ({@code null} is SQL NULL) &mdash; the shape of the data legend-lite
 * holds as values: the system metamodel seed, test data. The DATABASE does the
 * typing on every load path: the text path writes each cell as a string
 * literal; a {@link BulkLoad} stages the cells as text and casts in one
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
     *  every engine without a {@link BulkLoad} takes. */
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
}
