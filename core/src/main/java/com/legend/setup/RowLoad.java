package com.legend.setup;

import com.legend.base.Nullable;
import com.legend.sql.SqlDdl;
import com.legend.sql.SqlDml;
import com.legend.sql.SqlExpr;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Rows for ONE table, each cell the TEXT the model holds ({@code null} is SQL NULL) &mdash; the system metamodel seed,
 * a connection's test data, a project's Data, a payload's elements &mdash; and, per column, the type the table
 * DECLARES (docs/DATA_DELIVERY_DESIGN_2026_11_10.md §1.1, A2: "model data is SQL in the plan"). The load is written at
 * planning as multi-row inserts whose cells are the dialect's own TYPED LITERALS, the literal writer every query uses:
 * an integer column's cell an integer literal, a date column's a date literal, and so on ({@link #cell}). A cell its
 * column's type cannot take is refused here, at planning, naming the table, the column and the value; a column of no
 * declared type (a table no store declares) takes its text as a string literal, which the database assigns. Nothing
 * here spells SQL: the dialect renders the {@link SqlDml} this becomes.
 *
 * @param schema  the table's schema (null or {@code default}: the default one), its text
 * @param table   the table, its text
 * @param quoting which of the two names the store declared quoted (the dialect spells both, once)
 * @param columns the target columns, their text, or empty for every column in declared order
 * @param quotedColumns the ones of {@code columns} the store declared quoted (the dialect spells each column, once)
 * @param width   the number of cells in every row
 * @param rows    the rows; each exactly {@code width} cells
 * @param types   per cell position, the type its column declares, or empty for a table that declares none (each
 *                cell's text a string literal)
 */
public record RowLoad(@Nullable String schema, String table, com.legend.sql.NameQuoting quoting,
                      List<String> columns, java.util.Set<String> quotedColumns, int width, List<List<String>> rows,
                      List<SqlDdl.ColumnType> types) {

    /** The rows one insert carries at most: a load of more is that many inserts, in order, in the load's one
     *  transaction (a statement of thousands of rows is the parser's whole cost at once). */
    public static final int ROWS_PER_INSERT = 1000;

    /** A load whose table declares no type: every cell a string literal. */
    public RowLoad(@Nullable String schema, String table, com.legend.sql.NameQuoting quoting,
            List<String> columns, java.util.Set<String> quotedColumns, int width, List<List<String>> rows) {
        this(schema, table, quoting, columns, quotedColumns, width, rows, List.of());
    }

    public RowLoad {
        columns = List.copyOf(columns);
        quotedColumns = java.util.Collections.unmodifiableSet(new java.util.LinkedHashSet<>(quotedColumns));
        rows = List.copyOf(rows);
        types = List.copyOf(types);
        if (!types.isEmpty() && types.size() != width) {
            throw new IllegalArgumentException("row load into " + table + ": "
                    + types.size() + " types for rows of " + width);
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

    /** The load as its inserts, {@link #ROWS_PER_INSERT} rows each at most, in order: none for a load of no row. */
    public List<SqlDml.InsertValues> inserts() {
        List<SqlDml.InsertValues> out = new ArrayList<>();
        for (int from = 0; from < rows.size(); from += ROWS_PER_INSERT) {
            out.add(insert(rows.subList(from, Math.min(rows.size(), from + ROWS_PER_INSERT))));
        }
        return out;
    }

    /** The load as ONE insert of every row. */
    public SqlDml.InsertValues values() {
        return insert(rows);
    }

    private SqlDml.InsertValues insert(List<List<String>> chunk) {
        List<List<SqlExpr>> values = new ArrayList<>(chunk.size());
        for (List<String> row : chunk) {
            List<SqlExpr> cells = new ArrayList<>(row.size());
            for (int c = 0; c < row.size(); c++) {
                String text = row.get(c);
                cells.add(text == null ? new SqlExpr.NullLit() : cell(text, types.isEmpty() ? null : types.get(c), c));
            }
            values.add(cells);
        }
        return new SqlDml.InsertValues(schema, table, quoting, columns, quotedColumns, values);
    }

    /** {@link #inserts} as {@code dialect} writes them, each a statement sent as it is; a date-time cell finer than its
     *  column keeps there refused first ({@link #refuseFinerThanKept}). */
    public List<String> statements(com.legend.sql.dialect.SqlDialect dialect) {
        refuseFinerThanKept(dialect);
        return written(dialect);
    }

    /** {@link #inserts} as THE REFEREE'S COPY writes them ({@code CsvSeed.planned}): the compatibility edge's replay of
     *  the plan's rows on its own H2, which keeps what its columns keep -- the refusal of a cell finer than its column
     *  is the plan's target's ({@link #statements}), never the edge's (G5: the edge depends on the product, never the
     *  reverse; a SQLite target keeps nine digits its referee's H2 does not). */
    public List<String> refereeStatements(com.legend.sql.dialect.SqlDialect referee) {
        return written(referee);
    }

    private List<String> written(com.legend.sql.dialect.SqlDialect dialect) {
        List<String> out = new ArrayList<>();
        for (SqlDml.InsertValues insert : inserts()) {
            out.add(dialect.render(insert));
        }
        return out;
    }

    private static final Pattern INTEGER = Pattern.compile("[+-]?\\d+(\\.0*)?");
    private static final Pattern NUMBER = Pattern.compile("[+-]?(\\d+\\.?\\d*|\\.\\d+)([eE][+-]?\\d+)?");
    private static final Pattern DATE = Pattern.compile("\\d{4}-\\d{2}-\\d{2}");
    private static final Pattern DATE_TIME = Pattern.compile("\\d{4}-\\d{2}-\\d{2}([ T]\\d{2}:\\d{2}(:\\d{2}(\\.\\d{1,9})?)?)?");
    private static final Pattern TIME = Pattern.compile("\\d{2}:\\d{2}:\\d{2}(\\.\\d{1,9})?");

    /**
     * The cell {@code text} of column {@code c} as the literal of its column's declared {@code type}: an integer
     * column's an integer ({@code 1.0} as {@code 1}), any other number there its digits, which the database assigns as
     * it assigns upstream's raw token; a float's and a decimal's their digits as written; a bit's a boolean
     * ({@code true}, {@code false}, {@code 1}, {@code 0}, {@code y}, {@code n}, any case: upstream's H2 rule), any
     * other number there its digits; a date's, a date-time's and a time's their typed literal where the text is in its ISO shape,
     * any other text cast by the database (the user's ruling, 2026-11-10: as upstream's quoted cell, which its database
     * parses); a semi-structured column's the text as JSON; text, or a column of no declared type, a string. A number or
     * a boolean the type cannot take is refused naming the table, the column and the value; a column of a type no load
     * writes yet (BINARY, VARBINARY, OTHER, DISTINCT, ARRAY, OBJECT) is refused as not implemented yet.
     */
    private SqlExpr cell(String text, SqlDdl.@Nullable ColumnType type, int c) {
        if (type == null) {
            return new SqlExpr.StringLit(text);
        }
        String t = text.strip();
        return switch (type) {
            case SqlDdl.ColumnType.Sized s when s.kind().equalsIgnoreCase("VARCHAR")
                    || s.kind().equalsIgnoreCase("CHAR") -> new SqlExpr.StringLit(text);
            case SqlDdl.ColumnType.Sized s -> throw unsupported(c, s.kind());
            case SqlDdl.ColumnType.Scaled s -> {
                if (!NUMBER.matcher(t).matches()) {
                    throw refused(c, text, s.kind());
                }
                yield new SqlExpr.DecimalLit(new BigDecimal(t));
            }
            case SqlDdl.ColumnType.Plain p -> switch (p.kind()) {
                case BIGINT, SMALLINT, TINYINT, INTEGER -> {
                    if (!NUMBER.matcher(t).matches()) {
                        throw refused(c, text, p.kind().name());
                    }
                    BigDecimal n = new BigDecimal(t);
                    // an integer's digits an integer literal; any other number its digits, which the database assigns
                    // to the column as upstream's raw token is (legend-engine writes a number's cell as it is written)
                    yield INTEGER.matcher(t).matches() && n.compareTo(BigDecimal.valueOf(Long.MIN_VALUE)) >= 0
                            && n.compareTo(BigDecimal.valueOf(Long.MAX_VALUE)) <= 0
                            ? new SqlExpr.IntLit(n.longValueExact()) : new SqlExpr.DecimalLit(n);
                }
                // a float's cell its digits as written, which the database assigns to the column as it assigns
                // upstream's raw token (convertValuesToCsv writes a number's cell as it is written: the A checkpoint's
                // audits, T9 and C1, 2026-11-10 -- until then parsed to a double, so 1e400 became Infinity and an
                // unnamed failure); a cell that is no number (NaN, Infinity) is refused by name, as upstream's raw
                // token is no SQL value
                case FLOAT, DOUBLE, REAL -> {
                    if (!NUMBER.matcher(t).matches()) {
                        throw refused(c, text, p.kind().name());
                    }
                    // a cell in exponent form is a float of its digits as written, read by the database (the A delta
                    // audit's 3, 2026-11-11: as an exact decimal, 1e300 was a 301-digit literal BigQuery refuses, 1.5E10
                    // a Decimal(2,0) ClickHouse overflowed, 1e-50 a 51-digit DECIMAL on Trino); a plain one its digits,
                    // an exact decimal the database assigns to the column
                    yield t.indexOf('e') >= 0 || t.indexOf('E') >= 0
                            ? new SqlExpr.Cast(new SqlExpr.StringLit(t), com.legend.sql.SqlType.Scalar.DOUBLE)
                            : new SqlExpr.DecimalLit(new BigDecimal(t));
                }
                // upstream's H2 rule (h2Extension2_1_214.pure's bitValueFromString, read from the pinned sources):
                // y, 1 and true are true, n, 0 and false false, trimmed and in any case; any other number its digits,
                // which the database assigns as it assigns upstream's raw token; any other text refused by name
                case BIT -> switch (t.toLowerCase(java.util.Locale.ROOT)) {
                    case "true", "1", "y" -> new SqlExpr.BoolLit(true);
                    case "false", "0", "n" -> new SqlExpr.BoolLit(false);
                    default -> {
                        if (!NUMBER.matcher(t).matches()) {
                            throw refused(c, text, "BIT");
                        }
                        yield new SqlExpr.DecimalLit(new BigDecimal(t));
                    }
                };
                // an ISO shape is the dialect's typed literal; any other text the DATABASE casts, as upstream's quoted
                // cell is parsed by its database (the user's ruling, 2026-11-10, "Yes to both"): Java parses nothing,
                // and a text the database rejects fails under the load's name
                case DATE -> DATE.matcher(t).matches() ? new SqlExpr.DateLit(t)
                        : new SqlExpr.Cast(new SqlExpr.StringLit(text), com.legend.sql.SqlType.Scalar.DATE);
                case TIMESTAMP, TIMESTAMP_NS -> DATE_TIME.matcher(t).matches() ? new SqlExpr.TimestampLit(t)
                        : new SqlExpr.Cast(new SqlExpr.StringLit(text), com.legend.sql.SqlType.Scalar.TIMESTAMP);
                case TIME -> TIME.matcher(t).matches() ? new SqlExpr.TimeLit(t)
                        : new SqlExpr.Cast(new SqlExpr.StringLit(text), com.legend.sql.SqlType.Scalar.TIME);
                case JSON -> new SqlExpr.Cast(new SqlExpr.StringLit(text), com.legend.sql.SqlType.Scalar.JSON);
                case VARCHAR -> new SqlExpr.StringLit(text);
                case OTHER, DISTINCT, ARRAY, OBJECT -> throw unsupported(c, p.kind().name());
            };
        };
    }

    /**
     * A date-time cell, in its ISO shape, with more digits of a second than its column keeps on {@code dialect}
     * ({@code SqlDialect.timestampColumnDigits}; a {@code TIMESTAMP_NS} column nine) is refused by name at planning --
     * the table, the column, the value, its digits and the digits kept -- never cut or rounded by the database as it
     * stores it (S181, the user's ruling of 2026-11-10: a date-time with more fraction digits than its target keeps is
     * refused). Zeros past the kept digits are no digit of the value. A cell in another shape is the database's to read
     * ({@link #cell}: Java parses nothing), so its digits are the database's rule.
     */
    private void refuseFinerThanKept(com.legend.sql.dialect.SqlDialect dialect) {
        for (int c = 0; c < types.size(); c++) {
            if (!(types.get(c) instanceof SqlDdl.ColumnType.Plain p) || (p.kind() != SqlDdl.ColumnType.Kind.TIMESTAMP
                    && p.kind() != SqlDdl.ColumnType.Kind.TIMESTAMP_NS)) {
                continue;
            }
            int kept = p.kind() == SqlDdl.ColumnType.Kind.TIMESTAMP_NS ? 9 : dialect.timestampColumnDigits();
            for (List<String> row : rows) {
                String text = row.get(c);
                if (text == null || !DATE_TIME.matcher(text.strip()).matches()) {
                    continue;
                }
                int digits = fractionDigits(text.strip());
                if (digits > kept) {
                    throw new com.legend.error.DataError("the table " + where() + "'s column " + column(c) + " is "
                            + p.kind() + ", which keeps " + kept + " fraction digits on "
                            + dialect.getClass().getSimpleName() + ", and its data holds '" + text + "', of " + digits
                            + ": refused, never cut (S181)");
                }
            }
        }
    }

    /** The digits of a second an ISO date-time's text carries, its zeros past the last other digit not counted. */
    private static int fractionDigits(String iso) {
        int dot = iso.lastIndexOf('.');
        if (dot < 0) {
            return 0;
        }
        int end = iso.length();
        while (end > dot + 1 && iso.charAt(end - 1) == '0') {
            end--;
        }
        return end - dot - 1;
    }

    /** Column {@code c}'s name as a refusal names it: its own, or its place where the load names none. */
    private String column(int c) {
        return columns.isEmpty() ? "#" + (c + 1) : columns.get(c);
    }

    /** The table as a refusal names it: {@code schema.table}, or the table alone in the default schema. */
    public String where() {
        return (schema == null || schema.equals("default") ? "" : schema + ".") + table;
    }

    private com.legend.error.DataError refused(int c, String text, String type) {
        return new com.legend.error.DataError("the table " + where() + "'s column " + column(c) + " is " + type
                + ", and its data holds '" + text + "', which is not one");
    }

    private com.legend.error.NotImplementedException unsupported(int c, String type) {
        return new com.legend.error.NotImplementedException("data for the table " + where() + "'s column "
                + column(c) + ": a " + type + " column's data is not loaded yet");
    }
}
