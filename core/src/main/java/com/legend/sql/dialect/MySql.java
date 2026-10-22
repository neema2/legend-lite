// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import com.legend.sql.SqlAgg;
import com.legend.sql.SqlExpr;

import java.util.List;

/**
 * The MySQL EXECUTION dialect (2026-10-11, docs/CLOUD_BACKENDS_PCT_2026_10_11.md §14): {@link MariaDb}'s where the two
 * agree, its hooks spelled as MySQL 8.4 needs them. Every difference was measured on MySQL 8.4 (the pinned server,
 * tools/mysql) through MariaDB Connector/J:
 * <ul>
 *   <li>{@code JSON_ARRAYAGG} takes no ORDER BY or DISTINCT: an ordered array is GROUP_CONCAT's, each value wrapped
 *       in a one-element array and the wrapping read away ({@code $[*][0]});</li>
 *   <li>no {@code JSON_COMPACT}: a JSON value is normalized by its CAST to JSON (MySQL stores JSON parsed: an object's
 *       keys sorted, its text spaced);</li>
 *   <li>no negative JSON array index: {@code $[last-n]};</li>
 *   <li>regular expressions are ICU's: a replacement names a group {@code $n}, and REGEXP_REPLACE takes the
 *       occurrence to replace;</li>
 *   <li>a derived table and a recursive CTE may read an outer row, and LATERAL exists;</li>
 *   <li>the session: no {@code max_recursive_iterations} ({@code cte_max_recursion_depth}).</li>
 * </ul>
 */
public final class MySql extends MariaDb {

    public MySql() {
        // MariaDB's reserved words and quoting: the same lexicon, named
        super(MariaDb.LEXICON);
    }

    /** MariaDB's passes, then each window call inside an expression computed in a select below
     *  ({@link WindowsComputedBelow}: MySQL evaluates a subquery of an expression over a window call on every row,
     *  measured, so a guard's raise fired where its CASE did not reach it). */
    @Override
    protected List<com.legend.sql.SqlRewriter> passes() {
        List<com.legend.sql.SqlRewriter> ps = new java.util.ArrayList<>(super.passes());
        ps.add(new WindowsComputedBelow());
        return List.copyOf(ps);
    }

    /** Every scalar type's name: MariaDB's (the dialect program's Phase 1, 2026-10-13: an exhaustive
     *  switch, so a new scalar type asks this dialect too). */
    @Override
    protected String scalarTypeName(com.legend.sql.SqlType.Scalar s) {
        return switch (s) {
            case BOOLEAN, INTEGER, BIGINT, HUGEINT, DOUBLE, VARCHAR, DATE, TIMESTAMP, TIMESTAMPTZ, TIME, JSON,
                 LITERAL, TEMPORAL_TEXT, DECIMAL_TEXT ->
                    super.scalarTypeName(s);
        };
    }

    /** A date format's parts: MariaDB's (the dialect program's Phase 1, 2026-10-13: each dialect
     *  declares its date-format vocabulary). */
    @Override
    protected String formatText(SqlExpr.FormatLit fl) {
        return super.formatText(fl);
    }

    /** Every aggregate, by name: MariaDB's (the dialect program's Phase 1, 2026-10-13: an exhaustive switch, so a
     *  new aggregate asks this dialect too). */
    @Override
    protected SqlWriter reducer(SqlWriter writer, SqlAgg.Reducer r) {
        return switch (r.fn()) {
            case SUM, COUNT, AVG, MIN, MAX, ANY_VALUE, STDDEV_SAMP, STDDEV_POP, VAR_SAMP, VAR_POP, MEDIAN, MODE,
                 STRING_AGG, LIST, QUANTILE_CONT, QUANTILE_DISC, CORR, COVAR_SAMP, COVAR_POP, ARG_MAX, ARG_MIN, WAVG,
                 HASH_LIST, BOOL_AND, BOOL_OR, IS_DISTINCT_MARK, UNIQUE_VALUE_ONLY, VARIANCE, STDDEV, ROW_NUMBER,
                 RANK, DENSE_RANK, PERCENT_RANK, CUME_DIST, NTILE, LAG, LEAD, FIRST_VALUE, LAST_VALUE, NTH_VALUE ->
                    super.reducer(writer, r);
        };
    }

    /** Every function, by name: MariaDB's (the dialect program's Phase 1, 2026-10-13: MySQL decides each function
     *  itself, so a new function asks it as well as MariaDB; a spelling MySQL needs of its own becomes its own arm). */
    @Override
    protected SqlWriter call(SqlWriter writer, SqlExpr.Call c, int parentPrec) {
        return switch (c.fn()) {
            case AND, OR, NOT, EQUAL, NOT_EQUAL, LESS, LESS_EQUAL, GREATER, GREATER_EQUAL, PLUS, MINUS, TIMES,
                 DIVIDE, MOD, REM, NEGATE, ABS, IS_NULL, IS_NOT_NULL, IN, COALESCE, PARSE_INT, CONCAT, LENGTH, UPPER,
                 LOWER, GREATEST, LEAST, CONCAT_JOIN, LIST_FILTER, LIST_TRANSFORM, LIST_CONCAT, LIST_GET,
                 LIST_POSITION, LIST_EXISTS, LIST_FOR_ALL, STRUCT_INSERT, UNNEST, LIST_FLATTEN, JSON_MERGE_PATCH,
                 SQRT, CBRT, EXP, LN, LOG10, POW, PI, SIN, COS, TAN, ASIN, ACOS, ATAN, ATAN2, SINH, COSH, TANH,
                 CEILING, FLOOR, ROUND, SIGN, XOR, BIT_AND, BIT_OR, BIT_XOR, BIT_SHIFT_LEFT, BIT_SHIFT_RIGHT,
                 BIT_NOT, REGEXP_EXTRACT, REGEXP_EXTRACT_ALL, REGEXP_REPLACE, REGEXP_FULL_MATCH, MAP_FROM_LISTS,
                 MAP_FROM_ENTRIES, MAP_EMPTY, MAP_EXTRACT, MAP_KEYS, MAP_VALUES, MAP_CONCAT, SUBSTRING, STRPOS,
                 STARTS_WITH, ENDS_WITH, MATCHES, LEFT, RIGHT, LPAD, RPAD, TRIM, LTRIM, RTRIM, REPLACE, TRANSLATE, SPLIT,
                 SPLIT_PART, PURE_SPLIT_PART, REVERSE_STRING, ASCII_CODE, CHR, UC_FIRST, LC_FIRST, ENCODE_BASE64,
                 LEVENSHTEIN, GUID, RANDOM, FORMAT, HASH, MD5, SHA1, SHA256, EXTRACT, TODAY, NOW, DATE_TRUNC_DAY,
                 MAKE_DATE,
                 MAKE_TIMESTAMP, DATE_TRUNC, ADD_INTERVAL, DATE_DIFF, TIME_BUCKET, TIMEZONE, DATE_AT_TIME, ADD_INTERVAL_TEMPORAL,
                 EPOCH_SECONDS, EPOCH_MS, EPOCH_NS, FROM_EPOCH_NS, FROM_EPOCH_SECONDS, FROM_EPOCH_MS, DAYNAME, MONTHNAME, COT, RADIANS,
                 DEGREES, INT_DIVIDE, REPEAT_STR, JARO_WINKLER, DECODE_BASE64, CURRENT_USER_FN, LIST_LENGTH,
                 LIST_ZIP, LIST_DISTINCT, LIST_APPEND, LIST_SUM, LIST_MIN, LIST_MAX, LIST_AVG, LIST_MEDIAN,
                 LIST_MODE, LIST_SORT, LIST_SORT_DESC, LIST_TAIL, LIST_INIT, RANGE_FN, LIST_PRODUCT, LIST_REDUCE,
                 LIST_SLICE, REPEAT_VALUE, LIST_BOOL_AND, LIST_BOOL_OR, IS_DISTINCT_FROM, ALL_DISTINCT,
                 NULL_SAFE_EQUAL, NULL_SAFE_NOT_EQUAL, STRFTIME, STRPTIME, PARSE_DATE, LIST_REVERSE, TYPEOF,
                 ROUND_HALF_UP, ERROR, FLOOR_RAW, TO_VARIANT, BOOL_TO_TEXT, FLOAT_CANON, FLOAT_PRINT, FLOAT_PRINT_ENGINE, VARIANT_ELEMENTS,
                 VARIANT_GET, JSON_TYPE, JSON_ARRAY_LENGTH, JSON_PRETTY -> super.call(writer, c, parentPrec);
        };
    }

    @Override
    protected String product() {
        return "MySQL";
    }

    /** No quote and no backslash inside a literal: MySQL re-serializes a CTE that another CTE reads, writing its
     *  literals with backslash escapes ({@code '\''}, a backslash doubled), and re-parses that text under the session's
     *  NO_BACKSLASH_ESCAPES -- a syntax error at a quote, a doubled backslash silently (measured on 8.4 by the lane
     *  session, the judges' verdict statement: channel B's testConcatenateMixedType and three removeDuplicates tests,
     *  2026-10-20). Each is its own character, {@code CHAR(39 USING utf8mb4)} and {@code CHAR(92 USING utf8mb4)},
     *  between literal pieces; a text with neither is its plain literal (a JSON path of a name with neither). */
    @Override
    protected String stringLit(String value) {
        if (value.indexOf('\'') < 0 && value.indexOf('\\') < 0) {
            return "'" + value + "'";
        }
        StringBuilder sb = new StringBuilder("CONCAT(''");
        StringBuilder piece = new StringBuilder();
        for (int i = 0; i < value.length(); i++) {
            char ch = value.charAt(i);
            if (ch == '\'' || ch == '\\') {
                if (piece.length() > 0) {
                    sb.append(", '").append(piece).append('\'');
                    piece.setLength(0);
                }
                sb.append(ch == '\'' ? ", CHAR(39 USING utf8mb4)" : ", CHAR(92 USING utf8mb4)");
            } else {
                piece.append(ch);
            }
        }
        if (piece.length() > 0) {
            sb.append(", '").append(piece).append('\'');
        }
        return sb.append(')').toString();
    }

    @Override
    public List<String> sessionSetup() {
        // NO_UNSIGNED_SUBTRACTION: YEAR() and the like are unsigned here, and their difference is a signed one
        // (measured: "BIGINT UNSIGNED value is out of range" without it)
        return List.of("SET SESSION sql_mode = 'ANSI_QUOTES,PIPES_AS_CONCAT,NO_BACKSLASH_ESCAPES,STRICT_ALL_TABLES,"
                + "NO_ENGINE_SUBSTITUTION,NO_UNSIGNED_SUBTRACTION'", "SET SESSION time_zone = '+00:00'", "SET NAMES utf8mb4 COLLATE utf8mb4_bin",
                "SET SESSION group_concat_max_len = 1073741824", "SET SESSION cte_max_recursion_depth = 100000000",
                "SET SESSION div_precision_increment = 30");
    }

    @Override
    protected SqlWriter.Piece compact(SqlWriter.Piece json) {
        return w -> w.append("CAST(").piece(json).append(" AS JSON)");
    }

    /** {@code JSON_ARRAYAGG(value)} where no order is asked; else GROUP_CONCAT's ordered (or distinct) text of each
     *  value's one-element array, read as JSON and unwrapped: NULL over no rows, as JSON_ARRAYAGG's. */
    @Override
    protected SqlWriter.Piece arrayAgg(boolean distinct, SqlWriter.Piece value, SqlWriter.@com.legend.base.Nullable Piece order,
            boolean firstOnly) {
        if (order == null && !distinct) {
            return w -> w.append("JSON_ARRAYAGG(").piece(value).append(")");
        }
        return w -> {
            w.append("JSON_EXTRACT(CAST(CONCAT('[', GROUP_CONCAT(").append(distinct ? "DISTINCT " : "")
                    .append("JSON_ARRAY(").piece(value).append(")");
            if (order != null) {
                w.append(" ORDER BY ").piece(order);
            }
            w.append(" SEPARATOR ','), ']') AS JSON), '$[*][0]')");
        };
    }

    @Override
    protected SqlWriter.Piece indexPath(SqlWriter.Piece index) {
        return w -> w.append("(CASE WHEN (").piece(index).append(") < 0 THEN CONCAT('$[last-', -(").piece(index)
                .append(") - 1, ']') ELSE CONCAT('$[', ").piece(index).append(", ']') END)");
    }

    /** {@code JSON_VALUE} takes only a literal path here and answers in another collation (utf8mb4_0900_bin, an
     *  "Illegal mix" with the session's) as VARCHAR(512) (measured): a value at a path is {@code JSON_EXTRACT}'s,
     *  unquoted, a JSON null NULL. */
    @Override
    protected SqlWriter.Piece jsonText(SqlWriter.Piece json, SqlWriter.Piece path) {
        return w -> w.append("(CASE WHEN JSON_TYPE(JSON_EXTRACT(").piece(json).append(", ").piece(path)
                .append(")) = 'NULL' THEN NULL ELSE JSON_UNQUOTE(JSON_EXTRACT(").piece(json).append(", ").piece(path)
                .append(")) END)");
    }

    /** A JSON null is JSON here, which a string compares unequal to: the null is read by its type, as
     *  {@link #jsonText(SqlWriter.Piece, SqlWriter.Piece)} reads it. */
    @Override
    protected SqlWriter.Piece jsonMember(SqlWriter.Piece json, SqlWriter.Piece path) {
        return w -> w.append("(CASE WHEN JSON_TYPE(JSON_EXTRACT(").piece(json).append(", ").piece(path)
                .append(")) = 'NULL' THEN NULL ELSE JSON_EXTRACT(").piece(json).append(", ").piece(path)
                .append(") END)");
    }

    @Override
    protected String jsonText(String json, String path) {
        return "(CASE WHEN JSON_TYPE(JSON_EXTRACT(" + json + ", " + path + ")) = 'NULL' THEN NULL ELSE JSON_UNQUOTE("
                + "JSON_EXTRACT(" + json + ", " + path + ")) END)";
    }

    /** A table function's argument may not assign a variable here ("Incorrect arguments to JSON_TABLE", measured): the
     *  two rows are a UNION's, its first assigning, evaluated only where reached (measured). */
    @Override
    protected SqlWriter.Piece twoRows(SqlWriter.Piece message) {
        return w -> w.append("(SELECT @legend_raise := ").piece(message).append(" UNION ALL SELECT 0)");
    }

    /**
     * printf with no SFORMAT here: the format's literal pieces and each conversion written out, concatenated -- {@code
     * %s} the value's text, {@code %d} an integer padded to its width (zeros after a sign with the {@code 0} flag,
     * spaces before it, or after it with {@code -}), {@code %.Nf} a DECIMAL of scale N (6 by default), {@code %x} and
     * {@code %o} its digits in base 16 and 8; any other conversion is a wall.
     */
    @Override
    protected SqlWriter format(SqlWriter writer, List<SqlExpr> a) {
        String f = literal(a.get(0), "a format");
        java.util.regex.Matcher m = java.util.regex.Pattern.compile("%([-+ #0]*)([0-9]*)(?:\\.([0-9]+))?([a-zA-Z%])")
                .matcher(f);
        List<SqlWriter.Piece> parts = new java.util.ArrayList<>();
        int arg = 1;
        int last = 0;
        StringBuilder literal = new StringBuilder();
        while (m.find()) {
            literal.append(f, last, m.start());
            last = m.end();
            char conversion = m.group(4).charAt(0);
            if (conversion == '%') {
                literal.append('%');
                continue;
            }
            if (arg >= a.size()) {
                throw new DialectCapability("a format with more conversions than arguments reached MySQL");
            }
            String text = literal.toString();
            parts.add(w -> w.append(stringLit(text)));
            literal.setLength(0);
            parts.add(conversion(a.get(arg++), conversion, m.group(1), m.group(2), m.group(3)));
        }
        literal.append(f.substring(last));
        String tail = literal.toString();
        parts.add(w -> w.append(stringLit(tail)));
        return writer.append("CONCAT(").join(parts, ", ", SqlWriter::piece).append(")");
    }

    private SqlWriter.Piece conversion(SqlExpr x, char conversion, String flags, String width,
            @com.legend.base.Nullable String precision) {
        SqlWriter.Piece digits = switch (conversion) {
            case 's' -> asText(x);
            case 'd', 'i' -> w -> w.append("CAST(CAST(").expr(x, 0).append(" AS SIGNED) AS CHAR)");
            // a negative value that rounds to zero keeps its sign ('-0.00', as Java's and DuckDB's printf): a DECIMAL
            // zero has none (measured 2026-10-11, testFormatFloatWithRounding)
            case 'f', 'F' -> {
                String scale = precision == null ? "6" : precision;
                yield w -> w.append("CONCAT(CASE WHEN ").expr(x, 4).append(" < 0 AND CAST(").expr(x, 0)
                        .append(" AS DECIMAL(65," + scale + ")) = 0 THEN '-' ELSE '' END, CAST(CAST(").expr(x, 0)
                        .append(" AS DECIMAL(65," + scale + ")) AS CHAR))");
            }
            case 'x' -> w -> w.append("LOWER(CONV(").expr(x, 0).append(", 10, 16))");
            case 'X' -> w -> w.append("CONV(").expr(x, 0).append(", 10, 16)");
            case 'o' -> w -> w.append("CONV(").expr(x, 0).append(", 10, 8)");
            default -> throw new DialectCapability("printf conversion %" + conversion + " reached MySQL");
        };
        boolean plus = flags.contains("+") && (conversion == 'd' || conversion == 'i' || conversion == 'f');
        SqlWriter.Piece signed = !plus ? digits : w -> w.append("CONCAT(CASE WHEN ").expr(x, 4)
                .append(" >= 0 THEN '+' ELSE '' END, ").piece(digits).append(")");
        if (width.isEmpty()) {
            return signed;
        }
        String n = width;
        if (flags.contains("-")) {
            return w -> w.append("RPAD(").piece(signed).append(", GREATEST(" + n + ", CHAR_LENGTH(").piece(signed)
                    .append(")), ' ')");
        }
        if (flags.contains("0") && conversion != 's') {
            // the zeros after the sign: -5 as %05d is -0005
            return w -> w.append("(CASE WHEN LEFT(").piece(signed).append(", 1) IN ('-', '+') THEN CONCAT(LEFT(")
                    .piece(signed).append(", 1), LPAD(SUBSTRING(").piece(signed).append(", 2), GREATEST(" + n
                            + " - 1, CHAR_LENGTH(").piece(signed).append(") - 1), '0')) ELSE LPAD(").piece(signed)
                    .append(", GREATEST(" + n + ", CHAR_LENGTH(").piece(signed).append(")), '0') END)");
        }
        return w -> w.append("LPAD(").piece(signed).append(", GREATEST(" + n + ", CHAR_LENGTH(").piece(signed)
                .append(")), ' ')");
    }

    /** A grouped mode is counted too: a table function's argument may be no aggregate here ("Incorrect arguments to
     *  JSON_TABLE", measured), so MariaDB's array of the group's values cannot be read. */
    /** A bare integer past a signed BIGINT that fits an unsigned one is a BIGINT UNSIGNED here, and its arithmetic
     *  stays one (9223372036854775898 - 132 reads BIGINT, measured 2026-10-11 by the census): a HUGEINT literal is a
     *  DECIMAL, as MariaDB delivers it. */
    @Override
    protected String hugeIntLiteral(java.math.BigDecimal v) {
        return "CAST(" + v.toPlainString() + " AS DECIMAL(38,0))";
    }

    /** YEAR() answers in MySQL's own YEAR type here, which the driver reads as a date (measured 2026-10-11: testYear's
     *  "StrictDate cannot be cast to Integer", the census's BIGINT label over a YEAR column): an integer by its cast. */
    @Override
    protected SqlWriter.Piece yearOf(SqlExpr t) {
        return w -> w.append("CAST(YEAR(").expr(t, 0).append(") AS SIGNED)");
    }

    /** A JSON value's text puts a space after every ',' and ':' outside a string here, and only there (MySQL stores
     *  JSON parsed; measured 2026-10-11 by channel B: a Variant's text '[1, 2, 3]' where Pure writes '[1,2,3]'): a
     *  string-aware replace drops exactly those, as Postgres's (a string, with its escapes, is matched whole and kept).
     *  Only where a JSON value BECOMES text: inside JSON_ARRAY or JSON_OBJECT the value stays JSON. */
    @Override
    protected SqlWriter.Piece jsonAsText(SqlWriter.Piece json) {
        return w -> w.append("REGEXP_REPLACE(CAST(").piece(json).append(" AS CHAR), ")
                .append(stringLit("(\"(?:[^\"\\\\]|\\\\.)*\")|([,:]) ")).append(", '$1$2')");
    }

    @Override
    protected boolean groupedModesCounted() {
        return true;
    }

    @Override
    protected String groupRef(int n) {
        return "$" + n;
    }

    /** A JSON_TABLE BOOLEAN column is a BIT here (measured); an INT reads a JSON true as 1. */
    @Override
    protected String booleanColumn() {
        return "INT";
    }

    /** No PERCENTILE_CONT, PERCENTILE_DISC or MEDIAN here (measured): a whole partition's median or quantile is read
     *  from each row's rank (MariaDb.rankedQuantile, 2026-10-11); an ordered or framed one is a wall; the grouped ones
     *  are MariaDB's (a sorted array, indexed). */
    @Override
    protected boolean windowQuantilesNative() {
        return false;
    }

    @Override
    protected SqlWriter windowCall(SqlWriter writer, SqlExpr.WindowCall w) {
        if (w.fn() instanceof SqlAgg.Reducer r && (r.fn() == SqlAgg.Fn.MEDIAN || r.fn() == SqlAgg.Fn.QUANTILE_CONT
                || r.fn() == SqlAgg.Fn.QUANTILE_DISC) && (!w.orderBy().isEmpty() || w.frame() != null)) {
            throw new DialectCapability("a windowed " + r.fn() + " over an ordered or framed window reached MySQL,"
                    + " which has no PERCENTILE_CONT");
        }
        return super.windowCall(writer, w);
    }

    /** A table function's argument may hold no subquery here ("Incorrect arguments to JSON_TABLE", measured), and a
     *  list operation's is often one: the argument is bound first, as the one column of a LATERAL derived table (which
     *  may read the outer row, measured), and the table function reads that column. */
    @Override
    protected SqlWriter.Piece jsonTable(SqlWriter.Piece json, String spec, String alias) {
        String bound = ident(alias.replace("\"", "") + "__j");
        return w -> w.append("(LATERAL (SELECT ").piece(json).append(" AS \"j\") AS " + bound + " CROSS JOIN JSON_TABLE("
                + bound + ".\"j\", " + spec + ") AS " + alias + ")");
    }

    @Override
    protected boolean jsonIsBinary() {
        return true;
    }

    @Override
    protected boolean correlatesDerived() {
        return true;
    }

    /** DuckDB's regexp_replace(s, p, r[, options]): the first match unless the options say 'g' (ICU's REGEXP_REPLACE
     *  takes the occurrence: 1, or 0 for each); the replacement's {@code \N} is ICU's {@code $N}, and a literal
     *  {@code $} or backslash is escaped. */
    @Override
    protected SqlWriter.Piece regexpReplace(List<SqlExpr> a) {
        String p = literal(a.get(1), "a regexp_replace pattern");
        String r = literal(a.get(2), "a regexp_replace replacement");
        String options = a.size() > 3 ? literal(a.get(3), "regexp_replace options") : "";
        boolean global = false;
        String flags = "";
        for (char o : options.toCharArray()) {
            switch (o) {
                case 'g' -> global = true;
                case 'i' -> flags = "(?i)";
                case 'c' -> flags = "";
                default -> throw new DialectCapability("regexp_replace option '" + o + "' reached MySQL");
            }
        }
        StringBuilder icu = new StringBuilder();
        for (int i = 0; i < r.length(); i++) {
            char ch = r.charAt(i);
            if (ch == '\\' && i + 1 < r.length() && Character.isDigit(r.charAt(i + 1))) {
                icu.append('$').append(r.charAt(++i));
            } else if (ch == '\\' && i + 1 < r.length()) {
                icu.append('\\').append(r.charAt(++i));
            } else if (ch == '$' || ch == '\\') {
                icu.append('\\').append(ch);
            } else {
                icu.append(ch);
            }
        }
        String pat = flags + p;
        String occurrence = global ? "0" : "1";
        return w -> w.append("REGEXP_REPLACE(").expr(a.get(0), 0).append(", " + stringLit(pat) + ", "
                + stringLit(icu.toString()) + ", 1, " + occurrence + ")");
    }
}
