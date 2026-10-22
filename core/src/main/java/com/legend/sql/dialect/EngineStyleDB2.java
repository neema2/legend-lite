// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import com.legend.sql.SqlExpr;
import com.legend.sql.SqlType;
import java.util.List;

/**
 * The engine's DB2 SQL text (toSQLString parity) — shares the
 * engine-style select/alias plan with {@link EngineStyleH2} and respells
 * only the dialect-owned function forms the DB2 goldens pin:
 * infix {@code concat}, bare {@code trim}, {@code varchar(16000)} string
 * casts, {@code reverse}. Every unpinned form inherits the H2 spelling —
 * an unmatched golden stays an HONEST text FAIL, never a guessed
 * respell.
 */
public class EngineStyleDB2 extends EngineStyleH2 {

    public EngineStyleDB2() {
    }

    public EngineStyleDB2(boolean quoteIdentifiers,
            @com.legend.base.Nullable String timeZone) {
        super(quoteIdentifiers, timeZone);
    }

    /** Every scalar type's name: the engine H2 text's (the dialect program's Phase 1, 2026-10-13: an exhaustive
     *  switch, so a new scalar type asks this dialect too). */
    @Override
    protected String scalarTypeName(com.legend.sql.SqlType.Scalar s) {
        return switch (s) {
            case BOOLEAN, INTEGER, BIGINT, HUGEINT, DOUBLE, VARCHAR, DATE, TIMESTAMP, TIMESTAMPTZ, TIME, JSON,
                 LITERAL, TEMPORAL_TEXT, DECIMAL_TEXT ->
                    super.scalarTypeName(s);
        };
    }

    /** A date format's parts: the engine H2 text's (the dialect program's Phase 1, 2026-10-13: each dialect
     *  declares its date-format vocabulary). */
    @Override
    protected String formatText(SqlExpr.FormatLit fl) {
        return super.formatText(fl);
    }

    /** Every aggregate, by name: the engine H2 text's (the dialect program's Phase 1, 2026-10-13: an exhaustive
     *  switch, so a new aggregate asks this dialect too). */
    @Override
    protected SqlWriter reducer(SqlWriter writer, com.legend.sql.SqlAgg.Reducer r) {
        return switch (r.fn()) {
            case SUM, COUNT, AVG, MIN, MAX, ANY_VALUE, STDDEV_SAMP, STDDEV_POP, VAR_SAMP, VAR_POP, MEDIAN, MODE,
                 STRING_AGG, LIST, QUANTILE_CONT, QUANTILE_DISC, CORR, COVAR_SAMP, COVAR_POP, ARG_MAX, ARG_MIN, WAVG,
                 HASH_LIST, BOOL_AND, BOOL_OR, IS_DISTINCT_MARK, UNIQUE_VALUE_ONLY, VARIANCE, STDDEV, ROW_NUMBER,
                 RANK, DENSE_RANK, PERCENT_RANK, CUME_DIST, NTILE, LAG, LEAD, FIRST_VALUE, LAST_VALUE, NTH_VALUE ->
                    super.reducer(writer, r);
        };
    }

    /** DB2 plan goldens wrap a top-level WHERE conjunction in one extra
     * paren pair ({@code where ((A) and (B))}). */
    @Override
    protected String groupBySeparator() {
        return ",";
    }

    /** Engine extensionDefaults.pure:254-255 — dialects without a
     *  native null-safe operator spell the OR-expansion; the format
     *  strings carry their own parens (incl. around {@code not (..)}),
     *  so precedence is correct by construction. */
    @Override
    protected SqlWriter expr(SqlWriter writer, SqlExpr e, int parentPrec) {
        if (e instanceof com.legend.sql.SqlExpr.Call c) {
            SqlWriter.Piece a = w -> w.expr(c.args().get(0), 4);
            SqlWriter.Piece b = w -> w.expr(c.args().get(1), 4);
            if (c.fn() == com.legend.sql.SqlFn.NULL_SAFE_EQUAL) {
                return writer.append("(").piece(a).append(" = ").piece(b).append(" or (").piece(a)
                        .append(" is null and ").piece(b).append(" is null))");
            }
            if (c.fn() == com.legend.sql.SqlFn.NULL_SAFE_NOT_EQUAL) {
                return writer.append("(not (").piece(a).append(" = ").piece(b).append(") or (").piece(a)
                        .append(" is null and ").piece(b).append(" is not null) or (").piece(a)
                        .append(" is not null and ").piece(b).append(" is null))");
            }
        }
        return super.expr(writer, e, parentPrec);
    }

    @Override
    protected SqlWriter whereSql(SqlWriter writer, SqlExpr w) {
        // DB2 plan goldens wrap a top-level AND only when a conjunct is
        // OR-ROOTED (the null-safe expansions) — 'where ((A) and (B))';
        // plain conjuncts render bare ('where A in (...) and B in (...)'
        // — the temp-table IN protocol golden pins the bare form)
        boolean wrap = w instanceof SqlExpr.Call c
                && c.fn() == com.legend.sql.SqlFn.AND
                && c.args().stream().anyMatch(EngineStyleDB2::orRooted);
        return wrap ? writer.append("(").expr(w, 0).append(")") : writer.expr(w, 0);
    }

    /** An OR-rooted conjunct — explicit or/null-safe nodes AND the
     * equality-over-OPTIONAL-param form, whose null-safe or-expansion
     * happens at RENDER time ({@code nullSafeEq}); Groups unwrap. */
    private static boolean orRooted(SqlExpr a) {
        if (a instanceof SqlExpr.Group g) {
            return orRooted(g.inner());
        }
        if (!(a instanceof SqlExpr.Call oc)) {
            return false;
        }
        if (oc.fn() == com.legend.sql.SqlFn.OR
                || oc.fn() == com.legend.sql.SqlFn.NULL_SAFE_EQUAL
                || oc.fn() == com.legend.sql.SqlFn.NULL_SAFE_NOT_EQUAL) {
            return true;
        }
        return (oc.fn() == com.legend.sql.SqlFn.EQUAL
                        || oc.fn() == com.legend.sql.SqlFn.NOT_EQUAL)
                && oc.args().stream().anyMatch(x ->
                        x instanceof SqlExpr.PlanParam p && p.optional());
    }

    @Override
    protected SqlWriter.Piece nullSafeEq(SqlWriter.Piece l, SqlWriter.Piece r) {
        // DB2 has no IS NOT DISTINCT FROM — the engine expands the OR
        // form (testFilterEqualsWithOptionalParameter_DB2)
        return w -> w.append("(").piece(l).append(" = ").piece(r).append(" or (").piece(l).append(" is null and ")
                .piece(r).append(" is null))");
    }

    /** DB2 QUOTES boolean placeholders — the mapped case-expr yields
     * {@code 'true'}/{@code 'false'} strings. */
    @Override
    protected String holderArgs(SqlExpr.PlanParam.Kind k) {
        return k == SqlExpr.PlanParam.Kind.BOOLEAN
                ? "\"'\" \"'\" {}" : super.holderArgs(k);
    }

    /** DB2 has no group-by-alias: every key spells the PHYSICAL
     * expression (testToSQLStringWithAggregationDB2 golden —
     * {@code group by "root".FIRSTNAME}); the engine resolves aliases
     * before rendering for DB2 where H2 keeps the TDS alias text. */
    @Override
    protected SqlWriter groupKey(SqlWriter writer, com.legend.sql.SqlSelect s, SqlExpr e) {
        return writer.expr(e, 0);
    }

    @Override
    protected String infixPad(com.legend.sql.SqlFn fn) {
        // DB2 dynafunction templates print arithmetic TIGHT
        // (position(…)-1 — the sqlstring position golden)
        return fn == com.legend.sql.SqlFn.MINUS
                || fn == com.legend.sql.SqlFn.PLUS ? "" : " ";
    }

    @Override
    protected SqlWriter call(SqlWriter writer, SqlExpr.Call c, int parentPrec) {
        List<SqlExpr> a = c.args();
        return switch (c.fn()) {
            // DB2 string concatenation is the infix operator, the whole
            // chain parenthesized once: (a concat b concat c)
            case CONCAT -> writer.append("(").join(flattenConcat(a), " concat ", 0).append(")");
            // interleaved joinStrings unit: same infix chain, NEVER
            // spliced into the enclosing '+' chain (its parens pin it)
            case CONCAT_JOIN -> writer.append("(").join(a, " concat ", 0).append(")");
            case TRIM -> a.size() == 1
                    ? writer.append("trim(").expr(a.get(0), 0).append(")")
                    : super.call(writer, c, parentPrec);
            // native spellings (sqlstring 'common' goldens — the H2
            // parent's regexp/extension forms are H2-specific)
            case CBRT -> writer.append("cbrt(").expr(a.get(0), 0).append(")");
            case STRPOS -> writer.append("position(").expr(a.get(1), 0).append(", ").expr(a.get(0), 0).append(")");
            // DB2 spells the SHORT substr keyword (Composite keeps the
            // parent's full 'substring')
            case SUBSTRING -> writer.append("substr(").list(a).append(")");
            case LTRIM -> writer.append("ltrim(").expr(a.get(0), 0).append(")");
            case RTRIM -> writer.append("rtrim(").expr(a.get(0), 0).append(")");
            case LPAD -> writer.append("lpad(").list(a).append(")");
            case RPAD -> writer.append("rpad(").list(a).append(")");
            case REVERSE_STRING -> writer.append("reverse(").expr(a.get(0), 0).append(")");
            // DB2 spells dayOfYear as the bare function
            // (db2Extension.pure:91 — 'dayofyear(%s)')
            case EXTRACT -> extract(writer, c, parentPrec);
            // DB2 wraps left/right in trim (db2Extension.pure:100/:115 —
            // 'trim(left(%s, %s))'/'trim(right(%s, %s))')
            case LEFT -> writer.append("trim(left(").expr(a.get(0), 0).append(", ").expr(a.get(1), 0).append("))");
            case RIGHT -> writer.append("trim(right(").expr(a.get(0), 0).append(", ").expr(a.get(1), 0).append("))");
            case TODAY -> writer.append("date(current date)");
            case LENGTH -> writer.append("CHARACTER_LENGTH(").expr(a.get(0), 0).append(",CODEUNITS32)");
            // firstDayOf* family: DB2 rebuilds the truncation from epoch
            // date(1) with labeled year/month arithmetic (per-unit golden
            // text, parens verbatim — quarter parenthesizes the YEARS
            // term, month/year do not)
            case DATE_TRUNC -> dateTrunc(writer, c);
            // DB2 interval arithmetic: x - 1 MONTHS / x + 3 DAYS (the
            // relative-date goldens) — the IR's ADD_INTERVAL
            // [unitFn, count, anchor] respells as infix labeled units
            case ADD_INTERVAL -> addInterval(writer, c);
            // parse-date family: DB2's to_date/timestamp_format take the
            // whole string with the Java-style pattern (no substring, no
            // space after the comma — the goldens' exact text)
            case STRPTIME -> strptime(writer, c);
            // every other function, by name: inherited from the parent dialect as written today (the dialect
            // program's Phase 1, 2026-10-13: an exhaustive switch, so a new function asks this dialect too)
            case AND, OR, NOT, EQUAL, NOT_EQUAL, LESS, LESS_EQUAL, GREATER, GREATER_EQUAL, PLUS, MINUS, TIMES,
                 DIVIDE, MOD, REM, NEGATE, ABS, IS_NULL, IS_NOT_NULL, IN, COALESCE, PARSE_INT, UPPER, LOWER,
                 GREATEST, LEAST, LIST_FILTER, LIST_TRANSFORM, LIST_CONCAT, LIST_GET, LIST_POSITION, LIST_EXISTS,
                 LIST_FOR_ALL, STRUCT_INSERT, UNNEST, LIST_FLATTEN, JSON_MERGE_PATCH, SQRT, EXP, LN, LOG10, POW, PI,
                 SIN, COS, TAN, ASIN, ACOS, ATAN, ATAN2, SINH, COSH, TANH, CEILING, FLOOR, ROUND, SIGN, XOR, BIT_AND,
                 BIT_OR, BIT_XOR, BIT_SHIFT_LEFT, BIT_SHIFT_RIGHT, BIT_NOT, REGEXP_EXTRACT, REGEXP_EXTRACT_ALL,
                 REGEXP_REPLACE, REGEXP_FULL_MATCH, MAP_FROM_LISTS, MAP_FROM_ENTRIES, MAP_EMPTY, MAP_EXTRACT,
                 MAP_KEYS, MAP_VALUES, MAP_CONCAT, STARTS_WITH, ENDS_WITH, MATCHES, REPLACE, TRANSLATE, SPLIT, SPLIT_PART,
                 PURE_SPLIT_PART, ASCII_CODE, CHR, UC_FIRST, LC_FIRST, ENCODE_BASE64, LEVENSHTEIN, GUID, RANDOM,
                 FORMAT,
                 HASH, MD5, SHA1, SHA256, NOW, DATE_TRUNC_DAY, MAKE_DATE, MAKE_TIMESTAMP, DATE_DIFF, TIME_BUCKET,
                 TIMEZONE, DATE_AT_TIME, ADD_INTERVAL_TEMPORAL, EPOCH_SECONDS, EPOCH_MS, EPOCH_NS, FROM_EPOCH_NS, FROM_EPOCH_SECONDS, FROM_EPOCH_MS,
                 DAYNAME, MONTHNAME, COT, RADIANS, DEGREES, INT_DIVIDE, REPEAT_STR, JARO_WINKLER, DECODE_BASE64,
                 CURRENT_USER_FN, LIST_LENGTH, LIST_ZIP, LIST_DISTINCT, LIST_APPEND, LIST_SUM, LIST_MIN, LIST_MAX,
                 LIST_AVG, LIST_MEDIAN, LIST_MODE, LIST_SORT, LIST_SORT_DESC, LIST_TAIL, LIST_INIT, RANGE_FN,
                 LIST_PRODUCT, LIST_REDUCE, LIST_SLICE, REPEAT_VALUE, LIST_BOOL_AND, LIST_BOOL_OR, IS_DISTINCT_FROM,
                 ALL_DISTINCT, NULL_SAFE_EQUAL, NULL_SAFE_NOT_EQUAL, STRFTIME, PARSE_DATE, LIST_REVERSE, TYPEOF,
                 ROUND_HALF_UP, ERROR, FLOOR_RAW, TO_VARIANT, BOOL_TO_TEXT, FLOAT_CANON, FLOAT_PRINT, FLOAT_PRINT_ENGINE, VARIANT_ELEMENTS,
                 VARIANT_GET, JSON_TYPE, JSON_ARRAY_LENGTH, JSON_PRETTY -> super.call(writer, c, parentPrec);
        };
    }

    /** The {@code EXTRACT} arm of {@link #call}. */
    private SqlWriter extract(SqlWriter writer, SqlExpr.Call c, int parentPrec) {
        if (c.args().size() == 2 && part(c.args().get(0)) == com.legend.sql.DatePart.DAY_OF_YEAR) {
            writer.append("dayofyear(").expr(c.args().get(1), 0).append(")");
        } else {
            super.call(writer, c, parentPrec);
        }
        return writer;
    }

    /** The {@code DATE_TRUNC} arm of {@link #call}. */
    private SqlWriter dateTrunc(SqlWriter writer, SqlExpr.Call c) {
        List<SqlExpr> a = c.args();
        if (a.size() == 2) {
            SqlWriter.Piece anchor = a.get(1) instanceof SqlExpr.Call tc
                    && tc.fn() == com.legend.sql.SqlFn.TODAY
                    ? w -> w.append("current date") : w -> w.expr(a.get(1), 0);
            SqlWriter.Piece spelled = switch (part(a.get(0))) {
                case YEAR -> w -> w.append("date(1) + (year(").piece(anchor).append(")-1) YEARS");
                case MONTH -> w -> w.append("date(1) + (year(").piece(anchor)
                        .append(")-1) YEARS + (month(").piece(anchor).append(")-1) MONTHS");
                case QUARTER -> w -> w.append("date(1) + ((year(").piece(anchor)
                        .append(")-1) YEARS) + (3 * QUARTER(").piece(anchor).append(") - 3) MONTHS");
                case WEEK, DAY, HOUR, MINUTE, SECOND, MILLISECOND, MICROSECOND, NANOSECOND, DAY_OF_YEAR,
                        ISO_DAY_OF_WEEK -> null;
            };
            if (spelled != null) {
                return writer.piece(spelled);
            }
        }
        throw new IllegalStateException("date_trunc unit has no"
                + " engine-DB2 spelling yet: " + a);
    }

    /** The {@code ADD_INTERVAL} arm of {@link #call}. */
    private SqlWriter addInterval(SqlWriter writer, SqlExpr.Call c) {
        List<SqlExpr> a = c.args();
        if (a.size() == 3 && a.get(1) instanceof SqlExpr.IntLit n) {
            com.legend.sql.DatePart u = part(a.get(0));
            String unit = switch (u) {
                case YEAR -> "YEARS";
                case MONTH -> "MONTHS";
                case DAY -> "DAYS";
                case HOUR -> "HOURS";
                case MINUTE -> "MINUTES";
                case SECOND -> "SECONDS";
                case QUARTER, WEEK, MILLISECOND, MICROSECOND, NANOSECOND, DAY_OF_YEAR, ISO_DAY_OF_WEEK -> null;
            };
            long v = n.value();
            if (unit == null && u == com.legend.sql.DatePart.WEEK) {
                // DB2 has no WEEKS label — the engine spells
                // weeks as days ('- 14 DAYS' for 2 weeks)
                unit = "DAYS";
                v = v * 7;
            }
            if (unit != null) {
                return writer.expr(a.get(2), 0).append((v < 0 ? " - " + (-v) : " + " + v)).append(" ")
                        .append(unit);
            }
        }
        throw new IllegalStateException("interval shape has no"
                + " engine-DB2 spelling yet: " + a);
    }

    /** The {@code STRPTIME} arm of {@link #call}. */
    private SqlWriter strptime(SqlWriter writer, SqlExpr.Call c) {
        List<SqlExpr> a = c.args();
        if (a.size() == 2 && a.get(1) instanceof SqlExpr.FormatLit fl) {
            String java = db2Pattern(fl);
            if (java != null) {
                boolean dateOnly = !fl.parts()
                        .contains(com.legend.sql.DateFmt.Part.HOUR2);
                return writer.append((dateOnly ? "to_date(" : "timestamp_format(")).expr(a.get(0), 0)
                        .append(",'").append(java).append("')");
            }
        }
        throw new IllegalStateException("strptime format has no"
                + " engine-DB2 spelling yet: " + a);
    }

    /** TYPED format parts → DB2's pattern spelling; null when a part has
     * no mapping (the caller throws — never a silent fallback). */
    private static @com.legend.base.Nullable String db2Pattern(SqlExpr.FormatLit fl) {
        StringBuilder out = new StringBuilder();
        for (com.legend.sql.DateFmt d : fl.parts()) {
            switch (d) {
                case com.legend.sql.DateFmt.Text t -> out.append(t.s());
                case com.legend.sql.DateFmt.Part p -> {
                    String java = switch (p) {
                        case YEAR4 -> "yyyy";
                        case MONTH2 -> "MM";
                        case DAY2 -> "dd";
                        case HOUR2 -> "hh";
                        case MIN2 -> "mm";
                        case SEC2 -> "ss";
                        case SUBSEC_MIN -> "mmm";
                        default -> null;
                    };
                    if (java == null) {
                        return null;
                    }
                    out.append(java);
                }
            }
        }
        return out.toString();
    }

    @Override
    protected SqlWriter variantAwareCast(SqlWriter writer, SqlExpr.Cast c) {
        // T4 leg 1: synth-conformance casts are engine-TEXT-elided
        // (the wire-coercion precedent — the engine's SQL never
        // spells its decode-side coercion)
        if (c.conform()) {
            return writer.expr(c.value(), 0);
        }
        // DB2 string casts carry the engine's explicit width
        if (c.target() instanceof SqlType.Scalar s
                && s == SqlType.Scalar.VARCHAR) {
            return writer.append("cast(").expr(c.value(), 0).append(" as varchar(16000))");
        }
        // to_date/timestamp_format and infix interval arithmetic are
        // already date-typed — the IR's normalizing casts render bare
        // (the goldens carry no cast)
        if (c.target() instanceof SqlType.Scalar s2
                && (s2 == SqlType.Scalar.DATE || s2 == SqlType.Scalar.TIMESTAMP)
                && c.value() instanceof SqlExpr.Call pc
                && (pc.fn() == com.legend.sql.SqlFn.STRPTIME
                        || pc.fn() == com.legend.sql.SqlFn.ADD_INTERVAL)) {
            return writer.expr(pc, 0);
        }
        return super.variantAwareCast(writer, c);
    }

}
