// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import com.legend.sql.SqlExpr;

import java.util.List;

/**
 * The MODERN-H2 profile (2.3+, probed on 2.4.240) — {@link H2} plus the
 * typed-JSON navigation newer H2 grew: {@code (j)."field"} (the key
 * QUOTES — unquoted identifiers upcase and silently miss) and
 * {@code (j)[i]} with ONE-BASED indexes (the reference IR convention is
 * zero-based — the spelling shifts). Navigation reads TYPED JSON only:
 * the JSON_ARRAY / JSON_ARRAYAGG / JSON_OBJECT carriers and JSON
 * columns all feed it (probed); a CAST from VARCHAR does not. Selected
 * by CONNECTED VERSION at the driver seam ({@code Compiler.dialectOf})
 * — the 2.1.214 engine-parity target keeps {@link H2}'s walls.
 */
public class H2Modern extends H2 {

    /** Refused: this dialect is chosen for a server at run time ({@link H2#forServer}), never planned with, and the
     *  servers it is for (every one but {@link H2#SERVERS}) are none a plan can name (the dialect program's Phase 5,
     *  2026-10-16, the audit of the branch). */
    @Override
    public java.util.List<String> serverVersions() {
        throw new DialectCapability("H2Modern: a plan is written for H2's servers " + H2.SERVERS
                + ", and H2Modern is chosen for any other at run time");
    }

    /** Every scalar type's name: H2's (the dialect program's Phase 1, 2026-10-13: an exhaustive
     *  switch, so a new scalar type asks this dialect too). */
    @Override
    protected String scalarTypeName(com.legend.sql.SqlType.Scalar s) {
        return switch (s) {
            case BOOLEAN, INTEGER, BIGINT, HUGEINT, DOUBLE, VARCHAR, DATE, TIMESTAMP, TIMESTAMPTZ, TIME, JSON,
                 LITERAL, TEMPORAL_TEXT, DECIMAL_TEXT ->
                    super.scalarTypeName(s);
        };
    }

    /** A date format's parts: H2's (the dialect program's Phase 1, 2026-10-13: each dialect
     *  declares its date-format vocabulary). */
    @Override
    protected String formatText(SqlExpr.FormatLit fl) {
        return super.formatText(fl);
    }

    /** Every aggregate, by name: H2's (the dialect program's Phase 1, 2026-10-13: an exhaustive switch, so a new
     *  aggregate asks this dialect too). */
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

    /** Already the 2.3+ profile. */
    @Override
    public SqlDialect forServer(String version) {
        return this;
    }

    /** {@code (x)."key"} / {@code (x)[i+1]} — dynamic keys have no
     * spelling (field access is an identifier) and fall to the wall. */
    @Override
    protected SqlWriter variantGet(SqlWriter writer, List<SqlExpr> args) {
        if (args.get(1) instanceof SqlExpr.StringLit key) {
            return writer.append("(").expr(args.get(0), 7).append(").\"")
                    .append(key.value().replace("\"", "\"\"")).append("\"");
        }
        if (args.get(1) instanceof SqlExpr.IntLit ix) {
            return writer.append("(").expr(args.get(0), 7).append(")[").append(ix.value() + 1).append("]");
        }
        return super.variantGet(writer, args);
    }

    /** Struct values ride the JSON-object carrier here: literal ->
     * JSON_OBJECT (canonical field order), field read -> navigation. */
    @Override
    protected SqlWriter structLit(SqlWriter writer, SqlExpr.StructLit s) {
        return writer.append("JSON_OBJECT(")
                .join(s.fields(), ", ", (w, f) -> w.append(stringLit(f.name())).append(": ").expr(f.value(), 0))
                .append(")");
    }

    @Override
    protected SqlWriter structGet(SqlWriter writer, SqlExpr.StructGet g) {
        return writer.append("(").expr(g.source(), 7).append(").\"").append(g.field().replace("\"", "\"\""))
                .append("\"");
    }

    @Override
    protected SqlWriter call(SqlWriter writer, SqlExpr.Call c, int parentPrec) {
        // every function, by name: LIST_LENGTH this database's own, the rest the parent's (the dialect program's
        // Phase 1, 2026-10-13: an exhaustive switch, so a new function asks this dialect too)
        return switch (c.fn()) {
            case LIST_LENGTH -> listLength(writer, c, parentPrec);
            case AND, OR, NOT, EQUAL, NOT_EQUAL, LESS, LESS_EQUAL, GREATER, GREATER_EQUAL, PLUS, MINUS, TIMES, DIVIDE,
                 MOD, REM, NEGATE, ABS, IS_NULL, IS_NOT_NULL, IN, COALESCE, PARSE_INT, CONCAT, LENGTH, UPPER, LOWER,
                 GREATEST, LEAST, CONCAT_JOIN, LIST_FILTER, LIST_TRANSFORM, LIST_CONCAT, LIST_GET, LIST_POSITION,
                 LIST_EXISTS, LIST_FOR_ALL, STRUCT_INSERT, UNNEST, LIST_FLATTEN, JSON_MERGE_PATCH, SQRT, CBRT, EXP,
                 LN, LOG10, POW, PI, SIN, COS, TAN, ASIN, ACOS, ATAN, ATAN2, SINH, COSH, TANH, CEILING, FLOOR, ROUND,
                 SIGN, XOR, BIT_AND, BIT_OR, BIT_XOR, BIT_SHIFT_LEFT, BIT_SHIFT_RIGHT, BIT_NOT, REGEXP_EXTRACT,
                 REGEXP_EXTRACT_ALL, REGEXP_REPLACE, REGEXP_FULL_MATCH, MAP_FROM_LISTS, MAP_FROM_ENTRIES, MAP_EMPTY,
                 MAP_EXTRACT, MAP_KEYS, MAP_VALUES, MAP_CONCAT, SUBSTRING, STRPOS, STARTS_WITH, ENDS_WITH, MATCHES,
                 LEFT, RIGHT, LPAD, RPAD, TRIM, LTRIM, RTRIM, REPLACE, TRANSLATE, SPLIT, SPLIT_PART, PURE_SPLIT_PART,
                 REVERSE_STRING, ASCII_CODE, CHR, UC_FIRST, LC_FIRST, ENCODE_BASE64, LEVENSHTEIN, GUID, RANDOM, FORMAT,
                 HASH,
                 MD5, SHA1, SHA256, EXTRACT, TODAY, NOW, DATE_TRUNC_DAY, MAKE_DATE, MAKE_TIMESTAMP, DATE_TRUNC,
                 ADD_INTERVAL, DATE_DIFF, TIME_BUCKET, TIMEZONE, DATE_AT_TIME, ADD_INTERVAL_TEMPORAL, EPOCH_SECONDS, EPOCH_MS, EPOCH_NS, FROM_EPOCH_NS,
                 FROM_EPOCH_SECONDS, FROM_EPOCH_MS, DAYNAME, MONTHNAME, COT, RADIANS, DEGREES, INT_DIVIDE, REPEAT_STR,
                 JARO_WINKLER, DECODE_BASE64, CURRENT_USER_FN, LIST_ZIP, LIST_DISTINCT, LIST_APPEND, LIST_SUM,
                 LIST_MIN, LIST_MAX, LIST_AVG, LIST_MEDIAN, LIST_MODE, LIST_SORT, LIST_SORT_DESC, LIST_TAIL,
                 LIST_INIT, RANGE_FN, LIST_PRODUCT, LIST_REDUCE, LIST_SLICE, REPEAT_VALUE, LIST_BOOL_AND,
                 LIST_BOOL_OR, IS_DISTINCT_FROM, ALL_DISTINCT, NULL_SAFE_EQUAL, NULL_SAFE_NOT_EQUAL, STRFTIME,
                 STRPTIME, PARSE_DATE, LIST_REVERSE, TYPEOF, ROUND_HALF_UP, ERROR, FLOOR_RAW, TO_VARIANT,
                 BOOL_TO_TEXT, FLOAT_CANON, VARIANT_ELEMENTS, VARIANT_GET, JSON_TYPE, JSON_ARRAY_LENGTH, JSON_PRETTY ->
                    super.call(writer, c, parentPrec);
        };
    }

    /** {@code CARDINALITY} counts JSON-array elements on 2.3+ (probed:
     * 3 for '[1,2,3]') — the LIST_LENGTH spelling. HERE, not in
     * listCall: H2's own switch spells LIST_LENGTH as a plain len(...)
     * call, ahead of the idiom dispatch. The Array-cast wrapper only re-types and
     * unwraps. */
    private SqlWriter listLength(SqlWriter writer, SqlExpr.Call c, int parentPrec) {
        if (c.args().size() != 1) {
            return super.call(writer, c, parentPrec);
        }
        SqlExpr arg = c.args().get(0);
        if (arg instanceof SqlExpr.Cast ac
                && ac.target() instanceof com.legend.sql.SqlType.Array) {
            arg = ac.value();
        }
        return writer.append("CARDINALITY(").expr(arg, 0).append(")");
    }

    /** A cast TO JSON is the PARSE intent (DuckDB's CAST parses) — but
     * H2's CAST from VARCHAR QUOTES the text as a JSON string instead
     * (probed: CAST('[10,20]' AS JSON) -> "\"[10,20]\""). The parsing
     * spellings: {@code JSON '...'} for literals, {@code (x FORMAT
     * JSON)} for dynamic text (both probed to navigate). TO_VARIANT
     * keeps the quoting CAST — that IS toVariant's string semantics. */
    private SqlWriter jsonParseCast(SqlWriter writer, SqlExpr.Cast c) {
        if (c.value() instanceof SqlExpr.StringLit sl) {
            return writer.append("JSON ").append(stringLit(sl.value()));
        }
        return writer.append("(").expr(c.value(), 0).append(" FORMAT JSON)");
    }

    /** Scalar casts over a navigation extract TEXT first: H2 rejects
     * JSON -> number/boolean casts outright and its VARCHAR cast keeps
     * the JSON quoting (probed) — TRIM strips it; numbers and booleans
     * carry no quotes and convert cleanly. */
    @Override
    protected SqlWriter variantAwareCast(SqlWriter writer, SqlExpr.Cast c) {
        if (c.target() == com.legend.sql.SqlType.Scalar.JSON) {
            return jsonParseCast(writer, c);
        }
        boolean navigation = c.value() instanceof SqlExpr.Call call
                && (call.fn() == com.legend.sql.SqlFn.VARIANT_GET
                        || call.fn() == com.legend.sql.SqlFn.VARIANT_ELEMENTS)
                || c.value() instanceof SqlExpr.StructGet;
        if (navigation
                && c.target() instanceof com.legend.sql.SqlType.Scalar sc
                && sc != com.legend.sql.SqlType.Scalar.JSON) {
            return writer.append("CAST(TRIM(BOTH '\"' FROM CAST(").expr(c.value(), 0).append(" AS VARCHAR)) AS ")
                    .append(castTypeName(c.target())).append(")");
        }
        return super.variantAwareCast(writer, c);
    }
}
