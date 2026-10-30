// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import com.legend.sql.SqlExpr;

/**
 * The engine's COMPOSITE dialect text: the DEFAULT spellings — native
 * trim/pad/cbrt like DB2's 'common' goldens, but WITHOUT DB2's own
 * quirks ({@code CHARACTER_LENGTH(x,CODEUNITS32)} stays plain
 * {@code char_length}). Divergent goldens fail honestly.
 */
public final class EngineStyleComposite extends EngineStyleDB2 {

    /** Every scalar type's name: the engine DB2 text's (the dialect program's Phase 1, 2026-10-13: an exhaustive
     *  switch, so a new scalar type asks this dialect too). */
    @Override
    protected String scalarTypeName(com.legend.sql.SqlType.Scalar s) {
        return switch (s) {
            case BOOLEAN, INTEGER, BIGINT, HUGEINT, DOUBLE, VARCHAR, DATE, TIMESTAMP, TIMESTAMPTZ, TIME, JSON,
                 TEMPORAL_TEXT, DECIMAL_TEXT ->
                    super.scalarTypeName(s);
        };
    }

    /** A date format's parts: the engine DB2 text's (the dialect program's Phase 1, 2026-10-13: each dialect
     *  declares its date-format vocabulary). */
    @Override
    protected String formatText(SqlExpr.FormatLit fl) {
        return super.formatText(fl);
    }

    /** Every aggregate, by name: the engine DB2 text's (the dialect program's Phase 1, 2026-10-13: an exhaustive
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

    @Override
    protected SqlWriter call(SqlWriter writer, SqlExpr.Call c, int parentPrec) {
        // every function, by name: LENGTH and SUBSTRING this text's own, the rest the parent's (the dialect program's
        // Phase 1, 2026-10-13: an exhaustive switch, so a new function asks this dialect too)
        return switch (c.fn()) {
            case LENGTH -> writer.append("char_length(").expr(c.args().get(0), 0).append(")");
            // Composite keeps the FULL substring keyword (DB2 shortens)
            case SUBSTRING -> writer.append("substring(").list(c.args()).append(")");
            case AND, OR, NOT, EQUAL, NOT_EQUAL, LESS, LESS_EQUAL, GREATER, GREATER_EQUAL, PLUS, MINUS, TIMES, DIVIDE,
                 MOD, REM, NEGATE, ABS, IS_NULL, IS_NOT_NULL, IN, COALESCE, PARSE_INT, CONCAT, UPPER, LOWER, GREATEST,
                 LEAST, CONCAT_JOIN, LIST_FILTER, LIST_TRANSFORM, LIST_CONCAT, LIST_GET, LIST_POSITION, LIST_EXISTS,
                 LIST_FOR_ALL, STRUCT_INSERT, UNNEST, LIST_FLATTEN, JSON_MERGE_PATCH, SQRT, CBRT, EXP, LN, LOG10, POW,
                 PI, SIN, COS, TAN, ASIN, ACOS, ATAN, ATAN2, SINH, COSH, TANH, CEILING, FLOOR, ROUND, SIGN, XOR,
                 BIT_AND, BIT_OR, BIT_XOR, BIT_SHIFT_LEFT, BIT_SHIFT_RIGHT, BIT_NOT, REGEXP_EXTRACT,
                 REGEXP_EXTRACT_ALL, REGEXP_REPLACE, REGEXP_FULL_MATCH, MAP_FROM_LISTS, MAP_FROM_ENTRIES, MAP_EMPTY,
                 MAP_EXTRACT, MAP_KEYS, MAP_VALUES, MAP_CONCAT, STRPOS, STARTS_WITH, ENDS_WITH, MATCHES, LEFT, RIGHT,
                 LPAD, RPAD, TRIM, LTRIM, RTRIM, REPLACE, TRANSLATE, SPLIT, SPLIT_PART, PURE_SPLIT_PART, REVERSE_STRING,
                 ASCII_CODE, CHR, UC_FIRST, LC_FIRST, ENCODE_BASE64, LEVENSHTEIN, GUID, RANDOM, FORMAT, HASH, MD5,
                 SHA1,
                 SHA256, EXTRACT, TODAY, NOW, DATE_TRUNC_DAY, MAKE_DATE, MAKE_TIMESTAMP, DATE_TRUNC, ADD_INTERVAL,
                 DATE_DIFF, TIME_BUCKET, TIMEZONE, DATE_AT_TIME, ADD_INTERVAL_TEMPORAL, EPOCH_SECONDS, EPOCH_MS, EPOCH_NS, FROM_EPOCH_NS, FROM_EPOCH_SECONDS,
                 FROM_EPOCH_MS, DAYNAME, MONTHNAME, COT, RADIANS, DEGREES, INT_DIVIDE, REPEAT_STR, JARO_WINKLER,
                 DECODE_BASE64, CURRENT_USER_FN, LIST_LENGTH, LIST_ZIP, LIST_DISTINCT, LIST_APPEND, LIST_SUM,
                 LIST_MIN, LIST_MAX, LIST_AVG, LIST_MEDIAN, LIST_MODE, LIST_SORT, LIST_SORT_DESC, LIST_TAIL,
                 LIST_INIT, RANGE_FN, LIST_PRODUCT, LIST_REDUCE, LIST_SLICE, REPEAT_VALUE, LIST_BOOL_AND,
                 LIST_BOOL_OR, IS_DISTINCT_FROM, ALL_DISTINCT, NULL_SAFE_EQUAL, NULL_SAFE_NOT_EQUAL, STRFTIME,
                 STRPTIME, PARSE_DATE, LIST_REVERSE, TYPEOF, ROUND_HALF_UP, ERROR, FLOOR_RAW, TO_VARIANT,
                 BOOL_TO_TEXT, FLOAT_CANON, FLOAT_PRINT, VARIANT_ELEMENTS, VARIANT_GET, JSON_TYPE, JSON_ARRAY_LENGTH, JSON_PRETTY ->
                    super.call(writer, c, parentPrec);
        };
    }
}
