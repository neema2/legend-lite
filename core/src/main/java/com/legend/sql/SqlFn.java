package com.legend.sql;

/**
 * THE semantic-function vocabulary of the SQL IR — the {@code CoreFn} of the
 * back half. Every {@link SqlExpr.Call} names one of these; every dialect
 * renders ALL of them (an exhaustive switch, enforced by javac — adding a
 * semantic function without a spelling in every dialect is a compile error,
 * not a runtime surprise). Lowering emits MEANING; dialects own spelling.
 */
public enum SqlFn {
    // Boolean composition
    AND, OR, NOT,
    // Comparison
    EQUAL, NOT_EQUAL, LESS, LESS_EQUAL, GREATER, GREATER_EQUAL,
    // Arithmetic (DIVIDE forces float; MOD is always-positive; REM keeps sign)
    PLUS, MINUS, TIMES, DIVIDE, MOD, REM, NEGATE, ABS,
    // Null / membership
    IS_NULL, IS_NOT_NULL, IN, COALESCE,
    // Strings
    CONCAT, LENGTH, UPPER, LOWER, GREATEST, LEAST,
    // Lists — SEMANTIC operations (encodings are the dialect's: DuckDB uses
    // list lambdas; Postgres would use unnest subqueries; SQLite json_each).
    // LIST_EXISTS/LIST_FOR_ALL carry (collection, predicate-lambda) and
    // include Pure's empty-collection semantics (exists([])=false,
    // forAll([])=true) — the dialect's expansion must honor them.
    LIST_FILTER, LIST_TRANSFORM, LIST_CONCAT, LIST_CONTAINS, LIST_GET, LIST_POSITION,
    LIST_EXISTS, LIST_FOR_ALL,
    // UNNEST: explode a collection into rows; PLACEMENT (select-list vs
    // LATERAL FROM) is dialect assembly.
    UNNEST,
    // Math (ROUND is HALF-EVEN — Pure's banker's rounding — per the contract)
    SQRT, CBRT, EXP, LN, LOG10, POW, PI,
    SIN, COS, TAN, ASIN, ACOS, ATAN, ATAN2, SINH, COSH, TANH,
    CEILING, FLOOR, ROUND, SIGN, XOR,
    BIT_AND, BIT_OR, BIT_XOR, BIT_SHIFT_LEFT, BIT_SHIFT_RIGHT, BIT_NOT,
    REGEXP_EXTRACT_ALL, REGEXP_REPLACE,
    MAP_FROM_LISTS, MAP_FROM_ENTRIES, MAP_EMPTY, MAP_EXTRACT, MAP_KEYS, MAP_VALUES, MAP_CONCAT,
    // Strings (SUBSTRING/STRPOS are 1-based SQL; frontends shift at lowering)
    SUBSTRING, STRPOS, STARTS_WITH, ENDS_WITH, MATCHES, LEFT, RIGHT,
    LPAD, RPAD, TRIM, LTRIM, RTRIM, REPLACE, SPLIT, SPLIT_PART,
    REVERSE_STRING, ASCII_CODE, CHR, UC_FIRST, LC_FIRST,
    ENCODE_BASE64, LEVENSHTEIN, GUID, FORMAT, HASH, MD5, SHA1, SHA256,
    // Temporal: EXTRACT takes a part-name literal first (one entry, all parts)
    EXTRACT, TODAY, NOW, DATE_TRUNC_DAY, MAKE_DATE, MAKE_TIMESTAMP,
    // Temporal (part-literal-first like EXTRACT): truncation, arithmetic,
    // bucketing, epoch conversion, calendar names
    DATE_TRUNC, ADD_INTERVAL, DATE_DIFF, TIME_BUCKET,
    EPOCH_SECONDS, EPOCH_MS, FROM_EPOCH_SECONDS, FROM_EPOCH_MS,
    DAYNAME, MONTHNAME,
    // Math extras
    COT, RADIANS, DEGREES, INT_DIVIDE,
    // String extras
    REPEAT_STR, JARO_WINKLER, DECODE_BASE64,
    // Runtime
    CURRENT_USER_FN,
    LIST_LENGTH,
    // Lists (beyond the lambda family)
    LIST_ZIP, LIST_DISTINCT, LIST_APPEND, LIST_SUM, LIST_MIN, LIST_MAX,
    LIST_AVG, LIST_MEDIAN, LIST_MODE, LIST_AGG, LIST_SORT, LIST_SORT_DESC,
    LIST_TAIL, LIST_INIT, RANGE_FN, LIST_PRODUCT, LIST_REDUCE, LIST_SLICE,
    LIST_BOOL_AND, LIST_BOOL_OR, IS_DISTINCT, STRFTIME, STRPTIME, LIST_REVERSE, TYPEOF,
    ROUND_HALF_UP, ERROR, FLOOR_RAW,
    // Variant construction
    TO_VARIANT,
    // Variant navigation: logical JSON access; text-extraction idioms are rendering
    VARIANT_ELEMENTS, VARIANT_GET
}
