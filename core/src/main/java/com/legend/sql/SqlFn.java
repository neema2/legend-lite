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
    // parseInteger: 64-bit SEMANTICS everywhere (PCT pins Long.MIN/MAX);
    // a semantic entry (not a bare Cast) so the engine-style dialect can
    // spell its golden 'integer' while execution casts BIGINT (the
    // per-dynafunction origin tag audit 19 F3 called for)
    PARSE_INT,
    // Strings
    CONCAT, LENGTH, UPPER, LOWER, GREATEST, LEAST,
    // joinStrings over a literal element list in VALUE (map) position:
    // separator INTERLEAVED between elements. A DISTINCT fn from CONCAT
    // so nested-concat splicing never flattens it (the engine keeps the
    // joinStrings unit nested inside a '+' chain; DB2 spells it as a
    // parenthesized infix chain).
    CONCAT_JOIN,
    // Lists — SEMANTIC operations (encodings are the dialect's: DuckDB uses
    // list lambdas; Postgres would use unnest subqueries; SQLite json_each).
    // LIST_EXISTS/LIST_FOR_ALL carry (collection, predicate-lambda) and
    // include Pure's empty-collection semantics (exists([])=false,
    // forAll([])=true) — the dialect's expansion must honor them.
    LIST_FILTER, LIST_TRANSFORM, LIST_CONCAT, LIST_GET, LIST_POSITION,
    LIST_EXISTS, LIST_FOR_ALL,
    /** struct_insert(struct, name, value): the struct with one field
     * appended (the construction-site canon rides the bound struct). */
    STRUCT_INSERT,
    // UNNEST: explode a collection into rows; PLACEMENT (select-list vs
    // LATERAL FROM) is dialect assembly.
    UNNEST,
    /** Flatten one level of a list-of-lists (pure map's collection-mapper flatten). */
    LIST_FLATTEN,
    JSON_MERGE_PATCH,   // json_merge_patch(obj, patch) — null patch keys DROP (RFC 7386)
    // Math (ROUND is HALF-EVEN — Pure's banker's rounding — per the contract)
    SQRT, CBRT, EXP, LN, LOG10, POW, PI,
    SIN, COS, TAN, ASIN, ACOS, ATAN, ATAN2, SINH, COSH, TANH,
    CEILING, FLOOR, ROUND, SIGN, XOR,
    BIT_AND, BIT_OR, BIT_XOR, BIT_SHIFT_LEFT, BIT_SHIFT_RIGHT, BIT_NOT,
    REGEXP_EXTRACT, REGEXP_EXTRACT_ALL, REGEXP_REPLACE, REGEXP_FULL_MATCH,
    MAP_FROM_LISTS, MAP_FROM_ENTRIES, MAP_EMPTY, MAP_EXTRACT, MAP_KEYS, MAP_VALUES, MAP_CONCAT,
    // Strings (SUBSTRING/STRPOS are 1-based SQL; frontends shift at lowering)
    SUBSTRING, STRPOS, STARTS_WITH, ENDS_WITH, MATCHES, LEFT, RIGHT,
    LPAD, RPAD, TRIM, LTRIM, RTRIM, REPLACE, SPLIT, SPLIT_PART,
    /** translate(str, from, to): each character of {@code from} in {@code str} becomes the character at its
     *  position in {@code to}, or is removed past {@code to}'s end. One pass, so a mapping never chains. The SQL
     *  islands' ASCII-only case folding (the tenth line's ask, answered 2026-10-15). */
    TRANSLATE,
    /** Pure's splitPart(str, token, 1-based part): split DROPPING empty tokens,
     *  indexed; NULL past the end (batch 139). Semantic — each dialect spells it. */
    PURE_SPLIT_PART,
    REVERSE_STRING, ASCII_CODE, CHR, UC_FIRST, LC_FIRST,
    ENCODE_BASE64, LEVENSHTEIN, GUID, FORMAT, HASH, MD5, SHA1, SHA256,
    /** The database's random float in [0, 1) (lite's {@code random()}, the Cypher island's {@code rand()}): a value
     *  of the call, not of the data, read once per row it is spelled in. */
    RANDOM,
    // Temporal: EXTRACT takes a part-name literal first (one entry, all parts)
    EXTRACT, TODAY, NOW, DATE_TRUNC_DAY, MAKE_DATE, MAKE_TIMESTAMP,
    // Temporal (part-literal-first like EXTRACT): truncation, arithmetic,
    // bucketing, epoch conversion, calendar names
    DATE_TRUNC, ADD_INTERVAL, DATE_DIFF, TIME_BUCKET, TIMEZONE,
    /** {@code (date, time)}: the date-time at that time of that day (Pure's {@code meta::legend::lite::atTime}). */
    DATE_AT_TIME,
    /** {@code (ts)}: the nanoseconds since the epoch, exact where the engine keeps nanoseconds (DuckDB's
     *  {@code epoch_ns} over a TIMESTAMP_NS); a microsecond engine refuses by name. What {@code dateDiff} with
     *  {@code NANOSECONDS} and {@code adjust} with {@code NANOSECONDS} lower to (the Cypher island's nanosecond
     *  leg, 2026-10-16): the nanosecond unit has no interval in any engine. */
    EPOCH_NS,
    /** {@code (n)}: the timestamp {@code n} nanoseconds after the epoch, to the nanosecond (DuckDB's
     *  {@code make_timestamp_ns}, a TIMESTAMP_NS); a microsecond engine refuses by name. */
    FROM_EPOCH_NS,
    /** milestoning adjust() shift — same semantics as ADD_INTERVAL; the
     *  engine-H2 golden spells its unit UPPERCASE (adjust/mapToDBUnitType
     *  channel: dateadd(DAY,...) vs the dynaFn's dateadd(day,...)). */
    ADD_INTERVAL_TEMPORAL,
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
    LIST_AVG, LIST_MEDIAN, LIST_MODE, LIST_SORT, LIST_SORT_DESC,
    LIST_TAIL, LIST_INIT, RANGE_FN, LIST_PRODUCT, LIST_REDUCE, LIST_SLICE,
    /** n copies of one value ({@code repeat(e, n)}). */
    REPEAT_VALUE,
    LIST_BOOL_AND, LIST_BOOL_OR, IS_DISTINCT_FROM, ALL_DISTINCT,
    NULL_SAFE_EQUAL, NULL_SAFE_NOT_EQUAL,
    STRFTIME, STRPTIME, PARSE_DATE, LIST_REVERSE, TYPEOF,
    ROUND_HALF_UP, ERROR, FLOOR_RAW,
    // Variant construction
    TO_VARIANT, BOOL_TO_TEXT,
    /** A Float's Pure literal spelling (2026-10-11): every zero {@code 0.0}, the exponent unfolded to fixed point, at
     *  least one fraction digit -- the kind-faithful carrier's text of a double (lowering/LiteralSpelling). One call
     *  per value, which each dialect spells (dialect/FloatCanonText the portable decomposition). */
    FLOAT_CANON,
    /** A Float's text as Pure's {@code toString} prints it (2026-10-22): the shortest digits, plain within the
     *  database's plain range and fixed point past it where exact, at least one fraction digit (lowering/LiteralSpelling).
     *  One call per value, which each dialect spells (dialect/FloatPrintText the portable decomposition). */
    FLOAT_PRINT,
    /** A Float's text as legend-engine's {@code pure/v1} execute writes it (2026-10-22): Java's
     *  {@code Double.toString} layout over the shortest digits. One call per value, as {@link #FLOAT_PRINT}. */
    FLOAT_PRINT_ENGINE,
    // Variant navigation: logical JSON access; text-extraction idioms are rendering
    VARIANT_ELEMENTS, VARIANT_GET, JSON_TYPE,
    /** json_array_length(doc): the element count of a JSON array (the size of a serialized class collection — database-mode assertSize). */
    JSON_ARRAY_LENGTH,
    /** The pretty-printed JSON text (meta::json::toPrettyJSONString). */
    JSON_PRETTY;

    /** Whether this function's RESULT is a list value — function
     * METADATA (moved here from the dissolved ListShapes): read by the
     * stamp invariant's shape prover and the toOne emission-form
     * dispatch. Never the LIST_AGG family (list_aggregate reduces to a
     * scalar). MAP_KEYS/MAP_VALUES, REGEXP_EXTRACT_ALL and SPLIT
     * produce lists (witnesses in the corpus: values()->sort(),
     * chunk()->sort(), split(':')->sort()->joinStrings). */
    public boolean producesList() {
        return switch (this) {
            // LIST_SORT_DESC + RANGE_FN joined 2026-08-21 (shortcut
            // audit §1a: ascending and descending sort had DIFFERENT
            // safety guarantees; range() was unlisted). This is honest
            // function METADATA — the toOne lane itself now decides
            // from the TYPED operand (CollectionLanes), not from here.
            case LIST_CONCAT, LIST_TRANSFORM, LIST_SORT, LIST_SORT_DESC,
                    LIST_FILTER, LIST_TAIL, LIST_INIT, LIST_SLICE,
                    LIST_APPEND, LIST_FLATTEN, LIST_DISTINCT,
                    LIST_REVERSE, LIST_ZIP, RANGE_FN, REPEAT_VALUE,
                    MAP_KEYS, MAP_VALUES, REGEXP_EXTRACT_ALL, SPLIT -> true;
            default -> false;
        };
    }
}
