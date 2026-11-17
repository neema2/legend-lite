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
    /** Texts joined by a separator ({@code sep}, then two or more parts), an empty ({@code NULL}) part skipped with its
     *  separator: the product's join of a row's cells, where an empty cell is skipped (the user's ruling of 2026-11-10,
     *  "makeString skips empty elements"; D1.3's join form as one call, Phase 3 of
     *  docs/records/total-wall-edge-program-2026-11-16.md, 2026-11-16). {@code CONCAT_WS} where the database's skips a
     *  {@code NULL} (measured per database); another correct form where it does not ({@code dialect.CellTexts}). */
    CONCAT_WS,
    // Lists — SEMANTIC operations (encodings are the dialect's: DuckDB uses
    // list lambdas; Postgres would use unnest subqueries; SQLite json_each).
    // LIST_EXISTS/LIST_FOR_ALL carry (collection, predicate-lambda) and
    // include Pure's empty-collection semantics (exists([])=false,
    // forAll([])=true) — the dialect's expansion must honor them.
    LIST_FILTER, LIST_TRANSFORM, LIST_CONCAT, LIST_GET, LIST_POSITION,
    /** {@code (list, position)}: the element at a 1-based position the list's own construction names (a tagged
     *  list's positions, never negative; past the end NULL), written with its list and its position ONCE -- where
     *  {@link #LIST_GET}'s negative-index arms write them two to five times, which nested positions multiply
     *  (2026-11-03: the tagged list's statements, 18.7K -> 721.8K chars on Snowflake, past SQLite's 1 MB). */
    LIST_AT,
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
    /** Its one text argument in the database's BINARY collation, an order key: texts ordered by their code points,
     *  whatever the column's or the database's own collation (the test-data generator's seed, the user's ruling R4 of
     *  docs/data-delivery-homework/TEST_DATA_GENERATOR_B2D3_DESIGN_2026_11_11.md §5, its design's §3.1; register S198's
     *  note; the B2d.3 audit's M4, 2026-11-12): Postgres {@code COLLATE "C"}, SQLite {@code COLLATE BINARY}; DuckDB and
     *  H2 compare texts by their code points already (measured 2026-11-12), so the text as it is; a dialect whose
     *  binary collation is not measured refuses it by name. */
    BINARY_COLLATE,
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
    /** {@code (x)}: a Decimal's or a Number's text, its plain digits (the tenets audit's M-6, A6, 2026-11-10): a semantic
     *  node the Lowerer marks by the operand's Pure type, as {@link #BOOL_TO_TEXT} is -- each dialect spells it, most
     *  as the VARCHAR cast they wrote until then; H2 with its number printer for every such operand (its DECFLOAT's
     *  cast writes {@code 1E+2}), where its dialect walked the expression's shape and missed some. */
    DECIMAL_TO_TEXT,
    /** LITE'S CANONICAL TEXT OF A FLOAT, its shortest round-trip digits (renamed from FLOAT_PRINT 2026-11-17, census B9,
     *  the orchestrator's ruling: measured equal to lite's writers' text, result.CellText.carriedFloat, inside its
     *  windows): plain within the database's plain range and fixed point past it where exact (whole values below 1e38,
     *  fractional ones from 1e-17 below 2^53), at least one fraction digit, NaN and the infinities by name. One call per
     *  value, which each dialect spells (dialect/FloatShortestText the portable decomposition; a dialect whose text of a
     *  double is long reads it once). A message's text of a Float (the judges', both judges alike, G2); legend-pure's
     *  print form is the same text, so the compatibility edge writes this node for {@link #FLOAT_TO_TEXT}. Outside the
     *  windows the database's own exponent notation stays, which differs per database: logged for after separation
     *  ("one canonical Float text outside the windows"). */
    FLOAT_SHORTEST_TEXT,
    /** A Float's text ({@code toString}, {@code format}'s {@code %f}): the product's is six decimals, the trailing
     *  zeros and a trailing point trimmed, NaN and the infinities by name (the user's ruling of 2026-11-10, D_BRIEF
     *  §1; phase D's D1.2, 2026-11-15) -- each dialect writes it ({@code AnsiSqlRenderer.floatToText}); the
     *  compatibility edge writes {@link #FLOAT_SHORTEST_TEXT} for it, legend-pure's print form (edge/PurePrintForms). */
    FLOAT_TO_TEXT,
    /** A DateTime's text ({@code toString}, {@code format}'s {@code %s}): the product's is ISO 8601 in UTC, six fraction
     *  digits and {@code Z} (the user's ruling of 2026-11-10, D_BRIEF §1; phase D's D1.2, 2026-11-15), a STRFTIME the
     *  base renderer expands it to ({@code dialect.DateTimeText}). One operand, the value (the audit's K15, 2026-11-17:
     *  a literal's written text, which legend-pure prints at its written precision, is the edge's alone, through the
     *  style's two-form DateTimeTexts). The compatibility edge's dialects print it legend-pure's way
     *  (edge.PureDateTimePrint; census B12, 2026-11-17). */
    DATETIME_TO_TEXT,
    // (DATETIME_PRINT, legend-pure's print form of a DateTime: gone 2026-11-17, census B12 -- the edge composes it from
    // the product's nodes, edge.PureDateTimePrint, at lowering and in its print pass)
    /** THE JUDGES' CANON OF A TIMESTAMP (census C5's step C, 2026-11-17): {@code T}-separated, the fraction's digits
     *  with their trailing zeros trimmed (a timestamp carries no written precision), {@code Z} -- lite's spelling, which
     *  every side of a comparison is written in, so lite's date equality (A1) is kept and no message reads it.
     *  Expanded before every other pass by the base renderer ({@code dialect.DateTimeCanonText}), from STRFTIME at
     *  nine digits, which every SQL dialect spells; one node in a plan's tree. */
    DATETIME_CANON_TEXT,
    /** An element's text in a {@code makeString} ({@code x}, one operand, {@code NULL} where the element is empty): the
     *  product's join skips an empty element (the user's ruling of 2026-11-10, D_BRIEF §1: "makeString skips empty
     *  elements"; phase D's D1.3, 2026-11-15) -- the base renderer writes the text alone ({@code dialect.CellTexts}),
     *  the join's {@code STRING_AGG} drops a {@code NULL}. legend-engine prints an empty TDS cell as {@code TDSNull}: the
     *  compatibility edge writes that (edge/PurePrintForms). */
    CELL_TEXT,
    // (FORMAT_PADDED, format's %0<w>d: gone 2026-11-17, the separation census's B8 (i) -- the edge composes it at
    // lowering through the plan style's PureOnlyTexts facet, edge.EdgePureOnlyTexts; the product refuses at lowering)
    // (an instance's toString: gone 2026-11-17, the separation census's B8 -- the edge writes '@_' and
    // the id at lowering through the plan style's PureOnlyTexts facet, read by Scalars.pureToString under the caller's
    // explicit Styled; the product refuses at lowering)
    // (a relation's text: gone 2026-11-17, the separation census's B8 -- the edge's
    // RelationTextLowering writes the text itself at lowering (B17's facet); the product refuses at lowering)
    // (a user's toRepresentation: gone 2026-11-17, the separation census's B8 (i) -- the edge
    // writes legend-pure's representation at lowering through the plan style's PureOnlyTexts facet; the product
    // refuses at lowering)
    // (a value's text in two forms: gone 2026-11-17, the separation census's B8 -- a list's text is the
    // plan style's two-form kind lowering.ListTexts: the product's JSON array, the edge's legend-pure [a, b])
    // (an execution's trace text: gone 2026-11-17, the separation census's B8 (ii) -- the plan style's
    // traceText, the product's the id, the edge's legend-engine's comment, written at lowering)
    // Variant navigation: logical JSON access; text-extraction idioms are rendering
    VARIANT_ELEMENTS, VARIANT_GET, JSON_TYPE,
    /** json_array_length(doc): the element count of a JSON array (the size of a serialized class collection — database-mode assertSize). */
    JSON_ARRAY_LENGTH,
    /** The pretty-printed JSON text (meta::json::toPrettyJSONString). */
    JSON_PRETTY;

    /** The functions that raise, or answer a fresh value, when evaluated (an ERROR, a parse, a domain or range check, a
     *  GUID): never moved out of a branch (the dialect's CommonSubexpressions), never dropped unread (the lowering's
     *  SubselectPrune). */
    public static final java.util.Set<SqlFn> RAISING = java.util.Collections.unmodifiableSet(java.util.EnumSet.of(ERROR,
            PARSE_INT, PARSE_DATE, ACOS, ASIN, ADD_INTERVAL, ADD_INTERVAL_TEMPORAL, STRPTIME, GUID, TIMEZONE, MAKE_DATE,
            MAKE_TIMESTAMP));

    /** The functions that may leave an argument unevaluated on SOME dialect: AND, OR and COALESCE (SQL promises no
     *  order of evaluation), and every function a dialect renders with a branch -- a CASE, ClickHouse's
     *  {@code if}/{@code multiIf} (its short_circuit_function_evaluation), an IIF, an IF or a DECODE -- read from each
     *  dialect's render arms (2026-11-15). Any other function evaluates all its arguments on every dialect: the
     *  lowering's LetBinding binds a raising repeat only through those (the always-evaluated set). A render arm that
     *  starts branching joins this set. */
    public static final java.util.Set<SqlFn> MAY_SKIP_AN_ARGUMENT = java.util.Collections.unmodifiableSet(
            java.util.EnumSet.of(AND, OR, COALESCE, ADD_INTERVAL, ADD_INTERVAL_TEMPORAL, ASCII_CODE, BIT_SHIFT_RIGHT,
                    BOOL_TO_TEXT, CBRT, FLOOR_RAW, INT_DIVIDE, JSON_TYPE, LIST_BOOL_AND, LIST_BOOL_OR, LIST_CONCAT,
                    LIST_DISTINCT, LIST_GET, LIST_POSITION, LIST_PRODUCT, MAP_EXTRACT, MAP_FROM_ENTRIES, MAP_FROM_LISTS,
                    MAP_KEYS, MAP_VALUES, MOD, PARSE_INT, REM, REPEAT_STR, REPEAT_VALUE, RIGHT, SPLIT, SPLIT_PART,
                    // SQLite's pad helper branches on the length (the PR1 re-review's B6, 2026-11-16: in a helper, not
                    // an arm, which the arm census missed; MaySkipCensusTest renders whole calls)
                    LPAD, RPAD,
                    // every function some dialect renders with a branch, found by MaySkipCensusTest (the PR1 re-review's
                    // A6, 2026-11-16, made conservative: a strict function read as may-skip costs a missed binding, the
                    // reverse a wrong raise; E-SQL may take one out with a per-dialect proof that it never skips)
                    ACOS, ASIN, CONCAT, CONCAT_JOIN, DAYNAME, DECIMAL_TO_TEXT, DECODE_BASE64, ENCODE_BASE64,
                    EPOCH_MS, EPOCH_SECONDS, FLOAT_SHORTEST_TEXT, HASH, JARO_WINKLER, LEFT, LENGTH, LEVENSHTEIN,
                    LIST_APPEND, LIST_AVG, LIST_FLATTEN, LIST_INIT, LIST_MEDIAN, LIST_MODE, LIST_REVERSE, LIST_SLICE,
                    LIST_SORT, LIST_SORT_DESC, LIST_TAIL, MD5, MONTHNAME, PARSE_DATE, PURE_SPLIT_PART, RANGE_FN,
                    REGEXP_EXTRACT_ALL, ROUND, ROUND_HALF_UP, SHA1, SHA256, TO_VARIANT, UNNEST, VARIANT_ELEMENTS,
                    VARIANT_GET,
                    // a Float's and a DateTime's text, branching on the value's magnitude and kind (D's W3, its
                    // instrument, and S312's form, 2026-11-16; found by MaySkipCensusTest at the merge of PR1's head)
                    FLOAT_TO_TEXT, DATETIME_CANON_TEXT, DATETIME_TO_TEXT,
                    // the product's join of a row's cells skips a NULL part by design (Phase 3, 2026-11-17; found by
                    // MaySkipCensusTest: MariaDB, MySQL, SQLite, BigQuery and MongoDB branch or skip)
                    CONCAT_WS));

    /** Whether this function draws afresh at every place it is written -- the database's random float ({@link #RANDOM})
     *  or uuid ({@link #GUID}) -- so two places of it are two values: a value holding one is computed once, in its own
     *  select, never written into its readers (the lowering's Fold.nondeterministic; the dialect's SelectLayers, the
     *  A2' review's finding 1, 2026-11-06). */
    public boolean drawsAfresh() {
        return this == RANDOM || this == GUID;
    }

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
