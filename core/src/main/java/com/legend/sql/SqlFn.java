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
    /** CAST_INT(text), CAST_FLOAT(text): the database's OWN cast of a text to its 64-bit integer and to its double
     *  (the edition program's step 5, 2026-11-26): every dialect writes its plain CAST, with no grammar check before
     *  it, so a text the database cannot convert is the database's own conversion error. Not {@link #PARSE_INT} nor
     *  a user's cast to a Float, which the product checks against Pure's grammar and raises as its own. Written by
     *  two of the lowering's named rules ({@code NamedRule.TEXT_CAST_TO_INTEGER}, {@code TEXT_CAST_TO_FLOAT}), which
     *  an edition's table alone names. */
    CAST_INT, CAST_FLOAT,
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
    /** acos and asin of a value the statement's own guard holds within [-1, 1] (the lowering's rule raises outside it,
     *  as legend-pure does: register S119): the database's function alone, with none of the dialect's own handling of
     *  a value outside the domain, which {@link #ACOS} and {@link #ASIN} keep for a style that writes the raise plain. */
    ACOS_IN_DOMAIN, ASIN_IN_DOMAIN,
    CEILING, FLOOR, ROUND, SIGN, XOR,
    BIT_AND, BIT_OR, BIT_XOR, BIT_SHIFT_LEFT, BIT_SHIFT_RIGHT, BIT_NOT,
    REGEXP_EXTRACT, REGEXP_EXTRACT_ALL, REGEXP_REPLACE, REGEXP_FULL_MATCH,
    /** isNumeric(s): whether the whole of {@code s} is a decimal number -- an optional sign, digits with an optional
     *  fraction (or a fraction alone), an optional exponent; no space, no other character; NULL on NULL. One rule on
     *  every database (the user's ruling of 2026-11-17 23:05, census J20): each dialect writes it as a full match of
     *  {@link #NUMERIC_TEXT}. */
    IS_NUMERIC,
    MAP_FROM_LISTS, MAP_FROM_ENTRIES, MAP_EMPTY, MAP_EXTRACT, MAP_KEYS, MAP_VALUES, MAP_CONCAT,
    // Strings (SUBSTRING/STRPOS are 1-based SQL; frontends shift at lowering)
    SUBSTRING,
    /** SUBSTRING_AS_GIVEN(str, start[, length]): the database's OWN SUBSTRING over the arguments as given (A3 of the
     *  compatibility program, 2026-11-19, docs/records/compat-homework/A3-engine-rewrite-design.md): every dialect
     *  writes it as its plain SUBSTRING, and no pass rewrites it -- {@code SubstringClamp} raises a start below 1 on
     *  {@link #SUBSTRING} alone -- so a start of 0 or below and a negative length answer as that database reads them,
     *  which is legend-engine's answer by definition. Written by two of the lowering's named rules
     *  ({@code NamedRule.SUBSTRING_AS_GIVEN}, and {@code SUBSTR_AS_GIVEN} over {@code start + 1} and
     *  {@code end - start}, 2026-11-20), which an edition's table alone names. */
    SUBSTRING_AS_GIVEN,
    STRPOS, STARTS_WITH, ENDS_WITH, MATCHES, LEFT, RIGHT,
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
    /** Two sides' typed values the same, IN ORDER (item 3's S4, 2026-11-22, the user: "Yes on list compare"): the
     *  arguments are each side's values as a list (its {@code LIST} in its order, NULL where it has none), then each
     *  side's rows as a subquery -- its values, one column ({@code __o}) or a grid row's cells, one column each, then
     *  {@code __rn}; a Boolean, never NULL. A database that compares lists writes the two lists' {@code IS NOT DISTINCT
     *  FROM}; every other the portable form over the rows ({@code dialect.SameValuesForm}). The two sides' values pair
     *  by position and are of ONE declared kind each: a mixed-kind side never reaches it, a grid's cross-kind column
     *  is its kind tag (register S127: a database's list equality coerces an Integer to a Float). */
    SAME_IN_ORDER,
    /** {@link #SAME_IN_ORDER} in any order: the two sides as multisets. */
    SAME_IN_ANY_ORDER,
    STRFTIME, STRPTIME, PARSE_DATE, LIST_REVERSE, TYPEOF,
    ROUND_HALF_UP,
    /** ROUND_RAW(x[, places]): the database's OWN ROUND over the arguments as given (the edition program's step
     *  5, 2026-11-26): every relational dialect writes it as its plain ROUND, whatever that database's ROUND does with
     *  a value exactly halfway (away from zero on DuckDB and H2). It is not {@link #ROUND}, Pure's half to even on
     *  every database, nor {@link #ROUND_HALF_UP}, half away from zero on every database. Written by one of the
     *  lowering's named rules ({@code NamedRule.ROUND_AS_GIVEN}), which an edition's table alone names. */
    ROUND_RAW,
    /** DECIMAL_RAW(x): the database's OWN cast of a value to its DECIMAL with no precision written (the edition
     *  program's step 5, 2026-11-26): whatever precision and scale that database gives a bare DECIMAL -- three
     *  places on DuckDB, none on H2, where 3.8 is 4 (measured,
     *  docs/records/edition-homework/r3/measured-step5-facts.md A1, B6). No type is stated for it: it differs by
     *  database. Written by one of the lowering's named rules ({@code NamedRule.CAST_TO_BARE_DECIMAL}), which an
     *  edition's table alone names; a dialect whose database's answer is not read refuses it by name. */
    DECIMAL_RAW,
    ERROR,
    // (FLOOR_RAW, the database's own floor of a double as a double: gone 2026-11-26, the edition program's step 8 --
    // its one writer was the composition of FLOAT_SHORTEST_TEXT, deleted the same day)
    // Variant construction
    TO_VARIANT, BOOL_TO_TEXT,
    /** {@code (x)}: a Decimal's or a Number's text, its plain digits (the tenets audit's M-6, A6, 2026-11-10): a semantic
     *  node the Lowerer marks by the operand's Pure type, as {@link #BOOL_TO_TEXT} is -- each dialect spells it, most
     *  as the VARCHAR cast they wrote until then; H2 with its number printer for every such operand (its DECFLOAT's
     *  cast writes {@code 1E+2}), where its dialect walked the expression's shape and missed some. */
    DECIMAL_TO_TEXT,
    // (FLOAT_SHORTEST_TEXT, a Float's shortest round-trip text made plain inside its windows: gone 2026-11-26, the
    // edition program's step 8 -- since B2b's S5a-S5c only the compatibility edge wrote it, for legend-pure's %r of a
    // Float used further in a program, which is lite's own text now, FLOAT_TO_TEXT (tenet 1 as amended 2026-11-25))
    /** A Float's text ({@code toString}, {@code format}'s {@code %f}): the product's is six decimals, the trailing
     *  zeros trimmed, a whole value keeping one ({@code 2.0}), NaN and the infinities by name (the user's ruling of
     *  2026-11-10, D_BRIEF §1; phase D's D1.2, 2026-11-15; S311 amended by the user's ruling of 2026-11-23, "I think
     *  we do it as 2.0") -- each dialect writes it ({@code AnsiSqlRenderer.floatToText}), the
     *  compatibility edge's dialects too (until 2026-11-26 a pass of theirs wrote legend-engine's in-query cast for
     *  it; that cast is a row of legend-engine's table now, {@code edge.EngineTable}, inside a store query alone). */
    FLOAT_TO_TEXT,
    /** A Float's FULL-PRECISION TEXT AS THE DATABASE ITSELF WRITES IT ({@code (x)}, one operand, a double; NULL stays
     *  NULL): toCSV's Float cell (the user's ruling of 2026-11-25, "okay make exact": each database's own full-precision
     *  text, its cast; S311's amendment). Each dialect writes its database's own cast or format function and never
     *  composes a text in SQL (B3's (B), 2026-11-25, under the user's objection to text algorithms composed in SQL):
     *  the plain cast to text on DuckDB, H2, Postgres (a whole value {@code 2}), Trino (always its exponent form,
     *  {@code 1.5E0}), Databricks and BigQuery, {@code CAST AS CHAR} on MariaDB and
     *  MySQL, {@code toString} on ClickHouse, {@code printf('%!.17g')} on SQLite (its cast writes fifteen digits),
     *  {@code $toString} on MongoDB, and on Snowflake its DECFLOAT cast, NaN and the infinities by its plain cast (its
     *  format models cap a FLOAT at sixteen digits, measured 2026-10-24; B3 agreed the same day). The text differs by database
     *  ({@code 1e-07} on DuckDB, {@code 1.0E-7} on H2). It is never one spelling across databases: it is each
     *  database's own. */
    FLOAT_FULL_TEXT,
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
    // RelationTextLowering wrote the text itself at lowering (B17's facet) until B2b S5c deleted it, 2026-11-25)
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

    /** {@link #IS_NUMERIC}'s rule as a regular expression every dialect's full match reads alike (classes and groups
     *  alone, no backslash: a backslash is an escape in some databases' string literals). */
    public static final String NUMERIC_TEXT = "[+-]?([0-9]+([.][0-9]*)?|[.][0-9]+)([eE][+-]?[0-9]+)?";

    /** The functions that raise, or answer a fresh value, when evaluated (an ERROR, a parse, a domain or range check, a
     *  GUID): never moved out of a branch (the dialect's CommonSubexpressions), never dropped unread (the lowering's
     *  SubselectPrune). */
    public static final java.util.Set<SqlFn> RAISING = java.util.Collections.unmodifiableSet(java.util.EnumSet.of(ERROR,
            PARSE_INT, CAST_INT, CAST_FLOAT, PARSE_DATE, ACOS, ASIN, ACOS_IN_DOMAIN, ASIN_IN_DOMAIN, ADD_INTERVAL,
            ADD_INTERVAL_TEMPORAL, STRPTIME, GUID, TIMEZONE, MAKE_DATE, MAKE_TIMESTAMP));

    /** The functions that may leave an argument unevaluated on SOME dialect: AND, OR and COALESCE (SQL promises no
     *  order of evaluation), and every function a dialect renders with a branch -- a CASE, ClickHouse's
     *  {@code if}/{@code multiIf} (its short_circuit_function_evaluation), an IIF, an IF or a DECODE -- read from each
     *  dialect's render arms (2026-11-15). Any other function evaluates all its arguments on every dialect: the
     *  lowering's LetBinding binds a raising repeat only through those (the always-evaluated set). A render arm that
     *  starts branching joins this set. */
    public static final java.util.Set<SqlFn> MAY_SKIP_AN_ARGUMENT = java.util.Collections.unmodifiableSet(
            java.util.EnumSet.of(AND, OR, COALESCE, ADD_INTERVAL, ADD_INTERVAL_TEMPORAL, ASCII_CODE, BIT_SHIFT_RIGHT,
                    BOOL_TO_TEXT, CBRT, INT_DIVIDE, JSON_TYPE, LIST_BOOL_AND, LIST_BOOL_OR, LIST_CONCAT,
                    LIST_DISTINCT, LIST_GET, LIST_POSITION, LIST_PRODUCT, MAP_EXTRACT, MAP_FROM_ENTRIES, MAP_FROM_LISTS,
                    MAP_KEYS, MAP_VALUES, MOD, PARSE_INT, REM, REPEAT_STR, REPEAT_VALUE, RIGHT, SPLIT, SPLIT_PART,
                    // SQLite's pad helper branches on the length (the PR1 re-review's B6, 2026-11-16: in a helper, not
                    // an arm, which the arm census missed; MaySkipCensusTest renders whole calls)
                    LPAD, RPAD,
                    // SQLite writes isNumeric as its regular expression's walk, NULL on NULL by a CASE (census J20, 2026-11-18)
                    IS_NUMERIC,
                    // every function some dialect renders with a branch, found by MaySkipCensusTest (the PR1 re-review's
                    // A6, 2026-11-16, made conservative: a strict function read as may-skip costs a missed binding, the
                    // reverse a wrong raise; E-SQL may take one out with a per-dialect proof that it never skips)
                    ACOS, ASIN, CONCAT, CONCAT_JOIN, DAYNAME, DECIMAL_TO_TEXT, DECODE_BASE64, ENCODE_BASE64,
                    EPOCH_MS, EPOCH_SECONDS, HASH, JARO_WINKLER, LEFT, LENGTH, LEVENSHTEIN,
                    LIST_APPEND, LIST_AVG, LIST_FLATTEN, LIST_INIT, LIST_MEDIAN, LIST_MODE, LIST_REVERSE, LIST_SLICE,
                    LIST_SORT, LIST_SORT_DESC, LIST_TAIL, MD5, MONTHNAME, PARSE_DATE, PURE_SPLIT_PART, RANGE_FN,
                    REGEXP_EXTRACT_ALL, ROUND, ROUND_HALF_UP, SHA1, SHA256, TO_VARIANT, UNNEST, VARIANT_ELEMENTS,
                    VARIANT_GET,
                    // a Float's and a DateTime's text, branching on the value's magnitude and kind (D's W3, its
                    // instrument, and S312's form, 2026-11-16; found by MaySkipCensusTest at the merge of PR1's head)
                    FLOAT_TO_TEXT, DATETIME_CANON_TEXT, DATETIME_TO_TEXT,
                    // a Float's full-precision text: SQLite keeps a NULL by a CASE around printf, Snowflake picks its
                    // DECFLOAT cast or its plain cast by a CASE (toCSV's Float cell, 2026-11-25)
                    FLOAT_FULL_TEXT,
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
