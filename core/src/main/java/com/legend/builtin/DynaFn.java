package com.legend.builtin;

import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * THE ENGINE'S DYNAFUNCTION REGISTRY, as data: every operator name a relational
 * mapping expression may use ({@code hash(col)}, {@code isDistinct(a, b)},
 * {@code concat(…)}), read from the pinned legend-engine checkout's SQL rendering
 * registries — {@code dynaFnToSql('<name>', …)} in {@code extensionDefaults.pure}
 * and every dialect extension — plus the engine's relational type-inference map
 * ({@code getDynaFunctionTypeInferenceMap}, relationalExtension.pure; five names
 * exist only there) — and how THIS platform resolves each one:
 * <ul>
 *   <li>{@link Resolution#PURE}: passes through to the Pure native(s) of the same
 *       bare name in {@link Pure} — the engine's operator IS pure's function;</li>
 *   <li>{@link Resolution#SHIM}: an engine-only operator with no pure signature
 *       (or a shape pure's differs from) — its {@link Pure.Lite} identity;</li>
 *   <li>{@link Resolution#TRANSLATED}: the mapping translator ({@code RelOpTranslator})
 *       rewrites the call into pure's own spelling (concat → the string run,
 *       add/sub → the arithmetic run, isNull → isEmpty, md5 → hash(…, MD5), …)
 *       and NOTHING passes through — a shape no arm rewrites is an error;</li>
 *   <li>{@link Resolution#UNSUPPORTED}: registered by the engine, handled by nothing
 *       here yet — a mapping using it fails LOUD naming the operator.</li>
 * </ul>
 * A PURE name may ALSO carry a translator arm for the engine's extra shape
 * ({@code and}/{@code or} with more than two operands, {@code parseDate} with a
 * format): the arm rewrites that shape and pure's own shape passes through —
 * {@code DynaFnArms.ARMS} lists every name with an arm, TRANSLATED or PURE.
 * GENERATED from the checkouts by {@code DynaFnRegistryTest -Ddynafn.generate=1}
 * (members, dialects); the resolution column is the platform's own decision, kept
 * by hand and VERIFIED by the same test (a PURE name must exist in the catalog, a
 * SHIM must name a Lite constant, every TRANSLATED name has an arm and every armed
 * name not TRANSLATED is PURE).
 * Never a name set anywhere else: {@link #of(String)} is the one lookup.
 */
public enum DynaFn {
    ABS("abs", Resolution.PURE, null, Inference.MAPPED, Dialect.DEFAULT),
    ACOS("acos", Resolution.PURE, null, Inference.MAPPED, Dialect.DEFAULT),
    ADD("add", Resolution.TRANSLATED, null, Inference.MAPPED, Dialect.DEFAULT),
    ADJUST("adjust", Resolution.TRANSLATED, null, Inference.MAPPED, Dialect.BIGQUERY, Dialect.CLICKHOUSE, Dialect.DATABRICKS, Dialect.DB2, Dialect.DUCKDB, Dialect.H2, Dialect.MEMSQL, Dialect.ORACLE, Dialect.POSTGRES, Dialect.PRESTO, Dialect.SNOWFLAKE, Dialect.SPARKSQL, Dialect.SQLSERVER, Dialect.SYBASE, Dialect.SYBASEIQ, Dialect.TRINO),
    ALL_OF("allOf", Resolution.UNSUPPORTED, null, Inference.MAPPED, Dialect.DEFAULT),
    AND("and", Resolution.PURE, null, Inference.MAPPED, Dialect.DEFAULT, Dialect.SPANNER),
    ANY_OF("anyOf", Resolution.UNSUPPORTED, null, Inference.MAPPED, Dialect.DEFAULT),
    ARRAY_APPEND("array_append", Resolution.UNSUPPORTED, null, Inference.NONE, Dialect.DATABRICKS, Dialect.DUCKDB, Dialect.SNOWFLAKE),
    ARRAY_CONCATENATE("array_concatenate", Resolution.UNSUPPORTED, null, Inference.NONE, Dialect.DATABRICKS, Dialect.DUCKDB, Dialect.SNOWFLAKE),
    ARRAY_CONTAINS("array_contains", Resolution.UNSUPPORTED, null, Inference.NONE, Dialect.DATABRICKS, Dialect.DUCKDB, Dialect.SNOWFLAKE),
    ARRAY_DISTINCT("array_distinct", Resolution.UNSUPPORTED, null, Inference.NONE, Dialect.DATABRICKS, Dialect.DUCKDB, Dialect.SNOWFLAKE),
    ARRAY_DROP("array_drop", Resolution.UNSUPPORTED, null, Inference.NONE, Dialect.DATABRICKS, Dialect.DUCKDB, Dialect.SNOWFLAKE),
    ARRAY_FIRST("array_first", Resolution.UNSUPPORTED, null, Inference.NONE, Dialect.DATABRICKS, Dialect.DUCKDB, Dialect.SNOWFLAKE),
    ARRAY_FLATTEN("array_flatten", Resolution.UNSUPPORTED, null, Inference.NONE, Dialect.DATABRICKS, Dialect.DUCKDB, Dialect.SNOWFLAKE),
    ARRAY_INIT("array_init", Resolution.UNSUPPORTED, null, Inference.NONE, Dialect.DATABRICKS, Dialect.DUCKDB, Dialect.SNOWFLAKE),
    ARRAY_LAST("array_last", Resolution.UNSUPPORTED, null, Inference.NONE, Dialect.DATABRICKS, Dialect.DUCKDB, Dialect.SNOWFLAKE),
    ARRAY_MAX("array_max", Resolution.UNSUPPORTED, null, Inference.NONE, Dialect.CLICKHOUSE, Dialect.DATABRICKS, Dialect.DUCKDB, Dialect.SNOWFLAKE),
    ARRAY_MIN("array_min", Resolution.UNSUPPORTED, null, Inference.NONE, Dialect.CLICKHOUSE, Dialect.DATABRICKS, Dialect.DUCKDB, Dialect.SNOWFLAKE),
    ARRAY_POSITION("array_position", Resolution.UNSUPPORTED, null, Inference.NONE, Dialect.DATABRICKS, Dialect.DUCKDB, Dialect.SNOWFLAKE),
    ARRAY_REVERSE("array_reverse", Resolution.UNSUPPORTED, null, Inference.NONE, Dialect.DATABRICKS, Dialect.DUCKDB, Dialect.SNOWFLAKE),
    ARRAY_SIZE("array_size", Resolution.UNSUPPORTED, null, Inference.NONE, Dialect.DATABRICKS, Dialect.DUCKDB, Dialect.SNOWFLAKE),
    ARRAY_SLICE("array_slice", Resolution.UNSUPPORTED, null, Inference.NONE, Dialect.DATABRICKS, Dialect.DUCKDB, Dialect.SNOWFLAKE),
    ARRAY_SORT("array_sort", Resolution.UNSUPPORTED, null, Inference.NONE, Dialect.DATABRICKS, Dialect.DUCKDB, Dialect.SNOWFLAKE),
    ARRAY_SUM("array_sum", Resolution.UNSUPPORTED, null, Inference.NONE, Dialect.DATABRICKS, Dialect.DUCKDB, Dialect.SNOWFLAKE),
    ARRAY_TAIL("array_tail", Resolution.UNSUPPORTED, null, Inference.NONE, Dialect.DATABRICKS, Dialect.DUCKDB, Dialect.SNOWFLAKE),
    ARRAY_TAKE("array_take", Resolution.UNSUPPORTED, null, Inference.NONE, Dialect.DATABRICKS, Dialect.DUCKDB, Dialect.SNOWFLAKE),
    ARRAY_TO_STRING("array_to_string", Resolution.UNSUPPORTED, null, Inference.NONE, Dialect.DATABRICKS, Dialect.DUCKDB, Dialect.SNOWFLAKE),
    ASCII("ascii", Resolution.PURE, null, Inference.MAPPED, Dialect.DEFAULT, Dialect.SPANNER),
    ASIN("asin", Resolution.PURE, null, Inference.MAPPED, Dialect.DEFAULT),
    ATAN("atan", Resolution.PURE, null, Inference.MAPPED, Dialect.DEFAULT),
    ATAN2("atan2", Resolution.PURE, null, Inference.MAPPED, Dialect.DEFAULT, Dialect.MEMSQL, Dialect.SQLSERVER, Dialect.SYBASE),
    AVERAGE("average", Resolution.PURE, null, Inference.MAPPED, Dialect.DEFAULT),
    AVERAGE_RANK("averageRank", Resolution.UNSUPPORTED, null, Inference.MAPPED, Dialect.DEFAULT),
    BETWEEN("between", Resolution.PURE, null, Inference.MAPPED),
    BIT_AND("bitAnd", Resolution.PURE, null, Inference.MAPPED, Dialect.DATABRICKS, Dialect.DUCKDB, Dialect.SNOWFLAKE),
    BIT_NOT("bitNot", Resolution.PURE, null, Inference.MAPPED, Dialect.DATABRICKS, Dialect.DUCKDB, Dialect.SNOWFLAKE),
    BIT_OR("bitOr", Resolution.PURE, null, Inference.MAPPED, Dialect.DATABRICKS, Dialect.DUCKDB, Dialect.SNOWFLAKE),
    BIT_SHIFT_LEFT("bitShiftLeft", Resolution.PURE, null, Inference.MAPPED, Dialect.DATABRICKS, Dialect.DUCKDB, Dialect.SNOWFLAKE),
    BIT_SHIFT_RIGHT("bitShiftRight", Resolution.PURE, null, Inference.MAPPED, Dialect.DATABRICKS, Dialect.DUCKDB, Dialect.SNOWFLAKE),
    BIT_XOR("bitXor", Resolution.PURE, null, Inference.MAPPED, Dialect.DATABRICKS, Dialect.DUCKDB, Dialect.SNOWFLAKE),
    BOOLAND("booland", Resolution.UNSUPPORTED, null, Inference.MAPPED, Dialect.BIGQUERY, Dialect.DATABRICKS, Dialect.DUCKDB, Dialect.H2, Dialect.MEMSQL, Dialect.ORACLE, Dialect.POSTGRES, Dialect.PRESTO, Dialect.REDSHIFT, Dialect.SNOWFLAKE, Dialect.SPANNER, Dialect.SQLSERVER, Dialect.SYBASE, Dialect.SYBASEIQ, Dialect.TRINO),
    BOOLOR("boolor", Resolution.UNSUPPORTED, null, Inference.MAPPED, Dialect.BIGQUERY, Dialect.DATABRICKS, Dialect.DUCKDB, Dialect.H2, Dialect.MEMSQL, Dialect.ORACLE, Dialect.POSTGRES, Dialect.PRESTO, Dialect.REDSHIFT, Dialect.SNOWFLAKE, Dialect.SPANNER, Dialect.SQLSERVER, Dialect.SYBASE, Dialect.SYBASEIQ, Dialect.TRINO),
    CASE("case", Resolution.TRANSLATED, null, Inference.MAPPED),
    CAST("cast", Resolution.PURE, null, Inference.MAPPED, Dialect.DEFAULT),
    CAST_BOOLEAN("castBoolean", Resolution.UNSUPPORTED, null, Inference.NONE, Dialect.DUCKDB),
    CBRT("cbrt", Resolution.PURE, null, Inference.MAPPED, Dialect.DEFAULT, Dialect.ORACLE),
    CEILING("ceiling", Resolution.PURE, null, Inference.MAPPED, Dialect.DEFAULT, Dialect.ORACLE, Dialect.SNOWFLAKE),
    CHAR("char", Resolution.PURE, null, Inference.MAPPED, Dialect.CLICKHOUSE, Dialect.DB2, Dialect.DEFAULT, Dialect.H2, Dialect.MEMSQL, Dialect.SPANNER, Dialect.SPARKSQL, Dialect.SQLSERVER, Dialect.SYBASE, Dialect.SYBASEIQ),
    CHR("chr", Resolution.UNSUPPORTED, null, Inference.NONE, Dialect.DUCKDB),
    COALESCE("coalesce", Resolution.PURE, null, Inference.MAPPED, Dialect.DEFAULT, Dialect.SPANNER),
    CONCAT("concat", Resolution.TRANSLATED, null, Inference.MAPPED, Dialect.BIGQUERY, Dialect.CLICKHOUSE, Dialect.DATABRICKS, Dialect.DB2, Dialect.DUCKDB, Dialect.H2, Dialect.MEMSQL, Dialect.ORACLE, Dialect.POSTGRES, Dialect.PRESTO, Dialect.REDSHIFT, Dialect.SNOWFLAKE, Dialect.SPANNER, Dialect.SPARKSQL, Dialect.SQLSERVER, Dialect.SYBASE, Dialect.SYBASEIQ, Dialect.TRINO),
    CONTAINS("contains", Resolution.PURE, null, Inference.MAPPED, Dialect.DEFAULT, Dialect.SNOWFLAKE, Dialect.SPANNER, Dialect.SPARKSQL, Dialect.SYBASEIQ),
    CONVERT_DATE("convertDate", Resolution.TRANSLATED, null, Inference.MAPPED, Dialect.DATABRICKS, Dialect.DB2, Dialect.DUCKDB, Dialect.H2, Dialect.MEMSQL, Dialect.PRESTO, Dialect.SNOWFLAKE, Dialect.SPARKSQL, Dialect.SYBASE, Dialect.SYBASEIQ, Dialect.TRINO),
    CONVERT_DATE_TIME("convertDateTime", Resolution.TRANSLATED, null, Inference.MAPPED, Dialect.DATABRICKS, Dialect.DB2, Dialect.DUCKDB, Dialect.H2, Dialect.PRESTO, Dialect.SNOWFLAKE, Dialect.SPARKSQL, Dialect.SYBASE, Dialect.SYBASEIQ, Dialect.TRINO),
    CONVERT_TIME_ZONE("convertTimeZone", Resolution.TRANSLATED, null, Inference.NONE, Dialect.H2, Dialect.MEMSQL, Dialect.SNOWFLAKE),
    CONVERT_VARCHAR128("convertVarchar128", Resolution.TRANSLATED, null, Inference.MAPPED, Dialect.DATABRICKS, Dialect.DB2, Dialect.DUCKDB, Dialect.H2, Dialect.MEMSQL, Dialect.PRESTO, Dialect.SNOWFLAKE, Dialect.SPARKSQL, Dialect.SYBASE, Dialect.SYBASEIQ, Dialect.TRINO),
    CORR("corr", Resolution.PURE, null, Inference.MAPPED, Dialect.DEFAULT),
    COS("cos", Resolution.PURE, null, Inference.MAPPED, Dialect.DEFAULT),
    COSH("cosh", Resolution.PURE, null, Inference.MAPPED, Dialect.DEFAULT),
    COT("cot", Resolution.PURE, null, Inference.MAPPED, Dialect.CLICKHOUSE, Dialect.DEFAULT, Dialect.TRINO),
    COUNT("count", Resolution.PURE, null, Inference.MAPPED, Dialect.DEFAULT, Dialect.SPANNER),
    COVAR_POPULATION("covarPopulation", Resolution.PURE, null, Inference.MAPPED, Dialect.DEFAULT),
    COVAR_SAMPLE("covarSample", Resolution.PURE, null, Inference.MAPPED, Dialect.DEFAULT),
    CUMULATIVE_DISTRIBUTION("cumulativeDistribution", Resolution.PURE, null, Inference.NONE, Dialect.DEFAULT),
    CURRENT_USER_ID("currentUserId", Resolution.PURE, null, Inference.NONE, Dialect.DEFAULT, Dialect.ORACLE, Dialect.SNOWFLAKE),
    DATE("date", Resolution.PURE, null, Inference.NONE, Dialect.CLICKHOUSE, Dialect.DATABRICKS, Dialect.DUCKDB, Dialect.SNOWFLAKE),
    DATE_DIFF("dateDiff", Resolution.PURE, null, Inference.MAPPED, Dialect.CLICKHOUSE, Dialect.DATABRICKS, Dialect.DUCKDB, Dialect.H2, Dialect.MEMSQL, Dialect.ORACLE, Dialect.POSTGRES, Dialect.PRESTO, Dialect.REDSHIFT, Dialect.SNOWFLAKE, Dialect.SPANNER, Dialect.SPARKSQL, Dialect.SQLSERVER, Dialect.SYBASE, Dialect.SYBASEIQ, Dialect.TRINO),
    DATE_PART("datePart", Resolution.PURE, null, Inference.MAPPED, Dialect.BIGQUERY, Dialect.CLICKHOUSE, Dialect.DATABRICKS, Dialect.DB2, Dialect.DUCKDB, Dialect.H2, Dialect.MEMSQL, Dialect.ORACLE, Dialect.POSTGRES, Dialect.PRESTO, Dialect.REDSHIFT, Dialect.SNOWFLAKE, Dialect.SPANNER, Dialect.SPARKSQL, Dialect.SQLSERVER, Dialect.SYBASE, Dialect.SYBASEIQ, Dialect.TRINO),
    DAY_OF_MWEEK("dayOfMWeek", Resolution.UNSUPPORTED, null, Inference.MAPPED),
    DAY_OF_MONTH("dayOfMonth", Resolution.PURE, null, Inference.MAPPED, Dialect.BIGQUERY, Dialect.CLICKHOUSE, Dialect.DATABRICKS, Dialect.DUCKDB, Dialect.H2, Dialect.MEMSQL, Dialect.ORACLE, Dialect.POSTGRES, Dialect.REDSHIFT, Dialect.SNOWFLAKE, Dialect.SPANNER, Dialect.SPARKSQL, Dialect.SQLSERVER, Dialect.SYBASE, Dialect.SYBASEIQ, Dialect.TRINO),
    DAY_OF_WEEK("dayOfWeek", Resolution.TRANSLATED, null, Inference.NONE, Dialect.BIGQUERY, Dialect.CLICKHOUSE, Dialect.DATABRICKS, Dialect.DB2, Dialect.DUCKDB, Dialect.H2, Dialect.MEMSQL, Dialect.ORACLE, Dialect.POSTGRES, Dialect.PRESTO, Dialect.REDSHIFT, Dialect.SNOWFLAKE, Dialect.SPANNER, Dialect.SPARKSQL, Dialect.SQLSERVER, Dialect.SYBASE, Dialect.SYBASEIQ, Dialect.TRINO),
    DAY_OF_WEEK_NUMBER("dayOfWeekNumber", Resolution.TRANSLATED, null, Inference.MAPPED, Dialect.BIGQUERY, Dialect.CLICKHOUSE, Dialect.DATABRICKS, Dialect.DB2, Dialect.DUCKDB, Dialect.H2, Dialect.MEMSQL, Dialect.ORACLE, Dialect.POSTGRES, Dialect.PRESTO, Dialect.REDSHIFT, Dialect.SNOWFLAKE, Dialect.SPANNER, Dialect.SPARKSQL, Dialect.SQLSERVER, Dialect.SYBASE, Dialect.SYBASEIQ, Dialect.TRINO),
    DAY_OF_YEAR("dayOfYear", Resolution.PURE, null, Inference.MAPPED, Dialect.BIGQUERY, Dialect.CLICKHOUSE, Dialect.DATABRICKS, Dialect.DB2, Dialect.DUCKDB, Dialect.H2, Dialect.MEMSQL, Dialect.ORACLE, Dialect.POSTGRES, Dialect.PRESTO, Dialect.REDSHIFT, Dialect.SNOWFLAKE, Dialect.SPANNER, Dialect.SPARKSQL, Dialect.SQLSERVER, Dialect.SYBASE, Dialect.SYBASEIQ, Dialect.TRINO),
    DECODE_BASE64("decodeBase64", Resolution.PURE, null, Inference.MAPPED, Dialect.BIGQUERY, Dialect.CLICKHOUSE, Dialect.DATABRICKS, Dialect.DUCKDB, Dialect.H2, Dialect.MEMSQL, Dialect.ORACLE, Dialect.POSTGRES, Dialect.SNOWFLAKE, Dialect.SPANNER, Dialect.SQLSERVER),
    DENSE_RANK("denseRank", Resolution.PURE, null, Inference.MAPPED, Dialect.DEFAULT),
    DISTINCT("distinct", Resolution.PURE, null, Inference.MAPPED, Dialect.DEFAULT, Dialect.SPANNER),
    DIVIDE("divide", Resolution.PURE, null, Inference.MAPPED, Dialect.DEFAULT),
    DIVIDE_ROUND("divideRound", Resolution.SHIM, Pure.Lite.DIVIDE_ROUND, Inference.NONE, Dialect.DEFAULT),
    ENCODE_BASE64("encodeBase64", Resolution.PURE, null, Inference.MAPPED, Dialect.BIGQUERY, Dialect.CLICKHOUSE, Dialect.DATABRICKS, Dialect.DUCKDB, Dialect.H2, Dialect.MEMSQL, Dialect.ORACLE, Dialect.POSTGRES, Dialect.SNOWFLAKE, Dialect.SPANNER, Dialect.SQLSERVER),
    ENDS_WITH("endsWith", Resolution.PURE, null, Inference.MAPPED, Dialect.DEFAULT, Dialect.SNOWFLAKE, Dialect.SPANNER, Dialect.SPARKSQL, Dialect.SYBASEIQ),
    EQUAL("equal", Resolution.PURE, null, Inference.MAPPED, Dialect.DEFAULT),
    EXISTS("exists", Resolution.PURE, null, Inference.MAPPED, Dialect.DEFAULT),
    EXP("exp", Resolution.PURE, null, Inference.MAPPED, Dialect.DEFAULT, Dialect.SPANNER),
    EXTRACT_FROM_SEMI_STRUCTURED("extractFromSemiStructured", Resolution.TRANSLATED, null, Inference.NONE, Dialect.DATABRICKS, Dialect.DUCKDB, Dialect.H2, Dialect.MEMSQL, Dialect.POSTGRES, Dialect.SNOWFLAKE),
    FIRST("first", Resolution.PURE, null, Inference.NONE, Dialect.DEFAULT, Dialect.DUCKDB),
    FIRST_DAY_OF_MONTH("firstDayOfMonth", Resolution.PURE, null, Inference.MAPPED, Dialect.BIGQUERY, Dialect.CLICKHOUSE, Dialect.DATABRICKS, Dialect.DB2, Dialect.DUCKDB, Dialect.H2, Dialect.MEMSQL, Dialect.ORACLE, Dialect.POSTGRES, Dialect.PRESTO, Dialect.REDSHIFT, Dialect.SNOWFLAKE, Dialect.SPANNER, Dialect.SPARKSQL, Dialect.SQLSERVER, Dialect.SYBASE, Dialect.SYBASEIQ, Dialect.TRINO),
    FIRST_DAY_OF_QUARTER("firstDayOfQuarter", Resolution.PURE, null, Inference.MAPPED, Dialect.BIGQUERY, Dialect.CLICKHOUSE, Dialect.DATABRICKS, Dialect.DB2, Dialect.DUCKDB, Dialect.H2, Dialect.MEMSQL, Dialect.ORACLE, Dialect.POSTGRES, Dialect.PRESTO, Dialect.REDSHIFT, Dialect.SNOWFLAKE, Dialect.SPANNER, Dialect.SPARKSQL, Dialect.SQLSERVER, Dialect.SYBASE, Dialect.SYBASEIQ, Dialect.TRINO),
    FIRST_DAY_OF_THIS_MONTH("firstDayOfThisMonth", Resolution.PURE, null, Inference.MAPPED, Dialect.BIGQUERY, Dialect.DATABRICKS, Dialect.DB2, Dialect.DUCKDB, Dialect.H2, Dialect.MEMSQL, Dialect.ORACLE, Dialect.POSTGRES, Dialect.PRESTO, Dialect.REDSHIFT, Dialect.SNOWFLAKE, Dialect.SPANNER, Dialect.SPARKSQL, Dialect.SQLSERVER, Dialect.SYBASE, Dialect.SYBASEIQ, Dialect.TRINO),
    FIRST_DAY_OF_THIS_QUARTER("firstDayOfThisQuarter", Resolution.PURE, null, Inference.MAPPED, Dialect.BIGQUERY, Dialect.DATABRICKS, Dialect.DB2, Dialect.DUCKDB, Dialect.H2, Dialect.MEMSQL, Dialect.ORACLE, Dialect.POSTGRES, Dialect.PRESTO, Dialect.REDSHIFT, Dialect.SNOWFLAKE, Dialect.SPANNER, Dialect.SPARKSQL, Dialect.SQLSERVER, Dialect.SYBASE, Dialect.SYBASEIQ, Dialect.TRINO),
    FIRST_DAY_OF_THIS_YEAR("firstDayOfThisYear", Resolution.PURE, null, Inference.MAPPED, Dialect.BIGQUERY, Dialect.DATABRICKS, Dialect.DB2, Dialect.DUCKDB, Dialect.H2, Dialect.MEMSQL, Dialect.ORACLE, Dialect.POSTGRES, Dialect.PRESTO, Dialect.REDSHIFT, Dialect.SNOWFLAKE, Dialect.SPANNER, Dialect.SPARKSQL, Dialect.SQLSERVER, Dialect.SYBASE, Dialect.SYBASEIQ, Dialect.TRINO),
    FIRST_DAY_OF_WEEK("firstDayOfWeek", Resolution.PURE, null, Inference.MAPPED, Dialect.BIGQUERY, Dialect.CLICKHOUSE, Dialect.DATABRICKS, Dialect.DUCKDB, Dialect.H2, Dialect.MEMSQL, Dialect.ORACLE, Dialect.POSTGRES, Dialect.PRESTO, Dialect.REDSHIFT, Dialect.SNOWFLAKE, Dialect.SPANNER, Dialect.SPARKSQL, Dialect.SQLSERVER, Dialect.SYBASE, Dialect.SYBASEIQ, Dialect.TRINO),
    FIRST_DAY_OF_YEAR("firstDayOfYear", Resolution.PURE, null, Inference.MAPPED, Dialect.BIGQUERY, Dialect.CLICKHOUSE, Dialect.DATABRICKS, Dialect.DB2, Dialect.DUCKDB, Dialect.H2, Dialect.MEMSQL, Dialect.ORACLE, Dialect.POSTGRES, Dialect.PRESTO, Dialect.REDSHIFT, Dialect.SNOWFLAKE, Dialect.SPANNER, Dialect.SPARKSQL, Dialect.SQLSERVER, Dialect.SYBASE, Dialect.SYBASEIQ, Dialect.TRINO),
    FIRST_HOUR_OF_DAY("firstHourOfDay", Resolution.PURE, null, Inference.MAPPED, Dialect.BIGQUERY, Dialect.CLICKHOUSE, Dialect.DATABRICKS, Dialect.DUCKDB, Dialect.H2, Dialect.MEMSQL, Dialect.ORACLE, Dialect.POSTGRES, Dialect.PRESTO, Dialect.REDSHIFT, Dialect.SNOWFLAKE, Dialect.SPANNER, Dialect.SPARKSQL, Dialect.SQLSERVER, Dialect.SYBASEIQ, Dialect.TRINO),
    FIRST_MILLISECOND_OF_SECOND("firstMillisecondOfSecond", Resolution.PURE, null, Inference.MAPPED, Dialect.BIGQUERY, Dialect.DATABRICKS, Dialect.DUCKDB, Dialect.H2, Dialect.MEMSQL, Dialect.ORACLE, Dialect.POSTGRES, Dialect.PRESTO, Dialect.REDSHIFT, Dialect.SNOWFLAKE, Dialect.SPANNER, Dialect.SPARKSQL, Dialect.SQLSERVER, Dialect.SYBASEIQ, Dialect.TRINO),
    FIRST_MINUTE_OF_HOUR("firstMinuteOfHour", Resolution.PURE, null, Inference.MAPPED, Dialect.BIGQUERY, Dialect.CLICKHOUSE, Dialect.DATABRICKS, Dialect.DUCKDB, Dialect.H2, Dialect.MEMSQL, Dialect.ORACLE, Dialect.POSTGRES, Dialect.PRESTO, Dialect.REDSHIFT, Dialect.SNOWFLAKE, Dialect.SPANNER, Dialect.SPARKSQL, Dialect.SQLSERVER, Dialect.SYBASEIQ, Dialect.TRINO),
    FIRST_SECOND_OF_MINUTE("firstSecondOfMinute", Resolution.PURE, null, Inference.MAPPED, Dialect.BIGQUERY, Dialect.CLICKHOUSE, Dialect.DATABRICKS, Dialect.DUCKDB, Dialect.H2, Dialect.MEMSQL, Dialect.ORACLE, Dialect.POSTGRES, Dialect.PRESTO, Dialect.REDSHIFT, Dialect.SNOWFLAKE, Dialect.SPANNER, Dialect.SPARKSQL, Dialect.SQLSERVER, Dialect.SYBASEIQ, Dialect.TRINO),
    FLOOR("floor", Resolution.PURE, null, Inference.MAPPED, Dialect.DEFAULT, Dialect.MEMSQL),
    FORMAT_DATE("formatDate", Resolution.PURE, null, Inference.NONE, Dialect.BIGQUERY, Dialect.CLICKHOUSE, Dialect.DATABRICKS, Dialect.DUCKDB, Dialect.H2, Dialect.MEMSQL, Dialect.ORACLE, Dialect.POSTGRES, Dialect.PRESTO, Dialect.REDSHIFT, Dialect.SNOWFLAKE, Dialect.SPANNER, Dialect.SPARKSQL, Dialect.SQLSERVER, Dialect.SYBASE, Dialect.SYBASEIQ, Dialect.TRINO),
    GENERATE_GUID("generateGuid", Resolution.PURE, null, Inference.NONE, Dialect.CLICKHOUSE, Dialect.DATABRICKS, Dialect.DUCKDB, Dialect.H2, Dialect.MEMSQL, Dialect.POSTGRES, Dialect.SNOWFLAKE, Dialect.SQLSERVER, Dialect.SYBASE, Dialect.SYBASEIQ),
    GREATER_THAN("greaterThan", Resolution.SHIM, Pure.Lite.GREATER_THAN_ANY, Inference.MAPPED, Dialect.DEFAULT),
    GREATER_THAN_EQUAL("greaterThanEqual", Resolution.SHIM, Pure.Lite.GREATER_THAN_EQUAL_ANY, Inference.MAPPED, Dialect.DEFAULT),
    GREATEST("greatest", Resolution.PURE, null, Inference.MAPPED, Dialect.DEFAULT, Dialect.SYBASE, Dialect.SYBASEIQ),
    GROUP("group", Resolution.TRANSLATED, null, Inference.MAPPED, Dialect.DEFAULT),
    HASH_AGG("hashAgg", Resolution.UNSUPPORTED, null, Inference.NONE, Dialect.SNOWFLAKE),
    HASH_CODE("hashCode", Resolution.PURE, null, Inference.NONE, Dialect.DATABRICKS, Dialect.DUCKDB, Dialect.SNOWFLAKE),
    HOUR("hour", Resolution.PURE, null, Inference.MAPPED, Dialect.BIGQUERY, Dialect.CLICKHOUSE, Dialect.COMPOSITE, Dialect.DATABRICKS, Dialect.DB2, Dialect.DUCKDB, Dialect.H2, Dialect.MEMSQL, Dialect.ORACLE, Dialect.POSTGRES, Dialect.PRESTO, Dialect.REDSHIFT, Dialect.SNOWFLAKE, Dialect.SPANNER, Dialect.SPARKSQL, Dialect.SQLSERVER, Dialect.SYBASE, Dialect.SYBASEIQ, Dialect.TRINO),
    IF("if", Resolution.TRANSLATED, null, Inference.MAPPED, Dialect.DEFAULT),
    IN("in", Resolution.PURE, null, Inference.MAPPED, Dialect.DEFAULT, Dialect.SPANNER),
    INDEX_OF("indexOf", Resolution.TRANSLATED, null, Inference.MAPPED, Dialect.BIGQUERY, Dialect.CLICKHOUSE, Dialect.DATABRICKS, Dialect.DUCKDB, Dialect.H2, Dialect.MEMSQL, Dialect.ORACLE, Dialect.POSTGRES, Dialect.PRESTO, Dialect.REDSHIFT, Dialect.SNOWFLAKE, Dialect.SPANNER, Dialect.SPARKSQL, Dialect.SQLSERVER, Dialect.SYBASE, Dialect.SYBASEIQ, Dialect.TRINO),
    IS_ALPHA_NUMERIC("isAlphaNumeric", Resolution.UNSUPPORTED, null, Inference.NONE, Dialect.DATABRICKS, Dialect.DUCKDB, Dialect.H2, Dialect.SNOWFLAKE, Dialect.SPARKSQL, Dialect.SYBASE, Dialect.SYBASEIQ),
    IS_DISTINCT("isDistinct", Resolution.SHIM, Pure.Lite.IS_DISTINCT, Inference.NONE, Dialect.DEFAULT),
    IS_EMPTY("isEmpty", Resolution.PURE, null, Inference.MAPPED, Dialect.DEFAULT, Dialect.ORACLE, Dialect.SPARKSQL, Dialect.SQLSERVER, Dialect.SYBASE, Dialect.SYBASEIQ),
    IS_NOT_EMPTY("isNotEmpty", Resolution.PURE, null, Inference.MAPPED, Dialect.DEFAULT, Dialect.ORACLE, Dialect.SPANNER, Dialect.SPARKSQL, Dialect.SQLSERVER, Dialect.SYBASE, Dialect.SYBASEIQ),
    IS_NOT_NULL("isNotNull", Resolution.TRANSLATED, null, Inference.MAPPED, Dialect.DEFAULT, Dialect.ORACLE, Dialect.SPARKSQL, Dialect.SQLSERVER, Dialect.SYBASE, Dialect.SYBASEIQ),
    IS_NULL("isNull", Resolution.TRANSLATED, null, Inference.MAPPED, Dialect.DEFAULT, Dialect.ORACLE, Dialect.SPANNER, Dialect.SPARKSQL, Dialect.SQLSERVER, Dialect.SYBASE, Dialect.SYBASEIQ),
    IS_NUMERIC("isNumeric", Resolution.SHIM, Pure.Lite.IS_NUMERIC, Inference.MAPPED, Dialect.DATABRICKS, Dialect.DUCKDB, Dialect.H2, Dialect.SPARKSQL, Dialect.SYBASE, Dialect.SYBASEIQ),
    JARO_WINKLER_SIMILARITY("jaroWinklerSimilarity", Resolution.PURE, null, Inference.MAPPED, Dialect.CLICKHOUSE, Dialect.DUCKDB, Dialect.H2, Dialect.SNOWFLAKE),
    JOIN_STRINGS("joinStrings", Resolution.PURE, null, Inference.MAPPED, Dialect.BIGQUERY, Dialect.CLICKHOUSE, Dialect.DATABRICKS, Dialect.DB2, Dialect.DUCKDB, Dialect.H2, Dialect.MEMSQL, Dialect.ORACLE, Dialect.POSTGRES, Dialect.REDSHIFT, Dialect.SNOWFLAKE, Dialect.SPANNER, Dialect.SPARKSQL, Dialect.SYBASE, Dialect.SYBASEIQ),
    KEYS("keys", Resolution.PURE, null, Inference.NONE, Dialect.DATABRICKS, Dialect.DUCKDB, Dialect.SNOWFLAKE),
    LAG("lag", Resolution.PURE, null, Inference.NONE, Dialect.CLICKHOUSE, Dialect.DEFAULT),
    LAST("last", Resolution.PURE, null, Inference.NONE, Dialect.DEFAULT, Dialect.DUCKDB),
    LEAD("lead", Resolution.PURE, null, Inference.NONE, Dialect.DEFAULT),
    LEAST("least", Resolution.PURE, null, Inference.MAPPED, Dialect.DEFAULT, Dialect.SYBASE, Dialect.SYBASEIQ),
    LEFT("left", Resolution.PURE, null, Inference.MAPPED, Dialect.DB2, Dialect.DEFAULT, Dialect.MEMSQL, Dialect.ORACLE, Dialect.PRESTO, Dialect.SPANNER, Dialect.TRINO),
    LENGTH("length", Resolution.PURE, null, Inference.MAPPED, Dialect.BIGQUERY, Dialect.CLICKHOUSE, Dialect.COMPOSITE, Dialect.DATABRICKS, Dialect.DB2, Dialect.DUCKDB, Dialect.H2, Dialect.MEMSQL, Dialect.ORACLE, Dialect.POSTGRES, Dialect.PRESTO, Dialect.REDSHIFT, Dialect.SNOWFLAKE, Dialect.SPANNER, Dialect.SPARKSQL, Dialect.SQLSERVER, Dialect.SYBASE, Dialect.SYBASEIQ, Dialect.TRINO),
    LESS_THAN("lessThan", Resolution.SHIM, Pure.Lite.LESS_THAN_ANY, Inference.MAPPED, Dialect.DEFAULT),
    LESS_THAN_EQUAL("lessThanEqual", Resolution.SHIM, Pure.Lite.LESS_THAN_EQUAL_ANY, Inference.MAPPED, Dialect.DEFAULT),
    LEVENSHTEIN_DISTANCE("levenshteinDistance", Resolution.PURE, null, Inference.MAPPED, Dialect.CLICKHOUSE, Dialect.DATABRICKS, Dialect.DUCKDB, Dialect.H2, Dialect.SNOWFLAKE),
    LOG("log", Resolution.PURE, null, Inference.MAPPED, Dialect.CLICKHOUSE, Dialect.DEFAULT, Dialect.SPANNER, Dialect.SQLSERVER, Dialect.SYBASE),
    LOG10("log10", Resolution.PURE, null, Inference.MAPPED, Dialect.CLICKHOUSE, Dialect.DEFAULT, Dialect.ORACLE, Dialect.POSTGRES, Dialect.REDSHIFT, Dialect.SNOWFLAKE),
    LPAD("lpad", Resolution.PURE, null, Inference.MAPPED, Dialect.DEFAULT, Dialect.DUCKDB, Dialect.MEMSQL, Dialect.SQLSERVER, Dialect.SYBASE, Dialect.TRINO),
    LTRIM("ltrim", Resolution.PURE, null, Inference.MAPPED, Dialect.DEFAULT),
    MAP_CONCATENATE("mapConcatenate", Resolution.UNSUPPORTED, null, Inference.NONE, Dialect.DATABRICKS, Dialect.DUCKDB, Dialect.SNOWFLAKE),
    MATCHES("matches", Resolution.PURE, null, Inference.NONE, Dialect.CLICKHOUSE, Dialect.DATABRICKS, Dialect.DUCKDB, Dialect.H2, Dialect.MEMSQL, Dialect.ORACLE, Dialect.POSTGRES, Dialect.PRESTO, Dialect.SNOWFLAKE, Dialect.SPANNER, Dialect.SPARKSQL, Dialect.SYBASE, Dialect.SYBASEIQ, Dialect.TRINO),
    MAX("max", Resolution.PURE, null, Inference.MAPPED, Dialect.DEFAULT),
    MAX_BY("maxBy", Resolution.PURE, null, Inference.NONE, Dialect.CLICKHOUSE, Dialect.DATABRICKS, Dialect.DUCKDB, Dialect.SNOWFLAKE),
    MD5("md5", Resolution.TRANSLATED, null, Inference.MAPPED, Dialect.CLICKHOUSE, Dialect.DB2, Dialect.DEFAULT, Dialect.DUCKDB, Dialect.H2, Dialect.SPARKSQL, Dialect.SQLSERVER, Dialect.SYBASE, Dialect.SYBASEIQ, Dialect.TRINO),
    MEDIAN("median", Resolution.PURE, null, Inference.MAPPED, Dialect.DEFAULT),
    MIN("min", Resolution.PURE, null, Inference.MAPPED, Dialect.DEFAULT),
    MIN_BY("minBy", Resolution.PURE, null, Inference.NONE, Dialect.CLICKHOUSE, Dialect.DATABRICKS, Dialect.DUCKDB, Dialect.SNOWFLAKE),
    MINUS("minus", Resolution.PURE, null, Inference.MAPPED, Dialect.DEFAULT),
    MINUTE("minute", Resolution.PURE, null, Inference.MAPPED, Dialect.BIGQUERY, Dialect.CLICKHOUSE, Dialect.DATABRICKS, Dialect.DB2, Dialect.DUCKDB, Dialect.H2, Dialect.MEMSQL, Dialect.ORACLE, Dialect.POSTGRES, Dialect.PRESTO, Dialect.REDSHIFT, Dialect.SNOWFLAKE, Dialect.SPANNER, Dialect.SPARKSQL, Dialect.SQLSERVER, Dialect.SYBASE, Dialect.SYBASEIQ, Dialect.TRINO),
    MOD("mod", Resolution.PURE, null, Inference.MAPPED, Dialect.DEFAULT, Dialect.DUCKDB, Dialect.SQLSERVER, Dialect.SYBASE),
    MODE("mode", Resolution.PURE, null, Inference.MAPPED, Dialect.CLICKHOUSE, Dialect.DEFAULT),
    MONTH("month", Resolution.PURE, null, Inference.MAPPED, Dialect.BIGQUERY, Dialect.CLICKHOUSE, Dialect.DATABRICKS, Dialect.DB2, Dialect.DUCKDB, Dialect.H2, Dialect.MEMSQL, Dialect.ORACLE, Dialect.POSTGRES, Dialect.PRESTO, Dialect.REDSHIFT, Dialect.SNOWFLAKE, Dialect.SPANNER, Dialect.SPARKSQL, Dialect.SQLSERVER, Dialect.SYBASE, Dialect.SYBASEIQ, Dialect.TRINO),
    MONTH_NAME("monthName", Resolution.UNSUPPORTED, null, Inference.MAPPED, Dialect.BIGQUERY, Dialect.CLICKHOUSE, Dialect.DATABRICKS, Dialect.DB2, Dialect.DUCKDB, Dialect.H2, Dialect.MEMSQL, Dialect.ORACLE, Dialect.POSTGRES, Dialect.PRESTO, Dialect.REDSHIFT, Dialect.SNOWFLAKE, Dialect.SQLSERVER, Dialect.SYBASE, Dialect.SYBASEIQ, Dialect.TRINO),
    MONTH_NUMBER("monthNumber", Resolution.PURE, null, Inference.MAPPED, Dialect.BIGQUERY, Dialect.CLICKHOUSE, Dialect.DATABRICKS, Dialect.DB2, Dialect.DUCKDB, Dialect.H2, Dialect.MEMSQL, Dialect.ORACLE, Dialect.POSTGRES, Dialect.PRESTO, Dialect.REDSHIFT, Dialect.SNOWFLAKE, Dialect.SPANNER, Dialect.SPARKSQL, Dialect.SQLSERVER, Dialect.SYBASE, Dialect.SYBASEIQ, Dialect.TRINO),
    MOST_RECENT_DAY_OF_WEEK("mostRecentDayOfWeek", Resolution.PURE, null, Inference.MAPPED, Dialect.DATABRICKS, Dialect.DB2, Dialect.DUCKDB, Dialect.H2, Dialect.MEMSQL, Dialect.ORACLE, Dialect.POSTGRES, Dialect.PRESTO, Dialect.SNOWFLAKE, Dialect.SPARKSQL, Dialect.SYBASE, Dialect.SYBASEIQ, Dialect.TRINO),
    NOT("not", Resolution.PURE, null, Inference.MAPPED),
    NOT_EQUAL("notEqual", Resolution.UNSUPPORTED, null, Inference.MAPPED, Dialect.DEFAULT),
    NOT_EQUAL_ANSI("notEqualAnsi", Resolution.SHIM, Pure.Lite.NOT_EQUAL_ANSI, Inference.MAPPED, Dialect.DEFAULT),
    NOW("now", Resolution.PURE, null, Inference.NONE, Dialect.BIGQUERY, Dialect.DATABRICKS, Dialect.DB2, Dialect.DUCKDB, Dialect.H2, Dialect.MEMSQL, Dialect.ORACLE, Dialect.POSTGRES, Dialect.PRESTO, Dialect.REDSHIFT, Dialect.SNOWFLAKE, Dialect.SPANNER, Dialect.SPARKSQL, Dialect.SQLSERVER, Dialect.SYBASE, Dialect.SYBASEIQ, Dialect.TRINO),
    NTH("nth", Resolution.PURE, null, Inference.NONE, Dialect.DEFAULT),
    NTILE("ntile", Resolution.PURE, null, Inference.NONE, Dialect.DEFAULT),
    NULL_SAFE_EQUAL("nullSafeEqual", Resolution.UNSUPPORTED, null, Inference.MAPPED, Dialect.BIGQUERY, Dialect.CLICKHOUSE, Dialect.DATABRICKS, Dialect.DEFAULT, Dialect.DUCKDB, Dialect.H2, Dialect.MEMSQL, Dialect.POSTGRES, Dialect.PRESTO, Dialect.SNOWFLAKE, Dialect.SPANNER, Dialect.SPARKSQL, Dialect.TRINO),
    NULL_SAFE_NOT_EQUAL("nullSafeNotEqual", Resolution.UNSUPPORTED, null, Inference.MAPPED, Dialect.BIGQUERY, Dialect.CLICKHOUSE, Dialect.DATABRICKS, Dialect.DEFAULT, Dialect.DUCKDB, Dialect.H2, Dialect.MEMSQL, Dialect.POSTGRES, Dialect.PRESTO, Dialect.SNOWFLAKE, Dialect.SPANNER, Dialect.SPARKSQL, Dialect.TRINO),
    OBJECT_REFERENCE_IN("objectReferenceIn", Resolution.PURE, null, Inference.NONE, Dialect.DEFAULT),
    OR("or", Resolution.PURE, null, Inference.MAPPED, Dialect.DEFAULT, Dialect.SPANNER),
    PARSE_BOOLEAN("parseBoolean", Resolution.PURE, null, Inference.NONE, Dialect.CLICKHOUSE, Dialect.DATABRICKS, Dialect.SNOWFLAKE),
    PARSE_DATE("parseDate", Resolution.PURE, null, Inference.MAPPED, Dialect.DATABRICKS, Dialect.DUCKDB, Dialect.H2, Dialect.PRESTO, Dialect.SNOWFLAKE, Dialect.SPARKSQL, Dialect.SYBASE, Dialect.SYBASEIQ, Dialect.TRINO),
    PARSE_DECIMAL("parseDecimal", Resolution.PURE, null, Inference.MAPPED, Dialect.BIGQUERY, Dialect.CLICKHOUSE, Dialect.DATABRICKS, Dialect.DUCKDB, Dialect.H2, Dialect.MEMSQL, Dialect.ORACLE, Dialect.POSTGRES, Dialect.REDSHIFT, Dialect.SNOWFLAKE, Dialect.SPANNER, Dialect.SPARKSQL, Dialect.SYBASE, Dialect.SYBASEIQ),
    PARSE_FLOAT("parseFloat", Resolution.PURE, null, Inference.MAPPED, Dialect.BIGQUERY, Dialect.CLICKHOUSE, Dialect.COMPOSITE, Dialect.DATABRICKS, Dialect.DB2, Dialect.DUCKDB, Dialect.H2, Dialect.MEMSQL, Dialect.ORACLE, Dialect.POSTGRES, Dialect.PRESTO, Dialect.REDSHIFT, Dialect.SNOWFLAKE, Dialect.SPANNER, Dialect.SPARKSQL, Dialect.SQLSERVER, Dialect.SYBASE, Dialect.SYBASEIQ, Dialect.TRINO),
    PARSE_INTEGER("parseInteger", Resolution.PURE, null, Inference.MAPPED, Dialect.BIGQUERY, Dialect.CLICKHOUSE, Dialect.COMPOSITE, Dialect.DATABRICKS, Dialect.DB2, Dialect.DUCKDB, Dialect.H2, Dialect.MEMSQL, Dialect.ORACLE, Dialect.POSTGRES, Dialect.PRESTO, Dialect.REDSHIFT, Dialect.SNOWFLAKE, Dialect.SPANNER, Dialect.SPARKSQL, Dialect.SQLSERVER, Dialect.SYBASE, Dialect.SYBASEIQ, Dialect.TRINO),
    PARSE_JSON("parseJson", Resolution.UNSUPPORTED, null, Inference.MAPPED, Dialect.DATABRICKS, Dialect.DUCKDB, Dialect.H2, Dialect.ORACLE, Dialect.POSTGRES, Dialect.SNOWFLAKE),
    PERCENT_RANK("percentRank", Resolution.PURE, null, Inference.NONE, Dialect.DEFAULT),
    PERCENTILE("percentile", Resolution.PURE, null, Inference.NONE, Dialect.CLICKHOUSE, Dialect.DEFAULT),
    PLUS("plus", Resolution.PURE, null, Inference.MAPPED, Dialect.DEFAULT, Dialect.SPANNER),
    POSITION("position", Resolution.TRANSLATED, null, Inference.NONE, Dialect.BIGQUERY, Dialect.COMPOSITE, Dialect.DATABRICKS, Dialect.DB2, Dialect.DUCKDB, Dialect.H2, Dialect.MEMSQL, Dialect.ORACLE, Dialect.POSTGRES, Dialect.PRESTO, Dialect.REDSHIFT, Dialect.SNOWFLAKE, Dialect.SPANNER, Dialect.SPARKSQL, Dialect.SQLSERVER, Dialect.SYBASE, Dialect.SYBASEIQ, Dialect.TRINO),
    POW("pow", Resolution.PURE, null, Inference.NONE, Dialect.DEFAULT, Dialect.SPANNER),
    PREVIOUS_DAY_OF_WEEK("previousDayOfWeek", Resolution.PURE, null, Inference.MAPPED, Dialect.DATABRICKS, Dialect.DB2, Dialect.DUCKDB, Dialect.H2, Dialect.MEMSQL, Dialect.ORACLE, Dialect.POSTGRES, Dialect.PRESTO, Dialect.SNOWFLAKE, Dialect.SPARKSQL, Dialect.SYBASE, Dialect.SYBASEIQ, Dialect.TRINO),
    QUARTER("quarter", Resolution.PURE, null, Inference.MAPPED, Dialect.DATABRICKS, Dialect.DB2, Dialect.DUCKDB, Dialect.H2, Dialect.MEMSQL, Dialect.ORACLE, Dialect.POSTGRES, Dialect.PRESTO, Dialect.SNOWFLAKE, Dialect.SPANNER, Dialect.SPARKSQL, Dialect.SQLSERVER, Dialect.SYBASE, Dialect.SYBASEIQ, Dialect.TRINO),
    QUARTER_NUMBER("quarterNumber", Resolution.PURE, null, Inference.MAPPED, Dialect.BIGQUERY, Dialect.DATABRICKS, Dialect.DB2, Dialect.DUCKDB, Dialect.H2, Dialect.MEMSQL, Dialect.ORACLE, Dialect.POSTGRES, Dialect.PRESTO, Dialect.REDSHIFT, Dialect.SNOWFLAKE, Dialect.SPANNER, Dialect.SPARKSQL, Dialect.SQLSERVER, Dialect.SYBASE, Dialect.SYBASEIQ, Dialect.TRINO),
    RANGE("range", Resolution.PURE, null, Inference.NONE, Dialect.CLICKHOUSE, Dialect.DATABRICKS, Dialect.DUCKDB, Dialect.SNOWFLAKE),
    RANK("rank", Resolution.PURE, null, Inference.MAPPED, Dialect.DEFAULT),
    REGEXP_COUNT("regexpCount", Resolution.PURE, null, Inference.NONE, Dialect.DATABRICKS, Dialect.DUCKDB, Dialect.POSTGRES, Dialect.SNOWFLAKE, Dialect.SPANNER, Dialect.TRINO),
    REGEXP_EXTRACT("regexpExtract", Resolution.PURE, null, Inference.NONE, Dialect.DATABRICKS, Dialect.DUCKDB, Dialect.MEMSQL, Dialect.POSTGRES, Dialect.SNOWFLAKE, Dialect.SPANNER, Dialect.TRINO),
    REGEXP_INDEX_OF("regexpIndexOf", Resolution.PURE, null, Inference.NONE, Dialect.DATABRICKS, Dialect.DUCKDB, Dialect.MEMSQL, Dialect.POSTGRES, Dialect.SNOWFLAKE, Dialect.SPANNER, Dialect.TRINO),
    REGEXP_LIKE("regexpLike", Resolution.PURE, null, Inference.NONE, Dialect.DATABRICKS, Dialect.DUCKDB, Dialect.MEMSQL, Dialect.POSTGRES, Dialect.SNOWFLAKE, Dialect.SPANNER, Dialect.TRINO),
    REGEXP_REPLACE("regexpReplace", Resolution.PURE, null, Inference.NONE, Dialect.DATABRICKS, Dialect.DUCKDB, Dialect.MEMSQL, Dialect.POSTGRES, Dialect.SNOWFLAKE, Dialect.SPANNER, Dialect.TRINO),
    REM("rem", Resolution.PURE, null, Inference.NONE, Dialect.DEFAULT, Dialect.MEMSQL, Dialect.SYBASE),
    REPEAT_STRING("repeatString", Resolution.PURE, null, Inference.NONE, Dialect.DEFAULT, Dialect.MEMSQL, Dialect.SQLSERVER, Dialect.SYBASE, Dialect.TRINO),
    REPLACE("replace", Resolution.PURE, null, Inference.MAPPED, Dialect.DEFAULT),
    REVERSE("reverse", Resolution.UNSUPPORTED, null, Inference.MAPPED),
    REVERSE_STRING("reverseString", Resolution.PURE, null, Inference.NONE, Dialect.DEFAULT, Dialect.DUCKDB, Dialect.H2),
    RIGHT("right", Resolution.PURE, null, Inference.MAPPED, Dialect.DB2, Dialect.DEFAULT, Dialect.MEMSQL, Dialect.ORACLE, Dialect.PRESTO, Dialect.SPANNER, Dialect.TRINO),
    ROUND("round", Resolution.PURE, null, Inference.MAPPED, Dialect.BIGQUERY, Dialect.CLICKHOUSE, Dialect.COMPOSITE, Dialect.DATABRICKS, Dialect.DB2, Dialect.DEFAULT, Dialect.DUCKDB, Dialect.H2, Dialect.MEMSQL, Dialect.ORACLE, Dialect.POSTGRES, Dialect.PRESTO, Dialect.REDSHIFT, Dialect.SNOWFLAKE, Dialect.SPANNER, Dialect.SPARKSQL, Dialect.SQLSERVER, Dialect.SYBASE, Dialect.SYBASEIQ, Dialect.TRINO),
    ROW_NUMBER("rowNumber", Resolution.PURE, null, Inference.MAPPED, Dialect.DEFAULT),
    RPAD("rpad", Resolution.PURE, null, Inference.MAPPED, Dialect.DEFAULT, Dialect.DUCKDB, Dialect.MEMSQL, Dialect.SQLSERVER, Dialect.SYBASE, Dialect.TRINO),
    RTRIM("rtrim", Resolution.PURE, null, Inference.MAPPED, Dialect.DEFAULT, Dialect.MEMSQL),
    SECOND("second", Resolution.PURE, null, Inference.MAPPED, Dialect.BIGQUERY, Dialect.CLICKHOUSE, Dialect.DATABRICKS, Dialect.DB2, Dialect.DUCKDB, Dialect.H2, Dialect.MEMSQL, Dialect.ORACLE, Dialect.POSTGRES, Dialect.PRESTO, Dialect.REDSHIFT, Dialect.SNOWFLAKE, Dialect.SPANNER, Dialect.SPARKSQL, Dialect.SQLSERVER, Dialect.SYBASE, Dialect.SYBASEIQ, Dialect.TRINO),
    SHA1("sha1", Resolution.TRANSLATED, null, Inference.MAPPED, Dialect.CLICKHOUSE, Dialect.DB2, Dialect.DEFAULT, Dialect.DUCKDB, Dialect.H2, Dialect.ORACLE, Dialect.POSTGRES, Dialect.SPARKSQL, Dialect.SQLSERVER, Dialect.SYBASE, Dialect.SYBASEIQ, Dialect.TRINO),
    SHA256("sha256", Resolution.TRANSLATED, null, Inference.MAPPED, Dialect.CLICKHOUSE, Dialect.DATABRICKS, Dialect.DB2, Dialect.DEFAULT, Dialect.DUCKDB, Dialect.H2, Dialect.MEMSQL, Dialect.ORACLE, Dialect.POSTGRES, Dialect.REDSHIFT, Dialect.SNOWFLAKE, Dialect.SPARKSQL, Dialect.SQLSERVER, Dialect.SYBASE, Dialect.SYBASEIQ, Dialect.TRINO),
    SIGN("sign", Resolution.PURE, null, Inference.MAPPED, Dialect.DEFAULT),
    SIN("sin", Resolution.PURE, null, Inference.MAPPED, Dialect.DEFAULT),
    SINH("sinh", Resolution.PURE, null, Inference.MAPPED, Dialect.DEFAULT),
    SIZE("size", Resolution.PURE, null, Inference.MAPPED, Dialect.DEFAULT, Dialect.SPANNER),
    SPLIT("split", Resolution.UNSUPPORTED, null, Inference.NONE, Dialect.BIGQUERY, Dialect.CLICKHOUSE, Dialect.DATABRICKS, Dialect.DUCKDB, Dialect.H2, Dialect.POSTGRES, Dialect.SNOWFLAKE),
    SPLIT_PART("splitPart", Resolution.TRANSLATED, null, Inference.NONE, Dialect.CLICKHOUSE, Dialect.DEFAULT, Dialect.DUCKDB, Dialect.H2, Dialect.SNOWFLAKE, Dialect.SPANNER, Dialect.SQLSERVER),
    SQL_FALSE("sqlFalse", Resolution.PURE, null, Inference.MAPPED, Dialect.DATABRICKS, Dialect.DEFAULT, Dialect.SPANNER),
    SQL_NULL("sqlNull", Resolution.PURE, null, Inference.MAPPED, Dialect.DEFAULT, Dialect.SPANNER),
    SQL_TRUE("sqlTrue", Resolution.PURE, null, Inference.MAPPED, Dialect.DATABRICKS, Dialect.DEFAULT, Dialect.SPANNER),
    SQRT("sqrt", Resolution.PURE, null, Inference.MAPPED, Dialect.DEFAULT),
    STARTS_WITH("startsWith", Resolution.PURE, null, Inference.MAPPED, Dialect.DEFAULT, Dialect.SNOWFLAKE, Dialect.SPANNER, Dialect.SPARKSQL, Dialect.SYBASEIQ),
    STD_DEV_POPULATION("stdDevPopulation", Resolution.PURE, null, Inference.MAPPED, Dialect.BIGQUERY, Dialect.CLICKHOUSE, Dialect.DATABRICKS, Dialect.DB2, Dialect.DUCKDB, Dialect.H2, Dialect.MEMSQL, Dialect.ORACLE, Dialect.POSTGRES, Dialect.PRESTO, Dialect.REDSHIFT, Dialect.SNOWFLAKE, Dialect.SPANNER, Dialect.SPARKSQL, Dialect.SQLSERVER, Dialect.SYBASE, Dialect.SYBASEIQ, Dialect.TRINO),
    STD_DEV_SAMPLE("stdDevSample", Resolution.PURE, null, Inference.MAPPED, Dialect.BIGQUERY, Dialect.CLICKHOUSE, Dialect.DATABRICKS, Dialect.DB2, Dialect.DUCKDB, Dialect.H2, Dialect.MEMSQL, Dialect.ORACLE, Dialect.POSTGRES, Dialect.PRESTO, Dialect.REDSHIFT, Dialect.SNOWFLAKE, Dialect.SPANNER, Dialect.SPARKSQL, Dialect.SQLSERVER, Dialect.SYBASE, Dialect.SYBASEIQ, Dialect.TRINO),
    SUB("sub", Resolution.TRANSLATED, null, Inference.MAPPED, Dialect.DEFAULT),
    SUBSTRING("substring", Resolution.TRANSLATED, null, Inference.MAPPED, Dialect.BIGQUERY, Dialect.CLICKHOUSE, Dialect.COMPOSITE, Dialect.DATABRICKS, Dialect.DB2, Dialect.DUCKDB, Dialect.H2, Dialect.MEMSQL, Dialect.ORACLE, Dialect.POSTGRES, Dialect.PRESTO, Dialect.REDSHIFT, Dialect.SNOWFLAKE, Dialect.SPANNER, Dialect.SPARKSQL, Dialect.SQLSERVER, Dialect.SYBASE, Dialect.SYBASEIQ, Dialect.TRINO),
    SUM("sum", Resolution.PURE, null, Inference.MAPPED, Dialect.DEFAULT),
    TAN("tan", Resolution.PURE, null, Inference.MAPPED, Dialect.DEFAULT),
    TANH("tanh", Resolution.PURE, null, Inference.MAPPED, Dialect.CLICKHOUSE, Dialect.DEFAULT),
    TIME_BUCKET("timeBucket", Resolution.PURE, null, Inference.MAPPED, Dialect.CLICKHOUSE, Dialect.DUCKDB, Dialect.SNOWFLAKE),
    TIMES("times", Resolution.PURE, null, Inference.MAPPED, Dialect.DEFAULT),
    TO_DECIMAL("toDecimal", Resolution.PURE, null, Inference.NONE, Dialect.CLICKHOUSE, Dialect.DATABRICKS, Dialect.DUCKDB, Dialect.H2, Dialect.ORACLE, Dialect.POSTGRES, Dialect.PRESTO, Dialect.REDSHIFT, Dialect.SNOWFLAKE, Dialect.SPARKSQL, Dialect.SQLSERVER, Dialect.SYBASE, Dialect.SYBASEIQ),
    TO_FLOAT("toFloat", Resolution.PURE, null, Inference.NONE, Dialect.BIGQUERY, Dialect.CLICKHOUSE, Dialect.DATABRICKS, Dialect.DUCKDB, Dialect.H2, Dialect.ORACLE, Dialect.POSTGRES, Dialect.PRESTO, Dialect.REDSHIFT, Dialect.SNOWFLAKE, Dialect.SPARKSQL, Dialect.SQLSERVER, Dialect.SYBASE, Dialect.SYBASEIQ),
    TO_JSON("toJson", Resolution.UNSUPPORTED, null, Inference.MAPPED, Dialect.DATABRICKS, Dialect.DUCKDB, Dialect.POSTGRES, Dialect.SNOWFLAKE),
    TO_LOWER("toLower", Resolution.PURE, null, Inference.MAPPED, Dialect.DEFAULT),
    TO_ONE("toOne", Resolution.PURE, null, Inference.MAPPED, Dialect.DEFAULT, Dialect.SPANNER),
    TO_STRING("toString", Resolution.PURE, null, Inference.MAPPED, Dialect.BIGQUERY, Dialect.CLICKHOUSE, Dialect.DATABRICKS, Dialect.DB2, Dialect.DUCKDB, Dialect.H2, Dialect.MEMSQL, Dialect.ORACLE, Dialect.POSTGRES, Dialect.PRESTO, Dialect.REDSHIFT, Dialect.SNOWFLAKE, Dialect.SPANNER, Dialect.SPARKSQL, Dialect.SQLSERVER, Dialect.SYBASE, Dialect.SYBASEIQ, Dialect.TRINO),
    TO_TIMESTAMP("toTimestamp", Resolution.TRANSLATED, null, Inference.NONE, Dialect.DUCKDB, Dialect.H2, Dialect.MEMSQL, Dialect.ORACLE, Dialect.POSTGRES, Dialect.PRESTO, Dialect.SNOWFLAKE, Dialect.SPARKSQL, Dialect.SYBASE, Dialect.SYBASEIQ, Dialect.TRINO),
    TO_UPPER("toUpper", Resolution.PURE, null, Inference.MAPPED, Dialect.DEFAULT),
    TO_VARIANT("toVariant", Resolution.PURE, null, Inference.MAPPED, Dialect.DATABRICKS, Dialect.DUCKDB, Dialect.POSTGRES, Dialect.SNOWFLAKE),
    TO_VARIANT_LIST("toVariantList", Resolution.UNSUPPORTED, null, Inference.NONE, Dialect.CLICKHOUSE, Dialect.DATABRICKS, Dialect.DUCKDB, Dialect.POSTGRES, Dialect.SNOWFLAKE),
    TO_VARIANT_OBJECT("toVariantObject", Resolution.UNSUPPORTED, null, Inference.NONE, Dialect.DATABRICKS, Dialect.DUCKDB, Dialect.POSTGRES, Dialect.SNOWFLAKE),
    TODAY("today", Resolution.PURE, null, Inference.MAPPED, Dialect.BIGQUERY, Dialect.CLICKHOUSE, Dialect.DATABRICKS, Dialect.DB2, Dialect.DUCKDB, Dialect.H2, Dialect.MEMSQL, Dialect.ORACLE, Dialect.POSTGRES, Dialect.PRESTO, Dialect.REDSHIFT, Dialect.SNOWFLAKE, Dialect.SPANNER, Dialect.SPARKSQL, Dialect.SQLSERVER, Dialect.SYBASE, Dialect.SYBASEIQ, Dialect.TRINO),
    TRIM("trim", Resolution.PURE, null, Inference.MAPPED, Dialect.DEFAULT, Dialect.SYBASE),
    VALUES("values", Resolution.PURE, null, Inference.NONE, Dialect.DATABRICKS, Dialect.DUCKDB, Dialect.SNOWFLAKE),
    VARIANCE("variance", Resolution.PURE, null, Inference.NONE, Dialect.CLICKHOUSE, Dialect.DEFAULT, Dialect.DUCKDB),
    VARIANCE_POPULATION("variancePopulation", Resolution.PURE, null, Inference.MAPPED, Dialect.CLICKHOUSE, Dialect.DEFAULT, Dialect.DUCKDB, Dialect.SQLSERVER),
    VARIANCE_SAMPLE("varianceSample", Resolution.PURE, null, Inference.MAPPED, Dialect.CLICKHOUSE, Dialect.DEFAULT, Dialect.DUCKDB, Dialect.SQLSERVER),
    VARIANT_TO("variantTo", Resolution.UNSUPPORTED, null, Inference.NONE, Dialect.DEFAULT, Dialect.DUCKDB, Dialect.POSTGRES),
    WEEK_OF_YEAR("weekOfYear", Resolution.PURE, null, Inference.MAPPED, Dialect.BIGQUERY, Dialect.CLICKHOUSE, Dialect.DATABRICKS, Dialect.DB2, Dialect.DUCKDB, Dialect.H2, Dialect.MEMSQL, Dialect.ORACLE, Dialect.POSTGRES, Dialect.PRESTO, Dialect.REDSHIFT, Dialect.SNOWFLAKE, Dialect.SPANNER, Dialect.SPARKSQL, Dialect.SQLSERVER, Dialect.SYBASE, Dialect.SYBASEIQ, Dialect.TRINO),
    YEAR("year", Resolution.PURE, null, Inference.MAPPED, Dialect.BIGQUERY, Dialect.CLICKHOUSE, Dialect.DATABRICKS, Dialect.DB2, Dialect.DUCKDB, Dialect.H2, Dialect.MEMSQL, Dialect.ORACLE, Dialect.POSTGRES, Dialect.PRESTO, Dialect.REDSHIFT, Dialect.SNOWFLAKE, Dialect.SPANNER, Dialect.SPARKSQL, Dialect.SQLSERVER, Dialect.SYBASE, Dialect.SYBASEIQ, Dialect.TRINO);

    /** How the platform resolves an engine dynafunction. */
    public enum Resolution { PURE, SHIM, TRANSLATED, UNSUPPORTED }

    /** Whether the engine's relational TYPE-INFERENCE map
     *  ({@code getDynaFunctionTypeInferenceMap} in relationalExtension.pure) has
     *  a rule for the name — the engine's second registry of dynafunction
     *  names; five names ({@code case}, {@code between}, {@code not}, …) exist
     *  only there. */
    public enum Inference { MAPPED, NONE }

    /** The engine dialect extension files that register a name. */
    public enum Dialect { BIGQUERY, CLICKHOUSE, COMPOSITE, DATABRICKS, DB2, DEFAULT, DUCKDB, H2, MEMSQL, ORACLE, POSTGRES, PRESTO, REDSHIFT, SNOWFLAKE, SPANNER, SPARKSQL, SQLSERVER, SYBASE, SYBASEIQ, TRINO }

    private final String name;
    private final Resolution resolution;
    private final @com.legend.Nullable String liteFqn;
    private final Inference inference;
    private final EnumSet<Dialect> dialects;

    DynaFn(String name, Resolution resolution, @com.legend.Nullable String liteFqn, Inference inference, Dialect... dialects) {
        this.name = name;
        this.resolution = resolution;
        this.liteFqn = liteFqn;
        this.inference = inference;
        this.dialects = EnumSet.noneOf(Dialect.class);
        this.dialects.addAll(List.of(dialects));
    }

    /** Whether the engine's type-inference map has a rule for the name. */
    public Inference inference() {
        return inference;
    }

    /** The engine's spelling of the operator. */
    public String dynaName() {
        return name;
    }

    public Resolution resolution() {
        return resolution;
    }

    /** The dialects whose rendering registry declares this name (empty for a
     *  name the engine knows only in its type-inference map). */
    public java.util.Set<Dialect> dialects() {
        return java.util.Collections.unmodifiableSet(dialects);
    }

    /** The {@link Pure.Lite} identity a SHIM resolves to (the constant itself,
     *  spelled in the member — the compiler holds it, never a lookup). */
    public String liteFqn() {
        return java.util.Objects.requireNonNull(liteFqn, name + " is not a SHIM");
    }

    private static final Map<String, DynaFn> BY_NAME;

    static {
        Map<String, DynaFn> m = new java.util.HashMap<>();
        for (DynaFn d : values()) {
            m.put(d.name, d);
        }
        BY_NAME = Map.copyOf(m);
    }

    /** The registry entry for an engine operator name, or empty when the engine
     *  registers no such dynafunction (the name is then a plain Pure function
     *  the mapping expression calls, resolved like any other). */
    public static Optional<DynaFn> of(String dynaName) {
        return Optional.ofNullable(BY_NAME.get(dynaName));
    }

    /** Every member of one resolution kind. */
    public static List<DynaFn> withResolution(Resolution r) {
        return java.util.Arrays.stream(values()).filter(d -> d.resolution == r).toList();
    }
}
