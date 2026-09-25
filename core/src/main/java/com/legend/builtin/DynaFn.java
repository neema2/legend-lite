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
 *   <li>{@link Resolution#PURE}: passes through to the Pure native(s) the row's
 *       {@link #fqns()} name — the engine's operator IS pure's function;</li>
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
 * GENERATED from the checkouts by {@code DynaFnGenerator} ({@code bazel run //:update_generated})
 * (members, dialects); the resolution column is the platform's own decision, kept
 * by hand and VERIFIED by the same test (a PURE name must exist in the catalog, a
 * SHIM must name a Lite constant, every TRANSLATED name has an arm and every armed
 * name not TRANSLATED is PURE).
 * Never a name set anywhere else: {@link #of(String)} is the one lookup.
 */
public enum DynaFn {
    ABS("abs", Resolution.PURE, List.of("meta::pure::functions::math::abs"), Inference.MAPPED, Dialect.DEFAULT),
    ACOS("acos", Resolution.PURE, List.of("meta::pure::functions::math::acos"), Inference.MAPPED, Dialect.DEFAULT),
    ADD("add", Resolution.TRANSLATED, List.of(), Inference.MAPPED, Dialect.DEFAULT),
    ADJUST("adjust", Resolution.TRANSLATED, List.of(), Inference.MAPPED, Dialect.BIGQUERY, Dialect.CLICKHOUSE, Dialect.DATABRICKS, Dialect.DB2, Dialect.DUCKDB, Dialect.H2, Dialect.MEMSQL, Dialect.ORACLE, Dialect.POSTGRES, Dialect.PRESTO, Dialect.SNOWFLAKE, Dialect.SPARKSQL, Dialect.SQLSERVER, Dialect.SYBASE, Dialect.SYBASEIQ, Dialect.TRINO),
    ALL_OF("allOf", Resolution.UNSUPPORTED, List.of(), Inference.MAPPED, Dialect.DEFAULT),
    AND("and", Resolution.PURE, List.of("meta::pure::functions::boolean::and", "meta::pure::functions::collection::and"), Inference.MAPPED, Dialect.DEFAULT, Dialect.SPANNER),
    ANY_OF("anyOf", Resolution.UNSUPPORTED, List.of(), Inference.MAPPED, Dialect.DEFAULT),
    ARRAY_APPEND("array_append", Resolution.UNSUPPORTED, List.of(), Inference.NONE, Dialect.DATABRICKS, Dialect.DUCKDB, Dialect.SNOWFLAKE),
    ARRAY_CONCATENATE("array_concatenate", Resolution.UNSUPPORTED, List.of(), Inference.NONE, Dialect.DATABRICKS, Dialect.DUCKDB, Dialect.SNOWFLAKE),
    ARRAY_CONTAINS("array_contains", Resolution.UNSUPPORTED, List.of(), Inference.NONE, Dialect.DATABRICKS, Dialect.DUCKDB, Dialect.SNOWFLAKE),
    ARRAY_DISTINCT("array_distinct", Resolution.UNSUPPORTED, List.of(), Inference.NONE, Dialect.DATABRICKS, Dialect.DUCKDB, Dialect.SNOWFLAKE),
    ARRAY_DROP("array_drop", Resolution.UNSUPPORTED, List.of(), Inference.NONE, Dialect.DATABRICKS, Dialect.DUCKDB, Dialect.SNOWFLAKE),
    ARRAY_FIRST("array_first", Resolution.UNSUPPORTED, List.of(), Inference.NONE, Dialect.DATABRICKS, Dialect.DUCKDB, Dialect.SNOWFLAKE),
    ARRAY_FLATTEN("array_flatten", Resolution.UNSUPPORTED, List.of(), Inference.NONE, Dialect.DATABRICKS, Dialect.DUCKDB, Dialect.SNOWFLAKE),
    ARRAY_INIT("array_init", Resolution.UNSUPPORTED, List.of(), Inference.NONE, Dialect.DATABRICKS, Dialect.DUCKDB, Dialect.SNOWFLAKE),
    ARRAY_LAST("array_last", Resolution.UNSUPPORTED, List.of(), Inference.NONE, Dialect.DATABRICKS, Dialect.DUCKDB, Dialect.SNOWFLAKE),
    ARRAY_MAX("array_max", Resolution.UNSUPPORTED, List.of(), Inference.NONE, Dialect.CLICKHOUSE, Dialect.DATABRICKS, Dialect.DUCKDB, Dialect.SNOWFLAKE),
    ARRAY_MIN("array_min", Resolution.UNSUPPORTED, List.of(), Inference.NONE, Dialect.CLICKHOUSE, Dialect.DATABRICKS, Dialect.DUCKDB, Dialect.SNOWFLAKE),
    ARRAY_POSITION("array_position", Resolution.UNSUPPORTED, List.of(), Inference.NONE, Dialect.DATABRICKS, Dialect.DUCKDB, Dialect.SNOWFLAKE),
    ARRAY_REVERSE("array_reverse", Resolution.UNSUPPORTED, List.of(), Inference.NONE, Dialect.DATABRICKS, Dialect.DUCKDB, Dialect.SNOWFLAKE),
    ARRAY_SIZE("array_size", Resolution.UNSUPPORTED, List.of(), Inference.NONE, Dialect.DATABRICKS, Dialect.DUCKDB, Dialect.SNOWFLAKE),
    ARRAY_SLICE("array_slice", Resolution.UNSUPPORTED, List.of(), Inference.NONE, Dialect.DATABRICKS, Dialect.DUCKDB, Dialect.SNOWFLAKE),
    ARRAY_SORT("array_sort", Resolution.UNSUPPORTED, List.of(), Inference.NONE, Dialect.DATABRICKS, Dialect.DUCKDB, Dialect.SNOWFLAKE),
    ARRAY_SUM("array_sum", Resolution.UNSUPPORTED, List.of(), Inference.NONE, Dialect.DATABRICKS, Dialect.DUCKDB, Dialect.SNOWFLAKE),
    ARRAY_TAIL("array_tail", Resolution.UNSUPPORTED, List.of(), Inference.NONE, Dialect.DATABRICKS, Dialect.DUCKDB, Dialect.SNOWFLAKE),
    ARRAY_TAKE("array_take", Resolution.UNSUPPORTED, List.of(), Inference.NONE, Dialect.DATABRICKS, Dialect.DUCKDB, Dialect.SNOWFLAKE),
    ARRAY_TO_STRING("array_to_string", Resolution.UNSUPPORTED, List.of(), Inference.NONE, Dialect.DATABRICKS, Dialect.DUCKDB, Dialect.SNOWFLAKE),
    ASCII("ascii", Resolution.PURE, List.of("meta::pure::functions::string::ascii"), Inference.MAPPED, Dialect.DEFAULT, Dialect.SPANNER),
    ASIN("asin", Resolution.PURE, List.of("meta::pure::functions::math::asin"), Inference.MAPPED, Dialect.DEFAULT),
    ATAN("atan", Resolution.PURE, List.of("meta::pure::functions::math::atan"), Inference.MAPPED, Dialect.DEFAULT),
    ATAN2("atan2", Resolution.PURE, List.of("meta::pure::functions::math::atan2"), Inference.MAPPED, Dialect.DEFAULT, Dialect.MEMSQL, Dialect.SQLSERVER, Dialect.SYBASE),
    AVERAGE("average", Resolution.PURE, List.of("meta::pure::functions::math::average"), Inference.MAPPED, Dialect.DEFAULT),
    AVERAGE_RANK("averageRank", Resolution.UNSUPPORTED, List.of(), Inference.MAPPED, Dialect.DEFAULT),
    BETWEEN("between", Resolution.PURE, List.of("meta::pure::functions::boolean::between"), Inference.MAPPED),
    BIT_AND("bitAnd", Resolution.PURE, List.of("meta::pure::functions::math::bitAnd"), Inference.MAPPED, Dialect.DATABRICKS, Dialect.DUCKDB, Dialect.SNOWFLAKE),
    BIT_NOT("bitNot", Resolution.PURE, List.of("meta::pure::functions::math::bitNot"), Inference.MAPPED, Dialect.DATABRICKS, Dialect.DUCKDB, Dialect.SNOWFLAKE),
    BIT_OR("bitOr", Resolution.PURE, List.of("meta::pure::functions::math::bitOr"), Inference.MAPPED, Dialect.DATABRICKS, Dialect.DUCKDB, Dialect.SNOWFLAKE),
    BIT_SHIFT_LEFT("bitShiftLeft", Resolution.PURE, List.of("meta::pure::functions::math::bitShiftLeft"), Inference.MAPPED, Dialect.DATABRICKS, Dialect.DUCKDB, Dialect.SNOWFLAKE),
    BIT_SHIFT_RIGHT("bitShiftRight", Resolution.PURE, List.of("meta::pure::functions::math::bitShiftRight"), Inference.MAPPED, Dialect.DATABRICKS, Dialect.DUCKDB, Dialect.SNOWFLAKE),
    BIT_XOR("bitXor", Resolution.PURE, List.of("meta::pure::functions::math::bitXor"), Inference.MAPPED, Dialect.DATABRICKS, Dialect.DUCKDB, Dialect.SNOWFLAKE),
    BOOLAND("booland", Resolution.UNSUPPORTED, List.of(), Inference.MAPPED, Dialect.BIGQUERY, Dialect.DATABRICKS, Dialect.DUCKDB, Dialect.H2, Dialect.MEMSQL, Dialect.ORACLE, Dialect.POSTGRES, Dialect.PRESTO, Dialect.REDSHIFT, Dialect.SNOWFLAKE, Dialect.SPANNER, Dialect.SQLSERVER, Dialect.SYBASE, Dialect.SYBASEIQ, Dialect.TRINO),
    BOOLOR("boolor", Resolution.UNSUPPORTED, List.of(), Inference.MAPPED, Dialect.BIGQUERY, Dialect.DATABRICKS, Dialect.DUCKDB, Dialect.H2, Dialect.MEMSQL, Dialect.ORACLE, Dialect.POSTGRES, Dialect.PRESTO, Dialect.REDSHIFT, Dialect.SNOWFLAKE, Dialect.SPANNER, Dialect.SQLSERVER, Dialect.SYBASE, Dialect.SYBASEIQ, Dialect.TRINO),
    CASE("case", Resolution.TRANSLATED, List.of(), Inference.MAPPED),
    CAST("cast", Resolution.PURE, List.of("meta::pure::functions::lang::cast"), Inference.MAPPED, Dialect.DEFAULT),
    CAST_BOOLEAN("castBoolean", Resolution.UNSUPPORTED, List.of(), Inference.NONE, Dialect.DUCKDB),
    CBRT("cbrt", Resolution.PURE, List.of("meta::pure::functions::math::cbrt"), Inference.MAPPED, Dialect.DEFAULT, Dialect.ORACLE),
    CEILING("ceiling", Resolution.PURE, List.of("meta::pure::functions::math::ceiling"), Inference.MAPPED, Dialect.DEFAULT, Dialect.ORACLE, Dialect.SNOWFLAKE),
    CHAR("char", Resolution.PURE, List.of("meta::pure::functions::string::char"), Inference.MAPPED, Dialect.CLICKHOUSE, Dialect.DB2, Dialect.DEFAULT, Dialect.H2, Dialect.MEMSQL, Dialect.SPANNER, Dialect.SPARKSQL, Dialect.SQLSERVER, Dialect.SYBASE, Dialect.SYBASEIQ),
    CHR("chr", Resolution.UNSUPPORTED, List.of(), Inference.NONE, Dialect.DUCKDB),
    COALESCE("coalesce", Resolution.PURE, List.of("meta::pure::functions::flow::coalesce"), Inference.MAPPED, Dialect.DEFAULT, Dialect.SPANNER),
    CONCAT("concat", Resolution.TRANSLATED, List.of(), Inference.MAPPED, Dialect.BIGQUERY, Dialect.CLICKHOUSE, Dialect.DATABRICKS, Dialect.DB2, Dialect.DUCKDB, Dialect.H2, Dialect.MEMSQL, Dialect.ORACLE, Dialect.POSTGRES, Dialect.PRESTO, Dialect.REDSHIFT, Dialect.SNOWFLAKE, Dialect.SPANNER, Dialect.SPARKSQL, Dialect.SQLSERVER, Dialect.SYBASE, Dialect.SYBASEIQ, Dialect.TRINO),
    CONTAINS("contains", Resolution.PURE, List.of("meta::pure::functions::collection::contains", "meta::pure::functions::string::contains"), Inference.MAPPED, Dialect.DEFAULT, Dialect.SNOWFLAKE, Dialect.SPANNER, Dialect.SPARKSQL, Dialect.SYBASEIQ),
    CONVERT_DATE("convertDate", Resolution.TRANSLATED, List.of(), Inference.MAPPED, Dialect.DATABRICKS, Dialect.DB2, Dialect.DUCKDB, Dialect.H2, Dialect.MEMSQL, Dialect.PRESTO, Dialect.SNOWFLAKE, Dialect.SPARKSQL, Dialect.SYBASE, Dialect.SYBASEIQ, Dialect.TRINO),
    CONVERT_DATE_TIME("convertDateTime", Resolution.TRANSLATED, List.of(), Inference.MAPPED, Dialect.DATABRICKS, Dialect.DB2, Dialect.DUCKDB, Dialect.H2, Dialect.PRESTO, Dialect.SNOWFLAKE, Dialect.SPARKSQL, Dialect.SYBASE, Dialect.SYBASEIQ, Dialect.TRINO),
    CONVERT_TIME_ZONE("convertTimeZone", Resolution.TRANSLATED, List.of(), Inference.NONE, Dialect.H2, Dialect.MEMSQL, Dialect.SNOWFLAKE),
    CONVERT_VARCHAR128("convertVarchar128", Resolution.TRANSLATED, List.of(), Inference.MAPPED, Dialect.DATABRICKS, Dialect.DB2, Dialect.DUCKDB, Dialect.H2, Dialect.MEMSQL, Dialect.PRESTO, Dialect.SNOWFLAKE, Dialect.SPARKSQL, Dialect.SYBASE, Dialect.SYBASEIQ, Dialect.TRINO),
    CORR("corr", Resolution.PURE, List.of("meta::pure::functions::math::corr"), Inference.MAPPED, Dialect.DEFAULT),
    COS("cos", Resolution.PURE, List.of("meta::pure::functions::math::cos"), Inference.MAPPED, Dialect.DEFAULT),
    COSH("cosh", Resolution.PURE, List.of("meta::pure::functions::math::cosh"), Inference.MAPPED, Dialect.DEFAULT),
    COT("cot", Resolution.PURE, List.of("meta::pure::functions::math::cot"), Inference.MAPPED, Dialect.CLICKHOUSE, Dialect.DEFAULT, Dialect.TRINO),
    COUNT("count", Resolution.PURE, List.of("meta::pure::functions::collection::count"), Inference.MAPPED, Dialect.DEFAULT, Dialect.SPANNER),
    COVAR_POPULATION("covarPopulation", Resolution.PURE, List.of("meta::pure::functions::math::covarPopulation"), Inference.MAPPED, Dialect.DEFAULT),
    COVAR_SAMPLE("covarSample", Resolution.PURE, List.of("meta::pure::functions::math::covarSample"), Inference.MAPPED, Dialect.DEFAULT),
    CUMULATIVE_DISTRIBUTION("cumulativeDistribution", Resolution.PURE, List.of("meta::pure::functions::relation::cumulativeDistribution"), Inference.NONE, Dialect.DEFAULT),
    CURRENT_USER_ID("currentUserId", Resolution.PURE, List.of("meta::pure::functions::runtime::currentUserId", "meta::core::runtime::currentUserId"), Inference.NONE, Dialect.DEFAULT, Dialect.ORACLE, Dialect.SNOWFLAKE),
    DATE("date", Resolution.PURE, List.of("meta::pure::functions::date::date"), Inference.NONE, Dialect.CLICKHOUSE, Dialect.DATABRICKS, Dialect.DUCKDB, Dialect.SNOWFLAKE),
    DATE_DIFF("dateDiff", Resolution.PURE, List.of("meta::pure::functions::date::dateDiff"), Inference.MAPPED, Dialect.CLICKHOUSE, Dialect.DATABRICKS, Dialect.DUCKDB, Dialect.H2, Dialect.MEMSQL, Dialect.ORACLE, Dialect.POSTGRES, Dialect.PRESTO, Dialect.REDSHIFT, Dialect.SNOWFLAKE, Dialect.SPANNER, Dialect.SPARKSQL, Dialect.SQLSERVER, Dialect.SYBASE, Dialect.SYBASEIQ, Dialect.TRINO),
    DATE_PART("datePart", Resolution.PURE, List.of("meta::pure::functions::date::datePart"), Inference.MAPPED, Dialect.BIGQUERY, Dialect.CLICKHOUSE, Dialect.DATABRICKS, Dialect.DB2, Dialect.DUCKDB, Dialect.H2, Dialect.MEMSQL, Dialect.ORACLE, Dialect.POSTGRES, Dialect.PRESTO, Dialect.REDSHIFT, Dialect.SNOWFLAKE, Dialect.SPANNER, Dialect.SPARKSQL, Dialect.SQLSERVER, Dialect.SYBASE, Dialect.SYBASEIQ, Dialect.TRINO),
    DAY_OF_MWEEK("dayOfMWeek", Resolution.UNSUPPORTED, List.of(), Inference.MAPPED),
    DAY_OF_MONTH("dayOfMonth", Resolution.PURE, List.of("meta::pure::functions::date::dayOfMonth"), Inference.MAPPED, Dialect.BIGQUERY, Dialect.CLICKHOUSE, Dialect.DATABRICKS, Dialect.DUCKDB, Dialect.H2, Dialect.MEMSQL, Dialect.ORACLE, Dialect.POSTGRES, Dialect.REDSHIFT, Dialect.SNOWFLAKE, Dialect.SPANNER, Dialect.SPARKSQL, Dialect.SQLSERVER, Dialect.SYBASE, Dialect.SYBASEIQ, Dialect.TRINO),
    DAY_OF_WEEK("dayOfWeek", Resolution.TRANSLATED, List.of(), Inference.NONE, Dialect.BIGQUERY, Dialect.CLICKHOUSE, Dialect.DATABRICKS, Dialect.DB2, Dialect.DUCKDB, Dialect.H2, Dialect.MEMSQL, Dialect.ORACLE, Dialect.POSTGRES, Dialect.PRESTO, Dialect.REDSHIFT, Dialect.SNOWFLAKE, Dialect.SPANNER, Dialect.SPARKSQL, Dialect.SQLSERVER, Dialect.SYBASE, Dialect.SYBASEIQ, Dialect.TRINO),
    DAY_OF_WEEK_NUMBER("dayOfWeekNumber", Resolution.TRANSLATED, List.of(), Inference.MAPPED, Dialect.BIGQUERY, Dialect.CLICKHOUSE, Dialect.DATABRICKS, Dialect.DB2, Dialect.DUCKDB, Dialect.H2, Dialect.MEMSQL, Dialect.ORACLE, Dialect.POSTGRES, Dialect.PRESTO, Dialect.REDSHIFT, Dialect.SNOWFLAKE, Dialect.SPANNER, Dialect.SPARKSQL, Dialect.SQLSERVER, Dialect.SYBASE, Dialect.SYBASEIQ, Dialect.TRINO),
    DAY_OF_YEAR("dayOfYear", Resolution.PURE, List.of("meta::pure::functions::date::dayOfYear"), Inference.MAPPED, Dialect.BIGQUERY, Dialect.CLICKHOUSE, Dialect.DATABRICKS, Dialect.DB2, Dialect.DUCKDB, Dialect.H2, Dialect.MEMSQL, Dialect.ORACLE, Dialect.POSTGRES, Dialect.PRESTO, Dialect.REDSHIFT, Dialect.SNOWFLAKE, Dialect.SPANNER, Dialect.SPARKSQL, Dialect.SQLSERVER, Dialect.SYBASE, Dialect.SYBASEIQ, Dialect.TRINO),
    DECODE_BASE64("decodeBase64", Resolution.PURE, List.of("meta::pure::functions::string::decodeBase64"), Inference.MAPPED, Dialect.BIGQUERY, Dialect.CLICKHOUSE, Dialect.DATABRICKS, Dialect.DUCKDB, Dialect.H2, Dialect.MEMSQL, Dialect.ORACLE, Dialect.POSTGRES, Dialect.SNOWFLAKE, Dialect.SPANNER, Dialect.SQLSERVER),
    DENSE_RANK("denseRank", Resolution.PURE, List.of("meta::pure::functions::relation::denseRank"), Inference.MAPPED, Dialect.DEFAULT),
    DISTINCT("distinct", Resolution.PURE, List.of("meta::pure::functions::relation::distinct", "meta::pure::functions::collection::distinct"), Inference.MAPPED, Dialect.DEFAULT, Dialect.SPANNER),
    DIVIDE("divide", Resolution.PURE, List.of("meta::pure::functions::math::divide"), Inference.MAPPED, Dialect.DEFAULT),
    DIVIDE_ROUND("divideRound", Resolution.SHIM, List.of(Pure.Lite.DIVIDE_ROUND), Inference.NONE, Dialect.DEFAULT),
    ENCODE_BASE64("encodeBase64", Resolution.PURE, List.of("meta::pure::functions::string::encodeBase64"), Inference.MAPPED, Dialect.BIGQUERY, Dialect.CLICKHOUSE, Dialect.DATABRICKS, Dialect.DUCKDB, Dialect.H2, Dialect.MEMSQL, Dialect.ORACLE, Dialect.POSTGRES, Dialect.SNOWFLAKE, Dialect.SPANNER, Dialect.SQLSERVER),
    ENDS_WITH("endsWith", Resolution.PURE, List.of("meta::pure::functions::string::endsWith"), Inference.MAPPED, Dialect.DEFAULT, Dialect.SNOWFLAKE, Dialect.SPANNER, Dialect.SPARKSQL, Dialect.SYBASEIQ),
    EQUAL("equal", Resolution.PURE, List.of("meta::pure::functions::boolean::equal"), Inference.MAPPED, Dialect.DEFAULT),
    EXISTS("exists", Resolution.PURE, List.of("meta::pure::functions::collection::exists", "meta::pure::functions::relation::exists"), Inference.MAPPED, Dialect.DEFAULT),
    EXP("exp", Resolution.PURE, List.of("meta::pure::functions::math::exp"), Inference.MAPPED, Dialect.DEFAULT, Dialect.SPANNER),
    EXTRACT_FROM_SEMI_STRUCTURED("extractFromSemiStructured", Resolution.TRANSLATED, List.of(), Inference.NONE, Dialect.DATABRICKS, Dialect.DUCKDB, Dialect.H2, Dialect.MEMSQL, Dialect.POSTGRES, Dialect.SNOWFLAKE),
    FIRST("first", Resolution.PURE, List.of("meta::pure::functions::relation::first", "meta::pure::functions::collection::first"), Inference.NONE, Dialect.DEFAULT, Dialect.DUCKDB),
    FIRST_DAY_OF_MONTH("firstDayOfMonth", Resolution.PURE, List.of("meta::pure::functions::date::firstDayOfMonth"), Inference.MAPPED, Dialect.BIGQUERY, Dialect.CLICKHOUSE, Dialect.DATABRICKS, Dialect.DB2, Dialect.DUCKDB, Dialect.H2, Dialect.MEMSQL, Dialect.ORACLE, Dialect.POSTGRES, Dialect.PRESTO, Dialect.REDSHIFT, Dialect.SNOWFLAKE, Dialect.SPANNER, Dialect.SPARKSQL, Dialect.SQLSERVER, Dialect.SYBASE, Dialect.SYBASEIQ, Dialect.TRINO),
    FIRST_DAY_OF_QUARTER("firstDayOfQuarter", Resolution.PURE, List.of("meta::pure::functions::date::firstDayOfQuarter"), Inference.MAPPED, Dialect.BIGQUERY, Dialect.CLICKHOUSE, Dialect.DATABRICKS, Dialect.DB2, Dialect.DUCKDB, Dialect.H2, Dialect.MEMSQL, Dialect.ORACLE, Dialect.POSTGRES, Dialect.PRESTO, Dialect.REDSHIFT, Dialect.SNOWFLAKE, Dialect.SPANNER, Dialect.SPARKSQL, Dialect.SQLSERVER, Dialect.SYBASE, Dialect.SYBASEIQ, Dialect.TRINO),
    FIRST_DAY_OF_THIS_MONTH("firstDayOfThisMonth", Resolution.PURE, List.of("meta::pure::functions::date::firstDayOfThisMonth"), Inference.MAPPED, Dialect.BIGQUERY, Dialect.DATABRICKS, Dialect.DB2, Dialect.DUCKDB, Dialect.H2, Dialect.MEMSQL, Dialect.ORACLE, Dialect.POSTGRES, Dialect.PRESTO, Dialect.REDSHIFT, Dialect.SNOWFLAKE, Dialect.SPANNER, Dialect.SPARKSQL, Dialect.SQLSERVER, Dialect.SYBASE, Dialect.SYBASEIQ, Dialect.TRINO),
    FIRST_DAY_OF_THIS_QUARTER("firstDayOfThisQuarter", Resolution.PURE, List.of("meta::pure::functions::date::firstDayOfThisQuarter"), Inference.MAPPED, Dialect.BIGQUERY, Dialect.DATABRICKS, Dialect.DB2, Dialect.DUCKDB, Dialect.H2, Dialect.MEMSQL, Dialect.ORACLE, Dialect.POSTGRES, Dialect.PRESTO, Dialect.REDSHIFT, Dialect.SNOWFLAKE, Dialect.SPANNER, Dialect.SPARKSQL, Dialect.SQLSERVER, Dialect.SYBASE, Dialect.SYBASEIQ, Dialect.TRINO),
    FIRST_DAY_OF_THIS_YEAR("firstDayOfThisYear", Resolution.PURE, List.of("meta::pure::functions::date::firstDayOfThisYear"), Inference.MAPPED, Dialect.BIGQUERY, Dialect.DATABRICKS, Dialect.DB2, Dialect.DUCKDB, Dialect.H2, Dialect.MEMSQL, Dialect.ORACLE, Dialect.POSTGRES, Dialect.PRESTO, Dialect.REDSHIFT, Dialect.SNOWFLAKE, Dialect.SPANNER, Dialect.SPARKSQL, Dialect.SQLSERVER, Dialect.SYBASE, Dialect.SYBASEIQ, Dialect.TRINO),
    FIRST_DAY_OF_WEEK("firstDayOfWeek", Resolution.PURE, List.of("meta::pure::functions::date::firstDayOfWeek"), Inference.MAPPED, Dialect.BIGQUERY, Dialect.CLICKHOUSE, Dialect.DATABRICKS, Dialect.DUCKDB, Dialect.H2, Dialect.MEMSQL, Dialect.ORACLE, Dialect.POSTGRES, Dialect.PRESTO, Dialect.REDSHIFT, Dialect.SNOWFLAKE, Dialect.SPANNER, Dialect.SPARKSQL, Dialect.SQLSERVER, Dialect.SYBASE, Dialect.SYBASEIQ, Dialect.TRINO),
    FIRST_DAY_OF_YEAR("firstDayOfYear", Resolution.PURE, List.of("meta::pure::functions::date::firstDayOfYear"), Inference.MAPPED, Dialect.BIGQUERY, Dialect.CLICKHOUSE, Dialect.DATABRICKS, Dialect.DB2, Dialect.DUCKDB, Dialect.H2, Dialect.MEMSQL, Dialect.ORACLE, Dialect.POSTGRES, Dialect.PRESTO, Dialect.REDSHIFT, Dialect.SNOWFLAKE, Dialect.SPANNER, Dialect.SPARKSQL, Dialect.SQLSERVER, Dialect.SYBASE, Dialect.SYBASEIQ, Dialect.TRINO),
    FIRST_HOUR_OF_DAY("firstHourOfDay", Resolution.PURE, List.of("meta::pure::functions::date::firstHourOfDay"), Inference.MAPPED, Dialect.BIGQUERY, Dialect.CLICKHOUSE, Dialect.DATABRICKS, Dialect.DUCKDB, Dialect.H2, Dialect.MEMSQL, Dialect.ORACLE, Dialect.POSTGRES, Dialect.PRESTO, Dialect.REDSHIFT, Dialect.SNOWFLAKE, Dialect.SPANNER, Dialect.SPARKSQL, Dialect.SQLSERVER, Dialect.SYBASEIQ, Dialect.TRINO),
    FIRST_MILLISECOND_OF_SECOND("firstMillisecondOfSecond", Resolution.PURE, List.of("meta::pure::functions::date::firstMillisecondOfSecond"), Inference.MAPPED, Dialect.BIGQUERY, Dialect.DATABRICKS, Dialect.DUCKDB, Dialect.H2, Dialect.MEMSQL, Dialect.ORACLE, Dialect.POSTGRES, Dialect.PRESTO, Dialect.REDSHIFT, Dialect.SNOWFLAKE, Dialect.SPANNER, Dialect.SPARKSQL, Dialect.SQLSERVER, Dialect.SYBASEIQ, Dialect.TRINO),
    FIRST_MINUTE_OF_HOUR("firstMinuteOfHour", Resolution.PURE, List.of("meta::pure::functions::date::firstMinuteOfHour"), Inference.MAPPED, Dialect.BIGQUERY, Dialect.CLICKHOUSE, Dialect.DATABRICKS, Dialect.DUCKDB, Dialect.H2, Dialect.MEMSQL, Dialect.ORACLE, Dialect.POSTGRES, Dialect.PRESTO, Dialect.REDSHIFT, Dialect.SNOWFLAKE, Dialect.SPANNER, Dialect.SPARKSQL, Dialect.SQLSERVER, Dialect.SYBASEIQ, Dialect.TRINO),
    FIRST_SECOND_OF_MINUTE("firstSecondOfMinute", Resolution.PURE, List.of("meta::pure::functions::date::firstSecondOfMinute"), Inference.MAPPED, Dialect.BIGQUERY, Dialect.CLICKHOUSE, Dialect.DATABRICKS, Dialect.DUCKDB, Dialect.H2, Dialect.MEMSQL, Dialect.ORACLE, Dialect.POSTGRES, Dialect.PRESTO, Dialect.REDSHIFT, Dialect.SNOWFLAKE, Dialect.SPANNER, Dialect.SPARKSQL, Dialect.SQLSERVER, Dialect.SYBASEIQ, Dialect.TRINO),
    FLOOR("floor", Resolution.PURE, List.of("meta::pure::functions::math::floor"), Inference.MAPPED, Dialect.DEFAULT, Dialect.MEMSQL),
    FORMAT_DATE("formatDate", Resolution.PURE, List.of("meta::pure::functions::date::formatDate"), Inference.NONE, Dialect.BIGQUERY, Dialect.CLICKHOUSE, Dialect.DATABRICKS, Dialect.DUCKDB, Dialect.H2, Dialect.MEMSQL, Dialect.ORACLE, Dialect.POSTGRES, Dialect.PRESTO, Dialect.REDSHIFT, Dialect.SNOWFLAKE, Dialect.SPANNER, Dialect.SPARKSQL, Dialect.SQLSERVER, Dialect.SYBASE, Dialect.SYBASEIQ, Dialect.TRINO),
    GENERATE_GUID("generateGuid", Resolution.PURE, List.of("meta::pure::functions::string::generation::generateGuid"), Inference.NONE, Dialect.CLICKHOUSE, Dialect.DATABRICKS, Dialect.DUCKDB, Dialect.H2, Dialect.MEMSQL, Dialect.POSTGRES, Dialect.SNOWFLAKE, Dialect.SQLSERVER, Dialect.SYBASE, Dialect.SYBASEIQ),
    GREATER_THAN("greaterThan", Resolution.SHIM, List.of(Pure.Lite.GREATER_THAN_ANY), Inference.MAPPED, Dialect.DEFAULT),
    GREATER_THAN_EQUAL("greaterThanEqual", Resolution.SHIM, List.of(Pure.Lite.GREATER_THAN_EQUAL_ANY), Inference.MAPPED, Dialect.DEFAULT),
    GREATEST("greatest", Resolution.PURE, List.of("meta::pure::functions::collection::greatest"), Inference.MAPPED, Dialect.DEFAULT, Dialect.SYBASE, Dialect.SYBASEIQ),
    GROUP("group", Resolution.TRANSLATED, List.of(), Inference.MAPPED, Dialect.DEFAULT),
    HASH_AGG("hashAgg", Resolution.UNSUPPORTED, List.of(), Inference.NONE, Dialect.SNOWFLAKE),
    HASH_CODE("hashCode", Resolution.PURE, List.of("meta::pure::functions::hash::hashCode"), Inference.NONE, Dialect.DATABRICKS, Dialect.DUCKDB, Dialect.SNOWFLAKE),
    HOUR("hour", Resolution.PURE, List.of("meta::pure::functions::date::hour"), Inference.MAPPED, Dialect.BIGQUERY, Dialect.CLICKHOUSE, Dialect.COMPOSITE, Dialect.DATABRICKS, Dialect.DB2, Dialect.DUCKDB, Dialect.H2, Dialect.MEMSQL, Dialect.ORACLE, Dialect.POSTGRES, Dialect.PRESTO, Dialect.REDSHIFT, Dialect.SNOWFLAKE, Dialect.SPANNER, Dialect.SPARKSQL, Dialect.SQLSERVER, Dialect.SYBASE, Dialect.SYBASEIQ, Dialect.TRINO),
    IF("if", Resolution.TRANSLATED, List.of(), Inference.MAPPED, Dialect.DEFAULT),
    IN("in", Resolution.PURE, List.of("meta::pure::functions::collection::in", "meta::pure::functions::relation::in"), Inference.MAPPED, Dialect.DEFAULT, Dialect.SPANNER),
    INDEX_OF("indexOf", Resolution.TRANSLATED, List.of(), Inference.MAPPED, Dialect.BIGQUERY, Dialect.CLICKHOUSE, Dialect.DATABRICKS, Dialect.DUCKDB, Dialect.H2, Dialect.MEMSQL, Dialect.ORACLE, Dialect.POSTGRES, Dialect.PRESTO, Dialect.REDSHIFT, Dialect.SNOWFLAKE, Dialect.SPANNER, Dialect.SPARKSQL, Dialect.SQLSERVER, Dialect.SYBASE, Dialect.SYBASEIQ, Dialect.TRINO),
    IS_ALPHA_NUMERIC("isAlphaNumeric", Resolution.PURE, List.of("meta::pure::functions::string::isAlphaNumeric"), Inference.NONE, Dialect.DATABRICKS, Dialect.DUCKDB, Dialect.H2, Dialect.SNOWFLAKE, Dialect.SPARKSQL, Dialect.SYBASE, Dialect.SYBASEIQ),
    IS_DISTINCT("isDistinct", Resolution.SHIM, List.of(Pure.Lite.IS_DISTINCT_FROM), Inference.NONE, Dialect.DEFAULT),
    IS_EMPTY("isEmpty", Resolution.PURE, List.of("meta::pure::functions::collection::isEmpty"), Inference.MAPPED, Dialect.DEFAULT, Dialect.ORACLE, Dialect.SPARKSQL, Dialect.SQLSERVER, Dialect.SYBASE, Dialect.SYBASEIQ),
    IS_NOT_EMPTY("isNotEmpty", Resolution.PURE, List.of("meta::pure::functions::collection::isNotEmpty"), Inference.MAPPED, Dialect.DEFAULT, Dialect.ORACLE, Dialect.SPANNER, Dialect.SPARKSQL, Dialect.SQLSERVER, Dialect.SYBASE, Dialect.SYBASEIQ),
    IS_NOT_NULL("isNotNull", Resolution.TRANSLATED, List.of(), Inference.MAPPED, Dialect.DEFAULT, Dialect.ORACLE, Dialect.SPARKSQL, Dialect.SQLSERVER, Dialect.SYBASE, Dialect.SYBASEIQ),
    IS_NULL("isNull", Resolution.TRANSLATED, List.of(), Inference.MAPPED, Dialect.DEFAULT, Dialect.ORACLE, Dialect.SPANNER, Dialect.SPARKSQL, Dialect.SQLSERVER, Dialect.SYBASE, Dialect.SYBASEIQ),
    IS_NUMERIC("isNumeric", Resolution.SHIM, List.of(Pure.Lite.IS_NUMERIC), Inference.MAPPED, Dialect.DATABRICKS, Dialect.DUCKDB, Dialect.H2, Dialect.SPARKSQL, Dialect.SYBASE, Dialect.SYBASEIQ),
    JARO_WINKLER_SIMILARITY("jaroWinklerSimilarity", Resolution.PURE, List.of("meta::pure::functions::string::jaroWinklerSimilarity"), Inference.MAPPED, Dialect.CLICKHOUSE, Dialect.DUCKDB, Dialect.H2, Dialect.SNOWFLAKE),
    JOIN_STRINGS("joinStrings", Resolution.PURE, List.of("meta::pure::functions::string::joinStrings", "meta::pure::functions::relation::joinStrings"), Inference.MAPPED, Dialect.BIGQUERY, Dialect.CLICKHOUSE, Dialect.DATABRICKS, Dialect.DB2, Dialect.DUCKDB, Dialect.H2, Dialect.MEMSQL, Dialect.ORACLE, Dialect.POSTGRES, Dialect.REDSHIFT, Dialect.SNOWFLAKE, Dialect.SPANNER, Dialect.SPARKSQL, Dialect.SYBASE, Dialect.SYBASEIQ),
    KEYS("keys", Resolution.PURE, List.of("meta::pure::functions::collection::keys"), Inference.NONE, Dialect.DATABRICKS, Dialect.DUCKDB, Dialect.SNOWFLAKE),
    LAG("lag", Resolution.PURE, List.of("meta::pure::functions::relation::lag"), Inference.NONE, Dialect.CLICKHOUSE, Dialect.DEFAULT),
    LAST("last", Resolution.PURE, List.of("meta::pure::functions::relation::last", "meta::pure::functions::collection::last"), Inference.NONE, Dialect.DEFAULT, Dialect.DUCKDB),
    LEAD("lead", Resolution.PURE, List.of("meta::pure::functions::relation::lead"), Inference.NONE, Dialect.DEFAULT),
    LEAST("least", Resolution.PURE, List.of("meta::pure::functions::collection::least"), Inference.MAPPED, Dialect.DEFAULT, Dialect.SYBASE, Dialect.SYBASEIQ),
    LEFT("left", Resolution.PURE, List.of("meta::pure::functions::string::left"), Inference.MAPPED, Dialect.DB2, Dialect.DEFAULT, Dialect.MEMSQL, Dialect.ORACLE, Dialect.PRESTO, Dialect.SPANNER, Dialect.TRINO),
    LENGTH("length", Resolution.PURE, List.of("meta::pure::functions::string::length"), Inference.MAPPED, Dialect.BIGQUERY, Dialect.CLICKHOUSE, Dialect.COMPOSITE, Dialect.DATABRICKS, Dialect.DB2, Dialect.DUCKDB, Dialect.H2, Dialect.MEMSQL, Dialect.ORACLE, Dialect.POSTGRES, Dialect.PRESTO, Dialect.REDSHIFT, Dialect.SNOWFLAKE, Dialect.SPANNER, Dialect.SPARKSQL, Dialect.SQLSERVER, Dialect.SYBASE, Dialect.SYBASEIQ, Dialect.TRINO),
    LESS_THAN("lessThan", Resolution.SHIM, List.of(Pure.Lite.LESS_THAN_ANY), Inference.MAPPED, Dialect.DEFAULT),
    LESS_THAN_EQUAL("lessThanEqual", Resolution.SHIM, List.of(Pure.Lite.LESS_THAN_EQUAL_ANY), Inference.MAPPED, Dialect.DEFAULT),
    LEVENSHTEIN_DISTANCE("levenshteinDistance", Resolution.PURE, List.of("meta::pure::functions::string::levenshteinDistance"), Inference.MAPPED, Dialect.CLICKHOUSE, Dialect.DATABRICKS, Dialect.DUCKDB, Dialect.H2, Dialect.SNOWFLAKE),
    LOG("log", Resolution.PURE, List.of("meta::pure::functions::math::log"), Inference.MAPPED, Dialect.CLICKHOUSE, Dialect.DEFAULT, Dialect.SPANNER, Dialect.SQLSERVER, Dialect.SYBASE),
    LOG10("log10", Resolution.PURE, List.of("meta::pure::functions::math::log10"), Inference.MAPPED, Dialect.CLICKHOUSE, Dialect.DEFAULT, Dialect.ORACLE, Dialect.POSTGRES, Dialect.REDSHIFT, Dialect.SNOWFLAKE),
    LPAD("lpad", Resolution.PURE, List.of("meta::pure::functions::string::lpad"), Inference.MAPPED, Dialect.DEFAULT, Dialect.DUCKDB, Dialect.MEMSQL, Dialect.SQLSERVER, Dialect.SYBASE, Dialect.TRINO),
    LTRIM("ltrim", Resolution.PURE, List.of("meta::pure::functions::string::ltrim"), Inference.MAPPED, Dialect.DEFAULT),
    MAP_CONCATENATE("mapConcatenate", Resolution.UNSUPPORTED, List.of(), Inference.NONE, Dialect.DATABRICKS, Dialect.DUCKDB, Dialect.SNOWFLAKE),
    MATCHES("matches", Resolution.PURE, List.of("meta::pure::functions::string::matches"), Inference.NONE, Dialect.CLICKHOUSE, Dialect.DATABRICKS, Dialect.DUCKDB, Dialect.H2, Dialect.MEMSQL, Dialect.ORACLE, Dialect.POSTGRES, Dialect.PRESTO, Dialect.SNOWFLAKE, Dialect.SPANNER, Dialect.SPARKSQL, Dialect.SYBASE, Dialect.SYBASEIQ, Dialect.TRINO),
    MAX("max", Resolution.PURE, List.of("meta::pure::functions::date::max", "meta::pure::functions::math::max", "meta::pure::functions::collection::max"), Inference.MAPPED, Dialect.DEFAULT),
    MAX_BY("maxBy", Resolution.PURE, List.of("meta::pure::functions::math::maxBy"), Inference.NONE, Dialect.CLICKHOUSE, Dialect.DATABRICKS, Dialect.DUCKDB, Dialect.SNOWFLAKE),
    MD5("md5", Resolution.TRANSLATED, List.of(), Inference.MAPPED, Dialect.CLICKHOUSE, Dialect.DB2, Dialect.DEFAULT, Dialect.DUCKDB, Dialect.H2, Dialect.SPARKSQL, Dialect.SQLSERVER, Dialect.SYBASE, Dialect.SYBASEIQ, Dialect.TRINO),
    MEDIAN("median", Resolution.PURE, List.of("meta::pure::functions::math::median"), Inference.MAPPED, Dialect.DEFAULT),
    MIN("min", Resolution.PURE, List.of("meta::pure::functions::date::min", "meta::pure::functions::math::min", "meta::pure::functions::collection::min"), Inference.MAPPED, Dialect.DEFAULT),
    MIN_BY("minBy", Resolution.PURE, List.of("meta::pure::functions::math::minBy"), Inference.NONE, Dialect.CLICKHOUSE, Dialect.DATABRICKS, Dialect.DUCKDB, Dialect.SNOWFLAKE),
    MINUS("minus", Resolution.PURE, List.of("meta::pure::functions::math::minus"), Inference.MAPPED, Dialect.DEFAULT),
    MINUTE("minute", Resolution.PURE, List.of("meta::pure::functions::date::minute"), Inference.MAPPED, Dialect.BIGQUERY, Dialect.CLICKHOUSE, Dialect.DATABRICKS, Dialect.DB2, Dialect.DUCKDB, Dialect.H2, Dialect.MEMSQL, Dialect.ORACLE, Dialect.POSTGRES, Dialect.PRESTO, Dialect.REDSHIFT, Dialect.SNOWFLAKE, Dialect.SPANNER, Dialect.SPARKSQL, Dialect.SQLSERVER, Dialect.SYBASE, Dialect.SYBASEIQ, Dialect.TRINO),
    MOD("mod", Resolution.PURE, List.of("meta::pure::functions::math::mod"), Inference.MAPPED, Dialect.DEFAULT, Dialect.DUCKDB, Dialect.SQLSERVER, Dialect.SYBASE),
    MODE("mode", Resolution.PURE, List.of("meta::pure::functions::math::mode"), Inference.MAPPED, Dialect.CLICKHOUSE, Dialect.DEFAULT),
    MONTH("month", Resolution.PURE, List.of("meta::pure::functions::date::month"), Inference.MAPPED, Dialect.BIGQUERY, Dialect.CLICKHOUSE, Dialect.DATABRICKS, Dialect.DB2, Dialect.DUCKDB, Dialect.H2, Dialect.MEMSQL, Dialect.ORACLE, Dialect.POSTGRES, Dialect.PRESTO, Dialect.REDSHIFT, Dialect.SNOWFLAKE, Dialect.SPANNER, Dialect.SPARKSQL, Dialect.SQLSERVER, Dialect.SYBASE, Dialect.SYBASEIQ, Dialect.TRINO),
    MONTH_NAME("monthName", Resolution.UNSUPPORTED, List.of(), Inference.MAPPED, Dialect.BIGQUERY, Dialect.CLICKHOUSE, Dialect.DATABRICKS, Dialect.DB2, Dialect.DUCKDB, Dialect.H2, Dialect.MEMSQL, Dialect.ORACLE, Dialect.POSTGRES, Dialect.PRESTO, Dialect.REDSHIFT, Dialect.SNOWFLAKE, Dialect.SQLSERVER, Dialect.SYBASE, Dialect.SYBASEIQ, Dialect.TRINO),
    MONTH_NUMBER("monthNumber", Resolution.PURE, List.of("meta::pure::functions::date::monthNumber"), Inference.MAPPED, Dialect.BIGQUERY, Dialect.CLICKHOUSE, Dialect.DATABRICKS, Dialect.DB2, Dialect.DUCKDB, Dialect.H2, Dialect.MEMSQL, Dialect.ORACLE, Dialect.POSTGRES, Dialect.PRESTO, Dialect.REDSHIFT, Dialect.SNOWFLAKE, Dialect.SPANNER, Dialect.SPARKSQL, Dialect.SQLSERVER, Dialect.SYBASE, Dialect.SYBASEIQ, Dialect.TRINO),
    MOST_RECENT_DAY_OF_WEEK("mostRecentDayOfWeek", Resolution.PURE, List.of("meta::pure::functions::date::mostRecentDayOfWeek"), Inference.MAPPED, Dialect.DATABRICKS, Dialect.DB2, Dialect.DUCKDB, Dialect.H2, Dialect.MEMSQL, Dialect.ORACLE, Dialect.POSTGRES, Dialect.PRESTO, Dialect.SNOWFLAKE, Dialect.SPARKSQL, Dialect.SYBASE, Dialect.SYBASEIQ, Dialect.TRINO),
    NOT("not", Resolution.PURE, List.of("meta::pure::functions::boolean::not"), Inference.MAPPED),
    NOT_EQUAL("notEqual", Resolution.UNSUPPORTED, List.of(), Inference.MAPPED, Dialect.DEFAULT),
    NOT_EQUAL_ANSI("notEqualAnsi", Resolution.SHIM, List.of(Pure.Lite.NOT_EQUAL_ANSI), Inference.MAPPED, Dialect.DEFAULT),
    NOW("now", Resolution.PURE, List.of("meta::pure::functions::date::now"), Inference.NONE, Dialect.BIGQUERY, Dialect.DATABRICKS, Dialect.DB2, Dialect.DUCKDB, Dialect.H2, Dialect.MEMSQL, Dialect.ORACLE, Dialect.POSTGRES, Dialect.PRESTO, Dialect.REDSHIFT, Dialect.SNOWFLAKE, Dialect.SPANNER, Dialect.SPARKSQL, Dialect.SQLSERVER, Dialect.SYBASE, Dialect.SYBASEIQ, Dialect.TRINO),
    NTH("nth", Resolution.PURE, List.of("meta::pure::functions::relation::nth"), Inference.NONE, Dialect.DEFAULT),
    NTILE("ntile", Resolution.PURE, List.of("meta::pure::functions::relation::ntile"), Inference.NONE, Dialect.DEFAULT),
    NULL_SAFE_EQUAL("nullSafeEqual", Resolution.UNSUPPORTED, List.of(), Inference.MAPPED, Dialect.BIGQUERY, Dialect.CLICKHOUSE, Dialect.DATABRICKS, Dialect.DEFAULT, Dialect.DUCKDB, Dialect.H2, Dialect.MEMSQL, Dialect.POSTGRES, Dialect.PRESTO, Dialect.SNOWFLAKE, Dialect.SPANNER, Dialect.SPARKSQL, Dialect.TRINO),
    NULL_SAFE_NOT_EQUAL("nullSafeNotEqual", Resolution.UNSUPPORTED, List.of(), Inference.MAPPED, Dialect.BIGQUERY, Dialect.CLICKHOUSE, Dialect.DATABRICKS, Dialect.DEFAULT, Dialect.DUCKDB, Dialect.H2, Dialect.MEMSQL, Dialect.POSTGRES, Dialect.PRESTO, Dialect.SNOWFLAKE, Dialect.SPANNER, Dialect.SPARKSQL, Dialect.TRINO),
    OBJECT_REFERENCE_IN("objectReferenceIn", Resolution.PURE, List.of("meta::pure::functions::collection::objectReferenceIn"), Inference.NONE, Dialect.DEFAULT),
    OR("or", Resolution.PURE, List.of("meta::pure::functions::boolean::or", "meta::pure::functions::collection::or"), Inference.MAPPED, Dialect.DEFAULT, Dialect.SPANNER),
    PARSE_BOOLEAN("parseBoolean", Resolution.PURE, List.of("meta::pure::functions::string::parseBoolean"), Inference.NONE, Dialect.CLICKHOUSE, Dialect.DATABRICKS, Dialect.SNOWFLAKE),
    PARSE_DATE("parseDate", Resolution.PURE, List.of("meta::pure::functions::string::parseDate"), Inference.MAPPED, Dialect.DATABRICKS, Dialect.DUCKDB, Dialect.H2, Dialect.PRESTO, Dialect.SNOWFLAKE, Dialect.SPARKSQL, Dialect.SYBASE, Dialect.SYBASEIQ, Dialect.TRINO),
    PARSE_DECIMAL("parseDecimal", Resolution.PURE, List.of("meta::pure::functions::string::parseDecimal"), Inference.MAPPED, Dialect.BIGQUERY, Dialect.CLICKHOUSE, Dialect.DATABRICKS, Dialect.DUCKDB, Dialect.H2, Dialect.MEMSQL, Dialect.ORACLE, Dialect.POSTGRES, Dialect.REDSHIFT, Dialect.SNOWFLAKE, Dialect.SPANNER, Dialect.SPARKSQL, Dialect.SYBASE, Dialect.SYBASEIQ),
    PARSE_FLOAT("parseFloat", Resolution.PURE, List.of("meta::pure::functions::string::parseFloat"), Inference.MAPPED, Dialect.BIGQUERY, Dialect.CLICKHOUSE, Dialect.COMPOSITE, Dialect.DATABRICKS, Dialect.DB2, Dialect.DUCKDB, Dialect.H2, Dialect.MEMSQL, Dialect.ORACLE, Dialect.POSTGRES, Dialect.PRESTO, Dialect.REDSHIFT, Dialect.SNOWFLAKE, Dialect.SPANNER, Dialect.SPARKSQL, Dialect.SQLSERVER, Dialect.SYBASE, Dialect.SYBASEIQ, Dialect.TRINO),
    PARSE_INTEGER("parseInteger", Resolution.PURE, List.of("meta::pure::functions::string::parseInteger"), Inference.MAPPED, Dialect.BIGQUERY, Dialect.CLICKHOUSE, Dialect.COMPOSITE, Dialect.DATABRICKS, Dialect.DB2, Dialect.DUCKDB, Dialect.H2, Dialect.MEMSQL, Dialect.ORACLE, Dialect.POSTGRES, Dialect.PRESTO, Dialect.REDSHIFT, Dialect.SNOWFLAKE, Dialect.SPANNER, Dialect.SPARKSQL, Dialect.SQLSERVER, Dialect.SYBASE, Dialect.SYBASEIQ, Dialect.TRINO),
    PARSE_JSON("parseJson", Resolution.UNSUPPORTED, List.of(), Inference.MAPPED, Dialect.DATABRICKS, Dialect.DUCKDB, Dialect.H2, Dialect.ORACLE, Dialect.POSTGRES, Dialect.SNOWFLAKE),
    PERCENT_RANK("percentRank", Resolution.PURE, List.of("meta::pure::functions::relation::percentRank"), Inference.NONE, Dialect.DEFAULT),
    PERCENTILE("percentile", Resolution.PURE, List.of("meta::pure::functions::math::percentile"), Inference.NONE, Dialect.CLICKHOUSE, Dialect.DEFAULT),
    PLUS("plus", Resolution.PURE, List.of("meta::pure::functions::math::plus", "meta::pure::functions::string::plus"), Inference.MAPPED, Dialect.DEFAULT, Dialect.SPANNER),
    POSITION("position", Resolution.TRANSLATED, List.of(), Inference.NONE, Dialect.BIGQUERY, Dialect.COMPOSITE, Dialect.DATABRICKS, Dialect.DB2, Dialect.DUCKDB, Dialect.H2, Dialect.MEMSQL, Dialect.ORACLE, Dialect.POSTGRES, Dialect.PRESTO, Dialect.REDSHIFT, Dialect.SNOWFLAKE, Dialect.SPANNER, Dialect.SPARKSQL, Dialect.SQLSERVER, Dialect.SYBASE, Dialect.SYBASEIQ, Dialect.TRINO),
    POW("pow", Resolution.PURE, List.of("meta::pure::functions::math::pow"), Inference.NONE, Dialect.DEFAULT, Dialect.SPANNER),
    PREVIOUS_DAY_OF_WEEK("previousDayOfWeek", Resolution.PURE, List.of("meta::pure::functions::date::previousDayOfWeek"), Inference.MAPPED, Dialect.DATABRICKS, Dialect.DB2, Dialect.DUCKDB, Dialect.H2, Dialect.MEMSQL, Dialect.ORACLE, Dialect.POSTGRES, Dialect.PRESTO, Dialect.SNOWFLAKE, Dialect.SPARKSQL, Dialect.SYBASE, Dialect.SYBASEIQ, Dialect.TRINO),
    QUARTER("quarter", Resolution.PURE, List.of("meta::pure::functions::date::quarter"), Inference.MAPPED, Dialect.DATABRICKS, Dialect.DB2, Dialect.DUCKDB, Dialect.H2, Dialect.MEMSQL, Dialect.ORACLE, Dialect.POSTGRES, Dialect.PRESTO, Dialect.SNOWFLAKE, Dialect.SPANNER, Dialect.SPARKSQL, Dialect.SQLSERVER, Dialect.SYBASE, Dialect.SYBASEIQ, Dialect.TRINO),
    QUARTER_NUMBER("quarterNumber", Resolution.PURE, List.of("meta::pure::functions::date::quarterNumber"), Inference.MAPPED, Dialect.BIGQUERY, Dialect.DATABRICKS, Dialect.DB2, Dialect.DUCKDB, Dialect.H2, Dialect.MEMSQL, Dialect.ORACLE, Dialect.POSTGRES, Dialect.PRESTO, Dialect.REDSHIFT, Dialect.SNOWFLAKE, Dialect.SPANNER, Dialect.SPARKSQL, Dialect.SQLSERVER, Dialect.SYBASE, Dialect.SYBASEIQ, Dialect.TRINO),
    RANGE("range", Resolution.PURE, List.of("meta::pure::functions::collection::range"), Inference.NONE, Dialect.CLICKHOUSE, Dialect.DATABRICKS, Dialect.DUCKDB, Dialect.SNOWFLAKE),
    RANK("rank", Resolution.PURE, List.of("meta::pure::functions::relation::rank"), Inference.MAPPED, Dialect.DEFAULT),
    REGEXP_COUNT("regexpCount", Resolution.PURE, List.of("meta::pure::functions::string::regexpCount"), Inference.NONE, Dialect.DATABRICKS, Dialect.DUCKDB, Dialect.POSTGRES, Dialect.SNOWFLAKE, Dialect.SPANNER, Dialect.TRINO),
    REGEXP_EXTRACT("regexpExtract", Resolution.PURE, List.of("meta::pure::functions::string::regexpExtract"), Inference.NONE, Dialect.DATABRICKS, Dialect.DUCKDB, Dialect.MEMSQL, Dialect.POSTGRES, Dialect.SNOWFLAKE, Dialect.SPANNER, Dialect.TRINO),
    REGEXP_INDEX_OF("regexpIndexOf", Resolution.PURE, List.of("meta::pure::functions::string::regexpIndexOf"), Inference.NONE, Dialect.DATABRICKS, Dialect.DUCKDB, Dialect.MEMSQL, Dialect.POSTGRES, Dialect.SNOWFLAKE, Dialect.SPANNER, Dialect.TRINO),
    REGEXP_LIKE("regexpLike", Resolution.PURE, List.of("meta::pure::functions::string::regexpLike"), Inference.NONE, Dialect.DATABRICKS, Dialect.DUCKDB, Dialect.MEMSQL, Dialect.POSTGRES, Dialect.SNOWFLAKE, Dialect.SPANNER, Dialect.TRINO),
    REGEXP_REPLACE("regexpReplace", Resolution.PURE, List.of("meta::pure::functions::string::regexpReplace"), Inference.NONE, Dialect.DATABRICKS, Dialect.DUCKDB, Dialect.MEMSQL, Dialect.POSTGRES, Dialect.SNOWFLAKE, Dialect.SPANNER, Dialect.TRINO),
    REM("rem", Resolution.PURE, List.of("meta::pure::functions::math::rem"), Inference.NONE, Dialect.DEFAULT, Dialect.MEMSQL, Dialect.SYBASE),
    REPEAT_STRING("repeatString", Resolution.PURE, List.of("meta::pure::functions::string::repeatString"), Inference.NONE, Dialect.DEFAULT, Dialect.MEMSQL, Dialect.SQLSERVER, Dialect.SYBASE, Dialect.TRINO),
    REPLACE("replace", Resolution.PURE, List.of("meta::pure::functions::string::replace"), Inference.MAPPED, Dialect.DEFAULT),
    REVERSE("reverse", Resolution.UNSUPPORTED, List.of(), Inference.MAPPED),
    REVERSE_STRING("reverseString", Resolution.PURE, List.of("meta::pure::functions::string::reverseString"), Inference.NONE, Dialect.DEFAULT, Dialect.DUCKDB, Dialect.H2),
    RIGHT("right", Resolution.PURE, List.of("meta::pure::functions::string::right"), Inference.MAPPED, Dialect.DB2, Dialect.DEFAULT, Dialect.MEMSQL, Dialect.ORACLE, Dialect.PRESTO, Dialect.SPANNER, Dialect.TRINO),
    ROUND("round", Resolution.PURE, List.of("meta::pure::functions::math::round"), Inference.MAPPED, Dialect.BIGQUERY, Dialect.CLICKHOUSE, Dialect.COMPOSITE, Dialect.DATABRICKS, Dialect.DB2, Dialect.DEFAULT, Dialect.DUCKDB, Dialect.H2, Dialect.MEMSQL, Dialect.ORACLE, Dialect.POSTGRES, Dialect.PRESTO, Dialect.REDSHIFT, Dialect.SNOWFLAKE, Dialect.SPANNER, Dialect.SPARKSQL, Dialect.SQLSERVER, Dialect.SYBASE, Dialect.SYBASEIQ, Dialect.TRINO),
    ROW_NUMBER("rowNumber", Resolution.PURE, List.of("meta::pure::functions::relation::rowNumber"), Inference.MAPPED, Dialect.DEFAULT),
    RPAD("rpad", Resolution.PURE, List.of("meta::pure::functions::string::rpad"), Inference.MAPPED, Dialect.DEFAULT, Dialect.DUCKDB, Dialect.MEMSQL, Dialect.SQLSERVER, Dialect.SYBASE, Dialect.TRINO),
    RTRIM("rtrim", Resolution.PURE, List.of("meta::pure::functions::string::rtrim"), Inference.MAPPED, Dialect.DEFAULT, Dialect.MEMSQL),
    SECOND("second", Resolution.PURE, List.of("meta::pure::functions::date::second"), Inference.MAPPED, Dialect.BIGQUERY, Dialect.CLICKHOUSE, Dialect.DATABRICKS, Dialect.DB2, Dialect.DUCKDB, Dialect.H2, Dialect.MEMSQL, Dialect.ORACLE, Dialect.POSTGRES, Dialect.PRESTO, Dialect.REDSHIFT, Dialect.SNOWFLAKE, Dialect.SPANNER, Dialect.SPARKSQL, Dialect.SQLSERVER, Dialect.SYBASE, Dialect.SYBASEIQ, Dialect.TRINO),
    SHA1("sha1", Resolution.TRANSLATED, List.of(), Inference.MAPPED, Dialect.CLICKHOUSE, Dialect.DB2, Dialect.DEFAULT, Dialect.DUCKDB, Dialect.H2, Dialect.ORACLE, Dialect.POSTGRES, Dialect.SPARKSQL, Dialect.SQLSERVER, Dialect.SYBASE, Dialect.SYBASEIQ, Dialect.TRINO),
    SHA256("sha256", Resolution.TRANSLATED, List.of(), Inference.MAPPED, Dialect.CLICKHOUSE, Dialect.DATABRICKS, Dialect.DB2, Dialect.DEFAULT, Dialect.DUCKDB, Dialect.H2, Dialect.MEMSQL, Dialect.ORACLE, Dialect.POSTGRES, Dialect.REDSHIFT, Dialect.SNOWFLAKE, Dialect.SPARKSQL, Dialect.SQLSERVER, Dialect.SYBASE, Dialect.SYBASEIQ, Dialect.TRINO),
    SIGN("sign", Resolution.PURE, List.of("meta::pure::functions::math::sign"), Inference.MAPPED, Dialect.DEFAULT),
    SIN("sin", Resolution.PURE, List.of("meta::pure::functions::math::sin"), Inference.MAPPED, Dialect.DEFAULT),
    SINH("sinh", Resolution.PURE, List.of("meta::pure::functions::math::sinh"), Inference.MAPPED, Dialect.DEFAULT),
    SIZE("size", Resolution.PURE, List.of("meta::pure::functions::relation::size", "meta::pure::functions::collection::size"), Inference.MAPPED, Dialect.DEFAULT, Dialect.SPANNER),
    SPLIT("split", Resolution.UNSUPPORTED, List.of(), Inference.NONE, Dialect.BIGQUERY, Dialect.CLICKHOUSE, Dialect.DATABRICKS, Dialect.DUCKDB, Dialect.H2, Dialect.POSTGRES, Dialect.SNOWFLAKE),
    SPLIT_PART("splitPart", Resolution.TRANSLATED, List.of(), Inference.NONE, Dialect.CLICKHOUSE, Dialect.DEFAULT, Dialect.DUCKDB, Dialect.H2, Dialect.SNOWFLAKE, Dialect.SPANNER, Dialect.SQLSERVER),
    SQL_FALSE("sqlFalse", Resolution.PURE, List.of("meta::relational::functions::sqlQueryToString::sqlFalse"), Inference.MAPPED, Dialect.DATABRICKS, Dialect.DEFAULT, Dialect.SPANNER),
    SQL_NULL("sqlNull", Resolution.PURE, List.of("meta::relational::functions::sqlQueryToString::sqlNull"), Inference.MAPPED, Dialect.DEFAULT, Dialect.SPANNER),
    SQL_TRUE("sqlTrue", Resolution.PURE, List.of("meta::relational::functions::sqlQueryToString::sqlTrue"), Inference.MAPPED, Dialect.DATABRICKS, Dialect.DEFAULT, Dialect.SPANNER),
    SQRT("sqrt", Resolution.PURE, List.of("meta::pure::functions::math::sqrt"), Inference.MAPPED, Dialect.DEFAULT),
    STARTS_WITH("startsWith", Resolution.PURE, List.of("meta::pure::functions::string::startsWith"), Inference.MAPPED, Dialect.DEFAULT, Dialect.SNOWFLAKE, Dialect.SPANNER, Dialect.SPARKSQL, Dialect.SYBASEIQ),
    STD_DEV_POPULATION("stdDevPopulation", Resolution.PURE, List.of("meta::pure::functions::math::stdDevPopulation"), Inference.MAPPED, Dialect.BIGQUERY, Dialect.CLICKHOUSE, Dialect.DATABRICKS, Dialect.DB2, Dialect.DUCKDB, Dialect.H2, Dialect.MEMSQL, Dialect.ORACLE, Dialect.POSTGRES, Dialect.PRESTO, Dialect.REDSHIFT, Dialect.SNOWFLAKE, Dialect.SPANNER, Dialect.SPARKSQL, Dialect.SQLSERVER, Dialect.SYBASE, Dialect.SYBASEIQ, Dialect.TRINO),
    STD_DEV_SAMPLE("stdDevSample", Resolution.PURE, List.of("meta::pure::functions::math::stdDevSample"), Inference.MAPPED, Dialect.BIGQUERY, Dialect.CLICKHOUSE, Dialect.DATABRICKS, Dialect.DB2, Dialect.DUCKDB, Dialect.H2, Dialect.MEMSQL, Dialect.ORACLE, Dialect.POSTGRES, Dialect.PRESTO, Dialect.REDSHIFT, Dialect.SNOWFLAKE, Dialect.SPANNER, Dialect.SPARKSQL, Dialect.SQLSERVER, Dialect.SYBASE, Dialect.SYBASEIQ, Dialect.TRINO),
    SUB("sub", Resolution.TRANSLATED, List.of(), Inference.MAPPED, Dialect.DEFAULT),
    SUBSTRING("substring", Resolution.TRANSLATED, List.of(), Inference.MAPPED, Dialect.BIGQUERY, Dialect.CLICKHOUSE, Dialect.COMPOSITE, Dialect.DATABRICKS, Dialect.DB2, Dialect.DUCKDB, Dialect.H2, Dialect.MEMSQL, Dialect.ORACLE, Dialect.POSTGRES, Dialect.PRESTO, Dialect.REDSHIFT, Dialect.SNOWFLAKE, Dialect.SPANNER, Dialect.SPARKSQL, Dialect.SQLSERVER, Dialect.SYBASE, Dialect.SYBASEIQ, Dialect.TRINO),
    SUM("sum", Resolution.PURE, List.of("meta::pure::functions::math::sum"), Inference.MAPPED, Dialect.DEFAULT),
    TAN("tan", Resolution.PURE, List.of("meta::pure::functions::math::tan"), Inference.MAPPED, Dialect.DEFAULT),
    TANH("tanh", Resolution.PURE, List.of("meta::pure::functions::math::tanh"), Inference.MAPPED, Dialect.CLICKHOUSE, Dialect.DEFAULT),
    TIME_BUCKET("timeBucket", Resolution.PURE, List.of("meta::pure::functions::date::timeBucket"), Inference.MAPPED, Dialect.CLICKHOUSE, Dialect.DUCKDB, Dialect.SNOWFLAKE),
    TIMES("times", Resolution.PURE, List.of("meta::pure::functions::math::times"), Inference.MAPPED, Dialect.DEFAULT),
    TO_DECIMAL("toDecimal", Resolution.PURE, List.of("meta::pure::functions::math::toDecimal"), Inference.NONE, Dialect.CLICKHOUSE, Dialect.DATABRICKS, Dialect.DUCKDB, Dialect.H2, Dialect.ORACLE, Dialect.POSTGRES, Dialect.PRESTO, Dialect.REDSHIFT, Dialect.SNOWFLAKE, Dialect.SPARKSQL, Dialect.SQLSERVER, Dialect.SYBASE, Dialect.SYBASEIQ),
    TO_FLOAT("toFloat", Resolution.PURE, List.of("meta::pure::functions::math::toFloat"), Inference.NONE, Dialect.BIGQUERY, Dialect.CLICKHOUSE, Dialect.DATABRICKS, Dialect.DUCKDB, Dialect.H2, Dialect.ORACLE, Dialect.POSTGRES, Dialect.PRESTO, Dialect.REDSHIFT, Dialect.SNOWFLAKE, Dialect.SPARKSQL, Dialect.SQLSERVER, Dialect.SYBASE, Dialect.SYBASEIQ),
    TO_JSON("toJson", Resolution.UNSUPPORTED, List.of(), Inference.MAPPED, Dialect.DATABRICKS, Dialect.DUCKDB, Dialect.POSTGRES, Dialect.SNOWFLAKE),
    TO_LOWER("toLower", Resolution.PURE, List.of("meta::pure::functions::string::toLower"), Inference.MAPPED, Dialect.DEFAULT),
    TO_ONE("toOne", Resolution.PURE, List.of("meta::pure::functions::multiplicity::toOne"), Inference.MAPPED, Dialect.DEFAULT, Dialect.SPANNER),
    TO_STRING("toString", Resolution.PURE, List.of("meta::pure::functions::relation::toString", "meta::pure::functions::string::toString"), Inference.MAPPED, Dialect.BIGQUERY, Dialect.CLICKHOUSE, Dialect.DATABRICKS, Dialect.DB2, Dialect.DUCKDB, Dialect.H2, Dialect.MEMSQL, Dialect.ORACLE, Dialect.POSTGRES, Dialect.PRESTO, Dialect.REDSHIFT, Dialect.SNOWFLAKE, Dialect.SPANNER, Dialect.SPARKSQL, Dialect.SQLSERVER, Dialect.SYBASE, Dialect.SYBASEIQ, Dialect.TRINO),
    TO_TIMESTAMP("toTimestamp", Resolution.TRANSLATED, List.of(), Inference.NONE, Dialect.DUCKDB, Dialect.H2, Dialect.MEMSQL, Dialect.ORACLE, Dialect.POSTGRES, Dialect.PRESTO, Dialect.SNOWFLAKE, Dialect.SPARKSQL, Dialect.SYBASE, Dialect.SYBASEIQ, Dialect.TRINO),
    TO_UPPER("toUpper", Resolution.PURE, List.of("meta::pure::functions::string::toUpper"), Inference.MAPPED, Dialect.DEFAULT),
    TO_VARIANT("toVariant", Resolution.PURE, List.of("meta::pure::functions::variant::convert::toVariant"), Inference.MAPPED, Dialect.DATABRICKS, Dialect.DUCKDB, Dialect.POSTGRES, Dialect.SNOWFLAKE),
    TO_VARIANT_LIST("toVariantList", Resolution.UNSUPPORTED, List.of(), Inference.NONE, Dialect.CLICKHOUSE, Dialect.DATABRICKS, Dialect.DUCKDB, Dialect.POSTGRES, Dialect.SNOWFLAKE),
    TO_VARIANT_OBJECT("toVariantObject", Resolution.UNSUPPORTED, List.of(), Inference.NONE, Dialect.DATABRICKS, Dialect.DUCKDB, Dialect.POSTGRES, Dialect.SNOWFLAKE),
    TODAY("today", Resolution.PURE, List.of("meta::pure::functions::date::today"), Inference.MAPPED, Dialect.BIGQUERY, Dialect.CLICKHOUSE, Dialect.DATABRICKS, Dialect.DB2, Dialect.DUCKDB, Dialect.H2, Dialect.MEMSQL, Dialect.ORACLE, Dialect.POSTGRES, Dialect.PRESTO, Dialect.REDSHIFT, Dialect.SNOWFLAKE, Dialect.SPANNER, Dialect.SPARKSQL, Dialect.SQLSERVER, Dialect.SYBASE, Dialect.SYBASEIQ, Dialect.TRINO),
    TRIM("trim", Resolution.PURE, List.of("meta::pure::functions::string::trim"), Inference.MAPPED, Dialect.DEFAULT, Dialect.SYBASE),
    VALUES("values", Resolution.PURE, List.of("meta::pure::functions::collection::values"), Inference.NONE, Dialect.DATABRICKS, Dialect.DUCKDB, Dialect.SNOWFLAKE),
    VARIANCE("variance", Resolution.PURE, List.of("meta::pure::functions::math::variance"), Inference.NONE, Dialect.CLICKHOUSE, Dialect.DEFAULT, Dialect.DUCKDB),
    VARIANCE_POPULATION("variancePopulation", Resolution.PURE, List.of("meta::pure::functions::math::variancePopulation"), Inference.MAPPED, Dialect.CLICKHOUSE, Dialect.DEFAULT, Dialect.DUCKDB, Dialect.SQLSERVER),
    VARIANCE_SAMPLE("varianceSample", Resolution.PURE, List.of("meta::pure::functions::math::varianceSample"), Inference.MAPPED, Dialect.CLICKHOUSE, Dialect.DEFAULT, Dialect.DUCKDB, Dialect.SQLSERVER),
    VARIANT_TO("variantTo", Resolution.UNSUPPORTED, List.of(), Inference.NONE, Dialect.DEFAULT, Dialect.DUCKDB, Dialect.POSTGRES),
    WEEK_OF_YEAR("weekOfYear", Resolution.PURE, List.of("meta::pure::functions::date::weekOfYear"), Inference.MAPPED, Dialect.BIGQUERY, Dialect.CLICKHOUSE, Dialect.DATABRICKS, Dialect.DB2, Dialect.DUCKDB, Dialect.H2, Dialect.MEMSQL, Dialect.ORACLE, Dialect.POSTGRES, Dialect.PRESTO, Dialect.REDSHIFT, Dialect.SNOWFLAKE, Dialect.SPANNER, Dialect.SPARKSQL, Dialect.SQLSERVER, Dialect.SYBASE, Dialect.SYBASEIQ, Dialect.TRINO),
    YEAR("year", Resolution.PURE, List.of("meta::pure::functions::date::year"), Inference.MAPPED, Dialect.BIGQUERY, Dialect.CLICKHOUSE, Dialect.DATABRICKS, Dialect.DB2, Dialect.DUCKDB, Dialect.H2, Dialect.MEMSQL, Dialect.ORACLE, Dialect.POSTGRES, Dialect.PRESTO, Dialect.REDSHIFT, Dialect.SNOWFLAKE, Dialect.SPANNER, Dialect.SPARKSQL, Dialect.SQLSERVER, Dialect.SYBASE, Dialect.SYBASEIQ, Dialect.TRINO);

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
    private final List<String> fqns;
    private final Inference inference;
    private final EnumSet<Dialect> dialects;

    DynaFn(String name, Resolution resolution, List<String> fqns, Inference inference, Dialect... dialects) {
        this.name = name;
        this.resolution = resolution;
        this.fqns = List.copyOf(fqns);
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

    /** THE DECLARATIONS this name resolves to: a PURE name's catalog FQNs (every
     *  package the platform declares the bare name in — generated from the
     *  catalog, verified by {@code DynaFnRegistryTest}), a SHIM's one
     *  {@link Pure.Lite} FQN; empty for TRANSLATED and UNSUPPORTED. The
     *  translator mints a PURE call carrying these as its candidates, so the
     *  typer never resolves a dynafunction by a bare spelling (untangle 4b.2). */
    public List<String> fqns() {
        return fqns;
    }

    /** The {@link Pure.Lite} identity a SHIM resolves to. */
    public String liteFqn() {
        if (resolution != Resolution.SHIM || fqns.size() != 1) {
            throw new IllegalStateException(name + " is not a SHIM");
        }
        return fqns.get(0);
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
