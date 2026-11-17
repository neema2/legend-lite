// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;
import com.legend.sql.dialect.SqlDialect;
import com.tngtech.archunit.core.domain.JavaClass;
import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.domain.JavaCodeUnit;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * EVERY SQL FUNCTION OF THE MIR SORTED, PRODUCT OR EDGE-ONLY (the GUARD stream, step 2's C.13; the edge audit of
 * 2026-11-17, section C.13: "one exact map from SqlFn.values() to PRODUCT or EDGE_ONLY"). The census counts a product
 * class naming an edge-only {@link SqlFn} ({@link EdgeSeparationTest#EDGE_ONLY_SQLFN}), and
 * {@code EdgeOnlyNodesCensusTest} holds the product's dialects refusing each by name: both read a list a new function
 * never joined -- an upstream node added to the MIR under a new name was counted nowhere. Here every constant of
 * {@code SqlFn.values()} is sorted, exactly: a function added fails until it is sorted, and the two lists the other
 * guards read are held equal to this map's EDGE_ONLY half (one source, a rename or a second list caught).
 *
 * <p>The sort is MEASURED (the review of C.12 and C.13, 2026-11-17): each function's writers are read from the product's,
 * the hosts' and the edge's jars -- a class naming its constant, the renderers, the typing rules, the codec and javac's
 * switch maps aside -- and a function the edge alone writes must be EDGE_ONLY, one the product writes PRODUCT, and one no
 * class writes is pinned ({@link #NO_WRITER}). The expression nodes are sorted the same way by who constructs them
 * ({@link #EDGE_ONLY_NODES}): a node the edge alone makes is named here, its product side a decoder or a rebuild.
 */
@Tag("guardrail")
class SqlFnSortedTest {

    enum Kind { PRODUCT, EDGE_ONLY }

    /** The edge-only functions: written by the edge alone, each refused by the product's dialects by name. */
    static final Set<String> EDGE_ONLY = Set.of(
            // legend-pure's DateTime print form, the edge's composition (census B12; C5 step C)
            "DATETIME_PRINT");

    /** Every other function, the product's: 188, sorted 2026-11-17 by the measured list (a new one is sorted here). */
    static final Set<String> PRODUCT = Set.of(
            "ABS", "ACOS", "ADD_INTERVAL", "ADD_INTERVAL_TEMPORAL", "ALL_DISTINCT", "AND", "ASCII_CODE", "ASIN",
            "ATAN", "ATAN2", "BINARY_COLLATE", "BIT_AND", "BIT_NOT", "BIT_OR", "BIT_SHIFT_LEFT", "BIT_SHIFT_RIGHT",
            "BIT_XOR", "BOOL_TO_TEXT", "CBRT", "CEILING", "CELL_TEXT", "CHR", "COALESCE", "CONCAT", "CONCAT_JOIN",
            "CONCAT_WS", "COS", "COSH", "COT", "CURRENT_USER_FN", "DATETIME_CANON_TEXT", "DATETIME_TO_TEXT",
            "DATE_AT_TIME", "DATE_DIFF", "DATE_TRUNC", "DATE_TRUNC_DAY", "DAYNAME", "DECIMAL_TO_TEXT",
            "DECODE_BASE64", "DEGREES", "DIVIDE", "ENCODE_BASE64", "ENDS_WITH", "EPOCH_MS", "EPOCH_NS",
            "EPOCH_SECONDS", "EQUAL", "ERROR", "EXP", "EXTRACT", "FLOAT_SHORTEST_TEXT", "FLOAT_TO_TEXT", "FLOOR",
            "FLOOR_RAW", "FORMAT", "FROM_EPOCH_MS", "FROM_EPOCH_NS", "FROM_EPOCH_SECONDS", "GREATER", "GREATER_EQUAL",
            "GREATEST", "GUID", "HASH", "IN", "INT_DIVIDE", "IS_DISTINCT_FROM", "IS_NOT_NULL", "IS_NULL",
            "JARO_WINKLER", "JSON_ARRAY_LENGTH", "JSON_MERGE_PATCH", "JSON_PRETTY", "JSON_TYPE", "LC_FIRST", "LEAST",
            "LEFT", "LENGTH", "LESS", "LESS_EQUAL", "LEVENSHTEIN", "LIST_APPEND", "LIST_AT", "LIST_AVG",
            "LIST_BOOL_AND", "LIST_BOOL_OR", "LIST_CONCAT", "LIST_DISTINCT", "LIST_EXISTS", "LIST_FILTER",
            "LIST_FLATTEN", "LIST_FOR_ALL", "LIST_GET", "LIST_INIT", "LIST_LENGTH", "LIST_MAX", "LIST_MEDIAN",
            "LIST_MIN", "LIST_MODE", "LIST_POSITION", "LIST_PRODUCT", "LIST_REDUCE", "LIST_REVERSE", "LIST_SLICE",
            "LIST_SORT", "LIST_SORT_DESC", "LIST_SUM", "LIST_TAIL", "LIST_TRANSFORM", "LIST_ZIP", "LN", "LOG10",
            "LOWER", "LPAD", "LTRIM", "MAKE_DATE", "MAKE_TIMESTAMP", "MAP_CONCAT", "MAP_EMPTY", "MAP_EXTRACT",
            "MAP_FROM_ENTRIES", "MAP_FROM_LISTS", "MAP_KEYS", "MAP_VALUES", "MATCHES", "MD5", "MINUS", "MOD",
            "MONTHNAME", "NEGATE", "NOT", "NOT_EQUAL", "NOW", "NULL_SAFE_EQUAL", "NULL_SAFE_NOT_EQUAL", "OR",
            "PARSE_DATE", "PARSE_INT", "PI", "PLUS", "POW", "PURE_SPLIT_PART", "RADIANS", "RANDOM", "RANGE_FN",
            "REGEXP_EXTRACT", "REGEXP_EXTRACT_ALL", "REGEXP_FULL_MATCH", "REGEXP_REPLACE", "REM", "REPEAT_STR",
            "REPEAT_VALUE", "REPLACE", "REVERSE_STRING", "RIGHT", "ROUND", "ROUND_HALF_UP", "RPAD", "RTRIM", "SHA1",
            "SHA256", "SIGN", "SIN", "SINH", "SPLIT", "SPLIT_PART", "SQRT", "STARTS_WITH", "STRFTIME", "STRPOS",
            "STRPTIME", "STRUCT_INSERT", "SUBSTRING", "TAN", "TANH", "TIMES", "TIMEZONE", "TIME_BUCKET", "TODAY",
            "TO_VARIANT", "TRANSLATE", "TRIM", "TYPEOF", "UC_FIRST", "UNNEST", "UPPER", "VARIANT_ELEMENTS",
            "VARIANT_GET", "XOR");

    /** The map, as the two sets give it. */
    static Map<String, Kind> sorted() {
        return sorted(PRODUCT, EDGE_ONLY);
    }

    /** The map {@code product} and {@code edgeOnly} give: a name in both is refused. */
    static Map<String, Kind> sorted(Set<String> product, Set<String> edgeOnly) {
        Map<String, Kind> out = new TreeMap<>();
        product.forEach(f -> out.put(f, Kind.PRODUCT));
        edgeOnly.forEach(f -> {
            if (out.put(f, Kind.EDGE_ONLY) != null) {
                throw new AssertionError(f + " is sorted both PRODUCT and EDGE_ONLY");
            }
        });
        return out;
    }

    /** What differs between the sorted names and {@code values}: "+ NAME" unsorted, "- NAME" sorted but gone. */
    static List<String> unsorted(Map<String, Kind> map, List<String> values) {
        Set<String> all = new TreeSet<>(map.keySet());
        all.addAll(values);
        return all.stream().filter(f -> !map.containsKey(f) || !values.contains(f))
                .map(f -> (map.containsKey(f) ? "- " : "+ ") + f).toList();
    }

    @Test
    void everySqlFnIsSorted() {
        List<String> values = Arrays.stream(SqlFn.values()).map(Enum::name).toList();
        assertEquals(List.of(), unsorted(sorted(), values), "SqlFn and its sorting differ: a function added is sorted"
                + " PRODUCT or EDGE_ONLY here (an edge-only one is the edge's to write, the product's dialects refusing"
                + " it by name); one gone is deleted here");
    }

    /** Each list another guard reads ("its name" -> its names) that is not {@code edgeOnly}, "name: its names". */
    static List<String> listsDiffer(Set<String> edgeOnly, Map<String, Set<String>> lists) {
        List<String> out = new ArrayList<>();
        new TreeMap<>(lists).forEach((name, names) -> {
            if (!names.equals(edgeOnly)) {
                out.add(name + ": " + new TreeSet<>(names));
            }
        });
        return out;
    }

    /** The census's list and the dialect census's list are this map's EDGE_ONLY half, exactly. */
    @Test
    void theOtherGuardsReadThisMapsEdgeOnlyHalf() {
        assertEquals(List.of(), listsDiffer(EDGE_ONLY, Map.of(
                "EdgeSeparationTest.EDGE_ONLY_SQLFN", EdgeSeparationTest.EDGE_ONLY_SQLFN,
                "EdgeOnlyNodesCensusTest.EDGE_ONLY", EdgeOnlyNodesCensusTest.EDGE_ONLY.stream().map(Enum::name)
                        .collect(Collectors.toSet()))), "a guard reads another list than this map's EDGE_ONLY half "
                + EDGE_ONLY);
    }

    // ---- the sort, measured (the review of C.12/C.13, 2026-11-17: "the PRODUCT half is asserted, not measured -- a
    // new function written only by the edge but sorted PRODUCT would pass all three guards") ---------------------------

    /** The PRODUCT functions no class writes: rendered by the product's dialects, written by no lowering. Measured
     *  2026-11-17; a writer added moves one out (delete its pin), and a writer on the edge alone makes it EDGE_ONLY. */
    static final Set<String> NO_WRITER = Set.of("DAYNAME", "FROM_EPOCH_MS", "FROM_EPOCH_SECONDS", "JSON_ARRAY_LENGTH",
            "MONTHNAME", "TRANSLATE");

    /** The functions both sides write, measured 2026-11-17: a function the edge writes and a product class only
     *  compares lands here as a new entry (the review of C.13: a comparison reads as a write). */
    static final Map<String, Set<String>> BOTH_WRITERS = Map.ofEntries(
            // the product's lowering and the edge's compositions (upstream's texts, its plans, its renderings) each
            // build calls of these, each with its product writers' top classes (the review of f788dd3230: names alone
            // let a function whose product writes all moved to the edge, a comparison left, pass) -- measured 2026-11-17
            Map.entry("ABS", Set.of(
                    "com.legend.lowering.MixedEncoding", "com.legend.lowering.Scalars",
                    "com.legend.lowering.VerdictSql", "com.legend.sql.dialect.FloatShortestText")),
            Map.entry("AND", Set.of(
                    "com.legend.lowering.CalendarAgg", "com.legend.lowering.CanonicalRenderSql",
                    "com.legend.lowering.CollectionLanes", "com.legend.lowering.ExistsJoinForm",
                    "com.legend.lowering.Fold", "com.legend.lowering.GraphAggDecorrelate",
                    "com.legend.lowering.KeyedCounts", "com.legend.lowering.LiteText", "com.legend.lowering.Lowerer",
                    "com.legend.lowering.NullSemantics", "com.legend.lowering.RelationPredicates",
                    "com.legend.lowering.Scalars", "com.legend.lowering.SqlPostProcessors",
                    "com.legend.lowering.TaggedLists", "com.legend.lowering.VerdictSql",
                    "com.legend.sql.dialect.CarrierStrategies", "com.legend.sql.dialect.FloatShortestText",
                    "com.legend.sql.dialect.MongoDbPipeline", "com.legend.sql.dialect.OuterJoinResidualsAsMatches",
                    "com.legend.sql.dialect.OuterJoinSubqueryFilters", "com.legend.sql.dialect.SelectMerge",
                    "com.legend.sql.dialect.WholePartitionOrderedSets",
                    "com.legend.sql.dialect.WindowAggregatesAsCorrelated",
                    "com.legend.testdatagen.TestDataGenerator", "com.legend.testdatagen.TestDataPlanner")),
            Map.entry("CELL_TEXT", Set.of(
                    "com.legend.lowering.Coercions", "com.legend.lowering.Lowerer", "com.legend.lowering.Scalars",
                    "com.legend.sql.dialect.CellTexts")),
            Map.entry("COALESCE", Set.of(
                    "com.legend.lowering.CanonicalRenderSql", "com.legend.lowering.CollectionLanes",
                    "com.legend.lowering.Dedup", "com.legend.lowering.GraphAggDecorrelate",
                    "com.legend.lowering.InstanceEquality", "com.legend.lowering.JsonEmission",
                    "com.legend.lowering.MixedEncoding", "com.legend.lowering.Scalars",
                    "com.legend.lowering.TaggedLists", "com.legend.lowering.ToOneGuard",
                    "com.legend.lowering.ValueCollections", "com.legend.lowering.VerdictSql",
                    "com.legend.sql.dialect.CarrierStrategies", "com.legend.sql.dialect.CellTexts",
                    "com.legend.sql.dialect.CheckedDefectsToLists", "com.legend.sql.dialect.FoldToListReduce",
                    "com.legend.testdatagen.TestDataPlanner")),
            Map.entry("CONCAT", Set.of(
                    "com.legend.lowering.AsorReaders", "com.legend.lowering.CanonicalRenderSql",
                    "com.legend.lowering.DateCtorRule", "com.legend.lowering.DateShifts",
                    "com.legend.lowering.LiteText", "com.legend.lowering.LiteralSpelling",
                    "com.legend.lowering.Lowerer", "com.legend.lowering.Pivots", "com.legend.lowering.RegexpRules",
                    "com.legend.lowering.Render", "com.legend.lowering.Scalars",
                    "com.legend.lowering.SnapshotEnvelope", "com.legend.lowering.ValueCollections",
                    "com.legend.lowering.VerdictSql", "com.legend.planner.PlanEnv",
                    "com.legend.sql.dialect.CarrierStrategies", "com.legend.sql.dialect.CellTexts",
                    "com.legend.sql.dialect.DateTimeCanonText", "com.legend.sql.dialect.FloatShortestText",
                    "com.legend.testdatagen.TestDataPlanner")),
            Map.entry("DATETIME_TO_TEXT", Set.of(
                    "com.legend.lowering.LiteText", "com.legend.lowering.Styled",
                    "com.legend.sql.dialect.DateTimeText", "com.legend.testdatagen.LitePrinting")),
            Map.entry("ENDS_WITH", Set.of(
                    "com.legend.lowering.DateCtorRule", "com.legend.lowering.Scalars",
                    "com.legend.sql.dialect.FloatShortestText")),
            Map.entry("EQUAL", Set.of(
                    "com.legend.lowering.AsorReaders", "com.legend.lowering.CalendarAgg",
                    "com.legend.lowering.CanonicalRenderSql", "com.legend.lowering.CollectionLanes",
                    "com.legend.lowering.CollectionRelations", "com.legend.lowering.DateShifts",
                    "com.legend.lowering.DecimalKindRules", "com.legend.lowering.Dedup",
                    "com.legend.lowering.EnumSourceValues", "com.legend.lowering.ExistsJoinForm",
                    "com.legend.lowering.Fold", "com.legend.lowering.GraphAggDecorrelate",
                    "com.legend.lowering.InstanceEquality", "com.legend.lowering.KeyedCounts",
                    "com.legend.lowering.KeyedPivot", "com.legend.lowering.ListEncodings",
                    "com.legend.lowering.ListRules", "com.legend.lowering.LiteText",
                    "com.legend.lowering.LiteralSpelling", "com.legend.lowering.Lowerer",
                    "com.legend.lowering.MixedEncoding", "com.legend.lowering.NullSemantics",
                    "com.legend.lowering.RelationPredicates", "com.legend.lowering.Scalars",
                    "com.legend.lowering.SnapshotEnvelope", "com.legend.lowering.SqlPostProcessors",
                    "com.legend.lowering.TaggedLists", "com.legend.lowering.ToOneGuard",
                    "com.legend.lowering.ValueCollections", "com.legend.lowering.VerdictSql",
                    "com.legend.sql.DecodeShapes", "com.legend.sql.EnumValueTables",
                    "com.legend.sql.dialect.AsOfAsPick", "com.legend.sql.dialect.CarrierStrategies",
                    "com.legend.sql.dialect.DateTimeCanonText", "com.legend.sql.dialect.FloatShortestText",
                    "com.legend.sql.dialect.LateralExplodeToUnion",
                    "com.legend.sql.dialect.OuterJoinResidualsAsMatches", "com.legend.sql.dialect.PivotAsGroupBy",
                    "com.legend.testdatagen.TestDataPlanner")),
            Map.entry("FLOAT_SHORTEST_TEXT", Set.of(
                    "com.legend.lowering.LiteText", "com.legend.sql.dialect.FloatShortestText")),
            Map.entry("FLOAT_TO_TEXT", Set.of("com.legend.lowering.LiteralSpelling")),
            Map.entry("GREATER", Set.of(
                    "com.legend.lowering.CalendarAgg", "com.legend.lowering.CanonicalRenderSql",
                    "com.legend.lowering.CollectionLanes", "com.legend.lowering.DateCtorRule",
                    "com.legend.lowering.DateShifts", "com.legend.lowering.Dedup", "com.legend.lowering.KeyedCounts",
                    "com.legend.lowering.ListEncodings", "com.legend.lowering.LiteText",
                    "com.legend.lowering.LiteralSpelling", "com.legend.lowering.Lowerer",
                    "com.legend.lowering.MixedEncoding", "com.legend.lowering.RelationPredicates",
                    "com.legend.lowering.Scalars", "com.legend.lowering.TaggedLists",
                    "com.legend.lowering.VerdictSql", "com.legend.sql.dialect.CarrierStrategies",
                    "com.legend.sql.dialect.FloatShortestText", "com.legend.sql.dialect.FrameGuard",
                    "com.legend.testdatagen.TestDataPlanner")),
            Map.entry("GREATER_EQUAL", Set.of(
                    "com.legend.lowering.CalendarAgg", "com.legend.lowering.DateCtorRule",
                    "com.legend.lowering.DateShifts", "com.legend.lowering.KeyedCounts",
                    "com.legend.lowering.Lowerer", "com.legend.lowering.RelationPredicates",
                    "com.legend.lowering.Scalars", "com.legend.lowering.TaggedLists",
                    "com.legend.lowering.VerdictSql", "com.legend.sql.dialect.CarrierStrategies",
                    "com.legend.sql.dialect.FloatShortestText",
                    "com.legend.sql.dialect.WindowAggregatesAsCorrelated", "com.legend.testdatagen.TestDataPlanner")),
            Map.entry("IN", Set.of(
                    "com.legend.lowering.CalendarAgg", "com.legend.lowering.Scalars",
                    "com.legend.sql.EnumValueTables", "com.legend.sql.dialect.FloatShortestText")),
            Map.entry("IS_NOT_NULL", Set.of(
                    "com.legend.lowering.CheckedEnvelope", "com.legend.lowering.CollectionLanes",
                    "com.legend.lowering.ExistsJoinForm", "com.legend.lowering.Fold",
                    "com.legend.lowering.KeyedPivot", "com.legend.lowering.NullSemantics",
                    "com.legend.lowering.RelationPredicates", "com.legend.lowering.Scalars",
                    "com.legend.lowering.TaggedLists", "com.legend.lowering.VariantShapes",
                    "com.legend.lowering.VerdictSql", "com.legend.sql.dialect.CarrierStrategies",
                    "com.legend.sql.dialect.LateralExplodeToUnion", "com.legend.testdatagen.TestDataPlanner")),
            Map.entry("IS_NULL", Set.of(
                    "com.legend.lowering.CanonicalRenderSql", "com.legend.lowering.CheckedEnvelope",
                    "com.legend.lowering.Coercions", "com.legend.lowering.CollectionLanes",
                    "com.legend.lowering.ExistsJoinForm", "com.legend.lowering.Fold",
                    "com.legend.lowering.ListEncodings", "com.legend.lowering.LiteralSpelling",
                    "com.legend.lowering.MixedEncoding", "com.legend.lowering.NullSemantics",
                    "com.legend.lowering.Scalars", "com.legend.lowering.VariantShapes",
                    "com.legend.lowering.VerdictSql", "com.legend.sql.dialect.AsOfAsPick",
                    "com.legend.sql.dialect.CarrierStrategies", "com.legend.sql.dialect.CellTexts",
                    "com.legend.sql.dialect.FrameGuard", "com.legend.sql.dialect.LateralExplodeToUnion",
                    "com.legend.testdatagen.LitePrinting", "com.legend.testdatagen.TestDataPlanner")),
            Map.entry("LENGTH", Set.of(
                    "com.legend.lowering.AsorReaders", "com.legend.lowering.LiteText", "com.legend.lowering.Scalars",
                    "com.legend.lowering.SnapshotEnvelope", "com.legend.lowering.VerdictSql",
                    "com.legend.sql.dialect.CarrierStrategies", "com.legend.sql.dialect.CellTexts",
                    "com.legend.testdatagen.TestDataPlanner")),
            Map.entry("LESS", Set.of(
                    "com.legend.lowering.DateCtorRule", "com.legend.lowering.KeyedCounts",
                    "com.legend.lowering.Lowerer", "com.legend.lowering.RelationPredicates",
                    "com.legend.lowering.Scalars", "com.legend.lowering.TaggedLists",
                    "com.legend.sql.dialect.CarrierStrategies", "com.legend.sql.dialect.FloatShortestText",
                    "com.legend.sql.dialect.FoldsAsRecursiveCtes", "com.legend.sql.dialect.FrameGuard",
                    "com.legend.testdatagen.TestDataPlanner")),
            Map.entry("LESS_EQUAL", Set.of(
                    "com.legend.lowering.CalendarAgg", "com.legend.lowering.KeyedCounts",
                    "com.legend.lowering.Lowerer", "com.legend.lowering.RelationPredicates",
                    "com.legend.lowering.Scalars", "com.legend.lowering.VerdictSql",
                    "com.legend.sql.dialect.CarrierStrategies",
                    "com.legend.sql.dialect.WindowAggregatesAsCorrelated", "com.legend.testdatagen.TestDataPlanner")),
            Map.entry("LIST_TRANSFORM", Set.of(
                    "com.legend.lowering.CanonicalRenderSql", "com.legend.lowering.CastPolicy",
                    "com.legend.lowering.ConstructionCanon", "com.legend.lowering.Dedup",
                    "com.legend.lowering.InstanceEquality", "com.legend.lowering.LambdaBinding",
                    "com.legend.lowering.ListEncodings", "com.legend.lowering.Lowerer",
                    "com.legend.lowering.MixedEncoding", "com.legend.lowering.Scalars",
                    "com.legend.lowering.TaggedLists", "com.legend.sql.dialect.CarrierStrategies",
                    "com.legend.sql.dialect.CellTexts", "com.legend.sql.dialect.CheckedDefectsToLists",
                    "com.legend.sql.dialect.CommonSubexpressions", "com.legend.sql.dialect.FoldToListReduce")),
            Map.entry("LPAD", Set.of(
                    "com.legend.lowering.DateCtorRule", "com.legend.lowering.Scalars",
                    "com.legend.lowering.SnapshotEnvelope", "com.legend.sql.dialect.StringCountsAsInt")),
            Map.entry("MINUS", Set.of(
                    "com.legend.lowering.AsorReaders", "com.legend.lowering.CalendarAgg",
                    "com.legend.lowering.Comparators", "com.legend.lowering.DateShifts",
                    "com.legend.lowering.FeatureRules", "com.legend.lowering.InstanceEquality",
                    "com.legend.lowering.ListEncodings", "com.legend.lowering.LiteText",
                    "com.legend.lowering.Lowerer", "com.legend.lowering.MixedEncoding",
                    "com.legend.lowering.Scalars", "com.legend.lowering.TaggedLists",
                    "com.legend.lowering.ValueCollections", "com.legend.lowering.VerdictSql",
                    "com.legend.sql.dialect.CarrierStrategies", "com.legend.sql.dialect.CheckedDefectsToLists",
                    "com.legend.sql.dialect.QuantileOrder", "com.legend.sql.dialect.WindowAggregatesAsCorrelated",
                    "com.legend.testdatagen.TestDataPlanner")),
            Map.entry("NOT", Set.of(
                    "com.legend.lowering.CheckedEnvelope", "com.legend.lowering.ExistsJoinForm",
                    "com.legend.lowering.Lowerer", "com.legend.lowering.NullSemantics",
                    "com.legend.lowering.RelationPredicates", "com.legend.lowering.Scalars",
                    "com.legend.lowering.TaggedLists", "com.legend.lowering.VerdictSql",
                    "com.legend.sql.EnumValueTables", "com.legend.sql.dialect.CarrierStrategies",
                    "com.legend.sql.dialect.FloatShortestText", "com.legend.sql.dialect.WholePartitionOrderedSets",
                    "com.legend.testdatagen.TestDataPlanner")),
            Map.entry("NOT_EQUAL", Set.of(
                    "com.legend.lowering.CollectionLanes", "com.legend.lowering.DateCtorRule",
                    "com.legend.lowering.NullSemantics", "com.legend.lowering.Scalars",
                    "com.legend.lowering.VerdictSql", "com.legend.sql.EnumValueTables",
                    "com.legend.testdatagen.TestDataPlanner")),
            Map.entry("OR", Set.of(
                    "com.legend.lowering.CanonicalRenderSql", "com.legend.lowering.DateCtorRule",
                    "com.legend.lowering.EnumSourceValues", "com.legend.lowering.MixedEncoding",
                    "com.legend.lowering.NullSemantics", "com.legend.lowering.Scalars",
                    "com.legend.lowering.TaggedLists", "com.legend.lowering.VerdictSql",
                    "com.legend.sql.DecodeShapes", "com.legend.sql.dialect.AsOfAsPick",
                    "com.legend.sql.dialect.CarrierStrategies", "com.legend.sql.dialect.FrameGuard",
                    "com.legend.testdatagen.TestDataPlanner")),
            Map.entry("PARSE_INT", Set.of("com.legend.lowering.Scalars")),
            Map.entry("REPLACE", Set.of(
                    "com.legend.lowering.LiteText", "com.legend.lowering.LiteralSpelling",
                    "com.legend.lowering.Scalars", "com.legend.lowering.TaggedLists",
                    "com.legend.lowering.VerdictSql", "com.legend.sql.dialect.CarrierStrategies",
                    "com.legend.testdatagen.TestDataPlanner")),
            Map.entry("RTRIM", Set.of(
                    "com.legend.lowering.DateCtorRule", "com.legend.lowering.LiteralSpelling",
                    "com.legend.lowering.Scalars", "com.legend.lowering.SnapshotEnvelope",
                    "com.legend.sql.dialect.DateTimeCanonText", "com.legend.sql.dialect.FloatShortestText")),
            Map.entry("STRFTIME", Set.of(
                    "com.legend.lowering.DateCtorRule", "com.legend.lowering.DateShifts", "com.legend.lowering.Fold",
                    "com.legend.lowering.LiteralSpelling", "com.legend.lowering.Scalars",
                    "com.legend.sql.dialect.DateTimeCanonText", "com.legend.sql.dialect.DateTimeText")),
            Map.entry("STRPOS", Set.of(
                    "com.legend.lowering.CanonicalRenderSql", "com.legend.lowering.LiteText",
                    "com.legend.lowering.LiteralSpelling", "com.legend.lowering.Scalars",
                    "com.legend.lowering.VerdictSql", "com.legend.sql.dialect.FloatShortestText",
                    "com.legend.testdatagen.TestDataPlanner")),
            Map.entry("SUBSTRING", Set.of(
                    "com.legend.lowering.AsorReaders", "com.legend.lowering.FeatureRules",
                    "com.legend.lowering.LiteText", "com.legend.lowering.Scalars", "com.legend.lowering.VerdictSql",
                    "com.legend.sql.dialect.CellTexts", "com.legend.sql.dialect.StringCountsAsInt",
                    "com.legend.sql.dialect.SubstringClamp", "com.legend.testdatagen.TestDataPlanner")));

    /** The functions both sides write, as {@code writers} has them, each with its product writers' top classes. */
    static Map<String, Set<String>> bothProduct(Map<String, Set<String>> writers, Collection<String> names) {
        Map<String, Set<String>> out = new TreeMap<>();
        both(writers).stream().filter(names::contains).forEach(n -> {
            Set<String> tops = new TreeSet<>();
            writers.get(n).stream().filter(x -> x.startsWith("product: "))
                    .forEach(x -> tops.add(top(x.substring("product: ".length()))));
            out.put(n, tops);
        });
        return out;
    }

    /** The functions both sides write, as {@code writers} has them. */
    static Set<String> both(Map<String, Set<String>> writers) {
        Set<String> out = new TreeSet<>();
        writers.forEach((n, w) -> {
            if (w.stream().anyMatch(x -> x.startsWith("product: ")) && w.stream().anyMatch(x -> x.startsWith("edge: "))) {
                out.add(n);
            }
        });
        return out;
    }

    /** The classes that name every function to type or decode it and write none. Beside them, two kinds of class are
     *  no writer: one assignable to {@code SqlDialect}, with its nested classes (a renderer, the product's or the edge's:
     *  an arm per function; a dialect's decompositions write MIR too, so leaving them out only makes the sort stricter --
     *  a function a renderer alone writes measures NO_WRITER and is pinned), and javac's switch map (a dispatch's
     *  table). */
    static final Map<String, String> FN_READERS = Map.of(
            "com.legend.sql.SqlFn", "the enum's own",
            "com.legend.sql.SqlTyping", "the typing rules, one per function",
            "com.legend.executionplan.MirJson", "the plan's codec: a node decoded as it was written");

    /** The edge's side: its package, and the census's classes standing in a product package (counted there:
     *  DateTimePrintText composes DATETIME_PRINT, census B12). */
    static boolean edgeSide(String name) {
        String top = top(name);
        return top.startsWith("com.legend.edge.") || EdgeSeparationTest.COMPAT_CLASSES.containsKey(top);
    }

    private static String top(String n) {
        int dollar = n.indexOf('$');
        return dollar < 0 ? n : n.substring(0, dollar);
    }

    private static boolean switchMap(JavaClass c) {
        return c.getFields().stream().anyMatch(f -> f.getName().startsWith("$SwitchMap$"));
    }

    /** Whether {@code c} is a renderer or nested in one, at any depth. */
    private static boolean renderer(JavaClass c) {
        for (java.util.Optional<JavaClass> x = java.util.Optional.of(c); x.isPresent(); x = x.get().getEnclosingClass()) {
            if (x.get().isAssignableTo(SqlDialect.class.getName())) {
                return true;
            }
        }
        return false;
    }

    /** Each function's writers over {@code classes}, "fn" -> {"product: class", "edge: class"}: a read of its constant
     *  ({@code SqlFn.X}) in a class not {@link #FN_READERS}', a renderer's or a switch map. Its limits: a comparison
     *  ({@code fn == SqlFn.X}) reads as a write, so a function the edge writes and a product class only compares measures
     *  PRODUCT -- which is why the functions both sides write are pinned ({@link #BOTH_WRITERS}): such a function is a
     *  new entry there, with its reason; a name the field accesses do not show -- {@code SqlFn.valueOf}, a qualified enum
     *  pattern's bootstrap -- is unseen (neither is in core's main sources, 2026-11-17). */
    static Map<String, Set<String>> fnWriters(JavaClasses classes, Predicate<String> edge) {
        return constantWriters(classes, edge, SqlFn.class.getName(), FN_READERS.keySet());
    }

    /** Each constant of the enum {@code owner} with its writers over {@code classes} (see {@link #fnWriters}), the
     *  classes {@code readers} names (top classes) aside. */
    static Map<String, Set<String>> constantWriters(JavaClasses classes, Predicate<String> edge, String owner,
                                                    Set<String> readers) {
        Map<String, Set<String>> out = new TreeMap<>();
        for (JavaClass c : classes) {
            String top = top(c.getName());
            if (readers.contains(top) || top.equals(top(owner)) || renderer(c) || switchMap(c)) {
                continue;
            }
            String side = (edge.test(c.getName()) ? "edge: " : "product: ") + c.getName();
            c.getFieldAccessesFromSelf().forEach(a -> {
                if (a.getTarget().getOwner().getName().equals(owner)) {
                    out.computeIfAbsent(a.getTarget().getName(), k -> new TreeSet<>()).add(side);
                }
            });
        }
        return out;
    }

    /** Each of {@code names}' kind as measured: PRODUCT (a product writer), EDGE_ONLY (the edge's writers alone),
     *  NO_WRITER (none). */
    static Map<String, String> measured(Map<String, Set<String>> writers, Collection<String> names) {
        Map<String, String> out = new TreeMap<>();
        for (String n : names) {
            Set<String> w = writers.getOrDefault(n, Set.of());
            out.put(n, w.stream().anyMatch(x -> x.startsWith("product: ")) ? "PRODUCT"
                    : w.isEmpty() ? "NO_WRITER" : "EDGE_ONLY");
        }
        return out;
    }

    /** Where the sort and the measurement differ, "NAME: sorted X, measured Y": a NO_WRITER pin is a PRODUCT name's. */
    static List<String> misSorted(Map<String, Kind> sorted, Set<String> noWriter, Map<String, String> measured) {
        List<String> out = new ArrayList<>();
        new TreeSet<>(noWriter).stream().filter(n -> sorted.get(n) != Kind.PRODUCT).forEach(n -> out.add(n
                + ": pinned NO_WRITER, sorted " + sorted.get(n) + " (no writer is the product's: its dialects render it)"));
        measured.forEach((n, m) -> {
            String expected = noWriter.contains(n) ? "NO_WRITER" : String.valueOf(sorted.get(n));
            if (!expected.equals(m)) {
                out.add(n + ": sorted " + expected + ", measured " + m);
            }
        });
        return out;
    }

    private static JavaClasses importJars(String... properties) {
        List<java.util.jar.JarFile> jars = new ArrayList<>();
        try {
            for (java.nio.file.Path jar : ConstantPools.jars(properties)) {
                jars.add(new java.util.jar.JarFile(jar.toFile()));
            }
        } catch (java.io.IOException e) {
            throw new java.io.UncheckedIOException(e);
        }
        return new ClassFileImporter().importJars(jars);
    }

    private static String explain(List<String> wrong, Map<String, Set<String>> writers) {
        StringBuilder out = new StringBuilder();
        wrong.forEach(w -> out.append("\n  ").append(w).append(" -- ")
                .append(writers.getOrDefault(w.substring(0, w.indexOf(':')), Set.of())));
        return out.toString();
    }

    @Test
    void theSortIsMeasured() {
        JavaClasses classes = importJars("legend.product.jars", "legend.authored.jars", "legend.edge.jars");
        Map<String, Set<String>> writers = fnWriters(classes, SqlFnSortedTest::edgeSide);
        assertTrue(writers.size() > 150, "the functions' writers are read: " + writers.size());
        List<String> values = Arrays.stream(SqlFn.values()).map(Enum::name).toList();
        List<String> wrong = misSorted(sorted(), NO_WRITER, measured(writers, values));
        assertEquals(List.of(), wrong, "a function's sort and its writers differ (one the edge alone writes is"
                + " EDGE_ONLY, the product's dialects refusing it by name; one no class writes is pinned NO_WRITER):"
                + explain(wrong, writers));
        assertEquals(new TreeMap<>(BOTH_WRITERS), bothProduct(writers, values), "the functions both sides write, or"
                + " their product writers, differ from the pin: a new one"
                + " (the edge writes it, a product class names it -- a comparison reads as a write) is pinned with its"
                + " reason, or sorted EDGE_ONLY; one gone is deleted");
    }

    // ---- the aggregate functions, measured the same way (the review of C.13's NOTE: SqlAgg.Fn was sorted nowhere) ----

    /** The aggregate functions only the edge writes, and the ones no class writes, measured 2026-11-17; every other is
     *  the product's. */
    static final Set<String> EDGE_ONLY_AGG = Set.of();
    static final Set<String> NO_WRITER_AGG = Set.of();

    /** The aggregate functions both sides write, measured 2026-11-17: a list and a string aggregation, the product's
     *  and the edge's compositions alike. */
    static final Map<String, Set<String>> BOTH_AGG = Map.ofEntries(
            Map.entry("LIST", Set.of(
                    "com.legend.lowering.Lowerer", "com.legend.lowering.Scalars", "com.legend.lowering.StampCensus",
                    "com.legend.lowering.ValueCollections", "com.legend.sql.SqlExpr",
                    "com.legend.sql.dialect.CarrierStrategies", "com.legend.sql.dialect.QuantileOrder",
                    "com.legend.sql.dialect.WindowAggregatesAsCorrelated")),
            Map.entry("STRING_AGG", Set.of(
                    "com.legend.lowering.Aggregates", "com.legend.lowering.Fold", "com.legend.lowering.Lowerer",
                    "com.legend.lowering.Scalars", "com.legend.lowering.TaggedLists",
                    "com.legend.lowering.ValueCollections", "com.legend.lowering.VerdictSql",
                    "com.legend.sql.dialect.CarrierStrategies", "com.legend.sql.dialect.CellTexts",
                    "com.legend.sql.dialect.WindowAggregatesAsCorrelated", "com.legend.testdatagen.TestDataPlanner")));

    @Test
    void theAggregatesAreSortedByTheirWriters() {
        JavaClasses classes = importJars("legend.product.jars", "legend.authored.jars", "legend.edge.jars");
        String owner = com.legend.sql.SqlAgg.Fn.class.getName();
        Map<String, Set<String>> writers = constantWriters(classes, SqlFnSortedTest::edgeSide, owner, FN_READERS.keySet());
        List<String> values = Arrays.stream(com.legend.sql.SqlAgg.Fn.values()).map(Enum::name).toList();
        Set<String> product = new TreeSet<>(values);
        product.removeAll(EDGE_ONLY_AGG);
        List<String> wrong = misSorted(sorted(product, EDGE_ONLY_AGG), NO_WRITER_AGG, measured(writers, values));
        assertEquals(List.of(), wrong, "an aggregate function's sort and its writers differ:" + explain(wrong, writers));
        assertEquals(new TreeMap<>(BOTH_AGG), bothProduct(writers, values), "the aggregate functions both sides write, or"
                + " their product writers,"
                + " pin: pin a new one with its reason (or sort it EDGE_ONLY); delete one gone");
    }

    // ---- the MIR's nodes, sorted by their writers (the same review: "the edge-only SqlExpr variants are sorted
    // nowhere") ---------------------------------------------------------------------------------------------------

    /** The expression nodes only the edge writes (each a product record the product decodes and rebuilds, never makes). */
    static final Set<String> EDGE_ONLY_NODES = Set.of(
            // a relation's text deferred to the database (legend-pure's TDS string): the edge's RelationTextLowering
            // (census B17)
            "com.legend.sql.SqlExpr$DeferredTdsString",
            // legend-engine's temp-table splice of an IN list: the edge's InProtocol
            "com.legend.sql.SqlExpr$TempTableInSplice");

    /** The nodes no class writes. Measured 2026-11-17. */
    static final Set<String> NO_WRITER_NODES = Set.of();

    /** The classes that make a node they were handed, writing none. */
    static final Map<String, String> NODE_REBUILDERS = Map.of(
            "com.legend.executionplan.MirJson", "the plan's codec: a node decoded as it was written",
            "com.legend.sql.SqlExpr", "the nodes' own: a node rebuilt with its children mapped",
            // the sources' and the aggregates' own (the review of f788dd3230, 2026-11-17: a record's secondary
            // constructor delegating to its canonical one made the node its own writer -- SqlSource$Values, GridReport,
            // Join, SqlAgg$Reducer measured PRODUCT vacuously); their factories write for their callers (made)
            "com.legend.sql.SqlSource", "the sources' own: a source built by its own constructors",
            "com.legend.sql.SqlAgg", "the aggregates' own: a reducer built by its own constructors",
            "com.legend.sql.SqlRewriter", "the generic rewrite: a node rebuilt with its children rewritten");

    /** The concrete variants of a sealed {@code root}, a sealed sub-interface's own included. */
    static Set<String> leaves(Class<?> root) {
        Set<String> out = new TreeSet<>();
        for (Class<?> c : root.getPermittedSubclasses()) {
            if (c.isSealed() && c.isInterface()) {
                out.addAll(leaves(c));
            } else {
                out.add(c.getName());
            }
        }
        return out;
    }

    /** The nodes {@code m} constructs (a call or a {@code ::new}) and does not rebuild: a factory's, whatever it
     *  returns and wherever it lives. A rebuild is told by what it reads of the node it remakes: its accessors or fields;
     *  an {@code instanceof} or a pattern's class (a switch over a parameter); or {@code this}, an instance method of a
     *  supertype of the node ({@code SqlExpr.withChildren}'s arms, which read nothing of the node: the review of
     *  f788dd3230, 2026-11-17). The codec decodes, so it makes none. Its limit: a rebuild that reads nothing of a node
     *  handed in some other way (a field holding it, an untyped list) reads as a factory. */
    static Set<String> made(JavaCodeUnit m, Set<String> nodes) {
        if (m.getOwner().getName().equals("com.legend.executionplan.MirJson")) {
            return Set.of();
        }
        Map<String, JavaClass> made = new TreeMap<>();
        Stream.concat(m.getConstructorCallsFromSelf().stream().map(x -> x.getTargetOwner()),
                m.getConstructorReferencesFromSelf().stream().map(x -> x.getTargetOwner()))
                .filter(o -> nodes.contains(o.getName())).forEach(o -> made.put(o.getName(), o));
        Stream.concat(m.getMethodCallsFromSelf().stream().map(x -> x.getTargetOwner().getName()),
                m.getFieldAccesses().stream().map(x -> x.getTargetOwner().getName())).forEach(made::remove);
        m.getInstanceofChecks().forEach(i -> made.remove(i.getRawType().getName()));
        m.getReferencedClassObjects().forEach(o -> made.remove(o.getRawType().getName()));
        if (!m.getModifiers().contains(com.tngtech.archunit.core.domain.JavaModifier.STATIC)) {
            made.values().removeIf(n -> n.isAssignableTo(m.getOwner().getName()));
        }
        return new TreeSet<>(made.keySet());
    }

    /** Each node's writers over {@code classes}, "node" -> {"product: class", "edge: class"}: in a class
     *  {@code rebuilder} does not name, a constructor call or reference, or a call or reference to a method that makes
     *  the node ({@link #made}: one level, so a factory returning a supertype, or a helper hosted in a rebuilder, is
     *  seen -- the review of C.13, 2026-11-17). */
    static Map<String, Set<String>> nodeWriters(JavaClasses classes, Set<String> nodes, Predicate<String> edge,
                                                Predicate<String> rebuilder) {
        Map<String, Set<String>> out = new TreeMap<>();
        for (JavaClass c : classes) {
            if (rebuilder.test(c.getName())) {
                continue;
            }
            String side = (edge.test(c.getName()) ? "edge: " : "product: ") + c.getName();
            for (JavaCodeUnit u : c.getCodeUnits()) {
                Stream.concat(u.getConstructorCallsFromSelf().stream().map(x -> x.getTargetOwner().getName()),
                        u.getConstructorReferencesFromSelf().stream().map(x -> x.getTargetOwner().getName()))
                        .filter(nodes::contains)
                        .forEach(n -> out.computeIfAbsent(n, k -> new TreeSet<>()).add(side));
                Stream.concat(u.getMethodCallsFromSelf().stream().map(x -> x.getTarget().resolveMember()),
                        u.getMethodReferencesFromSelf().stream().map(x -> x.getTarget().resolveMember()))
                        .flatMap(java.util.Optional::stream)
                        .forEach(m -> made(m, nodes).forEach(n -> out.computeIfAbsent(n, k -> new TreeSet<>())
                                .add(side)));
            }
        }
        return out;
    }

    private static boolean nodeRebuilder(String name) {
        return NODE_REBUILDERS.containsKey(top(name));
    }

    private static Map<String, Kind> nodeSort(Set<String> nodes, Set<String> edgeOnly) {
        Map<String, Kind> out = new TreeMap<>();
        nodes.forEach(n -> out.put(n, edgeOnly.contains(n) ? Kind.EDGE_ONLY : Kind.PRODUCT));
        edgeOnly.stream().filter(n -> !nodes.contains(n)).forEach(n -> out.put(n, Kind.EDGE_ONLY));
        return out;
    }

    @Test
    void theNodesAreSortedByTheirWriters() {
        // the expression nodes and the sources (the review's NOTE: SqlSource was sorted nowhere)
        Set<String> nodes = new TreeSet<>(leaves(SqlExpr.class));
        nodes.addAll(leaves(com.legend.sql.SqlSource.class));
        assertTrue(nodes.size() > 50, "the nodes are read: " + nodes.size());
        JavaClasses classes = importJars("legend.product.jars", "legend.authored.jars", "legend.edge.jars");
        Map<String, Set<String>> writers = nodeWriters(classes, nodes, SqlFnSortedTest::edgeSide,
                SqlFnSortedTest::nodeRebuilder);
        Map<String, Kind> sort = nodeSort(nodes, EDGE_ONLY_NODES);
        Set<String> all = new TreeSet<>(sort.keySet());
        List<String> wrong = misSorted(sort, NO_WRITER_NODES, measured(writers, all));
        assertEquals(List.of(), wrong, "a node's sort and its writers differ (one the edge alone writes is"
                + " EDGE_ONLY_NODES'; one no class writes is pinned NO_WRITER_NODES):" + explain(wrong, writers));
    }

    /** Planted: a function added and one gone are each found. */
    @Test
    void theSortBites() {
        Map<String, Kind> planted = new TreeMap<>(Map.of("AND", Kind.PRODUCT, "GONE_FN", Kind.PRODUCT,
                "DATETIME_PRINT", Kind.EDGE_ONLY));
        assertEquals(List.of("- GONE_FN", "+ UPSTREAM_PRINT"), unsorted(planted, List.of("AND", "DATETIME_PRINT",
                "UPSTREAM_PRINT")));
        AssertionError both = org.junit.jupiter.api.Assertions.assertThrows(AssertionError.class,
                () -> sorted(Set.of("AND", "DAYNAME"), Set.of("DAYNAME")));
        assertEquals("DAYNAME is sorted both PRODUCT and EDGE_ONLY", both.getMessage());
        assertEquals(List.of("a census's list: [DATETIME_PRINT, UPSTREAM_PRINT]"), listsDiffer(Set.of("DATETIME_PRINT"),
                Map.of("a census's list", Set.of("DATETIME_PRINT", "UPSTREAM_PRINT"), "the dialects' list",
                        Set.of("DATETIME_PRINT"))), "a list another guard reads, differing, is named");
    }

    /** The planted classes, read as the jars are: the switch's map javac writes beside them included. */
    private static JavaClasses planted() {
        return new ClassFileImporter().withImportOption(l -> l.contains("SqlFnSortedTest")
                || l.contains("SqlExpr$FloatLit") || l.contains("/SqlExpr.class") || l.contains("/SqlSource"))
                .importPackages(SqlFnSortedTest.class.getPackageName());
    }

    private static boolean plantedEdge(String name) {
        return name.endsWith("$PlantedEdgeWriter");
    }

    /** Planted: a product writer, an edge writer, a switch's map and a renderer read as the measurement reads them; a
     *  function the edge alone writes, sorted PRODUCT, and one no class writes, sorted EDGE_ONLY, are each named; a
     *  NO_WRITER pin on a name not PRODUCT is named. */
    @Test
    void theMeasurementBites() {
        JavaClasses classes = planted();
        Map<String, Set<String>> writers = fnWriters(classes, SqlFnSortedTest::plantedEdge);
        List<String> names = List.of("ABS", "DAYNAME", "MONTHNAME", "TRANSLATE", "LEFT");
        assertEquals(Map.of("ABS", "PRODUCT", "DAYNAME", "EDGE_ONLY", "MONTHNAME", "NO_WRITER", "TRANSLATE",
                "NO_WRITER", "LEFT", "NO_WRITER"), measured(writers, names), "a switch's map, a renderer and a class"
                + " nested in one write nothing: " + writers);
        assertEquals(Set.of(), both(writers));
        assertEquals(Set.of("ABS"), both(Map.of("ABS", Set.of("product: P", "edge: E"), "DAYNAME", Set.of("edge: E"))),
                "a function both sides write is read as both");
        assertEquals(Map.of("ABS", Set.of("a.P")), bothProduct(Map.of("ABS", Set.of("product: a.P$Inner", "product: a.P",
                "edge: E"), "DAYNAME", Set.of("edge: E")), List.of("ABS", "DAYNAME")), "each with its product writers'"
                + " top classes");
        Map<String, Kind> wrongSort = sorted(Set.of("ABS", "DAYNAME", "MONTHNAME", "LEFT"), Set.of("TRANSLATE"));
        assertEquals(List.of("DAYNAME: sorted PRODUCT, measured EDGE_ONLY", "TRANSLATE: sorted EDGE_ONLY, measured"
                + " NO_WRITER"), misSorted(wrongSort, Set.of("MONTHNAME", "LEFT"), measured(writers, names)));
        assertEquals(List.of("TRANSLATE: pinned NO_WRITER, sorted EDGE_ONLY (no writer is the product's: its dialects"
                + " render it)"), misSorted(wrongSort, Set.of("MONTHNAME", "TRANSLATE"), Map.of()));
        assertEquals(List.of(), misSorted(sorted(Set.of("ABS", "MONTHNAME", "TRANSLATE", "LEFT"), Set.of("DAYNAME")),
                Set.of("MONTHNAME", "TRANSLATE", "LEFT"), measured(writers, names)), "the right sort passes");

        Set<String> nodes = Set.of(SqlExpr.StringLit.class.getName(), SqlExpr.IntLit.class.getName(),
                SqlExpr.FloatLit.class.getName(), SqlExpr.NullLit.class.getName(), SqlExpr.BoolLit.class.getName(),
                SqlExpr.Group.class.getName(), com.legend.sql.SqlSource.Values.class.getName());
        Map<String, Set<String>> made = nodeWriters(classes, nodes, SqlFnSortedTest::plantedEdge,
                n -> n.endsWith("$PlantedRebuilder") || nodeRebuilder(n));
        assertEquals(Map.of(SqlExpr.StringLit.class.getName(), "EDGE_ONLY", SqlExpr.IntLit.class.getName(), "PRODUCT",
                SqlExpr.FloatLit.class.getName(), "PRODUCT", SqlExpr.NullLit.class.getName(), "NO_WRITER",
                SqlExpr.BoolLit.class.getName(), "PRODUCT", SqlExpr.Group.class.getName(), "NO_WRITER",
                com.legend.sql.SqlSource.Values.class.getName(), "NO_WRITER"), measured(made, nodes), "a constructor, a"
                + " constructor reference, a reference to a factory and a call to a factory hosted in a rebuilder each"
                + " write; a rebuild does not, nor withChildren over this (Group), nor a source's own delegating"
                + " constructor (Values): " + made);
        assertEquals(List.of(SqlExpr.StringLit.class.getName() + ": sorted PRODUCT, measured EDGE_ONLY"),
                misSorted(nodeSort(nodes, Set.of()), Set.of(SqlExpr.NullLit.class.getName(),
                        SqlExpr.Group.class.getName(), com.legend.sql.SqlSource.Values.class.getName()),
                        measured(made, nodes)));
    }

    /** Planted product body: a function's constant read; a node made by a constructor reference and a factory. */
    @SuppressWarnings("unused")
    static final class PlantedProductWriter {
        static Object write() {
            return SqlFn.ABS;
        }

        static Object make() {
            java.util.function.LongFunction<SqlExpr.IntLit> f = SqlExpr.IntLit::new;
            java.util.function.BiFunction<Double, SqlExpr.FloatLit.Kind, SqlExpr.FloatLit> g = SqlExpr.FloatLit::of;
            return List.of(f.apply(1), g.apply(1.0, SqlExpr.FloatLit.Kind.DOUBLE), PlantedRebuilder.boolOf(true),
                    PlantedRebuilder.rebuildNull(null));
        }

        /** A rebuild over this: withChildren's arms construct nodes they do not read (Group among them). */
        static Object rebuild(SqlExpr e) {
            return e.withChildren(List.of());
        }
    }

    /** Planted edge body: a function's constant read; a node constructed. */
    @SuppressWarnings("unused")
    static final class PlantedEdgeWriter {
        static Object write() {
            return SqlFn.DAYNAME;
        }

        static Object make() {
            return new SqlExpr.StringLit("x");
        }
    }

    /** Planted dispatch: a switch on a function (javac's map reads the constant; no write). */
    @SuppressWarnings("unused")
    static final class PlantedSwitch {
        static int dispatch(SqlFn f) {
            switch (f) {
                case MONTHNAME:
                    return 1;
                default:
                    return 0;
            }
        }
    }

    /** Planted renderer: an arm per function (no write), its nested class's too. */
    @SuppressWarnings("unused")
    abstract static class PlantedRenderer implements SqlDialect {
        static Object arm() {
            return SqlFn.TRANSLATE;
        }

        static final class Arms {
            static Object arm() {
                return SqlFn.LEFT;
            }
        }
    }

    /** Planted rebuilder: a factory it hosts (returning a supertype) writes for its caller; a rebuild does not. */
    @SuppressWarnings("unused")
    static final class PlantedRebuilder {
        static SqlExpr boolOf(boolean b) {
            return new SqlExpr.BoolLit(b);
        }

        static SqlExpr rebuildNull(SqlExpr.NullLit n) {
            return new SqlExpr.NullLit(n.type());
        }
    }
}
