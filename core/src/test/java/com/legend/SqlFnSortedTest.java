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
    static final Set<String> EDGE_ONLY = Set.of();
    // (DATETIME_PRINT gone 2026-11-17 with train 7's B12 close, a8b7e2e32e: legend-pure's DateTime print the edge's
    // dialects' over the product's DATETIME_TO_TEXT, no MIR function of its own)

    /** Every other function, the product's: 189, sorted 2026-11-17 by the measured list (a new one is sorted here; IS_NUMERIC
     *  2026-11-18, J20; SUBSTRING_AS_GIVEN 2026-11-19, A3 of the compatibility program, the user's ruling ~21:10 "Yes to
     *  all three for substring": written by Scalars, the product's, and rendered by every dialect -- not EDGE_ONLY, which
     *  is the product's dialects refusing it by name; SAME_IN_ORDER and SAME_IN_ANY_ORDER 2026-11-22, item 3's S4, by the
     *  user's word that day ("Yes on list compare", then "Yes, add both entries"): two sides' typed values the same,
     *  written by VerdictSql, each dialect writing its database's own form -- DuckDB's list equality, every other the
     *  portable one, MongoDB refusing it by name; JSON_EQUAL 2026-11-23, item 3's S4, the user's ruling of 2026-11-22
     *  (Variants compared by the database's typed JSON equality, "variant on duckdb and jsonb in postgres should both
     *  compare typed json"), its entry by the user's word of 2026-11-23 ("Approved", with the standing rule that a new
     *  function only product code writes is sorted PRODUCT with a dated note and reported): written by NullSemantics (a Variant's ==) and
     *  SameValuesForm (two lists of Variants), DuckDB's VARIANT and Postgres's jsonb writing it, every other dialect
     *  refusing it by name) -- its entry gone the same day: register S401 ruled by the user 2026-11-23 ("Okay"), two whole
     *  Variants refused by name at planning, so no plan compares two JSON documents. */
    static final Set<String> PRODUCT = Set.of(
            "ABS", "ACOS", "ADD_INTERVAL", "ADD_INTERVAL_TEMPORAL", "ALL_DISTINCT", "AND", "ASCII_CODE", "ASIN",
            "ATAN", "ATAN2", "BINARY_COLLATE", "BIT_AND", "BIT_NOT", "BIT_OR", "BIT_SHIFT_LEFT", "BIT_SHIFT_RIGHT",
            "BIT_XOR", "BOOL_TO_TEXT", "CBRT", "CEILING", "CELL_TEXT", "CHR", "COALESCE", "CONCAT", "CONCAT_JOIN",
            "CONCAT_WS", "COS", "COSH", "COT", "CURRENT_USER_FN", "DATETIME_CANON_TEXT", "DATETIME_TO_TEXT",
            "DATE_AT_TIME", "DATE_DIFF", "DATE_TRUNC", "DATE_TRUNC_DAY", "DAYNAME", "DECIMAL_TO_TEXT",
            "DECODE_BASE64", "DEGREES", "DIVIDE", "ENCODE_BASE64", "ENDS_WITH", "EPOCH_MS", "EPOCH_NS",
            "EPOCH_SECONDS", "EQUAL", "ERROR", "EXP", "EXTRACT", "FLOAT_SHORTEST_TEXT", "FLOAT_TO_TEXT",
            // FLOAT_FULL_TEXT added 2026-11-25 (B2b, toCSV's Float cell exact; the user's ruling "okay make exact" and
            // B3's (B) the same day, under the standing rule that a new function only product code writes is sorted
            // PRODUCT with a dated note and reported): the database's own full-precision text of a double, written by
            // lowering.ProductRelationText alone
            "FLOAT_FULL_TEXT",
            "FLOOR",
            "FLOOR_RAW", "FORMAT", "FROM_EPOCH_MS", "FROM_EPOCH_NS", "FROM_EPOCH_SECONDS", "GREATER", "GREATER_EQUAL",
            "GREATEST", "GUID", "HASH", "IN", "INT_DIVIDE", "IS_DISTINCT_FROM", "IS_NOT_NULL", "IS_NULL",
            // IS_NUMERIC added 2026-11-18 with J20 (222d136808, train 11, the user's ruling of 2026-11-17 23:05): the
            // product's real numeric check, written by ProductLoweringForms (measured); legend-engine's H2 lower = upper
            // the edge's forms', which write no function of their own
            "IS_NUMERIC",
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
            "REPEAT_VALUE", "REPLACE", "REVERSE_STRING", "RIGHT", "ROUND", "ROUND_HALF_UP",
            // ROUND_RAW (2026-11-26, the edition program's step 5): the database's own ROUND, written by the lowering's
            // named rule for it (DecimalKindRules.roundAsGiven, which a door's table names) -- product code alone
            // writes it, so PRODUCT, by the user's standing rule of 2026-11-23 (dated here and reported)
            "ROUND_RAW", "RPAD", "RTRIM",
            "SAME_IN_ANY_ORDER", "SAME_IN_ORDER", "SHA1",
            "SHA256", "SIGN", "SIN", "SINH", "SPLIT", "SPLIT_PART", "SQRT", "STARTS_WITH", "STRFTIME", "STRPOS",
            "STRPTIME", "STRUCT_INSERT", "SUBSTRING", "SUBSTRING_AS_GIVEN", "TAN", "TANH", "TIMES", "TIMEZONE", "TIME_BUCKET", "TODAY",
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
    // (2026-11-22, item 3's S4 step 4, shrinks under the user's standing word for cleanups: lowering.LiteText and
    // lowering.LiteVerdictWords deleted with the database judge's sentence rows -- out of AND, COALESCE, CONCAT,
    // DATETIME_TO_TEXT, EQUAL, FLOAT_SHORTEST_TEXT, GREATER, LENGTH, LOWER, MINUS, REPLACE, STRPOS, SUBSTRING; and
    // VerdictSql writes no GREATER_EQUAL, NOT_EQUAL or STRING_AGG now)
    // (2026-11-25, stage B's B2b S5a, measured by the full round: lowering.ProductRelationText joins the product's writers
    // of COALESCE, GREATER, IS_NULL, OR, REPLACE and STRPOS (and of the aggregate STRING_AGG, BOTH_AGG) -- a relation's
    // toString is lite's own text, computed by the database, the user's rulings of 2026-11-19 ~18:05 and ~18:36 and the
    // orchestrator's go of 2026-11-24: the direct, measured cost of the ruled change, done and reported)
    static final Map<String, Set<String>> BOTH_WRITERS = Map.ofEntries(
            // the product's lowering and the edge's compositions (upstream's texts, its plans, its renderings) each
            // build calls of these, each with its product writers' top classes (the review of f788dd3230: names alone
            // let a function whose product writes all moved to the edge, a comparison left, pass) -- measured 2026-11-17
            // (DateCtorRule joins 2026-11-21, the BE audit 2's S2: a date()'s fractional second kept unsigned, as
            // legend-pure's CoreHelper.newDate keeps it)
            Map.entry("ABS", Set.of(
                    "com.legend.lowering.DateCtorRule", "com.legend.lowering.MixedEncoding",
                    "com.legend.lowering.Scalars", "com.legend.lowering.VerdictSql",
                    "com.legend.sql.dialect.FloatShortestText")),
            // re-measured 2026-11-18 on d-int10 5f83349087: the EXISTS decomposition's writer is sql.ExistsJoins, not
            // lowering.ExistsJoinForm (F's J19, a3b8f3f3: ClickHouse decomposes the EXISTS it cannot correlate at render)
            // in AND, EQUAL, IS_NOT_NULL, IS_NULL and NOT; REGEXP_REPLACE joins (E-SQL's J2, train 9: the goldens' '}{'
            // tolerance is the edge's own REGEXP_REPLACE, EdgeLoweringForms; the product's writers keep theirs)
            // AND: + sql.dialect.SameValuesForm (2026-11-22, item 3's S4, by the user's word that day ("Yes on list compare",
            // then "Yes, add it"): the portable form of two sides' values the same, moved from VerdictSql into the dialect)
            // AND: + sql.dialect.CorrelatedFiltersAsJoins (2026-11-26, the edition program's step 1, the user's ruling of
            // 2026-11-25 on H2's lateral): the conjuncts a lateral's filter is split into are joined again by AND, for
            // the join's ON and for what stays inside -- a shape rearranged, nothing computed
            Map.entry("AND", Set.of(
                    "com.legend.sql.dialect.CorrelatedFiltersAsJoins",
                    "com.legend.sql.dialect.SameValuesForm",
                    "com.legend.lowering.CalendarAgg", "com.legend.lowering.CanonicalRenderSql",
                    "com.legend.lowering.CollectionLanes", "com.legend.sql.ExistsJoins",
                    "com.legend.lowering.Fold", "com.legend.lowering.GraphAggDecorrelate",
                    "com.legend.lowering.KeyedCounts", "com.legend.lowering.Lowerer",
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
                    "com.legend.lowering.ProductRelationText",   // 2026-11-25, B2b S5a: lite's relation toString
                    // (lowering.LiteVerdictWords, census J29's lite verdict sentences: gone 2026-11-22, item 3's S4 step 4)
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
                    "com.legend.lowering.DateCtorRule", "com.legend.lowering.DateShifts", "com.legend.lowering.LiteralSpelling",
                    "com.legend.lowering.Lowerer", "com.legend.lowering.Pivots", "com.legend.lowering.RegexpRules",
                    "com.legend.lowering.Render", "com.legend.lowering.Scalars",
                    "com.legend.lowering.SnapshotEnvelope", "com.legend.lowering.ValueCollections",
                    "com.legend.lowering.VerdictSql", "com.legend.planner.PlanEnv",
                    "com.legend.sql.dialect.CarrierStrategies", "com.legend.sql.dialect.CellTexts",
                    "com.legend.sql.dialect.DateTimeCanonText", "com.legend.sql.dialect.FloatShortestText",
                    "com.legend.testdatagen.TestDataPlanner")),
            Map.entry("DATETIME_TO_TEXT", Set.of(
                    "com.legend.lowering.Styled",
                    "com.legend.sql.dialect.DateTimeText", "com.legend.testdatagen.LitePrinting")),
            // (ENDS_WITH: both sides' no more, 2026-11-25, a shrink -- edge.RelationTextLowering, which wrote it,
            // went with stage B's B2b S5c (a TDS's csv refused by name in the product until the user's ruling of 2026-11-25, S405, a last step's printed by the doors' Java
            // printer); the product's writers keep theirs)
            // EQUAL: + sql.dialect.SameValuesForm (2026-11-22, item 3's S4, by the user's word that day ("Yes on list compare",
            // then "Yes, add it"): the portable form of two sides' values the same, moved from VerdictSql into the dialect)
            Map.entry("EQUAL", Set.of(
                    "com.legend.lowering.ProductRelationText",   // 2026-11-25, B2b S5b: the CSV's empty String (S420)
                    "com.legend.sql.dialect.SameValuesForm",
                    "com.legend.lowering.AsorReaders", "com.legend.lowering.CalendarAgg",
                    "com.legend.lowering.CanonicalRenderSql", "com.legend.lowering.CollectionLanes",
                    "com.legend.lowering.CollectionRelations", "com.legend.lowering.DateShifts",
                    "com.legend.lowering.DecimalKindRules", "com.legend.lowering.Dedup",
                    "com.legend.lowering.EnumSourceValues", "com.legend.sql.ExistsJoins",
                    "com.legend.lowering.Fold", "com.legend.lowering.GraphAggDecorrelate",
                    "com.legend.lowering.InstanceEquality", "com.legend.lowering.KeyedCounts",
                    "com.legend.lowering.KeyedPivot", "com.legend.lowering.ListEncodings",
                    "com.legend.lowering.ListRules",
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
                    "com.legend.sql.dialect.FloatShortestText")),
            Map.entry("FLOAT_TO_TEXT", Set.of("com.legend.lowering.LiteralSpelling")),
            Map.entry("GREATER", Set.of(
                    "com.legend.lowering.ProductRelationText",   // 2026-11-25, B2b S5a: lite's relation toString
                    "com.legend.lowering.CalendarAgg", "com.legend.lowering.CanonicalRenderSql",
                    "com.legend.lowering.CollectionLanes", "com.legend.lowering.DateCtorRule",
                    "com.legend.lowering.DateShifts", "com.legend.lowering.Dedup", "com.legend.lowering.KeyedCounts",
                    "com.legend.lowering.ListEncodings",
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
                    "com.legend.lowering.Scalars", "com.legend.lowering.TaggedLists", "com.legend.sql.dialect.CarrierStrategies",
                    "com.legend.sql.dialect.FloatShortestText",
                    "com.legend.sql.dialect.WindowAggregatesAsCorrelated", "com.legend.testdatagen.TestDataPlanner")),
            Map.entry("IN", Set.of(
                    "com.legend.lowering.CalendarAgg", "com.legend.lowering.Scalars",
                    "com.legend.sql.EnumValueTables", "com.legend.sql.dialect.FloatShortestText")),
            Map.entry("IS_NOT_NULL", Set.of(
                    "com.legend.lowering.CheckedEnvelope", "com.legend.lowering.CollectionLanes",
                    "com.legend.sql.ExistsJoins", "com.legend.lowering.Fold",
                    "com.legend.lowering.KeyedPivot", "com.legend.lowering.NullSemantics",
                    "com.legend.lowering.RelationPredicates", "com.legend.lowering.Scalars",
                    "com.legend.lowering.TaggedLists", "com.legend.lowering.VariantShapes",
                    "com.legend.lowering.VerdictSql", "com.legend.sql.dialect.CarrierStrategies",
                    "com.legend.sql.dialect.LateralExplodeToUnion", "com.legend.testdatagen.TestDataPlanner")),
            Map.entry("IS_NULL", Set.of(
                    "com.legend.lowering.ProductRelationText",   // 2026-11-25, B2b S5a: lite's relation toString
                    "com.legend.lowering.CanonicalRenderSql", "com.legend.lowering.CheckedEnvelope",
                    "com.legend.lowering.Coercions", "com.legend.lowering.CollectionLanes",
                    "com.legend.sql.ExistsJoins", "com.legend.lowering.Fold",
                    "com.legend.lowering.ListEncodings", "com.legend.lowering.LiteralSpelling",
                    "com.legend.lowering.MixedEncoding", "com.legend.lowering.NullSemantics",
                    "com.legend.lowering.Scalars", "com.legend.lowering.VariantShapes",
                    "com.legend.lowering.VerdictSql", "com.legend.sql.dialect.AsOfAsPick",
                    "com.legend.sql.dialect.CarrierStrategies", "com.legend.sql.dialect.CellTexts",
                    "com.legend.sql.dialect.FrameGuard", "com.legend.sql.dialect.LateralExplodeToUnion",
                    "com.legend.testdatagen.LitePrinting", "com.legend.testdatagen.TestDataPlanner")),
            // (LENGTH: both sides' no more, 2026-11-25, a shrink -- edge.RelationTextLowering, which wrote it,
            // went with stage B's B2b S5c (a TDS's csv refused by name in the product until the user's ruling of 2026-11-25, S405, a last step's printed by the doors' Java
            // printer); the product's writers keep theirs)
            Map.entry("LESS", Set.of(
                    "com.legend.lowering.DateCtorRule", "com.legend.lowering.KeyedCounts",
                    "com.legend.lowering.Lowerer", "com.legend.lowering.RelationPredicates",
                    "com.legend.lowering.Scalars", "com.legend.lowering.TaggedLists",
                    "com.legend.sql.dialect.CarrierStrategies", "com.legend.sql.dialect.FloatShortestText",
                    "com.legend.sql.dialect.FoldsAsRecursiveCtes", "com.legend.sql.dialect.FrameGuard",
                    "com.legend.testdatagen.TestDataPlanner")),
            // (DateCtorRule joins 2026-11-21, the BE audit 2's S2: a fractional second's whole part below 0 is -1 or less)
            Map.entry("LESS_EQUAL", Set.of(
                    "com.legend.lowering.CalendarAgg", "com.legend.lowering.DateCtorRule", "com.legend.lowering.KeyedCounts",
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
            // LOWER and UPPER join 2026-11-18 on d-int11 18208beaa6 (J20, 222d136808, train 11): legend-engine's H2
            // isNumeric, lower(x) = upper(x), is the edge's (EdgeLoweringForms); the product's writers keep theirs
            Map.entry("LOWER", Set.of(
                    "com.legend.lowering.Scalars",
                    "com.legend.sql.dialect.FloatShortestText", "com.legend.testdatagen.TestDataPlanner")),
            Map.entry("LPAD", Set.of(
                    "com.legend.lowering.DateCtorRule", "com.legend.lowering.Scalars",
                    "com.legend.lowering.SnapshotEnvelope", "com.legend.sql.dialect.StringCountsAsInt")),
            // (MINUS: both sides' no more, 2026-11-25, a shrink -- edge.RelationTextLowering, which wrote it,
            // went with stage B's B2b S5c (a TDS's csv refused by name in the product until the user's ruling of 2026-11-25, S405, a last step's printed by the doors' Java
            // printer); the product's writers keep theirs)
            Map.entry("NOT", Set.of(
                    "com.legend.lowering.CheckedEnvelope", "com.legend.sql.ExistsJoins",
                    "com.legend.lowering.Lowerer", "com.legend.lowering.NullSemantics",
                    "com.legend.lowering.RelationPredicates", "com.legend.lowering.Scalars",
                    "com.legend.lowering.TaggedLists", "com.legend.lowering.VerdictSql",
                    "com.legend.sql.EnumValueTables", "com.legend.sql.dialect.CarrierStrategies",
                    "com.legend.sql.dialect.FloatShortestText", "com.legend.sql.dialect.WholePartitionOrderedSets",
                    "com.legend.testdatagen.TestDataPlanner")),
            // NOT_EQUAL: + sql.dialect.SameValuesForm (2026-11-22, item 3's S4, by the user's word that day ("Yes on list compare",
            // then "Yes, add it"): the portable form of two sides' values the same, moved from VerdictSql into the dialect)
            Map.entry("NOT_EQUAL", Set.of(
                    "com.legend.sql.dialect.SameValuesForm",
                    "com.legend.lowering.CollectionLanes", "com.legend.lowering.DateCtorRule",
                    "com.legend.lowering.NullSemantics", "com.legend.lowering.Scalars", "com.legend.sql.EnumValueTables",
                    "com.legend.testdatagen.TestDataPlanner")),
            Map.entry("OR", Set.of(
                    "com.legend.lowering.ProductRelationText",   // 2026-11-25, B2b S5a: lite's relation toString
                    "com.legend.lowering.CanonicalRenderSql", "com.legend.lowering.DateCtorRule",
                    "com.legend.lowering.EnumSourceValues", "com.legend.lowering.MixedEncoding",
                    "com.legend.lowering.NullSemantics", "com.legend.lowering.Scalars",
                    "com.legend.lowering.TaggedLists", "com.legend.lowering.VerdictSql",
                    "com.legend.sql.DecodeShapes", "com.legend.sql.dialect.AsOfAsPick",
                    "com.legend.sql.dialect.CarrierStrategies", "com.legend.sql.dialect.FrameGuard",
                    "com.legend.testdatagen.TestDataPlanner")),
            Map.entry("PARSE_INT", Set.of("com.legend.lowering.Scalars")),
            Map.entry("REGEXP_REPLACE", Set.of(
                    "com.legend.lowering.AsorReaders", "com.legend.lowering.LetBinding",
                    "com.legend.lowering.LiteralSpelling", "com.legend.lowering.Scalars", "com.legend.lowering.VerdictSql",
                    "com.legend.sql.dialect.CommonSubexpressions")),
            Map.entry("REPLACE", Set.of(
                    "com.legend.lowering.ProductRelationText",   // 2026-11-25, B2b S5a: lite's relation toString
                    "com.legend.lowering.LiteralSpelling",
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
            // (STRPOS: both sides' no more, 2026-11-25, a shrink -- edge.RelationTextLowering, which wrote it,
            // went with stage B's B2b S5c (a TDS's csv refused by name in the product until the user's ruling of 2026-11-25, S405, a last step's printed by the doors' Java
            // printer); the product's writers keep theirs)
            // (SUBSTRING: both sides' no more, 2026-11-25, a shrink -- the edge's toString SQL that wrote it went with
            // stage B's B2b S5a, edge.RelationTextLowering's tdsCell and dateTimeTextTds; the product's writers keep theirs)
            // (DIVIDE, FLOOR, GREATEST, LN, POW and TIMES: both sides' no more -- compiled legend-pure's 2-ULP Float
            // tolerance, the edge's CompiledPureVerdictNumbers, left the database judge with its kind, VerdictNumbers,
            // in A4 of the compatibility program, stage A ratified by the user 2026-11-19; moved down by the user's
            // ruling of 2026-11-19 ~20:45 ("Approved to move guards down"): the host judge is the only judge in
            // compatibility, so the edge writes none of them; the product's writers keep their own)
            // (J20, as LOWER's note)
            Map.entry("UPPER", Set.of("com.legend.lowering.Scalars")));

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

    /** Whether {@code c} is javac's switch map: it holds a SYNTHETIC {@code $SwitchMap$} field (the review of
     *  11de9ae229, 2026-11-18: an ordinary field so named exempted its whole class). */
    static boolean switchMap(JavaClass c) {
        return c.getFields().stream().anyMatch(f -> f.getName().startsWith("$SwitchMap$")
                && f.getModifiers().contains(com.tngtech.archunit.core.domain.JavaModifier.SYNTHETIC));
    }

    /** Whether {@code c} is a renderer or nested in one, at any depth: a dialect -- a subclass of
     *  {@code AnsiSqlRenderer}, the one root of the product's dialects, MongoDb's too -- at its home, or an edge class
     *  implementing {@code SqlDialect} (the edge's own renderers). The review of 091b9bde36, 2026-11-18, MUST-FIX 3: any
     *  class implementing the interface in the dialects' package counted, so a helper nesting one two levels down
     *  passed its reads off as a renderer's. */
    static boolean renderer(JavaClass c) {
        for (java.util.Optional<JavaClass> x = java.util.Optional.of(c); x.isPresent(); x = x.get().getEnclosingClass()) {
            String name = x.get().getName();
            if ((x.get().isAssignableTo(ANSI) && rendererHome(name))
                    || (name.startsWith("com.legend.edge.") && x.get().isAssignableTo(SqlDialect.class.getName()))) {
                return true;
            }
        }
        return false;
    }

    private static final String ANSI = "com.legend.sql.dialect.AnsiSqlRenderer";

    /** What a renderer's code may hand back without carrying a value out: nothing, a primitive, text, or the writer. */
    private static final Set<String> RENDERED = Set.of("void", "boolean", "byte", "char", "short", "int", "long",
            "float", "double", "java.lang.String", "com.legend.sql.dialect.SqlWriter",
            "com.legend.sql.dialect.SqlWriter$Piece");

    /** Whether code unit {@code u} can carry a constant it reads out of its renderer (the review of 091b9bde36,
     *  2026-11-18, MUST-FIX 3): it returns anything but what {@link #RENDERED} names (a function, an Object, a node),
     *  builds a MIR node (a node's constructor, or a call answering one: {@code Call.of}), sets a field that can hold
     *  one, reads a container to store into, calls a method that can be handed it (a parameter that can hold it, or an
     *  array of such: a varargs keeper), or reads its ordinal or name and stores into a field or calls a method with
     *  arguments other than the writer's, text's and the enum's own (the review of the guard line, G, SHOULD 1,
     *  2026-11-18: keepInt(SqlFn.ABS.ordinal()) handed the number to an int parameter, unseen). The unit, not the
     *  value, is read -- ArchUnit traces no data flow -- so an arm reading a number or a name and calling any other
     *  method with arguments is named too, which fails closed. Its limit, named: a keeper reached only through a
     *  parameter typed narrower than the constant can be (none exists: a SqlFn is held as itself, an Enum, an Object
     *  or an array of them) is not traced, and a value computed from the constant other than its ordinal or name
     *  (a switch on it, its hash) is not followed (review F of the guard line, 2026-11-18, SHOULD 1). */
    static boolean carriesOut(com.tngtech.archunit.core.domain.JavaCodeUnit u) {
        if (!RENDERED.contains(u.getRawReturnType().getName())) {
            return true;
        }
        java.util.function.Predicate<JavaClass> node = t -> t.isAssignableTo("com.legend.sql.SqlExpr")
                || t.isAssignableTo("com.legend.sql.SqlSource") || t.isAssignableTo("com.legend.sql.SqlQuery");
        return u.getConstructorCallsFromSelf().stream().anyMatch(k -> node.test(k.getTargetOwner()))
                || u.getMethodCallsFromSelf().stream().anyMatch(k -> node.test(k.getTarget().getRawReturnType()))
                || u.getFieldAccesses().stream().anyMatch(a -> a.getAccessType()
                        == com.tngtech.archunit.core.domain.JavaFieldAccess.AccessType.SET
                        && !a.getTarget().getRawType().isPrimitive()
                        && !a.getTarget().getRawType().getName().equals("java.lang.String"))
                // a container read, to store into (an array, a collection, a map, an Object): the review of dd5deb781f,
                // 2026-11-18, MUST-FIX 2 -- KEPT[0] = SqlFn.ABS and LIST.add(SqlFn.ABS) set no field
                || u.getFieldAccesses().stream().anyMatch(a -> a.getAccessType()
                        == com.tngtech.archunit.core.domain.JavaFieldAccess.AccessType.GET && container(a.getTarget()
                        .getRawType()))
                // a call that can be handed the constant (a parameter an Object, an Enum, a SqlFn, a Comparable): a
                // throw's exception, a setter, a collection's add -- the enum's own methods and equals aside
                || u.getCallsFromSelf().stream().anyMatch(k -> handed(k.getTarget()))
                // the constant carried out as its number or its name, stored in a field of any type (review F of the
                // guard line, 2026-11-18, SHOULD 1: ORD = SqlFn.ABS.ordinal() set a primitive field) or handed to a
                // method of any parameter type (the review of the guard line, G, SHOULD 1, 2026-11-18)
                || u.getMethodCallsFromSelf().stream().anyMatch(k -> Set.of("ordinal", "name", "toString")
                        .contains(k.getName()) && (k.getTargetOwner().getName().equals(SqlFn.class.getName())
                        || k.getTargetOwner().getName().equals("java.lang.Enum")))
                        && (u.getFieldAccesses().stream().anyMatch(a -> a.getAccessType()
                        == com.tngtech.archunit.core.domain.JavaFieldAccess.AccessType.SET)
                        || u.getCallsFromSelf().stream().anyMatch(k -> !k.getTarget().getRawParameterTypes().isEmpty()
                        && !TEXT_OWNERS.contains(k.getTargetOwner().getName())));
    }

    /** The owners whose calls write or build text, or are the enum's own: a number or a name handed to one stays text. */
    private static final Set<String> TEXT_OWNERS = Set.of("com.legend.sql.dialect.SqlWriter",
            "com.legend.sql.dialect.SqlWriter$Piece", "java.lang.String", "java.lang.StringBuilder",
            "com.legend.sql.SqlFn", "java.lang.Enum", "java.lang.Object");

    /** A type that can hold a function's constant stored into it. */
    private static boolean container(JavaClass t) {
        return t.isArray() || t.isAssignableTo("java.util.Collection") || t.isAssignableTo("java.util.Map")
                || t.getName().equals("java.lang.Object");
    }

    /** A call target that can be handed a function's constant and keep it: one with a parameter typed Object, Enum,
     *  SqlFn, Comparable, Serializable or Constable, other than the enum's and Object's own methods and equals. */
    private static boolean handed(com.tngtech.archunit.core.domain.AccessTarget.CodeUnitCallTarget t) {
        String owner = t.getOwner().getName();
        if (owner.equals(SqlFn.class.getName()) || owner.equals("java.lang.Enum") || owner.equals("java.lang.Object")
                || t.getName().equals("equals") || (owner.equals("java.util.Objects") && t.getName().equals("equals"))) {
            return false;
        }
        return t.getRawParameterTypes().stream().anyMatch(SqlFnSortedTest::holdsAConstant);
    }

    /** A parameter type that can hold a function's constant: Object, Enum, SqlFn, Comparable, Serializable, Constable,
     *  or an array of one of them at any depth (review F of the guard line, 2026-11-18, SHOULD 1: keep(Object...) and
     *  keepAll(SqlFn[]) were handed it unseen). */
    private static boolean holdsAConstant(JavaClass p) {
        JavaClass t = p;
        while (t.isArray()) {
            t = t.getComponentType();
        }
        return java.util.Set.of("java.lang.Object", "java.lang.Enum", SqlFn.class.getName(), "java.lang.Comparable",
                "java.io.Serializable", "java.lang.constant.Constable").contains(t.getName());
    }

    /** Where a renderer lives: the dialects' package or the edge's (the review of 11de9ae229, 2026-11-18, MUST-FIX 1: a
     *  class implementing SqlDialect anywhere -- a product planner's nested class -- exempted its reads of an edge-only
     *  function); and this test's own planted renderers, which no jar carries. */
    static boolean rendererHome(String name) {
        return name.startsWith("com.legend.sql.dialect.") || name.startsWith("com.legend.edge.")
                || name.startsWith(SqlFnSortedTest.class.getName() + "$Planted");
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
            // COUNT and MIN: the edge writes them too since 2026-11-18 (E-SQL, the orchestrator's ruling on L6: legend-engine's
            // one-object-bare fold, SnapshotEnvelope.fold, moved to edge.EdgeLoweringForms.oneBare) -- the product's
            // writers as measured that day, SnapshotEnvelope no longer among them
            // COUNT: + sql.dialect.SameValuesForm (2026-11-22, item 3's S4, by the user's word that day ("Yes on list
            // compare", then "Yes, add both entries"): the portable form of two sides' values the same, moved from
            // VerdictSql into the dialect, which owns it)
            Map.entry("COUNT", Set.of(
                    "com.legend.lowering.Aggregates", "com.legend.lowering.CollectionLanes",
                    "com.legend.lowering.GraphAggDecorrelate", "com.legend.lowering.HostSideBatch",
                    "com.legend.lowering.KeyedCounts", "com.legend.lowering.KeyedPivot",
                    "com.legend.lowering.Lowerer", "com.legend.lowering.RelationPredicates",
                    "com.legend.lowering.Render", "com.legend.lowering.TaggedLists",
                    "com.legend.lowering.ToOneGuard", "com.legend.lowering.ValueCollections",
                    "com.legend.lowering.VerdictSql", "com.legend.planner.PlanMaker",
                    "com.legend.sql.dialect.CarrierStrategies", "com.legend.sql.dialect.QuantileOrder",
                    "com.legend.sql.dialect.SameValuesForm", "com.legend.testdatagen.TestDataPlanner")),
            Map.entry("MIN", Set.of(
                    "com.legend.lowering.Aggregates", "com.legend.lowering.Scalars",
                    "com.legend.lowering.VerdictSql", "com.legend.planner.DataSources",
                    "com.legend.sql.dialect.AsOfAsPick", "com.legend.sql.dialect.CarrierStrategies",
                    "com.legend.sql.dialect.ExplosionMarkers")),
            // LIST: + VerdictSql (2026-11-22, item 3's S4 step 2, by the user's word, "Yes, add it": the database
            // judge's typed members carry each side's failing values as a typed LIST, decoded by exec, where they
            // carried a sentence's STRING_AGG; VerdictSql leaves STRING_AGG with step 4's deletion of the sentence SQL)
            Map.entry("LIST", Set.of(
                    "com.legend.lowering.Lowerer", "com.legend.lowering.Scalars", "com.legend.lowering.StampCensus",
                    "com.legend.lowering.ValueCollections", "com.legend.lowering.VerdictSql", "com.legend.sql.SqlExpr",
                    "com.legend.sql.dialect.CarrierStrategies", "com.legend.sql.dialect.QuantileOrder",
                    "com.legend.sql.dialect.WindowAggregatesAsCorrelated")),
            Map.entry("STRING_AGG", Set.of(
                    "com.legend.lowering.ProductRelationText",   // 2026-11-25, B2b S5a: lite's relation toString
                    "com.legend.lowering.Aggregates", "com.legend.lowering.Fold", "com.legend.lowering.Lowerer",
                    "com.legend.lowering.Scalars", "com.legend.lowering.TaggedLists",
                    "com.legend.lowering.ValueCollections",
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
    static final Set<String> EDGE_ONLY_NODES = Set.of();
    // (SqlExpr$DeferredTdsString, legend-pure's TDS string deferred to the database, and SqlExpr$TempTableInSplice,
    // legend-engine's IN-list temp-table splice, left the product's MIR with E-SQL's J40 (9822fd89c8, the orchestrator's
    // ruling (8)); the edge carries its own forms -- emptied 2026-11-18 on train 17's integration)

    /** THE NODES ONLY THE EDGE WRITES THAT THE PRODUCT SHOULD NOT CARRY: open separation work, each counted by its audit
     *  item ({@code EdgeAuditOpenTest.AUDIT_OPEN}), not sorted EDGE_ONLY (the orchestrator's ruling of 2026-11-17, 23:27:
     *  "don't park it as an EDGE_ONLY pin ... COUNT it as open"). Measured EDGE_ONLY; it leaves when the edge carries
     *  its own form or the product makes it. */
    static final Map<String, String> OPEN_NODES = Map.of();
    // (SqlSource$VarSetPlaceholder, J28, left the product's MIR by E-P5's ab0e2ccde -- the edge writes legend-engine's
    // ${var} relation as its own (edge.PlanVariables); closed 2026-11-18, verified against d-int10 5f83349087)

    /** The nodes no class writes. Measured 2026-11-17. */
    static final Set<String> NO_WRITER_NODES = Set.of();

    /** THE REBUILDS OUTSIDE THE REBUILDERS OF A NODE NOT THE PRODUCT'S (the review of 260fdb147f, 2026-11-17: {@link #made}
     *  reads a unit handed a supertype of the node that reads one, by an instance member, an instanceof or a class
     *  object, as a rebuild -- so a product factory written that way would hide a writer of an EDGE_ONLY or NO_WRITER
     *  node): each such (unit, node), exactly, with why it rebuilds. A new one is a row with its dated reason. */
    static final Map<String, String> REBUILDS_OF_NON_PRODUCT_NODES = Map.of();
    // (the alias prefixer's rebuild of a variable set, AliasPrefix.source, gone 2026-11-18 with the node, J28; the
    // generic rewrite's rebuild of a deferred relation text, SqlRewriter.rewriteExpr's, gone 2026-11-18 with the node,
    // E-SQL's J40 9822fd89c8, train 17)

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
     *  returns and wherever it lives. A rebuild remakes a node it is handed (a parameter of the node's type or a
     *  supertype, Object aside) and tells it by what it reads of it: an instance accessor or field (a static member of
     *  the node is no read: ValueCollections' {@code TaggedList.ROW_CHANNEL}), an {@code instanceof} (a pattern's
     *  included) or its class object; or it is {@code this}, an instance method of a supertype of the node
     *  ({@code SqlExpr.withChildren}'s arms, which read nothing of the node: the review of f788dd3230, 2026-11-17). A
     *  unit handed no such node is a factory whatever it reads (MixedEncoding's {@code tl.withPositions} on the list it
     *  made: the re-review of 739f35f35d, 2026-11-17). The codec decodes, so it makes none. Its limits: a rebuild that
     *  reads nothing of a node handed in some other way (a field holding it, an untyped list), or tells it only by a
     *  switch's type pattern (an invokedynamic's argument ArchUnit does not record), reads as a factory -- a writer too
     *  many; and a unit handed a supertype of the node (SqlExpr, SqlSource, an interface or Record the node implements)
     *  that reads ANY instance of the node -- an instance member, an instanceof, its class object, not only of the value
     *  handed -- reads as a rebuild: a writer too few. Such a reading of a node the product does not write is pinned
     *  exactly ({@link #REBUILDS_OF_NON_PRODUCT_NODES}: the review of 260fdb147f, 2026-11-17), so it hides no writer. */
    static Set<String> made(JavaCodeUnit m, Set<String> nodes) {
        if (m.getOwner().getName().equals("com.legend.executionplan.MirJson")) {
            return Set.of();
        }
        Map<String, JavaClass> made = new TreeMap<>();
        Stream.concat(m.getConstructorCallsFromSelf().stream().map(x -> x.getTargetOwner()),
                m.getConstructorReferencesFromSelf().stream().map(x -> x.getTargetOwner()))
                .filter(o -> nodes.contains(o.getName())).forEach(o -> made.put(o.getName(), o));
        Set<String> read = new java.util.HashSet<>();
        m.getMethodCallsFromSelf().stream().filter(x -> !isStatic(x.getTarget().resolveMember()))
                .forEach(x -> read.add(x.getTargetOwner().getName()));
        m.getFieldAccesses().stream().filter(x -> !isStatic(x.getTarget().resolveMember()))
                .forEach(x -> read.add(x.getTargetOwner().getName()));
        m.getInstanceofChecks().forEach(i -> read.add(i.getRawType().getName()));
        m.getReferencedClassObjects().forEach(o -> read.add(o.getRawType().getName()));
        made.values().removeIf(n -> read.contains(n.getName()) && m.getRawParameterTypes().stream()
                .anyMatch(p -> !p.getName().equals("java.lang.Object") && n.isAssignableTo(p.getName())));
        if (!m.getModifiers().contains(com.tngtech.archunit.core.domain.JavaModifier.STATIC)) {
            made.values().removeIf(n -> n.isAssignableTo(m.getOwner().getName()));
        }
        return new TreeSet<>(made.keySet());
    }

    /** Whether a member resolved is static (one unresolved is read as an instance member). */
    private static boolean isStatic(java.util.Optional<? extends com.tngtech.archunit.core.domain.JavaMember> member) {
        return member.map(x -> x.getModifiers().contains(com.tngtech.archunit.core.domain.JavaModifier.STATIC))
                .orElse(false);
    }

    /** Each node's writers over {@code classes}, "node" -> {"product: class", "edge: class"}: in a class
     *  {@code rebuilder} does not name, a code unit that makes the node itself, or a call or reference to a method that
     *  does ({@link #made} both: one level, so a factory returning a supertype, or a helper hosted in a rebuilder, is
     *  seen -- the review of C.13, 2026-11-17; and a unit's own rebuild is not a write wherever it lives -- the
     *  re-review of 739f35f35d, 2026-11-17). */
    static Map<String, Set<String>> nodeWriters(JavaClasses classes, Set<String> nodes, Predicate<String> edge,
                                                Predicate<String> rebuilder) {
        return nodeWriters(classes, nodes, edge, rebuilder, new TreeSet<>());
    }

    /** {@link #nodeWriters(JavaClasses, Set, Predicate, Predicate)}, each unit's own construction that {@link #made}
     *  reads as a rebuild put to {@code rebuilds} as "unit -> node", the unit by its full signature (the third review's
     *  NOTE, 2026-11-17: an overload is its own row); and every construction in a rebuilder's code units, however it
     *  is reached, as "unit -> node" (the third review's M3 and the review of 5911e970e1, 2026-11-17: a factory on a
     *  node's own record, a default method, a chain, a constructor or a static field of a rebuilder was read
     *  nowhere). */
    static Map<String, Set<String>> nodeWriters(JavaClasses classes, Set<String> nodes, Predicate<String> edge,
                                                Predicate<String> rebuilder, Set<String> rebuilds) {
        Map<String, Set<String>> out = new TreeMap<>();
        for (JavaClass c : classes) {
            if (rebuilder.test(c.getName())) {
                // a rebuilder writes no node, but each construction in it -- any code unit's: a default method, a static
                // field's initializer, a node's constructor making another node, a factory however it is reached -- is
                // recorded as a rebuild, so one of a node the product does not write is a row of its own (the review of
                // 5911e970e1, 2026-11-17, MUST-FIX 1: only a parameter-shaped one called from outside was read). The
                // codec decodes; a node's own constructor delegating to itself makes no other node.
                if (!c.getName().equals("com.legend.executionplan.MirJson")) {
                    for (JavaCodeUnit u : c.getCodeUnits()) {
                        Stream.concat(u.getConstructorCallsFromSelf().stream().map(x -> x.getTargetOwner().getName()),
                                u.getConstructorReferencesFromSelf().stream().map(x -> x.getTargetOwner().getName()))
                                .filter(n -> nodes.contains(n)
                                        && !(u instanceof com.tngtech.archunit.core.domain.JavaConstructor
                                        && n.equals(c.getName())))
                                .forEach(n -> rebuilds.add(u.getFullName() + " -> " + n));
                    }
                }
                continue;
            }
            String side = (edge.test(c.getName()) ? "edge: " : "product: ") + c.getName();
            for (JavaCodeUnit u : c.getCodeUnits()) {
                Set<String> made = made(u, nodes);
                made.forEach(n -> out.computeIfAbsent(n, k -> new TreeSet<>()).add(side));
                Stream.concat(u.getConstructorCallsFromSelf().stream().map(x -> x.getTargetOwner().getName()),
                        u.getConstructorReferencesFromSelf().stream().map(x -> x.getTargetOwner().getName()))
                        .filter(n -> nodes.contains(n) && !made.contains(n))
                        .forEach(n -> rebuilds.add(u.getFullName() + " -> " + n));
                Stream.concat(u.getMethodCallsFromSelf().stream().map(x -> x.getTarget().resolveMember()),
                        u.getMethodReferencesFromSelf().stream().map(x -> x.getTarget().resolveMember()))
                        .flatMap(java.util.Optional::stream)
                        .forEach(m -> {
                            made(m, nodes).forEach(n -> out.computeIfAbsent(n, k -> new TreeSet<>()).add(side));
                        });
            }
        }
        return out;
    }

    /** The rebuilds of {@code rebuilds} ("unit -> node") whose node {@code sort} does not hold PRODUCT. */
    static Set<String> rebuildsOfNonProductNodes(Set<String> rebuilds, Map<String, Kind> sort, Set<String> noWriter) {
        Set<String> out = new TreeSet<>();
        for (String r : rebuilds) {
            String node = r.substring(r.indexOf(" -> ") + 4);
            if (sort.get(node) != Kind.PRODUCT || noWriter.contains(node)) {
                out.add(r);
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
        Set<String> rebuilds = new TreeSet<>();
        Map<String, Set<String>> writers = nodeWriters(classes, nodes, SqlFnSortedTest::edgeSide,
                SqlFnSortedTest::nodeRebuilder, rebuilds);
        Set<String> edgeWritten = new TreeSet<>(EDGE_ONLY_NODES);
        edgeWritten.addAll(OPEN_NODES.keySet());
        Map<String, Kind> sort = nodeSort(nodes, edgeWritten);
        Set<String> all = new TreeSet<>(sort.keySet());
        List<String> wrong = misSorted(sort, NO_WRITER_NODES, measured(writers, all));
        // each open node is counted: its audit item stands in the count (EdgeAuditOpenTest)
        assertTrue(EdgeAuditOpenTest.lines().containsAll(OPEN_NODES.values()) && java.util.Collections.disjoint(
                EDGE_ONLY_NODES, OPEN_NODES.keySet()), "an open node's audit item is not counted: " + OPEN_NODES);
        assertEquals(List.of(), wrong, "a node's sort and its writers differ (one the edge alone writes is"
                + " EDGE_ONLY_NODES'; one no class writes is pinned NO_WRITER_NODES):" + explain(wrong, writers));
        assertEquals(new TreeSet<>(REBUILDS_OF_NON_PRODUCT_NODES.keySet()), rebuildsOfNonProductNodes(rebuilds, sort,
                NO_WRITER_NODES), "a construction of a node the product does not write, read as a rebuild: a rebuild is"
                + " a row of REBUILDS_OF_NON_PRODUCT_NODES with its dated reason, a factory is a product writer");
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
                || l.contains("SqlExpr$FloatLit") || l.contains("SqlExpr$TaggedList") || l.contains("SqlExpr$DateLit")
                || l.contains("/SqlExpr.class") || l.contains("/SqlSource"))
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
        // the review of 11de9ae229 (2026-11-18): a class implementing SqlDialect outside the dialects' and the edge's
        // packages is no renderer -- its read of a function is a write
        JavaClasses hide = new ClassFileImporter().importClasses(com.legend.planner.PlantedRendererHide.class,
                com.legend.planner.PlantedRendererHide.R.class);
        assertEquals(2, hide.size(), "the planted classes are imported");
        assertEquals(Map.of("ABS", Set.of("product: com.legend.planner.PlantedRendererHide$R")),
                fnWriters(hide, n -> false), "a renderer by type alone, outside the dialects, writes");
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
                SqlExpr.Group.class.getName(), com.legend.sql.SqlSource.Values.class.getName(),
                SqlExpr.TaggedList.class.getName(), SqlExpr.DateLit.class.getName(),
                SqlExpr.TimeLit.class.getName());
        Set<String> rebuilds = new TreeSet<>();
        Map<String, Set<String>> made = nodeWriters(classes, nodes, SqlFnSortedTest::plantedEdge,
                n -> n.contains("$PlantedRebuilder") || nodeRebuilder(n), rebuilds);
        assertEquals(Map.of(SqlExpr.StringLit.class.getName(), "EDGE_ONLY", SqlExpr.IntLit.class.getName(), "PRODUCT",
                SqlExpr.FloatLit.class.getName(), "PRODUCT", SqlExpr.NullLit.class.getName(), "NO_WRITER",
                SqlExpr.BoolLit.class.getName(), "PRODUCT", SqlExpr.Group.class.getName(), "NO_WRITER",
                com.legend.sql.SqlSource.Values.class.getName(), "NO_WRITER", SqlExpr.TaggedList.class.getName(),
                "PRODUCT", SqlExpr.DateLit.class.getName(), "PRODUCT", SqlExpr.TimeLit.class.getName(), "NO_WRITER"), measured(made, nodes), "a constructor, a"
                + " constructor reference, a reference to a factory and a call to a factory hosted in a rebuilder each"
                + " write, and so does a factory reading a static of its node (TaggedList) or handed none (DateLit); a"
                + " rebuild does not, in a rebuilder or not (NullLit), nor withChildren over this (Group), nor"
                + " a source's own delegating constructor (Values): " + made);
        assertEquals(List.of(SqlExpr.StringLit.class.getName() + ": sorted PRODUCT, measured EDGE_ONLY"),
                misSorted(nodeSort(nodes, Set.of()), Set.of(SqlExpr.NullLit.class.getName(),
                        SqlExpr.Group.class.getName(), com.legend.sql.SqlSource.Values.class.getName(),
                        SqlExpr.TimeLit.class.getName()), measured(made, nodes)));
        // (the node the planted product does not write is TimeLit, a stand-in, since 2026-11-18 (GUARD, train 17's trial
        // merge): the measurement reads the planted classes alone, so any variant SqlExpr's own code never builds serves,
        // and a real one stays when the edge's nodes leave -- TempTableInSplice, the stand-in before, left the MIR with
        // E-SQL's J40 (9822fd89c8); VarSetPlaceholder, the first, with J28)
        // the review of 260fdb147f's scenario: a product factory that tests instanceof and makes a fresh edge-only node
        // reads as a rebuild, so it is named among the rebuilds of a node the product does not write -- an unpinned row,
        // by its full signature; and the third review's M3: the same factory hosted in a rebuilder, called from the
        // product, is named under its own name when it is called
        String temp = SqlExpr.TimeLit.class.getName();
        String r = PlantedRebuilder.class.getName();
        assertEquals(Set.of(PlantedProductWriter.class.getName() + ".temp(com.legend.sql.SqlExpr, java.lang.String)"
                + " -> " + temp, r + ".ensure(com.legend.sql.SqlExpr, java.lang.String) -> " + temp,
                r + ".<clinit>() -> " + temp, r + ".make(java.lang.String) -> " + temp,
                PlantedRebuilderRoot.class.getName() + ".asVar(java.lang.String) -> " + temp,
                PlantedRebuilderWrap.class.getName() + ".<init>(java.lang.String) -> " + temp),
                rebuildsOfNonProductNodes(rebuilds, nodeSort(nodes, Set.of(temp)), Set.of()).stream()
                        .filter(x -> x.startsWith(SqlFnSortedTest.class.getName())).collect(
                                java.util.stream.Collectors.toSet()),
                "each construction of a node the product does not write, in a rebuilder however it is reached (a"
                        + " factory, a static field, a chain's end, a default method, a constructor), is a row");
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
                    PlantedRebuilder.rebuildNull(null), PlantedRebuilder.ensure(null, "v"));
        }

        /** A factory handed a node that reads only a static member of the node it makes: a write. */
        static Object channel(SqlExpr e) {
            return List.of(new SqlExpr.TaggedList(e, e, e, List.of()), SqlExpr.TaggedList.ROW_CHANNEL(0));
        }

        /** A factory handed no node that reads the node it made: a write. */
        static Object dated(List<String> isos) {
            SqlExpr.DateLit d = new SqlExpr.DateLit(isos.get(0), null);
            return d.iso();
        }

        /** A factory that reads like a rebuild: handed a supertype, an instanceof of the node, then a fresh node (the
         *  review of 260fdb147f's scenario): a rebuild to {@link #made}, so a pinned exemption or a writer. */
        static Object temp(SqlExpr s, String v) {
            if (s instanceof SqlExpr.TimeLit) {
                return s;
            }
            return new SqlExpr.TimeLit(v, null);
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

        /** A rebuild in a class no rebuilder entry names: it reads the node it remakes, so it writes none. */
        static Object rebuild(SqlExpr.NullLit n) {
            return new SqlExpr.NullLit(n.type());
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
    abstract static class PlantedRenderer extends com.legend.sql.dialect.PlantedDialectBase {

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

        /** The third review's M3: a factory that reads like a rebuild (handed a supertype, an instanceof of the node, a
         *  fresh node), hosted in a rebuilder: a row. */
        static SqlExpr ensure(SqlExpr s, String v) {
            if (s instanceof SqlExpr.TimeLit) {
                return s;
            }
            return new SqlExpr.TimeLit(v, null);
        }

        /** The review of 5911e970e1 (E4): a rebuilder's static field holding a fresh node: its initializer is a row. */
        static final SqlExpr CONST = new SqlExpr.TimeLit("c", null);

        /** E2: a chain, wrap reaching make: make's construction is a row whoever reaches it. */
        static SqlExpr wrap(String v) {
            return make(v);
        }

        private static SqlExpr make(String v) {
            return new SqlExpr.TimeLit(v, null);
        }
    }

    /** E1: a default method of a rebuilder root that makes a fresh node. */
    @SuppressWarnings("unused")
    interface PlantedRebuilderRoot {
        default SqlExpr asVar(String v) {
            return new SqlExpr.TimeLit(v, null);
        }
    }

    /** E3: a rebuilder record's secondary constructor making another node; its delegation to itself is no row. */
    @SuppressWarnings("unused")
    record PlantedRebuilderWrap(SqlExpr expr) {
        PlantedRebuilderWrap(String v) {
            this(new SqlExpr.TimeLit(v, null));
        }
    }
}
