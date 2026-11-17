// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

import com.legend.sql.SqlFn;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * EVERY SQL FUNCTION OF THE MIR SORTED, PRODUCT OR EDGE-ONLY (the GUARD stream, step 2's C.13; the edge audit of
 * 2026-11-17, section C.13: "one exact map from SqlFn.values() to PRODUCT or EDGE_ONLY"). The census counts a product
 * class naming an edge-only {@link SqlFn} ({@link EdgeSeparationTest#EDGE_ONLY_SQLFN}), and
 * {@code EdgeOnlyNodesCensusTest} holds the product's dialects refusing each by name: both read a list a new function
 * never joined -- an upstream node added to the MIR under a new name was counted nowhere. Here every constant of
 * {@code SqlFn.values()} is sorted, exactly: a function added fails until it is sorted, and the two lists the other
 * guards read are held equal to this map's EDGE_ONLY half (one source, a rename or a second list caught).
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
        Map<String, Kind> out = new TreeMap<>();
        PRODUCT.forEach(f -> out.put(f, Kind.PRODUCT));
        EDGE_ONLY.forEach(f -> {
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

    /** The census's list and the dialect census's list are this map's EDGE_ONLY half, exactly. */
    @Test
    void theOtherGuardsReadThisMapsEdgeOnlyHalf() {
        assertEquals(EDGE_ONLY, EdgeSeparationTest.EDGE_ONLY_SQLFN, "EdgeSeparationTest.EDGE_ONLY_SQLFN");
        assertEquals(EDGE_ONLY, EdgeOnlyNodesCensusTest.EDGE_ONLY.stream().map(Enum::name)
                .collect(Collectors.toSet()), "EdgeOnlyNodesCensusTest.EDGE_ONLY");
    }

    /** Planted: a function added and one gone are each found. */
    @Test
    void theSortBites() {
        Map<String, Kind> planted = new TreeMap<>(Map.of("AND", Kind.PRODUCT, "GONE_FN", Kind.PRODUCT,
                "DATETIME_PRINT", Kind.EDGE_ONLY));
        assertEquals(List.of("- GONE_FN", "+ UPSTREAM_PRINT"), unsorted(planted, List.of("AND", "DATETIME_PRINT",
                "UPSTREAM_PRINT")));
    }
}
