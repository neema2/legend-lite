// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;
import com.legend.sql.dialect.SqlDialect;
import com.tngtech.archunit.core.domain.JavaClass;
import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.domain.JavaCodeUnit;
import com.tngtech.archunit.core.domain.JavaModifier;
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

    /** The classes that name every function to type or decode it and write none. Beside them, two kinds of class are
     *  no writer: one assignable to {@code SqlDialect}, with its nested classes (a renderer, the product's or the edge's:
     *  an arm per function), and javac's switch map (a dispatch's table). */
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

    private static boolean renderer(JavaClasses classes, String top) {
        return classes.contain(top) && classes.get(top).isAssignableTo(SqlDialect.class.getName());
    }

    /** Each function's writers over {@code classes}, "fn" -> {"product: class", "edge: class"}: a read of its constant
     *  ({@code SqlFn.X}) in a class not {@link #FN_READERS}', a renderer's or a switch map. Its limits: a comparison
     *  ({@code fn == SqlFn.X}) reads as a write, so a product class comparing an edge-only function makes it read
     *  PRODUCT (the census counts that naming, {@link EdgeSeparationTest#EDGE_ONLY_SQLFN}, once it is sorted EDGE_ONLY);
     *  a name the field accesses do not show -- {@code SqlFn.valueOf}, a qualified enum pattern's bootstrap -- is unseen
     *  (neither is in core's main sources, 2026-11-17). */
    static Map<String, Set<String>> fnWriters(JavaClasses classes, Predicate<String> edge) {
        Map<String, Set<String>> out = new TreeMap<>();
        for (JavaClass c : classes) {
            String top = top(c.getName());
            if (FN_READERS.containsKey(top) || c.isAssignableTo(SqlDialect.class.getName()) || renderer(classes, top)
                    || switchMap(c)) {
                continue;
            }
            String side = (edge.test(c.getName()) ? "edge: " : "product: ") + c.getName();
            c.getFieldAccessesFromSelf().forEach(a -> {
                if (a.getTarget().getOwner().getName().equals(SqlFn.class.getName())) {
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

    /** Each node's writers over {@code classes}, "node" -> {"product: class", "edge: class"}: a constructor call or
     *  reference, or a static factory of the node's own returning it, in a class not {@link #NODE_REBUILDERS}'. */
    static Map<String, Set<String>> nodeWriters(JavaClasses classes, Set<String> nodes, Predicate<String> edge) {
        Map<String, Set<String>> out = new TreeMap<>();
        for (JavaClass c : classes) {
            String top = top(c.getName());
            if (NODE_REBUILDERS.containsKey(top)) {
                continue;
            }
            String side = (edge.test(c.getName()) ? "edge: " : "product: ") + c.getName();
            for (JavaCodeUnit u : c.getCodeUnits()) {
                Stream.concat(u.getConstructorCallsFromSelf().stream().map(x -> x.getTargetOwner().getName()),
                        u.getConstructorReferencesFromSelf().stream().map(x -> x.getTargetOwner().getName()))
                        .filter(nodes::contains)
                        .forEach(n -> out.computeIfAbsent(n, k -> new TreeSet<>()).add(side));
                Stream.concat(u.getMethodCallsFromSelf().stream().map(x -> x.getTarget()),
                        u.getMethodReferencesFromSelf().stream().map(x -> x.getTarget()))
                        .filter(t -> nodes.contains(t.getOwner().getName())
                                && t.getRawReturnType().getName().equals(t.getOwner().getName())
                                && t.resolveMember().map(m -> m.getModifiers().contains(JavaModifier.STATIC))
                                .orElse(false))
                        .forEach(t -> out.computeIfAbsent(t.getOwner().getName(), k -> new TreeSet<>()).add(side));
            }
        }
        return out;
    }

    private static Map<String, Kind> nodeSort(Set<String> nodes, Set<String> edgeOnly) {
        Map<String, Kind> out = new TreeMap<>();
        nodes.forEach(n -> out.put(n, edgeOnly.contains(n) ? Kind.EDGE_ONLY : Kind.PRODUCT));
        edgeOnly.stream().filter(n -> !nodes.contains(n)).forEach(n -> out.put(n, Kind.EDGE_ONLY));
        return out;
    }

    @Test
    void theNodesAreSortedByTheirWriters() {
        Set<String> nodes = leaves(SqlExpr.class);
        assertTrue(nodes.size() > 40, "the nodes are read: " + nodes.size());
        JavaClasses classes = importJars("legend.product.jars", "legend.authored.jars", "legend.edge.jars");
        Map<String, Set<String>> writers = nodeWriters(classes, nodes, SqlFnSortedTest::edgeSide);
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
        return new ClassFileImporter().withImportOption(l -> l.contains("SqlFnSortedTest"))
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
        List<String> names = List.of("ABS", "DAYNAME", "MONTHNAME", "TRANSLATE");
        assertEquals(Map.of("ABS", "PRODUCT", "DAYNAME", "EDGE_ONLY", "MONTHNAME", "NO_WRITER", "TRANSLATE",
                "NO_WRITER"), measured(writers, names), "a switch's map and a renderer write nothing: " + writers);
        Map<String, Kind> wrongSort = sorted(Set.of("ABS", "DAYNAME", "MONTHNAME"), Set.of("TRANSLATE"));
        assertEquals(List.of("DAYNAME: sorted PRODUCT, measured EDGE_ONLY", "TRANSLATE: sorted EDGE_ONLY, measured"
                + " NO_WRITER"), misSorted(wrongSort, Set.of("MONTHNAME"), measured(writers, names)));
        assertEquals(List.of("TRANSLATE: pinned NO_WRITER, sorted EDGE_ONLY (no writer is the product's: its dialects"
                + " render it)"), misSorted(wrongSort, Set.of("MONTHNAME", "TRANSLATE"), Map.of()));
        assertEquals(List.of(), misSorted(sorted(Set.of("ABS", "MONTHNAME", "TRANSLATE"), Set.of("DAYNAME")),
                Set.of("MONTHNAME", "TRANSLATE"), measured(writers, names)), "the right sort passes");

        Set<String> nodes = Set.of(SqlExpr.StringLit.class.getName(), SqlExpr.IntLit.class.getName(),
                SqlExpr.FloatLit.class.getName(), SqlExpr.NullLit.class.getName());
        Map<String, Set<String>> made = nodeWriters(classes, nodes, SqlFnSortedTest::plantedEdge);
        assertEquals(Map.of(SqlExpr.StringLit.class.getName(), "EDGE_ONLY", SqlExpr.IntLit.class.getName(), "PRODUCT",
                SqlExpr.FloatLit.class.getName(), "PRODUCT", SqlExpr.NullLit.class.getName(), "NO_WRITER"),
                measured(made, nodes), "a constructor, a constructor reference and a static factory each write: " + made);
        assertEquals(List.of(SqlExpr.StringLit.class.getName() + ": sorted PRODUCT, measured EDGE_ONLY"),
                misSorted(nodeSort(nodes, Set.of()), Set.of(SqlExpr.NullLit.class.getName()), measured(made, nodes)));
    }

    /** Planted product body: a function's constant read; a node made by a constructor reference and a factory. */
    @SuppressWarnings("unused")
    static final class PlantedProductWriter {
        static Object write() {
            return SqlFn.ABS;
        }

        static Object make() {
            java.util.function.LongFunction<SqlExpr.IntLit> f = SqlExpr.IntLit::new;
            return List.of(f.apply(1), SqlExpr.FloatLit.of(1.0, SqlExpr.FloatLit.Kind.DOUBLE));
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

    /** Planted renderer: an arm per function (no write). */
    @SuppressWarnings("unused")
    abstract static class PlantedRenderer implements SqlDialect {
        static Object arm() {
            return SqlFn.TRANSLATE;
        }
    }
}
