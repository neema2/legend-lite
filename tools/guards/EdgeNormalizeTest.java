// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.tools.guards;

import com.legend.tools.edgenet.CorpusOverlay;
import com.legend.tools.edgenet.EdgeNormalize;
import com.legend.tools.edgenet.PlatformOverlay;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.Test;

/**
 * THE EDGE GOLDEN NET's PROOF, its normalizer's half (S0 of the presentation program, 2026-11-18). The review of PR
 * #123 (2026-11-19) moved it out of the PCT module, which may depend on no tool that sorts (PctDisciplineTest). It sits
 * in this package because this package may use JUnit: the jar pools' rule (tools/deps/pools.bzl) names no tools/edgenet
 * user, and none is added. Planted changes over relation lines as the PCT recorder writes them (rows separated by
 * U+001E), put through {@link EdgeNormalize#normalized}: what the net must pass passes (a swap within a sort-key tie,
 * the rows of a line with no order), what it must catch moves the line (a row-order change across distinct keys).
 * Each plant is built so a full sort of the rows would answer otherwise, so it fails if the normalizer sorts what it
 * must keep.
 *
 * <p>The lines are {@code testSortCanonicalNullOrdering}'s answer on DuckDB (sorted by {@code id}, nulls last).
 */
class EdgeNormalizeTest {

    private static final String RS = "\u001e";

    private static final String COLUMNS = "columns=[{\"name\":\"id\",\"type\":\"Integer\",\"relationalType\":\"INTEGER\"},"
            + "{\"name\":\"name\",\"type\":\"String\",\"relationalType\":\"VARCHAR(1024)\"}]";

    private static String relation(String order, String... rows) {
        return "testSortCanonicalNullOrdering\t001\tRELATION\t" + COLUMNS + (order == null ? "" : " order=" + order)
                + " rows=" + String.join(RS, rows);
    }

    /** The line with its rows joined as the normalizer joins them, in the order given. */
    private static String joined(String order, String... rows) {
        return relation(order, rows).replace(RS, ",");
    }

    /** The line with its rows fully sorted: what a normalizer that ignored the order would write. */
    private static String fullySorted(String order, String... rows) {
        List<String> sorted = new ArrayList<>(List.of(rows));
        Collections.sort(sorted);
        return joined(order, sorted.toArray(String[]::new));
    }

    private static final String SACHIN = "{\"values\":[1,\"Sachin\"]}";
    private static final String GEORGE = "{\"values\":[2,\"George\"]}";
    private static final String PIERRE = "{\"values\":[null,\"Pierre\"]}";
    private static final String GEORGE_TIED = "{\"values\":[2,\"Anne\"]}";
    /** A name holding a quote, as the recorder writes it: the JSON's {@code \"} with its backslash doubled. */
    private static final String OBRIEN = "{\"values\":[3,\"O\\\\\"Brien\"]}";

    @Test
    void aRowOrderChangeAcrossDistinctKeysMovesTheLine() {
        String recorded = EdgeNormalize.normalized(relation("[\"id\"]", SACHIN, GEORGE, PIERRE));
        String planted = EdgeNormalize.normalized(relation("[\"id\"]", GEORGE, SACHIN, PIERRE));
        assertNotEquals(recorded, planted, "the net sees a row-order change across distinct sort keys");
        assertEquals(joined("[\"id\"]", SACHIN, GEORGE, PIERRE), recorded,
                "a sorted answer keeps the database's order across distinct keys");
        assertNotEquals(fullySorted("[\"id\"]", GEORGE, SACHIN, PIERRE), planted,
                "the planted order is not the fully sorted one, so the plant could not pass by a sort");
    }

    @Test
    void aSwapWithinATiePasses() {
        String recorded = EdgeNormalize.normalized(relation("[\"id\"]", SACHIN, GEORGE, GEORGE_TIED, PIERRE));
        String planted = EdgeNormalize.normalized(relation("[\"id\"]", SACHIN, GEORGE_TIED, GEORGE, PIERRE));
        assertEquals(recorded, planted, "a tie's order is the database's: the net does not see it");
    }

    @Test
    void aKeyHoldingAnEscapedCharacterIsRead() {
        // sorted by name: O"Brien before Sachin, the reverse of their rows' text order ([3,...] after [1,...])
        assertEquals(joined("[\"name\"]", OBRIEN, SACHIN), EdgeNormalize.normalized(relation("[\"name\"]", OBRIEN, SACHIN)),
                "distinct keys keep their order, a key with an escaped quote read as JSON");
        assertNotEquals(fullySorted("[\"name\"]", OBRIEN, SACHIN), joined("[\"name\"]", OBRIEN, SACHIN),
                "the rows' order is not their sorted order, so the plant could not pass by a sort");
    }

    @Test
    void aLineWithNoOrderHasItsRowsSorted() {
        assertEquals(fullySorted(null, SACHIN, GEORGE, PIERRE),
                EdgeNormalize.normalized(relation(null, PIERRE, GEORGE, SACHIN)),
                "an unsorted answer's order is the database's, as before S0");
    }

    @Test
    void aFirstKeyNamingNoColumnSortsTheRows() {
        assertEquals(fullySorted("[\"?\"]", SACHIN, GEORGE, PIERRE),
                EdgeNormalize.normalized(relation("[\"?\"]", PIERRE, GEORGE, SACHIN)),
                "a sort on an expression no column projects cannot be followed, so its rows are sorted");
    }

    @Test
    void theLongestPrefixNamingColumnsIsFollowed() {
        // sorted by id, then by an expression no column projects: the net follows id and sorts within its ties
        String line = relation("[\"id\",\"?\"]", PIERRE, GEORGE, GEORGE_TIED, SACHIN);
        assertEquals(joined("[\"id\",\"?\"]", PIERRE, GEORGE_TIED, GEORGE, SACHIN), EdgeNormalize.normalized(line),
                "across distinct ids the rows keep their order; within id 2's tie they are sorted");
        assertNotEquals(EdgeNormalize.normalized(line),
                EdgeNormalize.normalized(relation("[\"id\",\"?\"]", SACHIN, GEORGE, GEORGE_TIED, PIERRE)),
                "a row-order change across distinct ids moves the line");
    }

    @Test
    void aNonRelationLineIsUnchanged() {
        String value = "testArcCosine\t002\tVALUE\ttype=FLOAT values=Double:3.141592653589793;";
        assertEquals(value, EdgeNormalize.normalized(value), "the normalizer orders a relation's rows and nothing else");
    }

    @Test
    void theScannerReadsTheRecordersJson() {
        assertEquals(List.of("1", "\"a,b\"", "[2,3]", "{\"k\":\"]\"}"),
                EdgeNormalize.elements("[1,\"a,b\",[2,3],{\"k\":\"]\"}]").orElseThrow(),
                "top-level elements, not split inside a string, an array or an object");
        assertEquals("O\"Brien\\né", EdgeNormalize.text("\"O\\\"Brien\\\\n\\u00e9\""),
                "a string's escapes read");
    }

    // THE PLATFORM OVERLAY (2026-11-19, the program owner's call on PR #123's first Windows run): an OS's rows rewrite
    // that OS's last-place maths to Linux's on that OS only; a stale row or another value fails; on every other OS the
    // row's Linux value is checked against the recording, so the overlay cannot drift from the golden

    private static final String SINH = "testSinH_Eval\t003\tVALUE\ttype=FLOAT values=Double:%s;";

    private static List<PlatformOverlay.Row> windowsRow() {
        return PlatformOverlay.parse("windows", List.of("suite\ttest\tcase\tdatabases\tfunction\tlinux\twindows",
                "standard\ttestSinH_Eval\t003\tduckdb,sqlite\tsinh\t-0.8423951260776953\t-0.8423951260776952"));
    }

    @Test
    void anOverlayRewritesItsOsValueToLinuxsOnItsOs() {
        List<String> windows = List.of(SINH.formatted("-0.8423951260776952"), "testOther\t001\tVALUE\tx");
        assertEquals(List.of(SINH.formatted("-0.8423951260776953"), "testOther\t001\tVALUE\tx"),
                PlatformOverlay.applied(windows, "sqlite_standard_answers.tsv", windowsRow(), "windows"),
                "on Windows the row's line takes Linux's value, and no other line moves");
    }

    @Test
    void onAnotherOsTheRowsLinuxValueIsChecked() {
        List<String> linux = List.of(SINH.formatted("-0.8423951260776953"));
        assertEquals(linux, PlatformOverlay.applied(linux, "duckdb_standard_answers.tsv", windowsRow(), "linux"),
                "on Linux a row whose Linux value is the recording's changes nothing");
        assertThrows(IllegalStateException.class, () -> PlatformOverlay.applied(
                List.of(SINH.formatted("-0.8423951260776954")), "duckdb_standard_answers.tsv", windowsRow(), "linux"),
                "a row whose Linux value is not the recording's (the golden's, on Linux) fails: the overlay drifted");
    }

    @Test
    void aStaleRowFails() {
        assertThrows(IllegalStateException.class, () -> PlatformOverlay.applied(
                List.of("testOther\t001\tVALUE\tx"), "duckdb_standard_answers.tsv", windowsRow(), "windows"),
                "a row that finds no line fails on its OS");
        assertThrows(IllegalStateException.class, () -> PlatformOverlay.applied(
                List.of("testOther\t001\tVALUE\tx"), "duckdb_standard_answers.tsv", windowsRow(), "linux"),
                "and on every other OS");
    }

    @Test
    void anotherValueOnItsOsFails() {
        assertThrows(IllegalStateException.class, () -> PlatformOverlay.applied(
                List.of(SINH.formatted("-0.8423951260776999")), "duckdb_standard_answers.tsv", windowsRow(), "windows"),
                "a line holding neither value is a new difference, and fails");
    }

    private static List<PlatformOverlay.Row> macosRow() {
        return PlatformOverlay.parse("macos", List.of("suite\ttest\tcase\tdatabases\tfunction\tlinux\tmacos",
                "standard\ttestSinH_Eval\t003\tduckdb,sqlite\tsinh\t-0.8423951260776953\t-0.8423951260776952"));
    }

    // a fact two OSes both move (2026-11-19: macOS and Windows both move testSinH_Eval 003): each OS's own row rewrites
    // its line before the other OS's row checks Linux's value, whichever overlay is listed first
    @Test
    void aFactTwoOsesMoveIsAppliedOnEachWhicheverOverlayComesFirst() {
        List<PlatformOverlay.Row> windowsFirst = new java.util.ArrayList<>(windowsRow());
        windowsFirst.addAll(macosRow());
        List<PlatformOverlay.Row> macosFirst = new java.util.ArrayList<>(macosRow());
        macosFirst.addAll(windowsRow());
        List<String> moved = List.of(SINH.formatted("-0.8423951260776952"));
        List<String> linux = List.of(SINH.formatted("-0.8423951260776953"));
        for (List<PlatformOverlay.Row> rows : List.of(windowsFirst, macosFirst)) {
            assertEquals(linux, PlatformOverlay.applied(moved, "duckdb_standard_answers.tsv", rows, "macos"),
                    "on macOS its row rewrites the line, then Windows' row finds Linux's value");
            assertEquals(linux, PlatformOverlay.applied(moved, "duckdb_standard_answers.tsv", rows, "windows"),
                    "on Windows likewise");
            assertEquals(linux, PlatformOverlay.applied(linux, "duckdb_standard_answers.tsv", rows, "linux"),
                    "on Linux both rows' Linux value is the recording's");
        }
    }

    @Test
    void anOverlayTouchesOnlyItsGoldens() {
        List<String> lines = List.of(SINH.formatted("-0.8423951260776952"));
        assertEquals(lines, PlatformOverlay.applied(lines, "duckdb_essential_answers.tsv", windowsRow(), "windows"),
                "another suite: the row is not this golden's");
        assertEquals(lines, PlatformOverlay.applied(lines, "h2_standard_answers.tsv", windowsRow(), "windows"),
                "another database: the row is not this golden's");
        assertEquals(lines, PlatformOverlay.applied(lines, "channel_b_duckdb_standard_outcomes.tsv", windowsRow(),
                "windows"), "an outcomes file: overlays are for answers");
    }

    // THE CORPUS's PLATFORM OVERLAY (2026-11-26, the user's "yes Macos"): on its OS a row's test FAILS on its last-place
    // maths (the test's literal is Linux's value); its golden lines are put back only when the recording shows exactly
    // that flip; on every other OS the row's test must pass in the recording, so a stale row fails. The lines are
    // acos::testProject's, as PR #141's macOS gate run (45788970603) recorded them and as the Linux golden holds them.

    private static final String ACOS = "meta::relational::tests::mapping::sqlFunction::acos::testProject";

    private static final List<String> LINUX_LEDGER = List.of(
            "meta::other::before\t0\tmeta::pure::functions::asserts::assertEquals\tPASS",
            ACOS + "\t0\tmeta::pure::functions::asserts::assertSameElements\tPASS",
            ACOS + "\t1\tmeta::pure::functions::asserts::assertEquals\tPASS",
            "meta::other::after\t0\tmeta::pure::functions::asserts::assertEquals\tPASS");

    private static final String MACOS_FAIL = ACOS + "\t0\tmeta::pure::functions::asserts::assertSameElements\tFAIL\t"
            + "AssertFailed: expected: [[%s], [0.0]], actual: [[%s], [0.0]]";

    private static List<String> macosLedger(String expected, String actual) {
        return List.of(LINUX_LEDGER.get(0), MACOS_FAIL.formatted(expected, actual), LINUX_LEDGER.get(3));
    }

    private static List<CorpusOverlay.Row> acosRow() {
        return CorpusOverlay.parse("macos", List.of("test\tdatabases\tfunction\tlinux\tmacos",
                ACOS + "\tduckdb\tacos\t1.0471975511965979\t1.0471975511965976"));
    }

    private static List<String> corpus(List<String> recording, List<String> golden, CorpusOverlay.Kind kind,
            String database, List<CorpusOverlay.Row> rows, String os) {
        return CorpusOverlay.applied(recording, golden, kind, database, rows, os);
    }

    @Test
    void aCorpusRowPutsTheGoldensLinesBackOnItsOs() {
        assertEquals(LINUX_LEDGER, corpus(macosLedger("1.0471975511965979", "1.0471975511965976"), LINUX_LEDGER,
                CorpusOverlay.Kind.LEDGER, "duckdb", acosRow(), "macos"),
                "on macOS the one failing assert gives way to the golden's two PASS lines, in place");
        List<String> roster = List.of("meta::a::x", ACOS, "meta::z::y");
        assertEquals(List.of("meta::a::x", "meta::z::y"), corpus(roster, List.of("meta::a::x", "meta::z::y"),
                CorpusOverlay.Kind.FAIL_ROSTER, "duckdb", acosRow(), "macos"), "the fail roster loses the test");
        List<String> register = List.of("unordered-chain meta::a::x", "unordered-chain meta::z::y");
        List<String> golden = List.of("unordered-chain meta::a::x", "unordered-chain " + ACOS,
                "unordered-chain meta::z::y");
        assertEquals(golden, corpus(register, golden, CorpusOverlay.Kind.UNORDERED_REGISTER, "duckdb", acosRow(),
                "macos"), "the unordered register gets its line back, in its sorted place");
    }

    @Test
    void aCorpusRowIsInertOnAnotherOsAndAnotherLane() {
        assertEquals(LINUX_LEDGER, corpus(LINUX_LEDGER, LINUX_LEDGER, CorpusOverlay.Kind.LEDGER, "duckdb", acosRow(),
                "linux"), "on Linux a row whose test passes changes nothing");
        List<String> moved = macosLedger("1.0471975511965979", "1.0471975511965976");
        assertEquals(moved, corpus(moved, LINUX_LEDGER, CorpusOverlay.Kind.LEDGER, "h2", acosRow(), "macos"),
                "another lane: the row is not this golden's");
    }

    @Test
    void aStaleCorpusRowFails() {
        assertThrows(IllegalStateException.class, () -> corpus(LINUX_LEDGER, LINUX_LEDGER, CorpusOverlay.Kind.LEDGER,
                "duckdb", acosRow(), "macos"), "on macOS a row whose test passes after all is stale");
        List<String> gone = List.of(LINUX_LEDGER.get(0), LINUX_LEDGER.get(3));
        assertThrows(IllegalStateException.class, () -> corpus(gone, gone, CorpusOverlay.Kind.LEDGER, "duckdb",
                acosRow(), "linux"), "on Linux a row whose test is not in the recording is stale");
        assertThrows(IllegalStateException.class, () -> corpus(List.of(ACOS), List.of(ACOS),
                CorpusOverlay.Kind.FAIL_ROSTER, "duckdb", acosRow(), "linux"),
                "on Linux a row whose test fails there too is stale (the test moved)");
        List<String> failing = macosLedger("1.0471975511965979", "1.0471975511965976");
        assertThrows(IllegalStateException.class, () -> corpus(failing, failing, CorpusOverlay.Kind.LEDGER, "duckdb",
                acosRow(), "linux"), "on Linux a failing test is no row's to hide");
    }

    @Test
    void anotherCorpusFailureOnItsOsFails() {
        assertThrows(IllegalStateException.class, () -> corpus(macosLedger("1.0471975511965979", "1.0471975511965975"),
                LINUX_LEDGER, CorpusOverlay.Kind.LEDGER, "duckdb", acosRow(), "macos"),
                "a failure naming another value is a new difference, and fails");
        assertThrows(IllegalStateException.class, () -> corpus(macosLedger("1.0471975511965978", "1.0471975511965976"),
                LINUX_LEDGER, CorpusOverlay.Kind.LEDGER, "duckdb", acosRow(), "macos"),
                "a failure whose expected value is not the row's Linux value fails: the test's literal moved");
        List<String> otherFailure = List.of(LINUX_LEDGER.get(0), ACOS
                + "\t0\tmeta::pure::functions::asserts::assertSameElements\tFAIL\tDataError: no such table",
                LINUX_LEDGER.get(3));
        assertThrows(IllegalStateException.class, () -> corpus(otherFailure, LINUX_LEDGER, CorpusOverlay.Kind.LEDGER,
                "duckdb", acosRow(), "macos"), "a failure of another kind fails");
        assertThrows(IllegalStateException.class, () -> corpus(List.of("unordered-chain " + ACOS),
                List.of("unordered-chain " + ACOS), CorpusOverlay.Kind.UNORDERED_REGISTER, "duckdb", acosRow(),
                "macos"), "a register that still holds the test's line does not show the flip");
    }

    @Test
    void aCorpusOverlayRefusesAMalformedRow() {
        assertThrows(IllegalArgumentException.class, () -> CorpusOverlay.parse("macos", List.of(
                "test\tdatabases\tfunction\tlinux\tmacos", ACOS + "\tpostgres\tacos\t1.0\t2.0")),
                "a lane the corpus has not: no normalizer would ever find the row stale");
        assertThrows(IllegalArgumentException.class, () -> CorpusOverlay.parse("macos", List.of(
                "test\tdatabases\tfunction\tlinux\tmacos", ACOS + "\tduckdb\tacos\t1.0\t2.0",
                ACOS + "\tduckdb\tacos\t1.0\t3.0")), "one test twice");
        assertThrows(IllegalArgumentException.class, () -> CorpusOverlay.parse("macos", List.of(
                "suite\ttest\tcase\tdatabases\tfunction\tlinux\tmacos")), "PCT's header is not the corpus's");
    }
}
