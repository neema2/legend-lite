// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0
package com.legend.testplan;


import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.legend.ExecuteOptions;
import com.legend.database.Databases;
import com.legend.error.AssertFailed;
import com.legend.testbackend.StorelessRuntime;
import com.legend.testbackend.TestBackends;
import com.legend.testing.Program;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.Test;

/**
 * THE ASSERT BAR (the user, 2026-11-04: "anyone can write a NEW test function using any of the assert* constructs and
 * they should just do what they say they do on the tin"; docs/ASSERT_NODES_PLAN_2026_11_04.md): tests a developer could
 * reasonably write, each with upstream's outcome -- a pass, or a failure in upstream's exact words (read in the pinned
 * legend-pure, essential/tests/*.pure) -- under the host judge, the database judge alone and BOTH alike.
 *
 * <p>A RATCHET: {@link #GAPS} names the probes that miss the bar today. A probe outside it that misses fails the test;
 * a probe inside it that now meets the bar fails it too, until its line is deleted -- the list only shrinks, each line
 * deleted by the change that closes it.
 */
class AssertBarTest {
    private static final String MODEL = """
            ###Pure
            Class t::Person { name: String[1]; }
            Class t::Employee extends t::Person {}
            function t::check(x: Integer[1]): Boolean[1] { assertEquals(2, $x) }
            function t::check2(x: Integer[1]): Boolean[1] { assertEquals(2, $x); true; }
            """;

    /** A probe: its body, and upstream's outcome -- null a pass, else the failure's exact message. */
    private record Probe(String id, String body, String failure) { }

    private static final List<Probe> PROBES = List.of(
            new Probe("assert-custom-message", "{|assert(1 == 2, 'one is not two')}", "one is not two"),
            new Probe("assertFalse-custom-message", "{|assertFalse(1 == 1, 'was true')}", "was true"),
            new Probe("assert-computed-message", "{|assert(false, | 'computed ' + toString(1 + 1))}", "computed 2"),
            new Probe("assertEquals-custom-message", "{|assertEquals(1, 2, 'custom')}", "custom"),
            new Probe("assertEquals-format-message", "{|assertEquals(1, 2, 'got %s', [2])}", "got 2"),
            new Probe("forAll-zip-assert-message",
                    "{|[1, 2]->zip([2, 1])->forAll(p | assert($p.first < $p.second, 'm'))}", "m"),
            new Probe("assertNotEq-holds", "{|assertNotEq(1, 2)}", null),
            new Probe("assertNotEq-fails", "{|assertNotEq(1, 1)}", "1 should not equal 1"),
            new Probe("assertNotSize-holds", "{|assertNotSize([1, 2], 3)}", null),
            new Probe("assertNotContains-holds", "{|assertNotContains([1, 2], 3)}", null),
            new Probe("one-statement-helper-holds", "{|t::check(1 + 1)}", null),
            new Probe("one-statement-helper-fails", "{|t::check(3)}", "\nexpected: 2\nactual:   3"),
            new Probe("two-statement-helper-fails", "{|t::check2(3)}", "\nexpected: 2\nactual:   3"),
            new Probe("assert-in-an-if", "{|if(1 == 1, |assertEquals(1, 1), |assertEquals(1, 2))}", null),
            // an assert under an if (lite's plan's Branch node, step 4): the side not taken is never evaluated, as
            // upstream never evaluates it -- the else side's toOne() over nothing would fail if it ran
            new Probe("assert-in-an-if-fails", "{|if(1 == 2, |assertEquals(1, 1), |assertEquals(1, 2))}",
                    "\nexpected: 1\nactual:   2"),
            new Probe("if-side-not-taken-never-runs",
                    "{|if(1 == 1, |assert(true), |assertEquals(1, [1, 2]->filter(x | $x > 5)->toOne()))}", null),
            new Probe("if-over-a-let", "{|let x = 2; if($x > 1, |assertEquals(2, $x), |assertEquals(1, $x));}", null),
            new Probe("if-over-a-query",
                    "{|if([1, 2, 3]->filter(x | $x > 1)->size() == 2, |assertEquals(1, 1), |assertEquals(1, 2))}",
                    null),
            new Probe("asserts-around-an-if",
                    "{|assertEquals(1, 1); if(2 > 1, |assertEquals('a', 'a'), |assert(false)); assertEquals(2, 3);}",
                    "\nexpected: 2\nactual:   3"),
            new Probe("nested-if",
                    "{|if(1 == 1, |if(2 == 3, |assert(false, 'inner then'), |assert(false, 'inner else')),"
                            + " |assert(true))}", "inner else"),
            new Probe("if-with-a-constant-then", "{|if(1 == 1, |true, |assert(false, 'not taken')); assert(true);}",
                    null),
            new Probe("if-with-a-constant-side", "{|if(1 == 1, |assertEquals(1, 2), |true); true;}",
                    "\nexpected: 1\nactual:   2"),
            new Probe("assert-bound-by-a-let", "{|let ok = assertEquals(1, 1); assert($ok);}", null),
            new Probe("forAll-over-range", "{|range(1, 4)->forAll(i | assertEquals($i, $i))}", null),
            // per element, the operands computed by the database (step 5): the first failing element in upstream's
            // words, each operand spelled by its own kind, an empty source holding
            new Probe("forAll-over-range-fails", "{|range(1, 4)->forAll(i | assertEquals($i, 2))}",
                    "\nexpected: 1\nactual:   2"),
            // register S139 (the user's ruling, 2026-11-05): lite compares a Decimal by value, its scale dropped
            new Probe("forAll-decimal-scale", "{|range(1, 3)->forAll(i | assertEquals(3.0D, 3.00D))}", null),
            new Probe("forAll-decimal-value", "{|range(1, 3)->forAll(i | assertEquals(3.0D, 3.10D))}",
                    "\nexpected: 3.0D\nactual:   3.10D"),
            new Probe("forAll-decimal-vs-integer", "{|range(1, 3)->forAll(i | assertEquals(1D, $i))}",
                    "\nexpected: 1D\nactual:   1"),
            new Probe("forAll-over-nothing", "{|[1, 2]->filter(x | $x > 5)->forAll(i | assertEquals($i, 99))}", null),
            new Probe("forAll-zip-computed-arguments",
                    "{|[1, 2]->zip([1, 2])->forAll(p | assertEquals($p.first + 1, $p.second + 1))}", null),
            new Probe("assertInstanceOf-custom-message", "{|assertInstanceOf(1, Integer, 'msg')}", null),
            new Probe("assertInstanceOf-mixed-element", "{|assertInstanceOf([1, 2.5]->at(0), Integer)}", null),
            new Probe("assertIs-dynamic-type",
                    "{|assertIs(t::Employee, [^t::Employee(name='a'), ^t::Person(name='b')]->at(0)->type())}", null),
            new Probe("assertIs-picked-element",
                    "{|assertIs(t::Person, [^t::Employee(name='a'), ^t::Person(name='b')]->at(1)->type())}", null),
            new Probe("assertIs-picked-element-fails",
                    "{|assertIs(t::Person, [^t::Employee(name='a'), ^t::Person(name='b')]->at(0)->type())}",
                    "\nexpected: t::Person\nactual:   t::Employee"),
            new Probe("concatenate-keeps-order", "{|assertEquals([3, 1, 2], [1, 2]->concatenate([3]))}",
                    "\nexpected: [3, 1, 2]\nactual:   [1, 2, 3]"),
            new Probe("union-keeps-order", "{|assertEquals([3, 1, 2], [1, 2]->union([3]))}",
                    "\nexpected: [3, 1, 2]\nactual:   [1, 2, 3]"),
            // exact, as upstream (S132's float tokens are backwards-compatibility mode's alone: ExecuteOptions
            // .withEngineCompat, the PCT and corpus harnesses; the user, 2026-11-04)
            new Probe("float-looking-strings", "{|assertEquals('x=1.0000000000000002', 'x=1.0')}",
                    "\nexpected: 'x=1.0000000000000002'\nactual:   'x=1.0'"),
            new Probe("decimal-scale", "{|assertEquals(3.0D, 3.00D)}", null),
            new Probe("decimal-value", "{|assertEquals(3.0D, 3.10D)}", "\nexpected: 3.0D\nactual:   3.10D"),
            new Probe("integer-vs-float", "{|assertEquals(1, 1.0)}", "\nexpected: 1\nactual:   1.0"),
            // the type is part of the value in lite's own runs (register S127, the user's ruling of 2026-11-04;
            // backwards-compatibility mode compares an Integer and a Decimal by text: below)
            new Probe("decimal-vs-integer", "{|assertEquals(1D, 1)}", "\nexpected: 1D\nactual:   1"),
            new Probe("float-vs-decimal", "{|assertEquals(1.5, 1.5D)}", "\nexpected: 1.5\nactual:   1.5D"),
            new Probe("assertNotEquals-across-kinds", "{|assertNotEquals(3, 3D)}", null),
            new Probe("mixed-list", "{|assertEquals([1, 'a', 2.5], [1, 'a', 2.5])}", null),
            new Probe("empty-vs-filtered", "{|assertEquals([], [1, 2]->filter(x | $x > 5))}", null),
            new Probe("assertError-let-message", "{|let m = 'boom'; assertError(|fail($m), $m);}", null),
            new Probe("assertError-over-a-failing-assert",
                    "{|assertError(|assertEquals(1, 2), '\\nexpected: 1\\nactual:   2')}", null),
            // an assert's failure is the error assertError matches, as upstream's assertError catches it
            new Probe("assertError-over-a-holding-assert", "{|assertError(|assertEquals(1, 1), 'boom')}",
                    "No error was thrown"),
            new Probe("assertError-over-a-different-failure", "{|assertError(|assert(false, 'one'), 'two')}",
                    "Execution error message mismatch.\nThe actual message was \"one\"\nwhere the expected"
                            + " message was:\"two\""),
            new Probe("assertEmpty-holds", "{|assertEmpty([1, 2]->filter(x | $x > 5))}", null),
            new Probe("assertSize-fails", "{|assertSize([1, 2], 3)}", "expected size: 3, actual size: 2"),
            new Probe("assertContains-fails", "{|assertContains([1, 2], 3)}", "[1, 2] does not contain 3"),
            new Probe("assertSameElements-holds", "{|assertSameElements([2, 1], [1, 2])}", null),
            new Probe("assertEqWithinTolerance-holds", "{|assertEqWithinTolerance(1.0, 1.0005, 0.001)}", null),
            new Probe("second-assert-fails", "{|assertEquals(1, 1); assertEquals('a', 'b');}",
                    "\nexpected: 'a'\nactual:   'b'"),
            new Probe("class-value-property", "{|assertEquals('a', ^t::Person(name='a').name)}", null),
            // 2026-11-04, the wider probe: a caller's message that READS (a let, a variable) -- dropped by every judge
            // until the message split carried it
            new Probe("message-reads-a-let", "{|let x = 3; assert(false, | $x->toString());}", "3"),
            new Probe("message-is-a-let", "{|let m = 'ab'; assertEquals(1, 2, $m);}", "ab"),
            new Probe("message-is-a-let-holds", "{|let m = 'ab'; assertEquals(1, 1, $m);}", null),
            // the message read only where the assert fails (upstream evaluates it only there): one that would raise
            // is never read while the assert holds
            new Probe("message-not-read-where-the-assert-holds",
                    "{|let x = []->cast(@Integer); assert($x->isEmpty(), | $x->toOne()->toString());}", null),
            // a DateTime written as upstream's toRepresentation writes it, '%' + its toString: a value with a minute
            // carries +0000, a written fraction its digits (the engine runner, 2026-11-04)
            new Probe("assertEquals-datetimes-message", "{|assertEquals(%2020-01-01T10:00:00, %2020-01-01T10:00:01)}",
                    "\nexpected: %2020-01-01T10:00:00+0000\nactual:   %2020-01-01T10:00:01+0000"),
            new Probe("assertEquals-datetime-precision-message",
                    "{|assertEquals(%2020-01-01T10:00:00.000+0000, %2020-01-01T10:00:00+0000)}",
                    "\nexpected: %2020-01-01T10:00:00.000+0000\nactual:   %2020-01-01T10:00:00+0000"),
            new Probe("assertEquals-datetime-hour-message", "{|assertEquals(%2020-01-01T10, %2020-01-01T11)}",
                    "\nexpected: %2020-01-01T10\nactual:   %2020-01-01T11"),
            // an instance of a generic class (a Pair) written as upstream's toRepresentation writes any instance,
            // '<id instanceOf path>', its path the raw class's
            new Probe("assertEquals-pairs-message", "{|assertEquals(pair(1, 'a'), pair(1, 'b'))}",
                    "\nexpected: <1|a instanceOf meta::pure::functions::collection::Pair>\nactual:   <1|b instanceOf"
                            + " meta::pure::functions::collection::Pair>"),
            new Probe("assertEquals-pair-collections-message",
                    "{|assertEquals([pair(1, 'a'), pair(2, 'b')], [pair(1, 'b'), pair(2, 'b')])}",
                    "\nexpected: [<1|a instanceOf meta::pure::functions::collection::Pair>, <2|b instanceOf"
                            + " meta::pure::functions::collection::Pair>]\nactual:   [<1|b instanceOf"
                            + " meta::pure::functions::collection::Pair>, <2|b instanceOf"
                            + " meta::pure::functions::collection::Pair>]"),
            new Probe("assertSameElements-pairs-message",
                    "{|assertSameElements([pair(2, 'b'), pair(1, 'a')], [pair(1, 'a'), pair(3, 'c')])}",
                    "\nexpected: [<1|a instanceOf meta::pure::functions::collection::Pair>, <2|b instanceOf"
                            + " meta::pure::functions::collection::Pair>]\nactual:   [<1|a instanceOf"
                            + " meta::pure::functions::collection::Pair>, <3|c instanceOf"
                            + " meta::pure::functions::collection::Pair>]"),
            new Probe("assertContains-pairs-message", "{|assertContains([pair(1, 'a')], pair(2, 'b'))}",
                    "[<1|a instanceOf meta::pure::functions::collection::Pair>] does not contain <2|b instanceOf"
                            + " meta::pure::functions::collection::Pair>"),
            new Probe("message-not-read-where-assertEmpty-holds",
                    "{|let x = [1, 2]; assertEmpty([], | $x->at(5)->toString());}", null),
            new Probe("format-message-reads-a-let",
                    "{|let x = [1, 2]; assertEquals(3, $x->size(), 'size %s of %s', [$x->size(), 7]);}",
                    "size 2 of 7"),
            // the failure messages upstream writes (essential/tests/*.pure), the same under every judge
            new Probe("assertSameElements-sorted-message", "{|assertSameElements([2, 11, 2], [11, 3])}",
                    "\nexpected: [2, 2, 11]\nactual:   [3, 11]"),
            new Probe("assertSameElements-strings-message", "{|assertSameElements(['b', 'a'], ['a', 'c'])}",
                    "\nexpected: ['a', 'b']\nactual:   ['a', 'c']"),
            new Probe("assertSameElements-one-element-bracketed", "{|assertSameElements([true, false], [true])}",
                    "\nexpected: [false, true]\nactual:   [true]"),
            new Probe("assertSameElements-dates-message", "{|assertSameElements([%2020-01-02, %2019-05-01], [%2019-05-01])}",
                    "\nexpected: [%2019-05-01, %2020-01-02]\nactual:   [%2019-05-01]"),
            new Probe("assertEquals-strings-message", "{|assertEquals(['a', 'b'], ['b', 'c'])}",
                    "\nexpected: ['a', 'b']\nactual:   ['b', 'c']"),
            new Probe("assertEquals-one-against-many", "{|assertEquals(1, [1, 2])}", "\nexpected: [1]\nactual:   [1, 2]"),
            new Probe("assertEquals-empty-against-one", "{|assertEquals([], [1])}", "\nexpected: []\nactual:   [1]"),
            new Probe("assertEmpty-message", "{|assertEmpty([1, 2])}", "[1, 2] is not empty"),
            new Probe("assertNotEmpty-message", "{|assertNotEmpty([])}", "Expected non-empty collection"),
            new Probe("assertContains-strings-message", "{|assertContains(['a'], 'b')}", "['a'] does not contain 'b'"),
            // assertIs over two primitives: compiled pure's `left == right`, their values (the engine runner)
            new Probe("assertIs-primitives-holds", "{|assertIs('a', 'a')}", null),
            new Probe("assertIs-primitives-fails", "{|assertIs(1, 2)}", "\nexpected: 1\nactual:   2"),
            // an if closing the body whose other side is the literal true
            new Probe("branch-true-side-last", "{|if(1 == 2, |assertEquals(1, 1), |true);}", null),
            new Probe("branch-true-side-last-fails", "{|if(1 == 1, |assertEquals(1, 2), |true);}",
                    "\nexpected: 1\nactual:   2"));

    /** A probe that answers a value: its body and the value upstream answers. */
    private record Answer(String id, String body, Object value) { }

    private static final List<Answer> ANSWERS = List.of(
            new Answer("assert-then-a-query", "{|assertEquals(1, 1); [1, 2]->map(x | $x + 1);}", List.of(2L, 3L)));

    /** The probes that miss the bar today (2026-11-04, measured at 963e45aa). SHRINK ONLY: delete a line in the change
     *  that closes it. */
    private static final Set<String> GAPS = new LinkedHashSet<>(List.of(
            // a new probe that misses goes here with the change that adds it, and leaves with the one that closes it
    ));

    private static final List<com.legend.testplan.PlanningOptions.JudgeMode> JUDGES = List.of(com.legend.testplan.PlanningOptions.JudgeMode.HOST,
            com.legend.testplan.PlanningOptions.JudgeMode.DATABASE, com.legend.testplan.PlanningOptions.JudgeMode.BOTH);

    /** A failing assert's message that raises: its evaluation's error is the test's, as upstream's (the assert bar,
     *  2026-11-04) -- under every judge. */
    @Test
    void aFailingAssertsMessageThatRaisesRaisesItsOwnError() throws Exception {
        try (Connection c = TestBackends.platform().open()) {
            for (com.legend.testplan.PlanningOptions.JudgeMode mode : JUDGES) {
                // the product's kind, in lite's words (D1.1)
                assertEquals("DataError: " + com.legend.error.ErrorKind.INDEX_OUT_OF_BOUNDS.sentence(),
                        outcome(c, "{|let x = [1, 2]; assertEmpty([1], | $x->at(5)->toString());}", mode),
                        mode.toString());
            }
        }
    }

    @Test
    void everyAssertDoesWhatItSaysUnderEveryJudge() throws Exception {
        List<String> missed = new ArrayList<>();
        List<String> closed = new ArrayList<>();
        try (Connection c = TestBackends.platform().open()) {
            for (Probe p : PROBES) {
                List<String> wrong = new ArrayList<>();
                for (com.legend.testplan.PlanningOptions.JudgeMode mode : JUDGES) {
                    String got = outcome(c, p.body(), mode);
                    String want = p.failure() == null ? "pass" : "AssertFailed: " + p.failure();
                    if (!want.equals(got)) {
                        wrong.add(mode + " " + oneLine(got));
                    }
                }
                record(p.id(), wrong, missed, closed);
            }
            for (Answer a : ANSWERS) {
                List<String> wrong = new ArrayList<>();
                for (com.legend.testplan.PlanningOptions.JudgeMode mode : JUDGES) {
                    String got;
                    try {
                        got = String.valueOf(Program.last(StorelessRuntime.with(MODEL, Databases.PLATFORM), a.body(),
                                StorelessRuntime.RUNTIME, c, ExecuteOptions.NONE.withJudgeMode(mode)).rows().stream()
                                .map(r -> r.get(0)).toList());
                    } catch (Throwable e) {
                        got = e.getClass().getSimpleName() + ": " + e.getMessage();
                    }
                    if (!String.valueOf(a.value()).equals(got)) {
                        wrong.add(mode + " " + oneLine(got));
                    }
                }
                record(a.id(), wrong, missed, closed);
            }
        }
        assertTrue(missed.isEmpty() && closed.isEmpty(), (missed.isEmpty() ? "" : "missing the bar (not in GAPS):\n  "
                + String.join("\n  ", missed) + "\n") + (closed.isEmpty() ? "" : "meeting the bar now -- delete from"
                + " GAPS: " + closed));
    }

    /** BACKWARDS-COMPATIBILITY MODE (register S127, the user's ruling of 2026-11-04: "the principled fix for the
     *  product, then compiled for backwards compatible"): compiled legend-pure's number equality -- an Integer and a
     *  Decimal equal by their texts, a Float equal to neither (CompiledSupport.eq; the engine asked:
     *  upstream-api/s127-number-equality.txt) -- under every judge alike. */
    @Test
    void backwardsCompatibilityModeComparesNumbersAsCompiledPureDoes() throws Exception {
        try (Connection c = TestBackends.platform().open()) {
            for (com.legend.testplan.PlanningOptions.JudgeMode mode : JUDGES) {
                ExecuteOptions compat = ExecuteOptions.NONE.withJudgeMode(mode).withEngineCompat(true);
                Program.last(StorelessRuntime.with(MODEL, Databases.PLATFORM), "{|assertEquals(1D, 1)}",
                        StorelessRuntime.RUNTIME, c, compat);
                Program.last(StorelessRuntime.with(MODEL, Databases.PLATFORM), "{|assertEquals(3, 3D)}",
                        StorelessRuntime.RUNTIME, c, compat);
                for (String body : List.of("{|assertEquals(1.5, 1.5D)}", "{|assertEquals(1, 1.0)}",
                        "{|assertEquals(1.0D, 1)}")) {
                    AssertFailed e = org.junit.jupiter.api.Assertions.assertThrows(AssertFailed.class,
                            () -> Program.last(StorelessRuntime.with(MODEL, Databases.PLATFORM), body,
                                    StorelessRuntime.RUNTIME, c, compat), mode + " " + body);
                    assertTrue(e.getMessage().startsWith("\nexpected: "), mode + " " + body + ": " + e.getMessage());
                }
            }
        }
    }

    /** The database judge sends a segment's asserts as ONE statement (the fused verdict statement, VerdictBatch), whatever
     *  their messages: a caller's message is a column of its own assert's row inside it (the user, 2026-11-04: "the
     *  'fuse' step to do all assert together in one go"). Measured as the statements a passing body sends. */
    @Test
    void aSegmentsAssertsAreOneStatementUnderTheDatabaseJudge() throws Exception {
        try (Connection c = TestBackends.platform().open()) {
            for (String body : List.of(
                    "{|assertEquals(1, 1); assertEquals('a', 'a'); assert(2 > 1);}",
                    "{|assertEquals(1, 1, 'one'); assertEquals('a', 'a', 'got %s', ['a']); assert(2 > 1, | 'two');}")) {
                int bodyOrigin = com.legend.exec.StatementOrigin.BODY.ordinal();
                long before = com.legend.exec.StatementOrigin.snapshot()[bodyOrigin];
                Program.last(StorelessRuntime.with(MODEL, Databases.PLATFORM), body, StorelessRuntime.RUNTIME, c,
                        ExecuteOptions.NONE.withJudgeMode(com.legend.testplan.PlanningOptions.JudgeMode.DATABASE));
                long sent = com.legend.exec.StatementOrigin.snapshot()[bodyOrigin] - before;
                org.junit.jupiter.api.Assertions.assertEquals(1L, sent, "one fused statement: " + body);
            }
        }
    }

    private static void record(String id, List<String> wrong, List<String> missed, List<String> closed) {
        if (wrong.isEmpty() && GAPS.contains(id)) {
            closed.add(id);
        } else if (!wrong.isEmpty() && !GAPS.contains(id)) {
            missed.add(id + ": " + String.join(" | ", wrong));
        }
    }

    private static String outcome(Connection c, String body, com.legend.testplan.PlanningOptions.JudgeMode mode) {
        try {
            Program.last(StorelessRuntime.with(MODEL, Databases.PLATFORM), body, StorelessRuntime.RUNTIME, c,
                    ExecuteOptions.NONE.withJudgeMode(mode));
            return "pass";
        } catch (AssertFailed e) {
            return "AssertFailed: " + e.getMessage();
        } catch (Throwable e) {
            return e.getClass().getSimpleName() + ": " + e.getMessage();
        }
    }

    private static String oneLine(String s) {
        String t = s.replace("\n", "\\n");
        return t.length() > 200 ? t.substring(0, 200) + "..." : t;
    }
}
