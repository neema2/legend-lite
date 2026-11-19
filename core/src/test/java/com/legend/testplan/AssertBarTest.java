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
            // the four default-message asserts (#28): platform natives, each its positive family negated, worded by
            // lite's own sentence under every judge
            new Probe("assertNotEq-fails", "{|assertNotEq(1, 1)}", "expected a value other than 1"),
            new Probe("assertNotSize-holds", "{|assertNotSize([1, 2], 3)}", null),
            new Probe("assertNotSize-fails", "{|assertNotSize([1, 2], 2)}", "expected a size other than 2"),
            new Probe("assertNotContains-holds", "{|assertNotContains([1, 2], 3)}", null),
            new Probe("assertNotContains-fails", "{|assertNotContains(['a', 'b'], 'b')}",
                    "expected [\"a\", \"b\"] not to contain \"b\""),
            new Probe("assertIsNot-holds", "{|assertIsNot(1, 2)}", null),
            new Probe("assertIsNot-fails", "{|assertIsNot('x', 'x')}", "expected an instance other than \"x\""),
            new Probe("assertNotEq-with-a-message", "{|assertNotEq(1, 1, 'mine')}", "mine"),
            new Probe("one-statement-helper-holds", "{|t::check(1 + 1)}", null),
            new Probe("one-statement-helper-fails", "{|t::check(3)}", "expected 2, got 3"),
            new Probe("two-statement-helper-fails", "{|t::check2(3)}", "expected 2, got 3"),
            new Probe("assert-in-an-if", "{|if(1 == 1, |assertEquals(1, 1), |assertEquals(1, 2))}", null),
            // an assert under an if (lite's plan's Branch node, step 4): the side not taken is never evaluated, as
            // upstream never evaluates it -- the else side's toOne() over nothing would fail if it ran
            new Probe("assert-in-an-if-fails", "{|if(1 == 2, |assertEquals(1, 1), |assertEquals(1, 2))}",
                    "expected 1, got 2"),
            new Probe("if-side-not-taken-never-runs",
                    "{|if(1 == 1, |assert(true), |assertEquals(1, [1, 2]->filter(x | $x > 5)->toOne()))}", null),
            new Probe("if-over-a-let", "{|let x = 2; if($x > 1, |assertEquals(2, $x), |assertEquals(1, $x));}", null),
            new Probe("if-over-a-query",
                    "{|if([1, 2, 3]->filter(x | $x > 1)->size() == 2, |assertEquals(1, 1), |assertEquals(1, 2))}",
                    null),
            new Probe("asserts-around-an-if",
                    "{|assertEquals(1, 1); if(2 > 1, |assertEquals('a', 'a'), |assert(false)); assertEquals(2, 3);}",
                    "expected 2, got 3"),
            new Probe("nested-if",
                    "{|if(1 == 1, |if(2 == 3, |assert(false, 'inner then'), |assert(false, 'inner else')),"
                            + " |assert(true))}", "inner else"),
            new Probe("if-with-a-constant-then", "{|if(1 == 1, |true, |assert(false, 'not taken')); assert(true);}",
                    null),
            new Probe("if-with-a-constant-side", "{|if(1 == 1, |assertEquals(1, 2), |true); true;}",
                    "expected 1, got 2"),
            new Probe("assert-bound-by-a-let", "{|let ok = assertEquals(1, 1); assert($ok);}", null),
            new Probe("forAll-over-range", "{|range(1, 4)->forAll(i | assertEquals($i, $i))}", null),
            // per element, the operands computed by the database (step 5): the first failing element in upstream's
            // words, each operand spelled by its own kind, an empty source holding
            new Probe("forAll-over-range-fails", "{|range(1, 4)->forAll(i | assertEquals($i, 2))}",
                    "expected 1, got 2 at index 0"),
            // register S139 (the user's ruling, 2026-11-05): lite compares a Decimal by value, its scale dropped
            new Probe("forAll-decimal-scale", "{|range(1, 3)->forAll(i | assertEquals(3.0D, 3.00D))}", null),
            new Probe("forAll-decimal-value", "{|range(1, 3)->forAll(i | assertEquals(3.0D, 3.10D))}",
                    "expected 3.0, got 3.10 at index 0"),
            new Probe("forAll-decimal-vs-integer", "{|range(1, 3)->forAll(i | assertEquals(1D, $i))}",
                    "expected 1 (Decimal), got 1 (Integer) at index 0"),
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
                    "expected t::Person, got t::Employee"),   // lite's sentence since census J29 (2026-11-18)
            new Probe("concatenate-keeps-order", "{|assertEquals([3, 1, 2], [1, 2]->concatenate([3]))}",
                    "expected [3, 1, 2], got [1, 2, 3]"),
            new Probe("union-keeps-order", "{|assertEquals([3, 1, 2], [1, 2]->union([3]))}",
                    "expected [3, 1, 2], got [1, 2, 3]"),
            // exact, as upstream (S132's float tokens are backwards-compatibility mode's alone: ExecuteOptions
            // .withEngineCompat, the PCT and corpus harnesses; the user, 2026-11-04)
            // a Float in a failure message at full round-trip precision, the same text under every judge on every
            // database (the orchestrator's ruling, 2026-11-17: a message is a diagnostic; S311's six decimals are a
            // Float's toString in a query)
            // the operands parsed so each is the double (a Float literal's arithmetic is the database's DECIMAL until
            // E-P5's S20 lands)
            new Probe("float-message-sum", "{|assertEquals(0.3, parseFloat('0.1') + parseFloat('0.2'))}",
                    "expected 0.3, got 0.30000000000000004"),
            // past DECIMAL(38,18): Java's exponent form, every database's text normalized to it
            new Probe("float-message-tiny", "{|assertEquals(1.0, parseFloat('1.0E-300'))}",
                    "expected 1.0, got 1.0E-300"),
            new Probe("float-message-negative-zero", "{|assertEquals(1.0, parseFloat('-0.0'))}",
                    "expected 1.0, got -0.0"),
            new Probe("float-message-product", "{|assertEquals(1.0, parseFloat('123456789.123456789') * 3.0)}",
                    "expected 1.0, got 370370367.3703704"),
            new Probe("float-looking-strings", "{|assertEquals('x=1.0000000000000002', 'x=1.0')}",
                    "expected \"x=1.0000000000000002\", got \"x=1.0\""),
            new Probe("decimal-scale", "{|assertEquals(3.0D, 3.00D)}", null),
            new Probe("decimal-value", "{|assertEquals(3.0D, 3.10D)}", "expected 3.0, got 3.10"),
            new Probe("integer-vs-float", "{|assertEquals(1, 1.0)}", "expected 1, got 1.0"),
            // the ledger audit's D4 (2026-11-16): a list of Integers against the same numbers as Floats fails, as the
            // one-value probe above does; the message is the failed typed comparison's, never an untyped re-compare
            // that held (which passed the assertSameElements and raised "verdict/message divergence" on assertEquals)
            new Probe("integer-vs-float-lists", "{|assertEquals([1, 2], [1.0, 2.0])}",
                    "expected [1, 2], got [1.0, 2.0]"),
            new Probe("integer-vs-float-same-elements", "{|assertSameElements([2, 1], [2.0, 1.0])}",
                    "expected the elements [2, 1] in any order, got [2.0, 1.0]"),
            // the type is part of the value in lite's own runs (register S127, the user's ruling of 2026-11-04;
            // backwards-compatibility mode compares an Integer and a Decimal by text: below)
            new Probe("decimal-vs-integer", "{|assertEquals(1D, 1)}", "expected 1 (Decimal), got 1 (Integer)"),
            new Probe("float-vs-decimal", "{|assertEquals(1.5, 1.5D)}", "expected 1.5 (Float), got 1.5 (Decimal)"),
            new Probe("assertNotEquals-across-kinds", "{|assertNotEquals(3, 3D)}", null),
            new Probe("mixed-list", "{|assertEquals([1, 'a', 2.5], [1, 'a', 2.5])}", null),
            new Probe("empty-vs-filtered", "{|assertEquals([], [1, 2]->filter(x | $x > 5))}", null),
            new Probe("assertError-let-message", "{|let m = 'boom'; assertError(|fail($m), $m);}", null),
            new Probe("assertError-over-a-failing-assert",
                    "{|assertError(|assertEquals(1, 2), 'expected 1, got 2')}", null),
            // an assert's failure is the error assertError matches, as upstream's assertError catches it
            new Probe("assertError-over-a-holding-assert", "{|assertError(|assertEquals(1, 1), 'boom')}",
                    "expected the error \"boom\", got none"),
            new Probe("assertError-over-a-different-failure", "{|assertError(|assert(false, 'one'), 'two')}",
                    "expected the error \"two\", got \"one\""),
            new Probe("assertEmpty-holds", "{|assertEmpty([1, 2]->filter(x | $x > 5))}", null),
            new Probe("assertSize-fails", "{|assertSize([1, 2], 3)}", "expected size 3, got 2"),
            new Probe("assertContains-fails", "{|assertContains([1, 2], 3)}", "expected [1, 2] to contain 3"),
            new Probe("assertSameElements-holds", "{|assertSameElements([2, 1], [1, 2])}", null),
            new Probe("assertEqWithinTolerance-holds", "{|assertEqWithinTolerance(1.0, 1.0005, 0.001)}", null),
            new Probe("second-assert-fails", "{|assertEquals(1, 1); assertEquals('a', 'b');}",
                    "expected \"a\", got \"b\""),
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
                    "expected 2020-01-01T10:00:00.000000Z, got 2020-01-01T10:00:01.000000Z"),
            new Probe("assertEquals-datetime-precision-message",
                    "{|assertEquals(%2020-01-01T10:00:00.000+0000, %2020-01-01T10:00:00+0000)}",
                    "expected 2020-01-01T10:00:00.000000Z (DateTime, subsecond), got 2020-01-01T10:00:00.000000Z (DateTime, second)"),
            new Probe("assertEquals-datetime-hour-message", "{|assertEquals(%2020-01-01T10, %2020-01-01T11)}",
                    "expected 2020-01-01T10:00:00.000000Z, got 2020-01-01T11:00:00.000000Z"),
            // an instance of a generic class (a Pair) written as upstream's toRepresentation writes any instance,
            // '<id instanceOf path>', its path the raw class's
            new Probe("assertEquals-pairs-message", "{|assertEquals(pair(1, 'a'), pair(1, 'b'))}",
                    "expected meta::pure::functions::collection::Pair {\"first\":1,\"second\":\"a\"}, got meta::pure::functions::collection::Pair {\"first\":1,\"second\":\"b\"}"),
            new Probe("assertEquals-pair-collections-message",
                    "{|assertEquals([pair(1, 'a'), pair(2, 'b')], [pair(1, 'b'), pair(2, 'b')])}",
                    "expected [meta::pure::functions::collection::Pair {\"first\":1,\"second\":\"a\"}, meta::pure::functions::collection::Pair {\"first\":2,\"second\":\"b\"}], got [meta::pure::functions::collection::Pair {\"first\":1,\"second\":\"b\"}, "
                            + "meta::pure::functions::collection::Pair {\"first\":2,\"second\":\"b\"}]"),
            new Probe("assertSameElements-pairs-message",
                    "{|assertSameElements([pair(2, 'b'), pair(1, 'a')], [pair(1, 'a'), pair(3, 'c')])}",
                    "expected the elements [meta::pure::functions::collection::Pair {\"first\":2,\"second\":\"b\"}, meta::pure::functions::collection::Pair {\"first\":1,\"second\":\"a\"}] in any order, "
                            + "got [meta::pure::functions::collection::Pair {\"first\":1,\"second\":\"a\"}, meta::pure::functions::collection::Pair {\"first\":3,\"second\":\"c\"}]"),
            new Probe("assertContains-pairs-message", "{|assertContains([pair(1, 'a')], pair(2, 'b'))}",
                    "expected [meta::pure::functions::collection::Pair {\"first\":1,\"second\":\"a\"}] to contain meta::pure::functions::collection::Pair {\"first\":2,\"second\":\"b\"}"),
            new Probe("message-not-read-where-assertEmpty-holds",
                    "{|let x = [1, 2]; assertEmpty([], | $x->at(5)->toString());}", null),
            new Probe("format-message-reads-a-let",
                    "{|let x = [1, 2]; assertEquals(3, $x->size(), 'size %s of %s', [$x->size(), 7]);}",
                    "size 2 of 7"),
            // the failure messages upstream writes (essential/tests/*.pure), the same under every judge
            new Probe("assertSameElements-sorted-message", "{|assertSameElements([2, 11, 2], [11, 3])}",
                    "expected the elements [2, 11, 2] in any order, got [11, 3]"),
            new Probe("assertSameElements-strings-message", "{|assertSameElements(['b', 'a'], ['a', 'c'])}",
                    "expected the elements [\"b\", \"a\"] in any order, got [\"a\", \"c\"]"),
            new Probe("assertSameElements-one-element-bracketed", "{|assertSameElements([true, false], [true])}",
                    "expected the elements [true, false] in any order, got [true]"),
            new Probe("assertSameElements-dates-message", "{|assertSameElements([%2020-01-02, %2019-05-01], [%2019-05-01])}",
                    "expected the elements [2020-01-02, 2019-05-01] in any order, got [2019-05-01]"),
            new Probe("assertEquals-strings-message", "{|assertEquals(['a', 'b'], ['b', 'c'])}",
                    "expected [\"a\", \"b\"], got [\"b\", \"c\"]"),
            new Probe("assertEquals-one-against-many", "{|assertEquals(1, [1, 2])}", "expected [1], got [1, 2]"),
            new Probe("assertEquals-empty-against-one", "{|assertEquals([], [1])}", "expected [], got [1]"),
            new Probe("assertEmpty-message", "{|assertEmpty([1, 2])}", "expected no values, got [1, 2]"),
            new Probe("assertNotEmpty-message", "{|assertNotEmpty([])}", "expected at least one value, got none"),
            new Probe("assertContains-strings-message", "{|assertContains(['a'], 'b')}", "expected [\"a\"] to contain \"b\""),
            // assertIs over two primitives: compiled pure's `left == right`, their values (the engine runner)
            new Probe("assertIs-primitives-holds", "{|assertIs('a', 'a')}", null),
            new Probe("assertIs-primitives-fails", "{|assertIs(1, 2)}", "expected 1, got 2"),
            // an if closing the body whose other side is the literal true
            new Probe("branch-true-side-last", "{|if(1 == 2, |assertEquals(1, 1), |true);}", null),
            new Probe("branch-true-side-last-fails", "{|if(1 == 1, |assertEquals(1, 2), |true);}",
                    "expected 1, got 2"));

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
    /** No assert body the prelude holds spells legend-pure's representation ({@code %r}, {@code toRepresentation}): the
     *  default-message asserts are natives whose words are each judge's (#28; F's 2b, the product refusing %r). */
    @Test
    void noPreludeAssertBodySpellsARepresentation() throws Exception {
        String prelude;
        try (var in = AssertBarTest.class.getClassLoader().getResourceAsStream("com/legend/builtin/prelude.pure")) {
            prelude = new String(java.util.Objects.requireNonNull(in, "the prelude").readAllBytes(),
                    java.nio.charset.StandardCharsets.UTF_8);
        }
        List<String> offenders = new ArrayList<>();
        for (String section : prelude.split("\n###Pure\n")) {
            if (section.contains("function meta::pure::functions::asserts::")
                    && (section.contains("%r") || section.contains("toRepresentation"))) {
                offenders.add(section.lines().filter(l -> l.startsWith("function ")).findFirst().orElse(section));
            }
        }
        assertTrue(offenders.isEmpty(), "assert bodies spelling a representation: " + offenders);
    }

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

    /** A Decimal property over an INTEGER column (PR #118 fix round, 2026-11-18, G2): its value arrives as the column's
     *  integer, its declaration says Decimal. Lite's own rule (register S127) compares two numbers by their DECLARED
     *  kinds, so the Integer 1 and the property's Decimal 1 are never equal -- under the host judge as under the database
     *  judge, and under both together: one outcome, and one message, per assert, on DuckDB, H2 and SQLite alike. Its row
     *  is the connection's declared test data, a load of the plan's; {@code %s} its database. */
    private static final String MAPPED_ON = """
            ###Pure
            Class p::P { id: Integer[1]; d: Decimal[1]; }
            Class p::Q { id: Integer[1]; e: Decimal[1]; }
            ###Relational
            Database p::DB ( Table T (ID INTEGER PRIMARY KEY, D INTEGER) Table U (ID INTEGER PRIMARY KEY, E DECIMAL(10,2)) )
            ###Mapping
            Mapping p::M ( p::P: Relational { ~mainTable [p::DB] T id: T.ID, d: T.D }
                p::Q: Relational { ~mainTable [p::DB] U id: U.ID, e: U.E } )
            ###Connection
            RelationalDatabaseConnection p::C { store: p::DB; type: %s;
                specification: LocalH2 { testDataSetupCSV: 'default\\nT\\nID,D\\n1,1\\n-----\\ndefault\\nU\\nID,E\\n1,1.50\\n2,-2.00\\n'; };
                auth: DefaultH2; }
            ###Runtime
            Runtime p::RT { mappings: [p::M]; connections: [ p::DB: [ c0: p::C ] ]; }
            """;

    @Test
    void anEqOverADeclaredDecimalIsOneVerdictUnderEveryJudge() throws Exception {
        String d = "p::P.all()->filter(x | $x.id == 1)->toOne().d";
        String mapped = "p::P.all()->map(x | $x.d)->toOne()";
        // the body, and its one outcome: the Integer 1 is not the Decimal 1 in lite's own runs (S127)
        java.util.Map<String, String> want = new java.util.LinkedHashMap<>();
        for (String side : List.of(d, mapped)) {
            want.put("{|assertEq(1, " + side + ")}", "AssertFailed: expected 1 (Integer), got 1 (Decimal)");
            want.put("{|assertIs(1, " + side + ")}", "AssertFailed: expected 1 (Integer), got 1 (Decimal)");
            want.put("{|assertNotEq(1, " + side + ")}", "pass");
            want.put("{|assertIsNot(1, " + side + ")}", "pass");
        }
        // the database judge's text of a store-read Decimal drops only its carrier's D: its sign and digits kept (a
        // DECIMAL(10,2) read decodes scale-canonical, 1.50 as 1.5, under both judges: LiteralSpelling.wireValueEgress's
        // R3), and a written literal keeps its scale
        want.put("{|assertEq(1, p::Q.all()->filter(x | $x.id == 1)->toOne().e)}", "AssertFailed: expected 1, got 1.5");
        want.put("{|assertEq(2, p::Q.all()->filter(x | $x.id == 2)->toOne().e)}", "AssertFailed: expected 2, got -2");
        want.put("{|assertEq(1, 1.50D)}", "AssertFailed: expected 1, got 1.50");
        want.put("{|assertEq(2, -2D)}", "AssertFailed: expected 2, got -2");
        List<String> wrong = new ArrayList<>();
        // the D's strip is the database judge's SQL, so on each database the platform runs in-process (the review of
        // PR #118's fix round, 2026-11-18: beyond the platform's, H2 and SQLite at least); the map's toOne reads a list,
        // which a dialect without a list encoding refuses by name (DialectCapability, under every judge alike), so its
        // shape runs on the platform's database
        for (var backend : TestBackends.inProcess()) {
            var database = backend.type();
            String mappedOn = MAPPED_ON.formatted(database);
            for (var e : want.entrySet()) {
                if (e.getKey().contains(mapped) && !database.equals(Databases.PLATFORM)) {
                    continue;
                }
                String wanted = e.getValue();
                for (com.legend.testplan.PlanningOptions.JudgeMode mode : JUDGES) {
                    String got;
                    try {
                        Program.runFresh(mappedOn, e.getKey(), "p::RT", database,
                                ExecuteOptions.NONE.withJudgeMode(mode), com.legend.testexec.NoEdgeRun.NONE);
                        got = "pass";
                    } catch (AssertFailed f) {
                        got = "AssertFailed: " + f.getMessage();
                    } catch (Throwable t) {
                        got = t.getClass().getSimpleName() + ": " + t.getMessage();
                    }
                    if (!wanted.equals(got)) {
                        wrong.add(database + " " + mode + " " + e.getKey() + " -> " + oneLine(got));
                    }
                }
            }
        }
        assertTrue(wrong.isEmpty(), "one verdict under every judge:\n  " + String.join("\n  ", wrong));
    }

    /** F4's equality over each numeric pairing the host's Equality decides (track A5, 2026-11-19; the fix batch's
     *  JUDGES-WORDS-8: F4's test covered an Integer literal against a Decimal over INTEGER alone): a Float property over
     *  an INTEGER column (its declaration Float, the column's carrier an integer), a Decimal property over a DOUBLE
     *  column, and a Number property -- an unrefined declaration, which keeps its carrier's kind -- over an INTEGER and a
     *  DOUBLE column. Lite's rules (S127: two numbers of different kinds never equal; S139: Decimals by value): one
     *  outcome, and one message, per assert under every judge, read from its row and from a map. */
    private static final String PAIRINGS_ON = """
            ###Pure
            Class p::N { id: Integer[1]; f: Float[1]; d: Decimal[1]; n: Number[1]; m: Number[1]; }
            ###Relational
            Database p::DB ( Table T (ID INTEGER PRIMARY KEY, F INTEGER, D DOUBLE, N INTEGER, M DOUBLE) )
            ###Mapping
            Mapping p::M ( p::N: Relational { ~mainTable [p::DB] T id: T.ID, f: T.F, d: T.D, n: T.N, m: T.M } )
            ###Connection
            RelationalDatabaseConnection p::C { store: p::DB; type: %s;
                specification: LocalH2 { testDataSetupCSV: 'default\\nT\\nID,F,D,N,M\\n1,1,1.5,1,1.5\\n'; }; auth: DefaultH2; }
            ###Runtime
            Runtime p::RT { mappings: [p::M]; connections: [ p::DB: [ c0: p::C ] ]; }
            """;

    @Test
    void anEqOverEachNumericPairingIsOneVerdictUnderEveryJudge() throws Exception {
        // property, the literal of its kind, a literal of another kind
        String[][] pairings = {{"f", "1.0", "1"}, {"d", "1.5D", "1.5"}, {"n", "1", "1.0"}, {"m", "1.5", "1.5D"}};
        java.util.Map<String, String> want = new java.util.LinkedHashMap<>();
        for (String[] p : pairings) {
            for (String side : List.of("p::N.all()->filter(x | $x.id == 1)->toOne()." + p[0],
                    "p::N.all()->map(x | $x." + p[0] + ")->toOne()")) {
                want.put("{|assertEq(" + p[1] + ", " + side + ")}", "pass");
                want.put("{|assertEq(" + p[2] + ", " + side + ")}", "AssertFailed");
                want.put("{|assertNotEq(" + p[1] + ", " + side + ")}", "AssertFailed");
                want.put("{|assertNotEq(" + p[2] + ", " + side + ")}", "pass");
            }
        }
        List<String> wrong = new ArrayList<>();
        for (var backend : TestBackends.inProcess()) {
            var database = backend.type();
            String model = PAIRINGS_ON.formatted(database);
            for (var e : want.entrySet()) {
                // the map reads a list, which a dialect without a list encoding refuses by name (as F4's test)
                if (e.getKey().contains("map(x") && !database.equals(Databases.PLATFORM)) {
                    continue;
                }
                String message = null;
                for (com.legend.testplan.PlanningOptions.JudgeMode mode : JUDGES) {
                    String got;
                    try {
                        Program.runFresh(model, e.getKey(), "p::RT", database, ExecuteOptions.NONE.withJudgeMode(mode),
                                com.legend.testexec.NoEdgeRun.NONE);
                        got = "pass";
                    } catch (AssertFailed f) {
                        got = "AssertFailed";
                        if (message == null) {
                            message = f.getMessage();
                        } else if (!message.equals(f.getMessage())) {
                            got = "AssertFailed, worded otherwise: " + f.getMessage() + " (not " + message + ")";
                        }
                    } catch (Throwable t) {
                        got = t.getClass().getSimpleName() + ": " + t.getMessage();
                    }
                    if (!e.getValue().equals(got)) {
                        wrong.add(database + " " + mode + " " + e.getKey() + " -> " + oneLine(got));
                    }
                }
            }
        }
        assertTrue(wrong.isEmpty(), "one verdict under every judge:\n  " + String.join("\n  ", wrong));
    }

    /** J3 (the PR #118 follow-ups, track A5, 2026-11-18; G2 under register S131: "each side typed by its own declared
     *  kind"): {@code assertContains}, {@code assertNotContains} and the forAll-contains subset compare a number by its
     *  side's DECLARED kind, under both judges -- as assertEq does (F4), and as legend-engine's result decode makes the
     *  value (a Decimal-declared cell through {@code toDecimal}, a Float-declared one through {@code * 1.0}:
     *  execution_relational_execute.pure's {@code dataTypeTransformer}). A Decimal property and a Float property over
     *  INTEGER columns: the Decimal 1 is never the Integer 1 (S127), the Float 1.0 is the Float 1.0 whatever the column.
     *  Before, the host compared by carrier (a map's values arrive as the column's integers) and the database judge by
     *  canon text, which drops the kind: one outcome, and one message, per assert under every judge now, on DuckDB, H2
     *  and SQLite alike. */
    private static final String MEMBERS_ON = """
            ###Pure
            Class p::P { id: Integer[1]; d: Decimal[1]; f: Float[1]; }
            ###Relational
            Database p::DB ( Table T (ID INTEGER PRIMARY KEY, D INTEGER, F INTEGER) )
            ###Mapping
            Mapping p::M ( p::P: Relational { ~mainTable [p::DB] T id: T.ID, d: T.D, f: T.F } )
            ###Connection
            RelationalDatabaseConnection p::C { store: p::DB; type: %s;
                specification: LocalH2 { testDataSetupCSV: 'default\\nT\\nID,D,F\\n1,1,1\\n'; }; auth: DefaultH2; }
            ###Runtime
            Runtime p::RT { mappings: [p::M]; connections: [ p::DB: [ c0: p::C ] ]; }
            """;

    @Test
    void aMembershipIsJudgedByEachSidesDeclaredKindUnderEveryJudge() throws Exception {
        java.util.Map<String, String> want = new java.util.LinkedHashMap<>();
        // the property read from its row (a store-read carrier), and from a map (the column's integers)
        for (String prop : List.of("d", "f")) {
            String same = prop.equals("d") ? "[1D, 2D]" : "[1.0, 2.0]";
            for (String one : List.of("p::P.all()->filter(x | $x.id == 1)->toOne()." + prop,
                    "p::P.all()->map(x | $x." + prop + ")->toOne()")) {
                want.put("{|assertContains(" + same + ", " + one + ")}", "pass");
                want.put("{|assertContains([1, 2], " + one + ")}", "AssertFailed");
                want.put("{|assertNotContains([1, 2], " + one + ")}", "pass");
                want.put("{|assertNotContains(" + same + ", " + one + ")}", "AssertFailed");
            }
            String elem = prop.equals("d") ? "1D" : "1.0";
            for (String all : List.of("p::P.all()." + prop, "p::P.all()->map(x | $x." + prop + ")")) {
                want.put("{|assertContains(" + all + ", " + elem + ")}", "pass");
                want.put("{|assertContains(" + all + ", 1)}", "AssertFailed");
                want.put("{|assert([" + elem + "]->forAll(e | " + all + "->contains($e)))}", "pass");
                want.put("{|assert([1]->forAll(e | " + all + "->contains($e)))}", SUBSET_FAILS);
                want.put("{|assert(" + all + "->forAll(e | " + same + "->contains($e)))}", "pass");
                want.put("{|assert(" + all + "->forAll(e | [1, 2]->contains($e)))}", SUBSET_FAILS);
            }
            // nothing needed: the subset holds whatever the kinds (forAll over nothing)
            want.put("{|assert(p::P.all()->filter(x | $x.id == 9)." + prop + "->forAll(e | [1, 2]->contains($e)))}",
                    "pass");
        }
        List<String> wrong = new ArrayList<>();
        for (var backend : TestBackends.inProcess()) {
            var database = backend.type();
            String model = MEMBERS_ON.formatted(database);
            for (var e : want.entrySet()) {
                // the map reads a list, which a dialect without a list encoding refuses by name (as F4's test)
                if (e.getKey().contains("map(x") && !database.equals(Databases.PLATFORM)) {
                    continue;
                }
                String message = null;
                for (com.legend.testplan.PlanningOptions.JudgeMode mode : JUDGES) {
                    String got;
                    try {
                        Program.runFresh(model, e.getKey(), "p::RT", database, ExecuteOptions.NONE.withJudgeMode(mode),
                                com.legend.testexec.NoEdgeRun.NONE);
                        got = "pass";
                    } catch (AssertFailed f) {
                        got = "AssertFailed";
                        // one message under every judge (an isolated BOTH compares them: JudgesDisagree otherwise)
                        if (message == null) {
                            message = f.getMessage();
                        } else if (!message.equals(f.getMessage())) {
                            got = "AssertFailed, worded otherwise: " + f.getMessage() + " (not " + message + ")";
                        }
                    } catch (Throwable t) {
                        got = t.getClass().getSimpleName() + ": " + t.getMessage();
                    }
                    String wanted = e.getValue();
                    if (wanted.equals(SUBSET_FAILS)) {
                        // both judges fail it, each in its own words (JUDGES-3, queued: the host's subset sentence is
                        // none of the run's words), so isolated BOTH answers JudgesDisagree naming two failures
                        wanted = mode == com.legend.testplan.PlanningOptions.JudgeMode.BOTH ? "both failed"
                                : "AssertFailed";
                        if (got.startsWith("JudgesDisagree") && got.contains("assert failed \"")
                                && got.contains("; database meta::pure::functions::asserts::assert failed")) {
                            got = "both failed";
                        } else if (got.startsWith("AssertFailed")) {
                            got = "AssertFailed";
                        }
                    }
                    if (!wanted.equals(got)) {
                        wrong.add(database + " " + mode + " " + e.getKey() + " -> " + oneLine(got));
                    }
                }
            }
        }
        assertTrue(wrong.isEmpty(), "one verdict under every judge:\n  " + String.join("\n  ", wrong));
    }

    /** An hour-precision DateTime ({@code date(y, m, d, h)} against its literal, legend-pure's testDateFromHour) under
     *  lite's own rules (track A5, 2026-11-19, from the compatibility judges homework): the database judge's message
     *  read the side's text carrier as a timestamp, which DuckDB refused at the hour alone ("invalid timestamp field
     *  format"), so its run failed where the host's held. Now one outcome and one message under every judge, on each
     *  in-process database: the hour written at its full text, as the host writes it. */
    @Test
    void anHourPrecisionDateTimeIsOneVerdictUnderEveryJudge() throws Exception {
        java.util.Map<String, String> want = new java.util.LinkedHashMap<>();
        want.put("{|assertEquals(%2015-04-16T14, date(2015, 4, 16, 14))}", "pass");
        want.put("{|assertEquals(%1973-11-13T23, date(1973, 11, 13, 23))}", "pass");
        want.put("{|assertEquals(%2015-04-16T14, date(2015, 4, 16, 15))}",
                "AssertFailed: expected 2015-04-16T14:00:00.000000Z, got 2015-04-16T15:00:00.000000Z");
        want.put("{|assertEquals(%2015-04-16T14:30, date(2015, 4, 16, 14, 31))}",
                "AssertFailed: expected 2015-04-16T14:30:00.000000Z, got 2015-04-16T14:31:00.000000Z");
        // Pure keeps a DateTime's precision (the orchestrator's ruling 3, 2026-11-19): an hour is not its minute --
        // legend-pure's DateWithHour.getMinute() is -1 where a minute's is 0 (m4 PureDate.equals, the pinned jar), so
        // %2015-04-16T14 == %2015-04-16T14:00 is false, under every judge (the text read to its minute is the message's
        // alone, never the comparison's)
        want.put("{|assertEquals(%2015-04-16T14, %2015-04-16T14:00)}", "AssertFailed: expected"
                + " 2015-04-16T14:00:00.000000Z (DateTime, hour), got 2015-04-16T14:00:00.000000Z (DateTime, minute)");
        want.put("{|assertNotEquals(%2015-04-16T14, %2015-04-16T14:00)}", "pass");
        want.put("{|assertNotEquals(%2015-04-16T14, date(2015, 4, 16, 14, 0))}", "pass");
        List<String> wrong = new ArrayList<>();
        for (var backend : TestBackends.inProcess()) {
            for (var e : want.entrySet()) {
                for (com.legend.testplan.PlanningOptions.JudgeMode mode : JUDGES) {
                    String got;
                    try (Connection c = backend.open()) {
                        Program.run(StorelessRuntime.with(MODEL, backend.type()), e.getKey(), StorelessRuntime.RUNTIME,
                                c, ExecuteOptions.NONE.withJudgeMode(mode));
                        got = "pass";
                    } catch (AssertFailed f) {
                        got = "AssertFailed: " + f.getMessage();
                    } catch (Throwable t) {
                        got = t.getClass().getSimpleName() + ": " + t.getMessage();
                    }
                    if (!e.getValue().equals(got)) {
                        wrong.add(backend.type() + " " + mode + " " + e.getKey() + " -> " + oneLine(got));
                    }
                }
            }
        }
        assertTrue(wrong.isEmpty(), "one verdict under every judge:\n  " + String.join("\n  ", wrong));
    }

    /** A Date-declared value is the kind the database computed it as (the orchestrator's ruling 2, track A5,
     *  2026-11-19, G2): legend-engine's {@code firstDayOfWeek} is {@code $date->datePart()->adjust(...)}, a StrictDate
     *  -- the host read it as the DateTime 2014-12-01T00:00:00 (the abstract Date's TIMESTAMP label) and failed an
     *  equality with its StrictDate literal that the database judge held; an {@code adjust} of a DateTime stays a
     *  DateTime, never equal to a StrictDate (legend-pure's precision), and both judges write it as a DateTime. One
     *  outcome and one message under every judge, on each in-process database. */
    @Test
    void aDateDeclaredValueIsTheKindTheDatabaseComputedUnderEveryJudge() throws Exception {
        java.util.Map<String, String> want = new java.util.LinkedHashMap<>();
        want.put("{|assertEquals(%2014-12-01, %2014-12-04->meta::pure::functions::date::firstDayOfWeek())}", "pass");
        want.put("{|assertEquals(%2014-12-01, %2014-12-04T10:30:00->meta::pure::functions::date::firstDayOfWeek())}",
                "pass");
        want.put("{|assertEquals(%2014-12-02, %2014-12-01->adjust(1, DurationUnit.DAYS))}", "pass");
        want.put("{|assertEquals(%2014-12-01, %2014-12-01T00:00:00->adjust(0, DurationUnit.DAYS))}",
                "AssertFailed: expected 2014-12-01, got 2014-12-01T00:00:00.000000Z");
        want.put("{|assertNotEquals(%2014-12-01, %2014-12-01T00:00:00->adjust(0, DurationUnit.DAYS))}", "pass");
        List<String> wrong = new ArrayList<>();
        for (var backend : TestBackends.inProcess()) {
            for (var e : want.entrySet()) {
                for (com.legend.testplan.PlanningOptions.JudgeMode mode : JUDGES) {
                    String got;
                    try (Connection c = backend.open()) {
                        Program.run(StorelessRuntime.with(MODEL, backend.type()), e.getKey(), StorelessRuntime.RUNTIME,
                                c, ExecuteOptions.NONE.withJudgeMode(mode));
                        got = "pass";
                    } catch (AssertFailed f) {
                        got = "AssertFailed: " + f.getMessage();
                    } catch (Throwable t) {
                        got = t.getClass().getSimpleName() + ": " + t.getMessage();
                    }
                    if (!e.getValue().equals(got)) {
                        wrong.add(backend.type() + " " + mode + " " + e.getKey() + " -> " + oneLine(got));
                    }
                }
            }
        }
        assertTrue(wrong.isEmpty(), "one verdict under every judge:\n  " + String.join("\n  ", wrong));
    }

    /** A forAll-contains subset that fails under each judge alone (its words: JUDGES-3, queued). */
    private static final String SUBSET_FAILS = "subset fails";

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
