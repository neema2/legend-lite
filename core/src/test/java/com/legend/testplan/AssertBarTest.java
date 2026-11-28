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
            new Probe("assertNotEq-fails", "{|assertNotEq(1, 1)}", "1 should not equal 1"),
            // the expected side's class printed as the actual side's (the review of B3 step 3's M1, 2026-11-24: the
            // expected side was printed with no class, "? {...}")
            new Probe("assertNotEquals-instance-fails", "{|let p = ^t::Person(name='a'); assertNotEquals($p, $p);}",
                    "t::Person {\"name\":\"a\"} should not equal t::Person {\"name\":\"a\"}"),
            new Probe("assertNotSize-holds", "{|assertNotSize([1, 2], 3)}", null),
            new Probe("assertNotSize-fails", "{|assertNotSize([1, 2], 2)}", "size should not equal: 2"),
            new Probe("assertNotContains-holds", "{|assertNotContains([1, 2], 3)}", null),
            new Probe("assertNotContains-fails", "{|assertNotContains(['a', 'b'], 'b')}",
                    "[\"a\", \"b\"] should not contain \"b\""),
            new Probe("assertIsNot-holds", "{|assertIsNot(1, 2)}", null),
            new Probe("assertIsNot-fails", "{|assertIsNot('x', 'x')}", "Should not be identical to: \"x\""),
            new Probe("assertNotEq-with-a-message", "{|assertNotEq(1, 1, 'mine')}", "mine"),
            new Probe("one-statement-helper-holds", "{|t::check(1 + 1)}", null),
            new Probe("one-statement-helper-fails", "{|t::check(3)}", "expected: 2, actual: 3"),
            new Probe("two-statement-helper-fails", "{|t::check2(3)}", "expected: 2, actual: 3"),
            new Probe("assert-in-an-if", "{|if(1 == 1, |assertEquals(1, 1), |assertEquals(1, 2))}", null),
            // an assert under an if (lite's plan's Branch node, step 4): the side not taken is never evaluated, as
            // upstream never evaluates it -- the else side's toOne() over nothing would fail if it ran
            new Probe("assert-in-an-if-fails", "{|if(1 == 2, |assertEquals(1, 1), |assertEquals(1, 2))}",
                    "expected: 1, actual: 2"),
            new Probe("if-side-not-taken-never-runs",
                    "{|if(1 == 1, |assert(true), |assertEquals(1, [1, 2]->filter(x | $x > 5)->toOne()))}", null),
            new Probe("if-over-a-let", "{|let x = 2; if($x > 1, |assertEquals(2, $x), |assertEquals(1, $x));}", null),
            new Probe("if-over-a-query",
                    "{|if([1, 2, 3]->filter(x | $x > 1)->size() == 2, |assertEquals(1, 1), |assertEquals(1, 2))}",
                    null),
            new Probe("asserts-around-an-if",
                    "{|assertEquals(1, 1); if(2 > 1, |assertEquals('a', 'a'), |assert(false)); assertEquals(2, 3);}",
                    "expected: 2, actual: 3"),
            new Probe("nested-if",
                    "{|if(1 == 1, |if(2 == 3, |assert(false, 'inner then'), |assert(false, 'inner else')),"
                            + " |assert(true))}", "inner else"),
            new Probe("if-with-a-constant-then", "{|if(1 == 1, |true, |assert(false, 'not taken')); assert(true);}",
                    null),
            new Probe("if-with-a-constant-side", "{|if(1 == 1, |assertEquals(1, 2), |true); true;}",
                    "expected: 1, actual: 2"),
            new Probe("assert-bound-by-a-let", "{|let ok = assertEquals(1, 1); assert($ok);}", null),
            new Probe("forAll-over-range", "{|range(1, 4)->forAll(i | assertEquals($i, $i))}", null),
            // per element, the operands computed by the database (step 5): the first failing element in upstream's
            // words, each operand spelled by its own kind, an empty source holding
            new Probe("forAll-over-range-fails", "{|range(1, 4)->forAll(i | assertEquals($i, 2))}",
                    "expected: 1, actual: 2 at index 0"),
            // register S139 (the user's ruling, 2026-11-05): lite compares a Decimal by value, its scale dropped
            new Probe("forAll-decimal-scale", "{|range(1, 3)->forAll(i | assertEquals(3.0D, 3.00D))}", null),
            new Probe("forAll-decimal-value", "{|range(1, 3)->forAll(i | assertEquals(3.0D, 3.10D))}",
                    "expected: 3.0, actual: 3.10 at index 0"),
            // an Integer and a Decimal are two exact numbers, equal by their value (the user's ruling of 2026-11-27;
            // until then two kinds never equal: "expected: 1 (Decimal), actual: 1 (Integer) at index 0")
            new Probe("forAll-decimal-vs-integer", "{|range(1, 3)->forAll(i | assertEquals(1D, $i))}",
                    "expected: 1, actual: 2 at index 1"),
            new Probe("forAll-over-nothing", "{|[1, 2]->filter(x | $x > 5)->forAll(i | assertEquals($i, 99))}", null),
            // an element's own caller's message (item 3's S4 step 3, the user's decision 4, 2026-11-22): read only where
            // THAT element fails, the failure whole as upstream raises it -- the element 1 holds, so its message, which
            // reads past the list's end, is never read (until then both judges dropped it: "expected: 1, actual: 0 at
            // index 1")
            new Probe("forAll-element-message-reads",
                    "{|let l = [10]; [1, 0]->forAll(i | assertEquals(1, $i, | $l->at($i)->toString()));}", "10"),
            // a mixed-kind list keeps each element's kind (register S127, the user's ruling of 2026-11-04): an Integer
            // and a Float are never equal, whatever a database's own list equality coerces (DuckDB types both
            // literals DOUBLE[]; the user, 2026-11-22)
            new Probe("mixed-kind-lists-differ", "{|assertEquals([1, 2.5, 3], [1.0, 2.5, 3])}",
                    "expected: [1, 2.5, 3], actual: [1.0, 2.5, 3]"),
            new Probe("mixed-kind-lists-equal", "{|assertEquals([1, 2.5, 3], [1, 2.5, 3])}", null),
            // a String whose text starts with a bracket is a String, written quoted, never read as a JSON carrier (the
            // S4 audit's HIGH 1, 2026-11-22: written as minified JSON until then, assertEq's differently per judge)
            new Probe("string-with-a-bracket", "{|assertEquals('[a b]', '[c]')}", "expected: \"[a b]\", actual: \"[c]\""),
            new Probe("eq-string-with-a-brace", "{|assertEq('{a}', '{b}')}", "expected: \"{a}\", actual: \"{b}\""),
            new Probe("forAll-element-literal-message", "{|[1, 2]->forAll(i | assertEquals(1, $i, 'mine'))}", "mine"),
            // the same over a row the database judge has not typed (a Decimal against an Integer, decided false at
            // planning; dates): the caller's message whole on every judge (the S4 step 3 audit's HIGH, 2026-11-22:
            // "mine at index 0" on the database judge until then)
            new Probe("forAll-element-message-cross-kinds", "{|[1, 2]->forAll(i | assertEquals(1D, $i, 'mine'))}",
                    "mine"),
            new Probe("forAll-element-message-dates",
                    "{|[%2020-01-01, %2020-01-02]->forAll(d | assertEquals(%2020-01-02, $d, 'mine'))}", "mine"),
            new Probe("forAll-element-message-holds",
                    "{|let l = [10]; [1, 1]->forAll(i | assertEquals(1, $i, | $l->at($i)->toString()));}", null),
            // a rendered text is a String, compared as one under every judge (item 3's S4, phase 2, the user's Q-A):
            // the golden computed by the database too, never joined in Java
            new Probe("rendered-join-holds", "{|assertEquals('a,b', ['a', 'b']->joinStrings(','))}", null),
            new Probe("rendered-join-golden-folded", "{|assertEquals('a' + ',' + 'b', ['a', 'b']->joinStrings(','))}",
                    null),
            new Probe("rendered-join-fails", "{|assertEquals('a;b', ['a', 'b']->joinStrings(','))}",
                    "expected: \"a;b\", actual: \"a,b\""),
            new Probe("rendered-makeString-fails", "{|assertEquals('[1|2]', [1, 2]->makeString('[', '|', ']') + '!')}",
                    "expected: \"[1|2]\", actual: \"[1|2]!\""),
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
                    "expected: t::Person, actual: t::Employee"),   // lite's sentence since census J29 (2026-11-18)
            new Probe("concatenate-keeps-order", "{|assertEquals([3, 1, 2], [1, 2]->concatenate([3]))}",
                    "expected: [3, 1, 2], actual: [1, 2, 3]"),
            new Probe("union-keeps-order", "{|assertEquals([3, 1, 2], [1, 2]->union([3]))}",
                    "expected: [3, 1, 2], actual: [1, 2, 3]"),
            // exact, as upstream (S132's float tokens are backwards-compatibility mode's alone: ExecuteOptions
            // .withEngineCompat, the PCT and corpus harnesses; the user, 2026-11-04)
            // a Float in a failure message at full round-trip precision, the same text under every judge on every
            // database (the orchestrator's ruling, 2026-11-17: a message is a diagnostic; S311's six decimals are a
            // Float's toString in a query)
            // the operands parsed so each is the double (a Float literal's arithmetic is the database's DECIMAL until
            // E-P5's S20 lands)
            new Probe("float-message-sum", "{|assertEquals(0.3, parseFloat('0.1') + parseFloat('0.2'))}",
                    "expected: 0.3, actual: 0.30000000000000004"),
            // past DECIMAL(38,18): Java's exponent form, every database's text normalized to it
            new Probe("float-message-tiny", "{|assertEquals(1.0, parseFloat('1.0E-300'))}",
                    "expected: 1.0, actual: 1.0E-300"),
            new Probe("float-message-negative-zero", "{|assertEquals(1.0, parseFloat('-0.0'))}",
                    "expected: 1.0, actual: -0.0"),
            new Probe("float-message-product", "{|assertEquals(1.0, parseFloat('123456789.123456789') * 3.0)}",
                    "expected: 1.0, actual: 370370367.3703704"),
            new Probe("float-looking-strings", "{|assertEquals('x=1.0000000000000002', 'x=1.0')}",
                    "expected: \"x=1.0000000000000002\", actual: \"x=1.0\""),
            new Probe("decimal-scale", "{|assertEquals(3.0D, 3.00D)}", null),
            new Probe("decimal-value", "{|assertEquals(3.0D, 3.10D)}", "expected: 3.0, actual: 3.10"),
            // (two written types are a compile error on lite's own door: anAssertOfTwoKnownTypesIsRefused...; the judge's
            // rule is held here by sides typed only when the test runs)
            new Probe("integer-vs-float", "{|assertEquals(1->cast(@Number), 1.0->cast(@Number))}",
                    "expected: 1, actual: 1.0"),
            // the ledger audit's D4 (2026-11-16): a list of Integers against the same numbers as Floats fails, as the
            // one-value probe above does; the message is the failed typed comparison's, never an untyped re-compare
            // that held (which passed the assertSameElements and raised "verdict/message divergence" on assertEquals)
            // (two sides of two KNOWN types are a compile error on lite's own door: anAssertOfTwoKnownTypesIsRefused...)
            new Probe("integer-vs-float-lists-typed-at-run-time",
                    "{|assertEquals([1, 2]->cast(@Number), [1.0, 2.0]->cast(@Number))}",
                    "expected: [1, 2], actual: [1.0, 2.0]"),
            new Probe("integer-vs-float-same-elements-typed-at-run-time",
                    "{|assertSameElements([2, 1]->cast(@Number), [2.0, 1.0]->cast(@Number))}",
                    "expected the elements [2, 1] in any order, got [2.0, 1.0]"),
            // the type is part of the value in lite's own runs (register S127, the user's ruling of 2026-11-04;
            // backwards-compatibility mode compares an Integer and a Decimal by text: below)
            // (an Integer and a Decimal of one value are equal: two exact numbers, the user's ruling of 2026-11-27)
            new Probe("decimal-vs-integer", "{|assertEquals(1D, 1)}", null),
            new Probe("decimal-vs-integer-values", "{|assertEquals(1.5D, 1)}", "expected: 1.5, actual: 1"),
            new Probe("float-vs-decimal-typed-at-run-time", "{|assertEquals(1.5->cast(@Number), 1.5D->cast(@Number))}",
                    "expected: 1.5 (Float), actual: 1.5 (Decimal)"),
            new Probe("assertNotEquals-across-kinds-typed-at-run-time",
                    "{|assertNotEquals(3->cast(@Number), 3.0->cast(@Number))}", null),
            new Probe("assertNotEquals-exact-numbers", "{|assertNotEquals(3, 3D)}", "3 should not equal 3"),
            new Probe("mixed-list", "{|assertEquals([1, 'a', 2.5], [1, 'a', 2.5])}", null),
            new Probe("empty-vs-filtered", "{|assertEquals([], [1, 2]->filter(x | $x > 5))}", null),
            new Probe("assertError-let-message", "{|let m = 'boom'; assertError(|fail($m), $m);}", null),
            new Probe("assertError-over-a-failing-assert",
                    "{|assertError(|assertEquals(1, 2), 'expected: 1, actual: 2')}", null),
            // an assert's failure is the error assertError matches, as upstream's assertError catches it
            new Probe("assertError-over-a-holding-assert", "{|assertError(|assertEquals(1, 1), 'boom')}",
                    "Execution error message mismatch. The actual message was \"No error was thrown\" where the expected message was:\"boom\""),
            new Probe("assertError-over-a-different-failure", "{|assertError(|assert(false, 'one'), 'two')}",
                    "Execution error message mismatch. The actual message was \"one\" where the expected message"
                            + " was:\"two\""),
            new Probe("assertEmpty-holds", "{|assertEmpty([1, 2]->filter(x | $x > 5))}", null),
            new Probe("assertSize-fails", "{|assertSize([1, 2], 3)}", "expected size: 3, actual size: 2"),
            new Probe("assertContains-fails", "{|assertContains([1, 2], 3)}", "[1, 2] does not contain 3"),
            new Probe("assertSameElements-holds", "{|assertSameElements([2, 1], [1, 2])}", null),
            new Probe("assertEqWithinTolerance-holds", "{|assertEqWithinTolerance(1.0, 1.0005, 0.001)}", null),
            new Probe("second-assert-fails", "{|assertEquals(1, 1); assertEquals('a', 'b');}",
                    "expected: \"a\", actual: \"b\""),
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
                    "expected: 2020-01-01T10:00:00.000000000Z, actual: 2020-01-01T10:00:01.000000000Z"),
            // register S399 (the user's ruling of 2026-11-22): lite's product compares a date by its kind and its
            // instant, so two date-times at one instant are equal whatever their written precision (legend-pure: not
            // equal, AbstractPureDate.java:30-59; every route lite's since B3's one rulebook, 2026-11-24)
            new Probe("assertEquals-datetime-precision-is-no-value",
                    "{|assertEquals(%2020-01-01T10:00:00.000+0000, %2020-01-01T10:00:00+0000)}", null),
            new Probe("datetime-trailing-zeros-equal", "{|assertEquals(%2020-01-01T10:00:00.5, %2020-01-01T10:00:00.500)}",
                    null),
            new Probe("datetime-hour-equals-its-instant", "{|assertEquals(%2020-01-01T10, %2020-01-01T10:00:00)}", null),
            new Probe("datetime-literal-against-computed",
                    "{|assertEquals(%2020-01-01T10:00:00, [%2020-01-01T10:00:00.000, %2020-01-01T11:00:00]->at(0))}",
                    null),
            new Probe("datetime-literal-against-computed-fails",
                    "{|assertEquals(%2020-01-01T10:00:00, [%2020-01-01T10:00:01]->at(0))}",
                    "expected: 2020-01-01T10:00:00.000000000Z, actual: 2020-01-01T10:00:01.000000000Z"),
            // the kinds stay apart (S399, as legend-pure): a day is no date-time, a year no year-month
            // (a written day against a written date-time is a compile error on lite's own door:
            // anAssertOfTwoKnownTypesIsRefused...)
            new Probe("strictdate-against-computed", "{|assertEquals(%2020-01-02, [%2020-01-01, %2020-01-02]->at(1))}",
                    null),
            new Probe("assertEquals-datetime-hour-message", "{|assertEquals(%2020-01-01T10, %2020-01-01T11)}",
                    "expected: 2020-01-01T10:00:00.000000000Z, actual: 2020-01-01T11:00:00.000000000Z"),
            // contains and the subset fold compare a date-time by its instant too (the dates audit's HIGH 1, 2026-11-22:
            // they compared the written texts on the database judge)
            new Probe("contains-datetime-by-instant", "{|assertContains([%2020-01-01T10:00:00.5,"
                    + " %2020-01-02T00:00:00.5], %2020-01-01T10:00:00.50)}", null),
            new Probe("contains-datetime-second-against-written-list", "{|assertContains([%2020-01-01T10:00:00.000,"
                    + " %2020-01-02T00:00:00.5], %2020-01-01T10:00:00)}", null),
            new Probe("subset-datetime-by-instant", "{|assert([%2020-01-01T10:00:00.5]->forAll(e |"
                    + " [%2020-01-01T10:00:00.50, %2020-01-02T10]->contains($e)))}", null),
            new Probe("contains-strictdate", "{|assertContains([%2020-01-01, %2020-01-02], %2020-01-02)}", null),
            // an empty side (Nil) against a typed one: the same only where both are empty (item 3's S4, 2026-11-22)
            new Probe("empty-against-filtered-empty", "{|assertEquals([], [1, 2]->filter(x | $x > 5))}", null),
            new Probe("empty-against-filtered-values", "{|assertEquals([], [1, 2]->filter(x | $x > 1))}",
                    "expected: [], actual: [2]"),
            new Probe("empty-against-strings", "{|assertEquals([], ['a', 'b']->filter(x | $x == 'c'))}", null),
            new Probe("empty-against-dates", "{|assertEquals([], [%2020-01-01]->filter(d | $d > %2021-01-01))}", null),
            // a side the types say holds nothing (Nil), its emptiness typed (item 3's S4)
            new Probe("nil-side-empty", "{|assertEmpty([]->limit(2))}", null),
            new Probe("nil-side-not-empty", "{|assertNotEmpty([]->drop(1))}", "Expected non-empty collection"),
            // assertInstanceOf decided by the value's static type: the value and the type ride the failure, the
            // host's record (item 3's S4)
            new Probe("instanceOf-decided-holds", "{|assertInstanceOf(1 + 1, Integer)}", null),
            new Probe("instanceOf-decided-fails", "{|assertInstanceOf('x', Integer)}",
                    "expected an instance of Integer, got \"x\""),
            new Probe("instanceOf-decided-fails-number", "{|assertInstanceOf(2.5, String)}",
                    "expected an instance of String, got 2.5"),
            // an instance of a generic class (a Pair) written as upstream's toRepresentation writes any instance,
            // '<id instanceOf path>', its path the raw class's
            new Probe("assertEquals-pairs-message", "{|assertEquals(pair(1, 'a'), pair(1, 'b'))}",
                    "expected: meta::pure::functions::collection::Pair {\"first\":1,\"second\":\"a\"}, actual: meta::pure::functions::collection::Pair {\"first\":1,\"second\":\"b\"}"),
            new Probe("assertEquals-pair-collections-message",
                    "{|assertEquals([pair(1, 'a'), pair(2, 'b')], [pair(1, 'b'), pair(2, 'b')])}",
                    "expected: [meta::pure::functions::collection::Pair {\"first\":1,\"second\":\"a\"}, meta::pure::functions::collection::Pair {\"first\":2,\"second\":\"b\"}], actual: [meta::pure::functions::collection::Pair {\"first\":1,\"second\":\"b\"}, "
                            + "meta::pure::functions::collection::Pair {\"first\":2,\"second\":\"b\"}]"),
            new Probe("assertSameElements-pairs-message",
                    "{|assertSameElements([pair(2, 'b'), pair(1, 'a')], [pair(1, 'a'), pair(3, 'c')])}",
                    "expected the elements [meta::pure::functions::collection::Pair {\"first\":2,\"second\":\"b\"}, meta::pure::functions::collection::Pair {\"first\":1,\"second\":\"a\"}] in any order, "
                            + "got [meta::pure::functions::collection::Pair {\"first\":1,\"second\":\"a\"}, meta::pure::functions::collection::Pair {\"first\":3,\"second\":\"c\"}]"),
            new Probe("assertContains-pairs-message", "{|assertContains([pair(1, 'a')], pair(2, 'b'))}",
                    "[meta::pure::functions::collection::Pair {\"first\":1,\"second\":\"a\"}] does not contain meta::pure::functions::collection::Pair {\"first\":2,\"second\":\"b\"}"),
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
                    "expected: [\"a\", \"b\"], actual: [\"b\", \"c\"]"),
            new Probe("assertEquals-one-against-many", "{|assertEquals(1, [1, 2])}", "expected: [1], actual: [1, 2]"),
            new Probe("assertEquals-empty-against-one", "{|assertEquals([], [1])}", "expected: [], actual: [1]"),
            new Probe("assertEmpty-message", "{|assertEmpty([1, 2])}", "[1, 2] is not empty"),
            new Probe("assertNotEmpty-message", "{|assertNotEmpty([])}", "Expected non-empty collection"),
            new Probe("assertContains-strings-message", "{|assertContains(['a'], 'b')}", "[\"a\"] does not contain \"b\""),
            // assertIs over two primitives: compiled pure's `left == right`, their values (the engine runner)
            new Probe("assertIs-primitives-holds", "{|assertIs('a', 'a')}", null),
            new Probe("assertIs-primitives-fails", "{|assertIs(1, 2)}", "expected: 1, actual: 2"),
            // the forAll-contains fold of an assert's condition fails as the condition does, under every judge (the
            // S2+S3 fixes' audit, 2026-11-21: the host wrote a contains record, the database judge a frame of its own)
            new Probe("assert-subset-fails", "{|assert([1, 3]->forAll(e | [1, 2]->contains($e)))}",
                    "Assert failed"),
            new Probe("assertFalse-subset-fails", "{|assertFalse([1]->forAll(e | [1, 2]->contains($e)))}",
                    "Assert failed"),
            // an if closing the body whose other side is the literal true
            new Probe("branch-true-side-last", "{|if(1 == 2, |assertEquals(1, 1), |true);}", null),
            new Probe("branch-true-side-last-fails", "{|if(1 == 1, |assertEquals(1, 2), |true);}",
                    "expected: 1, actual: 2"),
            // assertError when nothing is raised (item 3's S8, Q-B): legend-pure's matcher reads its own "nothing was
            // raised" text, so expecting it passes, the inner-assert form too; any other expected text fails NO_ERROR
            new Probe("assertError-expects-unraised", "{|assertError(|1, 'No error was thrown')}", null),
            new Probe("assertError-inner-assert-unraised",
                    "{|assertError(|assertEquals(1, 1), 'No error was thrown')}", null),
            new Probe("assertError-nothing-raised", "{|assertError(|1, 'boom')}",
                    "Execution error message mismatch. The actual message was \"No error was thrown\" where the expected message was:\"boom\""),
            // the table member (item 3's S4, 2026-11-22): the database's verdict over the typed cells, each grid's rows
            // riding its failure as typed row structs for the host's record -- one record under every judge, the
            // grids whose columns differ included (until then the database judge's sentence of its own)
            new Probe("tdsEquivalent-holds", "{|assertTdsEquivalent(#TDS\n  id, v\n  1, 2.00\n  2, 3.001\n#, #TDS\n"
                    + "  id, v\n  1, 2.0\n  2, 3.0\n#, 0.01)}", null),
            new Probe("tdsEquivalent-fails", "{|assertTdsEquivalent(#TDS\n  id, v\n  1, 2.5\n#, #TDS\n"
                    + "  id, v\n  1, 2.0\n#, 0.01)}", "expected the rows [[1, 2.5]] within 0.01, got [[1, 2.0]]"),
            new Probe("tdsEquivalent-text-cells", "{|assertTdsEquivalent(#TDS\n  id, s\n  1, x\n#, #TDS\n"
                    + "  id, s\n  1, y\n#, 0.01)}", "expected the rows [[1, \"x\"]] within 0.01, got [[1, \"y\"]]"),
            new Probe("tdsEquivalent-row-count", "{|assertTdsEquivalent(#TDS\n  id\n  1\n  2\n#, #TDS\n"
                    + "  id\n  1\n#, 0.01)}", "expected the rows [[1], [2]] within 0.01, got [[1]]"),
            new Probe("tdsEquivalent-columns-differ", "{|assertTdsEquivalent(#TDS\n  id\n  1\n#, #TDS\n"
                    + "  ix\n  1\n#, 0.01)}", "expected the rows [[1]] within 0.01, got [[1]]"));

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
                // the product's kind, in the one catalog's words: legend-pure's (stage B's BE, 2026-11-21)
                String said = outcome(c, "{|let x = [1, 2]; assertEmpty([1], | $x->at(5)->toString());}", mode);
                assertTrue(said.startsWith("DataError: The system is trying to get an element at offset 5 where the"
                        + " collection is of size "), mode + ": " + said);
            }
        }
    }

    /** An assertError over one assert whose own message raises: where the assert fails, that error is what f raised,
     *  so the assertError holds under the host judge; the database judge refuses the body by name (a raising row cannot
     *  raise another error, until S4), never dropping the error into the assert's own failure (the S2+S3 fixes' audit,
     *  HIGH, 2026-11-21). */
    @Test
    void anAssertErrorOverAnAssertWhoseMessageRaisesCatchesThatError() throws Exception {
        String body = "{|let x = [1, 2]; assertError(|assertEmpty([1], | $x->at(5)->toString()),"
                + " 'The system is trying to get an element at offset 5 where the collection is of size 2');}";
        try (Connection c = TestBackends.platform().open()) {
            for (com.legend.testplan.PlanningOptions.JudgeMode mode : JUDGES) {
                String said = outcome(c, body, mode);
                assertTrue(mode == com.legend.testplan.PlanningOptions.JudgeMode.HOST ? said.equals("pass")
                        : said.contains("the program road does not plan this body"), mode + ": " + said);
            }
        }
    }

    /** AN ASSERT OF TWO KNOWN TYPES IS A COMPILE ERROR ON LITE'S OWN DOOR (the user's ruling of 2026-11-26, "yes compile
     *  error": an assert that compares two different types is the mistake a comparison of them is; built with the
     *  edition program's step 6): refused when the body is compiled, under every judge alike, naming the assert and
     *  the two types. Numbers are of two families, so an Integer against a Decimal is by value and no mistake; a
     *  written literal reads as the type of the value it is written against; sides typed only when the test runs (a
     *  mixed list, a Number, an Any) are judged by the types they hold, as before. The compatibility doors judge an
     *  assert as they did ({@code NumberKindsEdgeTest}, {@code EqualityCompatEdgeTest}). */
    @Test
    void anAssertOfTwoKnownTypesIsRefusedWhenTheBodyIsCompiled() throws Exception {
        java.util.Map<String, String> refused = new java.util.LinkedHashMap<>();
        refused.put("{|assertEquals(1, 1.0)}", "assertEquals compares Integer with Float");
        refused.put("{|assertEquals(1.5, 1.5D)}", "assertEquals compares Float with Decimal");
        refused.put("{|assertEquals([1, 2], [1.0, 2.0])}", "assertEquals compares Integer with Float");
        refused.put("{|assertSameElements([2, 1], [2.0, 1.0])}", "assertSameElements compares Integer with Float");
        refused.put("{|assertNotEquals(3, 3.0)}", "assertNotEquals compares Integer with Float");
        refused.put("{|assertEq('a', 1)}", "assertEq compares String with Integer");
        refused.put("{|assertEquals(1, 1.0, 'mine')}", "assertEquals compares Integer with Float");
        refused.put("{|assertContains([1, 2], 1.0)}", "assertContains compares Integer with Float");
        refused.put("{|let x = 1; assertEquals(1.5, $x);}", "assertEquals compares Float with Integer");
        refused.put("{|assertEquals(%2020-01-01, %2020-01-01T00:00:00)}", "assertEquals compares StrictDate with DateTime");
        refused.put("{|assertEquals('1', 1)}", "assertEquals compares String with Integer");
        refused.put("{|assertEquals('1'->fromJson(), 1)}", "assertEquals compares " + "meta::pure::metamodel::variant::Variant" + " with Integer");
        java.util.Map<String, String> judged = new java.util.LinkedHashMap<>();
        judged.put("{|assertEquals(1D, 1)}", "pass");
        judged.put("{|assertEquals([1, 2], [1D, 2.00D])}", "pass");
        judged.put("{|let x = 1.0; assertEquals(1, $x);}", "pass");
        judged.put("{|let x = 2.5D; assertEquals(2.5, $x);}", "pass");
        judged.put("{|assertEquals([1, 'a'], [1, 'a'])}", "pass");
        List<String> wrong = new ArrayList<>();
        try (Connection c = TestBackends.platform().open()) {
            for (com.legend.testplan.PlanningOptions.JudgeMode mode : JUDGES) {
                for (var e : refused.entrySet()) {
                    String got = outcome(c, e.getKey(), mode);
                    if (!got.contains(e.getValue()) || !got.contains("two values are equal only when")) {
                        wrong.add(mode + " " + e.getKey() + " -> " + oneLine(got));
                    }
                }
                for (var e : judged.entrySet()) {
                    String got = outcome(c, e.getKey(), mode);
                    if (!got.equals(e.getValue())) {
                        wrong.add(mode + " " + e.getKey() + " -> " + oneLine(got));
                    }
                }
            }
        }
        assertTrue(wrong.isEmpty(), "\n  " + String.join("\n  ", wrong));
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
     *  integer, its declaration says Decimal. Lite's own rule (register S127 as amended 2026-11-27) calls an Integer and
     *  a Decimal of one value equal, two exact numbers, so the Integer 1 and the property's Decimal 1 are equal -- under
     *  the host judge as under the database judge, and under both together: one outcome, and one message, per assert,
     *  on DuckDB, H2 and SQLite alike (until that ruling two kinds never equal, compared by their DECLARED kinds). Its row
     *  is the connection's declared test data, a load of the plan's; {@code %s} its database. */
    private static final String MAPPED_ON = """
            ###Pure
            Class p::P { id: Integer[1]; d: Decimal[1]; }
            Class p::Q { id: Integer[1]; e: Decimal[1]; }
            Class p::K { <<equality.Key>> id: Integer[1]; <<equality.Key>> at: DateTime[1]; note: String[1]; }
            Class p::KN { <<equality.Key>> k: p::K[1]; <<equality.Key>> f: Float[1]; }
            Class p::N { name: String[1]; }
            Enum p::E { A, B }
            Class p::KA { <<equality.Key>> a: Any[1]; }
            Class p::KVN { <<equality.Key>> id: Integer[1]; v: Variant[1]; }
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
        // the body, and its one outcome: the Integer 1 is the Decimal 1 in lite's own runs -- two exact numbers, equal
        // by their value (S127 as amended by the user's ruling of 2026-11-27; until then two kinds never equal, and
        // each of the four the opposite)
        java.util.Map<String, String> want = new java.util.LinkedHashMap<>();
        for (String side : List.of(d, mapped)) {
            want.put("{|assertEq(1, " + side + ")}", "pass");
            want.put("{|assertIs(1, " + side + ")}", "pass");
            want.put("{|assertNotEq(1, " + side + ")}", "AssertFailed: 1 should not equal 1");
            want.put("{|assertIsNot(1, " + side + ")}", "AssertFailed: Should not be identical to: 1");
        }
        // the database judge's text of a store-read Decimal drops only its carrier's D: its sign and digits kept (a
        // DECIMAL(10,2) read decodes scale-canonical, 1.50 as 1.5, under both judges: LiteralSpelling.wireValueEgress's
        // R3), and a written literal keeps its scale
        want.put("{|assertEq(1, p::Q.all()->filter(x | $x.id == 1)->toOne().e)}", "AssertFailed: expected: 1, actual: 1.5");
        want.put("{|assertEq(2, p::Q.all()->filter(x | $x.id == 2)->toOne().e)}", "AssertFailed: expected: 2, actual: -2");
        want.put("{|assertEq(1, 1.50D)}", "AssertFailed: expected: 1, actual: 1.50");
        want.put("{|assertEq(2, -2D)}", "AssertFailed: expected: 2, actual: -2");
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

    /** Each body one outcome under every judge on each database the platform runs in-process, the host's there the
     *  reference (item 3's S4, batch 2's audit, 2026-11-22: an empty side on SQLite, whose list of no rows is
     *  {@code []}; Decimal literals' digits on SQLite, a REAL there; the cross-kind pairs a carrier check declined). The
     *  databases may answer apart (SQLite's REAL keeps fifteen digits; DuckDB's and SQLite's list of Decimals is one
     *  scale): the product's facts, not the judges'. */
    @Test
    void eachBodyIsOneOutcomeUnderEveryJudgeOnEachDatabase() throws Exception {
        List<String> bodies = List.of(
                "{|assertEquals([], [])}",
                "{|assertEquals([], [1, 2]->filter(x | $x > 5))}",
                "{|assertEquals([], [1, 2]->filter(x | $x > 1))}",
                "{|assertEquals([], [%2020-01-01]->filter(d | $d > %2021-01-01))}",
                "{|assertNotEquals([], [1, 2]->filter(x | $x > 5))}",
                "{|assertEmpty([1, 2]->filter(x | $x > 5))}",
                "{|assertEmpty([1, 2]->filter(x | $x > 1))}",
                "{|assertEquals([1, 2]->filter(x | $x > 5), [1.5]->filter(x | $x > 5.0))}",
                "{|assertEquals(2.5D, 1D)}",
                "{|assertEquals(1D, 2D)}",
                "{|assertEquals(1D, 1)}",
                "{|assertEquals(123456789012345.67D, 1D)}",
                "{|assertEquals([0.1234567890123456789D], [1D])}",
                "{|assertEquals([1.0D, 10.125D], [1.0D, 10.125D])}",
                "{|assertEquals([1.0D, 10.125D], [1.0D])}",
                "{|assertEquals(1, p::Q.all()->filter(x | $x.id == 1)->toOne().e)}",
                "{|assertEquals(2.5, p::Q.all()->filter(x | $x.id == 1)->toOne().e)}",
                // tagged values (a Number's max, a mixed list): each value its kind and its value (item 3's S4)
                "{|assertEquals(2.5, max([1, 2.5]))}",
                "{|assertEquals(1, max([1, 0.5]))}",
                "{|assertEquals(1.0, max([1, 0.5]))}",
                "{|assertEquals(0.5, min([1, 0.5]))}",
                "{|assertNotEquals(1, max([1, 0.5]))}",
                // a side the types say holds nothing
                "{|assertEmpty([]->limit(2))}",
                "{|assertNotEmpty([]->drop(1))}",
                // Pairs, on the seven-column row (a JSON carrier compares written text: batch 4's audit, H1; a Pair with
                // a DateTime or a Decimal field fails on H2's seven-column row, pre-existing, the remaining shapes)
                "{|assertEquals(pair(1, 'a'), pair(1, 'a'))}",
                "{|assertEquals(pair(1, 'a'), pair(1, 'b'))}",
                "{|assertEquals([pair(1, 'a'), pair(2, 'b')], [pair(1, 'b'), pair(2, 'b')])}",
                "{|assertSameElements([pair(1, 'a'), pair(2, 'b')], [pair(2, 'b'), pair(1, 'a')])}",
                // Decimals of several scales in a tagged list: each recorded at its own scale (batch 3's audit, H1)
                "{|assertEquals([1, 3.50D], [2, 3.5D])}",
                "{|assertEquals([2, 10.125D], [1, 1.5D])}",
                "{|assertEquals([1, 1.5D, 10.125D], [1, 1.5D, 10.12D])}",
                "{|assertEquals([3, 1.0, 2]->sort(), [1.0, 2, 3])}",
                // dates at the judge's instant (register S399; the dates round, 2026-11-22): one instant whatever its
                // written precision, to the microsecond, SQLite's date-time texts included
                "{|assertEquals(%2020-01-01T10:00:00.5, %2020-01-01T10:00:00.500)}",
                "{|assertNotEquals(%2020-01-01T10:00:00.000, %2020-01-01T10:00:00)}",
                "{|assertEquals(%2020-01-01T10:00:00.123456789, %2020-01-01T10:00:00.123456)}",
                "{|assertEquals(%2020-01-01T10, %2020-01-01T10:00:00)}",
                "{|assertContains([%2020-01-01T10:00:00.5, %2020-01-02T00:00:00.5], %2020-01-01T10:00:00.50)}",
                "{|assertEquals(%2020-01-01T10:00:00.123456789, [%2019-01-01, %2020-01-01T10:00:00.123456789]->max())}",
                "{|assertEquals(%2020-01-01T10:00, [%2019-01-01, %2020-01-01T10:00:00]->max())}",
                // a Date written as its text (a partial date, a mixed Date list's element): each value its kind, as
                // its text says, and its value (the dates audit's HIGH 2)
                "{|assertEquals(%2020-01, [%2020-01, %2021-01]->at(0))}",
                "{|assertEquals(%2020, [%2020, %2021]->at(1))}",
                "{|assertEquals(%2020-01, [%2020-01-01, %2021-01]->at(0))}",
                "{|assertEquals(%2020-01-01T10:00:00, [%2019-01, %2020-01-01T10:00:00.000]->at(1))}",
                "{|assertNotEquals(%2020-01-01, [%2019-01, %2020-01-01]->at(1))}",
                // assertIsNot over a tagged value: its record un-tagged (batch 3's audit, H2)
                "{|assertIsNot(3, [1, 2.0, 3]->max())}",
                "{|assertIsNot(4, [1, 2.0, 3]->max())}");
        List<String> wrong = new ArrayList<>();
        for (String body : bodies) {
            for (var backend : TestBackends.inProcess()) {
                String reference = null;
                var database = backend.type();
                String on = MAPPED_ON.formatted(database);
                for (com.legend.testplan.PlanningOptions.JudgeMode mode : JUDGES) {
                    String got;
                    try {
                        Program.runFresh(on, body, "p::RT", database, ExecuteOptions.NONE.withJudgeMode(mode),
                                com.legend.testexec.NoEdgeRun.NONE);
                        got = "pass";
                    } catch (AssertFailed f) {
                        got = "AssertFailed: " + f.getMessage();
                    } catch (Throwable t) {
                        got = t.getClass().getSimpleName() + ": " + t.getMessage();
                    }
                    if (reference == null) {
                        reference = got;
                    } else if (!reference.equals(got)) {
                        wrong.add(database + " " + mode + " " + body + " -> " + oneLine(got) + " (reference: "
                                + oneLine(reference) + ")");
                    }
                }
            }
        }
        assertTrue(wrong.isEmpty(), "one outcome under every judge on each database:\n  " + String.join("\n  ",
                wrong));
    }

    /** Failing asserts of every family, the bodies the two checks below run (item 3's S5). */
    private static final List<String> FAILING = List.of(
            "{|assertEquals(1, 2)}", "{|assertEquals([1, 2], [2, 1])}", "{|assertEquals('a', 'b')}",
            "{|assertEquals(1.5D, 2.5D)}", "{|assertEquals(%2020-01-01, %2020-01-02)}",
            "{|assertSameElements([1, 2], [1, 3])}", "{|assertNotEquals(1, 1)}", "{|assertSize([1, 2], 3)}",
            "{|assertEmpty([1])}", "{|assertNotEmpty([]->cast(@Integer))}", "{|assertContains([1, 2], 3)}",
            "{|assertNotContains([1, 2], 2)}", "{|assertEqWithinTolerance(1.0, 2.0, 0.1)}", "{|assert(1 == 2)}",
            "{|assertFalse(1 == 1)}", "{|assertInstanceOf(1, String)}", "{|assert(1 == 2, | 'my message')}",
            "{|assertEquals([1, 'a'], [1, 'b'])}", "{|assertJsonStringsEqual('{\"a\":1}', '{\"a\":2}')}",
            "{|[1, 2]->forAll(i | assertEquals(1, $i))}",
            "{|assertEquals('1'->fromJson()->cast(@Any), 2)}",
            // two primitives of different kinds, and an enumeration against its name's String (the final audit of
            // c758fe95: the database judge's cross-kind gate had failed with a planned sentence and no record), each
            // typed only when the test runs: two WRITTEN types are a compile error on lite's own door (2026-11-27)
            "{|assertEquals('1'->cast(@Any), 1)}", "{|assertSameElements(['1']->cast(@Any), [1])}",
            "{|assertEq(true->cast(@Any), 1)}", "{|assertEquals(p::E.A->cast(@Any), 'A')}",
            "{|assertEquals(execute(|p::Q.all()->project([x | $x.id], ['id'])->sort('id'), p::M, p::RT.runtimeValue,"
                    + " []).values, [1, 3])}");

    /** ONE CATALOG (item 3's S5, docs/records/compat-assert-records-2026-11-21.md section 6): every failed assert on
     *  lite's road, under every judge, is a record of the one catalog and its message is that record's sentence --
     *  no judge words a failure of its own. A refusal by name (an unjudged decline) is not a failure and carries
     *  none. */
    @Test
    void everyFailureIsItsRecordsSentence() throws Exception {
        List<String> wrong = new ArrayList<>();
        int checked = 0;
        for (var backend : TestBackends.inProcess()) {
            var database = backend.type();
            for (String body : FAILING) {
                for (com.legend.testplan.PlanningOptions.JudgeMode mode : JUDGES) {
                    try {
                        Program.runFresh(MAPPED_ON.formatted(database), body, "p::RT", database,
                                ExecuteOptions.NONE.withJudgeMode(mode), com.legend.testexec.NoEdgeRun.NONE);
                        wrong.add(database + " " + mode + " " + body + " -> passed (a FAILING body)");
                    } catch (AssertFailed f) {
                        if (f.unjudgedReason() != null) {
                            continue;
                        }
                        checked++;
                        if (f.record() == null) {
                            wrong.add(database + " " + mode + " " + body + " -> no record: " + oneLine(f.getMessage()));
                        } else if (!f.getMessage().equals(f.record().sentence())) {
                            wrong.add(database + " " + mode + " " + body + " -> \"" + oneLine(f.getMessage())
                                    + "\" is not its record's \"" + oneLine(f.record().sentence()) + "\"");
                        }
                    } catch (com.legend.sql.DialectCapability | com.legend.error.NotImplementedException refused) {
                        // a refusal by name (a database without a carrier for the shape): no failure, no record
                    } catch (RuntimeException other) {
                        wrong.add(database + " " + mode + " " + body + " -> " + oneLine(other.toString()));
                    }
                }
            }
        }
        assertTrue(wrong.isEmpty(), "one catalog:\n  " + String.join("\n  ", wrong));
        // a floor: most runs fail with a record (measured 188 of 198, 2026-11-23, the audit of 8477c703), so the check
        // reads records, never passes on refusals alone
        assertTrue(checked >= 150, "records checked: " + checked);
    }

    /** NO PROSE IN A STATEMENT (item 3's S5): the database judge plans no catalog sentence -- no template's fixed text
     *  ("expected: ", ", actual: ", " to contain ", ...) is in any planned statement or plan field of a failing assert's
     *  plan; the sentence is written from the record after the run, by the one printer. */
    @Test
    void noPlannedStatementHoldsACatalogSentence() throws Exception {
        List<String> fragments = new ArrayList<>();
        for (com.legend.error.AssertKind k : com.legend.error.AssertKind.values()) {
            for (String part : k.template().split("\\{[^}]*\\}")) {
                // a template's fixed text with its spaces, of two words or a joint (", got "): a lone word -- a slot's
                // name, "expected" -- is a record's data in the plan, never prose (the audit of 8477c703: the EQUALS
                // sentence's joint had gone unchecked)
                if (part.strip().length() >= 4 && part.contains(" ") && !part.strip().matches("[a-z]+")) {
                    fragments.add(part);
                }
            }
        }
        List<String> wrong = new ArrayList<>();
        int read = 0;
        for (var backend : TestBackends.inProcess()) {
            var database = backend.type();
            for (String body : FAILING) {
                String plan;
                read++;
                try {
                    plan = com.legend.testexecutionplan.TestPlanJson.write(Program.plan(MAPPED_ON.formatted(database),
                            body, "p::RT", ExecuteOptions.NONE.withJudgeMode(
                                    com.legend.testplan.PlanningOptions.JudgeMode.DATABASE)));
                } catch (RuntimeException refused) {
                    read--;
                    continue;   // refused at planning, by name: nothing planned
                }
                for (String f : fragments) {
                    if (plan.contains(f)) {
                        wrong.add(database + " " + body + " plans the catalog's text \"" + f + "\"");
                    }
                }
            }
        }
        assertTrue(!fragments.isEmpty() && wrong.isEmpty(), "no prose in a statement:\n  " + String.join("\n  ", wrong));
        // a floor: most bodies plan (measured 61 of 66, 2026-11-23), so the check reads plans, never passes on none
        assertTrue(read >= 50, "plans read: " + read);
    }

    /** Two sides of several set implementations (a metamodel's documents gathered into one JSON array), in order, under
     *  every judge on every in-process database (the audit of 3f2fe452: the side's order was numbered inside the
     *  documents' aggregate, which neither DuckDB nor H2 runs). Its model holds no test data: a metamodel query reads
     *  the system database alone. */
    @Test
    void aMetamodelPairOfSeveralSetsIsOneOutcome() throws Exception {
        String model = """
                ###Pure
                Class ext::A { id: Integer[1]; aName: String[1]; }
                Class ext::B extends ext::A { bName: String[1]; }
                ###Relational
                Database p::DB ( Table ABC (id INTEGER PRIMARY KEY, aName VARCHAR(20), bName VARCHAR(20)) )
                ###Mapping
                Mapping p::M ( ext::A[a] : Relational { id : [p::DB]ABC.id, aName : [p::DB]ABC.aName } )
                Mapping p::M2 ( include p::M
                    ext::B[b2] extends [a] : Relational { bName : [p::DB]ABC.bName } )
                ###Connection
                RelationalDatabaseConnection p::C { store: p::DB; type: %s; specification: LocalH2 { }; auth: DefaultH2; }
                ###Runtime
                Runtime p::RT { mappings: [p::M]; connections: [ p::DB: [ c0: p::C ] ]; }
                """;
        String m = "p::M->meta::pure::mapping::rootClassMappingByClass";
        String m2 = "p::M2->meta::pure::mapping::rootClassMappingByClass";
        java.util.Map<String, String> want = new java.util.LinkedHashMap<>();
        want.put("{|assertEquals(" + m + "(ext::A)->concatenate(" + m + "(ext::B)), " + m + "(ext::A)->concatenate(" + m
                + "(ext::B)))}", "pass");
        want.put("{|assertEquals(" + m + "(ext::A)->concatenate(" + m + "(ext::B)), " + m2 + "(ext::A)->concatenate("
                + m2 + "(ext::B)))}", "AssertFailed");
        List<String> wrong = new ArrayList<>();
        for (var backend : TestBackends.inProcess()) {
            var database = backend.type();
            for (var e : want.entrySet()) {
                for (com.legend.testplan.PlanningOptions.JudgeMode mode : JUDGES) {
                    String got;
                    try {
                        Program.runFresh(model.formatted(database), e.getKey(), "p::RT", database,
                                ExecuteOptions.NONE.withJudgeMode(mode), com.legend.testexec.NoEdgeRun.NONE);
                        got = "pass";
                    } catch (AssertFailed f) {
                        got = "AssertFailed: " + f.getMessage();
                    } catch (Throwable t) {
                        got = t.getClass().getSimpleName() + ": " + t.getMessage();
                    }
                    if (!got.startsWith(e.getValue())) {
                        wrong.add(database + " " + mode + " " + e.getKey() + " -> " + oneLine(got));
                    }
                }
            }
        }
        assertTrue(wrong.isEmpty(), "a metamodel pair:\n  " + String.join("\n  ", wrong));
    }

    /** A Decimal literal's typed member on every database the platform runs in-process (item 3's S4, 2026-11-22): its
     *  written scale rides its SQL type and the reader restores it where the driver drops it (SQLite holds a REAL), so
     *  the record writes {@code 1.50} on every database; two Decimals compare by value (register S139). */
    @Test
    void aDecimalLiteralIsOneOutcomeOnEveryDatabase() throws Exception {
        java.util.Map<String, String> want = new java.util.LinkedHashMap<>();
        want.put("{|assertEquals([1.50D, 2.0D], [1.50D, 2.0D])}", "pass");
        want.put("{|assertEquals(1.50D, 1.5D)}", "pass");
        want.put("{|assertEquals([1.50D], [2.50D])}", "AssertFailed: expected: 1.50, actual: 2.50");
        want.put("{|assertEquals([1.50D, 2.25D], [1.50D])}", "AssertFailed: expected: [1.50, 2.25], actual: [1.50]");
        // a list of Decimals at its database's own scale (S400 as amended by the user's ruling of 2026-11-22, the core
        // follows Pure and takes the database's native answer for a cosmetic difference): DuckDB and SQLite give the
        // list one type, H2 keeps each element's (Pure's answer); the verdict is the same everywhere (S139)
        want.put("{|assertEquals([1.0D, 10.125D], [1.0D])}", "AssertFailed: expected: [1.000, 10.125], actual: [1.0]"
                + "|H2=AssertFailed: expected: [1.0, 10.125], actual: [1.0]");
        want.put("{|assertEquals([1.0D, 10.125D], [1.000D, 10.125D])}", "pass");
        List<String> wrong = new ArrayList<>();
        for (var backend : TestBackends.inProcess()) {
            var database = backend.type();
            String on = MAPPED_ON.formatted(database);
            for (var e : want.entrySet()) {
                for (com.legend.testplan.PlanningOptions.JudgeMode mode : JUDGES) {
                    String got;
                    try {
                        Program.runFresh(on, e.getKey(), "p::RT", database,
                                ExecuteOptions.NONE.withJudgeMode(mode), com.legend.testexec.NoEdgeRun.NONE);
                        got = "pass";
                    } catch (AssertFailed f) {
                        got = "AssertFailed: " + f.getMessage();
                    } catch (Throwable t) {
                        got = t.getClass().getSimpleName() + ": " + t.getMessage();
                    }
                    int own = e.getValue().indexOf("|" + database + "=");
                    String expected = own >= 0 ? e.getValue().substring(own + database.toString().length() + 2)
                            : e.getValue().split("\\|", 2)[0];
                    if (!expected.equals(got)) {
                        wrong.add(database + " " + mode + " " + e.getKey() + " -> " + oneLine(got));
                    }
                }
            }
        }
        assertTrue(wrong.isEmpty(), "one outcome under every judge:\n  " + String.join("\n  ", wrong));
    }

    /** A date-time is compared at every subsecond digit it carries, on every database the platform runs in-process
     *  and under every judge (register S399; S181 and the user's rule of 2026-11-22, "if something has nanos we either
     *  compare properly or refuse if cannot; otherwise compare with micro fidelity"): nanoseconds are compared where
     *  the database holds them, and never cut. */
    @Test
    void aDateTimeIsComparedAtEveryDigitItCarries() throws Exception {
        java.util.Map<String, String> want = new java.util.LinkedHashMap<>();
        want.put("{|assertEquals(%2020-01-01T10:00:00.123456789, %2020-01-01T10:00:00.123456)}", "AssertFailed");
        want.put("{|assertEquals(%2020-01-01T10:00:00.123456789, %2020-01-01T10:00:00.1234567890)}", "pass");
        want.put("{|assertNotEquals(%2020-01-01T10:00:00.123456789, %2020-01-01T10:00:00.123456788)}", "pass");
        want.put("{|assertEquals([%2020-01-01T10:00:00.123456789], [%2020-01-01T10:00:00.123456])}", "AssertFailed");
        want.put("{|assertEquals([%2020-01-01T10:00:00.123456789], [%2020-01-01T10:00:00.123456789])}", "pass");
        want.put("{|assertContains([%2020-01-01T10:00:00.123456789], %2020-01-01T10:00:00.123456)}", "AssertFailed");
        // (a mixed Date list's max is refused by name on H2 today: a gap of its dialect, docs/DIFFERENCES_FROM_LEGEND_ENGINE.md)
        want.put("{|assertEquals(%2020-01-01T10:00:00.123456, [%2019-01-01, %2020-01-01T10:00:00.123456789]->max())}",
                "AssertFailed|H2=DialectCapability: LIST_POSITION");
        want.put("{|assertEquals(%2020-01-01T10:00:00.123456, [%2019-01, %2020-01-01T10:00:00.123456789]->at(1))}",
                "AssertFailed");
        want.put("{|assertEquals(%2020-01-01T10:00:00.5, [%2019-01, %2020-01-01T10:00:00.500]->at(1))}", "pass");
        // an assert with no nanoseconds is read to the microsecond in every year (legend-pure's adjust tests' years)
        want.put("{|assertEquals(%2407-05-19T19:15:12, %2407-05-19T19:15:12.000)}", "pass");
        want.put("{|assertEquals(%1624-12-10T04:44:48, %1624-12-10T04:44:49)}", "AssertFailed");
        // the precision audit (2026-11-22): a written Date's kind by its structure, in every year (H1); a date-time
        // outside SQLite's 0000 to 9999 (H2); a nanosecond assert beside another assert's year past 2262 (H3); a tenth
        // digit refused at planning (M1); a String that spells a date-time no nanosecond (M2); a record as written (M4)
        want.put("{|assertEquals(%0999, %0998)}", "AssertFailed");
        want.put("{|assertEquals(%25489, %25488)}", "AssertFailed");
        want.put("{|assertEquals(%-2020, %-2021)}", "AssertFailed");
        want.put("{|assertEquals(%25489-01, %25489-02)}", "AssertFailed");
        want.put("{|assertNotEquals(%25489, %25488)}", "pass");
        want.put("{|assertEquals(%0999, %0999)}", "pass");
        want.put("{|assertEquals(%25489-01-01T10:00:00, %25488-01-01T10:00:00)}", "AssertFailed");
        want.put("{|assertEquals(%25489-01-01T10:00:00, %25489-01-01T10:00:00.000)}", "pass");
        want.put("{|assertEquals(%2020-01-01T10:00:00.123456789, %2020-01-01T10:00:00.123456789);"
                + " assertEquals(%2407-01-01T10:00:00, %2407-01-01T10:00:01);}", "AssertFailed");
        want.put("{|assertEquals(%2020-01-01T10:00:00.0000000001, %2020-01-01T10:00:00.0000000002)}",
                "DialectCapability");
        want.put("{|assertEquals(%2407-05-19T19:15:12, if('2020-01-01T10:00:00.1234567' == 'x', |%2000-01-01T00:00:00,"
                + " |%2407-05-19T19:15:12))}", "pass");
        want.put("{|assertEquals(%2020-01-01T10:30, %2020-01-01T10:30:00.000000001)}", "AssertFailed");
        List<String> wrong = new ArrayList<>();
        for (var backend : TestBackends.inProcess()) {
            var database = backend.type();
            String on = MAPPED_ON.formatted(database);
            for (var e : want.entrySet()) {
                for (com.legend.testplan.PlanningOptions.JudgeMode mode : JUDGES) {
                    String got;
                    try {
                        Program.runFresh(on, e.getKey(), "p::RT", database,
                                ExecuteOptions.NONE.withJudgeMode(mode), com.legend.testexec.NoEdgeRun.NONE);
                        got = "pass";
                    } catch (AssertFailed f) {
                        got = "AssertFailed: " + f.getMessage();
                    } catch (Throwable t) {
                        got = t.getClass().getSimpleName() + ": " + t.getMessage();
                    }
                    String expected = e.getValue();
                    int other = expected.indexOf("|" + database + "=");
                    expected = other >= 0 ? expected.substring(other + database.toString().length() + 2)
                            : expected.contains("|") ? expected.substring(0, expected.indexOf('|')) : expected;
                    if (!got.startsWith(expected)) {
                        wrong.add(database + " " + mode + " " + e.getKey() + " -> " + oneLine(got));
                    }
                }
            }
        }
        assertTrue(wrong.isEmpty(), "every digit compared:\n  " + String.join("\n  ", wrong));
    }

    /** Instances of one class compared as rows of typed leaves on every database the platform runs in-process, under
     *  every judge (item 3's S4, instances, 2026-11-22): a keyed class by its classifier and its keys (a nested keyed
     *  instance's keys in its place; a non-key property no part of it), a keyless class by its identity (S111), a Pair
     *  by its two values; in order, or as a multiset where the elements are the same in any order. */
    @Test
    void instancesAreComparedByTheirKeysOrTheirIdentity() throws Exception {
        java.util.Map<String, String> want = new java.util.LinkedHashMap<>();
        // (a model class's instance is refused by name on H2 today, under every judge: its dialect has no list encoding
        // for the instance's carrier -- a gap of the dialect, docs/DIFFERENCES_FROM_LEGEND_ENGINE.md)
        String k1 = "^p::K(id=1, at=%2020-01-01T10:00:00.5, note='a')";
        want.put("{|assertEquals(" + k1 + ", ^p::K(id=1, at=%2020-01-01T10:00:00.500, note='b'))}", "pass|H2=DialectCapability: LIST_GET");
        want.put("{|assertEquals(" + k1 + ", ^p::K(id=2, at=%2020-01-01T10:00:00.5, note='a'))}", "AssertFailed|H2=DialectCapability: LIST_GET");
        want.put("{|assertEquals(" + k1 + ", ^p::K(id=1, at=%2020-01-01T10:00:00.6, note='a'))}", "AssertFailed|H2=DialectCapability: LIST_GET");
        want.put("{|assertNotEquals(" + k1 + ", ^p::K(id=2, at=%2020-01-01T10:00:00.5, note='a'))}", "pass|H2=DialectCapability: LIST_GET");
        want.put("{|assertEquals(^p::KN(k=" + k1 + ", f=1.5), ^p::KN(k=^p::K(id=1, at=%2020-01-01T10:00:00.5,"
                + " note='z'), f=1.5))}", "pass|H2=DialectCapability: LIST_GET");
        want.put("{|assertEquals(^p::KN(k=" + k1 + ", f=1.5), ^p::KN(k=" + k1 + ", f=2.5))}", "AssertFailed|H2=DialectCapability: LIST_GET");
        want.put("{|assertSameElements([" + k1 + ", ^p::K(id=2, at=%2020-01-01T10:00:00, note='c')], [^p::K(id=2,"
                + " at=%2020-01-01T10:00:00.000, note='d'), " + k1 + "])}", "pass|H2=DialectCapability: LIST_GET");
        want.put("{|let x = ^p::P(id=1, d=1.5D); assertEquals($x, $x);}", "pass|H2=DialectCapability: LIST_GET");
        want.put("{|assertEquals(^p::P(id=1, d=1.5D), ^p::P(id=1, d=1.5D))}", "AssertFailed|H2=DialectCapability: LIST_GET");
        want.put("{|assertEquals(pair(1, 'a'), pair(1, 'a'))}", "pass");
        want.put("{|assertEquals(pair(1, 2.5), pair(1, 2.5))}", "pass");
        want.put("{|assertEquals(pair(1, true), pair(1, false))}", "AssertFailed");
        want.put("{|assertEquals(pair(1, 1.50D), pair(1, 1.5D))}", "pass");
        want.put("{|assertEquals(pair(pair(1, 'a'), 2), pair(pair(1, 'b'), 2))}", "AssertFailed");
        want.put("{|assertSameElements([pair(1, 'a'), pair(2, 'b')], [pair(2, 'b'), pair(1, 'a')])}", "pass");
        List<String> wrong = new ArrayList<>();
        for (var backend : TestBackends.inProcess()) {
            var database = backend.type();
            String on = MAPPED_ON.formatted(database);
            for (var e : want.entrySet()) {
                for (com.legend.testplan.PlanningOptions.JudgeMode mode : JUDGES) {
                    String got;
                    try {
                        Program.runFresh(on, e.getKey(), "p::RT", database,
                                ExecuteOptions.NONE.withJudgeMode(mode), com.legend.testexec.NoEdgeRun.NONE);
                        got = "pass";
                    } catch (AssertFailed f) {
                        got = "AssertFailed: " + f.getMessage();
                    } catch (Throwable t) {
                        got = t.getClass().getSimpleName() + ": " + t.getMessage();
                    }
                    String expected = e.getValue();
                    int other = expected.indexOf("|" + database + "=");
                    expected = other >= 0 ? expected.substring(other + database.toString().length() + 2)
                            : expected.contains("|") ? expected.substring(0, expected.indexOf('|')) : expected;
                    if (!got.startsWith(expected)) {
                        wrong.add(database + " " + mode + " " + e.getKey() + " -> " + oneLine(got));
                    }
                }
            }
        }
        assertTrue(wrong.isEmpty(), "instances by keys or identity:\n  " + String.join("\n  ", wrong));
    }

    /** The last families typed (item 3's S4, 2026-11-22): values typed Any compared by their JSON kind and value; nested
     *  lists and maps as their database holds them (DuckDB natively; a database carrying them as JSON refuses by name
     *  under the database judge, never compares their text); a list literal's nanoseconds read to the nanosecond; an
     *  identity assert over two Pairs no value comparison. Each body's outcome per database and judge. */
    @Test
    void theLastFamiliesAreTypedOrRefusedByName() throws Exception {
        java.util.Map<String, String> want = new java.util.LinkedHashMap<>();
        // Any: a JSON-carried side against a typed or a tagged one
        want.put("{|assertEquals([1, 'a'], [1, 'a']->cast(@Any))}", "pass");
        want.put("{|assertEquals([1, 'a'], [1, 'b']->cast(@Any))}", "AssertFailed");
        want.put("{|assertEquals([1, 'a'], [1.0, 'a']->cast(@Any))}", "AssertFailed");
        // register S401 (ruled by the user 2026-11-23, "Okay"): legend-pure compares two Variants by identity, which a
        // database cannot compute, so an equality between two whole Variants -- or a Variant and an Any the plan cannot
        // see -- is refused by name, under every judge and in the product's == and membership alike; a Variant against a
        // value of another type is not equal, as in Pure, decided by the types
        String refused = "NotImplementedException: an equality between two whole Variants is not computed";
        for (String two : List.of("{|assertEquals('1'->fromJson(), '1'->fromJson())}",
                "{|assertEquals('1'->fromJson(), '2'->fromJson())}", "{|assertEquals(1->toVariant(), 1->toVariant())}",
                "{|assertEquals('{\"a\":1,\"b\":2}'->fromJson(), '{\"b\":2,\"a\":1}'->fromJson())}",
                "{|assertNotEquals('\"1\"'->fromJson(), '1'->fromJson())}",
                "{|assertSameElements(['1'->fromJson()], ['1'->fromJson()])}",
                "{|assertContains(['1'->fromJson()], '1'->fromJson())}",
                // two Variants cast to Any are Variants (read through the casts, VariantEquality.seen)
                "{|assertEquals('{\"a\":1}'->fromJson()->cast(@Any), '{\"a\":1}'->fromJson())}",
                // a Variant among a collection's other values (the audit of 581a93cb and 386db8fe, 2026-11-23): the
                // database would compare it with the other side's values
                "{|assertEquals(['1'->fromJson(), 2], [1, 2])}", "{|assertContains(['a', '\"b\"'->fromJson()], 'b')}",
                "{|assertSameElements([1, 2], ['1'->fromJson(), 2])}",
                // the identity asserts and their negations name the refusal, never a generic one (the same audit)
                "{|assertNotEq('1'->fromJson(), '2'->fromJson())}", "{|assertIs('1'->fromJson(), '1'->fromJson())}",
                "{|assertIsNot('1'->fromJson(), '2'->fromJson())}",
                // the product's equality and membership
                "{|assert('1'->fromJson() == '1'->fromJson())}", "{|assert('1'->fromJson() != '2'->fromJson())}",
                "{|assert(['1'->fromJson()]->contains('1'->fromJson()))}",
                "{|assert('1'->fromJson()->in(['1'->fromJson()]))}",
                "{|assert(['1'->fromJson(), 2]->contains(1))}", "{|assert(['1'->fromJson(), 2] == [1, 2])}",
                "{|assert('1'->fromJson()->cast(@Any) == '1'->fromJson()->cast(@Any))}",
                // the product's deduplications, union desugared to distinct (the same audit)
                "{|assert(['1'->fromJson(), 2, '1'->fromJson()]->removeDuplicates()->size() == 3)}",
                "{|assert(['1'->fromJson(), '1'->fromJson()]->distinct()->size() == 2)}",
                "{|assert(['1'->fromJson(), '2'->fromJson()]->isDistinct())}",
                "{|assert(['1'->fromJson()]->union(['1'->fromJson()])->size() == 2)}",
                "{|assert([1, 2]->removeDuplicatesBy(x | '1'->fromJson())->size() == 2)}",
                // a Variant reached through any expression typed Any, and one held in a Pair or a List (the final audit
                // of c758fe95: only casts, literals and concatenations had been read)
                "{|assert(if(true, |'1'->fromJson(), |2) == 1)}", "{|assert(['1'->fromJson(), 2]->filter(x | true)->contains(1))}",
                "{|assert([2]->add('1'->fromJson())->contains(1))}", "{|assertContains(['1'->fromJson(), 2]->map(x | $x), 1)}",
                "{|assert(pair(1, '1'->fromJson()) == pair(1, 1))}", "{|assertEquals(list(['1'->fromJson()]), list([1]))}",
                "{|assertEquals(1, ['1'->fromJson(), 2]->filter(x | true)->at(0))}",
                // an if's Boolean branch is a value, never a condition (the narrow review of f9e74256: it had been
                // skipped, the side read as a Variant alone and called unequal to true)
                "{|assertEquals(true, if(true, |true, |'1'->fromJson()))}",
                "{|assert(if(true, |true, |'1'->fromJson()) == true)}",
                // a Variant held in a member declared Any of a Pair the plan builds (the same review)
                "{|assert(pair(1, '1'->fromJson()->cast(@Any)) == pair(1, 1->cast(@Any)))}")) {
            want.put(two, refused);
        }
        // a pair and a list are compared by the types inside them (2026-11-27): a Variant against an Integer inside is
        // the compile error it is outside
        want.put("{|assert(pair(1, '1'->fromJson()) == pair(1, 1))}", "ModelException: == compares meta::pure::metamodel::variant::Variant with Integer");
        want.put("{|assertEquals(list(['1'->fromJson()]), list([1]))}",
                "ModelException: assertEquals compares meta::pure::metamodel::variant::Variant with Integer");
        // an object literal holding a Variant in a property its equality never reads: a keyed class by its keys (the
        // review of a39883f3: every property had been read, and the comparison refused); H2 has no list encoding for
        // the keyed instances' rows, as before the change
        want.put("{|assertEquals(^p::KVN(id=1, v='1'->fromJson()), ^p::KVN(id=1, v='2'->fromJson()))}",
                "pass|H2=DialectCapability");
        want.put("{|assert(^p::KVN(id=1, v='1'->fromJson()) != ^p::KVN(id=2, v='1'->fromJson()))}",
                "pass|H2=DialectCapability");
        // an index, a count and a map's parameter are not values of the side (the same review: they had been counted,
        // and the equality refused)
        want.put("{|assert(!(['1'->fromJson()]->cast(@Any)->at(0) == 'a'))}", "pass");
        // (H2, 2026-11-28, step 6B's slice 5: the type the list holds is now read through `take`, so this is the plain
        // `...->cast(@Any)->contains('a')` -- never a member, the list still evaluated (the user's ruling of 2026-11-27,
        // "Build 3"), which H2 has no SQL for and refuses by name, as it always did for the plain form. Until then
        // the take hid the list from that reading and a constant false answered with the list unread. Reported to
        // the user as the slice's one passing-to-refusing: for-the-user-2026-11-28.md, section E)
        want.put("{|assert(!['1'->fromJson(), '2'->fromJson()]->cast(@Any)->take(1)->contains('a'))}",
                "pass|H2=DialectCapability");
        want.put("{|assert(!['1'->fromJson(), '2'->fromJson()]->cast(@Any)->map(x | $x)->contains('a'))}", "pass");
        // a Variant against an empty side: the two the same only where both are empty, its values written as JSON in
        // the record, a NULL among them JSON null (the audit of 8477c703); H2's JSON array aggregate drops a JSON null,
        // so its database judge's record lists none there -- recorded, the verdict the same
        want.put("{|assertEquals([], 'null'->fromJson())}", "AssertFailed: expected: [], actual: [null]"
                + "|H2/DATABASE=AssertFailed|H2/BOTH=JudgesDisagree");
        want.put("{|assertEquals([], '1'->fromJson())}", "AssertFailed: expected: [], actual: [1]");
        // a Variant is never a value of another type, its text's included (the audit of c2217801; S401)
        // (both judges fail it, and write the same record: expected "abc" (Variant), actual "abc" (String). Until the
        // closing batch of step 6 the host judge read the Variant through Any decoded, wrote abc, and a both-judges
        // run stopped on the two records' difference -- found 2026-11-27 by this line)
        want.put("{|assertEquals('\"abc\"'->fromJson()->cast(@Any), 'abc')}", "AssertFailed");
        want.put("{|assertEquals('1'->fromJson()->cast(@Any), 1)}", "AssertFailed");
        // written as two types, the assert is the compile error a comparison of them is (the user's ruling of
        // 2026-11-26, "yes compile error", built 2026-11-27; until then judged, as above)
        want.put("{|assertEquals('\"abc\"'->fromJson(), 'abc')}", "ModelException: assertEquals compares meta::pure::metamodel::variant::Variant with String");
        want.put("{|assertEquals('1'->fromJson(), 1)}", "ModelException: assertEquals compares meta::pure::metamodel::variant::Variant with Integer");
        // a Variant and an Integer or a String are two types: the comparison is a compile error (B3 step 5, the one
        // equality; the user's ruling of 2026-11-24 -- until then false by the types, S401)
        want.put("{|assert('1'->fromJson() == 1)}", "ModelException: == compares meta::pure::metamodel::variant::Variant with Integer");
        want.put("{|assertFalse('\"a\"'->fromJson() == 'a')}", "ModelException: == compares meta::pure::metamodel::variant::Variant with String");
        // decided by the types under every judge, read through casts, the record writing a Variant as JSON with its kind
        // (the audit of 581a93cb and 386db8fe: the database judge had answered UNJUDGED, the host "got \"1\" (String)")
        String variantOne = "AssertFailed: expected: 1 (Integer), actual: 1 (Variant)";
        want.put("{|assertEquals(1, '1'->fromJson())}", "ModelException: assertEquals compares Integer with meta::pure::metamodel::variant::Variant");
        want.put("{|assertEquals(1, '1'->fromJson()->cast(@Any))}", variantOne);
        want.put("{|assertEquals(1->cast(@Any), '1'->fromJson())}", variantOne);
        want.put("{|assertEq(1, '1'->fromJson())}", "ModelException: assertEq compares Integer with meta::pure::metamodel::variant::Variant");
        // (until 2026-11-27 the host judge's identity held a Variant read through Any against the Integer of its
        // text, where the database judge failed it: found by this line, fixed the same day -- assertEq reads a Variant
        // side as assertEquals does)
        want.put("{|assertEq(1, '1'->fromJson()->cast(@Any))}", variantOne);
        // written as two types: the compile error (2026-11-27; until then held, a Variant never a value of another)
        want.put("{|assertNotEquals(1, '1'->fromJson())}", "ModelException: assertNotEquals compares Integer with meta::pure::metamodel::variant::Variant");
        want.put("{|assertNotEquals(true, 'true'->fromJson())}",
                "ModelException: assertNotEquals compares Boolean with meta::pure::metamodel::variant::Variant");
        want.put("{|assertNotEquals(1, '1'->fromJson()->cast(@Any))}", "pass");
        want.put("{|assert(!'1'->fromJson()->in([1, 2]))}",
                "ModelException: in compares meta::pure::metamodel::variant::Variant with Integer");
        want.put("{|assert(![1, 2]->contains('1'->fromJson()))}",
                "ModelException: contains compares Integer with meta::pure::metamodel::variant::Variant");
        want.put("{|assert([1, 2]->indexOf('1'->fromJson()) == -1)}",
                "ModelException: indexOf compares Integer with meta::pure::metamodel::variant::Variant");
        want.put("{|assertFalse('1'->fromJson()->cast(@Any) == 1)}", "pass");
        // a Variant against an Integer list is two types: a compile error even where both may be empty (B3 step 5, the
        // one equality; until then two empty sides were equal whatever their types, Pure's [] == [], the same audit's M1)
        want.put("{|assert('{}'->fromJson()->get('a') == []->cast(@Integer))}",
                "ModelException: == compares meta::pure::metamodel::variant::Variant with Integer");
        want.put("{|assertFalse('{\"a\":1}'->fromJson()->get('a') == []->cast(@Integer))}",
                "ModelException: == compares meta::pure::metamodel::variant::Variant with Integer");
        // two primitives of different kinds: one record under every judge (the database judge's typed pair, never a
        // planned sentence); an enumeration is never its name's String (register S169, the user's "false, matching")
        want.put("{|assertEquals('1'->cast(@Any), 1)}", "AssertFailed: expected: \"1\", actual: 1");
        want.put("{|assertEquals(true->cast(@Any), 1)}", "AssertFailed: expected: true, actual: 1");
        want.put("{|assertNotEquals('1'->cast(@Any), 1)}", "pass");
        want.put("{|assertEquals(p::E.A->cast(@Any), 'A')}", "AssertFailed: expected: A, actual: \"A\"");
        want.put("{|assertNotEquals(p::E.A->cast(@Any), 'A')}", "pass");
        want.put("{|assertEq(p::E.A->cast(@Any), 'A')}", "AssertFailed: expected: A, actual: \"A\"");
        // written as two types: the compile error (2026-11-27)
        want.put("{|assertEquals('1', 1)}", "ModelException: assertEquals compares String with Integer");
        want.put("{|assertEquals(true, 1)}", "ModelException: assertEquals compares Boolean with Integer");
        want.put("{|assertNotEquals('1', 1)}", "ModelException: assertNotEquals compares String with Integer");
        want.put("{|assertEquals(p::E.A, 'A')}", "ModelException: assertEquals compares p::E with String");
        want.put("{|assertNotEquals(p::E.A, 'A')}", "ModelException: assertNotEquals compares p::E with String");
        want.put("{|assertEq(p::E.A, 'A')}", "ModelException: assertEq compares p::E with String");
        want.put("{|assertEquals([p::E.A, p::E.B], [p::E.A, p::E.B])}", "pass");
        // a multiset of enumerations sorts them (the host's sort had no order for them: legend-engine's corpus on H2)
        want.put("{|assertSameElements([p::E.A, p::E.B], [p::E.B, p::E.A])}", "pass");
        // values holding both days and date-times: their days cannot be told apart in the database (S399); read
        // through a map too (the narrow review of f9e74256); a function's result keeps its kind (at's day is a day)
        String mixedDays = "NotImplementedException: values holding both days and date-times are not compared";
        want.put("{|assertContains([%2020-01-01T00:00, %2020-01-02]->map(x | $x), %2020-01-02T00:00)}", mixedDays);
        want.put("{|assertEquals(%2020-01-02T00:00, [%2020-01-02, %2020-01-01T00:00]->at(0))}",
                "AssertFailed: expected: 2020-01-02T00:00:00.000000000Z, actual: 2020-01-02");
        want.put("{|assertContains([%2020-01-01T00:00, %2020-01-02], %2020-01-02T00:00)}", mixedDays);
        want.put("{|assertEquals([%2020-01-01T00:00, %2020-01-02], [%2020-01-01T00:00, %2020-01-02T00:00])}", mixedDays);
        // a Variant's membership among values of other types: the host's answer; the database judge has no typed form
        // for it, refused by name (recorded, register S401)
        String noTypedForm = "AssertFailed: assertNotContains: UNJUDGED in database mode";
        want.put("{|assertNotContains([1, 2], '1'->fromJson()->cast(@Any))}", "pass|DuckDB/DATABASE=" + noTypedForm + "|DuckDB/BOTH="
                + noTypedForm + "|H2/DATABASE=" + noTypedForm + "|H2/BOTH=" + noTypedForm + "|SQLite/DATABASE="
                + noTypedForm + "|SQLite/BOTH=" + noTypedForm);
        want.put("{|assertNotContains([1, 2], '1'->fromJson())}",
                "ModelException: assertNotContains compares Integer with meta::pure::metamodel::variant::Variant");
        want.put("{|assertEquals(1.0, (0.5 + 0.5)->cast(@Any))}", "pass");
        want.put("{|assertEquals(true, 'true'->fromJson()->to(@Boolean))}", "pass");
        // a graph's size and emptiness (its root objects): typed, its failures the host's records
        want.put("{|assertSize(execute(|p::P.all(), p::M, p::RT.runtimeValue, []).values, 1)}", "pass");
        want.put("{|assertSize(execute(|p::P.all(), p::M, p::RT.runtimeValue, []).values, 2)}", "AssertFailed");
        want.put("{|assertNotEmpty(execute(|p::P.all(), p::M, p::RT.runtimeValue, []).values)}", "pass");
        want.put("{|assertEmpty(execute(|p::P.all()->filter(x | $x.id > 5), p::M, p::RT.runtimeValue, []).values)}",
                "pass");
        want.put("{|assertEmpty(execute(|p::P.all(), p::M, p::RT.runtimeValue, []).values)}", "AssertFailed");
        // eq over a Map is identity, never its values (the audit's M6): refused by name, as the host refuses it
        want.put("{|assertEq(newMap([pair('a', 1)]), newMap([pair('a', 1)]))}",
                "NotImplementedException|H2/HOST=DataError");
        // nested lists and maps
        want.put("{|assertEquals(list([1, 2]), list([1, 2]))}", "pass|H2/DATABASE=DialectCapability|H2/BOTH=DialectCapability|H2=DataError|SQLite/DATABASE=DialectCapability|SQLite/BOTH=DialectCapability");
        want.put("{|assertEquals(list([1, 2]), list([2, 1]))}",
                "AssertFailed|H2/DATABASE=DialectCapability|H2/BOTH=DialectCapability|H2=DataError|SQLite/DATABASE=DialectCapability|SQLite/BOTH=DialectCapability");
        want.put("{|assertEquals(newMap([pair('a', 1), pair('b', 2)]), newMap([pair('b', 2), pair('a', 1)]))}",
                "pass|H2/DATABASE=DialectCapability|H2/BOTH=DialectCapability|H2=DataError|SQLite/DATABASE=DialectCapability|SQLite/BOTH=DialectCapability");
        want.put("{|assertEquals(newMap([pair('a', 1)]), newMap([pair('a', 2)]))}",
                "AssertFailed|H2/DATABASE=DialectCapability|H2/BOTH=DialectCapability|H2=DataError|SQLite/DATABASE=DialectCapability|SQLite/BOTH=DialectCapability");
        // a list literal's nanoseconds (the instances audit's H-A) and a tenth digit refused when planned
        want.put("{|assertEquals([%2020-01-01T10:00:00.123456789, %2020-01-01T10:00:00], [%2020-01-01T10:00:00.123456789,"
                + " %2020-01-01T10:00:00.000])}", "pass|H2=DialectCapability|SQLite=DialectCapability");
        want.put("{|assertContains([%2020-01-01T10:00:00.123456789], %2020-01-01T10:00:00.123456789)}", "pass");
        want.put("{|assertEquals([%2020-01-01T10:00:00.0000000001, %2020-01-01T10:00:00],"
                + " [%2020-01-01T10:00:00.0000000002, %2020-01-01T10:00:00])}", "DialectCapability");
        // a nanosecond adjust is a nanosecond assert (the instances audit's M-a)
        want.put("{|assertEquals(%2020-01-01T10:00:00.123456, %2020-01-01T10:00:00.123456->adjust(789,"
                + " DurationUnit.NANOSECONDS))}", "AssertFailed|H2=DialectCapability|SQLite=DialectCapability");
        // the audit of 946d16f2: bodies that passed at its base, typed now
        want.put("{|assertContains([1, 2.5], 2.5)}", "pass");
        want.put("{|assertContains([1, 2.5], 2)}", "AssertFailed");
        // (SQLite holds no exact decimal: its Decimal arithmetic refused, never approximated)
        want.put("{|assertEqWithinTolerance(1.0D, 1.05D, 0.1D)}",
                "pass|SQLite/DATABASE=DialectCapability|SQLite/BOTH=DialectCapability");
        want.put("{|assertNotEquals(1->cast(@Any), 'a'->cast(@Any))}", "pass");
        want.put("{|assertNotEquals([1]->cast(@Any), [1.0]->cast(@Any))}", "pass");
        want.put("{|assertNotEquals(p::N, p::K)}", "pass");
        want.put("{|assertEmpty([1, 'a']->filter(x | false))}", "pass");
        // a wider table against a list: its cells row-major, each of its column's kind (the product's flatten)
        String grid = "execute(|p::Q.all()->project([x | $x.id, x | $x.id->toString()], ['id', 's'])->sort('id'), p::M,"
                + " p::RT.runtimeValue, []).values";
        want.put("{|assertEquals([1, '1', 2, '2'], " + grid + ")}", "pass");
        want.put("{|assertEquals([1, '1', 2, '3'], " + grid + ")}", "AssertFailed");
        // a table's values counted (the audit of the next batch, 2026-11-26, S1): a table is a relation, which
        // upstream counts as ONE value, so lite refuses its size assert by name under every judge (register S137) --
        // assertNotSize too, since the same day: it had missed the rule, the host judge refused it as a table, the
        // database judge counted its rows, and under both the judges disagreed.
        String relation = "ModelException: assertSize over a relation counts the relation as ONE value";
        want.put("{|assertSize(" + grid + ", 4)}", relation);
        want.put("{|assertSize(" + grid + ", 2)}", relation);
        want.put("{|assertNotSize(" + grid + ", 2)}", relation.replace("assertSize", "assertNotSize"));
        // the whole table's cells, <table>.rows.values (two rows of two columns: four values in Pure; a written
        // ->size() of it is right, the typer's rewrite to rows times columns). A RECORDED DEFECT, older than the size
        // slice (measured the same at its base, 2026-11-26): the typer keeps the bare read as the table's ROWS, so every
        // judge on every database counts 2. One count everywhere; owned with the row's cells below
        want.put("{|assertSize(" + grid + ".rows.values, 4)}", "AssertFailed: expected size: 4, actual size: 2");
        // ONE ROW's cells (an Integer and a String here: two values in Pure). A RECORDED DEFECT, measured 2026-11-26
        // and owned by the equality program's later slices (the plan record's carried-forward table): the typer keeps
        // a row's bare .values as the row itself, one value, so the host judge counts 1; the database judge asks the
        // product's size of it, whose value is the row's cells as a mixed list under a to-one stamp, and the lowering
        // refuses that by name. A MOVE of the first slice, measured against its base: until it the database judge
        // counted the side's rows itself and AGREED with the host on 1, the wrong count; it now refuses where it
        // would have agreed on a wrong answer. Never a count of 2 from one judge and 1 from the other
        String cells = grid.substring(0, grid.length() - ".values".length()) + ".values.rows->at(0).values";
        String unlowered = "DialectCapability: a tagged list reached the renderer";
        String everyDatabase = "|DuckDB/DATABASE=" + unlowered + "|DuckDB/BOTH=" + unlowered + "|H2/DATABASE=" + unlowered
                + "|H2/BOTH=" + unlowered + "|SQLite/DATABASE=" + unlowered + "|SQLite/BOTH=" + unlowered;
        want.put("{|assertSize(" + cells + ", 2)}", "AssertFailed: expected size: 2, actual size: 1" + everyDatabase);
        want.put("{|assertSize(" + cells + ", 3)}", "AssertFailed" + everyDatabase);
        want.put("{|assertNotSize(" + cells + ", 3)}", "pass" + everyDatabase);
        // the audit of 3f2fe452: a wider table's cells in the table's own order (a descending sort read descending); a
        // class reference against itself cast to Class<Any> one reference; a store-read Decimal carried as its text
        // against a Decimal literal refused by name, never its text joined against a number
        String desc = "execute(|p::Q.all()->project([x | $x.id, x | $x.id->toString()], ['id', 's'])->sort(descending('id')),"
                + " p::M, p::RT.runtimeValue, []).values";
        want.put("{|assertEquals([2, '2', 1, '1'], " + desc + ")}", "pass");
        want.put("{|assertEquals([1, '1', 2, '2'], " + desc + ")}", "AssertFailed");
        want.put("{|assertEquals(p::Q, p::Q->cast(@Class<Any>))}", "pass");
        want.put("{|assertNotEquals(p::Q, p::Q->cast(@Class<Any>))}", "AssertFailed");
        String unjudged = "AssertFailed: assertContains: UNJUDGED";
        want.put("{|assertContains(p::Q.all().e, 1.5D)}", "pass|DuckDB/DATABASE=" + unjudged + "|DuckDB/BOTH=" + unjudged
                + "|H2/DATABASE=" + unjudged + "|H2/BOTH=" + unjudged + "|SQLite/DATABASE=" + unjudged + "|SQLite/BOTH="
                + unjudged);
        // membership is upstream's equal: a keyed class by its keys, any other instance by its identity (H2: a model
        // class's instances refused by name by the database judge; a keyless class's identity under every judge)
        String k = "^p::K(id=1, at=%2020-01-01T10:00:00.5, note='a')";
        want.put("{|assertContains([" + k + "], " + k.replace("'a'", "'b'") + ")}", "pass|H2/DATABASE=DialectCapability: LIST_GET|H2/BOTH=DialectCapability: LIST_GET");
        want.put("{|assertContains([" + k + "], " + k.replace("id=1", "id=2") + ")}", "AssertFailed|H2/DATABASE=DialectCapability: LIST_GET|H2/BOTH=DialectCapability: LIST_GET");
        want.put("{|assertContains([^p::N(name='x')], ^p::N(name='x'))}", "AssertFailed|H2=DialectCapability: LIST_GET");
        want.put("{|let n = ^p::N(name='x'); assertContains([$n], $n);}", "pass|H2=DialectCapability: LIST_GET");
        // an instance whose data names no class is of its static class
        want.put("{|assertInstanceOf(^p::N(name='x'), p::N)}",
                "pass|H2/DATABASE=DialectCapability: LIST_GET|H2/BOTH=DialectCapability: LIST_GET");
        want.put("{|assertInstanceOf(^p::N(name='x'), p::K)}",
                "AssertFailed|H2/DATABASE=DialectCapability: LIST_GET|H2/BOTH=DialectCapability: LIST_GET");
        List<String> wrong = new ArrayList<>();
        List<String> seen = new ArrayList<>();
        for (var backend : TestBackends.inProcess()) {
            var database = backend.type();
            String on = MAPPED_ON.formatted(database);
            for (var e : want.entrySet()) {
                for (com.legend.testplan.PlanningOptions.JudgeMode mode : JUDGES) {
                    String got;
                    try {
                        Program.runFresh(on, e.getKey(), "p::RT", database,
                                ExecuteOptions.NONE.withJudgeMode(mode), com.legend.testexec.NoEdgeRun.NONE);
                        got = "pass";
                    } catch (AssertFailed f) {
                        got = "AssertFailed: " + f.getMessage();
                    } catch (Throwable t) {
                        got = t.getClass().getSimpleName() + ": " + t.getMessage();
                    }
                    seen.add(database + " " + mode + " " + e.getKey() + " -> " + oneLine(got));
                    String expected = e.getValue();
                    int byMode = expected.indexOf("|" + database + "/" + mode + "=");
                    if (byMode >= 0) {
                        expected = expected.substring(byMode + database.toString().length() + mode.toString().length()
                                + 3);
                    }
                    int other = byMode >= 0 ? -1 : expected.indexOf("|" + database + "=");
                    expected = other >= 0 ? expected.substring(other + database.toString().length() + 2)
                            : expected.contains("|") ? expected.substring(0, expected.indexOf('|')) : expected;
                    if (expected.contains("|")) {
                        expected = expected.substring(0, expected.indexOf('|'));
                    }
                    if (!got.startsWith(expected)) {
                        wrong.add(database + " " + mode + " " + e.getKey() + " -> " + oneLine(got));
                    }
                }
            }
        }
        assertTrue(wrong.isEmpty(), "the last families:\n  " + String.join("\n  ", wrong));
    }

    /** Two tables compared (assertTdsEquivalent, assertEquals, assertNotEquals) on every database the platform runs
     *  in-process (item 3's S4, tables, 2026-11-22): the database's verdict over the typed cells, each grid's rows riding
     *  a failure as its database carries a list of rows (DuckDB's LIST of STRUCT; H2's and SQLite's JSON) for the host's
     *  record -- one outcome under every judge. */
    @Test
    void aTableComparisonIsOneOutcomeOnEveryDatabase() throws Exception {
        java.util.Map<String, String> want = new java.util.LinkedHashMap<>();
        want.put("{|assertTdsEquivalent(#TDS\n  id, v\n  1, 2.00\n  2, 3.001\n#, #TDS\n  id, v\n  1, 2.0\n  2, 3.0\n#,"
                + " 0.01)}", "pass");
        want.put("{|assertTdsEquivalent(#TDS\n  id, v, s\n  1, 2.5, x\n#, #TDS\n  id, v, s\n  1, 2.0, x\n#, 0.01)}",
                "AssertFailed: expected the rows [[1, 2.5, \"x\"]] within 0.01, got [[1, 2.0, \"x\"]]");
        want.put("{|assertTdsEquivalent(#TDS\n  id\n  1\n  2\n#, #TDS\n  id\n  1\n#, 0.01)}",
                "AssertFailed: expected the rows [[1], [2]] within 0.01, got [[1]]");
        want.put("{|assertTdsEquivalent(#TDS\n  id\n  1\n#, #TDS\n  ix\n  1\n#, 0.01)}",
                "AssertFailed: expected the rows [[1]] within 0.01, got [[1]]");
        // two grids compared by assertEquals: in order, each by its own columns (B3 step 4's review, M4, 2026-11-24: a
        // side with no defined order is refused before); by assertSameElements, as a multiset of rows (M5)
        want.put("{|assertEquals(#TDS\n  id, s\n  1, x\n  2, y\n#, #TDS\n  id, s\n  2, y\n  1, x\n#)}",
                "AssertFailed: expected: [[1, \"x\"], [2, \"y\"]], actual: [[2, \"y\"], [1, \"x\"]]");
        want.put("{|assertSameElements(#TDS\n  id, s\n  1, x\n  2, y\n#, #TDS\n  id, s\n  2, y\n  1, x\n#)}", "pass");
        want.put("{|assertEquals(#TDS\n  id, s\n  1, x\n#, #TDS\n  id, s\n  1, z\n#)}",
                "AssertFailed: expected: [[1, \"x\"]], actual: [[1, \"z\"]]");
        want.put("{|assertEquals(#TDS\n  id\n  1\n  2\n#, #TDS\n  id\n  1\n  2\n#->sort(~id->descending()))}",
                "AssertFailed: expected: [[1], [2]], actual: [[2], [1]]");
        want.put("{|assertEquals(#TDS\n  id\n  1\n#, #TDS\n  ix\n  1\n#)}", "AssertFailed: expected: [[1]], actual: [[1]]");
        want.put("{|assertEquals(#TDS\n  id\n  1\n#, #TDS\n  id\n  1.0\n#)}",
                "AssertFailed: expected: [[1]], actual: [[1.0]]");
        want.put("{|assertNotEquals(#TDS\n  id\n  1\n#, #TDS\n  id\n  2\n#)}", "pass");
        // a Float cell written as its carrier holds it, the host's rows' carrier (the table slice's audit, M1)
        want.put("{|assertEquals(#TDS\n  v\n  2.50\n#, #TDS\n  v\n  2.6\n#)}", "AssertFailed: expected: [[2.5]], actual: [[2.6]]");
        List<String> wrong = new ArrayList<>();
        for (var backend : TestBackends.inProcess()) {
            var database = backend.type();
            String on = MAPPED_ON.formatted(database);
            for (var e : want.entrySet()) {
                for (com.legend.testplan.PlanningOptions.JudgeMode mode : JUDGES) {
                    String got;
                    try {
                        Program.runFresh(on, e.getKey(), "p::RT", database,
                                ExecuteOptions.NONE.withJudgeMode(mode), com.legend.testexec.NoEdgeRun.NONE);
                        got = "pass";
                    } catch (AssertFailed f) {
                        got = "AssertFailed: " + f.getMessage();
                    } catch (Throwable t) {
                        got = t.getClass().getSimpleName() + ": " + t.getMessage();
                    }
                    if (!e.getValue().equals(got)) {
                        wrong.add(database + " " + mode + " " + e.getKey() + " -> " + oneLine(got));
                    }
                }
            }
        }
        assertTrue(wrong.isEmpty(), "one outcome under every judge:\n  " + String.join("\n  ", wrong));
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
                "AssertFailed: expected: 2015-04-16T14:00:00.000000000Z, actual: 2015-04-16T15:00:00.000000000Z");
        want.put("{|assertEquals(%2015-04-16T14:30, date(2015, 4, 16, 14, 31))}",
                "AssertFailed: expected: 2015-04-16T14:30:00.000000000Z, actual: 2015-04-16T14:31:00.000000000Z");
        // register S399 (the user's ruling of 2026-11-22, superseding in the product the orchestrator's ruling 3 of
        // 2026-11-19): lite's product compares a date-time by its instant, so an hour equals its minute at one instant,
        // under every judge (legend-pure: not equal, DateWithHour.getMinute() is -1 where a minute's is 0; lite's rule
        // on every route since B3's one rulebook, 2026-11-24)
        want.put("{|assertEquals(%2015-04-16T14, %2015-04-16T14:00)}", "pass");
        want.put("{|assertNotEquals(%2015-04-16T14, %2015-04-16T14:00)}",
                "AssertFailed: 2015-04-16T14:00:00.000000000Z should not equal 2015-04-16T14:00:00.000000000Z");
        want.put("{|assertNotEquals(%2015-04-16T14, date(2015, 4, 16, 14, 0))}",
                "AssertFailed: 2015-04-16T14:00:00.000000000Z should not equal 2015-04-16T14:00:00.000000000Z");
        // no text carrier holds a zone for the message's read to its minute to trip on (PR #127's review, 2026-11-19):
        // a zoned minute is normalised to GMT at parse (PureDateLiteral, as legend-pure's DateFormat.parsePureDate),
        // and a zone on an hour is refused at parse
        want.put("{|assertEquals(%2015-04-16T14:30+0100, date(2015, 4, 16, 13, 30))}", "pass");
        want.put("{|assertEquals(%2015-04-16T14:30+0100, date(2015, 4, 16, 14, 30))}",
                "AssertFailed: expected: 2015-04-16T13:30:00.000000000Z, actual: 2015-04-16T14:30:00.000000000Z");
        want.put("{|assertEquals(%2015-04-16T14+0100, date(2015, 4, 16, 13))}", "ParseException: [1:16] invalid date"
                + " literal '%2015-04-16T14+0100': expected ':' after hour at position 13 in '2015-04-16T14+0100'");
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

    /** Two asserts whose sides the database cannot hold (PCT's testAdjustBy*BigNumber, the compatibility program's A4
     *  gate, 2026-11-19): one outcome under every judge, and the same on every run. Every value is the database's (the
     *  user, 2026-11-19: "everything must be pushed to database"): the expected literal {@code %33803336-12-17} is past
     *  DuckDB's DATE (to year 5881580), so the database refuses it under both judges, where legend-pure evaluates the
     *  literal in its own process (a register row, for the user's word). Until then the database judge's fused
     *  statement raised either assert's error, or either side's, by how DuckDB scheduled its threads (G10), and its
     *  rejection failed the run with no assert reported, where the host reported its first assert's failure: under
     *  lite's rules the judges disagreed. Now a test plan's DuckDB session runs single-threaded (its target's test
     *  session statement, {@code SqlDialect.testSessionSetup}), each verdict row reads its expected side before its
     *  actual (the side DuckDB builds first is a cross join's right), as the host's side batch reads them, and a
     *  rejected fused statement is its first assert's failure, the run ending with the database's own text. On the
     *  platform's database, the PCT lanes' (H2 writes its whole statement into its error's text, so two judges' texts of
     *  a rejected statement never match there: a separate finding, 2026-11-19). */
    @Test
    void sidesTheDatabaseCannotHoldAreOneOutcomeUnderEveryJudge() throws Exception {
        String body = "{|assertEquals(%33803336-12-17, %2016-02-29->adjust(12345678912, DurationUnit.DAYS));"
                + " assertEquals(%-33799305-05-13, %2016-02-29->adjust(-12345678912, DurationUnit.DAYS));}";
        var backend = TestBackends.platform();
        java.util.Set<String> outcomes = new java.util.LinkedHashSet<>();
        for (int run = 0; run < 10; run++) {
            for (com.legend.testplan.PlanningOptions.JudgeMode mode : JUDGES) {
                String got;
                try (Connection c = backend.open()) {
                    Program.run(StorelessRuntime.with(MODEL, backend.type()), body, StorelessRuntime.RUNTIME, c,
                            ExecuteOptions.NONE.withJudgeMode(mode));
                    got = "pass";
                } catch (Throwable x) {
                    got = x.getClass().getSimpleName() + ": " + x.getMessage();
                }
                outcomes.add(got);
            }
        }
        // one outcome, every judge, every run: the database's own refusal of the expected literal
        assertTrue(outcomes.size() == 1 && outcomes.iterator().next().startsWith("DataError: ")
                        && outcomes.iterator().next().contains("33803336-12-17"),
                "one outcome under every judge, on every run: " + outcomes.stream().map(AssertBarTest::oneLine).toList());
    }

    /** A graph's size is its ROOT objects, whatever each root holds (the audit of the next batch, 2026-11-26, S2: since
     *  the equality program's first slice the database judge asks the product's own {@code size} of a graph side, where
     *  it had counted the graph's JSON documents itself; the host judge counts the root objects it decoded). A root with
     *  three children and a root with none are two objects under every judge, on every database; the roots that have a
     *  child past the first are one, never the two child rows that match. (A graph fetch inside a test body's
     *  {@code execute} is not planned yet -- "class query under TypedGraphFetch is not resolvable" under every judge --
     *  so the children are reached through a filter over the to-many property.) */
    private static final String ROOTS_WITH_MANY = """
            ###Pure
            Class p::A { id: Integer[1]; bs: p::B[*]; }
            Class p::B { id: Integer[1]; }
            ###Relational
            Database p::DB ( Table TA (ID INTEGER PRIMARY KEY) Table TB (ID INTEGER PRIMARY KEY, AID INTEGER)
                Join AB(TA.ID = TB.AID) )
            ###Mapping
            Mapping p::M ( p::A: Relational { ~mainTable [p::DB] TA id: TA.ID, bs: [p::DB]@AB }
                p::B: Relational { ~mainTable [p::DB] TB id: TB.ID } )
            ###Connection
            RelationalDatabaseConnection p::C { store: p::DB; type: %s;
                specification: LocalH2 { testDataSetupCSV: 'default\\nTA\\nID\\n1\\n2\\n-----\\ndefault\\nTB\\nID,AID\\n1,1\\n2,1\\n3,1\\n'; };
                auth: DefaultH2; }
            ###Runtime
            Runtime p::RT { mappings: [p::M]; connections: [ p::DB: [ c0: p::C ] ]; }
            """;

    @Test
    void aGraphsSizeIsItsRootObjectsUnderEveryJudge() throws Exception {
        String roots = "execute(|p::A.all(), p::M, p::RT.runtimeValue, []).values";
        String withLaterChild = "execute(|p::A.all()->filter(a | $a.bs->exists(b | $b.id > 1)), p::M,"
                + " p::RT.runtimeValue, []).values";
        java.util.Map<String, String> want = new java.util.LinkedHashMap<>();
        want.put("{|assertSize(" + roots + ", 2)}", "pass");
        want.put("{|assertSize(" + roots + ", 3)}", "AssertFailed");
        want.put("{|assertNotSize(" + roots + ", 3)}", "pass");
        want.put("{|assertNotSize(" + roots + ", 2)}", "AssertFailed");
        want.put("{|assertSize(" + withLaterChild + ", 1)}", "pass");
        want.put("{|assertSize(" + withLaterChild + ", 2)}", "AssertFailed");
        want.put("{|assertNotSize(" + withLaterChild + ", 2)}", "pass");
        List<String> wrong = new ArrayList<>();
        for (var backend : TestBackends.inProcess()) {
            var database = backend.type();
            for (var e : want.entrySet()) {
                for (com.legend.testplan.PlanningOptions.JudgeMode mode : JUDGES) {
                    String got;
                    try {
                        Program.runFresh(ROOTS_WITH_MANY.formatted(database), e.getKey(), "p::RT", database,
                                ExecuteOptions.NONE.withJudgeMode(mode), com.legend.testexec.NoEdgeRun.NONE);
                        got = "pass";
                    } catch (AssertFailed f) {
                        got = "AssertFailed: " + f.getMessage();
                    } catch (Throwable t) {
                        got = t.getClass().getSimpleName() + ": " + t.getMessage();
                    }
                    if (!got.startsWith(e.getValue())) {
                        wrong.add(database + " " + mode + " " + e.getKey() + " -> " + oneLine(got));
                    }
                }
            }
        }
        assertTrue(wrong.isEmpty(), "a graph's size, its root objects:\n  " + String.join("\n  ", wrong));
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

    /** RULE 4, Q-D (item 3's S7, 2026-11-23): a COMPARED side the compiler would have to join is the database's to
     *  compute -- planned, never a joined constant -- and still judged the same on every judge; a caller's message (a
     *  final print) keeps its literals' join, marked {@code folded} in the plan; a joined assertError expected text and
     *  a joined JSON golden under the database judge are refused by name. */
    @Test
    void aComparedJoinIsTheDatabasesAndAMessageKeepsItsJoin() throws Exception {
        String model = StorelessRuntime.with(MODEL, Databases.PLATFORM);
        try (Connection c = TestBackends.platform().open()) {
            for (com.legend.testplan.PlanningOptions.JudgeMode mode : JUDGES) {
                assertEquals("pass", outcome(c, "{|assertEquals('ab', 'a' + 'b')}", mode), mode + ": a compared join");
                assertEquals("AssertFailed: got it", outcome(c, "{|assertEquals('ab', 'a' + 'c', 'got ' + 'it')}", mode),
                        mode + ": a caller's joined message");
                String joinedError = outcome(c, "{|assertError(|fail('ab'), 'a' + 'b')}", mode);
                assertTrue(joinedError.startsWith("NotImplementedException") && joinedError.contains("join of literals"),
                        mode + ": a joined expected text refused by name: " + joinedError);
            }
            String json = outcome(c, "{|assertJsonStringsEqual('{\"a\":' + '1}', '{\"a\":1}')}",
                    com.legend.testplan.PlanningOptions.JudgeMode.DATABASE);
            assertTrue(json.contains("not one literal"), "a joined JSON golden refused by name: " + json);
        }
        String compared = com.legend.testexecutionplan.TestPlanJson.write(Program.plan(model,
                "{|assertEquals('ab', 'a' + 'b')}", StorelessRuntime.RUNTIME, ExecuteOptions.NONE));
        assertTrue(!compared.contains("\"folded\":true"), "a compared join is no joined constant: " + compared);
        String messaged = com.legend.testexecutionplan.TestPlanJson.write(Program.plan(model,
                "{|assertEquals('ab', 'a' + 'c', 'got ' + 'it')}", StorelessRuntime.RUNTIME, ExecuteOptions.NONE));
        assertTrue(messaged.contains("\"folded\":true"), "a caller's message keeps its join: " + messaged);
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
