// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0
package com.legend.driver;

import com.legend.ExecuteOptions;
import com.legend.database.Databases;
import com.legend.test.StorelessRuntime;
import com.legend.testbackend.TestBackends;
import com.legend.testing.Program;

import java.sql.Connection;
import java.util.List;

import org.junit.jupiter.api.Test;

/**
 * MEASUREMENT ONLY (the assert audit's probes, 2026-11-04): tests a developer could reasonably write with the assert*
 * family, each run under the host judge, the database judge alone and BOTH; the outcome printed beside upstream's
 * expected outcome. Asserts nothing.
 */
class AssertProbeTest {
    private static final String MODEL = """
            ###Pure
            Class t::Person { name: String[1]; }
            Class t::Employee extends t::Person {}
            function t::check(x: Integer[1]): Boolean[1] { assertEquals(2, $x) }
            function t::check2(x: Integer[1]): Boolean[1] { assertEquals(2, $x); true; }
            """;

    private record P(String id, String body, String expected) { }

    private static final List<P> PROBES = List.of(
            new P("P1 assert custom message", "{|assert(1 == 2, 'one is not two')}", "fail: one is not two"),
            new P("P2a assertEquals custom message", "{|assertEquals(1, 2, 'custom')}", "fail: custom"),
            new P("P2b assertEquals format message", "{|assertEquals(1, 2, 'got %s', [2])}", "fail: got 2"),
            new P("P3 assert computed message", "{|assert(false, | 'computed ' + toString(1 + 1))}", "fail: computed 2"),
            new P("P4 forAll zip assert message", "{|[1, 2]->zip([2, 1])->forAll(p | assert($p.first < $p.second, 'm'))}", "fail: m"),
            new P("P5a assertNotEq", "{|assertNotEq(1, 2)}", "pass"),
            new P("P5b assertNotSize", "{|assertNotSize([1, 2], 3)}", "pass"),
            new P("P5c assertNotContains", "{|assertNotContains([1, 2], 3)}", "pass"),
            new P("P5d assertNotEq failing", "{|assertNotEq(1, 1)}", "fail"),
            new P("P6a one-statement helper passing", "{|t::check(1 + 1)}", "pass"),
            new P("P6b one-statement helper failing", "{|t::check(3)}", "fail: expected 2 actual 3"),
            new P("P6c two-statement helper failing", "{|t::check2(3)}", "fail: expected 2 actual 3"),
            new P("P7 assert in an if", "{|if(1 == 1, |assertEquals(1, 1), |assertEquals(1, 2))}", "pass"),
            new P("P8 assert bound by a let", "{|let ok = assertEquals(1, 1); assert($ok);}", "pass"),
            new P("P9 assert then a query", "{|assertEquals(1, 1); [1, 2]->map(x | $x + 1);}", "pass, answer [2, 3]"),
            new P("P10 forAll over range", "{|range(1, 4)->forAll(i | assertEquals($i, $i))}", "pass"),
            new P("P11 forAll zip computed args", "{|[1, 2]->zip([1, 2])->forAll(p | assertEquals($p.first + 1, $p.second + 1))}", "pass"),
            new P("P12a assertInstanceOf message", "{|assertInstanceOf(1, Integer, 'msg')}", "pass"),
            new P("P12b assertIs primitives", "{|assertIs(1, 1)}", "pass"),
            new P("P12c assertInstanceOf mixed at", "{|assertInstanceOf([1, 2.5]->at(0), Integer)}", "pass"),
            new P("P13 assertIs dynamic type", "{|assertIs(t::Employee, [^t::Employee(name='a'), ^t::Person(name='b')]->at(0)->type())}", "pass"),
            new P("P14a concatenate order", "{|assertEquals([3, 1, 2], [1, 2]->concatenate([3]))}", "fail"),
            new P("P14b union order", "{|assertEquals([3, 1, 2], [1, 2]->union([3]))}", "fail"),
            new P("P15 float-looking strings", "{|assertEquals('x=1.0000000000000002', 'x=1.0')}", "fail"),
            new P("P16a decimal scale", "{|assertEquals(3.0D, 3.00D)}", "fail"),
            new P("P16b integer vs float", "{|assertEquals(1, 1.0)}", "fail"),
            new P("P16c mixed list", "{|assertEquals([1, 'a', 2.5], [1, 'a', 2.5])}", "pass"),
            new P("P16d empty vs filtered", "{|assertEquals([], [1, 2]->filter(x | $x > 5))}", "pass"),
            new P("P17a assertError let message", "{|let m = 'boom'; assertError(|fail($m), $m);}", "pass"),
            new P("P17b assertError over a failing assert", "{|assertError(|assertEquals(1, 2), '\\nexpected: 1\\nactual:   2')}", "pass"),
            new P("P20 assertEmpty", "{|assertEmpty([1, 2]->filter(x | $x > 5))}", "pass"),
            new P("P21 assertSize failing", "{|assertSize([1, 2], 3)}", "fail"),
            new P("P22 assertContains failing", "{|assertContains([1, 2], 3)}", "fail"),
            new P("P23 assertSameElements", "{|assertSameElements([2, 1], [1, 2])}", "pass"),
            new P("P24 assertEqWithinTolerance", "{|assertEqWithinTolerance(1.0, 1.0005, 0.001)}", "pass"),
            new P("P25 two asserts, second fails", "{|assertEquals(1, 1); assertEquals('a', 'b');}", "fail: expected 'a' actual 'b'"),
            new P("P26 assert over a class value", "{|assertEquals('a', ^t::Person(name='a').name)}", "pass"),
            new P("P27 assertFalse custom message", "{|assertFalse(1 == 1, 'was true')}", "fail: was true"));

    @Test
    void probes() throws Exception {
        try (Connection c = TestBackends.platform().open()) {
            for (P p : PROBES) {
                for (ExecuteOptions.JudgeMode mode : List.of(ExecuteOptions.JudgeMode.HOST,
                        ExecuteOptions.JudgeMode.DATABASE, ExecuteOptions.JudgeMode.BOTH)) {
                    String out;
                    try {
                        Object v = Program.last(StorelessRuntime.with(MODEL, Databases.PLATFORM), p.body(),
                                StorelessRuntime.RUNTIME, c, ExecuteOptions.NONE.withJudgeMode(mode)).value();
                        out = "PASS (" + v + ")";
                    } catch (Throwable e) {
                        out = e.getClass().getSimpleName() + ": " + String.valueOf(e.getMessage()).replace('\n', '|');
                    }
                    if (out.length() > 260) {
                        out = out.substring(0, 260) + "...";
                    }
                    System.out.println("[probe] " + p.id() + " || " + mode + " || " + out + " || upstream: " + p.expected());
                }
            }
        }
    }
}
