// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.test.StorelessRuntime;

import com.legend.Execution;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * F13 — SYNTHETIC INSTANCE IDENTITY (OPEN_REGISTER): a KEYLESS class
 * (no {@code <<equality.Key>>} tree) has ENGINE equality = instance
 * identity. Identity travels as DATA: the verdict lane's identity
 * layout adds a {@code __id} field minted deterministically per
 * CONSTRUCTION SITE (node), never per evaluation — a let-bound
 * instance equals itself, two textually distinct constructors never
 * equal, a copy is a NEW instance. Both channels carry the same rule:
 * the byte canon renders {@code {_type,_id}}, the host lattice
 * compares wire maps that include the id (content fabrication dead).
 */
// reads process-wide counters (CanonicalDivergence, Census): one such class at a time if JUnit ever runs classes in
// parallel, so no other test's calls land between its reset and its assert (Bazel workplan P3-04, A9)
@org.junit.jupiter.api.parallel.ResourceLock("process-counters")
class InstanceIdentityTest {

    private static final String MODEL = """
            Class m::P
            {
              name: String[1];
            }
            Class m::K
            {
              <<equality.Key>> id: Integer[1];
              other: String[1];
            }
            Class m::Side
            {
              <<equality.Key>> sid: String[1];
            }
            Class m::Top
            {
              <<equality.Key>> name: String[1];
              <<equality.Key>> sides: m::Side[*];
              nick: String[0..1];
            }
            Class m::Sub extends m::Top
            {
              sides: m::Side[*];
              <<equality.Key>> n: Integer[1];
            }
            function meta::pure::functions::asserts::assertEquals(expected:Any[*], actual:Any[*]):Boolean[1]
            {
                assert(equal($expected, $actual), 'assert failed');
            }
            function meta::pure::functions::asserts::assertNotEquals(expected:Any[*], actual:Any[*]):Boolean[1]
            {
                assert(!equal($expected, $actual), 'assertNotEquals: both sides are equal');
            }
            function meta::pure::functions::asserts::assertEq(expected:Any[1], actual:Any[1]):Boolean[1]
            {
                assert(eq($expected, $actual), 'assert failed');
            }
            function meta::pure::functions::asserts::assert(condition:Boolean[1], message:String[1]):Boolean[1]
            {
                assert($condition, | $message);
            }
            """;

    private static Connection conn;

    @BeforeAll
    static void setUp() throws SQLException {
        conn = DriverManager.getConnection("jdbc:duckdb:");
    }

    @AfterAll
    static void tearDown() throws SQLException {
        conn.close();
    }

    private static ExecutionResult run(String query) throws SQLException {
        return Execution.execute(StorelessRuntime.with(MODEL, DatabaseType.DuckDB), query, StorelessRuntime.RUNTIME, conn);
    }

    @Test
    @DisplayName("a let-bound keyless instance equals ITSELF (identity)")
    void sameInstanceEqual() throws Exception {
        assertEquals(Boolean.TRUE, ((ExecutionResult.Scalar) run(
                "{|let p = ^m::P(name='a'); assertEquals($p, $p);}"))
                .value());
    }

    @Test
    @DisplayName("two constructor sites are DISTINCT instances even when"
            + " content-equal (the engine-FALSE the content fabrication hid)")
    void distinctSitesNotEqual() throws Exception {
        assertEquals(Boolean.TRUE, ((ExecutionResult.Scalar) run(
                "{|assertNotEquals(^m::P(name='a'), ^m::P(name='a'))}"))
                .value());
        assertEquals(Boolean.TRUE, ((ExecutionResult.Scalar) run(
                "{|let a = ^m::P(name='a'); let b = ^m::P(name='a');"
                        + " assertNotEquals($a, $b);}")).value());
    }

    @Test
    @DisplayName("a COPY is a NEW instance — its identity mints at the"
            + " copy site")
    void copyMintsNewIdentity() throws Exception {
        assertEquals(Boolean.TRUE, ((ExecutionResult.Scalar) run(
                "{|let p = ^m::P(name='a'); let q = ^$p(name='a');"
                        + " assertNotEquals($p, $q);}")).value());
    }

    @Test
    @DisplayName("eq() over keyless instances: identity is observable now"
            + " — the wire carries __id")
    void eqIdentity() throws Exception {
        assertEquals(Boolean.TRUE, ((ExecutionResult.Scalar) run(
                "{|let p = ^m::P(name='a'); assertEq($p, $p);}")).value());
        Exception e = assertThrows(Exception.class, () -> run(
                "{|let a = ^m::P(name='a'); let b = ^m::P(name='a');"
                        + " assertEq($a, $b);}"));
        assertTrue(e.getMessage().contains("distinct instances"),
                e.getMessage());
    }

    @Test
    @DisplayName("KEYED classes are untouched: key-property equality,"
            + " no identity field")
    void keyedClassesKeyEquality() throws Exception {
        // same key, different non-key content: equal (engine rule)
        assertEquals(Boolean.TRUE, ((ExecutionResult.Scalar) run(
                "{|assertEquals(^m::K(id=1, other='x'),"
                        + " ^m::K(id=1, other='y'))}")).value());
        // the failing keyed compare fails LOUDLY (the byte verdict is
        // FALSE; the failure-message repr of instance maps is its own
        // pre-existing loud wall — either way, never a silent pass)
        Exception e = assertThrows(Exception.class, () -> run(
                "{|assertEquals(^m::K(id=1, other='x'),"
                        + " ^m::K(id=2, other='x'))}"));
        assertNotNull(e.getMessage());
    }

    @Test
    @DisplayName("v1 exclusion: a keyless ctor under a LAMBDA declines the"
            + " byte verdict (counted), host referee still judges")
    void lambdaCtorDeclines() throws Exception {
        long before = CanonicalDivergence.sqlDeclinedCount();
        assertEquals(Boolean.TRUE, ((ExecutionResult.Scalar) run(
                "{|let l = [1,2]->map(x|^m::P(name=$x->toString()));"
                        + " assertEquals($l, $l);}")).value());
        assertTrue(CanonicalDivergence.sqlDeclinedCount() > before,
                "the lambda-minted ctor pair must DECLINE the byte"
                        + " verdict, never claim it");
    }

    @Test
    @DisplayName("F13c: eq inside a CONDITION is identity — content-equal"
            + " keyed instances are distinct objects")
    void eqInCondition() throws Exception {
        // mirrors PCT testEqNonPrimitive: side1/side2 content-identical
        assertEquals(Boolean.TRUE, ((ExecutionResult.Scalar) run(
                "{|let a = ^m::K(id=1, other='x');"
                        + " let b = ^m::K(id=1, other='x');"
                        + " assert(eq($a, $a) && !eq($a, $b));}")).value());
    }

    @Test
    @DisplayName("F13c: equal inside a CONDITION is the KEY relation —"
            + " non-key content is outside it, cross-class is false")
    void equalInCondition() throws Exception {
        // mirrors PCT testEqualNonPrimitive: right11 == right12 differ
        // ONLY in a non-key property
        assertEquals(Boolean.TRUE, ((ExecutionResult.Scalar) run(
                "{|let a = ^m::K(id=1, other='x');"
                        + " let b = ^m::K(id=1, other='y');"
                        + " assert(equal($a, $b) && ($a == $b));}")).value());
        assertEquals(Boolean.TRUE, ((ExecutionResult.Scalar) run(
                "{|let a = ^m::K(id=1, other='x');"
                        + " let c = ^m::K(id=2, other='x');"
                        + " assert(!equal($a, $c) && ($a != $c));}")).value());
        // cross-class: never equal (engine classifier rule)
        assertEquals(Boolean.TRUE, ((ExecutionResult.Scalar) run(
                "{|let a = ^m::K(id=1, other='x');"
                        + " let p = ^m::P(name='x');"
                        + " assert(!equal($a, $p));}")).value());
    }

    @Test
    @DisplayName("F13c: keyless equal/eq in a CONDITION are both identity")
    void keylessConditionIdentity() throws Exception {
        assertEquals(Boolean.TRUE, ((ExecutionResult.Scalar) run(
                "{|let a = ^m::P(name='x'); let b = ^m::P(name='x');"
                        + " assert(equal($a, $a) && !equal($a, $b)"
                        + " && eq($a, $a) && !eq($a, $b));}")).value());
    }

    @Test
    @DisplayName("F13c: nested to-many keys + the un-keyed redeclaration"
            + " SHADOW (engine _Class: simple properties filtered by"
            + " stereotype)")
    void keyShadowAndNestedKeys() throws Exception {
        // Top's keys = name + sides (class-typed to-many): key-equal
        // sides through KEY-equal (not identical) elements
        assertEquals(Boolean.TRUE, ((ExecutionResult.Scalar) run(
                "{|let s1 = ^m::Side(sid='a'); let s2 = ^m::Side(sid='a');"
                        + " let t1 = ^m::Top(name='t', sides=[$s1]);"
                        + " let t2 = ^m::Top(name='t', sides=[$s2]);"
                        + " assert(equal($t1, $t2));}")).value());
        // Sub REDECLARES sides WITHOUT the key stereotype: it stops
        // being a key — Subs differing only in sides stay equal, and n
        // (Sub's own key) still separates
        assertEquals(Boolean.TRUE, ((ExecutionResult.Scalar) run(
                "{|let s1 = ^m::Side(sid='a'); let s3 = ^m::Side(sid='b');"
                        + " let u1 = ^m::Sub(name='t', n=1, sides=[$s1]);"
                        + " let u2 = ^m::Sub(name='t', n=1, sides=[$s3]);"
                        + " let u3 = ^m::Sub(name='t', n=2, sides=[$s1]);"
                        + " assert(equal($u1, $u2) && !equal($u1, $u3));}"))
                .value());
    }

    @Test
    @DisplayName("F13c: contains/in over keyed instances is equal() per"
            + " element — a fresh key-matching instance IS contained")
    void containsIsKeyedEquality() throws Exception {
        assertEquals(Boolean.TRUE, ((ExecutionResult.Scalar) run(
                "{|let a = ^m::K(id=1, other='x'); let b = ^m::K(id=2, other='y');"
                        + " assert([$a, $b]->contains(^m::K(id=1, other='z'))"
                        + " && !([$a, $b]->contains(^m::K(id=3, other='x'))));}"))
                .value());
    }

    @Test
    @DisplayName("Pair-of-Pairs: substitution-aware key trees — a nested"
            + " Pair key leaf renders, never declines")
    void pairOfPairsKeys() throws Exception {
        long before = CanonicalDivergence.sqlDeclinedCount();
        assertEquals(Boolean.TRUE, ((ExecutionResult.Scalar) run(
                "{|assert(equal(pair(pair(1, 'a'), 2),"
                        + " pair(pair(1, 'a'), 2)));}")).value());
        assertEquals(Boolean.TRUE, ((ExecutionResult.Scalar) run(
                "{|assert(!equal(pair(pair(1, 'a'), 2),"
                        + " pair(pair(1, 'b'), 2)));}")).value());
        // and through the VERDICT wrap (the zip family's shape):
        assertEquals(Boolean.TRUE, ((ExecutionResult.Scalar) run(
                "{|assertEquals(pair(pair(1, 'a'), 2),"
                        + " pair(pair(1, 'a'), 2))}")).value());
        assertEquals(before, CanonicalDivergence.sqlDeclinedCount(),
                "Pair-of-Pairs must CLAIM, not decline: "
                        + CanonicalDivergence.summary());
    }

    @Test
    @DisplayName("plain (rider-less) value reads keep the PLAIN layout —"
            + " no __id leaks into ordinary wire maps")
    void plainLaneUnperturbed() throws Exception {
        Object v = ((ExecutionResult.Scalar) run(
                "{|^m::P(name='a')}")).value();
        assertTrue(v instanceof java.util.Map<?, ?> m
                        && !m.containsKey("__id")
                        && "a".equals(m.get("name")),
                "plain-lane instance wire changed: " + v);
    }

    @Test
    @DisplayName("D91: equal over KEYED instances in VALUE position (the"
            + " execute path, no rider) is the KEY relation — non-key"
            + " content is outside it; the structural fallback that read"
            + " every field is gone")
    void keyedEqualInValuePosition() throws Exception {
        // same key, different non-key content: TRUE (the engine rule);
        // the old structural lowering compared 'other' and said FALSE
        assertEquals(Boolean.TRUE, ((ExecutionResult.Scalar) run(
                "{|let a = ^m::K(id=1, other='x');"
                        + " let b = ^m::K(id=1, other='y');"
                        + " equal($a, $b);}")).value());
        // different key: FALSE
        assertEquals(Boolean.FALSE, ((ExecutionResult.Scalar) run(
                "{|let a = ^m::K(id=1, other='x');"
                        + " let c = ^m::K(id=2, other='x');"
                        + " equal($a, $c);}")).value());
    }

    @Test
    @DisplayName("D91: keyed membership in VALUE position rides the same"
            + " key canon")
    void keyedMembershipInValuePosition() throws Exception {
        assertEquals(Boolean.TRUE, ((ExecutionResult.Scalar) run(
                "{|let a = ^m::K(id=1, other='x');"
                        + " [^m::K(id=1, other='y'), ^m::K(id=3, other='z')]"
                        + "->contains($a);}")).value());
        assertEquals(Boolean.FALSE, ((ExecutionResult.Scalar) run(
                "{|let a = ^m::K(id=9, other='x');"
                        + " [^m::K(id=1, other='y'), ^m::K(id=3, other='z')]"
                        + "->contains($a);}")).value());
    }
}
