// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.compiler.spec;

import com.legend.testing.Program;

import com.legend.testing.Lean;

import com.legend.database.Databases;
import com.legend.testbackend.TestBackends;
import com.legend.test.StorelessRuntime;

import com.legend.Compiler;
import com.legend.compiler.spec.typed.TypedSpec;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.Connection;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Pins the STRICT lower-bound kernel (multiplicity audit
 * docs/MULTIPLICITY_AUDIT_2026_08_20.md §1, slice 2): real pure's
 * {@code MultiplicityMatch} rejects {@code [0..1]} into a {@code [1]}
 * slot — that is precisely why {@code toOne()} exists. Before this
 * slice the kernel accepted it and manufactured a false {@code [1]}
 * on the most common expression shape in Legend; ZERO tests pinned
 * the rejection.
 */
class MultiplicityStrictnessTest {

    private static final String MODEL =
            "Class m::Person { name: String[1]; middleName: String[0..1]; "
                    + "nicks: String[*]; }\n";

    private static Exception rejects(String query) {
        return assertThrows(Exception.class,
                () -> Compiler.query(Compiler.compileModel(MODEL), query).expression());
    }

    @Test
    @DisplayName("audit §1: [0..1] property into a [1] native slot is REJECTED")
    void optionalPropertyIntoToOneSlotRejects() {
        // the audit's reproduction table, row 1: this used to stamp [1]
        Exception e = rejects(
                "m::Person.all()->map(p|$p.middleName->toUpper())");
        assertTrue(e.getMessage().contains("[0..1] is not compatible with [1]"),
                e.getMessage());
    }

    @Test
    @DisplayName("real pure: [0..1] into infix plus is REFUSED — the run's literal takes each element [1]")
    void optionalIntoArithmeticRefusedLikeRealPure() {
        // the parser's run plus([$p.middleName, '!']) is a literal of two values; legend-pure
        // (InstanceValueValidator) and legend-engine (ValueSpecificationBuilder.visit(Collection))
        // require each element [1] before any overload is matched. Accepting it (2026-09-11)
        // rested on "real pure accepts it", which legend-pure's source contradicts.
        Exception e = rejects("m::Person.all()->map(p|$p.middleName + '!')");
        assertTrue(e.getMessage().contains("Collection element must have a multiplicity [1], found [0..1]"),
                e.getMessage());
        // the toOne() spelling says what is meant
        TypedSpec ok = Compiler.query(Compiler.compileModel(MODEL), "m::Person.all()->map(p|$p.middleName->toOne() + '!')").expression();
        assertEquals("[*]", ok.info().multiplicity().text());
    }

    @Test
    @DisplayName("control: [*] into [1] still rejected (the pre-existing guard)")
    void manyIntoToOneSlotStillRejects() {
        assertTrue(rejects("m::Person.all()->map(p|$p.nicks->toUpper())")
                .getMessage().contains("not compatible"));
    }

    @Test
    @DisplayName("audit §1b: declared return [1] with a [0..1] body is REJECTED at inline")
    void declaredReturnStricterThanBodyRejects() throws Exception {
        try (Connection c = TestBackends.platform().open()) {
            Exception e = assertThrows(Exception.class, () -> Lean.run(StorelessRuntime.with(MODEL + "function m::f(a: String[0..1]): String[1] { $a }\n", Databases.PLATFORM),
                    "|m::f('x')", StorelessRuntime.RUNTIME, c));
            assertTrue(String.valueOf(e.getMessage())
                            .contains("[0..1] is not compatible with [1]"),
                    e.getMessage());
        }
    }

    @Test
    @DisplayName("audit §1b: declared return [3] with a [2] body is REJECTED at inline")
    void declaredReturnCountMismatchRejects() throws Exception {
        try (Connection c = TestBackends.platform().open()) {
            Exception e = assertThrows(Exception.class, () -> Lean.run(StorelessRuntime.with(MODEL + "function m::g(): String[3] { ['a', 'b'] }\n", Databases.PLATFORM),
                    "|m::g()", StorelessRuntime.RUNTIME, c));
            assertTrue(String.valueOf(e.getMessage()).contains("not compatible"),
                    e.getMessage());
        }
    }

    @Test
    @DisplayName("audit §2: [1,2]->toOne() raises PURE's user error in the database, not an internal assertion")
    void literalCollectionToOneRaisesUserError() throws Exception {
        try (Connection c = TestBackends.platform().open()) {
            Exception e = assertThrows(Exception.class,
                    () -> Lean.run(StorelessRuntime.with("", Databases.PLATFORM), "{| [1,2]->toOne() }", StorelessRuntime.RUNTIME, c));
            assertTrue(String.valueOf(e.getMessage())
                            .contains("Cannot cast a collection of size 2"
                                    + " to multiplicity [1]"),
                    e.getMessage());
            // the singleton extracts — the guard is size-exact
            var ok = Lean.run(StorelessRuntime.with("", Databases.PLATFORM), "{| [7]->toOne() }", StorelessRuntime.RUNTIME, c);
            assertEquals(7L, ((Number) ok.one()).longValue());
        }
    }

    @Test
    @DisplayName("audit §3: a runtime-emptied list through toOne() raises size-0 (the lower bound, checked in SQL)")
    void runtimeEmptyListToOneRaises() throws Exception {
        try (Connection c = TestBackends.platform().open()) {
            Exception e = assertThrows(Exception.class, () -> Lean.run(StorelessRuntime.with("", Databases.PLATFORM), "{| [1,2,3]->filter(x|$x > 10)->toOne() }", StorelessRuntime.RUNTIME, c));
            assertTrue(String.valueOf(e.getMessage())
                            .contains("Cannot cast a collection of size 0"),
                    e.getMessage());
        }
    }

    @Test
    @DisplayName("egress slice A: a [1]-declared scalar result with ZERO rows raises (the engine's resultSizeRange)")
    void scalarEgressLowerBoundRaisesOnZeroRows() throws Exception {
        try (Connection c = TestBackends.platform().open()) {
            // relation lane: the in-expression toOne FLOWS (row-lane
            // adjudication), so the FINISH-LINE check is what catches a
            // broken exactly-one promise — engine parity, egress-side
            Exception e = assertThrows(Exception.class, () -> Lean.run(StorelessRuntime.with("", Databases.PLATFORM),
                    "{| #TDS\n  x:Integer\n  1\n#"
                            + "->filter(r|$r.x > 5)->map(r|$r.x)->toOne() }", StorelessRuntime.RUNTIME,
                    c));
            assertTrue(String.valueOf(e.getMessage())
                            .contains("Cannot cast a collection of size 0"),
                    e.getMessage());
            // control: a satisfied promise still flows
            var ok = Lean.run(StorelessRuntime.with("", Databases.PLATFORM),
                    "{| #TDS\n  x:Integer\n  7\n#"
                            + "->filter(r|$r.x > 5)->map(r|$r.x)->toOne() }", StorelessRuntime.RUNTIME, c);
            assertEquals(7L, ((Number) ok.one()).longValue());
            // TWO rows at the root raise PURE's size message, not the
            // backend's bare more-than-one-row subquery error
            Exception e2 = assertThrows(Exception.class, () -> Lean.run(StorelessRuntime.with("", Databases.PLATFORM),
                    "{| #TDS\n  x:Integer\n  1\n  2\n#"
                            + "->map(r|$r.x)->toOne() }", StorelessRuntime.RUNTIME, c));
            assertTrue(String.valueOf(e2.getMessage())
                            .contains("Cannot cast a collection of size 2"
                                    + " to multiplicity [1]"),
                    e2.getMessage());
        }
    }

    @Test
    @DisplayName("egress slice A: a [1..*]-declared collection result with ZERO rows raises")
    void collectionEgressLowerBoundRaisesOnZeroRows() throws Exception {
        try (Connection c = TestBackends.platform().open()) {
            // the judges raise Pure's error; the plan path writes [] (the T-b2 ledger, D46: queued to track F)
            Exception e = assertThrows(Exception.class, () -> Program.last(StorelessRuntime.with("", Databases.PLATFORM),
                    "{| #TDS\n  x:Integer\n  1\n#"
                            + "->filter(r|$r.x > 5)->map(r|$r.x)"
                            + "->toOneMany() }", StorelessRuntime.RUNTIME, c));
            assertTrue(String.valueOf(e.getMessage())
                            .contains("Cannot cast a collection of size 0"),
                    e.getMessage());
            // control: satisfied [1..*] still yields the collection
            var ok = Lean.run(StorelessRuntime.with("", Databases.PLATFORM),
                    "{| #TDS\n  x:Integer\n  7\n  9\n#"
                            + "->map(r|$r.x)->toOneMany() }", StorelessRuntime.RUNTIME, c);
            assertEquals(java.util.List.of(7L, 9L),
                    (ok)
                            .values().stream().map(v -> ((Number) v)
                                    .longValue()).toList());
        }
    }

    @Test
    @DisplayName("audit §4: runtime-empty [0..1] takes PURE's empty identities — and/or/joinStrings/makeString")
    void emptyIdentityForkIsClosed() throws Exception {
        try (Connection c = TestBackends.platform().open()) {
            record Case(String q, Object want) { }
            for (Case k : new Case[] {
                    new Case("{| [true,false]->filter(x|false)->head()->and() }", true),
                    new Case("{| [true,false]->filter(x|false)->head()->or() }", false),
                    new Case("{| ['a','b']->filter(x|false)->head()->joinStrings('-') }", ""),
                    new Case("{| ['a','b']->filter(x|false)->head()->makeString() }", ""),
            }) {
                Object got = Lean.run(StorelessRuntime.with("", Databases.PLATFORM), k.q(), StorelessRuntime.RUNTIME, c).one();
                assertEquals(k.want(), got, k.q());
            }
        }
    }

    @Test
    @DisplayName("lambda-RESULT covariance: a [0..1] key conforms to sortBy's {T[1]->U[1]} (engine-observed)")
    void lambdaResultLowerBoundIsCovariant() {
        // the reference's own corpus compiles sortBy over optional
        // association paths; only the VALUE slots are strict
        Compiler.query(Compiler.compileModel(MODEL), "m::Person.all()->sortBy(p|$p.middleName)").expression();
    }
}
