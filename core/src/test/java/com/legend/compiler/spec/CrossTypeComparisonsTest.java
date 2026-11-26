// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.compiler.spec;

import com.legend.Compiler;
import com.legend.TypedQuery;
import com.legend.compiler.element.ModelContext;
import com.legend.compiler.spec.typed.TypedCrossTypeComparison;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.error.ModelException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * THE ONE EQUALITY'S TYPE RULE (B3 step 5; the user's ruling of 2026-11-24): a comparison whose two sides' types are
 * known and differ is typed as the marker ({@link TypedCrossTypeComparison}), which lite's planner refuses by name; a
 * comparison of one type, or of a type known only when the query runs, is the call itself.
 */
class CrossTypeComparisonsTest {

    private static final String MODEL = """
            Enum x::Kind { CITY, REGION }
            Class x::A { n: Integer[1]; }
            Class x::B extends x::A { }
            Class x::C { n: Integer[1]; }
            Class x::P { p: Integer[1]; }
            Class x::Q { q: Integer[1]; }
            Class x::PQ extends x::P, x::Q { }
            function x::loose(a: Integer[1]): Boolean[1] { $a == 1.0 }
            function x::keep<T>(r: Relation<T>[1], b: Boolean[1]): Relation<T>[1] { $r->filter(x | $b) }
            """;

    private static List<TypedCrossTypeComparison> crossings(String query) {
        ModelContext ctx = Compiler.compileModel(MODEL);
        TypedQuery typed = Compiler.query(ctx, query);
        return CrossTypeComparisons.in(typed.body());
    }

    @Test
    @DisplayName("numbers of different types, an enumeration against a String, a day against a date-time: marked")
    void comparisonsAcrossTypesAreMarked() {
        for (String q : List.of("|1 == 1.0", "|1D == 1", "|1 != 1.5", "|x::Kind.CITY == 'CITY'",
                "|%2020-01-02 == %2020-01-02T00:00:00", "|1 < 1.5", "|1->in([1.0, 2.0])", "|[1.0, 2.0]->contains(1)",
                "|'a' == 1", "|^x::A(n=1) == ^x::C(n=1)", "|max(%2020-01-02, %2020-01-02T00:00:00)")) {
            assertEquals(1, crossings(q).size(), q);
        }
        TypedCrossTypeComparison c = crossings("|1 == 1.0").get(0);
        assertEquals(TypedCrossTypeComparison.Kind.EQUALITY, c.kind());
        assertEquals("==", c.operator());
        assertEquals(com.legend.compiler.element.type.Type.Primitive.INTEGER, c.left());
        assertEquals(com.legend.compiler.element.type.Type.Primitive.FLOAT, c.right());
    }

    @Test
    @DisplayName("one type on both sides (a Decimal of any scale is a Decimal; a subclass is its class), or a type known"
            + " only when the query runs: the call itself")
    void oneTypeOrARunTimeTypeIsTheCall() {
        for (String q : List.of("|1 == 2", "|1.0 == 2.5", "|3.0D == 3.00D", "|'a' == 'b'",
                "|x::Kind.CITY == x::Kind.REGION", "|%2020-01-02 == %2020-01-03", "|1 < 2",
                "|^x::A(n=1) == ^x::B(n=1)", "|let l = [1, 2.0]; $l->at(0) == 1;", "|1->in([1, 2])")) {
            assertEquals(List.of(), crossings(q), q);
        }
    }

    @Test
    @DisplayName("step 5a's review: a function's own body unmarked; two classes one value can be of unmarked; an"
            + " argument of an inlined call keeps its marker")
    void theReviewsCases() {
        assertEquals(List.of(), crossings("|x::loose(1)"));
        assertEquals(List.of(), crossings(
                "|let p = ^x::PQ(p=1, q=1)->cast(@x::P); let q = ^x::PQ(p=1, q=1)->cast(@x::Q); $p == $q;"));
        assertEquals(1, crossings("|#TDS\nn\n1\n#->x::keep(1 == 1.0)").size());
    }

    @Test
    @DisplayName("the planner refuses the marker by name, naming both types and the fix")
    void thePlannerRefusesItByName() {
        ModelContext ctx = Compiler.compileModel(MODEL);
        List<TypedSpec> body = Compiler.query(ctx, "|1 == 1.0").body();
        ModelException e = assertThrows(ModelException.class, () -> CrossTypeComparisons.refuse(body));
        assertTrue(e.getMessage().startsWith("== compares Integer with Float: two values are equal only when"
                + " they have the same type and the same value"), e.getMessage());
    }

    @Test
    @DisplayName("a marker wraps the call the typer typed: taken off, the statement holds none")
    void aMarkerWrapsTheCallTheTyperTyped() {
        ModelContext ctx = Compiler.compileModel(MODEL);
        TypedSpec statement = Compiler.query(ctx, "|(1 == 1.0) && (2 == 2.0)").body().get(0);
        assertEquals(2, CrossTypeComparisons.in(List.of(statement)).size());
        TypedSpec engine = unwrapped(statement);
        assertEquals(List.of(), CrossTypeComparisons.in(List.of(engine)));
    }

    /** {@code n} with each marker replaced by the call it wraps (legend-engine's setting answers that call where the
     *  comparison is typed, edge.EngineComparisons). */
    private static TypedSpec unwrapped(TypedSpec n) {
        TypedSpec rebuilt = n.mapChildren(CrossTypeComparisonsTest::unwrapped);
        return rebuilt instanceof TypedCrossTypeComparison c ? c.comparison() : rebuilt;
    }
}
