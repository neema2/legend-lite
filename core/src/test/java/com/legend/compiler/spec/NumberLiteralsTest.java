// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.compiler.spec;

import com.legend.Compiler;
import com.legend.TypedQuery;
import com.legend.compiler.element.ModelContext;
import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.typed.TypedCDecimal;
import com.legend.compiler.spec.typed.TypedCFloat;
import com.legend.compiler.spec.typed.TypedCInteger;
import com.legend.compiler.spec.typed.TypedSpec;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * THE LITERAL RULE ({@link NumberLiterals}; the user's ruling of 2026-11-26): under lite's comparison setting a number
 * literal written against a value of another number type reads as that type when it is exact there, and the
 * comparison is no mistake; everything else across types stays one.
 */
class NumberLiteralsTest {

    private static final ModelContext CTX = Compiler.compileModel("Class x::A { n: Integer[1]; }");

    private static TypedQuery typed(String query) {
        return Compiler.query(CTX, query);
    }

    private static int crossings(String query) {
        return CrossTypeComparisons.in(typed(query).body()).size();
    }

    /** Every number literal in {@code query} as typed, in order. */
    private static List<TypedSpec> literals(String query) {
        List<TypedSpec> out = new ArrayList<>();
        for (TypedSpec statement : typed(query).body()) {
            collect(statement, out);
        }
        return out;
    }

    private static void collect(TypedSpec n, List<TypedSpec> out) {
        if (n instanceof TypedCInteger || n instanceof TypedCFloat || n instanceof TypedCDecimal) {
            out.add(n);
        }
        for (TypedSpec k : n.children()) {
            collect(k, out);
        }
    }

    @Test
    @DisplayName("a whole number against a Float or a Decimal, a fraction against a Decimal, a whole Float against an"
            + " Integer: the literal reads as the value's type, and nothing is marked")
    void aLiteralReadsAsTheValuesType() {
        for (String q : List.of("|let a = 1.5; $a > 0;", "|let a = 1.5; 0 < $a;", "|let a = 1.5; $a == -1;",
                "|let a = 1.5D; $a < 50000;", "|let a = 1.5D; $a >= 2.375;", "|let a = 1.5D; $a != 0.1;",
                "|let a = 1; $a == 2.0;", "|let a = 1.5; $a->in([0, 1]);", "|let a = 1.5; [0, 1]->contains($a);",
                "|let a = 1.5; max($a, 0);")) {
            assertEquals(0, crossings(q), q);
        }
        // the literal's own node carries the new type and the same number
        TypedSpec zero = literals("|let a = 1.5; $a > 0;").get(1);
        assertTrue(zero instanceof TypedCFloat f && f.value() == 0.0 && f.info().type() == Type.Primitive.FLOAT,
                zero.toString());
        TypedSpec minusOne = literals("|let a = 1.5; $a == -1;").get(1);
        assertTrue(minusOne instanceof TypedCFloat f && f.value() == -1.0, minusOne.toString());
        // read by its written digits: 0.1 against a Decimal is the Decimal 0.1, never the double nearest to it
        TypedSpec tenth = literals("|let a = 1.5D; $a != 0.1;").get(1);
        assertTrue(tenth instanceof TypedCDecimal d && d.value().compareTo(new java.math.BigDecimal("0.1")) == 0,
                tenth.toString());
        TypedSpec two = literals("|let a = 1; $a == 2.0;").get(1);
        assertTrue(two instanceof TypedCInteger i && i.value().longValue() == 2L, two.toString());
    }

    @Test
    @DisplayName("not exact in the other type, a Decimal literal, two literals, two values: still a comparison across"
            + " types")
    void whatTheRuleDoesNotReach() {
        for (String q : List.of("|let a = 1; $a == 1.5;", "|let a = 1; $a->in([0.5, 1.0]);",
                "|let a = 1.5; $a == 9007199254740993;", "|let a = 1.5; $a == 1D;", "|1 == 1.0", "|1->in([1.0, 2.0])",
                "|let a = 1.5; let b = 1; $a == $b;", "|let a = 1.5; let b = 1; $a > $b + 1;")) {
            assertEquals(1, crossings(q), q);
        }
    }

    @Test
    @DisplayName("a literal against a value of its own type, and arithmetic, are untouched")
    void oneTypeIsUntouched() {
        assertTrue(literals("|let a = 1; $a > 0;").get(1) instanceof TypedCInteger);
        assertTrue(literals("|let a = 1.5; $a + 1 > 0.5;").get(1) instanceof TypedCInteger,
                "arithmetic keeps its typed promotion: the 1 of $a + 1 stays an Integer");
    }
}
