// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.compiler.spec;

import com.legend.database.Databases;
import com.legend.testing.Lean;
import com.legend.testbackend.StorelessRuntime;
import com.legend.testbackend.TestBackends;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * THE LITERAL RULE'S ANSWERS ({@link NumberLiterals}; the user's ruling of 2026-11-26), run: a number literal written
 * against a value of another number type reads as that type, so the comparison answers by VALUE -- also where the
 * value is itself a literal once a call is inlined, the place a compiler's fold could answer in the database's stead
 * (the audit of the edition program's steps 4 and 5, 2026-11-26: {@code f(x: Float[1]) { $x->in([0, 1]) }} called as
 * {@code f(1.0)} was folded to false, a Float beside two Integers compared as Java objects; a fold never answers a
 * comparison across types).
 */
class LiteralRuleAnswersTest {

    private static final String MODEL = String.join("\n",
            "function x::inZeroOne(v: Float[1]): Boolean[1] { $v->in([0, 1]) }",
            "function x::isOne(v: Float[1]): Boolean[1] { $v == 1 }",
            "function x::holdsIt(v: Float[1]): Boolean[1] { [0, 1]->contains($v) }",
            "function x::above(v: Decimal[1]): Boolean[1] { $v > 1 }");

    private static List<String> values(String query) throws Exception {
        try (Connection c = TestBackends.platform().open()) {
            Lean.Result r = Lean.run(StorelessRuntime.with(MODEL, Databases.PLATFORM), query, StorelessRuntime.RUNTIME, c);
            if (!(r.value() instanceof List<?> col)) {
                throw new IllegalStateException("expected a collection frame for " + query + ", got " + r);
            }
            return col.stream().map(String::valueOf).toList();
        }
    }

    @Test
    @DisplayName("a literal argument inlined into a body that compares it with whole-number literals: answered by"
            + " value, never folded as two kinds")
    void aLiteralArgumentAgainstWholeNumberLiterals() throws Exception {
        assertEquals(List.of("true", "false"), values("|[x::inZeroOne(1.0), x::inZeroOne(0.5)]"));
        assertEquals(List.of("true", "false"), values("|[x::isOne(1.0), x::isOne(1.5)]"));
        assertEquals(List.of("true", "false"), values("|[x::holdsIt(0.0), x::holdsIt(0.25)]"));
        assertEquals(List.of("true", "false"), values("|[x::above(1.5D), x::above(1.0D)]"));
    }

    @Test
    @DisplayName("the same bodies over a value that is no literal: the database's answer, the same")
    void aComputedArgument() throws Exception {
        assertEquals(List.of("true", "false"), values("|[1.0, 0.5]->map(v | x::inZeroOne($v))"));
        assertEquals(List.of("true", "false"), values("|[1.0, 1.5]->map(v | x::isOne($v))"));
        assertEquals(List.of("true", "false"), values("|[0.0, 0.25]->map(v | x::holdsIt($v))"));
    }
}
