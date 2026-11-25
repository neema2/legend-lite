// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.compiler.spec;

import com.legend.Compiler;
import com.legend.compiler.element.ModelContext;
import com.legend.error.ModelException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * THE ORDER RULE'S WALK (B3 step 4 and its review, 2026-11-24): an {@code assertEquals} or {@code assertNotEquals} of rows
 * with no defined order is refused, unless either side holds at most one value; a lambda's parameters are values, never
 * frames; an {@code assertError}'s lambda is not walked.
 */
class OrderedAssertsTest {

    private static final String MODEL = "Class x::A { n: Integer[1]; }\n";

    private static void refuse(String query) {
        ModelContext ctx = Compiler.compileModel(MODEL);
        OrderedAsserts.refuse(Compiler.query(ctx, query).body());
    }

    @Test
    @DisplayName("an unsorted store read compared in order with several values: refused, naming the fixes")
    void anUnsortedReadIsRefused() {
        for (String q : List.of("|assertEquals([1, 2], x::A.all().n)", "|assertEquals(x::A.all().n, [1, 2])",
                "|assertNotEquals([1, 2], x::A.all().n)", "|let r = x::A.all(); assertEquals([1, 2], $r.n);")) {
            ModelException e = assertThrows(ModelException.class, () -> refuse(q), q);
            assertTrue(e.getMessage().contains("sort it, or say assertSameElements"), e.getMessage());
        }
    }

    @Test
    @DisplayName("sorted, one value on either side, order-free, a lambda's parameter, an assertError's lambda: admitted")
    void theseAreAdmitted() {
        for (String q : List.of("|assertEquals([1, 2], x::A.all().n->sort())", "|assertEquals([1], x::A.all().n)",
                "|assertEquals(1, x::A.all().n->toOne())", "|assertSameElements([1, 2], x::A.all().n)",
                "|let f = {l: Integer[*] | assertEquals([1, 2], $l)}; true;",
                "|assertError(| assertEquals([1, 2], x::A.all().n), 'no')")) {
            assertDoesNotThrow(() -> refuse(q), q);
        }
    }
}
