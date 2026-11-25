// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge;

import com.legend.Compiler;
import com.legend.TypedQuery;
import com.legend.compiler.element.ModelContext;
import com.legend.compiler.spec.CrossTypeComparisons;
import com.legend.compiler.spec.typed.TypedCBoolean;
import com.legend.compiler.spec.typed.TypedCInteger;
import com.legend.compiler.spec.typed.TypedSpec;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * THE TWO DOORS' ANSWERS TO A COMPARISON ACROSS TYPES (B3 step 5a; its review, 2026-11-25): legend-pure's by the types
 * alone -- an equality false, a membership false or -1, an ordering the database's comparison -- and legend-engine's
 * the database's comparison. Neither leaves a marker behind.
 */
class PureComparisonsTest {

    private static TypedQuery typed(String query) {
        ModelContext ctx = Compiler.compileModel("Class x::A { n: Integer[1]; }\n");
        return Compiler.query(ctx, query);
    }

    private static TypedSpec pures(String query) {
        TypedQuery answered = PureComparisons.asPures(typed(query));
        assertEquals(List.of(), CrossTypeComparisons.in(answered.body()), query);
        return answered.body().get(answered.body().size() - 1);
    }

    @Test
    @DisplayName("legend-pure's door: an equality false, a membership false, an index -1, numbers ordered as written")
    void legendPuresAnswers() {
        assertEquals(false, assertInstanceOf(TypedCBoolean.class, pures("|1 == 1.0")).value());
        assertEquals(false, assertInstanceOf(TypedCBoolean.class, pures("|1->in([1.0, 2.0])")).value());
        assertEquals(-1L, assertInstanceOf(TypedCInteger.class, pures("|[1.0, 2.0]->indexOf(1)")).value());
        assertTrue(pures("|1 < 1.5").children().size() == 2, "the comparison itself");
    }

    @Test
    @DisplayName("legend-pure's door: an ordering of two non-numbers the database's comparison (a day against a"
            + " date-time; the tie at one instant recorded)")
    void legendPureOrdersOtherTypesAsTheDatabase() {
        assertTrue(pures("|%2020-01-02 < %2020-01-03T00:00:00").children().size() == 2, "the comparison itself");
    }

    @Test
    @DisplayName("legend-engine's door: each comparison the database's, no marker left")
    void legendEnginesAnswer() {
        TypedQuery answered = EngineComparisons.asTheDatabases(typed("|(1 == 1.0) && (%2020-01-02 < %2020-01-03T00:00:00)"));
        assertEquals(List.of(), CrossTypeComparisons.in(answered.body()));
    }
}
