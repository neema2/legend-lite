// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0
package com.legend.driver;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.legend.compiler.element.type.Type;
import com.legend.exec.PlanRunner;
import com.legend.executionplan.ExecutionPlan;
import com.legend.judge.TypedSide;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * A constant side of the plan (the seam program's S2f, 2026-10-30): the fold's value written as the program's text
 * under one typed column, read back by that column's type into the value the judge sees, and checked against the
 * compiler's value -- a round trip through text that must lose nothing for the three kinds the folds admit, and
 * refuse by name for any other.
 */
class ConstantSideTest {

    private static PlanRunner.Rows handed(ExecutionPlan.Side.Constant c) {
        List<List<Object>> rows = new ArrayList<>();
        for (List<String> row : c.cells()) {
            rows.add(new ArrayList<Object>(row));
        }
        return new PlanRunner.Rows(c.columns(), rows);
    }

    @Test
    @DisplayName("a String, an Integer and a Boolean scalar: the text is the token, and reads back as the value")
    void scalarsRoundTrip() {
        TypedSide s = new TypedSide.Scalar("a'b", Type.Primitive.STRING);
        ExecutionPlan.Side.Constant cs = JudgePlans.constantSide(s, false);
        assertEquals(List.of(List.of("a'b")), cs.cells());
        assertEquals("String", cs.columns().get(0).type());
        assertEquals(new ExecutionPlan.Multiplicity(1, 1), cs.columns().get(0).multiplicity());
        assertEquals(s, JudgePlans.decodeConstant(cs, s, handed(cs)));

        TypedSide i = new TypedSide.Scalar(123456789012345L, Type.Primitive.INTEGER);
        ExecutionPlan.Side.Constant ci = JudgePlans.constantSide(i, false);
        assertEquals(List.of(List.of("123456789012345")), ci.cells());
        assertEquals(i, JudgePlans.decodeConstant(ci, i, handed(ci)));

        TypedSide b = new TypedSide.Scalar(Boolean.TRUE, Type.Primitive.BOOLEAN);
        ExecutionPlan.Side.Constant cb = JudgePlans.constantSide(b, false);
        assertEquals(List.of(List.of("true")), cb.cells());
        assertEquals(b, JudgePlans.decodeConstant(cb, b, handed(cb)));
    }

    @Test
    @DisplayName("a collection: one row per value; an Integer past 63 bits keeps its digits; folded is carried")
    void collectionsRoundTrip() {
        TypedSide c = new TypedSide.Collection(List.of("x", "y"), Type.Primitive.STRING);
        ExecutionPlan.Side.Constant cc = JudgePlans.constantSide(c, true);
        assertEquals(List.of(List.of("x"), List.of("y")), cc.cells());
        assertTrue(cc.folded(), "the compiler's join is marked");
        assertEquals(new ExecutionPlan.Multiplicity(0, null), cc.columns().get(0).multiplicity());
        assertEquals(c, JudgePlans.decodeConstant(cc, c, handed(cc)));

        java.math.BigInteger wide = new java.math.BigInteger("123456789012345678901234567890");
        TypedSide w = new TypedSide.Collection(List.of(1L, wide), Type.Primitive.INTEGER);
        ExecutionPlan.Side.Constant cw = JudgePlans.constantSide(w, false);
        TypedSide read = JudgePlans.decodeConstant(cw, w, handed(cw));
        assertEquals(List.of(1L, wide), ((TypedSide.Collection) read).values());

        TypedSide empty = new TypedSide.Collection(List.of(), Type.Primitive.STRING);
        ExecutionPlan.Side.Constant ce = JudgePlans.constantSide(empty, false);
        assertEquals(List.of(), ce.cells());
        assertEquals(empty, JudgePlans.decodeConstant(ce, empty, handed(ce)));
    }

    @Test
    @DisplayName("a null scalar is an absent cell under [0..1]; a text that differs from the value is refused by name")
    void absentAndDrift() {
        TypedSide n = new TypedSide.Scalar(null, Type.Primitive.STRING);
        ExecutionPlan.Side.Constant cn = JudgePlans.constantSide(n, false);
        assertEquals(Arrays.asList((String) null), cn.cells().get(0));
        assertEquals(new ExecutionPlan.Multiplicity(0, 1), cn.columns().get(0).multiplicity());
        assertEquals(n, JudgePlans.decodeConstant(cn, n, handed(cn)));

        TypedSide i = new TypedSide.Scalar(7L, Type.Primitive.INTEGER);
        ExecutionPlan.Side.Constant ci = JudgePlans.constantSide(i, false);
        PlanRunner.Rows drifted = new PlanRunner.Rows(ci.columns(), List.of(List.of("8")));
        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> JudgePlans.decodeConstant(ci, i, drifted));
        assertTrue(e.getMessage().contains("differs from the compiler's value"), e.getMessage());
    }

    @Test
    @DisplayName("a kind the folds never answer is refused by name, never spelled")
    void otherKindsRefused() {
        TypedSide f = new TypedSide.Scalar(1.5d, Type.Primitive.FLOAT);
        com.legend.error.NotImplementedException e = assertThrows(com.legend.error.NotImplementedException.class,
                () -> JudgePlans.constantSide(f, false));
        assertTrue(e.getMessage().contains("String, Integer and Boolean"), e.getMessage());
    }
}
