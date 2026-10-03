// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.compiler.element;

import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.test.StorelessRuntime;

import com.legend.Compiler;
import com.legend.compiler.element.type.Multiplicity;
import com.legend.compiler.element.type.Type;
import com.legend.exec.ExecutionResult;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.DriverManager;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * D94 (the layout row, F10 slice 4) — the [1]-vs-[*] diamond: two
 * supertypes declaring one property at different multiplicities used
 * to give {@code findProperty} = the FIRST super's answer while the
 * layout kept the LAST super's — a list under an {@code Integer[1]}
 * stamp (audit A28's {@code ^model::DiamondMult(w=1)} repro). One
 * resolution rule now: the layout keeps the first super's declaration,
 * exactly findProperty's extends-order walk.
 */
class ClassLayoutsDiamondTest {

    private static final String MODEL = """
            Class m::A { w: Integer[1]; }
            Class m::B { w: Integer[*]; }
            Class m::D extends m::A, m::B {}
            """;

    @Test
    @DisplayName("diamond duplicate keeps the FIRST super's multiplicity —"
            + " layout and findProperty agree")
    void diamondLayoutMatchesFindProperty() {
        ModelContext ctx = Compiler.compileModel(MODEL);
        List<Type.Column> layout = ClassLayouts
                .layoutOf(ctx, new Type.ClassType("m::D")).orElseThrow();
        Type.Column w = layout.stream()
                .filter(c -> c.name().equals("w")).findFirst().orElseThrow();
        Property found = ctx.findProperty("m::D", "w").orElseThrow();
        assertEquals(found.multiplicity(), w.multiplicity(),
                "layout and findProperty disagree on the diamond property");
        assertTrue(w.multiplicity() instanceof Multiplicity.Bounded b
                        && b.lower() == 1 && Integer.valueOf(1).equals(b.upper()),
                "the FIRST super declares [1]; layout says "
                        + w.multiplicity());
    }

    @Test
    @DisplayName("the executed value follows: a [1] diamond property"
            + " reads back as a SCALAR, never a list")
    void diamondInstancePropertyIsScalar() throws Exception {
        try (var conn = DriverManager.getConnection("jdbc:duckdb:")) {
            ExecutionResult r = Compiler.execute(StorelessRuntime.with(MODEL, DatabaseType.DuckDB),
                    "{|let d = ^m::D(w=1); $d.w;}", StorelessRuntime.RUNTIME, conn);
            Object v = ((ExecutionResult.Scalar) r).value();
            assertTrue(v instanceof Number n && n.longValue() == 1L,
                    "diamond [1] property delivered " + v + " ("
                            + (v == null ? "null" : v.getClass().getName())
                            + ") — a list under an Integer[1] stamp is D94");
        }
    }
}
