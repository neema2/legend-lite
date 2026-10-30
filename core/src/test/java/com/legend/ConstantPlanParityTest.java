// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

import com.legend.testing.Lean;
import com.legend.testbackend.TestBackends;
import com.legend.compiler.element.type.ExprType;
import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.typed.TypedCBoolean;
import com.legend.compiler.spec.typed.TypedCInteger;
import com.legend.compiler.spec.typed.TypedCString;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * THE CONSTANT-PLAN BOUNDARY PIN ({@link LiteralFold}): each ADMITTED
 * literal kind proves fold == SQL-path DIFFERENTIALLY — the same value
 * executed as a bare literal (folds) and wrapped so it must lower to
 * SQL, results compared frame-for-frame. Admitting a kind to the fold
 * is a green differential here, never an argument. The unadmitted
 * kinds are pinned OUT (their DB paths carry coercion rules — lattice
 * promotion, carriers — that folding would duplicate).
 */
class ConstantPlanParityTest {

    private static final String MODEL = """
            ###Pure
            Class t::Marker {}
            ###Relational
            Database t::Db ( Table T_X (id INTEGER PRIMARY KEY) )
            ###Connection
            RelationalDatabaseConnection t::Conn
            {
                store: t::Db;
                type: DuckDB;
                specification: DuckDB { };
                auth: Test;
            }
            ###Runtime
            Runtime t::RT
            {
                mappings: [];
                connections: [ t::Db: [ env: t::Conn ] ];
            }
            """;

    @Test
    void admittedKindsAgreeWithTheSqlPath() throws Exception {
        try (Connection c = TestBackends.platform().open()) {
            // String: bare literal folds; ->toOne() is [1]-identity that
            // MUST lower to SQL — same value, both routes
            Lean.Result folded = Lean.run(MODEL, "|'px'", "t::RT", c);
            Lean.Result viaSql = Lean.run(MODEL, "|'px'->toOne()", "t::RT", c);
            assertEquals(scalar(viaSql), scalar(folded), "String parity");
            assertEquals(scalar(viaSql).getClass(), scalar(folded).getClass(),
                    "String Java carrier parity");
            // Boolean
            Lean.Result fb = Lean.run(MODEL, "|true", "t::RT", c);
            Lean.Result sb = Lean.run(MODEL, "|true->toOne()", "t::RT", c);
            assertEquals(scalar(sb), scalar(fb), "Boolean parity");
            assertEquals(scalar(sb).getClass(), scalar(fb).getClass(),
                    "Boolean Java carrier parity");
        }
    }

    @Test
    void unadmittedKindsNeverFold() {
        // Integer's DB path runs the type lattice (driver-width +
        // promotion + the BigInteger extension) — folding would
        // duplicate that rule; the node must return null here.
        assertNull(LiteralFold.fold(new TypedCInteger(5,
                ExprType.one(Type.Primitive.INTEGER))));
    }

    @Test
    void admittedSetIsPinned() {
        assertEquals(Set.of("String", "Boolean"), LiteralFold.ADMITTED,
                "the constant-plan boundary moved — a new kind needs its"
                        + " differential parity leg in THIS test first");
        assertTrue(LiteralFold.fold(new TypedCString("x",
                ExprType.one(Type.Primitive.STRING)))
                instanceof com.legend.exec.TypedSide.Scalar);
        assertTrue(LiteralFold.fold(new TypedCBoolean(true,
                ExprType.one(Type.Primitive.BOOLEAN)))
                instanceof com.legend.exec.TypedSide.Scalar);
    }

    private static Object scalar(Lean.Result r) {
        assertEquals(com.legend.executionplan.ExecutionPlan.TdsResult.Shape.VALUE, r.shape(), "expected a scalar frame");
        return r.one();
    }
}
