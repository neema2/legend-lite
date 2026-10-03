// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

import com.legend.compiler.element.type.ExprType;
import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.typed.TypedCBoolean;
import com.legend.compiler.spec.typed.TypedCInteger;
import com.legend.compiler.spec.typed.TypedCString;
import com.legend.exec.ExecutionResult;
import com.legend.server.QueryService;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
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
        try (Connection c = DriverManager.getConnection("jdbc:duckdb:")) {
            QueryService qs = new QueryService();
            // String: bare literal folds; ->toOne() is [1]-identity that
            // MUST lower to SQL — same value, both routes
            ExecutionResult folded = qs.execute(MODEL, "|'px'", "t::RT", c);
            ExecutionResult viaSql = qs.execute(MODEL, "|'px'->toOne()", "t::RT", c);
            assertEquals(scalar(viaSql), scalar(folded), "String parity");
            assertEquals(scalar(viaSql).getClass(), scalar(folded).getClass(),
                    "String Java carrier parity");
            // Boolean
            ExecutionResult fb = qs.execute(MODEL, "|true", "t::RT", c);
            ExecutionResult sb = qs.execute(MODEL, "|true->toOne()", "t::RT", c);
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
                instanceof ExecutionResult.Scalar);
        assertTrue(LiteralFold.fold(new TypedCBoolean(true,
                ExprType.one(Type.Primitive.BOOLEAN)))
                instanceof ExecutionResult.Scalar);
    }

    private static Object scalar(ExecutionResult r) {
        assertTrue(r instanceof ExecutionResult.Scalar,
                "expected a scalar frame, got " + r.getClass().getSimpleName());
        return ((ExecutionResult.Scalar) r).value();
    }
}
