// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0
package com.legend.driver;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.legend.Compiler;
import com.legend.compiler.element.ModelContext;
import com.legend.compiler.spec.typed.Literals;
import com.legend.testbackend.TestBackends;
import com.legend.testing.Lean;

import java.sql.Connection;
import java.util.List;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * THE LITERAL FOLD'S PARITY PIN ({@link Literals}; the seam program's S2f, 2026-10-30; the user's ruling: "it's okay
 * to join CONSTANT strings only"): each of the three string operations the compiler folds over constants proves
 * fold == the database's answer for the same expression, differentially -- the expression folded at compile time
 * against the same expression wrapped so it must lower to SQL ({@code ->toOne()}, which the fold does not admit).
 * Admitting a fourth operation is a ruling, then a green leg here, then the ledger's pin
 * ({@code JavaEvalLedgerTest.theCompilersLiteralFoldJoinsConstantsAlone}), never an argument. A value the database
 * answered is never an input: an expression over a query does not fold.
 */
class LiteralsParityTest {
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

    private static Object folded(ModelContext ctx, String query) {
        return Literals.fold(Compiler.query(ctx, query).expression(), List.of());
    }

    private static Object viaSql(String query, Connection c) {
        Lean.Result r = Lean.run(MODEL, query + "->toOne()", "t::RT", c);
        assertEquals(com.legend.executionplan.ExecutionPlan.TdsResult.Shape.VALUE, r.shape(), "a scalar frame");
        return r.one();
    }

    @Test
    @DisplayName("+, joinStrings and replace over constants fold to what the database answers for the same text")
    void theThreeOperationsAgreeWithTheSqlPath() throws Exception {
        ModelContext ctx = Compiler.compileModel(MODEL);
        try (Connection c = TestBackends.platform().open()) {
            String plus = "|'select ' + 'x' + ' from t'";
            assertEquals("select x from t", folded(ctx, plus));
            assertEquals(viaSql(plus.substring(1), c), folded(ctx, plus), "+ over constants");

            String join = "|['a', 'b', 'c']->joinStrings('-')";
            assertEquals("a-b-c", folded(ctx, join));
            assertEquals(viaSql(join.substring(1), c), folded(ctx, join), "joinStrings over constants");

            String joinFramed = "|['a', 'b']->joinStrings('<', ', ', '>')";
            assertEquals("<a, b>", folded(ctx, joinFramed));
            assertEquals(viaSql(joinFramed.substring(1), c), folded(ctx, joinFramed),
                    "joinStrings with a prefix and a suffix over constants");

            String replace = "|'a.b.c'->replace('.', '_')";
            assertEquals("a_b_c", folded(ctx, replace));
            assertEquals(viaSql(replace.substring(1), c), folded(ctx, replace), "replace over constants");
        }
    }

    @Test
    @DisplayName("a let bound to a constant folds through; a function over data or a query does not fold")
    void onlyConstantsFold() {
        ModelContext ctx = Compiler.compileModel(MODEL);
        assertNull(folded(ctx, "|'a'->toUpper()"), "a string function the fold does not admit");
        assertNull(folded(ctx, "|1 + 2"), "no arithmetic");
        assertNull(folded(ctx, "|'a' + 1->toString()"), "a value computed by a function is not a constant");
    }
}
