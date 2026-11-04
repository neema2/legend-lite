// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import com.legend.Compiler;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A relation the statement reads more than once is one common table expression at its head ({@code
 * lowering.SharedRelations}, 2026-11-04), but where H2 would lose a bound value inside it ({@link
 * H2LostCteParameterTest}) it is written back at each read ({@link SharedRelationsInPlace}); one that binds nothing
 * stays shared on H2 too.
 */
class SharedRelationsInPlaceTest {

    /** A let read twice: its relation joined to itself. */
    private static String model(String type) {
        return """
                ###Relational
                Database s::DB ( Table T ( ID INTEGER PRIMARY KEY, AGE INTEGER ) )
                ###Connection
                RelationalDatabaseConnection s::Conn
                {
                    store: s::DB;
                    type: %s;
                    specification: LocalH2 { };
                    auth: DefaultH2;
                }
                ###Runtime
                Runtime s::RT { mappings: []; connections: [ s::DB: [ c1: s::Conn ] ]; }
                ###Pure
                function s::selfJoined(t: Relation<(ID:Integer, AGE:Integer)>[1]): Relation<(ID:Integer, AGE:Integer)>[1]
                {
                  let base = $t->filter(r|$r.AGE > 0);
                  $base->join($base->rename(~ID, ~ID2)->rename(~AGE, ~AGE2), meta::pure::functions::relation::JoinKind.INNER,
                              {a, b|$a.ID == $b.ID2})->select(~[ID, AGE]);
                }
                """.formatted(type);
    }

    private static String statement(String type, String query) {
        return Compiler.query(Compiler.compileModel(model(type)), query).executionPlan("s::RT").answerSql()
                .statement();
    }

    private static final String WITH_PARAMETER =
            "{min: Integer[1] | s::selfJoined(#>{s::DB.T}#->select(~[ID, AGE])->filter(r|$r.AGE > $min))}";

    @Test
    void aSharedRelationThatBindsAValueIsWrittenAtEachReadOnH2() {
        String h2 = statement("H2", WITH_PARAMETER);
        assertFalse(h2.contains("__let"), h2);
        assertEquals(2, h2.split("\\?", -1).length - 1, h2);
        // each copy's aliases its own
        assertTrue(h2.contains("\"s0_t0\"") && h2.contains("\"s1_t0\""), h2);
    }

    @Test
    void elsewhereItIsWrittenOnce() {
        String duckDb = statement("DuckDB", WITH_PARAMETER);
        assertTrue(duckDb.startsWith("WITH __let0 AS ("), duckDb);
        assertEquals(1, duckDb.split("\\?", -1).length - 1, duckDb);
    }

    @Test
    void aSharedRelationThatBindsNothingStaysSharedOnH2() {
        String h2 = statement("H2", "|s::selfJoined(#>{s::DB.T}#->select(~[ID, AGE])->filter(r|$r.AGE > 1))");
        assertTrue(h2.contains("__let0"), h2);
    }
}
