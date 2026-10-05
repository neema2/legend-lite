// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.compiler;

import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.test.StorelessRuntime;

import com.legend.Execution;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * D6b frontend-leniency batch pins (docs/STAMP_DISCIPLINE_PROGRAM.md):
 * models the engine rejects at compile must reject HERE too, eagerly and
 * attributed — each of these previously slid through to a lazy crash
 * (StackOverflow on demanded cyclic extends), a silent last-wins
 * (duplicate FQNs / properties / enum values), an unattributed
 * {@code IllegalArgumentException} (inverted bounds), or SQL that only
 * the database refused (ghost join columns, out-of-range dates).
 *
 * <p>The one deliberately KEPT leniency this batch adjudicated: the
 * ElementParser stray-paren skip is bounded ("skip exactly this token")
 * and corpus-witnessed — engine tolerance, not a hole.
 */
class LeniencyD6Test {

    private static String reject(Connection c, String model, String q) {
        Exception e = assertThrows(Exception.class,
                () -> Execution.execute(StorelessRuntime.with(model, DatabaseType.DuckDB), q, StorelessRuntime.RUNTIME, c));
        assertNotNull(e.getMessage());
        return e.getMessage().split("\n")[0];
    }

    private static void expect(Connection c, String model, String q,
            String fragment) {
        String msg = reject(c, model, q);
        assertTrue(msg.contains(fragment),
                "expected rejection containing <" + fragment + ">, got: " + msg);
    }

    @Test
    void frontendLeniencyBatchRejects() throws Exception {
        try (Connection c = DriverManager.getConnection("jdbc:duckdb:")) {
            // inheritance cycle: previously ACCEPTED, StackOverflow at demand
            expect(c, "Class m::A extends m::B {}\nClass m::B extends m::A {}",
                    "{|1}", "Inheritance cycle: m::A -> m::B -> m::A");
            // duplicate stored property: engine "Found duplicated property"
            expect(c, "Class m::A { x: String[1]; x: Integer[1]; }",
                    "{|1}", "Found duplicated property 'x' in class 'm::A'");
            // duplicate element FQN (shared namespace): engine "Duplicated element"
            expect(c, "Class m::A { x: String[1]; }\nClass m::A { y: Integer[1]; }",
                    "{|1}", "Duplicated element 'm::A'");
            // cross-KIND duplicate — one packageable-element namespace
            expect(c, "Class m::A { x: String[1]; }\nEnum m::A { V }",
                    "{|1}", "Duplicated element 'm::A'");
            // duplicate enum value: engine "Found duplicated value"
            expect(c, "Enum m::E { A, A }",
                    "{|1}", "Found duplicated value 'A' in enumeration 'm::E'");
            // inverted multiplicity bounds: was a lazy, unattributed
            // IllegalArgumentException from the Bounded ctor guard
            expect(c, "Class m::A { x: String[2..1]; }",
                    "{|1}", "property 'x' of m::A: invalid multiplicity");
            // ghost join column: was accepted and shipped as failing SQL
            expect(c, """
                    Class m::P { id: Integer[1]; }
                    ###Relational
                    Database s::DB ( Table T (ID INTEGER)
                      Join bad(T.ID = T.GHOST) )
                    ###Mapping
                    Mapping m::M ( *m::P: Relational { ~mainTable [s::DB] T id: T.ID } )
                    ###Runtime
                    Runtime m::RT { mappings: [m::M]; }
                    """,
                    "{|1}",
                    "The column 'GHOST' can't be found in the table 'T'");
            // ghost join TABLE
            expect(c, """
                    Class m::P { id: Integer[1]; }
                    ###Relational
                    Database s::DB ( Table T (ID INTEGER)
                      Join bad(T.ID = NOPE.ID) )
                    ###Mapping
                    Mapping m::M ( *m::P: Relational { ~mainTable [s::DB] T id: T.ID } )
                    ###Runtime
                    Runtime m::RT { mappings: [m::M]; }
                    """,
                    "{|1}",
                    "The table 'NOPE' can't be found in the database 's::DB'");
            // out-of-range date components: LEGEND_LITE validates at PARSE
            // (was: DuckDB "Conversion Error: date field value out of
            // range"); LEGEND_ENGINE stays deferred — oracle byte-parity.
            expect(c, "", "{|%2020-99-99}",
                    "invalid date literal '%2020-99-99'");
        }
    }

    /** The checks are integrity-pass rejections, not parse rejections —
     * a VALID model with the same shapes still compiles and runs. */
    @Test
    void validNeighborsStillCompile() throws Exception {
        try (Connection c = DriverManager.getConnection("jdbc:duckdb:")) {
            Execution.execute(StorelessRuntime.with("""
                    Class m::B { y: Integer[1]; }
                    Class m::A extends m::B { x: String[0..1]; }
                    Enum m::E { A, B }
                    """, DatabaseType.DuckDB), "{|1}", StorelessRuntime.RUNTIME, c);
        }
    }
}
