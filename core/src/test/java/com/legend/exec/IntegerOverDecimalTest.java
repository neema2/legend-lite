// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.legend.testbackend.TestBackends;
import com.legend.testing.Lean;

import java.sql.Connection;
import java.util.List;

import org.junit.jupiter.api.Test;

/**
 * AN INTEGER OVER A DECIMAL SLOT (the PR #118 follow-up, 2026-11-19, Q8): an Integer property mapped to a DECIMAL(10,0)
 * column is the Integer the column holds, on each database the platform runs in-process. A driver hands its cell as
 * the decimal it is (a {@code BigDecimal}); the decode reads it as the Integer's carrier, its exact integral, as an
 * integral slot's scale-0 decimal is read (JdbcDecode; the decode's own case in JdbcDecodeDuckDbTest). A cell with a
 * fraction under an Integer is refused by name, never truncated (AGENTS.md invariant 4). Until then the product's batch
 * refused the column's every cell ("a cell BigDecimal 5 is not its type's carrier").
 */
class IntegerOverDecimalTest {

    private static final String MODEL = """
            ###Pure
            Class p::R { id: Integer[1]; n: Integer[1]; }
            Class p::S { id: Integer[1]; f: Integer[1]; }
            ###Relational
            Database p::DB ( Table V (ID INTEGER PRIMARY KEY, N DECIMAL(10,0))
                Table W (ID INTEGER PRIMARY KEY, F DECIMAL(10,2)) )
            ###Mapping
            Mapping p::M ( p::R: Relational { ~mainTable [p::DB] V id: V.ID, n: V.N }
                p::S: Relational { ~mainTable [p::DB] W id: W.ID, f: W.F } )
            ###Connection
            RelationalDatabaseConnection p::C { store: p::DB; type: %s;
                specification: LocalH2 {
                    testDataSetupCSV: 'default\\nV\\nID,N\\n1,5\\n-----\\ndefault\\nW\\nID,F\\n1,5.50\\n2,5.00\\n3,-0.00\\n'; };
                auth: DefaultH2; }
            ###Runtime
            Runtime p::RT { mappings: [p::M]; connections: [ p::DB: [ c0: p::C ] ]; }
            """;

    /** The product's door: a relation of the column, and the property read alone, each the Integer 5, on each database
     *  the platform runs in-process; the fraction refused by name. */
    @Test
    void theProductAnswersTheInteger() throws Exception {
        for (var backend : TestBackends.inProcess()) {
            String model = MODEL.formatted(backend.type());
            try (Connection c = backend.open()) {
                Lean.Result relation = Lean.established(model, "|p::R.all()->project(~[n: x|$x.n])", "p::RT", c);
                assertEquals("Integer", relation.columns().get(0).type(), backend.type() + ": " + relation.text());
                assertEquals(List.of(List.of(5L)), relation.rows(), backend.type() + ": " + relation.text());
                assertEquals("[{\"n\":5}]", relation.text().replace(" ", "").replace("\n", ""), backend.type().toString());
            }
            try (Connection c = backend.open()) {
                Lean.Result one = Lean.established(model, "|p::R.all()->filter(x | $x.id == 1)->toOne().n", "p::RT", c);
                assertEquals(List.of(List.of(5L)), one.rows(), backend.type() + ": " + one.text());
            }
            // a whole value at a scale above zero is its Integer (the review of PR #121, 2026-11-19, B2)
            for (var whole : List.of(List.of(2, 5L), List.of(3, 0L))) {
                try (Connection c = backend.open()) {
                    Lean.Result one = Lean.established(model, "|p::S.all()->filter(x | $x.id == " + whole.get(0)
                            + ")->toOne().f", "p::RT", c);
                    assertEquals(List.of(List.of(whole.get(1))), one.rows(), backend.type() + ": " + one.text());
                }
            }
            try (Connection c = backend.open()) {
                Throwable refused = assertThrows(Throwable.class,
                        () -> Lean.established(model, "|p::S.all()->project(~[f: x|$x.f])", "p::RT", c));
                String words = String.valueOf(refused.getMessage());
                assertTrue(words.contains("column 'f'") && words.contains("declared Integer") && words.contains("5.5"),
                        backend.type() + ": " + refused);
            }
        }
    }
}
