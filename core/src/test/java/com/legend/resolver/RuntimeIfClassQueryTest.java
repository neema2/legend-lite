// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.resolver;

import com.legend.Compiler;
import com.legend.exec.ExecutionResult;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * {@code if(c, |A, |B)} over CLASS queries with a condition only the data
 * decides: the union of the branches, each guarded by the condition
 * ({@code ChainDispatch.ifAsUnion}) — pure evaluates exactly one branch, so
 * exactly one guard holds. The condition here is itself an existence test
 * of a class query ({@code isEmpty}: NOT EXISTS). The engine's own
 * {@code Mapping.resolveStore} is this shape; so is any "this one, else
 * the default" query.
 */
class RuntimeIfClassQueryTest {

    private static final String MODEL = """
            Class x::Firm { name: String[1]; }
            ###Relational
            Database x::DB ( Table FIRM (ID INTEGER PRIMARY KEY, NAME VARCHAR(32)) )
            ###Mapping
            Mapping x::M ( *x::Firm: Relational { ~mainTable [x::DB] FIRM name: [x::DB] FIRM.NAME } )
            ###Connection
            RelationalDatabaseConnection x::DBDuckDB { store: x::DB; type: DuckDB; specification: DuckDB { }; auth: Test; }
            ###Runtime
            Runtime x::RT { mappings: [x::M]; connections: [ x::DB: [ c0: x::DBDuckDB ] ]; }
            """;

    private static String pick(String wanted) {
        return "|let hit = x::Firm.all()->filter(f|$f.name == '" + wanted + "');"
                + " if($hit->isEmpty(), |x::Firm.all()->filter(f|$f.name == 'Default')->toOne(),"
                + " |$hit->toOne()).name;";
    }

    private static List<Object> run(String query) throws Exception {
        try (Connection c = DriverManager.getConnection("jdbc:duckdb:")) {
            try (Statement st = c.createStatement()) {
                st.execute("CREATE TABLE FIRM (ID INTEGER, NAME VARCHAR)");
                st.execute("INSERT INTO FIRM VALUES (1, 'Acme'), (2, 'Default')");
            }
            ExecutionResult r = Compiler.execute(MODEL, query, "x::RT", c);
            if (r instanceof ExecutionResult.Collection coll) {
                return coll.values();
            }
            Object v = ((ExecutionResult.Scalar) r).value();
            return v == null ? List.of() : List.of(v);
        }
    }

    @Test
    @DisplayName("the condition holds: the then-branch's instance")
    void conditionHolds() throws Exception {
        assertEquals(List.of("Default"), run(pick("Nobody")));
    }

    @Test
    @DisplayName("the condition fails: the else-branch's instance, the then-branch empty")
    void conditionFails() throws Exception {
        assertEquals(List.of("Acme"), run(pick("Acme")));
    }
}
