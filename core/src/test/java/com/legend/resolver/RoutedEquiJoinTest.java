// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.resolver;

import com.legend.Compiler;
import com.legend.Execution;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * A navigation between two class hierarchies each mapped over ONE table
 * (one filtered set per subclass, an inheritance operation over them) whose
 * association is mapped for every (source set, target set) pair — the
 * system metamodel's inferredType shape. Every pair reads the same join, so
 * the routed join is ONE equality, never an OR of per-pair equalities (a
 * nested loop the database cannot hash: 105 terms and ~800ms per query for
 * inferredType's 5 x 21 sets).
 */
class RoutedEquiJoinTest {

    private static final String INHERITANCE =
            "meta::pure::router::operations::inheritance_OperationSetImplementation_1__SetImplementation_MANY_()";

    private static String sets(String base, String table, String key, String prop,
            String... idFilter) {
        StringBuilder sb = new StringBuilder("  *x::" + base + ": Operation { " + INHERITANCE + " }\n");
        for (int i = 0; i < idFilter.length; i += 3) {
            sb.append("  x::").append(idFilter[i]).append("[").append(idFilter[i + 1])
                    .append("]: Relational { ~filter [x::DB]").append(idFilter[i + 2])
                    .append(" ~primaryKey([x::DB]").append(table).append(".ID) ~mainTable [x::DB]")
                    .append(table).append(" ").append(prop).append(": [x::DB]").append(table)
                    .append(".").append(key).append(" }\n");
        }
        return sb.toString();
    }

    private static String pairs() {
        StringBuilder sb = new StringBuilder();
        for (String op : List.of("opA", "opB")) {
            for (String ty : List.of("tInt", "tStr", "tBit")) {
                sb.append(sb.isEmpty() ? "" : ",\n").append("      type[").append(op).append(", ")
                        .append(ty).append("]: [x::DB]@OpTy,\n      typeOf[").append(ty).append(", ")
                        .append(op).append("]: [x::DB]@OpTy");
            }
        }
        return sb.toString();
    }

    private static final String MODEL = """
            Class x::Op { id: Integer[1]; }
            Class x::OpA extends x::Op { }
            Class x::OpB extends x::Op { }
            Class x::Ty { name: String[1]; }
            Class x::TyInt extends x::Ty { }
            Class x::TyStr extends x::Ty { }
            Class x::TyBit extends x::Ty { }
            Association x::OpToTy { type: x::Ty[0..1]; typeOf: x::Op[*]; }
            ###Relational
            Database x::DB (
              Table OPS (ID INTEGER PRIMARY KEY, KIND VARCHAR(8), TYPE_ID INTEGER)
              Table TYS (ID INTEGER PRIMARY KEY, KIND VARCHAR(8), NAME VARCHAR(16))
              Join OpTy (OPS.TYPE_ID = TYS.ID)
              Filter IsA (OPS.KIND = 'A')
              Filter IsB (OPS.KIND = 'B')
              Filter IsInt (TYS.KIND = 'I')
              Filter IsStr (TYS.KIND = 'S')
              Filter IsBit (TYS.KIND = 'B')
            )
            ###Mapping
            Mapping x::M (
            """
            + sets("Op", "OPS", "ID", "id", "OpA", "opA", "IsA", "OpB", "opB", "IsB")
            + sets("Ty", "TYS", "NAME", "name", "TyInt", "tInt", "IsInt", "TyStr", "tStr", "IsStr",
                    "TyBit", "tBit", "IsBit")
            + "  x::OpToTy: Relational { AssociationMapping (\n" + pairs() + "\n  ) }\n"
            + """
            )
            ###Connection
            RelationalDatabaseConnection x::DBDuckDB { store: x::DB; type: DuckDB; specification: DuckDB { }; auth: Test; }
            ###Runtime
            Runtime x::RT { mappings: [x::M]; connections: [ x::DB: [ c0: x::DBDuckDB ] ]; }
            """;

    private static final String QUERY =
            "|x::Op.all()->project(~[id: o|$o.id, t: o|$o.type.name])->sort(~id->ascending())";

    @Test
    @DisplayName("every (arm, target set) pair on one join navigates the right rows")
    void rectangleNavigates() throws Exception {
        try (Connection c = DriverManager.getConnection("jdbc:duckdb:")) {
            try (Statement st = c.createStatement()) {
                st.execute("CREATE TABLE OPS (ID INTEGER, KIND VARCHAR, TYPE_ID INTEGER)");
                st.execute("CREATE TABLE TYS (ID INTEGER, KIND VARCHAR, NAME VARCHAR)");
                st.execute("INSERT INTO OPS VALUES (1,'A',10), (2,'B',20), (3,'A',30), (4,'B',NULL)");
                st.execute("INSERT INTO TYS VALUES (10,'I','INT'), (20,'S','VARCHAR'), (30,'B','BIT')");
            }
            var r = Execution.execute(MODEL, QUERY, "x::RT", c);
            assertEquals(List.of("1|INT", "2|VARCHAR", "3|BIT", "4|null"),
                    r.rows().stream().map(row -> row.get(0) + "|" + row.get(1)).toList());
        }
    }

    @Test
    @DisplayName("the routed join is one equality, not an OR per pair")
    void rectangleIsOneEquality() {
        String sql = Compiler.query(Compiler.compileModel(MODEL), QUERY.substring(1)).plan("x::RT").sql();
        assertFalse(sql.contains(" OR "), "a routed join over one shared condition must be"
                + " one equality (hashable), not an OR per (arm, target set) pair:\n" + sql);
    }
}
