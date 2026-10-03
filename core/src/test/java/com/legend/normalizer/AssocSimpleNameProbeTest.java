// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.normalizer;

import com.legend.Compiler;
import com.legend.exec.ExecutionResult;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Class-mapping-header AssociationMapping with a SIMPLE name (corpus
 * inheritence::assocMapping: {@code Driver : Relational {
 * AssociationMapping (...) }} under an import of the model package) —
 * the binding must key by the association's FQN through the mapping's
 * import scope.
 */
class AssocSimpleNameProbeTest {

    private static final String MODEL = """
            Class a::m::Person { name: String[1]; }
            Class a::m::RoadVehicle { rid: Integer[1]; }
            Class a::m::Car extends a::m::RoadVehicle { plate: String[1]; }
            Class a::m::Bicycle extends a::m::RoadVehicle { brand: String[1]; }
            Association a::m::Driver { person: a::m::Person[0..1]; roadVehicles: a::m::RoadVehicle[*]; }
            ###Relational
            Database a::DB (
              Table PT (ID INTEGER PRIMARY KEY, NAME VARCHAR(200))
              Table CT (ID INTEGER PRIMARY KEY, PLATE VARCHAR(200), OWNER INTEGER)
              Table BT (ID INTEGER PRIMARY KEY, BRAND VARCHAR(200), OWNER INTEGER)
              Join PersonCar (PT.ID = CT.OWNER)
              Join PersonBicycle (PT.ID = BT.OWNER)
            )
            ###Mapping
            import a::m::*;
            Mapping a::Child (
              Person[per1] : Relational { ~mainTable [a::DB] PT
                name: PT.NAME }
              Car[map1] : Relational { ~mainTable [a::DB] CT
                rid: CT.ID,
                plate: CT.PLATE }
              Bicycle[map2] : Relational { ~mainTable [a::DB] BT
                rid: BT.ID,
                brand: BT.BRAND }
            )
            ###Mapping
            import a::m::*;
            Mapping a::M (
              include a::Child

              RoadVehicle : Operation
              {
                meta::pure::router::operations::inheritance_OperationSetImplementation_1__SetImplementation_MANY_()
              }

              Driver : Relational
              {
                AssociationMapping
                (
                  roadVehicles[per1, map1] : [a::DB] @PersonCar,
                  person[map1, per1] : [a::DB] @PersonCar,
                  roadVehicles[per1, map2] : [a::DB] @PersonBicycle,
                  person[map2, per1] : [a::DB] @PersonBicycle
                )
              }
            )
            ###Connection
            RelationalDatabaseConnection a::DBDuckDB { store: a::DB; type: DuckDB; specification: DuckDB { }; auth: Test; }
            ###Runtime
            Runtime a::RT { mappings: [a::M]; connections: [ a::DB: [ c0: a::DBDuckDB ] ]; }
            """;

    @Test
    @DisplayName("simple-name AssociationMapping binds by FQN (same-package fallback)")
    void simpleNameAssociationMapping() throws SQLException {
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:")) {
            try (Statement st = conn.createStatement()) {
                st.execute("CREATE TABLE PT (ID INTEGER, NAME VARCHAR(200))");
                st.execute("CREATE TABLE CT (ID INTEGER, PLATE VARCHAR(200), OWNER INTEGER)");
                st.execute("CREATE TABLE BT (ID INTEGER, BRAND VARCHAR(200), OWNER INTEGER)");
                st.execute("INSERT INTO PT VALUES (1,'ann'),(2,'bob')");
                st.execute("INSERT INTO CT VALUES (10,'X1',1)");
            }
            ExecutionResult r = Compiler.execute(MODEL,
                    "a::m::Person.all()->filter(p|$p.roadVehicles->isNotEmpty())"
                    + "->project([p|$p.name], ['n'])->from(a::M, a::RT)",
                    "a::RT", conn);
            assertTrue(String.valueOf(r).contains("ann"), String.valueOf(r));
        }
    }
}
