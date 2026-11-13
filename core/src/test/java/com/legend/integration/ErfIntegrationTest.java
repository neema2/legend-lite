// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.integration;

import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.testing.Lean;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end execution tests for {@code meta::legend::lite::erf} across DuckDB, H2, and SQLite.
 */
public class ErfIntegrationTest extends AbstractDatabaseTest {

    @BeforeEach
    void setUp() throws SQLException {
        connection = backend().open();
        setupDatabase();
    }

    @AfterEach
    void tearDown() throws SQLException {
        if (connection != null) {
            connection.close();
        }
    }

    @Test
    void testErfRegionCoverageAndExactBoundaries() throws SQLException {
        double[][] cases = {
                {0.0, 0.0},
                {0.3, 0.3286267594591274},
                {0.46874, 0.49260441524411136},
                {0.46875, 0.49261347321793797},
                {0.46876, 0.4926225311068465},
                {-0.46874, -0.49260441524411136},
                {-0.46875, -0.49261347321793797},
                {-0.46876, -0.4926225311068465},
                {0.5, 0.5204998778130465},
                {-0.5, -0.5204998778130465},
                {3.999, 0.9999999844552505},
                {4.0, 0.9999999845827421},
                {4.001, 0.9999999847092178},
                {-3.999, -0.9999999844552505},
                {-4.0, -0.9999999845827421},
                {-4.001, -0.9999999847092178},
                {4.5, 0.9999999998033839},
                {5.0, 0.9999999999984626},
                {-5.0, -0.9999999999984626},
                {26.9, 1.0},
                {27.0, 1.0},
                {-30.0, -1.0},
        };
        for (double[] tc : cases) {
            double actual = ((Number) executeRelation("|erf(" + tc[0] + ")").rows().get(0).get(0)).doubleValue();
            assertEquals(tc[1], actual, 1e-14, "erf(" + tc[0] + ")");
        }
        // Integer, Decimal, and nested erf
        assertEquals(0.8427007929497149,
                ((Number) executeRelation("|erf(1)").rows().get(0).get(0)).doubleValue(), 1e-14);
        assertEquals(0.5204998778130465,
                ((Number) executeRelation("|erf(0.5d)").rows().get(0).get(0)).doubleValue(), 1e-14);
        if (backend().type() == DatabaseType.H2) {
            assertThrows(Exception.class, () -> executeRelation("|erf(erf(0.5))"));
        } else {
            assertEquals(0.538328931891944,
                    ((Number) executeRelation("|erf(erf(0.5))").rows().get(0).get(0)).doubleValue(), 1e-14);
        }
    }

    @Test
    void testErfOverTableColumnsAllRegionsNullAndCompoundRemediation() throws Exception {
        // 1) Real SqlExpr.Column coverage across NULL + all 4 Cody regions (|x| <= 0.46875, <= 4.0, < 27.0, >= 27.0)
        Double[] xs = {null, -30.0, -4.5, -2.0, -0.5, -0.3, 0.0, 0.3, 0.46875, 0.5, 2.0, 4.0, 4.5, 27.0};
        try (Statement st = connection.createStatement()) {
            st.execute("CREATE TABLE T_ERF_X (ID INT, X DOUBLE PRECISION)");
            for (int i = 0; i < xs.length; i++) {
                st.execute("INSERT INTO T_ERF_X VALUES (" + i + ", " + (xs[i] == null ? "NULL" : xs[i]) + ")");
            }
        }
        String colModel = """
                ###Relational
                Database erf::Db
                (
                   Table T_ERF_X ( ID INT PRIMARY KEY, X FLOAT )
                )
                ###Connection
                RelationalDatabaseConnection erf::Conn
                {
                   %s
                }
                ###Runtime
                Runtime erf::Rt
                {
                   mappings: [ ];
                   connections: [ erf::Db: [ environment: erf::Conn ] ];
                }
                """.formatted(backend().connectionBody());
        var colRows = Lean.run(colModel,
                "#>{erf::Db.T_ERF_X}#->extend(~[e: r | if($r.X->isEmpty(), |[]->cast(@Float), |$r.X->toOne()->erf())])->select(~[ID, X, e])->sort(ascending(~ID))",
                "erf::Rt", connection).rows();
        assertEquals(xs.length, colRows.size());
        assertNull(colRows.get(0).get(2), "NULL column input must preserve NULL");
        for (int i = 1; i < xs.length; i++) {
            double x = xs[i];
            double expected = x <= -27.0 ? -1.0 : (x >= 27.0 ? 1.0 : erfRef(x));
            double actual = ((Number) colRows.get(i).get(2)).doubleValue();
            assertEquals(expected, actual, 1e-13, "Column erf(" + x + ")");
        }

        // 2) Compound argument policy + verified subquery-materialization remediation on H2
        var ages = executeRelation("|#>{store::PersonDatabase.T_PERSON}#->select(~[ID, AGE_VAL])->sort(ascending(~ID))").rows();
        String compoundQuery = "|#>{store::PersonDatabase.T_PERSON}#->extend(~[e: r | erf(($r.AGE_VAL - 45) / 10.0)])->select(~[ID, e])->sort(ascending(~ID))";
        String fusedExtendQuery = "|#>{store::PersonDatabase.T_PERSON}#->extend(~[z: r | ($r.AGE_VAL - 45) / 10.0])->extend(~[e: r | erf($r.z)])->select(~[ID, e])->sort(ascending(~ID))";
        String materializedQuery = "|#>{store::PersonDatabase.T_PERSON}#->select(~[ID, AGE_VAL])->extend(~[z: r | ($r.AGE_VAL - 45) / 10.0])->distinct()->extend(~[e: r | erf($r.z)])->select(~[ID, e])->sort(ascending(~ID))";

        if (backend().type() == DatabaseType.H2) {
            Exception ex1 = assertThrows(Exception.class, () -> executeRelation(compoundQuery));
            assertTrue(ex1.getMessage().contains("subquery column") && ex1.getMessage().contains("distinct()"), ex1.getMessage());
            Exception ex2 = assertThrows(Exception.class, () -> executeRelation(fusedExtendQuery));
            assertTrue(ex2.getMessage().contains("subquery column") && ex2.getMessage().contains("distinct()"), ex2.getMessage());

            // The documented remediation (extend() before distinct()) executes on H2 and matches erfRef
            var matRows = executeRelation(materializedQuery).rows();
            assertEquals(ages.size(), matRows.size());
            for (int i = 0; i < ages.size(); i++) {
                double age = ((Number) ages.get(i).get(1)).doubleValue();
                double actual = ((Number) matRows.get(i).get(1)).doubleValue();
                assertEquals(erfRef((age - 45.0) / 10.0), actual, 1e-14);
            }
        } else {
            var compoundRows = executeRelation(compoundQuery).rows();
            assertEquals(ages.size(), compoundRows.size());
            for (int i = 0; i < ages.size(); i++) {
                double age = ((Number) ages.get(i).get(1)).doubleValue();
                double actual = ((Number) compoundRows.get(i).get(1)).doubleValue();
                assertEquals(erfRef((age - 45.0) / 10.0), actual, 1e-14);
            }
        }
    }

    private static double erfRef(double x) {
        int n = 20000;
        double h = x / n;
        double sum = 1.0 + Math.exp(-x * x);
        for (int i = 1; i < n; i++) {
            double t = i * h;
            sum += (i % 2 == 0 ? 2.0 : 4.0) * Math.exp(-t * t);
        }
        return (2.0 / Math.sqrt(Math.PI)) * (h / 3.0) * sum;
    }
}
