// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.integration;

import com.legend.database.Databases;
import com.legend.sql.DialectCapability;
import com.legend.testing.Lean;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end execution tests for {@code meta::legend::lite::erf} across the active test lane.
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
            assertEquals(tc[1], actual, 1e-15, "erf(" + tc[0] + ")");
        }
        // Integer, Decimal, and nested depth-2 / depth-3 erf
        assertEquals(0.8427007929497149,
                ((Number) executeRelation("|erf(1)").rows().get(0).get(0)).doubleValue(), 1e-15);
        assertEquals(0.5204998778130465,
                ((Number) executeRelation("|erf(0.5d)").rows().get(0).get(0)).doubleValue(), 1e-15);
        try {
            double d2 = ((Number) executeRelation("|erf(erf(0.5))").rows().get(0).get(0)).doubleValue();
            double d3 = ((Number) executeRelation("|erf(erf(erf(0.5)))").rows().get(0).get(0)).doubleValue();
            assertEquals(0.5383289318919441, d2, 1e-15);
            assertEquals(0.5535293109128794, d3, 1e-15);
        } catch (DialectCapability refused) {
            assertTrue(backend().type() != Databases.PLATFORM,
                    "Platform engine must evaluate nested erf directly");
            assertTrue(refused.getMessage().contains("subquery column")
                    && refused.getMessage().contains("distinct()"), refused.getMessage());
        }
    }

    @Test
    void testErfOverTableColumnsAllRegionsNullNanInfAndCompoundRemediation() throws Exception {
        // 1) Real SqlExpr.Column coverage across NULL + all 4 regions via declared CSV setup (zero raw JDBC writes)
        Double[] xs = {null, -30.0, -4.5, -2.0, -0.5, -0.3, 0.0, 0.3, 0.46875, 0.5, 2.0, 4.0, 4.5, 27.0};
        StringBuilder csv = new StringBuilder("default\\nT_ERF_X\\nID,X\\n");
        for (int i = 0; i < xs.length; i++) {
            csv.append(i).append(',').append(xs[i] == null ? "" : xs[i]).append("\\n");
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
                   store: erf::Db;
                   type: %s;
                   specification: LocalH2
                   {
                      testDataSetupCSV: '%s';
                   };
                   auth: DefaultH2;
                }
                ###Runtime
                Runtime erf::Rt
                {
                   mappings: [ ];
                   connections: [ erf::Db: [ environment: erf::Conn ] ];
                }
                """.formatted(backend().type(), csv);

        try (var colConn = backend().open()) {
            var colRows = Lean.run(colModel,
                    "#>{erf::Db.T_ERF_X}#->extend(~[e: r | if($r.X->isEmpty(), |[]->cast(@Float), |$r.X->toOne()->erf())])->select(~[ID, X, e])->sort(ascending(~ID))",
                    "erf::Rt", colConn).rows();
            assertEquals(xs.length, colRows.size());
            assertNull(colRows.get(0).get(2), "NULL column input must preserve NULL");
            for (int i = 1; i < xs.length; i++) {
                double x = xs[i];
                double expected = x <= -27.0 ? -1.0 : (x >= 27.0 ? 1.0 : erfRef(x));
                double actual = ((Number) colRows.get(i).get(2)).doubleValue();
                assertEquals(expected, actual, 1e-15, "Column erf(" + x + ")");
            }
        }

        // 2) IEEE +/-Infinity and NaN over materialized subquery columns
        var infRows = executeRelation(
                "|#TDS{id, s\n1, Infinity\n2, -Infinity}#->extend(~[x: r | $r.s->parseFloat()])->distinct()->extend(~[e: r | erf($r.x)])->select(~[id, e])->sort(ascending(~id))")
                .rows();
        assertEquals(2, infRows.size());
        assertEquals(1.0, ((Number) infRows.get(0).get(1)).doubleValue(), 0.0, "erf(+Inf)");
        assertEquals(-1.0, ((Number) infRows.get(1).get(1)).doubleValue(), 0.0, "erf(-Inf)");

        try {
            var nanRows = executeRelation(
                    "|#TDS{id, s\n1, NaN}#->extend(~[x: r | $r.s->parseFloat()])->distinct()->extend(~[e: r | erf($r.x)])->extend(~[isNan: r | ($r.e != $r.e) || (($r.e > 0.0) && ((-$r.e) > 0.0))])->select(~[isNan])")
                    .rows();
            assertEquals(Boolean.TRUE, nanRows.get(0).get(0), "erf(NaN) must preserve NaN");
        } catch (RuntimeException refused) {
            // Databases without IEEE NaN storage refuse 'NaN'->parseFloat() with VALUE_NOT_HELD (S138)
            assertTrue(backend().type() != Databases.PLATFORM, "Platform engine must preserve NaN through erf()");
            assertTrue(String.valueOf(refused.getMessage()).contains("NaN"), String.valueOf(refused.getMessage()));
        }

        // 3) Compound argument policy + verified subquery-materialization remediation across all lanes
        var ages = executeRelation("|#>{store::PersonDatabase.T_PERSON}#->select(~[ID, AGE_VAL])->sort(ascending(~ID))").rows();
        String compoundQuery = "|#>{store::PersonDatabase.T_PERSON}#->extend(~[e: r | erf(($r.AGE_VAL - 45) / 10.0)])->select(~[ID, e])->sort(ascending(~ID))";
        String fusedExtendQuery = "|#>{store::PersonDatabase.T_PERSON}#->extend(~[z: r | ($r.AGE_VAL - 45) / 10.0])->extend(~[e: r | erf($r.z)])->select(~[ID, e])->sort(ascending(~ID))";
        String materializedQuery = "|#>{store::PersonDatabase.T_PERSON}#->select(~[ID, AGE_VAL])->extend(~[z: r | ($r.AGE_VAL - 45) / 10.0])->distinct()->extend(~[e: r | erf($r.z)])->select(~[ID, e])->sort(ascending(~ID))";

        // The documented remediation (extend() before key-preserving distinct()) executes on EVERY lane
        var matRows = executeRelation(materializedQuery).rows();
        assertEquals(ages.size(), matRows.size());
        for (int i = 0; i < ages.size(); i++) {
            double age = ((Number) ages.get(i).get(1)).doubleValue();
            double actual = ((Number) matRows.get(i).get(1)).doubleValue();
            assertEquals(erfRef((age - 45.0) / 10.0), actual, 1e-15);
        }

        try {
            var compoundRows = executeRelation(compoundQuery).rows();
            assertEquals(ages.size(), compoundRows.size());
            for (int i = 0; i < ages.size(); i++) {
                double age = ((Number) ages.get(i).get(1)).doubleValue();
                double actual = ((Number) compoundRows.get(i).get(1)).doubleValue();
                assertEquals(erfRef((age - 45.0) / 10.0), actual, 1e-15);
            }
        } catch (DialectCapability ex1) {
            assertTrue(backend().type() != Databases.PLATFORM, "Platform engine must evaluate compound erf()");
            assertTrue(ex1.getMessage().contains("subquery column") && ex1.getMessage().contains("distinct()"), ex1.getMessage());
            DialectCapability ex2 = assertThrows(DialectCapability.class, () -> executeRelation(fusedExtendQuery));
            assertTrue(ex2.getMessage().contains("subquery column") && ex2.getMessage().contains("distinct()"), ex2.getMessage());
        }
    }

    private static double erfRef(double x) {
        int n = 4000;
        double h = x / n;
        double sum = 1.0 + Math.exp(-x * x);
        double comp = 0.0;
        for (int i = 1; i < n; i++) {
            double t = i * h;
            double term = (i % 2 == 0 ? 2.0 : 4.0) * Math.exp(-t * t) - comp;
            double next = sum + term;
            comp = (next - sum) - term;
            sum = next;
        }
        return (2.0 / Math.sqrt(Math.PI)) * (h / 3.0) * sum;
    }
}
