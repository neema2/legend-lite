// SPDX-License-Identifier: Apache-2.0

package com.legend.planner;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import com.legend.Compiler;
import com.legend.compiler.element.ModelContext;
import com.legend.executionplan.ExecutionPlan;
import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.testbackend.TestBackends;
import com.legend.testcases.PlanCases;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * A RESULT'S COLUMNS ARE NAMED BY ITS QUERY, NEVER BY HOW THE DATABASE STORES AN IDENTIFIER'S CASE (2026-11-12, the
 * total wall's B: the orchestrator's ruling (a) on the demo's start). A model declaring its columns in lower case,
 * unquoted, whose table lite's own DDL makes -- the plan's load, from its connection's test data -- has them stored
 * folded UPPER on DuckDB and H2 (DuckDb.ddlIdentifier, the standard's identity). Every statement the plan answers
 * with labels each column with its Pure name (AnsiSqlRenderer#labeled), as upstream labels every projected column
 * ({@code select "trades_0".region as "region"}: upstream-api/e9-groupby-sort.json). That covers a bare table read, a
 * projection, a grouping, the distinct read a dynamic pivot's keys are, and the dynamic pivot itself. Until then an
 * unlabeled column answered its stored spelling ({@code YEAR} for {@code year}), and a host that types rows by name
 * refused them: the demo's start pivoting on year, in the tab.
 */
class ResultLabelsTest {

    /** The table declared lower case and unquoted; its rows the connection's test data, made by the plan's load. */
    private static String model(DatabaseType type) {
        return """
                ###Relational
                Database s::DB ( Table T ( region VARCHAR(8), year INTEGER, qty INTEGER ) )
                ###Connection
                RelationalDatabaseConnection s::Conn
                {
                    store: s::DB;
                    type: %s;
                    specification: LocalH2 { testDataSetupCSV: 'default\\nT\\nregion,year,qty\\nEMEA,2021,1\\nAMER,2021,2\\nEMEA,2022,3\\n'; };
                    auth: DefaultH2;
                }
                ###Runtime
                Runtime s::RT { mappings: [ ]; connections: [ s::DB: [ c1: s::Conn ] ]; }
                """.formatted(type.name());
    }

    private static final List<String> QUERIES = List.of(
            "|#>{s::DB.T}#",
            "|#>{s::DB.T}#->select(~[region, qty])",
            "|#>{s::DB.T}#->groupBy(~region, ~total:x|$x.qty:y|$y->plus())",
            // a dynamic pivot's keys, as DataCube reads them (query.ts pivotValuesLambda)
            "|#>{s::DB.T}#->select(~year)->distinct()->sort(~year->ascending())",
            // and the pivot over them, its columns named by its cast, as DataCube then plans it (the demo's start)
            "|#>{s::DB.T}#->pivot(~[year], ~['n' : x | $x.qty : y | $y->plus()])->cast(@meta::pure::metamodel::relation"
                    + "::Relation<(region:String, '2021__|__n':Integer, '2022__|__n':Integer)>)");

    private static final List<DatabaseType> FOLDING = List.of(DatabaseType.DuckDB, DatabaseType.H2);

    @Test
    void eachColumnIsLabeledWithItsPureNameOverATableStoredUpperCase() throws Exception {
        for (DatabaseType type : FOLDING) {
            ModelContext ctx = Compiler.compileModel(model(type));
            for (String query : QUERIES) {
                ExecutionPlan plan = Compiler.query(ctx, query).executionPlan("s::RT");
                ExecutionPlan.TdsResult result = assertInstanceOf(ExecutionPlan.TdsResult.class, plan.root());
                try (Connection c = TestBackends.of(type).open()) {
                    // its load makes the table, its answer runs
                    PlanCases.write(plan, c, Map.of(), PlanCases.Written.JSON::writer);
                    assertEquals(List.of("REGION", "YEAR", "QTY"), stored(c, type),
                            type + ": the table as lite's DDL made it, its names folded upper");
                    assertEquals(result.columns().stream().map(ExecutionPlan.Column::name).toList(),
                            labels(c, plan.answerSql().statement()), type + " " + query + ": "
                                    + plan.answerSql().statement());
                }
            }
        }
    }

    /** A pivot no cast names (keyed: its keys read first, then its body per key), run whole by the runner. */
    @Test
    void aDynamicPivotsKeysAndColumnsAreItsOwnNames() throws Exception {
        for (DatabaseType type : FOLDING) {
            ExecutionPlan plan = Compiler.query(Compiler.compileModel(model(type)),
                    "|#>{s::DB.T}#->pivot(~[year], ~['n' : x | $x.qty : y | $y->plus()])").executionPlan("s::RT");
            ExecutionPlan.Keyed keyed = assertInstanceOf(ExecutionPlan.Keyed.class, plan.root());
            try (Connection c = TestBackends.of(type).open()) {
                // the whole plan runs: its keys read, its body written once per key
                String rows = PlanCases.write(plan, c, Map.of(), PlanCases.Written.JSON::writer);
                assertEquals(keyed.keyFields(), labels(c, keyed.keys().statement()), type + ": the keys' statement "
                        + keyed.keys().statement());
                assertEquals(true, rows.contains("2021") && rows.contains("2022"), type + ": " + rows);
            }
        }
    }

    /** The columns of {@code T} as the database stores them. */
    private static List<String> stored(Connection c, DatabaseType type) throws Exception {
        try (ResultSet rs = c.getMetaData().getColumns(null, null, type == DatabaseType.H2 ? "T" : "%", null)) {
            List<String> out = new ArrayList<>();
            while (rs.next()) {
                if (rs.getString("TABLE_NAME").equalsIgnoreCase("T")) {
                    out.add(rs.getString("COLUMN_NAME"));
                }
            }
            return out;
        }
    }

    /** The labels the database reports for {@code statement}'s columns. */
    private static List<String> labels(Connection c, String statement) throws Exception {
        try (PreparedStatement st = c.prepareStatement(statement)) {
            java.sql.ResultSetMetaData md = st.getMetaData();
            List<String> out = new ArrayList<>();
            for (int i = 1; i <= md.getColumnCount(); i++) {
                out.add(md.getColumnLabel(i));
            }
            return out;
        }
    }
}
