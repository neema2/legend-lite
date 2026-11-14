// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.planner;

import com.legend.executionplan.DatabaseType;
import com.legend.executionplan.ExecutionPlan;
import com.legend.executionplan.PlanJson;
import com.legend.sql.dialect.DuckDb;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * AN INGEST'S PLANS, ON DUCKDB (docs/DESK_DATA_ROADMAP_2026_10_29.md C0, the user's go of 2026-11-14): a dataset's
 * landing planned once from its declaration -- the table made empty as declared, each batch put in by the dataset's
 * write mode from the relation the host attaches by name, its retention kept by arrival at a tenth of it -- each a
 * HOST_SESSION load that survives its JSON and runs on DuckDB as planned. A time retention, a table no ingest makes, a
 * table no Database declares, are refused by name.
 */
class IngestPlannerTest {

    private static final String MODEL = """
            ###Lakehouse
            Ingest t::Feed Stream
            [
              PRINTS( SYMBOL: Varchar(8), SEQ: BigInt[1], PRICE: Double ) pk=[ SEQ ] writeMode=AppendOnly retention=LastRows(20);
              QUOTES( SYMBOL: Varchar(8)[1], BID: Double ) pk=[ SYMBOL ] writeMode=LatestByKey;
            ]

            ###Relational
            Database t::DB
            (
              include Ingest t::Feed
              Table OWN (ID INTEGER PRIMARY KEY)
            )
            """;

    private static IngestPlanner.Plans plans(String model, String table) {
        return IngestPlanner.plans(new DuckDb(), DatabaseType.DuckDB,
                new LoadPlanner.DeclaredTable(com.legend.Compiler.compileModel(model), "t::DB", "default", table), "batch");
    }

    private static ExecutionPlan.Load load(ExecutionPlan plan) {
        assertEquals(plan, PlanJson.read(PlanJson.write(plan)), "the plan survives its JSON");
        ExecutionPlan.Load load = (ExecutionPlan.Load) plan.root();
        assertEquals(ExecutionPlan.Load.Cadence.HOST_SESSION, load.cadence());
        return load;
    }

    /** The plan's load run on {@code st}, as a host runs it: each statement as it is, an attach the host's -- here the
     *  test's: {@code held}'s rows made the relation the plan names. */
    private static void run(Statement st, ExecutionPlan plan, String held) throws Exception {
        ExecutionPlan.Load load = load(plan);
        for (ExecutionPlan.LoadStep step : load.steps()) {
            switch (step) {
                case ExecutionPlan.LoadStep.Statement s -> st.execute(s.sql());
                case ExecutionPlan.LoadStep.Statements s -> {
                    for (var sql : load.plannedStatements(s)) {
                        st.execute(sql.text());
                    }
                }
                case ExecutionPlan.LoadStep.Attach a -> {
                    assertEquals("batch", a.held());
                    assertEquals(ExecutionPlan.Reach.DATABASE, a.reach());
                    st.execute("CREATE TABLE \"" + a.as() + "\" AS " + held);
                }
                default -> throw new AssertionError("an ingest's plan holds statements and its attach: " + step);
            }
        }
    }

    private static List<String> rows(Statement st, String sql) throws Exception {
        List<String> out = new ArrayList<>();
        try (ResultSet rs = st.executeQuery(sql)) {
            while (rs.next()) {
                out.add(rs.getString(1));
            }
        }
        return out;
    }

    @Test
    void anAppendedDatasetIsMadeLandedAndKeptByArrival() throws Exception {
        IngestPlanner.Plans p = plans(MODEL, "PRINTS");
        assertEquals(2L, p.keepEvery(), "kept when a tenth of its retention has arrived");
        assertTrue(load(p.land()).steps().stream().anyMatch(s -> s instanceof ExecutionPlan.LoadStep.Attach a
                && a.as().equals("__land_PRINTS")), "the batch is attached as the planner's relation");
        try (Connection c = DriverManager.getConnection("jdbc:duckdb:"); Statement st = c.createStatement()) {
            run(st, p.make(), null);
            assertEquals(List.of("0"), rows(st, "SELECT count(*) FROM PRINTS"), "made empty");
            assertEquals(List.of("SYMBOL VARCHAR", "SEQ BIGINT", "PRICE DOUBLE"), rows(st, "SELECT column_name || ' ' "
                    + "|| data_type FROM information_schema.columns WHERE table_name = 'PRINTS' ORDER BY ordinal_position"));
            // the batch's columns in another order: put in by name
            for (int batch = 0; batch < 3; batch++) {
                run(st, p.land(), "SELECT 100.0 + " + (batch * 10) + " + i AS PRICE, 'AAPL' AS SYMBOL, "
                        + (batch * 10) + " + i AS SEQ FROM range(10) t(i)");
            }
            assertEquals(List.of("30"), rows(st, "SELECT count(*) FROM PRINTS"));
            assertEquals(List.of("0"), rows(st, "SELECT count(*) FROM information_schema.tables WHERE table_name = "
                    + "'__land_PRINTS'"), "the attached relation dropped after it lands");
            run(st, p.keep(), null);
            assertEquals(List.of("10 29 20"), rows(st, "SELECT min(SEQ) || ' ' || max(SEQ) || ' ' || count(*) FROM PRINTS"),
                    "the latest 20 by arrival kept");
            assertEquals(List.of("110.0"), rows(st, "SELECT PRICE FROM PRINTS WHERE SEQ = 10"));
        }
    }

    @Test
    void aLatestByKeyDatasetKeepsEachKeysLastRowAndNoRetention() throws Exception {
        IngestPlanner.Plans p = plans(MODEL, "QUOTES");
        assertNull(p.keep());
        assertNull(p.keepEvery());
        try (Connection c = DriverManager.getConnection("jdbc:duckdb:"); Statement st = c.createStatement()) {
            run(st, p.make(), null);
            run(st, p.land(), "SELECT * FROM (VALUES ('AAPL', 1.0), ('MSFT', 10.0), ('AAPL', 3.0)) t(SYMBOL, BID)");
            run(st, p.land(), "SELECT * FROM (VALUES ('MSFT', 12.0)) t(SYMBOL, BID)");
            assertEquals(List.of("AAPL 3.0", "MSFT 12.0"), rows(st, "SELECT SYMBOL || ' ' || BID FROM QUOTES ORDER BY SYMBOL"));
        }
    }

    @Test
    void whatAHostTableCannotLandIsRefusedByName() {
        var time = assertThrows(IllegalArgumentException.class, () -> plans(
                MODEL.replace("retention=LastRows(20)", "retention=Last('PT30S')"), "PRINTS"));
        assertTrue(time.getMessage().contains("keeps Last('PT30S'), which reads when each row arrived"), time.getMessage());
        var own = assertThrows(IllegalArgumentException.class, () -> plans(MODEL, "OWN"));
        assertTrue(own.getMessage().contains("is the Database t::DB's own, not an ingest's dataset"), own.getMessage());
        var none = assertThrows(IllegalArgumentException.class, () -> plans(MODEL, "NOPE"));
        assertTrue(none.getMessage().contains("declares no table default.NOPE"), none.getMessage());
    }
}
