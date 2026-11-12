// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.planner;

import com.legend.executionplan.DatabaseType;
import com.legend.executionplan.ExecutionPlan;
import com.legend.executionplan.ExecutionPlan.LoadStep;
import com.legend.executionplan.PlanJson;
import com.legend.sql.dialect.DuckDb;
import com.legend.sql.dialect.HostOperation;
import com.legend.sql.dialect.HostOperations;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A SNAPSHOT'S STREAM AND A HOST VALUE'S ATTACH, AS PLANNED LOADS, ON DUCKDB (the user's ruling of 2026-11-11, "This
 * is the right plan"; the orchestrator's simplification of 2026-11-12, no hash): a snapshot from a remote plane is one
 * HOST_SESSION load whose stream step names the plane, its statement and the table -- the database's own statements
 * around it, the transaction's among them -- and a Python frame is one whose attach step names the value the host
 * holds. Neither plan carries a row or a value's bytes; the JSON reads back exactly the writer's keys, so none can ride
 * in one.
 */
class DuckDbHostStreamAndAttachTest {

    private static final HostOperations DUCK = HostOperations.of(new DuckDb());

    private static ExecutionPlan.Load load(ExecutionPlan plan) {
        assertEquals(plan, PlanJson.read(PlanJson.write(plan)), "the plan survives its JSON");
        ExecutionPlan.Load load = (ExecutionPlan.Load) plan.root();
        assertEquals(ExecutionPlan.Load.Cadence.HOST_SESSION, load.cadence());
        assertEquals(Map.of(), load.content(), "a host's load carries no content blob");
        return load;
    }

    private static ExecutionPlan snap(String plane, String sql, String schema, String table) {
        return LoadPlanner.snapStream(new DuckDb(), DatabaseType.DuckDB,
                new LoadPlanner.SnapStream(plane, sql, schema, table));
    }

    @Test
    void aSnapIsOneStreamStepTheDatabasesOwnStatementsAround() {
        ExecutionPlan.Load load = load(snap("warehouse", "SELECT \"region\" FROM \"main\".\"TRADES\"", "sales", "TRADES"));
        assertEquals(1, load.steps().size());
        LoadStep.Stream stream = (LoadStep.Stream) load.steps().get(0);
        assertEquals("warehouse", stream.plane());
        assertEquals("SELECT \"region\" FROM \"main\".\"TRADES\"", stream.sql());
        assertEquals("sales", stream.schema());
        assertEquals("TRADES", stream.table());
        HostOperation.Name table = new HostOperation.Name(null, "sales", "TRADES", false);
        // its schema made and the table there before dropped, in the stream's transaction
        List<String> prepare = new java.util.ArrayList<>(DUCK.statements(new HostOperation.MakeSchema(null, "sales")));
        prepare.addAll(DUCK.statements(new HostOperation.Drop(HostOperation.Kind.TABLE, table)));
        assertEquals(prepare, stream.prepare());
        assertEquals(List.of("CREATE SCHEMA IF NOT EXISTS \"sales\"", "DROP TABLE IF EXISTS \"sales\".\"TRADES\""),
                stream.prepare());
        // the database counts what landed; the transaction is the database's own
        assertEquals("SELECT count(*) AS n FROM \"sales\".\"TRADES\"", stream.count());
        assertEquals(DUCK.statements(new HostOperation.Transaction(HostOperation.Transaction.Step.BEGIN)),
                stream.transaction().begin());
        assertEquals(DUCK.statements(new HostOperation.Transaction(HostOperation.Transaction.Step.COMMIT)),
                stream.transaction().commit());
        assertEquals(DUCK.statements(new HostOperation.Transaction(HostOperation.Transaction.Step.ROLLBACK)),
                stream.transaction().rollback());
        // no schema: the database's own, none made
        LoadStep.Stream bare = (LoadStep.Stream) load(snap("warehouse", "SELECT 1", null, "snap")).steps().get(0);
        assertEquals(List.of("DROP TABLE IF EXISTS \"snap\""), bare.prepare());
    }

    @Test
    void aSnapsIdentityIsWhatItNames() {
        String one = load(snap("warehouse", "SELECT 1", null, "t")).identity();
        assertEquals(one, load(snap("warehouse", "SELECT 1", null, "t")).identity(), "the same names, the same load");
        assertNotEquals(one, load(snap("warehouse", "SELECT 2", null, "t")).identity());
        assertNotEquals(one, load(snap("warehouse", "SELECT 1", null, "u")).identity());
        assertNotEquals(one, load(snap("lake", "SELECT 1", null, "t")).identity());
        assertNotEquals(one, load(snap("warehouse", "SELECT 1", "s", "t")).identity());
    }

    @Test
    void aFrameIsAttachedByNameAndReadThroughAViewOrCopiedOnce() {
        ExecutionPlan.Load live = load(LoadPlanner.frameLoad(new DuckDb(), DatabaseType.DuckDB,
                new LoadPlanner.FrameLoad("trades", "__legend_lite_trades", true, "main", "trades")));
        assertEquals(new LoadStep.Attach("trades", "__legend_lite_trades", ExecutionPlan.Reach.CONNECTION),
                live.steps().get(0), "a live frame's attach is the connection's: the host attaches it on every cursor");
        assertEquals(List.of("DROP VIEW IF EXISTS \"main\".\"trades\"",
                "CREATE VIEW \"main\".\"trades\" AS SELECT * FROM \"__legend_lite_trades\""), statements(live));
        ExecutionPlan.Load once = load(LoadPlanner.frameLoad(new DuckDb(), DatabaseType.DuckDB,
                new LoadPlanner.FrameLoad("stream", "__legend_lite_stream", false, "main", "stream")));
        assertEquals(new LoadStep.Attach("stream", "__legend_lite_stream", ExecutionPlan.Reach.DATABASE),
                once.steps().get(0), "a stream read once is attached for the load alone");
        assertEquals(List.of("DROP TABLE IF EXISTS \"main\".\"stream\"",
                "CREATE TABLE \"main\".\"stream\" AS SELECT * FROM \"__legend_lite_stream\""), statements(once));
        assertTrue(assertThrows(IllegalArgumentException.class, () -> LoadPlanner.frameLoad(new DuckDb(),
                DatabaseType.DuckDB, new LoadPlanner.FrameLoad("a frame", "x", true, null, "t"))).getMessage()
                .contains("attached by its name"));
    }

    private static List<String> statements(ExecutionPlan.Load load) {
        return load.steps().stream().skip(1).map(s -> ((LoadStep.Statement) s).sql()).toList();
    }

    @Test
    void aStreamAndAnAttachAreAHostSessionLoadsAlone() {
        for (ExecutionPlan.Load load : List.of(load(snap("warehouse", "SELECT 1", null, "t")),
                load(LoadPlanner.frameLoad(new DuckDb(), DatabaseType.DuckDB,
                        new LoadPlanner.FrameLoad("f", "__legend_lite_f", true, null, "f"))))) {
            // exec, which holds no host's planes or values, never meets one
            for (ExecutionPlan.Load.Cadence cadence : List.of(ExecutionPlan.Load.Cadence.ONCE,
                    ExecutionPlan.Load.Cadence.PER_ACQUISITION)) {
                assertTrue(assertThrows(IllegalArgumentException.class, () -> new ExecutionPlan.Load("load:x", cadence,
                        load.steps(), load.target(), Map.of())).getMessage().contains("HOST_SESSION load's alone"));
            }
        }
    }

    @Test
    void noRowAndNoValueRidesInThePlan() {
        String sentinel = "SENTINEL_ROW_7f3a";
        String snapJson = PlanJson.write(snap("warehouse", "SELECT 1", null, "t"));
        String frameJson = PlanJson.write(LoadPlanner.frameLoad(new DuckDb(), DatabaseType.DuckDB,
                new LoadPlanner.FrameLoad("f", "__legend_lite_f", true, null, "f")));
        // a key that could carry rows or bytes is refused as unknown, wherever it is put
        for (String[] tamper : List.of(
                new String[] {snapJson, "\"plane\":\"warehouse\"", "\"plane\":\"warehouse\",\"rows\":[[\"" + sentinel + "\"]]"},
                new String[] {snapJson, "\"rollback\":[", "\"chunks\":[\"" + sentinel + "\"],\"rollback\":["},
                new String[] {frameJson, "\"held\":\"f\"", "\"held\":\"f\",\"bytes\":\"" + sentinel + "\""})) {
            String tampered = tamper[0].replace(tamper[1], tamper[2]);
            assertNotEquals(tamper[0], tampered, tamper[1]);
            IllegalArgumentException refused = assertThrows(IllegalArgumentException.class, () -> PlanJson.read(tampered));
            assertTrue(refused.getMessage().contains("has no key"), refused.getMessage());
            assertFalse(refused.getMessage().contains(sentinel), "a refusal never repeats what rode in");
        }
        // what the request holds is all the plan holds: no field of a snap's or a frame's request is data
        assertFalse(snapJson.contains("\"rows\"") || frameJson.contains("\"rows\""));
    }

    @Test
    void whatAStreamOrAnAttachCannotBeIsRefusedByName() {
        assertThrows(IllegalArgumentException.class, () -> snap("a plane", "SELECT 1", null, "t"));
        assertThrows(IllegalArgumentException.class, () -> snap("warehouse", "  ", null, "t"));
        assertThrows(IllegalArgumentException.class, () -> new LoadStep.Stream("w", "SELECT 1", null, "t", List.of(),
                "SELECT 1", new ExecutionPlan.Transaction(List.of(), List.of("COMMIT"), List.of("ROLLBACK"))));
        assertThrows(IllegalArgumentException.class, () -> new LoadStep.Attach("f", "a b", ExecutionPlan.Reach.CONNECTION));
    }
}
