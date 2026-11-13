// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.planner;

import com.legend.executionplan.DataRef;
import com.legend.executionplan.DatabaseType;
import com.legend.executionplan.ExecutionPlan;
import com.legend.executionplan.PlanJson;
import com.legend.sql.dialect.DuckDb;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A HOST'S OWN FILE AS A PLANNED LOAD, ON DUCKDB (B3, 2026-11-11, the orchestrator's go: uploads and Studio's file puts as planned
 * loads, the file a pointer; census M14): {@code LoadPlanner.hostFile} plans one HOST_SESSION load whose file step names
 * the host's file by reference -- never its path -- and whose delivery is the database's reader with the file bound to
 * its {@code ?}; a table of the file's own made from it, or a declared table made as declared and filled from it. The
 * plan survives its JSON. A format the destination cannot take is refused by name, and so is a table the model does not
 * declare.
 */
class DuckDbHostFileLoadTest {

    private static final String DIGEST = "ab".repeat(32);

    private static final String MODEL = """
            ###Relational
            Database x::DB ( Table T (ID INTEGER PRIMARY KEY, NAME VARCHAR(20)) )
            """;

    private static DataRef file(String name, DataRef.Format format) {
        return new DataRef(DataRef.Namespace.HOST, name, DIGEST, 12, format);
    }

    private static ExecutionPlan.Load load(ExecutionPlan plan) {
        assertEquals(plan, PlanJson.read(PlanJson.write(plan)), "the plan survives its JSON");
        ExecutionPlan.Load load = (ExecutionPlan.Load) plan.root();
        assertEquals(ExecutionPlan.Load.Cadence.HOST_SESSION, load.cadence());
        return load;
    }

    private static List<String> reads(ExecutionPlan.Load load) {
        ExecutionPlan.LoadStep.File f = (ExecutionPlan.LoadStep.File) load.steps().get(load.steps().size() - 1);
        assertEquals(DataRef.Namespace.HOST, f.ref().namespace());
        return ((ExecutionPlan.LoadStep.Delivery.Reads) f.delivery()).statements();
    }

    /**
     * A HOST'S TRANSACTION IS THE PLAN'S (the re-audit's S8, 2026-11-12: until then a host's session asked the planner
     * for its begin and commit at run, and a run never plans, G4): a target on DuckDB carries the statements a host
     * opens, commits and rolls back a transaction with, and they survive the plan's JSON; a database no host runs a plan
     * on carries none; a transaction with a key it does not have, or a step with no statement, is refused by name.
     */
    @Test
    void aTargetCarriesTheTransactionAHostOpens() {
        ExecutionPlan plan = LoadPlanner.hostFile(new DuckDb(), DatabaseType.DuckDB,
                file("upload_sales.csv", DataRef.DetectedCsv.DETECTED), new LoadPlanner.NewTable("sales"));
        assertEquals(new ExecutionPlan.Transaction(List.of("BEGIN TRANSACTION"), List.of("COMMIT"), List.of("ROLLBACK")),
                load(plan).target().transaction());
        String json = PlanJson.write(plan);
        String written = "\"transaction\":{\"begin\":[\"BEGIN TRANSACTION\"],\"commit\":[\"COMMIT\"],"
                + "\"rollback\":[\"ROLLBACK\"]}";
        assertTrue(json.contains(written), json);
        // every other database lite runs on carries none: no host runs a plan there (each named by the enumeration,
        // never by this DuckDB owner: DialectOwnerExclusivityTest, the merge of 2026-11-12)
        for (var other : com.legend.database.Databases.implemented()) {
            if (other != com.legend.model.ConnectionDefinition.DatabaseType.DuckDB) {
                assertNull(Targets.of(new ExecutionPlan.Database.Platform(ConnectionPlans.type(other), null),
                        com.legend.database.Databases.dialect(other)).transaction(), other.name());
            }
        }
        var unknown = assertThrows(IllegalArgumentException.class, () -> PlanJson.read(json.replace(written,
                written.substring(0, written.length() - 1) + ",\"savepoint\":[]}")));
        assertEquals("a target's transaction has no key savepoint", unknown.getMessage());
        assertThrows(IllegalArgumentException.class, () -> PlanJson.read(json.replace(
                "\"begin\":[\"BEGIN TRANSACTION\"]", "\"begin\":[]")));
    }

    @Test
    void aTableOfItsOwnIsMadeFromTheFileItsReaderBound() {
        assertEquals(List.of("CREATE OR REPLACE TABLE \"sales\" AS SELECT * FROM read_csv(?, AUTO_DETECT=TRUE, HEADER=TRUE)"),
                reads(load(LoadPlanner.hostFile(new DuckDb(), DatabaseType.DuckDB,
                        file("upload_sales.csv", DataRef.DetectedCsv.DETECTED), new LoadPlanner.NewTable("sales")))));
        assertEquals(List.of("CREATE OR REPLACE TABLE \"t\" AS SELECT * FROM read_parquet(?)"),
                reads(load(LoadPlanner.hostFile(new DuckDb(), DatabaseType.DuckDB,
                        file("upload_t.parquet", DataRef.Parquet.PARQUET), new LoadPlanner.NewTable("t")))));
        assertEquals(List.of("CREATE OR REPLACE TABLE \"t\" AS SELECT * FROM read_json(?, auto_detect=true)"),
                reads(load(LoadPlanner.hostFile(new DuckDb(), DatabaseType.DuckDB,
                        file("upload_t.json", DataRef.Json.RECORDS), new LoadPlanner.NewTable("t")))));
    }

    @Test
    void aDeclaredTableIsMadeAsDeclaredAndFilledFromTheFileByItsColumnsNames() {
        var ctx = com.legend.Compiler.compileModel(MODEL);
        ExecutionPlan.Load load = load(LoadPlanner.hostFile(new DuckDb(), DatabaseType.DuckDB,
                file("upload-1.csv", DataRef.Csv.DEFAULT), new LoadPlanner.DeclaredTable(ctx, "x::DB", "default", "T")));
        assertTrue(load.steps().stream().anyMatch(s -> s instanceof ExecutionPlan.LoadStep.Statement st
                && st.sql().toUpperCase(java.util.Locale.ROOT).startsWith("CREATE TABLE T")), load.steps()::toString);
        assertEquals(List.of("INSERT INTO T (ID, NAME) SELECT \"ID\", \"NAME\" FROM read_csv(?, header = true, all_varchar = true)"),
                reads(load));
        for (String sql : reads(load)) {
            assertFalse(sql.contains("upload-1"), "the file is bound, never named in a statement: " + sql);
        }
    }

    @Test
    void whatADestinationCannotTakeIsRefusedByName() {
        var ctx = com.legend.Compiler.compileModel(MODEL);
        assertTrue(assertThrows(IllegalArgumentException.class, () -> LoadPlanner.hostFile(new DuckDb(),
                DatabaseType.DuckDB, file("f.csv", DataRef.Csv.DEFAULT), new LoadPlanner.NewTable("t"))).getMessage()
                .contains("detects a CSV's columns"));
        assertTrue(assertThrows(IllegalArgumentException.class, () -> LoadPlanner.hostFile(new DuckDb(),
                DatabaseType.DuckDB, file("f.json", DataRef.Json.RECORDS),
                new LoadPlanner.DeclaredTable(ctx, "x::DB", "default", "T"))).getMessage().contains("not JSON"));
        assertTrue(assertThrows(IllegalArgumentException.class, () -> LoadPlanner.hostFile(new DuckDb(),
                DatabaseType.DuckDB, file("f.csv", DataRef.Csv.DEFAULT),
                new LoadPlanner.DeclaredTable(ctx, "x::DB", "default", "NOPE"))).getMessage()
                .contains("default.NOPE, which the Database x::DB declares not"));
        assertThrows(IllegalArgumentException.class, () -> LoadPlanner.hostFile(new DuckDb(), DatabaseType.DuckDB,
                new DataRef(DataRef.Namespace.RESOURCE, "f.csv", DIGEST, 12, DataRef.DetectedCsv.DETECTED),
                new LoadPlanner.NewTable("t")));
        // never a machine's path: the reference refuses one
        assertThrows(IllegalArgumentException.class, () -> file("/home/me/f.csv", DataRef.DetectedCsv.DETECTED));
    }
}
