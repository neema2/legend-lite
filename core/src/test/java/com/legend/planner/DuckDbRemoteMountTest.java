// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.planner;

import com.legend.executionplan.DatabaseType;
import com.legend.executionplan.ExecutionPlan;
import com.legend.executionplan.ExecutionPlan.LoadStep;
import com.legend.executionplan.PlanJson;
import com.legend.sql.dialect.DuckDb;
import com.legend.sql.dialect.HostOperation;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A REMOTE FILE MOUNTED BY A PLANNED LOAD, ON DUCKDB (B3's Q3, 2026-11-11, the user's ruling of that day: "Yes do option
 * 2"; docs/data-delivery-homework/B3_REMOTE_MOUNT_DESIGN_2026_11_11.md): {@code LoadPlanner.remoteMount} plans one
 * HOST_SESSION load -- the extensions, the S3 secret bound by name, a bearer URL's session variable bound by name (the
 * connection's: the host replays it), the view -- and no credential and no token is ever in a statement or in the plan:
 * the planner is never handed one. The plan survives its JSON, a bound step's binds by name and field; what a mount
 * cannot be is refused by name.
 */
class DuckDbRemoteMountTest {

    private static ExecutionPlan.Load mounted(LoadPlanner.RemoteMount mount) {
        ExecutionPlan plan = LoadPlanner.remoteMount(new DuckDb(), DatabaseType.DuckDB, mount);
        assertEquals(plan, PlanJson.read(PlanJson.write(plan)), "the plan survives its JSON");
        ExecutionPlan.Load load = (ExecutionPlan.Load) plan.root();
        assertEquals(ExecutionPlan.Load.Cadence.HOST_SESSION, load.cadence());
        return load;
    }

    private static List<String> sql(ExecutionPlan.Load load) {
        return load.steps().stream().map(s -> switch (s) {
            case LoadStep.Statement st -> st.sql();
            case LoadStep.Bound b -> b.sql();
            case LoadStep.Statements st -> throw new AssertionError("a mount reads no statements blob");
            case LoadStep.File f -> throw new AssertionError("a mount reads no file");
        }).toList();
    }

    @Test
    void anAddressThatCarriesNoTokenIsReadAsItIs() {
        assertEquals(List.of("INSTALL httpfs", "LOAD httpfs",
                "CREATE OR REPLACE VIEW \"trades\" AS SELECT * FROM read_parquet('s3://b/trades.parquet')"),
                sql(mounted(new LoadPlanner.RemoteMount("trades", new LoadPlanner.PlainLocation("s3://b/trades.parquet"),
                        HostOperation.RemoteFormat.PARQUET, null))));
        assertEquals(List.of("INSTALL httpfs", "LOAD httpfs",
                "CREATE OR REPLACE VIEW \"t\" AS SELECT * FROM read_csv_auto('https://h/x.csv')"),
                sql(mounted(new LoadPlanner.RemoteMount("t", new LoadPlanner.PlainLocation("https://h/x.csv"),
                        HostOperation.RemoteFormat.CSV, null))));
        assertEquals(List.of("INSTALL httpfs", "LOAD httpfs", "INSTALL iceberg", "LOAD iceberg",
                "CREATE OR REPLACE VIEW \"t\" AS SELECT * FROM iceberg_scan('s3://b/t')"),
                sql(mounted(new LoadPlanner.RemoteMount("t", new LoadPlanner.PlainLocation("s3://b/t"),
                        HostOperation.RemoteFormat.ICEBERG, null))));
    }

    @Test
    void aBearerUrlIsTheConnectionsVariableBoundByName() {
        ExecutionPlan.Load load = mounted(new LoadPlanner.RemoteMount("trades",
                new LoadPlanner.BearerLocation("datacube_url_trades", "https://b.s3.amazonaws.com/trades.parquet"),
                HostOperation.RemoteFormat.PARQUET, null));
        String variable = HostOperation.variable("datacube_url_trades");
        assertTrue(variable.matches("lite_url_[0-9a-f]+"), variable);
        assertEquals(List.of("INSTALL httpfs", "LOAD httpfs", "SET VARIABLE " + variable + " = ?",
                "CREATE OR REPLACE VIEW \"trades\" AS SELECT * FROM read_parquet(getvariable('" + variable + "'))"), sql(load));
        LoadStep.Bound set = (LoadStep.Bound) load.steps().get(2);
        assertEquals(ExecutionPlan.Reach.CONNECTION, set.reach(), "a variable is one connection's: the host replays it");
        assertEquals(List.of(new LoadStep.Bind.Held("datacube_url_trades", "URL")), set.binds());
        for (String statement : sql(load)) {
            assertFalse(statement.contains("amazonaws"), "no statement holds the address: " + statement);
        }
    }

    @Test
    void aSecretsFieldsAreBoundByNameAndItsScopeIsThePlansOwn() {
        ExecutionPlan.Load load = mounted(new LoadPlanner.RemoteMount("sales",
                new LoadPlanner.PlainLocation("s3://bucket/sales/*.parquet"), HostOperation.RemoteFormat.PARQUET,
                new LoadPlanner.RemoteCredentials("datacube_s3_sales", List.of("KEY_ID", "SECRET", "SESSION_TOKEN"))));
        LoadStep.Bound secret = (LoadStep.Bound) load.steps().get(2);
        assertEquals("CREATE OR REPLACE TEMPORARY SECRET \"datacube_s3_sales\" (TYPE S3, KEY_ID ?, SECRET ?, SESSION_TOKEN ?,"
                + " SCOPE ?)", secret.sql());
        assertEquals(ExecutionPlan.Reach.DATABASE, secret.reach());
        assertEquals(List.of(new LoadStep.Bind.Held("datacube_s3_sales", "KEY_ID"),
                new LoadStep.Bind.Held("datacube_s3_sales", "SECRET"),
                new LoadStep.Bind.Held("datacube_s3_sales", "SESSION_TOKEN"),
                new LoadStep.Bind.Value("s3://bucket/sales/")), secret.binds(), "each field held, the scope cut at the glob");
        // an Iceberg table's keys apply to its folder, its metadata's parent
        LoadStep.Bound iceberg = (LoadStep.Bound) mounted(new LoadPlanner.RemoteMount("t",
                new LoadPlanner.PlainLocation("s3://b/t/metadata"), HostOperation.RemoteFormat.ICEBERG,
                new LoadPlanner.RemoteCredentials("datacube_s3_t", List.of("KEY_ID")))).steps().get(4);
        assertEquals(new LoadStep.Bind.Value("s3://b/t"), iceberg.binds().get(1));
        // the access alone (no view): Python's checks of an Iceberg table's files before any view reads it
        assertEquals(List.of("INSTALL httpfs", "LOAD httpfs",
                "CREATE OR REPLACE TEMPORARY SECRET \"datacube_s3_t\" (TYPE S3, KEY_ID ?, SCOPE ?)"),
                sql(mounted(new LoadPlanner.RemoteMount(null, new LoadPlanner.PlainLocation("s3://b/t"),
                        HostOperation.RemoteFormat.PARQUET, new LoadPlanner.RemoteCredentials("datacube_s3_t",
                                List.of("KEY_ID"))))));
    }

    @Test
    void whatAMountCannotBeIsRefusedByName() {
        assertTrue(assertThrows(IllegalArgumentException.class, () -> new LoadPlanner.PlainLocation(
                "https://b.s3.amazonaws.com/t.parquet?X-Amz-Signature=SENTINEL")).getMessage().contains("carries its token"));
        assertThrows(IllegalArgumentException.class, () -> new LoadPlanner.BearerLocation("n", "https://h/t?x=1"));
        assertTrue(assertThrows(IllegalArgumentException.class, () -> LoadPlanner.remoteMount(new DuckDb(),
                DatabaseType.DuckDB, new LoadPlanner.RemoteMount("t", new LoadPlanner.BearerLocation("datacube_url_t",
                        "https://h/t"), HostOperation.RemoteFormat.ICEBERG, null))).getMessage().contains("a folder"));
        assertThrows(IllegalArgumentException.class, () -> LoadPlanner.remoteMount(new DuckDb(), DatabaseType.DuckDB,
                new LoadPlanner.RemoteMount("t", new LoadPlanner.PlainLocation("s3://b/t"),
                        HostOperation.RemoteFormat.PARQUET, new LoadPlanner.RemoteCredentials("s", List.of("PASSWORD")))));
        assertThrows(IllegalArgumentException.class, () -> LoadPlanner.remoteMount(new DuckDb(), DatabaseType.DuckDB,
                new LoadPlanner.RemoteMount("t", new LoadPlanner.BearerLocation("not a store's name", "https://h/t"),
                        HostOperation.RemoteFormat.PARQUET, null)));
    }

    @Test
    void aMountsIdentityIsWhatItNamesNeverWhatTheHostHolds() {
        LoadPlanner.RemoteMount one = new LoadPlanner.RemoteMount("t", new LoadPlanner.BearerLocation("datacube_url_t",
                "https://h/t.parquet"), HostOperation.RemoteFormat.PARQUET, null);
        assertEquals(mounted(one).identity(), mounted(one).identity(), "the same names, the same load");
        assertNotEquals(mounted(one).identity(), mounted(new LoadPlanner.RemoteMount("t",
                new LoadPlanner.PlainLocation("https://h/t.parquet"), HostOperation.RemoteFormat.PARQUET, null)).identity());
    }

    @Test
    void aBoundStatementIsAHostSessionLoadsAloneAndItsJsonNamesNoValue() {
        ExecutionPlan.Load load = mounted(new LoadPlanner.RemoteMount("t", new LoadPlanner.BearerLocation("datacube_url_t",
                "https://h/t.parquet"), HostOperation.RemoteFormat.PARQUET, null));
        // only a HOST_SESSION load holds a statement the host binds: exec, which holds no host's store, never meets one
        assertTrue(assertThrows(IllegalArgumentException.class, () -> new ExecutionPlan.Load("load:x",
                ExecutionPlan.Load.Cadence.PER_ACQUISITION, load.steps(), load.target(), Map.of())).getMessage()
                .contains("HOST_SESSION load's alone"));
        // a bound step's JSON: its binds by name and field; a key that could carry a value is refused as unknown
        String json = PlanJson.write(ExecutionPlan.writing(List.of(), load));
        assertTrue(json.contains("\"_type\":\"held\",\"name\":\"datacube_url_t\",\"field\":\"URL\""), json);
        String tampered = json.replace("\"field\":\"URL\"", "\"field\":\"URL\",\"value\":\"SENTINEL\"");
        assertNotEquals(json, tampered);
        assertTrue(assertThrows(IllegalArgumentException.class, () -> PlanJson.read(tampered)).getMessage()
                .contains("has no key value"));
    }
}
