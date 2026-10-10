// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.executionplan;

import com.legend.executionplan.ExecutionPlan.Column;
import com.legend.executionplan.ExecutionPlan.Database;
import com.legend.executionplan.ExecutionPlan.Format;
import com.legend.executionplan.ExecutionPlan.Multiplicity;
import com.legend.executionplan.ExecutionPlan.Parameter;
import com.legend.executionplan.ExecutionPlan.Relation;
import com.legend.executionplan.ExecutionPlan.Sequence;
import com.legend.executionplan.ExecutionPlan.Servers;
import com.legend.executionplan.ExecutionPlan.SetupStep;
import com.legend.executionplan.ExecutionPlan.Slot;
import com.legend.executionplan.ExecutionPlan.Sql;
import com.legend.executionplan.ExecutionPlan.Target;
import com.legend.executionplan.ExecutionPlan.TdsColumn;
import com.legend.executionplan.ExecutionPlan.TdsResult;
import com.legend.executionplan.ExecutionPlan.TextResult;
import com.legend.executionplan.ExecutionPlan.Value;
import com.legend.model.AuthenticationSpec;
import com.legend.model.ConnectionDefinition;
import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.model.ConnectionSpecification;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The lite plan format reads back what it writes: every node kind, every connection specification and authentication. */
class PlanJsonTest {

    private static final List<ConnectionSpecification> SPECIFICATIONS = List.of(
            new ConnectionSpecification.InMemory(),
            new ConnectionSpecification.LocalFile("/tmp/x.duckdb"),
            new ConnectionSpecification.LocalH2(null),
            new ConnectionSpecification.LocalH2("jdbc:h2:mem:x", "default\nT\nID\n1", List.of("create table T(ID INT)")),
            new ConnectionSpecification.EmbeddedH2("db", "/tmp", true),
            new ConnectionSpecification.StaticDatasource("127.0.0.1", 5432, "postgres"),
            new ConnectionSpecification.Snowflake("DB", "acct", "wh", "us-east-1", null, "aws", true, null, "ROLE"),
            new ConnectionSpecification.Spanner("p", "i", "d"),
            new ConnectionSpecification.Databricks("host", "443", "https", "/sql/1"),
            new ConnectionSpecification.BigQuery("p", "ds"));

    private static final List<AuthenticationSpec> AUTHENTICATIONS = List.of(
            new AuthenticationSpec.NoAuth(), new AuthenticationSpec.DefaultH2(), new AuthenticationSpec.TestAuth(),
            new AuthenticationSpec.GCPApplicationDefaultCredentials(),
            new AuthenticationSpec.UsernamePassword("u", "vault/p"),
            new AuthenticationSpec.DelegatedKerberos("svc@REALM", null, true),
            new AuthenticationSpec.VaultUserNamePassword(null, "vault/u", "vault/p"),
            new AuthenticationSpec.SnowflakePublic("u", "vault/k", "vault/pp"),
            new AuthenticationSpec.ApiToken("t"), new AuthenticationSpec.MiddleTierUserNamePassword("vault/m"),
            new AuthenticationSpec.OAuth("k", "s"),
            new AuthenticationSpec.GcpWorkloadIdentityFederation("sa@x", List.of("scope")));

    private static Target target(ConnectionSpecification spec, AuthenticationSpec auth) {
        return new Target(new Database.Declared(new ConnectionDefinition("store::Conn", "store::DB", DatabaseType.H2,
                spec, auth)), new Servers.Versions(List.of("2.1", "2.2")), List.of(),
                List.of(new SetupStep.Statement("create table T(ID INT)")));
    }

    @Test
    void everyNodeKindAndParameterReadsBack() {
        Target t = target(new ConnectionSpecification.InMemory(), new AuthenticationSpec.TestAuth());
        ExecutionPlan plan = new ExecutionPlan(
                List.of(new Parameter("name", "String", new Multiplicity(1, 1), List.of()),
                        new Parameter("ids", "Integer", new Multiplicity(0, null), List.of()),
                        new Parameter("status", "model::Status", new Multiplicity(1, 1), List.of("ACTIVE", "CLOSED"))),
                new Sequence(List.of(
                        new TdsResult(List.of(new TdsColumn("name", "String", "VARCHAR(100)")),
                                new Sql("select NAME as \"name\" from T where NAME = ? and ID = ANY(?)",
                                        List.of(new Slot("name", new ExecutionPlan.Binding.One("VARCHAR", null)),
                                                new Slot("ids", new ExecutionPlan.Binding.Array("INTEGER"))), t, null)),
                        // a placeholder typed by its value (H2): its type hole, with each kind's spelling
                        new TextResult(Format.JSON, new Relation(List.of(new Column("x", "Float"))),
                                new Sql("select ID * CAST(? AS ) as x from T", List.of(new Slot("f",
                                        new ExecutionPlan.Binding.One("DECIMAL", new ExecutionPlan.TypeHole(22,
                                                java.util.Map.of(ExecutionPlan.ValueKind.DECIMAL,
                                                        new ExecutionPlan.TypeSpelling("NUMERIC",
                                                                ExecutionPlan.Digits.PRECISION_AND_SCALE),
                                                        ExecutionPlan.ValueKind.FLOATING,
                                                        new ExecutionPlan.TypeSpelling("DECFLOAT",
                                                                ExecutionPlan.Digits.PRECISION)),
                                                ExecutionPlan.ValueKind.DECIMAL)))), t, null)),
                        // a date-time's hole, a finer one cut to six digits (Postgres)
                        new TextResult(Format.JSON, new Relation(List.of(new Column("t", "DateTime"))),
                                new Sql("select CAST(? AS ) as t", List.of(new Slot("t",
                                        new ExecutionPlan.Binding.One("TIMESTAMP", new ExecutionPlan.TypeHole(17,
                                                java.util.Map.of(ExecutionPlan.ValueKind.DATE_TIME,
                                                        new ExecutionPlan.TypeSpelling("TIMESTAMP",
                                                                ExecutionPlan.Digits.NONE),
                                                        ExecutionPlan.ValueKind.DATE_TIME_NANOS,
                                                        new ExecutionPlan.TypeSpelling("TIMESTAMP",
                                                                ExecutionPlan.Digits.NONE, 6)),
                                                ExecutionPlan.ValueKind.DATE_TIME)))), t, null)),
                        new TextResult(Format.JSON, new Value("model::Person", new Multiplicity(0, null)),
                                new Sql("select json_group_array(...) from T", List.of(), t, null)),
                        new TextResult(Format.CSV, new Relation(List.of(new Column("name", "String"))),
                                new Sql("select ... from T", List.of(), t, null)),
                        new TextResult(Format.JSON_PER_ROW, new Relation(List.of()),
                                new Sql("select ... from T", List.of(), t, null)))));
        assertEquals(plan, PlanJson.read(PlanJson.write(plan)));
    }

    @Test
    void bothSetupStepKindsReadBack_aNullCellStaysNull() {
        Target t = new Target(new Database.Declared(new ConnectionDefinition("store::Conn", "store::DB",
                DatabaseType.DuckDB, new ConnectionSpecification.InMemory(), new AuthenticationSpec.TestAuth())),
                new Servers.Every(), List.of("SET TimeZone='UTC'"),
                List.of(new SetupStep.Statement("create table T(ID INTEGER, NAME VARCHAR)"),
                        new SetupStep.Rows("legend_row_load", "create temporary table legend_row_load(c0 VARCHAR, c1 VARCHAR)",
                                "insert into T(ID, NAME) select c0, c1 from legend_row_load", "drop table legend_row_load",
                                List.of(List.of("1", "O'Brien"), Arrays.asList("2", null)))));
        ExecutionPlan plan = new ExecutionPlan(List.of(), new TdsResult(List.of(), new Sql("select 1", List.of(), t, null)));
        ExecutionPlan back = PlanJson.read(PlanJson.write(plan));
        assertEquals(plan, back);
        SetupStep.Rows rows = (SetupStep.Rows) ((TdsResult) back.root()).sql().target().setup().get(1);
        assertEquals(null, rows.rows().get(1).get(1));
    }

    @Test
    void thePlatformsEngineReadsBack_withNoSetup() {
        Target t = new Target(new Database.Platform(DatabaseType.DuckDB), new Servers.Every(),
                List.of("SET TimeZone='UTC'"), List.of());
        ExecutionPlan plan = new ExecutionPlan(List.of(), new TextResult(Format.JSON,
                new Relation(List.of(new Column("value", "Integer"))), new Sql("select 1", List.of(), t, null)));
        assertEquals(plan, PlanJson.read(PlanJson.write(plan)));
    }

    @Test
    void aStatementWrittenForSomeServerVersionsNamesOne() {
        var refused = assertThrows(IllegalArgumentException.class, () -> new Servers.Versions(List.of()));
        assertTrue(refused.getMessage().contains("names at least one"), refused.getMessage());
    }

    @Test
    void rowsOfDifferentWidthsAreRefused() {
        var refused = assertThrows(IllegalArgumentException.class, () -> new SetupStep.Rows("s", "c", "i", "d",
                List.of(List.of("1", "a"), List.of("2"))));
        assertTrue(refused.getMessage().contains("a row of 1 cells among rows of 2"), refused.getMessage());
    }

    @Test
    void everyConnectionSpecificationAndAuthenticationReadsBack() {
        for (ConnectionSpecification spec : SPECIFICATIONS) {
            for (AuthenticationSpec auth : AUTHENTICATIONS) {
                ExecutionPlan plan = new ExecutionPlan(List.of(), new TdsResult(List.of(),
                        new Sql("select 1", List.of(), target(spec, auth), null)));
                assertEquals(plan, PlanJson.read(PlanJson.write(plan)), spec + " / " + auth);
            }
        }
    }

    @Test
    void theTypedTreeIsNotWrittenYet() {
        Target t = target(new ConnectionSpecification.InMemory(), new AuthenticationSpec.TestAuth());
        String json = PlanJson.write(new ExecutionPlan(List.of(), new TdsResult(List.of(),
                new Sql("select 1", List.of(), t, null))));
        assertTrue(json.startsWith("{\"format\":\"legend-lite-plan\",\"version\":4,"), json);
    }

    @Test
    void anotherFormatIsRefusedByName() {
        var refused = assertThrows(IllegalArgumentException.class,
                () -> PlanJson.read("{\"_type\":\"simple\",\"rootExecutionNode\":{}}"));
        assertTrue(refused.getMessage().contains("not a legend-lite-plan v4 plan"), refused.getMessage());
    }

    @Test
    void aParameterWithoutItsEnumValuesIsRefused_neverReadAsNone() {
        Target t = target(new ConnectionSpecification.InMemory(), new AuthenticationSpec.TestAuth());
        String json = PlanJson.write(new ExecutionPlan(List.of(new Parameter("s", "model::Status",
                new Multiplicity(1, 1), List.of("A"))), new TdsResult(List.of(), new Sql("select 1", List.of(), t, null))))
                .replace(",\"enumValues\":[\"A\"]", "");
        assertThrows(RuntimeException.class, () -> PlanJson.read(json));
    }

    @Test
    void anEarlierVersionIsRefused() {
        for (int version = 1; version <= 3; version++) {
            int v = version;
            var refused = assertThrows(IllegalArgumentException.class, () -> PlanJson.read(
                    "{\"format\":\"legend-lite-plan\",\"version\":" + v + ",\"parameters\":[],\"root\":{}}"));
            assertTrue(refused.getMessage().contains("(format legend-lite-plan, version " + v + ")"),
                    refused.getMessage());
        }
    }

    @Test
    void anUnknownFormatOfTextIsRefusedByName() {
        Target t = target(new ConnectionSpecification.InMemory(), new AuthenticationSpec.TestAuth());
        String json = PlanJson.write(new ExecutionPlan(List.of(), new TextResult(Format.CSV, new Relation(List.of()),
                new Sql("select 1", List.of(), t, null)))).replace("\"format\":\"CSV\"", "\"format\":\"XML\"");
        var refused = assertThrows(IllegalArgumentException.class, () -> PlanJson.read(json));
        assertTrue(refused.getMessage().contains("unknown text result format 'XML'"), refused.getMessage());
    }
}
