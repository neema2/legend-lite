// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.executionplan;

import com.legend.executionplan.ExecutionPlan.EnumValue;
import com.legend.executionplan.ExecutionPlan.JsonResult;
import com.legend.executionplan.ExecutionPlan.Multiplicity;
import com.legend.executionplan.ExecutionPlan.Parameter;
import com.legend.executionplan.ExecutionPlan.Sequence;
import com.legend.executionplan.ExecutionPlan.Slot;
import com.legend.executionplan.ExecutionPlan.Sql;
import com.legend.executionplan.ExecutionPlan.Target;
import com.legend.executionplan.ExecutionPlan.TdsColumn;
import com.legend.executionplan.ExecutionPlan.TdsResult;
import com.legend.model.AuthenticationSpec;
import com.legend.model.ConnectionDefinition;
import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.model.ConnectionSpecification;
import org.junit.jupiter.api.Test;

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
        return new Target(new ConnectionDefinition("store::Conn", "store::DB", DatabaseType.H2, spec, auth),
                List.of("create table T(ID INT)"), "c_0123456789abcdef");
    }

    @Test
    void everyNodeKindAndParameterReadsBack() {
        Target t = target(new ConnectionSpecification.InMemory(), new AuthenticationSpec.TestAuth());
        ExecutionPlan plan = new ExecutionPlan(
                List.of(new Parameter("name", "String", new Multiplicity(1, 1), List.of()),
                        new Parameter("ids", "Integer", new Multiplicity(0, null), List.of()),
                        new Parameter("status", "model::Status", new Multiplicity(1, 1),
                                List.of(new EnumValue("ACTIVE", List.of("A", 1L)), new EnumValue("CLOSED", List.of("C"))))),
                new Sequence(List.of(
                        new TdsResult(List.of(new TdsColumn("name", "String", "VARCHAR(100)")),
                                new Sql("select NAME as \"name\" from T where NAME = ? and ID = ANY(?)",
                                        List.of(new Slot("name", null), new Slot("ids", "INTEGER")), t, null)),
                        new JsonResult("model::Person", new Multiplicity(0, null),
                                new Sql("select json_group_array(...) from T", List.of(), t, null)))));
        assertEquals(plan, PlanJson.read(PlanJson.write(plan)));
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
        assertTrue(json.startsWith("{\"format\":\"legend-lite-plan\",\"version\":1,"), json);
    }

    @Test
    void anotherFormatIsRefusedByName() {
        var refused = assertThrows(IllegalArgumentException.class,
                () -> PlanJson.read("{\"_type\":\"simple\",\"rootExecutionNode\":{}}"));
        assertTrue(refused.getMessage().contains("not a legend-lite-plan v1 plan"), refused.getMessage());
    }
}
