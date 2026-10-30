// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.model;

import com.legend.executionplan.ExecutionPlan;
import com.legend.executionplan.ExecutionPlan.Database;
import com.legend.executionplan.ExecutionPlan.Servers;
import com.legend.executionplan.ExecutionPlan.Sql;
import com.legend.executionplan.ExecutionPlan.Target;
import com.legend.executionplan.ExecutionPlan.TdsResult;
import com.legend.executionplan.PlanJson;
import com.legend.model.ConnectionDefinition.DatabaseType;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * The connection codec ({@link ConnectionWire}, the vocabulary's) reads back what it writes, through the lite plan
 * format: every database type, connection specification and authentication (PlanJsonTest's case until the dialect
 * program's Phase 6, 2026-10-16, when the codec had moved into the model).
 */
class ConnectionWireTest {

    private static final List<ConnectionSpecification> SPECIFICATIONS = List.of(
            new ConnectionSpecification.InMemory(),
            new ConnectionSpecification.LocalFile("/tmp/x.duckdb"),
            new ConnectionSpecification.LocalH2(null),
            new ConnectionSpecification.LocalH2("jdbc:h2:mem:x", "default\nT\nID\n1", List.of("create table T(ID INT)")),
            new ConnectionSpecification.EmbeddedH2("db", "/tmp", true),
            new ConnectionSpecification.StaticDatasource("127.0.0.1", 5432, "postgres"),
            new ConnectionSpecification.Trino("127.0.0.1", 8080, "memory", "default", null, false),
            new ConnectionSpecification.Trino("h", 443, null, null, "tag", true),
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

    @Test
    void everyConnectionSpecificationAndAuthenticationReadsBack() {
        for (DatabaseType type : DatabaseType.values()) {
            for (ConnectionSpecification spec : SPECIFICATIONS) {
                for (AuthenticationSpec auth : AUTHENTICATIONS) {
                    Target t = new Target(new Database.Declared(new ConnectionDefinition("store::Conn", "store::DB",
                            type, spec, auth)), new Servers.Every(), List.of(), List.of());
                    ExecutionPlan plan = new ExecutionPlan(List.of(), new TdsResult(TdsResult.Shape.RELATION, List.of(),
                            new Sql("select 1", List.of(), t, null)));
                    assertEquals(plan, PlanJson.read(PlanJson.write(plan)), type + " / " + spec + " / " + auth);
                }
            }
        }
    }
}
