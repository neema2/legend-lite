// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.planner;

import com.legend.executionplan.AuthenticationSpec;
import com.legend.executionplan.Connection;
import com.legend.executionplan.ConnectionSpecification;
import com.legend.executionplan.DatabaseType;
import com.legend.model.ConnectionDefinition;

/**
 * THE PLAN'S CONNECTION DESCRIPTION, COPIED FROM THE MODEL'S (docs/PLAN_AS_DATA_2026_11_07.md 2.6, the design's step 7,
 * 2026-11-08): each variant to the plan's variant of the same name, field by field -- the plan holds no model type, and
 * the model depends on no plan (invariant 6j). An exhaustive switch each way: a variant the plan lacks does not compile.
 */
public final class ConnectionPlans {

    private ConnectionPlans() {
    }

    /** The plan's description of {@code d}. */
    public static Connection of(ConnectionDefinition d) {
        return new Connection(d.qualifiedName(), d.storeName(), type(d.databaseType()), specification(d.specification()),
                authentication(d.authentication()));
    }

    /** The plan's name of the model's database type (the same constant). */
    public static DatabaseType type(ConnectionDefinition.DatabaseType t) {
        return DatabaseType.valueOf(t.name());
    }

    /** The model's name of the plan's database type (the same constant). */
    public static ConnectionDefinition.DatabaseType model(DatabaseType t) {
        return ConnectionDefinition.DatabaseType.valueOf(t.name());
    }

    private static ConnectionSpecification specification(com.legend.model.ConnectionSpecification s) {
        return switch (s) {
            case com.legend.model.ConnectionSpecification.InMemory x -> new ConnectionSpecification.InMemory();
            case com.legend.model.ConnectionSpecification.LocalFile x -> new ConnectionSpecification.LocalFile(x.path());
            case com.legend.model.ConnectionSpecification.LocalH2 x -> new ConnectionSpecification.LocalH2(x.url(),
                    x.testDataSetupCsv(), x.testDataSetupSqls());
            case com.legend.model.ConnectionSpecification.EmbeddedH2 x -> new ConnectionSpecification.EmbeddedH2(
                    x.databaseName(), x.directory(), x.autoServerMode());
            case com.legend.model.ConnectionSpecification.StaticDatasource x ->
                    new ConnectionSpecification.StaticDatasource(x.host(), x.port(), x.database());
            case com.legend.model.ConnectionSpecification.Trino x -> new ConnectionSpecification.Trino(x.host(),
                    x.port(), x.catalog(), x.schema(), x.clientTags(), x.ssl());
            case com.legend.model.ConnectionSpecification.Snowflake x -> new ConnectionSpecification.Snowflake(
                    x.databaseName(), x.accountName(), x.warehouseName(), x.region(), x.accountType(), x.cloudType(),
                    x.enableQueryTags(), x.organization(), x.role());
            case com.legend.model.ConnectionSpecification.Spanner x -> new ConnectionSpecification.Spanner(
                    x.projectId(), x.instanceId(), x.databaseId());
            case com.legend.model.ConnectionSpecification.Databricks x -> new ConnectionSpecification.Databricks(
                    x.hostname(), x.port(), x.protocol(), x.httpPath());
            case com.legend.model.ConnectionSpecification.BigQuery x -> new ConnectionSpecification.BigQuery(
                    x.projectId(), x.defaultDataset());
        };
    }

    private static AuthenticationSpec authentication(com.legend.model.AuthenticationSpec a) {
        return switch (a) {
            case com.legend.model.AuthenticationSpec.NoAuth x -> new AuthenticationSpec.NoAuth();
            case com.legend.model.AuthenticationSpec.DefaultH2 x -> new AuthenticationSpec.DefaultH2();
            case com.legend.model.AuthenticationSpec.UsernamePassword x -> new AuthenticationSpec.UsernamePassword(
                    x.username(), x.passwordVaultRef());
            case com.legend.model.AuthenticationSpec.TestAuth x -> new AuthenticationSpec.TestAuth();
            case com.legend.model.AuthenticationSpec.DelegatedKerberos x -> new AuthenticationSpec.DelegatedKerberos(
                    x.serverPrincipal(), x.kerberosRemoteServiceName(), x.kerberosUseCanonicalHostname());
            case com.legend.model.AuthenticationSpec.VaultUserNamePassword x ->
                    new AuthenticationSpec.VaultUserNamePassword(x.baseVaultReference(), x.userNameVaultReference(),
                            x.passwordVaultReference());
            case com.legend.model.AuthenticationSpec.SnowflakePublic x -> new AuthenticationSpec.SnowflakePublic(
                    x.publicUserName(), x.privateKeyVaultReference(), x.passPhraseVaultReference());
            case com.legend.model.AuthenticationSpec.GCPApplicationDefaultCredentials x ->
                    new AuthenticationSpec.GCPApplicationDefaultCredentials();
            case com.legend.model.AuthenticationSpec.ApiToken x -> new AuthenticationSpec.ApiToken(x.apiToken());
            case com.legend.model.AuthenticationSpec.MiddleTierUserNamePassword x ->
                    new AuthenticationSpec.MiddleTierUserNamePassword(x.vaultReference());
            case com.legend.model.AuthenticationSpec.OAuth x -> new AuthenticationSpec.OAuth(x.oauthKey(),
                    x.scopeName());
            case com.legend.model.AuthenticationSpec.GcpWorkloadIdentityFederation x ->
                    new AuthenticationSpec.GcpWorkloadIdentityFederation(x.serviceAccountEmail(),
                            x.additionalGcpScopes());
        };
    }
}
