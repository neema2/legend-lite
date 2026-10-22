// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.model;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A connection written as a record's fields, and read back: the lite plan format's connection
 * ({@code executionplan.PlanJson}), each specification and authentication by its tag and its own fields. The model's
 * vocabulary carries every database's name (D9); the plan format names none (the dialect program's Phase 5,
 * 2026-10-16: until then PlanJson held this codec). The model depends on no JSON: the format reads through
 * {@link Fields} and writes the maps this returns.
 */
public final class ConnectionWire {

    private ConnectionWire() {
    }

    /** A written record's fields, as the format reads them. */
    public interface Fields {

        /** The text at {@code key}, which is present. */
        String string(String key);

        /** The text at {@code key}, or null when it is absent. */
        @com.legend.base.Nullable String optionalString(String key);

        /** The integer at {@code key}, which is present. */
        int integer(String key);

        /** The boolean at {@code key}, which is present. */
        boolean bool(String key);

        /** Whether {@code key} is present. */
        boolean has(String key);

        /** The texts at {@code key}, which is present. */
        List<String> strings(String key);

        /** The record at {@code key}, which is present. */
        Fields object(String key);
    }

    /** The {@code _type} tags, one spelling for the writer and the reader: an exhaustive switch over each, an unknown
     *  tag refused by name ({@link #named}). */
    enum SpecificationTag { InMemory, LocalFile, LocalH2, EmbeddedH2, StaticDatasource, Snowflake, Spanner, Databricks,
        BigQuery, Deephaven }

    enum AuthenticationTag { NoAuth, DefaultH2, TestAuth, GCPApplicationDefaultCredentials, UsernamePassword,
        DelegatedKerberos, VaultUserNamePassword, SnowflakePublic, ApiToken, MiddleTierUserNamePassword, OAuth,
        GcpWorkloadIdentityFederation, PreSharedKey }

    private static <E extends Enum<E>> E named(E[] family, String name, String what) {
        for (E e : family) {
            if (e.name().equals(name)) {
                return e;
            }
        }
        throw new IllegalArgumentException("unknown " + what + " '" + name + "'");
    }

    /** {@code c} as its fields, in order: its name, store, declared type, specification and authentication, a field the
     *  definition leaves unset omitted. */
    public static Map<String, Object> write(ConnectionDefinition c) {
        Map<String, Object> o = new LinkedHashMap<>();
        o.put("name", c.qualifiedName());
        if (c.storeName() != null) {
            o.put("store", c.storeName());
        }
        o.put("databaseType", c.databaseType().name());
        o.put("specification", specification(c.specification()));
        o.put("authentication", authentication(c.authentication()));
        return o;
    }

    private static Map<String, Object> specification(ConnectionSpecification spec) {
        Map<String, Object> o = new LinkedHashMap<>();
        switch (spec) {
            case ConnectionSpecification.InMemory x -> o.put("_type", SpecificationTag.InMemory.name());
            case ConnectionSpecification.LocalFile x -> {
                o.put("_type", SpecificationTag.LocalFile.name());
                o.put("path", x.path());
            }
            case ConnectionSpecification.LocalH2 x -> {
                o.put("_type", SpecificationTag.LocalH2.name());
                putIfSet(o, "url", x.url());
                putIfSet(o, "testDataSetupCsv", x.testDataSetupCsv());
                if (x.testDataSetupSqls() != null) {
                    o.put("testDataSetupSqls", x.testDataSetupSqls());
                }
            }
            case ConnectionSpecification.EmbeddedH2 x -> {
                o.put("_type", SpecificationTag.EmbeddedH2.name());
                o.put("databaseName", x.databaseName());
                o.put("directory", x.directory());
                o.put("autoServerMode", x.autoServerMode());
            }
            case ConnectionSpecification.StaticDatasource x -> {
                o.put("_type", SpecificationTag.StaticDatasource.name());
                o.put("host", x.host());
                o.put("port", x.port());
                o.put("database", x.database());
            }
            case ConnectionSpecification.Snowflake x -> {
                o.put("_type", SpecificationTag.Snowflake.name());
                o.put("databaseName", x.databaseName());
                o.put("accountName", x.accountName());
                o.put("warehouseName", x.warehouseName());
                o.put("region", x.region());
                putIfSet(o, "accountType", x.accountType());
                putIfSet(o, "cloudType", x.cloudType());
                if (x.enableQueryTags() != null) {
                    o.put("enableQueryTags", x.enableQueryTags());
                }
                putIfSet(o, "organization", x.organization());
                putIfSet(o, "role", x.role());
            }
            case ConnectionSpecification.Spanner x -> {
                o.put("_type", SpecificationTag.Spanner.name());
                o.put("projectId", x.projectId());
                o.put("instanceId", x.instanceId());
                o.put("databaseId", x.databaseId());
            }
            case ConnectionSpecification.Databricks x -> {
                o.put("_type", SpecificationTag.Databricks.name());
                o.put("hostname", x.hostname());
                o.put("port", x.port());
                o.put("protocol", x.protocol());
                o.put("httpPath", x.httpPath());
            }
            case ConnectionSpecification.BigQuery x -> {
                o.put("_type", SpecificationTag.BigQuery.name());
                o.put("projectId", x.projectId());
                o.put("defaultDataset", x.defaultDataset());
            }
            case ConnectionSpecification.Deephaven x -> {
                o.put("_type", SpecificationTag.Deephaven.name());
                o.put("host", x.host());
                o.put("port", x.port());
                o.put("useTls", x.useTls());
            }
        }
        return o;
    }

    private static Map<String, Object> authentication(AuthenticationSpec auth) {
        Map<String, Object> o = new LinkedHashMap<>();
        switch (auth) {
            case AuthenticationSpec.NoAuth x -> o.put("_type", AuthenticationTag.NoAuth.name());
            case AuthenticationSpec.DefaultH2 x -> o.put("_type", AuthenticationTag.DefaultH2.name());
            case AuthenticationSpec.TestAuth x -> o.put("_type", AuthenticationTag.TestAuth.name());
            case AuthenticationSpec.GCPApplicationDefaultCredentials x ->
                    o.put("_type", AuthenticationTag.GCPApplicationDefaultCredentials.name());
            case AuthenticationSpec.UsernamePassword x -> {
                o.put("_type", AuthenticationTag.UsernamePassword.name());
                o.put("username", x.username());
                o.put("passwordVaultRef", x.passwordVaultRef());
            }
            case AuthenticationSpec.DelegatedKerberos x -> {
                o.put("_type", AuthenticationTag.DelegatedKerberos.name());
                putIfSet(o, "serverPrincipal", x.serverPrincipal());
                putIfSet(o, "kerberosRemoteServiceName", x.kerberosRemoteServiceName());
                if (x.kerberosUseCanonicalHostname() != null) {
                    o.put("kerberosUseCanonicalHostname", x.kerberosUseCanonicalHostname());
                }
            }
            case AuthenticationSpec.VaultUserNamePassword x -> {
                o.put("_type", AuthenticationTag.VaultUserNamePassword.name());
                putIfSet(o, "baseVaultReference", x.baseVaultReference());
                o.put("userNameVaultReference", x.userNameVaultReference());
                o.put("passwordVaultReference", x.passwordVaultReference());
            }
            case AuthenticationSpec.SnowflakePublic x -> {
                o.put("_type", AuthenticationTag.SnowflakePublic.name());
                o.put("publicUserName", x.publicUserName());
                o.put("privateKeyVaultReference", x.privateKeyVaultReference());
                o.put("passPhraseVaultReference", x.passPhraseVaultReference());
            }
            case AuthenticationSpec.ApiToken x -> {
                o.put("_type", AuthenticationTag.ApiToken.name());
                o.put("apiToken", x.apiToken());
            }
            case AuthenticationSpec.MiddleTierUserNamePassword x -> {
                o.put("_type", AuthenticationTag.MiddleTierUserNamePassword.name());
                o.put("vaultReference", x.vaultReference());
            }
            case AuthenticationSpec.OAuth x -> {
                o.put("_type", AuthenticationTag.OAuth.name());
                o.put("oauthKey", x.oauthKey());
                o.put("scopeName", x.scopeName());
            }
            case AuthenticationSpec.GcpWorkloadIdentityFederation x -> {
                o.put("_type", AuthenticationTag.GcpWorkloadIdentityFederation.name());
                o.put("serviceAccountEmail", x.serviceAccountEmail());
                o.put("additionalGcpScopes", x.additionalGcpScopes());
            }
            case AuthenticationSpec.PreSharedKey x -> {
                o.put("_type", AuthenticationTag.PreSharedKey.name());
                o.put("keyVaultReference", x.keyVaultReference());
            }
        }
        return o;
    }

    private static void putIfSet(Map<String, Object> o, String key, @com.legend.base.Nullable String value) {
        if (value != null) {
            o.put(key, value);
        }
    }

    /** The connection {@link #write} wrote as {@code f}; an unknown tag or database type refused by name. */
    public static ConnectionDefinition read(Fields f) {
        return new ConnectionDefinition(f.string("name"), f.optionalString("store"),
                databaseType(f.string("databaseType")), readSpecification(f.object("specification")),
                readAuthentication(f.object("authentication")));
    }

    /** The declared database type named {@code name}; an unknown one refused by name. */
    public static ConnectionDefinition.DatabaseType databaseType(String name) {
        return named(ConnectionDefinition.DatabaseType.values(), name, "database type");
    }

    private static ConnectionSpecification readSpecification(Fields o) {
        return switch (named(SpecificationTag.values(), o.string("_type"), "connection specification _type")) {
            case InMemory -> new ConnectionSpecification.InMemory();
            case LocalFile -> new ConnectionSpecification.LocalFile(o.string("path"));
            case LocalH2 -> new ConnectionSpecification.LocalH2(o.optionalString("url"),
                    o.optionalString("testDataSetupCsv"),
                    o.has("testDataSetupSqls") ? o.strings("testDataSetupSqls") : null);
            case EmbeddedH2 -> new ConnectionSpecification.EmbeddedH2(o.string("databaseName"),
                    o.string("directory"), o.bool("autoServerMode"));
            case StaticDatasource -> new ConnectionSpecification.StaticDatasource(o.string("host"),
                    o.integer("port"), o.string("database"));
            case Snowflake -> new ConnectionSpecification.Snowflake(o.string("databaseName"),
                    o.string("accountName"), o.string("warehouseName"), o.string("region"),
                    o.optionalString("accountType"), o.optionalString("cloudType"),
                    o.has("enableQueryTags") ? o.bool("enableQueryTags") : null,
                    o.optionalString("organization"), o.optionalString("role"));
            case Spanner -> new ConnectionSpecification.Spanner(o.string("projectId"), o.string("instanceId"),
                    o.string("databaseId"));
            case Databricks -> new ConnectionSpecification.Databricks(o.string("hostname"), o.string("port"),
                    o.string("protocol"), o.string("httpPath"));
            case BigQuery -> new ConnectionSpecification.BigQuery(o.string("projectId"),
                    o.string("defaultDataset"));
            case Deephaven -> new ConnectionSpecification.Deephaven(o.string("host"), o.integer("port"),
                    o.bool("useTls"));
        };
    }

    private static AuthenticationSpec readAuthentication(Fields o) {
        return switch (named(AuthenticationTag.values(), o.string("_type"), "authentication _type")) {
            case NoAuth -> new AuthenticationSpec.NoAuth();
            case DefaultH2 -> new AuthenticationSpec.DefaultH2();
            case TestAuth -> new AuthenticationSpec.TestAuth();
            case GCPApplicationDefaultCredentials -> new AuthenticationSpec.GCPApplicationDefaultCredentials();
            case UsernamePassword -> new AuthenticationSpec.UsernamePassword(o.string("username"),
                    o.string("passwordVaultRef"));
            case DelegatedKerberos -> new AuthenticationSpec.DelegatedKerberos(o.optionalString("serverPrincipal"),
                    o.optionalString("kerberosRemoteServiceName"),
                    o.has("kerberosUseCanonicalHostname") ? o.bool("kerberosUseCanonicalHostname") : null);
            case VaultUserNamePassword -> new AuthenticationSpec.VaultUserNamePassword(
                    o.optionalString("baseVaultReference"), o.string("userNameVaultReference"),
                    o.string("passwordVaultReference"));
            case SnowflakePublic -> new AuthenticationSpec.SnowflakePublic(o.string("publicUserName"),
                    o.string("privateKeyVaultReference"), o.string("passPhraseVaultReference"));
            case ApiToken -> new AuthenticationSpec.ApiToken(o.string("apiToken"));
            case MiddleTierUserNamePassword -> new AuthenticationSpec.MiddleTierUserNamePassword(
                    o.string("vaultReference"));
            case OAuth -> new AuthenticationSpec.OAuth(o.string("oauthKey"), o.string("scopeName"));
            case GcpWorkloadIdentityFederation -> new AuthenticationSpec.GcpWorkloadIdentityFederation(
                    o.string("serviceAccountEmail"), o.strings("additionalGcpScopes"));
            case PreSharedKey -> new AuthenticationSpec.PreSharedKey(o.string("keyVaultReference"));
        };
    }
}
