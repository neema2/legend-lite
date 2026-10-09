// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol;

import java.util.ArrayList;
import java.util.List;

import static com.legend.protocol.Composing.TAB;
import static com.legend.protocol.Composing.convertString;
import static com.legend.protocol.Composing.tab;

/**
 * A {@code RelationalDatabaseConnection} as upstream prints it: the relational extension's connection
 * value composer, its datasource specifications and authentication strategies, and every database
 * extension's (Snowflake, BigQuery, Databricks, Spanner, Trino, Redshift, Athena, Aurora, MemSQL,
 * Oracle, DuckDB) -- over the record ({@link Protocol.PRelationalDatabaseConnection}; the protocol program's leg 2,
 * step 3). Each specification and strategy is a block of {@code key: value;} lines: its keyword and its fields, each
 * field the grammar's key, the record's value, how it prints and whether it may be absent.
 */
final class RelationalConnectionComposer {

    /** How a field's value prints. */
    enum Kind {
        /** {@code convertString(value, true)}. */
        STRING,
        /** The value as Java's {@code toString} gives it. */
        RAW,
        /** {@code convertString(String.valueOf(value), true)}. */
        STRING_OF,
        /** {@code [ 'a', 'b' ]} on one line. */
        INLINE_LIST,
        /** {@code [} then each string on its own line, closed one tab deeper; printed only when non-empty. */
        LINE_LIST
    }

    /** One {@code key: value;} line: the grammar's key, the record's value, how it prints, whether it may be absent. */
    private record Field(String key, @com.legend.base.Nullable Object value, Kind kind, boolean optional) {
    }

    private static Field req(String key, @com.legend.base.Nullable Object value, Kind kind) {
        return new Field(key, value, kind, false);
    }

    private static Field opt(String key, @com.legend.base.Nullable Object value, Kind kind) {
        return new Field(key, value, kind, true);
    }

    private static final String TRINO = "Trino";

    private RelationalConnectionComposer() {
    }

    /** The connection's body, at the context's indentation {@code i}. */
    static String connection(Protocol.PRelationalDatabaseConnection c, String i) {
        StringBuilder b = new StringBuilder(i).append("{\n");
        if (c.element() != null) {
            b.append(i).append(TAB).append("store: ").append(c.element()).append(";\n");
        }
        b.append(i).append(TAB).append("type: ").append(c.databaseType()).append(";\n");
        String timeZone = c.timeZone();
        if (timeZone != null) {
            b.append(i).append(TAB).append("timezone: ")
                    .append(utcOffset(timeZone) ? timeZone : convertString(timeZone, true)).append(";\n");
        }
        if (c.quoteIdentifiers() != null) {
            b.append(i).append(TAB).append("quoteIdentifiers: ").append(c.quoteIdentifiers()).append(";\n");
        }
        String specification = specification(c.datasourceSpecification(), i);
        String auth = authentication(c.authenticationStrategy(), i);
        if (c.localMode() != null && c.localMode()) {
            b.append(i).append(TAB).append("mode: local;\n");
        } else {
            b.append(i).append(TAB).append("specification: ").append(specification).append(";\n");
            b.append(i).append(TAB).append("auth: ").append(auth).append(";\n");
        }
        if (c.queryTimeOutInSeconds() != null) {
            b.append(i).append(TAB).append("queryTimeOutInSeconds: ").append(c.queryTimeOutInSeconds()).append(";\n");
        }
        if (!c.postProcessors().isEmpty()) {
            List<String> ps = new ArrayList<>();
            for (Protocol.PPostProcessor p : c.postProcessors()) {
                ps.add(postProcessor(p, i));
            }
            b.append(i).append(TAB).append("postProcessors:\n").append(TAB).append("[\n").append(String.join(",\n", ps))
                    .append("\n").append(TAB).append("];\n");
        }
        List<Protocol.PGenerationFeaturesConfig> configs = c.queryGenerationConfigs();
        if (configs != null && !configs.isEmpty()) {
            List<String> cs = new ArrayList<>();
            for (Protocol.PGenerationFeaturesConfig q : configs) {
                cs.add(i + tab(2) + "GenerationFeaturesConfig\n" + i + tab(2) + "{\n"
                        + i + tab(3) + "enabled: [" + features(q.enabled()) + "];\n"
                        + i + tab(3) + "disabled: [" + features(q.disabled()) + "];\n"
                        + i + tab(2) + "}");
            }
            b.append(i).append(TAB).append("queryGenerationConfigs: [\n").append(String.join(",\n", cs)).append("\n")
                    .append(i).append(TAB).append("];\n");
        }
        return b.append(i).append("}").toString();
    }

    /** A bare offset from UTC ({@code [+-]dddd}), which the grammar takes unquoted; any other zone is quoted. */
    private static boolean utcOffset(String zone) {
        if (zone.length() != 5 || (zone.charAt(0) != '+' && zone.charAt(0) != '-')) {
            return false;
        }
        for (int k = 1; k < 5; k++) {
            if (zone.charAt(k) < '0' || zone.charAt(k) > '9') {
                return false;
            }
        }
        return true;
    }

    private static String specification(Protocol.PDatasourceSpec spec, String i) {
        return switch (spec) {
            case Protocol.PH2Local s -> block("LocalH2", i,
                    opt("testDataSetupCSV", s.testDataSetupCsv(), Kind.STRING),
                    opt("testDataSetupSqls", s.testDataSetupSqls(), Kind.LINE_LIST));
            case Protocol.PH2EmbeddedSpec s -> block("EmbeddedH2", i, req("name", s.databaseName(), Kind.STRING),
                    req("directory", s.directory(), Kind.STRING), req("autoServerMode", s.autoServerMode(), Kind.RAW));
            case Protocol.PStaticSpec s -> block("Static", i, req("name", s.databaseName(), Kind.STRING),
                    req("host", s.host(), Kind.STRING), req("port", s.port(), Kind.RAW));
            case Protocol.PAthenaSpec s -> block("Athena", i, req("region", s.region(), Kind.STRING),
                    opt("database", s.database(), Kind.STRING), opt("workGroup", s.workGroup(), Kind.STRING),
                    opt("outputLocation", s.outputLocation(), Kind.STRING), opt("catalog", s.catalog(), Kind.STRING),
                    opt("athenaEndpoint", s.athenaEndpoint(), Kind.STRING));
            case Protocol.PAuroraSpec s -> block("Aurora", i, req("host", s.host(), Kind.STRING),
                    req("port", s.port(), Kind.RAW), req("name", s.name(), Kind.STRING),
                    opt("clusterInstanceHostPattern", s.clusterInstanceHostPattern(), Kind.STRING));
            case Protocol.PGlobalAuroraSpec s -> block("GlobalAurora", i, req("host", s.host(), Kind.STRING),
                    req("port", s.port(), Kind.RAW), req("name", s.name(), Kind.STRING),
                    req("region", s.region(), Kind.STRING),
                    req("globalClusterInstanceHostPatterns", s.globalClusterInstanceHostPatterns(), Kind.INLINE_LIST));
            case Protocol.PBigQuerySpec s -> block("BigQuery", i, req("projectId", s.projectId(), Kind.STRING),
                    req("defaultDataset", s.defaultDataset(), Kind.STRING), opt("proxyHost", s.proxyHost(), Kind.STRING),
                    opt("proxyPort", s.proxyPort(), Kind.STRING));
            case Protocol.PDatabricksSpec s -> block("Databricks", i, req("hostname", s.hostname(), Kind.STRING),
                    req("port", s.port(), Kind.STRING), req("protocol", s.protocol(), Kind.STRING),
                    req("httpPath", s.httpPath(), Kind.STRING));
            case Protocol.PDuckDBSpec s -> block("DuckDB", i, req("path", s.path(), Kind.STRING));
            case Protocol.PMemSqlSpec s -> block("MemSql", i, req("host", s.host(), Kind.STRING),
                    req("port", s.port(), Kind.STRING_OF), req("databaseName", s.databaseName(), Kind.STRING),
                    req("useSsl", s.useSsl(), Kind.STRING_OF));
            case Protocol.POracleSpec s -> block("Oracle", i, req("host", s.host(), Kind.STRING),
                    req("port", s.port(), Kind.RAW), req("serviceName", s.serviceName(), Kind.STRING));
            case Protocol.PRedshiftSpec s -> block("Redshift", i, req("host", s.host(), Kind.STRING),
                    req("port", s.port(), Kind.RAW), req("name", s.databaseName(), Kind.STRING),
                    req("region", s.region(), Kind.STRING), req("clusterID", s.clusterID(), Kind.STRING),
                    req("endpointURL", s.endpointURL(), Kind.STRING));
            case Protocol.PSnowflakeSpec s -> block("Snowflake", i, req("name", s.databaseName(), Kind.STRING),
                    req("account", s.accountName(), Kind.STRING), req("warehouse", s.warehouseName(), Kind.STRING),
                    req("region", s.region(), Kind.STRING), opt("cloudType", s.cloudType(), Kind.STRING),
                    opt("quotedIdentifiersIgnoreCase", s.quotedIdentifiersIgnoreCase(), Kind.RAW),
                    opt("enableQueryTags", s.enableQueryTags(), Kind.RAW), opt("proxyHost", s.proxyHost(), Kind.STRING),
                    opt("proxyPort", s.proxyPort(), Kind.STRING), opt("nonProxyHosts", s.nonProxyHosts(), Kind.STRING),
                    opt("tempTableDb", s.tempTableDb(), Kind.STRING),
                    opt("tempTableSchema", s.tempTableSchema(), Kind.STRING),
                    opt("accountType", s.accountType(), Kind.RAW), opt("organization", s.organization(), Kind.STRING),
                    opt("role", s.role(), Kind.STRING));
            case Protocol.PSpannerSpec s -> block("Spanner", i, req("projectId", s.projectId(), Kind.STRING),
                    req("instanceId", s.instanceId(), Kind.STRING), req("databaseId", s.databaseId(), Kind.STRING),
                    opt("proxyHost", s.proxyHost(), Kind.STRING), opt("proxyPort", s.proxyPort(), Kind.RAW));
            case Protocol.PTrinoSpec s -> trino(s, i);
            case Protocol.PSQLiteSpec s -> throw Composing.refused("no composer rule for a datasource specification of"
                    + " _type 'sqlite'");
        };
    }

    private static String authentication(Protocol.PAuthStrategy auth, String i) {
        return switch (auth) {
            case Protocol.PTestAuth a -> "Test";
            case Protocol.PH2Default a -> "DefaultH2";
            case Protocol.PGCPApplicationDefaultCredentials a -> "GCPApplicationDefaultCredentials";
            case Protocol.PApiToken a -> block("ApiToken", i, req("apiToken", a.apiToken(), Kind.STRING));
            case Protocol.PUserNamePassword a -> block("UserNamePassword", i,
                    opt("baseVaultReference", a.baseVaultReference(), Kind.STRING),
                    req("userNameVaultReference", a.userNameVaultReference(), Kind.STRING),
                    req("passwordVaultReference", a.passwordVaultReference(), Kind.STRING));
            case Protocol.PGcpWifAuth a -> block("GCPWorkloadIdentityFederation", i,
                    req("serviceAccountEmail", a.serviceAccountEmail(), Kind.STRING),
                    opt("additionalGcpScopes", a.additionalGcpScopes(), Kind.LINE_LIST));
            case Protocol.POAuth a -> block("OAuth", i, req("oauthKey", a.oauthKey(), Kind.STRING),
                    req("scopeName", a.scopeName(), Kind.STRING));
            case Protocol.PSnowflakePublic a -> block("SnowflakePublic", i,
                    req("publicUserName", a.publicUserName(), Kind.STRING),
                    req("privateKeyVaultReference", a.privateKeyVaultReference(), Kind.STRING),
                    req("passPhraseVaultReference", a.passPhraseVaultReference(), Kind.STRING));
            case Protocol.PTrinoKerberosAuth a -> block("TrinoDelegatedKerberos", i,
                    opt("serverPrincipal", a.serverPrincipal(), Kind.RAW),
                    opt("kerberosUseCanonicalHostname", a.kerberosUseCanonicalHostname(), Kind.RAW),
                    req("kerberosRemoteServiceName", a.kerberosRemoteServiceName(), Kind.STRING));
            // a strategy whose block is printed only when its one optional field is present
            case Protocol.PDelegatedKerberos a -> a.serverPrincipal() == null ? "DelegatedKerberos"
                    : block("DelegatedKerberos", i, opt("serverPrincipal", a.serverPrincipal(), Kind.STRING));
            case Protocol.PMiddleTierUserNamePassword a -> a.vaultReference() == null ? "MiddleTierUserNamePassword"
                    : block("MiddleTierUserNamePassword", i, opt("vaultReference", a.vaultReference(), Kind.STRING));
        };
    }

    /** A {@code Keyword} then its {@code { key: value; }} block, at indentation {@code i} plus one tab; no fields at all
     *  is the keyword alone. */
    private static String block(String keyword, String i, Field... fields) {
        if (fields.length == 0) {
            return keyword;
        }
        StringBuilder b = new StringBuilder(keyword).append("\n").append(i).append(TAB).append("{\n");
        for (Field f : fields) {
            b.append(field(f, i + tab(2)));
        }
        return b.append(i).append(TAB).append("}").toString();
    }

    /** One field's line (or nothing, when an optional field is absent), at indentation {@code at}. */
    private static String field(Field f, String at) {
        Object v = f.value();
        if (v == null) {
            if (f.optional()) {
                return "";
            }
            throw Composing.refused("a required field '" + f.key() + "' is absent (upstream cannot print it)");
        }
        return switch (f.kind()) {
            case STRING -> at + f.key() + ": " + convertString((String) v, true) + ";\n";
            case RAW -> at + f.key() + ": " + v + ";\n";
            case STRING_OF -> at + f.key() + ": " + convertString(String.valueOf(v), true) + ";\n";
            case INLINE_LIST -> at + f.key() + ": [" + String.join(", ", quoted(v)) + "];\n";
            case LINE_LIST -> {
                List<String> items = quoted(v);
                if (items.isEmpty()) {
                    yield "";
                }
                List<String> lines = new ArrayList<>();
                for (String s : items) {
                    lines.add(at + TAB + s);
                }
                yield at + f.key() + ": [\n" + String.join(",\n", lines) + "\n" + at + TAB + "];\n";
            }
        };
    }

    private static List<String> quoted(Object v) {
        List<String> out = new ArrayList<>();
        for (Object s : (List<?>) v) {
            out.add(convertString((String) s, true));
        }
        return out;
    }

    private static String postProcessor(Protocol.PPostProcessor p, String i) {
        return switch (p) {
            case Protocol.PMapperPostProcessor m -> {
                List<String> mappers = new ArrayList<>();
                for (Protocol.PMapper n : m.mappers()) {
                    mappers.add(switch (n) {
                        case Protocol.PTableMapper t -> tab(4) + "table {from: '" + t.from() + "'; to: '" + t.to()
                                + "'; schemaFrom: '" + t.schemaFrom() + "'; schemaTo: '" + t.schemaTo() + "';}";
                        case Protocol.PSchemaMapper s -> tab(4) + "schema {from: '" + s.from() + "'; to: '" + s.to() + "';}";
                    });
                }
                yield tab(2) + "mapper\n" + tab(2) + "{\n" + tab(3) + "mappers:\n" + tab(3) + "[\n"
                        + String.join(",\n" + i, mappers) + "\n" + tab(3) + "];\n" + tab(2) + "}";
            }
            case Protocol.PRelationalMapperPostProcessor r -> {
                List<String> paths = new ArrayList<>();
                for (Protocol.PPointer m : r.relationalMappers()) {
                    paths.add(m.path());
                }
                yield tab(2) + "relationalMapper\n" + tab(2) + "{\n" + tab(3) + String.join(", " + i, paths) + "\n"
                        + tab(2) + "}";
            }
            case Protocol.PExtractSubQueriesAsCtesPostProcessor x ->
                    tab(2) + "ExtractSubQueriesAsCTEsPostProcessor\n" + tab(2) + "{\n" + tab(2) + "}";
        };
    }

    private static String features(List<String> names) {
        List<String> out = new ArrayList<>();
        for (String s : names) {
            out.add("'" + s + "'");
        }
        return String.join(", ", out);
    }

    /** {@code Trino}'s specification: its SSL specification is a nested block. */
    private static String trino(Protocol.PTrinoSpec spec, String i) {
        StringBuilder b = new StringBuilder(TRINO).append("\n").append(i).append(TAB).append("{\n");
        String at = i + tab(2);
        b.append(field(req("host", spec.host(), Kind.STRING), at)).append(field(req("port", spec.port(), Kind.RAW), at))
                .append(field(opt("catalog", spec.catalog(), Kind.STRING), at))
                .append(field(opt("schema", spec.schema(), Kind.STRING), at))
                .append(field(opt("clientTags", spec.clientTags(), Kind.STRING), at));
        Protocol.PTrinoSsl ssl = spec.sslSpecification();
        if (ssl != null) {
            b.append(at).append("sslSpecification:\n").append(at).append("{\n")
                    .append(field(req("ssl", ssl.ssl(), Kind.RAW), at + TAB))
                    .append(field(opt("trustStorePathVaultReference", ssl.trustStorePathVaultReference(), Kind.STRING),
                            at + TAB))
                    .append(field(opt("trustStorePasswordVaultReference", ssl.trustStorePasswordVaultReference(),
                            Kind.STRING), at + TAB))
                    .append(at).append("};\n");
        }
        return b.append(i).append(TAB).append("}").toString();
    }
}
