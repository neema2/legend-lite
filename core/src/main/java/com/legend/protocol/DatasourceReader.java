// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol;

import com.legend.json.Json;

import java.util.Map;
import java.util.function.Function;

/**
 * A relational connection's datasource specification read back -- the mirror of
 * {@link ConnectionEmitters#datasourceSpec} and {@code vendorDatasourceSpec}: fields alphabetical, nulls
 * omitted. The lite-only flavors (SQLite, the in-memory forms) have no wire shape and so no rule.
 */
final class DatasourceReader {

    private DatasourceReader() {
    }

    /** The reader rule for each datasource-specification {@code _type}. */
    private static final Map<String, Function<Wire, Protocol.PDatasourceSpec>> SPECS = Map.ofEntries(
            Map.entry("h2Local", w -> new Protocol.PH2Local(w.optStr("testDataSetupCsv"),
                    w.optStrings("testDataSetupSqls"), w.span())),
            Map.entry("duckDB", w -> new Protocol.PDuckDBSpec(w.optStr("path"), w.span())),
            Map.entry("h2Embedded", w -> new Protocol.PH2EmbeddedSpec(w.str("databaseName"), w.str("directory"),
                    w.bool("autoServerMode"), w.span())),
            Map.entry("static", w -> new Protocol.PStaticSpec(w.str("databaseName"), w.str("host"), w.lng("port"),
                    w.span())),
            Map.entry("snowflake", DatasourceReader::snowflake),
            Map.entry("spanner", w -> new Protocol.PSpannerSpec(w.str("databaseId"), w.str("instanceId"),
                    w.str("projectId"), w.optStr("proxyHost"), w.optLong("proxyPort"), w.span())),
            // the redshift extension's wire carries no sourceInformation
            Map.entry("redshift", w -> new Protocol.PRedshiftSpec(w.str("clusterID"), w.str("databaseName"),
                    w.optStr("endpointURL"), w.str("host"), w.lng("port"), w.str("region"), null)),
            Map.entry("athena", w -> new Protocol.PAthenaSpec(w.optStr("athenaEndpoint"), w.optStr("catalog"),
                    w.optStr("database"), w.optStr("outputLocation"), w.str("region"), w.optStr("workGroup"),
                    w.span())),
            Map.entry("aurora", w -> new Protocol.PAuroraSpec(w.optStr("clusterInstanceHostPattern"), w.str("host"),
                    w.str("name"), w.lng("port"), w.span())),
            Map.entry("globalAurora", w -> new Protocol.PGlobalAuroraSpec(
                    w.strings("globalClusterInstanceHostPatterns"), w.str("host"), w.str("name"), w.lng("port"),
                    w.str("region"), w.span())),
            Map.entry("memSql", w -> new Protocol.PMemSqlSpec(w.optStr("databaseName"), w.str("host"), w.lng("port"),
                    w.optBool("useSsl"), w.span())),
            Map.entry("oracle", w -> new Protocol.POracleSpec(w.str("host"), w.lng("port"), w.optStr("serviceName"),
                    w.span())),
            Map.entry("Trino", DatasourceReader::trino),
            Map.entry("databricks", w -> new Protocol.PDatabricksSpec(w.str("hostname"), w.str("httpPath"),
                    w.str("port"), w.str("protocol"), w.span())),
            Map.entry("bigQuery", w -> new Protocol.PBigQuerySpec(w.str("defaultDataset"), w.str("projectId"),
                    w.optStr("proxyHost"), w.optStr("proxyPort"), w.span())));

    static Protocol.PDatasourceSpec datasourceSpec(Json.Node node) {
        Wire w = Wire.of(node, "datasource specification");
        return w.done(Wire.rule(SPECS, w.type(), "datasource specification").apply(w));
    }

    private static Protocol.PDatasourceSpec snowflake(Wire w) {
        return new Protocol.PSnowflakeSpec(w.str("accountName"), w.optStr("accountType"), w.optStr("cloudType"),
                w.str("databaseName"), w.optBool("enableQueryTags"), w.optStr("nonProxyHosts"),
                w.optStr("organization"), w.optStr("proxyHost"), w.optStr("proxyPort"),
                w.optBool("quotedIdentifiersIgnoreCase"), w.str("region"), w.optStr("role"), w.optStr("tempTableDb"),
                w.optStr("tempTableSchema"), w.str("warehouseName"), w.span());
    }

    private static Protocol.PDatasourceSpec trino(Wire w) {
        Protocol.PTrinoSsl ssl = null;
        Wire s = w.optObj("sslSpecification");
        if (s != null) {
            ssl = s.done(new Protocol.PTrinoSsl(s.bool("ssl"), s.optStr("trustStorePathVaultReference"),
                    s.optStr("trustStorePasswordVaultReference")));
        }
        return new Protocol.PTrinoSpec(w.optStr("catalog"), w.optStr("clientTags"), w.str("host"), w.lng("port"),
                w.optStr("schema"), ssl, w.span());
    }
}
