// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol;

import com.legend.json.Json;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.legend.protocol.Composing.TAB;
import static com.legend.protocol.Composing.convertString;
import static com.legend.protocol.Composing.objOr;
import static com.legend.protocol.Composing.objs;
import static com.legend.protocol.Composing.str;
import static com.legend.protocol.Composing.tab;

/**
 * A {@code RelationalDatabaseConnection} as upstream prints it: the relational extension's connection
 * value composer, its datasource specifications and authentication strategies, and every database
 * extension's (Snowflake, BigQuery, Databricks, Spanner, Trino, Redshift, Athena, Aurora, MemSQL,
 * Oracle, DuckDB). Each specification and strategy is a block of {@code key: value;} lines, so each is
 * a row of a table here: its keyword and its fields.
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

    /** One {@code key: value;} line: the grammar's key, the wire field, how it prints, whether it may be absent. */
    record Field(String key, String json, Kind kind, boolean optional) {
    }

    /** A block: its keyword and fields. A block with no fields prints its keyword alone. */
    record Block(String keyword, List<Field> fields) {
    }

    private static Field req(String key, String json, Kind kind) {
        return new Field(key, json, kind, false);
    }

    private static Field opt(String key, String json, Kind kind) {
        return new Field(key, json, kind, true);
    }

    private static final Map<String, Block> SPECIFICATIONS = Map.ofEntries(
            Map.entry("h2Local", new Block("LocalH2", List.of(
                    opt("testDataSetupCSV", "testDataSetupCsv", Kind.STRING),
                    opt("testDataSetupSqls", "testDataSetupSqls", Kind.LINE_LIST)))),
            Map.entry("h2Embedded", new Block("EmbeddedH2", List.of(
                    req("name", "databaseName", Kind.STRING), req("directory", "directory", Kind.STRING),
                    req("autoServerMode", "autoServerMode", Kind.RAW)))),
            Map.entry("static", new Block("Static", List.of(
                    req("name", "databaseName", Kind.STRING), req("host", "host", Kind.STRING), req("port", "port", Kind.RAW)))),
            Map.entry("athena", new Block("Athena", List.of(
                    req("region", "region", Kind.STRING), opt("database", "database", Kind.STRING),
                    opt("workGroup", "workGroup", Kind.STRING), opt("outputLocation", "outputLocation", Kind.STRING),
                    opt("catalog", "catalog", Kind.STRING), opt("athenaEndpoint", "athenaEndpoint", Kind.STRING)))),
            Map.entry("aurora", new Block("Aurora", List.of(
                    req("host", "host", Kind.STRING), req("port", "port", Kind.RAW), req("name", "name", Kind.STRING),
                    opt("clusterInstanceHostPattern", "clusterInstanceHostPattern", Kind.STRING)))),
            Map.entry("globalAurora", new Block("GlobalAurora", List.of(
                    req("host", "host", Kind.STRING), req("port", "port", Kind.RAW), req("name", "name", Kind.STRING),
                    req("region", "region", Kind.STRING),
                    req("globalClusterInstanceHostPatterns", "globalClusterInstanceHostPatterns", Kind.INLINE_LIST)))),
            Map.entry("bigQuery", new Block("BigQuery", List.of(
                    req("projectId", "projectId", Kind.STRING), req("defaultDataset", "defaultDataset", Kind.STRING),
                    opt("proxyHost", "proxyHost", Kind.STRING), opt("proxyPort", "proxyPort", Kind.STRING)))),
            Map.entry("databricks", new Block("Databricks", List.of(
                    req("hostname", "hostname", Kind.STRING), req("port", "port", Kind.STRING),
                    req("protocol", "protocol", Kind.STRING), req("httpPath", "httpPath", Kind.STRING)))),
            Map.entry("duckDB", new Block("DuckDB", List.of(req("path", "path", Kind.STRING)))),
            Map.entry("memSql", new Block("MemSql", List.of(
                    req("host", "host", Kind.STRING), req("port", "port", Kind.STRING_OF),
                    req("databaseName", "databaseName", Kind.STRING), req("useSsl", "useSsl", Kind.STRING_OF)))),
            Map.entry("oracle", new Block("Oracle", List.of(
                    req("host", "host", Kind.STRING), req("port", "port", Kind.RAW), req("serviceName", "serviceName", Kind.STRING)))),
            Map.entry("redshift", new Block("Redshift", List.of(
                    req("host", "host", Kind.STRING), req("port", "port", Kind.RAW), req("name", "databaseName", Kind.STRING),
                    req("region", "region", Kind.STRING), req("clusterID", "clusterID", Kind.STRING),
                    req("endpointURL", "endpointURL", Kind.STRING)))),
            Map.entry("snowflake", new Block("Snowflake", List.of(
                    req("name", "databaseName", Kind.STRING), req("account", "accountName", Kind.STRING),
                    req("warehouse", "warehouseName", Kind.STRING), req("region", "region", Kind.STRING),
                    opt("cloudType", "cloudType", Kind.STRING),
                    opt("quotedIdentifiersIgnoreCase", "quotedIdentifiersIgnoreCase", Kind.RAW),
                    opt("enableQueryTags", "enableQueryTags", Kind.RAW), opt("proxyHost", "proxyHost", Kind.STRING),
                    opt("proxyPort", "proxyPort", Kind.STRING), opt("nonProxyHosts", "nonProxyHosts", Kind.STRING),
                    opt("tempTableDb", "tempTableDb", Kind.STRING), opt("tempTableSchema", "tempTableSchema", Kind.STRING),
                    opt("accountType", "accountType", Kind.RAW), opt("organization", "organization", Kind.STRING),
                    opt("role", "role", Kind.STRING)))),
            Map.entry("spanner", new Block("Spanner", List.of(
                    req("projectId", "projectId", Kind.STRING), req("instanceId", "instanceId", Kind.STRING),
                    req("databaseId", "databaseId", Kind.STRING), opt("proxyHost", "proxyHost", Kind.STRING),
                    opt("proxyPort", "proxyPort", Kind.RAW)))));

    private static final Map<String, Block> AUTHENTICATIONS = Map.ofEntries(
            Map.entry("test", new Block("Test", List.of())),
            Map.entry("h2Default", new Block("DefaultH2", List.of())),
            Map.entry("gcpApplicationDefaultCredentials", new Block("GCPApplicationDefaultCredentials", List.of())),
            Map.entry("apiToken", new Block("ApiToken", List.of(req("apiToken", "apiToken", Kind.STRING)))),
            Map.entry("userNamePassword", new Block("UserNamePassword", List.of(
                    opt("baseVaultReference", "baseVaultReference", Kind.STRING),
                    req("userNameVaultReference", "userNameVaultReference", Kind.STRING),
                    req("passwordVaultReference", "passwordVaultReference", Kind.STRING)))),
            Map.entry("gcpWorkloadIdentityFederation", new Block("GCPWorkloadIdentityFederation", List.of(
                    req("serviceAccountEmail", "serviceAccountEmail", Kind.STRING),
                    opt("additionalGcpScopes", "additionalGcpScopes", Kind.LINE_LIST)))),
            Map.entry("oauth", new Block("OAuth", List.of(
                    req("oauthKey", "oauthKey", Kind.STRING), req("scopeName", "scopeName", Kind.STRING)))),
            Map.entry("snowflakePublic", new Block("SnowflakePublic", List.of(
                    req("publicUserName", "publicUserName", Kind.STRING),
                    req("privateKeyVaultReference", "privateKeyVaultReference", Kind.STRING),
                    req("passPhraseVaultReference", "passPhraseVaultReference", Kind.STRING)))),
            Map.entry("TrinoDelegatedKerberosAuth", new Block("TrinoDelegatedKerberos", List.of(
                    opt("serverPrincipal", "serverPrincipal", Kind.RAW),
                    opt("kerberosUseCanonicalHostname", "kerberosUseCanonicalHostname", Kind.RAW),
                    req("kerberosRemoteServiceName", "kerberosRemoteServiceName", Kind.STRING)))));

    /** Strategies whose block is printed only when their one optional field is present. */
    private static final Map<String, Field> OPTIONAL_BLOCKS = Map.of(
            "delegatedKerberos", opt("serverPrincipal", "serverPrincipal", Kind.STRING),
            "middleTierUserNamePassword", opt("vaultReference", "vaultReference", Kind.STRING));
    private static final Map<String, String> OPTIONAL_BLOCK_KEYWORDS = Map.of(
            "delegatedKerberos", "DelegatedKerberos", "middleTierUserNamePassword", "MiddleTierUserNamePassword");

    private RelationalConnectionComposer() {
    }

    /** The connection's body, at the context's indentation {@code i}. */
    static String connection(Json.Obj c, String i) {
        String store = str(c, "element");
        String timeZone = str(c, "timeZone");
        Json.Node quote = Composing.value(c, "quoteIdentifiers");
        boolean local = c.getBoolOr("localMode", false);
        Json.Node timeout = Composing.value(c, "queryTimeOutInSeconds");
        StringBuilder b = new StringBuilder(i).append("{\n");
        if (store != null) {
            b.append(i).append(TAB).append("store: ").append(store).append(";\n");
        }
        b.append(i).append(TAB).append("type: ").append(c.getString("type")).append(";\n");
        if (timeZone != null) {
            b.append(i).append(TAB).append("timezone: ")
                    .append(utcOffset(timeZone) ? timeZone : convertString(timeZone, true)).append(";\n");
        }
        if (quote != null) {
            b.append(i).append(TAB).append("quoteIdentifiers: ").append(raw(quote)).append(";\n");
        }
        String specification = block(SPECIFICATIONS, c.getObj("datasourceSpecification"), i, "datasource specification");
        String auth = authentication(c.getObj("authenticationStrategy"), i);
        if (local) {
            b.append(i).append(TAB).append("mode: local;\n");
        } else {
            b.append(i).append(TAB).append("specification: ").append(specification).append(";\n");
            b.append(i).append(TAB).append("auth: ").append(auth).append(";\n");
        }
        if (timeout != null) {
            b.append(i).append(TAB).append("queryTimeOutInSeconds: ").append(raw(timeout)).append(";\n");
        }
        List<Json.Obj> postProcessors = objs(c, "postProcessors");
        if (!postProcessors.isEmpty()) {
            List<String> ps = new ArrayList<>();
            for (Json.Obj p : postProcessors) {
                ps.add(postProcessor(p, i));
            }
            b.append(i).append(TAB).append("postProcessors:\n").append(TAB).append("[\n").append(String.join(",\n", ps))
                    .append("\n").append(TAB).append("];\n");
        }
        List<Json.Obj> configs = objs(c, "queryGenerationConfigs");
        if (!configs.isEmpty()) {
            List<String> cs = new ArrayList<>();
            for (Json.Obj q : configs) {
                cs.add(queryGenerationConfig(q, i));
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

    private static String authentication(Json.Obj auth, String i) {
        String type = Composing.type(auth);
        Field only = OPTIONAL_BLOCKS.get(type);
        if (only != null) {
            String keyword = OPTIONAL_BLOCK_KEYWORDS.get(type);
            return Composing.value(auth, only.json()) != null && keyword != null
                    ? block(new Block(keyword, List.of(only)), auth, i) : String.valueOf(keyword);
        }
        return block(AUTHENTICATIONS, auth, i, "authentication strategy");
    }

    private static String block(Map<String, Block> table, Json.Obj o, String i, String what) {
        if (TRINO.equals(Composing.type(o)) && table == SPECIFICATIONS) {
            return trino(o, i);
        }
        Block block = table.get(Composing.type(o));
        if (block == null) {
            throw Composing.refused("no composer rule for a " + what + " of _type '" + Composing.type(o) + "'");
        }
        return block(block, o, i);
    }

    private static final String TRINO = "Trino";

    /** A {@code Keyword} then its {@code { key: value; }} block, at indentation {@code i} plus one tab. */
    static String block(Block block, Json.Obj o, String i) {
        if (block.fields().isEmpty()) {
            return block.keyword();
        }
        StringBuilder b = new StringBuilder(block.keyword()).append("\n").append(i).append(TAB).append("{\n");
        for (Field f : block.fields()) {
            b.append(field(f, o, i + tab(2)));
        }
        return b.append(i).append(TAB).append("}").toString();
    }

    /** One field's line (or nothing, when an optional field is absent), at indentation {@code at}. */
    static String field(Field f, Json.Obj o, String at) {
        Json.Node v = Composing.value(o, f.json());
        if (v == null) {
            if (f.optional()) {
                return "";
            }
            throw Composing.refused("a required field '" + f.json() + "' is absent (upstream cannot print it)");
        }
        return switch (f.kind()) {
            case STRING -> at + f.key() + ": " + convertString(((Json.Str) v).value(), true) + ";\n";
            case RAW -> at + f.key() + ": " + raw(v) + ";\n";
            case STRING_OF -> at + f.key() + ": " + convertString(raw(v), true) + ";\n";
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

    private static List<String> quoted(Json.Node v) {
        List<String> out = new ArrayList<>();
        for (Json.Node n : ((Json.Arr) v).items()) {
            out.add(convertString(((Json.Str) n).value(), true));
        }
        return out;
    }

    /** A scalar as Java's {@code toString} prints the deserialized value. */
    static String raw(Json.Node v) {
        if (v instanceof Json.Str s) {
            return s.value();
        }
        if (v instanceof Json.Bool b) {
            return String.valueOf(b.value());
        }
        if (v instanceof Json.Num n) {
            return n.isInteger() ? Long.toString(n.longValue()) : Double.toString(n.doubleValue());
        }
        throw Composing.refused("a scalar field whose value is " + v);
    }

    private static String postProcessor(Json.Obj p, String i) {
        String type = Composing.type(p);
        if ("mapper".equals(type)) {
            List<String> mappers = new ArrayList<>();
            for (Json.Obj m : objs(p, "mappers")) {
                mappers.add(nameMapper(m));
            }
            return tab(2) + "mapper\n" + tab(2) + "{\n" + tab(3) + "mappers:\n" + tab(3) + "[\n"
                    + String.join(",\n" + i, mappers) + "\n" + tab(3) + "];\n" + tab(2) + "}";
        }
        if ("relationalMapper".equals(type)) {
            List<String> paths = new ArrayList<>();
            for (Json.Obj m : objs(p, "relationalMappers")) {
                paths.add(m.getString("path"));
            }
            return tab(2) + "relationalMapper\n" + tab(2) + "{\n" + tab(3) + String.join(", " + i, paths) + "\n" + tab(2) + "}";
        }
        if ("ExtractSubQueriesAsCTEsPostProcessor".equals(type)) {
            return tab(2) + "ExtractSubQueriesAsCTEsPostProcessor\n" + tab(2) + "{\n" + tab(2) + "}";
        }
        throw Composing.refused("no composer rule for a post processor of _type '" + type + "'");
    }

    private static String nameMapper(Json.Obj m) {
        String type = Composing.type(m);
        if ("table".equals(type)) {
            Json.Obj schema = m.getObj("schema");
            return tab(4) + "table {from: '" + m.getString("from") + "'; to: '" + m.getString("to") + "'; schemaFrom: '"
                    + schema.getString("from") + "'; schemaTo: '" + schema.getString("to") + "';}";
        }
        if ("schema".equals(type)) {
            return tab(4) + "schema {from: '" + m.getString("from") + "'; to: '" + m.getString("to") + "';}";
        }
        throw Composing.refused("no composer rule for a name mapper of _type '" + type + "'");
    }

    private static String queryGenerationConfig(Json.Obj q, String i) {
        if (!"generationFeaturesConfig".equals(Composing.type(q))) {
            throw Composing.refused("no composer rule for a query generation config of _type '" + Composing.type(q) + "'");
        }
        return i + tab(2) + "GenerationFeaturesConfig\n" + i + tab(2) + "{\n"
                + i + tab(3) + "enabled: [" + features(q, "enabled") + "];\n"
                + i + tab(3) + "disabled: [" + features(q, "disabled") + "];\n"
                + i + tab(2) + "}";
    }

    private static String features(Json.Obj q, String key) {
        List<String> out = new ArrayList<>();
        for (String s : q.getStringArrayOr(key, List.of())) {
            out.add("'" + s + "'");
        }
        return String.join(", ", out);
    }

    /** {@code Trino}'s specification: its SSL specification is a nested block. */
    static String trino(Json.Obj spec, String i) {
        StringBuilder b = new StringBuilder(TRINO).append("\n").append(i).append(TAB).append("{\n");
        String at = i + tab(2);
        b.append(field(req("host", "host", Kind.STRING), spec, at)).append(field(req("port", "port", Kind.RAW), spec, at))
                .append(field(opt("catalog", "catalog", Kind.STRING), spec, at))
                .append(field(opt("schema", "schema", Kind.STRING), spec, at))
                .append(field(opt("clientTags", "clientTags", Kind.STRING), spec, at));
        Json.Obj ssl = objOr(spec, "sslSpecification");
        if (ssl != null) {
            b.append(at).append("sslSpecification:\n").append(at).append("{\n")
                    .append(field(req("ssl", "ssl", Kind.RAW), ssl, at + TAB))
                    .append(field(opt("trustStorePathVaultReference", "trustStorePathVaultReference", Kind.STRING), ssl, at + TAB))
                    .append(field(opt("trustStorePasswordVaultReference", "trustStorePasswordVaultReference", Kind.STRING), ssl, at + TAB))
                    .append(at).append("};\n");
        }
        return b.append(i).append(TAB).append("}").toString();
    }
}
