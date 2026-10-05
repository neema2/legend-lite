// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol;

import com.legend.json.Json;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * Connections and runtimes read back -- the mirror of {@link ProtocolEmitter}'s {@code connection},
 * {@code connectionValue}, {@code runtime} and {@code runtimeArrays} rules, of
 * {@link ConnectionEmitters} (auth strategies, datasource specifications, mapper post-processors) and of
 * {@link AuthSpecEmitter} (the authentication-module islands and their vault secrets).
 */
final class ConnectionReader {

    private ConnectionReader() {
    }

    static Protocol.Element connection(Wire w) {
        return new Protocol.PConnection(w.str("package"), w.str("name"), connectionValue(w.take("connectionValue")),
                w.span());
    }

    /** {@code _type:"runtime"}: the element and its runtime value share one span. */
    static Protocol.Element runtime(Wire w) {
        Wire v = w.obj("runtimeValue");
        String kind = v.type();
        boolean single = "localEngineRuntime".equals(kind);
        if (!single && !"engineRuntime".equals(kind)) {
            throw Wire.refuse("no reader rule for runtime value _type '" + kind + "'");
        }
        Arrays arrays = arrays(v);
        SourceInfo inner = v.done(v.span());
        SourceInfo outer = w.span();
        StoreReader.sameSpan(inner, outer, "runtime");
        return new Protocol.PRuntime(w.str("package"), w.str("name"), single, arrays.mappings(),
                arrays.connections(), arrays.connectionStores(), outer);
    }

    /** The engineRuntime arrays every runtime shape writes: connectionStores, connections, mappings. */
    record Arrays(List<Protocol.PConnectionStores> connectionStores, List<Protocol.PStoreConnections> connections,
            List<Protocol.PPointer> mappings) {
    }

    static Arrays arrays(Wire v) {
        return new Arrays(v.list("connectionStores", ConnectionReader::connectionStores),
                v.list("connections", ConnectionReader::storeConnections),
                v.list("mappings", DomainReader::pointer));
    }

    private static Protocol.PConnectionStores connectionStores(Json.Node node) {
        Wire c = Wire.of(node, "connection stores");
        return c.done(new Protocol.PConnectionStores(connectionValue(c.take("connectionPointer")),
                c.list("storePointers", ConnectionReader::storePointer), c.span()));
    }

    private static Protocol.PStorePointer storePointer(Json.Node node) {
        Wire p = Wire.of(node, "store pointer");
        return p.done(new Protocol.PStorePointer(p.str("path"), p.span(), p.optStr("type")));
    }

    private static Protocol.PStoreConnections storeConnections(Json.Node node) {
        Wire s = Wire.of(node, "store connections");
        return s.done(new Protocol.PStoreConnections(DomainReader.pointer(s.take("store")),
                s.list("storeConnections", ConnectionReader::identifiedConnection), s.span()));
    }

    private static Protocol.PIdentifiedConnection identifiedConnection(Json.Node node) {
        Wire i = Wire.of(node, "identified connection");
        return i.done(new Protocol.PIdentifiedConnection(i.str("id"), connectionValue(i.take("connection")),
                i.span()));
    }

    // ---------------------------------------------------------------------
    // Connection values
    // ---------------------------------------------------------------------

    /** The reader rule for each connection-value {@code _type}. */
    private static final Map<String, Function<Wire, Protocol.PConnectionValue>> VALUES = Map.of(
            "connectionPointer", w -> new Protocol.PConnectionPointer(w.str("connection"), w.span()),
            "JsonModelConnection", w -> new Protocol.PJsonModelConnection(w.str("class"),
                    w.span("classSourceInformation"), w.optStr("element"), w.str("url"), w.span()),
            "XmlModelConnection", w -> new Protocol.PXmlModelConnection(w.str("class"),
                    w.span("classSourceInformation"), w.optStr("element"), w.str("url"), w.span()),
            "ModelChainConnection", w -> new Protocol.PModelChainConnection(w.optStr("element"),
                    w.strings("mappings"), w.span("mappingsSourceInformation"), w.span()),
            "serviceStore", w -> new Protocol.PServiceStoreConnection(w.str("baseUrl"), w.optStr("element"),
                    w.span("elementSourceInformation"), w.span()),
            "deephavenConnection", ConnectionReader::deephaven,
            "elasticsearch7StoreConnection", w -> {
                String url = sourceSpecUrl(w);
                return new Protocol.PElasticsearchConnection(w.str("element"), w.span("elementSourceInformation"),
                        url, authSpec(w.take("authSpec")), w.span());
            },
            "MongoDBConnection", ConnectionReader::mongo,
            "RelationalDatabaseConnection", ConnectionReader::relational);

    static Protocol.PConnectionValue connectionValue(Json.Node node) {
        Wire w = Wire.of(node, "connection value");
        return w.done(Wire.rule(VALUES, w.type(), "connection value").apply(w));
    }

    private static String sourceSpecUrl(Wire w) {
        Wire spec = w.obj("sourceSpec");
        return spec.done(spec.str("url"));
    }

    private static Protocol.PConnectionValue deephaven(Wire w) {
        Wire auth = w.obj("authSpec");
        auth.constant("_type", "PSK");
        String psk = auth.done(auth.str("psk"));
        return new Protocol.PDeephavenConnection(sourceSpecUrl(w), psk, w.optStr("element"),
                w.span("elementSourceInformation"), w.span());
    }

    private static Protocol.PConnectionValue mongo(Wire w) {
        w.constant("type", "MongoDb");
        Wire ds = w.obj("dataSourceSpecification");
        String database = ds.str("databaseName");
        List<Protocol.PMongoServerUrl> urls = ds.list("serverURLs", n -> {
            Wire u = Wire.of(n, "MongoDB server URL");
            return u.done(new Protocol.PMongoServerUrl(u.str("baseUrl"), u.lng("port")));
        });
        ds.done(urls);
        return new Protocol.PMongoDbConnection(database, urls, authSpec(w.take("authenticationSpecification")),
                w.optStr("element"), w.span("elementSourceInformation"), w.span());
    }

    /** A relational database connection: {@code databaseType} written twice ({@code type} too). */
    private static Protocol.PConnectionValue relational(Wire w) {
        String databaseType = w.str("databaseType");
        w.constant("type", databaseType);
        w.emptyArray("postProcessorWithParameter");
        List<Protocol.PPostProcessor> post = StoreReader.nonEmpty(w, "postProcessors", ConnectionReader::postProcessor);
        return new Protocol.PRelationalDatabaseConnection(authStrategy(w.take("authenticationStrategy")),
                databaseType, DatasourceReader.datasourceSpec(w.take("datasourceSpecification")), w.optStr("element"),
                w.span("elementSourceInformation"), w.optBool("localMode"), post,
                w.optList("queryGenerationConfigs", ConnectionReader::generationConfig), w.optLong("queryTimeOutInSeconds"),
                w.optBool("quoteIdentifiers"), w.optStr("timeZone"), w.span());
    }

    private static Protocol.PGenerationFeaturesConfig generationConfig(Json.Node node) {
        Wire g = Wire.of(node, "generation features config");
        g.constant("_type", "generationFeaturesConfig");
        return g.done(new Protocol.PGenerationFeaturesConfig(g.strings("enabled"), g.strings("disabled"), g.span()));
    }

    private static Protocol.PPostProcessor postProcessor(Json.Node node) {
        Wire p = Wire.of(node, "post-processor");
        String type = p.type();
        Protocol.PPostProcessor out;
        if ("mapper".equals(type)) {
            out = new Protocol.PMapperPostProcessor(p.list("mappers", ConnectionReader::mapper));
        } else if ("relationalMapper".equals(type)) {
            out = new Protocol.PRelationalMapperPostProcessor(p.list("relationalMappers", DomainReader::pointer));
        } else if ("ExtractSubQueriesAsCTEsPostProcessor".equals(type)) {
            out = new Protocol.PExtractSubQueriesAsCtesPostProcessor();
        } else {
            throw Wire.refuse("no reader rule for post-processor _type '" + type + "'");
        }
        return p.done(out);
    }

    /** A table mapper's schema pair rides a NESTED {@code schema} object on the wire. */
    private static Protocol.PMapper mapper(Json.Node node) {
        Wire m = Wire.of(node, "mapper");
        String type = m.type();
        if ("schema".equals(type)) {
            return m.done(new Protocol.PSchemaMapper(m.str("from"), m.str("to")));
        }
        if (!"table".equals(type)) {
            throw Wire.refuse("no reader rule for mapper _type '" + type + "'");
        }
        Wire s = m.obj("schema");
        s.constant("_type", "schema");
        String schemaFrom = s.str("from");
        String schemaTo = s.done(s.str("to"));
        return m.done(new Protocol.PTableMapper(m.str("from"), m.str("to"), schemaFrom, schemaTo));
    }

    // ---------------------------------------------------------------------
    // Authentication strategies
    // ---------------------------------------------------------------------

    /** The reader rule for each relational authentication strategy {@code _type}. */
    private static final Map<String, Function<Wire, Protocol.PAuthStrategy>> STRATEGIES = Map.ofEntries(
            Map.entry("h2Default", w -> new Protocol.PH2Default(w.span())),
            Map.entry("test", w -> new Protocol.PTestAuth(w.span())),
            Map.entry("userNamePassword", w -> new Protocol.PUserNamePassword(w.optStr("baseVaultReference"),
                    w.str("userNameVaultReference"), w.str("passwordVaultReference"), w.span())),
            Map.entry("oauth", w -> new Protocol.POAuth(w.str("oauthKey"), w.str("scopeName"), w.span())),
            Map.entry("delegatedKerberos", w -> new Protocol.PDelegatedKerberos(w.optStr("serverPrincipal"),
                    w.span())),
            Map.entry("snowflakePublic", w -> new Protocol.PSnowflakePublic(w.str("passPhraseVaultReference"),
                    w.str("privateKeyVaultReference"), w.str("publicUserName"), w.span())),
            Map.entry("gcpApplicationDefaultCredentials", w -> new Protocol.PGCPApplicationDefaultCredentials(
                    w.span())),
            Map.entry("apiToken", w -> new Protocol.PApiToken(w.str("apiToken"), w.span())),
            Map.entry("middleTierUserNamePassword", w -> new Protocol.PMiddleTierUserNamePassword(
                    w.str("vaultReference"), w.span())),
            Map.entry("TrinoDelegatedKerberosAuth", w -> new Protocol.PTrinoKerberosAuth(
                    w.optStr("kerberosRemoteServiceName"), w.optBool("kerberosUseCanonicalHostname"),
                    w.optStr("serverPrincipal"), w.span())),
            Map.entry("gcpWorkloadIdentityFederation", w -> new Protocol.PGcpWifAuth(
                    w.optStrings("additionalGcpScopes"), w.str("serviceAccountEmail"), w.span())));

    static Protocol.PAuthStrategy authStrategy(Json.Node node) {
        Wire w = Wire.of(node, "authentication strategy");
        return w.done(Wire.rule(STRATEGIES, w.type(), "authentication strategy").apply(w));
    }

    // ---------------------------------------------------------------------
    // Authentication-module islands and secrets
    // ---------------------------------------------------------------------

    /** The reader rule for each authentication-module island {@code _type}. */
    private static final Map<String, Function<Wire, Protocol.PAuthSpecValue>> AUTH_SPECS = Map.of(
            "userPassword", w -> new Protocol.PMongoAuth(w.str("username"), vaultSecret(w.take("password")), w.span()),
            "apiKey", w -> new Protocol.PApiKeyAuth(w.str("keyName"), w.str("location"), vaultSecret(w.take("value")),
                    w.span()),
            "kerberos", w -> new Protocol.PKerberosAuth(w.span()),
            "PSK", w -> new Protocol.PPskAuth(w.str("psk")),
            "gcpWithAWSIdP", ConnectionReader::gcpWif,
            "encryptedPrivateKey", w -> new Protocol.PEpkAuth(w.str("userName"), vaultSecret(w.take("privateKey")),
                    vaultSecret(w.take("passphrase")), w.span()));

    static Protocol.PAuthSpecValue authSpec(Json.Node node) {
        Wire w = Wire.of(node, "authentication specification");
        return w.done(Wire.rule(AUTH_SPECS, w.type(), "authentication specification").apply(w));
    }

    private static Protocol.PAuthSpecValue gcpWif(Wire w) {
        w.emptyArray("additionalGcpScopes");
        Wire idp = w.obj("idPConfiguration");
        String accountId = idp.str("accountId");
        Protocol.PAwsCredentials creds = awsCredentials(idp.take("awsCredentials"));
        String region = idp.str("region");
        String role = idp.done(idp.str("role"));
        Wire wl = w.obj("workloadConfiguration");
        String poolId = wl.str("poolId");
        String projectNumber = wl.str("projectNumber");
        String providerId = wl.done(wl.str("providerId"));
        return new Protocol.PGcpWifIslandAuth(w.str("serviceAccountEmail"), accountId, region, role, creds,
                projectNumber, providerId, poolId, w.span());
    }

    private static Protocol.PAwsCredentials awsCredentials(Json.Node node) {
        Wire c = Wire.of(node, "AWS credentials");
        String type = c.type();
        Protocol.PAwsCredentials out;
        if ("awsDefault".equals(type)) {
            out = new Protocol.PAwsDefault();
        } else if ("awsStatic".equals(type)) {
            out = new Protocol.PAwsStatic(mongoSecret(c.take("accessKeyId")), mongoSecret(c.take("secretAccessKey")),
                    c.span());
        } else if ("awsSTSAssumeRole".equals(type)) {
            out = new Protocol.PAwsStsRole(c.str("roleArn"), c.str("roleSessionName"),
                    awsCredentials(c.take("awsCredentials")), c.span());
        } else {
            throw Wire.refuse("no reader rule for AWS credentials _type '" + type + "'");
        }
        return c.done(out);
    }

    /**
     * A vault secret: the AWS secrets-manager shape (no span of its own; its credentials carry the kind and
     * an optional span), or a single-field secret whose one field (its kind's key) sits alphabetically
     * around {@code sourceInformation}.
     */
    static Protocol.PVaultSecret vaultSecret(Json.Node node) {
        Wire s = Wire.of(node, "vault secret");
        String kind = s.type();
        if (kind == null) {
            throw Wire.refuse("a vault secret without its _type");
        }
        if ("awssecretsmanager".equals(kind)) {
            Wire c = s.obj("awsCredentials");
            String credsKind = c.type();
            if (credsKind == null) {
                throw Wire.refuse("AWS secret credentials without their _type");
            }
            Json.Node ak = c.opt("accessKeyId");
            Json.Node sk = c.opt("secretAccessKey");
            Protocol.PAwsSecret aws = c.done(new Protocol.PAwsSecret(s.str("secretId"), s.optStr("versionId"),
                    s.optStr("versionStage"), credsKind, ak == null ? null : mongoSecret(ak),
                    sk == null ? null : mongoSecret(sk), c.span()));
            return s.done(aws);
        }
        SourceInfo span = s.span();
        String fieldKey = null;
        for (String k : s.json().fields().keySet()) {
            if (!k.equals("_type") && !k.equals("sourceInformation")) {
                if (fieldKey != null) {
                    throw Wire.refuse("a '" + kind + "' secret with two value fields: " + fieldKey + ", " + k);
                }
                fieldKey = k;
            }
        }
        if (fieldKey == null) {
            throw Wire.refuse("a '" + kind + "' secret without its value field");
        }
        return s.done(new Protocol.PMongoSecret(kind, fieldKey, s.str(fieldKey), span));
    }

    private static Protocol.PMongoSecret mongoSecret(Json.Node node) {
        if (vaultSecret(node) instanceof Protocol.PMongoSecret m) {
            return m;
        }
        throw Wire.refuse("an AWS credential key that is itself an AWS secret");
    }
}
