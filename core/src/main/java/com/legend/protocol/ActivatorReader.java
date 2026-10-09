// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * The function activators read back -- the mirror of {@link ProtocolEmitter}'s {@code functionActivator}
 * (SnowflakeApp, SnowflakeM2MUdf, MemSqlFunction, BigQueryFunction, HostedService, FunctionJar,
 * DeephavenApp): one alphabetical slot sequence, a FUNCTION pointer, ownership, and an optional
 * activation configuration. The NAMELESS deployment-configuration elements (hosted service, BigQuery and
 * MemSql configs) carry no name, package or function on the wire, so no activator record can be read from
 * them: they have no rule here and are refused by name.
 */
final class ActivatorReader {

    private ActivatorReader() {
    }

    /** Each activator's wire {@code _type} and the grammar keyword its record keeps. */
    static final Map<String, String> KINDS = Map.of(
            "snowflakeApp", "SnowflakeApp",
            "snowflakeM2MUdf", "SnowflakeM2MUdf",
            "memSqlFunction", "MemSqlFunction",
            "bigQueryFunction", "BigQueryFunction",
            "hostedService", "HostedService",
            "functionJar", "FunctionJar",
            "DeephavenApp", "DeephavenApp");

    /** The activation configuration's {@code _type} per kind (the kinds whose wire carries one). */
    private static final Map<String, String> CONFIGS = Map.of(
            "SnowflakeApp", "snowflakeDeploymentConfiguration",
            "SnowflakeM2MUdf", "snowflakeM2MUdfDeploymentConfiguration",
            "MemSqlFunction", "memSqlFunctionConfig",
            "BigQueryFunction", "bigQueryFunctionConfig");

    /** The optional string slots, by wire key. */
    private static final List<String> SCALARS = List.of("applicationName", "deploymentSchema", "deploymentStage",
            "description", "documentation", "functionName", "pattern", "permissionScheme", "udfName", "usageRole");

    /** The reader for one activator {@code _type}. */
    static Function<Wire, Protocol.Element> reader(String wireType) {
        String kind = KINDS.get(wireType);
        return w -> activator(w, java.util.Objects.requireNonNull(kind, wireType));
    }

    private static Protocol.Element activator(Wire w, String kind) {
        w.emptyArray("actions");
        String connection = null;
        SourceInfo connectionSpan = null;
        Wire cfg = w.optObj("activationConfiguration");
        if (cfg != null) {
            String cfgType = CONFIGS.get(kind);
            if (cfgType == null) {
                throw Wire.refuse("a " + kind + " activation configuration (the wire never writes one)");
            }
            cfg.constant("_type", cfgType);
            Wire ptr = cfg.obj("activationConnection");
            ptr.constant("_type", "connectionPointer");
            connection = ptr.str("connection");
            connectionSpan = ptr.span();
            ptr.done(connection);
            cfg.done(connection);
        }
        if ("HostedService".equals(kind)) {
            // the walker parses generateLineage/storeModel and discards them: the wire always spells false
            w.constant("generateLineage", false);
            w.constant("storeModel", false);
        }
        Map<String, String> scalars = new LinkedHashMap<>();
        for (String key : SCALARS) {
            String v = w.optStr(key);
            if (v != null) {
                scalars.put(key, v);
            }
        }
        Map<String, Boolean> booleans = new LinkedHashMap<>();
        Boolean auto = w.optBool("autoActivateUpdates");
        if (auto != null) {
            booleans.put("autoActivateUpdates", auto);
        }
        Wire fn = w.obj("function");
        fn.constant("type", "FUNCTION");
        String functionPath = fn.str("path");
        SourceInfo functionSpan = fn.done(fn.span());
        String ownerId = null;
        List<String> users = null;
        Wire own = w.optObj("ownership");
        if (own != null) {
            String type = own.type();
            if ("DeploymentOwner".equals(type)) {
                ownerId = own.str("id");
            } else if ("userList".equals(type)) {
                // left out, the engine's UserList starts it empty
                List<String> written = own.optStrings("users");
                users = written == null ? List.of() : written;
            } else {
                throw Wire.refuse("no reader rule for activator ownership _type '" + type + "'");
            }
            own.done(type);
        }
        return new Protocol.PFunctionActivator(w.str("package"), w.str("name"), kind,
                w.list("stereotypes", DomainReader::stereotype), w.list("taggedValues", DomainReader::taggedValue),
                scalars, booleans, functionPath, functionSpan, ownerId, users, connection, connectionSpan, w.span());
    }
}
