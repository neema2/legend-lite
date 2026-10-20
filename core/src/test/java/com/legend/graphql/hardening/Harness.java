// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.graphql.hardening;

import com.legend.json.Json;
import com.legend.server.PureV1Api;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;

/**
 * THE PRODUCT PATH for the hardening suites: a document sent to {@code POST /api/lite/v1/graphql/query} exactly as the
 * server receives it (the GraphQL-over-HTTP envelope, every root, variables, aliases, introspection), executed on the
 * caller's own database connection (the runner the server hands in, bound to it). What a client sees is what the
 * reference judges.
 */
public final class Harness {

    static final Json.Config JSON = new Json.Config(1024);

    private final String model;
    private final String runtime;
    private final PureV1Api.Runner runner;

    public Harness(String model, String runtime, Connection connection) {
        this.model = model;
        this.runtime = runtime;
        this.runner = (m, lambda, rt, rows) -> {
            try {
                return com.legend.Execution.executeWire(m, lambda, rt, connection, rows);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        };
    }

    public static String resource(String path) {
        try (InputStream in = Harness.class.getResourceAsStream("/graphql-parity/" + path)) {
            if (in == null) {
                throw new IllegalStateException("no resource graphql-parity/" + path);
            }
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** The request's body: the model as text, the runtime, the document, its variables. */
    String body(String document, @com.legend.base.Nullable String variablesJson,
            @com.legend.base.Nullable String operationName, String extra) {
        Json.Obj m = Json.obj();
        m.fields().put("_type", Json.str("text"));
        m.fields().put("code", Json.str(model));
        StringBuilder b = new StringBuilder("{\"model\":").append(Json.toCompact(m))
                .append(",\"runtime\":{\"runtime\":").append(Json.toCompact(runtime)).append("}")
                .append(",\"query\":").append(Json.toCompact(document));
        if (variablesJson != null) {
            b.append(",\"variables\":").append(variablesJson);
        }
        if (operationName != null) {
            b.append(",\"operationName\":").append(Json.toCompact(operationName));
        }
        if (!extra.isEmpty()) {
            b.append(',').append(extra);
        }
        return b.append('}').toString();
    }

    /** The answer: the envelope as the client receives it, with its HTTP status under {@code _status}. */
    public Json.Obj query(String document) {
        return query(document, null, null);
    }

    public Json.Obj query(String document, @com.legend.base.Nullable String variablesJson,
            @com.legend.base.Nullable String operationName) {
        return route("/api/lite/v1/graphql/query", body(document, variablesJson, operationName, ""));
    }

    public Json.Obj explain(String document) {
        return route("/api/lite/v1/graphql/explain", body(document, null, null, ""));
    }

    Json.Obj route(String path, String body) {
        PureV1Api.Answer a = PureV1Api.route(path, null, body, runner);
        Json.Obj o = (Json.Obj) Json.parse(a.json(), JSON);
        o.fields().put("_status", Json.num(a.status()));
        return o;
    }

    static String pretty(Json.Node n) {
        StringWriter w = new StringWriter();
        w.write(Json.toPretty(n));
        return w.toString();
    }
}
