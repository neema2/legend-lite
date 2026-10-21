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
    private final com.legend.compiler.element.@com.legend.base.Nullable ModelContext compiled;
    private final java.util.@com.legend.base.Nullable List<String> classes;

    public Harness(String model, String runtime, Connection connection) {
        this(null, model, runtime, connection, null);
    }

    /** Over {@code model} already compiled ({@code compiled}, null to compile it per request as the server
     *  does): the query route reads the compilation and the request carries no model text (the stress corpus's
     *  model, 7,600 elements and 14 MB, compiles in seconds); {@code classes}, the schema's scope (the request's
     *  {@code classes}), or null for every class. */
    public Harness(com.legend.compiler.element.@com.legend.base.Nullable ModelContext compiled, String model,
            String runtime, Connection connection, java.util.@com.legend.base.Nullable List<String> classes) {
        this.model = model;
        this.runtime = runtime;
        this.compiled = compiled;
        this.classes = classes;
        this.runner = new PureV1Api.Runner() {
            @Override
            public com.legend.plan.QueryPlan run(String m, com.legend.protocol.spec.LambdaFunction lambda,
                    String rt, java.io.Writer rows) {
                try {
                    return com.legend.Execution.executeWire(m, lambda, rt, connection, rows);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }

            @Override
            public com.legend.plan.QueryPlan run(com.legend.compiler.element.ModelContext ctx, String m,
                    com.legend.protocol.spec.LambdaFunction lambda, String rt, java.io.Writer rows) {
                try {
                    return com.legend.Execution.executeWire(ctx, lambda, rt, connection, rows);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
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
        m.fields().put("code", Json.str(compiled != null ? "" : model));
        StringBuilder b = new StringBuilder("{\"model\":").append(Json.toCompact(m))
                .append(",\"runtime\":{\"runtime\":").append(Json.toCompact(runtime)).append("}")
                .append(",\"query\":").append(Json.toCompact(document));
        if (variablesJson != null) {
            b.append(",\"variables\":").append(variablesJson);
        }
        if (operationName != null) {
            b.append(",\"operationName\":").append(Json.toCompact(operationName));
        }
        if (classes != null) {
            b.append(",\"classes\":[");
            for (int i = 0; i < classes.size(); i++) {
                b.append(i > 0 ? "," : "").append(Json.toCompact(classes.get(i)));
            }
            b.append(']');
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
        PureV1Api.Answer a = compiled != null && path.equals("/api/lite/v1/graphql/query")
                ? com.legend.server.GraphqlApi.query(compiled, body, runner)
                : PureV1Api.route(path, null, body, runner);
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
