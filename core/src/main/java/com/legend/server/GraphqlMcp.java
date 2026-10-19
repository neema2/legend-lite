// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.server;

import com.legend.json.Json;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * THE MCP WRAPPER over the four GraphQL routes (docs/GRAPHQL_STUDY_2026_10_18.md §8.4, G14; PR6): the Model
 * Context Protocol's Streamable HTTP transport, one JSON-RPC 2.0 message per {@code POST /mcp}, answered as JSON.
 * The methods: {@code initialize} (the server's name, its capabilities, the one-page reference as its
 * instructions), {@code notifications/initialized} (no answer), {@code ping}, {@code tools/list} (the four tools
 * with their input schemas, each description the reference's line), {@code tools/call} (the route's answer as the
 * tool's text content; {@code isError} where the route refused). Nothing here plans or runs a query: a call is
 * the HTTP route's body built from the arguments and routed as the HTTP handler routes it.
 *
 * <p>A server started with a model ({@code --graphql-model FILE}) answers calls that name none; a call's own
 * {@code model} (the Pure text) wins. {@code dataSpace} or {@code classes} scope the schema as the routes do.
 */
public final class GraphqlMcp {

    public static final String PROTOCOL_VERSION = "2025-06-18";

    /** The four tools (G14), each the route it names. */
    public enum Tool {
        DESCRIBE("describe", "/api/lite/v1/graphql/schema"),
        VALIDATE("validate", "/api/lite/v1/graphql/validate"),
        QUERY("query", "/api/lite/v1/graphql/query"),
        EXPLAIN("explain", "/api/lite/v1/graphql/explain");

        public final String toolName;
        public final String route;

        Tool(String toolName, String route) {
            this.toolName = toolName;
            this.route = route;
        }

        /** The tool a call names, or null. */
        static @com.legend.base.Nullable Tool named(String name) {
            for (Tool t : values()) {
                if (t.toolName.equals(name)) {
                    return t;
                }
            }
            return null;
        }
    }

    private final PureV1Api.Runner runner;
    private final @com.legend.base.Nullable String pinnedModel;

    public GraphqlMcp(PureV1Api.Runner runner, @com.legend.base.Nullable String pinnedModel) {
        this.runner = runner;
        this.pinnedModel = pinnedModel;
    }

    /** One message in; its response out, or null for a notification (the transport answers 202). */
    public @com.legend.base.Nullable String handle(String body) {
        Json.Obj message;
        try {
            message = PureV1Api.request(body);
        } catch (RuntimeException e) {
            return error(null, -32700, "parse error: " + e.getMessage());
        }
        Object id = message.getOr("id", null);
        String method = message.getStringOr("method", null);
        if (method == null) {
            return error(id, -32600, "a request names its method");
        }
        if (id == null) {
            return null;    // a notification (notifications/initialized, notifications/cancelled): no answer
        }
        Json.Obj params = message.getObjOr("params", null);
        return switch (method) {
            case "initialize" -> result(id, initialize());
            case "ping" -> result(id, new LinkedHashMap<>());
            case "tools/list" -> {
                Map<String, Object> r = new LinkedHashMap<>();
                r.put("tools", tools());
                yield result(id, r);
            }
            case "tools/call" -> {
                if (params == null) {
                    yield error(id, -32602, "tools/call takes params { name, arguments }");
                }
                String name = params.getStringOr("name", null);
                if (name == null) {
                    yield error(id, -32602, "tools/call names its tool");
                }
                Tool tool = Tool.named(name);
                if (tool == null) {
                    yield error(id, -32602, "no tool named " + name + ": describe, validate, query, explain");
                }
                yield call(id, tool, params.getObjOr("arguments", null));
            }
            default -> error(id, -32601, "method not found: " + method);
        };
    }

    private static Map<String, Object> initialize() {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("protocolVersion", PROTOCOL_VERSION);
        Map<String, Object> capabilities = new LinkedHashMap<>();
        Map<String, Object> tools = new LinkedHashMap<>();
        tools.put("listChanged", false);
        capabilities.put("tools", tools);
        out.put("capabilities", capabilities);
        Map<String, Object> info = new LinkedHashMap<>();
        info.put("name", "legend-lite-graphql");
        info.put("title", "legend-lite GraphQL");
        info.put("version", "0.1");
        out.put("serverInfo", info);
        out.put("instructions", GraphqlReference.TEXT);
        return out;
    }

    /** The four tools (G14), each with its input schema. */
    public static List<Map<String, Object>> tools() {
        List<Map<String, Object>> out = new ArrayList<>();
        out.add(tool(Tool.DESCRIBE.toolName, "Describe the schema",
                "The SDL generated for the data space (dataSpace) or for the classes named (classes), with the"
                + " one-page reference of the dialect. Ask for the classes you need, not the whole model.",
                scopeProperties(), List.of()));
        Map<String, Object> q = scopeProperties();
        q.put("query", string("the GraphQL document"));
        q.put("variables", object("the operation's variables, by name"));
        q.put("operationName", string("the operation to run, when the document holds several"));
        out.add(tool(Tool.VALIDATE.toolName, "Validate a document",
                "The document's verdict without running it: valid, the typed result shape, every error with its"
                + " path and the nearest valid name, the warnings (a limit without an orderBy).",
                new LinkedHashMap<>(q), List.of("query")));
        Map<String, Object> run = new LinkedHashMap<>(q);
        run.put("runtime", string("the runtime to run on (the model's, when it has one)"));
        out.add(tool(Tool.QUERY.toolName, "Run a document",
                "The document run over the data space, answered in the GraphQL-over-HTTP envelope: data under each"
                + " root field's key, errors with their paths, extensions.pure with the Pure each root ran. All or"
                + " nothing: an error fails the whole request.",
                run, List.of("query")));
        out.add(tool(Tool.EXPLAIN.toolName, "Explain a document",
                "The verdict of validate and, per root field, the Pure the document means and the SQL it runs on"
                + " the runtime, without running it.",
                new LinkedHashMap<>(run), List.of("query")));
        return out;
    }

    private static Map<String, Object> tool(String name, String title, String description,
            Map<String, Object> properties, List<String> required) {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("name", name);
        out.put("title", title);
        out.put("description", description);
        Map<String, Object> schema = new LinkedHashMap<>();
        schema.put("type", "object");
        schema.put("properties", properties);
        if (!required.isEmpty()) {
            schema.put("required", required);
        }
        out.put("inputSchema", schema);
        return out;
    }

    private static Map<String, Object> scopeProperties() {
        Map<String, Object> p = new LinkedHashMap<>();
        p.put("model", string("the Pure model text; optional where the server was started with one"));
        p.put("dataSpace", string("the data space whose classes the schema covers"));
        Map<String, Object> classes = new LinkedHashMap<>();
        classes.put("type", "array");
        classes.put("items", string("a class's full name"));
        classes.put("description", "the classes the schema covers, when no data space is named");
        p.put("classes", classes);
        return p;
    }

    private static Map<String, Object> string(String description) {
        Map<String, Object> s = new LinkedHashMap<>();
        s.put("type", "string");
        s.put("description", description);
        return s;
    }

    private static Map<String, Object> object(String description) {
        Map<String, Object> s = new LinkedHashMap<>();
        s.put("type", "object");
        s.put("description", description);
        return s;
    }

    /** A tool called: the route's body built from the arguments, routed, its answer the tool's text; a call that
     *  names no model where the server pins none is the protocol's invalid-params error. */
    private String call(Object id, Tool tool, @com.legend.base.Nullable Json.Obj arguments) {
        Json.Obj args = arguments == null ? Json.obj() : arguments;
        String model = args.getStringOr("model", null);
        if (model == null) {
            model = pinnedModel;
        }
        if (model == null) {
            return error(id, -32602, "the call names no model and the server was started without one"
                    + " (--graphql-model FILE)");
        }
        Json.Obj body = Json.obj();
        Json.Obj m = Json.obj();
        m.fields().put("_type", Json.str("text"));
        m.fields().put("code", Json.str(model));
        body.fields().put("model", m);
        for (String key : List.of("dataSpace", "classes", "query", "variables", "operationName", "limits")) {
            if (args.has(key)) {
                body.fields().put(key, args.get(key));
            }
        }
        String runtime = args.getStringOr("runtime", null);
        if (runtime != null) {
            Json.Obj rt = Json.obj();
            rt.fields().put("runtime", Json.str(runtime));
            body.fields().put("runtime", rt);
        }
        PureV1Api.Answer answer = PureV1Api.route(tool.route, null, Json.toCompact(body), runner);
        String text = answer.json();
        if (tool == Tool.DESCRIBE && answer.status() == 200) {
            Json.Obj described = (Json.Obj) Json.parse(text, PureV1Api.REQUEST);
            described.fields().put("reference", Json.str(GraphqlReference.TEXT));
            text = Json.toCompact(described);
        }
        Map<String, Object> out = new LinkedHashMap<>();
        Map<String, Object> content = new LinkedHashMap<>();
        content.put("type", "text");
        content.put("text", text);
        out.put("content", List.of(content));
        out.put("isError", answer.status() >= 400);
        return result(id, out);
    }

    private static String result(Object id, Map<String, Object> result) {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("jsonrpc", "2.0");
        out.put("id", id);
        out.put("result", result);
        return Json.toCompact(out);
    }

    private static String error(@com.legend.base.Nullable Object id, int code, String message) {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("jsonrpc", "2.0");
        out.put("id", id);
        Map<String, Object> error = new LinkedHashMap<>();
        error.put("code", code);
        error.put("message", message);
        out.put("error", error);
        return Json.toCompact(out);
    }
}
