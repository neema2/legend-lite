// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.legend.json.Json;

import org.junit.jupiter.api.Test;

/**
 * The MCP wrapper (docs/GRAPHQL_STUDY_2026_10_18.md §8.4, G14; PR6): JSON-RPC in, the four tools, a call routed
 * to the GraphQL route it names, the pinned model, the errors of the protocol.
 */
class GraphqlMcpTest {

    static final GraphqlMcp MCP = new GraphqlMcp(GraphqlApiTest.RUN, null);
    static final GraphqlMcp PINNED = new GraphqlMcp(GraphqlApiTest.RUN, GraphqlApiTest.STORE);

    static Json.Obj rpc(GraphqlMcp mcp, String method, String params) {
        String body = "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":" + Json.toCompact(method)
                + (params == null ? "" : ",\"params\":" + params) + "}";
        String answer = mcp.handle(body);
        assertTrue(answer != null, "a request is answered");
        return (Json.Obj) Json.parse(answer, PureV1Api.REQUEST);
    }

    static String call(GraphqlMcp mcp, String tool, String arguments) {
        Json.Obj a = rpc(mcp, "tools/call", "{\"name\":" + Json.toCompact(tool) + ",\"arguments\":" + arguments + "}");
        assertTrue(a.has("result"), a.toString());
        Json.Obj result = a.getObj("result");
        Json.Obj content = (Json.Obj) result.getArr("content").items().get(0);
        assertEquals("text", content.getString("type"));
        return content.getString("text");
    }

    @Test
    void initializeAnswersTheServerAndTheReference() {
        Json.Obj a = rpc(MCP, "initialize", "{\"protocolVersion\":\"2025-06-18\",\"capabilities\":{},\"clientInfo\":{\"name\":\"t\",\"version\":\"0\"}}");
        Json.Obj r = a.getObj("result");
        assertEquals(GraphqlMcp.PROTOCOL_VERSION, r.getString("protocolVersion"));
        assertEquals("legend-lite-graphql", r.getObj("serverInfo").getString("name"));
        assertTrue(r.getObj("capabilities").has("tools"));
        assertTrue(r.getString("instructions").contains("one-page reference"), "the reference is the instructions");
        assertTrue(r.getString("instructions").contains("_gt"), "the wrong example is in it");
        assertNull(MCP.handle("{\"jsonrpc\":\"2.0\",\"method\":\"notifications/initialized\"}"), "a notification has no answer");
        assertTrue(rpc(MCP, "ping", null).has("result"));
    }

    @Test
    void theFourTools() {
        Json.Obj a = rpc(MCP, "tools/list", null);
        Json.Arr tools = a.getObj("result").getArr("tools");
        assertEquals(4, tools.items().size(), a.toString());
        java.util.List<String> names = new java.util.ArrayList<>();
        for (Json.Node t : tools.items()) {
            Json.Obj tool = (Json.Obj) t;
            names.add(tool.getString("name"));
            assertTrue(tool.getObj("inputSchema").getObj("properties").has("model"), tool.toString());
        }
        assertEquals(java.util.List.of("describe", "validate", "query", "explain"), names);
    }

    @Test
    void describeValidateQueryExplainRouteToTheRoutes() {
        String model = Json.toCompact(GraphqlApiTest.STORE);
        String described = call(MCP, "describe", "{\"model\":" + model + ",\"classes\":[\"demo::Firm\"]}");
        assertTrue(described.contains("type Firm") && described.contains("one-page reference"), described);
        assertFalse(described.contains("type Address"), "scoped to the classes named");
        String verdict = call(MCP, "validate", "{\"model\":" + model
                + ",\"query\":\"{ Person(where: { age: { _gt: 30 } }) { fullName } }\"}");
        assertTrue(verdict.contains("\"valid\":false") && verdict.contains("_gt"), verdict);
        String answer = call(MCP, "query", "{\"model\":" + model + ",\"runtime\":\"demo::Runtime\","
                + "\"query\":\"{ Person(orderBy: [{ age: DESC }], limit: 1) { fullName } }\"}");
        assertTrue(answer.contains("\"data\"") && answer.contains("Carol White"), answer);
        String plan = call(MCP, "explain", "{\"model\":" + model + ",\"runtime\":\"demo::Runtime\","
                + "\"query\":\"{ PersonAggregate { count } }\"}");
        assertTrue(plan.contains("\"sql\"") && plan.contains("\"ROW\""), plan);
    }

    @Test
    void thePinnedModelAnswersACallThatNamesNone() {
        String described = call(PINNED, "describe", "{\"classes\":[\"demo::Person\"]}");
        assertTrue(described.contains("type Person"), described);
        Json.Obj unnamed = rpc(MCP, "tools/call", "{\"name\":\"describe\",\"arguments\":{}}");
        assertTrue(unnamed.has("error") && unnamed.getObj("error").getString("message").contains("--graphql-model"),
                unnamed.toString());
    }

    @Test
    void theProtocolsErrors() {
        Json.Obj unknown = rpc(MCP, "resources/list", null);
        assertEquals(-32601, unknown.getObj("error").getInt("code"), unknown.toString());
        Json.Obj noTool = rpc(MCP, "tools/call", "{\"name\":\"run\",\"arguments\":{}}");
        assertEquals(-32602, noTool.getObj("error").getInt("code"), noTool.toString());
        String bad = MCP.handle("{not json");
        assertTrue(bad != null && bad.contains("-32700"), bad);
        // a route's refusal is the tool's error content, not a protocol error
        Json.Obj refused = rpc(MCP, "tools/call", "{\"name\":\"query\",\"arguments\":{\"model\":"
                + Json.toCompact(GraphqlApiTest.STORE) + ",\"query\":\"{ Nobody { x } }\"}}");
        assertTrue(refused.getObj("result").getBool("isError"), refused.toString());
    }
}
