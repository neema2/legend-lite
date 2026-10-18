// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.legend.json.Json;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.Test;

/**
 * The GraphQL routes (docs/GRAPHQL_STUDY_2026_10_18.md §8.4): {@code /api/lite/v1/graphql/schema} answers the SDL
 * of a model's data space or class list, {@code /api/lite/v1/graphql/validate} a document's verdict with every
 * error, every warning and the shape; a model the generator refuses is 400 COMPILATION like any other.
 */
class GraphqlApiTest {

    static final PureV1Api.Runner NO_RUN = (m, l, r, w) -> {
        throw new AssertionError("no execution in these routes");
    };

    static String demo() {
        try (InputStream in = GraphqlApiTest.class.getResourceAsStream("/graphql-parity/models/demo.pure")) {
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    static String body(String model, String extra) {
        Json.Obj m = Json.obj();
        m.fields().put("_type", Json.str("text"));
        m.fields().put("code", Json.str(model));
        return "{\"model\":" + Json.toCompact(m) + (extra.isEmpty() ? "" : "," + extra) + "}";
    }

    static Json.Obj answer(String path, String body) {
        PureV1Api.Answer a = PureV1Api.route(path, null, body, NO_RUN);
        return (Json.Obj) Json.parse(a.json(), PureV1Api.REQUEST);
    }

    @Test
    void theSchemaRouteAnswersTheSdlOfTheWholeModel() {
        Json.Obj a = answer("/api/lite/v1/graphql/schema", body(demo(), ""));
        assertTrue(a.getString("sdl").startsWith("type Query {"), a.getString("sdl").substring(0, 40));
        assertEquals(java.util.List.of("demo::Address", "demo::Employee", "demo::Firm", "demo::Manager",
                "demo::Person"), a.getStringArray("classes"));
    }

    @Test
    void theSchemaRouteScopesByClassList() {
        Json.Obj a = answer("/api/lite/v1/graphql/schema",
                body(demo(), "\"classes\":[\"demo::Address\"]"));
        assertEquals(java.util.List.of("demo::Address"), a.getStringArray("classes"));
        assertTrue(a.getString("sdl").contains("type Address {"));
        assertFalse(a.getString("sdl").contains("type Firm"), "Firm is out of scope");
    }

    @Test
    void theSchemaRouteRefusesAModelTheGeneratorRefuses() {
        PureV1Api.Answer a = PureV1Api.route("/api/lite/v1/graphql/schema", null, body("""
                ###Pure
                Class demo::Firm { legalName: String[1]; employeesAggregate: Integer[1]; }
                Class demo::Person { name: String[1]; }
                Association demo::Employment { firm: demo::Firm[0..1]; employees: demo::Person[*]; }
                """, ""), NO_RUN);
        assertEquals(400, a.status());
        assertTrue(a.json().contains("demo::Firm.employeesAggregate"), a.json());
    }

    @Test
    void theValidateRouteAnswersTheShapeOrEveryError() {
        Json.Obj ok = answer("/api/lite/v1/graphql/validate",
                body(demo(), "\"query\":\"{ Person(limit: 2) { fullName firm { legalName } } }\""));
        assertTrue(ok.getBool("valid"));
        assertEquals("{ Person: [{ fullName: String!, firm: { legalName: String! }? }] }", ok.getString("shape"));
        assertEquals(1, ok.getArr("warnings").items().size(), "limit without orderBy");
        Json.Obj bad = answer("/api/lite/v1/graphql/validate",
                body(demo(), "\"query\":\"{ Person(where: { agee: { gt: 1 } }) { fullNam } }\""));
        assertFalse(bad.getBool("valid"));
        assertEquals(2, bad.getArr("errors").items().size());
        Json.Obj first = (Json.Obj) bad.getArr("errors").items().get(0);
        assertEquals("Person(where).agee", first.getString("path"));
        assertTrue(first.getString("message").contains("did you mean age"), first.getString("message"));
    }

    @Test
    void theValidateRouteAnswersAParseErrorAsAnError() {
        Json.Obj a = answer("/api/lite/v1/graphql/validate", body(demo(), "\"query\":\"{ Person { \""));
        assertFalse(a.getBool("valid"));
        Json.Obj first = (Json.Obj) a.getArr("errors").items().get(0);
        assertEquals("PARSE", first.getString("phase"));
    }

    @Test
    void theLimitsAreTheRequests() {
        Json.Obj a = answer("/api/lite/v1/graphql/validate", body(demo(),
                "\"query\":\"{ Firm { employees { firm { legalName } } } }\",\"limits\":{\"maxDepth\":2}"));
        assertFalse(a.getBool("valid"));
        assertTrue(a.toString().contains("over the limit of 2"), a.toString());
    }
}
