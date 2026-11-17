// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.legend.json.Json;
import com.legend.protocol.PureComposer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * LITE'S OWN GRAMMAR AND TYPING ROUTES ({@link LiteV1Planning}; the edge program's HOST stream, 2026-11-17), reached as a
 * host reaches them: {@code /api/lite/v1/...} through {@link LiteV1Api#route}. Each answers what the product's own call
 * answers -- the call the tab's lite export makes -- and each request is closed: a key it lacks or does not read is
 * refused by name.
 */
class LiteV1PlanningTest {

    private static final String MODEL = """
            Class demo::Person
            {
              name: String[1];
              age: Integer[1];
            }
            ###Relational
            Database demo::DB ( Table PERSON ( NAME VARCHAR(100), AGE INTEGER ) )
            ###Mapping
            Mapping demo::M
            (
              *demo::Person: Relational
              {
                ~mainTable [demo::DB]PERSON
                name: [demo::DB]PERSON.NAME,
                age: [demo::DB]PERSON.AGE
              }
            )
            """;

    private static final String QUERY = "|demo::Person.all()->project(~[name: p|$p.name, age: p|$p.age])";

    private static LiteV1Api.Answer call(String path, Map<String, Object> request) {
        return LiteV1Api.route(path, null, Json.toCompact(request), (q, r, output) -> {
            throw new AssertionError("nothing is run here");
        });
    }

    private static Map<String, Object> request(Object... pairs) {
        Map<String, Object> out = new LinkedHashMap<>();
        for (int i = 0; i < pairs.length; i += 2) {
            out.put((String) pairs[i], pairs[i + 1]);
        }
        return out;
    }

    private static Map<String, Object> textModel() {
        return request("_type", "text", "code", MODEL);
    }

    private static Object lambdaOf(String text) {
        return Json.parse(com.legend.door.Parse.lambdaJson(text, false));
    }

    @Test
    @DisplayName("a model's text as its protocol JSON, the parse door's answer")
    void modelElements() {
        for (boolean sourceInformation : new boolean[] {false, true}) {
            LiteV1Api.Answer a = call(LiteV1Planning.MODEL_ELEMENTS, request("text", MODEL, "sourceInformation", sourceInformation));
            assertEquals(200, a.status(), a.json());
            assertEquals(com.legend.door.Parse.documentJson(MODEL, sourceInformation), a.json());
        }
    }

    @Test
    @DisplayName("a lambda's text as its protocol JSON, and its JSON as its text, as the product reads and prints them")
    void parseAndPrintLambda() {
        LiteV1Api.Answer parsed = call(LiteV1Planning.PARSE_LAMBDA, request("text", QUERY, "sourceInformation", false));
        assertEquals(200, parsed.status(), parsed.json());
        assertEquals(com.legend.door.Parse.lambdaJson(QUERY, false), parsed.json());
        for (String style : new String[] {"PRETTY", "STANDARD"}) {
            LiteV1Api.Answer printed = call(LiteV1Planning.PRINT_LAMBDA, request("lambda", Json.parse(parsed.json()), "style", style));
            assertEquals(200, printed.status(), printed.json());
            assertEquals(PureComposer.lambda(parsed.json(), PureComposer.Style.valueOf(style)),
                    Json.parseObject(printed.json()).getString("text"));
        }
    }

    @Test
    @DisplayName("a lambda's relation type over a model given as text, the compiler's type of its result")
    void lambdaRelationType() {
        LiteV1Api.Answer a = call(LiteV1Planning.LAMBDA_RELATION_TYPE, request("model", textModel(), "lambda", lambdaOf(QUERY)));
        assertEquals(200, a.status(), a.json());
        assertEquals(Json.toCompact(com.legend.plan.RelationTypeJson.of(com.legend.Compiler.query(
                com.legend.Compiler.compileModel(MODEL), com.legend.protocol.ProtocolReader.lambda(
                        com.legend.door.Parse.lambdaJson(QUERY, false))).resultType())), a.json());
        assertTrue(a.json().contains("\"name\"") && a.json().contains("age"), a.json());
    }

    @Test
    @DisplayName("text that does not parse or compile is refused with its kind: PARSER, COMPILATION")
    void refusalsWithTheirKind() {
        LiteV1Api.Answer badModel = call(LiteV1Planning.MODEL_ELEMENTS, request("text", "Class demo::{", "sourceInformation", false));
        assertEquals(400, badModel.status(), badModel.json());
        assertEquals("PARSER", Json.parseObject(badModel.json()).getString("errorType"));
        LiteV1Api.Answer badLambda = call(LiteV1Planning.PARSE_LAMBDA, request("text", "|demo::Person.all()->", "sourceInformation", false));
        assertEquals(400, badLambda.status(), badLambda.json());
        assertEquals("PARSER", Json.parseObject(badLambda.json()).getString("errorType"));
        LiteV1Api.Answer badType = call(LiteV1Planning.LAMBDA_RELATION_TYPE,
                request("model", textModel(), "lambda", lambdaOf("|demo::Person.all()->project(~[x: p|$p.nope])")));
        assertEquals(400, badType.status(), badType.json());
        assertEquals("COMPILATION", Json.parseObject(badType.json()).getString("errorType"));
    }

    @Test
    @DisplayName("each request is closed: a key it lacks, a key it does not read, and a style it does not print, refused by name")
    void closedRequests() {
        LiteV1Api.Answer extra = call(LiteV1Planning.PARSE_LAMBDA, request("text", QUERY, "sourceInformation", false, "returnSourceInformation", false));
        assertEquals(500, extra.status(), extra.json());
        assertTrue(Json.parseObject(extra.json()).getString("message").contains("a parseLambda request has no key returnSourceInformation"), extra.json());
        LiteV1Api.Answer missing = call(LiteV1Planning.MODEL_ELEMENTS, request("text", MODEL));
        assertEquals(500, missing.status(), missing.json());
        assertTrue(Json.parseObject(missing.json()).getString("message").contains("a modelElements request names no sourceInformation"), missing.json());
        LiteV1Api.Answer style = call(LiteV1Planning.PRINT_LAMBDA, request("lambda", lambdaOf(QUERY), "style", "COMPACT"));
        assertEquals(500, style.status(), style.json());
        assertTrue(Json.parseObject(style.json()).getString("message").contains("a lambda prints PRETTY or STANDARD, not 'COMPACT'"), style.json());
    }

    @Test
    @DisplayName("a path under lite's API that is none of its routes is still LiteV1Api's 404")
    void anyOtherPath() {
        LiteV1Api.Answer a = call("/api/lite/v1/grammar/lambdaText", request());
        assertEquals(404, a.status(), a.json());
    }
}
