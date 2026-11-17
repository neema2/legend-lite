// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.planner;

import planner.Boundary;

import com.legend.json.Json;
import com.legend.testing.Runfile;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * LITE API V1'S CONFORMANCE SUITE, THE BROWSER BINDING (docs/records/lite-api-v1-2026-11-17.md §5, the user's go of
 * 2026-11-17): each case of engine-client/test/fixtures/lite-api-cases.json asked of the browser's operations -- the
 * planner {@link Boundary}'s members the tab's exports fold -- and its answer held to the case's, as the server's
 * routes and Python's native entries are by their own readers of the same file, so the three bindings cannot drift.
 */
class LiteApiConformanceTest {

    @Test
    void theBrowsersOperationsAnswerEachCase() throws Exception {
        Json.Obj fixture = (Json.Obj) Json.parse(Files.readString(Runfile.property("lite.api.cases"),
                StandardCharsets.UTF_8));
        List<String> failures = new ArrayList<>();
        int asked = 0;
        for (Json.Node n : fixture.getArr("cases").items()) {
            Json.Obj c = (Json.Obj) n;
            String name = c.getString("name");
            Json.Node answer = Json.parse(asked(c.getString("op"), Json.toCompact(c.get("request"))));
            asked++;
            if (c.has("answer")) {
                if (!c.get("answer").equals(answer)) {
                    failures.add(name + ": answered " + Json.toCompact(answer));
                }
            } else if (!includes(c.get("answerIncludes"), answer)) {
                failures.add(name + ": answered " + Json.toCompact(answer));
            }
        }
        assertTrue(asked > 0, "the suite holds cases");
        assertEquals(List.of(), failures);
    }

    /** {@code op} asked of the browser's operation of that name (the spec's §2): its answer's JSON. */
    private static String asked(String op, String request) {
        return switch (op) {
            case "model/check" -> Boundary.modelCheck(request);
            case "model/elements" -> Boundary.modelElements(request);
            case "query/parse" -> Boundary.queryParse(request);
            case "query/print" -> Boundary.queryPrint(request);
            case "query/type" -> Boundary.queryType(request);
            case "query/plan" -> Boundary.queryPlan(request);
            case "model/createClass" -> Boundary.modelCreateClass(request);
            case "model/createAssociation" -> Boundary.modelCreateAssociation(request);
            case "model/createDataSpace" -> Boundary.modelCreateDataSpace(request);
            case "ts/statement" -> Boundary.tsStatement(request);
            default -> throw new AssertionError("the browser binding has no operation '" + op + "'");
        };
    }

    /** Whether {@code actual} holds {@code expected}: each key it names equal, recursively; each array item it names
     *  held by some item of the actual array; any other node equal. */
    private static boolean includes(Json.Node expected, Json.Node actual) {
        if (expected instanceof Json.Obj e) {
            if (!(actual instanceof Json.Obj a)) {
                return false;
            }
            for (Map.Entry<String, Json.Node> f : e.fields().entrySet()) {
                if (!a.has(f.getKey()) || !includes(f.getValue(), a.get(f.getKey()))) {
                    return false;
                }
            }
            return true;
        }
        if (expected instanceof Json.Arr e) {
            if (!(actual instanceof Json.Arr a)) {
                return false;
            }
            for (Json.Node item : e.items()) {
                if (a.items().stream().noneMatch(x -> includes(item, x))) {
                    return false;
                }
            }
            return true;
        }
        return expected.equals(actual);
    }
}
