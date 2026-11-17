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

    /** The suite's own reading depth: a case's tree nests as deep as a grid's query (the server reads a request to 4096,
     *  LiteV1Api's REQUEST), past the parser's default of 64. */
    private static final Json.Config DEEP = new Json.Config(4096);

    @Test
    void theBrowsersOperationsAnswerEachCase() throws Exception {
        Json.Obj fixture = (Json.Obj) Json.parse(Files.readString(Runfile.property("lite.api.cases"),
                StandardCharsets.UTF_8), DEEP);
        List<String> failures = new ArrayList<>();
        int asked = 0;
        for (Json.Node n : fixture.getArr("cases").items()) {
            Json.Obj c = (Json.Obj) n;
            String name = c.getString("name");
            if (!servedHere(c)) {
                continue;
            }
            String folded = folded(c.getString("op"), Json.toCompact(c.get("request")));
            asked++;
            if (c.has("refusal")) {
                // a refusal case names its kind alone (§3: the kind is the contract, the words the product's)
                if (!folded.startsWith("ERR\n") || !includes(c.get("refusal"),
                        Json.parse(folded.substring("ERR\n".length()), DEEP))) {
                    failures.add(name + ": answered " + folded);
                }
                continue;
            }
            if (!folded.startsWith("OK\n")) {
                failures.add(name + ": refused " + folded);
                continue;
            }
            Json.Node answer = Json.parse(folded.substring("OK\n".length()), DEEP);
            if (c.has("answerShape")) {
                if (!shaped(c.getObj("answerShape"), answer)) {
                    failures.add(name + ": answered " + Json.toCompact(answer));
                }
            } else if (c.has("answer")) {
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

    /** Whether the case is the browser binding's: it names no bindings, or names {@code browser}; a binding that is
     *  none of the three is refused by name. */
    private static boolean servedHere(Json.Obj c) {
        if (!c.has("bindings")) {
            return true;
        }
        boolean here = false;
        for (Json.Node b : c.getArr("bindings").items()) {
            String binding = ((Json.Str) b).value();
            if (!java.util.Set.of("browser", "server", "python").contains(binding)) {
                // a binding no reader is: a typo would skip the case silently everywhere
                throw new AssertionError("a case names a binding that is none of browser, server, python: " + binding);
            }
            here |= binding.equals("browser");
        }
        // a case naming the browser is asked of it, and an operation it has not fails by name (asked's default)
        return here;
    }

    /** Whether {@code answer} holds each key {@code shape} names, of the JSON type it names. */
    private static boolean shaped(Json.Obj shape, Json.Node answer) {
        if (!(answer instanceof Json.Obj a)) {
            return false;
        }
        for (var e : shape.fields().entrySet()) {
            Json.Node v = a.fields().get(e.getKey());
            String type = ((Json.Str) e.getValue()).value();
            boolean ok = switch (type) {
                case "string" -> v instanceof Json.Str;
                case "object" -> v instanceof Json.Obj;
                case "array" -> v instanceof Json.Arr;
                case "number" -> v instanceof Json.Num;
                case "boolean" -> v instanceof Json.Bool;
                default -> throw new AssertionError("a shape's type this reader does not know: " + type);
            };
            if (!ok) {
                return false;
            }
        }
        return true;
    }

    /** {@code op} asked as the tab's export folds it: {@code "OK\n"} and the answer, or {@code "ERR\n"} and the
     *  refusal ({@code planner.Folded}). */
    private static String folded(String op, String request) {
        return planner.Folded.of(() -> asked(op, request));
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
            case "ts/item" -> Boundary.tsItem(request);
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
