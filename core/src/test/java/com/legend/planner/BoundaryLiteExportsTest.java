package com.legend.planner;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.legend.json.Json;
import com.legend.server.LiteV1Api;
import org.junit.jupiter.api.Test;

/**
 * A REFUSAL, FOLDED FOR A HOST (lite API v1, docs/records/lite-api-v1-2026-11-17.md §3, the user's go of 2026-11-17):
 * the tab's exports and Python's native entries answer a refusal as {@code "ERR\n"} and lite's one error shape, its JSON,
 * classified by the one classifier the server's routes use ({@link LiteV1Api#refusal}). Until 2026-11-17 this class held
 * D-1's four exports byte-identical to legend-engine's routes; the operations they were are lite API v1's now, held to
 * their answers by the conformance suite (//core:lite_api_conformance_test).
 */
class BoundaryLiteExportsTest {

    /** {@code e} folded: {@code "ERR\n"} and its refusal's JSON, as the server's routes classify it. */
    private static String refusal(Throwable e) {
        LiteV1Api.Refusal r = LiteV1Api.refusal(e);
        java.util.Map<String, Object> out = new java.util.LinkedHashMap<>();
        out.put("kind", r.kind().text());
        out.put("message", r.message());
        return "ERR\n" + Json.toCompact(out);
    }

    @Test
    void aRefusalIsFoldedAsItsJson() {
        RuntimeException parse = assertThrows(RuntimeException.class, () -> com.legend.Compiler.parseQuery("1 +"));
        assertEquals(refusal(parse), planner.Folded.failure(parse));
        RuntimeException compile = assertThrows(RuntimeException.class,
                () -> com.legend.Compiler.compileModel("Class demo::Desk\n{\n  name: Strin[1];\n}\n"));
        assertEquals(refusal(compile), planner.Folded.failure(compile));
        IllegalArgumentException other = new IllegalArgumentException("no");
        assertEquals(refusal(other), planner.Folded.failure(other));
        Json.Obj folded = Json.parseObject(planner.Folded.failure(parse).substring("ERR\n".length()));
        assertEquals(parse.getMessage(), folded.getString("message"));
    }
}
