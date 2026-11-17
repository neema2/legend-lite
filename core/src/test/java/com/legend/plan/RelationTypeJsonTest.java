// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.plan;

import com.legend.Compiler;
import com.legend.json.Json;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * A lite plan export's {@code type} is the product's own form of the compiler's type ({@link RelationTypeJson}): its
 * bytes unchanged by Phase 6's separation (2026-11-17, the orchestrator's ruling (a) -- the wasm boundary's exports
 * reach no edge class), which until then were {@code edge.UpstreamRelationType}'s, the compatibility edge's form for
 * pure/v1's {@code lambdaRelationType}. Pinned two ways: a scalar root's bytes literally, and every root below equal
 * to the edge's form (the same bytes today; they may part only by a ruled move).
 */
class RelationTypeJsonTest {

    private static String type(String query) {
        return Json.toCompact(RelationTypeJson.of(Compiler.query(Compiler.compileModel(""), query).resultType()));
    }

    @Test
    void aScalarRootsTypeIsItsBytesAsBefore() {
        assertEquals("{\"_type\":\"relationType\",\"columns\":[{\"genericType\":{\"multiplicityArguments\":[],"
                + "\"rawType\":{\"_type\":\"packageableType\",\"fullPath\":\"Integer\"},\"typeArguments\":[],"
                + "\"typeVariableValues\":[]},\"multiplicity\":{\"lowerBound\":1,\"upperBound\":1},\"name\":\"value\"}]}",
                type("|1"));
    }

    @Test
    void theProductsTypeIsTheBytesTheExportsWroteBefore() {
        for (String query : List.of("|1", "|'a'", "|[1, 2]", "|1.5D",
                "|#TDS\n  a:Integer, b:String, c:Float, d:DateTime\n  1, x, 1.5, 2024-01-02T10:30:00\n#",
                "|#TDS\n  a:Integer, b:String\n  1, x\n#->filter(r|$r.a > 0)->select(~[a])")) {
            var root = Compiler.query(Compiler.compileModel(""), query).resultType();
            assertEquals(Json.toCompact(com.legend.edge.UpstreamRelationType.of(root)),
                    Json.toCompact(RelationTypeJson.of(root)), query);
        }
    }
}
