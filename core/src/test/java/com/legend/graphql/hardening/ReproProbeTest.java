// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.graphql.hardening;

import com.legend.json.Json;
import com.legend.testbackend.TestBackends;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;

import org.junit.jupiter.api.Test;

/** DEVELOPER EXPLORER (runs only with {@code -Dgql.probe=true}): each document of /tmp/repro.txt answered, judged
 *  against the reference and explained (its Pure and SQL), written to /tmp/repro-out.md. */
@org.junit.jupiter.api.condition.EnabledIfSystemProperty(named = "gql.probe", matches = "true")
class ReproProbeTest {
    @Test
    void repro() throws Exception {
        StringBuilder out = new StringBuilder();
        try (Connection c = TestBackends.platform().open()) {
            ShopFixture.seed(c);
            Harness h = new Harness(ShopFixture.MODEL, ShopFixture.RUNTIME, c);
            for (String d : Files.readAllLines(Path.of("/tmp/repro.txt"))) {
                if (d.isBlank()) {
                    continue;
                }
                out.append("### ").append(d).append("\n");
                out.append("lite: ").append(Json.toCompact(h.query(d))).append("\n");
                try {
                    out.append("reference: ").append(OVal.render(Reference.answer(ShopReference.MODEL, d, null, null))).append("\n");
                } catch (RuntimeException e) {
                    out.append("reference: ").append(e).append("\n");
                }
                out.append("verdict: ").append(Fuzz.verdict(h, ShopReference.MODEL, d)).append("\n");
                Json.Obj ex = h.explain(d);
                if (ex.fields().get("roots") instanceof Json.Arr roots) {
                    for (Json.Node r : roots.items()) {
                        out.append("pure: ").append(((Json.Obj) r).getString("pure")).append("\n");
                        out.append("sql: ").append(((Json.Obj) r).getString("sql")).append("\n");
                    }
                } else {
                    out.append("explain: ").append(Json.toCompact(ex)).append("\n");
                }
                out.append("\n");
            }
        }
        Files.writeString(Path.of("/tmp/repro-out.md"), out.toString());
    }
}
