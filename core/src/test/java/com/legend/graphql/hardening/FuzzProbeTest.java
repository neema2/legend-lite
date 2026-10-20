// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.graphql.hardening;

import com.legend.testbackend.TestBackends;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

/** DEVELOPER EXPLORER (runs only with {@code -Dgql.probe=true}): a fuzz run in report mode over {@code -Dfuzz.seeds}
 *  ({@code -Dfuzz.n} documents each), failures clustered and shrunk, the report written to /tmp/fuzz-report.md. The
 *  committed judge is {@link GraphQLHardeningTest}. */
@org.junit.jupiter.api.condition.EnabledIfSystemProperty(named = "gql.probe", matches = "true")
class FuzzProbeTest {

    @Test
    void fuzz() throws Exception {
        int n = Integer.getInteger("fuzz.n", 300);
        String seeds = System.getProperty("fuzz.seeds", "1");
        long seed = 0;
        Map<String, Integer> clusters = new LinkedHashMap<>();
        Map<String, String> examples = new LinkedHashMap<>();
        int agree = 0;
        long t0 = System.nanoTime();
        try (Connection c = TestBackends.platform().open()) {
            ShopFixture.seed(c);
            Harness h = new Harness(ShopFixture.MODEL, ShopFixture.RUNTIME, c);
            for (String sd : seeds.split(",")) {
            seed = Long.parseLong(sd.trim());
            Fuzz f = new Fuzz(ShopReference.MODEL, seed, Map.of("Customer", "id", "Order", "id", "Line", "id",
                    "Product", "sku", "Tag", "id"), Map.of("Customer.tier", List.of("GOLD", "SILVER", "BRONZE"),
                    "Order.status", List.of("OPEN", "SHIPPED", "CANCELLED"),
                    "Product.category", List.of("BOOKS", "TOYS", "GARDEN")));
            for (int i = 0; i < n; i++) {
                Fuzz.GDoc d = f.document();
                String v = Fuzz.verdict(h, ShopReference.MODEL, d.render());
                if (v == null) {
                    agree++;
                    continue;
                }
                String sig = Fuzz.signature(v);
                clusters.merge(sig, 1, Integer::sum);
                if (!examples.containsKey(sig)) {
                    Fuzz.GDoc small = Fuzz.shrink(h, ShopReference.MODEL, d, 150);
                    examples.put(sig, small.render() + "\n     " + Fuzz.verdict(h, ShopReference.MODEL, small.render()));
                }
            }
            }
        }
        StringBuilder out = new StringBuilder();
        out.append(n).append(" documents, seed ").append(seed).append(": ").append(agree).append(" agree, ")
                .append(String.format(java.util.Locale.ROOT, "%.1f s", (System.nanoTime() - t0) / 1e9)).append("\n\n");
        for (Map.Entry<String, Integer> e : clusters.entrySet()) {
            out.append(e.getValue()).append("x  ").append(e.getKey()).append("\n     ")
                    .append(examples.get(e.getKey())).append("\n\n");
        }
        Files.writeString(Path.of("/tmp/fuzz-report.md"), out.toString());
    }
}
