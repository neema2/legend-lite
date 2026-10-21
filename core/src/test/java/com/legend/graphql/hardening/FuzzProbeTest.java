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
        Map<String, Integer> kinds = new java.util.TreeMap<>();
        Map<String, java.util.List<String>> originals = new LinkedHashMap<>();
        int agree = 0;
        long t0 = System.nanoTime();
        try (Connection c = TestBackends.platform().open()) {
            ShopFixture.seed(c);
            Harness h = new Harness(ShopFixture.MODEL, ShopFixture.RUNTIME, c);
            for (String sd : seeds.split(",")) {
            seed = Long.parseLong(sd.trim());
            Fuzz f = new Fuzz(ShopReference.MODEL, seed, Map.of("Customer", "id", "Order", "id", "Line", "id",
                    "Product", "sku", "Tag", "id", "Payment", "id", "CardPayment", "id", "VoucherPayment", "id",
                    "Listing", "sku"), Map.of("Customer.tier", List.of("GOLD", "SILVER", "BRONZE"),
                    "Order.status", List.of("OPEN", "SHIPPED", "CANCELLED"),
                    "Product.category", List.of("BOOKS", "TOYS", "GARDEN")),
                    Map.of("Customer.tier", "Tier", "Order.status", "Status", "Product.category", "Category"));
            for (int i = 0; i < n; i++) {
                Fuzz.GDoc d = f.document();
                try {
                    Reference.answer(ShopReference.MODEL, d.render(), d.variables() == null ? null
                            : (com.legend.json.Json.Obj) com.legend.json.Json.parse(d.variables(), Harness.JSON), null);
                    kinds.merge("answered", 1, Integer::sum);
                } catch (Reference.Refused rf) {
                    kinds.merge("refused: " + rf.getMessage().replaceAll("[0-9]+", "N").replaceAll("\\[.*", ""), 1,
                            Integer::sum);
                } catch (Reference.Undetermined ud) {
                    kinds.merge("undetermined", 1, Integer::sum);
                } catch (RuntimeException re) {
                    kinds.merge("reference threw: " + re, 1, Integer::sum);
                }
                for (String root : List.of("Aggregate", "Groups", "... on", "...F", "@include", "@skip", ": ", "$v",
                        "businessDate")) {
                    if (d.render().contains(root)) {
                        kinds.merge("feature " + root, 1, Integer::sum);
                    }
                }
                String v = Fuzz.verdict(h, ShopReference.MODEL, d);
                if (v == null) {
                    agree++;
                    continue;
                }
                String sig = Fuzz.signature(v);
                clusters.merge(sig, 1, Integer::sum);
                originals.computeIfAbsent(sig, k -> new java.util.ArrayList<>()).add(d.render()
                        + (d.variables() == null ? "" : "  variables " + d.variables()) + "\n       " + v);
                if (!examples.containsKey(sig)) {
                    Fuzz.GDoc small = Fuzz.shrink(h, ShopReference.MODEL, d, 150);
                    examples.put(sig, small.render() + (small.variables() == null ? "" : "  variables "
                            + small.variables()) + "\n     " + Fuzz.verdict(h, ShopReference.MODEL, small));
                }
            }
            }
        }
        StringBuilder out = new StringBuilder();
        out.append(n).append(" documents, seed ").append(seed).append(": ").append(agree).append(" agree, ")
                .append(String.format(java.util.Locale.ROOT, "%.1f s", (System.nanoTime() - t0) / 1e9)).append("\n");
        for (Map.Entry<String, Integer> k : kinds.entrySet()) {
            out.append("  ").append(k.getValue()).append("  ").append(k.getKey()).append("\n");
        }
        out.append("\n");
        for (Map.Entry<String, Integer> e : clusters.entrySet()) {
            out.append(e.getValue()).append("x  ").append(e.getKey()).append("\n     ")
                    .append(examples.get(e.getKey())).append("\n");
            for (String o : originals.get(e.getKey()).subList(0, Math.min(3, originals.get(e.getKey()).size()))) {
                out.append("   original: ").append(o).append("\n");
            }
            out.append("\n");
        }
        Files.writeString(Path.of("/tmp/fuzz-report.md"), out.toString());
    }
}
