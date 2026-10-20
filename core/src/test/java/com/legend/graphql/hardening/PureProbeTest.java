// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.graphql.hardening;

import com.legend.testbackend.TestBackends;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;

import org.junit.jupiter.api.Test;

/** DEVELOPER EXPLORER (runs only with {@code -Dgql.probe=true}): each Pure query of /tmp/pure.txt over the shop
 *  model (two derived collections added), answered or its stack and resolved tree written to /tmp/pure-out.md. */
@org.junit.jupiter.api.condition.EnabledIfSystemProperty(named = "gql.probe", matches = "true")
class PureProbeTest {
    @Test
    void probe() throws Exception {
        StringBuilder out = new StringBuilder();
        String model = ShopFixture.MODEL.replace("  orderCount() {",
                "  referredLoud() { $this.referred->sortBy(x|$x.loud()) }: shop::Customer[*];\n"
                + "  referredHandle() { $this.referred->sortBy(x|$x.handle()) }: shop::Customer[*];\n  orderCount() {");
        try (Connection c = TestBackends.platform().open()) {
            ShopFixture.seed(c);
            for (String q : Files.readAllLines(Path.of("/tmp/pure.txt"))) {
                if (q.isBlank()) {
                    continue;
                }
                out.append("### ").append(q).append("\n");
                try {
                    var l = com.legend.Compiler.query(com.legend.Compiler.compileModel(model), q)
                            .lower(ShopFixture.RUNTIME, false);
                    out.append("sql: ").append(com.legend.Execution.class.getName()).append("\n");
                    StringWriter w = new StringWriter();
                    com.legend.Execution.executeWire(model, q, ShopFixture.RUNTIME, c,
                            com.legend.lowering.WireRender.Format.JSON, w);
                    out.append("ok: ").append(w).append("\n");
                } catch (Throwable e) {
                    try {
                        var tq = com.legend.Compiler.query(com.legend.Compiler.compileModel(model), q);
                        var specs = new com.legend.compiler.spec.SpecCompiler(tq.context());
                        var b = new com.legend.compiler.spec.UserCallInliner(specs).inlineBody(
                                new java.util.ArrayList<>(tq.body()));
                        b = new com.legend.resolver.StoreResolver(tq.context(), specs).resolve(b, ShopFixture.RUNTIME);
                        out.append("resolved: ").append(b.get(b.size() - 1)).append("\n");
                    } catch (Throwable e2) {
                        out.append("resolve failed: ").append(e2).append("\n");
                    }
                    StringWriter sw = new StringWriter();
                    e.printStackTrace(new PrintWriter(sw));
                    String[] lines = sw.toString().split("\n");
                    for (int i = 0; i < Math.min(lines.length, 40); i++) {
                        out.append(lines[i]).append("\n");
                    }
                }
                out.append("\n");
            }
        }
        Files.writeString(Path.of("/tmp/pure-out.md"), out.toString());
    }
}
