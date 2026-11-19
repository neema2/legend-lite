// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package org.finos.legend.lite.pct.oracle;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

/**
 * THE BATTERIES' PER-PUSH HALF (H7; CP-9 of docs/records/compat-program-plan-2026-11-19.md): a battery's requests
 * answered by lite as legend-engine's audience -- pure/v1's execute route as the edge ships it
 * ({@code PureV1Routes.execute} over {@code EdgeServer.runner}) -- each answer written as the oracle's are
 * ({@code planoracle.enginelane.Batteries#answer}), for comparison with legend-engine's pinned answers. No legend-engine
 * JVM runs. The PCT package hosts it because it reaches the edge already; it compares nothing ({@code PctDisciplineTest}):
 * the comparison is {@code Batteries}' own.
 *
 * <ul>
 *   <li>{@code seed --model=<pure> --duckdb=<file> --digest=<txt>}: a DuckDB file seeded with lite's own driver from
 *       the statements the model's LocalH2 connection carries ({@code testDataSetupSqls}), the statements
 *       legend-engine's oracle seeds its file from; and the rows it holds ({@code Batteries} digest).</li>
 *   <li>{@code run --model=<pure> [--duckdb-connection=<pure> --duckdb=<file>] --requests=<tsv> --out=<tsv>}: each
 *       request answered on the model's H2, or with its connection replaced by the DuckDB one on a copy of the
 *       file.</li>
 * </ul>
 */
public final class LiteBatteries {

    private LiteBatteries() {
    }

    public static void main(String[] args) throws Exception {
        Map<String, String> o = new java.util.LinkedHashMap<>();
        for (int i = 1; i < args.length; i++) {
            o.put(args[i].substring(2, args[i].indexOf('=')), args[i].substring(args[i].indexOf('=') + 1));
        }
        switch (args.length == 0 ? "" : args[0]) {
            case "seed" -> seed(o.get("model"), o.get("duckdb"), o.get("digest"));
            case "run" -> run(o);
            default -> throw new IllegalArgumentException("seed or run");
        }
        // the edge's server leaves non-daemon threads (its connection pools) behind
        System.exit(0);
    }

    /** The statements the model's LocalH2 connection seeds its database with. */
    private static List<String> seedOf(String model) {
        for (com.legend.model.PackageableElement e : com.legend.door.ModelFromText.parse(model,
                com.legend.parser.GrammarLevel.LEGEND_ENGINE).elements()) {
            if (e instanceof com.legend.model.ConnectionDefinition c
                    && c.specification() instanceof com.legend.model.ConnectionSpecification.LocalH2 h2
                    && h2.testDataSetupSqls() != null) {
                return h2.testDataSetupSqls();
            }
        }
        throw new IllegalStateException("the model has no LocalH2 connection carrying testDataSetupSqls");
    }

    private static void seed(String modelFile, String duckdb, String digest) throws Exception {
        List<String> statements = seedOf(Files.readString(Path.of(modelFile)));
        try (java.sql.Connection c = java.sql.DriverManager.getConnection("jdbc:duckdb:" + duckdb);
                java.sql.Statement s = c.createStatement()) {
            for (String sql : statements) {
                s.execute(sql);
            }
        }
        Files.writeString(Path.of(digest), planoracle.enginelane.Batteries.digestOf(duckdb) + "\n",
                StandardCharsets.UTF_8);
    }

    private static void run(Map<String, String> o) throws Exception {
        String model = Files.readString(Path.of(o.get("model")));
        if (o.containsKey("duckdb")) {
            // the model's connection replaced by the DuckDB one, pointed at a copy (DuckDB opens a file to write, and
            // a build input is read-only)
            Path copy = Files.createTempDirectory("lite-batteries").resolve("trades.duckdb");
            Files.copy(Path.of(o.get("duckdb")), copy);
            String connection = Files.readString(Path.of(o.get("duckdb-connection")))
                    .replace("${DUCKDB_PATH}", copy.toString());
            int from = model.indexOf("###Connection");
            int to = model.indexOf("###Runtime");
            if (from < 0 || to < from) {
                throw new IllegalStateException("the model's ###Connection section is not before its ###Runtime");
            }
            model = model.substring(0, from) + connection.substring(connection.indexOf("###Connection")) + "\n\n"
                    + model.substring(to);
        }
        String modelJson = planoracle.enginelane.Batteries.textModel(model);
        StringBuilder out = new StringBuilder("# lite's answers to the battery, as legend-engine's audience (pure/v1's"
                + " execute route)\n");
        for (String line : Files.readAllLines(Path.of(o.get("requests")), StandardCharsets.UTF_8)) {
            if (line.isBlank() || line.startsWith("#")) {
                continue;
            }
            int t = line.indexOf('\t');
            String name = line.substring(0, t);
            String function = com.legend.edge.PureV1Routes.grammarToJsonLambda(line.substring(t + 1), false).json();
            String body = "{\"clientVersion\":\"vX_X_X\",\"function\":" + function + ",\"model\":" + modelJson
                    + ",\"context\":{\"_type\":\"BaseExecutionContext\"}}";
            com.legend.server.LiteV1Api.Answer a = com.legend.edge.PureV1Routes.execute(body,
                    com.legend.edge.EdgeServer.runner(null));
            out.append(planoracle.enginelane.Batteries.answer(name, a.status(), a.json())).append('\n');
        }
        Files.writeString(Path.of(o.get("out")), out, StandardCharsets.UTF_8);
    }
}
