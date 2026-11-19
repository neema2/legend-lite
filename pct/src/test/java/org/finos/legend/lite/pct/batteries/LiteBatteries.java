// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package org.finos.legend.lite.pct.batteries;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

/**
 * THE BATTERIES' PER-PUSH HALF (H7; CP-9 of docs/records/compat-program-plan-2026-11-19.md): a battery's requests
 * answered by lite as legend-engine's audience -- pure/v1's execute route as the edge ships it
 * ({@code PureV1Routes.execute} over {@code EdgeServer.runner}) -- each answer written as the oracle's are
 * ({@code com.legend.tools.enginelane.Batteries#answer}), for comparison with legend-engine's pinned answers. No legend-engine
 * JVM runs. The PCT package hosts it because it reaches the edge already; it compares nothing ({@code PctDisciplineTest})
 * and names no database: the model it is handed names its connection, and a file-backed database's file is handed by
 * path ({@code --database-file}, its token in the model {@code --file-token}), seeded by the oracle's package
 * ({@code //tools/engine-runner:lite_battery_seed}).
 *
 * <p>{@code run --model=<pure> --requests=<tsv> --out=<tsv> [--database-file=<file> --file-token=<text>]}.
 */
public final class LiteBatteries {

    private LiteBatteries() {
    }

    public static void main(String[] args) throws Exception {
        Map<String, String> o = new java.util.LinkedHashMap<>();
        for (int i = 1; i < args.length; i++) {
            o.put(args[i].substring(2, args[i].indexOf('=')), args[i].substring(args[i].indexOf('=') + 1));
        }
        if (args.length == 0 || !"run".equals(args[0])) {
            throw new IllegalArgumentException("run --model= --requests= --out= [--database-file= --file-token=]");
        }
        run(o);
        // the edge's server leaves non-daemon threads (its connection pools) behind
        System.exit(0);
    }

    private static void run(Map<String, String> o) throws Exception {
        String model = Files.readString(Path.of(o.get("model")));
        if (o.containsKey("database-file")) {
            // a copy, which the database may write (a build input is read-only), named where the model's token is. Its
            // path goes into a Pure string, which the parser unescapes (on Windows a backslash then a 't' reads as a
            // tab), so it is written with '/', as LegendHttpServerIntegrationTest and ConnectionLeaseTest write theirs
            Path copy = Files.createTempDirectory("lite-batteries").resolve(Path.of(o.get("database-file")).getFileName());
            Files.copy(Path.of(o.get("database-file")), copy);
            // a copy keeps its source's mode, and a build output is read-only: the database opens it to write, which a
            // user other than root may not do (CI's first run, 2026-11-19: every DuckDB request a database error)
            if (!copy.toFile().setWritable(true) || !Files.isWritable(copy)) {
                throw new IllegalStateException("the database file's copy cannot be made writable: " + copy);
            }
            model = model.replace(o.get("file-token"), copy.toAbsolutePath().toString().replace('\\', '/'));
        }
        String modelJson = com.legend.tools.enginelane.Batteries.textModel(model);
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
            out.append(com.legend.tools.enginelane.Batteries.answer(name, a.status(), a.json())).append('\n');
        }
        Files.writeString(Path.of(o.get("out")), out, StandardCharsets.UTF_8);
    }
}
