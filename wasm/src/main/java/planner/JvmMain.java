package planner;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * The JVM half of a differential: runs the tab's own exports' composition (lite API v1: a text query's
 * {@code queryParseOrError}, then {@code queryPlanOrError}, each {@code Folded.of} of the {@link Boundary} member), over a query
 * list and writes the answers for the WASM half to compare against. Same source,
 * same inputs — only the backend differs. A BUILD ACTION (wasm/BUILD.bazel,
 * jvm_answers): the answers are a function of the planner's source and the
 * queries, so Bazel caches them and re-plans only when either changes.
 *
 * <p>Usage: {@code JvmMain <model-file> <queries-tsv> <runtime> <out-file> [json]}. Each
 * TSV line is {@code name<TAB>query}: Pure text, or with {@code json} the lambda's
 * protocol JSON ({@code queryPlanOrError} alone, over {@link Boundary#queryPlan}); the output is one block per query,
 * {@code <<<name>>>\n<answer>\n<<<END>>>\n}, in the TSV's order.
 *
 * <p>It does not time anything. A build action shares its machine with every
 * other action Bazel is running, so a latency measured here would be noise
 * presented as a number; {@code startup.mjs} measures the module, by hand, on an
 * idle machine.
 */
public final class JvmMain {

    private JvmMain() {
    }

    public static void main(String[] args) throws IOException {
        String model = Files.readString(Path.of(args[0]), StandardCharsets.UTF_8);
        Map<String, String> queries = new LinkedHashMap<>();
        for (String line : Files.readAllLines(Path.of(args[1]), StandardCharsets.UTF_8)) {
            if (line.isEmpty()) {
                continue;
            }
            int tab = line.indexOf('\t');
            // The TSV escapes nothing but the separator itself, so a name never
            // contains a tab and the rest of the line is the query.
            queries.put(line.substring(0, tab), line.substring(tab + 1));
        }
        String runtime = args[2];
        boolean json = args.length > 4 && "json".equals(args[4]);

        StringBuilder out = new StringBuilder();
        for (Map.Entry<String, String> e : queries.entrySet()) {
            out.append("<<<").append(e.getKey()).append(">>>\n")
                    .append(json ? planned(model, e.getValue(), runtime) : parsedThenPlanned(model, e.getValue(), runtime))
                    .append('\n')
                    .append("<<<END>>>\n");
        }
        Files.writeString(Path.of(args[3]), out.toString(), StandardCharsets.UTF_8);
    }

    /** {@code queryPlanOrError} of a query's tree. */
    static String planned(String model, String queryJson, String runtime) {
        return Folded.of(() -> Boundary.queryPlan("{\"model\":" + com.legend.json.Json.toCompact(model) + ",\"query\":"
                + queryJson + ",\"runtime\":" + com.legend.json.Json.toCompact(runtime) + "}"));
    }

    /** A text query as the tab composes it: {@code queryParseOrError}, then -- parsed -- {@code queryPlanOrError} of its
     *  tree; a refusal of either its answer. */
    static String parsedThenPlanned(String model, String text, String runtime) {
        String parsed = Folded.of(() -> Boundary.queryParse("{\"text\":" + com.legend.json.Json.toCompact(text) + "}"));
        if (!parsed.startsWith("OK\n")) {
            return parsed;
        }
        return planned(model, com.legend.json.Json.toCompact(com.legend.json.Json.parseObject(
                parsed.substring("OK\n".length())).get("query")), runtime);
    }
}
