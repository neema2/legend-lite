package planner;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The JVM half of the differential: runs {@link Wasm#planOrError} over
 * the corpus and writes the answers where the WASM half's runner can
 * diff them. Same source, same inputs — only the backend differs.
 *
 * <p>Usage: {@code JvmMain <model-file> <queries-tsv> <runtime> <out-file>}
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
            // The TSV writer escapes nothing but the separator itself, so a
            // name never contains a tab and the rest of the line is the query.
            queries.put(line.substring(0, tab), line.substring(tab + 1));
        }
        String runtime = args[2];

        // Boot cost is the first plan: class init plus parsing the prelude.
        long b0 = System.nanoTime();
        Wasm.planOrError(model, queries.values().iterator().next(), runtime);
        double bootMs = (System.nanoTime() - b0) / 1e6;

        StringBuilder out = new StringBuilder();
        for (Map.Entry<String, String> e : queries.entrySet()) {
            out.append("<<<").append(e.getKey()).append(">>>\n")
                    .append(Wasm.planOrError(model, e.getValue(), runtime)).append('\n')
                    .append("<<<END>>>\n");
        }
        Files.writeString(Path.of(args[3]), out.toString(), StandardCharsets.UTF_8);

        List<Double> samples = new ArrayList<>();
        String warm = queries.getOrDefault("pipeline", queries.values().iterator().next());
        for (int i = 0; i < 500; i++) {
            long a = System.nanoTime();
            Wasm.planOrError(model, warm, runtime);
            samples.add((System.nanoTime() - a) / 1e6);
        }
        double[] s = samples.stream().mapToDouble(Double::doubleValue).toArray();
        Arrays.sort(s);
        System.err.printf("JVM bootMs=%.2f p50=%.3f p95=%.3f p99=%.3f min=%.3f max=%.3f n=%d%n",
                bootMs, s[s.length / 2], s[(int) (s.length * 0.95)],
                s[(int) (s.length * 0.99)], s[0], s[s.length - 1], s.length);
    }
}
