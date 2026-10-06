package com.legend.equivalence;

import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

/** THE FIXTURE WORKLIST, a measurement asked for by name (//parser-equivalence:pmcd_worklist, Bazel workplan
 *  P3-17): the roster's UNCOVERED protocol tags (//parser-equivalence:gen_roster: what the corpus, the fixtures and
 *  our own test snippets cover) against the upstream record of the classes reachable from PureModelContextData
 *  ({@link PmcdReachability}, parser-equivalence/pmcd-reachable.tsv). Reachable and uncovered is the fixture
 *  worklist; unreachable is proven out of text-parity scope. */
public final class PmcdWorklist {

    private PmcdWorklist() {}

    /** {@code args[0]}: the directory the report goes in (the action's {@code {OUT_DIR}}). */
    public static void main(String[] args) throws Exception {
        Path outDir = Path.of(args[0]);
        com.legend.testing.Programs.captureConsole(outDir);
        Set<String> reachable = Files.readAllLines(Path.of(Objects.requireNonNull(System.getProperty("pe.reachable"),
                        "-Dpe.reachable names pmcd-reachable.tsv"))).stream()
                .filter(l -> !l.isBlank() && !l.startsWith("#"))
                .collect(Collectors.toSet());
        // the roster, as //parser-equivalence:gen_roster makes it (Bazel workplan P2-19), by its exec path
        List<String> lines = Files.readAllLines(Path.of(Objects.requireNonNull(System.getProperty("pe.roster"),
                "-Dpe.roster names :gen_roster's output")));
        Map<String, List<String>> inScope = new TreeMap<>();
        int outOfScope = 0;
        int inScopeCount = 0;
        for (String line : lines) {
            String[] parts = line.split("\t");
            if (parts.length < 3 || !"UNCOVERED".equals(parts[2])) {
                continue;
            }
            if (reachable.contains(parts[1])) {
                inScopeCount++;
                String pkg = parts[1].substring(0, parts[1].lastIndexOf('.'))
                        .replace("org.finos.legend.engine.protocol.", "");
                inScope.computeIfAbsent(pkg, k -> new ArrayList<>()).add(parts[0]);
            } else {
                outOfScope++;
            }
        }
        try (PrintWriter out = new PrintWriter(Files.newBufferedWriter(outDir.resolve("pmcd-worklist.txt")))) {
            out.println("@@ reachable protocol classes: " + reachable.size());
            out.println("@@ uncovered & IN-SCOPE (fixture worklist): " + inScopeCount
                    + "; uncovered & UNREACHABLE (proven out): " + outOfScope);
            inScope.forEach((pkg, tags) -> out.println("@@ IN [" + pkg + "] " + String.join(", ", tags)));
        }
    }
}
