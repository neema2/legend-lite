import com.legend.Compiler;
import com.legend.parser.Dialect;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;

/** Homework probe (not committed): parse, then build, a list of upstream .pure files with legend-lite's own compiler. */
public class Probe {
    static String clip(String s, int n) { return s.substring(0, Math.min(n, s.length())); }
    public static void main(String[] a) throws Exception {
        List<Path> files = Files.readAllLines(Path.of(a[0])).stream().filter(s -> !s.isBlank()).map(Path::of).toList();
        boolean build = a.length > 1 && a[1].equals("build");
        List<Compiler.ModelSource> srcs = new ArrayList<>();
        long bytes = 0;
        for (Path f : files) { String t = Files.readString(f, StandardCharsets.UTF_8); bytes += t.length(); srcs.add(new Compiler.ModelSource(f.toString(), t)); }
        Map<String, String> parseWalls = new LinkedHashMap<>();
        long t0 = System.nanoTime();
        Compiler.ParsedModule pm = Compiler.parseSources(srcs, parseWalls::put, Dialect.LEGEND_PLATFORM);
        long t1 = System.nanoTime();
        System.out.printf("files=%d chars=%,d elements=%d parseWalls=%d duplicates=%d parseMs=%d%n", files.size(), bytes,
                pm.model().elements().size(), parseWalls.size(), pm.duplicateElements().size(), (t1 - t0) / 1_000_000);
        Map<String, Integer> why = new TreeMap<>();
        parseWalls.forEach((f, e) -> why.merge(clip(e.replaceAll("\\d+", "N").replaceAll("'[^']*'", "'X'").replaceAll("\\s+", " "), 90), 1, Integer::sum));
        why.entrySet().stream().sorted((x, y) -> y.getValue() - x.getValue()).limit(8).forEach(e -> System.out.printf("  parse wall x%d: %s%n", e.getValue(), e.getKey()));
        parseWalls.entrySet().stream().limit(5).forEach(e -> System.out.printf("  e.g. %s: %s%n", e.getKey().replaceAll(".*/src/main/resources/", ""), e.getValue().substring(0, Math.min(120, e.getValue().length()))));
        pm.duplicateElements().stream().limit(5).forEach(d -> System.out.println("  duplicate: " + d));
        if (!build) return;
        long t2 = System.nanoTime();
        Compiler.BuiltModule bm;
        try { bm = Compiler.buildModule(pm.model()); }
        catch (Throwable e) { System.out.println("BUILD THREW: " + e); return; }
        long t3 = System.nanoTime();
        System.out.printf("buildWalls=%d buildMs=%d%n", bm.walls().size(), (t3 - t2) / 1_000_000);
        Map<String, Integer> bw = new TreeMap<>();
        bm.walls().values().forEach(m -> bw.merge(clip(m.replaceAll("'[^']*'", "'X'").replaceAll("\\b[a-z_]+(::[A-Za-z0-9_]+)+", "FQN").replaceAll("\\d+", "N").replaceAll("\\s+", " "), 100), 1, Integer::sum));
        bw.entrySet().stream().sorted((x, y) -> y.getValue() - x.getValue()).limit(12).forEach(e -> System.out.printf("  build wall x%d: %s%n", e.getValue(), e.getKey()));
        // the duplicates: which FQNs, so the caller can check them against the prelude and Pure.java
        Files.write(Path.of(a[0] + ".buildwalls"), bm.walls().entrySet().stream().map(e -> e.getKey() + "\t" + e.getValue().replace('\n', ' ')).toList());
        if (a.length > 2 && a[2].equals("bodies")) {
            long t4 = System.nanoTime();
            Map<String, String> bodyWalls = Compiler.compileAllBodies(bm.context());
            long t5 = System.nanoTime();
            long fns = pm.model().elements().stream().filter(e -> e instanceof com.legend.model.FunctionDefinition).count();
            System.out.printf("functions in world=%d bodyWalls=%d bodiesMs=%d%n", fns, bodyWalls.size(), (t5 - t4) / 1_000_000);
            Map<String, Integer> bb = new TreeMap<>();
            bodyWalls.values().forEach(m -> bb.merge(clip(m.replaceAll("'[^']*'", "'X'").replaceAll("\\b[a-z_]+(::[A-Za-z0-9_]+)+", "FQN").replaceAll("\\d+", "N").replaceAll("\\s+", " "), 90), 1, Integer::sum));
            bb.entrySet().stream().sorted((x, y) -> y.getValue() - x.getValue()).limit(14).forEach(e -> System.out.printf("  body wall x%d: %s%n", e.getValue(), e.getKey()));
            Files.write(Path.of(a[0] + ".bodywalls"), bodyWalls.entrySet().stream().map(e -> e.getKey() + "\t" + e.getValue().replace('\n', ' ')).toList());
        }
    }
}
