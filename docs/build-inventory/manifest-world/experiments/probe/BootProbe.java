import com.legend.Compiler;
import com.legend.parser.Dialect;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;

/** Homework probe (not committed): what boot costs today, and what a world of upstream files costs on top of it. */
public class BootProbe {
    public static void main(String[] a) throws Exception {
        long t0 = System.nanoTime();
        Compiler.buildModule(Compiler.parseSources(List.of()).model());   // the boot layer: today's prelude + system metamodel
        long t1 = System.nanoTime();
        System.out.printf("today's boot (cold, first in JVM) ms=%d%n", (t1 - t0) / 1_000_000);
        if (a.length == 0) return;
        List<Compiler.ModelSource> srcs = new ArrayList<>();
        long chars = 0;
        for (String f : Files.readAllLines(Path.of(a[0]))) {
            if (f.isBlank()) continue;
            String t = Files.readString(Path.of(f), StandardCharsets.UTF_8); chars += t.length(); srcs.add(new Compiler.ModelSource(f, t));
        }
        int reps = a.length > 1 ? Integer.parseInt(a[1]) : 5;
        long[] parse = new long[reps], build = new long[reps];
        int elements = 0, walls = 0, pwalls = 0;
        for (int r = 0; r < reps; r++) {
            Map<String, String> pw = new HashMap<>();
            long s0 = System.nanoTime();
            var pm = Compiler.parseSources(srcs, pw::put, Dialect.LEGEND_PLATFORM);
            long s1 = System.nanoTime();
            var bm = Compiler.buildModule(pm.model());
            long s2 = System.nanoTime();
            parse[r] = (s1 - s0) / 1_000_000; build[r] = (s2 - s1) / 1_000_000;
            elements = pm.model().elements().size(); walls = bm.walls().size(); pwalls = pw.size();
        }
        long e0 = System.nanoTime();
        Compiler.buildModule(Compiler.parseSources(List.of()).model());
        long e1 = System.nanoTime();
        System.out.printf("world files=%d chars=%,d elements=%d parseWalls=%d buildWalls=%d | per-graph constant (empty module, warm) ms=%d%n",
                srcs.size(), chars, elements, pwalls, walls, (e1 - e0) / 1_000_000);
        System.out.printf("world cold: parse ms=%d build ms=%d | warm median: parse ms=%d build ms=%d%n",
                parse[0], build[0], median(parse), median(build));
    }
    static long median(long[] x) { long[] y = Arrays.copyOfRange(x, 1, x.length); Arrays.sort(y); return y[y.length / 2]; }
}
