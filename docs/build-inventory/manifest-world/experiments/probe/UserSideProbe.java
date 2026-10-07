import com.legend.Compiler;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.stream.*;

/** Homework probe (not committed): boot whatever prelude.pure is first on the classpath, then compile each user
 *  module (a project directory, a demo) as the product does — parse, build, type-check every body — and report
 *  every wall. Run once with today's prelude and once per swapped world; the difference is what the world lacks. */
public class UserSideProbe {
    public static void main(String[] a) throws Exception {
        long t0 = System.nanoTime();
        try {
            Compiler.buildModule(Compiler.parseSources(List.of()).model());
        } catch (Throwable e) {
            System.out.println("BOOT FAILED: " + chain(e));
            System.exit(2);
        }
        long t1 = System.nanoTime();
        System.out.printf("boot ms=%d%n", (t1 - t0) / 1_000_000);
        int totFiles = 0, totEls = 0, totBuild = 0, totBody = 0, totParse = 0;
        try (var out = Files.newBufferedWriter(Path.of(a[0]))) {
            for (int i = 1; i < a.length; i++) {
                Path dir = Path.of(a[i]);
                List<Path> files;
                try (Stream<Path> s = Files.walk(dir)) { files = s.filter(p -> p.toString().endsWith(".pure")).sorted().toList(); }
                if (files.isEmpty()) continue;
                List<Compiler.ModelSource> srcs = new ArrayList<>();
                for (Path f : files) srcs.add(new Compiler.ModelSource(f.toString(), Files.readString(f, StandardCharsets.UTF_8)));
                Map<String, String> pw = new LinkedHashMap<>();
                int els = 0, build = 0, body = 0;
                try {
                    var pm = Compiler.parseSources(srcs, pw::put);
                    els = pm.model().elements().size();
                    var bm = Compiler.buildModule(pm.model());
                    build = bm.walls().size();
                    for (var e : bm.walls().entrySet()) out.write(dir.getFileName() + "\tbuild\t" + e.getKey() + "\t" + one(e.getValue()) + "\n");
                    var bw = Compiler.compileAllBodies(bm.context());
                    body = bw.size();
                    for (var e : bw.entrySet()) out.write(dir.getFileName() + "\tbody\t" + e.getKey() + "\t" + one(e.getValue()) + "\n");
                } catch (Throwable e) {
                    out.write(dir.getFileName() + "\tTHREW\t\t" + one(chain(e)) + "\n");
                    build = -1;
                }
                for (var e : pw.entrySet()) out.write(dir.getFileName() + "\tparse\t" + e.getKey() + "\t" + one(e.getValue()) + "\n");
                totFiles += files.size(); totEls += els; totParse += pw.size(); totBuild += Math.max(build, 0); totBody += body;
                if (pw.size() + Math.max(build, 0) + body > 0 || build < 0)
                    System.out.printf("  %-28s files=%d elements=%d parseWalls=%d buildWalls=%s bodyWalls=%d%n", dir.getFileName(), files.size(), els, pw.size(), build < 0 ? "THREW" : build, body);
            }
        }
        System.out.printf("modules=%d files=%d elements=%d parseWalls=%d buildWalls=%d bodyWalls=%d%n", a.length - 1, totFiles, totEls, totParse, totBuild, totBody);
    }
    static String one(String s) { return s == null ? "" : s.replace('\n', ' ').replace('\t', ' '); }
    static String chain(Throwable e) { StringBuilder b = new StringBuilder(); for (Throwable c = e; c != null; c = c.getCause()) b.append(c).append(" | "); return b.toString(); }
}
