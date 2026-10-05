package com.legend.tools.junit.compare;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Stream;
import javax.xml.parsers.DocumentBuilderFactory;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

/**
 * Do two test runs select the same tests? (Bazel workplan P1-02.) Each argument is a directory of
 * {@code bazel-testlogs} (a CI lane's uploaded artifact). Per test target it reads the old runner's
 * {@code test.outputs/junit/TEST-*.xml} or, without them, the target's {@code test.xml} (every shard's),
 * and prints the sorted {@code classname#name} testcases only one side has: {@code -} the first, {@code +}
 * the second. A target only one side ran is named as such. Nothing printed: identical selection. Exits 1
 * on any difference. With {@code --union}, every target's testcases are one set on each side: the proof of a split
 * (one target into many, Bazel workplan P3-05), where no target name is on both sides.
 *
 * <pre>bazel run //tools/junit:compare_testcases -- [--union] &lt;baseline-dir&gt; &lt;after-dir&gt;</pre>
 */
public final class CompareTestcases {

    private CompareTestcases() {}

    public static void main(String[] args) throws Exception {
        boolean union = args.length == 3 && args[0].equals("--union");
        if (args.length != 2 && !union) {
            throw new IllegalArgumentException("usage: compare_testcases [--union] <baseline-dir> <after-dir>");
        }
        Map<String, Set<String>> before = testcases(Path.of(args[union ? 1 : 0]));
        Map<String, Set<String>> after = testcases(Path.of(args[union ? 2 : 1]));
        if (union) {
            before = merged(before);
            after = merged(after);
        }
        boolean differ = false;
        Set<String> targets = new TreeSet<>(before.keySet());
        targets.addAll(after.keySet());
        for (String target : targets) {
            Set<String> b = before.get(target);
            Set<String> a = after.get(target);
            if (b == null || a == null) {
                System.out.println(target + ": only in the " + (b == null ? "second" : "first") + " run");
                differ = true;
                continue;
            }
            for (String id : b) {
                if (!a.contains(id)) {
                    System.out.println(target + ": - " + id);
                    differ = true;
                }
            }
            for (String id : a) {
                if (!b.contains(id)) {
                    System.out.println(target + ": + " + id);
                    differ = true;
                }
            }
        }
        System.exit(differ ? 1 : 0);
    }

    /** Target (its testlogs path) → its testcases. */
    static Map<String, Set<String>> testcases(Path root) throws Exception {
        Map<String, List<Path>> legacy = new TreeMap<>();
        Map<String, List<Path>> bazel = new TreeMap<>();
        try (Stream<Path> files = Files.walk(root)) {
            for (Path f : files.filter(Files::isRegularFile).toList()) {
                String rel = root.relativize(f).toString().replace('\\', '/');
                int junit = rel.indexOf("/test.outputs/junit/");
                if (junit >= 0 && rel.endsWith(".xml")) {
                    legacy.computeIfAbsent(rel.substring(0, junit), k -> new ArrayList<>()).add(f);
                } else if (rel.endsWith("/test.xml")) {
                    String dir = rel.substring(0, rel.length() - "/test.xml".length());
                    bazel.computeIfAbsent(dir.replaceFirst("/shard_\\d+_of_\\d+$", ""), k -> new ArrayList<>()).add(f);
                }
            }
        }
        Map<String, Set<String>> out = new TreeMap<>();
        for (var e : bazel.entrySet()) {
            out.put(e.getKey(), read(legacy.containsKey(e.getKey()) ? legacy.get(e.getKey()) : e.getValue()));
        }
        for (var e : legacy.entrySet()) {
            out.putIfAbsent(e.getKey(), read(e.getValue()));
        }
        return out;
    }

    private static Set<String> read(List<Path> files) throws IOException {
        Set<String> ids = new TreeSet<>();
        try {
            var builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
            for (Path f : files) {
                NodeList cases = builder.parse(f.toFile()).getElementsByTagName("testcase");
                for (int i = 0; i < cases.getLength(); i++) {
                    Element c = (Element) cases.item(i);
                    ids.add(c.getAttribute("classname") + "#" + c.getAttribute("name"));
                }
            }
        } catch (javax.xml.parsers.ParserConfigurationException | org.xml.sax.SAXException e) {
            throw new IOException(files + ": " + e.getMessage(), e);
        }
        return ids;
    }

    /** Every target's testcases as one set, under the name "(all targets)". */
    private static Map<String, Set<String>> merged(Map<String, Set<String>> byTarget) {
        Set<String> all = new TreeSet<>();
        byTarget.values().forEach(all::addAll);
        return Map.of("(all targets)", all);
    }
}
