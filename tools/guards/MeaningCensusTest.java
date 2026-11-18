// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.tools.guards;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.legend.testing.Runfile;
import com.legend.testing.SourceFiles;
import com.legend.testing.TestOutputs;
import com.legend.tools.meaning.ClassFiles;
import com.legend.tools.meaning.ClassFiles.ClassFile;
import com.legend.tools.meaning.FormsLens;
import com.legend.tools.meaning.HostSources;
import com.legend.tools.meaning.MeaningCensus;
import com.legend.tools.meaning.HostSources.Source;
import com.legend.tools.meaning.MeaningCensus.Hit;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;

/**
 * THE MEANING CENSUS, REPORT-ONLY (census B2, 2026-11-19; the follow-ups' B2: "a census that can see"; its lenses are
 * //tools/meaning's, its test here, where the JUnit pool may be used). Every lens of {@link MeaningCensus} and
 * {@link FormsLens} runs over the whole product -- core's Java (its jars' class files), its
 * resources, the hosts' Java (the native library's and the tab's entries, the queries page), the TypeScript product
 * and the Python product -- and each hit is classified by the committed inventory ({@code inventory.tsv}): (a)
 * upstream meaning in the product, (b) covered by a user ruling, (c) no upstream meaning, with the reason. The report
 * is written to the test's outputs ({@code meaning-census.tsv}, {@code meaning-census-summary.txt}) and its counts
 * printed.
 *
 * <p>REPORT-ONLY: no hit fails the build, classified or not ({@link #BLOCKING}, false until B3 has closed the items; a
 * later decision). What fails is a lens going blind: each lens has a planted violator it must find
 * ({@link #eachLensBitesItsPlant}), and each input a floor the scan must reach ({@link #FLOORS}), so a scope that
 * rots reads as a failure, not as a clean count.
 */
class MeaningCensusTest {

    /** Whether an (a) hit or an unclassified one fails the build: false (report-only) until B3 closes the items. */
    static final boolean BLOCKING = false;

    /** Each input's floor, 2026-11-19 (a fifth under the measure: growth is free, a fall below is a scope that rotted). */
    static final Map<String, Integer> FLOORS = Map.of(
            "product classes", 2800,
            "edge classes", 150,
            "vocabulary names", 4000,
            "vocabulary sentences", 10000,
            "vocabulary routes", 100,
            "typescript product files", 300,
            "python product files", 25,
            "java product sources", 900);

    private static final Predicate<String> EDGE_CLASS = n -> n.startsWith("com/legend/edge/");

    // -----------------------------------------------------------------------------------------------------------------
    // The real run

    @Test
    void theProductIsReadAndItsMeaningReported() throws IOException {
        Map<String, ClassFile> product = ClassFiles.ofJars(Runfile.listed("meaning.product.jars"));
        Map<String, ClassFile> edge = ClassFiles.ofJars(Runfile.listed("meaning.edge.jars"));
        Map<String, ClassFile> hosts = ClassFiles.ofJars(Runfile.listed("meaning.host.jars"));
        hosts.forEach((n, c) -> (EDGE_CLASS.test(n) ? edge : product).putIfAbsent(n, c));
        product.keySet().removeIf(EDGE_CLASS);
        Map<String, String> resources = ClassFiles.resourcesOfJars(Runfile.listed("meaning.product.jars"));
        HostRule rule = HostRule.read(SourceFiles.file("tools/guards/host_sql.txt"));
        Set<String> edgePaths = edgeSources(Runfile.property("meaning.host.edge_srcs"));
        List<Source> hostProduct = new ArrayList<>();
        List<Source> hostEdge = new ArrayList<>();
        readHosts(rule, edgePaths, hostProduct, hostEdge);
        Map<String, String> javaSources = new TreeMap<>();
        for (String dir : List.of("core/src/main/java", "native/src/main/java", "wasm/src/main/java")) {
            for (Path p : SourceFiles.under(dir)) {
                String path = SourceFiles.path(p);
                if (path.endsWith(".java") && !path.contains("/com/legend/edge/")) {
                    javaSources.put(path, Files.readString(p, StandardCharsets.UTF_8));
                }
            }
        }
        Predicate<String> isTest = rule.isTest();

        Set<String> lite = new HashSet<>(MeaningCensus.liteNames(resources));
        lite.addAll(MeaningCensus.liteDeclared(product, hostProduct));
        MeaningCensus.Vocabulary v = MeaningCensus.Vocabulary.of(
                Files.readAllLines(Runfile.property("meaning.vocabulary"), StandardCharsets.UTF_8), lite,
                MeaningCensus.jdkNames()).withEdge(edgeStrings(edge, hostEdge), lite);

        Map<String, Integer> measured = new LinkedHashMap<>();
        measured.put("product classes", product.size());
        measured.put("edge classes", edge.size());
        measured.put("vocabulary names", v.names().size());
        measured.put("vocabulary sentences", v.sentences().size());
        measured.put("vocabulary routes", v.routes().size());
        measured.put("typescript product files", (int) hostProduct.stream()
                .filter(s -> s.lang() == HostSources.Lang.TYPESCRIPT && !isTest.test(s.path())).count());
        measured.put("python product files", (int) hostProduct.stream()
                .filter(s -> s.lang() == HostSources.Lang.PYTHON && !isTest.test(s.path())).count());
        measured.put("java product sources", javaSources.size());
        FLOORS.forEach((what, floor) -> assertTrue(measured.get(what) >= floor, "the census's scope rotted: " + what
                + " " + measured.get(what) + " < its floor " + floor + " (re-point the input before trusting a count)"));

        List<Source> productNoTests = hostProduct.stream().filter(s -> !isTest.test(s.path())).toList();
        Map<String, ClassFile> all = new LinkedHashMap<>(product);
        all.putAll(edge);
        Set<String> known = new HashSet<>();
        hostProduct.forEach(s -> known.add(s.path()));
        hostEdge.forEach(s -> known.add(s.path()));

        List<Hit> hits = new ArrayList<>();
        hits.addAll(MeaningCensus.text(MeaningCensus.javaStrings(product), v));
        hits.addAll(MeaningCensus.text(MeaningCensus.resourceStrings(resources), v));
        hits.addAll(MeaningCensus.text(MeaningCensus.hostStrings(productNoTests), v));
        hits.addAll(MeaningCensus.javaNames(product));
        hits.addAll(MeaningCensus.hostNames(productNoTests));
        for (FormsLens.Finding f : new FormsLens(all, EDGE_CLASS).findings()) {
            hits.add(new Hit("M3", f.shape(), f.where(), f.subject(), f.what()));
        }
        hits.addAll(MeaningCensus.printBodies(resources));
        hits.addAll(MeaningCensus.javaComments(javaSources));
        hits.addAll(MeaningCensus.hostComments(productNoTests));
        hits.addAll(MeaningCensus.edgeOnly(hostProduct, hostEdge, isTest));
        hits.addAll(MeaningCensus.hostReach(hostProduct, edgePaths, known, isTest, List.of("legend_lite_engine")));
        for (Map.Entry<String, List<String>> root : JAVA_ROOTS.entrySet()) {
            hits.addAll(MeaningCensus.javaReach(all, Set.copyOf(root.getValue()), EDGE_CLASS, root.getKey()));
        }
        hits.addAll(MeaningCensus.buildReach(Files.readAllLines(Runfile.property("meaning.shipped_reach")),
                "K2 with every host's edge targets"));

        Inventory inventory = Inventory.read(SourceFiles.file("tools/meaning/inventory.tsv"));
        List<String[]> rows = new ArrayList<>();
        Map<String, Integer> byClass = new TreeMap<>();
        Map<String, Integer> byLens = new TreeMap<>();
        List<Hit> unclassified = new ArrayList<>();
        for (Hit h : hits) {
            Inventory.Row r = inventory.classify(h);
            String cls = r == null ? "?" : r.cls();
            if (r == null) {
                unclassified.add(h);
            }
            byClass.merge(cls, 1, Integer::sum);
            byLens.merge(h.lens() + " " + h.category() + " " + cls, 1, Integer::sum);
            rows.add(new String[] {h.lens(), h.category(), h.where(), h.term(), cls, r == null ? "" : r.reason(),
                    h.detail()});
        }
        StringBuilder tsv = new StringBuilder("# lens\tcategory\twhere\tterm\tclass\treason\tdetail\n");
        for (String[] row : rows) {
            for (int k = 0; k < row.length; k++) {
                tsv.append(row[k].replace('\t', ' ').replace('\n', ' ')).append(k + 1 < row.length ? '\t' : '\n');
            }
        }
        Files.writeString(TestOutputs.file("meaning-census.tsv"), tsv.toString(), StandardCharsets.UTF_8);
        StringBuilder summary = new StringBuilder();
        summary.append("THE MEANING CENSUS (report-only; BLOCKING=").append(BLOCKING).append(")\n");
        summary.append("inputs: ").append(measured).append('\n');
        summary.append("vocabulary: ").append(v.counts()).append('\n');
        summary.append("hits: ").append(hits.size()).append(" by class ").append(byClass).append('\n');
        byLens.forEach((k, n) -> summary.append("  ").append(k).append(": ").append(n).append('\n'));
        summary.append("known items:\n");
        for (Inventory.Known k : inventory.known()) {
            long found = rows.stream().filter(r -> r[5].contains("[" + k.id() + "]")).count();
            summary.append("  ").append(k.id()).append(found > 0 ? " FOUND (" + found + " hits)" : " NOT FOUND: "
                    + k.unseen()).append(" -- ").append(k.what()).append('\n');
        }
        summary.append("unclassified: ").append(unclassified.size()).append('\n');
        unclassified.stream().limit(400).forEach(h -> summary.append("  ? ").append(h.lens()).append(' ')
                .append(h.category()).append(" | ").append(h.file()).append(" | ").append(h.term()).append('\n'));
        Files.writeString(TestOutputs.file("meaning-census-summary.txt"), summary.toString(), StandardCharsets.UTF_8);
        System.out.println(summary);
        if (BLOCKING) {
            assertEquals(0, byClass.getOrDefault("a", 0) + unclassified.size(), "upstream meaning in the product");
        }
    }

    /** The Java hosts' product entries: the classes M7's reach starts at, per shipped artifact. */
    static final Map<String, List<String>> JAVA_ROOTS = Map.of(
            "//core:server", List.of("com/legend/server/LegendHttpServer"),
            "//wasm:planner", List.of("planner/TabExports", "planner/Boundary"),
            "//native:compiler", List.of("com/legend/nativelib/Compiler", "com/legend/nativelib/ModelHome",
                    "com/legend/nativelib/WarehouseCalls", "com/legend/nativelib/SavedStores"),
            "//query-store:page", List.of("com/legend/queries/page/QueriesPage"));

    // -----------------------------------------------------------------------------------------------------------------
    // The plants: each lens must find its own

    @Test
    void eachLensBitesItsPlant() throws IOException {
        Map<String, ClassFile> planted = ClassFiles.ofJars(Runfile.listed("meaning.planted.jars"));
        Predicate<String> plantedEdge = n -> n.contains("/planted/edge/");
        Map<String, ClassFile> plantedProduct = new LinkedHashMap<>(planted);
        plantedProduct.keySet().removeIf(plantedEdge);
        assertTrue(plantedProduct.containsKey("com/legend/tools/meaning/planted/PlantedLowering"), "the plant is read");

        // M3: the forms
        List<FormsLens.Finding> forms = new FormsLens(planted, plantedEdge).findings();
        assertHas(forms, "UNUSED", "PlantedHook#raise(String, String)", "parameter 2");
        assertHas(forms, "ANSWER", "PlantedHook#frame()", "answers null");
        assertHas(forms, "BRANCH", "PlantedLowering#select", "PlantedHook#frame()");
        assertHas(forms, "BRANCH", "PlantedLowering#wrap", "parameter 2");
        assertHas(forms, "EDGE_LIVE", "PlantedLowering#prune(String, String)", "parameter 2");

        // M1 in Java: a legend-engine class name and sentence in a product string
        MeaningCensus.Vocabulary v = MeaningCensus.Vocabulary.of(List.of(
                "RelationalTdsInstantiationExecutionNode\tTYPE\tEXECUTION\t1\tplanted",
                "Query project group ID is invalid\tSENTENCE\tEXECUTION\t1\tplanted",
                "rootExecutionNode\tFIELD\tEXECUTION\t2\tplanted",
                "tdsColumns\tFIELD\tEXECUTION\t1\tplanted",
                "relationalType\tFIELD\tEXECUTION\t1\tplanted",
                "server/v1/currentUser\tROUTE\tEXECUTION\t1\tplanted",
                "fullPath\tFIELD\tLANGUAGE\t1\tplanted",
                "/api/server/v1/currentUser\tTEXT\t-\t1\t-"), Set.of(), Set.of());
        assertTrue(!v.names().containsKey("fullPath"), "a LANGUAGE surface name is the language's, left out");
        List<Hit> javaText = MeaningCensus.text(MeaningCensus.javaStrings(plantedProduct), v);
        assertHit(javaText, "M1", "NAME", "PlantedLowering", "RelationalTdsInstantiationExecutionNode");
        assertHit(javaText, "M1", "SENTENCE", "PlantedLowering", "Query project group ID is invalid");

        // M1 with the edge's own strings: a string the edge holds and upstream holds too, moved into the product
        Map<String, ClassFile> edgeOnly = new LinkedHashMap<>(planted);
        edgeOnly.keySet().removeIf(plantedEdge.negate());
        MeaningCensus.Vocabulary withEdge = MeaningCensus.Vocabulary.of(List.of(
                "/api/server/v1/currentUser\tTEXT\t-\t1\t-"), Set.of(), Set.of()).withEdge(edgeStrings(edgeOnly,
                List.of()), Set.of());
        assertTrue(withEdge.edge().containsKey("/api/server/v1/currentUser"), "the edge's verified string is vocabulary");

        // the hosts' plants: TypeScript and Python product files reading legend-engine's shapes
        Map<String, Source> plants = new TreeMap<>();
        for (Path p : SourceFiles.under("tools/meaning/planted")) {
            String path = SourceFiles.path(p);
            if (HostSources.Lang.of(path) != null) {
                plants.put(path, HostSources.read(path, Files.readString(p, StandardCharsets.UTF_8)));
            }
        }
        List<Source> hostProduct = plants.values().stream().filter(s -> !s.path().contains("/edge/")).toList();
        List<Source> hostEdge = plants.values().stream().filter(s -> s.path().contains("/edge/")).toList();
        assertTrue(hostProduct.size() >= 3 && hostEdge.size() >= 3, "the host plants are read: " + plants.keySet());
        List<Hit> hostText = MeaningCensus.text(MeaningCensus.hostStrings(hostProduct), v);
        assertHit(hostText, "M1", "NAME", "planted/ts/product/relation-reader.ts", "rootExecutionNode");
        assertHit(hostText, "M1", "NAME", "planted/ts/product/relation-reader.ts", "tdsColumns");
        assertHit(hostText, "M1", "NAME", "planted/py/legend_lite/_planted_reader.py", "relationalType");
        assertHit(hostText, "M1", "NAME", "planted/py/legend_lite/_planted_reader.py", "tdsColumns");

        // M2: a name naming upstream
        List<Hit> names = new ArrayList<>(MeaningCensus.hostNames(hostProduct));
        assertHit(names, "M2", "NAME", "relation-reader.ts", "legendEngineShape");
        assertHit(names, "M2", "NAME", "_planted_reader.py", "LEGEND_ENGINE_ROUTES");

        // M4: a print form shipped as Pure
        Map<String, String> pure = Map.of("planted.pure", Files.readString(
                SourceFiles.file("tools/meaning/planted/pure/planted.pure"), StandardCharsets.UTF_8));
        assertHit(MeaningCensus.printBodies(pure), "M4", "TOSTRING", "planted.pure", "meta::planted::Cell");

        // M5: a reproduction declared, in TypeScript, Python and Java
        List<Hit> comments = MeaningCensus.hostComments(hostProduct);
        assertHit(comments, "M5", "COMMENT", "relation-reader.ts", "QUIRK");
        assertHit(comments, "M5", "COMMENT", "_planted_reader.py", "ENGINE GAP");
        assertHit(MeaningCensus.javaComments(Map.of("Planted.java",
                "class P { // ENGINE QUIRK: the double-added anchor\n}")), "M5", "COMMENT", "Planted.java", "QUIRK");

        // M6: an export only the edge uses
        List<Hit> edgeOnlyHits = MeaningCensus.edgeOnly(hostProduct, hostEdge, p -> false);
        assertHit(edgeOnlyHits, "M6", "EXPORT", "relation-reader.ts", "tdsColumnsOf");
        assertHit(edgeOnlyHits, "M6", "EXPORT", "_planted_reader.py", "engine_columns");

        // M7: each host's product entry importing an edge module, statically
        Set<String> known = plants.keySet();
        Set<String> edgePaths = new HashSet<>();
        hostEdge.forEach(s -> edgePaths.add(s.path()));
        List<Hit> reach = MeaningCensus.hostReach(hostProduct, edgePaths, known, p -> false, List.of());
        assertHit(reach, "M7", "STATIC", "relation-reader.ts", "../edge/engine-routes.ts");
        assertHit(reach, "M7", "STATIC", "_planted_reader.py", ".edge");
        for (String entry : List.of("Server", "Tab", "Native", "QueriesPage")) {
            List<Hit> java = MeaningCensus.javaReach(planted, Set.of("com/legend/tools/meaning/planted/PlantedEntries$"
                    + entry), plantedEdge, "planted " + entry);
            assertHit(java, "M7", "STATIC", "planted.edge", "PlantedEdgeRoutes");
        }
        // and the build's reach: a planted shipped target depending on an edge target
        List<Hit> build = MeaningCensus.buildReach(Files.readAllLines(Runfile.property("meaning.shipped_reach_planted")),
                "planted");
        assertTrue(build.stream().anyMatch(h -> h.where().contains("planted_shipped_reach")), "the build reach bites: "
                + build);
        assertTrue(Files.readAllLines(Runfile.property("meaning.shipped_reach")).stream()
                .noneMatch(l -> l.contains("planted")), "the real query does not see the plant");
    }

    private static void assertHas(List<FormsLens.Finding> forms, String shape, String subject, String what) {
        assertTrue(forms.stream().anyMatch(f -> f.shape().equals(shape) && (f.subject().contains(subject)
                || f.where().contains(subject)) && f.what().contains(what)), "M3 " + shape + " " + subject
                + " (" + what + ") not found: the lens went blind. It found: " + forms);
    }

    private static void assertHit(List<Hit> hits, String lens, String category, String where, String term) {
        assertTrue(hits.stream().anyMatch(h -> h.lens().equals(lens) && h.category().equals(category)
                && h.where().contains(where) && h.term().contains(term)), lens + " " + category + " " + term + " in "
                + where + " not found: the lens went blind. It found: " + hits);
    }

    // -----------------------------------------------------------------------------------------------------------------
    // Inputs

    /** The edge's own strings: its classes' pool strings, and its TypeScript and Python modules' literals. */
    static Map<String, String> edgeStrings(Map<String, ClassFile> edge, List<Source> hostEdge) {
        Map<String, String> out = new TreeMap<>();
        for (ClassFile c : edge.values()) {
            // an edge class that is a SQL dialect holds SQL: the database's words, not upstream's
            if (isDialect(c, edge)) {
                continue;
            }
            for (String s : c.strings()) {
                for (String part : s.split("[\u0001\u0002]")) {
                    out.putIfAbsent(part, c.binaryName());
                }
            }
        }
        for (Source s : hostEdge) {
            for (HostSources.Token t : s.literals()) {
                out.putIfAbsent(t.text(), s.path());
            }
        }
        return out;
    }

    private static boolean isDialect(ClassFile c, Map<String, ClassFile> edge) {
        for (ClassFile k = c; k != null; k = k.superName() == null ? null : edge.get(k.superName())) {
            if (k.superName() != null && k.superName().startsWith("com/legend/sql/dialect/")) {
                return true;
            }
        }
        return false;
    }

    /** The hosts' edge sources: a genquery's labels (//pkg:path) as repository paths. */
    static Set<String> edgeSources(Path genquery) throws IOException {
        Set<String> out = new TreeSet<>();
        for (String l : Files.readAllLines(genquery, StandardCharsets.UTF_8)) {
            String t = l.strip();
            if (t.startsWith("//") && t.contains(":")) {
                out.add(t.substring(2, t.indexOf(':')) + "/" + t.substring(t.indexOf(':') + 1));
            }
        }
        return out;
    }

    /** Reads every TypeScript and Python file under the host rule's folders, product or edge. */
    static void readHosts(HostRule rule, Set<String> edgePaths, List<Source> product, List<Source> edge)
            throws IOException {
        List<String> dirs = new ArrayList<>(rule.typescript());
        dirs.addAll(rule.python());
        dirs.add("python/legend_lite_engine");
        for (String dir : dirs) {
            for (Path p : SourceFiles.under(dir)) {
                String path = SourceFiles.path(p);
                if (HostSources.Lang.of(path) == null) {
                    continue;
                }
                Source s = HostSources.read(path, Files.readString(p, StandardCharsets.UTF_8));
                (edgePaths.contains(path) ? edge : product).add(s);
            }
        }
    }

    /** The host-SQL rule's folders and harnesses (tools/guards/host_sql.txt): the hosts' product sources, and the
     *  scripts that are tests in all but their folder. */
    record HostRule(List<String> typescript, List<String> python, Set<String> harnesses) {

        static HostRule read(Path file) throws IOException {
            Map<String, List<String>> sections = new LinkedHashMap<>();
            List<String> current = null;
            for (String line : Files.readAllLines(file, StandardCharsets.UTF_8)) {
                if (line.isBlank() || line.startsWith("#")) {
                    continue;
                }
                if (line.startsWith("[") && line.endsWith("]")) {
                    current = sections.computeIfAbsent(line.substring(1, line.length() - 1), k -> new ArrayList<>());
                } else if (current != null) {
                    current.add(line.strip());
                }
            }
            Set<String> harnesses = new TreeSet<>();
            for (String h : sections.getOrDefault("harnesses", List.of())) {
                harnesses.add(h.split(" \\| ", 2)[0].strip());
            }
            return new HostRule(sections.get("typescript"), sections.get("python"), harnesses);
        }

        /** A test: under a test folder, a {@code .test.} file, or a harness the rule names. */
        Predicate<String> isTest() {
            return p -> p.contains("/test/") || p.contains("/tests/") || p.contains(".test.") || harnesses.contains(p);
        }
    }

    /** The committed classification: rows "lens | category | file glob | term glob | class | reason" (first match
     *  wins), and the known items "KNOWN | id | what | why the census may not see it". */
    record Inventory(List<Row> rows, List<Known> known) {

        record Row(String lens, String category, Pattern file, Pattern term, String cls, String reason) {
        }

        record Known(String id, String what, String unseen) {
        }

        static Inventory read(Path file) {
            List<Row> rows = new ArrayList<>();
            List<Known> known = new ArrayList<>();
            List<String> lines;
            try {
                lines = Files.readAllLines(file, StandardCharsets.UTF_8);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            for (String line : lines) {
                if (line.isBlank() || line.startsWith("#")) {
                    continue;
                }
                String[] f = line.split(" \\| ", -1);
                if (f[0].strip().equals("KNOWN")) {
                    known.add(new Known(f[1].strip(), f[2].strip(), f.length > 3 ? f[3].strip() : ""));
                    continue;
                }
                if (f.length < 6) {
                    throw new IllegalArgumentException("inventory.tsv: a row is lens | category | file | term | class |"
                            + " reason: " + line);
                }
                String cls = f[4].strip();
                if (!Set.of("a", "b", "c").contains(cls)) {
                    throw new IllegalArgumentException("inventory.tsv: a class is a, b or c: " + line);
                }
                rows.add(new Row(f[0].strip(), f[1].strip(), glob(f[2].strip()), glob(f[3].strip()), cls,
                        f[5].strip()));
            }
            return new Inventory(rows, known);
        }

        Row classify(Hit h) {
            for (Row r : rows) {
                if ((r.lens().equals("*") || r.lens().equals(h.lens()))
                        && (r.category().equals("*") || r.category().equals(h.category()))
                        && r.file().matcher(h.file()).matches() && r.term().matcher(h.term()).matches()) {
                    return r;
                }
            }
            return null;
        }

        static Pattern glob(String g) {
            StringBuilder b = new StringBuilder();
            for (char c : g.toCharArray()) {
                b.append(c == '*' ? ".*" : Pattern.quote(String.valueOf(c)));
            }
            return Pattern.compile(b.toString(), Pattern.DOTALL);
        }
    }
}
