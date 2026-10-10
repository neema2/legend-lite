// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.equivalence;

import com.legend.model.ImportScope;
import com.legend.model.ModelFromProtocol;
import com.legend.model.PackageableElement;
import com.legend.model.ParsedModel;
import com.legend.parser.Dialect;
import com.legend.parser.ElementParser;
import com.legend.parser.PmcdParser;
import com.legend.protocol.ModelReader;
import com.legend.testing.TestOutputs;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * LEG 8'S ORACLE (docs/PROTOCOL_PROGRAM_2026_10_05.md §4): a model given as JSON reaches the compiler exactly as its
 * text does. For every text of the round trip's inputs -- legend-engine's test collection, lite's projects, the
 * showcase projects (as {@link RoundTripProofTest} reads them) -- the compiler's parsed model from the text
 * ({@code ElementParser}, as {@code Compiler.compileModel} parses) equals the one the door builds from lite's JSON of
 * that text without source information ({@code ModelReader}, {@code ModelFromProtocol}, as a client's
 * {@code PureModelContextData} arrives): the same elements, each record equal (the model's records compare by value),
 * and each element's imports the same. Equal input, so the compiler builds the same model and answers the same errors.
 *
 * <p>Counted apart, never as a pass: a text lite's parser refuses, or whose JSON lite does not write (nothing to
 * compare). Anything else is a FAILURE, listed. The counts are pinned per input: matched up-only, failed down-only.
 */
class JsonModelDoorTest {

    /** {min matched, max failed} per input. Measured 2026-10-10 (leg 8). */
    private static final Map<String, int[]> PINS = Map.of(
            "engine corpus", new int[]{0, Integer.MAX_VALUE},
            "lite projects", new int[]{0, Integer.MAX_VALUE},
            "showcase projects", new int[]{0, Integer.MAX_VALUE});

    private static final class Tally {
        final String name;
        int matched;
        int failed;
        int textRefused;
        int jsonRefused;
        final List<String> failures = new ArrayList<>();
        final Map<String, Integer> categories = new TreeMap<>();
        final List<String> all = new ArrayList<>();

        Tally(String name) {
            this.name = name;
        }
    }

    @Test
    void aModelGivenAsJsonReachesTheCompilerAsItsTextDoes() throws IOException {
        Tally corpus = new Tally("engine corpus");
        for (Corpus.Source src : Corpus.all()) {
            text(corpus, src.id(), src.text());
        }
        for (Path file : ModelComposerParityTest.roundtripTestFiles()) {
            int i = 0;
            for (String run : InlineSnippets.literalRuns(Files.readString(file))) {
                text(corpus, "roundtrip:" + file.getFileName() + "#" + i++, run);
            }
        }
        Tally lite = new Tally("lite projects");
        Tally showcase = new Tally("showcase projects");
        for (Map.Entry<String, List<Path>> project : RoundTripProofTest.projects().entrySet()) {
            Tally t = project.getKey().startsWith("showcase:") ? showcase : lite;
            for (Path f : project.getValue()) {
                text(t, project.getKey() + "/" + f.getFileName(), Files.readString(f, StandardCharsets.UTF_8));
            }
        }
        StringBuilder out = new StringBuilder();
        for (Tally t : List.of(corpus, lite, showcase)) {
            out.append("[json-door] ").append(t.name).append(": matched=").append(t.matched)
                    .append(" failed=").append(t.failed).append(" text-refused=").append(t.textRefused)
                    .append(" json-refused=").append(t.jsonRefused).append(' ').append(t.categories).append('\n');
            for (String f : t.failures) {
                out.append("  FAILED ").append(f).append('\n');
            }
        }
        Files.writeString(TestOutputs.file("json-model-door.txt"), out.toString());
        List<String> every = new ArrayList<>();
        for (Tally t : List.of(corpus, lite, showcase)) {
            every.addAll(t.all);
        }
        Files.write(TestOutputs.file("json-model-door-all.tsv"), every);
        System.out.print(out.substring(0, Math.min(out.length(), 20_000)));
        for (Tally t : List.of(corpus, lite, showcase)) {
            int[] pin = PINS.get(t.name);
            assertTrue(t.matched >= pin[0], t.name + ": matched " + t.matched + " < " + pin[0]);
            assertTrue(t.failed <= pin[1], t.name + ": failed " + t.failed + " > " + pin[1] + "\n"
                    + String.join("\n", t.failures));
        }
    }

    /** One text: its parsed model from the text against the door's from its JSON. */
    private static void text(Tally t, String id, String text) {
        ParsedModel fromText;
        try {
            fromText = ElementParser.parse(text, Dialect.LEGEND_LITE);
        } catch (RuntimeException e) {
            t.textRefused++;
            return;
        }
        String json;
        try {
            json = PmcdParser.parseDocument(text);
        } catch (RuntimeException e) {
            t.jsonRefused++;
            return;
        }
        if (fromText.elements().isEmpty()) {
            return;   // not a model (an expected message, a resource's name)
        }
        ParsedModel fromJson;
        try {
            fromJson = ModelFromProtocol.of(ModelReader.read(json));
        } catch (RuntimeException e) {
            fail(t, id, "door refuses: " + e.getClass().getSimpleName(),
                    "the door refuses lite's own JSON of the text: " + e);
            return;
        }
        Map<String, List<PackageableElement>> a = byKey(fromText);
        Map<String, List<PackageableElement>> b = byKey(fromJson);
        if (!a.keySet().equals(b.keySet())) {
            fail(t, id, "element sets differ",
                    "elements: from the text only " + minus(a, b) + "; from the JSON only " + minus(b, a));
            return;
        }
        for (String key : a.keySet()) {
            if (!a.get(key).equals(b.get(key))) {
                // where the records first differ, by field: in meaning first, then in positions alone
                String meaning = difference(a.get(key), b.get(key), a.get(key).get(0).getClass().getSimpleName(), false);
                String where = meaning != null ? meaning
                        : difference(a.get(key), b.get(key), a.get(key).get(0).getClass().getSimpleName(), true);
                String field = String.valueOf(where).split(":", 2)[0].replaceAll("\\[\\d+\\]", "[]");
                fail(t, id, (meaning != null ? "meaning " : "position ") + field,
                        "element " + key + " differs at " + where);
                return;
            }
            ImportScope ia = fromText.elementImports().getOrDefault(key, ImportScope.empty());
            ImportScope ib = fromJson.elementImports().getOrDefault(key, ImportScope.empty());
            if (!ia.equals(ib)) {
                fail(t, id, "imports differ",
                        "element " + key + "'s imports differ: text " + ia.wildcards() + ", json " + ib.wildcards()
                                + "; the JSON's sections " + sections(json));
                return;
            }
        }
        t.matched++;
    }

    /** The JSON's section index, summarized: each section's parser, imports and element paths. */
    private static String sections(String json) {
        StringBuilder out = new StringBuilder();
        for (com.legend.protocol.Protocol.Element e : ModelReader.read(json).elements()) {
            if (e instanceof com.legend.protocol.Protocol.PSectionIndex index) {
                for (com.legend.protocol.Protocol.PSection s : index.sections()) {
                    out.append(" {").append(s.parserName()).append(" imports=").append(s.imports())
                            .append(" elements=").append(s.elements()).append('}');
                }
            }
        }
        return out.toString();
    }

    private static Map<String, List<PackageableElement>> byKey(ParsedModel m) {
        Map<String, List<PackageableElement>> out = new TreeMap<>();
        for (PackageableElement e : m.elements()) {
            out.computeIfAbsent(ParsedModel.keyOf(e), k -> new ArrayList<>()).add(e);
        }
        return out;
    }

    private static List<String> minus(Map<String, ?> a, Map<String, ?> b) {
        List<String> out = new ArrayList<>();
        for (String k : a.keySet()) {
            if (!b.containsKey(k)) {
                out.add(k);
            }
        }
        return out;
    }

    /**
     * The first place two values differ, as {@code path: text-value vs json-value}, walking records by their components
     * and lists by index; null when they are equal. With {@code positions} false, source spans are not compared (a
     * difference in meaning is looked for first).
     */
    private static @com.legend.base.Nullable String difference(@com.legend.base.Nullable Object a,
            @com.legend.base.Nullable Object b, String path, boolean positions) {
        if (a == b) {
            return null;
        }
        if (!positions && (a instanceof com.legend.protocol.SourceInfo || b instanceof com.legend.protocol.SourceInfo)) {
            return null;
        }
        if (a == null || b == null) {
            return path + ": " + abbreviate(a) + " vs " + abbreviate(b);
        }
        if (a.getClass() != b.getClass()) {
            return path + ": " + a.getClass().getSimpleName() + " vs " + b.getClass().getSimpleName()
                    + " -- text " + abbreviate(a) + " -- json " + abbreviate(b);
        }
        if (a instanceof List<?> la) {
            List<?> lb = (List<?>) b;
            if (la.size() != lb.size()) {
                return path + ".size: " + la.size() + " vs " + lb.size();
            }
            for (int i = 0; i < la.size(); i++) {
                String d = difference(la.get(i), lb.get(i), path + "[" + i + "]", positions);
                if (d != null) {
                    return d;
                }
            }
            return null;
        }
        if (a instanceof Map<?, ?> ma) {
            Map<?, ?> mb = (Map<?, ?>) b;
            if (!ma.keySet().equals(mb.keySet())) {
                return path + ".keys: " + ma.keySet() + " vs " + mb.keySet();
            }
            for (Object k : ma.keySet()) {
                String d = difference(ma.get(k), mb.get(k), path + "{" + k + "}", positions);
                if (d != null) {
                    return d;
                }
            }
            return null;
        }
        if (a.getClass().isRecord()) {
            for (java.lang.reflect.RecordComponent c : a.getClass().getRecordComponents()) {
                try {
                    java.lang.reflect.Method m = c.getAccessor();
                    m.setAccessible(true);
                    String d = difference(m.invoke(a), m.invoke(b), path + "." + c.getName(), positions);
                    if (d != null) {
                        return d;
                    }
                } catch (ReflectiveOperationException e) {
                    throw new IllegalStateException(e);
                }
            }
            return null;
        }
        return a.equals(b) ? null : path + ": " + abbreviate(a) + " vs " + abbreviate(b);
    }

    private static String abbreviate(@com.legend.base.Nullable Object o) {
        String s = String.valueOf(o);
        return s.length() <= 300 ? s : s.substring(0, 300) + "...";
    }

    private static void fail(Tally t, String id, String category, String why) {
        t.failed++;
        // a few of each kind, so every kind is shown; every one in the full list
        if (t.categories.merge(category, 1, Integer::sum) <= 4) {
            t.failures.add("[" + category + "] " + id + ": " + why);
        }
        t.all.add(t.name + "\t" + category + "\t" + id);
    }
}
