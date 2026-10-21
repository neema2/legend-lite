// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.equivalence;

import com.legend.model.ImportScope;
import com.legend.model.ModelFromProtocol;
import com.legend.model.PackageableElement;
import com.legend.model.ParsedModel;
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * LEG 8'S REFEREE (docs/PROTOCOL_PROGRAM_2026_10_05.md §4): a model given as JSON reaches the compiler exactly as its
 * text does. With one parser (step 1), the text and the JSON differ only by the JSON hop, so for every text of the
 * round trip's inputs -- legend-engine's test collection, lite's projects, the showcase projects (as
 * {@link RoundTripProofTest} reads them) -- two things are checked:
 * <ol>
 *   <li><b>the hop</b>: the text's records (the engine's dialect, what a client's JSON carries), written as JSON and
 *       read back ({@code ModelReader}), are the same records, field by field, positions included;</li>
 *   <li><b>lite's dialect</b>: the compiler's parsed model from the text at lite's dialect ({@code Compiler.parseModel},
 *       as {@code Compiler.compileModel} parses) equals the door's over the engine's records ({@code ModelFromProtocol}):
 *       the same elements, each equal, each element's imports the same.</li>
 * </ol>
 * Together: equal input to the compiler, so the same model and the same errors.
 *
 * <p>Where the two dialects read a text differently, they may only do so by the user's rule A
 * (docs/SEMANTICS_REGISTER.md S89, 2026-10-21): text the engine's grammar accepts and its parser drops, which lite's
 * dialect keeps. Such a text's records at lite's dialect, written as JSON, are then either refused by name (the
 * construct the engine's JSON cannot carry as written: counted per construct, pinned) or written whole and read back to
 * the same records and the same compiler input (lite's own JSON round trip: counted, pinned). Anything else is a
 * FAILURE.
 *
 * <p>Counted apart, never as a pass: a text lite's parser refuses, or whose JSON lite does not write (nothing to
 * compare). Anything else is a FAILURE, listed by kind. The counts are pinned per input: matched up-only, failed
 * down-only, and the rule-A counts exactly.
 */
class JsonModelDoorTest {

    /** {min matched, max failed} per input: matched up-only, failed down-only. Measured 2026-10-11 (leg 8, step 2's
     *  first part landed: the engine collection's failures are step 2's remaining work list; 401 -> 363 with a
     *  relation column's [0..1]; 363 -> 267 with a relation shape read as the parser reads it; 267 -> 149 with
     *  persistence spans as the JSON has them, a graphFetch partition's key read back, a let's TDS value's position
     *  out of equality, a reference body read as the parser reads it, and two engine-dialect refusals). Measured
     *  2026-10-21 (rule A, S89; rebased on main, lite's projects 167): 149 -> 4 -- the dropped text read at the engine's
     *  dialect as the engine reads it (rule A's counts below), a model store's -1 one literal, a service store's
     *  description, a path's prop() its property. The 4 left: two quoted trees over a let-bound string
     *  (compileLegendValueSpecification($treeString)), which the parser folds and a JSON model cannot carry (the fold
     *  belongs after the door, in name resolution: docs/PROTOCOL_PROGRAM_2026_10_05.md leg 8's as-built note), and
     *  two variables typed meta::pure::mapping::Result, which legend-engine's own reader brings up to
     *  Result<Any|1..*> (ProtocolUpgrade; its print measured in PureV1ApiTest), so its own JSON hop changes them.
     *  7099 -> 7085 (2026-10-21, the reference checks): a function's constraints are the compiler's now, and the
     *  engine's parser drops them (rule A), so the 14 texts holding them (AbstractTestConstraints 5, TestNestedCollection
     *  2, TestPureRuntimeFunction_Constraint 7, by name in the GATES entry) are rule A's, written whole, below. */
    private static final Map<String, int[]> PINS = Map.of(
            "engine corpus", new int[]{7085, 4},
            "lite projects", new int[]{167, 0},
            "showcase projects", new int[]{123, 0});

    /** Rule A's counts, exact (they move only with a corpus or a rule): the texts lite's dialect keeps and writes whole,
     *  and those whose JSON it refuses, by construct. Measured 2026-10-21; 102 -> 116 the same day (the 14 texts with
     *  a function's constraints, from matched above). */
    private static final Map<String, Integer> RULE_A_WRITTEN = Map.of("engine corpus", 116);
    private static final Map<String, Map<String, Integer>> RULE_A_REFUSED = Map.of("engine corpus", Map.of(
            "a dotted key (the engine keys addr.city = .. by addr)", 1,
            "a supertype's type arguments", 1,
            "an include's several store substitutions", 8));

    private static final class Tally {
        final String name;
        int matched;
        int failed;
        int textRefused;
        int jsonRefused;
        /** rule A: lite's dialect keeps what the engine's drops, and lite's JSON refuses it by name, per construct */
        final Map<String, Integer> engineDrops = new TreeMap<>();
        /** rule A: lite's dialect keeps what the engine's drops, and lite's JSON carries it whole, exactly */
        int liteWritten;
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
                    .append(" json-refused=").append(t.jsonRefused).append(" rule-A-written=").append(t.liteWritten)
                    .append(" rule-A-refused=").append(t.engineDrops).append(' ').append(t.categories).append('\n');
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
            assertEquals(RULE_A_WRITTEN.getOrDefault(t.name, 0), t.liteWritten, t.name + ": rule A's texts written whole");
            assertEquals(RULE_A_REFUSED.getOrDefault(t.name, Map.of()), t.engineDrops,
                    t.name + ": rule A's texts refused, by construct");
        }
    }

    /** One text: its parsed model from the text against the door's from its JSON. */
    private static void text(Tally t, String id, String text) {
        // the records a client's JSON carries: the text at the engine's dialect
        com.legend.protocol.Protocol.PureModelContextData records;
        try {
            records = PmcdParser.parseModel(text);
        } catch (RuntimeException e) {
            t.jsonRefused++;
            return;
        }
        if (records.elements().stream().allMatch(e -> e instanceof com.legend.protocol.Protocol.PSectionIndex)) {
            return;   // not a model (an expected message, a resource's name)
        }
        // 1. THE JSON HOP: the records written as JSON and read back are the same records, field by field
        com.legend.protocol.Protocol.PureModelContextData read;
        try {
            read = ModelReader.read(com.legend.protocol.ProtocolEmitter.emit(records));
        } catch (RuntimeException e) {
            fail(t, id, "hop: unread " + e.getClass().getSimpleName(), "lite's reader refuses its own JSON: " + e);
            return;
        }
        if (!read.elements().equals(records.elements())) {
            String meaning = difference(records.elements(), read.elements(), "records", false);
            String where = meaning != null ? meaning : difference(records.elements(), read.elements(), "records", true);
            fail(t, id, "hop " + (meaning != null ? "meaning " : "position ") + field(where), "the records differ at "
                    + where);
            return;
        }
        // 2. LITE'S DIALECT: the compiler's input from the text (lite's dialect) is the door's over the engine's records
        ParsedModel fromText;
        try {
            fromText = com.legend.Compiler.parseModel(text);   // the product's text door (lite's dialect)
        } catch (RuntimeException e) {
            t.textRefused++;
            return;
        }
        String differs = modelDifference(fromText, ModelFromProtocol.of(records));
        if (differs == null) {
            t.matched++;
            return;
        }
        // 3. RULE A: the dialects read the text differently -- only by what the engine's parser drops and lite's dialect
        // keeps. Lite's records, written as JSON, are refused by name, or carry it whole and read back exactly.
        com.legend.protocol.Protocol.PureModelContextData liteRecords =
                PmcdParser.parse(text, com.legend.parser.Dialect.LEGEND_LITE).model();
        String liteJson;
        try {
            liteJson = com.legend.protocol.ProtocolEmitter.emit(liteRecords);
        } catch (UnsupportedOperationException e) {
            String message = String.valueOf(e.getMessage());
            int at = message.indexOf(" is not written as legend-engine's JSON");
            if (at < 0) {
                fail(t, id, "dialect: lite's JSON unwritten", message + " -- where " + differs);
                return;
            }
            t.engineDrops.merge(message.substring(0, at), 1, Integer::sum);
            return;
        }
        com.legend.protocol.Protocol.PureModelContextData liteRead;
        try {
            liteRead = ModelReader.read(liteJson);
        } catch (RuntimeException e) {
            fail(t, id, "lite hop: unread " + e.getClass().getSimpleName(), "lite's reader refuses its own JSON: " + e);
            return;
        }
        if (!liteRead.elements().equals(liteRecords.elements())) {
            String meaning = difference(liteRecords.elements(), liteRead.elements(), "records", false);
            String where = meaning != null ? meaning
                    : difference(liteRecords.elements(), liteRead.elements(), "records", true);
            fail(t, id, "lite hop " + (meaning != null ? "meaning " : "position ") + field(where),
                    "lite's records differ at " + where + " (the dialects first differed at " + differs + ")");
            return;
        }
        String liteDiffers = modelDifference(fromText, ModelFromProtocol.of(liteRead));
        if (liteDiffers != null) {
            fail(t, id, "dialect " + field(liteDiffers), liteDiffers + " -- against the engine's records: " + differs);
            return;
        }
        t.liteWritten++;
    }

    /** Where two parsed models first differ -- an element set, an element, or an element's imports -- or null. */
    private static @com.legend.base.Nullable String modelDifference(ParsedModel fromText, ParsedModel fromRecords) {
        Map<String, List<PackageableElement>> a = byKey(fromText);
        Map<String, List<PackageableElement>> b = byKey(fromRecords);
        if (!a.keySet().equals(b.keySet())) {
            return "element sets: lite's only " + minus(a, b) + "; the records' only " + minus(b, a);
        }
        for (String key : a.keySet()) {
            if (!a.get(key).equals(b.get(key))) {
                // "meaning <path>: <values> (<element>)": the kind and the field first, for counting kinds together
                String meaning = difference(a.get(key), b.get(key), a.get(key).get(0).getClass().getSimpleName(), false);
                return (meaning != null ? "meaning " + meaning
                        : "position " + difference(a.get(key), b.get(key), a.get(key).get(0).getClass().getSimpleName(),
                                true)) + " (" + key + ")";
            }
            ImportScope ia = fromText.elementImports().getOrDefault(key, ImportScope.empty());
            ImportScope ib = fromRecords.elementImports().getOrDefault(key, ImportScope.empty());
            if (!ia.equals(ib)) {
                return "imports: lite " + ia.wildcards() + ", the records " + ib.wildcards() + " (" + key + ")";
            }
        }
        return null;
    }

    /** A difference's place, its indices dropped: the field it is in, for counting kinds together. */
    private static String field(@com.legend.base.Nullable String where) {
        return String.valueOf(where).split(":", 2)[0].replaceAll("\\[\\d+\\]", "[]");
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
        if (a.equals(b)) {
            return null;   // equal values (two List implementations with the same contents among them)
        }
        if (a instanceof List<?> && b instanceof List<?>) {
            // lists by their contents, whichever implementation holds them
        } else if (a.getClass() != b.getClass()) {
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
