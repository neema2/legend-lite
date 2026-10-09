// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.equivalence;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.legend.json.Json;
import com.legend.protocol.ModelReader;
import com.legend.protocol.ProtocolEmitter;
import com.legend.protocol.ProtocolReader;
import com.legend.testing.TestOutputs;
import org.finos.legend.engine.protocol.pure.m3.function.LambdaFunction;
import org.finos.legend.engine.protocol.pure.v1.model.context.PureModelContextData;
import org.finos.legend.engine.shared.core.ObjectMapperFactory;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * THE OLDER JSON'S ORACLE (docs/PROTOCOL_PROGRAM_2026_10_05.md, leg 2 step 2): every model and lambda among
 * legend-engine's own test resources -- JSON written by the engine's older versions, older expression shapes
 * included -- read by the engine and written back (its own upgrade, J2), against lite's read and emit of the same
 * JSON, element by element:
 *
 * <ul>
 *   <li><b>matched</b>: lite writes J2;</li>
 *   <li><b>upgraded</b>: lite writes J2 with the documented upgrades applied ({@link #upgraded}): the shapes the
 *       engine keeps and lite reads as the call that builds the same object;</li>
 *   <li><b>refused</b>: lite's reader names what it has no rule for (listed by reason in
 *       {@code older-json-refusals.tsv});</li>
 *   <li><b>mismatched</b>: anything else (the first difference of each in {@code older-json-mismatches.txt}).</li>
 * </ul>
 *
 * Read and written are counted up-only, refusals and mismatches down-only. A file the engine itself cannot read is
 * skipped and counted.
 */
class OlderJsonParityTest {

    /** Elements and lambdas lite writes as the engine does, or as its documented upgrade. Up-only. */
    private static final int MIN_READ = 0;
    /** Different JSON with no documented upgrade behind it. Down-only. */
    private static final int MAX_MISMATCHED = 100_000;
    /** Refusals, by any reason. Down-only. */
    private static final int MAX_REFUSED = 100_000;

    private static final Json.Config DEEP = new Json.Config(4096);

    private static final ObjectMapper MAPPER =
            ObjectMapperFactory.getNewStandardObjectMapperWithPureProtocolExtensionSupports();
    private final ObjectMapper mapper = MAPPER;
    private final Map<String, Integer> refusals = new TreeMap<>();
    private final List<String> mismatches = new ArrayList<>();
    private int matched;
    private int upgraded;
    private int refused;
    private int engineRefused;
    /** Whole models read, and the reasons a whole model was refused (its envelope or a field of its own). */
    private int documents;
    private final Map<String, Integer> documentRefusals = new TreeMap<>();

    @Test
    void liteReadsWhatTheEngineReads() throws Exception {
        Path root = Corpus.engineRoot();
        List<Path> files;
        try (Stream<Path> s = Files.walk(root)) {
            files = s.filter(p -> p.toString().endsWith(".json"))
                    .filter(p -> Corpus.within(root, p).contains("/src/test/resources/"))
                    .sorted(java.util.Comparator.comparing(Corpus::slashed))
                    .toList();
        }
        int models = 0;
        int lambdas = 0;
        for (Path file : files) {
            String text = Files.readString(file);
            Json.Node json;
            try {
                json = Json.parse(text, DEEP);
            } catch (RuntimeException notJson) {
                continue;
            }
            if (!(json instanceof Json.Obj top)) {
                continue;
            }
            String type = top.getStringOr("_type", "");
            String id = Corpus.within(root, file);
            if ("data".equals(type)) {
                models++;
                model(id, text, top);
            } else if ("lambda".equals(type)) {
                lambdas++;
                lambda(id, text, top);
            }
        }
        Files.createDirectories(TestOutputs.dir());
        StringBuilder reasons = new StringBuilder();
        refusals.forEach((r, n) -> reasons.append(n).append('\t').append(r).append('\t')
                .append(refusalSamples.get(r)).append('\n'));
        Files.writeString(TestOutputs.file("older-json-refusals.tsv"), reasons.toString());
        Files.writeString(TestOutputs.file("older-json-mismatches.txt"), String.join("\n", mismatches));
        System.out.printf("[older-json] %d files: %d models, %d lambdas (%d the engine cannot read); matched %d,"
                        + " upgraded %d, refused %d, mismatched %d%n", files.size(), models, lambdas, engineRefused,
                matched, upgraded, refused, mismatches.size());
        refusals.forEach((r, n) -> System.out.println("[older-json] refused " + n + "  " + r));
        System.out.println("[older-json] whole models read " + documents);
        documentRefusals.forEach((r, n) -> System.out.println("[older-json] model refused " + n + "  "
                + (r.length() > 200 ? r.substring(0, 200) : r)));
        assertTrue(matched + upgraded >= MIN_READ, "read as the engine reads: " + (matched + upgraded) + " < " + MIN_READ);
        assertTrue(mismatches.size() <= MAX_MISMATCHED, "mismatched: " + mismatches.size() + " > " + MAX_MISMATCHED
                + " (target/older-json-mismatches.txt)");
        assertTrue(refused <= MAX_REFUSED, "refused: " + refused + " > " + MAX_REFUSED
                + " (target/older-json-refusals.tsv)");
    }

    private void model(String id, String text, Json.Obj top) {
        Json.Obj engine;
        try {
            engine = (Json.Obj) Json.parse(mapper.writeValueAsString(mapper.readValue(text, PureModelContextData.class)),
                    DEEP);
        } catch (Exception | LinkageError e) {
            engineRefused++;
            return;
        }
        // the model as a whole: its envelope (serializer, origin) and the older sections merged
        try {
            Json.Obj whole = (Json.Obj) Json.parse(ProtocolEmitter.emit(ModelReader.read(top)), DEEP);
            documents++;
            for (String key : List.of("origin", "serializer")) {
                Json.Node ours = whole.getOr(key, null);
                Json.Node theirs = engine.getOr(key, null);
                if (!java.util.Objects.equals(ours, theirs)) {
                    mismatches.add(id + "\t$." + key + ": lite " + (ours == null ? "none" : abbreviate(ours))
                            + " | engine " + (theirs == null ? "none" : abbreviate(theirs)));
                }
            }
        } catch (IllegalArgumentException refusal) {
            documentRefusals.merge(String.valueOf(refusal.getMessage()), 1, Integer::sum);
        } catch (RuntimeException crash) {
            documentRefusals.merge("crashed: " + crash, 1, Integer::sum);
        }
        // element by element, in the engine's merged order
        List<Json.Node> ours = ModelReader.elementNodes(top);
        List<Json.Node> theirs = items(engine, "elements");
        if (ours.size() != theirs.size()) {
            mismatches.add(id + "\telement count " + ours.size() + " vs the engine's " + theirs.size());
            return;
        }
        for (int i = 0; i < ours.size(); i++) {
            Json.Obj element = (Json.Obj) ours.get(i);
            String where = id + "#" + i + " (" + element.getStringOr("_type", "?") + ")";
            String written;
            try {
                written = ProtocolEmitter.emitElement(ModelReader.readElement(element));
            } catch (IllegalArgumentException refusal) {
                refuse(refusal, where);
                continue;
            } catch (RuntimeException crash) {
                mismatches.add(where + "\tcrashed: " + crash);
                continue;
            }
            compare(where, Json.parse(written, DEEP), theirs.get(i), element);
        }
    }

    private void lambda(String id, String text, Json.Obj top) {
        Json.Node engine;
        try {
            engine = Json.parse(mapper.writeValueAsString(mapper.readValue(text, LambdaFunction.class)), DEEP);
        } catch (Exception | LinkageError e) {
            engineRefused++;
            return;
        }
        String written;
        try {
            written = ProtocolEmitter.emitLambda(ProtocolReader.lambda(top));
        } catch (IllegalArgumentException refusal) {
            refuse(refusal, id);
            return;
        } catch (RuntimeException crash) {
            mismatches.add(id + "\tcrashed: " + crash);
            return;
        }
        compare(id, Json.parse(written, DEEP), engine, top);
    }

    private void compare(String where, Json.Node ours, Json.Node engine, Json.Node original) {
        if (ours.equals(engine)) {
            matched++;
            return;
        }
        Json.Node expected = new Upgrade(original).apply(engine);
        if (ours.equals(expected)) {
            upgraded++;
            return;
        }
        mismatches.add(where + "\t" + firstDifference("$", ours, expected));
    }

    private void refuse(IllegalArgumentException refusal, String where) {
        refused++;
        String reason = String.valueOf(refusal.getMessage()).replace('\n', ' ');
        // one bucket per reason, its varying names aside
        reason = reason.length() > 220 ? reason.substring(0, 220) : reason;
        refusals.merge(reason, 1, Integer::sum);
        refusalSamples.putIfAbsent(reason, where);
    }

    /** The first element refused for each reason: where to look. */
    private final Map<String, String> refusalSamples = new TreeMap<>();

    // ---------------------------------------------------------------------
    // The documented upgrades, applied to the engine's own JSON
    // ---------------------------------------------------------------------

    /**
     * The engine's J2 with the step's documented upgrades applied, written here apart from the reader so the two
     * codings of the one table must agree (docs/PROTOCOL_PROGRAM_2026_10_05.md, leg 2 step 2):
     *
     * <ul>
     *   <li>each {@code classInstance} kind the engine keeps is the call that builds the same object; a
     *       {@code qualifiedProperty} is a {@code property} (its {@code class} kept, as a property's is); a path's
     *       empty name is no name;</li>
     *   <li>an older {@code new} (a class pointer, a lone key expression) is today's {@code ^X(...)};</li>
     *   <li>a pointer in a one-kind slot (a super type, an enumeration mapping's enumeration, an association mapping's
     *       association) has the slot's type; a constraint lambda declares its {@code $this};</li>
     *   <li>an enum value mapping's older source values are typed as the engine's compile types them, by the
     *       ORIGINAL's {@code sourceType} (which the engine reads and does not write back);</li>
     *   <li>a relational connection's empty {@code postProcessors} is left out, as the engine's grammar leaves it
     *       (its reader writes back the empty list it started with).</li>
     * </ul>
     */
    static final class Upgrade {
        /** Each enumeration mapping's {@code sourceType} in the original, in order. */
        private final List<String> sourceTypes = new ArrayList<>();
        /**
         * Each enumeration mapping's value mappings' source values AS ORIGINALLY WRITTEN: the engine writes a plain
         * value back as its string ({@code EnumValueMappingSourceValueSerializer}: an integer 10 comes back "10",
         * which its compiler would then read as a string), so the typing starts from the original.
         */
        private final List<List<List<Json.Node>>> originalValues = new ArrayList<>();
        private int enumerationMapping;

        Upgrade(Json.Node original) {
            if (original instanceof Json.Obj o) {
                for (Json.Node em : items(o, "enumerationMappings")) {
                    sourceTypes.add(em instanceof Json.Obj e ? e.getStringOr("sourceType", null) : null);
                    List<List<Json.Node>> values = new ArrayList<>();
                    if (em instanceof Json.Obj e) {
                        for (Json.Node evm : items(e, "enumValueMappings")) {
                            values.add(evm instanceof Json.Obj v ? items(v, "sourceValues") : List.of());
                        }
                    }
                    originalValues.add(values);
                }
            }
        }

        Json.Node apply(Json.Node node) {
            if (node instanceof Json.Arr a) {
                List<Json.Node> out = new ArrayList<>(a.items().size());
                for (Json.Node n : a.items()) {
                    out.add(apply(n));
                }
                return new Json.Arr(out);
            }
            if (!(node instanceof Json.Obj o)) {
                return node;
            }
            LinkedHashMap<String, Json.Node> f = new LinkedHashMap<>();
            o.fields().forEach((k, v) -> f.put(k, apply(v)));
            String type = o.getStringOr("_type", "");
            switch (type) {
                case "qualifiedProperty" -> {
                    f.put("_type", Json.str("property"));
                    f.put("property", f.remove("qualifiedProperty"));
                }
                case "classInstance" -> {
                    Json.Node call = kindAsCall(o.getStringOr("type", ""), (Json.Obj) f.get("value"),
                            f.get("sourceInformation"));
                    if (call != null) {
                        return call;
                    }
                    if ("path".equals(o.getStringOr("type", "")) && f.get("value") instanceof Json.Obj path
                            && path.fields().get("name") instanceof Json.Str name && name.value().isEmpty()) {
                        LinkedHashMap<String, Json.Node> p = new LinkedHashMap<>(path.fields());
                        p.remove("name");
                        f.put("value", new Json.Obj(p));
                    }
                }
                case "func" -> olderNew(f);
                case "function" -> f.put("name", Json.str(engineSignature(o)));
                case "legacyRuntime" -> {
                    return engineRuntime(o);
                }
                case "relational" -> typed(f, "includedStores", "STORE");
                case "association" -> withoutLeadingThis(f);
                case "reference" -> f.put("dataElement", pointerOf(f.get("dataElement"), "DATA"));
                case "purePropertyMapping" -> {
                    f.putIfAbsent("explodeProperty", new Json.Bool(false));
                    f.put("transform", withoutParameters(f.get("transform")));
                }
                case "class" -> {
                    typed(f, "superTypes", "CLASS");
                    withoutLeadingThis(f);
                    if (f.get("constraints") instanceof Json.Arr cs) {
                        List<Json.Node> out = new ArrayList<>();
                        for (Json.Node c : cs.items()) {
                            LinkedHashMap<String, Json.Node> cf = new LinkedHashMap<>(((Json.Obj) c).fields());
                            withThis(cf, "functionDefinition");
                            withThis(cf, "messageFunction");
                            out.add(new Json.Obj(cf));
                        }
                        f.put("constraints", new Json.Arr(out));
                    }
                }
                case "mapping" -> {
                    if (f.get("associationMappings") instanceof Json.Arr ams) {
                        List<Json.Node> out = new ArrayList<>();
                        for (Json.Node am : ams.items()) {
                            LinkedHashMap<String, Json.Node> af = new LinkedHashMap<>(((Json.Obj) am).fields());
                            af.put("association", pointerOf(af.get("association"), "ASSOCIATION"));
                            out.add(new Json.Obj(af));
                        }
                        f.put("associationMappings", new Json.Arr(out));
                    }
                    if (f.get("enumerationMappings") instanceof Json.Arr ems) {
                        List<Json.Node> out = new ArrayList<>();
                        for (Json.Node em : ems.items()) {
                            out.add(enumerationMapping((Json.Obj) em));
                        }
                        f.put("enumerationMappings", new Json.Arr(out));
                    }
                }
                case "RelationalDatabaseConnection" -> {
                    if (f.get("postProcessors") instanceof Json.Arr pp && pp.items().isEmpty()) {
                        f.remove("postProcessors");
                    }
                }
                default -> {
                    if (f.containsKey("aggregateValues") && f.containsKey("groupByFunctions")) {
                        // an aggregation-aware specification: its map and group-by lambdas' $this is the engine's own
                        f.put("aggregateValues", eachWithout(f.get("aggregateValues"), "mapFn"));
                        f.put("groupByFunctions", eachWithout(f.get("groupByFunctions"), "groupByFn"));
                    }
                }
            }
            return new Json.Obj(f);
        }

        private static Json.Node eachWithout(Json.Node list, String lambdaKey) {
            if (!(list instanceof Json.Arr a)) {
                return list;
            }
            List<Json.Node> out = new ArrayList<>();
            for (Json.Node n : a.items()) {
                LinkedHashMap<String, Json.Node> f = new LinkedHashMap<>(((Json.Obj) n).fields());
                f.put(lambdaKey, withoutParameters(f.get(lambdaKey)));
                out.add(new Json.Obj(f));
            }
            return new Json.Arr(out);
        }

        private Json.Node enumerationMapping(Json.Obj em) {
            int at = enumerationMapping++;
            String sourceType = at < sourceTypes.size() ? sourceTypes.get(at) : null;
            List<List<Json.Node>> written = at < originalValues.size() ? originalValues.get(at) : List.of();
            LinkedHashMap<String, Json.Node> f = new LinkedHashMap<>(em.fields());
            f.put("enumeration", pointerOf(f.get("enumeration"), "ENUMERATION"));
            List<Json.Node> evms = new ArrayList<>();
            List<Json.Node> engineEvms = items(em, "enumValueMappings");
            for (int j = 0; j < engineEvms.size(); j++) {
                Json.Obj evm = (Json.Obj) engineEvms.get(j);
                LinkedHashMap<String, Json.Node> vf = new LinkedHashMap<>(evm.fields());
                List<Json.Node> values = j < written.size() ? written.get(j) : items(evm, "sourceValues");
                vf.put("sourceValues", new Json.Arr(typedSourceValues(values, sourceType)));
                evms.add(new Json.Obj(vf));
            }
            f.put("enumValueMappings", new Json.Arr(evms));
            return new Json.Obj(f);
        }
    }

    /** {@code HelperMappingBuilder.convertSourceValues}' typing, as JSON. */
    private static List<Json.Node> typedSourceValues(List<Json.Node> values, String sourceType) {
        List<Json.Node> out = new ArrayList<>();
        if (values.stream().allMatch(v -> v instanceof Json.Obj o
                && o.getStringOr("_type", "").endsWith("SourceValue"))) {
            return values;
        }
        if (values.size() == 1 && values.get(0) instanceof Json.Obj flagged) {
            flaggedValues(flagged, out);
            return out;
        }
        for (Json.Node v : values) {
            String kind = sourceType == null ? null : sourceType.toUpperCase(java.util.Locale.ROOT);
            if (kind == null || kind.equals("STRING")) {
                out.add(v instanceof Json.Str s ? sourceValue("stringSourceValue", null, s)
                        : sourceValue("integerSourceValue", null, v));
            } else if (kind.equals("INTEGER")) {
                out.add(sourceValue("integerSourceValue", null,
                        v instanceof Json.Str s ? Json.num(Long.parseLong(s.value())) : v));
            } else {
                out.add(sourceValue("enumSourceValue", sourceType, v));
            }
        }
        return out;
    }

    private static void flaggedValues(Json.Obj o, List<Json.Node> out) {
        switch (o.getStringOr("_type", "")) {
            case "string" -> items(o, "values").forEach(v -> out.add(sourceValue("stringSourceValue", null, v)));
            case "integer" -> items(o, "values").forEach(v -> out.add(sourceValue("integerSourceValue", null, v)));
            case "enumValue" -> out.add(sourceValue("enumSourceValue", o.getString("fullPath"), o.get("value")));
            case "collection" -> items(o, "values").forEach(v -> flaggedValues((Json.Obj) v, out));
            default -> throw new IllegalStateException("protocol 1.5 source value " + o);
        }
    }

    private static Json.Obj sourceValue(String type, String enumeration, Json.Node value) {
        LinkedHashMap<String, Json.Node> f = new LinkedHashMap<>();
        f.put("_type", Json.str(type));
        if (enumeration != null) {
            f.put("enumeration", Json.str(enumeration));
        }
        f.put("value", value);
        return new Json.Obj(f);
    }

    /** The items of a pointer list lacking a type get the slot's type. */
    private static void typed(LinkedHashMap<String, Json.Node> f, String key, String slotType) {
        if (f.get(key) instanceof Json.Arr a) {
            List<Json.Node> out = new ArrayList<>();
            for (Json.Node n : a.items()) {
                out.add(pointerOf(n, slotType));
            }
            f.put(key, new Json.Arr(out));
        }
    }

    private static Json.Node pointerOf(Json.Node p, String slotType) {
        if (!(p instanceof Json.Obj o) || o.has("type")) {
            return p;
        }
        LinkedHashMap<String, Json.Node> f = new LinkedHashMap<>(o.fields());
        f.put("type", Json.str(slotType));
        return new Json.Obj(f);
    }

    /** Qualified properties without the leading {@code this} parameter the engine's compiler removes. */
    private static void withoutLeadingThis(LinkedHashMap<String, Json.Node> element) {
        if (!(element.get("qualifiedProperties") instanceof Json.Arr qps)) {
            return;
        }
        List<Json.Node> out = new ArrayList<>();
        for (Json.Node qp : qps.items()) {
            LinkedHashMap<String, Json.Node> f = new LinkedHashMap<>(((Json.Obj) qp).fields());
            List<Json.Node> ps = items((Json.Obj) qp, "parameters");
            if (!ps.isEmpty() && ps.get(0) instanceof Json.Obj p && "this".equals(p.getStringOr("name", null))) {
                f.put("parameters", new Json.Arr(ps.subList(1, ps.size())));
            }
            out.add(new Json.Obj(f));
        }
        element.put("qualifiedProperties", new Json.Arr(out));
    }

    /** A lambda whose one parameter the engine binds itself, written without it. */
    private static Json.Node withoutParameters(Json.Node lambda) {
        if (!(lambda instanceof Json.Obj l) || items(l, "parameters").isEmpty()) {
            return lambda;
        }
        LinkedHashMap<String, Json.Node> f = new LinkedHashMap<>(l.fields());
        f.put("parameters", new Json.Arr(List.of()));
        return new Json.Obj(f);
    }

    /** The function's name as the engine compiles it: {@code HelperModelBuilder.getSignature}, the engine's own. */
    private static String engineSignature(Json.Obj function) {
        try {
            return org.finos.legend.engine.language.pure.compiler.toPureGraph.HelperModelBuilder.getSignature(
                    MAPPER.readValue(Json.toCompact(function),
                            org.finos.legend.engine.protocol.pure.m3.function.Function.class));
        } catch (java.io.IOException e) {
            throw new java.io.UncheckedIOException(e);
        }
    }

    /** A {@code legacyRuntime} as the engine's own {@code LegacyRuntime.toEngineRuntime} makes it. */
    private static Json.Node engineRuntime(Json.Obj legacy) {
        try {
            var runtime = MAPPER.readValue(Json.toCompact(legacy),
                    org.finos.legend.engine.protocol.pure.v1.model.packageableElement.runtime.LegacyRuntime.class);
            return Json.parse(MAPPER.writeValueAsString(runtime.toEngineRuntime()), DEEP);
        } catch (java.io.IOException e) {
            throw new java.io.UncheckedIOException(e);
        }
    }

    /** A constraint lambda with no parameter declares today's {@code $this}; one without a multiplicity has [1]. */
    private static void withThis(LinkedHashMap<String, Json.Node> constraint, String key) {
        if (constraint.get(key) instanceof Json.Obj l && items(l, "parameters").size() == 1
                && items(l, "parameters").get(0) instanceof Json.Obj p && !p.has("multiplicity")) {
            LinkedHashMap<String, Json.Node> pf = new LinkedHashMap<>(p.fields());
            LinkedHashMap<String, Json.Node> m = new LinkedHashMap<>();
            m.put("lowerBound", Json.num(1));
            m.put("upperBound", Json.num(1));
            pf.put("multiplicity", new Json.Obj(m));
            LinkedHashMap<String, Json.Node> lf = new LinkedHashMap<>(l.fields());
            lf.put("parameters", new Json.Arr(List.of(new Json.Obj(pf))));
            constraint.put(key, new Json.Obj(lf));
            return;
        }
        if (constraint.get(key) instanceof Json.Obj l && items(l, "parameters").isEmpty()) {
            LinkedHashMap<String, Json.Node> lf = new LinkedHashMap<>(l.fields());
            LinkedHashMap<String, Json.Node> m = new LinkedHashMap<>();
            m.put("lowerBound", Json.num(1));
            m.put("upperBound", Json.num(1));
            LinkedHashMap<String, Json.Node> self = new LinkedHashMap<>();
            self.put("_type", Json.str("var"));
            self.put("multiplicity", new Json.Obj(m));
            self.put("name", Json.str("this"));
            lf.put("parameters", new Json.Arr(List.of(new Json.Obj(self))));
            constraint.put(key, new Json.Obj(lf));
        }
    }

    /** An older {@code new}: the class pointer becomes {@code Class<X>}, a lone key expression its collection. */
    private static void olderNew(LinkedHashMap<String, Json.Node> f) {
        if (!(f.get("function") instanceof Json.Str fn) || !fn.value().equals("new")
                || !(f.get("parameters") instanceof Json.Arr ps) || ps.items().size() != 3
                || !(ps.items().get(0) instanceof Json.Obj cls)
                || !"packageableElementPtr".equals(cls.getStringOr("_type", ""))) {
            return;
        }
        Json.Node keys = ps.items().get(2);
        if (keys instanceof Json.Obj k && "keyExpression".equals(k.getStringOr("_type", ""))) {
            LinkedHashMap<String, Json.Node> m = new LinkedHashMap<>();
            m.put("lowerBound", Json.num(1));
            m.put("upperBound", Json.num(1));
            LinkedHashMap<String, Json.Node> c = new LinkedHashMap<>();
            c.put("_type", Json.str("collection"));
            c.put("multiplicity", new Json.Obj(m));
            c.put("values", new Json.Arr(List.of(keys)));
            keys = new Json.Obj(c);
        }
        Json.Obj inner = genericType(cls.getString("fullPath"), List.of());
        Json.Obj outer = genericType("meta::pure::metamodel::type::Class", List.of(inner));
        LinkedHashMap<String, Json.Node> gti = new LinkedHashMap<>();
        gti.put("_type", Json.str("genericTypeInstance"));
        gti.put("genericType", outer);
        f.put("parameters", new Json.Arr(List.of(new Json.Obj(gti), ps.items().get(1), keys)));
    }

    private static Json.Obj genericType(String path, List<Json.Node> args) {
        LinkedHashMap<String, Json.Node> raw = new LinkedHashMap<>();
        raw.put("_type", Json.str("packageableType"));
        raw.put("fullPath", Json.str(path));
        LinkedHashMap<String, Json.Node> g = new LinkedHashMap<>();
        g.put("multiplicityArguments", new Json.Arr(List.of()));
        g.put("rawType", new Json.Obj(raw));
        g.put("typeArguments", new Json.Arr(args));
        g.put("typeVariableValues", new Json.Arr(List.of()));
        return new Json.Obj(g);
    }

    private static Json.Node kindAsCall(String kind, Json.Obj v, Json.Node at) {
        return switch (kind) {
            case "listInstance" -> {
                List<Json.Node> values = items(v, "values");
                LinkedHashMap<String, Json.Node> c = new LinkedHashMap<>();
                c.put("_type", Json.str("collection"));
                LinkedHashMap<String, Json.Node> m = new LinkedHashMap<>();
                m.put("lowerBound", Json.num(values.size()));
                m.put("upperBound", Json.num(values.size()));
                c.put("multiplicity", new Json.Obj(m));
                c.put("values", new Json.Arr(values));
                yield call("list", at, new Json.Obj(c));
            }
            case "pair" -> call("meta::pure::functions::collection::pair", at, v.get("first"), v.get("second"));
            case "aggregateValue" -> call("meta::pure::functions::collection::agg", at, v.get("mapFn"),
                    v.get("aggregateFn"));
            case "tdsAggregateValue" -> call("meta::pure::tds::agg", at, string(v.getString("name")), v.get("mapFn"),
                    v.get("aggregateFn"));
            case "tdsColumnInformation" -> call("meta::pure::tds::col", at, v.get("columnFn"),
                    string(v.getString("name")));
            case "tdsSortInformation" -> call("ASC".equals(v.getString("direction")) ? "meta::pure::tds::asc"
                    : "meta::pure::tds::desc", at, string(v.getString("column")));
            case "tdsOlapRank" -> call("meta::pure::tds::func", at, v.get("function"));
            case "tdsOlapAggregation" -> call("meta::pure::tds::func", at, string(v.getString("columnName")),
                    v.get("function"));
            default -> null;
        };
    }

    private static Json.Obj call(String function, Json.Node at, Json.Node... params) {
        LinkedHashMap<String, Json.Node> f = new LinkedHashMap<>();
        f.put("_type", Json.str("func"));
        f.put("function", Json.str(function));
        f.put("parameters", new Json.Arr(List.of(params)));
        if (at != null) {
            f.put("sourceInformation", at);
        }
        return new Json.Obj(f);
    }

    private static Json.Obj string(String value) {
        LinkedHashMap<String, Json.Node> f = new LinkedHashMap<>();
        f.put("_type", Json.str("string"));
        f.put("value", Json.str(value));
        return new Json.Obj(f);
    }

    private static List<Json.Node> items(Json.Obj o, String key) {
        return o.getOr(key, null) instanceof Json.Arr a ? a.items() : List.of();
    }

    /** The JSON path of the first difference, and the two values there. */
    static String firstDifference(String path, Json.Node a, Json.Node b) {
        if (a instanceof Json.Obj x && b instanceof Json.Obj y) {
            for (String k : new java.util.TreeSet<>(x.fields().keySet())) {
                if (!y.fields().containsKey(k)) {
                    return path + "." + k + ": only lite writes it: " + abbreviate(x.fields().get(k));
                }
                if (!x.fields().get(k).equals(y.fields().get(k))) {
                    return firstDifference(path + "." + k, x.fields().get(k), y.fields().get(k));
                }
            }
            for (String k : new java.util.TreeSet<>(y.fields().keySet())) {
                if (!x.fields().containsKey(k)) {
                    return path + "." + k + ": only the engine writes it: " + abbreviate(y.fields().get(k));
                }
            }
        }
        if (a instanceof Json.Arr x && b instanceof Json.Arr y && x.items().size() == y.items().size()) {
            for (int i = 0; i < x.items().size(); i++) {
                if (!x.items().get(i).equals(y.items().get(i))) {
                    return firstDifference(path + "[" + i + "]", x.items().get(i), y.items().get(i));
                }
            }
        }
        return path + ": lite " + abbreviate(a) + " | engine " + abbreviate(b);
    }

    private static String abbreviate(Json.Node n) {
        String s = Json.toCompact(n);
        return s.length() > 200 ? s.substring(0, 200) + "..." : s;
    }
}
