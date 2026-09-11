package com.legend.equivalence;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.finos.legend.engine.language.pure.grammar.from.PureGrammarParser;
import org.finos.legend.engine.protocol.pure.v1.extension.PureProtocolExtensionLoader;
import org.finos.legend.engine.shared.core.ObjectMapperFactory;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** PROBE (complete protocol-type census): the FULL Jackson subtype roster
 *  from every protocol jar on the oracle classpath (all @JsonSubTypes,
 *  not a hand-picked walk) + the extension registry; coverage unioned
 *  over BOTH corpuses (engine checkouts AND our own test snippets);
 *  every uncovered tag printed with its full class for triage; the roster
 *  is COMMITTED as docs/protocol-roster.tsv and held equal (batch 6). */
class ProtocolRosterCensusTest {


    @Test
    void completeCensus() throws Exception {
        materializeRoster();
    }

    /** The census body, callable — PmcdReachabilityCensusTest depends
     * on the roster this writes; SELF-SUFFICIENCY (user ruling
     * 2026-08-21: a class that always skips inside a gate is roster
     * theater) means the reader materializes it itself instead of
     * skipping on ordering. */
    static void materializeRoster() throws Exception {
        // ---- roster: every @JsonSubTypes in protocol packages ----
        Map<String, String> tagToClass = new TreeMap<>();
        String cp = System.getProperty("java.class.path");
        for (String entry : cp.split(java.io.File.pathSeparator)) {
            if (!entry.endsWith(".jar") || !entry.contains("legend-engine")) {
                continue;
            }
            try (JarFile jar = new JarFile(entry)) {
                Enumeration<JarEntry> es = jar.entries();
                while (es.hasMoreElements()) {
                    String name = es.nextElement().getName();
                    if (!name.endsWith(".class")
                            || !name.startsWith("org/finos/legend/engine/protocol/")) {
                        continue;
                    }
                    String cls = name.substring(0, name.length() - 6)
                            .replace('/', '.');
                    try {
                        Class<?> c = Class.forName(cls, false,
                                ProtocolRosterCensusTest.class.getClassLoader());
                        JsonSubTypes st = c.getAnnotation(JsonSubTypes.class);
                        if (st != null) {
                            for (JsonSubTypes.Type t : st.value()) {
                                tagToClass.putIfAbsent(t.name(),
                                        t.value().getName());
                            }
                        }
                    } catch (Throwable ignored) {
                        // unloadable class — not a protocol roster entry
                    }
                }
            } catch (Throwable ignored) {
                // unreadable jar
            }
        }
        PureProtocolExtensionLoader.extensions().forEach(ext ->
                ext.getExtraProtocolSubTypeInfoCollectors().forEach(c ->
                        c.value().forEach(info -> info.getSubTypes().forEach(
                                p -> tagToClass.putIfAbsent(p.getTwo(),
                                        p.getOne().getName())))));
        System.out.println("@@ FULL roster: " + tagToClass.size() + " tags");

        // ---- coverage: engine corpus + OUR OWN corpus ----
        PureGrammarParser oracle = PureGrammarParser.newInstance();
        ObjectMapper mapper = ObjectMapperFactory
                .getNewStandardObjectMapperWithPureProtocolExtensionSupports();
        Pattern tag = Pattern.compile("\"_type\"\\s*:\\s*\"([^\"]+)\"");
        Set<String> seen = new TreeSet<>();
        List<Corpus.Source> universe = new ArrayList<>(Corpus.all());
        universe.addAll(Corpus.engineFixtures());
        Path repo = Path.of(System.getProperty("user.dir")).getParent();
        for (String module : new String[]{"core", "spec", "pct", "nlq"}) {
            universe.addAll(InlineSnippets.extract(repo.resolve(module),
                    "own-" + module, InlineSnippets.OWN_DECL));
        }
        int accepted = 0;
        for (Corpus.Source src : universe) {
            String json;
            try {
                json = mapper.writeValueAsString(
                        oracle.parseModel(src.text()));
                accepted++;
            } catch (Throwable t) {
                continue;
            }
            Matcher m = tag.matcher(json);
            while (m.find()) {
                seen.add(m.group(1));
            }
        }
        System.out.println("@@ accepted (both corpuses): " + accepted
                + "; tags seen: " + seen.size());

        // ---- classification + report ----
        Map<String, List<String>> buckets = new TreeMap<>();
        for (var e : tagToClass.entrySet()) {
            if (seen.contains(e.getKey())) {
                continue;
            }
            String cls = e.getValue();
            String bucket;
            if (cls.contains(".executionPlan.")
                    || cls.contains("ExecutionNode")) {
                bucket = "RUNTIME-plan";
            } else if (cls.contains("Artifact")
                    || cls.contains("DeploymentC")
                    || cls.contains("DeploymentD")) {
                bucket = "RUNTIME-deployment";
            } else if (cls.contains(".test.") && (cls.contains("Result")
                    || cls.contains("Status") || cls.contains("Debug"))) {
                bucket = "RUNTIME-test-result";
            } else if (cls.contains("deprecated")
                    || cls.contains(".application.")) {
                bucket = "LEGACY-wire";
            } else {
                bucket = "USER-TYPABLE?";
            }
            buckets.computeIfAbsent(bucket, k -> new ArrayList<>())
                    .add(e.getKey() + "\t" + cls);
        }
        int uncovered = buckets.values().stream().mapToInt(List::size).sum();
        System.out.println("@@ covered: " + (tagToClass.size() - uncovered)
                + "/" + tagToClass.size());
        buckets.forEach((b, tags) -> {
            System.out.println("@@ ==== " + b + " (" + tags.size() + ")");
            tags.forEach(t -> System.out.println("@@   " + t));
        });
        StringBuilder dump = new StringBuilder();
        tagToClass.forEach((t, c) -> dump.append(t).append('\t').append(c)
                .append('\t').append(seen.contains(t) ? "COVERED"
                        : "UNCOVERED").append('\n'));
        java.nio.file.Files.writeString(
                Path.of("target", "protocol-roster.txt"), dump.toString());
        // THE LEDGER (upstream boundary batch 6): the roster is COMMITTED
        // (docs/protocol-roster.tsv) and held equal — a new upstream tag
        // adds/removes protocol types as a REVIEWED diff, each row marked
        // COVERED (some source in either corpus reaches it) or UNCOVERED.
        // Regenerate with -Droster.generate=1.
        Path ledger = Path.of("..", "docs", "protocol-roster.tsv");
        String header = "# PROTOCOL-TYPE ROSTER — every @JsonSubTypes tag the pinned engine's protocol jars declare"
                + " (+ the extension registry),\n# COVERED when a source in the engine corpus, the fixtures or our own"
                + " test snippets reaches it (ProtocolRosterCensusTest).\n# THE LEDGER IS THIS FILE: a bump that adds"
                + " or removes a tag, or moves a tag between COVERED and UNCOVERED, is a reviewed diff."
                + " Regenerate: -Droster.generate=1.\n";
        if ("1".equals(System.getProperty("roster.generate"))) {
            java.nio.file.Files.writeString(ledger, header + dump);
            System.out.println("@@ roster ledger regenerated: " + tagToClass.size() + " tags");
        } else {
            String committed = java.nio.file.Files.exists(ledger)
                    ? java.nio.file.Files.readString(ledger).lines().filter(l -> !l.startsWith("#"))
                            .collect(java.util.stream.Collectors.joining("\n", "", "\n"))
                    : "";
            org.junit.jupiter.api.Assertions.assertEquals(committed, dump.toString(),
                    "the protocol-type roster moved (new/removed tags, or coverage changed) — review"
                    + " target/protocol-roster.txt against docs/protocol-roster.tsv; regenerate with -Droster.generate=1");
        }
        Set<String> unrostered = new TreeSet<>(seen);
        unrostered.removeAll(tagToClass.keySet());
        System.out.println("@@ seen-but-unrostered: " + unrostered.size()
                + " " + unrostered);
    }
}
