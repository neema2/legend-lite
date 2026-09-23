package com.legend.equivalence;


import com.legend.testing.Repo;
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

/**
 * GENERATES docs/protocol-roster.tsv — every Jackson subtype tag the pinned
 * engine's protocol jars declare (+ the extension registry), each (tag, class)
 * marked COVERED when a source in the engine corpus, the fixture snapshot or our
 * own test snippets reaches it ({@link ProtocolRosterCensusTest} holds it equal).
 * The jars are this program's classpath; in a bump the fixture snapshot is the new
 * harvest ({@code -Dlegend.engine.fixtures}, parser-equivalence's :gen_roster).
 *
 * <pre>
 *   RosterGenerator &lt;output&gt;
 * </pre>
 */
public final class RosterGenerator {

    private RosterGenerator() {}

    public static final String HEADER = "# PROTOCOL-TYPE ROSTER — every @JsonSubTypes tag the pinned engine's protocol jars declare"
                + " (+ the extension registry),\n# one row per (tag, class): unrelated protocols reuse a tag, and every class declaring it is listed.\n# COVERED when a source in the engine corpus, the fixtures or our own"
                + " test snippets reaches it (ProtocolRosterCensusTest).\n# THE LEDGER IS THIS FILE: a bump that adds"
                + " or removes a tag, or moves a tag between COVERED and UNCOVERED, is a reviewed diff."
                + " Regenerate: bazel run //:update_generated.\n";

    /** The roster: tag → every class declaring it, the tags seen, and the rows. */
    public record Roster(Map<String, Set<String>> tagToClass, Set<String> seen, String dump) {
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            throw new IllegalArgumentException("usage: RosterGenerator <output>");
        }
        Roster r = compute();
        java.nio.file.Files.writeString(Path.of(args[0]), HEADER + r.dump());
        System.out.println("@@ roster: " + r.tagToClass().size() + " tags");
    }

    public static Roster compute() throws Exception {
        // ---- roster: every @JsonSubTypes in protocol packages ----
        // A tag maps to EVERY class declaring it: unrelated protocols reuse
        // tags (sql and relational both declare "literal", protobuf3 and
        // haskell both "bool"). Keeping only the first class seen made the
        // ledger depend on classpath order — Maven's and Bazel's differ, and
        // the two builds pinned different classes for 8 tags (2026-09-22).
        Map<String, Set<String>> tagToClass = new TreeMap<>();
        for (java.nio.file.Path jarPath : com.legend.testing.Repo.listed("legend.engine.jars")) {
            String entry = jarPath.toString();
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
                                tagToClass.computeIfAbsent(t.name(), k -> new TreeSet<>())
                                        .add(t.value().getName());
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
                                p -> tagToClass.computeIfAbsent(p.getTwo(), k -> new TreeSet<>())
                                        .add(p.getOne().getName())))));
        System.out.println("@@ FULL roster: " + tagToClass.size() + " tags");

        // ---- coverage: engine corpus + OUR OWN corpus ----
        PureGrammarParser oracle = PureGrammarParser.newInstance();
        ObjectMapper mapper = ObjectMapperFactory
                .getNewStandardObjectMapperWithPureProtocolExtensionSupports();
        Pattern tag = Pattern.compile("\"_type\"\\s*:\\s*\"([^\"]+)\"");
        Set<String> seen = new TreeSet<>();
        List<Corpus.Source> universe = new ArrayList<>(Corpus.all());
        universe.addAll(Corpus.engineFixtures());
        for (String module : new String[]{"core", "spec", "pct"}) {
            universe.addAll(InlineSnippets.extract(Repo.path(module),
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
        int uncovered = 0;
        for (var e : tagToClass.entrySet()) {
            if (seen.contains(e.getKey())) {
                continue;
            }
            uncovered++;
            for (String cls : e.getValue()) {
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
        }
        System.out.println("@@ covered: " + (tagToClass.size() - uncovered)
                + "/" + tagToClass.size());
        buckets.forEach((b, tags) -> {
            System.out.println("@@ ==== " + b + " (" + tags.size() + ")");
            tags.forEach(t -> System.out.println("@@   " + t));
        });
        StringBuilder dump = new StringBuilder();
        tagToClass.forEach((t, classes) -> classes.forEach(c -> dump.append(t)
                .append('\t').append(c).append('\t')
                .append(seen.contains(t) ? "COVERED" : "UNCOVERED").append('\n')));
        return new Roster(tagToClass, seen, dump.toString());
    }
}
