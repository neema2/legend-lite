package com.legend.equivalence.harvest;

import com.legend.testing.Repo;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.finos.legend.engine.language.pure.grammar.from.PureGrammarParser;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.TreeMap;

/** PROBE: pre-ingestion adjudication of the harvested engine fixtures —
 *  oracle verdict x lite verdict per fixture, byte parity where both
 *  accept. Diagnostic. */
class ZFixtureAdjudicationProbe {


    private static Throwable rootOf(Throwable t) {
        Throwable r = t;
        while (r.getCause() != null && r.getCause() != r) {
            r = r.getCause();
        }
        return r;
    }

    @Test
    void adjudicate() throws Exception {
        PureGrammarParser oracle = PureGrammarParser.newInstance();
        ObjectMapper mapper = org.finos.legend.engine.shared.core
                .ObjectMapperFactory
                .getNewStandardObjectMapperWithPureProtocolExtensionSupports();
        ObjectMapper json = new ObjectMapper();
        Map<String, Integer> verdicts = new TreeMap<>();
        Map<String, Integer> refuseMsgs = new TreeMap<>();
        int diffs = 0;
        java.util.List<String> diffSamples = new java.util.ArrayList<>();
        java.util.List<String> refuseSamples = new java.util.ArrayList<>();
        // fresh harvest dump if present, else the committed snapshot —
        // NOTE: honest verdicts need the PRODUCTION oracle (run on the
        // ordinary test classpath, never :harvest_lib — the tests-jars alter it)
        Path dump = Files.exists(Repo.out("engine-fixtures.jsonl"))
                ? Repo.out("engine-fixtures.jsonl")
                : Repo.module("src/test/resources/"
                        + "engine-grammar-fixtures.jsonl");
        for (String line : Files.readAllLines(dump)) {
            JsonNode n = json.readTree(line);
            String src = n.get("source").asText();
            String expectedJson;
            try {
                expectedJson = mapper.writeValueAsString(
                        oracle.parseModel(src));
            } catch (Throwable t) {
                boolean liteAccepts;
                try {
                    com.legend.equivalence.Surfaces.platform(src);
                    liteAccepts = true;
                } catch (Throwable lt) {
                    liteAccepts = false;
                }
                String cls = liteAccepts
                        ? com.legend.equivalence.CorpusSweepTest
                                .classify(rootOf(t), src)
                        : "both-refuse";
                verdicts.merge("oracle-refuses:" + (liteAccepts
                        ? (cls == null ? "LENIENT-UNCLASSIFIED" : cls)
                        : "both-refuse"), 1, Integer::sum);
                if (liteAccepts && cls == null) {
                    System.out.println("@@ UNCLASS "
                            + n.get("origin").asText() + " :: "
                            + String.valueOf(rootOf(t).getMessage())
                                    .replaceAll("\\s+", " "));
                }
                continue;
            }
            String actual;
            try {
                actual = com.legend.parser.PmcdParser.parseDocument(src);
            } catch (Throwable t) {
                verdicts.merge("PROTOCOL-REFUSES-accepted", 1, Integer::sum);
                Throwable r = rootOf(t);
                String m = String.valueOf(r.getMessage())
                        .replaceAll("\\[\\d+:\\d+\\]", "[N:N]")
                        .replaceAll("'[^']*'", "'X'");
                System.out.println("@@ PREF " + n.get("origin").asText()
                        .replaceAll(".*\\.", "") + " :: "
                        + (m.length() > 90 ? m.substring(0, 90) : m));
                try {
                    java.nio.file.Files.writeString(Repo.out("pref-" + Math.abs(src.hashCode())
                                    + ".pure"), src);
                } catch (Exception e) {
                    // best-effort dump
                }
                continue;
            }
            // model path SEPARATELY — its refusals are lite's COMPILE
            // stage, adjudicated against the engine's compile verdicts
            try {
                com.legend.equivalence.Surfaces.platform(src);
            } catch (Throwable t) {
                verdicts.merge("MODEL-PATH-refuses", 1, Integer::sum);
                // STILL byte-compare the protocol parse — the engine also
                // parse-accepts these and only its COMPILER refuses
                if (expectedJson.equals(actual)) {
                    verdicts.merge("MODEL-PATH-refuses:BYTES-MATCH", 1,
                            Integer::sum);
                } else {
                    int i = 0;
                    int n2 = Math.min(expectedJson.length(), actual.length());
                    while (i < n2 && expectedJson.charAt(i)
                            == actual.charAt(i)) {
                        i++;
                    }
                    System.out.println("@@ MPDIFF "
                            + n.get("origin").asText() + " @char" + i + " …"
                            + expectedJson.substring(Math.max(0, i - 40),
                                    Math.min(expectedJson.length(), i + 50))
                            + "… vs …" + actual.substring(Math.max(0, i - 40),
                                    Math.min(actual.length(), i + 50)) + "…");
                }
                Throwable r = t;
                while (r.getCause() != null && r.getCause() != r) {
                    r = r.getCause();
                }
                String m = String.valueOf(r.getMessage())
                        .replaceAll("\\[\\d+:\\d+\\]", "[N:N]")
                        .replaceAll("'[^']*'", "'…'");
                refuseMsgs.merge(m.length() > 70 ? m.substring(0, 70) : m,
                        1, Integer::sum);
                if (refuseSamples.size() < 30) {
                    refuseSamples.add(n.get("origin").asText() + " :: " + m);
                }
                if (n.get("origin").asText().contains("RelationFunction")
                        || m.contains("trailing tokens")) {
                    System.out.println("@@ SRC " + src.replace("\n", " ⏎ "));
                }
                continue;
            }
            if (expectedJson.equals(actual)) {
                verdicts.merge("MATCH", 1, Integer::sum);
            } else {
                diffs++;
                verdicts.merge("DIFF", 1, Integer::sum);
                Files.writeString(Repo.out("diff-" + diffs + "-src.pure"), src);
                Files.writeString(Repo.out("diff-" + diffs + "-expected.json"), expectedJson);
                Files.writeString(Repo.out("diff-" + diffs + "-actual.json"), actual);
                if (diffSamples.size() < 8) {
                    int i = 0;
                    int n2 = Math.min(expectedJson.length(), actual.length());
                    while (i < n2 && expectedJson.charAt(i)
                            == actual.charAt(i)) {
                        i++;
                    }
                    diffSamples.add(n.get("origin").asText() + " @char" + i
                            + " …" + expectedJson.substring(
                                    Math.max(0, i - 30),
                                    Math.min(expectedJson.length(), i + 40))
                            + "… vs …" + actual.substring(Math.max(0, i - 30),
                                    Math.min(actual.length(), i + 40)) + "…");
                }
            }
        }
        System.out.println("@@ verdicts: " + verdicts);
        refuseMsgs.entrySet().stream().sorted((a, b) ->
                b.getValue() - a.getValue()).limit(12).forEach(e ->
                System.out.println("@@ REFUSE [" + e.getValue() + "] "
                        + e.getKey()));
        refuseSamples.forEach(s -> System.out.println("@@ R " + s));
        diffSamples.forEach(s -> System.out.println("@@ D " + s));
    }
}
