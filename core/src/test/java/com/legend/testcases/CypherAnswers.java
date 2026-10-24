// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testcases;

import com.legend.json.Json;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Neo4j's snapped answers ({@code core/src/test/resources/cypher-parity/neo4j-answers.jsonl}, written by
 * {@code tools/neo4j-oracle/snap.py}; docs/CYPHER_STUDY_2026_10_15.md §3.3, §8): one line of metadata (the server
 * version the answers were snapped on), then one line per case: the Cypher version it ran under, the sha of what
 * the answer is for, the graph the case set up (what the lanes seed from), and its rows in the TCK's value grammar
 * or Neo4j's error; for a write, the graph it left and its control query's answer. The guards: a case with no
 * answer is UNSNAPPED, one whose sha moved is STALE, an answer with no case is GONE, and answers from another server
 * version than the pinned one are a VERSION problem.
 */
public final class CypherAnswers {

    private CypherAnswers() {
    }

    public record Meta(String server, String edition, String defaultCypher) {
    }

    public record Error(String code, String message) {
    }

    /** A node of a dumped graph: its ordinal, its labels (sorted), its properties in the value grammar. */
    public record GraphNode(int id, List<String> labels, Map<String, String> props) {
    }

    /** A relationship of a dumped graph: its ordinal, its two nodes' ordinals, its type, its properties. */
    public record GraphRel(int id, int start, int end, String type, Map<String, String> props) {
    }

    public record Graph(List<GraphNode> nodes, List<GraphRel> rels) {
    }

    /**
     * One case's answer: its rows ({@code columns} and {@code rows}, each cell in the value grammar), or Neo4j's
     * {@code error}; a {@code setupError} when Neo4j refused the case's setup (then nothing else ran). For a query
     * Neo4j's summary marks as writing, {@code after}: the graph it left, each element {@code graph} held keeping its
     * ordinal there (the side effects are the two graphs' diff, {@link CypherParity#sideEffects}); and for a case
     * with a control query, {@code control}: Neo4j's answer to it, over that graph (the writes design's §6).
     */
    public record Answer(String id, String cypher, String sha, @com.legend.base.Nullable List<String> columns,
            @com.legend.base.Nullable List<List<String>> rows, @com.legend.base.Nullable Error error,
            @com.legend.base.Nullable Error setupError, @com.legend.base.Nullable Graph graph,
            @com.legend.base.Nullable Graph after, @com.legend.base.Nullable Control control) {
    }

    /** Neo4j's answer to a control query: its rows, or its error. */
    public record Control(@com.legend.base.Nullable List<String> columns,
            @com.legend.base.Nullable List<List<String>> rows, @com.legend.base.Nullable Error error) {
    }

    public record File(@com.legend.base.Nullable Meta meta, Map<String, Answer> answers) {
    }

    public static File read(Path file) throws IOException {
        Meta meta = null;
        Map<String, Answer> answers = new LinkedHashMap<>();
        for (String line : Files.readAllLines(file, StandardCharsets.UTF_8)) {
            if (line.isBlank()) {
                continue;
            }
            Json.Obj o = (Json.Obj) Json.parse(line);
            Json.Obj m = o.getObjOr("meta", null);
            if (m != null) {
                meta = new Meta(string(m, "server"), string(m, "edition"), string(m, "defaultCypher"));
                continue;
            }
            Answer a = new Answer(string(o, "id"), string(o, "cypher"), string(o, "sha"), strings(o.getArrOr("columns",
                    null)), rows(o.getArrOr("rows", null)), error(o.getObjOr("error", null)),
                    error(o.getObjOr("setupError", null)), graph(o.getObjOr("graph", null)),
                    graph(o.getObjOr("after", null)), control(o.getObjOr("control", null)));
            answers.put(a.id(), a);
        }
        return new File(meta, answers);
    }

    private static String string(Json.Obj o, String key) {
        String s = o.getStringOr(key, null);
        if (s == null) {
            throw new IllegalStateException("the answers file: no \"" + key + "\" in " + o.fields().keySet());
        }
        return s;
    }

    private static @com.legend.base.Nullable List<String> strings(Json.@com.legend.base.Nullable Arr a) {
        if (a == null) {
            return null;
        }
        List<String> out = new ArrayList<>();
        for (Json.Node n : a.items()) {
            out.add(((Json.Str) n).value());
        }
        return out;
    }

    private static @com.legend.base.Nullable List<List<String>> rows(Json.@com.legend.base.Nullable Arr a) {
        if (a == null) {
            return null;
        }
        List<List<String>> out = new ArrayList<>();
        for (Json.Node r : a.items()) {
            out.add(java.util.Objects.requireNonNull(strings((Json.Arr) r)));
        }
        return out;
    }

    private static @com.legend.base.Nullable Error error(Json.@com.legend.base.Nullable Obj o) {
        return o == null ? null : new Error(string(o, "code"), string(o, "message"));
    }

    private static @com.legend.base.Nullable Control control(Json.@com.legend.base.Nullable Obj o) {
        return o == null ? null : new Control(strings(o.getArrOr("columns", null)), rows(o.getArrOr("rows", null)),
                error(o.getObjOr("error", null)));
    }

    private static @com.legend.base.Nullable Graph graph(Json.@com.legend.base.Nullable Obj o) {
        if (o == null) {
            return null;
        }
        List<GraphNode> nodes = new ArrayList<>();
        for (Json.Node n : java.util.Objects.requireNonNull(o.getArrOr("nodes", null)).items()) {
            Json.Obj node = (Json.Obj) n;
            nodes.add(new GraphNode((int) ((Json.Num) node.get("id")).longValue(),
                    java.util.Objects.requireNonNull(strings(node.getArrOr("labels", null))),
                    props(node.getObjOr("props", null))));
        }
        List<GraphRel> rels = new ArrayList<>();
        for (Json.Node n : java.util.Objects.requireNonNull(o.getArrOr("rels", null)).items()) {
            Json.Obj rel = (Json.Obj) n;
            rels.add(new GraphRel((int) ((Json.Num) rel.get("id")).longValue(),
                    (int) ((Json.Num) rel.get("start")).longValue(), (int) ((Json.Num) rel.get("end")).longValue(),
                    string(rel, "type"), props(rel.getObjOr("props", null))));
        }
        return new Graph(nodes, rels);
    }

    private static Map<String, String> props(Json.@com.legend.base.Nullable Obj o) {
        Map<String, String> out = new TreeMap<>();
        if (o != null) {
            o.fields().forEach((k, v) -> out.put(k, ((Json.Str) v).value()));
        }
        return out;
    }

    // ------------------------------------------------------------------ the sha

    /** What an answer is for, hashed as the oracle hashes it ({@code snap.py}, {@code sha}): the query, its Cypher
     *  version, the setup statements, the parameters as written, a lite case's decision and expect, the procedures
     *  a case declares, and its control query. */
    public static String sha(String query, String version, List<String> setup, String paramsText,
            @com.legend.base.Nullable String decision, @com.legend.base.Nullable String expectText,
            String proceduresText, @com.legend.base.Nullable String control) {
        String text = String.join("\n\0\n", query, version, String.join("\n;\n", setup), paramsText,
                decision == null ? "" : decision, expectText == null ? "" : expectText);
        if (!proceduresText.isEmpty()) {
            text += "\n\0\n" + proceduresText;   // (a case declaring procedures, the study's §12.60)
        }
        if (control != null) {
            text += "\n\0\ncontrol:\n" + control;   // (a write case's control query, the writes design's §6)
        }
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(text.getBytes(
                    StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    // ------------------------------------------------------------------ the guards

    /** The problems between the cases and the answers: UNSNAPPED, STALE, GONE, and VERSION when the answers were
     *  snapped on another server than {@code pinnedServer}. A case the harness skips needs no answer. */
    public static List<String> guards(List<CypherParity.Case> cases, File file, String pinnedServer) {
        List<String> problems = new ArrayList<>();
        Meta meta = file.meta();
        if (meta == null) {
            problems.add("VERSION: the answers file has no meta line");
        } else if (!meta.server().equals(pinnedServer)) {
            problems.add("VERSION: the answers were snapped on Neo4j " + meta.server() + ", the pinned server is "
                    + pinnedServer);
        }
        java.util.Set<String> keys = new java.util.HashSet<>();
        for (CypherParity.Case c : cases) {
            keys.add(c.key());
            if (c.skip() != null) {
                continue;
            }
            Answer a = file.answers().get(c.key());
            if (a == null) {
                problems.add("UNSNAPPED: " + c.key());
            } else if (!a.sha().equals(c.sha())) {
                problems.add("STALE: " + c.key());
            }
        }
        for (String key : file.answers().keySet()) {
            if (!keys.contains(key)) {
                problems.add("GONE: " + key);
            }
        }
        return problems;
    }
}
