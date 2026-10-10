// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.equivalence;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.legend.testing.ProgramPaths;
import planner.TabExports;

import java.io.IOException;
import java.io.Writer;
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

/**
 * The round trip in the tab, its JVM half (docs/PROTOCOL_PROGRAM_2026_10_05.md §4, leg 5; {@code wasm/round_trip.mjs}
 * the other half): every input {@link RoundTripProofTest} reads -- legend-engine's test collection, lite's projects
 * and the upstream showcase projects, file by file -- asked of the tab's own adapter as the tab asks it
 * ({@link TabExports#pureV1OrError}, legend-engine's {@code pure/v1}): {@code grammarToJson/model} without source
 * information, then {@code jsonToGrammar/model} of the JSON it answered, in each style. Refusals are answers too, so
 * every input is asked, the ones the engine refuses included.
 *
 * <p>Writes one JSON line per input: where it comes from, its id, its text, and the SHA-256 of each answer in the
 * order asked. The digests stand for the answers, which would be several times the texts' size; the module's half
 * asks the same requests -- the second and third with the JSON its own first answered, the same when the first
 * matched -- and compares the digests, so a byte of difference shows. A BUILD ACTION
 * ({@code //parser-equivalence:tab_round_trip}): a function of the inputs and the tab's source, cached until either
 * changes.
 *
 * <p>Usage: {@code TabRoundTripRequests <out-file>}, with the corpus's flags ({@link Corpus}) and
 * {@code -Dlegend.roundtrip.projects}, the projects' files by exec path.
 */
public final class TabRoundTripRequests {

    static final String GRAMMAR_TO_JSON = "/api/pure/v1/grammar/grammarToJson/model";
    static final String JSON_TO_GRAMMAR = "/api/pure/v1/grammar/jsonToGrammar/model";
    static final List<String> STYLES = List.of("STANDARD", "PRETTY");
    private static final ObjectMapper JSON = new ObjectMapper();

    private TabRoundTripRequests() {
    }

    public static void main(String[] args) throws IOException {
        int inputs = 0;
        int converted = 0;
        try (Writer out = Files.newBufferedWriter(Path.of(args[0]), StandardCharsets.UTF_8)) {
            for (Corpus.Source source : Corpus.all()) {
                inputs++;
                converted += write(out, "engine corpus", source.id(), source.text()) ? 1 : 0;
            }
            for (Path file : ProgramPaths.listed("legend.roundtrip.projects")) {
                inputs++;
                String from = file.toString().contains("legend_showcase_") ? "showcase projects" : "lite projects";
                converted += write(out, from, file.toString().replace('\\', '/'),
                        Files.readString(file, StandardCharsets.UTF_8)) ? 1 : 0;
            }
        }
        System.out.println("[tab-round-trip] inputs=" + inputs + " converted=" + converted);
    }

    /** One input's line; whether its text converted (its JSON then printed in each style). */
    private static boolean write(Writer out, String from, String id, String text) throws IOException {
        String toJson = TabExports.pureV1OrError(GRAMMAR_TO_JSON, "returnSourceInformation=false", text);
        List<String> answers = new ArrayList<>();
        answers.add(sha256(toJson));
        String json = okBody(toJson);
        if (json != null) {
            for (String style : STYLES) {
                answers.add(sha256(TabExports.pureV1OrError(JSON_TO_GRAMMAR, "renderStyle=" + style, json)));
            }
        }
        Map<String, Object> line = new LinkedHashMap<>();
        line.put("from", from);
        line.put("id", id);
        line.put("text", text);
        line.put("answers", answers);
        out.write(JSON.writeValueAsString(line));
        out.write('\n');
        return json != null;
    }

    /** A folded answer's body when it is a 200 ({@code OK\n200\n<type>\n<body>}), else null. */
    static String okBody(String folded) {
        String prefix = "OK\n200\n";
        if (!folded.startsWith(prefix)) {
            return null;
        }
        int type = folded.indexOf('\n', prefix.length());
        return type < 0 ? null : folded.substring(type + 1);
    }

    static String sha256(String answer) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
                    .digest(answer.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }
}
