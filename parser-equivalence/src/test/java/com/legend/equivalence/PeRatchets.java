// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.equivalence;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.TreeMap;

/**
 * PARSER-EQUIVALENCE'S MEASURED RATCHET VALUES (Bazel workplan P2-16, decision D9 (b)): counts tests pinned by
 * hand-copied constants -- the own-corpus elements matching the referee, the mutant deck's size -- measured by this
 * program into {@code ratchets.tsv} ({@code key<TAB>value}, sorted). {@code //parser-equivalence:ratchets} runs it,
 * {@code bazel run //parser-equivalence:update_ratchets} writes the committed copy and {@code //:generated} fails when
 * it is stale; a test compares its live value with the committed one ({@link #measured}). Allowances (the per-host
 * and per-file pins) stay hand-owned in Java.
 *
 * <pre>
 *   PeRatchets &lt;output&gt;
 * </pre>
 */
public final class PeRatchets {

    private PeRatchets() {}

    /** Each move of a value, dated and with its reason (AGENTS.md: a pin moves only with one; the A delta audit's note
     *  16, 2026-11-11), written into the file as comments above the values. A move appends a line here in the change
     *  that regenerates the file. */
    static final java.util.List<String> HISTORY = java.util.List.of(
            "own_corpus.matched 3090 -> 3091 (2026-11-09, 8ff49fc0): the follow-up audit's tests' models",
            "own_corpus.matched 3091 -> 3093 (2026-11-10, 59758819): the A2-A5 tests' models",
            "own_corpus.matched 3093 -> 3096 (2026-11-10, b8337475): A6's time-zone and number-text tests' models",
            "own_corpus.matched 3096 -> 3100 (2026-11-10, 1e2c23b3): A7's date-time fraction tests' models",
            "own_corpus.matched 3100 -> 3106 (2026-11-10, c755071b): the A checkpoint's tests' models (QueryRuntimeTest,"
                    + " RowLoadCellsTest, TextPlanSourceTest and the rest)",
            "own_corpus.matched 3106 -> 3116 (2026-11-11, B2e): EmbeddedDataKindsTest's ten elements (a Database and three"
                    + " Data elements; a service's model of six)",
            "own_corpus.matched 3116 -> 3117 (2026-11-11, B2a): JsonSourceFrameArrayTextTest's class, the frame's model",
            "own_corpus.matched 3117 -> 3136 (2026-11-11, B2d.2 and its audit round): the test-data generator's tests'"
                    + " models (GeneratedTestDataProgramTest's store, mapping, helpers and runtimes, TestDataPlannerTest's"
                    + " and ReservedNamesGenerationTest's)",
            "own_corpus.matched 3136 (2026-11-11, the JSON pass, 101b557, merged onto B2d.2): it measured 3117 -> 3126 on its"
                    + " own branch; the merged tree measures 3136, the two branches' moves not additive (as measured, not"
                    + " attributed)",
            "own_corpus.matched 3136 -> 3137 (2026-11-11, B3 merged onto the total wall, f1a6eea2c): B3's tests' models (its"
                    + " remote mount's and host file's tests)",
            "own_corpus.matched 3137 -> 3140 (2026-11-11, the B checkpoint's audit fixes on claude/total-wall: M1, C1/C9,"
                    + " C3, LOW 8): their tests' models (a Databricks runtime with a declared parameter, the catalog's"
                    + " staged reads, the text plan's catalog read)",
            "own_corpus.matched 3140 -> 3159 (2026-11-12, B2d.3a-e on claude/total-wall-b2d3): the generator's slices'"
                    + " tests' models (TestDataPlannerTest's view, seed, schema and reload models, TestDataRoutesTest's"
                    + " store, mapping and runtime)",
            "own_corpus.matched 3159 -> 3163 (2026-11-12, the B checkpoint's G4 and the routes' parameters on"
                    + " claude/total-wall-b2d3): their tests' models (TestDataPlannerTest's keyless table and lower-case"
                    + " view, GeneratedTestDataProgramTest's lower-case table, TestDataRoutesTest's parameterised query)",
            "own_corpus.matched 3163 -> 3164 (2026-11-12, G7 built as ruled, S302, on claude/total-wall-b2d3):"
                    + " TestDataTextAsWrittenTest's store",
            "own_corpus.matched 3164 -> 3166 (2026-11-12, the simple synthesize slice merged, a24f3a5b4):"
                    + " DuckDbSynthesizeTest's models (its TRADES table and the refusals' tables)",
            "own_corpus.matched 3166 -> 3179 (2026-11-12, the B2d.3 audit's findings M1-M5, L1-L9): their tests'"
                    + " models (TestDataPlannerTest's route text, seed shapes, default-schema and view-with-note models,"
                    + " TestDataTextAsWrittenTest's two-database connection, TestDataRoutesTest's view and unmapped"
                    + " runtime, PostgresArmTest's seed model)",
            "own_corpus.matched 3179 -> 3192 (2026-11-12, L9's printing merged, 7d1bfcf8e; it measured 3164 -> 3177 on its"
                    + " branch): its tests' models (TestDataPlannerTest's timestamp store, GeneratedTestDataPrintingTest's,"
                    + " TestDataRoutesTest's timestamp route)",
            "own_corpus.matched 3192 -> 3211 (2026-11-13, the final full audit's fixes, d7c037ebf, found red by the gate on"
                    + " that head): their tests' models (TestDataRoutesTest's temporal keys, TdsTemporalCellTest,"
                    + " EngineSeedRowTest's refusals, DuckDbOutputPlanTest's every-type and decimal-sum tables, the"
                    + " snapshot-copy and output-plan tests)",
            "own_corpus.matched 3211 -> 3221 (2026-11-13, claude/total-wall-d with main merged onto total-wall-e,"
                    + " 8f0037994d, found by //:generated on 3a711976af): as measured, not attributed -- the merge"
                    + " joined D's, main's and E's new tests (RaisedKindTest, ErrorKindTest, EdgeSpellingHookTest,"
                    + " LetBindingTest, FusedHostColumnsTest and their siblings); the ten models were not traced one by"
                    + " one",
            "own_corpus.matched 3221 -> 3222 (2026-11-13, D3 and S341 on claude/total-wall-e): TypeToStringTest's model"
                    + " (its d3::Car class)",
            "own_corpus.matched 3211 -> 3246 (2026-11-13, main's #95 merged into the timeseries branch, 599d3c4c8, PR"
                    + " #81; the branch read 3126 alone at 27a35760b): the timeseries branch's tests' models (the #TS"
                    + " island's, the library's, the documents', the adversarial fixture's and the frame and as-of"
                    + " tests')",
            "own_corpus.matched 3222/3246 -> 3257 (2026-11-14, merged main 3a217086a (#81) into claude/total-wall-e, measured on"
                    + " the merged tree):"
                    + " both sides' models joined, 3222 + 3246 - 3211 = 3257, not one side's number",
            "own_corpus.matched 3246 -> 3251 (2026-11-14, main 3a217086a (#81) merged into claude/total-wall-d,"
                    + " whose own tests had not been measured: phase D's, the edge's and the raise kinds'), as measured"
                    + " on the merged tree, not attributed",
            "own_corpus.matched 3257/3251 -> 3257 (2026-11-14, claude/total-wall-d 392ce271b merged into"
                    + " claude/total-wall-e, as measured): D's added models were already E's, through E's earlier merge of D",
            "own_corpus.matched 3211 -> 3226 (2026-11-14, C4's must-haves M1-M5, found red by the gate on b915e29405):"
                    + " the stream's new tests' models (PreQuotedNamesTest's H2 model for the auditor's FILE_READ case among"
                    + " them)",
            "own_corpus.matched 3226, 3246 -> 3261 (2026-11-14, merged main 3a217086a (#81) into the separation stream,"
                    + " measured by //parser-equivalence:update_ratchets on the merged tree): both sides' models together",
            "own_corpus.matched 3261 -> 3266 (2026-11-15, main cf865632c (#101, C4) merged into claude/total-wall-d,"
                    + " measured by //parser-equivalence:update_ratchets on the merged tree): both sides' models together",
            "own_corpus.matched 3261 -> 3262 (2026-11-15, C5-J, found by the gate on its branch): its new tests' models,"
                    + " measured by //parser-equivalence:update_ratchets",
            "own_corpus.matched 3266, 3262 -> 3267 (2026-11-15, the merge of #106 into #104: both sides' tests'"
                    + " models together; TO RE-MEASURE by //parser-equivalence:update_ratchets after the build)",
            "own_corpus.matched 3257/3261 -> 3272 (2026-11-15, merged main cf865632c (#101) into claude/total-wall-e,"
                    + " measured on the merged tree): E's 3257 and C4's fifteen (3261 - 3246), joined",
            "own_corpus.matched 3272/3262 -> 3273 (2026-11-15, main 0c9032f56 (C5-J's 3261 -> 3262) merged into"
                    + " claude/total-wall-e, measured on the merged tree): C5-J's one new model joined E's",
            "own_corpus.matched 3267/3273 -> TO RE-MEASURE (2026-11-16, W1 of docs/records/d-plan-2026-11-16.md: E's PR1"
                    + " merged into D's stage, both sides' models together; by //parser-equivalence:update_ratchets)",
            "own_corpus.matched 3273 -> 3281 (2026-11-16, D's stage at 97c178a44, the first full CI's parser_equivalence"
                    + " lane): eight more of lite's own models match, a ratchet UP",
            "own_corpus.matched 3281 -> 3284 (2026-11-16, D's stage: PlanDeclaresItsRaisesTest's MongoDB model written"
                    + " as the reference grammar reads it -- serverURLs [localhost:27017], where an IP address was refused"
                    + " (GRAMMAR-REFUSAL-nullmsg, stage-linux 43917161620) -- and the stage's new test models; UP)",
            "own_corpus.matched 3284 -> 3287 (2026-11-17, phase D's Phase 2, 8d38788b0: InterimEdgeSitesTest's model --"
                    + " its s::DB, s::Conn and s::RT -- matches the reference grammar; UP)",
            "own_corpus.matched 3287 -> 3290 (2026-11-17, A1 0902f0f33 (its 1c18e97f4): BoundaryLiteExportsTest's model --"
                    + " its t::P, t::DB and t::M -- matches the reference grammar; UP; stage-linux run 44097841621)");

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            throw new IllegalArgumentException("usage: PeRatchets <output>");
        }
        Map<String, Integer> out = new TreeMap<>();
        out.put("mutation.deck", MutationFuzzTest.deckSize());
        OwnCorpusLedgerDraft.Diffs own = OwnCorpusLedgerDraft.diffs();
        out.put("own_corpus.matched", own.matched());
        // the count's verdicts by kind on one line: two measurements that differ diff to one line
        System.out.println("[pe-ratchets] own_corpus " + own.kinds());
        StringBuilder text = new StringBuilder("# parser-equivalence's measured ratchet values (PeRatchets) --"
                + " regenerate: bazel run //parser-equivalence:update_ratchets\n");
        HISTORY.forEach(h -> text.append("# ").append(h).append('\n'));
        out.forEach((k, v) -> text.append(k).append('\t').append(v).append('\n'));
        Files.writeString(Path.of(args[0]), text.toString(), StandardCharsets.UTF_8);
    }

    /** The committed file, read on first use only: the generator ({@link #main}) never reads it, so a damaged copy
     *  cannot block its own regeneration. */
    private static final class Committed {
        static final Map<String, Integer> VALUES = load();

        private static Map<String, Integer> load() {
            Map<String, Integer> m = new TreeMap<>();
            try (InputStream in = PeRatchets.class.getResourceAsStream("ratchets.tsv")) {
                if (in == null) {
                    throw new IllegalStateException("ratchets.tsv is not on the classpath -- bazel run //parser-equivalence:update_ratchets");
                }
                int n = 0;
                for (String line : new String(in.readAllBytes(), StandardCharsets.UTF_8).split("\n")) {
                    n++;
                    if (line.isBlank() || line.startsWith("#")) {
                        continue;
                    }
                    String[] kv = line.split("\t", -1);
                    if (kv.length != 2 || !kv[1].matches("-?\\d+")) {
                        throw new IllegalStateException("ratchets.tsv line " + n + " is no key<TAB>integer: " + line
                                + " -- bazel run //parser-equivalence:update_ratchets");
                    }
                    m.put(kv[0], Integer.parseInt(kv[1]));
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            return m;
        }
    }

    /** The committed measured value for {@code key}. */
    static int measured(String key) {
        Integer v = Committed.VALUES.get(key);
        if (v == null) {
            throw new IllegalStateException("ratchets.tsv has no " + key
                    + " -- bazel run //parser-equivalence:update_ratchets");
        }
        return v;
    }
}
