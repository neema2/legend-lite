// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.equivalence;

import com.legend.testing.Repo;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.finos.legend.engine.language.pure.grammar.from.PureGrammarParser;
import org.finos.legend.engine.language.pure.grammar.from.extension.PureGrammarParserExtensions;
import org.finos.legend.engine.shared.core.ObjectMapperFactory;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * THE SWEEP — the equivalence harness's end state
 * (HARNESS_SIMPLIFICATION_PLAN): ONE pass over the manifest-pinned
 * corpus, ONE oracle parse per source, every claim computed as a column
 * of the same iteration and asserted TOGETHER at the end. It replaces
 * six test classes (CorpusEquivalence, PmcdEquivalence, RefusalSymmetry,
 * LeniencyCatalog, StrictDialectParity, SpiSeamProof) and the
 * {@code OracleParses} cache whose eviction tuning existed only because
 * those six each ran their own corpus loop.
 *
 * <p>THE CLAIM FAMILIES (each with its numeric ratchets):
 * <ol>
 *   <li><b>Byte equality where both accept</b> — lite's document parser
 *       AND the SPI seam (engine+lite bridge) byte-match the oracle's
 *       full PMCD; the seam's engine-serialize-only bucket is
 *       membership-proven per row ({@link Comparators}).</li>
 *   <li><b>Verdict symmetry</b> — oracle threw ⟺ we threw, every
 *       exception a line of a checked-in, shrink-only allowlist file
 *       ({@code docs/refusal-allowlist.tsv},
 *       {@code docs/model-refuse-allowlist.tsv}); stale lines are
 *       reported for removal.</li>
 *   <li><b>Instrument honesty</b> — the M3 second-reference calibrates
 *       itself every run; the comparator self-test lives in
 *       {@code ComparatorSelfTest}; the corpus is SHA-pinned by
 *       {@code CorpusManifestTest}.</li>
 * </ol>
 *
 * <p>Element-level comparison is a DIAGNOSTIC ({@code ParserEquivalence}
 * localises a document failure to an element index and JSON path); byte-
 * equal documents imply byte-equal elements — proven empirically at the
 * Phase-2 demotion (joint property zero over the full corpus, commit
 * 89c5907d) and structurally since the ledger feeds from the same
 * {@code parseSections} the document serialises.
 *
 * <p>Row-level attribution does NOT live here: the allowlist TSVs are
 * the system of record and only shrink; this sweep merely enforces them.
 */
public class CorpusSweepTest {

    private static final Pattern SECTION = Pattern.compile("(?m)^###(\\w+)");

    // ------------------------------------------------------------------
    // Ratchets (histories in git: SpiSeamProofTest / PmcdEquivalenceTest
    // / RefusalSymmetryTest at commit d4a70c00 and earlier)
    // ------------------------------------------------------------------

    /** Oracle-accepted documents lite must byte-match. Up-only. */
    private static final int MIN_DOCS_MATCHED = 6471;   // 2026-08-14: EVERY oracle-accepted source byte-matches — 100%; 6489 -> 6471 on 2026-09-10 (upstream boundary batch 1): the SOURCE pin moved to the 4.138.2 TAG and the oracle-accepted population is 6,471 there (57 manifest rows left with the 20 newer commits) — still 100%, a denominator move

    /** Seam byte coverage floor. Up-only. */
    private static final int MIN_SEAM_MATCHED = 6462;   // 2026-08-14: the strictness batch (stray-')' + copy-new gates) earned +569; 6480 -> 6462 on 2026-09-10 (batch 1): the same denominator move (9 engine JSON-asymmetry rows unchanged)

    /** Vanilla-rejected sources the SPI seam accepts — post-flip residue
     *  (upstream walker defects + the reviewed allowlist). Down-only. */
    private static final int MAX_SEAM_ORACLE_ASYMMETRY = 0;   // 2026-08-14: Primitive + brace-less #TDS gates closed the last 18

    /** Seam rows whose delta is the ENGINE's serialize-only field,
     *  membership-proven per row. Down-only. */
    private static final int MAX_ENGINE_JSON_ASYMMETRY = 9;

    /** Pure-only vanilla-rejected files raw parseStrict accepts — the
     *  strict element surface's own census. Down-only. */
    private static final int MAX_STRICT_ORACLE_ASYMMETRY = 2;   // 2026-08-14: was 181 -> 22 (stray-')' gate) -> 2 (Primitive + brace-less #TDS gates); the 2 are ORACLE-DEFECT-crash walker NPEs (U3), named in UPSTREAM_DEFECTS.md

    /** CEILING on the platform-COVERAGE catalog (each oracle-refused source the PLATFORM dialect accepts, classified) (deep-audit #2
     *  2d: the population was unbounded — every row classifies, but
     *  nothing stopped the TOTAL from growing silently). Shrink-only;
     *  measured 2026-08-14. */
    // 2026-08-23 relation wall burn: +2 ADJUDICATED rows — Profile
    // self-stereotypes (m4-pure syntax; the engine grammar has no such
    // slot, verified in DomainParserGrammar, so the oracle refuses).
    // The platform lane parses-and-DROPS them faithfully (the engine
    // protocol Profile carries no applied-annotation field); the LEGEND
    // dialect still refuses verbatim (refusesLiteExtensions-gated). The
    // two sources are the PCT infrastructure files pct_core.pure +
    // pctQualifiers.pure — the latter unlocked the 68-test over.pure
    // window scope (relation discovery 287 -> 355).
    // 2026-09-04 (batch 54, option S): +3 adjudicated rows — m3 VARIANCE
    // markers on class type parameters (real m3 path.pure: Path<-U,V|m>)
    // now parse in the platform dialect (the prelude generator reads the
    // spec's declaration files); the engine grammar has no variance slot
    // (its refusal: type/multiplicity parameters not authorized) —
    // PURE-DIALECT-generics family; the exact-engine surface is untouched.
    private static final int MAX_PLATFORM_CATALOG = 1633;   // 2026-09-09 parser leg (batch 174): +32 rows the PLATFORM dialect now reads and the engine refuses (null-message GRAMMAR-REFUSAL / PURE-DIALECT-generics): type variables `Class X(x:Integer[1])` / `Primitive P(x) extends`, `@[m]`, `^X(v)(…)` — legend-pure's cast.pure, toMultiplicity.pure, addColumns.pure, new.pure, precisePrimitives.pure and their inline tests; the exact-engine surface refuses them as before (dialect quarantine). CORRECTION 2026-09-10 (upstream boundary batch 1): bare `@(…)` was listed here too, but the 4.138.2 oracle ACCEPTS it in every probed position (bare, `->cast(@(…))`, `@(name:Varchar(200))->genericType()` — the engine's own testSchema.pure carries it), so SpecParser now reads it on every surface
    // was 1601;   // 2026-09-08 units (census batch 150): +37 PURE-DIALECT-unit-instance rows (AbstractTestMeasure, AbstractTestToJson)
    // was 1564;   // 2026-08-15 doc-string burn: +47 adjudicated rows (PURE-DIALECT-doc-string=40 + members), A5 gap 273->226
    // 2026-08-19 Phase-4 entry-gate m3 burn: +42 adjudicated rows — the
    // PCT test-file surface (236/236 now parses at LEGEND_PLATFORM):
    // tagged-value string concatenation, negative-year date literals,
    // class multiplicity-params (<|m>), m3 range literals ([a:b:c] ->
    // range), and the (?:?) column-type wildcard. All platform-dialect
    // acceptances classifying into the existing PURE-DIALECT-* families;
    // the exact-engine surface is untouched (byte/rejection parity green).

    /** M3 second-reference agreement floor on oracle-accepted
     *  section-free sources — below this the "m3-corroborated"
     *  allowlist label stops meaning anything. */
    /** A6: verbatim first-line message matches on both-reject rows.
     *  UP-only; measured 2026-08-15 (first measurement pins at 0 until
     *  the run below replaces it). */
    private static final int MSG_VERBATIM_FLOOR = 863;   // 2026-08-15 first pin: position-normalized text parity on both-reject rows (ours carries [l:c], the oracle does not); 1,531 mismatch rows in target/message-parity.txt are the A6 closure work-list

    /** A5: m3-accepts-platform-refuses ceiling — measured 2026-08-15;
     *  placeholder until the first run pins it. */
    private static final int MAX_M3_ONLY_PLATFORM_GAPS = 226;   // 2026-08-15: doc-string family burned (was 273)

    /** A6 classified tiers — measured 2026-08-15, placeholder until
     *  the first run pins them. */
    private static final int MSG_RICHER_FLOOR = 1240;   // 2026-08-15: oracle-degenerate rows (NPE-text/null/generic) where ours is a positioned specific diagnostic; 1277 -> 1240 on 2026-09-10 (upstream boundary batch 1): the SOURCE pin moved from 4.137.0+36 back to the 4.138.2 TAG — 57 manifest rows left (8,891 -> 8,834, mostly C4 engine-inline snippets from the 20 newer commits), and 37 of them were oracle-degenerate rows; a denominator move (GENUINE mismatches unchanged)
    private static final int MAX_MSG_GENUINE_MISMATCH = 254;   // 2026-08-15 first pin: the REAL error-voice divergence tail — adjudicate down

    private static final double M3_CALIBRATION_FLOOR = 95.0;

    @Test
    void oneSweep() throws Exception {
        List<Corpus.Source> sources = Corpus.all();
        // CORPUS-SIZE FLOOR, not an Assumptions skip (deep-audit: "an
        // absent corpus skips GREEN") — a missing checkout must fail
        org.junit.jupiter.api.Assertions.assertTrue(sources.size() > 7000,
                "corpus floor: only " + sources.size() + " sources loaded —"
                        + " check -Dlegend.engine.root/-Dlegend.pure.root");
        Assumptions.assumeTrue(!sources.isEmpty(),
                "no corpus on disk — set -Dlegend.engine.root / -Dlegend.pure.root");
        ObjectMapper mapper = ObjectMapperFactory
                .getNewStandardObjectMapperWithPureProtocolExtensionSupports();
        PureGrammarParser oracle = PureGrammarParser.newInstance();
        List<org.finos.legend.engine.language.pure.grammar.from.extension
                .PureGrammarParserExtension> withLite = new ArrayList<>();
        withLite.add(LegendLiteSectionParser.extension());
        withLite.addAll(org.finos.legend.engine.language.pure.grammar.from
                .extension.PureGrammarParserExtensionLoader.extensions());
        PureGrammarParser spi = PureGrammarParser.newInstance(
                PureGrammarParserExtensions.fromExtensions(withLite));

        Map<String, String> refusalAllow =
                readAllowlist("refusal-allowlist.tsv");
        java.util.Map<String, String> c12Walls =
                readAllowlist("c12-walls.tsv");
        java.util.Map<String, String> c12Diffs =
                readAllowlist("c12-known-diffs.tsv");
        Map<String, String> modelRefuseAllow =
                readAllowlist("model-refuse-allowlist.tsv");

        // F3.7b ledger accounting: which pardon rows were actually NEEDED
        java.util.Set<String> c12DiffsUsed = new java.util.TreeSet<>();
        java.util.Set<String> c12WallsUsed = new java.util.TreeSet<>();
        java.util.Set<String> modelRefuseUsed = new java.util.TreeSet<>();

        // accumulators
        int docsMatched = 0;
        int seamMatched = 0;
        int bothReject = 0;
        int msgVerbatim = 0;
        int msgRicher = 0;
        List<String> msgMismatch = new ArrayList<>();
        int m3PlatformAgree = 0;
        List<String> m3OnlyRows = new ArrayList<>();
        int oracleAccepts = 0;
        int calAccepted = 0;
        int calAgree = 0;
        int strictAsymmetry = 0;
        List<String> docDiffs = new ArrayList<>();
        List<String> weRefuse = new ArrayList<>();
        List<String> modelRefuse = new ArrayList<>();
        List<String> seamDiffs = new ArrayList<>();
        List<String> seamRejects = new ArrayList<>();
        List<String> seamAccepts = new ArrayList<>();
        List<String> engineAsym = new ArrayList<>();
        List<String> asymRows = new ArrayList<>();
        List<String> unlistedAsym = new ArrayList<>();
        List<String> dialectLeaks = new ArrayList<>();
        List<String> unclassified = new ArrayList<>();
        List<String> strictAsymmetryIds = new ArrayList<>();
        List<String> strictUnexplained = new ArrayList<>();
        List<String> protocolInvalid = new ArrayList<>();
        Map<String, Integer> strictAsymmetryByClass = new TreeMap<>();
        Map<String, Integer> catalogByClass = new TreeMap<>();
        StringBuilder catalog = new StringBuilder();
        java.util.Set<String> asymIds = new java.util.HashSet<>();

        for (Corpus.Source src : sources) {
            String oracleJson = null;
            Throwable oracleRoot = null;
            try {
                oracleJson = mapper.writeValueAsString(
                        oracle.parseModel(src.text()));
            } catch (Throwable t) {
                oracleRoot = rootOf(t);
            }

            if (oracleJson != null) {
                oracleAccepts++;
                // CLAIM 1a: the document parser byte-matches
                try {
                    String doc = com.legend.parser.PmcdParser
                            .parseDocument(src.text());
                    if (Comparators.sameBytes(oracleJson, doc)) {
                        docsMatched++;
                    } else if (c12Diffs.containsKey(src.id())) {
                        c12DiffsUsed.add(src.id());
                    } else {
                        docDiffs.add(src.id() + " :: "
                                + firstDivergence(oracleJson, doc));
                    }
                } catch (Throwable t) {
                    if (c12Walls.containsKey(src.id())) {
                        c12WallsUsed.add(src.id());
                    } else {
                        weRefuse.add(src.id() + " :: "
                                + msgOf(rootOf(t)));
                    }
                }
                // CLAIM 2a: the MODEL transform reads every accepted
                // source (compile-seam family excepted, BY ID)
                try {
                    Surfaces.platform(src.text());
                } catch (Throwable t) {
                    if (modelRefuseAllow.containsKey(src.id())) {
                        modelRefuseUsed.add(src.id());
                    } else {
                        modelRefuse.add(src.id() + " :: "
                                + msgOf(rootOf(t)));
                    }
                }
                // CLAIM 1b: the SPI seam byte-matches (the drop-in shape)
                try {
                    String spiJson = mapper.writeValueAsString(
                            spi.parseModel(src.text()));
                    if (Comparators.sameBytes(oracleJson, spiJson)) {
                        seamMatched++;
                    } else if (Comparators.engineSerializeOnlyDelta(mapper,
                            oracleJson, spiJson)) {
                        engineAsym.add(src.id());
                    } else {
                        seamDiffs.add(src.id());
                    }
                } catch (Throwable t) {
                    seamRejects.add(src.id() + " :: " + msgOf(rootOf(t)));
                }
                // CLAIM 3a: M3 self-calibration (section-free only)
                if (!SECTION.matcher(src.text()).find()) {
                    calAccepted++;
                    if (m3Accepts(src.text())) {
                        calAgree++;
                    }
                }
                continue;
            }

            // ---------------- oracle REFUSED this source ----------------
            boolean docAccepts = accepts(() -> com.legend.parser.PmcdParser
                    .parseDocument(src.text()));
            boolean strictAccepts = accepts(() -> Surfaces.engine(src.text()));
            boolean platformAccepts = accepts(() -> Surfaces.platform(src.text()));
            boolean pureOnly = !SECTION.matcher(src.text()).find();
            // A5 (platform differential): on rows where BOTH verdicts
            // exist, legend-pure's own M3 parser vs our PLATFORM tier.
            // m3-accepts-platform-REFUSES is the finding that matters:
            // a genuine platform-tier gap. Down-only.
            if (pureOnly) {
                boolean m3 = m3Accepts(src.text());
                if (m3 == platformAccepts) {
                    m3PlatformAgree++;
                } else if (m3) {
                    m3OnlyRows.add(src.id());
                }
            }

            if (!docAccepts && !strictAccepts) {
                bothReject++;
                // A6 (message-parity floor): on both-reject rows, does
                // OUR refusal say what the engine's says? First line,
                // verbatim. The floor ratchets the drop-in error voice.
                try {
                    com.legend.parser.PmcdParser.parseDocument(src.text());
                } catch (Throwable ours) {
                    String om = msgOf(oracleRoot);
                    String lm = String.valueOf(rootOf(ours).getMessage())
                            .split("\n")[0]
                            // ours carries a [line:col] prefix the oracle
                            // lacks — RICHER, not divergent; compare text
                            .replaceFirst("^\\[\\d+:\\d+\\]\\s*", "");
                    if (om.equals(lm)) {
                        msgVerbatim++;
                    } else if (om.equals("null")
                            || om.startsWith("Cannot invoke")
                            || om.startsWith("Cannot read")
                            || om.startsWith("Index ")
                            || om.equals("Unexpected token")
                            || om.equals("Unsupported syntax")) {
                        // ORACLE-DEGENERATE: the engine's "message" is a
                        // raw NPE string, null, or its generic catch-all
                        // while ours is a positioned specific diagnostic
                        // — richer BY DESIGN (never regress to match)
                        msgRicher++;
                    } else {
                        msgMismatch.add(src.id() + "\n  oracle: " + om
                                + "\n  lite:   " + lm);
                    }
                }
            } else {
                if (docAccepts) {
                    // PROTOCOL-CHECK (sibling handoff instrument, adopted
                    // 2026-08-14): a document WE accept that the oracle
                    // refuses must still emit JSON the ENGINE's own
                    // protocol classes deserialize — a wrong-kind key
                    // emits wire no downstream consumer can read (the
                    // Persistence corruption class). Byte-matched docs
                    // are exempt by construction: their wire IS the
                    // engine's.
                    try {
                        mapper.readValue(com.legend.parser.PmcdParser
                                .parseDocument(src.text()),
                                org.finos.legend.engine.protocol.pure.v1
                                        .model.context.PureModelContextData.class);
                    } catch (Throwable t) {
                        protocolInvalid.add(src.id() + " :: "
                                + msgOf(rootOf(t)));
                    }
                }
                // CLAIM 2b: verdict symmetry — every asymmetry is an
                // allowlist line
                String category = !pureOnly ? "sectioned"
                        : m3Accepts(src.text()) ? "m3-corroborated"
                                : "m3-rejects";
                String surfaces = (docAccepts ? "document" : "")
                        + (docAccepts && strictAccepts ? "+" : "")
                        + (strictAccepts ? "strict" : "");
                asymIds.add(src.id());
                asymRows.add(src.id() + "\t" + category + "\t" + surfaces
                        + "\t" + msgOf(oracleRoot));
                if (!refusalAllow.containsKey(src.id())) {
                    unlistedAsym.add(src.id() + " [" + category + "/"
                            + surfaces + "] " + msgOf(oracleRoot));
                }
            }
            if (pureOnly && strictAccepts) {
                strictAsymmetry++;
                strictAsymmetryIds.add(src.id() + " :: vanilla: "
                        + msgOf(oracleRoot));
                // LEG-1 CLOSURE (2026-08-13): every strict-lenient row must
                // CLASSIFY — the bare count looked like unadjudicated debt
                // when it was really 154 oracle-NPE + skew-claims + nullmsg
                // pure-dialect files; an unexplained row goes red here
                CLASSIFYING_ID.set(src.id());
                String scls = classify(oracleRoot, src.text());
                CLASSIFYING_ID.remove();
                if (scls == null) {
                    strictUnexplained.add(src.id() + " :: "
                            + msgOf(oracleRoot));
                } else {
                    strictAsymmetryByClass.merge(scls, 1, Integer::sum);
                }
            }
            // seam census: the drop-in accepting what vanilla refuses
            if (accepts(() -> spi.parseModel(src.text()))) {
                seamAccepts.add(src.id() + " :: vanilla: "
                        + msgOf(oracleRoot));
            }
            // leniency catalog + dialect parity (the lenient MODEL surface)
            if (platformAccepts) {
                CLASSIFYING_ID.set(src.id());
                String cls = classify(oracleRoot, src.text());
                CLASSIFYING_ID.remove();
                if (cls == null) {
                    unclassified.add(src.id() + " :: " + msgOf(oracleRoot));
                    cls = "UNCLASSIFIED";
                }
                catalogByClass.merge(cls, 1, Integer::sum);
                catalog.append(cls).append('\t').append(src.id())
                        .append('\t').append(msgOf(oracleRoot)).append('\n');
                if (cls.startsWith("DIALECT-") && strictAccepts) {
                    dialectLeaks.add(cls + " :: " + src.id());
                }
            }
        }

        // stale allowlist rows — parity fixed, lines to REMOVE
        List<String> stale = refusalAllow.keySet().stream()
                .filter(id -> !asymIds.contains(id)).toList();
        // F3.7b: staleness is ENFORCED, not just reported, for all four
        // pardon ledgers; sizes are pinned so growth is a reviewed event
        assertEquals(List.of(), stale.stream().sorted().toList(),
                "stale refusal-allowlist rows (parity fixed) — remove them"
                        + " from docs/refusal-allowlist.tsv");
        assertEquals(List.of(), modelRefuseAllow.keySet().stream()
                        .filter(id -> !modelRefuseUsed.contains(id))
                        .sorted().toList(),
                "stale model-refuse-allowlist rows (the platform transform"
                        + " now reads them, or the id left the corpus) —"
                        + " remove them from docs/model-refuse-allowlist.tsv");
        assertEquals(List.of(), c12Walls.keySet().stream()
                        .filter(id -> !c12WallsUsed.contains(id))
                        .sorted().toList(),
                "stale c12-walls rows — remove them");
        assertEquals(List.of(), c12Diffs.keySet().stream()
                        .filter(id -> !c12DiffsUsed.contains(id))
                        .sorted().toList(),
                "stale c12-known-diffs rows — remove them");
        assertEquals(REFUSAL_ALLOW_TOTAL, refusalAllow.size(),
                "refusal-allowlist size drifted — re-pin with review");
        assertEquals(MODEL_REFUSE_ALLOW_TOTAL, modelRefuseAllow.size(),
                "model-refuse-allowlist size drifted — re-pin with review");
        assertEquals(0, c12Walls.size() + c12Diffs.size(),
                "the c12 ledgers are EMPTY and stay empty — a new row is a"
                        + " reviewed event (raise this pin deliberately)");

        writeReports(sources.size(), oracleAccepts, docsMatched, docDiffs,
                weRefuse, bothReject, seamMatched, engineAsym, seamDiffs,
                seamAccepts, seamRejects, strictAsymmetry, strictAsymmetryIds,
                asymRows, catalog, catalogByClass, stale);
        double calibration = calAccepted == 0 ? 0
                : 100.0 * calAgree / calAccepted;
        Files.writeString(Repo.out("m3-platform-differential.txt"),
                "agree " + m3PlatformAgree + "; m3-only (PLATFORM GAP) "
                        + m3OnlyRows.size() + "\n"
                        + String.join("\n", m3OnlyRows));
        // A5 ceiling (measured 2026-08-15): sources legend-pure's M3
        // parser accepts that the PLATFORM tier refuses. Down-only —
        // each is a candidate platform gap (or an M3-only construct,
        // adjudicated like every ledger).
        assertTrue(m3OnlyRows.size() <= MAX_M3_ONLY_PLATFORM_GAPS,
                "m3-accepts-platform-refuses grew: " + m3OnlyRows.size()
                        + " > " + MAX_M3_ONLY_PLATFORM_GAPS
                        + " (see target/m3-platform-differential.txt)");
        Files.writeString(Repo.out("message-parity.txt"),
                "verbatim " + msgVerbatim + " richer " + msgRicher
                        + " genuine-mismatch " + msgMismatch.size()
                        + " / " + bothReject + "\n\n"
                        + String.join("\n", msgMismatch));
        // ours-richer floor + GENUINE divergence ceiling (the real
        // error-voice number once oracle-degenerate rows are classified)
        assertTrue(msgRicher >= MSG_RICHER_FLOOR,
                "ours-richer message rows fell: " + msgRicher);
        assertTrue(msgMismatch.size() <= MAX_MSG_GENUINE_MISMATCH,
                "GENUINE message divergences grew: " + msgMismatch.size()
                        + " (target/message-parity.txt)");
        // A6 floor (measured 2026-08-15): verbatim-match count on
        // both-reject rows is UP-only — a drop means our refusal voice
        // drifted from the engine's.
        assertTrue(msgVerbatim >= MSG_VERBATIM_FLOOR,
                "message parity fell: " + msgVerbatim + " verbatim < floor "
                        + MSG_VERBATIM_FLOOR + " (see target/message-parity.txt)");

        System.out.printf("SWEEP: %d sources | oracle accepts %d | docs"
                + " matched %d diff %d weRefuse %d | seam %d/%d asym %d |"
                + " both-reject %d asymmetric %d (stale %d) | strictAsymmetry"
                + " %d | M3 cal %.1f%%%n",
                sources.size(), oracleAccepts, docsMatched, docDiffs.size(),
                weRefuse.size(), seamMatched, seamRejects.size(),
                engineAsym.size(), bothReject, asymRows.size(), stale.size(),
                strictAsymmetry, calibration);

        final int fDocs = docsMatched;
        final int fSeam = seamMatched;
        final int fStrict = strictAsymmetry;
        final double fCal = calibration;
        final int fAccepts = oracleAccepts;
        final int fBoth = bothReject;
        final int fCatalogTotal = catalogByClass.values().stream()
                .mapToInt(Integer::intValue).sum();
        System.out.println("strictAsymmetry by class: " + strictAsymmetryByClass);
        assertAll(
                () -> assertEquals(0, strictUnexplained.size(),
                        () -> "STRICT-LENIENT rows with NO catalog class —"
                                + " unadjudicated acceptance:\n  "
                                + head(strictUnexplained)),
                () -> assertEquals(0, docDiffs.size(), () -> "document byte"
                        + " diffs:\n  " + head(docDiffs)),
                () -> assertEquals(0, weRefuse.size(), () -> "oracle-accepted"
                        + " sources the document parser refuses:\n  "
                        + head(weRefuse)),
                () -> {
                    try {
                        java.nio.file.Files.write(Repo.out("model-refuse.tsv"), modelRefuse);
                    } catch (java.io.IOException ignored) {
                        // diagnostic artifact only
                    }
                    assertEquals(0, modelRefuse.size(), () -> "oracle-"
                        + "accepted sources the MODEL path refuses (not in"
                        + " model-refuse-allowlist.tsv):\n  "
                        + head(modelRefuse));
                },
                () -> assertEquals(0, seamDiffs.size(), () -> "SPI seam byte"
                        + " diffs:\n  " + head(seamDiffs)),
                () -> assertEquals(0, seamRejects.size(), () -> "the SPI seam"
                        + " refused vanilla-accepted sources:\n  "
                        + head(seamRejects)),
                () -> assertEquals(0, unlistedAsym.size(), () -> "NEW refusal"
                        + " asymmetries not in docs/refusal-allowlist.tsv —"
                        + " fix the parity or add a line WITH A REASON:\n  "
                        + head(unlistedAsym)),
                () -> assertEquals(0, dialectLeaks.size(), () -> "DIALECT"
                        + " rows still parse on the strict surface:\n  "
                        + head(dialectLeaks)),
                () -> assertEquals(0, unclassified.size(), () -> "UNCLASSIFIED"
                        + " leniency rows — adjudicate or fix, never"
                        + " ignore:\n  " + head(unclassified)),
                () -> assertTrue(fDocs >= MIN_DOCS_MATCHED,
                        "document coverage shrank: " + fDocs + " < "
                                + MIN_DOCS_MATCHED),
                () -> assertTrue(fSeam >= MIN_SEAM_MATCHED,
                        "seam coverage shrank: " + fSeam + " < "
                                + MIN_SEAM_MATCHED),
                () -> assertTrue(seamAccepts.size() <= MAX_SEAM_ORACLE_ASYMMETRY,
                        "seam leniency census grew: " + seamAccepts.size()
                                + " > " + MAX_SEAM_ORACLE_ASYMMETRY),
                () -> assertTrue(engineAsym.size() <= MAX_ENGINE_JSON_ASYMMETRY,
                        "engine JSON-asymmetry bucket grew: "
                                + engineAsym.size()),
                () -> assertTrue(fStrict <= MAX_STRICT_ORACLE_ASYMMETRY,
                        "parseStrict leniency census grew: " + fStrict
                                + " > " + MAX_STRICT_ORACLE_ASYMMETRY),
                () -> assertTrue(fCal >= M3_CALIBRATION_FLOOR,
                        String.format("M3 second-reference calibration %.1f%%"
                                + " below floor %.1f%% — the m3-corroborated"
                                + " label is no longer trustworthy",
                                fCal, M3_CALIBRATION_FLOOR)),
                () -> assertTrue(fAccepts > 0 && fBoth > 0,
                        "degenerate sweep: the corpus did not load"),
                () -> assertTrue(fCatalogTotal <= MAX_PLATFORM_CATALOG,
                        "leniency catalog grew: " + fCatalogTotal + " > "
                                + MAX_PLATFORM_CATALOG
                                + " — new leniency must be adjudicated"),
                () -> assertEquals(0, protocolInvalid.size(),
                        () -> "PROTOCOL-INVALID wire on lenient-accepted"
                                + " docs — the engine's own deserializer"
                                + " refuses what we emit:\n  "
                                + head(protocolInvalid)));
    }

    /** F3.7: the skew-claims ledger gets stale-row + total accounting.
     *  The ledger has TWO consumers with different populations (this
     *  sweep's classifier and SectionParseSentinelTest's), so liveness
     *  is asserted SEMANTICALLY, not by per-consumer usage: a claim row
     *  is live iff its source still EXISTS in the corpus, the pinned
     *  oracle still REFUSES it (the definition of checkout-unreleased
     *  skew), and lite still ACCEPTS it (otherwise there is nothing to
     *  pardon). Any other row is stale and must leave
     *  docs/version-skew-claims.tsv. */
    @Test
    void skewClaimsLedgerAccounting() throws Exception {
        List<Corpus.Source> sources = Corpus.all();
        org.junit.jupiter.api.Assertions.assertTrue(sources.size() > 7000,
                "corpus floor: only " + sources.size() + " sources loaded —"
                        + " check -Dlegend.engine.root/-Dlegend.pure.root");
        java.util.Map<String, String> byId = new java.util.HashMap<>();
        for (Corpus.Source s : sources) {
            byId.put(s.id(), s.text());
        }
        PureGrammarParser oracle = PureGrammarParser.newInstance();
        List<String> stale = new java.util.ArrayList<>();
        for (String claim : new java.util.TreeSet<>(SKEW_CLAIMS)) {
            String text = byId.get(claim);
            if (text == null) {
                stale.add(claim + " (source left the corpus)");
                continue;
            }
            boolean oracleRefuses;
            try {
                oracle.parseModel(text);
                oracleRefuses = false;
            } catch (Throwable t) {
                oracleRefuses = true;
            }
            if (!oracleRefuses) {
                stale.add(claim + " (the pinned oracle now ACCEPTS it —"
                        + " not skew anymore)");
                continue;
            }
            // the consumers pardon OUR checkout-side surfaces (the
            // sweep's platform/strict arms and the sentinel's section
            // walk), so liveness tests those — not the LEGEND document
            // parser
            boolean oursAccepts;
            try {
                Surfaces.platform(text);
                oursAccepts = true;
            } catch (Throwable t) {
                try {
                    Surfaces.engine(text);
                    oursAccepts = true;
                } catch (Throwable t2) {
                    oursAccepts = false;
                }
            }
            if (!oursAccepts) {
                stale.add(claim + " (our platform and strict surfaces"
                        + " refuse it too — nothing to pardon)");
            }
        }
        assertEquals(List.of(), stale,
                "stale version-skew claims — remove the rows from"
                        + " docs/version-skew-claims.tsv");
        assertEquals(SKEW_CLAIMS_TOTAL, SKEW_CLAIMS.size(),
                "skew-claims ledger size drifted — re-pin with row-by-row"
                        + " review");
    }

    // ------------------------------------------------------------------
    // The leniency classifier. NOTE (deep-audit H2 correction): for the
    // PLATFORM-surface population this method IS the gate — a non-null
    // return is the pass (unclassified==0 asserts below). The allowlist
    // TSVs gate only the docAccepts/strictAccepts population. Returning
    // a label here is therefore an ACCEPTANCE decision, not diagnostics.
    // ------------------------------------------------------------------

    /** Class a refusal by the ORACLE'S OWN evidence (message/exception),
     *  with one mechanical assist: a bare "Unexpected token" is adjudicated
     *  by OUR strict surface — its engine-verbatim gates name the dialect
     *  construct row by row (ZSkewResidueProbe), and a strict ACCEPT means
     *  the construct is checkout-unreleased grammar (true version skew).
     *  Returns null when nothing matches — the failing case. */
    /** The shrink-only skew-claims ledger (docs/version-skew-claims.tsv):
     *  corpus-source ids whose generic grammar refusals are CLAIMED as
     *  pinned-oracle-vs-checkout skew, pending row-by-row adjudication. */
    private static final java.util.Set<String> SKEW_CLAIMS = loadSkewClaims();

    private static java.util.Set<String> loadSkewClaims() {
        try {
            java.nio.file.Path f = Repo.path("docs",
                    "version-skew-claims.tsv");
            // one answer: the repository path (the cwd-relative second guess this had
            // only existed while tests ran from the module directory)
            java.util.Set<String> out = new java.util.HashSet<>();
            for (String line : java.nio.file.Files.readAllLines(f)) {
                int tab = line.indexOf('\t');
                out.add(tab < 0 ? line : line.substring(0, tab));
            }
            return out;
        } catch (java.io.IOException e) {
            throw new java.io.UncheckedIOException(e);
        }
    }

    private static final ThreadLocal<String> CLASSIFYING_ID = new ThreadLocal<>();

    /** F3.7b: pardon-ledger size pins — growth is a reviewed event. */
    private static final int REFUSAL_ALLOW_TOTAL = 8;
    private static final int MODEL_REFUSE_ALLOW_TOTAL = 4;   // 2026-08-16 F3.7b: 67 stale rows pruned (the ledger predated full section parity)

    /** F3.7: the ledger's exact size — a new claim is a reviewed event. */
    private static final int SKEW_CLAIMS_TOTAL = 25;

    private static boolean versionSkewClaim(Throwable ignored) {
        String id = CLASSIFYING_ID.get();
        return id != null && SKEW_CLAIMS.contains(id);
    }

    public static @com.legend.Nullable String classify(Throwable root, String text) {
        String msg = String.valueOf(root.getMessage());
        if ("Unexpected token".equals(msg.trim())) {
            try {
                Surfaces.engine(text);
                // OUR engine-dialect parser accepting is NOT evidence of
                // unreleased grammar — that's what an over-permissive
                // lite bug looks like (deep-audit #2 2c: this arm was
                // CIRCULAR). Skew is a CLAIM: each row needs a reviewed
                // line in version-skew-claims.tsv or it goes red.
                return versionSkewClaim(root) ? "VERSION-SKEW-claimed"
                        : null;
            } catch (Throwable strict) {
                String sm = String.valueOf(strict.getMessage());
                if (sm.contains("not authorized in Legend Engine")) {
                    return "PURE-DIALECT-generics";
                }
                if (sm.contains("is not supported yet")) {
                    return "PURE-DIALECT-function-types";
                }
                if (sm.contains(".allVersionsInRange")) {
                    return "DIALECT-milestoning-range";
                }
                if (sm.contains("Unsupported syntax")) {
                    return "DIALECT-native-or-m2";
                }
                if (sm.contains("copy-with-update")) {
                    // ^$x(...) — pure-dialect copy-new, adopted on the
                    // LITE surface, refused drop-in (2026-08-14 gate)
                    return "DIALECT-copy-new";
                }
                if (sm.contains("DOC_STRING")) {
                    // m3 triple-quote documentation literals — PLATFORM
                    // trivia (A5 burn 2026-08-15); engine refuses them
                    return "PURE-DIALECT-doc-string";
                }
                if (sm.contains("brace-less #TDS")) {
                    // platform_dsl_tds spelling — the engine's xt-tds
                    // accepts only #TDS{...}# (adjudication 2026-08-14)
                    return "PURE-DIALECT-tds-braceless";
                }
                if (sm.contains("Primitive type declarations")) {
                    // pure-m2 Primitive element — the engine wire parser
                    // CRASHES on it (raw InputMismatchException, null
                    // message; adjudication 2026-08-14)
                    return "PURE-DIALECT-primitive";
                }
                // an UNRECOGNIZED strict refusal is not skew — it must be
                // named (deep-audit H2: this arm was a pardon)
                return versionSkewClaim(root) ? "VERSION-SKEW-claimed" : null;
            }
        }
        // DIALECT-GAP — the engine names its own subset
        if (msg.contains("not authorized in Legend")) {
            return "PURE-DIALECT-generics";
        }
        // legend-pure's unit instance literal (5 RomanLength~Pes — the m3
        // unit tests, the engine's toJson tests): the engine's grammar
        // refuses it verbatim; the platform dialect reads it (census batch
        // 150, units). The ENGINE dialect refuses with the same message.
        if (msg.contains("Unit instance not supported")) {
            return "PURE-DIALECT-unit-instance";
        }
        if (msg.matches("(?s).*The type \\{.*}.* is not sup.*")) {
            return "PURE-DIALECT-function-types";
        }
        if (msg.contains(".allVersionsInRange(")
                && msg.contains("is not supported")) {
            return "DIALECT-milestoning-range";
        }
        if (msg.contains("Unsupported syntax")) {
            // verified: this engine message fires at native-function
            // declarations and m2 mapping forms (catalog DIALECT-GAP)
            return "DIALECT-native-or-m2";
        }
        // EXTENSION-GAP
        if (msg.contains("Can't find an embedded Pure parser")) {
            return "EXTENSION-island-parser";
        }
        if (msg.contains("is not a known section parser")) {
            // AuthenticationDemo: the section parser exists ONLY in the
            // engine's own test sources (no published jar) — a PRODUCTION
            // engine refuses these files exactly as our strict surface
            // does; pure mode skips the section as an unclaimed carrier
            return "ENGINE-TEST-SCOPED-section";
        }
        if (msg.contains("Unknown schema format")
                || msg.contains("Unknown embedded data type")
                || msg.contains("Unknown permission scheme")) {
            return "EXTENSION-format";
        }
        // ORACLE-DEFECT — a crash, not a refusal
        if (root instanceof NullPointerException
                || msg.contains("Cannot invoke")
                || msg.contains("NullPointerException")
                || msg.contains("please notify developer")
                // NumberFormatException escaping the oracle's unicode-escape
                // parser (TestProfile.java#52: For input string "sers"
                // under radix 16)
                || msg.contains("under radix")) {
            return "ORACLE-DEFECT-crash";
        }
        if ("null".equals(msg)) {
            // an InputMismatchException with a null message is ANTLR's
            // ORDINARY grammar-refusal path, NOT an oracle defect — the
            // audit found 346 rows laundered under the old label
            // (HARNESS_SIMPLIFICATION_PLAN Phase 7). Real crashes carry
            // messages and classify above.
            if (root.getClass().getSimpleName()
                    .equals("InputMismatchException")) {
                return "GRAMMAR-REFUSAL-nullmsg";
            }
            return "ORACLE-DEFECT-" + root.getClass().getSimpleName();
        }
        // VERSION-SKEW is a CLAIM, not a default: the old fall-through
        // pardoned every generic ANTLR refusal as skew (deep-audit H2 —
        // "essentially any grammar divergence on that surface is labelled
        // version skew"). A claim must be a ROW in
        // docs/version-skew-claims.tsv (shrink-only; each row is an
        // adjudication obligation per DEEP_AUDIT_HANDOFF Leg 1) — an
        // unclaimed generic refusal goes UNCLASSIFIED and the build reds.
        if (msg.startsWith("Unexpected token")
                || msg.contains("mismatched input")
                || msg.contains("extraneous input")
                || msg.contains("missing ") && msg.contains(" at ")) {
            return versionSkewClaim(root) ? "VERSION-SKEW-claimed" : null;
        }
        if (msg.contains("Field '") && msg.contains("' is required")) {
            // any REMAINING required-field refusal is a NEW genuinely-lite
            // leniency — those must be fixed, never cataloged
            return null;
        }
        return null;
    }

    // ------------------------------------------------------------------
    // helpers
    // ------------------------------------------------------------------

    private static Map<String, String> readAllowlist(String name)
            throws java.io.IOException {
        Map<String, String> out = new LinkedHashMap<>();
        for (String line : Files.readAllLines(
                Repo.path("docs", name))) {
            if (!line.startsWith("#") && !line.isBlank()) {
                String[] f = line.split("\t", 2);
                out.put(f[0], f.length > 1 ? f[1] : "");
            }
        }
        return out;
    }

    private interface ThrowingRunnable {
        void run() throws Throwable;
    }

    private static boolean accepts(ThrowingRunnable parse) {
        try {
            parse.run();
            return true;
        } catch (Throwable t) {
            return false;
        }
    }

    private static Throwable rootOf(Throwable t) {
        Throwable r = t;
        while (r.getCause() != null && r.getCause() != r) {
            r = r.getCause();
        }
        return r;
    }

    private static String msgOf(Throwable root) {
        String m = String.valueOf(root.getMessage())
                .replaceAll("\\s+", " ").trim();
        return m.length() > 160 ? m.substring(0, 160) : m;
    }

    private static String head(List<String> rows) {
        return String.join("\n  ",
                rows.subList(0, Math.min(10, rows.size())));
    }

    private static String firstDivergence(String expected, String actual) {
        int n = Math.min(expected.length(), actual.length());
        int i = 0;
        while (i < n && expected.charAt(i) == actual.charAt(i)) {
            i++;
        }
        return "byte " + i + " | expected …"
                + expected.substring(Math.max(0, i - 30),
                        Math.min(expected.length(), i + 50))
                + "… | actual …"
                + actual.substring(Math.max(0, i - 30),
                        Math.min(actual.length(), i + 50)) + "…";
    }

    /** Raw ANTLR syntax adjudication by legend-pure's OWN grammar — the
     *  second reference. Accept iff {@code definition()} consumes the
     *  source with zero syntax errors. */
    private static boolean m3Accepts(String text) {
        var errors = new ArrayList<String>();
        var listener = new BaseErrorListener() {
            @Override
            public void syntaxError(Recognizer<?, ?> recognizer,
                    Object offendingSymbol, int line, int charPositionInLine,
                    String msg, RecognitionException e) {
                errors.add(line + ":" + charPositionInLine + " " + msg);
            }
        };
        try {
            var lexer = new org.finos.legend.pure.m3.serialization.grammar
                    .m3parser.antlr.M3Lexer(CharStreams.fromString(text));
            lexer.removeErrorListeners();
            lexer.addErrorListener(listener);
            var parser = new org.finos.legend.pure.m3.serialization.grammar
                    .m3parser.antlr.M3Parser(new CommonTokenStream(lexer));
            parser.removeErrorListeners();
            parser.addErrorListener(listener);
            parser.definition();
            return errors.isEmpty();
        } catch (Throwable t) {
            return false;
        }
    }

    private static void writeReports(int sources, int oracleAccepts,
            int docsMatched, List<String> docDiffs, List<String> weRefuse,
            int bothReject, int seamMatched, List<String> engineAsym,
            List<String> seamDiffs, List<String> seamAccepts,
            List<String> seamRejects, int strictAsymmetry,
            List<String> strictAsymmetryIds, List<String> asymRows,
            StringBuilder catalog, Map<String, Integer> catalogByClass,
            List<String> stale) throws java.io.IOException {
        // equivalence-report.txt — the gate-log extraction reads lines 4-10
        StringBuilder eq = new StringBuilder();
        eq.append("CORPUS SWEEP — one pass, every claim (CorpusSweepTest)\n")
                .append("=".repeat(72)).append('\n')
                .append(String.format("corpus sources        : %d%n", sources))
                .append(String.format("oracle accepts        : %d%n",
                        oracleAccepts))
                .append(String.format("  docs byte-MATCH     : %d%n",
                        docsMatched))
                .append(String.format("  docs DIFF (BUG)     : %d%n",
                        docDiffs.size()))
                .append(String.format("  we-refuse (BUG)     : %d%n",
                        weRefuse.size()))
                .append(String.format("oracle rejects        : %d (both-reject %d)%n",
                        sources - oracleAccepts, bothReject));
        docDiffs.stream().limit(15).forEach(d ->
                eq.append("  DIFF ").append(d).append('\n'));
        try {
            java.nio.file.Files.write(Repo.out("we-refuse.tsv"), weRefuse);
        } catch (java.io.IOException ignored) {
            // diagnostic artifact only
        }
        Files.writeString(Repo.out("equivalence-report.txt"),
                eq.toString());

        StringBuilder seam = new StringBuilder();
        seam.append("SPI SEAM — engine+legend-lite vs vanilla engine, full PMCD\n")
                .append("=".repeat(72)).append('\n')
                .append(String.format("files byte-identical  : %d%n", seamMatched))
                .append(String.format("engine JSON-asymmetry : %d%n",
                        engineAsym.size()))
                .append(String.format("DIFF (BUG)            : %d%n",
                        seamDiffs.size()))
                .append(String.format("asymmetric rejects    : %d%n",
                        seamAccepts.size() + seamRejects.size()))
                .append(String.format("strict/oracle asym    : %d%n",
                        strictAsymmetry));
        engineAsym.stream().limit(20).forEach(d ->
                seam.append("  ENGINE-ASYM ").append(d).append('\n'));
        seamAccepts.stream().limit(400).forEach(d ->
                seam.append("  SPI-ACCEPTS ").append(d).append('\n'));
        Files.writeString(Repo.out("spi-seam-report.txt"),
                seam.toString());

        StringBuilder asym = new StringBuilder();
        asym.append("# REFUSAL ASYMMETRY — sources the ORACLE rejects that a lite surface accepts\n");
        asym.append("# stale allowlist rows (REMOVE): ").append(stale.size());
        stale.stream().limit(20).forEach(s2 ->
                asym.append("\n#   STALE ").append(s2));
        asym.append("\n# id\tcategory\taccepting-surfaces\toracle-message\n");
        asymRows.stream().sorted().forEach(r ->
                asym.append(r).append('\n'));
        Files.writeString(Repo.out("refusal-asymmetry.tsv"),
                asym.toString());

        Files.writeString(Repo.out("platform-coverage-catalog.txt"),
                "by class: " + catalogByClass + "\n" + catalog);
        Files.writeString(Repo.out("strict-oracle-asymmetry.txt"),
                String.join("\n", strictAsymmetryIds));
        if (!stale.isEmpty()) {
            System.out.println("refusal-allowlist STALE rows (fixed parity —"
                    + " REMOVE the lines): " + stale.size());
            stale.stream().limit(10).forEach(s2 ->
                    System.out.println("  STALE " + s2));
        }
    }
}
