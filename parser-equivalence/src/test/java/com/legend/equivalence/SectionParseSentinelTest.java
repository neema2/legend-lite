package com.legend.equivalence;

import com.legend.testing.Repo;
import com.legend.parser.ElementParser;
import org.finos.legend.engine.language.pure.grammar.from.PureGrammarParser;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * THE PULL SENTINEL. When the upstream checkouts move, new grammar spellings in the
 * sections our pipeline consumes (###Mapping/###Relational/###Connection/###Runtime —
 * the lexed sections beyond ###Pure, which byte-parity owns) can silently poison the
 * corpus gate: the 2026-08-04 pull introduced {@code ~src}-style relation mappings and
 * gate 4 collapsed to 2/2567 with nobody watching. This test converts that failure mode
 * into a named, immediate signal: every corpus file containing those sections goes through
 * the DROP-IN surface ({@code ElementParser.parseStrict} — the parser that ships, not the
 * deliberately broader internal dialect) AND through the reference, and the two verdicts
 * are compared.
 *
 * <p>The ratchet guards <b>agreement</b>, not throughput. It used to count "files that
 * parse", which quietly made leniency look like coverage and correctness look like
 * regression — refusing a file the engine ALSO refuses scored as a loss. Three buckets
 * now: MATCHED (both accept or both refuse), LENIENT (we accept, the engine refuses) and
 * DEFECT (the engine accepts, we refuse). MATCHED may not fall; LENIENT and DEFECT may
 * only fall.
 *
 * <p>This is parse BEHAVIOUR, not byte parity — no bytes are compared here. Failures are
 * reported by file with the parse error so a pull regression reads as "wall with a name".
 */
class SectionParseSentinelTest {

    /**
     * WHY we accepted a file the reference refused. legend-lite sits between
     * legend-pure and legend-engine, so some leniency is the project working as
     * intended — but "we are a superset" is a rationalisation magnet, so the
     * split is made by EVIDENCE, never by our own say-so. The rule:
     *
     * <p><b>A leniency is justified only if we can NAME the construct we accept
     * and we actually PARSED it. Accepting because we IGNORED something is a
     * bug wearing a superset's clothes.</b>
     *
     * <ul>
     *   <li>{@code JUSTIFIED-crash} — the engine did not refuse, it CRASHED
     *       (NPE, "please notify developer"). Reproducing a crash was never a
     *       compatibility property.</li>
     *   <li>{@code JUSTIFIED-engine-subsets-pure} — the engine deliberately
     *       refuses a construct that is legal Pure and says so ("not supported
     *       yet", "not authorized in Legend Engine"). These files are the
     *       engine's OWN platform sources, which legend-pure compiles in
     *       production; parsing them is the blend thesis, working.</li>
     *   <li>{@code UNJUSTIFIED-we-skipped-it} — the reference has no grammar
     *       registered for a section and refuses the file; we take it only
     *       because unknown {@code ###Section} headers are skipped in silence.
     *       We do not support that section — we cannot see it, and we would
     *       accept arbitrary nonsense inside it just as happily. This bucket
     *       is the audit's "reject unknown sections loudly", and it is also
     *       partly an artifact of which grammar jars the oracle loads.</li>
     *   <li>{@code UNJUSTIFIED-unclassified} — everything else. Unexamined
     *       leniency is not credited.</li>
     * </ul>
     */
    /** The shrink-only skew-claims ledger — SHARED with CorpusSweepTest's
     *  classifier (docs/version-skew-claims.tsv): a row here is an
     *  adjudicated post-4.138.2 construct, not an unjustified leniency. */
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

    private static String leniencyKind(Throwable referenceRefuses, String id) {
        if (SKEW_CLAIMS.contains(id)) {
            return "JUSTIFIED-skew-claim (named in version-skew-claims.tsv)";
        }
        return leniencyKind(referenceRefuses);
    }

    private static String leniencyKind(Throwable referenceRefuses) {
        Throwable root = referenceRefuses;
        while (root.getCause() != null && root.getCause() != root) {
            root = root.getCause();
        }
        String msg = String.valueOf(referenceRefuses.getMessage());
        if (root instanceof NullPointerException
                || root instanceof IndexOutOfBoundsException
                || root instanceof ClassCastException
                || msg.contains("please notify developer")) {
            return "JUSTIFIED-crash";
        }
        if (msg.contains("is not supported yet")
                || msg.contains("not authorized in Legend Engine")) {
            return "JUSTIFIED-engine-subsets-pure";
        }
        // ADJUDICATED 2026-08-13 (version-skew-claims.tsv construct pass):
        // 'Primitive X extends Y' is in the CHECKOUT's DomainParser and
        // legend-pure's M3CoreLexer — true skew vs the 4.138.2 reference;
        // lite parses it as pure dialect. The remaining bare-token rows in
        // the tests/mapping/relation + sql/dynamic + mft families are the
        // post-4.138.2 relation-mapping / #TDS-position constructs named
        // in the same ledger.
        if (msg.startsWith("Unexpected token 'Primitive'")) {
            return "JUSTIFIED-checkout-unreleased-grammar (primitive-type-extension)";
        }
        if (msg.contains("is not a known section parser")) {
            return "UNJUSTIFIED-we-skipped-it";
        }
        return "UNJUSTIFIED-unclassified";
    }

    /** The lexed sections beyond Pure (Lexer.LEXABLE_SECTIONS minus Pure). */
    private static final Pattern SENTINEL_SECTIONS =
            Pattern.compile("(?m)^###(Mapping|Relational|Connection|Runtime)\\b");

    @Test
    void everyMappingRelationalSectionFileStillParses() throws Exception {
        List<Corpus.Source> sources = Corpus.all();
        Assumptions.assumeTrue(!sources.isEmpty(),
                "no corpus on disk — set -Dlegend.engine.root / -Dlegend.pure.root");

        int inScope = 0;
        int parsed = 0;
        int defects = 0;
        int legalRefusals = 0;
        int matched = 0;
        int lenient = 0;
        int unjustifiedLeniency = 0;
        PureGrammarParser reference = PureGrammarParser.newInstance();
        Map<String, Integer> lenientByKind = new TreeMap<>();
        List<String> lenientFiles = new ArrayList<>();
        List<String> failures = new ArrayList<>();
        Map<String, Integer> byMessage = new TreeMap<>();
        for (Corpus.Source src : sources) {
            Matcher m = SENTINEL_SECTIONS.matcher(src.text());
            if (!m.find()) {
                continue;
            }
            inScope++;
            try {
                // THE PARSER THAT SHIPS (audit fix #2). Measuring the internal
                // dialect against the engine measures the wrong parser:
                // parse() is DELIBERATELY broader (it must load real models
                // whose ###Service/###DataSpace sections legend-lite does not
                // implement). parseLegendEngine() is the drop-in surface, and it is
                // the only one whose accept/reject has to match.
                Surfaces.engine(src.text());
                parsed++;
                // ACCEPTING is only right when the reference accepts too. A file
                // the reference REFUSES and we take is leniency — the divergence
                // a raw parse count cannot see, and the one that made this gate
                // punish correctness (rejecting legend-pure's ###Diagram dialect
                // the way the engine does used to read here as a REGRESSION).
                try {
                    reference.parseModel(src.text());
                    matched++;
                } catch (Throwable referenceRefuses) {
                    lenient++;
                    String why = leniencyKind(referenceRefuses, src.id());
                    if (why.startsWith("UNJUSTIFIED")) {
                        unjustifiedLeniency++;
                    }
                    lenientByKind.merge(why, 1, Integer::sum);
                    lenientFiles.add(why + "  " + src.id() + " :: "
                            + String.valueOf(referenceRefuses.getMessage())
                                    .replaceAll("\\s+", " "));
                }
            } catch (Throwable t) {
                // THE ORACLE (implementation audit §3.4): a failure only counts
                // as a drop-in DEFECT when the reference parser ACCEPTS the
                // file — a file the reference also rejects is a legal refusal,
                // not a gap. Without this split the ratchet cannot tell a
                // fixed defect from an upstream file that got less legal.
                String kind;
                try {
                    reference.parseModel(src.text());
                    defects++;
                    kind = "DEFECT";
                } catch (Throwable alsoRejected) {
                    legalRefusals++;
                    matched++;                  // both refuse — matching behaviour
                    kind = "LEGAL-REFUSAL";
                }
                String msg = String.valueOf(t.getMessage()).replaceAll("\\s+", " ");
                msg = msg.substring(0, Math.min(120, msg.length()));
                failures.add(kind + " " + src.id() + " :: " + msg);
                byMessage.merge(kind + " :: " + msg, 1, Integer::sum);
            }
        }

        StringBuilder report = new StringBuilder();
        report.append("SECTION PARSE SENTINEL — Mapping/Relational/Connection/Runtime\n")
                .append("=".repeat(72)).append('\n')
                .append(String.format("files in scope        : %d%n", inScope))
                .append(String.format("parse cleanly         : %d%n", parsed))
                .append(String.format("parse failures        : %d%n", failures.size()))
                .append(String.format("  reference ACCEPTS   : %d (drop-in DEFECTS)%n", defects))
                .append(String.format("  reference rejects   : %d (legal refusals)%n",
                        legalRefusals))
                .append(String.format("%nBEHAVIOUR vs the reference (what the ratchet"
                        + " actually guards)%n"))
                .append(String.format("  MATCHED             : %d"
                        + " (both accept, or both refuse)%n", matched))
                .append(String.format("  LENIENT             : %d"
                        + " (we accept, reference REFUSES)%n", lenient))
                .append(String.format("  DEFECT              : %d"
                        + " (reference accepts, we refuse)%n", defects));
        if (!lenientFiles.isEmpty()) {
            report.append("\nLENIENT by JUSTIFICATION — see leniencyKind(): a superset"
                            + " claim needs a NAMED construct we actually parsed\n")
                    .append("-".repeat(72)).append('\n');
            lenientByKind.entrySet().stream()
                    .sorted((x, y) -> y.getValue() - x.getValue())
                    .forEach(e -> report.append(String.format("  %5d  %s%n",
                            e.getValue(), e.getKey())));
            report.append("\nLENIENT — files we take that the engine will not\n")
                    .append("-".repeat(72)).append('\n');
            // NOT truncated: this is the bucket MAX_LENIENT guards, and a
            // capped list makes a +2 growth impossible to diagnose — you
            // cannot diff two truncated lists, though you can waste time
            // trying (2026-08-08). It is bounded by the ratchet anyway.
            lenientFiles.stream().forEach(f ->
                    report.append("  ").append(f).append('\n'));
        }
        report.append("\nFAILURES by message (a NEW message after a pull = the drift)\n")
                .append("-".repeat(72)).append('\n');
        byMessage.entrySet().stream().sorted((x, y) -> y.getValue() - x.getValue())
                .limit(20)
                .forEach(e -> report.append(String.format("  %5d  %s%n",
                        e.getValue(), e.getKey())));
        report.append('\n');
        // NOT truncated, same reason the LENIENT list is not: DEFECT is the
        // "engine accepts, we refuse" bucket — the coverage debt — and a
        // capped list means you can quote 198 without ever seeing 198.
        failures.forEach(f -> report.append("  ").append(f).append('\n'));
        Files.writeString(Repo.out("section-sentinel-report.txt"),
                report.toString());
        System.out.println(report);

        assertTrue(inScope > 0, "sentinel matched no files: the corpus shape changed");
        assertTrue(matched >= MIN_BEHAVIOUR_MATCHED,
                "files whose accept/reject MATCHES the engine DROPPED: " + matched
                        + " < " + MIN_BEHAVIOUR_MATCHED + " — an upstream pull likely"
                        + " introduced new grammar; see"
                        + " target/section-sentinel-report.txt before running the"
                        + " corpus gates.");
        assertTrue(lenient <= MAX_LENIENT,
                "files we accept that the engine REFUSES grew: " + lenient + " > "
                        + MAX_LENIENT + " — a drop-in that takes what the engine"
                        + " rejects is not a drop-in; see"
                        + " target/section-sentinel-report.txt");
        assertTrue(unjustifiedLeniency <= MAX_UNJUSTIFIED_LENIENCY,
                "leniency we cannot justify grew: " + unjustifiedLeniency + " > "
                        + MAX_UNJUSTIFIED_LENIENCY + " — accepting a file because we"
                        + " SKIPPED what we could not read is a bug, not a superset;"
                        + " see the LENIENT-by-justification table in"
                        + " target/section-sentinel-report.txt");
        assertTrue(defects <= MAX_DROP_IN_DEFECTS,
                "reference-accepted files we fail to parse GREW: " + defects + " > "
                        + MAX_DROP_IN_DEFECTS + " — a real drop-in gap opened; see"
                        + " target/section-sentinel-report.txt");
    }

    /** Failures on files the reference ACCEPTS — the honest defect count the
     *  coverage ratchet above cannot see (implementation audit §3.4). Ratcheted
     *  DOWN only; section parity burns it to zero. */
    // 266 -> 184 (2026-08-09). ~src is OPTIONAL in Legend and we required it
    // on every Pure class mapping, which cost 17 files; making it optional
    // closed 14 outright and moved 3 on to their NEXT gap (unbuilt sections),
    // which is why the total falls by 14 rather than 17. The ceiling had been
    // sitting 82 above the actual count, so it could not have caught a
    // regression of any size worth catching.
    // 2026-08-14: ZERO. The last two (~src inline-expression Relation
    // mappings) landed when the model carried the expression as the
    // pipeline itself (engine #4941).
    private static final int MAX_DROP_IN_DEFECTS = 0;
    // 126 -> 127 is the ONE increment this ratchet has ever taken, and it is a
    // gap becoming VISIBLE rather than a capability being lost: refusing
    // unknown sections turned a silently-skipped ###Diagram that the engine
    // ACCEPTS (TestDiagramCompilationFromGrammar.java#0) from invisible
    // leniency into a named defect. The diagram leg takes it back down.

    /**
     * Files whose ACCEPT/REJECT decision matches the engine's — the property a
     * drop-in actually has to hold, and the pull-drift signal this test exists
     * for. It replaced a raw "files that parse" count, which silently rewarded
     * LENIENCY: refusing a file the engine also refuses scored as a coverage
     * regression, so every fix in the leniency programme (742 cases queued)
     * would have had to fight its own gate. Bump when matching grows; a drop
     * means a pull moved the grammar under us.
     */
    private static final int MIN_BEHAVIOUR_MATCHED = 2093;   // ratcheted to measured 2026-08-14 (deep audit #2 §2e)
    // 932 -> 793 is the ORACLE getting honest, not the parser getting worse.
    // With three grammar jars the reference could not read ###Service /
    // ###Persistence / ###DataSpace either, so 139 files where we fail scored
    // as "both refuse" — MATCHED. Loading all 33 grammars moved them to their
    // true bucket, DEFECT. The code did not change; the mirror did.

    /** Files we accept that the engine REFUSES. Ratcheted DOWN only — this is
     *  the leniency surface, and a drop-in's is zero. */
    private static final int MAX_LENIENT = 17;   // measured 2026-08-12 post burn-down
    // 55 -> 68 (2026-08-09, ###Service SectionGrammar). Two effects, both
    // NAMED:
    // (1) NEW rows are oracle-version-skew: the five service-new-grammar
    //     *.pure files plus the Java-snippet services with suite doc
    //     strings (testSuite_1 'Happy path') and parameterised asserts use
    //     grammar the then-ORACLE (5.88.1, later 5.92.0) refused while the
    //     then-checkout (5.92.1-SNAPSHOT, 4.137.0+36) accepted (its own
    //     build compiled these fixtures). Since 2026-09-10 the oracle jars
    //     and the checkout are ONE release (tools/oracle-pins.env), so a
    //     remaining row here is a lite bug, not skew; the corpus decides
    //     (the xStore rule), and this ratchets back DOWN as rows match.
    // (2) UNMASKED rows: files whose ###Service section we previously
    //     refused (masking everything after it) now parse to the content
    //     BEHIND the service — mostly the same skew category. The two
    //     unmasked leniencies that were genuinely OURS were fixed instead
    //     of absorbed: join types validate against engine's exact
    //     {INNER, OUTER} set at EVERY parse site
    //     (TokenStreamCursor.validateJoinType), and legacy mapping-test
    //     input formats validate against {JSON, XML} / {SQL, CSV}.
    // 68 -> 69 (2026-08-09, burn-to-zero endgame): one more legend-pure
    // relational-grammar fixture in the SAME extension-less-oracle family
    // reached once the mapping/persistence tails cleared.
    // 57 -> 55 (2026-08-08, ###Mapping protocol switch). The switch first
    // pushed this to 59: the protocol parser read a Pure property mapping's
    // RHS with `parseCodeBlock`, so a stray `;` passed as a statement
    // separator where engine's rule is `... COLON combinedExpression` with
    // COMMA between mappings and no terminator at all. The legacy parser had
    // refused it ("trailing tokens after expression") and deleting that
    // parser removed the refusal — a leniency INHERITED by the migration, not
    // authored by it, and invisible to every corpus sweep because the walls
    // it hides behind are in files engine also rejects. Refusing the `;` at
    // the scan took 59 -> 55, below where main stood.
    //
    // (historical) 55 -> 57, and that increment was NAMED rather than absorbed. Teaching the
    // runner parser `~primaryKey: [COL]` in a Relation class mapping made three
    // of legend-engine's OWN relation-emit-models parse — and the pinned 4.133.0
    // oracle refuses them, because the corpus is 4.137.0-36 and the syntax is
    // newer than the jar. We read what a current engine reads; the old oracle
    // cannot. It resolves when the oracle is version-matched (audit fix #6), not
    // by changing the parser:
    //   relation-emit-models/relation-embedded/mapping/embeddedMapping.pure
    //   relation-emit-models/relation-enumeration/mapping/enumMapping.pure
    //   relation-emit-models/relation-expression-rhs/mapping/exprMapping.pure

    /** Leniency we CANNOT justify — files we take only because we skipped what
     *  we could not read, plus anything unexamined. Ratcheted DOWN only; this
     *  is the half of {@link #MAX_LENIENT} that is simply a bug. */
    private static final int MAX_UNJUSTIFIED_LENIENCY = 0;    // 127 -> 39 -> 51 -> 52 -> 5 -> 0 (claims-ledger adjudication 2026-08-13)
    // +2 is the same version-skew trio above: unexamined is not the same as
    // innocent, so they stay in the unjustified column until a version-matched
    // oracle can adjudicate them.
    // 39 -> 51 (2026-08-09): the ###Service-claim rows (see MAX_LENIENT
    // above) land here because the classifier has no reliable message
    // signal for oracle version skew; they sit in the unjustified column
    // until the oracle is version-matched.
}
