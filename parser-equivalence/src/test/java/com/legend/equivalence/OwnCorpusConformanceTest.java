package com.legend.equivalence;

import org.finos.legend.engine.language.pure.grammar.from.PureGrammarParser;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * THE MIRROR CORPUS (deep audit): the same Java-literal extractor that
 * harvests Pure from the ENGINE'S tests, run over OUR OWN test sources —
 * every Pure snippet legend-lite's core/pct/nlq tests embed goes to
 * the REAL engine oracle. A refusal must classify exactly like a corpus
 * leniency row (the dialect constructs our tests deliberately exercise);
 * an UNCLASSIFIED refusal means our own test surface bakes in grammar
 * that is neither engine nor named pure-dialect — a lite-only invention,
 * and a build failure here.
 */
class OwnCorpusConformanceTest {

    static List<Corpus.Source> ownSnippets() {
        Path repo = Path.of(System.getProperty("user.dir")).getParent();
        List<Corpus.Source> ours = new ArrayList<>();
        for (String module : new String[]{"core", "spec", "pct", "nlq"}) {
            ours.addAll(InlineSnippets.extract(repo.resolve(module),
                    "lite-" + module, InlineSnippets.OWN_DECL));
        }
        return ours;
    }

    /**
     * SECTIONS-NORMALIZATION RATCHET: no test snippet declares a non-Pure
     * element (Database/Mapping/Runtime/Connection/Service/...) without its
     * {@code ###Section} header. The check is TOTAL (every extracted
     * snippet, oracle not consulted) and mechanical: a non-empty
     * {@link Sectionize} plan IS the violation — the engine binds element
     * kinds to sections, so a headerless one is a lite-only convenience
     * the strict flip would have to refuse. Normalized 2026-08-11 by
     * {@link ZSectionNormalizeRewrite} (579 snippets, 1170 headers, two
     * passes: PURE_DECL then the widened OWN_DECL population) plus hand
     * edits for the formatted/generated models (union probes, stress
     * generators); this pin keeps the population at ZERO.
     */
    @Test
    void noSectionlessSnippets() {
        List<String> violations = new ArrayList<>();
        for (Corpus.Source src : ownSnippets()) {
            List<Sectionize.Insertion> plan = Sectionize.plan(src.text());
            if (plan != null && !plan.isEmpty()) {
                violations.add(src.id() + " :: needs "
                        + plan.stream().map(Sectionize.Insertion::section)
                                .toList());
            }
        }
        assertTrue(violations.isEmpty(),
                () -> "SECTIONLESS test snippets (write the ###Section"
                        + " headers — ZSectionNormalizeRewrite mechanizes"
                        + " it):\n  " + String.join("\n  ",
                                violations.subList(0,
                                        Math.min(25, violations.size()))));
    }

    /**
     * DELIBERATE lite design surfaces our own tests exercise — the short,
     * explicit exceptions list (never grown casually; every entry names
     * the design that wants it). The engine's own message is the evidence
     * the construct was hit.
     */
    private static @com.legend.Nullable String liteDesign(Throwable root,
            String text) {
        String msg = String.valueOf(root.getMessage());
        if (msg.contains("No parser for AssociationMapping")) {
            // clean-sheet inline association predicate:
            //   Assoc: AssociationMapping { {p, f | ...} }
            // (MAPPING_CLEAN_SHEET; MappingNormalizerTest M5) — engine has
            // no such mapping-element parser, by design on our side
            return "LITE-DESIGN-inline-association";
        }
        if (msg.contains("Unknown database type 'SQLite'")) {
            // the SQLite execution backend (BACKEND_PORTABILITY) — a lite
            // connection type the engine's DatabaseType enum lacks
            return "LITE-DESIGN-sqlite-backend";
        }
        // mappings-as-functions (the standing design exception): a
        // CLEAN-SHEET mapping body parses to MappingDefinition (function
        // form) — the engine's grammar has no arm for it. Also covers the
        // function-REF service query (query: my::fn; not query: |...).
        try {
            for (var e : Surfaces.platform(text)
                    .elements()) {
                if (e instanceof com.legend.model.CleanSheetMappingDefinition) {
                    return "LITE-DESIGN-mapping-as-function";
                }
            }
        } catch (Throwable ignored) {
            // lenient parse failed — not a design row
        }
        if (text.matches("(?s).*query:\\s*[\\w$]+(::[\\w$]+)+\\s*;.*")) {
            return "LITE-DESIGN-mapping-as-function";
        }
        // legend-pure m2 diagram spelling (geometry attrs) — pure-dialect,
        // not engine grammar (DiagramAntlrParser.g4 widthFirst/heightFirst)
        if (text.contains("Diagram ") && text.contains("(width=")) {
            return "PURE-DIALECT-diagram";
        }
        // the RELATION-CONTRACT signature dialect, ported verbatim from
        // legend-pure (.pure relation contracts; see
        // verify-signatures-against-real-legend-pure): column-set algebra
        // (T+R / T-Z+V), subset constraints (⊆), spec column forms — plus
        // `native function` declarations, pure-dialect by definition
        if (text.contains("native function") || text.contains("⊆")
                || text.matches("(?s).*Relation<\\w+[+-].*")
                || text.contains("AggColSpec<{")
                || text.contains("FuncColSpec<{")) {
            return "PURE-DIALECT-signatures";
        }
        // the section-registry machinery fixture: a deliberately UNKNOWN
        // element kind ('Whatever my::D;') proving foreign-section routing
        if (text.contains("Whatever my::")) {
            return "TEST-MACHINERY-fixture";
        }
        // XStore missing-comma tolerance: CORPUS-PROVEN pure-dialect —
        // legend-pure's compiler completes the rule and drops the rest
        // (testMappingCrossStore.pure:238); the fixture pins exactly that
        // (see ElementParserTest's XStore comment: the engine parser
        // rejects, the pure compiler tolerates, the golden encodes it)
        if (text.contains(": XStore {")) {
            return "PURE-DIALECT-xstore-tolerance";
        }
        return null;
    }

    /** A required-field row is acceptable ONLY as a deliberate
     *  lenient-tier fixture: our STRICT surface must refuse it with the
     *  same required-field story (ElementParserTest pins the lenient
     *  default; the drop-in surface stays engine-parity). Strict
     *  accepting it would be a genuine leniency — unclassified. */
    private static @com.legend.Nullable String lenientTierFixture(
            Throwable root, String text) {
        String msg = String.valueOf(root.getMessage());
        if (!(msg.contains("Field '") && msg.contains("' is required"))) {
            return null;
        }
        try {
            Surfaces.engine(text);
        } catch (Throwable strict) {
            if (String.valueOf(strict.getMessage()).contains("is required")) {
                return "LENIENT-TIER-fixture";
            }
        }
        return null;
    }

    @Test
    void ourOwnTestPureIsRealLegend() throws Exception {
        PureGrammarParser oracle = PureGrammarParser.newInstance();
        List<Corpus.Source> ours = ownSnippets();
        Map<String, Integer> byClass = new TreeMap<>();
        List<String> unclassified = new ArrayList<>();
        int accepted = 0;
        int bothRefuse = 0;
        StringBuilder report = new StringBuilder();
        for (Corpus.Source src : ours) {
            Throwable refusal;
            try {
                oracle.parseModel(src.text());
                accepted++;
                continue;
            } catch (Throwable t) {
                refusal = t;
            }
            try {
                Surfaces.platform(src.text());
            } catch (Throwable oursToo) {
                bothRefuse++;
                continue;       // we refuse it too — a negative fixture
            }
            Throwable root = refusal;
            while (root.getCause() != null && root.getCause() != root) {
                root = root.getCause();
            }
            String cls = liteDesign(root, src.text());
            if (cls == null) {
                cls = lenientTierFixture(root, src.text());
            }
            if (cls == null) {
                cls = CorpusSweepTest.classify(root, src.text());
            }
            String msg = String.valueOf(root.getMessage())
                    .replaceAll("\\s+", " ");
            if (cls == null) {
                unclassified.add(src.id() + " :: " + msg);
                cls = "UNCLASSIFIED";
            }
            byClass.merge(cls, 1, Integer::sum);
            report.append(cls).append('\t').append(src.id()).append('\t')
                    .append(msg, 0, Math.min(160, msg.length()))
                    .append('\n');
        }
        java.nio.file.Files.createDirectories(Path.of("target"));
        java.nio.file.Files.writeString(
                Path.of("target", "own-corpus-conformance.txt"),
                report.toString());
        System.out.println("own-corpus: " + ours.size() + " snippets, "
                + accepted + " oracle-accepted, " + bothRefuse
                + " both-refuse, refusal classes: " + byClass);
        // THE RATCHET (invention census COMPLETE, 2026-08-11): 758/949
        // oracle-accepted (OWN_DECL population), 75 classified leniency
        // rows — and ZERO unresolved: every class below is a NAMED,
        // deliberate classification (pure-dialect constructs our tests
        // exercise, the explicit lite design surfaces, and two named
        // fixtures). The invention families are DEAD on both surfaces:
        // specific imports, mid-file import groups (ImportScope is
        // wildcards-only) and mapping-list trailing commas (entryComma —
        // both references spell lists `x (COMMA x)*`). Every class is
        // PINNED — a new row in any class (or a new class) fails the
        // build, so own-corpus leniency can only shrink.
        // 74 rows after the 2026-08-11 decision review
        // (docs/OWN_CORPUS_DECISIONS.md): the LENIENT-TIER service
        // pattern-default was CONFORMED away (required on both surfaces);
        // every remaining class is DECIDED-KEEP or proposed-KEEP there.
        Map<String, Integer> pins = new TreeMap<>(Map.ofEntries(
                // 13 -> 15 (2026-08-21, REVIEWED): the D4 variance pins
                // (VarianceD4Test, extension host declared in
                // OwnDialectCensusTest) — the contravariance spec cannot
                // be written without Function<{...}> parameter signatures
                // 15 -> 16 (2026-08-28, REVIEWED): charter §4V lambda-
                // classifier witness — the negative case (Function<{...}>-
                // typed variable must NOT conform to FunctionDefinition<Any>)
                // requires a function-type parameter spelling
                // 16 -> 18 (2026-08-28, REVIEWED): §4W audit fix-slice —
                // the two verbatim-signature NEGATIVES (router execute,
                // concatenateTemporalTdsQueries) spell Function<{...}>
                // parameters to witness the nominal gate's reject direction
                Map.entry("PURE-DIALECT-function-types", 18),
                // 7 -> 6 (batch 115, 2026-09-06): one generics row rode a
                // test file deleted with the old corpus harness
                Map.entry("PURE-DIALECT-generics", 6),
                Map.entry("DIALECT-milestoning-range", 1),
                Map.entry("ENGINE-TEST-SCOPED-section", 1),
                Map.entry("LITE-DESIGN-inline-association", 2),
                Map.entry("LITE-DESIGN-mapping-as-function", 20),
                Map.entry("LITE-DESIGN-sqlite-backend", 2),
                Map.entry("PURE-DIALECT-diagram", 1),
                Map.entry("PURE-DIALECT-signatures", 16),
                Map.entry("PURE-DIALECT-xstore-tolerance", 1),
                Map.entry("TEST-MACHINERY-fixture", 1)));
        // F3.7: EXACT accounting, both directions (the FixtureCorpusParity
        // shape) — overflow-only let a class shrink or vanish while its
        // pin sat stale, leaving silent headroom for new rows
        org.junit.jupiter.api.Assertions.assertEquals(pins,
                new TreeMap<>(byClass),
                "own-corpus leniency ledger drifted — GROWTH means conform"
                        + " the new test text to the oracle (or re-pin with"
                        + " review); SHRINKAGE means the pin is stale —"
                        + " ratchet it down in the same commit");
        assertTrue(unclassified.isEmpty(),
                () -> "OUR OWN test Pure contains grammar that is neither"
                        + " engine nor named pure-dialect (lite-only"
                        + " inventions — fix the tests or the parser):\n  "
                        + String.join("\n  ", unclassified.subList(0,
                                Math.min(25, unclassified.size()))));
    }
}
