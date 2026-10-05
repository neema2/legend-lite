// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.equivalence;

import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * THE OWN-TEST DIALECT CENSUS (provenance routing, user directive
 * 2026-08-12): lite's OWN corpus should compile at LEGEND_LITE — exact
 * engine plus the DECLARED extensions and nothing more. This census
 * measures the distance: every own-corpus snippet the PLATFORM surface
 * accepts is parsed again at LEGEND_LITE; a refusal means either an
 * UNDECLARED extension (declare it in OWN_CORPUS_DECISIONS or burn it)
 * or platform machinery masquerading as a user test (relocate it).
 *
 * <p>ASSERTED (the user's quarantined-set design, 2026-08-12): LITE-
 * refused rows must live in PLATFORM parser tests; LITE-accepts/ENGINE-
 * refuses rows must live in the MARKED extension-test hosts. Reports to
 * {@code target/own-dialect-census.tsv}.
 */
class OwnDialectCensusTest {

    /** Tests OF the platform parser itself — their snippets exercise
     *  platform dialect BY PURPOSE (they test parseLegendPlatform); the
     *  census excuses rows hosted here. Shrink-only. */
    private static final java.util.Set<String> PLATFORM_TEST_HOSTS =
            java.util.Set.of("ElementParserTest.java", "LexerTest.java",
                    "SectionGrammarRegistryTest.java",
                    "TdsLambdaProbeTest.java", "ProbeWireShapes.java",
                    "TypeCheckerTest.java",
                    "PlatformInliningTest.java", "CompileFunctionTest.java",
                    // NEGATIVE differential fixtures — rows built to
                    // REFUSE on purpose (verdict/byte/message parity
                    // gates); not own-dialect code
                    "AdversarialParityTest.java", "MessageParityTest.java");

    /** THE MARKED EXTENSION-TEST SET (user directive 2026-08-12): the
     *  only hosts allowed to carry snippets that pass at LEGEND_LITE and
     *  fail at LEGEND_ENGINE — i.e. tests of the DECLARED extensions.
     *  Shrink-only; a new host here is a reviewed declaration that a
     *  test exercises extension grammar. */
    private static final java.util.Set<String> EXTENSION_TEST_HOSTS =
            java.util.Set.of(
                    // LITE-DESIGN-function-types-generics (§11)
                    "UserFunctionIntegrationTest.java",
                    "UserCallInlinerTest.java", "CompileFunctionTest.java",
                    "TypeCheckerTest.java",
                    // LITE-DESIGN mapping-as-function / inline-association
                    // / sqlite (§6, §8, §9) pin hosts — json-column-get
                    // (§7) RETIRED 2026-08-12: respelled to the engine's
                    // extractFromSemiStructured (probe json-get-spelling)
                    "ElementParserTest.java", "MappingNormalizerTest.java",
                    "LegacyCleanSheetConvergenceTest.java",
                    // ModelNormalizerTest REMOVED 2026-08-16 (F3.7): it
                    // hosted zero extension rows — a stale declaration
                    "PureModelContextTest.java",
                    "CleanSheetProtocolShapeTest.java",
                    "SQLiteIntegrationTest.java",
                    // D4 variance pins (2026-08-21): the contravariance
                    // spec REQUIRES Function<{...}> parameter signatures
                    "VarianceD4Test.java",
                    // legacy routes as composition, step 1 (2026-09-13,
                    // REVIEWED): the hand-written function-form mapping
                    // whose Firm navigates a two-set Person through one
                    // several-route legacyNavigate — mapping-as-function
                    // grammar by design (docs/LEGACY_ROUTES_AS_COMPOSITION)
                    "RoutedNavigateTest.java");


    private static String hostOf(String id) {
        String f = id.substring(id.lastIndexOf('/') + 1);
        int cut = f.indexOf('#');
        return cut < 0 ? f : f.substring(0, cut);
    }

    /** The own corpus at legend-lite: the platform-accepted snippets LEGEND_LITE refuses (rows) and those it accepts
     *  that legend-engine refuses (extension rows), and how many each host carries (PeRatchets writes those counts
     *  into ratchets.tsv; Bazel workplan P3-30). */
    record Census(List<String> rows, List<String> extensionRows, Map<String, Integer> byMsg, int platformAccepts,
            int liteAccepts, int engineAccepts, Map<String, Integer> platformHosted,
            Map<String, Integer> extensionHosted) {
    }

    static Census measure() {
        List<Corpus.Source> own = new ArrayList<>();
        for (String module : List.of("core", "parser-equivalence", "pct")) {
            own.addAll(InlineSnippets.extract(module,
                    "lite-" + module, InlineSnippets.OWN_DECL));
        }
        org.junit.jupiter.api.Assertions.assertTrue(!own.isEmpty(), "no own corpus found");

        int platformAccepts = 0;
        int liteAccepts = 0;
        int engineAccepts = 0;
        List<String> rows = new ArrayList<>();
        List<String> extensionRows = new ArrayList<>();
        Map<String, Integer> byMsg = new TreeMap<>();
        for (Corpus.Source s : own) {
            boolean platform;
            try {
                Surfaces.platform(s.text());
                platform = true;
                platformAccepts++;
            } catch (Throwable t) {
                platform = false;
            }
            if (!platform) {
                continue;               // not even platform-parseable — not this census's row
            }
            boolean engine = accepts(() -> Surfaces.engine(s.text()));
            if (engine) {
                engineAccepts++;
            }
            boolean lite = false;
            try {
                Surfaces.lite(s.text());
                liteAccepts++;
                lite = true;
            } catch (Throwable t) {
                Throwable r = t;
                while (r.getCause() != null && r.getCause() != r) {
                    r = r.getCause();
                }
                String m = String.valueOf(r.getMessage())
                        .replaceAll("\\s+", " ").trim();
                String key = m.length() > 70 ? m.substring(0, 70) : m;
                byMsg.merge(key, 1, Integer::sum);
                rows.add(s.id() + "\t" + (m.length() > 160
                        ? m.substring(0, 160) : m));
            }
            if (lite && !engine) {
                extensionRows.add(s.id());
            }
        }
        Map<String, Integer> platformHosted = new TreeMap<>();
        for (String r : rows) {
            platformHosted.merge(
                    hostOf(r.substring(0, r.indexOf('\t'))), 1, Integer::sum);
        }
        Map<String, Integer> extensionHosted = new TreeMap<>();
        for (String id : extensionRows) {
            extensionHosted.merge(hostOf(id), 1, Integer::sum);
        }
        return new Census(rows, extensionRows, byMsg, platformAccepts, liteAccepts, engineAccepts, platformHosted,
                extensionHosted);
    }

    /** ratchets.tsv's key prefixes for the per-host counts. */
    static final String PLATFORM_KEY = "own_dialect.platform.";
    static final String EXTENSION_KEY = "own_dialect.extension.";

    @Test
    void ownCorpusAtLegendLite() throws Exception {
        Census c = measure();
        List<String> rows = new ArrayList<>(c.rows());
        List<String> extensionRows = c.extensionRows();
        Map<String, Integer> byMsg = c.byMsg();
        int platformAccepts = c.platformAccepts();
        int liteAccepts = c.liteAccepts();
        int engineAccepts = c.engineAccepts();
        // THE INVARIANTS (quarantined, marked, ratcheted):
        // (1) a LITE-refused row must be hosted in a PLATFORM parser test
        List<String> unquarantined = rows.stream()
                .filter(r -> !PLATFORM_TEST_HOSTS.contains(
                        hostOf(r.substring(0, r.indexOf('\t')))))
                .toList();
        // (2) a LITE-accepts/ENGINE-refuses row must be hosted in a
        // MARKED extension test (or a platform parser test)
        List<String> unmarkedExtension = extensionRows.stream()
                .filter(id -> !EXTENSION_TEST_HOSTS.contains(hostOf(id))
                        && !PLATFORM_TEST_HOSTS.contains(hostOf(id)))
                .toList();

        StringBuilder b = new StringBuilder();
        b.append("# OWN-TEST DIALECT CENSUS — platform-accepted own snippets that LEGEND_LITE refuses\n");
        b.append("# platform-accepts ").append(platformAccepts)
                .append(" | LEGEND_LITE-accepts ").append(liteAccepts)
                .append(" | LEGEND_ENGINE-accepts ").append(engineAccepts)
                .append(" | census ").append(rows.size()).append('\n');
        byMsg.forEach((k, v) -> b.append("#   ").append(v).append("x  ")
                .append(k).append('\n'));
        b.append("# id\trefusal\n");
        rows.sort(String::compareTo);
        rows.forEach(r -> b.append(r).append('\n'));
        Files.writeString(com.legend.testing.TestOutputs.file("own-dialect-census.tsv"),
                b.toString());
        System.out.println("own-dialect census: " + platformAccepts
                + " platform-accepted, " + liteAccepts + " LITE-accepted, "
                + engineAccepts + " ENGINE-accepted, " + rows.size()
                + " census rows — " + byMsg);
        assertTrue(platformAccepts > 0, "the own corpus did not load");
        assertTrue(unquarantined.isEmpty(),
                () -> "own snippets REFUSED at LEGEND_LITE outside the"
                        + " platform parser tests — an undeclared extension"
                        + " or misplaced platform machinery:\n  "
                        + String.join("\n  ", unquarantined.subList(0,
                                Math.min(10, unquarantined.size()))));
        assertTrue(unmarkedExtension.isEmpty(),
                () -> "own snippets using EXTENSION grammar outside the"
                        + " MARKED extension-test hosts — declare the host"
                        + " or fix the test:\n  "
                        + String.join("\n  ", unmarkedExtension.subList(0,
                                Math.min(10, unmarkedExtension.size()))));
        // F3.7: per-host EXACT accounting: the host sets say WHO may excuse (policy, here), and how much each one
        // currently does is MEASURED (PeRatchets, ratchets.tsv; Bazel workplan P3-30, D9): a snippet in another module
        // moves a generated file here (bazel run //parser-equivalence:update_ratchets), reviewed in its diff
        Map<String, Integer> platformHosted = c.platformHosted();
        Map<String, Integer> extensionHosted = c.extensionHosted();
        org.junit.jupiter.api.Assertions.assertEquals(
                PeRatchets.measuredWithPrefix(PLATFORM_KEY), platformHosted,
                "LITE-refused rows per platform-test host moved -- bazel run"
                        + " //parser-equivalence:update_ratchets, and review the diff");
        org.junit.jupiter.api.Assertions.assertEquals(
                PeRatchets.measuredWithPrefix(EXTENSION_KEY), extensionHosted,
                "extension-grammar rows per host moved -- bazel run"
                        + " //parser-equivalence:update_ratchets, and review the diff");
        // a declared host excusing nothing anywhere is a stale pardon
        List<String> staleHosts = new ArrayList<>();
        for (String h : PLATFORM_TEST_HOSTS) {
            if (!platformHosted.containsKey(h)
                    && !extensionHosted.containsKey(h)) {
                staleHosts.add(h + " (platform set)");
            }
        }
        for (String h : EXTENSION_TEST_HOSTS) {
            if (!extensionHosted.containsKey(h)) {
                staleHosts.add(h + " (extension set)");
            }
        }
        org.junit.jupiter.api.Assertions.assertEquals(List.of(),
                staleHosts.stream().sorted().toList(),
                "declared hosts excuse ZERO rows — remove the stale"
                        + " declarations");
    }

    private interface ThrowingRunnable {
        void run() throws Throwable;
    }

    private static boolean accepts(ThrowingRunnable r) {
        try {
            r.run();
            return true;
        } catch (Throwable t) {
            return false;
        }
    }
}
