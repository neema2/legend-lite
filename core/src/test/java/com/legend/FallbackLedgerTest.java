// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

import com.legend.testing.Repo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * THE FALLBACK LEDGER (audit 2026-09-15 P5-6). AGENTS.md invariant 4 — NO
 * FALLBACKS — was {@code [CONVENTION]}: nothing checked it, and the
 * normalizer's own "every empty answer is censused" claim lived in a
 * javadoc, so the zero could regrow silently.
 *
 * <p>Two mechanical pins, both shrink-only:
 *
 * <ul>
 *   <li>a BARE {@code orElse(null)} anywhere in the package is ZERO. An
 *       empty answer either funnels through {@link MissProbe} (a censused
 *       "the miss IS the answer" site) or is loud;</li>
 *   <li>the funnel itself is a REGISTER: how many censused empty-answer
 *       sites each file carries. Growth is a NEW silent default and needs a
 *       row bump with the batch's name; shrinkage means a site went loud —
 *       ratchet the row down in the same commit.</li>
 * </ul>
 *
 * <p>NOT covered, on purpose: the lenient NAME-RESOLUTION fallbacks Phase E
 * still carries (a wildcard association end, an unqualified store ref, the
 * signature-mangle path). Those are Phase-D debt — making Phase D total for
 * mapping bodies and deleting them is FIXLIST P7-3, which is not scheduled
 * work; they are not silent (each is commented at its site) and they are not
 * in this package's empty-answer funnel.
 */
class FallbackLedgerTest {

    private static final Path PACKAGE = Repo.module("src/main/java/com/legend/normalizer");

    /** file &rarr; censused empty-answer sites ({@code MissProbe::miss},
     * {@code MissProbe.miss()}, {@code MissProbe.knownMiss(}). Measured
     * 2026-09-15 (audit fix A10b). */
    private static final Map<String, Integer> FUNNEL = new TreeMap<>(Map.ofEntries(
            Map.entry("AssociationSynthesis.java", 1),
            Map.entry("DeclaredCoercions.java", 2),
            Map.entry("ImplicitInheritance.java", 1),
            Map.entry("JoinChainEmission.java", 4),
            Map.entry("M2mRouteGuards.java", 1),
            Map.entry("MappingNormalizer.java", 9),
            Map.entry("MappingValidation.java", 2),
            Map.entry("MissProbe.java", 1),
            Map.entry("RelOpTranslator.java", 1),
            Map.entry("StoreSubstitutionRewrite.java", 1),
            Map.entry("UnionSynthesis.java", 4),
            Map.entry("ViewRelation.java", 5)));

    /** Loud "this default never fired" sites. Growth is GOOD (a silent
     * default went loud); a drop means one went quiet again.
     * 12 -> 10 (views stage 3, 2026-09-22): ViewRelation#6/#7 guarded the
     * normalizer's copy of the view main-table rule; the rule's one home is
     * now ModelBuilder.viewMainTable, whose miss (a navigated join its
     * database does not declare) is a ModelException — still loud, in the
     * kernel's own vocabulary. */
    private static final int NEVER_FIRED_FLOOR = 10;

    @org.junit.jupiter.api.Test
    void noBareEmptyAnswerOutsideTheFunnel() throws IOException {
        List<String> strays = new ArrayList<>();
        for (Path p : sources()) {
            if (p.getFileName().toString().equals("MissProbe.java")) {
                continue;   // the funnel itself IS the orElse(null)
            }
            if (Pattern.compile("orElse\\(null\\)").matcher(Files.readString(p)).find()) {
                strays.add(p.getFileName().toString());
            }
        }
        assertEquals(List.of(), strays,
                "a BARE orElse(null) is a silent default (AGENTS.md invariant 4): funnel it"
                + " through MissProbe with a census entry, or make the empty answer loud");
    }

    @org.junit.jupiter.api.Test
    void theEmptyAnswerFunnelIsPinned() throws IOException {
        Pattern site = Pattern.compile(
                "MissProbe::miss|MissProbe\\.miss\\(\\)|MissProbe\\.knownMiss\\(");
        Pattern neverFired = Pattern.compile("MissProbe\\.neverFired\\(");
        Map<String, Integer> actual = new TreeMap<>();
        int loud = 0;
        for (Path p : sources()) {
            String code = Files.readString(p);
            int n = 0;
            Matcher m = site.matcher(code);
            while (m.find()) {
                n++;
            }
            if (n > 0) {
                actual.put(p.getFileName().toString(), n);
            }
            Matcher l = neverFired.matcher(code);
            while (l.find()) {
                loud++;
            }
        }
        assertEquals(FUNNEL, actual,
                "the censused empty-answer sites moved: GROWTH is a new silent default — funnel"
                + " it only with a written census entry; SHRINKAGE means a site went loud, so"
                + " ratchet the row down in the same commit");
        assertTrue(loud >= NEVER_FIRED_FLOOR,
                "loud never-fired guards dropped to " + loud + " (floor " + NEVER_FIRED_FLOOR
                + "): a default that used to be loud went quiet");
    }

    private static List<Path> sources() throws IOException {
        try (Stream<Path> s = Files.walk(PACKAGE)) {
            List<Path> out = s.filter(p -> p.toString().endsWith(".java")).toList();
            GuardCoverage.assertFloor("FallbackLedgerTest", out.size(), 20);
            return out;
        }
    }
}
