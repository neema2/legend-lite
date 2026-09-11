// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * F1.4 — a POSITIVE rule on the harness (Charter C2.3): result
 * reordering is comparison POLICY only — two-sided, declared, and
 * enumerated (every unordered compare gated on a compile-time fact
 * about the QUERY — the old runner's {@code ordered && sortedChain()}
 * doctrine); this test makes the discipline required rather than
 * voluntary. The allowlist is EXACT-MATCH: a new sort/distinct site in
 * the harness fails until it is either gated-and-listed here (with the
 * reason) or removed; a removed site forces the list to shrink.
 */
class HarnessDisciplineTest {

    /** The audited sites (F1.11 re-enumeration — the first census
     *  missed the {@code List.sort(cmp)} spelling, and audit A7's own
     *  site was among the escapees):
     *  (the old runner's five sites — makeString split-multiset order
     *  policy, TDS-text unordered compare, graph-triples canon — died
     *  with it in batch 115);
     *  H2Verify 2 — the replay oracle's order-insensitive row multiset
     *  (two-sided BY DESIGN, counted by F2.4);
     *  JsonAssertCanon 1 — audit A7 RESOLVED by F6.5 (2026-08-17): the
     *  site re-creates the TEST'S OWN canonicalization idiom
     *  (^JSONArray(values=...->sortBy(getValue('K')))) with pure
     *  sortBy comparator semantics — numbers numerically, mixed kinds
     *  wall — replacing the lexical String.valueOf sort. The sort
     *  itself is the test's, not harness compensation, so the SITE
     *  stays listed (the JSON metamodel never executes through the
     *  SQL pipeline);
     *  LineageForm 1 — want.sort on the property-name existence check
     *  (two-sided: both lists sorted before compare);
     *  Runner 2 / RelationalCorpusRunner 15 — rcorpus orchestration and
     *  scoreboard-RENDER ordering (deterministic output, not result
     *  comparison) — in scope so comparison sorts cannot hide here.
     *  The 15th (2026-08-20): the h2-verdicts.txt roster dump sorts by
     *  key for a DIFFABLE diagnostic file (the floor-attribution
     *  instrument) — deterministic output, never comparison. */
    private static final Map<String, Integer> ALLOWED = Map.ofEntries(
            // F4.3 ratchet-DOWN 5 -> 3: the harness RENDERER died (the
            // platform's RENDER lowerings produce the text; the probe and
            // its sorts died with it) — the survivors are the makeString
            // split-multiset order policy. 3 -> 4 (audit-of-audits #9):
            // the pool.remove( spelling joined the regex and FOUND the
            // assertSameElements value-multiset loop this guard had
            // never seen — order-insensitivity there is the assert's
            // own PURE-SPEC semantics (assertSameElements), not
            // leniency; registered, [ord]-tagged.
            // 4 -> 3 (TDG S4): a sort site died with csvCensusAssert —
            // the census asserts route through the platform now
            // 2 -> 4 (diff-noreplay burndown 2026-08-28): the GRAPH
            // frame compare (goldenGraphCompare) sorts BOTH sides for
            // the same order-insensitive row-multiset verdict the
            // tabular pair implements — two-sided by design, the §4AB
            // label-mapped twin of the positional oracle
            // 4 -> 7 (TDG 49er replay): tdgSqlReplay's TWO-SIDED
            // multiset compare (both row lists sort under the shared
            // canon) + rawRows' column-NAME normalization — comparison
            // policy gated on the compile-time no-ORDER-BY fact (an
            // ordered fetch declines by name before any reorder)
            // (chained-fetch live-session refereeing, §S5-L: the
            // transcript side renders through the SAME name-order
            // policy as rawRows — nameOrder() is now the one owner,
            // so the count STAYS 7; two-sided by construction, same
            // no-ORDER-BY compile-time gate upstream)
            // 7 -> 8 (row-13 adjudication burn 2026-09-01, SQLTEXT
            // charter §6.1): the graph compare's golden-side
            // pk-collapse re-sorts the COLLAPSED golden list into the
            // same order-insensitive row-multiset verdict (the frame
            // side is already sorted at the main compare — two-sided
            // by construction), gated on the COMPILE-TIME
            // extent-subset fact of the typed query chain (the verify
            // site's caller sets H2Verify.EXTENT_SUBSET).
            // 8 -> 10 (§7 flip, same charter): orderedVerdict's TIE
            // GROUPS — within each run of equal sort-key rows BOTH
            // sides sort before comparing (two-sided by construction;
            // rows tied on the key have no defined relative order on
            // either backend), gated on the COMPILE-TIME sort-key
            // derivation (H2Verify.SORT_KEYS). Phase 0.4 (2026-09-08):
            // the two sort-key sites are DEAD today — ORDERED_QUERY and
            // SORT_KEYS lost their writer in batch 115 and are registered
            // dangling in DanglingStateGuardTest; Phase 0.5 rewires them
            // from AssertVerdicts.orderView. The count stays 10 because
            // the sites exist; their gate is the fact this comment names.
            Map.entry("H2Verify.java", 10),
            // 15 -> 17 (SQLTEXT slice-3 step 0, 2026-09-01): the shape
            // census dump's two sorts — count-descending histogram +
            // name-sorted roster for a DIFFABLE census file (the
            // h2-verdicts display-ordering class; no comparison flows
            // through them). 17 -> 18 (slice 3a): the emission
            // census's count-descending text-verdict print — same
            // display-only class.
            // MinimalCorpus (harness rebuild, 2026-09-06): DISCOVERY order
            // only — source files by name, tests by the engine suite
            // order, setup packages by nesting depth; no result flows
            // through a sort (the platform judges every verdict).
            // 4 -> 2 (batch 7a, 2026-09-11): the test order and the setup
            // packages' nesting order moved INTO THE PRODUCT with discovery
            // and the runner (com.legend.test); the harness keeps only its
            // source-file order.
            Map.entry("MinimalCorpus.java", 2),
            // the timing ledger: the slowest tests DISPLAYED, no comparison
            Map.entry("MinimalCorpusTest.java", 1),
            // the eager corpus compile PROBE (COMPILE_EVERYTHING_HOMEWORK §10, run
            // by name, not a gate): sorted REPORT lines — failures by reason,
            // package, source file, name — a display, never a verdict
            Map.entry("EagerCorpusCompileProbe.java", 19),
            // PX.1: TreeSet as a deterministic-iteration REGISTRY
            // (workspace names), not a result reorder
            Map.entry("DuckWorkspaces.java", 1),
            // ---- src/main/com/legend/exec (audit-of-audits #9): the
            // comparison policy MOVED here from the harness and walked
            // out of this guard's scope — the walk now covers it. ----
            // TdsCompare: the FOUR pool-matching loops (row multiset,
            // row-TUPLE multiset [ap.remove — the regex blind spot the
            // audit-of-Blocker-3 closed], CSVJOIN cell multiset, text
            // line multiset) — two-sided comparison policy gated on the
            // chain's sortedness, each with a distinct [ord] tag (#10)
            // PureAsserts: the typeRank sort inside sameElements — the
            // pure total-order comparator applied to BOTH sides
            Map.entry("PureAsserts.java", 1),
            // TYPED-IR Slice 1: census-class DISPLAY ordering
            // (largest-first report lines) — reporting, never a result
            // reordering; two-sided by construction (both sides of no
            // comparison flow through it). 1 -> 2 (§E3 M-N1): the
            // nullability differential's report sorts its OWN class
            // map largest-first. 2 -> 3 (§E3 slack census): the slack
            // report, same display-only shape.
            Map.entry("SqlTypeCensus.java", 3),
            // CanonicalDivergence: the assertSameElements byte-channel
            // stand-in sorts RENDERED STRINGS on BOTH sides (two-sided
            // comparison policy — the census-side mirror of R2's
            // canonical ORDER BY; CANONICAL_FORM_SPEC §0), and the R1b
            // grid-text CLASSIFIER sorts both sides' lines to name
            // row-order-only divergences. Measurement only — no probe
            // can affect a verdict. 2→4 (2026-08-22, R1b). 4→6
            // (2026-08-28, V7 batch 1): the dual-channel census REPORT
            // sorts its own form/decline tables for stable console
            // diffs — DISPLAY ordering only, the SqlTypeCensus report
            // precedent; no comparison flows through it.
            // 6→7 (2026-08-30, step-0 residue census): the per-row
            // decline-witness list sorts for stable console diffs —
            // the SAME display-ordering class; no comparison flows
            // through it (FULL_RESIDUE_CENSUS_2026_08_30.md §0).
            Map.entry("CanonicalDivergence.java", 7),
            // (MetamodelWalk.java: its 3 record-accessor `.distinct()`
            // sites left with the resolvePrimaryKey arm — metamodel-as-
            // relations batch 5, 2026-09-02)
            // SQLTEXT slice-3 step 0 (2026-09-01): the shape census's
            // TreeMap is a deterministic-iteration REGISTRY (stable
            // shape-combination keys for the diffable census file, the
            // DuckWorkspaces precedent) — no comparison flows through
            // it
            // §8.3b wobbler attribution: the flipped-test roster dump
            // sorts test names for a DIFFABLE file (display ordering,
            // the h2-verdicts class; no comparison flows through it).
            // 1 -> 3 (metamodel handoff §5 step 1, 2026-09-02): the
            // bucket→tests roster dump sorts bucket names and, within
            // a bucket, test names — the same display-only class
            // (WholeTestFlip, the old runner and the walk: DELETED in
            // batch 115 — their rows left this list)
            Map.entry("TdsCompare.java", 4));

    /** Extremum spellings joined 2026-08-18 (Tier-2 audit; the
     * original audit's probe 12 — {@code Collections.max} in the
     * harness — landed GREEN). Zero sites today; a new one registers
     * like any reorder. */
    private static final Pattern SITE = Pattern.compile(
            "Collections\\.sort\\(|\\.sorted\\(|\\.distinct\\(\\)"
            + "|\\.sort\\(|new TreeSet|new TreeMap"
            + "|Collections\\.max\\(|Collections\\.min\\("
            + "|new PriorityQueue|\\.stream\\(\\)\\.max\\("
            + "|\\.stream\\(\\)\\.min\\("
            // audit-of-audits #9: the POOL-MATCHING loop spelling —
            // TdsCompare's hand-rolled multiset compares carry no
            // .sorted( for the regex to find. `.remove(hit)` is the
            // idiom's TRUE stable token (the first spelling,
            // `pool.remove(`, missed rowTupleMultiset's `ap.remove(hit)`
            // — caught by the audit-of-Blocker-3 pass).
            + "|\\.remove\\(hit\\)");

    @Test
    void resultReorderingIsEnumeratedComparisonPolicyOnly()
            throws IOException {
        Map<String, Integer> found = new TreeMap<>();
        int scanned = 0;
        for (Path root : new Path[] {
                Path.of("src/test/java/com/legend/harness"),
                Path.of("src/test/java/com/legend/rcorpus"),
                // audit-of-audits #9: the comparison policy lives in
                // PRODUCTION exec now (TdsCompare/PureAsserts moved
                // from the harness) — the discipline follows the code
                Path.of("src/main/java/com/legend/exec")}) {
            try (Stream<Path> files = Files.walk(root)) {
                for (Path f : files
                        .filter(p -> p.toString().endsWith(".java"))
                        .toList()) {
                    scanned++;
                    Matcher m = SITE.matcher(Files.readString(f));
                    int n = 0;
                    while (m.find()) {
                        n++;
                    }
                    if (n > 0) {
                        found.put(f.getFileName().toString(), n);
                    }
                }
            }
        }
        GuardCoverage.assertFloor("HarnessDisciplineTest", scanned, 22);
        assertEquals(new TreeMap<>(ALLOWED), found,
                "harness sort/distinct sites moved — a NEW site must be"
                + " two-sided comparison policy, gated on a compile-time"
                + " fact, and listed here with its reason (Charter C2.3);"
                + " a removed site shrinks the list");
    }
}
