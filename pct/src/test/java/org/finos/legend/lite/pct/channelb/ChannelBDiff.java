// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package org.finos.legend.lite.pct.channelb;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;

/**
 * The per-suite channel diff (One-Platform Plan Phase 4): channel B's
 * outcomes against TWO oracles — channel A's own expectedFailures ledger
 * (the interpreted reference) and the engine's relational-DuckDB PCT
 * manifest (the pinned release's, read from @legend_engine_src). AGREE rows corroborate;
 * WIRE-BUG rows split into ENGINE-FRONTIER (the reference relational
 * executor fails them too) and TRUE wire bugs — the number that burns.
 */
final class ChannelBDiff {

    private ChannelBDiff() {
    }

    record Counts(int pass, int agreePass, int agreeFail, int wireBug,
            int bFixesA, int declined, int frontier, int trueWireBug) {
    }

    static Counts report(String tag, List<ChannelB.Outcome> out,
            Path channelASuite, Path duckDbManifest) throws IOException {
        Set<String> aFail = scan(channelASuite,
                "one\\(\"(meta::[a-zA-Z:_0-9]+)_Function",
                "channel A ledger scan found nothing — the suite moved");
        Set<String> engineExcluded = scan(duckDbManifest,
                "\"test\"\\s*:\\s*\"(meta::[a-zA-Z:_0-9]+?)_Function_",
                "engine DuckDB manifest scan found nothing — the oracle"
                        + " snapshot moved");
        int pass = 0;
        int agreePass = 0;
        int agreeFail = 0;
        int wireBug = 0;
        int bFixesA = 0;
        int declined = 0;
        int frontier = 0;
        int trueWireBug = 0;
        for (ChannelB.Outcome o : out) {
            boolean aFails = aFail.contains(o.testFqn());
            switch (o.status()) {
                case PASS -> {
                    pass++;
                    if (aFails) {
                        bFixesA++;
                    } else {
                        agreePass++;
                    }
                }
                case DECLINED -> declined++;
                default -> {
                    if (aFails) {
                        agreeFail++;
                    } else {
                        wireBug++;
                        if (engineExcluded.contains(o.testFqn())) {
                            frontier++;
                        } else {
                            trueWireBug++;
                            System.out.println("[" + tag + "] TRUE-WIRE-BUG "
                                    + o.testFqn());
                        }
                    }
                }
            }
        }
        System.out.println("[" + tag + "] diff: AGREE-PASS=" + agreePass
                + " AGREE-FAIL=" + agreeFail + " WIRE-BUG=" + wireBug
                + " B-FIXES-A=" + bFixesA + " DECLINED=" + declined);
        System.out.println("[" + tag + "] frontier: ENGINE-FRONTIER="
                + frontier + " TRUE-WIRE-BUG=" + trueWireBug);
        // R1 canonical-byte-channel divergence table (CANONICAL_FORM_SPEC
        // §0) — cumulative across this JVM's platform-executed asserts
        System.out.println("[" + tag + "] canon: "
                + com.legend.exec.CanonicalDivergence.summary());
        com.legend.exec.CanonicalDivergence.samples().forEach(r ->
                System.out.println("[" + tag + "] canon-sample: "
                        + r.family() + " " + r.detail()));
        // TYPED-IR Slice 1: the label-lie census (instrument -> census
        // -> flip) — printed per suite, cumulative per JVM
        System.out.println("[" + tag + "] sqltypes: "
                + com.legend.exec.SqlTypeCensus.summary());
        com.legend.exec.SqlTypeCensus.classes(40).forEach(c ->
                System.out.println("[" + tag + "] sqltypes-class: " + c));
        com.legend.exec.SqlTypeCensus.allSamples().forEach((cls, ws) ->
                ws.forEach(w -> System.out.println("[" + tag
                        + "] sqltypes-witness: " + cls + " :: " + w)));
        return new Counts(pass, agreePass, agreeFail, wireBug, bFixesA,
                declined, frontier, trueWireBug);
    }

    private static Set<String> scan(Path file, String pattern, String emptyMsg)
            throws IOException {
        Set<String> names = new java.util.HashSet<>();
        String src = Files.readString(file);
        var m = java.util.regex.Pattern.compile(pattern).matcher(src);
        while (m.find()) {
            names.add(m.group(1));
        }
        if (names.isEmpty()
                && !src.contains("= Lists.mutable.empty()")) {
            // a DECLARED-empty ledger (Standard: channel A expects zero
            // failures) is legitimate; silence anywhere else means the
            // file moved under the regex — loud
            throw new IllegalStateException(emptyMsg);
        }
        return names;
    }
}
