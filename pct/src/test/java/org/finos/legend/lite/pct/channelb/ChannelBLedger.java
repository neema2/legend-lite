// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package org.finos.legend.lite.pct.channelb;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * CHANNEL B'S OWN EXPECTED FAILURES (2026-11-03, the channel B audit: the suites classified a failure against channel
 * A's ledger by test NAME alone and pinned floors with slack, so a failure that changed kind, a regression of a test
 * channel A's ledger lists, or a body the closed walk refused could pass unseen). Each suite names every test channel B
 * fails, with the text its failure says -- legend-pure's own rule for an expected failure
 * ({@code PureTestBuilderInterpreted}: the error's message CONTAINS the ledgered text) -- and the run must match it
 * exactly: every failure ledgered with its text, every ledgered test failing. A test that starts passing leaves the
 * ledger; one that starts failing joins it only with a dated reason, as every guard's entry. Every lane's rows are one
 * file, {@code ledger.tsv} ({@link #of}): the reference lane's and each other database's alike (2026-11-03, round 14:
 * the other lanes were held to counts, a floor and two ceilings, until each lane printed its failures' texts).
 */
final class ChannelBLedger {

    /** One expected failure: the test's full name and a text its failure's detail contains. */
    record Expected(String testFqn, String contains) {
    }

    private ChannelBLedger() {
    }

    /** {@code suite}'s expected failures on {@code lane}: the rows of {@code ledger.tsv} (beside this class on the
     *  classpath) naming both, each a test and the text its failure contains. A row that is not four tab-separated
     *  fields is refused by name. */
    static List<Expected> of(String lane, String suite) {
        String text;
        try (java.io.InputStream in = ChannelBLedger.class.getResourceAsStream("ledger.tsv")) {
            if (in == null) {
                throw new IllegalStateException("channel B's ledger (channelb/ledger.tsv) is not on the classpath");
            }
            text = new String(in.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
        } catch (java.io.IOException e) {
            throw new java.io.UncheckedIOException(e);
        }
        List<Expected> out = new ArrayList<>();
        for (String line : text.split("\n", -1)) {
            if (line.isBlank() || line.startsWith("#")) {
                continue;
            }
            String[] f = line.split("\t", -1);
            if (f.length != 4 || f[3].isEmpty()) {
                throw new IllegalStateException("channel B's ledger: a row is a lane, a suite, a test and the text its"
                        + " failure contains: " + line);
            }
            if (f[0].equals(lane) && f[1].equals(suite)) {
                out.add(new Expected(f[2], f[3]));
            }
        }
        return List.copyOf(out);
    }

    /** {@code out} judged against {@code ledger}: throws naming every difference. */
    static void judge(String suite, List<ChannelB.Outcome> out, List<Expected> ledger) {
        Map<String, Expected> byTest = new LinkedHashMap<>();
        for (Expected e : ledger) {
            if (byTest.put(e.testFqn(), e) != null) {
                throw new AssertionError(suite + " ledger names " + e.testFqn() + " twice");
            }
        }
        List<String> wrong = new ArrayList<>();
        Map<String, ChannelB.Outcome> seen = new LinkedHashMap<>();
        for (ChannelB.Outcome o : out) {
            seen.put(o.testFqn(), o);
            Expected e = byTest.get(o.testFqn());
            if (o.status() == ChannelB.Status.PASS) {
                if (e != null) {
                    wrong.add("PASSES, ledgered as failing (remove it from the ledger): " + o.testFqn());
                }
            } else if (e == null) {
                wrong.add("FAILS, not ledgered: " + o.testFqn() + " :: " + o.status() + " " + o.detail());
            } else if (!o.detail().contains(e.contains())) {
                wrong.add("FAILS DIFFERENTLY: " + o.testFqn() + " :: " + o.detail() + " -- the ledger expects it to"
                        + " contain: " + e.contains());
            }
        }
        for (Expected e : ledger) {
            if (!seen.containsKey(e.testFqn())) {
                wrong.add("ledgered but not discovered: " + e.testFqn());
            }
        }
        if (!wrong.isEmpty()) {
            throw new AssertionError("channel B " + suite + " does not match its ledger (" + wrong.size() + "):\n  "
                    + String.join("\n  ", wrong));
        }
    }
}
