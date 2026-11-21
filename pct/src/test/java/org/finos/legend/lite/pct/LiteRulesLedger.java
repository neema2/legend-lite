// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package org.finos.legend.lite.pct;

import org.eclipse.collections.api.factory.Lists;
import org.eclipse.collections.api.list.MutableList;
import org.finos.legend.pure.m3.pct.reports.config.exclusion.ExclusionOneTest;
import org.finos.legend.pure.m3.pct.reports.config.exclusion.ExclusionSpecification;

import java.util.HashSet;
import java.util.Set;

/**
 * THE LITE-PCT LANE'S CHANNEL A LEDGERS (the compatibility program's stage A, step A4's gate, 2026-11-19): channel A
 * under lite's own rules ({@code -Dpct.channela.rules=LITE}, PctPlanRoute: the product's style and default features,
 * every error in lite's own words), legend-pure's interpreter judging lite's answers by value. Each suite's expected
 * failures are its COMPAT ledger's ({@code Test_LegendLite_<Suite>Functions_PCT}) with lite's own rows in place: a
 * test the LITE rows name is theirs alone, every other keeps its COMPAT row. Measured at claude/compat-judges-parity, 2026-11-19:
 * 26 rows differ, 7 COMPAT failures failing with lite's text and 19 new; the relation and unclassified suites did not
 * move (lite's relation answers are legend-pure's by value). 2026-11-21 (stage B's B1, PR #130): a compile error too is
 * in lite's words on this lane (the lane chooses its words once), so the relation suite holds 4 rows, the inverted
 * window frame's tests.
 */
final class LiteRulesLedger {

    private LiteRulesLedger() {
    }

    /** {@code compat}'s rows with {@code lite}'s in their place, each LITE row one test's. */
    static MutableList<ExclusionSpecification> under(MutableList<ExclusionSpecification> compat,
            ExclusionSpecification... lite) {
        Set<String> theirs = new HashSet<>();
        for (ExclusionSpecification l : lite) {
            if (!(l instanceof ExclusionOneTest one) || !theirs.add(one.testFullPath)) {
                throw new IllegalStateException("a LITE row names one test, once: " + l);
            }
        }
        MutableList<ExclusionSpecification> out = Lists.mutable.empty();
        for (ExclusionSpecification c : compat) {
            if (!(c instanceof ExclusionOneTest one && theirs.contains(one.testFullPath))) {
                out.add(c);
            }
        }
        out.addAll(java.util.List.of(lite));
        return out;
    }

    /** The LITE ledgers run under lite's rules alone: refused by name under any other. */
    static void requireLite(String suite) {
        if (!"LITE".equals(System.getProperty("pct.channela.rules"))) {
            throw new IllegalStateException(suite + "'s LITE ledger runs under -Dpct.channela.rules=LITE alone");
        }
    }
}
