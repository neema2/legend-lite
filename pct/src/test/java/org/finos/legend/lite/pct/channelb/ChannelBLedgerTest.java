// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package org.finos.legend.lite.pct.channelb;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The ledger matches a failure's FULL text, never its display (2026-11-15, Databricks round 53: the display's clipped
 *  middle removed the ledger's text, and four rows read "FAILS DIFFERENTLY"). */
class ChannelBLedgerTest {

    private static final String WORDS = "[INVALID_TYPED_LITERAL] The value of the typed literal \"DATE\" is invalid";

    @Test
    void aLedgerRowMatchesTextTheDisplayClipped() {
        String full = "x".repeat(600) + WORDS + "y".repeat(600);
        ChannelB.Outcome o = new ChannelB.Outcome("t::a", ChannelB.Status.FAIL,
                full.substring(0, 400) + "…" + full.substring(full.length() - 400), full);
        // the display does not hold the words; the full text does, and the ledger matches it
        assertTrue(!o.detail().contains(WORDS));
        ChannelBLedger.judge("s", List.of(o), List.of(new ChannelBLedger.Expected("t::a", WORDS)));
    }

    @Test
    void aLedgerRowTheFullTextDoesNotHoldStillFails() {
        ChannelB.Outcome o = new ChannelB.Outcome("t::a", ChannelB.Status.FAIL, "other", "other words");
        AssertionError e = assertThrows(AssertionError.class, () -> ChannelBLedger.judge("s", List.of(o),
                List.of(new ChannelBLedger.Expected("t::a", WORDS))));
        assertTrue(e.getMessage().contains("FAILS DIFFERENTLY"), e.getMessage());
    }

    /** A judge disagreement is refused by name whatever row it would match (the compatibility owner, 2026-11-19): its
     *  text quotes the database's run, so a row naming the database's error matched it, and the LITE lane passed on
     *  four rows (PCT's testAdjustBy*BigNumber) while its judges disagreed. The plant: such an outcome and the row it
     *  contains. */
    @Test
    void aJudgeDisagreementIsRefusedWhateverRowItWouldMatch() {
        String error = "DataError: Conversion Error: date field value out of range: \"33803336-12-17\"";
        String full = ChannelBLedger.DISAGREEMENT + "the judges disagree at assert 0: host"
                + " meta::pure::functions::asserts::assertEquals failed \"Conversion Error: date field value out of"
                + " range: \"33803336-12-17\"\"; database (none) (database's run \"" + error + "\")";
        ChannelB.Outcome o = new ChannelB.Outcome("t::a", ChannelB.Status.ERROR, full, full);
        assertTrue(o.full().contains(error));
        AssertionError e = assertThrows(AssertionError.class, () -> ChannelBLedger.judge("s", List.of(o),
                List.of(new ChannelBLedger.Expected("t::a", error))));
        assertTrue(e.getMessage().contains("THE JUDGES DISAGREE"), e.getMessage());
    }

    /** The guard's head is the judges' disagreement's own class name, which the runner writes before its words: a
     *  rename of {@code testexec.IsolatedRun.JudgesDisagree} fails here, never turning the guard off silently (PR #127's
     *  review, 2026-11-19; the class is package-private there, so it is named). */
    @Test
    void theDisagreementsHeadIsItsClassesName() throws ClassNotFoundException {
        org.junit.jupiter.api.Assertions.assertEquals(
                Class.forName("com.legend.testexec.IsolatedRun$JudgesDisagree").getSimpleName() + ": ",
                ChannelBLedger.DISAGREEMENT);
    }
}
