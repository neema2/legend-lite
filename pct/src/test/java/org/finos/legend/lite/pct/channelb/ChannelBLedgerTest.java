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
}
