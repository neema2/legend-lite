// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.leansql;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * THE PRODUCT-SQL SIZE BASELINE's guard ({@link LeanSqlBaseline}; G3, ruled 2026-11-12): the fixed sets planned on
 * every database, held to their pins -- growth fails, a shrink re-pins -- and the rule itself proven on planted
 * measurements, both of its failure modes.
 */
class LeanSqlBaselineTest {

    @Test
    void theProductsSqlIsHeldToItsBaseline() throws Exception {
        Map<LeanSqlBaseline.Key, LeanSqlBaseline.Measure> pinned = LeanSqlBaseline.read(Files.readString(
                com.legend.testing.Runfile.property("legend.lean_sql.baseline"), StandardCharsets.UTF_8));
        Map<LeanSqlBaseline.Key, LeanSqlBaseline.Measure> measured = LeanSqlBaseline.measure(
                Map.of("channel_a", com.legend.testing.Runfile.property("legend.lean_sql.channel_a")));
        List<String> drift = LeanSqlBaseline.drift(pinned, measured);
        assertTrue(drift.isEmpty(), "the product's SQL left its baseline (lean-sql/baseline.tsv):\n  "
                + String.join("\n  ", drift));
    }

    private static final LeanSqlBaseline.Key A = new LeanSqlBaseline.Key("set", "DuckDB", "f");
    private static final LeanSqlBaseline.Key B = new LeanSqlBaseline.Key("set", "DuckDB", "g");

    private static Map<LeanSqlBaseline.Key, LeanSqlBaseline.Measure> of(long aChars, long aPlan, long bChars,
            long bPlan) {
        Map<LeanSqlBaseline.Key, LeanSqlBaseline.Measure> m = new TreeMap<>();
        m.put(A, new LeanSqlBaseline.Measure(10, aChars, aPlan, "a's worst"));
        m.put(B, new LeanSqlBaseline.Measure(10, bChars, bPlan, "b's worst"));
        return m;
    }

    /** The planted violators: a family's SQL one character over its pin fails, though another family shrank by more
     *  (no family pays for another); SQL moved out of the text into the plan (the text smaller, the plan's JSON one
     *  character larger) fails on the plan; a statement added fails on the count; the pins themselves pass. */
    @Test
    void theRuleCatchesGrowthAndBytesMovedIntoThePlan() {
        Map<LeanSqlBaseline.Key, LeanSqlBaseline.Measure> pins = of(1000, 5000, 1000, 5000);
        assertEquals(List.of(), LeanSqlBaseline.drift(pins, of(1000, 5000, 1000, 5000)));
        assertEquals(List.of(), LeanSqlBaseline.drift(pins, of(995, 4990, 1000, 5000)), "a shrink inside the slack");

        // growth in one family, a bigger shrink in another: the growth fails by name
        List<String> grew = LeanSqlBaseline.drift(pins, of(1001, 5001, 1000, 5000));
        assertTrue(grew.stream().anyMatch(l -> l.startsWith(A + ": chars GREW 1000 -> 1001")), grew.toString());
        assertTrue(grew.stream().anyMatch(l -> l.contains("a's worst")), grew.toString());

        // the text moved into the plan (a bound value, plan data): the text shrank inside the slack, the plan grew
        List<String> moved = LeanSqlBaseline.drift(pins, of(995, 5001, 1000, 5000));
        assertEquals(1, moved.size(), moved.toString());
        assertTrue(moved.get(0).startsWith(A + ": plan JSON chars GREW 5000 -> 5001"), moved.toString());

        // a second statement
        Map<LeanSqlBaseline.Key, LeanSqlBaseline.Measure> split = of(1000, 5000, 1000, 5000);
        split.put(A, new LeanSqlBaseline.Measure(11, 1000, 5000, "a's worst"));
        assertTrue(LeanSqlBaseline.drift(pins, split).get(0).startsWith(A + ": statements GREW 10 -> 11"));

        // a shrink past the slack asks for a re-pin; a new or a gone family too
        assertTrue(LeanSqlBaseline.drift(pins, of(900, 5000, 1000, 5000)).get(0).contains("shrank 1000 -> 900"));
        Map<LeanSqlBaseline.Key, LeanSqlBaseline.Measure> gone = of(1000, 5000, 1000, 5000);
        gone.remove(B);
        assertTrue(LeanSqlBaseline.drift(pins, gone).get(0).contains("pinned and gone"));
        assertEquals(pins.keySet(), LeanSqlBaseline.read(LeanSqlBaseline.write(pins)).keySet());
    }
}
