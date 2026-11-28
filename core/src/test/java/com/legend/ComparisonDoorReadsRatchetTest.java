// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

import com.legend.testing.SourceFiles;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * THE LOWERING'S READS OF THE DOOR FOR A COMPARISON ONLY SHRINK (step 6B, 2026-11-28; the user: "I thought part of
 * this was fixing the doors to be passed in and nothing knowing or guessing about which mode/door was?"). Today the
 * code that turns a comparison into SQL still asks the door's table which rule a comparison takes
 * ({@code Scalars.doorsRule}), asks it whether an empty side is false ({@code EditionTable.emptySideIsFalse}), and
 * carries one "two families or three types" argument it branches on ({@code NumberFamilies}). Step 6B's slice 7
 * removes all three: a door's comparison becomes its own function, chosen before the lowering, and the lowering has
 * one rule per function. Until it lands these counts are a ceiling: a new read fails here, a removed one asks for
 * the pin to be tightened. A pin moves UP only with a dated note naming the task (AGENTS.md, "Guard files are
 * load-bearing").
 */
@Tag("guardrail")
class ComparisonDoorReadsRatchetTest {

    /** Pattern, counted over the sources after the type-checker (lowering, planner, test planning, SQL), and its
     *  ceiling: the count on 2026-11-28, slices 5 and 6 of step 6B. */
    private static final Map<String, Integer> PINS = pins();

    private static Map<String, Integer> pins() {
        Map<String, Integer> pins = new LinkedHashMap<>();
        // the lookup's declaration, its one use in Scalars.lower and four in Lowerer (==, membership's rows, the two
        // whole-side entries)
        pins.put("doorsRule\\(", 6);
        // Lowerer.written, the one call
        pins.put("\\.emptySideIsFalse\\(", 1);
        // Scalars 9, Equalities 9, SideEqualities 2, HeldKinds 1, TaggedLists 1
        pins.put("NumberFamilies\\.(TWO_FAMILIES|THREE_TYPES)", 22);
        return pins;
    }

    private static final java.util.List<String> AFTER_THE_TYPE_CHECKER = java.util.List.of(
            "core/src/main/java/com/legend/lowering", "core/src/main/java/com/legend/planner",
            "core/src/main/java/com/legend/testplan", "core/src/main/java/com/legend/sql");

    @Test
    void theLoweringsReadsOfTheDoorForAComparisonOnlyShrink() throws IOException {
        for (Map.Entry<String, Integer> pin : PINS.entrySet()) {
            Pattern p = Pattern.compile(pin.getKey());
            int n = 0;
            int files = 0;
            for (String root : AFTER_THE_TYPE_CHECKER) {
                for (Path f : SourceFiles.under(root)) {
                    if (!f.toString().endsWith(".java")) {
                        continue;
                    }
                    files++;
                    Matcher m = p.matcher(Files.readString(f));
                    while (m.find()) {
                        n++;
                    }
                }
            }
            assertTrue(files > 100, "the guard read " + files + " source files: it is not looking at the tree");
            assertTrue(n <= pin.getValue(), "'" + pin.getKey() + "' is read " + n + " times after the type-checker"
                    + " (ceiling " + pin.getValue() + "): the lowering may not learn a new way to ask the door about a"
                    + " comparison -- step 6B's slice 7 removes these reads; give the door's meaning its own function"
                    + " instead");
            // (headroom is not a pin: a read removed moves the pin down in the same change, as the ledger asks)
            assertTrue(n == pin.getValue(), "'" + pin.getKey() + "' shrank to " + n + " (pin " + pin.getValue()
                    + "): tighten the pin to " + n);
        }
    }
}
