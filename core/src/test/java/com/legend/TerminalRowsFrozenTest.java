// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A ROW CANNOT LEAVE THE COUNT BY BEING RELABELLED (the GUARD stream, step 2's item 2; the edge audit of 2026-11-17,
 * section C.2: "terminal register rows (STAYS/RULED/HOOK-RULED) frozen as an exact set with ruling ids"). A row of
 * {@link CompatSpellingGuardTest#REGISTER} whose step is terminal is not counted ({@code EdgeSeparationTest}'s
 * {@code everyOpenCompatRowIsCounted}), so relabelling an open row STAYS would close it with nothing moved -- the
 * closure-by-redefinition the audit found. The terminal rows are therefore an exact set, each with the ruling that made
 * it terminal: a new terminal row, or a row renamed into one, fails here until its ruling is written beside it.
 */
@Tag("guardrail")
class TerminalRowsFrozenTest {

    /** The register's terminal steps: a row with one of these is not counted. */
    static final Set<String> TERMINAL_STEPS = Set.of("STAYS", "RULED", "HOOK-RULED");

    /** Every terminal row, "file | spelling | step" (its count the register's to hold), with its ruling. */
    static final Map<String, String> TERMINAL = Map.ofEntries(
            // a product semantic sharing the spelling: the date pattern's letter Z (phase D's register, 2026-11-12)
            Map.entry("lowering/DateFormats.java | +0000 | STAYS", "D register 2026-11-12 (pattern letter Z)"),
            // INPUT read, no spelling written
            Map.entry("lowering/LiteralSpelling.java | +0000 | STAYS", "C5 step C 2026-11-17 (input read)"),
            Map.entry("lowering/Scalars.java | +0000 | STAYS", "D1.2 2026-11-15 (input read)"),
            Map.entry("protocol/ts/TsShow.java | +0000 | STAYS", "main 3a217086a (#81) 2026-11-14 (input read)"),
            // the user's ruling on H2's keywords and its session half
            Map.entry("sql/dialect/H2.java | ENGINE_LEXICON | RULED", "the user's ruling (H2's lexicon), listed 2026-11-16"),
            Map.entry("sql/dialect/H2.java | SET NON_KEYWORDS | RULED", "S5 2026-11-16 (the same ruling's session half)"),
            // TDSNull as INPUT and LANGUAGE: the #TDS cell token, Pure's class, the sentinel ^TDSNull() lowers to
            Map.entry("compiler/element/type/PlatformTypes.java | TDSNull | STAYS", "C5 step C, C-2 2026-11-17 (language)"),
            Map.entry("compiler/spec/TdsChecker.java | TDSNull | STAYS", "C5 step C, C-2 2026-11-17 (language)"),
            Map.entry("compiler/spec/TdsNullForms.java | TDSNull | STAYS", "C5 step C, C-2 2026-11-17 (language)"),
            Map.entry("compiler/spec/Typer.java | TDSNull | STAYS", "C5 step C, C-2 2026-11-17 (language)"),
            Map.entry("lowering/Scalars.java | TDSNull | STAYS", "C5 step C, C-2 2026-11-17 (the #TDS literal's input)"),
            Map.entry("compiler/spec/VerdictQueries.java | TDSNull | STAYS", "C5 step C, C-2 2026-11-17 (input read)"));

    /** The terminal rows of {@code register} as this test keys them. */
    static Set<String> terminal(List<String> register) {
        Set<String> out = new TreeSet<>();
        for (String row : register) {
            String[] p = row.split("\\|");
            String step = p[3].trim();
            if (TERMINAL_STEPS.contains(step)) {
                out.add(p[0].trim() + " | " + p[1].trim() + " | " + step);
            }
        }
        return out;
    }

    @Test
    void theTerminalRowsAreExactlyTheRuledOnes() {
        assertEquals(new TreeSet<>(TERMINAL.keySet()), terminal(CompatSpellingGuardTest.REGISTER), "a terminal row"
                + " of the spelling register differs from the frozen set: a new one (or one relabelled) needs its ruling"
                + " here; a gone one is deleted here");
        TERMINAL.forEach((row, ruling) -> assertTrue(ruling.matches(".*\\d{4}-\\d{2}-\\d{2}.*"),
                row + ": its ruling is dated"));
    }

    /** An open row relabelled STAYS, and a new RULED row, are each found. */
    @Test
    void aRelabelledRowIsFound() {
        List<String> planted = new ArrayList<>(CompatSpellingGuardTest.REGISTER);
        planted.add("lowering/Repr.java | Repr | 2 | STAYS");
        planted.add("sql/dialect/Planted.java | PLANTED | 1 | RULED");
        Set<String> found = terminal(planted);
        found.removeAll(TERMINAL.keySet());
        assertEquals(Set.of("lowering/Repr.java | Repr | STAYS", "sql/dialect/Planted.java | PLANTED | RULED"), found,
                "the freeze bites");
    }
}
