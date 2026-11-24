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

    /** The one catalog's rows: the user's ruling of 2026-11-21 (stage B's BE, "Agreed on both"). */
    private static final String ONE_CATALOG = "the one catalog, the user's ruling of 2026-11-21 (stage B's BE):"
            + " upstream's sentence wherever upstream has one";

    /** Every terminal row, "file | spelling | count | step", with its ruling: its count frozen with it (the review of
     *  5911e970e1, 2026-11-17: a terminal row keyed without its count grew unseen, since the census counts no terminal
     *  row). */
    static final Map<String, String> TERMINAL = Map.ofEntries(
            // the one catalog: upstream's compile sentences in error/CompileKind.java (stage B's BE)
            // (5 -> 6, 2026-11-21, the BE audit 2's S2: MAIN_TABLE_DATABASES_INCONSISTENT; for the user's word)
            // item 3's S8: assertError's "nothing was raised" text, Pure's semantics (the user's Q-B, 2026-11-22)
            Map.entry("testexecutionplan/HostJudgment.java | No error was thrown | 1 | RULED", "Q-B, the user's ruling of"
                    + " 2026-11-22 (\"Q-S8 okay\"): an assertError expecting legend-pure's unraised text passes, as upstream"),
            Map.entry("error/CompileKind.java | Can't find | 6 | RULED", ONE_CATALOG),
            Map.entry("error/CompileKind.java | Default values are supported only | 1 | RULED", ONE_CATALOG),
            Map.entry("error/CompileKind.java | Duplicated class mappings | 1 | RULED", ONE_CATALOG),
            Map.entry("error/CompileKind.java | Duplicated element | 1 | RULED", ONE_CATALOG),
            Map.entry("error/CompileKind.java | Duplicated mapping include | 1 | RULED", ONE_CATALOG),
            Map.entry("error/CompileKind.java | Invalid supertype | 1 | RULED", ONE_CATALOG),
            Map.entry("error/CompileKind.java | Invalid window frame boundary | 2 | RULED", ONE_CATALOG),
            Map.entry("error/CompileKind.java | arguments count ( | 1 | RULED", ONE_CATALOG),
            // (3 -> 4, 2026-11-21, the BE audit 3's S1: MISSING_STORE_SCHEMA; for the user's word)
            Map.entry("error/CompileKind.java | can't be found | 4 | RULED", ONE_CATALOG),
            Map.entry("error/CompileKind.java | trying to get an element at offset | 1 | RULED", ONE_CATALOG),
            Map.entry("error/CompileKind.java | variables count ( | 1 | RULED", ONE_CATALOG),
            Map.entry("error/ErrorKind.java | Cannot cast a collection of size | 2 | RULED", ONE_CATALOG),
            Map.entry("error/ErrorKind.java | Cannot divide | 1 | RULED", ONE_CATALOG),
            Map.entry("error/ErrorKind.java | Cast exception: | 2 | RULED", ONE_CATALOG),
            Map.entry("error/ErrorKind.java | Could not convert string ' | 2 | RULED", ONE_CATALOG),
            Map.entry("error/ErrorKind.java | Could not parse ' | 1 | RULED", ONE_CATALOG),
            Map.entry("error/ErrorKind.java | Division by zero | 1 | RULED", ONE_CATALOG),
            Map.entry("error/ErrorKind.java | Invalid day: | 1 | RULED", ONE_CATALOG),
            Map.entry("error/ErrorKind.java | Invalid window frame boundary | 2 | RULED", ONE_CATALOG),
            Map.entry("error/ErrorKind.java | Map keys can not be NULL | 1 | RULED", ONE_CATALOG),
            Map.entry("error/ErrorKind.java | Map keys must be unique | 1 | RULED", ONE_CATALOG),
            Map.entry("error/ErrorKind.java | Match failure: | 2 | RULED", ONE_CATALOG),
            Map.entry("error/ErrorKind.java | The map key list does not align | 1 | RULED", ONE_CATALOG),
            Map.entry("error/ErrorKind.java | Unable to compute sqrt of | 1 | RULED", ONE_CATALOG),
            Map.entry("error/ErrorKind.java | Unable to evaluate constraint | 1 | RULED", ONE_CATALOG),
            Map.entry("error/ErrorKind.java | Unsupported number of bits to shift | 1 | RULED", ONE_CATALOG),
            // stage B's B2b S3 (2026-11-24): legend-pure's default constraint sentence (DefaultConstraintHandler.java:90)
            Map.entry("error/ErrorKind.java | violated in the Class | 1 | RULED", "the user's ruling of 2026-11-24"
                    + " (\"keep errors close to pure when cheap/fast so don't have to reword everything on compatibility\";"
                    + " relayed by the orchestrator for B2b S3): legend-pure's default constraint sentence is the catalog's"),
            Map.entry("error/ErrorKind.java | add(set, index, value): index out of bounds | 1 | RULED", ONE_CATALOG),
            Map.entry("error/ErrorKind.java | can't be higher than the high bound | 1 | RULED", ONE_CATALOG),
            Map.entry("error/ErrorKind.java | range step must not be 0 | 1 | RULED", ONE_CATALOG),
            Map.entry("error/ErrorKind.java | trying to get an element at offset | 1 | RULED", ONE_CATALOG),
            Map.entry("result/RaiseWords.java | +0000 | 1 | RULED", ONE_CATALOG),
            // a product semantic sharing the spelling: the date pattern's letter Z (phase D's register, 2026-11-12)
            Map.entry("lowering/DateFormats.java | +0000 | 1 | STAYS", "D register 2026-11-12 (pattern letter Z)"),
            // INPUT read, no spelling written
            Map.entry("lowering/LiteralSpelling.java | +0000 | 1 | STAYS", "C5 step C 2026-11-17 (input read)"),
            Map.entry("lowering/Scalars.java | +0000 | 1 | STAYS", "D1.2 2026-11-15 (input read)"),
            Map.entry("protocol/ts/TsShow.java | +0000 | 1 | STAYS", "main 3a217086a (#81) 2026-11-14 (input read)"),
            // the user's ruling on H2's keywords and its session half
            Map.entry("sql/dialect/H2.java | ENGINE_LEXICON | 3 | RULED", "the user's ruling (H2's lexicon), listed 2026-11-16"),
            Map.entry("sql/dialect/H2.java | SET NON_KEYWORDS | 1 | RULED", "S5 2026-11-16 (the same ruling's session half)"),
            // TDSNull as INPUT and LANGUAGE: the #TDS cell token, Pure's class, the sentinel ^TDSNull() lowers to
            Map.entry("compiler/element/type/PlatformTypes.java | TDSNull | 1 | STAYS", "C5 step C, C-2 2026-11-17 (language)"),
            Map.entry("compiler/spec/TdsChecker.java | TDSNull | 1 | STAYS", "C5 step C, C-2 2026-11-17 (language)"),
            Map.entry("compiler/spec/TdsNullForms.java | TDSNull | 3 | STAYS", "C5 step C, C-2 2026-11-17 (language)"),
            Map.entry("compiler/spec/Typer.java | TDSNull | 4 | STAYS", "C5 step C, C-2 2026-11-17 (language)"),
            Map.entry("lowering/Scalars.java | TDSNull | 2 | STAYS", "C5 step C, C-2 2026-11-17 (the #TDS literal's input)"),
            // census J29 (E-Java's 7c754b9432, train 14): the platform assert native's default message, as two compiler
            // classes plan its body -- RULED STAYS by the orchestrator (2026-11-18): lite's assert native's default
            // sentence is lite's own, the same words as assert.pure's; frozen by the GUARD stream's re-cut on 1931d419b1
            // (compiler/spec/LambdaBodies.java | Assert failed | 1 | STAYS: gone 2026-11-23, item 3's S6 -- the user's Q-C =
            // C1 and Q-S6: an in-body guard with no message of its caller's raises its assert kind, a site the one catalog
            // words; the plan carries no sentence)
            // (compiler/spec/VerdictQueries.java | Assert failed | 1 | STAYS: gone 2026-11-18, PR #118's fix round, N2 --
            // the user's R1 ruling that day, "For 118 r1-r3 are fine": the product's default is lite's sentence, the
            // vector assert's worded by the run's words; legend-pure's "Assert failed" is the edge's alone)
            // (compiler/spec/VerdictQueries.java | TDSNull | STAYS: gone from the register 2026-11-17 on d-stage c37207858,
            // train 7's C-2 fix-forward)
            // generateSeedDataString's value (TestDataPlanner.seedString): STAYS, the orchestrator's ruling of 2026-11-17,
            // ratified by the user's R2 (2026-11-18, PR #118, "For 118 r1-r3 are fine"): stays for now; revisited as
            // ENGINE-mode output in the three-mode program
            Map.entry("testdatagen/TestDataPlanner.java | createRowIdentifier([ | 1 | STAYS",
                    "the seed string, STAYS: the orchestrator's 2026-11-17, ruled by the user's R2 2026-11-18"
                            + " (toRepresentation precedent)"),
            Map.entry("testdatagen/TestDataPlanner.java | createTableRowIdentifiers( | 1 | STAYS",
                    "the seed string, STAYS: the orchestrator's 2026-11-17, ruled by the user's R2 2026-11-18"
                            + " (toRepresentation precedent)"));

    /** The terminal rows of {@code register} as this test keys them, by the register's one terminal predicate (the one
     *  the census reads). */
    static Set<String> terminal(List<String> register) {
        Set<String> out = new TreeSet<>();
        for (String row : register) {
            String[] p = row.split("\\|");
            String step = p[3].trim();
            if (CompatSpellingGuardTest.TERMINAL_STEPS.contains(step)) {
                out.add(p[0].trim() + " | " + p[1].trim() + " | " + p[2].trim() + " | " + step);
            }
        }
        return out;
    }

    /** The rows of {@code rulings} whose ruling names no date. */
    static List<String> undated(Map<String, String> rulings) {
        return rulings.entrySet().stream().filter(e -> !e.getValue().matches(".*\\d{4}-\\d{2}-\\d{2}.*"))
                .map(Map.Entry::getKey).sorted().toList();
    }

    @Test
    void theTerminalRowsAreExactlyTheRuledOnes() {
        assertEquals(new TreeSet<>(TERMINAL.keySet()), terminal(CompatSpellingGuardTest.REGISTER), "a terminal row"
                + " of the spelling register differs from the frozen set: a new one (or one relabelled) needs its ruling"
                + " here; a gone one is deleted here");
        assertEquals(List.of(), undated(TERMINAL), "a terminal row's ruling is dated");
    }

    /** The terminal predicate itself is pinned: widening it (a JUDGE or HOOK row out of the count) closes rows by
     *  redefinition, so it moves only here, with its ruling. */
    @Test
    void theTerminalStepsArePinned() {
        assertEquals(Set.of("STAYS", "RULED", "HOOK-RULED"), CompatSpellingGuardTest.TERMINAL_STEPS,
                "the register's terminal steps: a step added here takes rows out of the census's count");
    }

    /** An open row relabelled STAYS, a new RULED row and a row relabelled HOOK-RULED are each found; an undated ruling
     *  is found. */
    @Test
    void aRelabelledRowIsFound() {
        List<String> planted = new ArrayList<>(CompatSpellingGuardTest.REGISTER);
        planted.add("lowering/Repr.java | Repr | 2 | STAYS");
        planted.add("sql/dialect/Planted.java | PLANTED | 1 | RULED");
        planted.add("lowering/VerdictSql.java | Repr | 1 | HOOK-RULED");
        Set<String> found = terminal(planted);
        found.removeAll(TERMINAL.keySet());
        assertEquals(Set.of("lowering/Repr.java | Repr | 2 | STAYS", "sql/dialect/Planted.java | PLANTED | 1 | RULED",
                "lowering/VerdictSql.java | Repr | 1 | HOOK-RULED"), found, "the freeze bites");
        // a terminal row grown: its count is in its key, so the grown row is a new one (the review of 5911e970e1)
        List<String> grown = new ArrayList<>(CompatSpellingGuardTest.REGISTER);
        grown.replaceAll(r -> r.replace("lowering/Scalars.java | TDSNull | 2 | STAYS",
                "lowering/Scalars.java | TDSNull | 5 | STAYS"));
        Set<String> grownFound = terminal(grown);
        grownFound.removeAll(TERMINAL.keySet());
        assertEquals(Set.of("lowering/Scalars.java | TDSNull | 5 | STAYS"), grownFound, "a terminal row grown is found");
        Map<String, String> undatedRuling = new java.util.TreeMap<>(TERMINAL);
        undatedRuling.put("sql/dialect/Planted.java | PLANTED | 1 | RULED", "the user said so");
        assertEquals(List.of("sql/dialect/Planted.java | PLANTED | 1 | RULED"), undated(undatedRuling),
                "an undated ruling is found");
    }
}
