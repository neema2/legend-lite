// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package org.finos.legend.lite.pct;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * F1.11 — PCT's best property, pinned (audit §4.3.1, verbatim: "There
 * is no comparison logic in Java anywhere in the PCT module... A 'fix'
 * that moved comparison into Java to dodge the rendering problem would
 * be far worse than what is there"). assertEquals stays in interpreted
 * Pure with both sides in Pure's own value domain; row order is the
 * database's. Zero sort/dedupe/tolerance spellings in this module;
 * stays zero.
 *
 * <p>ONE sanctioned indirection: {@code com.legend.testing.SourceWalk.inOrder} sorts the INPUT files Channel B
 * loads (Bazel workplan P3-08), so a load is the same on every filesystem; it compares no verdict. Any other call
 * from this module into a helper that sorts, dedupes or tolerates is the dodge this guard exists to stop.
 */
class PctDisciplineTest {

    private static final Pattern SITE = Pattern.compile(
            "Collections\\.sort\\(|\\.sorted\\(|\\.distinct\\(\\)"
            + "|\\.sort\\(|Math\\.abs\\(.*<|new TreeSet|new TreeMap"
            + "|Collections\\.max\\(|Collections\\.min\\("
            + "|new PriorityQueue|\\.stream\\(\\)\\.max\\("
            + "|\\.stream\\(\\)\\.min\\(");

    /** The same discipline for the module's PURE sources — the audit's
     * theater #9: the walk visited the directory holding
     * {@code pct_adapter.pure} and excluded it one line later with
     * {@code .endsWith(".java")}, so the ONE guard in the module could
     * not see the file holding finding G. Adapter-side reordering or
     * dedup would dodge a rendering problem exactly the same way. */
    private static final Pattern PURE_SITE = Pattern.compile(
            "->sort\\(|->distinct\\(|->removeDuplicates\\(");

    /** pct_adapter.pure size pin (the eval-ledger convention): the
     * adapter is the CONTRACT surface — growth is new adapter
     * compensation and needs a pin bump with a written justification.
     * Seeded 2026-08-18 (audit finding G resolution: dead widening
     * deleted, exactness relabel documented in place).
     * 320 → 400 (2026-08-27, R1 — ADAPTER_NECESSITY_CENSUS §5b): the
     * semantic dependency collector moved INTO pure (the M3 walk that
     * replaces the Java side's five discovery regexes — deleted in the
     * same commit). The growth is ANTI-compensation: discovery now
     * reads the tree the interpreter holds instead of pattern-guessing
     * printed text; the adapter PAIR shrinks net. (410: the collector's
     * cycle-guard block — differential run 1's 23 stack overflows on
     * cyclic captured-instance graphs, cured by chain-seen breaking.)
     * 410 → 485 (B9 upgrade, user-directed): captured lambda VALUES
     * close by construction (recursive environment resolution) and the
     * capture-safety proof gained its EXECUTABLE WALL (assertClosed —
     * the free-variable postcondition checked on every test). Wall
     * growth; the proof stopped being testimony. */
    // 485 -> 490 (2026-10-17, the MongoDB line's lane, docs/MONGODB_PLAN_2026_10_16.md §6): a lane whose results all
    // cross as Pure's TDS text carries a value as the one-column relation `value`; the adapter unwraps it by the
    // function's own return type (one `if`, five lines). ANTI-compensation: one representation crosses the wire for
    // every shape, and the Java bridge's typed value conversions are not needed on that route.
    // 490 -> 500 (2026-10-17, the lane's first run): a Date or Decimal value read from the text's lines by Pure's own
    // parsers, since the TDS reader builds neither from a cell (seven lines)
    // 2026-10-17, the MongoDB lane's burn-down (docs/MONGODB_PLAN_2026_10_16.md §11.4): the value relation's
    // Number and String cells read by the function's return type (the reader types a cell by its text), the
    // quoted String cells read whole across line breaks (quotedCells, closesCell, unquoted); 500 -> 540.
    // 2026-10-18, the text carrier (docs/PLAN_PATH_PROGRAM_2026_10_17.md §4, step 3): the TDS reader reads a relation
    // alone; an enumeration's values by extractEnumValue; every other value read by compileValueSpecification and
    // reactivate (readPureLiteral, seven lines, with its reason) -- Pure reads what the database spelled, no
    // comparison, no Java; 540 -> 557.
    // 2026-10-18, the DuckDB lane on the plan route (the plan path program's step 9): a Number-typed function's
    // value read by the wire header's type (value:Decimal[1]), where the text alone read a Decimal as a Float; the
    // Decimal arm reads a D the Number root spells (four lines); 557 -> 561.
    // 2026-10-20, the user's rule (as many tests as possible without hacks): legend-engine's grammar printer prints a
    // copy with updated properties without the values (^$p(k = v) as copy('', k)), so the adapter prints it itself
    // (the CopyWithUpdate marker, its print handler, its arms in the substitution, the roots walk and the
    // closedness wall), and the substitution runs when nothing was captured too; testFold, testFoldFiltering,
    // testFoldToMany; 561 -> 586.
    // 2026-10-20, object identity (docs/OBJECT_IDENTITY_2026_10_20.md): the product compares objects by identity, so
    // the adapter binds each captured object ONCE by a let at the front of the text (capturedObjects,
    // isCapturedObject, objectLets; the substitution keeps those names as variables) instead of printing each use as
    // its own ^C(...), which built a new object per use; testEqVarIdentity, testEqNonPrimitive; 586 -> 626.
    private static final int ADAPTER_MAX_LINES = 626;

    @Test
    void noJavaSideComparisonInPct() throws IOException {
        List<String> bad = new ArrayList<>();
        int javaScanned = 0;
        int pureScanned = 0;
        try (Stream<Path> files = com.legend.testing.SourceFiles.under("pct/src").stream()) {
            for (Path f : files
                    .filter(p -> p.toString().endsWith(".java")
                            || p.toString().endsWith(".pure"))
                    .filter(p -> !p.getFileName().toString()
                            .equals("PctDisciplineTest.java"))
                    .toList()) {
                boolean pure = f.toString().endsWith(".pure");
                if (pure) {
                    pureScanned++;
                } else {
                    javaScanned++;
                }
                String src = Files.readString(f);
                Matcher m = (pure ? PURE_SITE : SITE).matcher(src);
                while (m.find()) {
                    bad.add(f.getFileName() + ": " + m.group());
                }
                if (f.getFileName().toString().equals("pct_adapter.pure")) {
                    long lines = src.lines().count();
                    assertTrue(lines <= ADAPTER_MAX_LINES,
                            "pct_adapter.pure grew to " + lines
                            + " lines (pinned " + ADAPTER_MAX_LINES
                            + ") — new adapter compensation needs a"
                            + " deliberate pin bump with a written"
                            + " justification (audit finding G)");
                }
            }
        }
        // audit item 7: the guard asserts its own coverage
        assertTrue(javaScanned >= 9 && pureScanned >= 3,
                "PctDisciplineTest coverage DROPPED: scanned "
                + javaScanned + " java + " + pureScanned + " pure files"
                + " (floors 9/3) — the walk root or filters rotted");
        assertTrue(bad.isEmpty(),
                "comparison/ordering machinery appeared in the"
                + " PCT module: " + bad + " — comparison stays in"
                + " interpreted Pure (audit §4.3.1); do not dodge a"
                + " rendering problem by comparing or reordering"
                + " elsewhere");
    }
}
