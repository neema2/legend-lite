// Copyright 2026 Legend Lite Contributors
// SPDX-License-Identifier: Apache-2.0
package com.legend.equalitymeasure;

import com.legend.testing.Runfile;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.fail;

/**
 * THE POSITION MATRIX, PINNED (step 6B's slice 2, 2026-11-28; docs/records/edition-homework/step-6b's plan of
 * record, "the measure is the acceptance instrument"). The matrix asks one comparison in every place a comparison
 * can stand, on every in-process database, and {@link PositionReport} says which comparisons are not right
 * everywhere. This test runs it and requires its result to EQUAL {@code
 * tools/equality-measure/positions-baseline.tsv}, line for line: a comparison that gets worse fails here, and one
 * that gets better fails too until the baseline is regenerated in the same commit, so every move of the measure is a
 * line of a reviewed diff. The baseline is a guard file: a line that says a comparison got worse moves only with a
 * dated reason in the commit that moves it.
 *
 * <p>To regenerate: {@code bazel run //core:equality_measure -- --positions <out dir>}, then copy {@code <out
 * dir>/positions-kinds.tsv} over the baseline; {@code <out dir>/positions.md} says, place by place, why each line
 * is there.
 */
class EqualityPositionsPinTest {

    @Test
    void theMatrixIsAsPinned(@TempDir Path out) throws Exception {
        List<String> run = EqualityMeasure.positions(out, Runfile.property("equality.positions.model"),
                Runfile.property("equality.positions.store_model"), null);
        List<String> pinned = Files.readAllLines(Runfile.property("equality.positions.baseline"),
                StandardCharsets.UTF_8);
        if (run.equals(pinned)) {
            return;
        }
        List<String> gone = new ArrayList<>(pinned);
        gone.removeAll(run);
        List<String> come = new ArrayList<>(run);
        come.removeAll(pinned);
        fail("the position matrix's result is not the pinned one (tools/equality-measure/positions-baseline.tsv)."
                + "\nPinned and no longer so (" + gone.size() + "):\n  " + String.join("\n  ", gone)
                + "\nNot pinned (" + come.size() + "):\n  " + String.join("\n  ", come)
                + (gone.isEmpty() && come.isEmpty() ? "\n(the same lines in another order)" : "")
                + "\nIf every line is a comparison that got better, regenerate the baseline in the same commit (this"
                + " class's javadoc says how). The run's report, place by place:\n\n"
                + Files.readString(out.resolve("positions.md"), StandardCharsets.UTF_8));
    }
}
