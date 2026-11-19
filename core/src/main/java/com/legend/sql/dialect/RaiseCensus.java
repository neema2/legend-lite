// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import com.legend.sql.SqlExpr;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * PROBE, NOT PRODUCT (the compatibility program's homework H9, 2026-11-19; on the scratch branch
 * claude/compat-b1-homework only, never merged): every raise a dialect writes, its kind and the shape of each operand,
 * appended to the file the environment's {@code LEGEND_PROBE_RAISES} names (unset: nothing). It answers whether slice
 * 1.2's {@code Operand} (a typed literal per value kind, or Computed) can carry every operand {@link RaiseSite} holds.
 */
final class RaiseCensus {

    /** Where the census writes: the environment's {@code LEGEND_PROBE_RAISES}, else the probe's scratch file (the net's
     *  recording actions pass no environment; they run unsandboxed for the probe, --strategy=...=local). */
    private static final String FILE = System.getenv("LEGEND_PROBE_RAISES") != null
            ? System.getenv("LEGEND_PROBE_RAISES")
            : "/tmp/claude-0/-home-user-legend-lite/e322a567-cf95-5504-a839-fce3267656ec/scratchpad/raises/r";

    private RaiseCensus() {
    }

    static void note(String dialect, SqlExpr.RaiseText r) {
        if (FILE == null || FILE.isBlank()) {
            return;
        }
        StringBuilder line = new StringBuilder();
        line.append(dialect).append('\t').append(r.kind()).append('\t')
                .append(r.asData() != null ? "asData" : r.where() != null ? "placed" : "raise").append('\t')
                .append(r.operands().size());
        for (SqlExpr x : r.operands()) {
            line.append('\t').append(shape(x, 0));
        }
        line.append('\n');
        synchronized (RaiseCensus.class) {
            try {
                Files.writeString(Path.of(FILE + "." + ProcessHandle.current().pid()), line,
                        StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
            } catch (IOException | RuntimeException e) {
                // a sandboxed run cannot write there: the probe never breaks the run it measures
            }
        }
    }

    /** An operand's shape: a literal's record name (with a Float's or Decimal's digits as written), a cast of one
     *  as {@code Cast(<literal>-><type>)}, anything else {@code Computed:<record>}. */
    private static String shape(SqlExpr x, int depth) {
        String name = x.getClass().getSimpleName();
        if (name.endsWith("Lit")) {
            return name;
        }
        if (x instanceof SqlExpr.Cast c && depth == 0) {
            String inner = shape(c.value(), 1);
            return inner.startsWith("Computed") ? "Computed:Cast" : "Cast(" + inner + "->" + c.target() + ")";
        }
        return "Computed:" + name;
    }
}
