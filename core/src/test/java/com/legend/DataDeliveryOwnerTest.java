// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

import com.legend.testing.SourceFiles;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * THE DELIVERY IS THE DIALECT'S ALONE (the user, 2026-11-11: "Shouldn't that all be delegated to dialect to decide?"):
 * how a {@code data:} payload reaches a database -- a session table, the statement's own rows, one bound parameter --
 * is {@code SqlDialect.deliver}'s answer. The planner hands the payload to it in ONE place ({@code DataSources}) and
 * places the answer; no planner source makes a session table's statements or a payload's rows itself, nor reads a
 * declared delivery kind (until 2026-11-11 it switched on {@code SqlDialect.dataDelivery()} and built the rows).
 */
@Tag("census")
class DataDeliveryOwnerTest {

    @Test
    void thePlannerHoldsNoDeliveryDecision() throws Exception {
        List<Path> planner = SourceFiles.under("core/src/main/java/com/legend/planner");
        assertTrue(!planner.isEmpty(), "the planner's sources are not declared");
        int delivers = 0;
        for (Path f : planner) {
            if (!f.toString().endsWith(".java")) {
                continue;
            }
            String text = Files.readString(f, StandardCharsets.UTF_8);
            delivers += text.split("\\.deliver\\(", -1).length - 1;
            for (String decided : List.of(".dataTable(", ".inlineRows(", "DataDelivery")) {
                assertTrue(!text.contains(decided), f.getFileName() + " holds a delivery decision: " + decided);
            }
        }
        assertEquals(1, delivers, "the planner asks the dialect once, in DataSources");
    }
}
