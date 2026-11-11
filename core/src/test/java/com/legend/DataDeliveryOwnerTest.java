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
 * how a {@code data:} payload reaches a database -- one bound parameter the database splits, or the statement's own
 * rows (the JSON pass, 2026-11-11: the session table went) -- is {@code SqlDialect.deliver}'s answer. The planner hands
 * the payload to it in ONE place ({@code DataSources}) and places the answer; no planner source builds a payload's rows
 * or its bound source itself, nor reads a declared delivery kind (until 2026-11-11 it switched on
 * {@code SqlDialect.dataDelivery()} and built the rows).
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
            // 2026-11-11, the JSON pass: the session table's spelling (SqlDialect's dataTable) left with the table;
            // the bound source's own spelling (".bound()") joins the rows'
            for (String decided : List.of(".inlineRows(", ".bound()", "DataDelivery")) {
                assertTrue(!text.contains(decided), f.getFileName() + " holds a delivery decision: " + decided);
            }
        }
        assertEquals(1, delivers, "the planner asks the dialect once, in DataSources");
    }
}
