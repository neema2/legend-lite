// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.integration;

import com.legend.testbackend.StorelessRuntime;
import com.legend.testbackend.TestBackend;
import com.legend.testing.LaneFloor;
import com.legend.testing.Lean;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * A JOIN SKIPS AN EMPTY ELEMENT ON THE PRODUCT ROAD (phase D's D1.3, 2026-11-15; register S313, the user's ruling of
 * 2026-11-10: "makeString skips empty elements"): a row's cells joined ({@code $r.values->makeString}) leave an empty
 * cell out, separator and all, on each lane's database (DuckDB, H2, SQLite). legend-engine prints the empty cell as
 * {@code TDSNull}: the compatibility edge's ({@code edge.PurePrintForms}), pinned beside it.
 */
@org.junit.jupiter.api.extension.ExtendWith(LaneFloor.class)
class CellTextsTest {

    private static final String ROWS = "#TDS\nx:Integer, y:String\n1, a\n2, null\n#";

    @Test
    void aJoinSkipsAnEmptyCell() throws Exception {
        TestBackend backend = LaneFloor.backend();
        String model = backend.withStorelessRuntime("");
        String query = "|" + ROWS + "->extend(~t: r|$r.values->makeString(','))->select(~t)";
        String wrapped = "|" + ROWS + "->extend(~t: r|$r.values->makeString('[', ',', ']'))->select(~t)";
        try (Connection c = backend.open()) {
            assertEquals(List.of("1,a", "2"), column(Lean.run(model, query, StorelessRuntime.RUNTIME, c)),
                    backend.name());
            assertEquals(List.of("[1,a]", "[2]"), column(Lean.run(model, wrapped, StorelessRuntime.RUNTIME, c)),
                    backend.name());
            assertEquals("1,2", Lean.run(model, "|[1, 2]->makeString(',')", StorelessRuntime.RUNTIME, c)
                    .rows().get(0).get(0).toString(), backend.name());
            // a single cell's text (S316 (a)): the empty value where the cell is; legend-engine's TDSNull at the edge
            String cell = "|" + ROWS + "->extend(~t: r|$r.get('y')->toString())->select(~t)";
            assertEquals(List.of("a", "null"), column(Lean.run(model, cell, StorelessRuntime.RUNTIME, c)),
                    backend.name());
            assertEquals(List.of("a", "TDSNull"), column(Lean.runInStyle(model, cell, StorelessRuntime.RUNTIME, backend,
                    com.legend.edge.PureText.UPSTREAM)), backend.name());
            // legend-engine's text of the empty cell, at the compatibility edge
            assertEquals(List.of("1,a", "2,TDSNull"), column(Lean.runInStyle(model, query, StorelessRuntime.RUNTIME,
                    backend, com.legend.edge.PureText.UPSTREAM)), backend.name());
        }
    }

    private static List<String> column(Lean.Result r) {
        List<String> out = new ArrayList<>();
        for (List<Object> row : r.rows()) {
            out.add(String.valueOf(row.get(0)));
        }
        return out;
    }
}
