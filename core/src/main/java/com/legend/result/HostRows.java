// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.result;

import com.legend.executionplan.ExecutionPlan;
import com.legend.json.Json;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;

/**
 * The rows a HOST's database answered a plan's statement with, handed back as JSON (docs/RESULT_BOUNDARY_PROGRAM_2026_10_26.md,
 * the end state of 2026-10-27: the tab's DuckDB, Python's), decoded by the plan's own columns and streamed to a sink --
 * so a host's answer is written by the writers legend-lite's server writes with. The host's rows are a JSON array of
 * objects, one per row, each member a result column's name and its cell exactly as the host's driver read it: an
 * integer's digits and a double's shortest digits as numbers (a non-finite double as its name in a string), a decimal,
 * a date-time and a date as their text, a boolean, a text, a structure as its JSON or as that JSON's text (a host whose
 * driver hands it a structure as text cannot tell it from a string; the plan's slot can), null. Each cell decodes by
 * {@link RowDecode}'s rule, the slot first; nothing is computed, a token no rule reads is refused by name.
 */
public final class HostRows {

    private HostRows() {
    }

    /** {@code rowsJson}, the host's rows of a statement whose result columns are {@code columns}, streamed to
     *  {@code sink} in batches. */
    public static void stream(String rowsJson, List<ExecutionPlan.Column> columns, ResultSink sink)
            throws IOException {
        Json.Node parsed;
        try {
            parsed = Json.parse(rowsJson);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("the host's rows do not parse (" + e.getMessage() + "); they begin: "
                    + head(rowsJson), e);
        }
        if (!(parsed instanceof Json.Arr rows)) {
            throw new IllegalArgumentException("the host's rows are not a JSON array of rows; they begin: "
                    + head(rowsJson));
        }
        Batcher batcher = new Batcher(columns, sink, Batcher.DEFAULT_CAPACITY);
        batcher.start();
        for (Json.Node row : rows.items()) {
            if (!(row instanceof Json.Obj r)) {
                throw new IllegalArgumentException("a host's row is not a JSON object of its columns: " + head(
                        Json.toCompact(row)));
            }
            List<Object> cells = RowDecode.cells(structuresRead(r, columns), columns);
            for (int i = 0; i < columns.size(); i++) {
                ExecutionPlan.Column c = columns.get(i);
                // a Variant handed as its JSON's text is that JSON, a JSON null a value (as the server reads one:
                // JdbcDecode's JSON arm), never the SQL null its parse would be
                if (c.type().equals(RowDecode.VARIANT) && r.fields().get(c.name()) instanceof Json.Str text) {
                    cells.set(i, Json.toCompact(Json.parse(text.value())));
                }
            }
            batcher.row(cells);
        }
        batcher.end();
    }

    /** {@code row} with each structured column's cell given as its JSON's text read as that JSON: by the column's slot
     *  (an array, a struct, a map, JSON), never by the text's look -- a tagged column's slot columns beside it too. A
     *  Variant's text is read by {@link #stream}. */
    private static Json.Obj structuresRead(Json.Obj row, List<ExecutionPlan.Column> columns) {
        LinkedHashMap<String, Json.Node> out = new LinkedHashMap<>(row.fields());
        for (ExecutionPlan.Column c : columns) {
            if (RowDecode.slotWord(c.slot()).equals("TAGGED")) {
                for (int i = 0; i < RowDecode.physicalColumns(List.of(c)) - 1; i++) {
                    read(out, RowDecode.taggedSlot(c, i));
                }
            } else if (!c.type().equals(RowDecode.VARIANT)) {
                read(out, c);
            }
        }
        return new Json.Obj(out);
    }

    private static void read(LinkedHashMap<String, Json.Node> row, ExecutionPlan.Column c) {
        String slot = RowDecode.slotWord(c.slot());
        if ((slot.equals("ARRAY") || slot.equals("STRUCT") || slot.equals("MAP") || slot.equals("JSON"))
                && row.get(c.name()) instanceof Json.Str text) {
            row.put(c.name(), Json.parse(text.value()));
        }
    }

    private static String head(String text) {
        return text.length() <= 200 ? text : text.substring(0, 200) + "...";
    }
}
