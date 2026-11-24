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
 * AN ABSTRACT DATE HOLDING A DAY PRINTS ITS DAY on the product road, as legend-pure prints it, on each lane's database
 * (DuckDB, H2, SQLite): {@code 2000-10-25}. Pinned 2026-11-24 (B2a item 4, held -- the B3 session's word, the user's
 * standing "if it's the right thing to do", option C) so that it cannot regress while a date with a time waits for its
 * own text: lite types every abstract-Date SQL slot TIMESTAMP, so a rule keyed on the slot printed this day as
 * {@code 2000-10-25T00:00:00.000000000Z} (measured that day); the nine-digit text of a date with a time lands once an
 * abstract Date carries its kind beside the value (B3's one-equality step), by the carried kind.
 */
@org.junit.jupiter.api.extension.ExtendWith(LaneFloor.class)
class AbstractDateDayTextTest {

    @Test
    void anAbstractDateHoldingADayPrintsItsDay() throws Exception {
        TestBackend backend = LaneFloor.backend();
        String model = backend.withStorelessRuntime("");
        try (Connection c = backend.open()) {
            assertEquals(List.of("2000-10-25", "2026-01-07"), column(Lean.run(model, "|#TDS\n  i:Integer, d:Date\n"
                    + "  1, 2000-10-25\n  2, 2026-01-07\n#->sort(~i->ascending())->extend(~t: r|$r.d->toOne()"
                    + "->toString())->select(~t)", StorelessRuntime.RUNTIME, c)), backend.name());
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
