// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.integration;

import com.legend.testbackend.StorelessRuntime;
import com.legend.testbackend.TestBackend;
import com.legend.testing.LaneFloor;
import com.legend.testing.Lean;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * A DATETIME'S TEXT INSIDE A STORE QUERY AT LEGEND-ENGINE'S DOOR, RUN (the edition program's step 8, 2026-11-26; the
 * user's rulings of that day, "datetime for engine should use engines database form" and, for a literal, "match
 * engine for engine"): where legend-engine's table holds the row ({@code edge.EngineTable}; its SQL is held on every
 * database by {@code edge.EngineDateTimeTextRowTest}) the database's own cast of the timestamp writes the text, as
 * legend-engine's {@code cast(%s as varchar)} does -- a space, the fraction the value holds, no zone. Run on each
 * lane's database (2026-11-26): the two lanes whose database legend-engine has each wrote the first form below for
 * every value; the lane whose database it does not have the second, the call's own rule -- lite's text of a computed
 * value, a literal's written text.
 */
@org.junit.jupiter.api.extension.ExtendWith(LaneFloor.class)
class EngineDateTimeTextTest {

    /** Each value as a TDS writes it, to the database's cast text of it and to lite's own text of it. */
    private static final Map<String, List<String>> TEXTS = Map.of(
            "2014-01-01T10:20:30.25", List.of("2014-01-01 10:20:30.25", "2014-01-01T10:20:30.250000000Z"),
            "2014-01-02T00:00:00", List.of("2014-01-02 00:00:00", "2014-01-02T00:00:00.000000000Z"),
            "2014-01-01T10:20:30.123456", List.of("2014-01-01 10:20:30.123456", "2014-01-01T10:20:30.123456000Z"));

    @Test
    void aDateTimesTextIsTheDatabasesCastWhereTheRowAnswers() throws Exception {
        TestBackend backend = LaneFloor.backend();
        String model = backend.withStorelessRuntime("");
        // the cast's text exactly where the row answers on the lane's database, the call's own rule where it does
        // not (the independent review of 2026-11-26: the row must be seen to answer)
        int form = com.legend.edge.PureText.legendEngineHas(backend.type()) ? 0 : 1;
        for (var e : TEXTS.entrySet()) {
            assertEquals(e.getValue().get(form), atEnginesDoor(model, backend, "|#TDS\n  at:DateTime\n  " + e.getKey()
                    + "\n#->extend(~t: r|$r.at->toOne()->toString())->select(~t)->from(" + StorelessRuntime.RUNTIME
                    + ")"), backend.name() + " " + e.getKey());
        }
        // a literal: the database's cast of the timestamp too where the row answers (legend-engine writes a literal
        // as a timestamp and casts it: PCT's testDateTimeToString fails there as legend-engine's does); its written
        // text, at its written precision, where it does not
        assertEquals(List.of("2014-01-01 00:00:00", "2014-01-01T00:00:00.000+0000").get(form), atEnginesDoor(model,
                backend, "|#TDS\n  i:Integer\n  1\n#->extend(~t: r|%2014-01-01T00:00:00.000+0000->toString())"
                        + "->select(~t)->from(" + StorelessRuntime.RUNTIME + ")"), backend.name());
    }

    private static String atEnginesDoor(String model, TestBackend backend, String query) throws Exception {
        return Lean.runInStyle(model, query, StorelessRuntime.RUNTIME, backend, com.legend.edge.PureText.PURE_V1)
                .rows().get(0).get(0).toString();
    }
}
