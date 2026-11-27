// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.legend.testbackend.TestBackends;
import com.legend.testing.Lean;

import java.sql.Connection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

/**
 * A PROPERTY DECLARED AS THE ABSTRACT DATE TAKES ITS COLUMN'S KIND (the edition program's step 6; the user's ruling
 * of 2026-11-27, "You can do the error way if it's only for lite mode"): a day's column or a date-time's. A day
 * against a date-time is the mistake it is between two declared types, refused where the query is planned, for
 * "the same" and for "the later" alike -- a written day does not read as a date-time, because a day is no instant
 * ({@code $x.at == %2020-01-01} would mean "at midnight exactly"). The fix is the instant, or the day of the
 * date-time. Lite's own mode alone ({@code lowering.HeldKinds}).
 */
class DatesTypedByTheirColumnsTest {

    private static final String MODEL = """
            ###Pure
            Class p::Trade { id: Integer[1]; day: Date[1]; at: Date[1]; }
            ###Relational
            Database p::DB ( Table TRADE (ID INTEGER PRIMARY KEY, DAY DATE, AT TIMESTAMP) )
            ###Mapping
            Mapping p::M ( p::Trade: Relational { ~mainTable [p::DB] TRADE id: TRADE.ID, day: TRADE.DAY, at: TRADE.AT } )
            ###Connection
            RelationalDatabaseConnection p::C { store: p::DB; type: %s;
                specification: LocalH2 {
                    testDataSetupCSV: 'default\\nTRADE\\nID,DAY,AT\\n1,2020-01-01,2020-01-01 00:00:00\\n2,2020-01-02,2020-01-02 10:30:00\\n'; };
                auth: DefaultH2; }
            ###Runtime
            Runtime p::RT { mappings: [p::M]; connections: [ p::DB: [ c0: p::C ] ]; }
            """;

    @Test
    void aDayAgainstADateTimeBehindAnAbstractDateIsRefusedWhereTheQueryIsPlanned() throws Exception {
        String model = MODEL.formatted(TestBackends.platform().type());
        // one kind on both sides: compared
        Map<String, Object> holds = new LinkedHashMap<>();
        holds.put("|p::Trade.all()->filter(x|$x.day == %2020-01-02)->project(~[id: x|$x.id])", List.of(List.of(2L)));
        holds.put("|p::Trade.all()->filter(x|$x.day >= %2020-01-02)->project(~[id: x|$x.id])", List.of(List.of(2L)));
        holds.put("|p::Trade.all()->filter(x|$x.at == %2020-01-02T10:30:00)->project(~[id: x|$x.id])",
                List.of(List.of(2L)));
        holds.put("|p::Trade.all()->filter(x|$x.at >= %2020-01-02T00:00:00)->project(~[id: x|$x.id])",
                List.of(List.of(2L)));
        // a day's first day of its week or month is a day, and a day adjusted by years is a day: compared with a
        // day (the time-series documents' FILTER date >= '2024-01-01' and date > latest - 2y over a sampled line)
        holds.put("|p::Trade.all()->filter(x|$x.day->firstDayOfMonth() >= %2020-01-01)->project(~[id: x|$x.id])"
                + "->sort(~id->ascending())", List.of(List.of(1L), List.of(2L)));
        holds.put("|p::Trade.all()->filter(x|$x.day->firstDayOfWeek() == %2019-12-30)->project(~[id: x|$x.id])"
                + "->sort(~id->ascending())", List.of(List.of(1L), List.of(2L)));
        holds.put("|p::Trade.all()->filter(x|$x.day > $x.day->adjust(-2, meta::pure::functions::date::DurationUnit.YEARS))->project(~[id: x|$x.id])"
                + "->sort(~id->ascending())", List.of(List.of(1L), List.of(2L)));
        for (Map.Entry<String, Object> c : holds.entrySet()) {
            try (Connection connection = TestBackends.platform().open()) {
                Lean.Result r = Lean.established(model, c.getKey(), "p::RT", connection);
                assertEquals(c.getValue(), r.rows(), c.getKey() + "\n" + r.text());
            }
        }
        // a day against a date-time: refused, naming the two kinds and the fix; no day reads as midnight
        Map<String, String> refused = new LinkedHashMap<>();
        refused.put("|p::Trade.all()->filter(x|$x.at == %2020-01-01)->project(~[id: x|$x.id])",
                "== compares DateTime with StrictDate");
        refused.put("|p::Trade.all()->filter(x|$x.at >= %2020-01-01)->project(~[id: x|$x.id])",
                ">= compares DateTime with StrictDate");
        refused.put("|p::Trade.all()->filter(x|$x.day < $x.at)->project(~[id: x|$x.id])",
                "< compares StrictDate with DateTime");
        refused.put("|p::Trade.all()->filter(x|$x.day == %2020-01-01T00:00:00)->project(~[id: x|$x.id])",
                "== compares StrictDate with DateTime");
        refused.put("|p::Trade.all()->filter(x|$x.day == $x.at)->project(~[id: x|$x.id])",
                "== compares StrictDate with DateTime");
        refused.put("|p::Trade.all()->filter(x|$x.at->in([%2020-01-01, %2020-01-02]))->project(~[id: x|$x.id])",
                "in compares DateTime with StrictDate");
        for (Map.Entry<String, String> c : refused.entrySet()) {
            try (Connection connection = TestBackends.platform().open()) {
                RuntimeException e = assertThrows(RuntimeException.class,
                        () -> Lean.established(model, c.getKey(), "p::RT", connection), c.getKey());
                assertTrue(String.valueOf(e.getMessage()).contains(c.getValue())
                        && e.getMessage().contains("the types of the columns its two sides read")
                        && e.getMessage().contains("datePart()"), c.getKey() + ": " + e.getMessage());
            }
        }
    }

    /** A list holding both days and date-times, read through a map, reaches the database as one kind: refused by
     *  name, as an assert of it is; written where it is compared, each element is the kind it was written as. */
    @Test
    void daysAndDateTimesInOneListAreToldApartOrRefused() throws Exception {
        String model = MODEL.formatted(TestBackends.platform().type());
        Map<String, Boolean> written = new LinkedHashMap<>();
        written.put("|[%2020-01-01T00:00, %2020-01-02]->contains(%2020-01-02T00:00)", false);
        written.put("|[%2020-01-01T00:00, %2020-01-02]->contains(%2020-01-02)", true);
        for (Map.Entry<String, Boolean> c : written.entrySet()) {
            try (Connection connection = TestBackends.platform().open()) {
                Lean.Result r = Lean.established(model, c.getKey(), "p::RT", connection);
                assertEquals(List.of(List.of(c.getValue())), r.rows(), c.getKey() + "\n" + r.text());
            }
        }
        try (Connection connection = TestBackends.platform().open()) {
            String query = "|[%2020-01-01T00:00, %2020-01-02]->map(x|$x)->contains(%2020-01-02T00:00)";
            RuntimeException e = assertThrows(RuntimeException.class,
                    () -> Lean.established(model, query, "p::RT", connection), query);
            assertTrue(String.valueOf(e.getMessage()).contains("values holding both days and date-times are not"
                    + " compared"), e.getMessage());
        }
    }
}
