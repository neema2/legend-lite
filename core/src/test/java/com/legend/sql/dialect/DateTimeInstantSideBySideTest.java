// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.testbackend.StorelessRuntime;
import com.legend.testbackend.TestBackends;
import com.legend.testing.Lean;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * A DATE-TIME IS COMPARED BY ITS INSTANT, on SQLite as on DuckDB (the equality rule's "a date-time by its instant",
 * register S399; the owner's ruling of 2026-11-26 on SQLite: "Shouldn't we normalize to at least pretend it's a date?
 * ... Is it not just a cheap string format?"). SQLite has no date-time type: its dialect holds one as ONE text per
 * instant ({@code 'YYYY-MM-DD HH:MM:SS'}, then a point and its fraction's digits only where it has one, no trailing
 * zero: {@code SQLite.timestampLit}), made at each source of a date-time text, so the text's own comparison is the
 * instant's. Until 2026-11-26 the fraction was held as written, and {@code '2014-01-01 00:00:00' =
 * '2014-01-01 00:00:00.000'} is false there: measured that day on SQLite, a table's cell {@code 00:00:00.000} was not
 * {@code ==} the literal {@code %2014-01-01T00:00:00}, two such values were two under {@code removeDuplicates}, and
 * the cell sorted after the literal's own row.
 *
 * <p>Each case is one expression run on SQLite and on DuckDB, side by side: the two answers are held equal, and equal
 * to the answer the instants give. The values are written, loaded and computed at different precisions: a table's
 * cells (a load), literals, a text read as a date-time (the one source read into the one text by SQL at run), and the
 * dialect's own date arithmetic. Not held here: a store's own column in a table the dialect did not load, which is
 * compared as it is held ({@code SQLite.timestampLit}'s note). A test of the dialect package, which names the
 * databases it compares (DatabaseOwners).
 */
class DateTimeInstantSideBySideTest {

    private static final List<DatabaseType> DATABASES = List.of(DatabaseType.SQLite, DatabaseType.DuckDB);

    /** Two tables of date-times on {@code type}: T's rows 1 and 2 are one instant written twice, 3 and 4 another; U's
     *  rows are those two instants written a third way, each beside a text of it written a fourth way, and one more.
     *  The ids run against the texts' own order, so an order by the text is not the order by the instant then the
     *  id. */
    private static String model(DatabaseType type) {
        return """
                ###Relational
                Database s::DB
                (
                    Table T ( ID INTEGER PRIMARY KEY, TS TIMESTAMP )
                    Table U ( UID INTEGER PRIMARY KEY, UTS TIMESTAMP, TXT VARCHAR(40) )
                )
                ###Connection
                RelationalDatabaseConnection s::Conn
                {
                    store: s::DB;
                    type: %s;
                    specification: LocalH2 { testDataSetupCSV: '%s'; };
                    auth: DefaultH2;
                }
                ###Runtime
                Runtime s::RT { mappings: [ ]; connections: [ s::DB: [ c1: s::Conn ] ]; }
                """.formatted(type.name(), String.join("\\n", "default", "T", "ID,TS",
                        "1,2014-01-01 00:00:00.000", "2,2014-01-01 00:00:00", "3,2014-01-01 00:00:00.500000",
                        "4,2014-01-01 00:00:00.5", "5,2014-01-02 10:30:00.123456", "6,2013-12-31 23:59:59.999",
                        "-----", "default", "U", "UID,UTS,TXT", "10,2014-01-01 00:00:00.0,2014-01-01T00:00:00.00",
                        "11,2014-01-01 00:00:00.50,2014-01-01 00:00:00.5000",
                        "12,2015-01-01 00:00:00,2016-06-06 06:06:06", ""));
    }

    /** {@code query}'s rows on {@code type}, each row its cells' text. */
    private static List<String> rows(DatabaseType type, String query, boolean stored) throws Exception {
        try (Connection c = TestBackends.of(type).open()) {
            Lean.Result r = stored ? Lean.run(model(type), query, "s::RT", c)
                    : Lean.run(StorelessRuntime.with("", type), query, StorelessRuntime.RUNTIME, c);
            List<String> out = new ArrayList<>();
            for (List<Object> row : r.rows()) {
                out.add(String.valueOf(row.size() == 1 ? row.get(0) : row));
            }
            return out;
        }
    }

    /** {@code query} answers {@code expected} on SQLite and on DuckDB. */
    private static void sideBySide(String query, boolean stored, String... expected) throws Exception {
        for (DatabaseType type : DATABASES) {
            assertEquals(List.of(expected), rows(type, query, stored), type + ": " + query);
        }
    }

    private static void table(String query, String... expected) throws Exception {
        sideBySide(query, true, expected);
    }

    private static void value(String query, String... expected) throws Exception {
        sideBySide(query, false, expected);
    }

    private static final String T = "|#>{s::DB.T}#";
    private static final String MONTHS = "meta::pure::functions::date::DurationUnit.MONTHS";
    private static final String MILLIS = "meta::pure::functions::date::DurationUnit.MILLISECONDS";

    // ---- == and != ------------------------------------------------------------------------------------------------

    @Test
    void equalIsTheInstants_aColumnAgainstALiteralWrittenAnyWay() throws Exception {
        for (String literal : List.of("%2014-01-01T00:00:00", "%2014-01-01T00:00:00.0",
                "%2014-01-01T00:00:00.000000000")) {
            table(T + "->filter(r|$r.TS == " + literal + ")->select(~[ID])->sort(~ID->ascending())", "1", "2");
        }
        for (String literal : List.of("%2014-01-01T00:00:00.5", "%2014-01-01T00:00:00.50",
                "%2014-01-01T00:00:00.500000")) {
            table(T + "->filter(r|$r.TS == " + literal + ")->select(~[ID])->sort(~ID->ascending())", "3", "4");
        }
    }

    @Test
    void notEqualIsTheInstants() throws Exception {
        table(T + "->filter(r|$r.TS != %2014-01-01T00:00:00.00)->select(~[ID])->sort(~ID->ascending())",
                "3", "4", "5", "6");
    }

    @Test
    void equalIsTheInstants_aComputedValueAgainstALiteral() throws Exception {
        value("|%2014-01-01T00:00:00.000->adjust(1, " + MONTHS + ") == %2014-02-01T00:00:00", "true");
        value("|%2014-01-01T00:00:00.5->adjust(500, " + MILLIS + ") == %2014-01-01T00:00:01", "true");
        value("|%2014-01-01T00:00:00.5->adjust(1, " + MONTHS + ") != %2014-02-01T00:00:00.50", "false");
    }

    @Test
    void equalIsTheInstants_aTextCastToADateTime() throws Exception {
        // a source read into the one text by SQL at run: a user's own text
        table("|#>{s::DB.U}#->filter(r|$r.TXT->toOne()->cast(@String)->parseDate() == $r.UTS->toOne())"
                + "->select(~[UID])->sort(~UID->ascending())", "10", "11");
    }

    // ---- in and contains ------------------------------------------------------------------------------------------

    @Test
    void inIsTheInstants() throws Exception {
        table(T + "->filter(r|$r.TS->in([%2014-01-01T00:00:00.0, %2014-01-01T00:00:00.50]))->select(~[ID])"
                + "->sort(~ID->ascending())", "1", "2", "3", "4");
    }

    @Test
    void containsIsTheInstants() throws Exception {
        value("|[%2014-01-01T00:00:00.000->adjust(1, " + MONTHS + ")->cast(@DateTime)]"
                + "->contains(%2014-02-01T00:00:00)", "true");
        value("|[%2014-01-01T00:00:00.000->adjust(1, " + MONTHS + ")->cast(@DateTime), %2015-01-01T00:00:00]"
                + "->contains(%2014-02-01T00:00:00.5)", "false");
        // a mixed list's date-time, computed, so the database compares it (a mixed list of LITERALS written at two
        // precisions is compared by the lowering before any database: its own matter, on every database alike)
        value("|[%2014-01-01T00:00:00.000->adjust(1, " + MONTHS + ")->cast(@DateTime), 1]"
                + "->contains(%2014-02-01T00:00:00)", "true");
    }

    // ---- < and > --------------------------------------------------------------------------------------------------

    @Test
    void orderComparisonsAreTheInstants() throws Exception {
        // by its text, '...00.000' is greater than '...00': row 1 is not after its own instant
        table(T + "->filter(r|$r.TS > %2014-01-01T00:00:00)->select(~[ID])->sort(~ID->ascending())", "3", "4", "5");
        table(T + "->filter(r|$r.TS >= %2014-01-01T00:00:00.000)->select(~[ID])->sort(~ID->ascending())",
                "1", "2", "3", "4", "5");
        table(T + "->filter(r|$r.TS < %2014-01-01T00:00:00.50)->select(~[ID])->sort(~ID->ascending())",
                "1", "2", "6");
        table(T + "->filter(r|$r.TS <= %2014-01-01T00:00:00.5)->select(~[ID])->sort(~ID->ascending())",
                "1", "2", "3", "4", "6");
    }

    // ---- sort -----------------------------------------------------------------------------------------------------

    @Test
    void sortIsByTheInstant_tiesByTheNextKey() throws Exception {
        // by the texts the order would be 6, 2, 1, 4, 3, 5
        table(T + "->sort([~TS->ascending(), ~ID->ascending()])->select(~[ID])", "6", "1", "2", "3", "4", "5");
        table(T + "->sort([~TS->descending(), ~ID->ascending()])->select(~[ID])", "5", "3", "4", "1", "2", "6");
    }

    @Test
    void aListSortsByTheInstant() throws Exception {
        value("|[%2014-01-01T00:00:00.5, %2014-01-01T00:00:00.000, %2013-12-31T23:59:59.999]->sort()"
                + "->map(d|$d == %2014-01-01T00:00:00)", "false", "true", "false");
    }

    @Test
    void theGreatestAndTheLeastAreByTheInstant() throws Exception {
        // by the texts as they were written, the greatest of rows 1 to 4 was row 3's '...00.500000'
        table(T + "->filter(r|$r.ID <= 4)->groupBy(~[], ~[hi: r|$r.TS : y|$y->max(), lo: r|$r.TS : y|$y->min()])"
                + "->extend(~[h: r|$r.hi == %2014-01-01T00:00:00.5, l: r|$r.lo == %2014-01-01T00:00:00])"
                + "->select(~[h, l])", "[true, true]");
    }

    // ---- removeDuplicates, distinct and groupBy -------------------------------------------------------------------

    @Test
    void removeDuplicatesIsByTheInstant() throws Exception {
        value("|[%2014-02-01T00:00:00.000, %2014-02-01T00:00:00, %2014-02-01T00:00:00.5, %2014-02-01T00:00:00.50]"
                + "->removeDuplicates()->size()", "2");
    }

    @Test
    void distinctIsByTheInstant() throws Exception {
        table(T + "->select(~[TS])->distinct()->groupBy(~[], ~[n: r|$r.TS : y|$y->count()])", "4");
    }

    @Test
    void groupByIsByTheInstant() throws Exception {
        table(T + "->groupBy(~[TS], ~[n: r|$r.ID : y|$y->count()])->sort(~TS->ascending())->select(~[n])",
                "1", "2", "2", "1");
    }

    // ---- joins ----------------------------------------------------------------------------------------------------

    @Test
    void aJoinsKeyIsTheInstant() throws Exception {
        table(T + "->join(#>{s::DB.U}#, meta::pure::functions::relation::JoinKind.INNER, {a, b|$a.TS == $b.UTS})"
                + "->select(~[ID, UID])->sort([~ID->ascending()])", "[1, 10]", "[2, 10]", "[3, 11]", "[4, 11]");
    }
}
