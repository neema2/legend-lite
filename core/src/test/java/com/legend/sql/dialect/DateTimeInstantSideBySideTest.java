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
                    Table V ( VID INTEGER PRIMARY KEY, VTS TIMESTAMP, VTXT VARCHAR(40) )
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
                        "12,2015-01-01 00:00:00,2016-06-06 06:06:06",
                        // V: instants before 1970 and at it, written with trailing zeros, each beside a text of it
                        "-----", "default", "V", "VID,VTS,VTXT", "20,1969-07-20 20:17:40.250,1969-07-20T20:17:40.25",
                        "21,1969-12-31 23:59:59.500,1969-12-31 23:59:59.5", "22,1970-01-01 00:00:00.000,1970-01-01T00:00:00.000",
                        "23,2014-01-01 10:20:30.50,2014-01-01T10:20:30.500", ""));
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

    private static final String V = "|#>{s::DB.V}#";
    private static final String UNIT = "meta::pure::functions::date::DurationUnit.";

    // ---- values computed at run, from a column (the independent audit's finding 4, 2026-11-26) ----------------------

    @Test
    void beforeNineteenSeventyAsAfter() throws Exception {
        table(V + "->filter(r|$r.VTS < %1970-01-01T00:00:00)->select(~[VID])->sort(~VID->ascending())", "20", "21");
        table(V + "->filter(r|$r.VTS == %1969-07-20T20:17:40.25)->select(~[VID])", "20");
        table(V + "->filter(r|$r.VTS == %1970-01-01T00:00:00)->select(~[VID])", "22");
        table(V + "->sort(~VTS->descending())->select(~[VID])", "23", "22", "21", "20");
    }

    @Test
    void adjustOverAColumnByWholeSecondsIsTheInstants() throws Exception {
        table(V + "->filter(r|$r.VTS->toOne()->adjust(1, " + UNIT + "SECONDS) == %1969-07-20T20:17:41.25)"
                + "->select(~[VID])", "20");
        table(V + "->filter(r|$r.VTS->toOne()->adjust(1, " + UNIT + "SECONDS) == %1970-01-01T00:00:00.5)"
                + "->select(~[VID])", "21");
        table(V + "->filter(r|$r.VTS->toOne()->adjust(-1, " + UNIT + "HOURS) == %1969-12-31T23:00:00)"
                + "->select(~[VID])", "22");
        table(V + "->filter(r|$r.VTS->toOne()->adjust(-45, " + UNIT + "MINUTES) == %2014-01-01T09:35:30.5)"
                + "->select(~[VID])", "23");
        table(V + "->filter(r|$r.VTS->toOne()->adjust(2, " + UNIT + "DAYS) == %2014-01-03T10:20:30.500)"
                + "->select(~[VID])", "23");
        table(V + "->filter(r|$r.VTS->toOne()->adjust(-1, " + UNIT + "WEEKS) == %1969-07-13T20:17:40.250)"
                + "->select(~[VID])", "20");
        // a sum of an adjust and its inverse is the instant it started from
        table(V + "->filter(r|$r.VTS->toOne()->adjust(90, " + UNIT + "SECONDS)->adjust(-90, " + UNIT
                + "SECONDS) == $r.VTS->toOne())->select(~[VID])->sort(~VID->ascending())", "20", "21", "22", "23");
    }

    /** dateDiff by seconds, minutes and hours is the time elapsed, in whole units toward zero (so half a second
     *  across midnight is none): the same over a column on both databases, before 1970 as after. */
    @Test
    void dateDiffOverAColumnIsTheSame() throws Exception {
        String diff = "->extend(~D: r|dateDiff($r.VTS->toOne(), ";
        table(V + "->filter(r|$r.VID == 21)" + diff + "%1970-01-01T00:00:00.4, " + UNIT + "SECONDS))->select(~[D])",
                "0");
        table(V + "->filter(r|$r.VID == 21)" + diff + "%1970-01-01T00:00:01.4, " + UNIT + "SECONDS))->select(~[D])",
                "1");
        table(V + "->filter(r|$r.VID == 21)" + diff + "%1970-01-01T00:00:01.5, " + UNIT + "SECONDS))->select(~[D])",
                "2");
        table(V + "->filter(r|$r.VID == 20)" + diff + "%1969-07-20T21:00:00, " + UNIT + "HOURS))->select(~[D])", "0");
        table(V + "->filter(r|$r.VID == 20)" + diff + "%1969-07-20T21:17:40.25, " + UNIT + "HOURS))->select(~[D])",
                "1");
        table(V + "->filter(r|$r.VID == 23)" + diff + "%2014-01-01T10:19:59.9, " + UNIT + "MINUTES))->select(~[D])",
                "0");
        table(V + "->filter(r|$r.VID == 23)" + diff + "%2014-01-01T10:19:30.5, " + UNIT + "MINUTES))->select(~[D])",
                "-1");
    }

    @Test
    void aTextOfAColumnParsedIsItsInstant_beforeNineteenSeventyToo() throws Exception {
        table(V + "->filter(r|$r.VTXT->toOne()->parseDate() == $r.VTS->toOne())->select(~[VID])"
                + "->sort(~VID->ascending())", "20", "21", "22", "23");
    }

    @Test
    void aVariantsStringReadAsADateTimeIsItsInstant() throws Exception {
        table(V + "->filter(r|$r.VTXT->toOne()->toVariant()->to(@DateTime) == $r.VTS->toOne())->select(~[VID])"
                + "->sort(~VID->ascending())", "20", "21", "22", "23");
    }

    /** A date built of whole parts is its instant. (Built with a FRACTIONAL second, or of fewer parts, it is the
     *  lowering's text carrier on every database -- {@code lowering.DateCtorRule}: Pure's own print of it -- which
     *  DuckDB reads as a date-time where it is compared and SQLite compares as the text it is: not this dialect's
     *  text, and not held here.) */
    @Test
    void aDateBuiltOfWholePartsIsItsInstant() throws Exception {
        table(V + "->filter(r|date(2014, 1, 1, 10, 20, $r.VID->toOne() + 7) == %2014-01-01T10:20:30.000)"
                + "->select(~[VID])", "23");
        table(V + "->filter(r|date(1969, 12, 31, 23, 59, $r.VID->toOne() + 30) < $r.VTS->toOne())"
                + "->select(~[VID])->sort(~VID->ascending())", "21", "22", "23");
    }

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
