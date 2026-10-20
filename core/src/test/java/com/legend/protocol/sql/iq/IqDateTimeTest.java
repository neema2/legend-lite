// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol.sql.iq;

import com.legend.protocol.sql.SqlAst;
import com.legend.protocol.sql.SqlTranslationException;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * IQ island, 2026-10-14 (M1e; docs/IQ_SQL_STUDY_2026_10_13.md §9.7): {@link IqDateTime}, IQ's reading of a constant
 * date or timestamp string by DATE_ORDER and NEAREST_CENTURY, and its writing of one as text. Every expected value
 * below was measured on SQL Anywhere 17 (CAST of the string to TIMESTAMP, in an IQ-like database and a default one,
 * under each DATE_ORDER; two of M1e's probes, kept with the private research: 508 readings of this reader agree, none
 * differs).
 */
class IqDateTimeTest {

    private static final SqlAst.Pos AT = new SqlAst.Pos(1, 1);

    /** {@code text} read under YMD (iq16-default) and under MDY (iq16-tds), each as {@code "yyyy-mm-dd hh:nn:ss"} and
     *  its fraction, or null when the reader does not read it. */
    private static String[] both(String text) {
        return new String[] {read(text, IqProfile.IQ16_DEFAULT.settings()), read(text, IqProfile.IQ16_TDS.settings())};
    }

    private static String read(String text, IqSettings profile) {
        String[] r = IqDateTime.read(text, profile, AT);
        return r == null ? null : r[0] + " " + r[1];
    }

    @Test
    void theDateOrderDecidesThreeShortNumbers() {
        String[][] cases = {
                // text, YMD, MDY
                {"03/04/05", "2003-04-05 00:00:00", "2005-03-04 00:00:00"},
                {"3.4.05", "2003-04-05 00:00:00", "2005-03-04 00:00:00"},
                {"05-03-04", "2005-03-04 00:00:00", "2004-05-03 00:00:00"},
                {"1/2/3", "2001-02-03 00:00:00", "2003-01-02 00:00:00"},
                {"3/4/2005", null, "2005-03-04 00:00:00"},
                {"04.03.2005", null, "2005-04-03 00:00:00"},
                {"12/31/99", null, "1999-12-31 00:00:00"},
                {"99/12/31", "1999-12-31 00:00:00", null},
                {"13/04/05", "2013-04-05 00:00:00", null},
                {"1/1/0", null, "2000-01-01 00:00:00"},
                // a four-digit year first: every order alike, either separator at each place
                {"2005/3/4", "2005-03-04 00:00:00", "2005-03-04 00:00:00"},
                {"2005-03/04", "2005-03-04 00:00:00", "2005-03-04 00:00:00"},
                {"20050304", "2005-03-04 00:00:00", "2005-03-04 00:00:00"},
                {"990304", "1999-03-04 00:00:00", "1999-03-04 00:00:00"},
        };
        for (String[] c : cases) {
            assertEquals(Arrays.asList(c[1], c[2]), Arrays.asList(both(c[0])), c[0]);
        }
    }

    @Test
    void nearestCenturyIs50() {
        assertEquals("2049-01-01 00:00:00", read("49-01-01", IqProfile.IQ16_DEFAULT.settings()));
        assertEquals("1950-01-01 00:00:00", read("50-01-01", IqProfile.IQ16_DEFAULT.settings()));
        assertEquals("2049-01-01 00:00:00", read("1/1/49", IqProfile.IQ16_TDS.settings()));
        assertEquals("1950-01-01 00:00:00", read("1/1/50", IqProfile.IQ16_TDS.settings()));
        assertEquals("2005-03-04 00:00:00", read("Mar 4 05", IqProfile.IQ16_DEFAULT.settings()));
    }

    @Test
    void monthNames() {
        for (String text : new String[] {"Mar 4 2005", "March 4, 2005", "mar 4,2005", "MAR 04 2005", "4 Mar 2005",
                "04-MAR-2005", "4/Mar/2005", "2005 Mar 04", "2005-Mar-04", "2005 March 4"}) {
            assertEquals(Arrays.asList("2005-03-04 00:00:00", "2005-03-04 00:00:00"), Arrays.asList(both(text)), text);
        }
        // the forms IQ reads otherwise or by DATE_ORDER, and its other abbreviations (it matches any prefix)
        for (String text : new String[] {"4 Mar 05", "Sept 4 2005", "Ma 4 2005", "Mar4 2005",
                "Friday Mar 4 2005", "2005 4 Mar"}) {
            assertEquals(Arrays.asList(null, null), Arrays.asList(both(text)), text);
        }
    }

    /** IQ island, 2026-10-17 (the corpus lane; measured on IQ 17.1 in both its engines, and on SQL Anywhere 17): a
     *  year alone, or a year and a month, is the month's first day, in every DATE_ORDER; a time may follow. */
    @Test
    void aYearAloneOrAYearAndAMonth() {
        String[][] cases = {
                {"2010", "2010-01-01 00:00:00"},
                {" 2010 ", "2010-01-01 00:00:00"},
                {"0999", "0999-01-01 00:00:00"},
                {"2010-05", "2010-05-01 00:00:00"},
                {"2010/5", "2010-05-01 00:00:00"},
                {"2010.05", "2010-05-01 00:00:00"},
                {"2010 May", "2010-05-01 00:00:00"},
                {"May 2010", "2010-05-01 00:00:00"},
                {"Mar 2005", "2005-03-01 00:00:00"},
                {"2010 10:30", "2010-01-01 10:30:00"},
                {"2010T10:30", "2010-01-01 10:30:00"},
                {"2010-05 10:30", "2010-05-01 10:30:00"},
        };
        for (String[] c : cases) {
            assertEquals(Arrays.asList(c[1], c[1]), Arrays.asList(both(c[0])), c[0]);
        }
        // IQ's errors (-157), and two digits alone, which IQ reads as a century ('20' is 2000-01-01, '00' its error)
        for (String text : new String[] {"999", "5", "20101", "10000", "2010-13", "2010 Mai", "20"}) {
            assertEquals(Arrays.asList(null, null), Arrays.asList(both(text)), text);
        }
    }

    /** IQ island, 2026-10-17 (the corpus lane's SEDE; measured on IQ 17.1): numbers ending in a four-digit year are no
     *  date under DATE_ORDER YMD -- IQ's error wherever it converts one -- and are read under MDY and DMY. */
    @Test
    void numbersEndingInAFourDigitYearAreNoDateUnderYmd() {
        IqSettings ymd = IqProfile.IQ16_DEFAULT.settings();
        IqSettings mdy = IqProfile.IQ16_TDS.settings();
        for (String text : new String[] {"01/02/2018", "8/1/2018", "1-1-2009", "13/01/2018", "8-16-2011", "05/2010",
                "1/01/2012 12:00AM", "1.1.2009"}) {
            assertEquals(true, IqDateTime.unconvertible(text, ymd), text);
            assertEquals(false, IqDateTime.unconvertible(text, mdy), text);
        }
        for (String text : new String[] {"2018/01/02", "01/02/18", "20-Oct-17", "Oct 20 2017", "x", "", "2010"}) {
            assertEquals(false, IqDateTime.unconvertible(text, ymd), text);
        }
    }

    @Test
    void times() {
        String[][] cases = {
                {"2005-03-04 10:11:12:7", "2005-03-04 10:11:12.700000"},
                {"2005-03-04 10:11:12:123", "2005-03-04 10:11:12.123000"},
                {"2005-03-04 10:11:12,5", "2005-03-04 10:11:12.500000"},
                {"2005-03-04 10:11:12.", "2005-03-04 10:11:12"},
                {"2005-03-04 10:11:12.1234567", "2005-03-04 10:11:12.123456"},
                {"1999-12-31 23:59:59.9999995", "1999-12-31 23:59:59.999999"},
                {"2005-03-04 10:11:12 PM", "2005-03-04 22:11:12"},
                {"2005-03-04 12:00 AM", "2005-03-04 00:00:00"},
                {"2005-03-04 12:30 PM", "2005-03-04 12:30:00"},
                {"2005-03-04 13:11:12 PM", "2005-03-04 13:11:12"},
                {"2005-03-04 0:11:12 AM", "2005-03-04 00:11:12"},
                {"2005-03-04T10:11", "2005-03-04 10:11:00"},
                {"2005-03-04 10", "2005-03-04 10:00:00"},
                {"2005-3-4 1:2:3", "2005-03-04 01:02:03"},
                {"2005-03-04 24:00:00", "2005-03-05 00:00:00"},
                {"2005-12-31 24:00", "2006-01-01 00:00:00"},
        };
        for (String[] c : cases) {
            assertEquals(c[1], read(c[0], IqProfile.IQ16_DEFAULT.settings()), c[0]);
        }
        // IQ's errors, and spellings not read: a time alone, a zone, spaces inside a time, a tab
        for (String text : new String[] {"2005-03-04 25:00:00", "2005-03-04 24:00:01", "2005-03-04 13:00 AM",
                "2005-03-04 00:00 PM", "2005-03-04 10:60:00", "2005-03-04 001:02:03", "10:11:12",
                "2005-03-04 10:11:12+05:00", "2005-03-04 10: 11", "2005-3-4\t10:11", "2005-02-29", ""}) {
            assertNull(read(text, IqProfile.IQ16_DEFAULT.settings()), text);
        }
    }

    @Test
    void aYearBelow100IsRefused() {
        assertThrows(SqlTranslationException.class, () -> IqDateTime.read("0099-03-04", IqProfile.IQ16_DEFAULT.settings(), AT));
        assertEquals("0100-01-01 00:00:00", read("0100-01-01", IqProfile.IQ16_DEFAULT.settings()));
    }

    /** IQ island, 2026-10-14 (the merge of M1e): review 4's M2 refusal, moved here from M1a's reader -- SQL Anywhere 17
     *  drops the hours and minutes of a timestamp from 7911 on, so such a constant is refused by name; its seconds,
     *  a midnight and the years up to 7910 are read. */
    @Test
    void aTimedTimestampAfter7910IsRefused() {
        for (String text : new String[] {"7911-01-01 10:30", "9999-12-31 22:30:00", "8000-06-01 00:01",
                "8000/06/01 13:00"}) {
            assertThrows(SqlTranslationException.class, () -> IqDateTime.read(text, IqProfile.IQ16_DEFAULT.settings(), AT), text);
        }
        assertEquals("7910-12-31 10:30:00", read("7910-12-31 10:30", IqProfile.IQ16_DEFAULT.settings()));
        assertEquals("8000-01-01 00:00:59", read("8000-01-01 00:00:59", IqProfile.IQ16_DEFAULT.settings()));
        assertEquals("7911-01-01 00:00:00", read("7910-12-31 24:00", IqProfile.IQ16_DEFAULT.settings()));
        assertEquals("9999-12-31 00:00:00", read("9999-12-31", IqProfile.IQ16_DEFAULT.settings()));
    }

    @Test
    void aTimestampsTextHasTheProfilesPlaces() {
        assertEquals(3, IqDateTime.places(IqProfile.IQ16_DEFAULT.settings()));
        assertEquals(3, IqDateTime.places(IqProfile.IQ16_TDS.settings()));
        assertEquals(6, IqDateTime.places(IqProfile.IQ16_ODBC.settings()));
        assertEquals(23, IqDateTime.length(true, IqProfile.SQLA17_DEFAULT.settings()));
        assertEquals(26, IqDateTime.length(true, IqProfile.IQ16_ODBC.settings()));
        assertEquals(10, IqDateTime.length(false, IqProfile.IQ16_ODBC.settings()));
    }
}
