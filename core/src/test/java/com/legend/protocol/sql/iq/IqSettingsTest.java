// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol.sql.iq;

import com.legend.protocol.PureComposer;
import com.legend.protocol.spec.ValueSpecification;
import com.legend.protocol.sql.SqlTranslationException;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The options framework (docs/IQ_SQL_STUDY_2026_10_13.md §9.17; IQ island, 2026-10-14): the registry
 * ({@link IqOption}), the settings vector ({@link IqSettings}) -- its presets, the settings given at the database's and
 * the query's level ({@link IqSettings#resolve}), IQ's errors, what is refused. The answers are the iq-parity lanes'
 * (lite/options/*.yaml, judged by SQL Anywhere 17); these pin the rules the lanes cannot see.
 */
class IqSettingsTest {

    private static final IqSettings IQ = IqProfile.IQ16_DEFAULT.settings();
    private static final IqSettings SA = IqProfile.SQLA17_DEFAULT.settings();

    private static SqlTranslationException invalid(Runnable r) {
        SqlTranslationException e = assertThrows(SqlTranslationException.class, r::run);
        assertEquals(SqlTranslationException.Kind.INVALID, e.kind(), e.getMessage());
        return e;
    }

    private static SqlTranslationException refused(Runnable r) {
        SqlTranslationException e = assertThrows(SqlTranslationException.class, r::run);
        assertEquals(SqlTranslationException.Kind.UNSUPPORTED, e.kind(), e.getMessage());
        return e;
    }

    // ------------------------------------------------------------------ the registry and the presets

    @Test
    void theRegistrysNamesAreDistinctAndTheTier4NamesApart() {
        Set<String> seen = new HashSet<>();
        for (IqOption o : IqOption.values()) {
            assertTrue(seen.add(IqLexer.lower(o.spelling())), o.spelling());
            assertFalse(IqOption.ignored(o.spelling(), IqSettings.Base.IQ16), o.spelling());
            assertTrue(o.defaultFor(IqSettings.Base.IQ16) != null, o.spelling() + " has an IQ 16 value");
        }
        assertEquals(IqOption.DATE_ORDER, IqOption.named("Date_Order"));
        assertEquals(IqOption.CASE_SENSITIVE, IqOption.named("casesensitive"));
        assertNull(IqOption.named("chained"));
        assertTrue(IqOption.ignored("CHAINED", IqSettings.Base.SA17));
        // IQ's own options with no effect on a SELECT: IQ 16's alone
        assertTrue(IqOption.ignored("iqgovern_priority", IqSettings.Base.IQ16));
        assertFalse(IqOption.ignored("iqgovern_priority", IqSettings.Base.SA17));
        assertFalse(IqOption.ROW_COUNT.settable(IqSettings.Base.SA17));
        assertFalse(IqOption.MIN_NUMERIC_DIVISION_SCALE.settable(IqSettings.Base.SA17));
        assertFalse(IqOption.CASE_SENSITIVE.settable(IqSettings.Base.IQ16));
    }

    @Test
    void thePresetsAreABaseAndALogin() {
        IqSettings tds = IqProfile.IQ16_TDS.settings();
        IqSettings odbc = IqProfile.IQ16_ODBC.settings();
        assertTrue(IQ.caseSensitive() && IQ.blankPadding() && IQ.latin1() && IQ.iqDatabase());
        assertFalse(SA.caseSensitive() || SA.blankPadding() || SA.latin1() || SA.iqDatabase());
        assertEquals(126, IQ.precision());
        assertEquals(38, IQ.scale());
        assertEquals(30, SA.precision());
        assertEquals(6, SA.scale());
        assertFalse(tds.ansinull() || tds.quotedIdentifier() || tds.escapeCharacter());
        assertTrue(tds.tsqlVariables());
        assertEquals("MDY", tds.dateOrder());
        assertEquals("TDS", tds.value(IqOption.COMM_PROTOCOL));
        assertEquals("hh:nn:ss", odbc.timeFormat());
        assertEquals("yyyy-mm-dd hh:nn:ss.ssssss", odbc.timestampFormat());
        assertEquals(50, IQ.nearestCentury());
        assertEquals(7, IQ.firstDayOfWeek());
        assertTrue(IQ.divideByZeroError() && IQ.ansiSubstring());
        assertEquals("iq16-tds", tds.spelling());
        for (IqProfile p : IqProfile.values()) {
            p.settings().check();
        }
    }

    // ------------------------------------------------------------------ the settings given: IqSettings.resolve

    private static IqSettings.Setting db(String name, String value) {
        return new IqSettings.Setting(IqSettings.Level.DATABASE, name, value);
    }

    private static IqSettings.Setting q(String name, String value) {
        return new IqSettings.Setting(IqSettings.Level.QUERY, name, value);
    }

    private static IqSettings resolve(IqProfile p, IqSettings.Setting... settings) {
        return IqSettings.resolve(p, List.of(settings));
    }

    @Test
    void eachSettingAtItsLevel() {
        IqSettings s = resolve(IqProfile.IQ16_TDS, db("CaseSensitive", "Off"), db("precision", "40"),
                db("scale", "10"), q("date_order", "DMY"), q("chained", "Off"));
        assertFalse(s.caseSensitive());
        assertEquals(40, s.precision());
        assertEquals(10, s.scale());
        assertEquals("DMY", s.dateOrder());
        // the login's other overrides stay
        assertFalse(s.ansinull());
        assertEquals("iq16-tds(database.CaseSensitive='Off', database.precision='40', database.scale='10',"
                + " query.date_order='DMY', query.chained='Off')", s.spelling());
        s.check();
    }

    @Test
    void theDatabasesOptionIsPublicsWhichTheLoginHides() {
        assertEquals("DMY", resolve(IqProfile.IQ16_DEFAULT, db("date_order", "DMY")).dateOrder());
        assertEquals("MDY", resolve(IqProfile.IQ16_TDS, db("date_order", "DMY")).dateOrder());
        assertEquals("YMD", resolve(IqProfile.IQ16_DEFAULT, db("date_order", "DMY"), q("date_order", "ymd"))
                .dateOrder());
    }

    @Test
    void commProtocolIsTheLogin() {
        IqSettings tds = resolve(IqProfile.IQ16_DEFAULT, db("CommProtocol", "TDS"));
        assertFalse(tds.ansinull());
        assertTrue(tds.tsqlVariables());
        IqSettings cmdSeq = resolve(IqProfile.SQLA17_TDS, db("commprotocol", "CmdSeq"), q("date_order", "DMY"));
        assertTrue(cmdSeq.ansinull());
        assertEquals("DMY", cmdSeq.dateOrder());
        refused(() -> resolve(IqProfile.IQ16_ODBC, db("CommProtocol", "TDS")));
        refused(() -> resolve(IqProfile.IQ16_DEFAULT, db("CommProtocol", "HTTP")));
        // tsql_variables is an option of its own (a TDS login's On)
        assertTrue(resolve(IqProfile.IQ16_DEFAULT, q("tsql_variables", "On")).tsqlVariables());
    }

    @Test
    void theCollationAndTheCharsetFollowEachOther() {
        IqSettings utf8 = resolve(IqProfile.IQ16_DEFAULT, db("Collation", "UTF8BIN"));
        assertFalse(utf8.latin1());
        assertEquals("UTF-8", utf8.value(IqOption.CHAR_SET));
        IqSettings latin1 = resolve(IqProfile.SQLA17_DEFAULT, db("CharSet", "ISO_8859-1:1987"));
        assertTrue(latin1.latin1());
        assertEquals("ISO_BINENG", latin1.value(IqOption.COLLATION));
        utf8.check();
        latin1.check();
    }

    /** A real IQ 17.1 (a HANA Cloud data lake) reads back CaseSensitive On, BlankPadding On, Collation UTF8BIN,
     *  CharSet UTF-8 and CommProtocol CmdSeq: given at the database's level over the IQ base, each is read, under any
     *  preset of that base (CmdSeq drops a TDS preset's login), and none of them is refused. */
    @Test
    void anIq17DataLakesSettingsAreExpressed() {
        for (IqProfile p : List.of(IqProfile.IQ16_DEFAULT, IqProfile.IQ16_TDS)) {
            IqSettings s = resolve(p, db("CaseSensitive", "On"), db("BlankPadding", "On"), db("Collation", "UTF8BIN"),
                    db("CharSet", "UTF-8"), db("CommProtocol", "CmdSeq"));
            assertTrue(s.iqDatabase() && s.caseSensitive() && s.blankPadding(), s.spelling());
            assertFalse(s.latin1(), s.spelling());
            assertEquals("UTF8BIN", s.value(IqOption.COLLATION));
            assertEquals("UTF-8", s.value(IqOption.CHAR_SET));
            assertEquals("CmdSeq", s.value(IqOption.COMM_PROTOCOL));
            assertEquals(IqSettings.Client.DEFAULT, s.client());
            assertTrue(s.ansinull() && s.quotedIdentifier() && !s.tsqlVariables(), s.spelling());
            s.check();
        }
    }

    @Test
    void theErrorsAreIqs() {
        // at the query's level: a name of no option, a database property, CommProtocol, a PUBLIC-only option, an
        // option the base lacks -- IQ sets no such temporary option (-203)
        for (IqSettings.Setting s : List.of(q("date_ordr", "DMY"), q("CaseSensitive", "Off"), q("CommProtocol", "TDS"),
                q("precision", "40"), q("scale", "10"))) {
            SqlTranslationException e = invalid(() -> resolve(IqProfile.IQ16_DEFAULT, s));
            assertTrue(e.getMessage().contains("SQLCODE -203"), e.getMessage());
            assertEquals(0, e.line());
        }
        assertTrue(invalid(() -> resolve(IqProfile.SQLA17_DEFAULT, q("min_numeric_division_scale", "6")))
                .getMessage().contains("-203"));
        assertTrue(invalid(() -> resolve(IqProfile.SQLA17_DEFAULT, q("row_count", "0"))).getMessage()
                .contains("-203"));
        // at the database's level a name of no setting
        assertTrue(invalid(() -> resolve(IqProfile.IQ16_DEFAULT, db("date_ordr", "DMY"))).getMessage()
                .contains("-200"));
        // a value the setting does not take, at either level
        for (IqSettings.Setting s : List.of(q("date_order", "YDM"), q("ansinull", "1"), q("ansinull", "yes"),
                q("nearest_century", "101"), q("nearest_century", " 60 "), db("precision", "128"),
                db("date_order", "XYZ"), db("CaseSensitive", "maybe"))) {
            assertTrue(invalid(() -> resolve(IqProfile.IQ16_DEFAULT, s)).getMessage().contains("-201"),
                    s.toString());
        }
        assertTrue(invalid(() -> resolve(IqProfile.SQLA17_DEFAULT, q("nearest_century", "60.8"))).getMessage()
                .contains("-201"));
        // the research's D-Q2 ("IQ cuts a fraction") is refuted by IQ 17.1 (review 8, 2026-10-14): -201 there too
        assertTrue(invalid(() -> resolve(IqProfile.IQ16_DEFAULT, q("nearest_century", "60.8"))).getMessage()
                .contains("-201"));
        refused(() -> resolve(IqProfile.IQ16_DEFAULT, q("date_order", "DMY"), q("DATE_ORDER", "MDY")));
        // an option with no effect on a SELECT, at either level; IQ 16's own only under IQ 16
        assertTrue(resolve(IqProfile.IQ16_DEFAULT, q("iqgovern_priority", "2"), db("chained", "Off")).ansinull());
        assertTrue(invalid(() -> resolve(IqProfile.SQLA17_DEFAULT, q("iqgovern_priority", "2"))).getMessage()
                .contains("-203"));
    }

    @Test
    void valuesAsIqReadsThem() {
        assertEquals(60, resolve(IqProfile.IQ16_DEFAULT, q("nearest_century", "060")).nearestCentury());
        assertFalse(resolve(IqProfile.IQ16_DEFAULT, q("ansinull", "OFF")).ansinull());
        assertEquals("XYZ", resolve(IqProfile.SQLA17_DEFAULT, q("date_format", "XYZ")).dateFormat());
        assertEquals("DMY", resolve(IqProfile.IQ16_DEFAULT, q("date_order", "dmy")).dateOrder());
    }

    // ------------------------------------------------------------------ what is built

    @Test
    void whatIsNotBuiltIsRefusedByName() {
        for (List<IqSettings.Setting> given : List.of(List.of(q("conversion_error", "Off")),
                List.of(q("string_rtruncation", "Off")), List.of(q("min_numeric_division_scale", "4")),
                List.of(q("date_first_day_of_week", "1")), List.of(q("row_count", "5")),
                List.of(q("return_date_time_as_string", "On")), List.of(db("reserved_keywords", "LIMIT")),
                List.of(q("max_cartesian_result", "10")), List.of(q("floating_point_accumulator", "1")),
                List.of(q("sort_collation", "1252LATIN1")), List.of(db("BlankPadding", "Off")),
                List.of(db("Collation", "1252LATIN1")), List.of(db("AccentSensitive", "On")),
                List.of(db("precision", "20"), db("scale", "30")),
                List.of(db("Collation", "UTF8BIN"), db("CharSet", "ISO_8859-1:1987")))) {
            SqlTranslationException e = refused(() -> IqSettings.resolve(IqProfile.IQ16_DEFAULT, given).check());
            assertEquals(0, e.line(), given.toString());
        }
        // their defaults, and a Tier 3's default, are accepted
        resolve(IqProfile.IQ16_DEFAULT, q("conversion_error", "On"), q("min_numeric_division_scale", "6"),
                q("sort_collation", "internal"), q("max_cartesian_result", "100000000")).check();
        resolve(IqProfile.SQLA17_DEFAULT, db("BlankPadding", "On"), db("CaseSensitive", "On"),
                db("Collation", "ISO_BINENG")).check();
        // the formats are accepted here and refused where a value is written in them
        resolve(IqProfile.IQ16_DEFAULT, q("date_format", "DD/MM/YYYY"), q("time_format", "HH:NN")).check();
    }

    // ------------------------------------------------------------------ the translation under them

    private static final String T = "tb('s::DB.T')";

    private static String pure(String sql, IqSettings settings) {
        ValueSpecification vs = com.legend.testcases.SqlText.translate(sql, (source, column) -> {
            column.column("id", "Integer", false, "INTEGER");
            column.column("vc", "String", true, "VARCHAR(20)");
            column.column("nm", "Decimal", true, "NUMERIC(10,2)");
            column.column("dt", "StrictDate", true, "DATE");
        }, IqDialect.of(settings));
        return PureComposer.valueSpecification(vs, PureComposer.Style.STANDARD);
    }

    /** The IQ base's default preset with {@code settings} given. */
    private static IqSettings iq(IqSettings.Setting... settings) {
        return resolve(IqProfile.IQ16_DEFAULT, settings);
    }

    @Test
    void theOptionsTheTranslationReads() {
        // nearest_century
        assertTrue(pure("SELECT CAST('29/04/05' AS DATE) AS d", iq(q("nearest_century", "30"))).contains("2029-04-05"));
        assertTrue(pure("SELECT CAST('30/04/05' AS DATE) AS d", iq(q("nearest_century", "30"))).contains("1930-04-05"));
        // divide_by_zero_error Off: NULL, guarded where the divisor reads a column, refused where it is folded
        IqSettings off = iq(q("divide_by_zero_error", "Off"));
        assertFalse(pure("SELECT 1/0 AS q", off).contains("rem"));
        assertTrue(pure("SELECT 100 / (id - 3) AS q FROM " + T, off).contains("if("));
        refused(() -> pure("SELECT 1/(1-1) AS q", off));
        // ansi_substring Off: no error for a negative length
        pure("SELECT SUBSTRING('abc', 2, -1) AS s", iq(q("ansi_substring", "Off")));
        refused(() -> pure("SELECT SUBSTRING('abc', 2, -1) AS s", IQ));
        // first_day_of_week moves DATEPART's weekday only, and is refused under IQ's engine
        String sql = "SELECT DATEPART(dw, '2026-10-11') AS w, DATENAME(dw, '2026-10-11') AS n";
        assertNotEquals(pure(sql, IQ), pure(sql, iq(q("first_day_of_week", "3"))));
        refused(() -> pure("SELECT DATEPART(dw, dt) AS w FROM " + T, iq(q("first_day_of_week", "3"))));
        pure("SELECT DATEPART(dw, dt) AS w FROM " + T, resolve(IqProfile.SQLA17_DEFAULT, q("first_day_of_week", "3")));
        // a charset is CharSet's, whatever the base
        refused(() -> pure("SELECT CHAR(233) AS c", iq(db("Collation", "UTF8BIN"))));
        pure("SELECT CHAR(233) AS c", resolve(IqProfile.SQLA17_DEFAULT, db("Collation", "ISO_BINENG")));
    }

    @Test
    void aStringIsANumericColumnsTypeFirst() {
        // measured: compared with a NUMERIC(10,2) column, '0.005' is 0.01; with an expression, 0.005
        refused(() -> pure("SELECT id FROM " + T + " WHERE nm > '0.005'", SA));
        pure("SELECT id FROM " + T + " WHERE nm > '0.01'", SA);
        pure("SELECT id FROM " + T + " WHERE nm + 0 > '0.005'", SA);
    }

    // ------------------------------------------------------------------ review 8 (2026-10-14), measured on IQ 17.1

    private static IqSettings.Value str(String v) {
        return new IqSettings.Value(IqSettings.Value.Form.STRING, v);
    }

    private static IqSettings.Value num(String v) {
        return new IqSettings.Value(IqSettings.Value.Form.NUMBER, v);
    }

    /** The message of IQ's error for setting {@code name} to {@code v} at {@code scope} under {@code s}. */
    private static String error(IqSettings s, IqSettings.Scope scope, String name, IqSettings.Value v) {
        return invalid(() -> s.set(scope, name, v, IqSettings.SETTINGS)).getMessage();
    }

    @Test
    void anIntegerOptionsStringIsReadAsIqReadsIt() {
        IqSettings.Scope tmp = IqSettings.Scope.TEMPORARY;
        for (IqSettings base : List.of(IQ, SA)) {
            assertEquals(60, base.set(tmp, "nearest_century", str(" +60"), IqSettings.SETTINGS).nearestCentury());
            assertEquals(0, base.set(tmp, "nearest_century", str(""), IqSettings.SETTINGS).nearestCentury());
            for (String bad : List.of("60.0", "60 ", "-1", "+ 60", "++60", "6e1")) {
                assertTrue(error(base, tmp, "nearest_century", str(bad)).contains("-201"), bad);
            }
            // a fraction is -201 under either base: IQ 17.1 refutes the research's D-Q2
            assertTrue(error(base, tmp, "nearest_century", num("60.8")).contains("-201"));
        }
    }

    @Test
    void anOptionWithNoTemporaryLevelGetsItsOwnError() {
        IqSettings.Scope tmp = IqSettings.Scope.TEMPORARY;
        assertTrue(error(IQ, tmp, "reserved_keywords", str("LIMIT")).contains("-202"));
        assertTrue(error(SA, tmp, "reserved_keywords", str("LIMIT")).contains("-202"));
        assertTrue(error(IQ, tmp, "precision", num("30")).contains("-203"));
        assertTrue(error(IQ, IqSettings.Scope.USER, "precision", num("30")).contains("-202"));
        assertTrue(error(IQ, tmp, "blob_threshold", num("256")).contains("-203"));
        assertTrue(error(IQ, tmp, "isolation_level", num("9")).contains("-201"));
    }

    @Test
    void existingWithAPublicNameOfNoOptionIs200() {
        IQ.set(IqSettings.Scope.PUBLIC, "no_such_option_r8", num("1"), IqSettings.SETTINGS);
        assertTrue(invalid(() -> IQ.set(IqSettings.Scope.PUBLIC, "no_such_option_r8", num("1"), IqSettings.SETTINGS,
                true)).getMessage().contains("-200"));
    }
}
