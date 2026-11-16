// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol.sql.iq;

import com.legend.protocol.PureComposer;
import com.legend.protocol.spec.ValueSpecification;
import com.legend.protocol.sql.SqlAst;
import com.legend.protocol.sql.SqlSyntaxException;
import com.legend.protocol.sql.SqlTranslationException;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * IQ's SET statements before an {@code #IQ{...}#} island's SELECT (docs/IQ_SQL_STUDY_2026_10_13.md §9.17; IQ island,
 * 2026-10-14, the options framework): {@link IqSetPrefix} and the levels {@link IqSettings#set} gives an option, IQ's
 * errors and the translation under the settings they make. The answers are the iq-parity lanes'
 * (lite/options/set_statements.yaml, judged by SQL Anywhere 17); these pin the rules the lanes cannot see.
 */
class IqSetPrefixTest {

    private static final IqSettings IQ = IqProfile.IQ16_DEFAULT.settings();
    private static final IqSettings SA = IqProfile.SQLA17_DEFAULT.settings();
    private static final SqlAst.Pos AT = new SqlAst.Pos(1, 1);

    private static IqSettings.Value word(String w) {
        return new IqSettings.Value(IqSettings.Value.Form.WORD, w);
    }

    private static IqSettings.Value string(String s) {
        return new IqSettings.Value(IqSettings.Value.Form.STRING, s);
    }

    private static IqSettings.Value number(String n) {
        return new IqSettings.Value(IqSettings.Value.Form.NUMBER, n);
    }

    private static final IqSettings.Value NONE = new IqSettings.Value(IqSettings.Value.Form.NONE, "");

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

    // ------------------------------------------------------------------ SET OPTION's levels

    @Test
    void theLevelsOfAnOption() {
        IqSettings tds = IqProfile.IQ16_TDS.settings();
        assertEquals("DMY", tds.set(IqSettings.Scope.TEMPORARY, "date_order", string("DMY"), AT).dateOrder());
        // the user's value replaces the connection's temporary one at once
        assertEquals("DMY", tds.set(IqSettings.Scope.USER, "date_order", word("dmy"), AT).dateOrder());
        // PUBLIC's is hidden by it, and by the login's
        assertEquals("MDY", tds.set(IqSettings.Scope.PUBLIC, "date_order", string("DMY"), AT).dateOrder());
        assertEquals("MDY", tds.set(IqSettings.Scope.PUBLIC_TEMPORARY, "date_order", string("DMY"), AT).dateOrder());
        assertEquals("DMY", IQ.set(IqSettings.Scope.PUBLIC, "date_order", string("DMY"), AT).dateOrder());
        // resets: a temporary value to the user's, the user's (and the temporary one) to PUBLIC's
        IqSettings u = IQ.set(IqSettings.Scope.USER, "date_order", string("DMY"), AT)
                .set(IqSettings.Scope.TEMPORARY, "date_order", string("MDY"), AT);
        assertEquals("MDY", u.dateOrder());
        assertEquals("DMY", u.set(IqSettings.Scope.TEMPORARY, "date_order", NONE, AT).dateOrder());
        assertEquals("YMD", tds.set(IqSettings.Scope.USER, "date_order", NONE, AT).dateOrder());
        refused(() -> IQ.set(IqSettings.Scope.PUBLIC, "date_order", NONE, AT));
        // a PUBLIC-only option
        assertEquals(40, IQ.set(IqSettings.Scope.PUBLIC, "precision", number("40"), AT).precision());
        assertTrue(invalid(() -> IQ.set(IqSettings.Scope.TEMPORARY, "precision", number("40"), AT)).getMessage()
                .contains("-203"));
        assertTrue(invalid(() -> IQ.set(IqSettings.Scope.USER, "scale", number("4"), AT)).getMessage()
                .contains("-202"));
    }

    @Test
    void aNameOfNoOption() {
        assertTrue(invalid(() -> IQ.set(IqSettings.Scope.TEMPORARY, "nope", number("1"), AT)).getMessage()
                .contains("-203"));
        assertTrue(invalid(() -> IQ.set(IqSettings.Scope.USER, "nope", number("1"), AT)).getMessage()
                .contains("-200"));
        // PUBLIC: IQ's own new option, which no SELECT reads; so a database property and CommProtocol
        assertEquals(IQ.caseSensitive(), IQ.set(IqSettings.Scope.PUBLIC, "CaseSensitive", string("Off"), AT)
                .caseSensitive());
        assertTrue(invalid(() -> IQ.set(IqSettings.Scope.TEMPORARY, "CaseSensitive", string("Off"), AT))
                .getMessage().contains("-203"));
        assertTrue(invalid(() -> IQ.set(IqSettings.Scope.TEMPORARY, "CommProtocol", string("TDS"), AT))
                .getMessage().contains("-203"));
        assertTrue(invalid(() -> SA.set(IqSettings.Scope.TEMPORARY, "row_count", number("5"), AT)).getMessage()
                .contains("-203"));
    }

    @Test
    void valuesAsSetOptionReadsThem() {
        assertEquals(60, IQ.set(IqSettings.Scope.TEMPORARY, "nearest_century", number("6e1"), AT).nearestCentury());
        assertEquals(60, IQ.set(IqSettings.Scope.TEMPORARY, "nearest_century", string("060"), AT)
                .nearestCentury());
        assertEquals(50, IQ.set(IqSettings.Scope.TEMPORARY, "nearest_century", number(".5e2"), AT)
                .nearestCentury());
        assertFalse(IQ.set(IqSettings.Scope.TEMPORARY, "ansinull", word("OFF"), AT).ansinull());
        invalid(() -> SA.set(IqSettings.Scope.TEMPORARY, "nearest_century", string(" 60 "), AT));
        invalid(() -> SA.set(IqSettings.Scope.TEMPORARY, "nearest_century", string("1e1"), AT));
        invalid(() -> SA.set(IqSettings.Scope.TEMPORARY, "first_day_of_week", number("0"), AT));
        invalid(() -> SA.set(IqSettings.Scope.TEMPORARY, "ansinull", string("yes"), AT));
        assertEquals("XYZ", SA.set(IqSettings.Scope.TEMPORARY, "date_format", string("XYZ"), AT).dateFormat());
    }

    // ------------------------------------------------------------------ the SET statements before a SELECT

    @Test
    void noSetNoPrefix() {
        assertNull(IqSetPrefix.read("SELECT 1", IQ));
        assertNull(IqSetPrefix.read("  -- settings\n SELECT settings FROM t", IQ));
        assertNull(IqSetPrefix.read("SELECT 1; SET OPTION x = 1", IQ));
    }

    @Test
    void theStatementsAreBlankedTheirLinesKept() {
        String sql = "SET TEMPORARY OPTION date_order = 'DMY';\n set temporary option ansinull = Off ;  SELECT x";
        IqSetPrefix.Prefix p = IqSetPrefix.read(sql, IQ);
        assertEquals("DMY", p.settings().dateOrder());
        assertFalse(p.settings().ansinull());
        assertEquals(sql.length(), p.select().length());
        assertEquals("\n", p.select().substring(40, 41));
        assertEquals("SELECT x", p.select().strip());
        assertEquals(sql.indexOf("SELECT"), p.select().indexOf("SELECT"));
    }

    @Test
    void eachStatementIsReadUnderTheOnesBefore() {
        // quoted identifiers off: the second statement's "DMY" is a string
        IqSetPrefix.Prefix p = IqSetPrefix.read("SET QUOTED_IDENTIFIER OFF; SET TEMPORARY OPTION date_order = \"DMY\";"
                + " SELECT 1", IQ);
        assertEquals("DMY", p.settings().dateOrder());
        assertFalse(p.settings().quotedIdentifier());
    }

    @Test
    void tsqlForms() {
        IqSettings s = IqSetPrefix.read("SET ANSI_NULLS OFF; SET DATEFIRST 1; SET DATEFORMAT dmy; SET ROWCOUNT 0;"
                + " SET NOCOUNT ON; SET TRANSACTION ISOLATION LEVEL READ COMMITTED; SET STATISTICS IO ON; SELECT 1",
                IQ).settings();
        assertFalse(s.ansinull());
        assertEquals(1, s.firstDayOfWeek());
        assertEquals("DMY", s.dateOrder());
        s.check();
        assertEquals(SqlTranslationException.Kind.UNSUPPORTED, assertThrows(SqlTranslationException.class,
                () -> IqSetPrefix.read("SET ROWCOUNT 5; SELECT 1", SA).settings().check()).kind());
        assertTrue(invalid(() -> IqSetPrefix.read("SET DATEFIRST 8; SELECT 1", IQ)).getMessage().contains("-201"));
        assertTrue(invalid(() -> IqSetPrefix.read("SET DATEFORMAT ydm; SELECT 1", IQ)).getMessage()
                .contains("-201"));
        assertTrue(invalid(() -> IqSetPrefix.read("SET LANGUAGE us_english; SELECT 1", IQ)).getMessage()
                .contains("-611"));
        for (String bad : List.of("SET ANSINULL 'OFF'; SELECT 1", "SET ROWCOUNT -1; SELECT 1",
                "SET ROWCOUNT 2147483648; SELECT 1", "SET ESCAPE_CHARACTER OFF; SELECT 1", "SET; SELECT 1",
                "SET TEMPORARY OPTION date_order == 'DMY'; SELECT 1", "SET TEMPORARY OPTION x = +1; SELECT 1",
                "SET TEMPORARY OPTION x = 'a' 'b'; SELECT 1", "SET TEMPORARY date_order = 'DMY'; SELECT 1")) {
            SqlSyntaxException e = assertThrows(SqlSyntaxException.class, () -> IqSetPrefix.read(bad, IQ), bad);
            assertFalse(e.unsupported(), bad);
            assertTrue(e.reason().contains("-131"), bad);
        }
        for (String unread : List.of("SET @x = 1; SELECT 1", "SET OPTION bob.date_order = 'DMY'; SELECT 1",
                "SET TEMPORARY OPTION date_order = @v; SELECT 1", "SET ROWCOUNT 5 SELECT 1",
                "SET TEMPORARY OPTION date_order = 'DMY'")) {
            SqlSyntaxException e = assertThrows(SqlSyntaxException.class, () -> IqSetPrefix.read(unread, IQ), unread);
            assertTrue(e.unsupported(), unread);
        }
    }

    @Test
    void anErrorIsPlacedAtItsStatement() {
        SqlTranslationException e = invalid(() -> IqSetPrefix.read("SET OPTION date_order = 'DMY';\n"
                + "  SET TEMPORARY OPTION nope = 1; SELECT 1", IQ));
        assertEquals(2, e.line());
        assertEquals(24, e.column());
    }

    // ------------------------------------------------------------------ the translation under them

    private static String pure(String sql, IqSettings settings) {
        ValueSpecification vs = com.legend.testcases.SqlText.translate(sql, (source, column) -> {
            column.column("id", "Integer", false, "INTEGER");
            column.column("dt", "StrictDate", true, "DATE");
        }, IqDialect.of(settings));
        return PureComposer.valueSpecification(vs, PureComposer.Style.STANDARD);
    }

    @Test
    void aSetStatementTranslatesAsTheSettingsGivenDo() {
        String select = "SELECT CAST('01/02/03' AS DATE) AS d";
        assertEquals(pure(select, IqSettings.resolve(IqProfile.IQ16_DEFAULT, List.of(new IqSettings.Setting(
                IqSettings.Level.QUERY, "date_order", "DMY")))),
                pure("SET TEMPORARY OPTION date_order = 'DMY'; " + select, IQ));
        assertNotEquals(pure(select, IQ), pure("SET DATEFORMAT dmy; " + select, IQ));
        // what a statement sets that is not built is refused where the SELECT is read
        refused(() -> pure("SET TEMPORARY OPTION conversion_error = 'Off'; " + select, IQ));
    }
}
