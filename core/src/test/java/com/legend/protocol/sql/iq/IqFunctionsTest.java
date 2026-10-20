// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol.sql.iq;

import com.legend.protocol.PureComposer;
import com.legend.protocol.spec.CString;
import com.legend.protocol.spec.ValueSpecification;
import com.legend.protocol.sql.SqlAst;
import com.legend.protocol.sql.SqlSyntaxException;
import com.legend.protocol.sql.SqlToPure;
import com.legend.protocol.sql.SqlTranslationException;
import org.junit.jupiter.api.Test;

import java.math.BigInteger;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * M1d (docs/IQ_SQL_STUDY_2026_10_13.md §9.6): IQ's functions, CAST and CONVERT -- how {@link IqParser} reads their
 * special forms, the Pure {@link IqFunctions} writes for them, and each refusal by name. Their values are the iq-parity
 * lanes' (lite/m1d.yaml, judged by SQL Anywhere 17); these pin what the lanes cannot see: the translation's shapes and
 * the engine split.
 */
class IqFunctionsTest {

    /** A source column, as the typer reports it: name, Pure type, nullable, store type. */
    private record Col(String name, String type, boolean nullable, String store) {
    }

    private static final List<Col> COLUMNS = List.of(
            new Col("id", "Integer", false, "INTEGER"),
            new Col("sm", "Integer", true, "SMALLINT"),
            new Col("bg", "Integer", true, "BIGINT"),
            new Col("nm", "Decimal", true, "NUMERIC(10,2)"),
            new Col("d", "Float", true, "DOUBLE"),
            new Col("ch", "String", true, "CHAR(5)"),
            new Col("vc", "String", true, "VARCHAR(20)"),
            new Col("dt", "StrictDate", true, "DATE"),
            new Col("ts", "DateTime", true, "TIMESTAMP"));

    private static final String T = "tb('s::DB.T')";

    private static String pure(String sql) {
        return pure(sql, IqProfile.IQ16_DEFAULT.settings());
    }

    private static String pure(String sql, IqSettings profile) {
        ValueSpecification vs = SqlToPure.translate(sql, (source, column) -> COLUMNS.forEach(
                c -> column.column(c.name(), c.type(), c.nullable(), c.store())), IqDialect.of(profile));
        return PureComposer.valueSpecification(vs, PureComposer.Style.STANDARD);
    }

    private static SqlTranslationException refusal(String sql) {
        return refusal(sql, IqProfile.IQ16_DEFAULT.settings());
    }

    private static SqlTranslationException refusal(String sql, IqSettings profile) {
        SqlTranslationException e = assertThrows(SqlTranslationException.class, () -> pure(sql, profile));
        assertEquals(SqlTranslationException.Kind.UNSUPPORTED, e.kind(), sql + ": " + e.getMessage());
        return e;
    }

    private static SqlAst.Expr item(String sql) {
        SqlAst.Select s = (SqlAst.Select) IqParser.parse(sql, IqProfile.IQ16_DEFAULT.settings());
        return ((SqlAst.SelectExpr) s.items().get(0)).expr();
    }

    // ------------------------------------------------------------------ the reader

    @Test
    void castConvertAndTheSpecialValuesRead() {
        SqlAst.Cast c = assertInstanceOf(SqlAst.Cast.class, item("SELECT CAST(id AS DECIMAL(10, 2)) AS x"));
        assertEquals("NUMERIC", c.type().name());
        assertEquals(List.of(10, 2), c.type().modifiers());
        // a style-less CONVERT is a Cast its type name marks
        SqlAst.Cast v = assertInstanceOf(SqlAst.Cast.class, item("SELECT CONVERT(CHAR VARYING(5), id) AS x"));
        assertEquals(IqParser.CONVERT + "VARCHAR", v.type().name());
        assertEquals(List.of(5), v.type().modifiers());
        assertEquals("DOUBLE", ((SqlAst.Cast) item("SELECT CAST(id AS FLOAT(25)) AS x")).type().name());
        assertEquals("TIMESTAMP", ((SqlAst.Cast) item("SELECT CAST(id AS DATETIME) AS x")).type().name());
        // CURRENT DATE, CURRENT TIMESTAMP, CURRENT_TIMESTAMP: calls the reader names; f(*) has no arguments
        assertEquals("CURRENT DATE", ((SqlAst.FunctionCall) item("SELECT CURRENT DATE AS x")).name().get(0).text());
        assertEquals("CURRENT TIMESTAMP", ((SqlAst.FunctionCall) item("SELECT CURRENT_TIMESTAMP AS x")).name().get(0)
                .text());
        assertTrue(((SqlAst.FunctionCall) item("SELECT NOW(*) AS x")).args().isEmpty());
        // a date part is a keyword of its own: the constant of its full name, delimited or not
        SqlAst.FunctionCall dp = (SqlAst.FunctionCall) item("SELECT DATEPART([Dw], dt) AS x");
        assertEquals("weekday", ((SqlAst.StringLit) dp.args().get(0)).value());
        // a reserved word heads its call
        assertEquals("LEFT", ((SqlAst.FunctionCall) item("SELECT LEFT(vc, 2) AS x")).name().get(0).text());
    }

    @Test
    void iqsEngineReadsAStringAsANumberWithoutAnExponent() {
        // IQ island, 2026-10-15 (checklist 0.5, measured on IQ 17.1): white space at both ends, a sign, digits with one
        // point at most; '' and an exponent are IQ's -1009145
        assertEquals(new java.math.BigDecimal("1.555"), IqFunctions.iqNumberText(" 1.555\t"));
        assertEquals(new java.math.BigDecimal("12"), IqFunctions.iqNumberText("+12."));
        assertEquals(new java.math.BigDecimal("-0.9"), IqFunctions.iqNumberText("\n-.9\r"));
        for (String no : List.of("", " ", "1e2", "1.2.3", "- 1", "+", ".", "1,5", "\u00a01")) {
            assertEquals(null, IqFunctions.iqNumberText(no), no);
        }
    }

    @Test
    void castsSyntaxErrorsAreIqs() {
        for (String sql : List.of("SELECT CAST(1 AS VARCHAR(0)) AS x", "SELECT CAST(1 AS NUMERIC(2, 3)) AS x",
                "SELECT CAST(1 INT) AS x", "SELECT CAST(1 = 1 AS INT) AS x", "SELECT CONVERT(INT) AS x",
                "SELECT CONVERT(INT, 1, DEFAULT) AS x", "SELECT CURRENT_TIMESTAMP() AS x",
                "SELECT DATEPART('dd', dt) AS x", "SELECT DATEPART(yyyy, dt) AS x",
                "SELECT DATEADD(tz, 1, dt) AS x", "SELECT CAST('a' AS NCHAR(10 CHAR)) AS x")) {
            SqlSyntaxException e = assertThrows(SqlSyntaxException.class, () -> IqParser.parse(sql,
                    IqProfile.IQ16_DEFAULT.settings()), sql);
            assertFalse(e.unsupported(), sql);
            assertTrue(e.reason().contains("SQLCODE -131"), sql + ": " + e.reason());
        }
        // a style the island does not read, a domain, TIMESTAMP WITH TIME ZONE, a type of no value read (TIME, TINYINT,
        // unsigned, binary), a FLOAT's precision outside 1 to 53, a constant IQ cannot convert: refused by name (IQ
        // island, 2026-10-15: REAL and FLOAT(24) read now, IqToPureTest)
        // IQ island, 2026-10-17: a style IQ's engine raises -1009145 for is read past the reader (IqFunctions
        // .convertStyle), and refused by name under SQL Anywhere's front end (IqDialect.frontEndStyles)
        assertTrue(assertThrows(SqlSyntaxException.class, () -> IqDialect.of(IqProfile.IQ16_DEFAULT.settings())
                .parse("SELECT CONVERT(VARCHAR(10), CURRENT DATE, 23) AS x")).unsupported());
        for (String sql : List.of("SELECT CONVERT(INT, dt, 112) AS x",
                "SELECT CONVERT(VARCHAR(10), dt, 99999999999) AS x", "SELECT CAST(1 AS my_domain) AS x",
                "SELECT CAST(ts AS TIMESTAMP WITH TIME ZONE) AS x", "SELECT CURRENT TIME AS x",
                "SELECT CAST(id AS FLOAT(54)) AS x", "SELECT CAST(id AS FLOAT(0)) AS x", "SELECT CAST(id AS TIME) AS x",
                "SELECT CAST(id AS TINYINT) AS x", "SELECT CAST(id AS INT UNSIGNED) AS x",
                "SELECT CAST(id AS VARBINARY(4)) AS x", "SELECT CAST('abc' AS INT) AS x",
                "SELECT CAST('40000' AS SMALLINT) AS x", "SELECT CAST(2 AS BIT) AS x",
                "SELECT CAST('2026-02-30' AS DATE) AS x", "SELECT CAST(12345 AS CHAR(3)) AS x",
                "SELECT CAST(1000.5 AS NUMERIC(3, 0)) AS x", "SELECT CONVERT(DATE, 'not a date') AS x")) {
            assertTrue(assertThrows(SqlSyntaxException.class, () -> IqParser.parse(sql, IqProfile.IQ16_DEFAULT.settings()))
                    .unsupported(), sql);
        }
        // IQ island, 2026-10-15 (checklist 0.5, measured on IQ 17.1): what only SQL Anywhere's front end raises for --
        // a value its rounding takes past a NUMERIC's digits, a string longer than its target -- is refused there, and
        // read under IQ's engine, which truncates and cuts
        String[][] frontEndOnly = {{"SELECT CAST(999.5 AS NUMERIC(3, 0)) AS x", "floor("},
                {"SELECT CAST('abcdef' AS VARCHAR(3)) AS x", "left(3)"}};
        for (String[] c : frontEndOnly) {
            assertTrue(assertThrows(SqlSyntaxException.class, () -> IqDialect.of(IqProfile.IQ16_DEFAULT.settings())
                    .parse(c[0])).unsupported(), c[0]);
            assertTrue(pure(c[0] + " FROM " + T).contains(c[1]), c[0]);
        }
    }

    @Test
    void aNameOfNoFunctionOfIqsIsItsErrorInIqsOrder() {
        // IQ island, 2026-10-17 (measured on IQ 17.1): -265 once its arguments are read; a grouped query's column
        // outside the GROUP BY first (-149); a syntax error in its arguments first of all (-131)
        assertTrue(assertThrows(SqlTranslationException.class, () -> pure("SELECT STRING_AGG(vc, ',') AS a FROM " + T))
                .getMessage().contains("-265"));
        assertTrue(assertThrows(SqlTranslationException.class, () -> pure("SELECT sm, STRING_AGG(vc, ',') AS a FROM "
                + T + " GROUP BY sm")).getMessage().contains("-149"));
        assertTrue(assertThrows(SqlTranslationException.class, () -> pure("SELECT COUNT(*) AS n, STDEV(id) AS s FROM "
                + T)).getMessage().contains("-149"));
        assertTrue(assertThrows(RuntimeException.class, () -> pure("SELECT STRING_AGG(DATEPART(yyyy, dt), ',') AS a FROM "
                + T)).getMessage().contains("-131"));
        // the aggregates' forms after the name stay refused by name, as -265
        assertTrue(assertThrows(RuntimeException.class, () -> pure("SELECT STRING_AGG(vc, ',') WITHIN GROUP (ORDER BY"
                + " id) AS a FROM " + T)).getMessage().contains("-265"));
    }

    @Test
    void otherDialectsFormsIqRaises154ForAreRefusedBeforeTranslation() {
        for (String sql : List.of("SELECT CHARINDEX('b', vc, 2) AS x FROM " + T, "SELECT ROUND(nm, 1, 1) AS x FROM "
                + T, "SELECT LOG(2, 8) AS x", "SELECT SUBSTRING('abc', 1, -1) AS x")) {
            SqlTranslationException e = assertThrows(SqlTranslationException.class, () -> IqDialect.of(
                    IqProfile.IQ16_DEFAULT.settings()).parse(sql), sql);
            assertEquals(SqlTranslationException.Kind.UNSUPPORTED, e.kind(), sql);
        }
        // a wrong count of arguments is IQ's -154 before translation
        assertEquals(SqlTranslationException.Kind.INVALID, assertThrows(SqlTranslationException.class,
                () -> IqDialect.of(IqProfile.IQ16_DEFAULT.settings()).parse("SELECT LEFT('abc') AS x")).kind());
    }

    // ------------------------------------------------------------------ strings

    @Test
    void substringReadsAnsiVirtualPositions() {
        // SQL Anywhere's front end
        IqSettings sa = IqProfile.SQLA17_DEFAULT.settings();
        String p = pure("SELECT SUBSTRING(vc, sm, 3) AS s FROM " + T, sa);
        // a start below 1: the positions from 1 to start + length - 1
        assertTrue(p.contains("substring(1, "), p);
        assertTrue(p.contains("< 2"), p);
        // a length that may be negative is IQ's -1090 where computed
        assertTrue(pure("SELECT SUBSTRING(vc, 1, sm) AS s FROM " + T, sa).contains("cast(@String)->toOne()"));
        assertFalse(pure("SELECT SUBSTRING(vc, 1, 3) AS s FROM " + T, sa).contains("cast(@String)->toOne()"));
        // without a length: the largest INTEGER
        assertTrue(pure("SELECT SUBSTR(vc, 2) AS s FROM " + T, sa).contains("2147483647"));
        // IQ island, 2026-10-15 (measured on IQ 17.1): IQ's engine has no virtual positions -- a start of 0 is 1, a
        // negative one counts from the end -- and a start beyond 32,767 either way is NULL (IqFunctions.substringIq)
        String iq = pure("SELECT SUBSTRING(vc, sm, 3) AS s FROM " + T);
        assertTrue(iq.contains("> 32767") && iq.contains("< -32767"), iq);
        assertTrue(pure("SELECT SUBSTRING(vc, 1, sm) AS s FROM " + T).contains("cast(@String)->toOne()"));
    }

    @Test
    void countsLiteComputesIn64BitsAreReadAndCheckedAtRunTime() {
        // IQ island, 2026-10-17 (the corpus lane; measured on IQ 17.1): a count or a position computed in 64 bits, or
        // a BIGINT, is read; one past 32 bits is refused by name where the row computes it (IQ is not consistent
        // there), and Postgres's dialect narrows it to its substr's, left's, right's and repeat's 32-bit integer
        for (String sql : List.of("SELECT SUBSTRING(vc, bg, 1) AS s FROM " + T,
                "SELECT LEFT(vc, sm + 1) AS s FROM " + T, "SELECT REPEAT(vc, id * 2) AS s FROM " + T,
                "SELECT LEFT('abc', row_num) AS s FROM sa_rowgenerator(1, 3)")) {
            assertFalse(pure(sql).isEmpty(), sql);
        }
        assertTrue(pure("SELECT SUBSTRING(vc, bg, 1) AS s FROM " + T).contains("past 32 bits"));
        // IQ island, 2026-10-17: DuckDB's and Postgres's chr are given a 32-bit integer, so a computed code is read; a
        // code that may be 0 is refused where a row is 0 (a NUL), a constant 0 before translating
        assertTrue(pure("SELECT CHAR(sm + 1) AS c FROM " + T).contains("NUL character"));
        assertTrue(pure("SELECT CHAR(sm) AS c FROM " + T).contains("NUL character"));
        assertTrue(refusal("SELECT CHAR(0) AS c FROM " + T).getMessage().contains("NUL"));
    }

    @Test
    void trimDropsWhatAnIso88591DatabaseTrims() {
        String p = pure("SELECT TRIM(vc) AS t FROM " + T);
        // the line feed apart: Postgres's lowering reads a bracket expression newline-sensitively
        assertTrue(p.contains("regexpReplace('(\\\\n|[\\\\t-\\\\r ])+$'"), p);
        assertTrue(p.contains("regexpReplace('^(\\\\n|[\\\\t-\\\\r ])+'"), p);
        assertFalse(p.contains("trim("), p);
        assertTrue(pure("SELECT TRIM(vc, ']-^') AS t FROM " + T).contains("[\\\\]\\\\-\\\\^]"));
        // IQ island, 2026-10-14 (natives): a UTF-8 database trims the same six (measured), a set without case
        assertEquals(p, pure("SELECT TRIM(vc) AS t FROM " + T, IqProfile.SQLA17_DEFAULT.settings()));
        assertTrue(pure("SELECT LTRIM(vc, 'ab') AS t FROM " + T, IqProfile.SQLA17_DEFAULT.settings()).contains("^([aAbB])+"));
    }

    @Test
    void matchingIsCaseSensitiveInAnIqDatabaseAndAsciiCaselessInSqlAnywheres() {
        assertTrue(pure("SELECT REPLACE(vc, 'a', 'b') AS r FROM " + T).contains("replace('a', 'b')"));
        // IQ island, 2026-10-14 (natives): a sqla17 profile's matching ignores the ASCII letters' case, composed
        String replace = pure("SELECT REPLACE(vc, 'a.b', 'x\\\\y') AS r FROM " + T, IqProfile.SQLA17_DEFAULT.settings());
        assertTrue(replace.contains("regexpReplace('[aA]\\\\.[bB]', 'x\\\\\\\\y', true)"), replace);
        assertTrue(pure("SELECT REPLACE(vc, '1.', 'x') AS r FROM " + T, IqProfile.SQLA17_DEFAULT.settings())
                .contains("replace('1.', 'x')"));
        assertTrue(refusal("SELECT REPLACE(vc, vc, 'x') AS r FROM " + T, IqProfile.SQLA17_DEFAULT.settings()).getMessage()
                .contains("no constant"));
        for (String sql : List.of("SELECT CHARINDEX('a', vc) AS r FROM " + T,
                "SELECT LOCATE(vc, 'a', -1) AS r FROM " + T, "SELECT PATINDEX('%a%', vc) AS r FROM " + T)) {
            String p = pure(sql, IqProfile.SQLA17_DEFAULT.settings());
            assertTrue(p.contains("replace('A', 'a')") && !p.contains("toLower"), p);
            assertFalse(pure(sql).contains("replace('A', 'a')"), sql);
        }
        // UPPER and LOWER fold the 26 ASCII letters only, in every profile
        String upper = pure("SELECT UPPER(vc) AS u, LCASE(vc) AS l FROM " + T);
        assertTrue(upper.contains("replace('a', 'A')") && upper.contains("replace('z', 'Z')")
                && upper.contains("replace('Z', 'z')") && !upper.contains("toUpper"), upper);
    }

    @Test
    void theAsciiFoldAndTheCaselessPattern() {
        // IQ island, 2026-10-14 (natives)
        assertEquals("[aA]\\.[bB]\\[1é", IqFunctions.caselessPattern("a.B[1é"));
        assertEquals(new CString("1 é_"), IqFunctions.asciiFold(new CString("1 é_"), false));
        String folded = PureComposer.valueSpecification(IqFunctions.asciiFold(new CString("aB"), true),
                PureComposer.Style.STANDARD);
        assertEquals(26, folded.split("replace\\(", -1).length - 1, folded);
    }

    @Test
    void numericQuotientsAreSqlAnywheres() {
        // IQ island, 2026-10-14 (natives): exact, from double estimates checked by decimal arithmetic
        String q = pure("SELECT 10.0 / 3 AS q");
        assertTrue(q.contains("floor") && q.contains("toDecimal") && q.contains("0.9999999999999432"), q);
        assertTrue(pure("SELECT 7.5 % 2 AS r").contains("rem("));
        assertTrue(refusal("SELECT (10.0 / 3) * 3 AS q").getMessage().contains("select item"));
        assertTrue(refusal("SELECT DISTINCT 10.0 / 3 AS q").getMessage().contains("select item"));
        assertTrue(refusal("SELECT (2 + 3) / 1.5 AS q").getMessage().contains("constant integer expression"));
        assertTrue(refusal("SELECT NULL % 1.5 AS q").getMessage().contains("untyped NULL"));
        // under IQ's engine a NUMERIC quotient is cut toward zero at scale max(6, ...) (IQ island, 2026-10-15, D-N1
        // measured on IQ 17.1)
        assertTrue(pure("SELECT nm / 2 AS q FROM " + T).contains("0.000001"));
        assertTrue(refusal("SELECT CAST(1 AS NUMERIC(30,10)) / CAST(1 AS NUMERIC(30,10)) AS q",
                IqProfile.SQLA17_DEFAULT.settings()).getMessage().contains("'?'"));
        assertTrue(IqFunctions.quotientFits(2, 8, 2, 8, 21, 11));
        assertFalse(IqFunctions.quotientFits(10, 20, 0, 10, 31, 11));
    }

    @Test
    void aSearchOf255CharactersIsNull() {
        String p = pure("SELECT REPLACE(vc, vc, 'x') AS r FROM " + T);
        assertTrue(p.contains("length() > 254"), p);
        assertFalse(pure("SELECT REPLACE(vc, 'a', 'x') AS r FROM " + T).contains("254"));
    }

    // ------------------------------------------------------------------ numbers and the engine split

    @Test
    void absTakesTheSmallestTypeThatHoldsItUnderTheIqEngine() {
        // IQ island, 2026-10-16 (measured on IQ 17.1, replacing the research's D-F5): the smallest SMALLINT's absolute
        // value is an INTEGER's, a BIGINT's a NUMERIC(20,0)'s, under the IQ engine as in the front end
        String iq = pure("SELECT ABS(sm) AS a, ABS(bg) AS b FROM " + T);
        assertFalse(iq.contains("32767"), iq);
        assertTrue(iq.contains("toDecimal()->abs()"), iq);
        // SQL Anywhere's front end: an unsigned SMALLINT, a BIGINT's NUMERIC(20,0)
        String sa = pure("SELECT ABS(sm) AS a, ABS(bg) AS b FROM " + T, IqProfile.SQLA17_DEFAULT.settings());
        assertFalse(sa.contains("32767"), sa);
        assertTrue(sa.contains("toDecimal()->abs()"), sa);
    }

    @Test
    void whatTheIqEngineTypesOrComputesOtherwiseIsRefusedByItsDecision() {
        String[][] decided = {
                {"SELECT PATINDEX('%', vc) AS p FROM " + T, "D-F6"},
        };
        for (String[] d : decided) {
            assertTrue(refusal(d[0]).getMessage().contains(d[1]), d[0]);
        }
        // IQ island, 2026-10-17 (D-F3, measured on IQ 17.1): STUFF appends at a start just past the string under IQ's
        // engine too, and a length past 32,767 is NULL there
        assertTrue(pure("SELECT STUFF(vc, 1, 1, 'x') AS s FROM " + T).contains("32767"));
        // IQ island, 2026-10-15 (checklist 0.5; D-C2, D-N6, D-C1, measured on IQ 17.1): under IQ's engine a longer
        // string is cut to the target's bytes (characters in ISO-8859-1, whole characters in UTF-8), a DOUBLE to a
        // NUMERIC is its DOUBLE product cut toward zero, a NUMERIC whose text is too long raises for that row
        assertTrue(pure("SELECT CAST(vc AS VARCHAR(3)) AS c FROM " + T).contains("left(3)"));
        assertTrue(pure("SELECT CAST(vc AS VARCHAR(3)) AS c FROM " + T, IqProfile.IQ17_DEFAULT.settings())
                .contains("'xxE'"));
        String dbl = pure("SELECT CAST(d AS NUMERIC(10, 2)) AS c FROM " + T);
        assertTrue(dbl.contains("100.0") && dbl.contains("floor("), dbl);
        assertTrue(refusal("SELECT CAST(d AS NUMERIC(19, 2)) AS c FROM " + T).getMessage().contains("18 digits"));
        assertTrue(pure("SELECT CAST(nm AS VARCHAR(4)) AS c FROM " + T).contains("cast(@String)->toOne()"));
        // IQ island, 2026-10-15 (D-T10, measured on IQ 17.1): the calendar parts are read under IQ's engine, its weeks
        // from Sunday (the week's Wednesday decides its year and number)
        String cdw = pure("SELECT DATEPART(cdw, dt) AS d, DATEPART(cwk, dt) AS w FROM " + T);
        assertTrue(cdw.contains("DayOfWeek.Sunday") && cdw.contains("dayOfYear()"), cdw);
        // IQ island, 2026-10-15 (D-F1, D-F2, measured on IQ 17.1): ROUND and CEILING of a NUMERIC(p,s) are a
        // NUMERIC(p+1,s) under IQ's engine, unchecked
        assertFalse(pure("SELECT ROUND(nm, 1) AS r FROM " + T).contains("cast(@Decimal)->toOne()"));
        assertTrue(pure("SELECT CEILING(nm) AS c FROM " + T).contains("ceiling()"));
        // IQ island, 2026-10-15 (D-T2, D-T11, measured on IQ 17.1): DATEADD of a DATE stays a DATE under IQ's
        // engine; DATEDIFF below a day counts whole elapsed units; DAYOFYEAR is IQ's -1001015
        assertTrue(pure("SELECT DATEADD(dd, 1, dt) AS d FROM " + T).contains("datePart"));
        assertTrue(pure("SELECT DATEDIFF(hh, dt, ts) AS d FROM " + T).contains("3600000000"));
        assertTrue(org.junit.jupiter.api.Assertions.assertThrows(SqlTranslationException.class,
                () -> pure("SELECT DATEDIFF(dy, dt, ts) AS d FROM " + T)).getMessage().contains("-1001015"));
        // the IQ engine raises outside a function's domain (D-F9), SQL Anywhere's front end answers NULL
        assertTrue(pure("SELECT LN(d) AS l FROM " + T).contains("cast(@Float)->toOne()"));
        assertFalse(pure("SELECT LN(d) AS l FROM " + T, IqProfile.SQLA17_DEFAULT.settings()).contains("cast(@Float)->toOne()"));
        // IQ island, 2026-10-15 (the corpus lane, measured on IQ 17.1 and SQL Anywhere 17): ROUND of a DOUBLE is read,
        // floor(|x| * 10^n + 0.5) / 10^n in double steps; TRUNCNUM of one is not yet
        assertTrue(pure("SELECT ROUND(d, 1) AS r FROM " + T, IqProfile.SQLA17_DEFAULT.settings()).contains("floor"));
        assertTrue(refusal("SELECT TRUNCNUM(d, 1) AS r FROM " + T, IqProfile.SQLA17_DEFAULT.settings()).getMessage()
                .contains("DOUBLE"));
        assertTrue(refusal("SELECT POWER(2, 3) AS p").getMessage().contains("Infinity"));
    }

    @Test
    void extractReadsSixPartsAndSecondsWithTheirFraction() {
        // IQ island, 2026-10-15 (measured on IQ 17.1): DATEPART's value; SECOND a NUMERIC with the microseconds
        assertTrue(pure("SELECT EXTRACT(YEAR FROM dt) AS y FROM " + T).contains("year()"));
        String second = pure("SELECT EXTRACT(SECOND FROM ts) AS s FROM " + T);
        assertTrue(second.contains("second()") && second.contains("0.000001"), second);
        // another part is IQ's syntax error; a DATE's time IQ's engine's -1009145
        SqlSyntaxException quarter = assertThrows(SqlSyntaxException.class, () -> IqParser.parse(
                "SELECT EXTRACT(QUARTER FROM dt) AS q FROM " + T, IqProfile.IQ16_DEFAULT.settings()));
        assertTrue(quarter.reason().contains("SQLCODE -131"), quarter.reason());
        assertTrue(refusal("SELECT EXTRACT(HOUR FROM dt) AS h FROM " + T).getMessage().contains("-1009145"));
    }

    @Test
    void strIsReadUnderTheIqEngineOnly() {
        // IQ island, 2026-10-15 (measured on IQ 17.1): cut toward zero, right-justified, a zero's text constant
        String str = pure("SELECT STR(nm, 6, 1) AS s FROM " + T);
        assertTrue(str.contains("lpad(") && str.contains(", 6, ' ')"), str);
        assertTrue(str.contains("'0.0   '"), str);
        assertFalse(str.contains("round"), str);
        // SQL Anywhere's front end rounds a DOUBLE's binary value; a DOUBLE's own binary value is cut: refused by name
        assertTrue(refusal("SELECT STR(nm) AS s FROM " + T, IqProfile.SQLA17_DEFAULT.settings()).getMessage()
                .contains("IQ's engine only"));
        assertTrue(refusal("SELECT STR(d) AS s FROM " + T).getMessage().contains("DOUBLE"));
        // a length over 255 and more decimals than the length are IQ's errors
        assertTrue(refusal("SELECT STR(id, 256) AS s FROM " + T).getMessage().contains("-1001005"));
        assertTrue(refusal("SELECT STR(id, 4, 5) AS s FROM " + T).getMessage().contains("-1009145"));
    }

    @Test
    void roundOfAColumnChecksItsPrecisionAndOfAConstantNot() {
        String column = pure("SELECT ROUND(nm, 1) AS r FROM " + T, IqProfile.SQLA17_DEFAULT.settings());
        assertTrue(column.contains("cast(@Decimal)->toOne()"), column);
        String constant = pure("SELECT ROUND(CAST(9.99 AS NUMERIC(3, 2)), 1) AS r");
        assertFalse(constant.contains("cast(@Decimal)->toOne()"), constant);
    }

    // ------------------------------------------------------------------ CAST

    @Test
    void castToCharPadsUnderTheIqEngineAndLikeOverItIsRefused() {
        assertTrue(pure("SELECT CAST(vc AS CHAR(22)) AS c FROM " + T).contains("rpad(22, ' ')"));
        assertFalse(pure("SELECT CAST(vc AS CHAR(22)) AS c FROM " + T, IqProfile.SQLA17_DEFAULT.settings()).contains("rpad"));
        // D-S5 (IQ island, 2026-10-15, measured on IQ 17.1): IQ matches the pad in LIKE, which lite reads
        assertTrue(pure("SELECT id FROM " + T + " WHERE CAST(vc AS CHAR(22)) LIKE 'a%'").contains("rpad(22, ' ')"));
    }

    @Test
    void anOperationThatCanRaiseUnderARowLimitIsComputedForTheRowsKept() {
        // IQ island, 2026-10-16 (the user's decision; these were refused): IQ computes the items its ORDER BY does not
        // read for the rows it keeps, so they are computed after the limit (IqToPureTest pins the order)
        for (String sql : new String[] {"SELECT TOP 2 id, CAST(nm AS INT) AS c FROM " + T + " ORDER BY id",
                "SELECT TOP 2 id, SUBSTRING(vc, 1, sm) AS c FROM " + T + " ORDER BY id"}) {
            assertTrue(pure(sql).contains("->limit(2)->extend("), sql);
        }
    }

    @Test
    void iqReadsAStringAsAnInteger() {
        assertEquals(BigInteger.valueOf(42), IqFunctions.integerText(" 42 "));
        assertEquals(BigInteger.ZERO, IqFunctions.integerText(""));
        assertEquals(BigInteger.ZERO, IqFunctions.integerText(" - "));
        assertEquals(BigInteger.valueOf(-42), IqFunctions.integerText("-0042"));
        // a string that begins or ends with a space loses its white space at both ends (measured)
        assertEquals(BigInteger.ONE, IqFunctions.integerText(" \t1"));
        assertEquals(BigInteger.ONE, IqFunctions.integerText("1\t "));
        assertNull(IqFunctions.integerText("\t1"));
        assertNull(IqFunctions.integerText("1\t"));
        assertNull(IqFunctions.integerText("+ 1"));
        assertNull(IqFunctions.integerText("2.5"));
        assertNull(IqFunctions.integerText("1e2"));
    }

    @Test
    void theStatementsTimeIsNowAndToday() {
        String p = pure("SELECT NOW(*) AS b, CURRENT TIMESTAMP AS c, TODAY() AS d, CURRENT DATE AS e");
        assertEquals(2, p.split("now\\(\\)", -1).length - 1, p);
        assertEquals(2, p.split("today\\(\\)", -1).length - 1, p);
        assertEquals(1, pure("SELECT GETDATE() AS a").split("now\\(\\)", -1).length - 1);
        // IQ island, 2026-10-14 (review 3's M3, the study's D-T13): SQL Anywhere reads GETDATE() apart from the
        // statement's other clock readings: beside one, refused
        assertThrows(SqlTranslationException.class, () -> pure("SELECT GETDATE() AS a, NOW(*) AS b"));
    }

    /** IQ island, 2026-10-15 (the corpus lane; measured on IQ 17.1, lite/iq_reals.yaml): REAL and FLOAT are single
     *  precision, rounded by Veltkamp's split of 2^29 + 1; FLOAT(25) is a DOUBLE; a precision outside 1 to 53 refused. */
    @Test
    void realsAreRoundedToSinglePrecision() {
        assertTrue(pure("SELECT CAST(id AS REAL) AS x FROM " + T, IqProfile.IQ17_DEFAULT.settings())
                .matches("(?s).*(5\\.36870913E8|536870913\\.0).*"));
        assertTrue(pure("SELECT CAST(id AS FLOAT) AS x FROM " + T, IqProfile.IQ17_DEFAULT.settings())
                .matches("(?s).*(5\\.36870913E8|536870913\\.0).*"));
        assertFalse(pure("SELECT CAST(id AS FLOAT(25)) AS x FROM " + T, IqProfile.IQ17_DEFAULT.settings())
                .matches("(?s).*(5\\.36870913E8|536870913\\.0).*"));
        assertTrue(assertThrows(SqlSyntaxException.class, () -> IqParser.parse("SELECT CAST(id AS FLOAT(54)) AS x",
                IqProfile.IQ17_DEFAULT.settings())).unsupported());
    }
}
