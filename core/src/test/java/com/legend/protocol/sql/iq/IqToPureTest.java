// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol.sql.iq;

import com.legend.protocol.PureComposer;
import com.legend.protocol.spec.ValueSpecification;
import com.legend.protocol.sql.SqlAst;
import com.legend.protocol.sql.SqlToPure;
import com.legend.protocol.sql.SqlTranslationException;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * IQ's SQL through the one translator ({@link SqlToPure} reading {@link IqDialect}): the Pure each query becomes,
 * printed as a person would write it, IQ's name, NULL, blank-padding and CHAR rules, the engine a statement runs in,
 * and each refusal's kind. The FROM source's columns, with their store types, come from a stand-in for the typer;
 * the iq-parity lanes (IqParityTest, IqParityPostgresTest) run the same pipeline end to end.
 */
class IqToPureTest {

    /** A source column, as the typer reports it: name, Pure type, nullable, store type. */
    private record Col(String name, String type, boolean nullable, String store) {
    }

    /** The source table: id INTEGER not null, sm SMALLINT, vc VARCHAR(20), dt DATE. */
    private static final List<Col> COLUMNS = List.of(
            new Col("id", "Integer", false, "INTEGER"),
            new Col("sm", "Integer", true, "SMALLINT"),
            new Col("vc", "String", true, "VARCHAR(20)"),
            new Col("dt", "StrictDate", true, "DATE"));

    /** A table of every integer type: i INTEGER not null, sm SMALLINT, bg BIGINT, bt BIT (the store's Boolean), and
     *  nm NUMERIC(10,2), d DOUBLE. */
    private static final List<Col> NUMBERS = List.of(
            new Col("i", "Integer", false, "INTEGER"),
            new Col("sm", "Integer", true, "SMALLINT"),
            new Col("bg", "Integer", true, "BIGINT"),
            new Col("bt", "Boolean", true, "BIT"),
            new Col("nm", "Decimal", true, "NUMERIC(10,2)"),
            new Col("d", "Float", true, "DOUBLE"));

    /** A table with a CHAR(5) column. */
    private static final List<Col> WITH_CHAR = List.of(
            new Col("id", "Integer", false, "INTEGER"),
            new Col("ch", "String", true, "CHAR(5)"),
            new Col("vc", "String", true, "VARCHAR(20)"));

    private static final String T = "tb('s::DB.T')";

    private static String pure(String sql) {
        return pure(sql, IqProfile.IQ16_DEFAULT.settings(), COLUMNS);
    }

    private static String pure(String sql, IqSettings profile) {
        return pure(sql, profile, COLUMNS);
    }

    private static String pure(String sql, IqSettings profile, List<Col> columns) {
        ValueSpecification vs = SqlToPure.translate(sql, (source, column) -> columns.forEach(
                c -> column.column(c.name(), c.type(), c.nullable(), c.store())), IqDialect.of(profile));
        return PureComposer.valueSpecification(vs, PureComposer.Style.STANDARD);
    }

    private static SqlTranslationException refusal(String sql) {
        return refusal(sql, IqProfile.IQ16_DEFAULT.settings(), COLUMNS);
    }

    private static SqlTranslationException refusal(String sql, IqSettings profile, List<Col> columns) {
        return assertThrows(SqlTranslationException.class, () -> pure(sql, profile, columns));
    }

    // ------------------------------------------------------------------ shapes and names

    @Test
    void aPlainQueryIsFilterSortSelect_withNullTheLowestValue() {
        assertEquals("#>{s::DB.T}#->filter(r|$r.id > 2)"
                        + "->sort([~sm->descending(meta::pure::functions::relation::NullOrder.LAST),"
                        + " ~id->ascending(meta::pure::functions::relation::NullOrder.FIRST)])->select(~[id, sm])",
                pure("SELECT id, sm FROM " + T + " WHERE id > 2 ORDER BY sm DESC, id"));
    }

    @Test
    void namesMatchWithoutCase_quotedOrNot_andAColumnShowsAsWritten() {
        // a column shown as written, in a column of its own name only last (2026-10-16): a database that reads names
        // without regard to case (DuckDB) binds ID beside id to one of them
        assertEquals("#>{s::DB.T}#->extend(~[_sql_1:r|$r.id, _sql_2:r|$r.sm])->select(~[_sql_1, _sql_2])"
                + "->rename(~_sql_1, ~ID)->rename(~_sql_2, ~SM)", pure("SELECT ID, [SM] FROM " + T));
        assertEquals("#>{s::DB.T}#->filter(r|$r.id == 1)->select(~[id])", pure("SELECT `id` FROM " + T + " a WHERE"
                + " A.ID = 1"));
        // two names a reference matches without regard to case are an ambiguity, quoted or not
        List<Col> twins = List.of(new Col("id", "Integer", false, "INTEGER"), new Col("ID", "Integer", false,
                "INTEGER"));
        SqlTranslationException e = refusal("SELECT [id] FROM " + T, IqProfile.IQ16_DEFAULT.settings(), twins);
        assertEquals(SqlTranslationException.Kind.INVALID, e.kind());
    }

    @Test
    void anUnaliasedConstantIsNamedByIqsRenderingOfIt() {
        // a numeric constant is written at IQ's scale for it: 1.50 is NUMERIC(2,1), 1.5
        assertEquals("#>{s::DB.T}#->extend(~['1.5':r|1.5D, '-1':r|-1, '\\'a\\'':r|'a', null:r|[]->cast(@Integer)])"
                        + "->select(~['1.5', '-1', '\\'a\\'', null])",
                pure("SELECT 1.50, -1, 'a', NULL FROM " + T));
        // IQ island, 2026-10-14 (M1b): a whole numeric beyond an INT's range is named by its digits (measured:
        // 2147483648.0 is 2147483648, 2147483647.0 is 2147483647.0), no longer refused
        assertTrue(pure("SELECT 4000000000., 2147483647.0 FROM " + T).contains("->select(~['4000000000',"
                + " '2147483647.0'])"));
    }

    @Test
    void aSelectListAliasIsReadWhereIqReadsIt() {
        // IQ island, 2026-10-14 (the third landing, measured on SQL Anywhere 17; it was refused): IQ reads an alias
        // before a column of its name in WHERE, ON, the select list and inside ORDER BY, as its expression written
        // there -- a copy of it
        assertEquals("#>{s::DB.T}#->filter(r|$r.id == 1)->extend(~[x:r|$r.id])->select(~[x])",
                pure("SELECT id AS x FROM " + T + " WHERE x = 1"));
        assertTrue(pure("SELECT vc AS id FROM " + T + " WHERE id = 'a'").contains("$r.vc"));
        for (String key : new String[] {"x + 0", "-x", "+x", "ISNULL(x, 0)", "CASE WHEN X > 0 THEN 0 ELSE 1 END",
                "IF x > 0 THEN 0 ELSE 1 ENDIF"}) {
            assertTrue(pure("SELECT id, sm AS x FROM " + T + " ORDER BY " + key).contains("$r.sm"), key);
        }
        // an alias defined later in the list, an alias of an alias; inside its own definition its name is the column
        // (IQ island, 2026-10-15: IQ's engine widens, D-N3 -- no range check)
        assertTrue(pure("SELECT x + 1 AS y, id AS x FROM " + T).contains("y:r|$r.id->floor() + 1"));
        assertTrue(pure("SELECT id + 1 AS id FROM " + T + " WHERE id > 3").contains("->filter(r|$r.id->floor() + 1"
                + " > 3)"));
        // a definition that reaches itself through another alias is IQ's error -831
        assertTrue(refusal("SELECT id AS a, a + 1 AS id FROM " + T).getMessage().contains("-831"));
        // an aggregate's alias in WHERE is IQ's -150
        assertTrue(refusal("SELECT COUNT(*) AS n FROM " + T + " WHERE n > 1").getMessage().contains("-150"));
        // inside a subquery an enclosing query's alias is read before its columns, a subquery's own column first:
        // refused by name where the subquery's own FROM items hold no column of the name
        assertTrue(refusal("SELECT id AS k FROM " + T + " a WHERE EXISTS (SELECT 1 FROM " + T + " b WHERE b.sm = k)")
                .getMessage().contains("enclosing query's select list"));
        pure("SELECT id AS sm FROM " + T + " a WHERE EXISTS (SELECT 1 FROM " + T + " b WHERE b.id = sm)");
        // an unaliased item reading an alias is named as written
        assertTrue(pure("SELECT id AS x, x + 1 FROM " + T).contains("'x+1'"));
        assertTrue(pure("SELECT id AS id, id + 1 FROM " + T).contains("'id+1'"));
        SqlTranslationException shadow = refusal("SELECT id AS sm, sm FROM " + T);
        assertTrue(shadow.getMessage().contains("appears twice"), shadow.getMessage());
        // a bare alias, a position and a qualified column are read as before
        String bare = pure("SELECT id, sm AS x FROM " + T + " ORDER BY x");
        assertTrue(bare.contains("->sort([~x->ascending("), bare);
        pure("SELECT id, sm AS x FROM " + T + " ORDER BY 2");
        // an alias of the column of its own name reads the same
        assertEquals("#>{s::DB.T}#->filter(r|$r.id == 1)->extend(~[_sql_1:r|$r.id])->select(~[_sql_1])"
                + "->rename(~_sql_1, ~ID)", pure("SELECT id AS ID FROM " + T + " WHERE id = 1"));
    }

    // ------------------------------------------------------------------ NULL

    @Test
    void equalsNullIsUnknown_orIsNullUnderATdsLogin() {
        assertEquals("#>{s::DB.T}#->filter(r|false)->select(~[id])", pure("SELECT id FROM " + T + " WHERE sm = NULL"));
        assertEquals("#>{s::DB.T}#->filter(r|$r.sm->isEmpty())->select(~[id])",
                pure("SELECT id FROM " + T + " WHERE sm = NULL", IqProfile.IQ16_TDS.settings()));
        assertEquals("#>{s::DB.T}#->filter(r|$r.sm->isNotEmpty())->select(~[id])",
                pure("SELECT id FROM " + T + " WHERE NULL != sm", IqProfile.IQ16_TDS.settings()));
        // only = and != are rewritten
        assertEquals("#>{s::DB.T}#->filter(r|false)->select(~[id])",
                pure("SELECT id FROM " + T + " WHERE sm < NULL", IqProfile.IQ16_TDS.settings()));
    }

    // ------------------------------------------------------------------ strings

    @Test
    void stringsAreEqualIgnoringTrailingBlanks() {
        assertEquals("#>{s::DB.T}#->filter(r|$r.vc->isNotEmpty() && (($r.vc->toOne()->startsWith('x') ||"
                        + " 'x'->startsWith($r.vc->toOne())) && ($r.vc->toOne()->replace(' ', '') =="
                        + " 'x'->replace(' ', ''))))->select(~[id])",
                pure("SELECT id FROM " + T + " WHERE vc = 'x'"));
    }

    @Test
    void aCharColumnIsPaddedToItsLengthUnderTheIqEngine() {
        assertEquals("#>{s::DB.T}#->extend(~[_sql_1:r|if($r.ch->isEmpty(), |[], |$r.ch->toOne()->rpad(5, ' '))])"
                        + "->select(~[id, _sql_1, vc])->rename(~_sql_1, ~ch)->select(~[ch])",
                pure("SELECT ch FROM " + T, IqProfile.IQ16_DEFAULT.settings(), WITH_CHAR));
    }

    @Test
    void orderingStringsAndCaseInsensitiveComparisonsAreIqsCollation() {
        // IQ island, 2026-10-14 (natives): composed now -- an ordering drops the trailing spaces (U+0020 only), a
        // sqla17 profile's comparison folds the ASCII letters to lower case; keys still wait for the seam
        String lt = pure("SELECT id FROM " + T + " WHERE vc < 'b'");
        assertTrue(lt.contains("regexpReplace(' +$', '', false)") && !lt.contains("rtrim"), lt);
        // IQ island, 2026-10-14 (the merge): the seam's key takes the dialect's collation -- ORDER BY sorts by it;
        // DISTINCT groups by it and shows one value of each group (IQ island, 2026-10-15: the least, a member), and
        // strings that are their own keys are their own DISTINCT
        String ordered = pure("SELECT id FROM " + T + " ORDER BY vc");
        assertTrue(ordered.contains("regexpReplace(' +$', '', false))])->sort("), ordered);
        String distinct = pure("SELECT DISTINCT vc FROM " + T);
        assertTrue(distinct.contains("regexpReplace(' +$', '', false)") && distinct.contains("groupBy(")
                && distinct.contains("->min()") && !distinct.contains("distinct()"), distinct);
        assertFalse(pure("SELECT DISTINCT RTRIM(vc) AS r FROM " + T).contains("regexpReplace(' +$'"));
        String eq = pure("SELECT id FROM " + T + " WHERE vc = 'x'", IqProfile.SQLA17_DEFAULT.settings());
        assertTrue(eq.contains("replace('A', 'a')") && eq.contains("replace('Z', 'z')") && !eq.contains("toLower"),
                eq);
        assertFalse(eq.contains("regexpReplace"), eq);
    }

    // ------------------------------------------------------------------ kinds and refusals

    @Test
    void conversionsBetweenKindsAreLaterSlices() {
        // IQ island, 2026-10-14 (M1a): a string constant compared with a date is read now (below); these are not
        for (String where : new String[] {"vc = 1", "dt = vc", "vc + 1 = 'b'"}) {
            assertEquals(SqlTranslationException.Kind.UNSUPPORTED,
                    refusal("SELECT id FROM " + T + " WHERE " + where).kind(), where);
        }
        // IQ island, 2026-10-15 (measured on IQ 17.1): under IQ's engine a date moves by whole days
        // (IqFunctions.dateArithmetic); SQL Anywhere's front end refuses it by name still
        assertTrue(pure("SELECT id FROM " + T + " WHERE dt + 1 = dt").contains("adjust("));
        assertEquals(SqlTranslationException.Kind.UNSUPPORTED, refusal("SELECT id FROM " + T + " WHERE dt + 1 = dt",
                IqProfile.SQLA17_DEFAULT.settings(), COLUMNS).kind());
    }

    // ------------------------------------------------------------------ M0's rest

    private static String numbers(String sql) {
        return pure(sql, IqProfile.IQ16_DEFAULT.settings(), NUMBERS);
    }

    private static SqlTranslationException numbersRefusal(String sql) {
        return refusal(sql, IqProfile.IQ16_DEFAULT.settings(), NUMBERS);
    }

    @Test
    void aBitIsReadAsTheInteger0Or1_andComparesWithIntegers() {
        String p = numbers("SELECT i FROM " + T + " WHERE bt = 1");
        assertTrue(p.contains("if($r.bt->isNotEmpty() && $r.bt->toOne(), |1, |if($r.bt->isNotEmpty() &&"
                + " !$r.bt->toOne(), |0, |[]))"), p);
    }

    @Test
    void integerArithmeticIsTypedAndCheckedAsIqs() {
        // SQL Anywhere's front end (a sqla17 profile): smallint + smallint constant is a SMALLINT, checked against its
        // range, the column widened to 64 bits first
        String plus = saNumbers("SELECT sm + 1 AS n FROM " + T);
        assertTrue(plus.contains("$r.sm->toOne()->floor() + 1"), plus);
        assertTrue(plus.contains("< -32768") && plus.contains("> 32767"), plus);
        // two constants that cannot leave their type: no check, no widening
        assertTrue(saNumbers("SELECT 2 + 3 * 4 AS n FROM " + T).contains("n:r|2 + (3 * 4)"));
        // a constant result out of its type's range is IQ's error now (-158)
        SqlTranslationException over = saNumbersRefusal("SELECT 32767 + 1 AS n FROM " + T);
        assertEquals(SqlTranslationException.Kind.INVALID, over.kind());
        assertTrue(over.getMessage().contains("SQLCODE -158"), over.getMessage());
        // a result that cannot leave its type is not checked: a BIT and a SMALLINT give an INTEGER
        assertFalse(saNumbers("SELECT bt + sm AS n FROM " + T).contains("2147483647"));
        // a BIGINT result is never checked: the database computes it in 64 bits and raises its own overflow first
        String wide = saNumbers("SELECT i + bg AS n FROM " + T);
        assertFalse(wide.contains("9223372036854775807") || wide.contains("cast(@Integer)->toOne()"), wide);
        // / truncates toward zero (lite's quotient, the database's integer division: 2026-10-17); % is rem
        assertTrue(saNumbers("SELECT i / 2 AS q FROM " + T).contains("quotient(2)"));
        assertTrue(saNumbers("SELECT i % 2 AS r FROM " + T).contains("->rem(2)->cast(@Integer)"));
        // a minus before a column is checked in its type; before a constant it is a constant typed by its value
        assertTrue(saNumbers("SELECT -sm AS n FROM " + T).contains("> 32767"));
        assertEquals(SqlTranslationException.Kind.INVALID, saNumbersRefusal("SELECT -(-32768) AS n FROM " + T).kind());
        // IQ island, 2026-10-15 (D-N3, measured on IQ 17.1): IQ's engine widens -- no check, the result of the
        // narrowest type that holds it (sm + 1 an INTEGER, 32767 + 1 the INTEGER 32768), the guards of a chain merged
        String iq = numbers("SELECT sm + 1 AS n FROM " + T);
        assertTrue(iq.contains("$r.sm->toOne()->floor() + 1") && !iq.contains("32767"), iq);
        assertTrue(numbers("SELECT 32767 + 1 AS n FROM " + T).contains("32767 + 1"));
        assertFalse(numbers("SELECT -sm AS n FROM " + T).contains("32767"));
        String chain = numbers("SELECT sm + sm + sm + sm AS n FROM " + T);
        assertEquals(4, chain.split("isEmpty", -1).length - 1, chain);
    }

    /** {@link #numbers} in SQL Anywhere's front end: a sqla17 profile's database (IQ island, 2026-10-15). */
    private static String saNumbers(String sql) {
        return pure(sql, IqProfile.SQLA17_DEFAULT.settings(), NUMBERS);
    }

    private static SqlTranslationException saNumbersRefusal(String sql) {
        return refusal(sql, IqProfile.SQLA17_DEFAULT.settings(), NUMBERS);
    }

    @Test
    void aConstantThatMayBeNullIsCheckedWhereItIsComputed() {
        // the second review's M3, in SQL Anywhere's front end: it answers NULL where the IF is unknown, and raises only
        // where it computes 32768 (IQ island, 2026-10-15: IQ's engine widens instead, D-N3)
        for (String sql : new String[] {"SELECT IF i = 1 THEN 32767 ENDIF + 1 AS n FROM ",
                "SELECT CASE WHEN i = 1 THEN 2147483647 END + 1 AS n FROM ",
                "SELECT NULLIF(32767, sm) + 1 AS n FROM "}) {
            String p = saNumbers(sql + T);
            assertTrue(p.contains("cast(@Integer)->toOne()"), p);
        }
        assertTrue(pure("SELECT IF 1 = 2 THEN 32767 ENDIF + 1 AS n").contains("cast(@Integer)->toOne()"));
        // constants SQL Anywhere folds still raise at once, as it does
        assertEquals(SqlTranslationException.Kind.INVALID, saNumbersRefusal("SELECT (40000 - 39999) + 2147483647 AS n"
                + " FROM " + T).kind());
        assertEquals(SqlTranslationException.Kind.INVALID, saNumbersRefusal("SELECT NULLIF(sm, 100000) AS n FROM " + T)
                .kind());
        // under IQ's engine none of these raises
        assertFalse(numbers("SELECT IF i = 1 THEN 32767 ENDIF + 1 AS n FROM " + T).contains("cast(@Integer)->toOne()"));
    }

    @Test
    void checkedArithmeticPastItsSizeIsRefusedQuickly() {
        // the second review's M1: each checked operation repeats its operands, fourfold per level
        String twelve = String.join(" + ", java.util.Collections.nCopies(12, "sm"));
        // (in SQL Anywhere's front end, which checks each step; IQ island, 2026-10-15: IQ's engine checks none, D-N3,
        // so there a chain stays linear, below)
        // IQ island, 2026-10-17: past the bound, the item's long operands are computed once each, a column before it,
        // so the chain translates, small and quickly
        String chained = assertTimeoutPreemptively(java.time.Duration.ofSeconds(10),
                () -> saNumbers("SELECT " + twelve + " AS n FROM " + T));
        assertTrue(chained.length() < 100_000, "" + chained.length());
        assertTrue(chained.contains("->extend(~[_sql_"), chained.substring(0, Math.min(400, chained.length())));
        // IQ island, 2026-10-17: a quotient reads each operand once now (lite's quotient), so nested ones stay small
        String quotients = saNumbers("SELECT i / 2 / 2 / 2 AS n FROM " + T);
        assertTrue(quotients.length() < 20_000, "" + quotients.length());
        String chain = numbers("SELECT " + String.join(" + ", java.util.Collections.nCopies(200, "sm")) + " AS n FROM "
                + T);
        assertTrue(chain.length() < 20_000, "" + chain.length());
        // what is translated stays small
        String six = numbers("SELECT " + String.join(" + ", java.util.Collections.nCopies(6, "sm")) + " AS n FROM "
                + T);
        assertTrue(six.length() < 100_000, "" + six.length());
        String bigints = numbers("SELECT " + String.join(" + ", java.util.Collections.nCopies(10, "bg")) + " AS n FROM "
                + T);
        assertTrue(bigints.length() < 100_000, "" + bigints.length());
    }

    @Test
    void aRowLimitComputesTheItemsItsOrderByDoesNotReadAfterIt() {
        // IQ island, 2026-10-16 (the user's decision; measured on IQ 17.1 and SQL Anywhere 17, lite/corpus_gaps.yaml's
        // TOP cases; this pin replaces the second review's M2 refusal): IQ computes the items its ORDER BY does not read
        // for the rows it keeps, so they are computed after the sort and the limit. Under IQ's engine, the rows up to
        // the limit's last, START AT's skipped ones too (dropped after them)
        String iq = numbers("SELECT TOP 1 START AT 2 i, 100 / sm AS n FROM " + T + " ORDER BY i");
        assertTrue(iq.contains("->limit((2 - 1) + 1)->extend(") && iq.endsWith("->drop(2 - 1)"), iq);
        assertTrue(numbers("SELECT FIRST i, i * 2 AS n FROM " + T + " ORDER BY i").contains("->limit(1)->extend("));
        // in SQL Anywhere's front end, the rows it returns
        String sa = saNumbers("SELECT TOP 1 START AT 2 i, -sm AS n FROM " + T + " ORDER BY i");
        assertTrue(sa.contains("->drop(2 - 1)->limit(1)->extend("), sa);
        assertTrue(saNumbers("SELECT TOP 1 i, NULLIF(sm, bg) AS n FROM " + T + " ORDER BY i")
                .contains("->limit(1)->extend("));
        // an item ORDER BY reads, by its alias or written there, is computed for every row before the sort
        for (String sql : new String[] {"SELECT TOP 1 i, 100 / sm AS n FROM " + T + " ORDER BY n, i",
                "SELECT TOP 1 i FROM " + T + " ORDER BY bg + 1"}) {
            String p = numbers(sql);
            assertTrue(p.indexOf("->extend(") < p.indexOf("->sort(") && !p.contains("->limit(1)->extend("), p);
        }
        // as is every item of a DISTINCT, and of a statement with no row limit
        for (String sql : new String[] {"SELECT DISTINCT TOP 1 i, 100 / sm AS n FROM " + T + " ORDER BY i",
                "SELECT i, sm + 1 AS n FROM " + T + " ORDER BY i DESC"}) {
            String p = numbers(sql);
            assertTrue(p.indexOf("->extend(") < p.indexOf("->sort("), p);
        }
        // nothing computed: the limit as written
        assertTrue(numbers("SELECT TOP 2 i FROM " + T + " ORDER BY i DESC").endsWith("->limit(2)"));
    }

    @Test
    void arithmeticIqTypesAsNumericOrUnsignedIsRefused() {
        // IQ island, 2026-10-14 (M1a): DOUBLE arithmetic is read now (below), and NUMERIC arithmetic without FROM
        for (String sql : new String[] {"SELECT d % 2 AS n", "SELECT bt + bt AS n",
                "SELECT bt + bg AS n"}) {
            SqlTranslationException e = numbersRefusal(sql + " FROM " + T);
            assertEquals(SqlTranslationException.Kind.UNSUPPORTED, e.kind(), sql);
        }
        // IQ island, 2026-10-15 (D-N3, measured on IQ 17.1): IQ's engine shows a BIGINT's remainder and a constant
        // past an INTEGER plus one as scale-0 NUMERICs of whole values, read as the integers they are; SQL Anywhere's
        // front end refuses both
        assertTrue(numbers("SELECT bg % 2 AS n FROM " + T).contains("->rem(2)"));
        assertEquals(SqlTranslationException.Kind.UNSUPPORTED, saNumbersRefusal("SELECT bg % 2 AS n FROM " + T)
                .kind());
        // a constant past an INTEGER is a NUMERIC constant to IQ, whose sums IQ's engine types as SQL Anywhere's
        numbers("SELECT 2147483648 + 1 AS n FROM " + T);
    }

    @Test
    void concatenationReadsASingleNullAsEmpty() {
        String p = pure("SELECT vc || '!' AS s FROM " + T);
        assertTrue(p.contains("$r.vc->coalesce('') + '!'"), p);
        String both = pure("SELECT vc + vc AS s FROM " + T);
        assertTrue(both.contains("if($r.vc->isEmpty() && $r.vc->isEmpty(), |[]"), both);
        assertTrue(pure("SELECT id || 'x' AS s FROM " + T).contains("$r.id->toString() + 'x'"));
        // + of a string and anything else is IQ's NUMERIC addition
        assertEquals(SqlTranslationException.Kind.UNSUPPORTED, refusal("SELECT vc + 1 AS s FROM " + T).kind());
        // IQ island, 2026-10-15 (measured on IQ 17.1): under IQ's engine + of a string and an untyped NULL joins them,
        // the NULL as nothing; SQL Anywhere's front end refuses it by name still
        assertTrue(pure("SELECT 'a' + NULL AS s FROM " + T).contains("'a'"));
        assertEquals(SqlTranslationException.Kind.UNSUPPORTED, refusal("SELECT 'a' + NULL AS s FROM " + T,
                IqProfile.SQLA17_DEFAULT.settings(), COLUMNS).kind());
        // IQ island, 2026-10-14 (M1e): a date joins as its text in the profile's DATE_FORMAT (IqDateTime), no longer
        // refused
        String date = pure("SELECT dt || 'x' AS s FROM " + T);
        assertTrue(date.contains("year()->toString()->lpad(4, '0') + '-'"), date);
    }

    @Test
    void ifAndCaseTakeIqsUnifiedType() {
        String p = numbers("SELECT IF sm > 0 THEN sm ELSE bg ENDIF AS n FROM " + T);
        assertTrue(p.contains("if("), p);
        // CHAR results of different lengths: under IQ's engine a VARCHAR, each value as it is (IQ island, 2026-10-15,
        // D-S1 measured on IQ 17.1)
        assertTrue(pure("SELECT IF id > 0 THEN 'pos' ELSE 'nonpos' ENDIF AS s FROM " + T).contains("'nonpos'"));
        // SQL Anywhere's front end (no FROM) does not pad: answered
        assertTrue(pure("SELECT IF 1 > 0 THEN 'pos' ELSE 'nonpos' ENDIF AS s").contains("'nonpos'"));
        assertEquals(SqlTranslationException.Kind.UNSUPPORTED,
                refusal("SELECT CASE WHEN id = 1 THEN 'a' ELSE 1 END AS s FROM " + T).kind());
    }

    @Test
    void equalsNullInACaseConditionIsRewrittenByTheIqEngineOnly() {
        String iq = pure("SELECT IF sm = NULL THEN 1 ELSE 0 ENDIF AS s FROM " + T, IqProfile.IQ16_TDS.settings());
        assertTrue(iq.contains("$r.sm->isEmpty()"), iq);
        String sa = pure("SELECT IF NULL = NULL THEN 1 ELSE 0 ENDIF AS s", IqProfile.IQ16_TDS.settings());
        assertFalse(sa.contains("isEmpty"), sa);
    }

    @Test
    void theCatalogsFunctions() {
        assertTrue(numbers("SELECT ISNULL(sm, i, 0) AS n FROM " + T).contains("coalesce("));
        String ifnull = numbers("SELECT IFNULL(sm, 99) AS n FROM " + T);
        assertTrue(ifnull.contains("if($r.sm->isEmpty(), |99, |[]"), ifnull);
        String nullif = saNumbers("SELECT NULLIF(sm, bg) AS n FROM " + T);
        assertTrue(nullif.contains("> 32767"), nullif);
        SqlTranslationException arity = numbersRefusal("SELECT ISNULL(sm) AS n FROM " + T);
        assertEquals(SqlTranslationException.Kind.INVALID, arity.kind());
        // IQ island, 2026-10-14 (natives): UPPER is read now (an integer as its digits); IQ island, 2026-10-15
        // (checklist 1.10): POWER under IQ's engine (its NULLs decided before the power), in SQL Anywhere's still refused
        assertTrue(numbers("SELECT power(i, 2) AS n FROM " + T).contains("pow("));
        assertEquals(SqlTranslationException.Kind.UNSUPPORTED, saNumbersRefusal("SELECT power(i, 2) AS n FROM " + T)
                .kind());
    }

    @Test
    void numberStarIsOneWindowTheStatementSortsByLast() {
        // 2026-10-16 (the landing's review): a ROW_NUMBER over the ORDER BY's keys and the statement's own sort, apart,
        // numbered tied rows otherwise than they were shown (300,000 rows on DuckDB); one window, the last sort key
        String p = pure("SELECT NUMBER(*) AS n, NUMBER(*) * 10 AS m, id FROM " + T + " ORDER BY sm");
        assertEquals(1, p.split("rowNumber", -1).length - 1, p);
        String sort = p.substring(p.indexOf("->sort(["));
        assertTrue(sort.startsWith("->sort([~sm->ascending(") && sort.indexOf("), ~") < sort.indexOf("])"), p);
        // none, or a constant: the rows numbered in the order shown, as IQ numbers them
        assertTrue(pure("SELECT NUMBER(*) AS n FROM " + T).contains("->sort(["));
        assertTrue(pure("SELECT 'x' AS c, NUMBER(*) AS n FROM " + T + " ORDER BY c").contains("rowNumber"));
    }

    @Test
    void rowLimitsAreConstantsThePlannerFolds() {
        String p = pure("SELECT TOP (1+1) START AT 3 id FROM " + T + " ORDER BY id");
        assertTrue(p.endsWith("->drop(3 - 1)->limit(1 + 1)"), p);
        for (String sql : new String[] {"SELECT TOP 0 id FROM ", "SELECT TOP (1-2) id FROM ",
                "SELECT TOP 1 START AT 0 id FROM ", "SELECT TOP (65536*65536) id FROM "}) {
            assertEquals(SqlTranslationException.Kind.INVALID, refusal(sql + T).kind(), sql);
        }
        assertTrue(pure("SELECT TOP (1-1) id FROM " + T + " ORDER BY id").endsWith("->limit(1 - 1)"));
        // without ORDER BY the rows kept are the plan's choice, IQ's as the database's (IQ island, 2026-10-15, the
        // user's rule: any answer IQ could give is right): read as written; without FROM there is one row
        assertTrue(pure("SELECT TOP 2 id FROM " + T).endsWith("->limit(2)"));
        assertTrue(pure("SELECT FIRST id FROM " + T).endsWith("->limit(1)"));
        assertTrue(pure("SELECT TOP 3 START AT 2 id FROM " + T).contains("->drop(2 - 1)"));
        // its item computed for the row kept (IQ island, 2026-10-16)
        assertTrue(pure("SELECT TOP 1 1 AS x").contains("->limit(1)->extend("));
    }

    @Test
    void theReviewsRefusals() {
        // B3: a FROM function other than the table source
        assertEquals(SqlTranslationException.Kind.UNSUPPORTED, refusal("SELECT * FROM generate_series(1, 3)").kind());
        // B4: two aliases IQ cannot tell apart (-830)
        SqlTranslationException dup = refusal("SELECT sm AS k, id AS K FROM " + T);
        assertEquals(SqlTranslationException.Kind.INVALID, dup.kind());
        assertTrue(dup.getMessage().contains("-830"), dup.getMessage());
        // B6: more digits than a sqla17 database keeps
        assertEquals(SqlTranslationException.Kind.UNSUPPORTED, refusal("SELECT 1234567890123456789012345678901 AS v"
                + " FROM " + T, IqProfile.SQLA17_DEFAULT.settings(), COLUMNS).kind());
        // B1: a REAL column refuses its table
        assertEquals(SqlTranslationException.Kind.UNSUPPORTED, refusal("SELECT id FROM " + T, IqProfile.IQ16_DEFAULT.settings(),
                List.of(new Col("id", "Integer", false, "INTEGER"), new Col("r", "Float", true, "REAL"))).kind());
        // m4: a CHAR of no length read is refused by name, never a crash
        assertEquals(SqlTranslationException.Kind.UNSUPPORTED, refusal("SELECT c FROM " + T, IqProfile.IQ16_DEFAULT.settings(),
                List.of(new Col("c", "String", true, "CHAR"))).kind());
        // M4: a sqla17 profile has no IQ engine: no CHAR padding
        assertEquals("#>{s::DB.T}#->select(~[ch])", pure("SELECT ch FROM " + T, IqProfile.SQLA17_DEFAULT.settings(),
                WITH_CHAR));
    }

    @Test
    void aStoreTypeTheIslandDoesNotReadRefusesItsSource() {
        List<Col> binary = List.of(new Col("id", "Integer", false, "INTEGER"), new Col("b", "String", true,
                "BINARY(16)"));
        SqlTranslationException e = refusal("SELECT id FROM " + T, IqProfile.IQ16_DEFAULT.settings(), binary);
        assertEquals(SqlTranslationException.Kind.UNSUPPORTED, e.kind());
        assertTrue(e.getMessage().contains("BINARY(16)"), e.getMessage());
    }

    @Test
    void anUnknownColumnIsIqsErrorWithItsCode() {
        SqlTranslationException e = refusal("SELECT nope FROM " + T);
        assertEquals(SqlTranslationException.Kind.INVALID, e.kind());
        assertTrue(e.getMessage().contains("SQLCODE -143"), e.getMessage());
    }

    @Test
    void theEngineIsTheStatements_iqOverATable_saWithoutFrom() {
        assertEquals(IqDialect.Engine.IQ, IqDialect.engineOf(IqParser.parse("SELECT id FROM " + T,
                IqProfile.IQ16_DEFAULT.settings()), IqProfile.IQ16_DEFAULT.settings()));
        assertEquals(IqDialect.Engine.SA, IqDialect.engineOf(IqParser.parse("SELECT 1", IqProfile.IQ16_DEFAULT.settings()),
                IqProfile.IQ16_DEFAULT.settings()));
        // a CHAR read under SQL Anywhere's rules is not padded
        assertEquals("#>{s::DB.T}#->select(~[ch])", PureComposer.valueSpecification(SqlToPure.translate(
                "SELECT ch FROM " + T, (source, column) -> WITH_CHAR.forEach(c -> column.column(c.name(), c.type(),
                        c.nullable(), c.store())), IqDialect.of(IqProfile.IQ16_DEFAULT.settings(), IqDialect.Engine.SA)),
                PureComposer.Style.STANDARD));
    }

    @Test
    void withoutFromIsOneRow() {
        assertTrue(pure("SELECT 1 AS one WHERE 1 = 1").contains("->filter(r|1 == 1)"));
    }

    // ------------------------------------------------------------------ M1a: SQL Anywhere's own relations

    @Test
    void dummyAndTheRowGeneratorRunInSqlAnywheresFrontEnd() {
        for (String from : new String[] {"DUMMY", "sys.dummy", "\"DUMMY\"", "sa_rowgenerator(1, 2)",
                "dbo.sa_rowgenerator(1, 2) g", "SA_ROWGENERATOR()"}) {
            assertEquals(IqDialect.Engine.SA, IqDialect.engineOf(IqParser.parse("SELECT * FROM " + from,
                    IqProfile.IQ16_DEFAULT.settings()), IqProfile.IQ16_DEFAULT.settings()), from);
        }
        for (String from : new String[] {"dbo.dummy", "iq_dummy", "sys.sa_rowgenerator(1, 2)", T}) {
            assertEquals(IqDialect.Engine.IQ, IqDialect.engineOf(IqParser.parse("SELECT * FROM " + from,
                    IqProfile.IQ16_DEFAULT.settings()), IqProfile.IQ16_DEFAULT.settings()), from);
        }
    }

    @Test
    void dummyIsOneRowOfTheInteger0() {
        assertEquals("#TDS{\ndummy_col\n0\n}#->select(~[dummy_col])", pure("SELECT * FROM SYS.DUMMY"));
        assertTrue(pure("SELECT d.dummy_col + 1 AS n FROM dummy d").contains("$r.dummy_col + 1"));
    }

    @Test
    void theRowGeneratorIsARangeFlattened() {
        assertEquals("1->range(6, 1)->flatten(~row_num)->select(~[row_num])",
                pure("SELECT * FROM sa_rowgenerator(1, 5)"));
        // its stop is the constant after end, a 64-bit integer: no 32-bit end + 1 for a database to overflow
        assertTrue(pure("SELECT row_num FROM sa_rowgenerator(2147483646, 2147483647, 5)").contains(
                "2147483646->range(2147483648, 5)"));
        // not written: 0 to 100; no rows for a step that is not positive or a start beyond the end
        assertTrue(pure("SELECT row_num FROM sa_rowgenerator()").contains("0->range(101, 1)"));
        for (String args : new String[] {"5, 1", "1, 5, 0", "5, 1, -1", "1, 5, -1"}) {
            assertTrue(pure("SELECT row_num FROM sa_rowgenerator(" + args + ")").contains("0->range(0, 1)"), args);
        }
        // the column is an INTEGER of the range: + 1 cannot leave it, so it is not checked
        assertFalse(pure("SELECT row_num + 1 AS n FROM sa_rowgenerator(1, 5)").contains("2147483647"));
        assertTrue(pure("SELECT row_num * row_num AS n FROM sa_rowgenerator(46340, 46341)").contains("2147483647"));
        // IQ's errors, and the arguments not read yet
        assertTrue(refusal("SELECT row_num FROM sa_rowgenerator(1, 2, 1, 1)").getMessage().contains("SQLCODE -154"));
        assertTrue(refusal("SELECT row_num FROM sa_rowgenerator(1, 2147483648)").getMessage().contains("SQLCODE -158"));
        for (String args : new String[] {"NULL, 3", "1 + 1, 3", "1.5, 3", "'1', 3"}) {
            assertEquals(SqlTranslationException.Kind.UNSUPPORTED, refusal("SELECT row_num FROM sa_rowgenerator("
                    + args + ")").kind(), args);
        }
        assertEquals(SqlTranslationException.Kind.UNSUPPORTED, refusal("SELECT * FROM sys.sa_rowgenerator(1, 2)")
                .kind());
    }

    // ------------------------------------------------------------------ M1a: comparisons with IQ's conversions

    @Test
    void aStringComparedWithANumberIsItsNumericInSqlAnywheresFrontEnd() {
        String p = pure("SELECT row_num FROM sa_rowgenerator(0, 2) WHERE ' 1.50 ' = row_num");
        assertTrue(p.contains("1.5D"), p);
        assertTrue(pure("SELECT 1 AS x WHERE '' = 0").contains("0D"));
        assertTrue(pure("SELECT 1 AS x WHERE '1e1' > 9").contains("10D"));
        // IQ island, 2026-10-15 (measured on IQ 17.1): under IQ's engine it is its error -1001013; a string IQ reads
        // otherwise, or that SCALE rounds, is refused
        assertTrue(refusal("SELECT id FROM " + T + " WHERE id = '3'").getMessage().contains("-1001013"));
        for (String s : new String[] {"'abc'", "'1\t'", "'- 1'", "'1e'", "'.'", "'1e40'"}) {
            assertEquals(SqlTranslationException.Kind.UNSUPPORTED, refusal("SELECT 1 AS x WHERE " + s + " = 1")
                    .kind(), s);
        }
        assertEquals(SqlTranslationException.Kind.UNSUPPORTED, refusal("SELECT 1 AS x WHERE '0.1234567' = 1",
                IqProfile.SQLA17_DEFAULT.settings(), COLUMNS).kind());
        pure("SELECT 1 AS x WHERE '0.1234567' = 1");
        // under a SQL Anywhere profile, over a table too; a DOUBLE's NUMERIC is not read
        assertTrue(pure("SELECT id FROM " + T + " WHERE id = '3'", IqProfile.SQLA17_DEFAULT.settings()).contains("3D"));
        assertEquals(SqlTranslationException.Kind.UNSUPPORTED, refusal("SELECT i FROM " + T + " WHERE d = '1'",
                IqProfile.SQLA17_DEFAULT.settings(), NUMBERS).kind());
    }

    @Test
    void iqReportsItsErrorsInItsOrder() {
        // IQ island, 2026-10-17 (the corpus lane's IQ_ERROR verification, measured on IQ 17.1): a row limit in a
        // subquery before the outer join's ON rule (-1001030 before -1001047)
        String m = refusal("SELECT a.id FROM " + T + " a LEFT JOIN " + T + " b ON b.id = a.id AND b.id = (SELECT TOP 1"
                + " c.id FROM " + T + " c WHERE c.id = a.id ORDER BY c.id)").getMessage();
        assertTrue(m.contains("TOP") && m.contains("-1001030"), m);
        // a held item's subquery inside a conditional expression before a qualified read (-1001030 before -1005004)
        m = refusal("SELECT d.n FROM (SELECT id, COALESCE((SELECT MAX(c.id) FROM " + T + " c WHERE c.id = t.id), 0)"
                + " AS n FROM " + T + " t) d").getMessage();
        assertTrue(m.contains("-1001030") && m.contains("conditional"), m);
    }

    @Test
    void aStringComparedWithADateIsATimestamp() {
        assertTrue(pure("SELECT id FROM " + T + " WHERE dt = '2026-03-04'").contains("%2026-03-04"));
        assertTrue(pure("SELECT id FROM " + T + " WHERE dt >= '2026-03-04 00:00:00.000'").contains("%2026-03-04"));
        // IQ island, 2026-10-16 (measured on IQ 17.1): a string of no digit is no date in any spelling, IQ's -157
        assertEquals(SqlTranslationException.Kind.INVALID, refusal("SELECT id FROM " + T + " WHERE dt = 'x'").kind());
        for (String s : new String[] {"'2026-02-30'", "''", "'10:11:12'"}) {
            assertEquals(SqlTranslationException.Kind.UNSUPPORTED, refusal("SELECT id FROM " + T + " WHERE dt = " + s)
                    .kind(), s);
        }
        // IQ island, 2026-10-17 (measured on IQ 17.1): beside a DATE, IQ's engine reads the string as a DATE, its time
        // dropped; SQL Anywhere's front end as a TIMESTAMP, the DATE at its midnight
        String dated = pure("SELECT id FROM " + T + " WHERE dt = '2026-03-04 10:00'");
        assertTrue(dated.contains("%2026-03-04") && !dated.contains("10:00"), dated);
        assertTrue(pure("SELECT id FROM " + T + " WHERE dt = '2026-03-04 10:00'", IqProfile.SQLA17_DEFAULT.settings())
                .contains("%2026-03-04T10:00:00"));
        // IQ island, 2026-10-14 (M1e): IQ's other spellings are read now (IqDateTime, measured): '2026/03/04',
        // '2026-3-4', and a seventh place of the second, cut
        for (String s : new String[] {"'2026/03/04'", "'2026-3-4'", "'2026-03-04 00:00:00.0000009'"}) {
            assertTrue(pure("SELECT id FROM " + T + " WHERE dt = " + s).contains("%2026-03-04"), s);
        }
        // IQ island, 2026-10-17 (the corpus lane, measured on IQ 17.1): a month and a year are its first day; numbers
        // ending in a four-digit year are IQ's error under DATE_ORDER YMD, -157 beside a DATE column
        assertTrue(pure("SELECT id FROM " + T + " WHERE dt = 'Mar 2026'").contains("%2026-03-01"));
        assertTrue(refusal("SELECT id FROM " + T + " WHERE dt > '01/02/2018'").getMessage().contains("-157 here"));
        SqlAst.Pos at = new SqlAst.Pos(1, 1);
        assertEquals(java.util.Arrays.asList("2026-03-04", "05:06:07.100000"), java.util.Arrays.asList(
                IqDateTime.read("2026-03-04 05:06:07.1", IqProfile.IQ16_DEFAULT.settings(), at)));
        assertEquals(java.util.Arrays.asList("2000-02-29", "23:59:00"), java.util.Arrays.asList(
                IqDateTime.read("2000-02-29 23:59", IqProfile.IQ16_DEFAULT.settings(), at)));
        assertEquals(null, IqDateTime.read("1900-02-29", IqProfile.IQ16_DEFAULT.settings(), at));
    }

    @Test
    void theNumericOfAString() {
        assertEquals(0, new java.math.BigDecimal("0").compareTo(IqDialect.numericText("   ")));
        assertEquals(new java.math.BigDecimal("-0.5"), IqDialect.numericText(" -.5 "));
        assertEquals(new java.math.BigDecimal("1.5E+1"), IqDialect.numericText("+1.5e1"));
        assertEquals(new java.math.BigDecimal("1."), IqDialect.numericText("1."));
        for (String s : new String[] {".", "1e", "- 1", "1\t", "0x10", "1,000", "e5", "1e+12345"}) {
            assertEquals(null, IqDialect.numericText(s), s);
        }
    }

    // ------------------------------------------------------------------ M1a: BETWEEN, IN, IS DISTINCT FROM

    @Test
    void betweenAndInAreIqsComparisons_threeValued() {
        String between = pure("SELECT id FROM " + T + " WHERE sm BETWEEN 1 AND 5");
        assertTrue(between.contains("$r.sm->toOne() >= 1") && between.contains("$r.sm->toOne() <= 5"), between);
        // a NULL bound is UNKNOWN, never FALSE, whatever ansinull says
        assertTrue(pure("SELECT id FROM " + T + " WHERE sm NOT BETWEEN NULL AND 5", IqProfile.IQ16_TDS.settings())
                .contains("false"));
        // strings compare ignoring trailing blanks; a NULL in the list is UNKNOWN even under ansinull Off
        String in = pure("SELECT id FROM " + T + " WHERE vc IN ('a', NULL)", IqProfile.IQ16_TDS.settings());
        assertTrue(in.contains("startsWith") && !in.contains("isEmpty($r.vc)"), in);
        assertTrue(pure("SELECT id FROM " + T + " WHERE dt IN ('2026-03-04')").contains("%2026-03-04"));
        // IQ island, 2026-10-14 (natives): ordering strings drops their trailing spaces first
        assertTrue(pure("SELECT id FROM " + T + " WHERE vc BETWEEN 'a' AND 'b'").contains("regexpReplace(' +$'"));
    }

    @Test
    void isDistinctFromIsNeverUnknown() {
        String p = pure("SELECT id FROM " + T + " WHERE sm IS NOT DISTINCT FROM 1");
        assertTrue(p.contains("$r.sm->isEmpty()") || p.contains("isEmpty"), p);
        assertTrue(pure("SELECT 1 AS x WHERE NULL IS NOT DISTINCT FROM NULL").contains("true"));
        assertTrue(pure("SELECT id FROM " + T + " WHERE vc IS DISTINCT FROM 'x'").contains("startsWith"));
    }

    // ------------------------------------------------------------------ M1a: LIKE

    @Test
    void likeIsMatchedByItsPatternsShape() {
        assertTrue(pure("SELECT id FROM " + T + " WHERE vc LIKE 'h%'").contains("->startsWith('h')"));
        assertTrue(pure("SELECT id FROM " + T + " WHERE vc LIKE '%h'").contains("->endsWith('h')"));
        assertTrue(pure("SELECT id FROM " + T + " WHERE vc LIKE '%h%'").contains("->contains('h')"));
        // no wildcard: equality, trailing blanks significant (no padding)
        String eq = pure("SELECT id FROM " + T + " WHERE vc LIKE 'x'");
        assertTrue(eq.contains("== 'x'") && !eq.contains("startsWith"), eq);
        // _ matches a line feed too; the regular expression's own characters are themselves
        assertTrue(pure("SELECT id FROM " + T + " WHERE vc LIKE 'a_b.c%'").contains("'(?s)^a.b\\\\.c.*$'"));
        // ESCAPE: no default escape character
        assertTrue(pure("SELECT id FROM " + T + " WHERE vc LIKE 'a!%' ESCAPE '!'").contains("== 'a%'"));
        assertTrue(pure("SELECT id FROM " + T + " WHERE vc LIKE 'a\\\\%'").contains("startsWith('a\\\\')"));
        // an integer is matched as its digits on SQL Anywhere's front end; IQ island, 2026-10-15 (measured on IQ 17.1):
        // under IQ's engine LIKE over a number is its error -1001013
        assertTrue(pure("SELECT id FROM " + T + " WHERE id LIKE '1%'", IqProfile.SQLA17_DEFAULT.settings(), COLUMNS)
                .contains("toString()"));
        assertTrue(refusal("SELECT id FROM " + T + " WHERE id LIKE '1%'").getMessage().contains("-1001013"));
        // a NULL pattern or escape is UNKNOWN
        assertTrue(pure("SELECT id FROM " + T + " WHERE vc LIKE NULL").contains("filter(r|false)"));
    }

    @Test
    void likeRefusals() {
        // IQ island, 2026-10-14 (natives): a sqla17 profile's LIKE is read now, each letter a class of its two cases
        assertTrue(pure("SELECT id FROM " + T + " WHERE vc LIKE 'a'", IqProfile.SQLA17_DEFAULT.settings(), COLUMNS)
                .contains("matches('(?s)^[aA]$')"));
        // IQ island, 2026-10-15 (D-S5, measured on IQ 17.1): under IQ's engine LIKE matches a CHAR(n) column's padded
        // value, which lite reads (D-S1); a computed pattern takes a one-character ESCAPE
        pure("SELECT id FROM " + T + " WHERE ch LIKE 'a%'", IqProfile.IQ16_DEFAULT.settings(), WITH_CHAR);
        pure("SELECT id FROM " + T + " WHERE ch || '' LIKE 'a%'", IqProfile.IQ16_DEFAULT.settings(), WITH_CHAR);
        assertTrue(pure("SELECT id FROM " + T + " WHERE vc LIKE vc ESCAPE '!'").contains("matches("));
        // IQ island, 2026-10-15 (the corpus lane, measured on IQ 17.1): a set of ASCII members reads now; an
        // unclosed one, an empty one, a dash at its edge do not
        assertTrue(pure("SELECT id FROM " + T + " WHERE vc LIKE '%[^a-z]%'").contains("[^a-z]"));
        // IQ island, 2026-10-17 (the corpus lane, measured on IQ 17.1): every character a member as itself, a dash at
        // a set's edge too, ranges by code point; a range that descends and a character past U+FFFF are not read
        for (String like : new String[] {"vc LIKE '[e-]'", "vc LIKE '[-+]1%'", "vc LIKE '%[%_]'", "vc LIKE '%[é-ÿ]%'",
                "vc LIKE '[[(\\]%'", "vc LIKE 'x[, ][:,-]%'"}) {
            assertTrue(pure("SELECT id FROM " + T + " WHERE " + like).contains("matches("), like);
        }
        // a UTF-8 database's characters past ISO-8859-1's
        assertTrue(pure("SELECT id FROM " + T + " WHERE vc LIKE '%[あ-ん]%'", IqProfile.IQ17_DEFAULT.settings(), COLUMNS)
                .contains("matches("));
        for (String like : new String[] {"vc LIKE 'a[b'", "vc LIKE '[]a'", "vc LIKE '[z-a]'", "vc LIKE '[\uD83D\uDE00]'",
                "vc LIKE 'a' ESCAPE '!!'",
                "vc LIKE 'a' ESCAPE ''", "vc LIKE '!a' ESCAPE '!'", "vc LIKE 'a!' ESCAPE '!'",
                "vc LIKE '" + "a".repeat(127) + "'"}) {
            assertEquals(SqlTranslationException.Kind.UNSUPPORTED, refusal("SELECT id FROM " + T + " WHERE " + like)
                    .kind(), like);
        }
        // IQ island, 2026-10-14 (the third landing): a pattern the database computes is matched as a constant one is,
        // its regular expression built there; a [ in it (IQ's set class) or a pattern of more than 126 characters
        // raises
        String computed = pure("SELECT id FROM " + T + " WHERE vc LIKE '%' || vc");
        assertTrue(computed.contains("matches('(?s)' + ") && computed.contains(", '[')")
                && computed.contains(") > 126"), computed);
        assertEquals(SqlTranslationException.Kind.UNSUPPORTED, refusal("SELECT id FROM " + T + " WHERE vc LIKE vc",
                IqProfile.SQLA17_DEFAULT.settings(), COLUMNS).kind());
        // a pattern's errors are refused before translation, as the syntax-case ratchet reads them
        assertThrows(SqlTranslationException.class, () -> IqDialect.of(IqProfile.IQ16_DEFAULT.settings()).parse(
                "SELECT 1 AS x WHERE 'a' LIKE 'a' ESCAPE '!!'"));
        pure("SELECT id FROM " + T + " WHERE vc LIKE '" + "a".repeat(126) + "'");
        // IQ island, 2026-10-14 (M1e): a date is matched as its text in the profile's DATE_FORMAT (IqDateTime,
        // measured) on SQL Anywhere's front end; under IQ's engine it is IQ's error -1001013 (2026-10-15)
        assertTrue(pure("SELECT id FROM " + T + " WHERE dt LIKE '2%'", IqProfile.SQLA17_DEFAULT.settings(), COLUMNS)
                .contains("lpad(4, '0')"));
        // IQ island, 2026-10-17 (measured on IQ 17.1): IQ converts the pattern to the date's type first -- one that is
        // no date is its -1009145, one that is a date its -1001013
        assertTrue(refusal("SELECT id FROM " + T + " WHERE dt LIKE '2%'").getMessage().contains("-1009145"));
        assertTrue(refusal("SELECT id FROM " + T + " WHERE dt LIKE '2026-03-04'").getMessage().contains("-1001013"));
    }

    // ------------------------------------------------------------------ M1a: NUMERIC and DOUBLE arithmetic

    @Test
    void numericArithmeticInSqlAnywheresFrontEndIsExact() {
        // an integer is a 38-digit decimal of scale 0 first; a NUMERIC is itself
        String p = pure("SELECT row_num * 1.5 AS a FROM sa_rowgenerator(1, 3)");
        assertTrue(p.contains("$r.row_num->toOne()->toDecimal() * 1.5D"), p);
        assertTrue(pure("SELECT 1.5 + 2.25 AS a").contains("1.5D + 2.25D"));
        // a product of more than 18 digits widens an operand, so that DuckDB does not compute it in 64 bits
        String nm = numbersSa("SELECT nm * nm AS a FROM " + T);
        assertTrue(nm.contains("+ toDecimal(0)"), nm);
        // with an untyped NULL: a DOUBLE NULL
        assertTrue(pure("SELECT 1.5 + NULL AS a").contains("[]->cast(@Float)"));
        // refused: the IQ engine's (D-N1), a result PRECISION caps or lite cannot hold (IQ island, 2026-10-14
        // (natives): / and % are read now, IqFunctionsTest's numericQuotientsAreSqlAnywheres)
        // (IQ island, 2026-10-15, D-N1 measured on IQ 17.1: IQ's engine types + - * % and a negation as SQL
        // Anywhere's front end does, so they are read there too)
        assertTrue(numbers("SELECT nm + 1 AS a FROM " + T).contains("+ toDecimal(1)"));
        numbers("SELECT -nm AS a FROM " + T);
        for (String sql : new String[] {"SELECT (row_num * 1.123456789012) * (row_num * 1.123456789012) AS a FROM"
                + " sa_rowgenerator(1, 2)"}) {
            assertEquals(SqlTranslationException.Kind.UNSUPPORTED, refusal(sql).kind(), sql);
        }
        pure("SELECT row_num / 1.5 AS a, row_num % 1.5 AS b FROM sa_rowgenerator(1, 2)");
        assertTrue(refusal("SELECT (row_num * 1.5) * (row_num * 1.25) * (row_num * 0.5) AS a FROM sa_rowgenerator(1,"
                + " 2)", IqProfile.SQLA17_DEFAULT.settings(), COLUMNS).getMessage().contains("PRECISION"));
        pure("SELECT (row_num * 1.5) * (row_num * 1.25) * (row_num * 0.5) AS a FROM sa_rowgenerator(1, 2)");
    }

    private static String numbersSa(String sql) {
        return pure(sql, IqProfile.SQLA17_DEFAULT.settings(), NUMBERS);
    }

    @Test
    void doubleArithmeticRaisesWhereIqDoes() {
        String p = numbers("SELECT d * i AS a FROM " + T);
        // the integer converted; past the largest DOUBLE an error (DuckDB would answer an infinity)
        assertTrue(p.contains("$r.i->toFloat()") && p.contains("> 1.7976931348623157E308"), p);
        assertTrue(numbers("SELECT d / 2 AS a FROM " + T).contains("== 0.0"));
        assertTrue(numbers("SELECT -d AS a FROM " + T).contains("-$r.d"));
        // a NUMERIC with a DOUBLE is a DOUBLE, under IQ's engine too (IQ island, 2026-10-15, measured on IQ 17.1)
        numbers("SELECT d + nm AS a FROM " + T);
        numbersSa("SELECT d + nm AS a FROM " + T);
        // under a row limit, computed for the rows IQ keeps (IQ island, 2026-10-16, the user's decision; these were
        // refused): a quotient's zero divisor raises for those only, in both engines
        assertTrue(numbers("SELECT TOP 1 i, d / d AS a FROM " + T + " ORDER BY i").contains("->limit(1)->extend("));
        assertTrue(saNumbers("SELECT TOP 1 i, d * d AS a FROM " + T + " ORDER BY i").contains("->limit(1)->extend("));
        // under IQ's engine a chain of + - * tests the largest DOUBLE once, at its end, and answers NULL past it
        String chain = numbers("SELECT d + d + d + d AS a FROM " + T);
        assertEquals(1, chain.split("1.7976931348623157E308", -1).length - 1, chain);
        assertEquals(SqlTranslationException.Kind.UNSUPPORTED, numbersRefusal("SELECT d + 'a' AS a FROM " + T).kind());
    }

    // ------------------------------------------------------------------ M1b: names, joins, subqueries, set operations

    @Test
    void anUnaliasedItemIsNamedAsSqlAnywhereRendersIt() {
        // a column qualified by its FROM item's name, written tight; words lower case; an IN list's constants first
        // (on SQL Anywhere's front end: an IN list holding a column is IQ's engine's -1001030, IQ island, 2026-10-15)
        String p = pure("SELECT id + 1, (sm) * 2, IF id IN (sm, NULL, 1) THEN 'a' ELSE 'b' ENDIF, ISNULL( sm , 0 ),"
                + " id % 2, - -id, NULLIF(id, 1) FROM " + T + " a", IqProfile.SQLA17_DEFAULT.settings(), COLUMNS);
        assertTrue(p.contains("->select(~['a.id+1', '(a.sm)*2', 'if a.id in( 1,a.sm,null ) then \\'a\\' else"
                + " \\'b\\' endif', 'ISNULL(a.sm,0)', 'remainder(a.id,2)', '- -a.id', 'NULLIF(a.id,1)'])"), p);
        // an unaliased tb('...T') is known by its table's name, as IQ knows a table
        assertTrue(pure("SELECT T.id + 1 FROM " + T).contains("'T.id+1'"));
        // NOT before a predicate is written inside it; IS DISTINCT FROM is IS NOT DISTINCT FROM negated
        assertTrue(pure("SELECT IF NOT vc LIKE 'x' THEN 1 ENDIF, IF id IS DISTINCT FROM 1 THEN 1 ENDIF FROM " + T)
                .contains("['if T.vc not like \\'x\\' then 1 endif', 'if not T.id is not distinct from 1 then 1"
                        + " endif']"));
        // a parenthesized query shows as its one item, named in its own query
        assertTrue(pure("SELECT (SELECT b.sm + 1 FROM " + T + " b WHERE b.id = 1) FROM " + T + " a").contains(
                "'b.sm+1'"));
        // IQ island, 2026-10-14 (the third landing, measured): a comment without a line break leaves no mark, a line
        // break leaves a space (and two for each CASE value level around it) where SQL Anywhere marks one
        assertTrue(pure("SELECT id /* c */ + 1 FROM " + T).contains("'T.id+1'"));
        assertTrue(pure("SELECT id\n+ 1, COALESCE(\nsm, 0), CASE WHEN id > 1\nTHEN id\n+ 1\nELSE 0 END FROM " + T)
                .contains("['T.id +1', 'COALESCE( T.sm,0)', 'case when T.id > 1 then   T.id   +1 else 0 end']"));
        // SQL Anywhere's own folding of a constant test over a table names the item otherwise: not named here
        assertTrue(refusal("SELECT IF 1 = 3 THEN vc ELSE 'b' ENDIF FROM " + T).getMessage().contains("needs an"));
        // IQ island, 2026-10-17 (measured on IQ 17.1): a plain derived table's column by its definition (t.it as x+1,
        // t.it+2 as x+1); a CAST's definition is not rendered yet
        assertTrue(pure("SELECT x + 1 FROM (SELECT id AS x FROM " + T + ") d").contains(" as x+1"));
        assertTrue(pure("SELECT x + 1 FROM (SELECT id + 2 AS x FROM " + T + ") d").contains("+2 as x+1"));
        assertTrue(refusal("SELECT x + 1 FROM (SELECT CAST(id AS SMALLINT) AS x FROM " + T + ") d").getMessage()
                .contains("needs an alias"));
        // a name no one sees (an EXISTS subquery's item) is never refused
        pure("SELECT id FROM " + T + " a WHERE EXISTS (SELECT b.sm + b.id FROM " + T + " b WHERE b.id = a.id)");
        // IQ island, 2026-10-14 (review 7's M1): an enclosing query's column read there is (the planner's pending fix)
        assertTrue(refusal("SELECT id FROM " + T + " a WHERE EXISTS (SELECT a.id + b.id FROM " + T + " b)")
                .getMessage().contains("an enclosing query's column read in a subquery's select list"));
    }

    @Test
    void joinsReadIqsColumnsAndIqsComparisons() {
        // a NATURAL join keeps both sides' columns: its condition is IQ's = of each shared column, ANDed with any ON
        String n = pure("SELECT a.id, b.vc FROM " + T + " a NATURAL JOIN " + T + " b ON a.id > 1");
        assertTrue(n.contains("$a.id > 1") && n.contains("$a.id == $b.b_id") && n.contains("startsWith"), n);
        assertTrue(refusal("SELECT id FROM " + T + " a NATURAL JOIN " + T + " b").getMessage().contains("-144"));
        // the FROM clause's errors IQ raises before running a query
        assertEquals(SqlTranslationException.Kind.INVALID, refusal("SELECT 1 FROM " + T + " a, " + T + " a").kind());
        // IQ island, 2026-10-14 (M1c): names alike but for case, and an ON naming an item before the comma, are refused
        // by name with IQ's code (the research's syntax cases are split on both)
        assertTrue(refusal("SELECT 1 FROM " + T + " a, " + T + " A").getMessage().contains("-139"));
        assertTrue(refusal("SELECT 1 FROM " + T + " a, " + T + " b JOIN " + T + " c ON a.id = c.id").getMessage()
                .contains("-824"));
        assertEquals(SqlTranslationException.Kind.UNSUPPORTED, refusal("SELECT 1 FROM " + T + " a JOIN " + T
                + " b").kind());
        // two unaliased references to one table: SQL Anywhere reads them as one
        assertEquals(SqlTranslationException.Kind.UNSUPPORTED, refusal("SELECT 1 FROM " + T + ", " + T).kind());
    }

    @Test
    void subqueriesComparedByIqsRulesWhereIqsEngineTakesThem() {
        // ANY and IN compare each row as IQ's = does (blank padding)
        assertTrue(pure("SELECT id FROM " + T + " a WHERE vc = ANY (SELECT b.vc FROM " + T + " b)").contains(
                "startsWith"));
        // IQ island, 2026-10-17 (the corpus lane's SEDE, measured on IQ 17.1): an IN subquery that is a UNION is read
        assertTrue(pure("SELECT id FROM " + T + " WHERE id IN (SELECT 1 FROM DUMMY UNION SELECT 2 FROM DUMMY)")
                .contains("concatenate("));
        // the IQ engine's restrictions, refused by name; SQL Anywhere's front end takes them
        for (String sql : new String[] {"SELECT id FROM " + T + " WHERE id + (SELECT 1 FROM DUMMY) = 2",
                "SELECT IF id = 1 THEN (SELECT 1 FROM DUMMY) ENDIF AS x FROM " + T,
                "SELECT COALESCE((SELECT 1 FROM DUMMY), id) AS x FROM " + T,
                "SELECT SUM((SELECT 1 FROM DUMMY)) AS x FROM " + T,
                "SELECT id FROM " + T + " WHERE id IN (SELECT 1 FROM DUMMY INTERSECT SELECT 2 FROM DUMMY)"}) {
            // IQ island, 2026-10-15 (measured on IQ 17.1): one inside a conditional expression or an aggregate names
            // IQ's error, -1001030
            String m = refusal(sql).getMessage();
            assertTrue(m.contains("IQ's engine") || m.contains("IQ's error -1001030 under its engine"), sql);
            pure(sql, IqProfile.SQLA17_DEFAULT.settings());
        }
        // IQ island, 2026-10-17 (measured on IQ 17.1): a subquery in the statement's ORDER BY is IQ's -1005004; SQL
        // Anywhere's front end reads it
        String orderedBySubquery = "SELECT id FROM " + T + " ORDER BY (SELECT 1 FROM DUMMY)";
        assertTrue(refusal(orderedBySubquery).getMessage().contains("IQ's error -1005004"));
        pure(orderedBySubquery, IqProfile.SQLA17_DEFAULT.settings());
        // IQ island, 2026-10-15 (measured on IQ 17.1): it takes more than the research's flag list held -- a scalar
        // subquery in the select list's arithmetic, a function's arguments, a CAST; one in a join's ON and BETWEEN
        for (String sql : new String[] {"SELECT id + (SELECT 1 FROM DUMMY) AS x FROM " + T,
                "SELECT ABS((SELECT 1 FROM DUMMY) - id) AS x FROM " + T,
                "SELECT a.id FROM " + T + " a JOIN " + T + " b ON EXISTS (SELECT 1 FROM DUMMY)",
                "SELECT id FROM " + T + " WHERE id BETWEEN (SELECT 1 FROM DUMMY) AND 3"}) {
            pure(sql);
        }
        // one column where one is read (IQ's -151); a row limit in a subquery is refused
        assertTrue(refusal("SELECT id FROM " + T + " WHERE id IN (SELECT 1, 2 FROM DUMMY)").getMessage().contains(
                "-151"));
        assertEquals(SqlTranslationException.Kind.UNSUPPORTED, refusal("SELECT id FROM " + T + " WHERE id IN (SELECT"
                + " TOP 1 row_num FROM sa_rowgenerator(1, 2) ORDER BY 1)").kind());
        // the engine: a subquery reading a table makes the statement the IQ engine's
        assertEquals(IqDialect.Engine.IQ, IqDialect.engineOf(IqParser.parse("SELECT (SELECT id FROM " + T + ") AS x",
                IqProfile.IQ16_DEFAULT.settings()), IqProfile.IQ16_DEFAULT.settings()));
        assertEquals(IqDialect.Engine.SA, IqDialect.engineOf(IqParser.parse("SELECT (SELECT 1 FROM DUMMY) AS x UNION"
                + " SELECT row_num FROM sa_rowgenerator(1, 2)", IqProfile.IQ16_DEFAULT.settings()), IqProfile.IQ16_DEFAULT.settings()));
    }

    @Test
    void setOperationsTypeTheirColumnsByIqsUnification() {
        assertTrue(numbers("SELECT sm AS x FROM " + T + " UNION SELECT bg FROM " + T).contains("->distinct()"));
        pure("SELECT NULL UNION ALL SELECT id FROM " + T);
        for (String sql : new String[] {"SELECT vc FROM " + T + " UNION ALL SELECT id FROM " + T,
                "SELECT NULL INTERSECT SELECT NULL"}) {
            assertEquals(SqlTranslationException.Kind.UNSUPPORTED, refusal(sql).kind(), sql);
        }
        // IQ island, 2026-10-15 (measured on IQ 17.1): under IQ's engine a NULL takes the other side's type and
        // NUMERICs unify, each side's values converted; SQL Anywhere's front end refuses them by name still
        assertTrue(pure("SELECT NULL UNION ALL SELECT vc FROM " + T).contains("cast(@String)"));
        assertTrue(numbers("SELECT nm FROM " + T + " UNION SELECT i FROM " + T).contains("toDecimal()"));
        assertEquals(SqlTranslationException.Kind.UNSUPPORTED, refusal("SELECT NULL UNION ALL SELECT vc FROM " + T,
                IqProfile.SQLA17_DEFAULT.settings(), COLUMNS).kind());
        assertTrue(refusal("SELECT nm FROM " + T + " UNION SELECT nm FROM " + T, IqProfile.SQLA17_DEFAULT.settings(),
                NUMBERS).getMessage().contains("D-N1"));
        // a set operation's ORDER BY: a position or a first-branch name; anything else IQ's -152
        assertTrue(refusal("SELECT 1 a UNION SELECT 2 ORDER BY a + 1").getMessage().contains("-152"));
        assertTrue(refusal("SELECT 1 a UNION SELECT 2 b ORDER BY b").getMessage().contains("-854"));
        assertTrue(refusal("SELECT 1, 2 UNION SELECT 3").getMessage().contains("-153"));
    }

    @Test
    void groupingReadsIqsAliasesAndConstants() {
        // IQ island, 2026-10-14 (M1c): an alias before a column of its name, in GROUP BY and HAVING
        String byAlias = pure("SELECT sm AS id, COUNT(*) AS n FROM " + T + " GROUP BY id HAVING n > 1");
        assertTrue(byAlias.contains("groupBy(~[sm]"), byAlias);
        assertTrue(byAlias.contains("> 1"), byAlias);
        // an integer is a constant: IQ's -149 for a column it does not group; else every row one group (IQ island,
        // 2026-10-17, measured on IQ 17.1: GROUP BY 1, 'Total' and NULL), its key a computed column
        assertTrue(refusal("SELECT sm, COUNT(*) FROM " + T + " GROUP BY 1").getMessage().contains("-149"));
        for (String key : new String[] {"1", "2", "'Total'", "NULL"}) {
            assertTrue(pure("SELECT COUNT(*) FROM " + T + " GROUP BY " + key).contains("groupBy("), key);
        }
        // a constant beside another key divides no group
        assertTrue(pure("SELECT sm FROM " + T + " GROUP BY sm, 1").contains("distinct()"));
        // a string key shown shows one value of its group (IQ island, 2026-10-15); a column of a GROUP BY expression
        // read outside it is refused
        assertTrue(pure("SELECT vc FROM " + T + " GROUP BY vc").contains("->min()"));
        assertEquals(SqlTranslationException.Kind.UNSUPPORTED, refusal("SELECT 1 + sm FROM " + T + " GROUP BY sm + 1")
                .kind());
    }

    @Test
    void theAggregatesTypesAndChecks() {
        // SUM of an INTEGER in SQL Anywhere's front end: its running total checked, the positive and the negative
        // values' sums (D-E3); under IQ's engine none (IQ island, 2026-10-15, D-N3: IQ answers past 2^31)
        String sum = saNumbers("SELECT SUM(i) AS s FROM " + T);
        assertTrue(sum.contains("2147483647") && sum.contains("-2147483648"), sum);
        String iqSum = numbers("SELECT SUM(i) AS s FROM " + T);
        assertTrue(iqSum.contains("sum()") && !iqSum.contains("2147483647"), iqSum);
        // a DOUBLE's SUM and AVG checked against the largest DOUBLE
        assertTrue(numbers("SELECT AVG(d) AS a FROM " + T).contains("average()"));
        // IQ island, 2026-10-15 (D-N1, measured on IQ 17.1): under the IQ engine AVG of an integer is SUM / COUNT cut
        // toward zero at scale 16, and SUM of a NUMERIC(p,s) a NUMERIC(p+1,s)
        String avg = numbers("SELECT AVG(i) AS a FROM " + T);
        assertTrue(avg.contains("sum()") && avg.contains("count()") && avg.contains("10000000000000000"), avg);
        assertFalse(numbers("SELECT SUM(nm) AS s FROM " + T).contains("D-N1"));
        // COUNT is IQ's UNSIGNED BIGINT under its engine (D-F15): arithmetic on it is refused; INTEGER in SA's
        // (IQ island, 2026-10-15: IQ's engine widens a COUNT's arithmetic as any integer's, D-N3)
        assertFalse(numbers("SELECT COUNT(*) + 1 AS c FROM " + T).contains("UNSIGNED"));
        assertTrue(saNumbers("SELECT COUNT(*) + 1 AS c FROM " + T).contains("count()"));
        pure("SELECT COUNT(*) + 1 AS c FROM sa_rowgenerator(1, 3)");
        // MEDIAN of an integer floored; of a BIT IQ's -156; of a BIGINT refused
        assertTrue(numbers("SELECT MEDIAN(sm) AS m FROM " + T).contains("floor"));
        assertTrue(numbersRefusal("SELECT MEDIAN(bt) AS m FROM " + T).getMessage().contains("-156"));
        assertEquals(SqlTranslationException.Kind.UNSUPPORTED, numbersRefusal("SELECT MEDIAN(bg) AS m FROM " + T)
                .kind());
        // LIST of an integer column ordered by itself; any other order is the plan's
        assertTrue(pure("SELECT LIST(id ORDER BY id) AS l FROM " + T).contains("joinStrings"));
        // IQ island, 2026-10-15: without ORDER BY a LIST's values are in the plan's order, IQ's as the database's
        assertTrue(pure("SELECT LIST(id) AS l FROM " + T).contains("joinStrings"));
        assertTrue(refusal("SELECT LIST(row_num ORDER BY row_num), LIST(row_num ORDER BY row_num DESC) FROM"
                + " sa_rowgenerator(1, 3)").getMessage().contains("-904"));
        assertTrue(refusal("SELECT LIST(id ORDER BY id), LIST(sm ORDER BY sm) FROM " + T).getMessage().contains(
                "D-A1"));
        // IQ's errors raised before running: -154, -949, -152
        assertTrue(refusal("SELECT SUM(id, sm) FROM " + T).getMessage().contains("-154"));
        assertTrue(refusal("SELECT MIN(id ORDER BY id) FROM " + T).getMessage().contains("-949"));
        assertTrue(refusal("SELECT LIST(id ORDER BY 1) FROM " + T).getMessage().contains("-152"));
        // IQ island, 2026-10-14 (the merge): strings by IQ's collation -- COUNT(DISTINCT) counts the keys, MIN and MAX
        // read only strings that are their own keys (IQ shows the first value of a key it meets)
        // IQ island, 2026-10-15: MAX of a column of strings shows a value of the greatest key, packed behind its key
        String max = pure("SELECT MAX(vc) AS m FROM " + T);
        assertTrue(max.contains("->max()") && max.contains("indexOf") && max.contains("regexpReplace(' +$'"), max);
        assertTrue(pure("SELECT MAX(RTRIM(vc)) AS m FROM " + T).contains("->max()"));
        assertTrue(pure("SELECT COUNT(DISTINCT vc) AS m FROM " + T).contains("regexpReplace(' +$', '', false)"));
    }

    @Test
    void aCheckedSumReadForSomeGroupsOnlyIsRefused() {
        // IQ island, 2026-10-14 (review 3's B1): D-E3's check is computed where the SUM is read, so a SUM read inside
        // IF, CASE, COALESCE, under AND/OR in HAVING or under a row limit is refused; read everywhere, it is checked
        // (the third landing: a CASE's first test, a NULL-reading function's first argument, a NOT's operand are read
        // for every group)
        for (String sql : List.of(
                "SELECT sm, IF sm = 4 THEN 0 ELSE SUM(i) ENDIF AS s FROM " + T + " GROUP BY sm",
                "SELECT sm, COALESCE(MAX(i), SUM(i)) AS s FROM " + T + " GROUP BY sm",
                "SELECT sm FROM " + T + " GROUP BY sm HAVING COUNT(*) > 2 AND SUM(i) > 0",
                "SELECT TOP 1 sm, SUM(i) AS s FROM " + T + " GROUP BY sm ORDER BY sm")) {
            // SQL Anywhere's front end checks the running total (IQ island, 2026-10-15: IQ's engine checks none,
            // D-N3, so there each is read); IQ island, 2026-10-17: read so by the statement's own SELECT, the check is
            // a filter of every group after the aggregation (IqAggregates.readEverywhere's guard)
            assertTrue(saNumbers(sql).contains("running total leaves its type in a group"), sql);
            assertTrue(numbers(sql).contains("sum()"), sql);
        }
        // from a query around it, which may keep groups from the aggregation itself, still refused
        assertTrue(saNumbersRefusal("SELECT * FROM (SELECT sm, SUM(i) AS s FROM " + T + " GROUP BY sm) d WHERE sm = 1")
                .getMessage().contains("D-E3"));
        assertTrue(saNumbers("SELECT sm, SUM(i) AS s FROM " + T + " GROUP BY sm HAVING SUM(i) > 0").contains("sum()"));
        for (String sql : List.of("SELECT sm FROM " + T + " GROUP BY sm HAVING NOT (SUM(d) <= 0)",
                "SELECT sm, IF SUM(i) > 0 THEN 1 ELSE 0 ENDIF AS s FROM " + T + " GROUP BY sm",
                "SELECT sm, COALESCE(SUM(i), 0) AS s FROM " + T + " GROUP BY sm",
                "SELECT sm, CASE WHEN SUM(i) > 9 THEN 'a' WHEN SUM(i) > 0 THEN 'b' END AS s FROM " + T
                        + " GROUP BY sm")) {
            numbers(sql);
        }
        // a NUMERIC's SUM has no check to miss
        pure("SELECT SUM(nm) AS s FROM " + T + " HAVING COUNT(*) > 2 AND SUM(nm) > 0", IqProfile.SQLA17_DEFAULT.settings(),
                NUMBERS);
    }

    @Test
    void aStarBeforeADerivedTablesItemHidesWhichColumnItIs() {
        // IQ island, 2026-10-14 (review 4's B1): after a *, item i is some later column, so each name of the column
        // list from i on may be the folded constant's (a quotient over it is refused, IqDialect.folded)
        SqlAst.Statement s = IqParser.parse("SELECT c FROM (SELECT *, 2 + 3 FROM t) d(a, b, c)",
                IqProfile.IQ16_DEFAULT.settings());
        assertTrue(IqDialect.folded(s).containsAll(List.of("b", "c")), IqDialect.folded(s).toString());
        SqlAst.Statement plain = IqParser.parse("SELECT c FROM (SELECT id, 2 + 3 FROM t) d(a, c)",
                IqProfile.IQ16_DEFAULT.settings());
        assertEquals(java.util.Set.of("c"), IqDialect.folded(plain));
    }

    @Test
    void stringKeysAreIqsCollation() {
        // IQ island, 2026-10-14 (the merge of M1c and the natives slice): a GROUP BY key not shown groups by the
        // collation key; IQ island, 2026-10-15: one shown (or read in HAVING) groups by it too and shows one value of
        // each group, the least of its values (a member: which one IQ shows is its plan's choice); a set operation
        // reads only strings that are their own keys
        String hidden = pure("SELECT COUNT(*) AS n FROM " + T + " GROUP BY vc");
        assertTrue(hidden.contains("regexpReplace(' +$', '', false)") && hidden.contains("groupBy(")
                && !hidden.contains("->min()"), hidden);
        String shown = pure("SELECT vc, COUNT(*) AS n FROM " + T + " GROUP BY vc");
        assertTrue(shown.contains("regexpReplace(' +$', '', false)") && shown.contains("->min()"), shown);
        assertTrue(pure("SELECT COUNT(*) AS n FROM " + T + " GROUP BY vc HAVING vc > 'a'").contains("->min()"));
        assertFalse(pure("SELECT RTRIM(vc) AS r, COUNT(*) AS n FROM " + T + " GROUP BY RTRIM(vc)").contains(
                "regexpReplace(' +$'"));
        // IQ island, 2026-10-15: a UNION of strings that may share a key groups by it, showing a value of each
        String union = pure("SELECT vc FROM " + T + " UNION SELECT 'a'");
        assertTrue(union.contains("groupBy(") && union.contains("->min()") && !union.contains("distinct()"), union);
        assertTrue(refusal("SELECT vc FROM " + T + " INTERSECT SELECT 'a'", IqProfile.SQLA17_DEFAULT.settings(),
                COLUMNS).getMessage().contains("INTERSECT over strings"));
        // IQ island, 2026-10-15 (measured on IQ 17.1): IQ's engine runs no INTERSECT or EXCEPT at all (its -1001030)
        assertTrue(refusal("SELECT vc FROM " + T + " INTERSECT SELECT 'a'").getMessage().contains("-1001030"));
        pure("SELECT RTRIM(vc) AS r FROM " + T + " UNION SELECT 'a'");
        // a sqla17 profile folds the ASCII letters: LOWER is its own key there, RTRIM is not (a member is shown)
        assertTrue(pure("SELECT DISTINCT RTRIM(vc) AS r FROM " + T, IqProfile.SQLA17_DEFAULT.settings(), COLUMNS)
                .contains("->min()"));
        assertFalse(pure("SELECT DISTINCT LOWER(vc) AS r FROM " + T, IqProfile.SQLA17_DEFAULT.settings(), COLUMNS)
                .contains("->min()"));
        // a derived table's column is what its definition is
        assertFalse(pure("SELECT DISTINCT x FROM (SELECT RTRIM(vc) AS x FROM " + T + ") d").contains("->min()"));
        assertTrue(pure("SELECT DISTINCT x FROM (SELECT vc AS x FROM " + T + ") d").contains("->min()"));
    }

    @Test
    void theSeventhReviewsFindings() {
        // IQ island, 2026-10-14 (review 7, measured on SQL Anywhere 17). B2: a query without FROM read as its item is
        // named by its own alias, which no clause reads, and names no derived table's column
        assertTrue(pure("SELECT (SELECT id + 1 AS z) FROM " + T).contains("z:r|"));
        assertTrue(pure("SELECT (SELECT id AS z) FROM " + T).contains("z:r|"));
        SqlTranslationException nameless = refusal("SELECT * FROM (SELECT (SELECT id AS z) FROM " + T + ") d");
        assertEquals(SqlTranslationException.Kind.INVALID, nameless.kind());
        assertTrue(nameless.getMessage().contains("SQLCODE -163"), nameless.getMessage());
        assertEquals(SqlTranslationException.Kind.UNSUPPORTED, refusal("SELECT (SELECT id + 1 AS z) FROM " + T
                + " ORDER BY z").kind());
        // M1: an enclosing query's column read in a subquery's select list (the planner's fix is pending)
        assertTrue(refusal("SELECT id FROM " + T + " a WHERE id IN (SELECT a.id FROM " + T + " b)").getMessage()
                .contains("a planner defect whose fix is pending"));
        // M2: sa_rowgenerator's owner-qualified column is -845; a keyed LIST's ORDER BY is read for its errors
        assertTrue(refusal("SELECT dbo.sa_rowgenerator.row_num FROM sa_rowgenerator(1, 3)").getMessage().contains(
                "SQLCODE -845"));
        assertTrue(refusal("SELECT sm, LIST(sm ORDER BY SUM(id)) AS l FROM " + T + " GROUP BY sm").getMessage()
                .contains("SQLCODE -156"));
        assertEquals(SqlTranslationException.Kind.UNSUPPORTED, refusal("SELECT sm, LIST(sm ORDER BY id + 1) AS l"
                + " FROM " + T + " GROUP BY sm").kind());
        pure("SELECT sm, LIST(sm ORDER BY vc) AS l FROM " + T + " GROUP BY sm");
        // m1: LIST(DISTINCT key ORDER BY ...) under an ORDER BY
        assertEquals(SqlTranslationException.Kind.UNSUPPORTED, refusal("SELECT sm, LIST(DISTINCT sm ORDER BY sm) AS l"
                + " FROM " + T + " GROUP BY sm ORDER BY 1").kind());
        // m2: -831 where two aliases of one name each read it; -854 where the ORDER BY reads a column not grouped
        assertTrue(refusal("SELECT id AS ID, id + 1 AS Id FROM " + T).getMessage().contains("SQLCODE -831"));
        assertTrue(refusal("SELECT id AS ID, sm + 1 AS Id FROM " + T).getMessage().contains("SQLCODE -830"));
        assertTrue(refusal("SELECT sm, id + 1 FROM " + T + " GROUP BY sm ORDER BY id").getMessage().contains(
                "SQLCODE -854"));
        assertTrue(refusal("SELECT id FROM " + T + " UNION SELECT id, id FROM " + T).getMessage().contains(
                "SQLSTATE 53026"));
        // m3: a computed LIKE pattern IQ matches otherwise raises by name
        assertTrue(pure("SELECT id FROM " + T + " WHERE vc LIKE vc").contains("fail('"
                + IqDialect.REFUSED_AT_RUN_TIME + "a LIKE pattern"));
        // beside B1: an ORDER BY inside EXISTS is IQ's syntax error
        SqlTranslationException exists = refusal("SELECT id FROM " + T + " a WHERE EXISTS (SELECT 1 FROM " + T
                + " b WHERE b.id = a.id ORDER BY b.sm)");
        assertEquals(SqlTranslationException.Kind.SYNTAX, exists.kind());
        assertTrue(exists.getMessage().contains("SQLCODE -131"), exists.getMessage());
    }

    @Test
    void aGroupedQuerysItemsAreNamedAsIqFirstReadsTheirParts() {
        // in SQL Anywhere's front end (a SQL Anywhere database's tables), HAVING is read before the select list, which
        // is read from the left
        IqSettings sa = IqProfile.SQLA17_DEFAULT.settings();
        assertTrue(pure("SELECT SUM(SM) FROM " + T + " HAVING sum(sm) > 0", sa).contains("'sum(T.sm)'"));
        assertTrue(pure("SELECT SUM(sm), sum(sm) + 1 FROM " + T, sa).contains("'SUM(T.sm)+1'"));
        // a GROUP BY item's spelling, its parentheses gone
        assertTrue(pure("SELECT SM, sm * 2 FROM " + T + " GROUP BY sm", sa).contains("'T.sm*2'"));
        assertTrue(pure("SELECT (sm + 1) * 2 FROM " + T + " GROUP BY sm + 1", sa).contains("'T.sm+1*2'"));
        // an aggregate IQ may read as an earlier one, or one in parentheses: not known
        assertEquals(SqlTranslationException.Kind.UNSUPPORTED, refusal("SELECT SUM(sm + 1), SUM(1 + sm) FROM " + T, sa,
                COLUMNS).kind());
        assertEquals(SqlTranslationException.Kind.UNSUPPORTED, refusal("SELECT (SUM(sm)) * 2 FROM " + T).kind());
        // IQ island, 2026-10-16 (measured on IQ 17.1): IQ's engine names each item by its own spelling
        assertTrue(pure("SELECT SUM(SM) FROM " + T + " HAVING sum(sm) > 0").contains("'SUM(T.SM)'"));
        String spelled = pure("SELECT SM, sm * 2 FROM " + T + " GROUP BY sm");
        assertTrue(spelled.contains(":r|$r.sm, 'T.sm*2':") && spelled.endsWith("~SM)"), spelled);
        assertTrue(pure("SELECT (sm + 1) * 2 FROM " + T + " GROUP BY sm + 1").contains("'(T.sm+1)*2'"));
    }

    @Test
    void aColumnNamedByAReservedWordIsNamedInDoubleQuotes() {
        // IQ island, 2026-10-16 (measured on IQ 17.1; BIRD's judged bird-1039): t."date", t."order"; t.value bare
        List<Col> reserved = List.of(new Col("id", "Integer", false, "INTEGER"),
                new Col("date", "StrictDate", true, "DATE"), new Col("value", "Integer", true, "INTEGER"));
        String pure = pure("SELECT COUNT(q.\"date\"), q.value + 1 FROM " + T + " q GROUP BY q.value",
                IqProfile.IQ16_DEFAULT.settings(), reserved);
        assertTrue(pure.contains("'COUNT(q.\"date\")'"), pure);
        assertTrue(pure.contains("'q.value+1'"), pure);
    }

    @Test
    void anUnaliasedWindowIsNamedAsIqNamesIt() {
        // IQ island, 2026-10-15 (measured on IQ 17.1 and SQL Anywhere 17): the call, then over(...) in lower case
        assertTrue(pure("SELECT SUM(sm) OVER (PARTITION BY id ORDER BY sm DESC ROWS 2 PRECEDING) FROM " + T)
                .contains("'SUM(T.sm) over(partition by T.id order by T.sm desc rows 2 preceding)'"));
        assertTrue(pure("SELECT COUNT(*) OVER () FROM " + T).contains("'COUNT() over()'"));
        assertTrue(pure("SELECT ROW_NUMBER() OVER (ORDER BY id) - 1 FROM " + T)
                .contains("'ROW_NUMBER() over(order by T.id asc)-1'"));
        assertTrue(pure("SELECT MAX(sm) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM " + T)
                .contains("'MAX(T.sm) over(order by T.id asc rows between 1 preceding and current row)'"));
        assertTrue(pure("SELECT SUM(sm) OVER w FROM " + T + " WINDOW w AS (ORDER BY id)")
                .contains("'SUM(T.sm) over w'"));
        assertTrue(pure("SELECT SUM(sm) OVER (w ORDER BY id) FROM " + T + " WINDOW w AS (PARTITION BY sm)")
                .contains("'SUM(T.sm) over(w order by T.id asc)'"));
        // in parentheses inside an expression IQ keeps them or drops them by the operator: not known
        assertEquals(SqlTranslationException.Kind.UNSUPPORTED, refusal("SELECT (SUM(sm) OVER ()) * 2 FROM " + T)
                .kind());
    }
}
