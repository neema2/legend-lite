// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol.sql;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The Postgres reader ({@link SqlParser}, {@link SqlLexer}): Postgres's lexical rules and operator precedence, every
 * clause it accepts, and the refusals by name and position of what it does not accept yet.
 */
class PostgresParserTest {

    private static SqlAst.Select select(String sql) {
        return (SqlAst.Select) SqlParser.parse(sql);
    }

    private static SqlAst.Expr where(String condition) {
        return java.util.Objects.requireNonNull(select("select 1 from t where " + condition).where());
    }

    private static SqlAst.Expr item(String expr) {
        return ((SqlAst.SelectExpr) select("select " + expr + " from t").items().get(0)).expr();
    }

    // ------------------------------------------------------------------ clauses

    @Test
    void everyClauseOfASelect() {
        SqlAst.Select s = select("SELECT DISTINCT a, b AS c, d e FROM tb('p::DB.T') AS x WHERE a > 1"
                + " GROUP BY a, 2 HAVING count(*) > 1 ORDER BY a DESC NULLS LAST, 2 LIMIT 10 OFFSET 5;");
        assertTrue(s.distinct());
        assertEquals(3, s.items().size());
        assertEquals("c", java.util.Objects.requireNonNull(((SqlAst.SelectExpr) s.items().get(1)).alias()).text());
        assertEquals("e", java.util.Objects.requireNonNull(((SqlAst.SelectExpr) s.items().get(2)).alias()).text());
        SqlAst.TableFunction from = assertInstanceOf(SqlAst.TableFunction.class, s.from().get(0));
        assertEquals("tb", from.name().get(0).text());
        assertEquals("x", java.util.Objects.requireNonNull(from.alias()).text());
        assertEquals(2, s.groupBy().size());
        assertInstanceOf(SqlAst.Binary.class, s.having());
        assertTrue(s.orderBy().get(0).descending());
        assertEquals(Boolean.FALSE, s.orderBy().get(0).nullsFirst());
        assertNull(s.orderBy().get(1).nullsFirst());
        assertEquals(BigInteger.TEN, ((SqlAst.IntegerLit) java.util.Objects.requireNonNull(s.limit())).value());
        assertEquals(BigInteger.valueOf(5), ((SqlAst.IntegerLit) java.util.Objects.requireNonNull(s.offset())).value());
    }

    @Test
    void offsetBeforeLimitAndFetchFirst() {
        SqlAst.Select s = select("select a from t offset 2 rows limit 3");
        assertEquals(BigInteger.valueOf(3), ((SqlAst.IntegerLit) java.util.Objects.requireNonNull(s.limit())).value());
        SqlAst.Select f = select("select a from t fetch first 4 rows only");
        assertEquals(BigInteger.valueOf(4), ((SqlAst.IntegerLit) java.util.Objects.requireNonNull(f.limit())).value());
        SqlAst.Select one = select("select a from t fetch next row only");
        assertEquals(BigInteger.ONE, ((SqlAst.IntegerLit) java.util.Objects.requireNonNull(one.limit())).value());
        assertNull(select("select a from t limit all").limit());
    }

    @Test
    void starsAndQualifiedStars() {
        SqlAst.Select s = select("select *, t.* from t");
        assertNull(((SqlAst.AllColumns) s.items().get(0)).qualifier());
        assertEquals("t", java.util.Objects.requireNonNull(((SqlAst.AllColumns) s.items().get(1)).qualifier()).text());
    }

    @Test
    void namedArgumentsToATableFunction() {
        SqlAst.TableFunction f = (SqlAst.TableFunction) select("select a from service('/x', p => 1)").from().get(0);
        assertNull(f.args().get(0).name());
        assertEquals("p", java.util.Objects.requireNonNull(f.args().get(1).name()).text());
    }

    // ------------------------------------------------------------------ precedence

    @Test
    void andBindsTighterThanOrAndNotTighterThanAnd() {
        SqlAst.Binary or = (SqlAst.Binary) where("a = 1 or b = 2 and not c = 3");
        assertEquals(SqlAst.BinaryOp.OR, or.op());
        SqlAst.Binary and = (SqlAst.Binary) or.right();
        assertEquals(SqlAst.BinaryOp.AND, and.op());
        SqlAst.Unary not = (SqlAst.Unary) and.right();
        assertEquals(SqlAst.UnaryOp.NOT, not.op());
        assertEquals(SqlAst.BinaryOp.EQ, ((SqlAst.Binary) not.operand()).op());
    }

    @Test
    void multiplicationBindsTighterThanAdditionAndUnaryMinusTighterStill() {
        SqlAst.Binary plus = (SqlAst.Binary) item("a + b * -c");
        assertEquals(SqlAst.BinaryOp.PLUS, plus.op());
        SqlAst.Binary times = (SqlAst.Binary) plus.right();
        assertEquals(SqlAst.BinaryOp.TIMES, times.op());
        assertEquals(SqlAst.UnaryOp.MINUS, ((SqlAst.Unary) times.right()).op());
    }

    @Test
    void concatenationBindsLooserThanAddition() {
        SqlAst.Binary concat = (SqlAst.Binary) item("a || b + 1");
        assertEquals(SqlAst.BinaryOp.CONCAT, concat.op());
        assertEquals(SqlAst.BinaryOp.PLUS, ((SqlAst.Binary) concat.right()).op());
    }

    @Test
    void powerIsLeftAssociativeAndLooserThanASign() {
        // Postgres: - 2 ^ 2 is 4, 2 ^ 3 ^ 2 is 64
        SqlAst.Binary signed = (SqlAst.Binary) item("- 2 ^ 2");
        assertEquals(SqlAst.BinaryOp.POWER, signed.op());
        assertEquals(SqlAst.UnaryOp.MINUS, ((SqlAst.Unary) signed.left()).op());
        SqlAst.Binary chain = (SqlAst.Binary) item("2 ^ 3 ^ 2");
        assertEquals(SqlAst.BinaryOp.POWER, ((SqlAst.Binary) chain.left()).op());
        assertEquals(SqlAst.BinaryOp.POWER, ((SqlAst.Binary) ((SqlAst.Binary) item("a * b ^ c")).right()).op());
    }

    @Test
    void everyOtherOperatorSharesPostgresOpLevel() {
        // looser than + -, tighter than LIKE and =, left-associative with ||
        SqlAst.Operator regex = (SqlAst.Operator) item("a + b ~ c");
        assertEquals("~", regex.operator());
        assertEquals(SqlAst.BinaryOp.PLUS, ((SqlAst.Binary) java.util.Objects.requireNonNull(regex.left())).op());
        SqlAst.Operator afterConcat = (SqlAst.Operator) item("a || b ~ c");
        assertEquals(SqlAst.BinaryOp.CONCAT,
                ((SqlAst.Binary) java.util.Objects.requireNonNull(afterConcat.left())).op());
        SqlAst.Binary eq = (SqlAst.Binary) where("j ->> 'k' = 'v'");
        assertEquals("->>", ((SqlAst.Operator) eq.left()).operator());
        assertInstanceOf(SqlAst.Operator.class, ((SqlAst.Like) where("a ~ b like c")).operand());
        // a prefix operator takes all that binds tighter than its level
        SqlAst.Operator prefix = (SqlAst.Operator) item("@ a * 2 + 1");
        assertNull(prefix.left());
        assertEquals(SqlAst.BinaryOp.PLUS, ((SqlAst.Binary) prefix.right()).op());
    }

    @Test
    void operatorsAreReadByPostgresRule() {
        // =- has no character outside SQL's operators, so it is = then -
        SqlAst.Binary eq = (SqlAst.Binary) item("a=-1");
        assertEquals(SqlAst.BinaryOp.EQ, eq.op());
        assertEquals(SqlAst.UnaryOp.MINUS, ((SqlAst.Unary) eq.right()).op());
        assertEquals("@-", ((SqlAst.Operator) item("a @- 1")).operator());
        // an operator stops where a comment starts
        assertEquals(SqlAst.BinaryOp.LT, ((SqlAst.Binary) item("1 </*c*/ 2")).op());
        assertInstanceOf(SqlAst.IntegerLit.class, ((SqlAst.SelectExpr) select("select 2--1").items().get(0)).expr());
    }

    @Test
    void betweenTakesItsOwnAnd() {
        SqlAst.Binary and = (SqlAst.Binary) where("a between 1 and 2 and b");
        assertEquals(SqlAst.BinaryOp.AND, and.op());
        SqlAst.Between between = (SqlAst.Between) and.left();
        assertEquals(BigInteger.TWO, ((SqlAst.IntegerLit) between.high()).value());
        assertTrue(((SqlAst.Between) where("a not between 1 and 2")).negated());
    }

    @Test
    void isBindsLooserThanComparison() {
        SqlAst.IsNull isNull = (SqlAst.IsNull) where("a = b is not null");
        assertTrue(isNull.negated());
        assertEquals(SqlAst.BinaryOp.EQ, ((SqlAst.Binary) isNull.operand()).op());
        SqlAst.IsTruth truth = (SqlAst.IsTruth) where("a > 1 is not true");
        assertEquals(SqlAst.Truth.TRUE, truth.truth());
        assertTrue(truth.negated());
        SqlAst.IsDistinct distinct = (SqlAst.IsDistinct) where("a is distinct from b + 1");
        assertFalse(distinct.negated());
        assertInstanceOf(SqlAst.Binary.class, distinct.right());
        assertTrue(((SqlAst.IsNull) where("a notnull")).negated());
    }

    @Test
    void comparisonsDoNotAssociate() {
        assertThrows(SqlSyntaxException.class, () -> where("a < b < c"));
    }

    @Test
    void inLikeAndCase() {
        SqlAst.InList in = (SqlAst.InList) where("a not in (1, 2)");
        assertTrue(in.negated());
        assertEquals(2, in.values().size());
        SqlAst.Like like = (SqlAst.Like) where("a ilike 'x!%%' escape '!'");
        assertTrue(like.caseInsensitive());
        assertInstanceOf(SqlAst.StringLit.class, like.escape());
        SqlAst.Case simple = (SqlAst.Case) item("case a when 1 then 'one' else 'many' end");
        assertInstanceOf(SqlAst.ColumnRef.class, simple.operand());
        SqlAst.Case searched = (SqlAst.Case) item("case when a > 1 then 2 end");
        assertNull(searched.operand());
        assertNull(searched.otherwise());
    }

    @Test
    void castsInBothSpellings() {
        SqlAst.Cast cast = (SqlAst.Cast) item("cast(a as numeric(10, 2))");
        assertEquals("numeric", cast.type().name());
        assertEquals(List.of(10, 2), cast.type().modifiers());
        assertEquals("double precision", ((SqlAst.Cast) item("a::double precision")).type().name());
        assertEquals("timestamp with time zone", ((SqlAst.Cast) item("a::timestamp with time zone")).type().name());
    }

    // ------------------------------------------------------------------ lexical structure

    @Test
    void constants() {
        assertEquals(new BigDecimal("1.5E+3"), ((SqlAst.NumericLit) item("1.5e3")).value());
        assertEquals(new BigDecimal("0.5"), ((SqlAst.NumericLit) item(".5")).value());
        assertEquals(new BigInteger("99999999999999999999"),
                ((SqlAst.IntegerLit) item("99999999999999999999")).value());
        assertEquals("it's", ((SqlAst.StringLit) item("'it''s'")).value());
        assertEquals("a 'b' $c", ((SqlAst.StringLit) item("$q$a 'b' $c$q$")).value());
        assertEquals("x", ((SqlAst.StringLit) item("$$x$$")).value());
        SqlAst.TypedLit date = (SqlAst.TypedLit) item("DATE '2024-01-31'");
        assertEquals("date", date.type().name());
        assertEquals("2024-01-31", date.value());
        assertEquals("timestamp without time zone",
                ((SqlAst.TypedLit) item("timestamp without time zone '2024-01-31 10:00:00'")).type().name());
        assertEquals(1, ((SqlAst.Param) item("$1")).index());
    }

    @Test
    void postgres16IntegersAndDigitSeparators() {
        assertEquals(BigInteger.valueOf(31), ((SqlAst.IntegerLit) item("0x1F")).value());
        assertEquals(BigInteger.valueOf(15), ((SqlAst.IntegerLit) item("0o17")).value());
        assertEquals(BigInteger.valueOf(5), ((SqlAst.IntegerLit) item("0b101")).value());
        assertEquals(BigInteger.valueOf(31), ((SqlAst.IntegerLit) item("0x_1F")).value());
        assertEquals(BigInteger.valueOf(1_000_000), ((SqlAst.IntegerLit) item("1_000_000")).value());
        assertEquals(new BigDecimal("1000.0001"), ((SqlAst.NumericLit) item("1_000.000_1")).value());
        // Postgres's messages, the whole constant quoted
        assertEquals("invalid hexadecimal integer at or near \"0x\"",
                assertThrows(SqlSyntaxException.class, () -> SqlParser.parse("select 0x")).getMessage());
        for (String junk : List.of("1__0", "1_", "0o19", "123abc", "1e+")) {
            assertEquals("trailing junk after numeric literal at or near \"" + junk + "\"",
                    assertThrows(SqlSyntaxException.class, () -> SqlParser.parse("select " + junk)).getMessage(), junk);
        }
    }

    @Test
    void anyWordButTheAsLabelOnesIsABareAlias() {
        // Postgres 14+: select 1 select names its column "select"; day, filter, over ... need AS
        SqlAst.SelectExpr reserved = (SqlAst.SelectExpr) select("select 1 select").items().get(0);
        assertEquals("select", java.util.Objects.requireNonNull(reserved.alias()).text());
        assertEquals("desc", java.util.Objects.requireNonNull(
                ((SqlAst.SelectExpr) select("select a desc from t").items().get(0)).alias()).text());
        assertEquals("syntax error at or near \"day\"",
                assertThrows(SqlSyntaxException.class, () -> SqlParser.parse("select 1 day")).getMessage());
    }

    @Test
    void anEmptySelectList() {
        assertTrue(select("select from t").items().isEmpty());
        assertTrue(select("select").items().isEmpty());
        // but not after DISTINCT
        assertThrows(SqlSyntaxException.class, () -> SqlParser.parse("select distinct from t"));
    }

    @Test
    void identifiersKeepTheirCaseAndSayWhetherTheyWereQuoted() {
        SqlAst.ColumnRef unquoted = (SqlAst.ColumnRef) item("FirstName");
        assertEquals("FirstName", unquoted.parts().get(0).text());
        assertFalse(unquoted.parts().get(0).quoted());
        SqlAst.ColumnRef quoted = (SqlAst.ColumnRef) item("\"we\"\"ird\"");
        assertEquals("we\"ird", quoted.parts().get(0).text());
        assertTrue(quoted.parts().get(0).quoted());
        SqlAst.ColumnRef qualified = (SqlAst.ColumnRef) item("t.\"select\"");
        assertEquals(List.of("t", "select"), qualified.parts().stream().map(SqlAst.Ident::text).toList());
        // a non-reserved key word is a name
        assertEquals("name", ((SqlAst.ColumnRef) item("name")).parts().get(0).text());
    }

    @Test
    void keyWordsMatchInAnyCase() {
        SqlAst.Select s = select("SeLeCt a FrOm t WhErE a IS nOt NuLl");
        assertInstanceOf(SqlAst.IsNull.class, s.where());
    }

    @Test
    void commentsOfBothFormsNestedOnes() {
        SqlAst.Select s = select("select /* a /* nested */ comment */ a -- trailing\n from t");
        assertEquals(1, s.items().size());
    }

    @Test
    void valueFunctionsWithoutParentheses() {
        SqlAst.FunctionCall f = (SqlAst.FunctionCall) item("current_date");
        assertEquals("current_date", f.name().get(0).text());
        assertTrue(f.args().isEmpty());
    }

    // ------------------------------------------------------------------ refusals

    @Test
    void errorsSayWhere() {
        SqlSyntaxException e = assertThrows(SqlSyntaxException.class, () -> SqlParser.parse("select a\nfrom t where"));
        assertEquals(2, e.line());
        assertEquals(13, e.column());
        assertEquals("syntax error at end of input", e.getMessage());
        assertFalse(e.unsupported());
        SqlSyntaxException near = assertThrows(SqlSyntaxException.class,
                () -> SqlParser.parse("select a from t t2 t3"));
        assertEquals("syntax error at or near \"t3\"", near.getMessage());
        assertEquals(20, near.column());
    }

    @Test
    void lexicalErrors() {
        assertEquals("unterminated string constant",
                assertThrows(SqlSyntaxException.class, () -> SqlParser.parse("select 'abc")).getMessage());
        assertEquals("zero-length delimited identifier",
                assertThrows(SqlSyntaxException.class, () -> SqlParser.parse("select \"\" from t")).getMessage());
        assertTrue(assertThrows(SqlSyntaxException.class, () -> SqlParser.parse("select 123abc")).getMessage()
                .startsWith("trailing junk after numeric literal"));
    }

    @Test
    void onlyOneQueryStatement() {
        assertTrue(assertThrows(SqlSyntaxException.class, () -> SqlParser.parse("create table t (a int)"))
                .getMessage().startsWith("only queries and INSERT, UPDATE and DELETE are supported"));
        assertEquals("only one statement is supported",
                assertThrows(SqlSyntaxException.class, () -> SqlParser.parse("select 1; select 2")).getMessage());
    }

    /** The writes design's leg 4 (2026-10-20): INSERT, UPDATE and DELETE as the statement itself, never inside one
     *  (Postgres's syntax error), and refused by name as a WITH query. */
    @Test
    void statementsThatWriteATable() {
        SqlAst.Insert insert = assertInstanceOf(SqlAst.Insert.class, SqlParser.parse("insert into t as x (a, b)"
                + " values (1, 'p'), (2, null) on conflict (a) do update set b = excluded.b returning *"));
        assertEquals(List.of("a", "b"), insert.columns().stream().map(SqlAst.Ident::text).toList());
        assertTrue(insert.values());
        assertEquals("x", ((SqlAst.TableName) insert.target()).alias().text());
        assertEquals(List.of("a"), insert.onConflict().keys().stream().map(SqlAst.Ident::text).toList());
        assertEquals(1, insert.onConflict().update().size());
        assertEquals(1, insert.returning().size());
        SqlAst.Insert nothing = assertInstanceOf(SqlAst.Insert.class, SqlParser.parse("insert into t select * from u"
                + " on conflict do nothing"));
        assertFalse(nothing.values());
        assertNull(nothing.onConflict().update());
        SqlAst.Update update = assertInstanceOf(SqlAst.Update.class, SqlParser.parse("update t set a = a + 1,"
                + " b = null from u where u.k = t.k"));
        assertNull(((SqlAst.TableName) update.target()).alias());
        assertEquals(2, update.sets().size());
        assertEquals(1, update.from().size());
        SqlAst.Delete delete = assertInstanceOf(SqlAst.Delete.class, SqlParser.parse("delete from t x using u"
                + " where x.k = u.k returning x.k"));
        assertEquals("x", ((SqlAst.TableName) delete.target()).alias().text());
        assertEquals(1, delete.using().size());
        // (as Postgres reads it: INSERT is no reserved word, so the parenthesis's first word is a table's name)
        assertEquals("syntax error at or near \"into\"", assertThrows(SqlSyntaxException.class,
                () -> SqlParser.parse("select * from (insert into t values (1)) s")).getMessage());
        assertTrue(assertThrows(SqlSyntaxException.class, () -> SqlParser.parse("with w as (delete from t returning"
                + " *) select * from w")).unsupported());
        assertTrue(assertThrows(SqlSyntaxException.class, () -> SqlParser.parse("insert into t default values"))
                .unsupported());
    }

    @Test
    void escapeAndUnicodeStringsReadTheirEscapes() {
        // 2026-10-17, the regression suite: E'...' with its backslash escapes, U&'...' with its code points
        assertEquals("a\nb\t'c\\A\u00e9\ud83d\ude00",
                ((SqlAst.StringLit) item("E'a\\nb\\t\\'c\\\\\\101\\u00e9\\U0001F600'")).value());
        assertEquals("d\u0061t\u0061", ((SqlAst.StringLit) item("U&'d\\0061t\\+000061'")).value());
        assertEquals("d!at", ((SqlAst.StringLit) item("U&'d!!a!0074' UESCAPE '!'")).value());
        // and the review's of 2026-10-17: a code point past an int, a pair's low half not hex, a doubled escape
        // character for one, a sign, an escape character of two bytes
        for (String bad : List.of("E'\\x00'", "E'\\uD83D'", "U&'\\00'", "E'\\xE9'", "1 AS U&\"\"",
                "E'\\UFFFFFFFF'", "E'\\uD800\\UFFFFDC00'", "U&'\\D800\\zzzz'", "U&'\\D800\\\\abcd'",
                "U&'\\D800\\+-02400'", "U&'\u00e90041' UESCAPE '\u00e9'")) {
            assertThrows(SqlSyntaxException.class, () -> item(bad), bad);
        }
        assertEquals("A", ((SqlAst.StringLit) item("U&'!0041' UESCAPE E'!'")).value());
        // U&"..." is a quoted identifier of code points, not folded
        SqlAst.SelectExpr named = (SqlAst.SelectExpr) ((SqlAst.Select) SqlParser.parse(
                "select 1 AS U&\"D\\0061t\\+000061\", 2 AS U&\"d*0061t\\+000061\" UESCAPE '*'")).items().get(1);
        assertEquals("dat\\+000061", java.util.Objects.requireNonNull(named.alias()).text());
        assertEquals("Data", java.util.Objects.requireNonNull(((SqlAst.SelectExpr) ((SqlAst.Select) SqlParser.parse(
                "select 1 AS U&\"D\\0061t\\+000061\"")).items().get(0)).alias()).text());
    }

    @Test
    void aValuesListIsAUnionAllOfOneRowSelects() {
        // Postgres's definition (2026-10-17): VALUES (1, 'a'), (2, 'b') is SELECT 1 AS column1, 'a' AS column2 UNION
        // ALL SELECT 2, 'b'
        SqlAst.SetOperation u = (SqlAst.SetOperation) SqlParser.parse("values (1, 'a'), (2, 'b') order by 1");
        assertEquals(SqlAst.SetOp.UNION, u.op());
        assertTrue(u.all());
        assertEquals(1, u.orderBy().size());
        // marked as a VALUES list's, the sort attached too; a written UNION ALL is not
        assertTrue(u.values());
        assertFalse(((SqlAst.SetOperation) SqlParser.parse("select 1 union all select 2")).values());
        SqlAst.Select first = (SqlAst.Select) u.left();
        assertTrue(first.from().isEmpty());
        assertEquals(List.of("column1", "column2"), first.items().stream()
                .map(i -> java.util.Objects.requireNonNull(((SqlAst.SelectExpr) i).alias()).text()).toList());
        assertEquals("VALUES lists must all be the same length",
                assertThrows(SqlSyntaxException.class, () -> SqlParser.parse("values (1, 2), (3)")).getMessage());
        // a row is no select list: a set-returning function in one is Postgres's error, not more rows
        assertEquals("set-returning functions are not allowed in VALUES", assertThrows(SqlSyntaxException.class,
                () -> SqlParser.parse("values (1, generate_series(1, 2))")).getMessage());
        // nor an aggregate or a window function (the review of 2026-10-17); in a subquery they read its rows
        assertEquals("aggregate functions are not allowed in VALUES", assertThrows(SqlSyntaxException.class,
                () -> SqlParser.parse("select * from (values (count(*))) v")).getMessage());
        assertEquals("window functions are not allowed in VALUES", assertThrows(SqlSyntaxException.class,
                () -> SqlParser.parse("values (row_number() over ())")).getMessage());
        SqlParser.parse("values ((select count(*) from t))");
        // a column of NULLs alone is text, as Postgres types one; beside a value a NULL stays untyped
        SqlAst.SetOperation nulls = (SqlAst.SetOperation) SqlParser.parse("values (null, 1), (null, null)");
        List<SqlAst.SelectItem> row = ((SqlAst.Select) nulls.left()).items();
        assertInstanceOf(SqlAst.Cast.class, ((SqlAst.SelectExpr) row.get(0)).expr());
        assertInstanceOf(SqlAst.NullLit.class, ((SqlAst.SelectExpr) ((SqlAst.Select) nulls.right()).items().get(1))
                .expr());
        // a long list is a balanced tree: no recursion per row downstream
        StringBuilder many = new StringBuilder("values (0)");
        for (int i = 1; i < 1024; i++) {
            many.append(", (").append(i).append(')');
        }
        assertEquals(10, depth(SqlParser.parse(many.toString())));
    }

    private static int depth(SqlAst.Statement s) {
        return s instanceof SqlAst.SetOperation o ? 1 + Math.max(depth(o.left()), depth(o.right())) : 0;
    }

    @Test
    void tableNameIsSelectStarFromIt() {
        // 2026-10-17: TABLE t is SELECT * FROM t, the name a table's or the island's source function
        SqlAst.Select s = (SqlAst.Select) SqlParser.parse("table s.t");
        assertInstanceOf(SqlAst.AllColumns.class, s.items().get(0));
        assertEquals(List.of("s", "t"), ((SqlAst.TableName) s.from().get(0)).name().stream()
                .map(SqlAst.Ident::text).toList());
        assertInstanceOf(SqlAst.TableFunction.class,
                ((SqlAst.Select) SqlParser.parse("TABLE tb('pkg::Db.t')")).from().get(0));
        SqlAst.SetOperation u = (SqlAst.SetOperation) SqlParser.parse("select 1 union all table t*");
        assertInstanceOf(SqlAst.TableName.class, ((SqlAst.Select) u.right()).from().get(0));
        assertEquals("syntax error at or near \"(\"", assertThrows(SqlSyntaxException.class,
                () -> SqlParser.parse("table generate_series(1, 2)")).getMessage());
        assertEquals("TABLE ONLY is not supported yet", assertThrows(SqlSyntaxException.class,
                () -> SqlParser.parse("table only t")).getMessage());
    }

    @Test
    void aTableNamesStarIsTheTable() {
        // FROM t* reads t and its descendants, which a table read includes anyway
        SqlAst.Select s = (SqlAst.Select) SqlParser.parse("select * from a* x, b* where x.k = 1");
        assertEquals(2, s.from().size());
        SqlAst.TableName a = (SqlAst.TableName) s.from().get(0);
        assertEquals("x", java.util.Objects.requireNonNull(a.alias()).text());
        assertNull(((SqlAst.TableName) s.from().get(1)).alias());
    }

    @Test
    void notYetSupportedFormsAreRefusedByName() {
        List<String> forms = List.of(
                "select a from unnest(x) with ordinality", "WITH ORDINALITY is not supported yet",
                "select a = any(array[1]) from t", "ANY, SOME and ALL over an array are not supported yet",
                "select B'101'", "bit-string constants (B'...') are not supported yet",
                "select a[1:2] from t", "array slices are not supported yet",
                "select a from t group by rollup (a)", "ROLLUP, CUBE and GROUPING SETS are not supported yet",
                "select make_interval(years := 2)", "named arguments to scalar functions are not supported yet",
                "select concat_ws(',', variadic array[1])", "VARIADIC arguments are not supported yet",
                "select session_user, current_user", "SESSION_USER is not supported yet",
                "select current_schema", "CURRENT_SCHEMA is not supported yet",
                "select id from t AS x tablesample system (50)", "TABLESAMPLE is not supported yet",
                "select id from tb('d.t') x tablesample bernoulli (5)", "TABLESAMPLE is not supported yet");
        for (int i = 0; i < forms.size(); i += 2) {
            String sql = forms.get(i);
            SqlSyntaxException e = assertThrows(SqlSyntaxException.class, () -> SqlParser.parse(sql), sql);
            assertEquals(forms.get(i + 1), e.getMessage(), sql);
            assertTrue(e.unsupported(), sql);
        }
    }
}
