// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol.sql.iq;

import com.legend.protocol.sql.SqlAst;
import com.legend.protocol.sql.SqlSyntaxException;
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
 * IQ's SELECT read into the shared tree ({@link IqParser}): the shapes of M0's first slice, IQ's precedence and its
 * split of values from conditions, the syntax errors IQ makes too, and every form refused by name.
 */
class IqParserTest {

    private static SqlAst.Select select(String sql) {
        return (SqlAst.Select) IqParser.parse(sql, IqProfile.IQ16_DEFAULT.settings());
    }

    private static SqlSyntaxException error(String sql) {
        return assertThrows(SqlSyntaxException.class, () -> IqParser.parse(sql, IqProfile.IQ16_DEFAULT.settings()));
    }

    @Test
    void aSelectHasItsItemsFromWhereAndOrder() {
        SqlAst.Select s = select("SELECT DISTINCT id, [vc] AS \"v\", nm n FROM tb('db::D.t') a WHERE id > 1 ORDER BY 2"
                + " DESC, id");
        assertTrue(s.distinct());
        assertEquals(3, s.items().size());
        SqlAst.SelectExpr v = (SqlAst.SelectExpr) s.items().get(1);
        assertEquals("v", v.alias().text());
        assertTrue(v.alias().quoted());
        assertEquals("[vc]", v.text());
        assertEquals("n", ((SqlAst.SelectExpr) s.items().get(2)).alias().text());
        SqlAst.TableFunction from = (SqlAst.TableFunction) s.from().get(0);
        assertEquals("tb", from.name().get(0).text());
        assertEquals("a", from.alias().text());
        assertInstanceOf(SqlAst.Binary.class, s.where());
        assertTrue(s.orderBy().get(0).descending());
        assertNull(s.orderBy().get(0).nullsFirst());
        assertFalse(s.orderBy().get(1).descending());
    }

    @Test
    void anItemKeepsItsTextAndAStarMayBeQualified() {
        SqlAst.Select s = select("SELECT t.*, *, 'it''s' , - 1 FROM t");
        assertEquals("t", ((SqlAst.AllColumns) s.items().get(0)).qualifier().text());
        assertNull(((SqlAst.AllColumns) s.items().get(1)).qualifier());
        assertEquals("'it''s'", ((SqlAst.SelectExpr) s.items().get(2)).text());
        SqlAst.SelectExpr minus = (SqlAst.SelectExpr) s.items().get(3);
        assertEquals("- 1", minus.text());
        SqlAst.Unary u = (SqlAst.Unary) minus.expr();
        assertEquals(SqlAst.UnaryOp.MINUS, u.op());
        assertEquals(BigInteger.ONE, ((SqlAst.IntegerLit) u.operand()).value());
    }

    @Test
    void numbersAreIntegersOrNumerics() {
        SqlAst.Select s = select("SELECT 12, 1.50, 1e3, 99999999999999999999 FROM t");
        assertEquals(BigInteger.valueOf(12), ((SqlAst.IntegerLit) ((SqlAst.SelectExpr) s.items().get(0)).expr()).value());
        assertEquals(new BigDecimal("1.50"), ((SqlAst.NumericLit) ((SqlAst.SelectExpr) s.items().get(1)).expr()).value());
        assertEquals(new BigDecimal("1000"), ((SqlAst.NumericLit) ((SqlAst.SelectExpr) s.items().get(2)).expr()).value());
        assertEquals(new BigInteger("99999999999999999999"),
                ((SqlAst.IntegerLit) ((SqlAst.SelectExpr) s.items().get(3)).expr()).value());
    }

    @Test
    void precedenceIsIqs_notOverAndOverOr_comparisonsBelowTheValueOperators() {
        SqlAst.Binary or = (SqlAst.Binary) select("SELECT a FROM t WHERE NOT a = 1 AND b = 2 OR c IS NOT NULL").where();
        assertEquals(SqlAst.BinaryOp.OR, or.op());
        SqlAst.Binary and = (SqlAst.Binary) or.left();
        assertEquals(SqlAst.BinaryOp.AND, and.op());
        assertInstanceOf(SqlAst.Unary.class, and.left());
        assertTrue(((SqlAst.IsNull) or.right()).negated());
        SqlAst.Binary cmp = (SqlAst.Binary) select("SELECT a FROM t WHERE a || b = c + d * e").where();
        assertEquals(SqlAst.BinaryOp.EQ, cmp.op());
        assertEquals(SqlAst.BinaryOp.CONCAT, ((SqlAst.Binary) cmp.left()).op());
        SqlAst.Binary plus = (SqlAst.Binary) cmp.right();
        assertEquals(SqlAst.BinaryOp.PLUS, plus.op());
        assertEquals(SqlAst.BinaryOp.TIMES, ((SqlAst.Binary) plus.right()).op());
    }

    @Test
    void iqsComparisonSpellings() {
        assertEquals(SqlAst.BinaryOp.GE, ((SqlAst.Binary) select("SELECT a FROM t WHERE a !< 1").where()).op());
        assertEquals(SqlAst.BinaryOp.LE, ((SqlAst.Binary) select("SELECT a FROM t WHERE a !> 1").where()).op());
        assertEquals(SqlAst.BinaryOp.NE, ((SqlAst.Binary) select("SELECT a FROM t WHERE a != 1").where()).op());
    }

    @Test
    void valuesAndConditionsStayApart_asInIq() {
        // syntax errors in IQ too (SQLCODE -131), measured on SQL Anywhere 17
        for (String sql : new String[] {"SELECT id FROM t WHERE id", "SELECT (id = 1) FROM t",
                "SELECT id FROM t WHERE id = 1 = 1",
                "SELECT id FROM t ORDER BY id NULLS FIRST", "SELECT id AS from FROM t", "SELECT t.from FROM t"}) {
            SqlSyntaxException e = error(sql);
            assertFalse(e.unsupported(), sql + ": " + e.reason());
            assertTrue(e.reason().contains("SQLCODE -131"), e.reason());
        }
        // IQ island, 2026-10-14 (M1c): NOT NOT is IQ's syntax error, refused by name (the research's syntax cases are
        // split on it)
        SqlSyntaxException notNot = error("SELECT id FROM t WHERE NOT NOT id = 1");
        assertTrue(notNot.unsupported() && notNot.reason().contains("SQLCODE -131"), notNot.reason());
        assertInstanceOf(SqlAst.Unary.class, select("SELECT id FROM t WHERE NOT (NOT id = 1)").where());
    }

    @Test
    void formsNotReadYetAreRefusedByName() {
        String[][] refused = {
                // IQ island, 2026-10-14 (M1b): comma joins, JOIN ... ON and UNION are read now (below)
                // IQ island, 2026-10-14 (M1c): GROUP BY, HAVING and COUNT, SUM, AVG, MIN, MAX, MEDIAN, LIST are read
                // now (below); ROLLUP, CUBE, the statistics and the other aggregates are not
                // IQ island, 2026-10-15 (checklist 2.16b): ROLLUP, CUBE and ( ) read now (IqDialect.rolledUp); GROUPING
                // SETS and a key beside ROLLUP are IQ's -1001030
                {"SELECT id FROM t GROUP BY GROUPING SETS ((id))", "GROUPING SETS"},
                {"SELECT id FROM t GROUP BY id, ROLLUP(id)", "-1001030"},
                {"SELECT id FROM t GROUP BY (id, vc)", "composite"},
                {"SELECT stddev(id) FROM t", "loses precision"},
                {"SELECT corr(id, id) FROM t", "aggregate"},
                {"SELECT dba.f(id) FROM t", "owner-qualified"},
                // IQ island, 2026-10-14 (M1a): LIKE, IN lists and the truth tests are read now; these are not
                {"SELECT id FROM t WHERE vc SIMILAR TO 'a%'", "SIMILAR"},
                // IQ island, 2026-10-14 (M1b): IN (subquery) and scalar subqueries are read now (below); KEY JOIN
                // stays refused (IQ island, 2026-10-16, checklist 2.16c: APPLY is read)
                {"SELECT a.id FROM t a KEY JOIN u b", "KEY JOIN"},
                {"SELECT id FROM t WHERE id IS OF (INT)", "is not supported"},
                {"SELECT id & 1 FROM t", "bitwise"},
                // IQ island, 2026-10-14 (M1e): :name and @name are host variables now (below); ? names none
                {"SELECT ? FROM t", "positional parameter"},
                {"SELECT :? FROM t", "positional parameter"},
                {"SELECT :a.b FROM t", "part of a host variable"},
                {"SELECT dba.t.* FROM dba.t", "owner-qualified star"},
                {"SELECT @@version", "global variables"},
                // IQ island, 2026-10-15: WITH reads now (each CTE inlined as a derived table); RECURSIVE does not
                {"SELECT x FROM (WITH RECURSIVE q AS (SELECT 1 AS x) SELECT x FROM q) d", "RECURSIVE"},
                {"SELECT id INTO x FROM t", "INTO"},
                // IQ island, 2026-10-15 (the corpus lane): a trailing separator reads now (below); 2026-10-17, a batch's
                // later statements too (aBatchAnswersItsFirstStatement); a later one that is no SELECT does not
                {"SELECT id FROM t; INSERT INTO t VALUES (1)", "no SELECT"},
                {"SELECT TOP @n id FROM t", "variable"},
                {"SELECT @v = id FROM t", "assignment"},
                {"SELECT id '' FROM t", "empty"},
                // IQ island, 2026-10-14 (M1d): CAST reads now; 2026-10-15: EXTRACT too (IqFunctionsTest)
                {"SELECT 0.000000000000000000000000000000000000001234 FROM t", "38"},
                {"SELECT 1e-50 FROM t", "38"},
                {"SELECT id FROM t ORDER BY +(1)", "position"},
        };
        for (String[] r : refused) {
            SqlSyntaxException e = error(r[0]);
            assertTrue(e.unsupported(), r[0] + ": " + e.reason());
            assertTrue(e.reason().contains(r[1]), r[0] + ": " + e.reason());
        }
    }

    /** IQ island, 2026-10-15 (the corpus lane: 1,691 of SEDE's queries end so; measured on IQ 17.1): a statement's
     *  one trailing separator is read, blanks and a comment after it too. */
    @Test
    void aStatementsTrailingSeparatorIsRead() {
        assertEquals(1, select("SELECT id FROM t ORDER BY id;").orderBy().size());
        assertEquals(1, select("SELECT id FROM t ORDER BY id ;  -- done\n").orderBy().size());
        // IQ island, 2026-10-17 (measured on IQ 17.1): separators in a row are read too
        assertEquals(1, select("SELECT id FROM t ORDER BY id;;").orderBy().size());
    }

    @Test
    void aBatchAnswersItsFirstStatement() {
        // IQ island, 2026-10-17 (measured on IQ 17.1): a batch's later statements, separated by ';' or not, are read
        // for their syntax; the first is the answer
        assertEquals(1, select("SELECT id FROM t ORDER BY id SELECT vc FROM t; SELECT 2").orderBy().size());
        assertEquals(1, select("SELECT id FROM t ORDER BY id; WITH r AS (SELECT id FROM r) SELECT id FROM r").orderBy()
                .size());
        // 2026-10-18 (landing 5's review): what a later statement holds is not the first's -- a recursion, a LATERAL
        IqParser.Parsed later = IqParser.parsed("SELECT id FROM t; WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT"
                + " n + 1 FROM r WHERE n < 3) SELECT n FROM r; SELECT a.id FROM t a, LATERAL (SELECT b.id FROM u b"
                + " WHERE b.t_id = a.id) c", IqProfile.IQ17_DEFAULT.settings());
        assertFalse(later.recursion().with());
        assertTrue(later.recursion().reads().isEmpty());
        assertTrue(later.laterals().isEmpty());
        assertEquals(2, later.later().size());
        // one that does not parse fails the batch, IQ's syntax error
        SqlSyntaxException e = error("SELECT id FROM t; SELECT id FROM t LIMIT 5");
        assertFalse(e.unsupported());
        assertTrue(e.getMessage().contains("SQLCODE -131"), e.getMessage());
    }

    @Test
    void aRecursiveCteNamedInItsOwnReadingIsReadAsItsFirstBranch() {
        // IQ island, 2026-10-17 (measured on IQ 17.1): IQ raises its error for every recursion read (the dialect's
        // recursion); the names bind to the first branch under the CTE's column list
        IqParser.Parsed p = IqParser.parsed("WITH RECURSIVE r(n) AS (SELECT id FROM t UNION ALL SELECT n + 1 FROM r"
                + " WHERE n < 3) SELECT n FROM r", IqProfile.IQ17_DEFAULT.settings());
        assertTrue(p.recursion().with());
        assertEquals(1, p.recursion().reads().size());
        assertTrue(p.recursion().columnless().isEmpty());
        SqlAst.SubqueryFrom r = (SqlAst.SubqueryFrom) ((SqlAst.Select) p.statement()).from().get(0);
        SqlAst.SetOperation body = (SqlAst.SetOperation) r.query();
        SqlAst.SubqueryFrom standIn = (SqlAst.SubqueryFrom) ((SqlAst.Select) body.right()).from().get(0);
        assertEquals("n", standIn.columnAliases().get(0).text());
        assertInstanceOf(SqlAst.Select.class, standIn.query());
        // an unread one is never read; one with no column list is noted
        assertTrue(IqParser.parsed("WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n FROM r) SELECT id FROM t",
                IqProfile.IQ17_DEFAULT.settings()).recursion().reads().isEmpty());
        assertEquals(1, IqParser.parsed("WITH RECURSIVE r AS (SELECT 1 AS n UNION ALL SELECT n FROM r) SELECT n FROM r",
                IqProfile.IQ17_DEFAULT.settings()).recursion().columnless().size());
    }

    // ------------------------------------------------------------------ M1a: predicates

    @Test
    void predicatesAreConditions_eachWithItsOperands() {
        SqlAst.Between b = (SqlAst.Between) select("SELECT id FROM t WHERE id NOT BETWEEN 1 AND 2 + 1").where();
        assertTrue(b.negated());
        assertInstanceOf(SqlAst.Binary.class, b.high());
        // BETWEEN's AND binds first: the condition after it is the outer AND's
        SqlAst.Binary and = (SqlAst.Binary) select("SELECT id FROM t WHERE id BETWEEN 1 AND 3 AND id > 0").where();
        assertEquals(SqlAst.BinaryOp.AND, and.op());
        assertInstanceOf(SqlAst.Between.class, and.left());
        SqlAst.InList in = (SqlAst.InList) select("SELECT id FROM t WHERE id IN (1, NULL, (3))").where();
        assertEquals(3, in.values().size());
        assertFalse(in.negated());
        SqlAst.Like l = (SqlAst.Like) select("SELECT id FROM t WHERE vc NOT LIKE 'a!%' ESCAPE '!'").where();
        assertTrue(l.negated());
        assertEquals("!", ((SqlAst.StringLit) l.escape()).value());
        SqlAst.IsDistinct d = (SqlAst.IsDistinct) select("SELECT id FROM t WHERE id IS NOT DISTINCT FROM 1").where();
        assertTrue(d.negated());
    }

    @Test
    void aTruthTestFollowsAPredicateOrAParenthesizedCondition() {
        SqlAst.IsTruth t = (SqlAst.IsTruth) select("SELECT id FROM t WHERE (id = 1) IS NOT UNKNOWN").where();
        assertEquals(SqlAst.Truth.UNKNOWN, t.truth());
        assertTrue(t.negated());
        // IS binds tighter than NOT and AND, to the predicate before it
        SqlAst.Unary not = (SqlAst.Unary) select("SELECT id FROM t WHERE NOT id > 0 IS TRUE").where();
        assertInstanceOf(SqlAst.IsTruth.class, not.operand());
        SqlAst.Binary and = (SqlAst.Binary) select("SELECT id FROM t WHERE id > 0 AND id > 1 IS TRUE").where();
        assertInstanceOf(SqlAst.Binary.class, and.left());
        assertInstanceOf(SqlAst.IsTruth.class, and.right());
        assertInstanceOf(SqlAst.IsTruth.class, select("SELECT id FROM t WHERE id IS NULL IS FALSE").where());
        assertInstanceOf(SqlAst.IsTruth.class, select("SELECT id FROM t WHERE (id > 0 IS TRUE) IS TRUE").where());
        // TRUE is a name elsewhere
        assertInstanceOf(SqlAst.ColumnRef.class, ((SqlAst.SelectExpr) select("SELECT true FROM t").items().get(0))
                .expr());
    }

    @Test
    void predicatesIqRejects() {
        // syntax errors in IQ too (SQLCODE -131), measured on SQL Anywhere 17
        for (String sql : new String[] {"SELECT id FROM t WHERE id IS TRUE", "SELECT id FROM t WHERE (id > 0) IS NULL",
                "SELECT id FROM t WHERE id > 0 IS TRUE IS TRUE", "SELECT id FROM t WHERE id IN ()",
                "SELECT id FROM t WHERE id IN 1", "SELECT id FROM t WHERE id BETWEEN 1, 3",
                "SELECT id FROM t WHERE id BETWEEN SYMMETRIC 1 AND 3", "SELECT id FROM t WHERE id IS DISTINCT 1",
                "SELECT id BETWEEN 1 AND 2 FROM t", "SELECT vc LIKE 'a' AS x FROM t", "SELECT (1 = 1) IS TRUE",
                "SELECT id FROM t WHERE id IN (1 = 1)", "SELECT id FROM t WHERE id BETWEEN 1 AND 2 = 1"}) {
            SqlSyntaxException e = error(sql);
            assertFalse(e.unsupported(), sql + ": " + e.reason());
            assertTrue(e.reason().contains("SQLCODE -131"), e.reason());
        }
    }

    @Test
    void sqlAnywheresOwnRelationsAreFromItems() {
        SqlAst.TableFunction g = (SqlAst.TableFunction) select("SELECT row_num FROM sa_rowgenerator(1, 5) AS g")
                .from().get(0);
        assertEquals(2, g.args().size());
        assertEquals("g", g.alias().text());
        assertEquals(List.of("SYS", "DUMMY"), select("SELECT * FROM SYS.DUMMY").from().get(0) instanceof
                SqlAst.TableName n ? n.name().stream().map(SqlAst.Ident::text).toList() : List.of());
    }

    // ------------------------------------------------------------------ M0's rest: row limits, aliases, IF, CASE

    @Test
    void topFirstAndStartAtAreTheLimitAndAnOffsetOfOneLess() {
        SqlAst.Select s = select("SELECT TOP 2 START AT 3 id FROM t ORDER BY id");
        assertEquals(BigInteger.TWO, ((SqlAst.IntegerLit) s.limit()).value());
        SqlAst.Binary offset = (SqlAst.Binary) s.offset();
        assertEquals(SqlAst.BinaryOp.MINUS, offset.op());
        assertEquals(BigInteger.valueOf(3), ((SqlAst.IntegerLit) offset.left()).value());
        assertEquals(BigInteger.ONE, ((SqlAst.IntegerLit) offset.right()).value());
        assertEquals(BigInteger.ONE, ((SqlAst.IntegerLit) select("SELECT FIRST id FROM t").limit()).value());
        SqlAst.Select all = select("SELECT DISTINCT TOP ALL START AT 2 id FROM t");
        assertTrue(all.distinct());
        assertNull(all.limit());
        // a parenthesized count is + - * over integers, * binding tighter
        SqlAst.Binary sum = (SqlAst.Binary) select("SELECT TOP (1+2*3) id FROM t").limit();
        assertEquals(SqlAst.BinaryOp.PLUS, sum.op());
        assertEquals(SqlAst.BinaryOp.TIMES, ((SqlAst.Binary) sum.right()).op());
        // TOP 2+1 x is TOP 2 then the item +1 named x (the research's GRAMMAR S5 parse trap)
        SqlAst.Select trap = select("SELECT TOP 2+1 x FROM t");
        assertEquals(BigInteger.TWO, ((SqlAst.IntegerLit) trap.limit()).value());
        assertEquals("x", ((SqlAst.SelectExpr) trap.items().get(0)).alias().text());
        // a literal TOP 0 parses: IQ's error -674 is the dialect's, at translation
        assertEquals(BigInteger.ZERO, ((SqlAst.IntegerLit) select("SELECT TOP 0 id FROM t").limit()).value());
    }

    @Test
    void rowLimitsIqRejects() {
        for (String sql : new String[] {"SELECT TOP 2147483648 id FROM t", "SELECT TOP 1.5 id FROM t",
                "SELECT TOP -1 id FROM t", "SELECT TOP (2 - -1) id FROM t", "SELECT TOP (4/2) id FROM t",
                "SELECT TOP 2 DISTINCT id FROM t", "SELECT TOP 1 TOP 2 id FROM t", "SELECT FIRST START AT 2 id FROM t",
                "SELECT START AT 2 id FROM t", "SELECT TOP 2 WITH TIES id FROM t ORDER BY id"}) {
            SqlSyntaxException e = error(sql);
            assertFalse(e.unsupported(), sql + ": " + e.reason());
            assertTrue(e.reason().contains("SQLCODE -131"), e.reason());
        }
    }

    @Test
    void tsqlAndStringAliasesNameAnItem() {
        SqlAst.Select s = select("SELECT n = id + 1, 'm' = vc, id 'k', vc AS 'a b', \"q\" = 1 FROM t");
        assertEquals(List.of("n", "m", "k", "a b", "q"), s.items().stream()
                .map(i -> ((SqlAst.SelectExpr) i).alias().text()).toList());
        assertInstanceOf(SqlAst.Binary.class, ((SqlAst.SelectExpr) s.items().get(0)).expr());
        // under a TDS login "q" is a string: still an alias
        SqlAst.Select tds = (SqlAst.Select) IqParser.parse("SELECT \"q\" = id, id \"r\" FROM t", IqProfile.IQ16_TDS.settings());
        assertEquals("q", ((SqlAst.SelectExpr) tds.items().get(0)).alias().text());
        assertEquals("r", ((SqlAst.SelectExpr) tds.items().get(1)).alias().text());
        for (String sql : new String[] {"SELECT n = id = 1 FROM t", "SELECT n = id AS m FROM t",
                "SELECT t.n = id FROM t", "SELECT id 'a[b' FROM t", "SELECT id 'a\\x09' FROM t",
                "SELECT id + 1 = 2 FROM t"}) {
            SqlSyntaxException e = error(sql);
            assertFalse(e.unsupported(), sql + ": " + e.reason());
        }
    }

    @Test
    void ifIsTheCaseOfItsConditionAndItsNegation() {
        SqlAst.Case c = (SqlAst.Case) ((SqlAst.SelectExpr) select("SELECT IF a > 0 THEN 1 ELSE 2 ENDIF FROM t")
                .items().get(0)).expr();
        assertEquals(2, c.whens().size());
        assertNull(c.otherwise());
        SqlAst.Unary not = (SqlAst.Unary) c.whens().get(1).condition();
        assertEquals(SqlAst.UnaryOp.NOT, not.op());
        assertTrue(not.operand() == c.whens().get(0).condition());
        assertEquals(1, ((SqlAst.Case) ((SqlAst.SelectExpr) select("SELECT IF a > 0 THEN 1 END IF FROM t").items()
                .get(0)).expr()).whens().size());
        for (String sql : new String[] {"SELECT IF a > 0 THEN 1 END FROM t", "SELECT IF a THEN 1 ENDIF FROM t",
                "SELECT IF a > 0 THEN 1 ELSEIF a > 1 THEN 2 ENDIF FROM t", "SELECT IF a > 0 1 ENDIF FROM t"}) {
            assertFalse(error(sql).unsupported(), sql);
        }
    }

    @Test
    void aSimpleCaseIsTheSearchedCaseOfItsComparisons() {
        SqlAst.Case c = (SqlAst.Case) ((SqlAst.SelectExpr) select("SELECT CASE id WHEN 1 THEN 'a' WHEN 2 THEN 'b'"
                + " ELSE 'c' END CASE FROM t").items().get(0)).expr();
        assertNull(c.operand());
        SqlAst.Binary eq = (SqlAst.Binary) c.whens().get(1).condition();
        assertEquals(SqlAst.BinaryOp.EQ, eq.op());
        assertEquals(BigInteger.TWO, ((SqlAst.IntegerLit) eq.right()).value());
        assertInstanceOf(SqlAst.StringLit.class, c.otherwise());
        assertFalse(error("SELECT CASE WHEN id THEN 1 END FROM t").unsupported());
    }

    @Test
    void callsAndSignsAreValues() {
        SqlAst.FunctionCall f = (SqlAst.FunctionCall) ((SqlAst.SelectExpr) select("SELECT ISNULL(a, -b) FROM t")
                .items().get(0)).expr();
        assertEquals("ISNULL", f.name().get(0).text());
        assertInstanceOf(SqlAst.Unary.class, f.args().get(1));
        SqlAst.Unary twice = (SqlAst.Unary) ((SqlAst.SelectExpr) select("SELECT - -a FROM t").items().get(0)).expr();
        assertInstanceOf(SqlAst.Unary.class, twice.operand());
    }

    @Test
    void orderByASignedIntegerIsAPosition_aParenthesizedOneAConstant() {
        // measured on SQL Anywhere 17: +1 sorts by the first column, (1) by nothing
        assertEquals(BigInteger.ONE, ((SqlAst.IntegerLit) select("SELECT id FROM t ORDER BY +1 DESC").orderBy().get(0)
                .expr()).value());
        SqlAst.Select constant = select("SELECT sm, id FROM t ORDER BY (1) DESC, 2");
        assertEquals(1, constant.orderBy().size());
        assertEquals(BigInteger.TWO, ((SqlAst.IntegerLit) constant.orderBy().get(0).expr()).value());
    }

    @Test
    void anErrorKnowsItsPlace() {
        SqlSyntaxException e = error("SELECT id\nFROM t WHERE NOT NOT id = 1");
        assertEquals(2, e.line());
        assertEquals(18, e.column());
    }

    // ------------------------------------------------------------------ M1b: relations

    private static SqlAst.Statement statement(String sql) {
        return IqParser.parse(sql, IqProfile.IQ16_DEFAULT.settings());
    }

    @Test
    void joinsAssociateLeftAndACommaBindsLoosest() {
        SqlAst.Select s = select("SELECT a.id FROM t a JOIN u b ON a.id = b.t_id LEFT OUTER JOIN v c ON c.x = a.id, w");
        assertEquals(2, s.from().size());
        SqlAst.Join outer = (SqlAst.Join) s.from().get(0);
        assertEquals(SqlAst.JoinType.LEFT, outer.type());
        SqlAst.Join inner = (SqlAst.Join) outer.left();
        assertEquals(SqlAst.JoinType.INNER, inner.type());
        assertInstanceOf(SqlAst.Binary.class, inner.on());
        assertEquals("w", ((SqlAst.TableName) s.from().get(1)).name().get(0).text());
        SqlAst.Join natural = (SqlAst.Join) select("SELECT 1 FROM t NATURAL RIGHT JOIN u").from().get(0);
        assertTrue(natural.natural());
        assertEquals(SqlAst.JoinType.RIGHT, natural.type());
        assertNull(natural.on());
        assertEquals(SqlAst.JoinType.CROSS, ((SqlAst.Join) select("SELECT 1 FROM t CROSS JOIN u").from().get(0)).type());
        // a join without ON reads (IQ's KEY JOIN): the dialect refuses it by name
        assertNull(((SqlAst.Join) select("SELECT 1 FROM t JOIN u").from().get(0)).on());
        // parenthesized joins and tables, a comma inside the parentheses a cross join
        SqlAst.Join p = (SqlAst.Join) select("SELECT 1 FROM t JOIN (u JOIN v ON 1 = 1) ON 1 = 1").from().get(0);
        assertInstanceOf(SqlAst.Join.class, p.right());
        assertInstanceOf(SqlAst.TableName.class, select("SELECT 1 FROM ((t))").from().get(0));
        assertEquals(SqlAst.JoinType.CROSS, ((SqlAst.Join) select("SELECT 1 FROM (t, u)").from().get(0)).type());
    }

    @Test
    void joinSyntaxErrorsAreIqs() {
        for (String sql : new String[] {"SELECT 1 FROM t JOIN u USING (id)", "SELECT 1 FROM t JOIN u JOIN v ON 1 = 1 ON"
                + " 2 = 2", "SELECT 1 FROM t CROSS JOIN u ON 1 = 1", "SELECT 1 FROM t OUTER JOIN u ON 1 = 1",
                "SELECT 1 FROM t LEFT INNER JOIN u ON 1 = 1", "SELECT 1 FROM t NATURAL CROSS JOIN u",
                "SELECT 1 FROM (t JOIN u ON 1 = 1) AS x", "SELECT 1 FROM (SELECT 1 a)",
                "SELECT 1 UNION ALL DISTINCT SELECT 2", "SELECT 1 ORDER BY 1 UNION SELECT 2",
                "SELECT 1 UNION (WITH x AS (SELECT 1 a) SELECT a FROM x)", "SELECT EXISTS (SELECT 1)",
                "SELECT 1 FROM t WHERE 1 = ANY (1, 2)"}) {
            assertFalse(error(sql).unsupported(), sql);
        }
    }

    @Test
    void aDerivedTableNeedsItsNameAndMayNameItsColumns() {
        SqlAst.SubqueryFrom d = (SqlAst.SubqueryFrom) select("SELECT x FROM (SELECT 1 a, 2 b) AS d (x, y)").from()
                .get(0);
        assertEquals("d", d.alias().text());
        assertEquals(List.of("x", "y"), d.columnAliases().stream().map(SqlAst.Ident::text).toList());
        assertInstanceOf(SqlAst.Select.class, d.query());
        // a derived table in parentheses of its own, and one of a set operation of parenthesized queries
        assertInstanceOf(SqlAst.SubqueryFrom.class, select("SELECT 1 FROM ((SELECT 1 a) d)").from().get(0));
        SqlAst.SubqueryFrom u = (SqlAst.SubqueryFrom) select("SELECT 1 FROM ((SELECT 1 a) UNION (SELECT 2)) d")
                .from().get(0);
        assertInstanceOf(SqlAst.SetOperation.class, u.query());
    }

    @Test
    void setOperationsBindAsIqsDoAndTakeTheOrderBy() {
        SqlAst.SetOperation u = (SqlAst.SetOperation) statement("SELECT 1 UNION SELECT 2 INTERSECT SELECT 3 EXCEPT ALL"
                + " SELECT 4 ORDER BY 1 DESC");
        // INTERSECT binds tighter; UNION and EXCEPT associate left; the ORDER BY is the whole query's
        assertEquals(SqlAst.SetOp.EXCEPT, u.op());
        assertTrue(u.all());
        assertEquals(1, u.orderBy().size());
        SqlAst.SetOperation union = (SqlAst.SetOperation) u.left();
        assertEquals(SqlAst.SetOp.UNION, union.op());
        assertFalse(union.all());
        assertEquals(SqlAst.SetOp.INTERSECT, ((SqlAst.SetOperation) union.right()).op());
        assertFalse(((SqlAst.SetOperation) statement("SELECT 1 UNION DISTINCT SELECT 2")).all());
        // after a set operation, (1) is an expression (IQ's error -152, the dialect's to raise), +1 a position
        SqlAst.SetOperation p = (SqlAst.SetOperation) statement("SELECT 1 UNION SELECT 2 ORDER BY (1), +1");
        assertInstanceOf(SqlAst.Unary.class, p.orderBy().get(0).expr());
        assertInstanceOf(SqlAst.IntegerLit.class, p.orderBy().get(1).expr());
        // parenthesized branches, and a whole parenthesized query
        assertInstanceOf(SqlAst.SetOperation.class, statement("(SELECT 1) UNION ((SELECT 2))"));
        assertEquals(1, ((SqlAst.Select) statement("(SELECT a FROM t ORDER BY a)")).orderBy().size());
        assertTrue(error("(SELECT a FROM t ORDER BY a) UNION SELECT 1").unsupported());
    }

    @Test
    void subqueriesAreValuesOrPredicates() {
        SqlAst.SelectExpr item = (SqlAst.SelectExpr) select("SELECT (SELECT 1), ((SELECT 1) + 1) FROM t").items()
                .get(0);
        assertInstanceOf(SqlAst.ScalarSubquery.class, item.expr());
        SqlAst.Binary sum = (SqlAst.Binary) ((SqlAst.SelectExpr) select("SELECT ((SELECT 1) + 1) FROM t").items()
                .get(0)).expr();
        assertInstanceOf(SqlAst.ScalarSubquery.class, sum.left());
        assertInstanceOf(SqlAst.Exists.class, select("SELECT 1 FROM t WHERE NOT EXISTS (SELECT 1)").where() instanceof
                SqlAst.Unary n ? n.operand() : null);
        SqlAst.InSubquery in = (SqlAst.InSubquery) select("SELECT 1 FROM t WHERE a NOT IN (SELECT b FROM u)").where();
        assertTrue(in.negated());
        assertInstanceOf(SqlAst.SetOperation.class, ((SqlAst.InSubquery) select("SELECT 1 FROM t WHERE a IN"
                + " ((SELECT 1) UNION (SELECT 2))").where()).query());
        assertEquals(2, ((SqlAst.InList) select("SELECT 1 FROM t WHERE a IN ((SELECT 1), 2)").where()).values().size());
        SqlAst.Quantified q = (SqlAst.Quantified) select("SELECT 1 FROM t WHERE a >= SOME (SELECT b FROM u)").where();
        assertFalse(q.all());
        assertEquals(SqlAst.BinaryOp.GE, q.op());
        assertTrue(((SqlAst.Quantified) select("SELECT 1 FROM t WHERE a <> ALL (SELECT b FROM u)").where()).all());
    }

    @Test
    void groupByAndHavingAndTheAggregatesForms() {
        // IQ island, 2026-10-14 (M1c)
        SqlAst.Select s = select("SELECT k, COUNT(*) FROM t GROUP BY k, k + 1, 1 HAVING COUNT(DISTINCT v) > 1");
        assertEquals(3, s.groupBy().size());
        assertInstanceOf(SqlAst.IntegerLit.class, s.groupBy().get(2));
        SqlAst.FunctionCall count = (SqlAst.FunctionCall) ((SqlAst.SelectExpr) s.items().get(1)).expr();
        assertTrue(count.star());
        SqlAst.FunctionCall distinct = (SqlAst.FunctionCall) ((SqlAst.Binary) s.having()).left();
        assertTrue(distinct.distinct());
        assertTrue(((SqlAst.FunctionCall) ((SqlAst.SelectExpr) select("SELECT COUNT() FROM t").items().get(0)).expr())
                .star());
        SqlAst.FunctionCall list = (SqlAst.FunctionCall) ((SqlAst.SelectExpr) select("SELECT LIST(id, '-' ORDER BY"
                + " id DESC, k) FROM t").items().get(0)).expr();
        assertEquals(2, list.args().size());
        assertEquals(2, list.orderBy().size());
        assertTrue(list.orderBy().get(0).descending());
        // the texts kept for IQ's names of a grouped query's items
        IqParser.Parsed p = IqParser.parsed("SELECT SUM( v ) FROM t GROUP BY (k) + 1", IqProfile.IQ16_DEFAULT.settings());
        SqlAst.Select q = (SqlAst.Select) p.statement();
        assertEquals("(k) + 1", p.spans().get(q.groupBy().get(0)));
        assertEquals("SUM( v )", p.spans().get(((SqlAst.SelectExpr) q.items().get(0)).expr()));
        // IQ's syntax errors (SQLCODE -131)
        for (String sql : new String[] {"SELECT k FROM t GROUP BY ALL", "SELECT k FROM t GROUP BY",
                "SELECT k FROM t HAVING COUNT(*) > 1 GROUP BY k", "SELECT COUNT(DISTINCT *) FROM t",
                "SELECT LIST(id ORDER BY id NULLS LAST) FROM t", "SELECT COUNT(k > 1) FROM t"}) {
            SqlSyntaxException e = error(sql);
            assertFalse(e.unsupported(), sql + ": " + e.reason());
            assertTrue(e.reason().contains("SQLCODE -131"), e.reason());
        }
    }

    @Test
    void anItemIsReadAgainAsWritten() {
        IqParser.Written w = IqParser.written("((a + 1)) * CASE b WHEN 1 THEN 2 END + IF c = 1 THEN 1 ENDIF",
                IqProfile.IQ16_DEFAULT.settings());
        SqlAst.Binary plus = (SqlAst.Binary) w.expr();
        SqlAst.Binary times = (SqlAst.Binary) plus.left();
        assertEquals(2, w.parensOf(times.left()));
        assertTrue(w.simpleCases().contains(times.right()));
        assertTrue(w.ifs().contains(plus.right()));
        assertFalse(w.commented());
        assertTrue(IqParser.written("a /* x */ + 1", IqProfile.IQ16_DEFAULT.settings()).commented());
    }

    // ------------------------------------------------------------------ M1e: names and host variables

    /** IQ island, 2026-10-14 (M1e): a table's server, database and owner qualifiers, T-SQL's db..t (its owner omitted:
     *  an empty part), and IQ's syntax errors around them (measured on SQL Anywhere 17). */
    @Test
    void ownerQualifiedTableNames() {
        SqlAst.TableName four = (SqlAst.TableName) select("SELECT id FROM srv.db.DBA.t").from().get(0);
        assertEquals(List.of("srv", "db", "DBA", "t"), four.name().stream().map(SqlAst.Ident::text).toList());
        SqlAst.TableName omitted = (SqlAst.TableName) select("SELECT id FROM db..t x").from().get(0);
        assertEquals(List.of("db", "", "t"), omitted.name().stream().map(SqlAst.Ident::text).toList());
        assertEquals("x", java.util.Objects.requireNonNull(omitted.alias()).text());
        SqlAst.ColumnRef ref = (SqlAst.ColumnRef) ((SqlAst.SelectExpr) select("SELECT db..DUMMY.dummy_col FROM db..DUMMY")
                .items().get(0)).expr();
        assertEquals(4, ref.parts().size());
        for (String sql : new String[] {"SELECT id FROM a.b.c.d.e", "SELECT id FROM srv.db..t", "SELECT id FROM srv..o.t",
                "SELECT id FROM db..t.x"}) {
            SqlSyntaxException e = error(sql);
            assertFalse(e.unsupported(), sql + ": " + e.reason());
            assertTrue(e.reason().contains("SQLCODE -131"), e.reason());
        }
    }

    /** IQ island, 2026-10-14 (M1e): {@code @name} and {@code :name} are named parameters, one index per name as IQ
     *  matches names; {@code @@name} is a global, refused; IQ's syntax errors (measured: a space after the colon,
     *  {@code ::a}, {@code :1}, an indicator variable). */
    @Test
    void hostVariablesAreNamedParameters() {
        SqlAst.Select s = select("SELECT @x AS a, :y AS b, @X AS c FROM t WHERE id > :y");
        SqlAst.Param x = (SqlAst.Param) ((SqlAst.SelectExpr) s.items().get(0)).expr();
        SqlAst.Param y = (SqlAst.Param) ((SqlAst.SelectExpr) s.items().get(1)).expr();
        SqlAst.Param bigX = (SqlAst.Param) ((SqlAst.SelectExpr) s.items().get(2)).expr();
        assertEquals("@x", x.name());
        assertEquals(":y", y.name());
        assertEquals(x.index(), bigX.index());
        assertEquals(y.index(), ((SqlAst.Param) ((SqlAst.Binary) java.util.Objects.requireNonNull(s.where())).right())
                .index());
        for (String sql : new String[] {"SELECT : a", "SELECT ::a", "SELECT :1", "SELECT :a:b"}) {
            SqlSyntaxException e = error(sql);
            assertFalse(e.unsupported(), sql + ": " + e.reason());
            assertTrue(e.reason().contains("SQLCODE -131"), e.reason());
        }
    }

    @org.junit.jupiter.api.Test
    void aCommonTableExpressionIsReadAsTheDerivedTableItNames() {
        // IQ island, 2026-10-15: each reference a derived table of its own, read again from the CTE's tokens
        SqlAst.Statement s = IqParser.parse("WITH q (a) AS (SELECT id FROM t), r AS (SELECT a FROM q)"
                + " SELECT x.a, r.a FROM q x, r", IqProfile.IQ16_DEFAULT.settings());
        SqlAst.Select top = (SqlAst.Select) s;
        SqlAst.SubqueryFrom x = (SqlAst.SubqueryFrom) top.from().get(0);
        SqlAst.SubqueryFrom r = (SqlAst.SubqueryFrom) top.from().get(1);
        org.junit.jupiter.api.Assertions.assertEquals("x", java.util.Objects.requireNonNull(x.alias()).text());
        org.junit.jupiter.api.Assertions.assertEquals(List.of("a"), x.columnAliases().stream().map(SqlAst.Ident::text)
                .toList());
        org.junit.jupiter.api.Assertions.assertEquals("r", java.util.Objects.requireNonNull(r.alias()).text());
        // r's query reads q: inlined there too, a tree of its own
        SqlAst.SubqueryFrom inner = (SqlAst.SubqueryFrom) ((SqlAst.Select) r.query()).from().get(0);
        org.junit.jupiter.api.Assertions.assertNotSame(x.query(), inner.query());
    }

    @Test
    void commonTableExpressionsAreScopedAsIq17Scopes() {
        // IQ island, 2026-10-15, each measured on IQ 17.1: a CTE names one written after it
        SqlAst.SubqueryFrom a = (SqlAst.SubqueryFrom) select("WITH a AS (SELECT n FROM b), b (n) AS (SELECT 1)"
                + " SELECT n FROM a").from().get(0);
        assertInstanceOf(SqlAst.SubqueryFrom.class, ((SqlAst.Select) a.query()).from().get(0));
        // of two with one name, in any case, the first is read
        SqlAst.SubqueryFrom q = (SqlAst.SubqueryFrom) select("WITH q AS (SELECT 1 AS a), Q AS (SELECT 2 AS a)"
                + " SELECT a FROM q").from().get(0);
        assertEquals(BigInteger.ONE, ((SqlAst.IntegerLit) ((SqlAst.SelectExpr) ((SqlAst.Select) q.query()).items()
                .get(0)).expr()).value());
        // one named inside its own reading: -938 itself (a subquery's reference too), -914 a cycle; only where read
        assertTrue(error("WITH r (n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 3) SELECT n FROM r")
                .getMessage().contains("-938"));
        assertTrue(error("WITH r (n) AS (SELECT 1 FROM t WHERE id IN (SELECT n FROM r)) SELECT n FROM r")
                .getMessage().contains("-938"));
        assertTrue(error("WITH a AS (SELECT n FROM b), b (n) AS (SELECT n FROM a) SELECT n FROM a").getMessage()
                .contains("-914"));
        select("WITH r AS (SELECT 1 AS a FROM r) SELECT 1 AS x");
        // a derived table's WITH: read there, and blind to the CTEs outside it (a table of that name)
        SqlAst.SubqueryFrom d = (SqlAst.SubqueryFrom) select("WITH q AS (SELECT 5 AS a) SELECT a FROM (WITH r AS"
                + " (SELECT 1 AS b) SELECT a FROM q) d").from().get(0);
        assertInstanceOf(SqlAst.TableName.class, ((SqlAst.Select) d.query()).from().get(0));
        // a plain derived table and a subquery see them
        SqlAst.SubqueryFrom plain = (SqlAst.SubqueryFrom) select("WITH q AS (SELECT 5 AS a) SELECT a FROM (SELECT a"
                + " FROM q) d").from().get(0);
        assertInstanceOf(SqlAst.SubqueryFrom.class, ((SqlAst.Select) plain.query()).from().get(0));
        // WITH in a subquery, a CTE's query or a set operation's branch is IQ's syntax error
        assertTrue(error("SELECT id FROM t WHERE id IN (WITH q AS (SELECT 1 AS a) SELECT a FROM q)").getMessage()
                .contains("-131"));
        assertTrue(error("WITH p AS (WITH q AS (SELECT 1 AS a) SELECT a FROM q) SELECT a FROM p").getMessage()
                .contains("-131"));
        assertTrue(error("SELECT 1 AS a UNION ALL (WITH q AS (SELECT 2 AS a) SELECT a FROM q)").getMessage()
                .contains("-131"));
    }

    @Test
    void commonTableExpressionsReadAgainAreBounded() {
        // IQ island, 2026-10-15: a chain whose each link names the one before twice doubles with each link
        StringBuilder sql = new StringBuilder("WITH c0 AS (SELECT id FROM t)");
        for (int i = 1; i <= 20; i++) {
            sql.append(", c").append(i).append(" AS (SELECT x.id FROM c").append(i - 1).append(" x, c").append(i - 1)
                    .append(" y)");
        }
        sql.append(" SELECT id FROM c20");
        assertTrue(error(sql.toString()).getMessage().contains("name each fewer times"));
        // a chain of single links reads each once: 50 links, linear (a doubling read would never end); 200 overflowed
        // the Windows CI's stack, where the test JVM runs on C1, whose frames are larger (2026-10-16, PR #43's run)
        StringBuilder chain = new StringBuilder("WITH c0 AS (SELECT id FROM t)");
        for (int i = 1; i <= 50; i++) {
            chain.append(", c").append(i).append(" AS (SELECT id FROM c").append(i - 1).append(")");
        }
        select(chain.append(" SELECT id FROM c50").toString());
    }
}
