// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol.cypher;

import com.legend.protocol.cypher.CypherAst.Binary;
import com.legend.protocol.cypher.CypherAst.BinaryOp;
import com.legend.protocol.cypher.CypherAst.Clause;
import com.legend.protocol.cypher.CypherAst.Direction;
import com.legend.protocol.cypher.CypherAst.Expr;
import com.legend.protocol.cypher.CypherAst.Match;
import com.legend.protocol.cypher.CypherAst.NodePattern;
import com.legend.protocol.cypher.CypherAst.OtherClause;
import com.legend.protocol.cypher.CypherAst.RelPattern;
import com.legend.protocol.cypher.CypherAst.Return;
import com.legend.protocol.cypher.CypherAst.Statement;
import com.legend.protocol.cypher.CypherAst.Unary;
import com.legend.protocol.cypher.CypherAst.UnaryOp;
import com.legend.protocol.sql.SqlTranslationException;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The Cypher reader ({@link CypherLexer}, {@link CypherParser}) on its own (docs/CYPHER_STUDY_2026_10_15.md §5.1):
 * the shapes it reads, Neo4j's precedence, the clauses it keeps for the translator to refuse by name, and the
 * syntax errors it reports with their positions.
 */
class CypherParserTest {

    private static Statement parse(String text) {
        return CypherParser.parse(text);
    }

    @Test
    void aPatternInsideAnExpressionIsAPredicate() {
        assertEquals("(pattern:3)", tree(expr("(n)-[:T]->()")));
        assertEquals("NOT((pattern:3))", tree(expr("NOT (n)<-[]-(:B)")));
        assertEquals("AND((pattern:3),EQ(n.x,1))", tree(expr("(n)-->() AND n.x = 1")));
        assertEquals("(n)", tree(expr("(n)")));
        assertEquals("EXISTS{pattern:3 WHERE EQ(b.x,1)}", tree(expr("EXISTS { MATCH (n)-[:T]->(b) WHERE b.x = 1 RETURN b }")));
        assertEquals("EXISTS{pattern:3}", tree(expr("exists { (n)-->() }")));
        assertEquals("EXISTS{pattern:3}", tree(expr("EXISTS { MATCH (n)-->(m) RETURN * }")));
        assertEquals("EXISTS{pattern:3}", tree(expr("EXISTS { MATCH (n)-->(m) RETURN DISTINCT m AS x, n }")));
        // (the review's tenth finding: a labelled or empty start node, a map start, and arithmetic over
        // parenthesized values, which is no relationship)
        assertEquals("(pattern:3)", tree(expr("(:A)-[:K]->(a)")));
        assertEquals("(pattern:3)", tree(expr("()-->(a)")));
        assertEquals("(pattern:3)", tree(expr("({k: 1})-->(a)")));
        assertEquals("MINUS((a),(b))", tree(expr("(a) - (b)")));
        assertEquals("MINUS(({k:1}),(b))", tree(expr("({k: 1}) - (b)")));
        assertEquals(SqlTranslationException.Kind.UNSUPPORTED, refusal("MATCH (n) WHERE EXISTS { OPTIONAL MATCH (n)-->() } RETURN n").kind());
    }

    private static Expr expr(String text) {
        Return r = (Return) parse("RETURN " + text).clauses().get(0);
        return r.items().get(0).expr();
    }

    /** An expression as a bracketed string, for precedence checks. */
    private static String tree(Expr e) {
        return switch (e) {
            case CypherAst.IntLit n -> n.text();
            case CypherAst.FloatLit f -> f.text();
            case CypherAst.StringLit s -> "'" + s.value() + "'";
            case CypherAst.BoolLit b -> String.valueOf(b.value());
            case CypherAst.NullLit n -> "null";
            case CypherAst.Var v -> v.name();
            case CypherAst.Prop p -> tree(p.target()) + "." + p.name();
            case CypherAst.Param p -> "$" + p.name();
            case CypherAst.ListLit l -> "[" + String.join(",", l.elements().stream().map(CypherParserTest::tree)
                    .toList()) + "]";
            case CypherAst.MapLit m -> "{" + String.join(",", m.entries().entrySet().stream()
                    .map(x -> x.getKey() + ":" + tree(x.getValue())).toList()) + "}";
            case Unary u -> u.op() + "(" + tree(u.operand()) + ")";
            case Binary b -> b.op() + "(" + tree(b.left()) + "," + tree(b.right()) + ")";
            case CypherAst.Call c -> c.name() + (c.distinct() ? "<distinct>" : "") + "(" + String.join(",",
                    c.args().stream().map(CypherParserTest::tree).toList()) + ")";
            case CypherAst.CountStar c -> "count(*)";
            case CypherAst.Case c -> "CASE(" + (c.operand() == null ? "" : tree(c.operand()) + ";") + String.join(";",
                    c.whens().stream().map(w -> tree(w.condition()) + "->" + tree(w.result())).toList())
                    + (c.elseResult() == null ? "" : ";else " + tree(c.elseResult())) + ")";
            case CypherAst.Paren p -> "(" + tree(p.inner()) + ")";
            case CypherAst.Quantifier q -> q.name() + "(" + q.variable() + " IN " + tree(q.list()) + " WHERE "
                    + tree(q.where()) + ")";
            case CypherAst.ListComprehension lc -> "[" + lc.variable() + " IN " + tree(lc.list())
                    + (lc.where() == null ? "" : " WHERE " + tree(lc.where()))
                    + (lc.map() == null ? "" : " | " + tree(lc.map())) + "]";
            case CypherAst.LabelPredicate lp -> tree(lp.target()) + ":" + String.join(":", lp.labels());
            case CypherAst.PatternComprehension pc -> "[pattern" + (pc.pattern().variable() == null ? ""
                    : " " + pc.pattern().variable()) + ":" + pc.pattern().elements().size()
                    + (pc.where() == null ? "" : " WHERE " + tree(pc.where())) + " | " + tree(pc.map()) + "]";
            case CypherAst.PatternPredicate pp -> "(pattern:" + pp.pattern().elements().size() + ")";
            case CypherAst.ExistsSubquery ex -> "EXISTS{pattern:" + ex.pattern().elements().size()
                    + (ex.where() == null ? "" : " WHERE " + tree(ex.where())) + "}";
            case CypherAst.Index ix -> tree(ix.target()) + "[" + tree(ix.index()) + "]";
            case CypherAst.Slice sl -> tree(sl.target()) + "[" + (sl.from() == null ? "" : tree(sl.from())) + ".."
                    + (sl.to() == null ? "" : tree(sl.to())) + "]";
        };
    }

    private static SqlTranslationException refusal(String text) {
        return assertThrows(SqlTranslationException.class, () -> parse(text));
    }

    // ------------------------------------------------------------------ clauses

    @Test
    void aMatchReturnQueryIsItsTwoClauses() {
        Statement s = parse("MATCH (n:Person) RETURN n.name");
        assertNull(s.version());
        assertEquals(2, s.clauses().size());
        Match m = (Match) s.clauses().get(0);
        NodePattern n = (NodePattern) m.patterns().get(0).elements().get(0);
        assertEquals("n", n.variable());
        assertEquals(List.of("Person"), n.labels());
        Return r = (Return) s.clauses().get(1);
        assertEquals("n.name", r.items().get(0).text());
        assertNull(r.items().get(0).alias());
        assertEquals("PROP", "n.name".equals(tree(r.items().get(0).expr())) ? "PROP" : tree(r.items().get(0).expr()));
    }

    @Test
    void keywordsAreCaseInsensitiveAndNamesAreNot() {
        Statement s = parse("match (Person:person) where Person.Name = 'x' return Person.Name as NAME");
        Match m = (Match) s.clauses().get(0);
        NodePattern n = (NodePattern) m.patterns().get(0).elements().get(0);
        assertEquals("Person", n.variable());
        assertEquals(List.of("person"), n.labels());
        Return r = (Return) s.clauses().get(1);
        assertEquals("NAME", r.items().get(0).alias());
    }

    @Test
    void theVersionPrefixIsReadAndOtherVersionsAreNeo4jsError() {
        assertEquals(5, parse("CYPHER 5 MATCH (n) RETURN n").version());
        assertEquals(25, parse("CYPHER 25 MATCH (n) RETURN n").version());
        assertNull(parse("CYPHER MATCH (n) RETURN n").version());
        SqlTranslationException e = refusal("CYPHER 7 MATCH (n) RETURN n");
        assertEquals(SqlTranslationException.Kind.INVALID, e.kind());
        assertEquals("Cypher version 7 is not supported: 5 or 25", e.getMessage());
        SqlTranslationException option = refusal("CYPHER runtime=slotted MATCH (n) RETURN n");
        assertEquals(SqlTranslationException.Kind.UNSUPPORTED, option.kind());
    }

    @Test
    void withCarriesItsProjectionOrderingSliceAndFilter() {
        Statement s = parse("MATCH (n:A) WITH DISTINCT n.x AS x, n.y ORDER BY x DESC, n.y SKIP 1 LIMIT 2 WHERE x > 1"
                + " RETURN x");
        CypherAst.With w = (CypherAst.With) s.clauses().get(1);
        assertTrue(w.distinct());
        assertEquals(2, w.items().size());
        assertEquals("x", w.items().get(0).alias());
        assertEquals("n.y", w.items().get(1).text());
        assertEquals(2, w.orderBy().size());
        assertTrue(w.orderBy().get(0).descending());
        assertEquals("1", tree(w.skip()));
        assertEquals("2", tree(w.limit()));
        assertEquals("GT(x,1)", tree(w.where()));
    }

    @Test
    void offsetIsCypher25sSpellingOfSkip() {
        Return r = (Return) parse("MATCH (n) RETURN n.x OFFSET 3 LIMIT 4").clauses().get(1);
        assertEquals("3", tree(r.skip()));
        assertEquals("4", tree(r.limit()));
    }

    @Test
    void aClauseTheTranslatorDoesNotReadIsKeptWithItsTextForARefusalByName() {
        Statement s = parse("UNWIND [1, 2] AS x RETURN x");
        CypherAst.Unwind u = (CypherAst.Unwind) s.clauses().get(0);
        assertEquals("x", u.variable());
        assertEquals("[1,2]", tree(u.list()));
        assertInstanceOf(Return.class, s.clauses().get(1));
        // (n:Foo) alone is a label predicate, in parentheses and inside a list literal (a pattern comprehension
        // starts only where a property map or a relationship follows the labels)
        assertTrue(expr("[(n:Foo)]") instanceof CypherAst.ListLit l && l.elements().size() == 1
                && l.elements().get(0) instanceof CypherAst.Paren p && p.inner() instanceof CypherAst.LabelPredicate);
        assertTrue(expr("[(n:Foo)-[:T]->(m) | m]") instanceof CypherAst.PatternComprehension);
        // CALL is read since the procedures leg (the study's §12.60): the dotted name, the arguments when written
        // in parentheses (null for the implicit form), the yielded outputs under their aliases, YIELD *
        CypherAst.ProcedureCall call = (CypherAst.ProcedureCall) parse("CALL db.labels() YIELD label RETURN label")
                .clauses().get(0);
        assertEquals("db.labels", call.name());
        assertEquals(List.of(), call.args());
        assertEquals(List.of("label"), call.yields().stream().map(CypherAst.YieldItem::name).toList());
        CypherAst.ProcedureCall renamed = (CypherAst.ProcedureCall) parse("CALL test.my.proc('a', 1) YIELD city AS c,"
                + " code RETURN c").clauses().get(0);
        assertEquals("test.my.proc", renamed.name());
        assertEquals(2, renamed.args().size());
        assertEquals("c", renamed.yields().get(0).alias());
        assertEquals(null, renamed.yields().get(1).alias());
        CypherAst.ProcedureCall implicit = (CypherAst.ProcedureCall) parse("CALL test.my.proc").clauses().get(0);
        assertEquals(null, implicit.args());
        assertEquals(null, implicit.yields());
        assertTrue(((CypherAst.ProcedureCall) parse("CALL test.my.proc() YIELD *").clauses().get(0)).yieldAll());
        OtherClause foreach = (OtherClause) parse("MATCH (n) FOREACH (x IN [1] | SET n.p = x)").clauses().get(1);
        assertEquals("FOREACH", foreach.keyword());
    }

    @Test
    void theClausesThatUpdateTheGraphAreRead() {
        CypherAst.Create create = (CypherAst.Create) parse("CREATE (n:Person {name: 'a'})-[:KNOWS]->(m), (x)")
                .clauses().get(0);
        assertEquals(2, create.patterns().size());
        assertEquals(3, create.patterns().get(0).elements().size());
        CypherAst.SetClause set = (CypherAst.SetClause) parse("MATCH (n) SET n.x = n.x + 1, (n).y = 2, n = {a: 1},"
                + " n += {b: 2}, n:A:B RETURN n").clauses().get(1);
        assertEquals(5, set.items().size());
        assertEquals("PLUS(n.x,1)", tree(((CypherAst.SetProperty) set.items().get(0)).value()));
        assertEquals("y", ((CypherAst.SetProperty) set.items().get(1)).key());
        assertTrue(set.items().get(2) instanceof CypherAst.SetProperties p && !p.merge());
        assertTrue(set.items().get(3) instanceof CypherAst.SetProperties p && p.merge());
        assertEquals(List.of("A", "B"), ((CypherAst.SetLabels) set.items().get(4)).labels());
        CypherAst.Remove remove = (CypherAst.Remove) parse("MATCH (n) REMOVE n.x, n:A").clauses().get(1);
        assertInstanceOf(CypherAst.RemoveProperty.class, remove.items().get(0));
        assertInstanceOf(CypherAst.RemoveLabels.class, remove.items().get(1));
        CypherAst.Delete detach = (CypherAst.Delete) parse("MATCH (n)-[r]->() DETACH DELETE n, r").clauses().get(1);
        assertTrue(detach.detach());
        assertEquals(2, detach.targets().size());
        assertTrue(!((CypherAst.Delete) parse("MATCH (n) DELETE n").clauses().get(1)).detach());
        assertTrue(!((CypherAst.Delete) parse("MATCH (n) NODETACH DELETE n").clauses().get(1)).detach());
        assertEquals(SqlTranslationException.Kind.SYNTAX, refusal("MATCH (n) SET n").kind());
        assertEquals(SqlTranslationException.Kind.SYNTAX, refusal("MATCH (n) REMOVE n").kind());
    }

    @Test
    void useNamesTheGraph() {
        CypherAst.Use u = (CypherAst.Use) parse("USE demo::MovieSpace MATCH (n) RETURN n").clauses().get(0);
        assertEquals("demo::MovieSpace", u.graph());
        CypherAst.Use b = (CypherAst.Use) parse("USE `my graph` RETURN 1").clauses().get(0);
        assertEquals("my graph", b.graph());
    }

    @Test
    void oneStatementPerIslandAndATrailingSemicolonIsFine() {
        assertEquals(2, parse("MATCH (n) RETURN n;").clauses().size());
        SqlTranslationException e = refusal("MATCH (n) RETURN n; MATCH (m) RETURN m");
        assertEquals(SqlTranslationException.Kind.SYNTAX, e.kind());
        assertTrue(e.getMessage().startsWith("one statement per island"), e.getMessage());
    }

    // ------------------------------------------------------------------ patterns

    @Test
    void relationshipPatternsCarryDirectionTypesAndVariables() {
        Match m = (Match) parse("MATCH (a:A)-[r:KNOWS]->(b), (b)<-[:LIKES|LOVES]-(c:C:D), (c)-[]-(d) RETURN a")
                .clauses().get(0);
        assertEquals(3, m.patterns().size());
        RelPattern r = (RelPattern) m.patterns().get(0).elements().get(1);
        assertEquals("r", r.variable());
        assertEquals(List.of("KNOWS"), r.types());
        assertEquals(Direction.OUT, r.direction());
        RelPattern in = (RelPattern) m.patterns().get(1).elements().get(1);
        assertNull(in.variable());
        assertEquals(List.of("LIKES", "LOVES"), in.types());
        assertEquals(Direction.IN, in.direction());
        NodePattern c = (NodePattern) m.patterns().get(1).elements().get(2);
        assertEquals(List.of("C", "D"), c.labels());
        RelPattern both = (RelPattern) m.patterns().get(2).elements().get(1);
        assertEquals(Direction.BOTH, both.direction());
        assertTrue(both.types().isEmpty());
    }

    @Test
    void propertyMapsAndLengthRangesAreRead() {
        Match m = (Match) parse("MATCH (a {name: 'x', n: 1})-[*1..3]->(b)-[:T*]->(c)-[*2]->(d)-[*..4]->(e)"
                + " RETURN a").clauses().get(0);
        NodePattern a = (NodePattern) m.patterns().get(0).elements().get(0);
        assertEquals(List.of("name", "n"), List.copyOf(a.properties().keySet()));
        RelPattern r1 = (RelPattern) m.patterns().get(0).elements().get(1);
        assertEquals(new CypherAst.LengthRange(1, 3), r1.lengthRange());
        RelPattern r2 = (RelPattern) m.patterns().get(0).elements().get(3);
        assertEquals(new CypherAst.LengthRange(-1, -1), r2.lengthRange());
        RelPattern r3 = (RelPattern) m.patterns().get(0).elements().get(5);
        assertEquals(new CypherAst.LengthRange(2, 2), r3.lengthRange());
        RelPattern r4 = (RelPattern) m.patterns().get(0).elements().get(7);
        assertEquals(new CypherAst.LengthRange(-1, 4), r4.lengthRange());
    }

    @Test
    void aNamedPathIsItsVariable() {
        Match m = (Match) parse("MATCH p = (a)-[:X]->(b) RETURN p").clauses().get(0);
        assertEquals("p", m.patterns().get(0).variable());
    }

    @Test
    void anOptionalMatchIsMarked() {
        Match m = (Match) parse("OPTIONAL MATCH (a) RETURN a").clauses().get(0);
        assertTrue(m.optional());
    }

    @Test
    void patternFormsNotReadYetAreRefusedByName() {
        assertEquals(SqlTranslationException.Kind.UNSUPPORTED, refusal("MATCH (a:A|B) RETURN a").kind());
        assertEquals(SqlTranslationException.Kind.UNSUPPORTED, refusal("MATCH (a WHERE a.x = 1) RETURN a").kind());
        assertEquals(SqlTranslationException.Kind.UNSUPPORTED, refusal("MATCH SHORTEST 1 (a)-[:X]->+(b) RETURN a")
                .kind());
        assertEquals(SqlTranslationException.Kind.UNSUPPORTED, refusal("MATCH (a)-[:X]->{1,3}(b) RETURN a").kind());
        assertEquals(SqlTranslationException.Kind.UNSUPPORTED, refusal("MATCH (a:$($label)) RETURN a").kind());
    }

    // ------------------------------------------------------------------ expressions

    @Test
    void precedenceIsNeo4js() {
        assertEquals("OR(NOT(EQ(a,b)),AND(c,d))", tree(expr("NOT a = b OR c AND d")));
        assertEquals("XOR(a,AND(b,c))", tree(expr("a XOR b AND c")));
        assertEquals("PLUS(a,TIMES(b,c))", tree(expr("a + b * c")));
        assertEquals("MINUS(MINUS(a,b),c)", tree(expr("a - b - c")));
        // Neo4j's: ^ left-associative and looser than unary minus (the TCK's Precedence2, verified on 2026.09.0)
        assertEquals("POWER(POWER(2,3),2)", tree(expr("2 ^ 3 ^ 2")));
        assertEquals("POWER(NEG(2),2)", tree(expr("-2 ^ 2")));
        assertEquals("TIMES(2,POWER(3,2))", tree(expr("2 * 3 ^ 2")));
        assertEquals("TIMES(NEG(a),b)", tree(expr("-a * b")));
        assertEquals("EQ(PLUS(a,1),MODULO(b,2))", tree(expr("a + 1 = b % 2")));
        assertEquals("AND(LT(1,x),LT(x,3))", tree(expr("1 < x < 3")));
        assertEquals("AND(AND(LE(a,b),LT(b,c)),GE(c,d))", tree(expr("a <= b < c >= d")));
        assertEquals("NE(a,b)", tree(expr("a <> b")));
        assertEquals("STARTS_WITH(a.name,'A')", tree(expr("a.name STARTS WITH 'A'")));
        assertEquals("OR(ENDS_WITH(a,'z'),CONTAINS(a,'q'))", tree(expr("a ENDS WITH 'z' OR a CONTAINS 'q'")));
        assertEquals("IN(PLUS(a,1),[1,2,3])", tree(expr("a + 1 IN [1, 2, 3]")));
        assertEquals("IS_NULL(a.x)", tree(expr("a.x IS NULL")));
        assertEquals("NOT(IS_NOT_NULL(a.x))", tree(expr("NOT a.x IS NOT NULL")));
        assertEquals("REGEX(a,'.*')", tree(expr("a =~ '.*'")));
        assertEquals("TIMES((PLUS(a,b)),c)", tree(expr("(a + b) * c")));
    }

    @Test
    void literalsAreReadAsNeo4jReadsThem() {
        assertEquals(255L, ((CypherAst.IntLit) expr("0xFF")).value());
        assertEquals(8L, ((CypherAst.IntLit) expr("0o10")).value());
        assertEquals(1.5e3, ((CypherAst.FloatLit) expr("1.5e3")).value());
        assertEquals(0.5, ((CypherAst.FloatLit) expr(".5")).value());
        assertEquals("it's \"quoted\"\n\ttabé", ((CypherAst.StringLit) expr("'it\\'s \"quoted\"\\n\\ttab\\u00e9'"))
                .value());
        assertEquals("a\"b", ((CypherAst.StringLit) expr("\"a\\\"b\"")).value());
        assertTrue(((CypherAst.BoolLit) expr("TRUE")).value());
        assertInstanceOf(CypherAst.NullLit.class, expr("null"));
        assertEquals("p", ((CypherAst.Param) expr("$p")).name());
        assertEquals("[1,'a',null]", tree(expr("[1, 'a', null]")));
        assertEquals("{a:1,b:x.y}", tree(expr("{a: 1, b: x.y}")));
    }

    @Test
    void backtickedNamesHoldAnythingAndAreNeverKeywords() {
        Return r = (Return) parse("MATCH (`my node`:`Weird Label`) RETURN `my node`.`a prop` AS `MATCH`")
                .clauses().get(1);
        assertEquals("MATCH", r.items().get(0).alias());
        CypherAst.Prop p = (CypherAst.Prop) r.items().get(0).expr();
        assertEquals("a prop", p.name());
        assertEquals("my node", ((CypherAst.Var) p.target()).name());
        Match m = (Match) parse("MATCH (`my node`:`Weird Label`) RETURN 1").clauses().get(0);
        assertEquals(List.of("Weird Label"), ((NodePattern) m.patterns().get(0).elements().get(0)).labels());
        assertEquals("a`b", ((CypherAst.Var) expr("`a``b`")).name());
    }

    @Test
    void callsCountStarAndCaseAreRead() {
        assertEquals("count(*)", tree(expr("count(*)")));
        assertEquals("count<distinct>(a.x)", tree(expr("count(DISTINCT a.x)")));
        assertEquals("coalesce(a.x,0)", tree(expr("coalesce(a.x, 0)")));
        assertEquals("duration.between(a,b)", tree(expr("duration.between(a, b)")));
        assertEquals("CASE(GT(a,1)->'big';else 'small')", tree(expr("CASE WHEN a > 1 THEN 'big' ELSE 'small' END")));
        assertEquals("CASE(a;1->'one';2->'two')", tree(expr("CASE a WHEN 1 THEN 'one' WHEN 2 THEN 'two' END")));
    }

    @Test
    void anItemsTextIsItsSourceAsWritten() {
        Return r = (Return) parse("MATCH (n) RETURN n.name  +  'x', count( * ), toUpper(n.a) AS up").clauses().get(1);
        assertEquals("n.name  +  'x'", r.items().get(0).text());
        assertEquals("count( * )", r.items().get(1).text());
        assertEquals("toUpper(n.a)", r.items().get(2).text());
        assertEquals("up", r.items().get(2).alias());
    }

    @Test
    void commentsAreSkipped() {
        Statement s = parse("// a comment\nMATCH (n) /* inner */ RETURN n.x // trailing");
        assertEquals(2, s.clauses().size());
    }

    @Test
    void expressionFormsNotReadYetAreRefusedByName() {
        assertEquals("[x IN [1,2] | TIMES(x,2)]", tree(expr("[x IN [1,2] | x * 2]")));
        assertEquals("[x IN [1,2] WHERE GT(x,1)]", tree(expr("[x IN [1,2] WHERE x > 1]")));
        // a pattern comprehension: a pattern from a node, a named path, an untyped or an undirected hop; a
        // parenthesized expression in a list is not one
        assertEquals("[pattern p:3 WHERE GT(b.x,1) | p]", tree(expr("[p = (a)-[:T]->(b) WHERE b.x > 1 | p]")));
        assertEquals("[pattern:3 | 1]", tree(expr("[(a)-->() | 1]")));
        assertEquals("[pattern:3 | b.name]", tree(expr("[(a)--(b) | b.name]")));
        assertEquals("[pattern:3 | r]", tree(expr("[(a)<-[r:T]-() | r]")));
        assertEquals("[MINUS((a),1),2]", tree(expr("[(a) - 1, 2]")));
        // a label predicate, after a variable or a parenthesized expression
        assertEquals("n:A:B", tree(expr("n:A:B")));
        assertEquals("AND(n:A,GT(n.x,1))", tree(expr("n:A AND n.x > 1")));
        assertEquals("a[0]", tree(expr("a[0]")));
        assertEquals("a[1..]", tree(expr("a[1..]")));
        assertEquals("a[..2]", tree(expr("a[..2]")));
        assertEquals("a[-1..2][0]", tree(expr("a[-1..2][0]")));
        assertEquals("[[1]][0][0]", tree(expr("[[1]][0][0]")));
        assertEquals(SqlTranslationException.Kind.UNSUPPORTED, refusal("MATCH (n) WHERE COUNT { (n)-->() } > 1 RETURN n")
                .kind());
        assertEquals(SqlTranslationException.Kind.UNSUPPORTED, refusal("RETURN 1 IS :: INTEGER").kind());
    }

    // ------------------------------------------------------------------ syntax errors

    @Test
    void aSyntaxErrorNamesWhatWasExpectedAtItsPosition() {
        SqlTranslationException e = refusal("MATCH (n:Person\nRETURN n");
        assertEquals(SqlTranslationException.Kind.SYNTAX, e.kind());
        assertEquals("expected ')' but found 'RETURN'", e.getMessage());
        assertEquals(2, e.line());
        assertEquals(1, e.column());
        SqlTranslationException end = refusal("MATCH (n) RETURN n.");
        assertEquals("expected a property name but found ''", end.getMessage());
        assertEquals(SqlTranslationException.Kind.SYNTAX, refusal("").kind());
        assertEquals(SqlTranslationException.Kind.SYNTAX, refusal("MATCH (n) RETURN").kind());
        assertEquals(SqlTranslationException.Kind.SYNTAX, refusal("MATCH (n) RETURN n WHERE n.x = 1").kind());
        // <-[]-> points both ways: undirected, as Neo4j reads it
        assertEquals(Direction.BOTH, ((RelPattern) ((Match) parse("MATCH (a)<-[:X]->(b) RETURN a").clauses().get(0))
                .patterns().get(0).elements().get(1)).direction());
        assertEquals("unterminated string", refusal("RETURN 'abc").getMessage());
        assertEquals("unexpected character '@'", refusal("RETURN @").getMessage());
        assertEquals("a CASE needs at least one WHEN", refusal("RETURN CASE END").getMessage());
    }

    @Test
    void anIntegerTooLargeIsNeo4jsErrorAndTheSmallestIntegerIsRead() {
        SqlTranslationException e = refusal("RETURN 99999999999999999999");
        assertEquals(SqlTranslationException.Kind.INVALID, e.kind());
        assertEquals(SqlTranslationException.Kind.INVALID, refusal("RETURN 9223372036854775808").kind());
        assertEquals(Long.MIN_VALUE, ((CypherAst.IntLit) expr("-9223372036854775808")).value());
        assertEquals(Long.MIN_VALUE, ((CypherAst.IntLit) expr("-0x8000000000000000")).value());
        assertEquals("NEG((POWER(2,2)))", tree(expr("-(2 ^ 2)")));
        assertEquals("POWER(NEG(2),2)", tree(expr("-2 ^ 2")));
    }

    @Test
    void theReviewsReaderFindings() {
        // 017 is Neo4j's syntax error; 1<-1 is 1 < -1; end is a name outside a CASE; (x) + 1 is a parenthesised x
        assertEquals(SqlTranslationException.Kind.SYNTAX, refusal("RETURN 017").kind());
        assertEquals("LT(1,NEG(1))", tree(expr("1<-1")));
        assertEquals("end", ((Return) parse("WITH 1 AS end RETURN end").clauses().get(1)).items().get(0).alias() == null
                ? ((CypherAst.Var) ((Return) parse("WITH 1 AS end RETURN end").clauses().get(1)).items().get(0).expr())
                        .name() : "");
        assertEquals("PLUS((x),1)", tree(expr("(x) + 1")));
        // (a pattern after a parenthesized variable: a predicate, since the pattern-predicate leg; the translator
        // refuses it outside a boolean context, as Neo4j does)
        assertEquals("(pattern:3)", tree(expr("(x)-->() ")));
        assertEquals("CASE(GT(a,1)->end;else 2)", tree(expr("CASE WHEN a > 1 THEN end ELSE 2 END")));
    }

    @Test
    void mergeActionsListPredicatesAndKeywordMapKeys() {
        CypherAst.Merge merge = (CypherAst.Merge) parse("MERGE (a:L) ON MATCH SET a.x = 1 ON CREATE SET a.y = 2"
                + " ON MATCH SET a:M RETURN a").clauses().get(0);
        assertEquals(List.of("L"), ((CypherAst.NodePattern) merge.pattern().elements().get(0)).labels());
        assertEquals("y", ((CypherAst.SetProperty) merge.onCreate().get(0)).key());
        assertEquals(2, merge.onMatch().size());
        assertInstanceOf(CypherAst.SetLabels.class, merge.onMatch().get(1));
        assertEquals("all(x IN [1,2] WHERE GT(x,0))", tree(expr("all(x IN [1, 2] WHERE x > 0)")));
        assertEquals("single(x IN [1,2] WHERE GT(x,0))", tree(expr("single(x IN [1, 2] WHERE x > 0)")));
        assertEquals(SqlTranslationException.Kind.SYNTAX, refusal("RETURN any(x IN [1, 2])").kind());
        assertEquals(SqlTranslationException.Kind.UNSUPPORTED, refusal("RETURN reduce(s = 0, x IN [1] | s + x)").kind());
        assertEquals("{null:1,NULL:2}", tree(expr("{null: 1, NULL: 2}")));
    }
}
