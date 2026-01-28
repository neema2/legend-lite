package org.finos.legend.engine.sql;

import org.finos.legend.engine.sql.ast.*;
import org.finos.legend.engine.sql.ast.Expression.*;
import org.finos.legend.engine.sql.ast.FromItem.*;
import org.finos.legend.engine.sql.ast.SelectItem.*;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for the new SQL parser with SavePoint backtracking.
 */
@DisplayName("SelectParser Tests")
class SelectParserTest {

    // ==================== Lexer Tests ====================

    @Nested
    @DisplayName("Lexer Tests")
    class LexerTests {

        @Test
        @DisplayName("Tokenize simple SELECT")
        void testSimpleSelect() {
            Lexer lexer = new Lexer("SELECT * FROM t");
            
            assertEquals(Token.SELECT, lexer.token());
            lexer.nextToken();
            assertEquals(Token.STAR, lexer.token());
            lexer.nextToken();
            assertEquals(Token.FROM, lexer.token());
            lexer.nextToken();
            assertEquals(Token.IDENTIFIER, lexer.token());
            assertEquals("t", lexer.stringVal());
        }

        @Test
        @DisplayName("Tokenize string literal with escape")
        void testStringLiteral() {
            Lexer lexer = new Lexer("'It''s a test'");
            assertEquals(Token.STRING, lexer.token());
            assertEquals("It's a test", lexer.stringVal());
        }

        @Test
        @DisplayName("Tokenize quoted identifier")
        void testQuotedIdentifier() {
            Lexer lexer = new Lexer("\"My Column\"");
            assertEquals(Token.QUOTED_IDENTIFIER, lexer.token());
            assertEquals("My Column", lexer.stringVal());
        }

        @Test
        @DisplayName("Tokenize numbers")
        void testNumbers() {
            Lexer lexer = new Lexer("123 45.67");
            assertEquals(Token.INTEGER, lexer.token());
            assertEquals("123", lexer.stringVal());
            
            lexer.nextToken();
            assertEquals(Token.DECIMAL, lexer.token());
            assertEquals("45.67", lexer.stringVal());
        }

        @Test
        @DisplayName("Tokenize double colon (Postgres cast)")
        void testDoubleColon() {
            Lexer lexer = new Lexer("name::text");
            assertEquals(Token.IDENTIFIER, lexer.token());
            lexer.nextToken();
            assertEquals(Token.DOUBLE_COLON, lexer.token());
            lexer.nextToken();
            assertEquals(Token.IDENTIFIER, lexer.token());
        }

        @Test
        @DisplayName("SavePoint backtracking")
        void testSavePoint() {
            Lexer lexer = new Lexer("SELECT a, b FROM t");
            
            // Mark at SELECT
            Lexer.SavePoint mark = lexer.mark();
            assertEquals(Token.SELECT, lexer.token());
            
            // Advance several tokens
            lexer.nextToken(); // a
            lexer.nextToken(); // ,
            lexer.nextToken(); // b
            assertEquals(Token.IDENTIFIER, lexer.token());
            assertEquals("b", lexer.stringVal());
            
            // Reset to mark
            lexer.reset(mark);
            assertEquals(Token.SELECT, lexer.token());
        }

        @Test
        @DisplayName("Skip comments")
        void testComments() {
            Lexer lexer = new Lexer("SELECT -- comment\n * FROM t");
            assertEquals(Token.SELECT, lexer.token());
            lexer.nextToken();
            assertEquals(Token.STAR, lexer.token());
        }

        @Test
        @DisplayName("Legend-Engine keywords")
        void testLegendKeywords() {
            Lexer lexer = new Lexer("SERVICE TABLE CLASS");
            assertEquals(Token.SERVICE, lexer.token());
            lexer.nextToken();
            assertEquals(Token.TABLE, lexer.token());
            lexer.nextToken();
            assertEquals(Token.CLASS, lexer.token());
        }
    }

    // ==================== SELECT Statement Parsing ====================

    @Nested
    @DisplayName("SELECT Statement")
    class SelectTests {

        @Test
        @DisplayName("Parse SELECT * FROM table")
        void testSelectStar() {
            SelectParser parser = new SelectParser("SELECT * FROM users");
            SelectStatement stmt = parser.parseSelect();

            assertEquals(1, stmt.selectItems().size());
            assertInstanceOf(AllColumns.class, stmt.selectItems().getFirst());
            
            assertEquals(1, stmt.from().size());
            assertInstanceOf(TableRef.class, stmt.from().getFirst());
            TableRef table = (TableRef) stmt.from().getFirst();
            assertEquals("users", table.table());
        }

        @Test
        @DisplayName("Parse SELECT with columns")
        void testSelectColumns() {
            SelectParser parser = new SelectParser("SELECT id, name, age FROM users");
            SelectStatement stmt = parser.parseSelect();

            assertEquals(3, stmt.selectItems().size());
        }

        @Test
        @DisplayName("Parse SELECT with aliases")
        void testSelectWithAliases() {
            SelectParser parser = new SelectParser("SELECT firstName AS first, lastName last FROM users");
            SelectStatement stmt = parser.parseSelect();

            assertEquals(2, stmt.selectItems().size());
            ExpressionItem item1 = (ExpressionItem) stmt.selectItems().get(0);
            assertEquals("first", item1.alias());
            
            ExpressionItem item2 = (ExpressionItem) stmt.selectItems().get(1);
            assertEquals("last", item2.alias());
        }

        @Test
        @DisplayName("Parse SELECT DISTINCT")
        void testSelectDistinct() {
            SelectParser parser = new SelectParser("SELECT DISTINCT department FROM employees");
            SelectStatement stmt = parser.parseSelect();
            assertTrue(stmt.distinct());
        }
    }

    // ==================== WHERE Clause ====================

    @Nested
    @DisplayName("WHERE Clause")
    class WhereTests {

        @Test
        @DisplayName("WHERE with = comparison")
        void testWhereEquals() {
            SelectParser parser = new SelectParser("SELECT * FROM users WHERE id = 1");
            SelectStatement stmt = parser.parseSelect();

            assertTrue(stmt.hasWhere());
            assertInstanceOf(BinaryOp.class, stmt.where());
            BinaryOp where = (BinaryOp) stmt.where();
            assertEquals(BinaryOperator.EQ, where.operator());
        }

        @Test
        @DisplayName("WHERE with string literal")
        void testWhereStringLiteral() {
            SelectParser parser = new SelectParser("SELECT * FROM users WHERE name = 'John'");
            SelectStatement stmt = parser.parseSelect();

            BinaryOp where = (BinaryOp) stmt.where();
            assertInstanceOf(Literal.class, where.right());
            Literal lit = (Literal) where.right();
            assertEquals("John", lit.value());
        }

        @Test
        @DisplayName("WHERE IS NULL")
        void testWhereIsNull() {
            SelectParser parser = new SelectParser("SELECT * FROM users WHERE deleted_at IS NULL");
            SelectStatement stmt = parser.parseSelect();

            assertInstanceOf(IsNullExpr.class, stmt.where());
            assertFalse(((IsNullExpr) stmt.where()).negated());
        }

        @Test
        @DisplayName("WHERE IN list")
        void testWhereInList() {
            SelectParser parser = new SelectParser("SELECT * FROM users WHERE status IN ('active', 'pending')");
            SelectStatement stmt = parser.parseSelect();

            assertInstanceOf(InExpr.class, stmt.where());
            InExpr inExpr = (InExpr) stmt.where();
            assertEquals(2, inExpr.values().size());
        }
    }

    // ==================== FROM & JOINs ====================

    @Nested
    @DisplayName("FROM and JOIN")
    class FromJoinTests {

        @Test
        @DisplayName("FROM schema.table")
        void testFromSchemaTable() {
            SelectParser parser = new SelectParser("SELECT * FROM public.users");
            SelectStatement stmt = parser.parseSelect();

            TableRef table = (TableRef) stmt.from().getFirst();
            assertEquals("public", table.schema());
            assertEquals("users", table.table());
        }

        @Test
        @DisplayName("INNER JOIN")
        void testInnerJoin() {
            SelectParser parser = new SelectParser("SELECT * FROM orders o JOIN customers c ON o.customer_id = c.id");
            SelectStatement stmt = parser.parseSelect();

            assertInstanceOf(JoinedTable.class, stmt.from().getFirst());
            JoinedTable join = (JoinedTable) stmt.from().getFirst();
            assertEquals(JoinedTable.JoinType.INNER, join.joinType());
        }

        @Test
        @DisplayName("LEFT OUTER JOIN")
        void testLeftOuterJoin() {
            SelectParser parser = new SelectParser("SELECT * FROM orders LEFT OUTER JOIN customers ON orders.cid = customers.id");
            SelectStatement stmt = parser.parseSelect();

            JoinedTable join = (JoinedTable) stmt.from().getFirst();
            assertEquals(JoinedTable.JoinType.LEFT_OUTER, join.joinType());
        }

        @Test
        @DisplayName("Subquery in FROM")
        void testFromSubquery() {
            SelectParser parser = new SelectParser("SELECT * FROM (SELECT id, name FROM users) AS sub");
            SelectStatement stmt = parser.parseSelect();

            assertInstanceOf(SubQuery.class, stmt.from().getFirst());
            SubQuery sub = (SubQuery) stmt.from().getFirst();
            assertEquals("sub", sub.alias());
        }
    }

    // ==================== Legend-Engine Table Functions ====================

    @Nested
    @DisplayName("Legend-Engine Table Functions")
    class TableFunctionTests {

        @Test
        @DisplayName("FROM service('/path')")
        void testServiceFunction() {
            SelectParser parser = new SelectParser("SELECT * FROM service('/hosted/employee/service')");
            SelectStatement stmt = parser.parseSelect();

            assertInstanceOf(TableFunction.class, stmt.from().getFirst());
            TableFunction func = (TableFunction) stmt.from().getFirst();
            assertEquals("service", func.functionName().toLowerCase());
            assertEquals(1, func.arguments().size());
        }

        @Test
        @DisplayName("FROM table('store::DB.TABLE')")
        void testTableFunction() {
            SelectParser parser = new SelectParser("SELECT * FROM table('store::PersonDatabase.T_PERSON')");
            SelectStatement stmt = parser.parseSelect();

            TableFunction func = (TableFunction) stmt.from().getFirst();
            assertEquals("table", func.functionName().toLowerCase());
            assertEquals(1, func.arguments().size());
        }

        @Test
        @DisplayName("FROM class('my::Class')")
        void testClassFunction() {
            SelectParser parser = new SelectParser("SELECT * FROM class('my::domain::Employee')");
            SelectStatement stmt = parser.parseSelect();

            TableFunction func = (TableFunction) stmt.from().getFirst();
            assertEquals("class", func.functionName().toLowerCase());
        }

        @Test
        @DisplayName("Service function with alias")
        void testTableFunctionWithAlias() {
            SelectParser parser = new SelectParser("SELECT * FROM service('/svc') AS s");
            SelectStatement stmt = parser.parseSelect();

            TableFunction func = (TableFunction) stmt.from().getFirst();
            assertEquals("s", func.alias());
        }
    }

    // ==================== GROUP BY & ORDER BY ====================

    @Nested
    @DisplayName("GROUP BY and ORDER BY")
    class GroupOrderTests {

        @Test
        @DisplayName("GROUP BY")
        void testGroupBy() {
            SelectParser parser = new SelectParser("SELECT department, COUNT(*) FROM employees GROUP BY department");
            SelectStatement stmt = parser.parseSelect();

            assertTrue(stmt.hasGroupBy());
            assertEquals(1, stmt.groupBy().size());
        }

        @Test
        @DisplayName("ORDER BY")
        void testOrderBy() {
            SelectParser parser = new SelectParser("SELECT * FROM users ORDER BY lastName ASC, firstName DESC");
            SelectStatement stmt = parser.parseSelect();

            assertTrue(stmt.hasOrderBy());
            assertEquals(2, stmt.orderBy().size());
            assertEquals(OrderSpec.Direction.ASC, stmt.orderBy().get(0).direction());
            assertEquals(OrderSpec.Direction.DESC, stmt.orderBy().get(1).direction());
        }

        @Test
        @DisplayName("LIMIT and OFFSET")
        void testLimitOffset() {
            SelectParser parser = new SelectParser("SELECT * FROM users LIMIT 10 OFFSET 20");
            SelectStatement stmt = parser.parseSelect();

            assertTrue(stmt.hasLimit());
            assertEquals(10, stmt.limit());
            assertEquals(20, stmt.offset());
        }
    }

    // ==================== Aggregates & Window Functions ====================

    @Nested
    @DisplayName("Aggregate and Window Functions")
    class AggregateFunctionTests {

        @Test
        @DisplayName("COUNT(*)")
        void testCountStar() {
            SelectParser parser = new SelectParser("SELECT COUNT(*) FROM users");
            SelectStatement stmt = parser.parseSelect();

            ExpressionItem item = (ExpressionItem) stmt.selectItems().getFirst();
            assertInstanceOf(FunctionCall.class, item.expression());
        }

        @Test
        @DisplayName("SUM(column)")
        void testSumColumn() {
            SelectParser parser = new SelectParser("SELECT SUM(salary) FROM employees");
            SelectStatement stmt = parser.parseSelect();

            ExpressionItem item = (ExpressionItem) stmt.selectItems().getFirst();
            FunctionCall func = (FunctionCall) item.expression();
            assertEquals("SUM", func.functionName());
        }

        @Test
        @DisplayName("Window function with OVER")
        void testWindowFunction() {
            SelectParser parser = new SelectParser("SELECT ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) FROM employees");
            SelectStatement stmt = parser.parseSelect();

            ExpressionItem item = (ExpressionItem) stmt.selectItems().getFirst();
            assertInstanceOf(WindowExpr.class, item.expression());
            WindowExpr window = (WindowExpr) item.expression();
            assertTrue(window.hasPartition());
            assertTrue(window.hasOrderBy());
        }
    }

    // ==================== Complex Queries ====================

    @Nested
    @DisplayName("Complex Queries")
    class ComplexQueryTests {

        @Test
        @DisplayName("Complete query with all clauses")
        void testCompleteQuery() {
            String sql = """
                SELECT 
                    d.name AS department,
                    COUNT(*) AS count,
                    AVG(e.salary) AS avg
                FROM employees e
                JOIN departments d ON e.dept_id = d.id
                WHERE e.status = 'active'
                GROUP BY d.name
                ORDER BY avg DESC
                LIMIT 10
                """;

            SelectParser parser = new SelectParser(sql);
            SelectStatement stmt = parser.parseSelect();

            assertEquals(3, stmt.selectItems().size());
            assertTrue(stmt.hasWhere());
            assertTrue(stmt.hasGroupBy());
            assertTrue(stmt.hasOrderBy());
            assertTrue(stmt.hasLimit());
        }

        @Test
        @DisplayName("EXISTS subquery")
        void testExistsSubquery() {
            String sql = "SELECT * FROM orders o WHERE EXISTS (SELECT 1 FROM returns r WHERE r.order_id = o.id)";

            SelectParser parser = new SelectParser(sql);
            SelectStatement stmt = parser.parseSelect();

            assertInstanceOf(ExistsExpr.class, stmt.where());
        }

        @Test
        @DisplayName("CASE expression")
        void testCaseExpression() {
            SelectParser parser = new SelectParser("SELECT CASE WHEN status = 'A' THEN 'Active' ELSE 'Inactive' END FROM users");
            SelectStatement stmt = parser.parseSelect();

            ExpressionItem item = (ExpressionItem) stmt.selectItems().getFirst();
            assertInstanceOf(CaseExpr.class, item.expression());
        }
    }
}
