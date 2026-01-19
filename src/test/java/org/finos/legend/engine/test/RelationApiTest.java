package org.finos.legend.engine.test;

import org.finos.legend.pure.dsl.*;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for the Relation API - queries that start from Relation literals.
 * 
 * Relation syntax: #>{database.table}->operation()
 * 
 * This is distinct from Class-based queries which use:
 * Class.all()->filter()->project()
 */
@DisplayName("Relation API Tests")
class RelationApiTest {

    // ==================== Relation Literal Parsing ====================

    @Nested
    @DisplayName("Relation Literal Parsing: #>{db.table}")
    class RelationLiteralParsingTests {

        @Test
        @DisplayName("Parse simple Relation literal")
        void testParseSimpleRelationLiteral() {
            String query = "#>{MyDB.T_PERSON}";
            PureExpression expr = PureParser.parse(query);

            assertInstanceOf(RelationLiteral.class, expr);
            RelationLiteral literal = (RelationLiteral) expr;
            assertEquals("MyDB", literal.databaseRef());
            assertEquals("T_PERSON", literal.tableName());
        }

        @Test
        @DisplayName("Parse Relation literals with various names")
        void testParseRelationLiteralVariousNames() {
            assertRelationLiteral("#>{Store.EMPLOYEES}", "Store", "EMPLOYEES");
            assertRelationLiteral("#>{db.orders}", "db", "orders");
            assertRelationLiteral("#>{TestDB.T_CUSTOMER}", "TestDB", "T_CUSTOMER");
        }

        private void assertRelationLiteral(String query, String expectedDb, String expectedTable) {
            PureExpression expr = PureParser.parse(query);
            assertInstanceOf(RelationLiteral.class, expr);
            RelationLiteral literal = (RelationLiteral) expr;
            assertEquals(expectedDb, literal.databaseRef());
            assertEquals(expectedTable, literal.tableName());
        }
    }

    // ==================== ->select() Parsing ====================

    @Nested
    @DisplayName("Relation Select: ->select(~col1, ~col2)")
    class RelationSelectTests {

        @Test
        @DisplayName("Parse ->select() with single column")
        void testParseSelectSingleColumn() {
            String query = "#>{DB.TABLE}->select(~name)";
            PureExpression expr = PureParser.parse(query);

            assertInstanceOf(RelationSelectExpression.class, expr);
            RelationSelectExpression select = (RelationSelectExpression) expr;
            assertEquals(1, select.columns().size());
            assertEquals("name", select.columns().get(0));
        }

        @Test
        @DisplayName("Parse ->select() with multiple columns")
        void testParseSelectMultipleColumns() {
            String query = "#>{DB.TABLE}->select(~firstName, ~lastName, ~salary)";
            PureExpression expr = PureParser.parse(query);

            assertInstanceOf(RelationSelectExpression.class, expr);
            RelationSelectExpression select = (RelationSelectExpression) expr;
            assertEquals(3, select.columns().size());
            assertEquals("firstName", select.columns().get(0));
            assertEquals("lastName", select.columns().get(1));
            assertEquals("salary", select.columns().get(2));
        }
    }

    // ==================== ->extend() Parsing ====================

    @Nested
    @DisplayName("Relation Extend: ->extend(~newCol : lambda)")
    class RelationExtendTests {

        @Test
        @DisplayName("Parse ->extend() with column and lambda")
        void testParseExtendWithLambda() {
            String query = "#>{DB.TABLE}->extend(~fullName : {x | $x.firstName})";
            PureExpression expr = PureParser.parse(query);

            assertInstanceOf(RelationExtendExpression.class, expr);
            RelationExtendExpression extend = (RelationExtendExpression) expr;
            assertEquals("fullName", extend.newColumnName());
            assertNotNull(extend.expression());
            assertEquals("x", extend.expression().parameter());
        }
    }

    // ==================== ->from() Parsing ====================

    @Nested
    @DisplayName("Relation From: ->from(runtime)")
    class RelationFromTests {

        @Test
        @DisplayName("Parse ->from() with simple runtime name")
        void testParseFromBinding() {
            String query = "#>{DB.TABLE}->from(myRuntime)";
            PureExpression expr = PureParser.parse(query);

            assertInstanceOf(FromExpression.class, expr);
            FromExpression from = (FromExpression) expr;
            assertEquals("myRuntime", from.runtimeRef());
            assertInstanceOf(RelationLiteral.class, from.source());
        }

        @Test
        @DisplayName("Parse ->from() with qualified runtime name (My::Runtime::DuckDb)")
        void testParseFromQualifiedRuntime() {
            String query = "#>{MyDb.T_EMPLOYEE}->from(My::Runtime::DuckDb)";
            PureExpression expr = PureParser.parse(query);

            assertInstanceOf(FromExpression.class, expr);
            FromExpression from = (FromExpression) expr;
            assertEquals("My::Runtime::DuckDb", from.runtimeRef());
        }

        @Test
        @DisplayName("Parse chained operations with ->from()")
        void testParseChainedWithFrom() {
            String query = "#>{DB.TABLE}->select(~name, ~age)->from(duckdb)";
            PureExpression expr = PureParser.parse(query);

            assertInstanceOf(FromExpression.class, expr);
            FromExpression from = (FromExpression) expr;
            assertEquals("duckdb", from.runtimeRef());
            assertInstanceOf(RelationSelectExpression.class, from.source());
        }
    }

    // ==================== Lexer Tests ====================

    @Nested
    @DisplayName("Relation Lexer: Token generation")
    class RelationLexerTests {

        @Test
        @DisplayName("Lexer tokenizes #> as HASH_GREATER")
        void testLexerHashGreater() {
            var lexer = new PureLexer("#>{DB.TABLE}");
            var tokens = lexer.tokenize();

            assertEquals(Token.TokenType.HASH_GREATER, tokens.get(0).type());
            assertEquals("#>", tokens.get(0).value());
        }

        @Test
        @DisplayName("Lexer tokenizes ~ as TILDE")
        void testLexerTilde() {
            var lexer = new PureLexer("~column");
            var tokens = lexer.tokenize();

            assertEquals(Token.TokenType.TILDE, tokens.get(0).type());
        }

        @Test
        @DisplayName("Lexer tokenizes : as COLON")
        void testLexerColon() {
            var lexer = new PureLexer("name: value");
            var tokens = lexer.tokenize();

            boolean hasColon = tokens.stream()
                    .anyMatch(t -> t.type() == Token.TokenType.COLON);
            assertTrue(hasColon, "Should have COLON token");
        }
    }

    // ==================== Expression Types ====================

    @Nested
    @DisplayName("Relation Expression Types")
    class RelationExpressionTypeTests {

        @Test
        @DisplayName("RelationLiteral is a RelationExpression")
        void testRelationLiteralIsRelationExpression() {
            String query = "#>{DB.TABLE}";
            PureExpression expr = PureParser.parse(query);

            assertInstanceOf(RelationExpression.class, expr);
        }

        @Test
        @DisplayName("RelationSelectExpression is a RelationExpression")
        void testRelationSelectIsRelationExpression() {
            String query = "#>{DB.TABLE}->select(~col)";
            PureExpression expr = PureParser.parse(query);

            assertInstanceOf(RelationExpression.class, expr);
        }
    }
}
