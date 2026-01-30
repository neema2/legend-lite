package org.finos.legend.pure.dsl.antlr;

import org.finos.legend.pure.dsl.*;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for the ANTLR-based Pure parser.
 * Tests parsing Pure expressions using the merged legend-engine grammar.
 */
class AntlrPureParserTest {

    // ========================================
    // Class.all() Tests
    // ========================================

    @Nested
    class ClassAllTests {
        @Test
        void simpleClassAll() {
            PureExpression result = org.finos.legend.pure.dsl.PureParser.parse("Person.all()");
            assertInstanceOf(ClassAllExpression.class, result);
            ClassAllExpression expr = (ClassAllExpression) result;
            assertEquals("Person", expr.className());
        }

        @Test
        void qualifiedClassAll() {
            PureExpression result = org.finos.legend.pure.dsl.PureParser.parse("model::domain::Person.all()");
            assertInstanceOf(ClassAllExpression.class, result);
            ClassAllExpression expr = (ClassAllExpression) result;
            assertEquals("model::domain::Person", expr.className());
        }
    }

    // ========================================
    // Filter Tests
    // ========================================

    @Nested
    class FilterTests {
        @Test
        void classAllWithFilter() {
            PureExpression result = org.finos.legend.pure.dsl.PureParser.parse(
                    "Person.all()->filter(p | $p.age > 21)");
            assertNotNull(result);
        }
    }

    // ========================================
    // Method Chaining Tests
    // ========================================

    @Nested
    class MethodChainingTests {
        @Test
        void filterThenLimit() {
            PureExpression result = org.finos.legend.pure.dsl.PureParser.parse(
                    "Person.all()->filter(p | $p.active == true)->limit(10)");
            assertNotNull(result);
        }

        @Test
        void projectWithColumns() {
            PureExpression result = org.finos.legend.pure.dsl.PureParser.parse(
                    "Person.all()->project(~[firstName, lastName])");
            assertNotNull(result);
        }
    }

    // ========================================
    // Literal Tests
    // ========================================

    @Nested
    class LiteralTests {
        @Test
        void stringLiteral() {
            PureExpression result = org.finos.legend.pure.dsl.PureParser.parse("'hello'");
            assertInstanceOf(LiteralExpr.class, result);
        }

        @Test
        void integerLiteral() {
            PureExpression result = org.finos.legend.pure.dsl.PureParser.parse("42");
            assertInstanceOf(LiteralExpr.class, result);
        }

        @Test
        void booleanLiteral() {
            PureExpression result = org.finos.legend.pure.dsl.PureParser.parse("true");
            assertInstanceOf(LiteralExpr.class, result);
        }
    }

    // ========================================
    // Variable Tests
    // ========================================

    @Nested
    class VariableTests {
        @Test
        void simpleVariable() {
            PureExpression result = org.finos.legend.pure.dsl.PureParser.parse("$x");
            assertInstanceOf(VariableExpr.class, result);
            VariableExpr expr = (VariableExpr) result;
            assertEquals("x", expr.name());
        }
    }

    // ========================================
    // Lambda Tests
    // ========================================

    @Nested
    class LambdaTests {
        @Test
        void simpleLambda() {
            PureExpression result = org.finos.legend.pure.dsl.PureParser.parse("{x | $x.name}");
            assertInstanceOf(LambdaExpression.class, result);
        }

        @Test
        void lambdaWithPipe() {
            PureExpression result = org.finos.legend.pure.dsl.PureParser.parse("x | $x.age > 21");
            assertInstanceOf(LambdaExpression.class, result);
        }
    }

    // ========================================
    // Instance Expression Tests
    // ========================================

    @Nested
    class InstanceTests {
        @Test
        void simpleInstance() {
            PureExpression result = org.finos.legend.pure.dsl.PureParser.parse(
                    "^Person(firstName='John', lastName='Doe')");
            assertInstanceOf(InstanceExpression.class, result);
        }
    }

    // ========================================
    // Comparison Tests
    // ========================================

    @Nested
    class ComparisonTests {
        @Test
        void equalsComparison() {
            PureExpression result = org.finos.legend.pure.dsl.PureParser.parse("$p.name == 'John'");
            assertNotNull(result);
        }
    }

    // ========================================
    // Let Expression Tests
    // ========================================

    @Nested
    class LetTests {
        @Test
        void simpleLetExpression() {
            PureExpression result = org.finos.legend.pure.dsl.PureParser.parse("let x = 42");
            assertInstanceOf(LetExpression.class, result);
            LetExpression let = (LetExpression) result;
            assertEquals("x", let.variableName());
            assertInstanceOf(LiteralExpr.class, let.value());
        }

        @Test
        void letWithStringValue() {
            PureExpression result = org.finos.legend.pure.dsl.PureParser.parse("let name = 'John'");
            assertInstanceOf(LetExpression.class, result);
            LetExpression let = (LetExpression) result;
            assertEquals("name", let.variableName());
        }

        @Test
        void blockWithLetAndResult() {
            // Lambda containing let statement followed by expression
            PureExpression result = org.finos.legend.pure.dsl.PureParser.parse(
                    "{| let x = 42; $x;}");
            assertInstanceOf(LambdaExpression.class, result);
            LambdaExpression lambda = (LambdaExpression) result;
            assertInstanceOf(BlockExpression.class, lambda.body());
            BlockExpression block = (BlockExpression) lambda.body();
            assertEquals(1, block.letStatements().size());
            assertEquals("x", block.letStatements().get(0).variableName());
            assertInstanceOf(VariableExpr.class, block.result());
        }

        @Test
        void blockWithMultipleLets() {
            PureExpression result = org.finos.legend.pure.dsl.PureParser.parse(
                    "{| let x = 1; let y = 2; $x;}");
            assertInstanceOf(LambdaExpression.class, result);
            LambdaExpression lambda = (LambdaExpression) result;
            assertInstanceOf(BlockExpression.class, lambda.body());
            BlockExpression block = (BlockExpression) lambda.body();
            assertEquals(2, block.letStatements().size());
            assertEquals("x", block.letStatements().get(0).variableName());
            assertEquals("y", block.letStatements().get(1).variableName());
        }
    }
}
