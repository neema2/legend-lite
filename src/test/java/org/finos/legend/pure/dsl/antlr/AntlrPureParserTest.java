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
            PureExpression result = AntlrPureParserAdapter.parse("Person.all()");
            assertInstanceOf(ClassAllExpression.class, result);
            ClassAllExpression expr = (ClassAllExpression) result;
            assertEquals("Person", expr.className());
        }

        @Test
        void qualifiedClassAll() {
            PureExpression result = AntlrPureParserAdapter.parse("model::domain::Person.all()");
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
            PureExpression result = AntlrPureParserAdapter.parse(
                    "Person.all()->filter(p | $p.age > 21)");
            // Check it parsed successfully
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
            PureExpression result = AntlrPureParserAdapter.parse(
                    "Person.all()->filter(p | $p.active == true)->limit(10)");
            assertNotNull(result);
        }

        @Test
        void projectWithColumns() {
            PureExpression result = AntlrPureParserAdapter.parse(
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
            PureExpression result = AntlrPureParserAdapter.parse("'hello'");
            assertInstanceOf(LiteralExpr.class, result);
        }

        @Test
        void integerLiteral() {
            PureExpression result = AntlrPureParserAdapter.parse("42");
            assertInstanceOf(LiteralExpr.class, result);
        }

        @Test
        void booleanLiteral() {
            PureExpression result = AntlrPureParserAdapter.parse("true");
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
            PureExpression result = AntlrPureParserAdapter.parse("$x");
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
            PureExpression result = AntlrPureParserAdapter.parse("{x | $x.name}");
            assertInstanceOf(LambdaExpression.class, result);
        }

        @Test
        void lambdaWithPipe() {
            PureExpression result = AntlrPureParserAdapter.parse("x | $x.age > 21");
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
            PureExpression result = AntlrPureParserAdapter.parse(
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
            PureExpression result = AntlrPureParserAdapter.parse("$p.name == 'John'");
            // After processing the combinedExpression, we should have a comparison
            assertNotNull(result);
        }
    }
}
