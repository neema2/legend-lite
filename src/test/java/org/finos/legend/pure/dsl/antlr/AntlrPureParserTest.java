package org.finos.legend.pure.dsl.antlr;

import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for the ANTLR-generated Pure parser.
 * Validates that the grammar correctly parses all Pure query constructs.
 */
class AntlrPureParserTest {

    private PureParser.QueryContext parse(String query) {
        var lexer = new PureLexer(CharStreams.fromString(query));
        var tokens = new CommonTokenStream(lexer);
        var parser = new PureParser(tokens);

        // Fail on syntax errors
        parser.removeErrorListeners();
        parser.addErrorListener(new org.antlr.v4.runtime.BaseErrorListener() {
            @Override
            public void syntaxError(org.antlr.v4.runtime.Recognizer<?, ?> recognizer,
                    Object offendingSymbol, int line, int charPositionInLine,
                    String msg, org.antlr.v4.runtime.RecognitionException e) {
                throw new RuntimeException("Parse error at " + line + ":" + charPositionInLine + " - " + msg);
            }
        });

        return parser.query();
    }

    @Nested
    @DisplayName("Class.all() Expressions")
    class ClassAllTests {

        @Test
        void simpleClassAll() {
            var ctx = parse("Person.all()");
            assertNotNull(ctx);
            assertEquals(1, ctx.getChildCount() - 1); // -1 for EOF
        }

        @Test
        void qualifiedClassAll() {
            var ctx = parse("model::domain::Person.all()");
            assertNotNull(ctx);
        }
    }

    @Nested
    @DisplayName("Method Chaining")
    class MethodChainingTests {

        @Test
        void filterWithLambda() {
            var ctx = parse("Person.all()->filter({p | $p.lastName == 'Smith'})");
            assertNotNull(ctx);
            var expr = ctx.expression();
            assertEquals(1, expr.methodCall().size());
            assertEquals("filter", expr.methodCall(0).IDENTIFIER().getText());
        }

        @Test
        void projectWithLambdaList() {
            var ctx = parse("Person.all()->project([{p | $p.firstName}, {p | $p.lastName}])");
            assertNotNull(ctx);
            var expr = ctx.expression();
            assertEquals("project", expr.methodCall(0).IDENTIFIER().getText());
        }

        @Test
        void fullChain() {
            var ctx = parse("Person.all()->filter({p | $p.age > 21})->project([{p | $p.firstName}])->limit(10)");
            assertNotNull(ctx);
            var expr = ctx.expression();
            assertEquals(3, expr.methodCall().size());
        }
    }

    @Nested
    @DisplayName("Relation Expressions")
    class RelationTests {

        @Test
        void relationLiteral() {
            var ctx = parse("#>{store::PersonDb.T_PERSON}");
            assertNotNull(ctx);
        }

        @Test
        void relationWithFilter() {
            var ctx = parse("#>{store::PersonDb.T_PERSON}->filter({r | $r.age > 25})");
            assertNotNull(ctx);
        }

        @Test
        void relationExtend() {
            var ctx = parse("#>{store::PersonDb.T_PERSON}->extend(~fullName : x | $x.firstName)");
            assertNotNull(ctx);
        }

        @Test
        void columnSelect() {
            var ctx = parse("#>{store::PersonDb.T_PERSON}->select(~firstName, ~lastName)");
            assertNotNull(ctx);
        }
    }

    @Nested
    @DisplayName("GraphFetch Expressions")
    class GraphFetchTests {

        @Test
        void simpleGraphFetch() {
            var ctx = parse("Person.all()->graphFetch(#{Person { firstName, lastName }}#)");
            assertNotNull(ctx);
        }

        @Test
        void nestedGraphFetch() {
            var ctx = parse("Person.all()->graphFetch(#{Person { firstName, address { city, zipCode } }}#)");
            assertNotNull(ctx);
        }

        @Test
        void graphFetchWithSerialize() {
            var ctx = parse("Person.all()->graphFetch(#{Person { firstName }}#)->serialize(#{Person { firstName }}#)");
            assertNotNull(ctx);
            var expr = ctx.expression();
            assertEquals(2, expr.methodCall().size());
        }
    }

    @Nested
    @DisplayName("Instance Expressions")
    class InstanceTests {

        @Test
        void simpleInstance() {
            var ctx = parse("^Person(firstName='John', lastName='Doe')");
            assertNotNull(ctx);
        }

        @Test
        void instanceSave() {
            var ctx = parse("^Person(firstName='John', lastName='Doe')->save()");
            assertNotNull(ctx);
        }
    }

    @Nested
    @DisplayName("Comparison Operators")
    class ComparisonTests {

        @Test
        void allOperators() {
            assertDoesNotThrow(() -> parse("Person.all()->filter({p | $p.x == 1})"));
            assertDoesNotThrow(() -> parse("Person.all()->filter({p | $p.x != 1})"));
            assertDoesNotThrow(() -> parse("Person.all()->filter({p | $p.x < 1})"));
            assertDoesNotThrow(() -> parse("Person.all()->filter({p | $p.x <= 1})"));
            assertDoesNotThrow(() -> parse("Person.all()->filter({p | $p.x > 1})"));
            assertDoesNotThrow(() -> parse("Person.all()->filter({p | $p.x >= 1})"));
        }

        @Test
        void logicalOperators() {
            assertDoesNotThrow(() -> parse("Person.all()->filter({p | $p.x > 1 && $p.y < 10})"));
            assertDoesNotThrow(() -> parse("Person.all()->filter({p | $p.x > 1 || $p.y < 10})"));
        }
    }

    @Nested
    @DisplayName("Literal Values")
    class LiteralTests {

        @Test
        void stringLiteral() {
            var ctx = parse("Person.all()->filter({p | $p.name == 'John\\'s'})");
            assertNotNull(ctx);
        }

        @Test
        void integerLiteral() {
            var ctx = parse("Person.all()->filter({p | $p.age == 42})");
            assertNotNull(ctx);
        }

        @Test
        void floatLiteral() {
            var ctx = parse("Person.all()->filter({p | $p.salary > 50000.50})");
            assertNotNull(ctx);
        }

        @Test
        void booleanLiterals() {
            assertDoesNotThrow(() -> parse("Person.all()->filter({p | $p.active == true})"));
            assertDoesNotThrow(() -> parse("Person.all()->filter({p | $p.active == false})"));
        }

        @Test
        void negativeLiteral() {
            var ctx = parse("Person.all()->filter({p | $p.balance > -100})");
            assertNotNull(ctx);
        }
    }
}
