package org.finos.legend.pure.dsl.antlr;

import org.finos.legend.pure.dsl.*;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests comparing ANTLR parser output with hand-written parser.
 * These tests validate that the ANTLR parser produces semantically equivalent
 * ASTs.
 */
class AntlrParserIntegrationTest {

    /**
     * Parses using both parsers and validates the results match.
     */
    private void assertParsesEquivalent(String query) {
        // Parse with hand-written parser
        PureExpression handWritten = org.finos.legend.pure.dsl.PureParser.parse(query);

        // Parse with ANTLR parser
        PureExpression antlr = AntlrPureParserAdapter.parse(query);

        // Validate types match
        assertEquals(handWritten.getClass(), antlr.getClass(),
                "AST type mismatch for: " + query);

        // For simple expressions, validate details
        if (handWritten instanceof ClassAllExpression hw && antlr instanceof ClassAllExpression a) {
            assertEquals(hw.className(), a.className(), "Class name mismatch");
        }
    }

    @Nested
    @DisplayName("Basic Class Expressions")
    class BasicClassTests {

        @Test
        void classAll() {
            assertParsesEquivalent("Person.all()");
        }

        @Test
        void classAllWithFilter() {
            String query = "Person.all()->filter({p | $p.lastName == 'Smith'})";

            PureExpression hw = org.finos.legend.pure.dsl.PureParser.parse(query);
            PureExpression antlr = AntlrPureParserAdapter.parse(query);

            assertInstanceOf(ClassFilterExpression.class, hw);
            assertInstanceOf(ClassFilterExpression.class, antlr);
        }
    }

    @Nested
    @DisplayName("Relation Expressions")
    class RelationTests {

        @Test
        void relationLiteral() {
            String query = "#>{store::PersonDb.T_PERSON}";

            PureExpression hw = org.finos.legend.pure.dsl.PureParser.parse(query);
            PureExpression antlr = AntlrPureParserAdapter.parse(query);

            assertInstanceOf(RelationLiteral.class, hw);
            assertInstanceOf(RelationLiteral.class, antlr);

            var hwRel = (RelationLiteral) hw;
            var antlrRel = (RelationLiteral) antlr;

            assertEquals(hwRel.databaseRef(), antlrRel.databaseRef());
            assertEquals(hwRel.tableName(), antlrRel.tableName());
        }
    }

    @Nested
    @DisplayName("Limit/Pagination")
    class LimitTests {

        @Test
        void limitOnClass() {
            String query = "Person.all()->limit(10)";

            PureExpression hw = org.finos.legend.pure.dsl.PureParser.parse(query);
            PureExpression antlr = AntlrPureParserAdapter.parse(query);

            assertInstanceOf(ClassLimitExpression.class, hw);
            assertInstanceOf(ClassLimitExpression.class, antlr);
        }
    }

    @Nested
    @DisplayName("Mutation Expressions")
    class MutationTests {

        @Test
        void instanceSave() {
            String query = "^Person(firstName='John', lastName='Doe')->save()";

            PureExpression hw = org.finos.legend.pure.dsl.PureParser.parse(query);
            PureExpression antlr = AntlrPureParserAdapter.parse(query);

            assertInstanceOf(SaveExpression.class, hw);
            assertInstanceOf(SaveExpression.class, antlr);
        }

        @Test
        void deleteExpression() {
            String query = "Person.all()->filter({p | $p.firstName == 'John'})->delete()";

            PureExpression hw = org.finos.legend.pure.dsl.PureParser.parse(query);
            PureExpression antlr = AntlrPureParserAdapter.parse(query);

            assertInstanceOf(DeleteExpression.class, hw);
            assertInstanceOf(DeleteExpression.class, antlr);
        }
    }
}
