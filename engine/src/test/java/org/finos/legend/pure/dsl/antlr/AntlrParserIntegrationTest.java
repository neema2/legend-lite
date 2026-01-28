package org.finos.legend.pure.dsl.antlr;

import org.finos.legend.pure.dsl.*;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for the ANTLR-based Pure parser.
 * Validates parsing of various Pure expression types.
 */
class AntlrParserIntegrationTest {

    @Nested
    @DisplayName("Basic Class Expressions")
    class BasicClassTests {

        @Test
        void classAll() {
            PureExpression result = org.finos.legend.pure.dsl.PureParser.parse("Person.all()");
            assertInstanceOf(ClassAllExpression.class, result);
            assertEquals("Person", ((ClassAllExpression) result).className());
        }

        @Test
        void classAllWithFilter() {
            String query = "Person.all()->filter({p | $p.lastName == 'Smith'})";
            PureExpression result = org.finos.legend.pure.dsl.PureParser.parse(query);
            assertInstanceOf(ClassFilterExpression.class, result);
        }
    }

    @Nested
    @DisplayName("Relation Expressions")
    class RelationTests {

        @Test
        void relationLiteral() {
            String query = "#>{store::PersonDb.T_PERSON}";
            PureExpression result = org.finos.legend.pure.dsl.PureParser.parse(query);

            assertInstanceOf(RelationLiteral.class, result);
            var rel = (RelationLiteral) result;
            assertEquals("store::PersonDb", rel.databaseRef());
            assertEquals("T_PERSON", rel.tableName());
        }
    }

    @Nested
    @DisplayName("Limit/Pagination")
    class LimitTests {

        @Test
        void limitOnClass() {
            String query = "Person.all()->limit(10)";
            PureExpression result = org.finos.legend.pure.dsl.PureParser.parse(query);
            assertInstanceOf(ClassLimitExpression.class, result);
        }
    }

    @Nested
    @DisplayName("Mutation Expressions")
    class MutationTests {

        @Test
        void instanceSave() {
            String query = "^Person(firstName='John', lastName='Doe')->save()";
            PureExpression result = org.finos.legend.pure.dsl.PureParser.parse(query);
            assertInstanceOf(SaveExpression.class, result);
        }

        @Test
        void deleteExpression() {
            String query = "Person.all()->filter({p | $p.firstName == 'John'})->delete()";
            PureExpression result = org.finos.legend.pure.dsl.PureParser.parse(query);
            assertInstanceOf(DeleteExpression.class, result);
        }
    }
}
