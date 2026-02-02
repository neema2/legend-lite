package org.finos.legend.engine.test;

import org.finos.legend.pure.dsl.legend.*;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for PureLegendParser - the "thin" parser that produces minimal AST.
 */
public class PureLegendParserTest {

    @Test
    void testSimpleClassAll() {
        Expression expr = PureLegendParser.parse("Person.all()");

        // Should produce: Function("all", [Function("class", [Literal("Person")])])
        assertInstanceOf(Function.class, expr);
        Function all = (Function) expr;
        assertEquals("all", all.function());
        assertEquals(1, all.parameters().size());

        assertInstanceOf(Function.class, all.parameters().get(0));
        Function classRef = (Function) all.parameters().get(0);
        assertEquals("class", classRef.function());
    }

    @Test
    void testMethodCall() {
        Expression expr = PureLegendParser.parse("Person.all()->filter(x | $x.age > 21)");

        // Should produce: Function("filter", [Function("all", ...), Lambda(...)])
        assertInstanceOf(Function.class, expr);
        Function filter = (Function) expr;
        assertEquals("filter", filter.function());
        assertEquals(2, filter.parameters().size()); // receiver + predicate

        // First param is the receiver (Person.all())
        assertInstanceOf(Function.class, filter.parameters().get(0));

        // Second param is lambda
        assertInstanceOf(Lambda.class, filter.parameters().get(1));
    }

    @Test
    void testPropertyAccess() {
        Expression expr = PureLegendParser.parse("$x.name");

        assertInstanceOf(Property.class, expr);
        Property prop = (Property) expr;
        assertEquals("name", prop.property());

        assertInstanceOf(Variable.class, prop.source());
        assertEquals("x", ((Variable) prop.source()).name());
    }

    @Test
    void testArithmeticAsFunction() {
        Expression expr = PureLegendParser.parse("1 + 2");

        // Should become: Function("plus", [Literal(1), Literal(2)])
        assertInstanceOf(Function.class, expr);
        Function plus = (Function) expr;
        assertEquals("plus", plus.function());
        assertEquals(2, plus.parameters().size());
    }

    @Test
    void testColumnSpec() {
        Expression expr = PureLegendParser.parse("~name");

        // Should become: Function("column", [Literal("name")])
        assertInstanceOf(Function.class, expr);
        Function col = (Function) expr;
        assertEquals("column", col.function());
        assertEquals(1, col.parameters().size());

        assertInstanceOf(Literal.class, col.parameters().get(0));
        assertEquals("name", ((Literal) col.parameters().get(0)).value());
    }

    @Test
    void testColumnSpecWithLambda() {
        Expression expr = PureLegendParser.parse("~fullName:x|$x.firstName + ' ' + $x.lastName");

        // Should become: Function("column", [Literal("fullName"), Lambda(...)])
        assertInstanceOf(Function.class, expr);
        Function col = (Function) expr;
        assertEquals("column", col.function());
        assertEquals(2, col.parameters().size()); // name + lambda

        assertInstanceOf(Lambda.class, col.parameters().get(1));
    }

    @Test
    void testGroupByAsGenericFunction() {
        Expression expr = PureLegendParser.parse("$data->groupBy(~dept, ~total:x|$x.amount:y|$y->sum())");

        // Key test: groupBy should be a generic Function, not a specialized
        // GroupByExpression
        assertInstanceOf(Function.class, expr);
        Function groupBy = (Function) expr;
        assertEquals("groupBy", groupBy.function());

        // Should have 3 args: receiver, grouping col, agg col
        assertEquals(3, groupBy.parameters().size());

        // First is receiver ($data)
        assertInstanceOf(Variable.class, groupBy.parameters().get(0));

        // Second and third are column specs (Function("column", ...))
        assertInstanceOf(Function.class, groupBy.parameters().get(1));
        assertInstanceOf(Function.class, groupBy.parameters().get(2));
    }

    @Test
    void testChainedMethods() {
        Expression expr = PureLegendParser.parse("Person.all()->filter(p|true)->project(~name)->limit(10)");

        // Should be: Function("limit", [Function("project", [Function("filter",
        // [...])])])
        assertInstanceOf(Function.class, expr);
        Function limit = (Function) expr;
        assertEquals("limit", limit.function());

        assertInstanceOf(Function.class, limit.parameters().get(0));
        Function project = (Function) limit.parameters().get(0);
        assertEquals("project", project.function());

        assertInstanceOf(Function.class, project.parameters().get(0));
        Function filter = (Function) project.parameters().get(0);
        assertEquals("filter", filter.function());
    }

    @Test
    void testLiteralValues() {
        assertEquals(42L, ((Literal) PureLegendParser.parse("42")).value());
        assertEquals("hello", ((Literal) PureLegendParser.parse("'hello'")).value());
        assertEquals(true, ((Literal) PureLegendParser.parse("true")).value());
        assertEquals(3.14, ((Literal) PureLegendParser.parse("3.14")).value());
    }

    @Test
    void testVariable() {
        Expression expr = PureLegendParser.parse("$myVar");

        assertInstanceOf(Variable.class, expr);
        assertEquals("myVar", ((Variable) expr).name());
    }

    @Test
    void testLambdaWithMultipleParams() {
        Expression expr = PureLegendParser.parse("{x, y | $x + $y}");

        assertInstanceOf(Lambda.class, expr);
        Lambda lambda = (Lambda) expr;
        assertEquals(2, lambda.parameters().size());
        assertEquals("x", lambda.parameters().get(0));
        assertEquals("y", lambda.parameters().get(1));
    }

    @Test
    void testCollection() {
        Expression expr = PureLegendParser.parse("~[name, age, dept]");

        assertInstanceOf(Collection.class, expr);
        Collection col = (Collection) expr;
        assertEquals(3, col.values().size());
    }
}
