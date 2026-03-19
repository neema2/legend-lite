package com.gs.legend.antlr;

import com.gs.legend.ast.*;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests that ValueSpecificationBuilder produces correct ValueSpecification AST.
 * 
 * These tests verify the core contract: the parser produces ONLY generic
 * AppliedFunction nodes — no ProjectExpression, no GroupByExpression, etc.
 */
class ValueSpecificationBuilderTest {

    private ValueSpecification parse(String query) {
        PureLexer lexer = new PureLexer(CharStreams.fromString(query));
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        PureParser parser = new PureParser(tokens);
        PureParser.ProgramLineContext tree = parser.programLine();
        ValueSpecificationBuilder builder = new ValueSpecificationBuilder();
        return builder.visit(tree);
    }

    @Nested
    class ClassAllTests {
        @Test
        void personAll() {
            var ast = parse("Person.all()");
            assertInstanceOf(AppliedFunction.class, ast);
            var fn = (AppliedFunction) ast;
            assertEquals("getAll", fn.function());
            assertEquals(1, fn.parameters().size());
            assertInstanceOf(PackageableElementPtr.class, fn.parameters().get(0));
            assertEquals("Person", ((PackageableElementPtr) fn.parameters().get(0)).fullPath());
        }
    }

    @Nested
    class FilterTests {
        @Test
        void filterIsGenericAppliedFunction() {
            var ast = parse("Person.all()->filter({p|$p.age > 21})");
            assertInstanceOf(AppliedFunction.class, ast);
            var filter = (AppliedFunction) ast;
            assertEquals("filter", filter.function());
            assertEquals(2, filter.parameters().size()); // [source, lambda]

            // Source is getAll
            var source = (AppliedFunction) filter.parameters().get(0);
            assertEquals("getAll", source.function());

            // Second param is lambda
            assertInstanceOf(LambdaFunction.class, filter.parameters().get(1));
        }
    }

    @Nested
    class ProjectTests {
        @Test
        void projectIsGenericNotSpecialized() {
            var ast = parse("Person.all()->project([{p|$p.name}], ['name'])");
            assertInstanceOf(AppliedFunction.class, ast);
            var project = (AppliedFunction) ast;
            assertEquals("project", project.function());
            // NOT ProjectExpression — that's the whole point!
            assertEquals(3, project.parameters().size()); // [source, lambdaCollection, aliasCollection]
        }
    }

    @Nested
    class LiteralTests {
        @Test
        void integerLiteral() {
            var ast = parse("42");
            assertInstanceOf(CInteger.class, ast);
            assertEquals(42L, ((CInteger) ast).value());
        }

        @Test
        void stringLiteral() {
            var ast = parse("'hello'");
            assertInstanceOf(CString.class, ast);
            assertEquals("hello", ((CString) ast).value());
        }

        @Test
        void booleanLiteral() {
            var ast = parse("true");
            assertInstanceOf(CBoolean.class, ast);
            assertTrue(((CBoolean) ast).value());
        }
    }

    @Nested
    class VariableTests {
        @Test
        void variable() {
            var ast = parse("$x");
            assertInstanceOf(Variable.class, ast);
            assertEquals("x", ((Variable) ast).name());
        }
    }

    @Nested
    class PropertyAccessTests {
        @Test
        void propertyAccess() {
            var ast = parse("$x.name");
            assertInstanceOf(AppliedProperty.class, ast);
            var prop = (AppliedProperty) ast;
            assertEquals("name", prop.property());
            assertEquals(1, prop.parameters().size());
            assertInstanceOf(Variable.class, prop.parameters().get(0));
        }
    }

    @Nested
    class ArithmeticTests {
        @Test
        void addition() {
            var ast = parse("1 + 2");
            assertInstanceOf(AppliedFunction.class, ast);
            var fn = (AppliedFunction) ast;
            assertEquals("plus", fn.function());
        }
    }

    @Nested
    class ComparisonTests {
        @Test
        void equality() {
            var ast = parse("$x.age == 21");
            assertInstanceOf(AppliedFunction.class, ast);
            var fn = (AppliedFunction) ast;
            assertEquals("equal", fn.function());
        }
    }

    @Nested
    class LambdaTests {
        @Test
        void singleParamLambda() {
            var ast = parse("{p|$p.name}");
            assertInstanceOf(LambdaFunction.class, ast);
            var lambda = (LambdaFunction) ast;
            assertEquals(1, lambda.parameters().size());
            assertEquals("p", lambda.parameters().get(0).name());
        }
    }

    @Nested
    class ColumnSpecTests {
        @Test
        void simpleColSpec() {
            var ast = parse("~name");
            assertInstanceOf(ClassInstance.class, ast);
            var ci = (ClassInstance) ast;
            assertEquals("colSpec", ci.type());
            assertInstanceOf(ColSpec.class, ci.value());
            assertEquals("name", ((ColSpec) ci.value()).name());
        }
    }

    @Nested
    class RelationApiTests {
        @Test
        void selectIsGeneric() {
            var ast = parse("#>{store::DB.TABLE}#->select(~name)");
            assertInstanceOf(AppliedFunction.class, ast);
            var select = (AppliedFunction) ast;
            assertEquals("select", select.function());
        }

        @Test
        void groupByIsGenericNotSpecialized() {
            var ast = parse("Person.all()->groupBy(~name, ~total:x|$x.amount:y|$y->plus())");
            assertInstanceOf(AppliedFunction.class, ast);
            var groupBy = (AppliedFunction) ast;
            assertEquals("groupBy", groupBy.function());
            // NOT GroupByExpression — generic AppliedFunction
        }
    }

    @Nested
    class CollectionTests {
        @Test
        void array() {
            var ast = parse("[1, 2, 3]");
            assertInstanceOf(PureCollection.class, ast);
            var coll = (PureCollection) ast;
            assertEquals(3, coll.values().size());
        }
    }

    @Nested
    class PackageableElementTests {
        @Test
        void classReference() {
            var ast = parse("Person");
            assertInstanceOf(PackageableElementPtr.class, ast);
            assertEquals("Person", ((PackageableElementPtr) ast).fullPath());
        }

        @Test
        void qualifiedReference() {
            var ast = parse("store::MyDatabase");
            assertInstanceOf(PackageableElementPtr.class, ast);
            assertEquals("store::MyDatabase", ((PackageableElementPtr) ast).fullPath());
        }
    }
}
