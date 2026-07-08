package com.legend;

import com.legend.compiler.element.ModelContext;
import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.typed.TypedSpec;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The {@link Compiler} façade is the pipeline's honest front door: what it
 * exposes must be real, and what is not built must say so loudly.
 */
class CompilerFacadeTest {

    private static final String MODEL = """
            Class test::Person
            {
              name: String[1];
              age: Integer[1];
            }

            Database test::DB
            (
              Table T_PERSON ( NAME VARCHAR(200) NOT NULL, AGE INTEGER NOT NULL )
            )
            """;

    @Test
    void compileModelRunsFrontendToTypedModel() {
        ModelContext ctx = Compiler.compileModel(MODEL);
        assertTrue(ctx.findClass("test::Person").isPresent());
        assertEquals(2, ctx.findClass("test::Person").get().properties().size());
        assertTrue(ctx.findTable("test::DB", "T_PERSON").isPresent());
    }

    @Test
    void compileQueryCarriesAQueryThroughPhaseG() {
        TypedSpec typed = Compiler.compileQuery(MODEL,
                "#>{test::DB.T_PERSON}#->filter(x|$x.AGE > 30)->select(~NAME)");
        Type.RelationType rt = (Type.RelationType) typed.info().type();
        assertEquals(1, rt.columns().size());
        assertEquals("NAME", rt.columns().get(0).name());
        assertEquals(Type.Primitive.STRING, rt.columns().get(0).type());
    }

    @Test
    void compileQueryTypesClassQueriesToo() {
        TypedSpec typed = Compiler.compileQuery(MODEL,
                "test::Person.all()->filter(p|$p.age > 30)");
        assertEquals("test::Person", ((Type.ClassType) typed.info().type()).fqn());
    }

    @Test
    void queryResolvesPreludeNamesBare() {
        // Real legend's sectionless-lambda scope: META_IMPORTS always apply, so
        // platform enums resolve without qualification.
        TypedSpec typed = Compiler.compileQuery(MODEL,
                "#>{test::DB.T_PERSON}#->join(#>{test::DB.T_PERSON}#,"
                        + " JoinKind.INNER, {a, b | $a.AGE == $b.AGE}, 'r_')");
        assertEquals(4, ((Type.RelationType) typed.info().type()).columns().size(),
                "NAME, AGE + prefixed r_NAME, r_AGE");
    }

    @Test
    void queryRequiresFullPathsForUserElements() {
        // …and user elements need full paths, exactly like an ad-hoc engine lambda.
        Exception ex = assertThrows(Exception.class,
                () -> Compiler.compileQuery(MODEL, "Person.all()"));
        assertTrue(ex.getMessage().contains("fully qualified"),
                "the error must say how to fix it; got: " + ex.getMessage());
    }

    @Test
    void loweringIsHonestlyUnbuilt() {
        com.legend.error.NotImplementedException ex = assertThrows(com.legend.error.NotImplementedException.class,
                () -> Compiler.compile(MODEL, "test::Person.all()", "test::Rt"));
        assertTrue(ex.getMessage().contains("compileQuery"),
                "the error must point at what IS available");
    }
}
