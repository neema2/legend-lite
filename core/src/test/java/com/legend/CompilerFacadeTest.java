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

            ###Relational
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
        Type.RelationType rt = Type.requireRelationSchema(typed.info().type());
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
        assertEquals(4, Type.requireRelationSchema(typed.info().type())
                        .columns().size(),
                "NAME, AGE + prefixed r_NAME, r_AGE");
    }

    @Test
    void queryRequiresFullPathsForUserElements() {
        // A UNIQUE bare name resolves by simple name (engine leniency);
        // an unknown one still says how to fix it.
        Exception ex = assertThrows(Exception.class,
                () -> Compiler.compileQuery(MODEL, "Nobody.all()"));
        assertTrue(String.valueOf(ex.getMessage()).contains("fully qualified"),
                "the error must say how to fix it; got: " + ex.getMessage());
    }

    @Test
    void compileRendersTheFullPlanWithoutExecuting() {
        String planModel = MODEL + """

                ###Mapping
                Mapping test::M (
                  *test::Person: Relational { ~mainTable [test::DB] T_PERSON
                    name: T_PERSON.NAME, age: T_PERSON.AGE }
                )
                ###Connection
                RelationalDatabaseConnection test::DBDuckDB { store: test::DB; type: DuckDB; specification: DuckDB { }; auth: Test; }
                ###Runtime
                Runtime test::RT { mappings: [test::M]; connections: [ test::DB: [ c0: test::DBDuckDB ] ]; }
                """;
        com.legend.plan.QueryPlan plan = Compiler.plan(planModel,
                "test::Person.all()->project(~[name: p|$p.name])", "test::RT");
        assertEquals("SELECT t0.NAME AS name\nFROM T_PERSON AS t0", plan.sql());
        assertEquals(com.legend.plan.ResultShape.TABULAR, plan.shape(),
                "the plan carries the REAL shape — bridges re-wrap, never invent");
        assertEquals(plan.sql(), Compiler.compile(planModel,
                "test::Person.all()->project(~[name: p|$p.name])", "test::RT"));
    }

    @Test
    void undeclaredDialectIsHonestlyUnbuilt() {
        // 2026-10-01 W5.5/P1: Postgres is built now; Snowflake stands in as the unbuilt dialect
        String sfModel = MODEL + """

                ###Mapping
                Mapping test::M (
                  *test::Person: Relational { ~mainTable [test::DB] T_PERSON
                    name: T_PERSON.NAME, age: T_PERSON.AGE }
                )
                ###Connection
                RelationalDatabaseConnection test::Conn {
                  store: test::DB; type: Snowflake;
                  specification: DuckDB { }; auth: Test; }
                ###Runtime
                Runtime test::RT { mappings: [test::M];
                  connections: [ test::DB: [ environment: test::Conn ] ]; }
                """;
        com.legend.error.NotImplementedException ex =
                assertThrows(com.legend.error.NotImplementedException.class,
                        () -> Compiler.compile(sfModel,
                                "test::Person.all()->project(~[name: p|$p.name])",
                                "test::RT"));
        assertTrue(String.valueOf(ex.getMessage()).contains("Snowflake"),
                "the error must name the undeclared dialect: " + ex.getMessage());
    }
}
