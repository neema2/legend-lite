package com.legend.resolver;

import com.legend.Compiler;
import com.legend.compiler.NameResolver;
import com.legend.compiler.spec.SpecCompiler;
import com.legend.compiler.spec.typed.TypedFilter;
import com.legend.compiler.spec.typed.TypedFrom;
import com.legend.compiler.spec.typed.TypedProject;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.compiler.spec.typed.TypedTableReference;
import com.legend.error.MappingResolutionException;
import com.legend.parser.SpecParser;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * M-H2b typed-tree unit pins: the rewrite's SHAPE invariants (re-schema,
 * fresh-var hygiene, project-info stability, context precedence) asserted
 * on the typed output, independent of SQL rendering.
 */
class StoreResolverTest {

    private static final String MODEL = """
            Class m::Person { name: String[1]; age: Integer[1]; }
            Database s::DB ( Table T (NAME VARCHAR(50), AGE INTEGER) )
            Mapping m::M ( *m::Person: Relational { ~mainTable [s::DB] T
              name: T.NAME, age: T.AGE } )
            Runtime m::RT { mappings: [m::M]; }
            """;

    private static List<TypedSpec> resolve(String query, String driverRuntime) {
        var ctx = Compiler.compileModel(MODEL);
        SpecCompiler specs = new SpecCompiler(ctx);
        var body = specs.typeQueryBody(
                NameResolver.resolveQuery(SpecParser.parse(query)));
        return new StoreResolver(ctx, specs).resolve(body, driverRuntime);
    }

    @Test
    @DisplayName("filter re-schemas to the pipeline row; project info is UNCHANGED")
    void reSchemaAndProjectStability() {
        var ctx = Compiler.compileModel(MODEL);
        SpecCompiler specs = new SpecCompiler(ctx);
        var body = specs.typeQueryBody(NameResolver.resolveQuery(SpecParser.parse(
                "m::Person.all()->filter(p|$p.age > 30)"
                        + "->project(~[name: p|$p.name])->from(m::RT)")));
        var before = ((TypedFrom) body.get(0)).source();
        var after = new StoreResolver(ctx, specs).resolve(body, null);

        TypedFrom from = (TypedFrom) after.get(0);
        TypedProject project = assertInstanceOf(TypedProject.class, from.source());
        assertEquals(before.info(), project.info(),
                "the projection boundary's info must be UNCHANGED");
        TypedFilter filter = assertInstanceOf(TypedFilter.class, project.source());
        assertTrue(filter.info().type() instanceof com.legend.compiler.element.type.Type.RelationType,
                "the filter re-schemas from Person[*] to the pipeline row");
        assertInstanceOf(TypedTableReference.class, filter.source(),
                "the pipeline bottoms at the mapping's table scan");
        // Fresh-var hygiene: the rewritten lambda's parameter is the fresh
        // row var, not the user's 'p'.
        assertEquals(List.of("_r0"), filter.predicate().parameters());
    }

    @Test
    @DisplayName("no execution context is a loud user-facing error")
    void noContextIsLoud() {
        MappingResolutionException e = assertThrows(MappingResolutionException.class,
                () -> resolve("m::Person.all()->project(~[name: p|$p.name])", null));
        assertTrue(e.getMessage().contains("execution context"), e.getMessage());
    }

    @Test
    @DisplayName("driver-supplied runtime resolves a from-less query (the corpus shape)")
    void driverRuntimeResolves() {
        var resolved = resolve("m::Person.all()->project(~[name: p|$p.name])", "m::RT");
        assertInstanceOf(TypedProject.class, resolved.get(0));
    }

    @Test
    @DisplayName("a query with no class fetch passes through untouched")
    void relationQueryUntouched() {
        var body = resolve("#>{s::DB.T}#->filter(x|$x.AGE > 30)", null);
        assertInstanceOf(TypedFilter.class, body.get(0));
    }

    @Test
    @DisplayName("post-condition (README rule 9): a surviving TypedGetAll is a loud resolver-phase gap")
    void noEscapeWalkCatchesSurvivingGetAll() {
        var info = new com.legend.compiler.element.type.ExprType(
                new com.legend.compiler.element.type.Type.ClassType("test::C"),
                com.legend.compiler.element.type.Multiplicity.Bounded.ZERO_MANY);
        var escapee = new com.legend.compiler.spec.typed.TypedGetAll(
                "test::C", List.of(), false, info);
        var e = assertThrows(com.legend.error.NotImplementedException.class,
                () -> StoreResolver.assertNoStoreOnlyEscapees(escapee));
        assertTrue(e.getMessage().contains("test::C"), e.getMessage());
        assertTrue(e.getMessage().contains("resolver"), e.getMessage());
    }
}
