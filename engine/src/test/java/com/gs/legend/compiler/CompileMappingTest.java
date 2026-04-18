package com.gs.legend.compiler;

import com.gs.legend.ast.AppliedFunction;
import com.gs.legend.ast.PackageableElementPtr;
import com.gs.legend.compiled.MappingKind;
import com.gs.legend.model.PureModelBuilder;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Contract tests for {@link TypeChecker#check(com.gs.legend.model.def.MappingDefinition)}.
 *
 * <p>Covers:
 * <ul>
 *   <li>Relational class mapping: kind, sourceName value, wrapped AST identity,
 *       stamped depth</li>
 *   <li>M2M class mapping: kind, sourceName = source class FQN, stamped</li>
 *   <li>Multi-class mapping: every class appears as a CompiledMappedClass in
 *       input order</li>
 *   <li>Primitive unification: query path and build path share one types map
 *       and never double-stamp</li>
 *   <li>FQN-keyed memoization of the compiled result</li>
 * </ul>
 */
class CompileMappingTest {

    private static final String MODEL = """
            Class model::Person {
                firstName: String[1];
                lastName: String[1];
                age: Integer[1];
            }

            Database model::PersonDb (
                Table T_PERSON (
                    FIRST_NAME VARCHAR(100),
                    LAST_NAME VARCHAR(100),
                    AGE INTEGER
                )
            )

            Mapping model::PersonMapping (
                *model::Person: Relational
                {
                    ~mainTable [model::PersonDb] T_PERSON
                    firstName: [model::PersonDb] T_PERSON.FIRST_NAME,
                    lastName: [model::PersonDb] T_PERSON.LAST_NAME,
                    age: [model::PersonDb] T_PERSON.AGE
                }
            )

            ###Connection
            RelationalDatabaseConnection model::PersonConn
            {
                store: model::PersonDb;
                type: DuckDB;
                specification: InMemory {};
                auth: NoAuth {};
            }

            ###Runtime
            Runtime model::PersonRT
            {
                mappings: [ model::PersonMapping ];
                connections:
                [
                    model::PersonDb: [ environment: model::PersonConn ]
                ];
            }
            """;

    @Test
    void producesFullyTypedCompiledMapping() {
        var built = buildAndNormalize();
        var tc = new TypeChecker(built.modelCtx);
        var compiled = tc.check(built.mappingDef);

        // Output shape tracks input — no hardcoded strings.
        assertEquals(built.mappingDef.qualifiedName(), compiled.qualifiedName());
        assertEquals(1, compiled.mappedClasses().size(),
                "Mapping covers exactly one class (Person)");

        var mc = compiled.mappedClasses().get(0);
        assertEquals("model::Person", mc.classFqn());
        assertEquals(MappingKind.RELATIONAL, mc.kind());
        assertEquals("T_PERSON", mc.sourceName().orElseThrow(),
                "Relational mapping's sourceName must equal the root table name");

        // compileMapping wraps the exact AST ModelContext exposes — no reparse,
        // no clone. Identity proves the primitive hands through the same
        // ValueSpecification the query path would see.
        var contextSpec = built.modelCtx.findSourceSpec("model::Person").orElseThrow();
        assertSame(contextSpec, mc.sourceSpec().ast(),
                "compiled sourceSpec must wrap the same AST instance ModelContext exposes");

        // Type-stamping must cover more than just the root — a lazy walker
        // that stamped only the root would pass a simple containsKey(root)
        // check but fail this.
        assertTrue(mc.sourceSpec().types().size() > 1,
                "TypeChecker must stamp descendant sourceSpec nodes, not only the root");
    }

    @Test
    void queryAndBuildSharePrimitive() {
        var built = buildAndNormalize();
        var tc = new TypeChecker(built.modelCtx);

        // 1. Query path — GetAllChecker routes through env.compileSourceSpecFor,
        //    which stamps Person's sourceSpec subtree.
        var getAllPerson = new AppliedFunction(
                "getAll", List.of(new PackageableElementPtr("model::Person")), false);
        var queryCompiled = tc.check(getAllPerson);
        int typesAfterQuery = queryCompiled.types().size();
        assertTrue(typesAfterQuery > 1,
                "Query path must have stamped query AST + sourceSpec subtree");

        // 2. Build path — compileMapping fans out to the same primitive,
        //    which is memoized by classFqn. It must NOT restamp anything.
        var mappingCompiled = tc.check(built.mappingDef);
        var sharedTypes = mappingCompiled.mappedClasses().get(0).sourceSpec().types();

        assertSame(queryCompiled.types(), sharedTypes,
                "Query and build must share one identity-keyed types map instance");
        assertEquals(typesAfterQuery, sharedTypes.size(),
                "Build path must not restamp sourceSpec nodes the query already stamped — "
                        + "proves compileSourceSpecFor is memoized across triggers");
    }

    @Test
    void memoizesByFqn() {
        var built = buildAndNormalize();
        var tc = new TypeChecker(built.modelCtx);

        var first = tc.check(built.mappingDef);
        var second = tc.check(built.mappingDef);

        assertSame(first, second, "Repeat check(mappingDef) must return the cached instance");
    }

    @Test
    void coversM2MAndMultiClassMapping() {
        var built = buildAndNormalize(MIXED_MODEL, "model::MixedRT", "model::MixedMapping");
        var tc = new TypeChecker(built.modelCtx);
        var compiled = tc.check(built.mappingDef);

        // Multi-class: one CompiledMappedClass per ClassMappingDefinition, same order.
        assertEquals(2, compiled.mappedClasses().size(),
                "Mapping covers two classes (RawPerson + Person)");
        assertEquals(
                List.of("model::RawPerson", "model::Person"),
                compiled.mappedClasses().stream().map(mc -> mc.classFqn()).toList(),
                "mappedClasses must track ClassMappingDefinition input order");

        var raw = compiled.mappedClasses().get(0);
        var person = compiled.mappedClasses().get(1);

        // Relational branch: kind, sourceName = table, sourceSpec typed.
        assertEquals(MappingKind.RELATIONAL, raw.kind());
        assertEquals("T_RAW", raw.sourceName().orElseThrow());
        assertSame(
                built.modelCtx.findSourceSpec("model::RawPerson").orElseThrow(),
                raw.sourceSpec().ast(),
                "relational sourceSpec must be the ModelContext AST instance");

        // M2M branch: kind, sourceName = source class FQN, sourceSpec typed.
        assertEquals(MappingKind.M2M, person.kind());
        assertEquals("model::RawPerson", person.sourceName().orElseThrow(),
                "M2M sourceName must be the source class FQN, not a table name");
        assertSame(
                built.modelCtx.findSourceSpec("model::Person").orElseThrow(),
                person.sourceSpec().ast(),
                "M2M sourceSpec must be the ModelContext AST instance");
        assertTrue(person.sourceSpec().types().containsKey(person.sourceSpec().ast()),
                "M2M sourceSpec root must be stamped with TypeInfo");
    }

    // ---- fixture ----

    private Fixture buildAndNormalize() {
        return buildAndNormalize(MODEL, "model::PersonRT", "model::PersonMapping");
    }

    private Fixture buildAndNormalize(String source, String runtimeFqn, String mappingFqn) {
        var builder = new PureModelBuilder().addSource(source);
        var mappingNames = builder.resolveMappingNames(runtimeFqn);
        var normalizer = new com.gs.legend.compiler.MappingNormalizer(builder, mappingNames);
        var modelCtx = normalizer.modelContext();
        var mappingDef = builder.getMappingDefinition(mappingFqn);
        assertNotNull(mappingDef, "MappingDefinition must be retained on the builder: " + mappingFqn);
        return new Fixture(modelCtx, mappingDef);
    }

    private record Fixture(com.gs.legend.model.ModelContext modelCtx,
                           com.gs.legend.model.def.MappingDefinition mappingDef) {}

    /**
     * Mixed-kind mapping: a Relational source class (RawPerson) and an M2M
     * target class (Person) in the same Mapping definition. Exercises both
     * {@link MappingKind} branches plus multi-class ordering.
     */
    private static final String MIXED_MODEL = """
            Class model::RawPerson {
                givenName: String[1];
                surname: String[1];
            }

            Class model::Person {
                fullName: String[1];
            }

            Database model::RawDb (
                Table T_RAW (
                    GIVEN_NAME VARCHAR(100),
                    SURNAME VARCHAR(100)
                )
            )

            Mapping model::MixedMapping (
                *model::RawPerson: Relational
                {
                    ~mainTable [model::RawDb] T_RAW
                    givenName: [model::RawDb] T_RAW.GIVEN_NAME,
                    surname: [model::RawDb] T_RAW.SURNAME
                }

                *model::Person: Pure
                {
                    ~src model::RawPerson
                    fullName: $src.givenName + ' ' + $src.surname
                }
            )

            ###Connection
            RelationalDatabaseConnection model::RawConn
            {
                store: model::RawDb;
                type: DuckDB;
                specification: InMemory {};
                auth: NoAuth {};
            }

            ###Runtime
            Runtime model::MixedRT
            {
                mappings: [ model::MixedMapping ];
                connections:
                [
                    model::RawDb: [ environment: model::RawConn ]
                ];
            }
            """;
}
