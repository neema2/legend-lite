package com.legend.integration;

import com.legend.testing.LaneFloor;
import com.legend.testing.Lean;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * An M2M explosion, {@code legalName*: $src.firstName} (legend-studio's roundtrip case DSL_Mapping-xstore-mapping).
 * legend-engine 4.145.0 compiles it, checking the transform's multiplicity against {@code [*]} rather than the
 * property's ({@code ClassMappingThirdPassBuilder}), and runs it as a fan-out: per source, each explode property's
 * transform is a list, the lists must have one size ("Explode properties do not have the same size..."), and one
 * target is made per index ({@code graphFetchInMemory.pure} {@code propertyExplosionCodes}; the interpreted
 * {@code inMemory.pure} {@code transformWithMapping} zips the same lists). A transform that is exactly one value makes
 * exactly one target per source, which is the plain mapping: lite composes it so. Any other transform compiles, as on
 * the engine, and is refused when a query reads the class (register S418): lite composes no fan-out.
 */
@DisplayName("M2M explode property")
@org.junit.jupiter.api.extension.ExtendWith(LaneFloor.class)
class M2MExplodeTest {

    /** The roundtrip case's model, verbatim. */
    private static final String STUDIO_CASE = """
            ###Pure
            Class model::MapPerson
            {
              firstName: String[1];
            }

            Class model::MapFirm
            {
              firstName: String[1];
            }

            Class model::Person
            {
              firstName: String[1];
              lastName: String[1];
            }

            Class model::Firm
            {
              legalName: String[1];
            }

            Association model::FirmPerson
            {
              firm: model::Firm[1];
              employee: model::Person[1];
            }


            ###Mapping
            Mapping model::NewMapping
            (
              *model::Firm[firm]: Pure
              {
                ~src model::MapPerson
                legalName*: $src.firstName
              }
              *model::Person[person]: Pure
              {
                ~src model::MapFirm
                firstName: $src.firstName
              }

              model::FirmPerson: XStore
              {
                firm[person, firm]: $that.legalName == $this.firstName
              }
            )
            """;

    /** {@code transform}: the explode property's transform; the source's {@code one} is [1], {@code opt} [0..1],
     *  {@code many} [*]. */
    private static String model(String transform) {
        return """
                ###Pure
                Class t::Src { one: String[1]; opt: String[0..1]; many: String[*]; }
                Class t::Firm { legalName: String[1]; }
                ###Mapping
                Mapping t::M
                (
                  *t::Firm: Pure { ~src t::Src legalName*: %s }
                )
                ###Relational
                Database store::TestDb ( )
                ###Connection
                RelationalDatabaseConnection store::TestConnection { type: DuckDB; specification: DuckDB { }; auth: Test; }
                ###Runtime
                Runtime t::R
                {
                  mappings: [ t::M ];
                  connections: [ store::TestDb: [ env: store::TestConnection ], ModelStore: [ c: #{ JsonModelConnection { class: t::Src; url: 'data:application/json,[{"one":"a","opt":"x","many":["m","n"]},{"one":"b"}]'; } }# ] ];
                }
                """.formatted(transform);
    }

    private static final String FETCH =
            "t::Firm.all()->graphFetch(#{t::Firm{legalName}}#)->serialize(#{t::Firm{legalName}}#)->from(t::M, t::R)";

    private Connection connection;

    @BeforeEach
    void setUp() throws SQLException {
        connection = LaneFloor.backend().open();
    }

    @AfterEach
    void tearDown() throws SQLException {
        if (connection != null) {
            connection.close();
        }
    }

    @Test
    @DisplayName("the roundtrip case compiles, as on legend-engine")
    void theStudioCaseCompiles() {
        assertEquals(List.of(), com.legend.Compiler.compileErrors(STUDIO_CASE));
    }

    @Test
    @DisplayName("a transform of exactly one value makes one target per source: legend-engine's answer")
    void aToOneTransformMakesOneTargetPerSource() {
        assertEquals("[{\"legalName\":\"a\"},{\"legalName\":\"b\"}]",
                Lean.run(model("$src.one"), FETCH, null, connection).json());
    }

    @Test
    @DisplayName("a to-many or optional transform compiles, as on legend-engine, and is refused by name when read")
    void aFanOutCompilesAndIsRefusedWhenRead() {
        for (String transform : List.of("$src.many", "$src.opt", "$src.one->toOne()")) {
            assertEquals(List.of(), com.legend.Compiler.compileErrors(model(transform)), transform);
            Throwable t = assertThrows(Throwable.class, () -> Lean.run(model(transform), FETCH, null, connection));
            assertTrue(rootMessage(t).contains("explode property 'legalName*'"), transform + ": " + rootMessage(t));
        }
    }

    @Test
    @DisplayName("a fan-out's element is still typed against the property, as legend-engine's third pass types it")
    void aFanOutsElementIsTyped() {
        // the engine checks the transform's type for an explode property too (its multiplicity against [*]):
        // Integer into String is its "not in the class hierarchy" error; lite's constructor raises its own
        List<String> errors = com.legend.Compiler.compileErrors("""
                Class t::Src { n: Integer[*]; }
                Class t::Firm { legalName: String[1]; }
                ###Mapping
                Mapping t::M ( *t::Firm: Pure { ~src t::Src legalName*: $src.n } )
                """);
        assertEquals(1, errors.size(), errors.toString());
        assertTrue(errors.get(0).contains("legalName"), errors.toString());
    }

    @Test
    @DisplayName("a union whose fan-out branch is withheld is refused, never its other branch's rows alone")
    void aUnionWithAFanOutBranchIsRefused() {
        // the fan-out branch's binding is withheld, so one member survives; the class is the union, never that member
        // (ClassSources.findBinding, the independent review, 2026-11-16). The refusal precedes any read, so the table
        // needs no rows (until the fix, the query ran the relational branch alone)
        String model = """
                ###Pure
                Class u::C { name: String[1]; }
                Class u::S { names: String[*]; }
                ###Relational
                Database u::db ( Table T (ID INTEGER PRIMARY KEY, NAME VARCHAR(20)) )
                ###Mapping
                Mapping u::m
                (
                  *u::C: Operation
                  {
                    meta::pure::router::operations::union_OperationSetImplementation_1__SetImplementation_MANY_(r, p)
                  }
                  u::C[r]: Relational { ~mainTable [u::db]T name: [u::db]T.NAME }
                  u::C[p]: Pure { ~src u::S name*: $src.names }
                )
                ###Connection
                RelationalDatabaseConnection u::conn { store: u::db; type: DuckDB; specification: DuckDB { }; auth: Test; }
                ###Runtime
                Runtime u::R { mappings: [ u::m ]; connections: [ u::db: [ c: u::conn ] ]; }
                """;
        assertEquals(List.of(), com.legend.Compiler.compileErrors(model));
        Throwable t = assertThrows(Throwable.class, () -> Lean.run(model,
                "u::C.all()->project([x|$x.name], ['name'])->from(u::m, u::R)", null, connection));
        assertTrue(rootMessage(t).contains("explode property 'name*'"), rootMessage(t));
    }

    private static String rootMessage(Throwable t) {
        StringBuilder all = new StringBuilder();
        for (Throwable c = t; c != null; c = c.getCause() == c ? null : c.getCause()) {
            all.append(c.getMessage()).append(" | ");
        }
        return all.toString();
    }
}
