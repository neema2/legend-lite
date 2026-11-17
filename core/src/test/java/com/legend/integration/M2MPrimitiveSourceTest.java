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
 * A Pure class mapping whose {@code ~src} is a primitive type (legend-studio's roundtrip case
 * DSL_Mapping-m2m-primitivetype-source): {@code ~src StrictDate}, {@code d: $src}. legend-engine 4.145.0 compiles it
 * ({@code ClassMappingFirstPassBuilder} resolves the source as a type, not a class) and serves it as a child of another
 * set's class-typed property ({@code simpleObject.pure} {@code testPrimitiveTypeMapping}, AlloyOnly: {@code
 * {"c":{"d":"2020-01-01"}}}); as the root of a query it asserts "Pure mapping does not have a class as ~src for root
 * of tree" ({@code graphExtension.pure} {@code calculateSourceTree}).
 *
 * <p>Lite refused to compile it: the synthesized {@code getAll(StrictDate)} typed its element as a CLASS named
 * {@code meta::pure::metamodel::type::StrictDate}, a second spelling of the primitive that no {@code StrictDate} slot
 * accepted ("expected StrictDate, got meta::pure::metamodel::type::StrictDate"). The child read is lite's M2M
 * class-typed graph child, which lite does not serve yet for any source (H5c; register S417).
 */
@DisplayName("M2M class mapping with a primitive ~src")
@org.junit.jupiter.api.extension.ExtendWith(LaneFloor.class)
class M2MPrimitiveSourceTest {

    /** The roundtrip case's model, verbatim. */
    private static final String STUDIO_CASE = """
            ###Pure
            Class model::Source
            {
              d: StrictDate[1];
            }

            Class model::Wrapper
            {
              d: StrictDate[1];
            }

            Class model::Dest
            {
              w: model::Wrapper[1];
            }


            ###Mapping
            Mapping model::ModelToModelMappingFromPrimitiveType
            (
              *model::Dest: Pure
              {
                ~src model::Source
                w[dateMapping]: $src.d
              }
              model::Wrapper[dateMapping]: Pure
              {
                ~src StrictDate
                d: $src
              }
            )
            """;

    private static final String RUNTIME = """
            ###Relational
            Database store::TestDb ( )
            ###Connection
            RelationalDatabaseConnection store::TestConnection { type: DuckDB; specification: DuckDB { }; auth: Test; }
            ###Runtime
            Runtime t::R
            {
              mappings: [ model::ModelToModelMappingFromPrimitiveType ];
              connections: [ store::TestDb: [ env: store::TestConnection ], ModelStore: [ c: #{ JsonModelConnection { class: model::Source; url: 'data:application/json,[{"d":"2020-01-01"}]'; } }# ] ];
            }
            """;

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
    @DisplayName("the primitive-source set as a query's root is refused with legend-engine's reason")
    void asTheRootItIsRefusedWithTheEnginesReason() {
        Throwable t = assertThrows(Throwable.class, () -> Lean.run(STUDIO_CASE + RUNTIME,
                "model::Wrapper.all()->graphFetch(#{model::Wrapper{d}}#)->serialize(#{model::Wrapper{d}}#)"
                        + "->from(model::ModelToModelMappingFromPrimitiveType, t::R)", null, connection));
        assertTrue(rootMessage(t).contains("Pure mapping does not have a class as ~src for root of tree"),
                rootMessage(t));
    }

    @Test
    @DisplayName("as a child it is refused by name: lite serves no M2M class-typed graph child yet (H5c)")
    void asAChildItIsRefusedByName() {
        Throwable t = assertThrows(Throwable.class, () -> Lean.run(STUDIO_CASE + RUNTIME,
                "model::Dest.all()->graphFetch(#{model::Dest{w{d}}}#)->serialize(#{model::Dest{w{d}}}#)"
                        + "->from(model::ModelToModelMappingFromPrimitiveType, t::R)", null, connection));
        assertTrue(rootMessage(t).contains("H5c"), rootMessage(t));
    }

    private static String rootMessage(Throwable t) {
        StringBuilder all = new StringBuilder();
        for (Throwable c = t; c != null; c = c.getCause() == c ? null : c.getCause()) {
            all.append(c.getMessage()).append(" | ");
        }
        return all.toString();
    }
}
