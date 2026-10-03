package com.legend.integration;

import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.test.StorelessRuntime;

import com.legend.Compiler;
import com.legend.exec.ExecutionResult;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Metamodel-as-relations, prototype 1 (step 3, 2026-09-02): the MAPPING
 * metamodel lives in the system store as rows &mdash; {@code mappings},
 * {@code class_mappings} (the compiler's extends-resolved main table),
 * the include closure as a row entity, {@code tables} &mdash; and the
 * engine's navigation FUNCTIONS ({@code classMappingById},
 * {@code mainTable}, under their REAL names — the natives and the legacy
 * walk's arms are gone) are Pure bodies over those rows, inlined and
 * lowered through the ONE router. Nothing here is test-specific Java: every
 * verdict is a query the database answers.
 *
 * <p>Pinned mechanisms: the inheritance Operation over a metaclass
 * hierarchy (SetImplementation &rarr; RootRelationalInstanceSetImplementation);
 * D3 &mdash; an element REFERENCE ({@code ext::B1Mapping}) is its row by
 * key; {@code elementToPath} on rows and references; association ends
 * declared on a superclass mapped on a subclass's set.
 *
 * <p>The witness {@code testMainTableForB1} itself lands in
 * {@link #witnessMainTableForB1} (the navigation-depth leg, 2026-09-02).
 */
@DisplayName("Metamodel store: the mapping metamodel as relations")
class MetamodelMappingStoreTest {

    private static final String MODEL = """
            Class ext::A { id: Integer[1]; aName: String[1]; }
            Class ext::B extends ext::A { bName: String[1]; }
            Class ext::C extends ext::B { cName: String[1]; }
            ###Relational
            Database ext::testDatabase ( Table ABC (id INTEGER PRIMARY KEY, aName VARCHAR(20), bName VARCHAR(20), cName VARCHAR(20)) View AV ( id : ABC.id PRIMARY KEY, aName : ABC.aName ) )
            ###Mapping
            Mapping ext::AMapping ( ext::A[a] : Relational { id : [ext::testDatabase]ABC.id, aName : [ext::testDatabase]ABC.aName } )
            Mapping ext::B1Mapping ( include ext::AMapping  ext::B[b1] extends [a] : Relational { } )
            Mapping ext::B2Mapping ( include ext::AMapping  ext::B[b2] extends [a] : Relational { aName : concat('bName_', [ext::testDatabase]ABC.aName), bName : [ext::testDatabase]ABC.bName } )
            Mapping ext::C1Mapping ( include ext::B1Mapping  ext::C[c1] extends [b1] : Relational { } )
            Mapping ext::AGroupBy ( ext::A[a] : Relational { ~groupBy([ext::testDatabase]ABC.aName) id : [ext::testDatabase]ABC.id, aName : [ext::testDatabase]ABC.aName } )
            Mapping ext::BDistinctAGroupBy ( include ext::AGroupBy  ext::B[b] extends [a] : Relational { ~distinct } )
            Mapping ext::BNothingANothing ( include ext::AMapping  ext::B[b] extends [a] : Relational { } )
            Mapping ext::BUserPkANothing ( include ext::AMapping  ext::B[b] extends [a] : Relational { ~primaryKey([ext::testDatabase]ABC.bName) bName : [ext::testDatabase]ABC.bName } )
            Mapping ext::AMappingWithGroupBy ( ext::A[a] : Relational { ~groupBy([ext::testDatabase]ABC.aName) id : [ext::testDatabase]ABC.id, aName : [ext::testDatabase]ABC.aName } )
            Mapping ext::AMappingWithUserDefinedPrimaryKey ( ext::A[a] : Relational { ~primaryKey([ext::testDatabase]ABC.aName) id : [ext::testDatabase]ABC.id, aName : [ext::testDatabase]ABC.aName } )
            Mapping ext::BMappingWithDistinctAMappingWithGroupBy ( include AMappingWithGroupBy  ext::B[b] extends [a] : Relational { ~distinct bName : [ext::testDatabase]ABC.bName } )
            Mapping ext::BMappingWithUserDefinedPrimaryKeyAMappingWithGroupBy ( include AMappingWithGroupBy  ext::B[b] extends [a] : Relational { ~primaryKey([ext::testDatabase]ABC.bName) bName : [ext::testDatabase]ABC.bName } )
            Mapping ext::BMappingWithNothingAMappingWithGroupBy ( include AMappingWithGroupBy  ext::B[b] extends [a] : Relational { bName : [ext::testDatabase]ABC.bName } )
            Mapping ext::BMappingWithNothingAMappingWithUserDefinedPrimaryKey ( include AMappingWithUserDefinedPrimaryKey  ext::B[b] extends [a] : Relational { bName : [ext::testDatabase]ABC.bName } )
            Mapping ext::AOnView ( ext::A[a] : Relational { ~mainTable [ext::testDatabase]AV id : [ext::testDatabase]AV.id, aName : [ext::testDatabase]AV.aName } )
            Mapping ext::AMappingWithDistinct ( ext::A[a] : Relational { ~distinct id : [ext::testDatabase]ABC.id, aName : [ext::testDatabase]ABC.aName } )
            Mapping ext::BMappingWithNothingAMappingWithDistinct ( include AMappingWithDistinct  ext::B[b] extends [a] : Relational { bName : [ext::testDatabase]ABC.bName } )
            """;

    private static final String ROOT_SET =
            "meta::relational::mapping::RootRelationalInstanceSetImplementation";
    private static final String BY_ID = "meta::pure::mapping::classMappingById";
    private static final String MAIN_TABLE = "meta::relational::metamodel::mainTable";

    private Connection connection;

    @BeforeEach
    void setUp() throws SQLException {
        connection = DriverManager.getConnection("jdbc:duckdb:");
    }

    @AfterEach
    void tearDown() throws SQLException {
        connection.close();
    }

    private ExecutionResult run(String query) throws SQLException {
        return Compiler.execute(StorelessRuntime.with(MODEL, DatabaseType.DuckDB), query, StorelessRuntime.RUNTIME, connection);
    }

    private List<Object> values(String query) throws SQLException {
        ExecutionResult r = run(query);
        if (r instanceof ExecutionResult.Collection c) {
            return c.values();
        }
        Object v = ((ExecutionResult.Scalar) r).value();
        return v == null ? List.of() : List.of(v);
    }

    @Test
    @DisplayName("the mapping extent is seeded: Mapping.all() names every mapping")
    void mappingExtentIsSeeded() throws SQLException {
        List<Object> names = values("meta::pure::mapping::Mapping.all().name");
        assertTrue(names.containsAll(List.of("AMapping", "B1Mapping", "B2Mapping",
                "C1Mapping", "MetamodelMapping")), names.toString());
    }

    @Test
    @DisplayName("SetImplementation.all() reads through the inheritance Operation over the metaclass hierarchy")
    void setImplementationExtentThroughInheritanceOp() throws SQLException {
        List<Object> ids = values("meta::pure::mapping::SetImplementation.all().id");
        assertTrue(ids.containsAll(List.of("a", "b1", "b2", "c1")), ids.toString());
    }

    @Test
    @DisplayName("Table.all() names every store table, the system store's included")
    void tableExtentIsSeeded() throws SQLException {
        List<Object> names = values("meta::relational::metamodel::relation::Table.all().name");
        assertTrue(names.containsAll(List.of("ABC", "classes", "mappings",
                "class_mappings", "relational_elements")), names.toString());
    }

    @Test
    @DisplayName("classMappingById is a Pure body over the rows: own set, and an INCLUDED mapping's set")
    void classMappingByIdIsAQuery() throws SQLException {
        assertEquals(List.of("b1"),
                values("ext::B1Mapping->" + BY_ID + "('b1').id"));
        // 'a' is declared in AMapping and visible from B1Mapping through
        // the include closure — its parent is the DECLARING mapping
        assertEquals(List.of("AMapping"),
                values("ext::B1Mapping->" + BY_ID + "('a').parent.name"));
        assertEquals(List.of(),
                values("ext::AMapping->" + BY_ID + "('b1').id"),
                "AMapping does not include B1Mapping: b1 is not visible");
    }

    @Test
    @DisplayName("D3: an element reference is its row — navigations off ext::B1Mapping are store reads")
    void elementReferenceIsItsRow() throws SQLException {
        List<Object> visible = values(
                "ext::B1Mapping->map(m|$m.visibility.visible.name)");
        assertEquals(List.of("AMapping", "B1Mapping"),
                visible.stream().map(String::valueOf).sorted().toList());
        assertEquals(List.of("ext::B1Mapping"),
                values("ext::B1Mapping->map(m|$m->elementToPath())"));
    }

    @Test
    @DisplayName("mainTable's navigation: the set's alias, its table — the extends chain resolved by the compiler")
    void mainTableNavigation() throws SQLException {
        assertEquals(List.of("ABC"), values(ROOT_SET
                + ".all()->filter(s|$s.id == 'b1').mainTableAlias.name"));
        assertEquals(List.of("ABC"), values(ROOT_SET
                + ".all()->filter(s|$s.id == 'c1').mainTableAlias.relationalElement"
                + "->cast(@meta::relational::metamodel::relation::Table).name"),
                "c1 extends b1 extends a: the main table is a's");
    }

    @Test
    @DisplayName("mainTable() as a Pure body over a set row")
    void mainTableFunctionOverASetRow() throws SQLException {
        assertEquals(List.of("ABC"), values(ROOT_SET
                + ".all()->filter(s|$s.id == 'b1')->map(x|$x->" + MAIN_TABLE
                + "()).name"));
    }

    @Test
    @DisplayName("THE WITNESS testMainTableForB1: two mappings' main tables compared as rows in the database")
    void witnessMainTableForB1() throws SQLException {
        String witness = "{| let mainTable = ext::B1Mapping->" + BY_ID
                + "('b1')->cast(@" + ROOT_SET + ")->map(x|$x->" + MAIN_TABLE + "());"
                + " let superMappingMainTable = ext::AMapping->" + BY_ID
                + "('a')->cast(@" + ROOT_SET + ")->map(x|$x->" + MAIN_TABLE + "());"
                + " assertEquals($superMappingMainTable, $mainTable); }";
        var r = run(witness);
        assertEquals(Boolean.TRUE, ((ExecutionResult.Scalar) r).value());
        // the main-table instances themselves, as rows
        assertEquals(List.of("ABC"), values("ext::C1Mapping->" + BY_ID
                + "('c1')->cast(@" + ROOT_SET + ")->map(x|$x->" + MAIN_TABLE + "()).name"));
    }

    private static final String PMI = "meta::pure::mapping::PropertyMappingsImplementation";

    @Test
    @DisplayName("superMapping / allSuperSetImplementations read the extends chain as ancestry rows")
    void extendsChainAsRows() throws SQLException {
        assertEquals(List.of("b1"), values("ext::C1Mapping->" + BY_ID
                + "('c1')->cast(@" + PMI + ")->toOne()->meta::pure::mapping::superMapping().id"));
        List<Object> supers = values("meta::pure::mapping::allSuperSetImplementations("
                + "ext::C1Mapping->" + BY_ID + "('c1')->toOne()->cast(@" + PMI + "), ext::C1Mapping).id");
        assertEquals(List.of("a", "b1"),
                supers.stream().map(String::valueOf).sorted().toList());
        assertEquals(List.of(), values("ext::AMapping->" + BY_ID
                + "('a')->cast(@" + PMI + ")->toOne()->meta::pure::mapping::superMapping().id"),
                "a root set has no super");
    }

    @Test
    @DisplayName("resolvePrimaryKey: the engine's this-vs-super precedence over the compiled keys, as rows")
    void resolvePrimaryKeyPrecedence() throws SQLException {
        String tac = "meta::relational::metamodel::TableAliasColumn";
        String pk = "->cast(@" + ROOT_SET + ")->toOne()->meta::relational::mapping::resolvePrimaryKey()"
                + "->cast(@" + tac + ").column.name";
        assertEquals(List.of("aName"),
                values("ext::BDistinctAGroupBy->" + BY_ID + "('b')" + pk),
                "the super's ~groupBy beats this set's ~distinct");
        assertEquals(List.of("bName"),
                values("ext::BUserPkANothing->" + BY_ID + "('b')" + pk),
                "this set's ~primaryKey");
        assertEquals(List.of("id"),
                values("ext::BNothingANothing->" + BY_ID + "('b')" + pk),
                "nothing anywhere: the main table's PRIMARY KEY");
        assertEquals(List.of("aName"),
                values("ext::AGroupBy->" + BY_ID + "('a')" + pk),
                "a root set: its own compiled key (~groupBy columns)");
        // the engine's testPrimaryKeyForB shapes, bare include paths
        assertEquals(List.of("aName"),
                values("ext::BMappingWithDistinctAMappingWithGroupBy->" + BY_ID + "('b')" + pk));
        assertEquals(List.of("aName"),
                values("ext::BMappingWithUserDefinedPrimaryKeyAMappingWithGroupBy->" + BY_ID + "('b')" + pk));
        assertEquals(List.of("aName"),
                values("ext::BMappingWithNothingAMappingWithGroupBy->" + BY_ID + "('b')" + pk));
        assertEquals(List.of("aName"),
                values("ext::BMappingWithNothingAMappingWithUserDefinedPrimaryKey->" + BY_ID + "('b')" + pk));
        List<Object> superDistinct = values(
                "ext::BMappingWithNothingAMappingWithDistinct->" + BY_ID + "('b')" + pk);
        assertEquals(List.of("aName", "id"),
                superDistinct.stream().map(String::valueOf).sorted().toList(),
                "the super's ~distinct key is EVERY column it maps");
    }

    @Test
    @DisplayName("a VIEW as main table: the alias's element is the View row; mainTable() is the view's base TABLE (the real match body)")
    void viewAsMainTable() throws SQLException {
        assertEquals(List.of("AV"), values("ext::AOnView->" + BY_ID
                + "('a')->cast(@" + ROOT_SET + ").mainTableAlias.relationalElement"
                + "->cast(@meta::relational::metamodel::relation::View).name"));
        assertEquals(List.of("ABC"), values("ext::AOnView->" + BY_ID
                + "('a')->cast(@" + ROOT_SET + ")->map(x|$x->" + MAIN_TABLE + "()).name"));
        assertEquals(List.of("ABC"), values("ext::AMapping->" + BY_ID
                + "('a')->cast(@" + ROOT_SET + ")->map(x|$x->" + MAIN_TABLE + "()).name"),
                "a table-backed set still reads its table");
    }

    @Test
    @DisplayName("a FAILING assert over instance rows renders the spec's <id instanceOf Type> form")
    void instanceFailureMessageRenders() {
        var ex = assertThrows(com.legend.error.AssertFailed.class, () -> run(
                "{| assertEquals(ext::AMapping->" + BY_ID + "('a'), ext::B1Mapping->"
                        + BY_ID + "('b1')); }"));
        assertTrue(ex.getMessage().contains(" instanceOf meta::pure::mapping::SetImplementation>"),
                ex.getMessage());
        assertTrue(ex.getMessage().contains("expected: <a|"), ex.getMessage());
    }

    @Test
    @DisplayName("a user class with no execution context keeps the loud wall (D1 boundary)")
    void userClassWithoutContextStillWalls() {
        var ex = assertThrows(com.legend.error.MappingResolutionException.class,
                () -> Compiler.execute(MODEL, "ext::A.all()->size()", connection));
        assertEquals(Compiler.NO_RUNTIME, ex.getMessage());
    }
}
