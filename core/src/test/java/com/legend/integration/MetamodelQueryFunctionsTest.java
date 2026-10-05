package com.legend.integration;

import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.test.StorelessRuntime;

import com.legend.Execution;
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

/**
 * Metamodel-as-relations, group F (2026-09-02): the mapping-metamodel QUERY
 * functions the typeInference corpus family composes are Pure bodies over
 * seeded rows — {@code _classMappingByClass} / {@code rootClassMappingByClass}
 * (the include closure's visit rank, {@code root}, {@code class} as the
 * Class row), {@code view} (Database and Schema rows),
 * {@code propertyMappingsByPropertyName} / {@code allPropertyMappings}
 * (property-mapping rows across the extends chain, Property rows),
 * {@code inferRelationalType} (the compiler's inferred type stamped on every
 * relational-operation node row — the include-closure precedent: the engine
 * recurses per query, ours reads the fact) and {@code dataTypeToSqlText}
 * (the real match over the DataType subclass rows). Every verdict is a
 * query the database answers; nothing here is test-specific Java.
 */
@DisplayName("Metamodel store: mapping-metamodel query functions as Pure bodies over rows")
class MetamodelQueryFunctionsTest {

    private static final String MODEL = """
            Class ext::A { id: Integer[1]; aName: String[1]; }
            Class ext::B extends ext::A { bName: String[1]; amount: Float[1]; flag: Boolean[0..1]; }
            Class ext::Org { name: String[1]; }
            Association ext::A_Org { org: ext::Org[0..1]; members: ext::A[*]; }
            ###Relational
            Database ext::testDatabase
            (
                Table ABC (id INTEGER PRIMARY KEY, aName VARCHAR(20), bName VARCHAR(20), amount DOUBLE, qty DECIMAL(4, 1), n INTEGER, orgId INTEGER, ts DATE)
                Table ORG (id INTEGER PRIMARY KEY, name VARCHAR(50))
                Schema s2 ( Table T2 (id INTEGER PRIMARY KEY, code CHAR(3)) )
                View AV ( id : ABC.id PRIMARY KEY, aName : ABC.aName, orgName : @ABC_ORG | ORG.name )
                View AGG ( ~groupBy(ABC.id) id : ABC.id PRIMARY KEY, maxTs : max(ABC.ts), total : sum(ABC.amount) )
                View AVV ( id : AV.id PRIMARY KEY, aName : AV.aName )
                Join ABC_ORG(ABC.orgId = ORG.id)
            )
            ###Mapping
            Mapping ext::AMapping
            (
                ext::A[a] : Relational { id : [ext::testDatabase]ABC.id, aName : [ext::testDatabase]ABC.aName, org : [ext::testDatabase]@ABC_ORG }
                ext::Org[o] : Relational { name : [ext::testDatabase]ORG.name }
            )
            Mapping ext::B2Mapping
            (
                include ext::AMapping
                ext::B[b2] extends [a] : Relational
                {
                    aName : concat('bName_', [ext::testDatabase]ABC.aName),
                    bName : [ext::testDatabase]ABC.bName,
                    amount : plus([ext::testDatabase]ABC.qty, [ext::testDatabase]ABC.n),
                    flag : case(isNull([ext::testDatabase]ABC.n), sqlNull(), case(equal([ext::testDatabase]ABC.n, 1), sqlTrue(), sqlFalse()))
                }
            )
            Mapping ext::TwoSets
            (
                *ext::A[a1] : Relational { id : [ext::testDatabase]ABC.id }
                ext::A[a2] : Relational { id : [ext::testDatabase]ABC.bName }
            )
            """;

    private static final String ROOT_SET =
            "meta::relational::mapping::RootRelationalInstanceSetImplementation";
    private static final String RPM = "meta::relational::mapping::RelationalPropertyMapping";
    private static final String ROOT_BY_CLASS = "meta::pure::mapping::rootClassMappingByClass";
    private static final String BY_CLASS = "meta::pure::mapping::_classMappingByClass";
    private static final String BY_PROP = "meta::pure::mapping::propertyMappingsByPropertyName";
    private static final String INFER = "meta::relational::functions::typeInference::inferRelationalType";
    private static final String SQL_TEXT = "meta::relational::metamodel::datatype::dataTypeToSqlText";
    private static final String VIEW = "meta::relational::metamodel::view";

    private Connection connection;

    @BeforeEach
    void setUp() throws SQLException {
        connection = DriverManager.getConnection("jdbc:duckdb:");
    }

    @AfterEach
    void tearDown() throws SQLException {
        connection.close();
    }

    private List<Object> values(String query) throws SQLException {
        ExecutionResult r = Execution.execute(StorelessRuntime.with(MODEL, DatabaseType.DuckDB), query, StorelessRuntime.RUNTIME, connection);
        if (r instanceof ExecutionResult.Collection c) {
            return c.values();
        }
        Object v = ((ExecutionResult.Scalar) r).value();
        return v == null ? List.of() : List.of(v);
    }

    /** The corpus's view shape: {@code db.schemas->map(x|$x->view('V'))->toOne()
     * .columnMappings->filter(columnName).relationalOperationElement->toOne()
     * ->inferRelationalType()->toOne()->dataTypeToSqlText()}. */
    private String viewColumnType(String view, String column) {
        return "ext::testDatabase.schemas->map(x|$x->" + VIEW + "('" + view + "'))->toOne()"
                + ".columnMappings->filter(x|$x.columnName == '" + column + "')"
                + ".relationalOperationElement->toOne()->" + INFER + "()->toOne()->" + SQL_TEXT + "()";
    }

    /** The corpus's mapping shape: {@code M->rootClassMappingByClass(C)->toOne()
     * ->cast(@Root)->propertyMappingsByPropertyName('p')->cast(@RPM)
     * .relationalOperationElement->toOne()->inferRelationalType()->toOne()->dataTypeToSqlText()}. */
    private String propertyType(String mapping, String cls, String prop) {
        return mapping + "->" + ROOT_BY_CLASS + "(" + cls + ")->toOne()->cast(@" + ROOT_SET + ")"
                + "->" + BY_PROP + "('" + prop + "')->cast(@" + RPM + ").relationalOperationElement"
                + "->toOne()->" + INFER + "()->toOne()->" + SQL_TEXT + "()";
    }

    @Test
    @DisplayName("Database.schemas / Schema.views / view(): the store's schemas and views as rows")
    void schemasAndViews() throws SQLException {
        List<Object> schemas = values("ext::testDatabase.schemas.name");
        assertEquals(List.of("default", "s2"),
                schemas.stream().map(String::valueOf).sorted().toList());
        assertEquals(List.of("AV"), values(
                "ext::testDatabase.schemas->map(x|$x->" + VIEW + "('AV')).name"));
        assertEquals(List.of(), values(
                "ext::testDatabase.schemas->map(x|$x->" + VIEW + "('nope')).name"));
    }

    @Test
    @DisplayName("a view column's inferred type: a table column, a join-navigated column, aggregates, a view on a view")
    void viewColumnInference() throws SQLException {
        assertEquals(List.of("VARCHAR(20)"), values(viewColumnType("AV", "aName")));
        assertEquals(List.of("VARCHAR(50)"), values(viewColumnType("AV", "orgName")),
                "the join's terminal column types the element");
        assertEquals(List.of("DATE"), values(viewColumnType("AGG", "maxTs")));
        assertEquals(List.of("DOUBLE"), values(viewColumnType("AGG", "total")));
        assertEquals(List.of("VARCHAR(20)"), values(viewColumnType("AVV", "aName")),
                "view on view: through the underlying view's column expression");
    }

    @Test
    @DisplayName("rootClassMappingByClass / _classMappingByClass: root and class read off the set rows, the include closure in visit order")
    void classMappingsByClass() throws SQLException {
        assertEquals(List.of("a"), values("ext::B2Mapping->" + ROOT_BY_CLASS + "(ext::A).id"),
                "A's root set is visible through the include");
        assertEquals(List.of("b2"), values("ext::B2Mapping->" + ROOT_BY_CLASS + "(ext::B).id"));
        assertEquals(List.of(), values("ext::AMapping->" + ROOT_BY_CLASS + "(ext::B).id"),
                "AMapping does not map B");
        List<Object> two = values("ext::TwoSets->" + BY_CLASS + "(ext::A).id");
        assertEquals(List.of("a1", "a2"), two.stream().map(String::valueOf).sorted().toList());
        assertEquals(List.of("a1"), values("ext::TwoSets->" + ROOT_BY_CLASS + "(ext::A).id"),
                "the '*' set is the root when a class has several sets");
        assertEquals(List.of(true), values("ext::AMapping->" + ROOT_BY_CLASS + "(ext::A).root"),
                "a class's sole set is implicitly root (MappingValidator.validateStar)");
        // the corpus's testSubTypeMappingValidWhenMappedExplicitly shape:
        // the root-filtered candidates counted
        assertEquals(List.of(1L), values("ext::TwoSets->" + BY_CLASS
                + "(ext::A)->filter(s|$s.root == true)->size()"));
        assertEquals(List.of(1L), values("ext::B2Mapping->" + BY_CLASS
                + "(ext::A)->filter(s|$s.root == true)->size()"),
                "through an include");
        assertEquals(List.of("b2"), values("ext::B2Mapping->" + BY_CLASS
                + "(ext::B)->filter(s|$s.root == true).id"));
    }

    @Test
    @DisplayName("propertyMappingsByPropertyName over the effective property mappings: own, inherited, absent")
    void propertyMappingsByName() throws SQLException {
        String b2 = "ext::B2Mapping->" + ROOT_BY_CLASS + "(ext::B)->toOne()->cast(@" + ROOT_SET + ")";
        assertEquals(List.of("bName"), values(b2 + "->" + BY_PROP + "('bName').property.name"));
        assertEquals(List.of("id"), values(b2 + "->" + BY_PROP + "('id').property.name"),
                "inherited from the extended set 'a'");
        assertEquals(List.of(), values(b2 + "->" + BY_PROP + "('nope').property.name"));
        assertEquals(List.of("org"), values(b2 + "->" + BY_PROP + "('org').property.name"),
                "an association end is a Property row owned by the association");
    }

    @Test
    @DisplayName("a mapped property's inferred type: concat sums sizes, plus widens to the decimal, case takes the safe type")
    void propertyMappingInference() throws SQLException {
        assertEquals(List.of("VARCHAR(26)"), values(propertyType("ext::B2Mapping", "ext::B", "aName")));
        assertEquals(List.of("VARCHAR(20)"), values(propertyType("ext::B2Mapping", "ext::B", "bName")));
        assertEquals(List.of("DECIMAL(4, 1)"), values(propertyType("ext::B2Mapping", "ext::B", "amount")));
        assertEquals(List.of("BIT"), values(propertyType("ext::B2Mapping", "ext::B", "flag")),
                "sqlNull is OTHER and absorbs into the BIT branches");
        assertEquals(List.of("INT"), values(propertyType("ext::B2Mapping", "ext::B", "id")),
                "an inherited column mapping types through the extends chain");
    }

    @Test
    @DisplayName("a CONSTRUCTED relational-op instance is rows: ^DynaFunction(...) types through the same store")
    void constructedInstances() throws SQLException {
        String dyna = "meta::relational::metamodel::DynaFunction";
        String lit = "meta::relational::metamodel::Literal";
        assertEquals(List.of("OTHER"), values("^" + dyna + "(name = 'sqlNull')->" + INFER
                + "()->toOne()->" + SQL_TEXT + "()"));
        assertEquals(List.of("VARCHAR(3)"), values("^" + dyna + "(name = 'case', parameters = [^"
                + lit + "(value = true), ^" + lit + "(value = 'str'), ^" + dyna + "(name = 'sqlNull')])->"
                + INFER + "()->toOne()->" + SQL_TEXT + "()"));
        assertEquals(List.of("BIT"), values("^" + dyna + "(name = 'not', parameters = [^" + dyna
                + "(name = 'equal', parameters = [^" + lit + "(value = 1), ^" + lit + "(value = 2)])])->"
                + INFER + "()->toOne()->" + SQL_TEXT + "()"));
        // a row-valued argument under an argument-free rule (the corpus's
        // joinStrings shape): the constant part seeds, the type is exact
        assertEquals(List.of("VARCHAR(4000)"), values("^" + dyna + "(name = 'joinStrings', parameters = ["
                + "ext::B2Mapping->" + ROOT_BY_CLASS + "(ext::B)->cast(@" + ROOT_SET + ")->map(x|$x->"
                + BY_PROP + "('bName'))->cast(@" + RPM + ").relationalOperationElement->toOne(), ^"
                + lit + "(value = ',')])->" + INFER + "()->toOne()->" + SQL_TEXT + "()"));
    }

    @Test
    @DisplayName("Column.type and the DataType rows: dataTypeToSqlText is the real match over the subclass rows")
    void columnTypes() throws SQLException {
        assertEquals(List.of("CHAR(3)"), values(
                "meta::relational::metamodel::Column.all()->filter(c|$c.name == 'code').type->map(t|$t->" + SQL_TEXT + "())"));
        assertEquals(List.of("DECIMAL(4, 1)"), values(
                "meta::relational::metamodel::Column.all()->filter(c|$c.name == 'qty').type->map(t|$t->" + SQL_TEXT + "())"));
    }
}
