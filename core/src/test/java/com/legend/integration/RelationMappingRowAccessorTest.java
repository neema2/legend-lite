package com.legend.integration;

import com.legend.model.ClassMapping;
import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.Compiler;
import com.legend.protocol.spec.AppliedFunction;
import com.legend.protocol.spec.AppliedProperty;
import com.legend.protocol.spec.LambdaFunction;
import com.legend.protocol.spec.ValueSpecification;
import com.legend.protocol.spec.Variable;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A Relation class mapping's row accessor {@code $row}: legend-engine 4.151.0 (#5338, "Rename relation-mapping row
 * accessor from $src to $row") binds the row a property's expression reads as {@code row}
 * ({@code ClassMappingSecondPassBuilder.compileRelationPropertyLambda}); the pinned 4.145.0 binds it as {@code src}.
 * Lite binds both, built ahead of the pin on the user's ruling of 2026-11-16 ("Build ahead, ledgered";
 * docs/SEMANTICS_REGISTER.md S420). Until then a {@code $row} binding failed at compile, "unbound variable '$row'"
 * (the Studio roundtrip's STO_Relational-relation-function-mapping-with-binding, -with-valueFn and -advanced).
 *
 * <p>Every right-hand side form the Studio cases spell over {@code $row}: an expression, a quoted column, an
 * {@code if}, an EnumerationMapping and a Binding transformer over {@code $row.COL}, a line in an embedded block, and a
 * ModelJoin's condition through an expression binding (the association read, {@code RelationReads}). A lambda that
 * declares a parameter named {@code row} reads its own parameter ({@link #aLambdasOwnRowShadowsTheAccessor}: checked
 * on the substitution itself, since a {@code map} inside a property's expression is refused at planning whatever its
 * spelling, "row-read rewrite hit TypedMap", lite's gap with {@code $src} too). Every expected row is what the
 * mapping says the engine computes (each property's expression over the row); run on DuckDB, H2 and SQLite.
 */
@DisplayName("a Relation mapping's $row accessor")
class RelationMappingRowAccessorTest {

    private static String model(DatabaseType db) {
        return """
                ###Relational
                Database x::DB
                (
                  Table PERSON ( ID INTEGER PRIMARY KEY, FIRSTNAME VARCHAR(100), "last name" VARCHAR(100), AGE INTEGER,
                                 GENDER VARCHAR(1), FIRMID INTEGER, STREET VARCHAR(100), FIRM SEMISTRUCTURED )
                  Table FIRM ( ID INTEGER PRIMARY KEY, LEGALNAME VARCHAR(100) )
                )
                ###ExternalFormat
                Binding x::DetailsBinding
                {
                  contentType: 'application/json';
                  modelIncludes: [ x::Details ];
                }
                ###Pure
                Enum x::Gender { MALE, FEMALE }
                Class x::Details { legalName: String[1]; }
                Class x::Address { street: String[0..1]; }
                Class x::Firm { id: Integer[1]; legalName: String[1]; }
                Class x::Person
                {
                  firstName: String[1];
                  lastName: String[0..1];
                  greeted: String[1];
                  ageBucket: String[0..1];
                  gender: x::Gender[0..1];
                  firmId: Integer[0..1];
                  details: x::Details[0..1];
                  address: x::Address[0..1];
                }
                Association x::Firm_Person { firm: x::Firm[0..1]; staff: x::Person[*]; }
                function x::people(): meta::pure::metamodel::relation::Relation<meta::pure::metamodel::type::Any>[1]
                {
                  #>{x::DB.PERSON}#
                }
                function x::firms(): meta::pure::metamodel::relation::Relation<meta::pure::metamodel::type::Any>[1]
                {
                  #>{x::DB.FIRM}#
                }
                ###Mapping
                Mapping x::M
                (
                  *x::Person[person]: Relation
                  {
                    ~func x::people():Relation<Any>[1]
                    ~primaryKey: ID
                    firstName: FIRSTNAME,
                    lastName: $row.'last name',
                    greeted: 'Hi ' + $row.FIRSTNAME->toOne(),
                    ageBucket: if($row.AGE > 65, |'senior', |'other'),
                    gender: EnumerationMapping genders : $row.GENDER,
                    firmId: $row.FIRMID,
                    details: Binding x::DetailsBinding : $row.FIRM,
                    address ( street: $row.STREET )
                  }
                  *x::Firm[firm]: Relation
                  {
                    ~func x::firms():Relation<Any>[1]
                    id: ID,
                    legalName: $row.LEGALNAME
                  }
                  x::Firm_Person: ModelJoin
                  {
                    {firm: x::Firm[1], staff: x::Person[1]|$firm.id == $staff.firmId}
                  }
                  x::Gender: EnumerationMapping genders
                  {
                    MALE: ['M'],
                    FEMALE: ['F']
                  }
                )
                ###Connection
                """ + DataDeliveryMatrixTest.connection(db, "specification: LocalH2 { "
                + "testDataSetupCSV: 'default\\nPERSON\\nID,FIRSTNAME,last name,AGE,GENDER,FIRMID,STREET,FIRM\\n"
                + "1,Ann,Lee,70,F,1,Main St,\"{\"\"legalName\"\":\"\"Acme\"\"}\"\\n"
                + "2,Bob,,30,M,2,,\\n"
                + "-----\\n"
                + "default\\nFIRM\\nID,LEGALNAME\\n1,Acme Corp\\n2,Beta LLC\\n'; }; auth: DefaultH2;") + """
                ###Runtime
                Runtime x::RT { mappings: [ x::M ]; connections: [ x::DB: [ c: x::Conn ] ]; }
                """;
    }

    private static List<List<String>> read(String model, String query, DatabaseType db) throws Exception {
        return DataDeliveryMatrixTest.read(new DataDeliveryMatrixTest.Data(model, query, List.of(), "x::RT"), query,
                db);
    }

    private static final String QUERY = "|x::Person.all()->project(~[f:x|$x.firstName, l:x|$x.lastName,"
            + " g:x|$x.greeted, b:x|$x.ageBucket, gen:x|$x.gender, d:x|$x.details.legalName,"
            + " st:x|$x.address.street])->sort(~f->ascending())->from(x::M, x::RT)";

    private static final List<List<String>> ROWS = List.of(
            Arrays.asList("Ann", "Lee", "Hi Ann", "senior", "FEMALE", "Acme", "Main St"),
            Arrays.asList("Bob", "null", "Hi Bob", "other", "MALE", "null", "null"));

    @Test
    @DisplayName("every right-hand side over $row reads the row")
    void everyFormReadsTheRow() throws Exception {
        for (DatabaseType db : DataDeliveryMatrixTest.DATABASES) {
            assertEquals(ROWS, read(model(db), QUERY, db), db + ": the $row bindings");
        }
    }

    @Test
    @DisplayName("a ModelJoin's condition reads an expression binding over $row")
    void anAssociationReadsTheRow() throws Exception {
        String query = "|x::Person.all()->project(~[f:x|$x.firstName, n:x|$x.firm.legalName])"
                + "->sort(~f->ascending())->from(x::M, x::RT)";
        for (DatabaseType db : DataDeliveryMatrixTest.DATABASES) {
            assertEquals(List.of(Arrays.asList("Ann", "Acme Corp"), Arrays.asList("Bob", "Beta LLC")),
                    read(model(db), query, db), db + ": the association through firmId: $row.FIRMID");
        }
    }

    @Test
    @DisplayName("the pin's $src binds the same row: the two spellings answer alike")
    void thePinsSrcStillBinds() throws Exception {
        DatabaseType db = DataDeliveryMatrixTest.DATABASES.get(0);
        // every accessor respelled as the pin spells it
        String src = model(db).replace("$row.", "$src.");
        assertTrue(!src.contains("$row") && src.contains("$src.FIRM"), src);
        assertEquals(ROWS, read(src, QUERY, db), "the $src bindings");
    }

    @Test
    @DisplayName("a lambda's own parameter named row (or src) shadows the accessor in its body")
    void aLambdasOwnRowShadowsTheAccessor() {
        Variable rowVar = new Variable("rf_row");
        // $row.FIRSTNAME->map(row|$row->toUpper()): the receiver is the row, the lambda's $row its parameter
        AppliedFunction map = (AppliedFunction) ClassMapping.RelationFunction.Col.bindRow(
                body("|$row.FIRSTNAME->map(row|$row->toUpper())"), rowVar);
        assertEquals(rowVar, ((AppliedProperty) map.parameters().get(0)).receiver(), "the accessor bound");
        LambdaFunction lambda = (LambdaFunction) map.parameters().get(1);
        AppliedFunction upper = (AppliedFunction) lambda.body().get(0);
        assertEquals("row", ((Variable) upper.parameters().get(0)).name(), "the parameter kept");
        // a lambda naming only one of the two shadows that one: $src inside map(row|...) is still the row
        AppliedFunction mixed = (AppliedFunction) ClassMapping.RelationFunction.Col.bindRow(
                body("|$row.FIRSTNAME->map(row|$src.AGE)"), rowVar);
        AppliedProperty inner = (AppliedProperty) ((LambdaFunction) mixed.parameters().get(1)).body().get(0);
        assertEquals(rowVar, inner.receiver(), "$src bound inside a lambda that shadows only row");
    }

    private static ValueSpecification body(String lambda) {
        return ((LambdaFunction) Compiler.parseQuery(lambda)).body().get(0);
    }
}
