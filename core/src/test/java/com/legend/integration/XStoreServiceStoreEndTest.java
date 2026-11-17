// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.integration;

import com.legend.Compiler;
import com.legend.model.ConnectionDefinition.DatabaseType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * An XStore association whose end is a ServiceStore set ({@code employees[firm_set, person_set2]: $this.firmId ==
 * $that.firmId}, {@code person_set2} a ServiceStore class mapping with a local {@code +firmId}): the Studio roundtrip's
 * STO_ServiceStore-path-offset. Until 2026-11-16 lite refused the model at compile ("XStore/ModelJoin association end
 * class ... resolves to no Relation or Relational set for set id 'person_set2'").
 *
 * <p>legend-engine 4.145.0 compiles it ({@code PropertyMappingBuilder.visit(XStorePropertyMapping)} finds both sets by
 * id and types {@code $that} as the ServiceStore set's mapping class, which carries {@code +firmId}:
 * {@code HelperServiceStoreClassMappingBuilder.generateMappingClass}) and runs it, calling the services
 * ({@code TestServiceStorePathOffset.serviceStoreExampleWithCrossStore}). Lite executes no ServiceStore: the model
 * compiles, the association's binding is withheld, and a query navigating it is refused by name -- register row S413,
 * pending. On every in-process database.
 */
@DisplayName("an XStore association with a ServiceStore end")
class XStoreServiceStoreEndTest {

    /** One in-process database, for what does not depend on which (a compile, a refusal). */
    private static final DatabaseType ANY = DataDeliveryMatrixTest.DATABASES.get(0);

    /** The ServiceStore side: the case's schema, binding and store, cut to what the sets read. */
    private static final String SERVICE_STORE = """
            ###ExternalFormat
            SchemaSet s::ApiSchemaSet
            {
              format: JSON;
              schemas: [ { id: Api; content: '{"type": "object"}'; } ];
            }

            Binding s::ApiBinding
            {
              schemaSet: s::ApiSchemaSet;
              contentType: 'application/json';
              modelIncludes: [ s::Person, s::Firm, s::ApiResponse ];
            }

            ###Pure
            Class s::ApiResponse { employees: s::Person[*]; firms: s::Firm[*]; }
            Class s::Person { firstName: String[1]; }
            Class s::Firm { firmName: String[1]; firmId: Integer[1]; }
            Association s::Employment { employees: s::Person[*]; firm: s::Firm[0..1]; }

            ###ServiceStore
            ServiceStore s::Store
            (
              Service EmployeesService
              (
                path : '/employees';
                method : GET;
                parameters : ( constantParam : Integer ( location = query ) );
                response : [s::ApiResponse <- s::ApiBinding];
                security : [];
              )
              Service EmployeesServiceByFirmId
              (
                path : '/employeesByFirmId/{firmId}';
                method : GET;
                parameters : ( firmId : Integer ( location = path ) );
                response : [s::ApiResponse <- s::ApiBinding];
                security : [];
              )
            )
            """;

    private static final String PERSON_SETS = """
              *s::Person[person_set]: ServiceStore
              {
                ~service [s::Store] EmployeesService
                (
                  ~path $service.response.employees
                  ~request ( parameters ( constantParam = 1 ) )
                )
              }
              s::Person[person_set2]: ServiceStore
              {
                +firmId : Integer[1];

                ~service [s::Store] EmployeesServiceByFirmId
                (
                  ~path $service.response.employees
                  ~request ( parameters ( firmId = $this.firmId ) )
                )
              }
            """;

    /** The case's shape: every set a ServiceStore one. */
    private static final String ALL_SERVICE = SERVICE_STORE + """
            ###Mapping
            Mapping s::M
            (
              *s::Firm[firm_set]: ServiceStore
              {
                ~service [s::Store] EmployeesService
                (
                  ~path $service.response.firms
                  ~request ( parameters ( constantParam = 1 ) )
                )
              }
            %s
              s::Employment: XStore
              {
                employees[firm_set, %s]: $this.firmId == $that.firmId
              }
            )
            """;

    /** A relational Firm beside the ServiceStore Person: a query can reach the association from the Firm side. */
    private static String hybrid(DatabaseType db) {
        return SERVICE_STORE + """
                ###Relational
                Database s::Db ( Table FIRM ( FIRM_ID INTEGER PRIMARY KEY, FIRM_NAME VARCHAR(20) ) )
                ###Mapping
                Mapping s::M
                (
                  *s::Firm[firm_set]: Relational
                  {
                    ~mainTable [s::Db]FIRM
                    firmName: [s::Db]FIRM.FIRM_NAME,
                    firmId: [s::Db]FIRM.FIRM_ID
                  }
                %s
                  s::Employment: XStore
                  {
                    employees[firm_set, person_set2]: $this.firmId == $that.firmId
                  }
                )
                ###Connection
                RelationalDatabaseConnection s::Conn
                {
                  store: s::Db;
                  type: %s;
                  specification: LocalH2 { testDataSetupCSV: 'default\\nFIRM\\nFIRM_ID,FIRM_NAME\\n1,Firm A\\n'; };
                  auth: DefaultH2;
                }
                ###Runtime
                Runtime s::RT { mappings: [ s::M ]; connections: [ s::Db: [ c: s::Conn ] ]; }
                """.formatted(PERSON_SETS, db);
    }

    private static List<List<String>> read(String model, String query, DatabaseType db) throws Exception {
        return DataDeliveryMatrixTest.read(new DataDeliveryMatrixTest.Data(model, query, List.of(), "s::RT"), query,
                db);
    }

    @Test
    @DisplayName("every set a ServiceStore one: the model compiles, as legend-engine's does")
    void theCaseShapeCompiles() {
        assertEquals(List.of(), Compiler.compileErrors(ALL_SERVICE.formatted(PERSON_SETS, "person_set2")));
    }

    @Test
    @DisplayName("a relational Firm and a ServiceStore Person: compiles; navigating the association is refused by name")
    void navigatingTheAssociationIsRefused() throws Exception {
        assertEquals(List.of(), Compiler.compileErrors(hybrid(ANY)));
        for (DatabaseType db : DataDeliveryMatrixTest.DATABASES) {
            // the Firm's own rows still answer
            String names = "|s::Firm.all()->project(~[name: f|$f.firmName])->from(s::M, s::RT)";
            assertEquals(List.of(List.of("Firm A")), read(hybrid(db), names, db), db + "");
            String employees = "|s::Firm.all()->project(~[name: f|$f.firmName, employee: f|$f.employees.firstName])"
                    + "->from(s::M, s::RT)";
            var e = assertThrows(RuntimeException.class, () -> read(hybrid(db), employees, db));
            String message = String.valueOf(e.getMessage());
            assertTrue(message.contains("s::Employment") && message.contains("person_set2")
                    && message.contains("ServiceStore"), message);
        }
    }

    @Test
    @DisplayName("an XStore line naming no set of the mapping still fails to compile, as legend-engine's does")
    void aMissingSetStillFails() {
        // legend-engine: "Can't find class mapping 'person_set3' in mapping 's::M'" (PropertyMappingBuilder). Lite's
        // error is its XStore end's, unchanged by the ServiceStore end beside it (it names the first end it reads)
        List<String> errors = Compiler.compileErrors(ALL_SERVICE.formatted(PERSON_SETS, "person_set3"));
        assertFalse(errors.isEmpty());
        assertTrue(errors.get(0).contains("XStore/ModelJoin association end"), errors.toString());
    }
}
