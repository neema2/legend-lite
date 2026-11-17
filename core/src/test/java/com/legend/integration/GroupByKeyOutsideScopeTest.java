// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.integration;

import com.legend.Compiler;
import com.legend.model.ConnectionDefinition.DatabaseType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A {@code ~groupBy} key that names a table the class mapping does not reach ({@code ~groupBy(FIRM.ID,
 * PERSON.FIRST_NAME)} over {@code ~mainTable PERSON}, no join): the Studio roundtrip's
 * STO_Relational-relational-mapping-groupby. Until 2026-11-16 lite refused the model at compile ("ColumnRef references
 * table 'FirmTable' not in scope").
 *
 * <p>legend-engine 4.145.0 compiles it: the relational compiler resolves each key against the database alone
 * ({@code RelationalCompilerExtension}'s first pass, {@code processRelationalOperationElement} with no alias map). When
 * the mapping runs, {@code pureToSQLQuery.pure}'s {@code applyGroupBy} re-aliases EVERY key column onto the root node
 * ({@code reprocessAliases(first = <the key's alias>, second = <the last join node of the bare root>)}), and the SQL
 * writer prints the alias and the column's name: the key groups by the MAIN table's column of that name
 * ({@code group by "root".ID}). Two people named John in one firm are two groups. On every in-process database.
 */
@DisplayName("a ~groupBy key naming a table the class does not reach")
class GroupByKeyOutsideScopeTest {

    /** One in-process database, for what does not depend on which (a compile, a refusal). */
    private static final DatabaseType ANY = DataDeliveryMatrixTest.DATABASES.get(0);

    private static String model(DatabaseType db, String key, String firmColumns) {
        return """
                ###Relational
                Database g::Test
                (
                  Table FIRM ( %s )
                  Table PERSON ( ID INTEGER PRIMARY KEY, FIRM_ID INTEGER, FIRST_NAME VARCHAR(200) )
                  Join FirmPerson(PERSON.FIRM_ID = FIRM.ID)
                )
                ###Pure
                Class g::Firm { name: String[1]; }
                ###Mapping
                Mapping g::M
                (
                  *g::Firm: Relational
                  {
                    ~groupBy
                    (
                      [g::Test]%s,
                      [g::Test]PERSON.FIRST_NAME
                    )
                    ~mainTable [g::Test]PERSON
                    name: [g::Test]PERSON.FIRST_NAME
                  }
                )
                ###Connection
                RelationalDatabaseConnection g::Conn
                {
                  store: g::Test;
                  type: %s;
                  specification: LocalH2
                  {
                    testDataSetupCSV: 'default\\nPERSON\\nID,FIRM_ID,FIRST_NAME\\n1,1,John\\n2,1,John\\n3,2,Jane\\n-----\\ndefault\\nFIRM\\nID\\n1\\n2\\n';
                  };
                  auth: DefaultH2;
                }
                ###Runtime
                Runtime g::RT { mappings: [ g::M ]; connections: [ g::Test: [ c: g::Conn ] ]; }
                """.formatted(firmColumns, key, db);
    }

    /** The case's shape: the key names FIRM, which no join of the class reaches. */
    private static String firmKey(DatabaseType db) {
        return model(db, "FIRM.ID", "ID INTEGER PRIMARY KEY");
    }

    private static final String QUERY =
            "|g::Firm.all()->project(~[name: x|$x.name])->sort(~name->ascending())->from(g::M, g::RT)";

    private static List<List<String>> names(String model) throws Exception {
        return DataDeliveryMatrixTest.read(new DataDeliveryMatrixTest.Data(model, QUERY, List.of(), "g::RT"), QUERY,
                ANY);
    }

    @Test
    @DisplayName("the model compiles, as legend-engine's does")
    void theModelCompiles() {
        assertEquals(List.of(), Compiler.compileErrors(firmKey(ANY)));
    }

    @Test
    @DisplayName("the key groups by the main table's column of its name: legend-engine's rows")
    void theKeyGroupsByTheMainTablesColumn() throws Exception {
        // legend-engine: select "root".FIRST_NAME ... from PERSON as "root" group by "root".ID, "root".FIRST_NAME
        List<List<String>> engine = List.of(List.of("Jane"), List.of("John"), List.of("John"));
        for (DatabaseType db : DataDeliveryMatrixTest.DATABASES) {
            String firmKey = firmKey(db);
            assertEquals(engine, DataDeliveryMatrixTest.read(
                    new DataDeliveryMatrixTest.Data(firmKey, QUERY, List.of(), "g::RT"), QUERY, db), db + "");
            // the same rows as the key written on the main table
            String personKey = model(db, "PERSON.ID", "ID INTEGER PRIMARY KEY");
            assertEquals(engine, DataDeliveryMatrixTest.read(
                    new DataDeliveryMatrixTest.Data(personKey, QUERY, List.of(), "g::RT"), QUERY, db), db + "");
        }
    }

    @Test
    @DisplayName("a key column the main table lacks: compiles, and a query of the class is refused by name")
    void aKeyColumnTheMainTableLacksIsRefusedAtUse() {
        // legend-engine compiles this too; its query fails at the database ("root".NAME is no column of PERSON).
        // Lite cannot send that statement: the class is withheld and a query of it is refused (S414, pending)
        String model = model(ANY, "FIRM.NAME", "ID INTEGER PRIMARY KEY, NAME VARCHAR(20)");
        assertEquals(List.of(), Compiler.compileErrors(model));
        var e = assertThrows(RuntimeException.class, () -> names(model));
        assertTrue(String.valueOf(e.getMessage()).contains("~groupBy key FIRM.NAME"), e.getMessage());
    }

    @Test
    @DisplayName("the Studio case compiles")
    void theStudioCaseCompiles() {
        String studio = """
                ###Relational
                Database model::Test
                (
                  Table FirmTable
                  (
                    id INTEGER PRIMARY KEY
                  )
                  Table PersonTable
                  (
                    id INTEGER PRIMARY KEY,
                    firm_id INTEGER,
                    firstName VARCHAR(200),
                    lastName VARCHAR(200)
                  )

                  Join FirmPerson(PersonTable.firm_id = FirmTable.id)
                )


                ###Pure
                Class model::Firm
                {
                  name: String[1];
                  employees: model::Person[1..*];
                }

                Class model::Person
                {
                  firstName: String[1];
                  lastName: String[1];
                }


                ###Mapping
                Mapping model::NewMapping
                (
                  *model::Firm: Relational
                  {
                    ~groupBy
                    (
                      [model::Test]FirmTable.id,
                      [model::Test]PersonTable.firstName
                    )
                    ~mainTable [model::Test]PersonTable
                    name: [model::Test]PersonTable.firstName
                  }
                )
                """;
        assertEquals(List.of(), Compiler.compileErrors(studio));
    }
}
