// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.integration;

import com.legend.model.ConnectionDefinition.DatabaseType;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * A CSV WITH AN EMBEDDED JSON COLUMN (the total wall's item 5d, the user's question of 2026-11-11: can a CSV carry one?
 * It can: types come from the model, so a {@code SEMISTRUCTURED} column's cell text is taken by the database's own JSON
 * type). The cell holds a comma and CSV's doubled quotes (each of the JSON's own) inside its quoted field, and a nested
 * object; an empty cell and
 * a {@code ---null---} are NULL (the CSV test data's rule); a field is read by path, nested too. The same rows on every
 * in-process database; Postgres's run is {@link #onPostgres}, on its unprivileged lane.
 */
class CsvJsonColumnTest {

    /** The table: a key and a JSON document. */
    static String model(DatabaseType db) {
        return """
                ###Relational
                Database x::DB ( Table J (ID INTEGER PRIMARY KEY, DOC SEMISTRUCTURED) )
                ###Connection
                """ + DataDeliveryMatrixTest.connection(db, DataDeliveryMatrixTest.localH2("testDataSetupCSV: '"
                + "default\\nJ\\nID,DOC\\n"
                + "1,\"{\"\"a\"\":\"\"x,y\"\",\"\"n\"\":{\"\"k\"\":7}}\"\\n"
                + "2,\\n"
                + "3,---null---\\n'")) + """
                ###Runtime
                Runtime x::RT { mappings: []; connections: [ x::DB: [ c: x::Conn ] ]; }
                """;
    }

    /** Each document's fields by path: a string with a comma, a nested number. */
    static final String QUERY = "|#>{x::DB.J}#->extend(~[A: r|cast(get($r.DOC, 'a'), @meta::pure::metamodel::type::String),"
            + " K: r|cast(get(get($r.DOC, 'n'), 'k'), @meta::pure::metamodel::type::Integer)])->select(~[ID, A, K])"
            + "->sort(~ID->ascending())->from(x::RT)";

    static final List<List<String>> ROWS = List.of(List.of("1", "x,y", "7"), Arrays.asList("2", "null", "null"),
            Arrays.asList("3", "null", "null"));

    @Test
    void eachInProcessDatabaseReadsTheJsonCellsByPath() throws Exception {
        for (DatabaseType db : DataDeliveryMatrixTest.DATABASES) {
            check(db);
        }
    }

    /** Postgres's run (its unprivileged lane calls it). */
    static void onPostgres() throws Exception {
        check(DatabaseType.Postgres);
    }

    private static void check(DatabaseType db) throws Exception {
        DataDeliveryMatrixTest.Data data = new DataDeliveryMatrixTest.Data(model(db), QUERY, ROWS, "x::RT");
        assertEquals(ROWS, DataDeliveryMatrixTest.read(data, QUERY, db), db + ": the JSON cells by path");
    }
}
