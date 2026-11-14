// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.integration;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A CONNECTION ELEMENT'S POST-PROCESSORS (the third review, 2026-11-07): upstream applies a {@code ###Connection}'s
 * {@code postProcessors} to every query on it (legend-engine's postprocessor.pure: a mapper's table and schema
 * renames, a relational mapper's). Lite applies them not yet, and read them as nothing before: a mapper's query read
 * the tables as written, other rows than upstream's. A query on a connection whose post-processors map its tables is
 * refused by name now; CTE extraction, which spells the SQL only, plans as before.
 */
class ConnectionPostProcessorsTest extends AbstractDatabaseTest {

    private String withConnection(String postProcessors) {
        return getCompletePureModelWithRuntime() + "\n" + """
                ###Connection
                RelationalDatabaseConnection store::MappedConnection
                {
                    type: %s;
                    %s;
                    postProcessors:
                    [
                        %s
                    ];
                }

                ###Runtime
                Runtime test::MappedRuntime
                {
                    mappings:
                    [
                        model::PersonMapping
                    ];
                    connections:
                    [
                        store::PersonDatabase:
                        [
                            environment: store::MappedConnection
                        ]
                    ];
                }
                """.formatted(backend().type(), backend().specificationAndAuth(), postProcessors);
    }

    private static final String QUERY = "#>{store::PersonDatabase.T_PERSON}#->select(~[ID, AGE_VAL])"
            + "->from(test::MappedRuntime)";

    @Test
    @DisplayName("a mapper's renames are refused by name, never read as nothing")
    void aMapperIsRefusedByName() {
        String model = withConnection("""
                mapper
                {
                    mappers:
                    [
                        table { from: 'T_PERSON'; to: 'T_PERSON_ARCHIVE'; schemaFrom: 'default'; schemaTo: 'default'; }
                    ];
                }""");
        Exception e = assertThrows(Exception.class, () -> com.legend.Compiler.query(com.legend.Compiler.compileModel(
                model), QUERY).plan("test::MappedRuntime"));
        assertTrue(String.valueOf(e.getMessage()).contains("declares post-processors that map its tables (mapper)"),
                e.getMessage());
    }

    @Test
    @DisplayName("CTE extraction alone plans: it spells the SQL, the rows are the same")
    void cteExtractionAlonePlans() {
        String model = withConnection("ExtractSubQueriesAsCTEsPostProcessor");
        String sql = com.legend.Compiler.query(com.legend.Compiler.compileModel(model), QUERY)
                .plan("test::MappedRuntime").sql();
        assertFalse(sql.isEmpty(), sql);
    }
}
