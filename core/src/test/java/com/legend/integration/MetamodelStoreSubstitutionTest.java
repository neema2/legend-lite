// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0
package com.legend.integration;

import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.test.StorelessRuntime;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.legend.Compiler;
import com.legend.exec.ExecutionResult;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * STORE SUBSTITUTION AS RELATIONS (corpus-zero cluster D family 3): the
 * engine's {@code Mapping.resolveStore} (legend-pure functions_Mapping.pure:
 * the first include whose subtree substitutes, an include re-substituting
 * what its included mapping resolved) and {@code extractDBs} (every visible
 * root relational set's main-table database, the REFERENCED store) are
 * system-layer Pure over the system database — the resolutions seeded once
 * per (mapping, store) pair (the include-closure precedent), the alias's
 * database seeded from the mapping's own reference.
 */
class MetamodelStoreSubstitutionTest {
    private static final String MODEL = """
            ###Pure
            Class ss::A { id: Integer[1]; }
            ###Relational
            Database ss::dbInc ( Table T (id INT PRIMARY KEY) )
            Database ss::db ( include ss::dbInc )
            Database ss::other ( Table U (id INT PRIMARY KEY) )
            Database ss::third ( include ss::dbInc )
            ###Mapping
            Mapping ss::mInc ( ss::A[a]: Relational { ~mainTable [ss::dbInc]T id: [ss::dbInc]T.id } )
            Mapping ss::m ( include ss::mInc[ss::dbInc->ss::db] )
            Mapping ss::mTwo ( include ss::m[ss::db->ss::third] )
            Mapping ss::mRef ( ss::A[a]: Relational { ~mainTable [ss::db]T id: [ss::db]T.id } )
            """;

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
        ExecutionResult r = Compiler.execute(StorelessRuntime.with(MODEL, DatabaseType.DuckDB), query, StorelessRuntime.RUNTIME, connection);
        if (r instanceof ExecutionResult.Collection c) {
            return c.values();
        }
        Object v = ((ExecutionResult.Scalar) r).value();
        return v == null ? List.of() : List.of(v);
    }

    @Test
    @DisplayName("resolveStore: an include's substitution answers, an unsubstituted store is itself, substitutions compose through includes")
    void resolveStoreThroughIncludes() throws SQLException {
        assertEquals(List.of("dbInc"), values("ss::mInc->meta::pure::mapping::resolveStore(ss::dbInc).name"),
                "no includes: the store itself");
        assertEquals(List.of("db"), values("ss::m->meta::pure::mapping::resolveStore(ss::dbInc).name"),
                "the include substitutes dbInc by db");
        assertEquals(List.of("db"), values("ss::m->meta::pure::mapping::resolveStore(ss::db).name"),
                "a store nobody substitutes is itself");
        assertEquals(List.of("other"), values("ss::m->meta::pure::mapping::resolveStore(ss::other).name"),
                "a store the mapping never touches is itself");
        assertEquals(List.of("third"), values("ss::mTwo->meta::pure::mapping::resolveStore(ss::dbInc).name"),
                "two levels: dbInc -> db in the inner include, db -> third in the outer");
    }

    @Test
    @DisplayName("an include cycle stops compilation loudly, naming the mappings in it")
    void includeCycleIsLoud() {
        String cyclic = MODEL + """
                ###Mapping
                Mapping ss::c1 ( include ss::c2 )
                Mapping ss::c2 ( include ss::c1 )
                """;
        var e = org.junit.jupiter.api.Assertions.assertThrows(
                com.legend.error.LegendCompileException.class,
                () -> Compiler.execute(StorelessRuntime.with(cyclic, DatabaseType.DuckDB), "ss::c1->meta::pure::mapping::resolveStore(ss::db).name", StorelessRuntime.RUNTIME, connection));
        org.junit.jupiter.api.Assertions.assertTrue(
                String.valueOf(e.getMessage()).contains("mapping include cycle")
                        && e.getMessage().contains("ss::c1") && e.getMessage().contains("ss::c2"),
                e.getMessage());
    }

    @Test
    @DisplayName("extractDBs: the REFERENCED database of each visible root set, deduplicated")
    void extractDbsNamesTheReferencedStore() throws SQLException {
        assertEquals(List.of("db"), values("ss::mRef->meta::relational::runtime::extractDBs().name"),
                "[ss::db]T names db even though T is declared by the included dbInc");
        assertEquals(List.of("dbInc"), values("ss::mInc->meta::relational::runtime::extractDBs().name"));
        assertEquals(List.of("dbInc"), values("ss::m->meta::relational::runtime::extractDBs().name"),
                "extractDBs walks includes; substitution is resolveStore's job, not extractDBs'");
    }

    @Test
    @DisplayName("the engine tests' assert shapes: identity of a reference and a row, a positional pick, a size")
    void corpusAssertShapes() throws SQLException {
        assertEquals(List.of(1L), values("ss::mInc->meta::relational::runtime::extractDBs()->size()"));
        assertEquals(List.of(true), values("ss::m->meta::pure::mapping::resolveStore(ss::dbInc) == ss::db"));
        assertEquals(List.of(true), values("ss::mInc->meta::relational::runtime::extractDBs()->at(0) == ss::dbInc"));
    }
}
