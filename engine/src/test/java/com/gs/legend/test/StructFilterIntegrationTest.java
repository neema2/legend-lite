package com.gs.legend.test;
import com.gs.legend.ast.*;
import com.gs.legend.antlr.*;
import com.gs.legend.parser.*;
import com.gs.legend.compiler.*;
import com.gs.legend.model.*;
import com.gs.legend.model.def.*;
import com.gs.legend.model.m3.*;
import com.gs.legend.model.store.*;
import com.gs.legend.model.mapping.*;
import com.gs.legend.plan.*;
import com.gs.legend.exec.*;
import com.gs.legend.serial.*;
import com.gs.legend.sqlgen.*;
import com.gs.legend.server.*;
import com.gs.legend.service.*;
import com.gs.legend.sqlgen.DuckDBDialect;
import com.gs.legend.sqlgen.SQLDialect;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.DriverManager;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for struct collection filter with nested exists.
 * Replicates exact PCT expressions from
 * meta::pure::functions::collection::tests::exists::testExistsInSelect.
 *
 * These tests verify that filtering a struct array with ->exists() and then
 * accessing a property correctly compiles through the scalar list path
 * (list_filter + list_transform) rather than the relational path.
 */
public class StructFilterIntegrationTest extends AbstractDatabaseTest {

    @Override
    protected String getDatabaseType() {
        return "DuckDB";
    }

    @Override
    protected SQLDialect getDialect() {
        return DuckDBDialect.INSTANCE;
    }

    @Override
    protected String getJdbcUrl() {
        return "jdbc:duckdb:";
    }

    @BeforeEach
    void setUp() throws SQLException {
        connection = DriverManager.getConnection(getJdbcUrl());
        setupPureModel();
        setupDatabase();
    }

    @AfterEach
    void tearDown() throws SQLException {
        if (connection != null) {
            connection.close();
        }
    }

    // ==================== PCT: testExistsInSelect ====================
    // Exact expressions from the failing PCT test

    @Test
    @DisplayName("PCT: filter struct collection with exists, extract .legalName (match p1)")
    void testExistsInSelectFilterLegalName() throws SQLException {
        // Exact PCT expression: filter firms where any employee has lastName == 'p1', then get legalName
        // Expected: only firm 'f' has an employee with lastName 'p1'
        var result = executeRelation(
                "|[^meta::pure::functions::collection::tests::model::CO_Firm(" +
                "legalName='f'," +
                "employees=[^meta::pure::functions::collection::tests::model::CO_Person(firstName='p1',lastName='p1')," +
                "^meta::pure::functions::collection::tests::model::CO_Person(firstName='p2',lastName='p2')]" +
                "), ^meta::pure::functions::collection::tests::model::CO_Firm(" +
                "legalName='f2'," +
                "employees=[^meta::pure::functions::collection::tests::model::CO_Person(firstName='p3',lastName='p3')," +
                "^meta::pure::functions::collection::tests::model::CO_Person(firstName='p3',lastName='p4')]" +
                ")]->meta::pure::functions::collection::filter(f|$f.employees->meta::pure::functions::collection::exists(" +
                "e: meta::pure::functions::collection::tests::model::CO_Person[1]|$e.lastName == 'p1')).legalName");
        assertNotNull(result);
        assertEquals(1, result.rows().size(), "Should find exactly 1 firm with employee lastName='p1'");
        // PCT expects scalar 'f', not list-wrapped '[f]'
        assertEquals("f", result.rows().get(0).get(0).toString());
    }

    @Test
    @DisplayName("PCT: filter struct collection with exists, extract .legalName (match p3)")
    void testExistsInSelectFilterLegalNameP3() throws SQLException {
        // Same structure, but filter where lastName == 'p3'
        // Expected: both firms have an employee with lastName matching:
        //   f2 has p3 (firstName='p3',lastName='p3')
        // Only f2 matches since f doesn't have lastName='p3'
        // Wait: f has p1,p2 lastNames. f2 has p3,p4 lastNames. So only f2 matches.
        var result = executeRelation(
                "|[^meta::pure::functions::collection::tests::model::CO_Firm(" +
                "legalName='f'," +
                "employees=[^meta::pure::functions::collection::tests::model::CO_Person(firstName='p1',lastName='p1')," +
                "^meta::pure::functions::collection::tests::model::CO_Person(firstName='p2',lastName='p2')]" +
                "), ^meta::pure::functions::collection::tests::model::CO_Firm(" +
                "legalName='f2'," +
                "employees=[^meta::pure::functions::collection::tests::model::CO_Person(firstName='p3',lastName='p3')," +
                "^meta::pure::functions::collection::tests::model::CO_Person(firstName='p3',lastName='p4')]" +
                ")]->meta::pure::functions::collection::filter(f|$f.employees->meta::pure::functions::collection::exists(" +
                "e: meta::pure::functions::collection::tests::model::CO_Person[1]|$e.lastName == 'p3')).legalName");
        assertNotNull(result);
        assertEquals(1, result.rows().size(), "Should find exactly 1 firm with employee lastName='p3'");
        assertEquals("f2", result.rows().get(0).get(0).toString());
    }

    @Test
    @DisplayName("PCT: filter struct collection with exists, no match")
    void testExistsInSelectFilterNoMatch() throws SQLException {
        // Filter where lastName == 'nonexistent' — no firm matches
        var result = executeRelation(
                "|[^meta::pure::functions::collection::tests::model::CO_Firm(" +
                "legalName='f'," +
                "employees=[^meta::pure::functions::collection::tests::model::CO_Person(firstName='p1',lastName='p1')," +
                "^meta::pure::functions::collection::tests::model::CO_Person(firstName='p2',lastName='p2')]" +
                "), ^meta::pure::functions::collection::tests::model::CO_Firm(" +
                "legalName='f2'," +
                "employees=[^meta::pure::functions::collection::tests::model::CO_Person(firstName='p3',lastName='p3')," +
                "^meta::pure::functions::collection::tests::model::CO_Person(firstName='p3',lastName='p4')]" +
                ")]->meta::pure::functions::collection::filter(f|$f.employees->meta::pure::functions::collection::exists(" +
                "e: meta::pure::functions::collection::tests::model::CO_Person[1]|$e.lastName == 'nonexistent')).legalName");
        assertNotNull(result);
        assertTrue(result.rows().isEmpty(), "No firm should match lastName='nonexistent'");
    }

    @Test
    @DisplayName("PCT: simple struct filter without exists")
    void testStructFilterSimpleProperty() throws SQLException {
        // Simpler case: filter struct collection by direct property, then extract
        var result = executeRelation(
                "|[^meta::pure::functions::collection::tests::model::CO_Firm(" +
                "legalName='f'," +
                "employees=[^meta::pure::functions::collection::tests::model::CO_Person(firstName='p1',lastName='p1')]" +
                "), ^meta::pure::functions::collection::tests::model::CO_Firm(" +
                "legalName='f2'," +
                "employees=[^meta::pure::functions::collection::tests::model::CO_Person(firstName='p3',lastName='p3')]" +
                ")]->meta::pure::functions::collection::filter(f|$f.legalName == 'f2').legalName");
        assertNotNull(result);
        assertEquals(1, result.rows().size());
        assertEquals("f2", result.rows().get(0).get(0).toString());
    }
}
