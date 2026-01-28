package org.finos.legend.engine.test;

import org.finos.legend.engine.plan.WriteResult;
import org.finos.legend.engine.server.WriteService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for write operations using actual database.
 */
class WriteIntegrationTest {
    
    private static final String PERSON_MODEL = """
            Class model::Person {
                firstName: String[1];
                lastName: String[1];
                age: Integer[0..1];
            }
            """;
    
    private Connection connection;
    private WriteService writeService;
    
    @BeforeEach
    void setUp() throws SQLException {
        connection = DriverManager.getConnection("jdbc:duckdb:");
        writeService = new WriteService();
    }
    
    @AfterEach
    void tearDown() throws SQLException {
        if (connection != null && !connection.isClosed()) {
            connection.close();
        }
    }
    
    @Nested
    @DisplayName("Save (INSERT) Tests")
    class SaveTests {
        
        @Test
        @DisplayName("Execute save with class definition - end-to-end")
        void testExecuteWithClassDefinition() throws SQLException {
            WriteResult result = writeService.execute(
                    PERSON_MODEL,
                    "^Person(firstName='John', lastName='Doe', age=30)->save()",
                    connection
            );
            
            assertEquals(1, result.rowsAffected());
            
            // Verify data was inserted
            try (Statement stmt = connection.createStatement();
                    ResultSet rs = stmt.executeQuery("SELECT * FROM \"T_PERSON\"")) {
                assertTrue(rs.next());
                assertEquals("John", rs.getString("FIRST_NAME"));
                assertEquals("Doe", rs.getString("LAST_NAME"));
                assertEquals(30, rs.getInt("AGE"));
            }
        }
        
        @Test
        @DisplayName("Execute multiple saves with same class")
        void testMultipleSaves() throws SQLException {
            writeService.execute(PERSON_MODEL, 
                    "^Person(firstName='Alice', lastName='Smith', age=25)->save()", connection);
            writeService.execute(PERSON_MODEL,
                    "^Person(firstName='Bob', lastName='Jones', age=35)->save()", connection);
            
            try (Statement stmt = connection.createStatement();
                    ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM \"T_PERSON\"")) {
                assertTrue(rs.next());
                assertEquals(2, rs.getInt(1));
            }
        }
        
        @Test
        @DisplayName("Fail on undefined class")
        void testUndefinedClassFails() {
            String emptyModel = "";
            
            WriteService.WriteException ex = assertThrows(
                    WriteService.WriteException.class,
                    () -> writeService.execute(emptyModel, 
                            "^UnknownClass(name='test')->save()", connection)
            );
            
            assertTrue(ex.getMessage().contains("Class not found: UnknownClass"));
        }
    }
    
    @Nested
    @DisplayName("Transaction Tests")
    class TransactionTests {
        
        @Test
        @DisplayName("Transaction rollback on constraint violation")
        void testTransactionRollback() throws SQLException {
            // First save
            writeService.execute(PERSON_MODEL,
                    "^Person(firstName='Existing', lastName='Person')->save()", connection);
            
            // Add unique constraint
            try (Statement stmt = connection.createStatement()) {
                stmt.execute("CREATE UNIQUE INDEX idx_name ON \"T_PERSON\"(\"FIRST_NAME\", \"LAST_NAME\")");
            }
            
            // Try duplicate - should fail and rollback
            assertThrows(WriteService.WriteException.class,
                    () -> writeService.execute(PERSON_MODEL,
                            "^Person(firstName='Existing', lastName='Person')->save()", connection));
            
            // Original row still exists
            try (Statement stmt = connection.createStatement();
                    ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM \"T_PERSON\"")) {
                assertTrue(rs.next());
                assertEquals(1, rs.getInt(1));
            }
        }
    }
}
