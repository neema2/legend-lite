package org.finos.legend.engine.test;

import java.sql.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public class DuckDBStructSyntaxTest {
    
    @Test
    void testJsonStyleStruct() throws Exception {
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
             Statement stmt = conn.createStatement()) {
            
            // Test JSON-style: {'key': 'value'}
            ResultSet rs = stmt.executeQuery(
                "SELECT * FROM (VALUES ({'name': 'Alice', 'age': 30})) AS t(details)"
            );
            assertTrue(rs.next());
            System.out.println("JSON-style Result: " + rs.getObject(1));
        }
    }
    
    @Test
    void testSimpleValuesWithColumns() throws Exception {
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
             Statement stmt = conn.createStatement()) {
            
            // Test simple VALUES with named columns
            ResultSet rs = stmt.executeQuery(
                "SELECT * FROM (VALUES ('Alice', 30), ('Bob', 25)) AS t(name, age)"
            );
            assertTrue(rs.next());
            assertEquals("Alice", rs.getString("name"));
            assertEquals(30, rs.getInt("age"));
        }
    }
}
