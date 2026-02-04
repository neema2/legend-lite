import java.sql.*;
public class TestDuckDBStruct {
    public static void main(String[] args) throws Exception {
        Class.forName("org.duckdb.DuckDBDriver");
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
             Statement stmt = conn.createStatement()) {
            
            // Test 1: The curly brace JSON-style syntax
            try {
                ResultSet rs = stmt.executeQuery(
                    "SELECT * FROM (VALUES ({'name': 'Alice', 'age': 30})) AS t(details)"
                );
                System.out.println("Test 1 (JSON-style) PASSED");
                while (rs.next()) System.out.println("  Result: " + rs.getObject(1));
            } catch (Exception e) {
                System.out.println("Test 1 (JSON-style) FAILED: " + e.getMessage());
            }
            
            // Test 2: STRUCT_PACK function
            try {
                ResultSet rs = stmt.executeQuery(
                    "SELECT * FROM (VALUES (struct_pack(name := 'Alice', age := 30))) AS t(details)"
                );
                System.out.println("Test 2 (struct_pack :=) PASSED");
                while (rs.next()) System.out.println("  Result: " + rs.getObject(1));
            } catch (Exception e) {
                System.out.println("Test 2 (struct_pack :=) FAILED: " + e.getMessage());
            }
            
            // Test 3: Simple VALUES with columns
            try {
                ResultSet rs = stmt.executeQuery(
                    "SELECT * FROM (VALUES ('Alice', 30), ('Bob', 25)) AS t(name, age)"
                );
                System.out.println("Test 3 (Simple VALUES) PASSED");
                while (rs.next()) {
                    System.out.println("  Name: " + rs.getString(1) + ", Age: " + rs.getInt(2));
                }
            } catch (Exception e) {
                System.out.println("Test 3 (Simple VALUES) FAILED: " + e.getMessage());
            }
        }
    }
}
