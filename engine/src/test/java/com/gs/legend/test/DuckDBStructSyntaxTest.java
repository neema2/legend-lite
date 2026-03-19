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
