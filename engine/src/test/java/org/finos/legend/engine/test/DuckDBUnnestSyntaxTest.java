package org.finos.legend.engine.test;

import java.sql.*;
import org.junit.jupiter.api.Test;

public class DuckDBUnnestSyntaxTest {

    @Test
    void testMultipleUnnestDifferentSizes() throws Exception {
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
                Statement stmt = conn.createStatement()) {

            // Multiple UNNEST in SELECT with arrays of different sizes
            String sql = """
                    SELECT item.name,
                           unnest(item.addresses).val AS addr,
                           unnest(item.values).val AS num
                    FROM (VALUES
                        ({'name': 'ok', 'addresses': [{'val': 'addr1'}, {'val': 'addr2'}], 'values': [{'val': 1}, {'val': 2}, {'val': 3}]})
                    ) AS t(item)
                    """;

            System.out.println("Multiple UNNEST in SELECT (different array sizes):\n" + sql);
            System.out.println("\nResults:");
            ResultSet rs = stmt.executeQuery(sql);
            int count = 0;
            while (rs.next()) {
                System.out.println("  Row " + (++count) + ": name=" + rs.getString(1) +
                        ", addr=" + rs.getString(2) +
                        ", num=" + rs.getObject(3));
            }
            System.out.println("Total rows: " + count);
            System.out.println("(2 addrs x 3 values should be 6 rows if cross-product, but DuckDB pads shorter array)");
        }
    }

    @Test
    void testUnnestWithCrossJoin() throws Exception {
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
                Statement stmt = conn.createStatement()) {

            // Use subquery to properly cross join
            String sql = """
                    SELECT sub.name, addrs.val AS addr, vals.val AS num
                    FROM (
                        SELECT item.name, item.addresses, item.values
                        FROM (VALUES
                            ({'name': 'ok', 'addresses': [{'val': 'addr1'}, {'val': 'addr2'}], 'values': [{'val': 1}, {'val': 2}, {'val': 3}]})
                        ) AS t(item)
                    ) AS sub,
                    LATERAL (SELECT UNNEST(sub.addresses)) AS addrs(val),
                    LATERAL (SELECT UNNEST(sub.values)) AS vals(val)
                    """;

            System.out.println("\n\nCROSS JOIN via LATERAL:\n" + sql);
            try {
                System.out.println("\nResults:");
                ResultSet rs = stmt.executeQuery(sql);
                int count = 0;
                while (rs.next()) {
                    System.out.println("  Row " + (++count) + ": " + rs.getString(1) + ", " + rs.getString(2) + ", "
                            + rs.getObject(3));
                }
                System.out.println("Total rows: " + count);
            } catch (SQLException e) {
                System.out.println("  Error: " + e.getMessage());
            }
        }
    }
}
