package com.gs.legend.test;

import org.junit.jupiter.api.Test;
import java.sql.*;

/**
 * Exploratory test: can we use read_json_objects() + ::VARIANT as a base table?
 */
class DuckDBVariantLoadTest {

    @Test
    void testReadJsonObjects() throws Exception {
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:")) {
            var tmpFile = java.nio.file.Files.createTempFile("test", ".json");
            java.nio.file.Files.writeString(tmpFile,
                    "[{\"firstName\":\"John\",\"age\":30},{\"firstName\":\"Jane\",\"age\":25}]");

            // Test A: JSON ->> works (current approach)
            System.out.println("=== A: JSON ->> field access ===");
            try (Statement stmt = conn.createStatement()) {
                ResultSet rs = stmt.executeQuery(
                    "SELECT (json->>'firstName') AS fn FROM read_json_objects('" + tmpFile + "')");
                while (rs.next()) System.out.println("  " + rs.getString(1));
            } catch (Exception e) { System.out.println("  ERROR: " + e.getMessage()); }

            // Test B: VARIANT dot notation
            System.out.println("=== B: VARIANT dot notation ===");
            try (Statement stmt = conn.createStatement()) {
                ResultSet rs = stmt.executeQuery(
                    "SELECT data.firstName FROM (SELECT json::VARIANT AS data FROM read_json_objects('" + tmpFile + "'))");
                while (rs.next()) System.out.println("  " + rs.getObject(1));
            } catch (Exception e) { System.out.println("  ERROR: " + e.getMessage()); }

            // Test C: read_json_objects as base table — keep as JSON, use ->>
            System.out.println("=== C: read_json_objects + ->> as base table ===");
            try (Statement stmt = conn.createStatement()) {
                ResultSet rs = stmt.executeQuery(
                    "SELECT CAST((t0.json->>'firstName') AS VARCHAR) AS fn, CAST((t0.json->>'age') AS BIGINT) AS age " +
                    "FROM read_json_objects('" + tmpFile + "') AS t0");
                while (rs.next()) System.out.println("  " + rs.getString(1) + ", " + rs.getLong(2));
            } catch (Exception e) { System.out.println("  ERROR: " + e.getMessage()); }

            // Test D: CTE + PreparedStatement (inline data)
            System.out.println("=== D: CTE + PreparedStatement ===");
            try (PreparedStatement ps = conn.prepareStatement("""
                    WITH "_json_t" AS (
                        SELECT unnest(CAST(? AS JSON[])) AS "data"
                    )
                    SELECT CAST(("data"->>'firstName') AS VARCHAR) AS fn, CAST(("data"->>'age') AS BIGINT) AS age
                    FROM "_json_t"
                """)) {
                ps.setString(1, "[{\"firstName\":\"John\",\"age\":30},{\"firstName\":\"Jane\",\"age\":25}]");
                ResultSet rs = ps.executeQuery();
                while (rs.next()) System.out.println("  " + rs.getString(1) + ", " + rs.getLong(2));
            } catch (Exception e) { System.out.println("  ERROR: " + e.getMessage()); }

            // Test E: VALUES + VARIANT (inline data)
            System.out.println("=== E: VALUES + VARIANT ===");
            try (Statement stmt = conn.createStatement()) {
                ResultSet rs = stmt.executeQuery("""
                    SELECT CAST(t0."data".firstName AS VARCHAR) AS fn, CAST(t0."data".age AS BIGINT) AS age
                    FROM (VALUES ('{\"firstName\":\"John\",\"age\":30}'::VARIANT),
                                 ('{\"firstName\":\"Jane\",\"age\":25}'::VARIANT)) AS t0("data")
                """);
                while (rs.next()) System.out.println("  " + rs.getString(1) + ", " + rs.getLong(2));
            } catch (Exception e) { System.out.println("  ERROR: " + e.getMessage()); }

            // Test F: read_json_objects -> VARIANT + dot notation
            System.out.println("=== F: read_json_objects -> VARIANT + dot notation ===");
            try (Statement stmt = conn.createStatement()) {
                ResultSet rs = stmt.executeQuery(
                    "SELECT CAST(t0.\"data\".firstName AS VARCHAR) AS fn, CAST(t0.\"data\".age AS BIGINT) AS age " +
                    "FROM (SELECT json::VARIANT AS \"data\" FROM read_json_objects('" + tmpFile + "')) AS t0");
                while (rs.next()) System.out.println("  " + rs.getString(1) + ", " + rs.getLong(2));
            } catch (Exception e) { System.out.println("  ERROR: " + e.getMessage()); }

            // Test G: VALUES + VARIANT + PreparedStatement
            System.out.println("=== G: VALUES + VARIANT + PreparedStatement ===");
            try (PreparedStatement ps = conn.prepareStatement("""
                    SELECT CAST(t0."data".firstName AS VARCHAR) AS fn, CAST(t0."data".age AS BIGINT) AS age
                    FROM (VALUES (CAST(? AS VARIANT))) AS t0("data")
                """)) {
                // Can we bind a JSON string that becomes multiple rows? No — one row per ?
                ps.setString(1, "{\"firstName\":\"John\",\"age\":30}");
                ResultSet rs = ps.executeQuery();
                while (rs.next()) System.out.println("  " + rs.getString(1) + ", " + rs.getLong(2));
            } catch (Exception e) { System.out.println("  ERROR: " + e.getMessage()); }

            // Test H: unnest VARIANT[] from PreparedStatement
            System.out.println("=== H: unnest VARIANT[] + PreparedStatement ===");
            try (PreparedStatement ps = conn.prepareStatement("""
                    SELECT CAST(t0."data".firstName AS VARCHAR) AS fn, CAST(t0."data".age AS BIGINT) AS age
                    FROM (SELECT unnest(CAST(? AS JSON[]))::VARIANT AS "data") AS t0
                """)) {
                ps.setString(1, "[{\"firstName\":\"John\",\"age\":30},{\"firstName\":\"Jane\",\"age\":25}]");
                ResultSet rs = ps.executeQuery();
                while (rs.next()) System.out.println("  " + rs.getString(1) + ", " + rs.getLong(2));
            } catch (Exception e) { System.out.println("  ERROR: " + e.getMessage()); }

            // Test I: DuckDB struct literal syntax in VALUES → VARIANT
            System.out.println("=== I: struct literal ::VARIANT in VALUES ===");
            try (Statement stmt = conn.createStatement()) {
                ResultSet rs = stmt.executeQuery("""
                    SELECT CAST(t0."data".firstName AS VARCHAR) AS fn, CAST(t0."data".age AS BIGINT) AS age
                    FROM (VALUES ({'firstName': 'John', 'age': 30}::VARIANT),
                                 ({'firstName': 'Jane', 'age': 25}::VARIANT)) AS t0("data")
                """);
                while (rs.next()) System.out.println("  " + rs.getString(1) + ", " + rs.getLong(2));
            } catch (Exception e) { System.out.println("  ERROR: " + e.getMessage()); }

            // Test J: struct literal without ::VARIANT (auto type)
            System.out.println("=== J: struct literal without cast ===");
            try (Statement stmt = conn.createStatement()) {
                ResultSet rs = stmt.executeQuery("""
                    SELECT t0."data".firstName, t0."data".age
                    FROM (VALUES ({'firstName': 'John', 'age': 30}),
                                 ({'firstName': 'Jane', 'age': 25})) AS t0("data")
                """);
                var meta = rs.getMetaData();
                System.out.println("  col1 type: " + meta.getColumnTypeName(1) + ", col2 type: " + meta.getColumnTypeName(2));
                while (rs.next()) System.out.println("  " + rs.getString(1) + ", " + rs.getLong(2));
            } catch (Exception e) { System.out.println("  ERROR: " + e.getMessage()); }

            // Test K: JSON string vs struct literal
            System.out.println("=== K: JSON string cast to VARIANT (previous E, re-verify) ===");
            try (Statement stmt = conn.createStatement()) {
                ResultSet rs = stmt.executeQuery("""
                    SELECT t0."data".firstName
                    FROM (VALUES ('{"firstName":"John"}'::JSON::VARIANT)) AS t0("data")
                """);
                while (rs.next()) System.out.println("  " + rs.getObject(1));
            } catch (Exception e) { System.out.println("  ERROR: " + e.getMessage()); }

            // Test L: JSON literal cast to JSON then VARIANT
            System.out.println("=== L: '...'::JSON then dot access (no VARIANT cast) ===");
            try (Statement stmt = conn.createStatement()) {
                ResultSet rs = stmt.executeQuery("""
                    SELECT t0."data"->>'firstName'
                    FROM (VALUES ('{"firstName":"John"}'::JSON)) AS t0("data")
                """);
                while (rs.next()) System.out.println("  " + rs.getObject(1));
            } catch (Exception e) { System.out.println("  ERROR: " + e.getMessage()); }

            // Test M: ::JSON::VARIANT in VALUES with full CAST field access
            System.out.println("=== M: VALUES + ::JSON::VARIANT + CAST field access ===");
            try (Statement stmt = conn.createStatement()) {
                ResultSet rs = stmt.executeQuery("""
                    SELECT CAST(t0."data".firstName AS VARCHAR) AS fn, CAST(t0."data".age AS BIGINT) AS age
                    FROM (VALUES ('{"firstName":"John","age":30}'::JSON::VARIANT),
                                 ('{"firstName":"Jane","age":25}'::JSON::VARIANT)) AS t0("data")
                """);
                while (rs.next()) System.out.println("  " + rs.getString(1) + ", " + rs.getLong(2));
            } catch (Exception e) { System.out.println("  ERROR: " + e.getMessage()); }

            // Test N: VALUES + ::JSON::VARIANT with PreparedStatement
            System.out.println("=== N: VALUES + CAST(? AS JSON)::VARIANT + PreparedStatement ===");
            try (PreparedStatement ps = conn.prepareStatement("""
                    SELECT CAST(t0."data".firstName AS VARCHAR) AS fn, CAST(t0."data".age AS BIGINT) AS age
                    FROM (VALUES (CAST(? AS JSON)::VARIANT), (CAST(? AS JSON)::VARIANT)) AS t0("data")
                """)) {
                ps.setString(1, "{\"firstName\":\"John\",\"age\":30}");
                ps.setString(2, "{\"firstName\":\"Jane\",\"age\":25}");
                ResultSet rs = ps.executeQuery();
                while (rs.next()) System.out.println("  " + rs.getString(1) + ", " + rs.getLong(2));
            } catch (Exception e) { System.out.println("  ERROR: " + e.getMessage()); }

            // Test O: unnest + ::JSON::VARIANT (single ? for array)
            System.out.println("=== O: unnest JSON[] then ::VARIANT (compare to H) ===");
            try (PreparedStatement ps = conn.prepareStatement("""
                    SELECT CAST(t0."data".firstName AS VARCHAR) AS fn, CAST(t0."data".age AS BIGINT) AS age
                    FROM (SELECT unnest(CAST(? AS JSON[]))::VARIANT AS "data") AS t0
                """)) {
                ps.setString(1, "[{\"firstName\":\"John\",\"age\":30},{\"firstName\":\"Jane\",\"age\":25}]");
                ResultSet rs = ps.executeQuery();
                while (rs.next()) System.out.println("  " + rs.getString(1) + ", " + rs.getLong(2));
            } catch (Exception e) { System.out.println("  ERROR: " + e.getMessage()); }

            // Test P: unnest directly in FROM (no subquery wrapper)
            System.out.println("=== P: FROM unnest(JSON[])::VARIANT ===");
            try (Statement stmt = conn.createStatement()) {
                ResultSet rs = stmt.executeQuery("""
                    SELECT CAST(t0."data".firstName AS VARCHAR) AS fn, CAST(t0."data".age AS BIGINT) AS age
                    FROM unnest(CAST('[{"firstName":"John","age":30},{"firstName":"Jane","age":25}]' AS JSON[]))::VARIANT AS t0("data")
                """);
                while (rs.next()) System.out.println("  " + rs.getString(1) + ", " + rs.getLong(2));
            } catch (Exception e) { System.out.println("  ERROR: " + e.getMessage()); }

            // Test Q: FROM unnest without VARIANT cast
            System.out.println("=== Q: FROM unnest(JSON[]) no VARIANT ===");
            try (Statement stmt = conn.createStatement()) {
                ResultSet rs = stmt.executeQuery("""
                    SELECT t0."data"->>'firstName' AS fn
                    FROM unnest(CAST('[{"firstName":"John"},{"firstName":"Jane"}]' AS JSON[])) AS t0("data")
                """);
                while (rs.next()) System.out.println("  " + rs.getString(1));
            } catch (Exception e) { System.out.println("  ERROR: " + e.getMessage()); }

            // Test R: FROM unnest with VARIANT via subquery (minimal)
            System.out.println("=== R: FROM (SELECT unnest(...)::VARIANT AS data) ===");
            try (Statement stmt = conn.createStatement()) {
                ResultSet rs = stmt.executeQuery("""
                    SELECT CAST(t0."data".firstName AS VARCHAR) AS fn, CAST(t0."data".age AS BIGINT) AS age
                    FROM (SELECT unnest(CAST('[{"firstName":"John","age":30},{"firstName":"Jane","age":25}]' AS JSON[]))::VARIANT AS "data") AS t0
                """);
                while (rs.next()) System.out.println("  " + rs.getString(1) + ", " + rs.getLong(2));
            } catch (Exception e) { System.out.println("  ERROR: " + e.getMessage()); }

            java.nio.file.Files.delete(tmpFile);
        }
    }
}
