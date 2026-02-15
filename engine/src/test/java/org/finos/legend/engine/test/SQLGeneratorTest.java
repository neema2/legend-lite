package org.finos.legend.engine.test;

import org.finos.legend.engine.plan.*;
import org.finos.legend.engine.store.*;
import org.finos.legend.engine.transpiler.DuckDBDialect;
import org.finos.legend.engine.transpiler.SQLGenerator;
import org.finos.legend.engine.transpiler.SQLiteDialect;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for SQLGenerator focusing on GroupByNode and aggregation
 * generation.
 */
class SQLGeneratorTest {

        private SQLGenerator sqlGenerator;

        @BeforeEach
        void setUp() {
                sqlGenerator = new SQLGenerator(DuckDBDialect.INSTANCE);
        }

        // ==================== Cross-Dialect Compatibility ====================

        @Test
        @DisplayName("SQL generated is compatible across dialects for standard queries")
        void testCrossDialectCompatibility() {
                // GIVEN: A simple projection IR (equivalent to
                // Person.all()->filter()->project())
                Table personTable = new Table("T_PERSON", List.of(
                                Column.required("ID", SqlDataType.INTEGER),
                                Column.required("FIRST_NAME", SqlDataType.VARCHAR),
                                Column.required("LAST_NAME", SqlDataType.VARCHAR)));

                TableNode tableNode = new TableNode(personTable, "t0");

                // Filter: lastName == 'Smith'
                FilterNode filterNode = new FilterNode(tableNode,
                                ComparisonExpression.equals(
                                                ColumnReference.of("t0", "LAST_NAME", GenericType.Primitive.ANY),
                                                Literal.string("Smith")));

                // Project: firstName, lastName
                ProjectNode projectNode = new ProjectNode(filterNode, List.of(
                                Projection.column("t0", "FIRST_NAME", "firstName", GenericType.Primitive.STRING),
                                Projection.column("t0", "LAST_NAME", "lastName", GenericType.Primitive.STRING)));

                // WHEN: We generate SQL with both dialects
                SQLGenerator duckdbGen = new SQLGenerator(DuckDBDialect.INSTANCE);
                SQLGenerator sqliteGen = new SQLGenerator(SQLiteDialect.INSTANCE);

                String duckdbSql = duckdbGen.generate(projectNode);
                String sqliteSql = sqliteGen.generate(projectNode);

                System.out.println("DuckDB SQL: " + duckdbSql);
                System.out.println("SQLite SQL: " + sqliteSql);

                // THEN: For standard queries, they should be identical
                assertEquals(duckdbSql, sqliteSql,
                                "Standard queries should generate identical SQL across dialects");
        }

        // ==================== GroupByNode Tests ====================

        @Test
        @DisplayName("GroupByNode generates SQL with GROUP BY clause")
        void testGroupByNodeGeneratesSql() {
                // GIVEN: A simple table with a GroupByNode on top
                Table personTable = new Table("T_PERSON", List.of(
                                Column.required("ID", SqlDataType.INTEGER),
                                Column.required("FIRST_NAME", SqlDataType.VARCHAR),
                                Column.required("AGE_VAL", SqlDataType.INTEGER),
                                Column.required("DEPARTMENT", SqlDataType.VARCHAR),
                                Column.required("SALARY", SqlDataType.INTEGER)));

                TableNode tableNode = new TableNode(personTable, "t0");

                // GroupBy department, SUM salary
                GroupByNode groupByNode = new GroupByNode(
                                tableNode,
                                List.of("DEPARTMENT"),
                                List.of(new GroupByNode.AggregateProjection(
                                                "totalSalary",
                                                "SALARY",
                                                AggregateExpression.AggregateFunction.SUM)));

                // WHEN: We generate SQL
                String sql = sqlGenerator.generate(groupByNode);

                // THEN: SQL contains GROUP BY clause
                System.out.println("Generated GroupBy SQL: " + sql);

                assertTrue(sql.contains("GROUP BY"), "SQL should contain GROUP BY");
                assertTrue(sql.contains("SUM"), "SQL should contain SUM aggregate");
                assertTrue(sql.contains("\"totalSalary\""), "SQL should contain alias");
                assertTrue(sql.contains("\"DEPARTMENT\""), "SQL should contain group column");
        }

        @Test
        @DisplayName("GroupByNode with multiple aggregations")
        void testGroupByMultipleAggregations() {
                // GIVEN: Table with GroupBy and multiple aggregations
                Table personTable = new Table("T_PERSON", List.of(
                                Column.required("DEPARTMENT", SqlDataType.VARCHAR),
                                Column.required("SALARY", SqlDataType.INTEGER),
                                Column.required("ID", SqlDataType.INTEGER)));

                TableNode tableNode = new TableNode(personTable, "t0");

                GroupByNode groupByNode = new GroupByNode(
                                tableNode,
                                List.of("DEPARTMENT"),
                                List.of(
                                                new GroupByNode.AggregateProjection("totalSalary", "SALARY",
                                                                AggregateExpression.AggregateFunction.SUM),
                                                new GroupByNode.AggregateProjection("avgSalary", "SALARY",
                                                                AggregateExpression.AggregateFunction.AVG),
                                                new GroupByNode.AggregateProjection("headcount", "ID",
                                                                AggregateExpression.AggregateFunction.COUNT)));

                // WHEN: We generate SQL
                String sql = sqlGenerator.generate(groupByNode);

                // THEN: SQL contains all aggregations
                System.out.println("Generated Multi-Agg SQL: " + sql);

                assertTrue(sql.contains("SUM"), "SQL should contain SUM");
                assertTrue(sql.contains("AVG"), "SQL should contain AVG");
                assertTrue(sql.contains("COUNT"), "SQL should contain COUNT");
                assertTrue(sql.contains("\"totalSalary\""), "SQL should contain totalSalary alias");
                assertTrue(sql.contains("\"avgSalary\""), "SQL should contain avgSalary alias");
                assertTrue(sql.contains("\"headcount\""), "SQL should contain headcount alias");
        }

        @Test
        @DisplayName("GroupByNode with FilterNode source")
        void testGroupByWithFilter() {
                // GIVEN: Filter -> GroupBy chain
                Table personTable = new Table("T_PERSON", List.of(
                                Column.required("DEPARTMENT", SqlDataType.VARCHAR),
                                Column.required("SALARY", SqlDataType.INTEGER),
                                Column.required("AGE_VAL", SqlDataType.INTEGER)));

                TableNode tableNode = new TableNode(personTable, "t0");

                // Filter: age > 18
                Expression ageFilter = ComparisonExpression.greaterThan(
                                ColumnReference.of("t0", "AGE_VAL", GenericType.Primitive.ANY),
                                Literal.integer(18));
                FilterNode filterNode = new FilterNode(tableNode, ageFilter);

                // GroupBy on filtered result
                GroupByNode groupByNode = new GroupByNode(
                                filterNode,
                                List.of("DEPARTMENT"),
                                List.of(new GroupByNode.AggregateProjection("totalSalary", "SALARY",
                                                AggregateExpression.AggregateFunction.SUM)));

                // WHEN: We generate SQL
                String sql = sqlGenerator.generate(groupByNode);

                // THEN: SQL contains both WHERE and GROUP BY
                System.out.println("Generated Filter+GroupBy SQL: " + sql);

                assertTrue(sql.contains("WHERE"), "SQL should contain WHERE from filter");
                assertTrue(sql.contains("GROUP BY"), "SQL should contain GROUP BY");
                assertTrue(sql.contains("SUM"), "SQL should contain SUM aggregate");
        }

        @Test
        @DisplayName("GroupByNode with multiple group columns")
        void testGroupByMultipleColumns() {
                // GIVEN: GroupBy on two columns
                Table personTable = new Table("T_PERSON", List.of(
                                Column.required("DEPARTMENT", SqlDataType.VARCHAR),
                                Column.required("CITY", SqlDataType.VARCHAR),
                                Column.required("SALARY", SqlDataType.INTEGER)));

                TableNode tableNode = new TableNode(personTable, "t0");

                GroupByNode groupByNode = new GroupByNode(
                                tableNode,
                                List.of("DEPARTMENT", "CITY"),
                                List.of(new GroupByNode.AggregateProjection("totalSalary", "SALARY",
                                                AggregateExpression.AggregateFunction.SUM)));

                // WHEN: We generate SQL
                String sql = sqlGenerator.generate(groupByNode);

                // THEN: SQL groups by both columns
                System.out.println("Generated Multi-Column GroupBy SQL: " + sql);

                assertTrue(sql.contains("\"DEPARTMENT\""), "SQL should contain DEPARTMENT");
                assertTrue(sql.contains("\"CITY\""), "SQL should contain CITY");
                assertTrue(sql.contains("GROUP BY"), "SQL should contain GROUP BY");
        }

        // ==================== JoinNode Tests ====================

        @Test
        @DisplayName("JoinNode generates INNER JOIN SQL")
        void testJoinNodeInnerJoin() {
                Table personTable = new Table("T_PERSON", List.of(
                                Column.required("ID", SqlDataType.INTEGER),
                                Column.required("FIRST_NAME", SqlDataType.VARCHAR)));

                Table addressTable = new Table("T_ADDRESS", List.of(
                                Column.required("PERSON_ID", SqlDataType.INTEGER),
                                Column.required("STREET", SqlDataType.VARCHAR)));

                TableNode personNode = new TableNode(personTable, "p");
                TableNode addressNode = new TableNode(addressTable, "a");

                Expression joinCondition = ComparisonExpression.equals(
                                ColumnReference.of("p", "ID", GenericType.Primitive.ANY),
                                ColumnReference.of("a", "PERSON_ID", GenericType.Primitive.ANY));

                JoinNode joinNode = JoinNode.inner(personNode, addressNode, joinCondition);

                ProjectNode projectNode = new ProjectNode(joinNode, List.of(
                                Projection.column("p", "FIRST_NAME", "firstName", GenericType.Primitive.STRING),
                                Projection.column("a", "STREET", "street", GenericType.Primitive.STRING)));

                String sql = sqlGenerator.generate(projectNode);

                assertTrue(sql.contains("INNER JOIN"), "SQL should contain INNER JOIN");
                assertTrue(sql.contains("ON"), "SQL should have ON clause");
        }

        @Test
        @DisplayName("JoinNode generates LEFT OUTER JOIN SQL")
        void testJoinNodeLeftOuterJoin() {
                Table personTable = new Table("T_PERSON", List.of(
                                Column.required("ID", SqlDataType.INTEGER),
                                Column.required("FIRST_NAME", SqlDataType.VARCHAR)));

                Table addressTable = new Table("T_ADDRESS", List.of(
                                Column.required("PERSON_ID", SqlDataType.INTEGER),
                                Column.required("STREET", SqlDataType.VARCHAR)));

                TableNode personNode = new TableNode(personTable, "p");
                TableNode addressNode = new TableNode(addressTable, "a");

                Expression joinCondition = ComparisonExpression.equals(
                                ColumnReference.of("p", "ID", GenericType.Primitive.ANY),
                                ColumnReference.of("a", "PERSON_ID", GenericType.Primitive.ANY));

                JoinNode joinNode = JoinNode.leftOuter(personNode, addressNode, joinCondition);

                ProjectNode projectNode = new ProjectNode(joinNode, List.of(
                                Projection.column("p", "FIRST_NAME", "firstName", GenericType.Primitive.STRING),
                                Projection.column("a", "STREET", "street", GenericType.Primitive.STRING)));

                String sql = sqlGenerator.generate(projectNode);

                assertTrue(sql.contains("LEFT OUTER JOIN"), "SQL should contain LEFT OUTER JOIN");
        }

        // ==================== AggregateExpression Tests ====================

        @Test
        @DisplayName("AggregateExpression generates correct SQL function")
        void testAggregateExpressionFunctions() {
                // Test each aggregate function
                ColumnReference col = ColumnReference.of("t0", "SALARY", GenericType.Primitive.ANY);

                assertEquals("SUM(\"t0\".\"SALARY\")",
                                sqlGenerator.generateExpression(new AggregateExpression(
                                                AggregateExpression.AggregateFunction.SUM, col)));

                assertEquals("COUNT(\"t0\".\"SALARY\")",
                                sqlGenerator.generateExpression(new AggregateExpression(
                                                AggregateExpression.AggregateFunction.COUNT, col)));

                assertEquals("AVG(\"t0\".\"SALARY\")",
                                sqlGenerator.generateExpression(new AggregateExpression(
                                                AggregateExpression.AggregateFunction.AVG, col)));

                assertEquals("MIN(\"t0\".\"SALARY\")",
                                sqlGenerator.generateExpression(new AggregateExpression(
                                                AggregateExpression.AggregateFunction.MIN, col)));

                assertEquals("MAX(\"t0\".\"SALARY\")",
                                sqlGenerator.generateExpression(new AggregateExpression(
                                                AggregateExpression.AggregateFunction.MAX, col)));
        }
}
