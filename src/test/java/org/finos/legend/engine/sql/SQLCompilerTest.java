package org.finos.legend.engine.sql;

import org.finos.legend.engine.plan.*;
import org.finos.legend.engine.sql.ast.*;
import org.finos.legend.engine.store.Column;
import org.finos.legend.engine.store.SqlDataType;
import org.finos.legend.engine.store.Table;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for SQLCompiler - transforms SQL AST to RelationNode IR.
 */
@DisplayName("SQLCompiler Tests")
class SQLCompilerTest {

    private SQLCompiler compiler;
    private Map<String, Table> tables;

    @BeforeEach
    void setUp() {
        // Set up test tables using correct API
        Table employees = new Table("", "T_EMPLOYEE", List.of(
            Column.required("id", SqlDataType.INTEGER),
            Column.required("name", SqlDataType.VARCHAR),
            Column.required("department", SqlDataType.VARCHAR),
            Column.required("salary", SqlDataType.INTEGER),
            Column.nullable("manager_id", SqlDataType.INTEGER)
        ));
        
        Table departments = new Table("", "T_DEPARTMENT", List.of(
            Column.required("id", SqlDataType.INTEGER),
            Column.required("name", SqlDataType.VARCHAR)
        ));
        
        tables = Map.of(
            "employees", employees,
            "departments", departments,
            "t_employee", employees,
            "t_department", departments
        );
        
        compiler = new SQLCompiler((schema, tableName) -> tables.get(tableName.toLowerCase()));
    }

    private SelectStatement parse(String sql) {
        return new SelectParser(sql).parseSelect();
    }

    // ==================== Basic SELECT ====================

    @Nested
    @DisplayName("Basic SELECT")
    class BasicSelectTests {

        @Test
        @DisplayName("SELECT * FROM table")
        void testSelectStar() {
            String sql = "SELECT * FROM employees";
            RelationNode ir = compiler.compile(parse(sql));

            assertInstanceOf(ProjectNode.class, ir);
            ProjectNode project = (ProjectNode) ir;
            assertEquals(1, project.projections().size());
            assertEquals("*", project.projections().get(0).alias());
        }

        @Test
        @DisplayName("SELECT columns FROM table")
        void testSelectColumns() {
            String sql = "SELECT name, salary FROM employees";
            RelationNode ir = compiler.compile(parse(sql));

            assertInstanceOf(ProjectNode.class, ir);
            ProjectNode project = (ProjectNode) ir;
            assertEquals(2, project.projections().size());
            assertEquals("name", project.projections().get(0).alias());
            assertEquals("salary", project.projections().get(1).alias());
        }

        @Test
        @DisplayName("SELECT with aliases")
        void testSelectWithAliases() {
            String sql = "SELECT name AS emp_name, salary AS pay FROM employees";
            RelationNode ir = compiler.compile(parse(sql));

            ProjectNode project = (ProjectNode) ir;
            assertEquals("emp_name", project.projections().get(0).alias());
            assertEquals("pay", project.projections().get(1).alias());
        }
    }

    // ==================== WHERE Clause ====================

    @Nested
    @DisplayName("WHERE Clause")
    class WhereTests {

        @Test
        @DisplayName("WHERE with equality")
        void testWhereEquals() {
            String sql = "SELECT * FROM employees WHERE department = 'Engineering'";
            RelationNode ir = compiler.compile(parse(sql));

            // Structure: ProjectNode <- FilterNode <- TableNode
            assertInstanceOf(ProjectNode.class, ir);
            ProjectNode project = (ProjectNode) ir;
            
            assertInstanceOf(FilterNode.class, project.source());
            FilterNode filter = (FilterNode) project.source();
            
            assertInstanceOf(ComparisonExpression.class, filter.condition());
        }

        @Test
        @DisplayName("WHERE with AND")
        void testWhereAnd() {
            String sql = "SELECT * FROM employees WHERE department = 'Eng' AND salary > 100000";
            RelationNode ir = compiler.compile(parse(sql));

            ProjectNode project = (ProjectNode) ir;
            FilterNode filter = (FilterNode) project.source();
            
            assertInstanceOf(LogicalExpression.class, filter.condition());
            LogicalExpression logical = (LogicalExpression) filter.condition();
            assertEquals(LogicalExpression.LogicalOperator.AND, logical.operator());
        }

        @Test
        @DisplayName("WHERE IS NULL")
        void testWhereIsNull() {
            String sql = "SELECT * FROM employees WHERE manager_id IS NULL";
            RelationNode ir = compiler.compile(parse(sql));

            ProjectNode project = (ProjectNode) ir;
            FilterNode filter = (FilterNode) project.source();
            
            assertInstanceOf(ComparisonExpression.class, filter.condition());
            ComparisonExpression cmp = (ComparisonExpression) filter.condition();
            assertEquals(ComparisonExpression.ComparisonOperator.IS_NULL, cmp.operator());
        }
    }

    // ==================== JOINs ====================

    @Nested
    @DisplayName("JOIN Operations")
    class JoinTests {

        @Test
        @DisplayName("INNER JOIN")
        void testInnerJoin() {
            String sql = "SELECT e.name, d.name FROM employees e JOIN departments d ON e.department = d.id";
            RelationNode ir = compiler.compile(parse(sql));

            // Structure: ProjectNode <- JoinNode
            assertInstanceOf(ProjectNode.class, ir);
            ProjectNode project = (ProjectNode) ir;
            
            assertInstanceOf(JoinNode.class, project.source());
            JoinNode join = (JoinNode) project.source();
            assertEquals(JoinNode.JoinType.INNER, join.joinType());
        }

        @Test
        @DisplayName("LEFT JOIN")
        void testLeftJoin() {
            String sql = "SELECT * FROM employees e LEFT JOIN departments d ON e.department = d.id";
            RelationNode ir = compiler.compile(parse(sql));

            ProjectNode project = (ProjectNode) ir;
            JoinNode join = (JoinNode) project.source();
            assertEquals(JoinNode.JoinType.LEFT_OUTER, join.joinType());
        }
    }

    // ==================== ORDER BY ====================

    @Nested
    @DisplayName("ORDER BY")
    class OrderByTests {

        @Test
        @DisplayName("ORDER BY column")
        void testOrderByColumn() {
            String sql = "SELECT * FROM employees ORDER BY name";
            RelationNode ir = compiler.compile(parse(sql));

            // Structure: SortNode <- ProjectNode <- TableNode
            assertInstanceOf(SortNode.class, ir);
            SortNode sort = (SortNode) ir;
            assertEquals(1, sort.columns().size());
            assertEquals("name", sort.columns().get(0).column());
            assertEquals(SortNode.SortDirection.ASC, sort.columns().get(0).direction());
        }

        @Test
        @DisplayName("ORDER BY DESC")
        void testOrderByDesc() {
            String sql = "SELECT * FROM employees ORDER BY salary DESC";
            RelationNode ir = compiler.compile(parse(sql));

            SortNode sort = (SortNode) ir;
            assertEquals(SortNode.SortDirection.DESC, sort.columns().get(0).direction());
        }
    }

    // ==================== LIMIT ====================

    @Nested
    @DisplayName("LIMIT and OFFSET")
    class LimitTests {

        @Test
        @DisplayName("LIMIT only")
        void testLimitOnly() {
            String sql = "SELECT * FROM employees LIMIT 10";
            RelationNode ir = compiler.compile(parse(sql));

            assertInstanceOf(LimitNode.class, ir);
            LimitNode limit = (LimitNode) ir;
            assertEquals(10, limit.limit());
            assertEquals(0, limit.offset());
        }

        @Test
        @DisplayName("LIMIT with OFFSET")
        void testLimitOffset() {
            String sql = "SELECT * FROM employees LIMIT 10 OFFSET 20";
            RelationNode ir = compiler.compile(parse(sql));

            LimitNode limit = (LimitNode) ir;
            assertEquals(10, limit.limit());
            assertEquals(20, limit.offset());
        }
    }

    // ==================== Expressions ====================

    @Nested
    @DisplayName("Expression Compilation")
    class ExpressionTests {

        @Test
        @DisplayName("Arithmetic expressions")
        void testArithmeticExpressions() {
            String sql = "SELECT salary + 1000 AS bonus FROM employees";
            RelationNode ir = compiler.compile(parse(sql));

            ProjectNode project = (ProjectNode) ir;
            assertInstanceOf(ArithmeticExpression.class, project.projections().get(0).expression());
        }

        @Test
        @DisplayName("String concatenation")
        void testStringConcat() {
            String sql = "SELECT name || ' - ' || department AS label FROM employees";
            RelationNode ir = compiler.compile(parse(sql));

            ProjectNode project = (ProjectNode) ir;
            assertInstanceOf(ConcatExpression.class, project.projections().get(0).expression());
        }

        @Test
        @DisplayName("Aggregate function COUNT")
        void testCountAggregate() {
            String sql = "SELECT COUNT(id) FROM employees";
            RelationNode ir = compiler.compile(parse(sql));

            ProjectNode project = (ProjectNode) ir;
            assertInstanceOf(AggregateExpression.class, project.projections().get(0).expression());
            AggregateExpression agg = (AggregateExpression) project.projections().get(0).expression();
            assertEquals(AggregateExpression.AggregateFunction.COUNT, agg.function());
        }
    }
}
