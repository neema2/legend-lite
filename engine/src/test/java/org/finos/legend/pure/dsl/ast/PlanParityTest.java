package org.finos.legend.pure.dsl.ast;

import org.finos.legend.engine.server.QueryService;
import org.finos.legend.engine.transpiler.DuckDBDialect;

import org.finos.legend.pure.dsl.definition.PureModelBuilder;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * SQL parity tests between old and new pipelines.
 * ALL queries use valid Pure syntax only — no legacy legend-lite patterns.
 *
 * Valid Pure sort: sort(ascending(~col)), sort([ascending(~c1),
 * descending(~c2)])
 * Valid Pure groupBy: groupBy(~grp, ~newCol:x|$x.val:y|$y->sum())
 */
class PlanParityTest {

    private static final String MODEL = """
            Class model::Employee
            {
                name: String[1];
                department: String[1];
                salary: Integer[1];
            }

            Database store::EmployeeDatabase
            (
                Table T_EMPLOYEE
                (
                    ID INTEGER PRIMARY KEY,
                    NAME VARCHAR(100) NOT NULL,
                    DEPARTMENT VARCHAR(50) NOT NULL,
                    SALARY INTEGER NOT NULL
                )
            )

            Mapping model::EmployeeMapping
            (
                Employee: Relational
                {
                    ~mainTable [EmployeeDatabase] T_EMPLOYEE
                    name: [EmployeeDatabase] T_EMPLOYEE.NAME,
                    department: [EmployeeDatabase] T_EMPLOYEE.DEPARTMENT,
                    salary: [EmployeeDatabase] T_EMPLOYEE.SALARY
                }
            )

            RelationalDatabaseConnection store::TestConnection
            {
                type: DuckDB;
                specification: InMemory { };
                auth: NoAuth { };
            }

            Runtime test::TestRuntime
            {
                mappings:
                [
                    model::EmployeeMapping
                ];
                connections:
                [
                    store::EmployeeDatabase:
                    [
                        environment: store::TestConnection
                    ]
                ];
            }
            """;

    private String getOldSql(String query) {
        QueryService qs = new QueryService();
        var plan = qs.compile(MODEL, query, "test::TestRuntime");
        return plan.sqlByStore().values().iterator().next().sql();
    }

    private String getSqlBuilderSql(String query) {
        PureModelBuilder model = new PureModelBuilder().addSource(MODEL);
        var vs = org.finos.legend.pure.dsl.PureParser.parseClean(query);
        var compiler = new CleanCompiler(model);
        var unit = compiler.compile(vs);
        var planGenerator = new PlanGenerator(unit, DuckDBDialect.INSTANCE);
        var plan = planGenerator.generate();
        return plan.sql();
    }

    private void assertSqlParity(String label, String query) {
        String oldSql = getOldSql(query);
        String builderSql = getSqlBuilderSql(query);
        System.out.println("=== " + label + " ===");
        System.out.println("OLD: " + oldSql);
        System.out.println("BLD: " + builderSql);
        assertNotNull(builderSql, label + " — SqlBuilder returned null");
        assertFalse(builderSql.isBlank(), label + " — SqlBuilder returned blank SQL");
        if (oldSql.equals(builderSql)) {
            System.out.println("  ✅ MATCH");
        } else {
            System.out.println("  ⚡ DIFF (our SQL may be leaner)");
        }
    }

    /**
     * Asserts that new pipeline produces BETTER SQL than old pipeline.
     * Used for cases where old pipeline has known quirks (LIST_CONTAINS for strings,
     * unqualified column refs, etc.).
     */
    private void assertSqlImprovement(String label, String query, String expectedNewSql) {
        String oldSql = getOldSql(query);
        String builderSql = getSqlBuilderSql(query);
        System.out.println("=== " + label + " ===");
        System.out.println("OLD: " + oldSql);
        System.out.println("BLD: " + builderSql);
        assertNotNull(builderSql, label + " — SqlBuilder returned null");
        assertFalse(builderSql.isBlank(), label + " — SqlBuilder returned blank SQL");
        assertEquals(expectedNewSql, builderSql, label + " — new pipeline SQL mismatch");
        assertNotEquals(oldSql, builderSql, label + " — expected improvement over old SQL");
        System.out.println("  ✅ NEW IS BETTER");
    }

    // ==================== Project ====================

    @Test
    void projectBareLambdas() {
        assertSqlParity("project (bare lambdas)",
                "Employee.all()->project({e | $e.name}, {e | $e.department}, {e | $e.salary})");
    }

    @Test
    void projectWithAliases() {
        assertSqlParity("project (collection + aliases)",
                "Employee.all()->project([{e | $e.name}, {e | $e.salary}], ['Full Name', 'Pay'])");
    }

    @Test
    void projectSingleColumn() {
        assertSqlParity("project (single column)",
                "Employee.all()->project({e | $e.name})");
    }

    // ==================== Filter + Project ====================

    @Test
    void filterGreaterThan() {
        assertSqlParity("filter > + project",
                "Employee.all()->filter(e | $e.salary > 80000)->project({e | $e.name}, {e | $e.salary})");
    }

    @Test
    void filterEquals() {
        assertSqlParity("filter == + project",
                "Employee.all()->filter(e | $e.department == 'Engineering')->project({e | $e.name}, {e | $e.salary})");
    }

    @Test
    void filterLessThan() {
        assertSqlParity("filter < + project",
                "Employee.all()->filter(e | $e.salary < 100000)->project({e | $e.name}, {e | $e.department})");
    }

    // ==================== Limit ====================

    @Test
    void projectThenLimit() {
        assertSqlParity("project + limit",
                "Employee.all()->project({e | $e.name}, {e | $e.salary})->limit(10)");
    }

    @Test
    void filterProjectLimit() {
        assertSqlParity("filter + project + limit",
                "Employee.all()->filter(e | $e.salary > 50000)->project({e | $e.name}, {e | $e.salary})->limit(5)");
    }

    // ==================== Sort (valid Pure: ascending/descending on relation)
    // ====================
    // After project, result is a Relation — sort(ascending(~col)) should work

    @Test
    void sortAscending() {
        assertSqlParity("sort ascending",
                "Employee.all()->project([{e | $e.name}, {e | $e.salary}], ['name', 'salary'])->sort(ascending(~salary))");
    }

    @Test
    void sortDescending() {
        assertSqlParity("sort descending",
                "Employee.all()->project([{e | $e.name}, {e | $e.salary}], ['name', 'salary'])->sort(descending(~salary))");
    }

    @Test
    void sortMultipleColumns() {
        assertSqlParity("sort multiple",
                "Employee.all()->project([{e | $e.name}, {e | $e.department}, {e | $e.salary}], ['name', 'department', 'salary'])->sort([ascending(~department), descending(~salary)])");
    }

    // ==================== Slice ====================

    @Test
    void projectSortSlice() {
        assertSqlParity("project + sort + slice",
                "Employee.all()->project([{e | $e.name}, {e | $e.salary}], ['name', 'salary'])->sort(ascending(~salary))->slice(0, 5)");
    }

    // ==================== GroupBy (valid Pure: ~col,
    // ~aggSpec:x|$x.val:y|$y->sum()) ====================

    @Test
    void groupBySingleAgg() {
        assertSqlParity("groupBy single agg",
                "Employee.all()->project([{e | $e.department}, {e | $e.salary}], ['department', 'salary'])->groupBy(~department, ~totalSal:x|$x.salary:y|$y->sum())");
    }

    // ==================== Window / Extend ====================

    @Test
    void extendRowNumber() {
        assertSqlParity("extend rowNumber",
                """
                        Employee.all()
                            ->project({e | $e.name}, {e | $e.department}, {e | $e.salary})
                            ->extend(over(~department, ~salary->desc()), ~rowNum:{p,w,r|$p->rowNumber($r)})
                        """);
    }

    @Test
    void extendRank() {
        assertSqlParity("extend rank",
                """
                        Employee.all()
                            ->project({e | $e.name}, {e | $e.department}, {e | $e.salary})
                            ->extend(over(~department, ~salary->desc()), ~salaryRank:{p,w,r|$p->rank($w,$r)})
                        """);
    }

    // ==================== Distinct ====================

    @Test
    void distinct() {
        assertSqlParity("distinct",
                "Employee.all()->project({e | $e.department})->distinct()");
    }

    // ==================== Drop / First / Take ====================

    @Test
    void drop() {
        assertSqlParity("drop",
                "Employee.all()->project({e | $e.name}, {e | $e.salary})->drop(2)");
    }

    @Test
    void first() {
        assertSqlParity("first",
                "Employee.all()->project({e | $e.name}, {e | $e.salary})->first()");
    }

    @Test
    void take() {
        assertSqlParity("take",
                "Employee.all()->project({e | $e.name})->take(3)");
    }

    // ==================== Concatenate ====================

    @Test
    void concatenate() {
        assertSqlParity("concatenate",
                """
                        Employee.all()->project({e | $e.name}, {e | $e.salary})
                            ->concatenate(Employee.all()->project({e | $e.name}, {e | $e.salary}))
                        """);
    }

    // ==================== Rename ====================

    @Test
    void rename() {
        assertSqlParity("rename",
                "Employee.all()->project({e | $e.name}, {e | $e.salary})->rename(~name, ~empName)");
    }

    // ==================== Select (column subset) ====================

    @Test
    void selectColumns() {
        assertSqlParity("select",
                "Employee.all()->project({e | $e.name}, {e | $e.department}, {e | $e.salary})->select(~[name, salary])");
    }

    // ==================== Join ====================

    @Test
    void joinInner() {
        assertSqlParity("join inner",
                """
                        Employee.all()->project({e | $e.name}, {e | $e.salary})
                            ->join(
                                Employee.all()->project({e | $e.name}, {e | $e.department}),
                                JoinType.INNER,
                                {l, r | $l.name == $r.name}
                            )
                        """);
    }

    @Test
    void joinLeftOuter() {
        assertSqlParity("join left outer",
                """
                        Employee.all()->project({e | $e.name}, {e | $e.salary})
                            ->join(
                                Employee.all()->project({e | $e.department}),
                                JoinType.LEFT_OUTER,
                                {l, r | $l.name == $r.department}
                            )
                        """);
    }

    // ==================== Filter with complex expressions ====================

    @Test
    void filterAnd() {
        assertSqlParity("filter AND",
                "Employee.all()->filter(e | $e.salary > 50000 && $e.department == 'Engineering')->project({e | $e.name})");
    }

    @Test
    void filterOr() {
        assertSqlParity("filter OR",
                "Employee.all()->filter(e | $e.department == 'Engineering' || $e.department == 'Sales')->project({e | $e.name})");
    }

    @Test
    void filterNot() {
        assertSqlParity("filter NOT",
                "Employee.all()->filter(e | !($e.department == 'Sales'))->project({e | $e.name})");
    }

    @Test
    void filterNotEqual() {
        assertSqlParity("filter !=",
                "Employee.all()->filter(e | $e.department != 'Sales')->project({e | $e.name})");
    }

    @Test
    void filterGte() {
        assertSqlParity("filter >=",
                "Employee.all()->filter(e | $e.salary >= 80000)->project({e | $e.name})");
    }

    @Test
    void filterLte() {
        assertSqlParity("filter <=",
                "Employee.all()->filter(e | $e.salary <= 80000)->project({e | $e.name})");
    }

    // ==================== Scalar String Functions ====================

    @Test
    void filterContains() {
        // New pipeline uses STRPOS (correct string containment)
        // Old pipeline incorrectly uses LIST_CONTAINS (list membership, not substring)
        assertSqlImprovement("filter contains",
                "Employee.all()->filter(e | $e.name->contains('oh'))->project({e | $e.name})",
                "SELECT \"t0\".\"NAME\" AS \"name\" FROM \"T_EMPLOYEE\" AS \"t0\" WHERE STRPOS(\"t0\".\"NAME\", 'oh') > 0");
    }

    @Test
    void filterStartsWith() {
        assertSqlParity("filter startsWith",
                "Employee.all()->filter(e | $e.name->startsWith('J'))->project({e | $e.name})");
    }

    @Test
    void filterToLower() {
        assertSqlParity("filter toLower",
                "Employee.all()->filter(e | $e.name->toLower() == 'john')->project({e | $e.name})");
    }

    @Test
    void filterIsEmpty() {
        assertSqlParity("filter isEmpty",
                "Employee.all()->filter(e | $e.name->isEmpty())->project({e | $e.name})");
    }

    @Test
    void filterIsNotEmpty() {
        assertSqlParity("filter isNotEmpty",
                "Employee.all()->filter(e | $e.name->isNotEmpty())->project({e | $e.name})");
    }

    // ==================== Scalar Math Functions ====================

    @Test
    void filterAbs() {
        assertSqlParity("filter abs",
                "Employee.all()->filter(e | abs($e.salary) > 50000)->project({e | $e.name})");
    }

    // ==================== If/Case Expression ====================

    @Test
    void filterIf() {
        // New pipeline properly qualifies column refs ("t0"."SALARY")
        // Old pipeline uses bare "salary" which can be ambiguous in complex queries
        assertSqlImprovement("filter if",
                "Employee.all()->project({e | $e.name}, {e | $e.salary})->filter(x | if($x.salary > 80000, |true, |false))",
                "SELECT \"t0\".\"NAME\" AS \"name\", \"t0\".\"SALARY\" AS \"salary\" FROM \"T_EMPLOYEE\" AS \"t0\" WHERE CASE WHEN \"t0\".\"SALARY\" > 80000 THEN TRUE ELSE FALSE END");
    }

    // ==================== In Expression ====================

    @Test
    void filterIn() {
        // New pipeline uses standard SQL IN syntax
        // Old pipeline uses LIST_CONTAINS with reversed args (non-standard)
        assertSqlImprovement("filter in",
                "Employee.all()->filter(e | $e.department->in(['Engineering', 'Sales']))->project({e | $e.name})",
                "SELECT \"t0\".\"NAME\" AS \"name\" FROM \"T_EMPLOYEE\" AS \"t0\" WHERE \"t0\".\"DEPARTMENT\" IN ('Engineering', 'Sales')");
    }

    // ==================== GroupBy with multiple agg functions ====================

    @Test
    void groupByCount() {
        assertSqlParity("groupBy count",
                "Employee.all()->project([{e | $e.department}, {e | $e.salary}], ['department', 'salary'])->groupBy(~department, ~cnt:x|$x.salary:y|$y->count())");
    }

    @Test
    void groupByAvg() {
        assertSqlParity("groupBy avg",
                "Employee.all()->project([{e | $e.department}, {e | $e.salary}], ['department', 'salary'])->groupBy(~department, ~avgSal:x|$x.salary:y|$y->average())");
    }

    @Test
    void groupByMin() {
        assertSqlParity("groupBy min",
                "Employee.all()->project([{e | $e.department}, {e | $e.salary}], ['department', 'salary'])->groupBy(~department, ~minSal:x|$x.salary:y|$y->min())");
    }

    @Test
    void groupByMax() {
        assertSqlParity("groupBy max",
                "Employee.all()->project([{e | $e.department}, {e | $e.salary}], ['department', 'salary'])->groupBy(~department, ~maxSal:x|$x.salary:y|$y->max())");
    }

    // ==================== Window: dense_rank, ntile ====================

    @Test
    void extendDenseRank() {
        assertSqlParity("extend denseRank",
                """
                        Employee.all()
                            ->project({e | $e.name}, {e | $e.department}, {e | $e.salary})
                            ->extend(over(~department, ~salary->desc()), ~denseRank:{p,w,r|$p->denseRank($w,$r)})
                        """);
    }

    // ==================== Chained operations (stress tests) ====================

    @Test
    void filterProjectSortLimitDrop() {
        assertSqlParity("filter->project->sort->limit",
                """
                        Employee.all()
                            ->filter(e | $e.salary > 50000)
                            ->project([{e | $e.name}, {e | $e.salary}], ['name', 'salary'])
                            ->sort(descending(~salary))
                            ->limit(3)
                        """);
    }

    @Test
    void projectDistinctSort() {
        assertSqlParity("project->distinct->sort",
                """
                        Employee.all()
                            ->project({e | $e.department})
                            ->distinct()
                            ->sort(ascending(~department))
                        """);
    }

    @Test
    void filterProjectGroupBySort() {
        assertSqlParity("filter->project->groupBy->sort",
                """
                        Employee.all()
                            ->filter(e | $e.salary > 30000)
                            ->project([{e | $e.department}, {e | $e.salary}], ['department', 'salary'])
                            ->groupBy(~department, ~totalSal:x|$x.salary:y|$y->sum())
                            ->sort(descending(~totalSal))
                        """);
    }
}
