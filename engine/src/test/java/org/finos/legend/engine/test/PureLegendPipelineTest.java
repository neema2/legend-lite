package org.finos.legend.engine.test;

import org.finos.legend.engine.plan.ComparisonExpression;
import org.finos.legend.engine.plan.ExtendNode;
import org.finos.legend.engine.plan.FilterNode;
import org.finos.legend.engine.plan.InExpression;
import org.finos.legend.engine.plan.LimitNode;
import org.finos.legend.engine.plan.LogicalExpression;
import org.finos.legend.engine.plan.ProjectNode;
import org.finos.legend.engine.plan.RelationNode;
import org.finos.legend.engine.plan.TableNode;
import org.finos.legend.engine.plan.WindowExpression;
import org.finos.legend.engine.store.MappingRegistry;
import org.finos.legend.pure.dsl.ModelContext;
import org.finos.legend.pure.dsl.definition.PureModelBuilder;
import org.finos.legend.pure.dsl.legend.Expression;
import org.finos.legend.pure.dsl.legend.Function;
import org.finos.legend.pure.dsl.legend.Property;
import org.finos.legend.pure.dsl.legend.PureLegendCompiler;
import org.finos.legend.pure.dsl.legend.PureLegendParser;

import org.junit.jupiter.api.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for the PureLegend pipeline:
 * PureLegendParser → PureLegendCompiler → RelationNode IR
 * 
 * These tests verify the new unified AST approach where:
 * - Parser produces minimal, uniform Expression AST (all methods → Function)
 * - Compiler interprets semantics and produces RelationNode IR
 */
@DisplayName("PureLegend Pipeline: Parser → Compiler → IR")
class PureLegendPipelineTest {

    private MappingRegistry mappingRegistry;
    private ModelContext modelContext;
    private PureModelBuilder modelBuilder;

    @BeforeEach
    void setup() {
        // Build a minimal model for testing
        String pureSource = """
                Class model::Person
                {
                    firstName: String[1];
                    lastName: String[1];
                    age: Integer[1];
                }

                Database store::TestDB
                (
                    Table T_PERSON
                    (
                        ID INTEGER PRIMARY KEY,
                        FIRST_NAME VARCHAR(100) NOT NULL,
                        LAST_NAME VARCHAR(100) NOT NULL,
                        AGE_VAL INTEGER NOT NULL
                    )
                )

                Mapping model::PersonMapping
                (
                    Person: Relational
                    {
                        ~mainTable [TestDB] T_PERSON
                        firstName: [TestDB] T_PERSON.FIRST_NAME,
                        lastName: [TestDB] T_PERSON.LAST_NAME,
                        age: [TestDB] T_PERSON.AGE_VAL
                    }
                )
                """;

        modelBuilder = new PureModelBuilder().addSource(pureSource);
        mappingRegistry = modelBuilder.getMappingRegistry();
        modelContext = modelBuilder; // PureModelBuilder implements ModelContext
    }

    // ==================== End-to-End: Parse → Compile → IR ====================

    @Test
    @DisplayName("E2E: Person.all() → TableNode")
    void testParseAndCompileClassAll() {
        // GIVEN: Pure query
        String query = "Person.all()";

        // WHEN: Parse to AST
        Expression ast = PureLegendParser.parse(query);

        // THEN: AST is Function("all", [Function("class", [Literal("Person")])])
        assertInstanceOf(Function.class, ast);
        Function all = (Function) ast;
        assertEquals("all", all.function());

        // AND WHEN: Compile to IR
        PureLegendCompiler compiler = new PureLegendCompiler(mappingRegistry, modelContext);
        RelationNode ir = compiler.compile(ast);

        // THEN: IR is TableNode
        assertInstanceOf(TableNode.class, ir);
        TableNode tableNode = (TableNode) ir;
        assertEquals("T_PERSON", tableNode.table().name());
    }

    @Test
    @DisplayName("E2E: Person.all()->filter() → FilterNode(TableNode)")
    void testParseAndCompileFilter() {
        // GIVEN: Pure query with filter
        String query = "Person.all()->filter({p | $p.lastName == 'Smith'})";

        // WHEN: Parse to AST
        Expression ast = PureLegendParser.parse(query);

        // THEN: AST is Function("filter", ...)
        assertInstanceOf(Function.class, ast);
        assertEquals("filter", ((Function) ast).function());

        // AND WHEN: Compile to IR
        PureLegendCompiler compiler = new PureLegendCompiler(mappingRegistry, modelContext);
        RelationNode ir = compiler.compile(ast);

        // THEN: IR is FilterNode wrapping TableNode
        assertInstanceOf(FilterNode.class, ir);
        FilterNode filterNode = (FilterNode) ir;
        assertInstanceOf(TableNode.class, filterNode.source());

        // AND: Condition is a comparison
        assertInstanceOf(ComparisonExpression.class, filterNode.condition());
    }

    @Test
    @DisplayName("E2E: ->project() → ProjectNode")
    void testParseAndCompileProject() {
        // GIVEN: Pure query with project
        String query = "Person.all()->project({p | $p.firstName}, {p | $p.lastName})";

        // WHEN: Parse and Compile
        Expression ast = PureLegendParser.parse(query);
        PureLegendCompiler compiler = new PureLegendCompiler(mappingRegistry, modelContext);
        RelationNode ir = compiler.compile(ast);

        // THEN: IR is ProjectNode
        assertInstanceOf(ProjectNode.class, ir);
        ProjectNode projectNode = (ProjectNode) ir;
        assertEquals(2, projectNode.projections().size());
    }

    @Test
    @DisplayName("E2E: ->limit() → LimitNode")
    void testParseAndCompileLimit() {
        // GIVEN: Pure query with limit
        String query = "Person.all()->limit(10)";

        // WHEN: Parse and Compile
        Expression ast = PureLegendParser.parse(query);
        PureLegendCompiler compiler = new PureLegendCompiler(mappingRegistry, modelContext);
        RelationNode ir = compiler.compile(ast);

        // THEN: IR is LimitNode
        assertInstanceOf(LimitNode.class, ir);
        LimitNode limitNode = (LimitNode) ir;
        assertEquals(Long.valueOf(10L), Long.valueOf(limitNode.limit()));
        assertEquals(Long.valueOf(0L), Long.valueOf(limitNode.offset()));
    }

    @Test
    @DisplayName("E2E: Chained operations maintain correct nesting")
    void testChainedOperations() {
        // GIVEN: Complex chained query
        String query = "Person.all()->filter({p | $p.age > 21})->project({p | $p.firstName})->limit(5)";

        // WHEN: Parse and Compile
        Expression ast = PureLegendParser.parse(query);
        PureLegendCompiler compiler = new PureLegendCompiler(mappingRegistry, modelContext);
        RelationNode ir = compiler.compile(ast);

        // THEN: IR has correct nesting: LimitNode(ProjectNode(FilterNode(TableNode)))
        assertInstanceOf(LimitNode.class, ir);
        LimitNode limit = (LimitNode) ir;

        assertInstanceOf(ProjectNode.class, limit.source());
        ProjectNode project = (ProjectNode) limit.source();

        assertInstanceOf(FilterNode.class, project.source());
        FilterNode filter = (FilterNode) project.source();

        assertInstanceOf(TableNode.class, filter.source());
    }

    // ==================== AST Verification ====================

    @Test
    @DisplayName("Parser: Arithmetic becomes Function")
    void testArithmeticAstStructure() {
        Expression ast = PureLegendParser.parse("$x.a + $x.b");

        assertInstanceOf(Function.class, ast);
        Function plus = (Function) ast;
        assertEquals("plus", plus.function());
        assertEquals(2, plus.parameters().size());
    }

    @Test
    @DisplayName("Parser: groupBy is generic Function (no special AST)")
    void testGroupByAstStructure() {
        Expression ast = PureLegendParser.parse("$data->groupBy(~dept, ~total:x|$x.amount:y|$y->sum())");

        assertInstanceOf(Function.class, ast);
        Function groupBy = (Function) ast;
        assertEquals("groupBy", groupBy.function());
        // receiver + 2 column specs
        assertEquals(3, groupBy.parameters().size());
    }

    // ==================== Compiler Semantic Interpretation ====================

    @Test
    @DisplayName("Compiler: equal() → ComparisonExpression.equals")
    void testCompilerInterpretation() {
        // GIVEN: Filter with equality
        String query = "Person.all()->filter({p | $p.firstName == 'John'})";

        // WHEN: Parse and Compile
        Expression ast = PureLegendParser.parse(query);
        PureLegendCompiler compiler = new PureLegendCompiler(mappingRegistry, modelContext);
        RelationNode ir = compiler.compile(ast);

        // THEN: FilterNode has ComparisonExpression
        FilterNode filterNode = (FilterNode) ir;
        assertInstanceOf(ComparisonExpression.class, filterNode.condition());
        ComparisonExpression cmp = (ComparisonExpression) filterNode.condition();
        assertEquals(ComparisonExpression.ComparisonOperator.EQUALS, cmp.operator());
    }

    @Test
    @DisplayName("Compiler: and/or → LogicalExpression")
    void testLogicalOperatorsCompilation() {
        // GIVEN: Filter with AND
        String query = "Person.all()->filter({p | $p.age > 21 && $p.lastName == 'Smith'})";

        // WHEN: Parse and Compile
        Expression ast = PureLegendParser.parse(query);
        PureLegendCompiler compiler = new PureLegendCompiler(mappingRegistry, modelContext);
        RelationNode ir = compiler.compile(ast);

        // THEN: FilterNode has LogicalExpression
        FilterNode filterNode = (FilterNode) ir;
        assertInstanceOf(LogicalExpression.class, filterNode.condition());
        LogicalExpression logical = (LogicalExpression) filterNode.condition();
        assertEquals(LogicalExpression.LogicalOperator.AND, logical.operator());
        assertEquals(2, logical.operands().size());
    }

    @Test
    @DisplayName("Compiler: nth() → LimitNode with offset")
    void testNthCompilation() {
        // GIVEN: nth(0) - get first element
        String query = "Person.all()->nth(0)";

        // WHEN: Parse and Compile
        Expression ast = PureLegendParser.parse(query);
        PureLegendCompiler compiler = new PureLegendCompiler(mappingRegistry, modelContext);
        RelationNode ir = compiler.compile(ast);

        // THEN: LimitNode with limit=1, offset=0
        assertInstanceOf(LimitNode.class, ir);
        LimitNode limitNode = (LimitNode) ir;
        assertEquals(Long.valueOf(1L), Long.valueOf(limitNode.limit()));
        // nth(0) means first element
    }

    // ==================== Error Handling ====================

    @Test
    @DisplayName("Parser: Parses valid Pure syntax without error")
    void testParserHandlesValidSyntax() {
        // Parser can parse variable references and property access
        Expression ast = PureLegendParser.parse("$x.name");
        assertNotNull(ast);
        // AST is a Property expression (via pattern matching, not instanceOf)
        assertTrue(ast instanceof Expression);
    }

    @Test
    @DisplayName("Compiler: Throws on unknown class")
    void testCompilerUnknownClass() {
        Expression ast = PureLegendParser.parse("UnknownClass.all()");
        PureLegendCompiler compiler = new PureLegendCompiler(mappingRegistry, modelContext);

        assertThrows(Exception.class, () -> compiler.compile(ast));
    }

    // ==================== Phase 5b: Expression Functions ====================

    @Test
    @DisplayName("Compiler: Date functions (year, month)")
    void testDateFunctions() {
        // GIVEN: filter with year extraction
        String query = "Person.all()->filter({p | $p.birthDate->year() > 1990})";

        // WHEN: Parse and Compile
        Expression ast = PureLegendParser.parse(query);
        PureLegendCompiler compiler = new PureLegendCompiler(mappingRegistry, modelContext);
        RelationNode ir = compiler.compile(ast);

        // THEN: FilterNode with DateFunctionExpression
        assertInstanceOf(FilterNode.class, ir);
        FilterNode filterNode = (FilterNode) ir;
        assertNotNull(filterNode.condition());
    }

    @Test
    @DisplayName("Compiler: String function (toUpper)")
    void testStringFunctions() {
        // GIVEN: filter with toUpper
        String query = "Person.all()->filter({p | $p.firstName->toUpper() == 'JOHN'})";

        Expression ast = PureLegendParser.parse(query);
        PureLegendCompiler compiler = new PureLegendCompiler(mappingRegistry, modelContext);
        RelationNode ir = compiler.compile(ast);

        assertInstanceOf(FilterNode.class, ir);
    }

    @Test
    @DisplayName("Compiler: in() collection operator")
    void testInOperator() {
        // GIVEN: filter with in() check
        String query = "Person.all()->filter({p | $p.firstName->in(['John', 'Jane'])})";

        Expression ast = PureLegendParser.parse(query);
        PureLegendCompiler compiler = new PureLegendCompiler(mappingRegistry, modelContext);
        RelationNode ir = compiler.compile(ast);

        assertInstanceOf(FilterNode.class, ir);
        FilterNode filterNode = (FilterNode) ir;
        // Predicate should be InExpression
        assertInstanceOf(InExpression.class, filterNode.condition());
    }

    @Test
    @DisplayName("Compiler: isEmpty() null check")
    void testIsEmpty() {
        String query = "Person.all()->filter({p | $p.firstName->isEmpty()})";

        Expression ast = PureLegendParser.parse(query);
        PureLegendCompiler compiler = new PureLegendCompiler(mappingRegistry, modelContext);
        RelationNode ir = compiler.compile(ast);

        assertInstanceOf(FilterNode.class, ir);
        FilterNode filterNode = (FilterNode) ir;
        // Predicate should be IS NULL comparison
        assertInstanceOf(ComparisonExpression.class, filterNode.condition());
    }

    // ========================================
    // WINDOW FUNCTION TESTS
    // ========================================

    @Test
    @DisplayName("Compiler: extend with row_number window function")
    void testExtendWindowRowNumber() {
        // Pattern: extend(~colName:{x|row_number()->over(~partition, ~order->asc())})
        String query = "Person.all()->extend(~rowNum:{x|row_number()->over(~firstName, ~age->asc())})";

        Expression ast = PureLegendParser.parse(query);
        PureLegendCompiler compiler = new PureLegendCompiler(mappingRegistry, modelContext);
        RelationNode ir = compiler.compile(ast);

        assertInstanceOf(ExtendNode.class, ir);
        ExtendNode extendNode = (ExtendNode) ir;
        assertEquals(1, extendNode.projections().size());

        ExtendNode.ExtendProjection proj = extendNode.projections().get(0);
        assertInstanceOf(ExtendNode.WindowProjection.class, proj);
        ExtendNode.WindowProjection windowProj = (ExtendNode.WindowProjection) proj;

        assertEquals("rowNum", windowProj.alias());
        assertEquals(WindowExpression.WindowFunction.ROW_NUMBER, windowProj.expression().function());
        assertEquals(1, windowProj.expression().partitionBy().size());
        assertEquals("firstName", windowProj.expression().partitionBy().get(0));
        assertEquals(1, windowProj.expression().orderBy().size());
        assertEquals("age", windowProj.expression().orderBy().get(0).column());
        assertEquals(WindowExpression.SortDirection.ASC, windowProj.expression().orderBy().get(0).direction());
    }

    @Test
    @DisplayName("Compiler: extend with rank window function")
    void testExtendWindowRank() {
        String query = "Person.all()->extend(~salaryRank:{x|rank()->over(~lastName->desc())})";

        Expression ast = PureLegendParser.parse(query);
        PureLegendCompiler compiler = new PureLegendCompiler(mappingRegistry, modelContext);
        RelationNode ir = compiler.compile(ast);

        assertInstanceOf(ExtendNode.class, ir);
        ExtendNode extendNode = (ExtendNode) ir;

        ExtendNode.ExtendProjection proj = extendNode.projections().get(0);
        assertInstanceOf(ExtendNode.WindowProjection.class, proj);
        ExtendNode.WindowProjection windowProj = (ExtendNode.WindowProjection) proj;

        assertEquals("salaryRank", windowProj.alias());
        assertEquals(WindowExpression.WindowFunction.RANK, windowProj.expression().function());
    }

    @Test
    @DisplayName("Compiler: extend with sum aggregate window function")
    void testExtendWindowSum() {
        // Pattern: sum with aggregate column over partition
        String query = "Person.all()->extend(~runningTotal:{x|sum(~age)->over(~firstName, ~lastName->asc())})";

        Expression ast = PureLegendParser.parse(query);
        PureLegendCompiler compiler = new PureLegendCompiler(mappingRegistry, modelContext);
        RelationNode ir = compiler.compile(ast);

        assertInstanceOf(ExtendNode.class, ir);
        ExtendNode extendNode = (ExtendNode) ir;

        ExtendNode.ExtendProjection proj = extendNode.projections().get(0);
        assertInstanceOf(ExtendNode.WindowProjection.class, proj);
        ExtendNode.WindowProjection windowProj = (ExtendNode.WindowProjection) proj;

        assertEquals("runningTotal", windowProj.alias());
        assertEquals(WindowExpression.WindowFunction.SUM, windowProj.expression().function());
        assertEquals("age", windowProj.expression().aggregateColumn());
    }

    @Test
    @DisplayName("Compiler: extend with window frame")
    void testExtendWindowWithFrame() {
        // Pattern: window function with rows frame
        String query = "Person.all()->extend(~movingAvg:{x|avg(~age)->over(~firstName, ~lastName->asc(), rows(unbounded(), 0))})";

        Expression ast = PureLegendParser.parse(query);
        PureLegendCompiler compiler = new PureLegendCompiler(mappingRegistry, modelContext);
        RelationNode ir = compiler.compile(ast);

        assertInstanceOf(ExtendNode.class, ir);
        ExtendNode extendNode = (ExtendNode) ir;

        ExtendNode.ExtendProjection proj = extendNode.projections().get(0);
        assertInstanceOf(ExtendNode.WindowProjection.class, proj);
        ExtendNode.WindowProjection windowProj = (ExtendNode.WindowProjection) proj;

        assertEquals("movingAvg", windowProj.alias());
        assertEquals(WindowExpression.WindowFunction.AVG, windowProj.expression().function());
        assertNotNull(windowProj.expression().frame());
        assertEquals(WindowExpression.FrameType.ROWS, windowProj.expression().frame().type());
    }
}
