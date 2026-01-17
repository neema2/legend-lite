package org.finos.legend.engine.test;

import org.finos.legend.engine.plan.*;
import org.finos.legend.engine.store.*;
import org.finos.legend.engine.transpiler.SQLDialect;
import org.finos.legend.engine.transpiler.SQLGenerator;
import org.finos.legend.pure.dsl.PureCompiler;
import org.finos.legend.pure.dsl.definition.PureModelBuilder;
import org.finos.legend.pure.m3.*;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Abstract base class for database integration tests.
 * Provides common setup, teardown, and utility methods.
 * 
 * Uses Pure syntax for defining Class, Database, and Mapping.
 */
public abstract class AbstractDatabaseTest {
    
    protected Connection connection;
    protected SQLGenerator sqlGenerator;
    protected MappingRegistry mappingRegistry;
    protected PureCompiler pureCompiler;
    protected PureModelBuilder modelBuilder;
    
    // ==================== Pure Definitions ====================
    
    /**
     * Pure Class definition for Person.
     */
    protected static final String PERSON_CLASS = """
            Class model::Person
            {
                firstName: String[1];
                lastName: String[1];
                age: Integer[1];
            }
            """;
    
    /**
     * Pure Database definition with T_PERSON table.
     */
    protected static final String PERSON_DATABASE = """
            Database store::PersonDatabase
            (
                Table T_PERSON
                (
                    ID INTEGER PRIMARY KEY,
                    FIRST_NAME VARCHAR(100) NOT NULL,
                    LAST_NAME VARCHAR(100) NOT NULL,
                    AGE_VAL INTEGER NOT NULL
                )
            )
            """;
    
    /**
     * Pure Mapping definition from Person to T_PERSON.
     */
    protected static final String PERSON_MAPPING = """
            Mapping model::PersonMapping
            (
                Person: Relational
                {
                    ~mainTable [PersonDatabase] T_PERSON
                    firstName: [PersonDatabase] T_PERSON.FIRST_NAME,
                    lastName: [PersonDatabase] T_PERSON.LAST_NAME,
                    age: [PersonDatabase] T_PERSON.AGE_VAL
                }
            )
            """;
    
    /**
     * Complete Pure model (Class + Database + Mapping).
     */
    protected static final String COMPLETE_PURE_MODEL = PERSON_CLASS + "\n" + PERSON_DATABASE + "\n" + PERSON_MAPPING;
    
    // ==================== Legacy Model Creation (for backward compatibility) ====================
    
    /**
     * Creates the Person Pure class programmatically.
     * @deprecated Use Pure syntax instead
     */
    @Deprecated
    protected PureClass createPersonClass() {
        return new PureClass("model::domain", "Person", List.of(
                Property.required("firstName", PrimitiveType.STRING),
                Property.required("lastName", PrimitiveType.STRING),
                Property.required("age", PrimitiveType.INTEGER)
        ));
    }
    
    /**
     * Creates the T_PERSON table schema programmatically.
     * @deprecated Use Pure syntax instead
     */
    @Deprecated
    protected Table createPersonTable() {
        return new Table("T_PERSON", List.of(
                Column.required("ID", SqlDataType.INTEGER),
                Column.required("FIRST_NAME", SqlDataType.VARCHAR),
                Column.required("LAST_NAME", SqlDataType.VARCHAR),
                Column.required("AGE_VAL", SqlDataType.INTEGER)
        ));
    }
    
    /**
     * Creates the mapping from Person to T_PERSON programmatically.
     * @deprecated Use Pure syntax instead
     */
    @Deprecated
    protected RelationalMapping createPersonMapping(PureClass personClass, Table personTable) {
        return new RelationalMapping(personClass, personTable, List.of(
                new PropertyMapping("firstName", "FIRST_NAME"),
                new PropertyMapping("lastName", "LAST_NAME"),
                new PropertyMapping("age", "AGE_VAL")
        ));
    }
    
    // ==================== Pure Model Setup ====================
    
    /**
     * Sets up the model from Pure syntax definitions.
     * This parses the Pure source and creates all metamodel objects.
     */
    protected void setupPureModel() {
        modelBuilder = new PureModelBuilder()
                .addSource(COMPLETE_PURE_MODEL);
        
        mappingRegistry = modelBuilder.getMappingRegistry();
        pureCompiler = new PureCompiler(mappingRegistry);
    }
    
    /**
     * Sets up the mapping registry using Pure syntax.
     * @see #setupPureModel()
     */
    protected void setupMappingRegistry() {
        setupPureModel();
    }
    
    /**
     * Compiles a Pure query and generates SQL.
     * 
     * @param pureQuery The Pure query string
     * @return The generated SQL
     */
    protected String compileAndGenerateSql(String pureQuery) {
        RelationNode plan = pureCompiler.compile(pureQuery);
        return sqlGenerator.generate(plan);
    }
    
    /**
     * Executes a Pure query and returns results.
     * 
     * @param pureQuery The Pure query string
     * @return List of PersonResult
     */
    protected List<PersonResult> executePureQuery(String pureQuery) throws SQLException {
        String sql = compileAndGenerateSql(pureQuery);
        System.out.println("Pure Query: " + pureQuery);
        System.out.println("Generated SQL: " + sql);
        return executePersonQuery(sql);
    }
    
    // ==================== SQL Dialect ====================
    
    /**
     * @return The SQL dialect for this test database
     */
    protected abstract SQLDialect getDialect();
    
    /**
     * @return The JDBC URL for the in-memory database
     */
    protected abstract String getJdbcUrl();
    
    // ==================== Database Setup ====================
    
    /**
     * Creates the test table and inserts sample data.
     * Uses the SQL from the Pure Database definition.
     */
    protected void setupDatabase() throws SQLException {
        // Create the T_PERSON table
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("""
                CREATE TABLE T_PERSON (
                    ID INTEGER PRIMARY KEY,
                    FIRST_NAME VARCHAR(100) NOT NULL,
                    LAST_NAME VARCHAR(100) NOT NULL,
                    AGE_VAL INTEGER NOT NULL
                )
                """);
        }
        
        // Insert test data
        try (PreparedStatement ps = connection.prepareStatement(
                "INSERT INTO T_PERSON (ID, FIRST_NAME, LAST_NAME, AGE_VAL) VALUES (?, ?, ?, ?)")) {
            
            insertPerson(ps, 1, "John", "Smith", 30);
            insertPerson(ps, 2, "Jane", "Smith", 28);
            insertPerson(ps, 3, "Bob", "Jones", 45);
        }
    }
    
    private void insertPerson(PreparedStatement ps, int id, String firstName, String lastName, int age) 
            throws SQLException {
        ps.setInt(1, id);
        ps.setString(2, firstName);
        ps.setString(3, lastName);
        ps.setInt(4, age);
        ps.executeUpdate();
    }
    
    // ==================== Query Execution ====================
    
    /**
     * Executes SQL and returns results as a list of PersonResult records.
     */
    protected List<PersonResult> executePersonQuery(String sql) throws SQLException {
        List<PersonResult> results = new ArrayList<>();
        
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            
            ResultSetMetaData meta = rs.getMetaData();
            int columnCount = meta.getColumnCount();
            
            while (rs.next()) {
                String firstName = null;
                String lastName = null;
                Integer age = null;
                
                for (int i = 1; i <= columnCount; i++) {
                    String columnLabel = meta.getColumnLabel(i);
                    switch (columnLabel) {
                        case "firstName" -> firstName = rs.getString(i);
                        case "lastName" -> lastName = rs.getString(i);
                        case "age" -> age = rs.getInt(i);
                    }
                }
                
                results.add(new PersonResult(firstName, lastName, age));
            }
        }
        
        return results;
    }
    
    /**
     * Builds the logical plan for: "Find all people with lastName 'Smith'"
     * In SQL: SELECT firstName, lastName FROM T_PERSON WHERE LAST_NAME = 'Smith'
     * 
     * @deprecated Use Pure query syntax instead
     */
    @Deprecated
    protected RelationNode buildFindSmithsQuery(Table personTable, RelationalMapping mapping) {
        // 1. Table scan
        TableNode tableNode = new TableNode(personTable, "t0");
        
        // 2. Filter: LAST_NAME = 'Smith'
        Expression filterCondition = ComparisonExpression.columnEquals("t0", "LAST_NAME", "Smith");
        FilterNode filterNode = new FilterNode(tableNode, filterCondition);
        
        // 3. Project: firstName, lastName (structural projections)
        ProjectNode projectNode = new ProjectNode(filterNode, List.of(
                Projection.column("t0", "FIRST_NAME", "firstName"),
                Projection.column("t0", "LAST_NAME", "lastName")
        ));
        
        return projectNode;
    }
    
    /**
     * Builds a query with multiple filter conditions:
     * "Find all people with lastName 'Smith' AND age > 25"
     * 
     * @deprecated Use Pure query syntax instead
     */
    @Deprecated
    protected RelationNode buildComplexFilterQuery(Table personTable) {
        // 1. Table scan
        TableNode tableNode = new TableNode(personTable, "t0");
        
        // 2. Complex filter: LAST_NAME = 'Smith' AND AGE_VAL > 25
        Expression lastNameFilter = ComparisonExpression.columnEquals("t0", "LAST_NAME", "Smith");
        Expression ageFilter = ComparisonExpression.greaterThan(
                ColumnReference.of("t0", "AGE_VAL"),
                Literal.integer(25)
        );
        Expression combinedFilter = LogicalExpression.and(lastNameFilter, ageFilter);
        
        FilterNode filterNode = new FilterNode(tableNode, combinedFilter);
        
        // 3. Project all relevant fields
        ProjectNode projectNode = new ProjectNode(filterNode, List.of(
                Projection.column("t0", "FIRST_NAME", "firstName"),
                Projection.column("t0", "LAST_NAME", "lastName"),
                Projection.column("t0", "AGE_VAL", "age")
        ));
        
        return projectNode;
    }
    
    /**
     * Simple record to hold person query results.
     */
    public record PersonResult(String firstName, String lastName, Integer age) {}
}
