# Legend Lite - Clean Room Implementation

A "Clean Room" implementation of the FINOS Legend Engine core, designed for 100% SQL Push-Down execution.

## Architecture Philosophy

**"Database-as-Runtime"** - The Java layer is purely a transpiler (Pure Language → SQL). It does not execute business logic. All computation is pushed down to the database engine.

## Stack

- **Language**: Java 21+ (LTS)
- **Paradigm**: Functional & Immutable (records, sealed interfaces, pattern matching)
- **Target Databases**: DuckDB, SQLite
- **GraalVM Native Image**: Compatible (no reflection, no dynamic class loading)

## Project Structure

```
src/main/java/org/finos/legend/
├── pure/m3/                    # Metamodel (M3)
│   ├── Type.java               # Sealed interface for types
│   ├── PrimitiveType.java      # String, Integer, Boolean, Date
│   ├── PureClass.java          # User-defined classes
│   ├── Property.java           # Class properties
│   └── Multiplicity.java       # Cardinality constraints
│
├── engine/store/               # Relational Store & Mapping
│   ├── Table.java              # Physical table definition
│   ├── Column.java             # Table columns
│   ├── SqlDataType.java        # SQL data types
│   ├── RelationalMapping.java  # Class-to-table mapping
│   ├── PropertyMapping.java    # Property-to-column mapping
│   └── MappingRegistry.java    # Mapping registry
│
├── engine/plan/                # Logical Plan (IR)
│   ├── RelationNode.java       # Sealed interface for plan nodes
│   ├── TableNode.java          # Table scan (FROM)
│   ├── FilterNode.java         # Row filter (WHERE)
│   ├── ProjectNode.java        # Column projection (SELECT)
│   ├── Projection.java         # Structural projection
│   ├── Expression.java         # Expression hierarchy
│   ├── ColumnReference.java    # Column reference
│   ├── Literal.java            # Literal values
│   ├── ComparisonExpression.java # Comparison ops
│   └── LogicalExpression.java  # AND/OR/NOT
│
├── engine/transpiler/          # SQL Transpiler
│   ├── SQLDialect.java         # Dialect interface
│   ├── DuckDBDialect.java      # DuckDB implementation
│   ├── SQLiteDialect.java      # SQLite implementation
│   └── SQLGenerator.java       # Plan-to-SQL transpiler
│
└── pure/dsl/                   # Pure Language DSL
    ├── definition/             # Pure Definition Parsing
    │   ├── PureDefinition.java # Sealed interface for definitions
    │   ├── ClassDefinition.java # Class model definition
    │   ├── DatabaseDefinition.java # Database/Store definition
    │   ├── MappingDefinition.java # Mapping definition
    │   ├── PureDefinitionParser.java # Definition parser
    │   └── PureModelBuilder.java # Builds runtime model from Pure
    ├── PureExpression.java     # Sealed AST interface
    ├── ClassAllExpression.java # Person.all()
    ├── FilterExpression.java   # ->filter({...})
    ├── ProjectExpression.java  # ->project({...})
    ├── LambdaExpression.java   # {p | ...}
    ├── Token.java              # Lexer tokens
    ├── PureLexer.java          # Tokenizer
    ├── PureParser.java         # Recursive descent parser
    └── PureCompiler.java       # Pure AST → RelationNode
```

## Full Pure Language Syntax

Legend Lite supports the full Pure syntax for defining models, including **Class**, **Database**, and **Mapping** definitions.

### Class Definition

Defines a Pure class with properties and multiplicities:

```pure
Class package::ClassName
{
    propertyName: Type[multiplicity];
}
```

**Example:**
```pure
Class model::Person
{
    firstName: String[1];
    lastName: String[1];
    age: Integer[1];
    email: String[0..1];      // Optional
    phoneNumbers: String[*];  // Many
}
```

### Database (Store) Definition

Defines a relational database with tables and columns:

```pure
Database package::DatabaseName
(
    Table TABLE_NAME
    (
        COLUMN_NAME DATA_TYPE [PRIMARY KEY] [NOT NULL],
        ...
    )
)
```

**Example:**
```pure
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
```

### Mapping Definition

Links a Pure class to a relational table:

```pure
Mapping package::MappingName
(
    ClassName: Relational
    {
        ~mainTable [DatabaseName] TABLE_NAME
        propertyName: [DatabaseName] TABLE_NAME.COLUMN_NAME,
        ...
    }
)
```

**Example:**
```pure
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
```

## Query Syntax

### Lambda Expressions
Lambdas are enclosed in curly braces: `{parameter | body}`

```pure
{p | $p.lastName == 'Smith'}     // Filter predicate
{p | $p.firstName}                // Property projection
```

### Query Functions

| Function | Description | Example |
|----------|-------------|---------|
| `.all()` | Get all instances of a class | `Person.all()` |
| `->filter({...})` | Filter rows | `->filter({p \| $p.age > 25})` |
| `->project({...}, {...})` | Select columns | `->project({p \| $p.firstName}, {p \| $p.lastName})` |

### Comparison Operators
- `==` equals
- `!=` not equals
- `<` less than
- `<=` less than or equal
- `>` greater than
- `>=` greater than or equal

### Logical Operators
- `&&` logical AND
- `||` logical OR

### Example Queries

```pure
// Simple filter
Person.all()->filter({p | $p.lastName == 'Smith'})->project({p | $p.firstName})

// Complex filter with AND
Person.all()->filter({p | $p.lastName == 'Smith' && $p.age > 25})->project({p | $p.firstName}, {p | $p.age})

// Filter with OR
Person.all()->filter({p | $p.age < 25 || $p.age > 40})->project({p | $p.firstName})
```

## Complete Example

### 1. Define the Model in Pure

```pure
Class model::Person
{
    firstName: String[1];
    lastName: String[1];
    age: Integer[1];
}

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
```

### 2. Load the Model in Java

```java
// Parse Pure definitions and build runtime model
PureModelBuilder modelBuilder = new PureModelBuilder()
    .addSource(pureModelSource);

MappingRegistry mappingRegistry = modelBuilder.getMappingRegistry();
PureCompiler compiler = new PureCompiler(mappingRegistry);
```

### 3. Execute Queries

```java
// Compile Pure query to logical plan
RelationNode plan = compiler.compile("""
    Person.all()
        ->filter({p | $p.lastName == 'Smith'})
        ->project({p | $p.firstName}, {p | $p.lastName})
    """);

// Transpile to SQL
SQLGenerator generator = new SQLGenerator(DuckDBDialect.INSTANCE);
String sql = generator.generate(plan);

// Execute via JDBC
try (Statement stmt = connection.createStatement();
     ResultSet rs = stmt.executeQuery(sql)) {
    while (rs.next()) {
        System.out.println(rs.getString("firstName") + " " + rs.getString("lastName"));
    }
}
```

### 4. Generated SQL (DuckDB)

```sql
SELECT "t0"."FIRST_NAME" AS "firstName", "t0"."LAST_NAME" AS "lastName"
FROM "T_PERSON" AS "t0"
WHERE "t0"."LAST_NAME" = 'Smith'
```

## Building & Testing

```bash
# Build (requires Java 21+)
mvn clean compile

# Run tests
mvn test
```

## Design Principles

1. **Immutability**: All model classes are records - no setters, no mutation
2. **Sealed Hierarchies**: `Type` and `RelationNode` are sealed for exhaustive pattern matching
3. **No Reflection**: GraalVM Native Image compatible
4. **Visitor Pattern**: Clean separation between data structure and operations
5. **Real Database Tests**: No mocking - tests run against actual DuckDB and SQLite instances

## Phase 1 Deliverables

- [x] M3 Metamodel (Type, Class, Property, PrimitiveType, Multiplicity)
- [x] Relational Store (Table, Column, RelationalMapping)
- [x] Logical Plan IR (TableNode, FilterNode, ProjectNode)
- [x] SQL Transpiler with dialect support
- [x] Pure Language DSL with parser and compiler
- [x] Pure Definition Syntax (Class, Database, Mapping)
- [x] Integration tests with DuckDB and SQLite using Pure syntax

## License

Apache 2.0 (Compatible with FINOS Legend)
