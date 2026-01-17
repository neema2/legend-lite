# Model-to-Model Transforms - Design Document

This document outlines how Legend Lite can support **Model-to-Model (M2M) transformations** using **100% database execution** (no in-memory processing of business logic).

## Overview

### What is Model-to-Model?

In Legend, M2M transforms allow you to:
1. Define a **source model** (e.g., `RawPerson` from a staging table)
2. Define a **target model** (e.g., `Person` with derived/computed properties)
3. Map source properties to target properties with **transformation expressions**

### Legend's Current Approach

Legend Engine today:
- Supports M2M mappings in Pure syntax with `$src` references
- Executes transformations **in-memory** (Java/Pure runtime)
- Can push *queries* to SQL, but M2M transform *logic* runs in application layer

### Legend Lite's Approach: Database-as-Runtime

We want M2M transforms to compile to **SQL only**. The database performs all computations:
- String concatenation → SQL `CONCAT()` or `||`
- Conditional logic → SQL `CASE WHEN`
- Aggregations → SQL `GROUP BY` + aggregate functions
- Filters → SQL `WHERE`

---

## Proposed Pure Syntax for M2M Mappings

### Current Relational Mapping (what we have)

```pure
Mapping model::PersonMapping
(
    Person: Relational
    {
        ~mainTable [PersonDatabase] T_PERSON
        firstName: [PersonDatabase] T_PERSON.FIRST_NAME,
        lastName: [PersonDatabase] T_PERSON.LAST_NAME
    }
)
```

### New Model-to-Model Mapping (proposed)

```pure
Mapping model::PersonTransform
(
    // Target class : Pure (indicates M2M, source is another Pure class)
    Person: Pure
    {
        ~src RawPerson
        
        // Simple property access
        firstName: $src.firstName,
        lastName: $src.lastName,
        
        // Derived/computed property - string concatenation
        fullName: $src.firstName + ' ' + $src.lastName,
        
        // Conditional logic
        ageGroup: if($src.age < 18, 'Minor', if($src.age < 65, 'Adult', 'Senior')),
        
        // Filter (only include records matching condition)
        ~filter $src.isActive == true
    }
)
```

### Key Syntax Elements

| Element | Description | Example |
|---------|-------------|---------|
| `~src ClassName` | Source class for the transform | `~src RawPerson` |
| `$src.property` | Reference to source property | `$src.firstName` |
| `+` on strings | String concatenation | `$src.first + ' ' + $src.last` |
| `if(cond, then, else)` | Conditional expression | `if($src.age < 18, 'Minor', 'Adult')` |
| `~filter expr` | Filter source rows | `~filter $src.isActive == true` |

---

## SQL Compilation Strategy

### Simple Property Mapping

**Pure:**
```pure
firstName: $src.firstName
```

**SQL:**
```sql
SELECT t0.FIRST_NAME AS firstName FROM T_RAW_PERSON AS t0
```

### Derived Property (String Concatenation)

**Pure:**
```pure
fullName: $src.firstName + ' ' + $src.lastName
```

**SQL (DuckDB/PostgreSQL):**
```sql
SELECT t0.FIRST_NAME || ' ' || t0.LAST_NAME AS fullName FROM T_RAW_PERSON AS t0
```

**SQL (MySQL):**
```sql
SELECT CONCAT(t0.FIRST_NAME, ' ', t0.LAST_NAME) AS fullName FROM T_RAW_PERSON AS t0
```

### Conditional Logic

**Pure:**
```pure
ageGroup: if($src.age < 18, 'Minor', if($src.age < 65, 'Adult', 'Senior'))
```

**SQL:**
```sql
SELECT 
    CASE 
        WHEN t0.AGE < 18 THEN 'Minor'
        WHEN t0.AGE < 65 THEN 'Adult'
        ELSE 'Senior'
    END AS ageGroup
FROM T_RAW_PERSON AS t0
```

### Aggregation (One-to-Many Rollup)

**Pure:**
```pure
Mapping model::OrderSummary
(
    CustomerSummary: Pure
    {
        ~src Order
        ~groupBy $src.customerId
        
        customerId: $src.customerId,
        orderCount: count($src),
        totalAmount: sum($src.amount),
        avgAmount: avg($src.amount)
    }
)
```

**SQL:**
```sql
SELECT 
    t0.CUSTOMER_ID AS customerId,
    COUNT(*) AS orderCount,
    SUM(t0.AMOUNT) AS totalAmount,
    AVG(t0.AMOUNT) AS avgAmount
FROM T_ORDER AS t0
GROUP BY t0.CUSTOMER_ID
```

---

## Architecture

### New Components Required

```
src/main/java/org/finos/legend/pure/dsl/
├── definition/
│   └── M2MMappingDefinition.java      # NEW: AST for M2M mapping
│
├── m2m/
│   ├── M2MExpression.java             # NEW: Sealed interface for M2M expressions
│   ├── SourcePropertyRef.java         # NEW: $src.property reference
│   ├── StringConcatExpr.java          # NEW: String concatenation
│   ├── IfExpression.java              # NEW: if(cond, then, else)
│   ├── AggregateExpr.java             # NEW: count(), sum(), avg(), etc.
│   └── M2MExpressionParser.java       # NEW: Parse M2M expressions
│
└── compiler/
    └── M2MCompiler.java               # NEW: Compile M2M mapping to RelationNode
```

### Expression Hierarchy

```java
public sealed interface M2MExpression 
    permits SourcePropertyRef, StringConcatExpr, IfExpression, 
            AggregateExpr, ArithmeticExpr, LiteralExpr, ComparisonExpr {
    
    /**
     * Compile this M2M expression to a SQL Expression in the logical plan.
     */
    Expression toSqlExpression(M2MContext context);
}
```

### Supported Functions (SQL-Translatable Subset)

| Category | Functions | SQL Equivalent |
|----------|-----------|----------------|
| **String** | `+` (concat), `toUpper()`, `toLower()`, `trim()`, `substring()`, `length()` | `\|\|`, `UPPER()`, `LOWER()`, `TRIM()`, `SUBSTR()`, `LENGTH()` |
| **Math** | `+`, `-`, `*`, `/`, `abs()`, `round()`, `ceil()`, `floor()` | Same |
| **Date** | `today()`, `year()`, `month()`, `day()`, `dateDiff()` | `CURRENT_DATE`, `YEAR()`, `MONTH()`, `DAY()`, dialect-specific |
| **Aggregate** | `count()`, `sum()`, `avg()`, `min()`, `max()` | Same |
| **Conditional** | `if(cond, then, else)`, `isNull()`, `coalesce()` | `CASE WHEN`, `IS NULL`, `COALESCE()` |
| **Boolean** | `&&`, `\|\|`, `!`, `==`, `!=`, `<`, `>`, `<=`, `>=` | `AND`, `OR`, `NOT`, `=`, `<>`, etc. |

### Functions NOT Supported (require in-memory fallback)

- Recursion / graph traversal
- External service calls
- Complex collection operations (map, filter with lambdas over collections)
- User-defined Pure functions (unless pre-registered as SQL UDFs)

---

## Chained Mappings

A key power of M2M is **chaining**: one M2M feeds into another.

```pure
// Stage 1: Raw data cleanup
Mapping model::CleanPerson
(
    CleanPerson: Pure
    {
        ~src RawPerson
        firstName: trim($src.firstName),
        lastName: toUpper($src.lastName)
    }
)

// Stage 2: Derive full name
Mapping model::PersonWithFullName
(
    PersonView: Pure
    {
        ~src CleanPerson
        firstName: $src.firstName,
        lastName: $src.lastName,
        fullName: $src.firstName + ' ' + $src.lastName
    }
)
```

### SQL Compilation (Chained as CTE or Subquery)

```sql
WITH clean_person AS (
    SELECT 
        TRIM(t0.FIRST_NAME) AS firstName,
        UPPER(t0.LAST_NAME) AS lastName
    FROM T_RAW_PERSON AS t0
)
SELECT 
    c.firstName,
    c.lastName,
    c.firstName || ' ' || c.lastName AS fullName
FROM clean_person AS c
```

### Alternative: Materialized View

For performance, Legend Lite could optionally materialize intermediate M2M results:

```sql
CREATE VIEW v_clean_person AS
SELECT TRIM(FIRST_NAME) AS firstName, UPPER(LAST_NAME) AS lastName 
FROM T_RAW_PERSON;

CREATE VIEW v_person_with_full_name AS
SELECT firstName, lastName, firstName || ' ' || lastName AS fullName
FROM v_clean_person;
```

---

## Integration with Existing Query Syntax

Once M2M mappings are defined, queries use the **target class** transparently:

```pure
// Query against the transformed model
PersonView.all()
    ->filter({p | $p.lastName == 'SMITH'})
    ->project({p | $p.fullName})
```

The compiler:
1. Sees `PersonView` is M2M-mapped from `CleanPerson`
2. Sees `CleanPerson` is M2M-mapped from `RawPerson`
3. Sees `RawPerson` is relationally-mapped to `T_RAW_PERSON`
4. Generates a single SQL query with all transforms inlined

```sql
SELECT TRIM(t0.FIRST_NAME) || ' ' || UPPER(t0.LAST_NAME) AS fullName
FROM T_RAW_PERSON AS t0
WHERE UPPER(t0.LAST_NAME) = 'SMITH'
```

---

## Implementation Phases

### Phase 1: Core M2M (Simple Properties + Derivations)

- [ ] `M2MExpression` sealed interface and implementations
- [ ] Parser for `~src` and `$src.property` syntax
- [ ] Compiler: `M2MExpression` → `Expression` (existing IR)
- [ ] Support string concat (`+`), basic arithmetic, comparisons
- [ ] Integration test: RawPerson → Person transform

### Phase 2: Conditional Logic

- [ ] `if(cond, then, else)` expression parsing
- [ ] Compile to `CASE WHEN` SQL
- [ ] Nested conditionals

### Phase 3: Aggregations

- [ ] `~groupBy` syntax in M2M mapping
- [ ] Aggregate functions: `count()`, `sum()`, `avg()`, `min()`, `max()`
- [ ] Compile to `GROUP BY` SQL

### Phase 4: Chained Mappings

- [ ] Resolve M2M → M2M → Relational chains
- [ ] Generate CTEs or nested subqueries
- [ ] Optional: CREATE VIEW generation

### Phase 5: Advanced

- [ ] Date/time functions with dialect support
- [ ] `coalesce()` / null handling
- [ ] Window functions (`rank()`, `rowNumber()`)

---

## Example: Complete End-to-End

### 1. Define Models

```pure
Class staging::RawPerson
{
    firstName: String[1];
    lastName: String[1];
    birthDate: Date[1];
    salary: Decimal[1];
    isActive: Boolean[1];
}

Class model::Person
{
    firstName: String[1];
    lastName: String[1];
    fullName: String[1];
    age: Integer[1];
    salaryBand: String[1];
}
```

### 2. Define Database & Relational Mapping

```pure
Database store::StagingDB
(
    Table T_RAW_PERSON
    (
        ID INTEGER PRIMARY KEY,
        FIRST_NAME VARCHAR(100),
        LAST_NAME VARCHAR(100),
        BIRTH_DATE DATE,
        SALARY DECIMAL(10,2),
        IS_ACTIVE BOOLEAN
    )
)

Mapping staging::RawPersonMapping
(
    RawPerson: Relational
    {
        ~mainTable [StagingDB] T_RAW_PERSON
        firstName: [StagingDB] T_RAW_PERSON.FIRST_NAME,
        lastName: [StagingDB] T_RAW_PERSON.LAST_NAME,
        birthDate: [StagingDB] T_RAW_PERSON.BIRTH_DATE,
        salary: [StagingDB] T_RAW_PERSON.SALARY,
        isActive: [StagingDB] T_RAW_PERSON.IS_ACTIVE
    }
)
```

### 3. Define M2M Transform

```pure
Mapping model::PersonTransform
(
    Person: Pure
    {
        ~src RawPerson
        ~filter $src.isActive == true
        
        firstName: $src.firstName,
        lastName: $src.lastName,
        fullName: $src.firstName + ' ' + $src.lastName,
        age: year(today()) - year($src.birthDate),
        salaryBand: if($src.salary < 50000, 'Entry', 
                      if($src.salary < 100000, 'Mid', 'Senior'))
    }
)
```

### 4. Query

```pure
Person.all()
    ->filter({p | $p.salaryBand == 'Senior'})
    ->project({p | $p.fullName}, {p | $p.age})
```

### 5. Generated SQL (DuckDB)

```sql
SELECT 
    t0.FIRST_NAME || ' ' || t0.LAST_NAME AS fullName,
    YEAR(CURRENT_DATE) - YEAR(t0.BIRTH_DATE) AS age
FROM T_RAW_PERSON AS t0
WHERE t0.IS_ACTIVE = true
  AND (
    CASE 
        WHEN t0.SALARY < 50000 THEN 'Entry'
        WHEN t0.SALARY < 100000 THEN 'Mid'
        ELSE 'Senior'
    END
  ) = 'Senior'
```

---

## Comparison: Legend Engine vs Legend Lite

| Aspect | Legend Engine | Legend Lite |
|--------|--------------|-------------|
| **Execution** | In-memory (Java/Pure) | Database only (SQL) |
| **Expressiveness** | Full Pure language | SQL-translatable subset |
| **Performance** | Memory-bound | Database-optimized |
| **Scalability** | Limited by JVM heap | Limited by database |
| **Complexity** | Higher (interpreter) | Lower (transpiler) |
| **Functions** | Any Pure function | Predefined SQL-mappable set |

---

## Open Questions

1. **Should we support M2M → M2M chains of arbitrary depth?**
   - Pro: Powerful composition
   - Con: Complex SQL (deeply nested CTEs)

2. **Should we auto-generate database VIEWs?**
   - Pro: Performance via materialization
   - Con: Schema management complexity

3. **How to handle M2M with associations?**
   - Same pattern as relational: EXISTS for filter, LEFT JOIN for project

4. **Should unsupported expressions fail-fast or silently skip?**
   - Recommendation: Fail-fast with clear error message

---

## References

- [Legend M2M Mapping Tutorial](https://legend.finos.org/docs/tutorials/studio-m2m-mapping)
- [Legend Language Reference](https://legend.finos.org/docs/reference/legend-language)
- [Legend Lite FAQ](./FAQ.md) - SQL generation patterns
