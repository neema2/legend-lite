# Model-to-Model Transforms - Design Document

This document outlines how Legend Lite can support **Model-to-Model (M2M) transformations** using **100% database execution** (no in-memory processing of business logic).

## Overview

### What is Model-to-Model?

In Legend, M2M transforms allow you to:
1. Define a **source model** (e.g., `Firm` from a staging table)
2. Define a **target model** (e.g., `NewFirm` with derived/computed properties)
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

## Pure Syntax for M2M Mappings (Legend-Compatible)

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

### Model-to-Model Mapping (Legend Syntax)

This is the **exact syntax** used by Legend Engine:

```pure
Class NewFirm
{
    shortenedCompanyType: String[1];
}

Mapping MyModelToModelMapping
(
    NewFirm[newFirmTag]: Pure
    {
        ~src Firm
        shortenedCompanyType: if(
            $src.companyType == CompanyType.LimitedLiabilityCorporation,
            |'LLC',
            |$src.companyType->toString()
        )
    }
)
```

### Key Syntax Elements (Legend-Compatible)

| Element | Description | Example |
|---------|-------------|---------|
| `TargetClass[tag]: Pure` | Declares Pure/M2M mapping with optional tag | `NewFirm[myTag]: Pure` |
| `~src SourceClass` | Declares the source class | `~src Firm` |
| `$src` | Reference to source instance | `$src.firstName` |
| `$src.property` | Access source property | `$src.companyType` |
| `+` on strings | String concatenation | `$src.first + ' ' + $src.last` |
| `if(cond, \|then, \|else)` | Conditional (**note the `\|` pipes**) | `if($src.age < 18, \|'Minor', \|'Adult')` |
| `->function()` | Function application (chained) | `$src.name->toUpper()` |
| `~filter expr` | Filter source rows | `~filter $src.isActive == true` |

### Lambda Syntax in Conditionals

Legend Pure uses **pipe `|`** before lambda bodies in `if` expressions:

```pure
// CORRECT Legend syntax - pipes before then/else values
if($src.age < 18, |'Minor', |'Adult')

// Nested conditionals
if($src.age < 18, 
   |'Minor', 
   |if($src.age < 65, |'Adult', |'Senior'))
```

### Function Chaining with `->`

Legend Pure uses arrow `->` for method-style function calls:

```pure
// Legend syntax - arrow chaining
$src.firstName->toUpper()
$src.name->trim()->toLower()
$src.companyType->toString()

// Also supports prefix style for some functions
toUpper($src.firstName)
```

---

## Complete M2M Examples (Legend Syntax)

### Example 1: Simple Property Mapping

```pure
Class TargetPerson
{
    fullName: String[1];
    upperLastName: String[1];
}

Mapping model::PersonTransform
(
    TargetPerson: Pure
    {
        ~src Person
        fullName: $src.firstName + ' ' + $src.lastName,
        upperLastName: $src.lastName->toUpper()
    }
)
```

### Example 2: With Filter

```pure
Mapping model::ActivePersonTransform
(
    ActivePerson: Pure
    {
        ~src Person
        ~filter $src.isActive == true
        
        name: $src.firstName + ' ' + $src.lastName
    }
)
```

### Example 3: Complex Conditional

```pure
Mapping model::PersonWithAgeGroup
(
    PersonView: Pure
    {
        ~src Person
        firstName: $src.firstName,
        lastName: $src.lastName,
        ageGroup: if($src.age < 18, 
                     |'Minor', 
                     |if($src.age < 65, |'Adult', |'Senior'))
    }
)
```

### Example 4: Enum Handling

```pure
Enum CompanyType
{
    LimitedLiabilityCorporation,
    Corporation,
    Partnership
}

Mapping model::FirmTransform
(
    NewFirm: Pure
    {
        ~src Firm
        shortenedCompanyType: if(
            $src.companyType == CompanyType.LimitedLiabilityCorporation,
            |'LLC',
            |$src.companyType->toString()
        )
    }
)
```

---

## SQL Compilation Strategy

### Simple Property Mapping

**Pure:**
```pure
firstName: $src.firstName
```

**SQL:**
```sql
SELECT t0.FIRST_NAME AS firstName FROM T_PERSON AS t0
```

### String Concatenation

**Pure:**
```pure
fullName: $src.firstName + ' ' + $src.lastName
```

**SQL (DuckDB/PostgreSQL/SQLite):**
```sql
SELECT t0.FIRST_NAME || ' ' || t0.LAST_NAME AS fullName FROM T_PERSON AS t0
```

### Function Chaining

**Pure:**
```pure
upperName: $src.firstName->toUpper()
cleanName: $src.name->trim()->toLower()
```

**SQL:**
```sql
SELECT UPPER(t0.FIRST_NAME) AS upperName FROM T_PERSON AS t0
SELECT LOWER(TRIM(t0.NAME)) AS cleanName FROM T_PERSON AS t0
```

### Conditional Logic with Pipes

**Pure:**
```pure
ageGroup: if($src.age < 18, |'Minor', |if($src.age < 65, |'Adult', |'Senior'))
```

**SQL:**
```sql
SELECT 
    CASE 
        WHEN t0.AGE < 18 THEN 'Minor'
        WHEN t0.AGE < 65 THEN 'Adult'
        ELSE 'Senior'
    END AS ageGroup
FROM T_PERSON AS t0
```

### Filter

**Pure:**
```pure
~filter $src.isActive == true
```

**SQL:**
```sql
SELECT ... FROM T_PERSON AS t0 WHERE t0.IS_ACTIVE = true
```

### Aggregation with GroupBy

**Pure:**
```pure
Mapping model::OrderSummary
(
    CustomerSummary: Pure
    {
        ~src Order
        ~groupBy $src.customerId
        
        customerId: $src.customerId,
        orderCount: $src->count(),
        totalAmount: $src.amount->sum(),
        avgAmount: $src.amount->avg()
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
│   ├── FunctionCallExpr.java          # NEW: ->function() calls
│   ├── IfExpression.java              # NEW: if(cond, |then, |else)
│   ├── AggregateExpr.java             # NEW: ->count(), ->sum(), etc.
│   └── M2MExpressionParser.java       # NEW: Parse M2M expressions
│
└── compiler/
    └── M2MCompiler.java               # NEW: Compile M2M mapping to RelationNode
```

### Parsing the Pipe Syntax

The `|` in `if(cond, |then, |else)` represents lambda expressions in Pure. For M2M, these are typically simple value expressions:

```java
// Lexer needs to handle:
// - PIPE token for |
// - Parsing |'value' as a lambda returning 'value'
// - Parsing |$src.prop as a lambda returning that property

public record IfExpression(
    M2MExpression condition,
    M2MExpression thenBranch,  // The expression after first |
    M2MExpression elseBranch   // The expression after second |
) implements M2MExpression { }
```

### Supported Functions (SQL-Translatable)

| Category | Pure Syntax | SQL Equivalent |
|----------|-------------|----------------|
| **String** | `->toUpper()`, `->toLower()`, `->trim()`, `->substring(start, end)`, `->length()`, `+` | `UPPER()`, `LOWER()`, `TRIM()`, `SUBSTR()`, `LENGTH()`, `\|\|` |
| **Math** | `+`, `-`, `*`, `/`, `->abs()`, `->round()`, `->ceiling()`, `->floor()` | Same |
| **Date** | `->today()`, `->year()`, `->month()`, `->dayOfMonth()` | `CURRENT_DATE`, `YEAR()`, `MONTH()`, `DAY()` |
| **Aggregate** | `->count()`, `->sum()`, `->avg()`, `->min()`, `->max()` | Same |
| **Conditional** | `if(cond, \|then, \|else)`, `->isEmpty()`, `->isNotEmpty()` | `CASE WHEN`, `IS NULL`, `IS NOT NULL` |
| **Boolean** | `&&`, `\|\|`, `!`, `==`, `!=`, `<`, `>`, `<=`, `>=` | `AND`, `OR`, `NOT`, `=`, `<>`, etc. |
| **Type** | `->toString()`, `->toInteger()`, `->toFloat()` | `CAST()` |

### Functions NOT Supported (require in-memory fallback)

- `->map()`, `->filter()` with complex lambdas on collections
- `->fold()`, `->reduce()` 
- Recursion / graph traversal
- External service calls
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
        firstName: $src.firstName->trim(),
        lastName: $src.lastName->toUpper()
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

### Phase 1: Core M2M (Simple Properties + String Functions)

- [ ] Parse `~src ClassName` in mapping definitions
- [ ] Parse `$src.property` references
- [ ] Parse `->function()` chained calls
- [ ] Support string concat (`+`), `->toUpper()`, `->toLower()`, `->trim()`
- [ ] Compiler: M2M expression → SQL Expression
- [ ] Integration test: Person → TargetPerson transform

### Phase 2: Conditional Logic

- [ ] Parse `if(cond, |then, |else)` with pipe syntax
- [ ] Handle nested conditionals
- [ ] Compile to `CASE WHEN` SQL

### Phase 3: Filters

- [ ] Parse `~filter $src.property == value`
- [ ] Compile to `WHERE` clause

### Phase 4: Aggregations

- [ ] Parse `~groupBy $src.property`
- [ ] Parse aggregate functions: `->count()`, `->sum()`, `->avg()`, `->min()`, `->max()`
- [ ] Compile to `GROUP BY` SQL

### Phase 5: Chained Mappings

- [ ] Resolve M2M → M2M → Relational chains
- [ ] Generate CTEs or nested subqueries
- [ ] Optional: CREATE VIEW generation

### Phase 6: Advanced

- [ ] Date/time functions with dialect support
- [ ] `->isEmpty()` / null handling
- [ ] Enum handling with `->toString()`
- [ ] Window functions (`->rank()`, `->rowNumber()`)

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

### 3. Define M2M Transform (Legend Syntax)

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
        age: today()->year() - $src.birthDate->year(),
        salaryBand: if($src.salary < 50000, 
                       |'Entry', 
                       |if($src.salary < 100000, |'Mid', |'Senior'))
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
| **Syntax** | Full Pure language | Same Pure syntax (subset) |
| **Expressiveness** | Any Pure function | SQL-translatable functions |
| **Performance** | Memory-bound | Database-optimized |
| **Scalability** | Limited by JVM heap | Limited by database |
| **Compatibility** | Full | Backwards-compatible subset |

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

4. **Should unsupported functions fail-fast or error?**
   - Recommendation: Fail-fast with clear error message listing supported functions

---

## References

- [Legend M2M Mapping Tutorial](https://legend.finos.org/docs/tutorials/studio-m2m-mapping)
- [Legend Language Reference](https://legend.finos.org/docs/reference/legend-language)
- [Legend Lite FAQ](../FAQ.md) - SQL generation patterns
