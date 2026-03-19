# Frequently Asked Questions

## General

### What is Legend Lite?

Legend Lite is a **clean-room reimplementation** of the [FINOS Legend](https://legend.finos.org/) Engine — the open-source data management platform originally created by Goldman Sachs. It reads the same Pure modeling language but compiles every query to a **single SQL statement** executed entirely inside the database.

### How is Legend Lite different from the original Legend Engine?

| Aspect | Legend Engine | Legend Lite |
|--------|-------------|-------------|
| **Size** | ~2M LOC, 400+ Maven modules | ~25K LOC, 3 modules |
| **Execution** | Mixed SQL + in-memory Java | 100% SQL push-down |
| **Build** | 15–30 minutes | 19 seconds (clean + 955 tests) |
| **Java** | Java 11, classes, visitors | Java 21, records, sealed interfaces, pattern matching |
| **Dependencies** | Hundreds of JARs | DuckDB, ANTLR, JUnit |
| **Databases** | Postgres, Databricks, Snowflake, etc. | DuckDB (primary), SQLite |
| **Pure coverage** | Full language | Relational subset (growing) |

### Is Legend Lite a fork of Legend Engine?

No. Legend Lite is a **clean-room implementation** — not a single line of code was copied from the Legend Engine. It reads the same Pure syntax but compiles it through a completely different pipeline built from scratch.

### What does "100% SQL push-down" mean?

Every Pure query compiles to a single SQL statement. No rows are fetched into the JVM for processing. Operations like `filter`, `groupBy`, `sort`, `if/else`, `fold`, struct construction, graph fetch, and string functions all compile directly to SQL constructs.

For example, this Pure:

```pure
Person.all()
  ->filter({p | $p.age > 30})
  ->project([p|$p.firstName, p|$p.lastName], ['first', 'last'])
  ->sort(ascending('last'))
  ->limit(10)
```

Compiles to:

```sql
SELECT t0.FIRST_NAME AS first, t0.LAST_NAME AS last
FROM T_PERSON AS t0
WHERE t0.AGE > 30
ORDER BY last ASC
LIMIT 10
```

---

## Architecture & Design

### Why Java records and sealed interfaces?

Legend Lite uses Java 21 features extensively:

**Records** — every AST node is a record (`CString`, `CInteger`, `AppliedFunction`, `LambdaFunction`, etc.), giving us:
- Immutability by default
- Automatic `equals()`, `hashCode()`, `toString()`
- Record deconstruction in pattern matching
- Dramatically less boilerplate

**Sealed interfaces** — `ValueSpecification`, `GenericType`, `SqlExpr` are sealed, enabling:
- Exhaustive `switch` expressions (compiler verifies all cases)
- No runtime `instanceof` surprises
- Clear, documented type hierarchies

```java
// The compiler guarantees every case is handled
return switch (vs) {
    case CString(String value) -> new SqlExpr.StringLiteral(value);
    case CInteger(Number value) -> new SqlExpr.NumericLiteral(value);
    case AppliedFunction af -> generateFunction(af);
    case LambdaFunction lf -> generateLambda(lf);
    // ... all other cases
};
```

### Why a hand-written parser instead of ANTLR?

The engine actually uses **both**:

- **ANTLR** — parses Pure *expressions* (value specifications, lambda bodies, function applications). The grammar is in `PureLexer.g4` and `PureParser.g4`.
- **Hand-written** — parses Pure *definitions* (classes, mappings, databases, runtimes). The recursive descent parser in `PureParser.java` handles the structural elements.

The hand-written parser was chosen for definitions because:
1. Pure's definition syntax is context-sensitive in ways that make ANTLR grammar rules awkward
2. No additional generated code for the definition layer
3. Easier to debug and extend
4. GraalVM native-image compatibility (ANTLR runtime uses reflection)

### What is the "side-table metadata" pattern?

The `TypeChecker` annotates every AST node with a `TypeInfo` record containing:
- Scalar type and multiplicity
- Relation columns (for tabular operations)
- Inlined body expressions (for compiler desugaring like `fold→concatenate`)
- Join path metadata

This metadata is stored in a `Map<ValueSpecification, TypeInfo>` (the "side table") rather than mutating the AST. The `PlanGenerator` reads it during SQL emission without re-walking the model. This keeps the AST immutable while still propagating rich type information.

### What is the dialect abstraction layer?

The `PlanGenerator` never emits raw SQL strings. Instead, it builds a structural `SqlExpr` tree using semantic names:

```java
// PlanGenerator emits this:
new SqlExpr.FunctionCall("listTransform", List.of(list, lambda))

// DuckDB dialect renders it as:
"LIST_TRANSFORM(list, lambda)"
```

This means adding a new database dialect only requires implementing the rendering layer — the compilation logic is shared.

---

## SQL Generation

### Why `EXISTS` instead of `INNER JOIN` for to-many filters?

When filtering `Person.all()->filter({p | $p.addresses.city == 'New York'})`, Legend Lite generates:

```sql
SELECT t0.* FROM T_PERSON AS t0
WHERE EXISTS (
    SELECT 1 FROM T_ADDRESS AS sub1
    WHERE sub1.PERSON_ID = t0.ID AND sub1.CITY = 'New York'
)
```

Not:

```sql
-- WRONG: causes row explosion + requires DISTINCT
SELECT DISTINCT t0.* FROM T_PERSON t0
INNER JOIN T_ADDRESS a ON t0.ID = a.PERSON_ID
WHERE a.CITY = 'New York'
```

**Why EXISTS is better:**
- No row explosion — each person appears exactly once
- Database can short-circuit on first matching address
- No expensive `DISTINCT` required
- Clear semantics: we're querying *people*, not person-address pairs

### Why `LEFT OUTER JOIN` for to-many projections?

When projecting through a to-many association, we intentionally want row multiplication:

```pure
Person.all()->project({p | $p.firstName}, {p | $p.addresses.street})
```

```sql
SELECT t0.FIRST_NAME, j2.STREET
FROM T_PERSON t0
LEFT OUTER JOIN T_ADDRESS j2 ON t0.ID = j2.PERSON_ID
```

**Why LEFT (not INNER)?** Preserves people with no addresses (NULL for street). This matches Pure's `[*]` multiplicity — "zero or more."

**Why JOIN (not EXISTS)?** We need actual data for the `SELECT` clause, not just an existence check.

### Should `WHERE EXISTS` come before or after the `JOIN`?

For queries that both filter AND project through an association, Legend Lite generates:

```sql
SELECT t0.FIRST_NAME, j2.STREET
FROM T_PERSON AS t0
LEFT OUTER JOIN T_ADDRESS AS j2 ON t0.ID = j2.PERSON_ID
WHERE EXISTS (SELECT 1 FROM T_ADDRESS AS sub1 WHERE sub1.PERSON_ID = t0.ID AND sub1.CITY = 'New York')
```

The JOIN and EXISTS serve **different purposes**:

| Construct | Purpose | Rows Affected |
|-----------|---------|---------------|
| `LEFT JOIN` | Retrieve data for projection | Multiplies rows (one per address) |
| `EXISTS` | Check if any matching row exists | Filters base entity rows |

Modern query optimizers (DuckDB, PostgreSQL, SQLite) will rewrite to the most efficient execution plan regardless of SQL text order. We chose the flat style because it generates simpler SQL and is easier to debug.

### How does struct/graph fetch compilation work?

Pure graph fetch queries like:

```pure
Person.all()->graphFetch(#{Person{firstName, addresses{city, street}}}#)
```

Compile to DuckDB STRUCT types:

```sql
SELECT ROW(t0.FIRST_NAME,
           (SELECT LIST(ROW(j1.CITY, j1.STREET))
            FROM T_ADDRESS j1 WHERE j1.PERSON_ID = t0.ID))
FROM T_PERSON t0
```

The nested association becomes a correlated subquery that returns a `LIST` of `STRUCT` values — the entire object graph is assembled in a single SQL statement.

---

## Results & Serialization

### What result formats are supported?

| Format | Serializer | Streaming | Dependencies |
|--------|-----------|-----------|--------------|
| **JSON** | `JsonSerializer` | ✅ | None |
| **CSV** | `CsvSerializer` | ✅ | None |

Both serializers are hand-rolled with zero external dependencies.

### How do I add a custom serializer?

Implement the `ResultSerializer` interface:

```java
public class ArrowSerializer implements ResultSerializer {
    @Override public String formatId() { return "arrow"; }
    @Override public String contentType() { return "application/vnd.apache.arrow.stream"; }
    @Override public void serialize(BufferedResult result, OutputStream out) { /* ... */ }
}
```

Register it:

```java
SerializerRegistry.register(ArrowSerializer.INSTANCE);
```

Use it:

```java
queryService.executeAndSerialize(pureSource, query, runtime, out, "arrow");
```

---

## NLQ (Natural Language Query)

### How does the NLQ pipeline work?

The pipeline has 5 stages:

1. **Semantic Retrieval** — TF-IDF index over class names, property names, descriptions, and NLQ annotations. Returns top-K candidate classes relevant to the question.
2. **Semantic Router** — LLM identifies the root class from candidates (e.g., "Trade" for "show me total notional by desk").
3. **Query Planner** — LLM builds a structured JSON plan (projections, filters, aggregations, sorts).
4. **Pure Generator** — LLM generates Pure syntax from the plan, with retry on parse failures.
5. **Parse Validation** — `PureParser.parse()` hard-gates the output — syntactically invalid queries are rejected.

### What LLM does NLQ use?

Currently Google Gemini (`gemini-3-flash-preview`). The provider and model are configurable via environment variables:

```bash
LLM_PROVIDER=gemini          # default
GEMINI_MODEL=gemini-3-flash-preview  # default
GEMINI_API_KEY=your-key       # required
```

### How do I improve NLQ accuracy for my model?

Add NLQ annotations to your Pure classes:

```pure
Profile nlq {
  tags: [description, synonyms, businessDomain, importance,
         exampleQuestions, displayName, sampleValues, unit];
}

Class {nlq.description = 'Individual trade execution'} model::Trade {
  {nlq.synonyms = 'deal, transaction, execution'} notional: Float[1];
  {nlq.unit = 'USD'} price: Float[1];
  {nlq.sampleValues = 'FX, Rates, Credit'} desk: String[1];
}
```

These annotations are indexed by the semantic retrieval stage and included in LLM prompts.

---

## Build & Development

### What are the build times?

| Command | Time | What it does |
|---------|------|-------------|
| `mvn clean install -DskipTests` | ~8s | ANTLR generate + compile all modules |
| `mvn clean test -pl engine` | ~19s | Clean compile + 955 tests |
| `mvn test -pl engine` | ~8s | Incremental (tests only) |

### Why is the build so fast?

1. **3 modules** instead of 400+ — minimal Maven overhead
2. **Zero heavyweight dependencies** — no Spring, no Guice, no ORM
3. **In-memory DuckDB** — tests create/destroy databases in microseconds
4. **No network I/O in tests** — everything runs locally
5. **Small codebase** — ~25K LOC compiles in seconds

### How do I run a single test?

```bash
mvn test -pl engine -Dtest="DuckDBIntegrationTest"
mvn test -pl engine -Dtest="TypeCheckerTest#testFilterWithAssociation"
```

### How is the project structured?

```
engine/
├── src/main/java/com/gs/legend/
│   ├── ast/          28 record types (ValueSpecification hierarchy)
│   ├── antlr/        ANTLR visitors → AST
│   ├── parser/       Pure definition parser
│   ├── compiler/     TypeChecker + TypeInfo
│   ├── model/        PureModelBuilder + ModelContext
│   ├── plan/         PlanGenerator + ExecutionNode
│   ├── sql/          SqlBuilder (structural SQL AST)
│   ├── sqlgen/       SqlDialect → SQL text
│   ├── exec/         PlanExecutor + ExecutionResult
│   ├── serial/       JSON/CSV serializers
│   ├── server/       LegendHttpServer
│   └── service/      QueryService orchestration
├── src/main/antlr4/  ANTLR grammars (PureLexer.g4, PureParser.g4)
└── src/test/java/    41 test files, 955 tests
```

---

## Compatibility

### Which Pure functions are supported?

Legend Lite supports a large subset of Pure functions. Major categories:

| Category | Functions |
|----------|----------|
| **Collection** | `filter`, `map`, `sort`, `limit`, `drop`, `take`, `slice`, `distinct`, `first`, `last`, `at`, `size`, `contains`, `in`, `isEmpty`, `isNotEmpty`, `fold`, `find`, `forAll`, `exists`, `concatenate`, `zip`, `head`, `tail`, `init`, `reverse`, `indexOf`, `removeDuplicates` |
| **Aggregation** | `sum`, `average`, `mean`, `min`, `max`, `count`, `percentile`, `variancePopulation`, `varianceSample`, `stdDevPopulation`, `stdDevSample` |
| **String** | `contains`, `startsWith`, `endsWith`, `toLower`, `toUpper`, `trim`, `length`, `substring`, `indexOf`, `replace`, `split`, `joinStrings`, `matches`, `left`, `right`, `ltrim`, `rtrim`, `repeatString`, `reverseString`, `lpad`, `rpad`, `ascii`, `char`, `parseInteger`, `parseFloat`, `parseDecimal`, `encodeBase64`, `decodeBase64`, `hash` |
| **Math** | `abs`, `ceiling`, `floor`, `round`, `sqrt`, `pow`, `log`, `exp`, `mod`, `rem`, `sign`, `cbrt` |
| **Trig** | `sin`, `cos`, `tan`, `asin`, `acos`, `atan`, `atan2`, `sinh`, `cosh`, `tanh`, `cot`, `toRadians`, `toDegrees`, `pi` |
| **Date/Time** | `year`, `month`, `dayOfMonth`, `hour`, `minute`, `second`, `quarter`, `dayOfWeek`, `dayOfYear`, `weekOfYear`, `adjust`, `dateDiff`, `datePart`, `date`, `now`, `today`, `firstDayOfMonth`, `firstDayOfYear`, `firstDayOfQuarter`, `hasHour`, `hasMinute`, `hasDay`, `hasMonth`, `hasSecond`, `hasSubsecond`, `timeBucket`, `parseDate`, `fromEpochValue`, `toEpochValue` |
| **Boolean** | `and`, `or`, `not`, `if`, `equal`, `lessThan`, `greaterThan` |
| **Bitwise** | `bitAnd`, `bitOr`, `bitXor`, `bitNot`, `bitShiftLeft`, `bitShiftRight` |
| **Type** | `cast`, `toOne`, `toOneMany`, `toString`, `toInteger`, `toFloat`, `toDecimal` |

### What about functions not listed above?

If a Pure function doesn't have a SQL translation, the compiler will throw a `PureCompileException` with a clear error message. No silent fallback to in-memory processing.

### Does Legend Lite support Model-to-Model (M2M) transforms?

M2M support is planned. See [docs/MODEL_TO_MODEL.md](docs/MODEL_TO_MODEL.md) for the design document.

---

## Contributing

Have a question not answered here? Open an issue on [GitHub](https://github.com/neema2/legend-lite)!
