# Frequently Asked Questions

## General

### What is Legend Lite?

Legend Lite is a **clean-sheet implementation** of the [FINOS Legend](https://legend.finos.org/) platform's Pure language and engine — the open-source data management platform originally created by Goldman Sachs — designed and written anew, taking [legend-pure](https://github.com/finos/legend-pure) and [legend-engine](https://github.com/finos/legend-engine) as its specification (see [`NOTICE`](NOTICE)). It reads the same Pure modeling language but compiles every query to a **single SQL statement** executed entirely inside the database.

### How is Legend Lite different from the original Legend Engine?

| Aspect | Legend Engine | Legend Lite |
|--------|-------------|-------------|
| **Size** | ~2M LOC, 400+ Maven modules | ~25K LOC, 3 modules |
| **Execution** | Mixed SQL + in-memory Java | 100% SQL push-down |
| **Build** | 15–30 minutes | 19 seconds (clean + 955 tests) |
| **Java** | Java 11, classes, visitors | Java 21, records, sealed interfaces, pattern matching |
| **Dependencies** | Hundreds of JARs | DuckDB, H2, JUnit — **no ANTLR** |
| **Databases** | Postgres, Databricks, Snowflake, etc. | DuckDB (primary), SQLite |
| **Pure coverage** | Full language | Relational subset (growing) |

### Is Legend Lite a fork of Legend Engine?

No. Legend Lite is a **clean-sheet implementation**: its code was written anew, not copied from legend-pure or legend-engine. It takes them as its specification — their grammar, typing rules and behaviour — and tests itself against their test suites and recorded answers, which [`NOTICE`](NOTICE) credits; some of those test fixtures are included, under their Apache 2.0 license. It reads the same Pure syntax but compiles it through a completely different pipeline built from scratch.

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

**There is no ANTLR anywhere in Legend Lite** — not in any pom, not in any
module. Both halves are hand-written recursive descent in `core/parser/`:
`ElementParser` for definitions (classes, mappings, databases, runtimes) and
`SpecParser` for expressions (value specifications, lambda bodies,
applications).

Upstream legend-engine uses ANTLR for both. The measured reasons for not
following it (`docs/PARSER_DROP_IN.md` §0):

1. **Memory** — 1,281 bytes allocated per source character vs ~39. That is
   3.6 GB to parse 2.8 MB of source, plus ~31 MB retained just to warm the
   parser. The strongest reason by a distance.
2. **Generated code** — 156 `.g4` files, 14,568 generated lines, 46% of the
   grammar jar's bytes.
3. **Build** — an `antlr4-maven-plugin` bound to `generate-sources` in 41 modules.
4. **Currency** — upstream is pinned to ANTLR 4.8-1 (2020) and can only move
   in lockstep with all generated code.
5. **Debuggability** — stepping recursive descent versus an ATN simulator.
6. Speed — ~54× on parse, and explicitly the *weakest* of the six. There is no
   latency gate; the project is justified at 0% speedup.

The parser is a byte-for-byte drop-in candidate for upstream's: 22,725/22,725
elements currently emit identical protocol JSON, checked against legend-engine
running live in-process (gate 8).

### How does type information flow to the backend?

**Through the tree, not beside it.** Phase G (`compiler/spec/SpecCompiler`,
`Typer`, `InferenceKernel`) consumes the untyped `protocol.spec.ValueSpecification`
and produces a *different*, typed tree: `compiler.spec.typed.TypedSpec`, a
sealed hierarchy of ~70 variants carrying type, multiplicity, relation columns
and resolved callee signatures on the nodes themselves.

> **The legacy engine used a side table** — a `Map<ValueSpecification, TypeInfo>`
> threaded alongside the AST. Core deliberately does not: `core/README.md`
> invariant 8 forbids mutable sidecar state across passes. Each phase takes
> input and returns output. If you find yourself wanting an
> `IdentityHashMap<TypedSpec, ?>` spanning a phase boundary, that is the smell
> the invariant names.

### What is the dialect abstraction layer?

The Lowerer (`lowering/Lowerer`, phase I) never emits raw SQL. It builds a
sealed, dialect-free MIR under `sql/` — `SqlQuery`, `SqlSource`, `SqlExpr`,
`SqlAgg` — which the dialect then renders through a single entry point,
`SqlDialect.render(SqlQuery)`.

Crucially, **there is no `FunctionCall(String name, args)` catch-all in the
MIR**. Every operation is its own typed record, so adding a Pure native means
adding a variant *and* a render arm — and because render methods are switch
expressions with no `default ->`, javac fails the build until you do. That is
`AGENTS.md` invariant 3a.

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
| `mvn clean install -DskipTests` | ~8s | compile all modules |
| `mvn clean test -pl engine` | ~19s | Clean compile + 955 tests |
| `mvn test -pl engine` | ~8s | Incremental (tests only) |

### Why is the build so fast?

1. **5 modules** instead of 400+ — minimal Maven overhead
2. **Zero heavyweight dependencies** — no Spring, no Guice, no ORM, no ANTLR
3. **In-memory DuckDB** — tests create/destroy databases in microseconds
4. **No network I/O in tests** — everything runs locally
5. **No code generation step** — the parser is hand-written

### How do I run a single test?

```bash
mvn test -pl engine -Dtest="DuckDBIntegrationTest"
mvn -pl core test -Dtest="TyperTest"     # core is where the compiler lives
```

### How is the project structured?

```
core/    the compiler — 418 files, ~120K LOC. THIS is where work happens.
  lexer/ parser/ protocol/ model/          text → parsed → wire records
  compiler/  NameResolver, element/ (F), spec/ (G — Typer, checkers)
  normalizer/ resolver/ lowering/           E, H, I
  sql/ + sql/dialect/                       MIR (sealed, pure data) → SQL
  exec/                                     JDBC → ExecutionResult
  Compiler.java                             the one driver

engine/  FROZEN legacy + the still-live wire layer:
  server/ (HTTP, LSP, diagrams)  service/  serial/  util/Json
  everything else is superseded by core/

nlq/     natural language → Pure (LLM pipeline, optional)
pct/     legend-pure's own PCT suite, run against legend-lite
parser-equivalence/  byte-equivalence vs legend-engine's parser
```

See `AGENTS.md` for the layer contract and `core/README.md` for per-package detail.


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
