# Legend Lite

A **clean-room reimplementation** of the [FINOS Legend](https://legend.finos.org/) Engine in ~25K lines of modern Java 21. Legend Lite compiles Pure models and queries to SQL and executes them entirely inside the database — **zero rows are ever fetched into the JVM**.

---

## What is Legend?

[Legend](https://legend.finos.org/) is an open-source data management platform created by Goldman Sachs and donated to FINOS. Its core idea: **define your data model once in a formal language (Pure), then map it to physical databases and query it with type-safe semantics.**

Legend Lite reimplements this in a fraction of the code, with 100% SQL push-down execution.

---

## The Pure Language

Pure is Legend's modeling and query language. You define **what your data looks like** (classes), **where it lives** (databases + mappings), and **how to access it** (runtimes + connections). Then you query it with a type-safe expression language that compiles to SQL.

### 1. Classes — Define Your Data Model

Classes describe the shape of your domain objects:

```pure
Class model::Person
{
    firstName: String[1];
    lastName:  String[1];
    age:       Integer[1];
}

Class model::Address
{
    street: String[1];
    city:   String[1];
}
```

- `String[1]` — required, exactly one value
- `String[0..1]` — optional
- `String[*]` — zero or more (collection)
- Types: `String`, `Integer`, `Float`, `Decimal`, `Boolean`, `Date`, `DateTime`, `StrictDate`

### 2. Associations — Link Classes Together

Associations define relationships between classes:

```pure
Association model::Person_Address
{
    person:    Person[1];
    addresses: Address[*];
}
```

This says: a Person has many Addresses, and each Address belongs to one Person. Both sides are navigable — you can traverse `$person.addresses` or `$address.person` in queries.

### 3. Inheritance — Extend Classes

```pure
Class model::GeographicEntity
{
}

Class model::Address extends model::GeographicEntity
{
    name: String[1];
}

Class model::Location extends model::GeographicEntity
{
    place: String[1];
}
```

### 4. Databases — Describe Physical Storage

Database definitions describe tables, columns, and joins:

```pure
Database store::PersonDatabase
(
    Table T_PERSON
    (
        ID         INTEGER PRIMARY KEY,
        FIRST_NAME VARCHAR(100) NOT NULL,
        LAST_NAME  VARCHAR(100) NOT NULL,
        AGE_VAL    INTEGER NOT NULL
    )
    Table T_ADDRESS
    (
        ID        INTEGER PRIMARY KEY,
        PERSON_ID INTEGER NOT NULL,
        STREET    VARCHAR(200) NOT NULL,
        CITY      VARCHAR(100) NOT NULL
    )
    Join Person_Address(T_PERSON.ID = T_ADDRESS.PERSON_ID)
)
```

### 5. Mappings — Connect Models to Storage

Mappings tell the engine how class properties map to table columns:

```pure
Mapping model::PersonMapping
(
    Person: Relational
    {
        ~mainTable [PersonDatabase] T_PERSON
        firstName: [PersonDatabase] T_PERSON.FIRST_NAME,
        lastName:  [PersonDatabase] T_PERSON.LAST_NAME,
        age:       [PersonDatabase] T_PERSON.AGE_VAL
    }

    Address: Relational
    {
        ~mainTable [PersonDatabase] T_ADDRESS
        street: [PersonDatabase] T_ADDRESS.STREET,
        city:   [PersonDatabase] T_ADDRESS.CITY
    }
)
```

A single class can be mapped to **different databases** — swap the mapping at runtime to query Postgres, DuckDB, or SQLite with the same model.

### 6. Model-to-Model Mappings — Transform Between Classes

M2M mappings transform one class into another using Pure expressions. The engine compiles all transforms to SQL — no in-memory processing:

```pure
// Source class (raw staging data)
Class model::RawPerson
{
    firstName: String[1];
    lastName:  String[1];
    age:       Integer[1];
    salary:    Float[1];
    isActive:  Boolean[1];
}

// Target class (clean business model)
Class model::Person
{
    fullName:      String[1];
    upperLastName: String[1];
}

// M2M mapping: RawPerson → Person
Mapping model::PersonM2MMapping
(
    Person: Pure
    {
        ~src RawPerson
        fullName:      $src.firstName + ' ' + $src.lastName,
        upperLastName: $src.lastName->toUpper()
    }
)
```

- `~src RawPerson` — declares the source class
- `$src.property` — references a source property
- String concat (`+`), function chaining (`->toUpper()`), and conditionals all compile to SQL

**Conditionals, filters, and nested if/else:**

```pure
// Conditional: age grouping with nested if/else → CASE WHEN
Mapping model::ConditionalMapping
(
    PersonView: Pure
    {
        ~src RawPerson
        firstName: $src.firstName,
        ageGroup:  if($src.age < 18, |'Minor', |if($src.age < 65, |'Adult', |'Senior'))
    }
)

// Filter: only include active people → WHERE clause
Mapping model::FilteredMapping
(
    ActivePerson: Pure
    {
        ~src RawPerson
        ~filter $src.isActive == true
        firstName: $src.firstName,
        lastName:  $src.lastName
    }
)
```

**Chained M2M — compose transforms through multiple layers:**

```pure
// PersonSummary → Person → RawPerson (resolves through the full chain to SQL)
Mapping model::ChainedM2MMapping
(
    PersonSummary: Pure
    {
        ~src Person
        name:      $src.fullName,
        nameUpper: $src.upperLastName
    }
)
```

**Nested associations in M2M — `@JoinName` for deep-fetch:**

```pure
Mapping model::DeepFetchMapping
(
    PersonWithAddress: Pure
    {
        ~src RawPerson
        fullName: $src.firstName + ' ' + $src.lastName,
        address:  @PersonAddress
    }
    Address: Pure
    {
        ~src RawAddress
        city:   $src.city,
        street: $src.street
    }
)
```

### 7. Graph Fetch — Nested / Structured Results

Instead of flat tabular output, `graphFetch` returns nested JSON objects matching the class graph shape:

```pure
// Flat properties
Person.all()
  ->graphFetch(#{ Person { fullName, upperLastName } }#)
  ->serialize(#{ Person { fullName, upperLastName } }#)
```

```json
[{"fullName": "John Smith", "upperLastName": "SMITH"},
 {"fullName": "Jane Doe", "upperLastName": "DOE"}]
```

**Nested associations — objects within objects:**

```pure
PersonWithAddress.all()
  ->graphFetch(#{ PersonWithAddress { fullName, address { city, street } } }#)
  ->serialize(#{ PersonWithAddress { fullName, address { city, street } } }#)
```

```json
[{"fullName": "John Smith", "address": [{"city": "New York", "street": "123 Main St"}]},
 {"fullName": "Bob Jones",  "address": [{"city": "Chicago", "street": "789 Pine Rd"},
                                         {"city": "Seattle", "street": "321 Elm Blvd"}]}]
```

The entire object graph — including nested one-to-many associations — is assembled in a **single SQL statement** using DuckDB's `ROW()` and `LIST()` constructs with correlated subqueries.

### 8. Connections & Runtimes — Bind to a Live Database

```pure
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
        model::PersonMapping
    ];
    connections:
    [
        store::PersonDatabase:
        [
            environment: store::TestConnection
        ]
    ];
}
```

The Runtime binds everything together: which mapping to use, and which database connection to target. Change the runtime to switch databases without touching your model or queries.

### 9. Queries — Type-Safe Expressions

Query the model with Pure expressions that compile to SQL:

```pure
// Filter + project (tabular)
Person.all()
  ->filter({p | $p.age > 30})
  ->project([p|$p.firstName, p|$p.lastName], ['first', 'last'])

// Navigate through associations
Person.all()
  ->filter({p | $p.addresses.city == 'New York'})
  ->project({p | $p.firstName}, {p | $p.addresses.street})

// GroupBy with aggregation
Person.all()
  ->project([p|$p.lastName, p|$p.age], ['name', 'age'])
  ->groupBy([{r|$r.name}], [{r|$r.age->average()}], ['name', 'avgAge'])

// Sort + limit
Person.all()
  ->project([p|$p.firstName, p|$p.age], ['name', 'age'])
  ->sort(descending('age'))
  ->limit(5)

// Graph fetch (nested/structured JSON)
Person.all()
  ->graphFetch(#{ Person { fullName, upperLastName } }#)
  ->serialize(#{ Person { fullName, upperLastName } }#)

// Conditional logic
Person.all()->project({p | if($p.age < 18, |'Minor', |'Adult')}, 'category')
```

**Every one of these compiles to a single SQL statement.** Associations become JOINs, to-many filters become EXISTS, graph fetches become nested STRUCTs, M2M transforms become inline SQL expressions — all pushed down to the database.

---

## Why Legend Lite?

| Dimension | Legend Engine (FINOS) | Legend Lite |
|-----------|----------------------|-------------|
| **Codebase** | ~2M LOC across 400+ Maven modules | ~25K LOC in 3 modules |
| **Execution** | Mixed: some SQL, some in-memory Java | 100% SQL push-down — always |
| **Build time** | 15–30 minutes | **19 seconds** (clean build + 955 tests) |
| **Dependencies** | Hundreds of JARs | DuckDB, ANTLR, JUnit — that's it |
| **Java version** | Java 11 | Java 21 (records, sealed interfaces, pattern matching) |
| **Databases** | Postgres, Databricks, Snowflake, etc. | DuckDB (primary), SQLite |

---

## Quick Start

### Prerequisites

- **Java 21+** — required for records, sealed interfaces, pattern matching
- **Maven 3.9+**
- **GEMINI_API_KEY** — required only for NLQ features

### Build & Test

```bash
mvn clean install -DskipTests     # ~8 seconds
mvn clean test -pl engine          # 955 tests in ~19 seconds
```

### Run the Server

```bash
# Engine only (LSP, query execution, SQL, diagrams)
mvn exec:java -pl engine \
  -Dexec.mainClass="com.gs.legend.server.LegendHttpServer"

# With NLQ (adds natural language → Pure endpoint)
GEMINI_API_KEY=your-key \
mvn exec:java -pl nlq \
  -Dexec.mainClass="org.finos.legend.engine.nlq.NlqHttpServer"
```

Both start on **port 8080**. Connect [Studio Lite](https://github.com/neema2/studio-lite) (the React IDE) to `http://localhost:8080`.

---

## Modules

```
legend-lite/
├── engine/     Core: parser, compiler, SQL generator, executor, HTTP server
├── nlq/        Natural Language Query — English → Pure via LLM pipeline
├── pct/        Pure Compatibility Tests against the Legend specification
└── docs/       Design documents (Model-to-Model transforms, etc.)
```

### Engine Architecture

Every Pure query flows through the same pipeline:

```
Pure Source → PureParser → AST → PureModelBuilder → Model
                                                      ↓
                                                  TypeChecker → Typed AST
                                                                   ↓
                                                             PlanGenerator → SqlBuilder
                                                                               ↓
                                                                          SqlDialect → SQL String
                                                                                         ↓
                                                                                    PlanExecutor → Rows
```

The engine is organized into 12 packages:

| Package | Purpose |
|---------|---------|
| `ast` | Value specification AST — all Pure expressions as Java records |
| `antlr` | ANTLR visitors that build the AST from parse trees |
| `parser` | Hand-written recursive-descent parser for Pure model definitions |
| `compiler` | `TypeChecker` — resolves types, validates semantics, annotates the AST |
| `model` | `PureModelBuilder` — constructs the in-memory model (classes, mappings, runtimes) |
| `plan` | `PlanGenerator` — compiles typed AST → SQL via `SqlBuilder` / `SqlExpr` |
| `sql` | `SqlBuilder` — structural SQL AST (SELECT, JOIN, WHERE, etc.) |
| `sqlgen` | Dialect layer — translates generic SQL nodes to database-specific SQL text |
| `exec` | `PlanExecutor` — sends SQL to DuckDB/SQLite, maps results to typed rows |
| `serial` | Result serializers (JSON, CSV) |
| `server` | `LegendHttpServer` — embedded HTTP server |
| `service` | `QueryService` — high-level orchestration |

### SQL Push-Down Strategy

| Pure Operation | SQL Translation |
|---------------|----------------|
| `filter({p\| ...})` | `WHERE` clause |
| `project([...])` | `SELECT` columns |
| `groupBy([...], [...])` | `GROUP BY` + aggregate functions |
| `sort(ascending('col'))` | `ORDER BY col ASC` |
| `limit(n)` / `drop(n)` | `LIMIT` / `OFFSET` |
| `distinct()` | `SELECT DISTINCT` |
| Association in filter | `WHERE EXISTS (...)` — prevents row explosion |
| Association in project | `LEFT OUTER JOIN` — preserves nulls |
| `if(cond, \|then, \|else)` | `CASE WHEN ... THEN ... ELSE ... END` |
| `fold({e,a\| ...}, init)` | `LIST_REDUCE(...)` |
| Graph fetch | Nested `ROW(...)` / `STRUCT(...)` with correlated subqueries |

---

## HTTP API

All endpoints are served on port 8080.

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/lsp` | LSP JSON-RPC — diagnostics, completions, hover |
| `POST` | `/engine/execute` | Compile + execute a Pure query against a database |
| `POST` | `/engine/sql` | Execute raw SQL against a Runtime's connection |
| `POST` | `/engine/diagram` | Extract class diagram data from a Pure model |
| `POST` | `/engine/nlq` | Translate natural language to Pure query (NLQ module) |
| `GET` | `/health` | Health check |

### Execute Endpoint

**Request:**

```json
{
  "code": "Class model::Person { ... }\nDatabase store::DB ( ... )\nMapping model::M ( ... )\nRuntime model::R { ... }\n\nPerson.all()->filter({p|$p.age > 30})->project({p|$p.firstName})"
}
```

The `code` field contains the full Pure source. The query is the last expression (after the Runtime definition). The engine parses the model, compiles the query, generates SQL, executes it, and returns tabular results.

---

## NLQ (Natural Language Query)

The NLQ module translates English questions into executable Pure queries using a 4-step LLM pipeline:

```
Question → Semantic Retrieval → LLM Router → Query Planner → Pure Generator → Parse Validation
              (TF-IDF)          (root class)   (JSON plan)    (Pure syntax)    (PureParser)
```

**Example:**

```
Input:  "show me total notional by desk"
Output: Trade.all()->project([t|$t.trader.desk.name, t|$t.notional], ['desk', 'notional'])
          ->groupBy([{r|$r.desk}], [{r|$r.notional->sum()}], ['desk', 'totalNotional'])
```

### NLQ Annotations

Improve NLQ accuracy by annotating your Pure model:

```pure
Profile nlq {
  tags: [description, synonyms, businessDomain, importance,
         exampleQuestions, displayName, sampleValues, unit];
}

Class {nlq.description = 'Individual trade execution'} model::Trade {
  {nlq.synonyms = 'deal, transaction'} notional: Float[1];
  {nlq.unit = 'USD'} price: Float[1];
}
```

### Configuration

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `GEMINI_API_KEY` | Yes (NLQ only) | — | Google Gemini API key |
| `GEMINI_MODEL` | No | `gemini-3-flash-preview` | Gemini model name |

---

## Testing

```bash
# Engine: full test suite (955 tests, ~19s clean / ~8s incremental)
mvn clean test -pl engine

# Engine: single test class
mvn test -pl engine -Dtest="DuckDBIntegrationTest"

# NLQ eval (requires GEMINI_API_KEY)
GEMINI_API_KEY=your-key mvn test -pl nlq -Dtest="NlqFullPipelineEvalTest"

# PCT compatibility tests
mvn test -pl pct
```

---

## Project Stats

| Metric | Value |
|--------|-------|
| Engine source files | 110 |
| Engine test files | 41 |
| Engine LOC | ~25,000 |
| Total project LOC | ~88,000 |
| Engine tests | 955 |
| Clean build + test | ~19 seconds |
| Incremental test | ~8 seconds |
| Java version | 21 |
| Dependencies | DuckDB 1.5, ANTLR 4.13.1, JUnit 5.11 |

---

## License

Apache 2.0
