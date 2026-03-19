# Legend Lite

A **clean-room reimplementation** of the [FINOS Legend](https://legend.finos.org/) Engine in ~25K lines of modern Java 21. Every Pure query compiles to a single SQL statement and executes entirely inside the database — **zero rows are ever fetched into the JVM**.

> **Legend** is an open-source data management platform created by Goldman Sachs and donated to FINOS. It provides a formal modeling language (Pure) for describing data models, mappings, and queries. Legend Lite reimplements the engine that compiles and executes Pure against relational databases.

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

Legend Lite is **not a fork** — it's a clean-room implementation that reads the same Pure language but compiles it through a completely different pipeline. The goal is to demonstrate that a modern, minimalist engine can achieve the same (and in many cases better) query semantics with orders-of-magnitude less complexity.

---

## Modules

```
legend-lite/
├── engine/     Core: parser, compiler, SQL generator, executor, HTTP server
├── nlq/        Natural Language Query — English → Pure via LLM pipeline
├── pct/        Pure Compatibility Tests against the Legend specification
├── docs/       Design documents (Model-to-Model, etc.)
└── pom.xml     Parent POM (Java 21, DuckDB 1.5, ANTLR 4.13)
```

### Engine (`engine/`)

The engine is the heart of Legend Lite — 110 source files organized into 12 packages:

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
| `server` | `LegendHttpServer` — embedded HTTP server with LSP, execute, diagram endpoints |
| `service` | `QueryService` — high-level orchestration of compile → plan → execute |

### NLQ (`nlq/`)

The NLQ module translates English questions into executable Pure queries using a 4-step LLM pipeline:

```
Question → Semantic Retrieval → LLM Router → Query Planner → Pure Generator → Parse Validation
              (TF-IDF)          (root class)   (JSON plan)    (Pure syntax)    (PureParser)
```

### PCT (`pct/`)

Pure Compatibility Tests — runs the Legend specification's PCT test suite against Legend Lite to measure function coverage and compatibility with the upstream engine.

---

## Quick Start

### Prerequisites

- **Java 21+** — required for records, sealed interfaces, pattern matching
- **Maven 3.9+**
- **GEMINI_API_KEY** — required only for NLQ features

### Build

```bash
mvn clean install -DskipTests     # ~8 seconds
mvn clean test -pl engine          # 955 tests in ~19 seconds
```

### Run the Server

**Engine only** (LSP, query execution, SQL, diagrams):

```bash
mvn exec:java -pl engine \
  -Dexec.mainClass="com.gs.legend.server.LegendHttpServer"
```

**With NLQ** (adds the `POST /engine/nlq` endpoint):

```bash
GEMINI_API_KEY=your-key-here \
mvn exec:java -pl nlq \
  -Dexec.mainClass="org.finos.legend.engine.nlq.NlqHttpServer"
```

Both start on **port 8080**. Override with a port argument:

```bash
mvn exec:java -pl engine \
  -Dexec.mainClass="com.gs.legend.server.LegendHttpServer" \
  -Dexec.args="9090"
```

### Connect the Frontend

See [Studio Lite](https://github.com/neema2/studio-lite) — the React-based IDE that connects to `http://localhost:8080`.

---

## Architecture

### Compilation Pipeline

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

**Key design decisions:**

1. **Immutable AST** — All AST nodes are Java records (`sealed interface ValueSpecification permits ...`). Pattern matching in `switch` expressions provides exhaustiveness guarantees.

2. **Single-pass SQL compilation** — The `PlanGenerator` walks the typed AST exactly once and emits a structural `SqlBuilder` (not raw SQL strings). The `SqlDialect` layer renders the builder to database-specific SQL.

3. **Side-table metadata** — The `TypeChecker` annotates each AST node with a `TypeInfo` record (scalar type, multiplicity, relation columns, inlined expressions). The `PlanGenerator` reads this metadata without re-walking the model.

4. **Dialect abstraction** — Database-specific SQL (DuckDB structs, `LIST_TRANSFORM`, `DATE_TRUNC`) is emitted as semantic function names (`listTransform`, `dateTruncDay`) and translated to real SQL by the dialect layer.

### SQL Push-Down Strategy

Legend Lite never fetches rows into the JVM for processing. Every Pure operation maps to a SQL construct:

| Pure Operation | SQL Translation |
|---------------|----------------|
| `filter({p\| ...})` | `WHERE` clause |
| `project([...])` | `SELECT` columns |
| `groupBy([...], [...])` | `GROUP BY` + aggregate functions |
| `sort(ascending('col'))` | `ORDER BY col ASC` |
| `limit(n)` | `LIMIT n` |
| `distinct()` | `SELECT DISTINCT` |
| `->contains()` (to-many) | `WHERE EXISTS (...)` |
| Property access through association | `LEFT OUTER JOIN` |
| `if(cond, \|then, \|else)` | `CASE WHEN ... THEN ... ELSE ... END` |
| `fold({e,a\| ...}, init)` | `LIST_REDUCE(...)` or desugared `CONCATENATE` |
| Struct construction (`^Class(...)`) | `ROW(...)` / `STRUCT(...)` |

---

## HTTP API

All endpoints are served on port 8080.

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/lsp` | LSP JSON-RPC — diagnostics, completions, hover |
| `POST` | `/engine/execute` | Compile + execute a Pure query against DuckDB |
| `POST` | `/engine/sql` | Execute raw SQL against a Runtime's connection |
| `POST` | `/engine/diagram` | Extract class diagram data from a Pure model |
| `POST` | `/engine/nlq` | Translate natural language to Pure query (NLQ module) |
| `GET` | `/health` | Health check |

### Execute Endpoint

**Request:**

```json
{
  "code": "Class model::Person { firstName: String[1]; age: Integer[1]; }\nDatabase store::DB ( Table T_PERSON (ID INT, FIRST_NAME VARCHAR(100), AGE INT) )\nMapping model::PersonMapping ( Person: Relational { ~mainTable [DB] T_PERSON  firstName: [DB] T_PERSON.FIRST_NAME, age: [DB] T_PERSON.AGE } )\nRuntime model::PersonRuntime { mappings: [model::PersonMapping]; connections: [store::DB: DuckDB]; }\n\nPerson.all()->filter({p|$p.age > 30})->project({p|$p.firstName})"
}
```

The `code` field contains the full Pure source. The query expression is the last statement (after the Runtime definition).

### NLQ Endpoint

**Request:**

```json
{
  "code": "Class model::Trade { trader: Trader[1]; notional: Float[1]; ... }",
  "question": "show me total notional by desk",
  "domain": "Trading"
}
```

**Response:**

```json
{
  "success": true,
  "rootClass": "Trade",
  "pureQuery": "Trade.all()->project([t|$t.trader.desk.name, t|$t.notional], ['desk', 'notional'])->groupBy([{r|$r.desk}], [{r|$r.notional->sum()}], ['desk', 'totalNotional'])",
  "queryPlan": "{ ... }",
  "retrievedClasses": ["Trade", "Trader", "Desk"],
  "latencyMs": 4200
}
```

---

## Pure Language Quick Reference

```pure
// Filter + project
Person.all()
  ->filter({p | $p.age > 30 && $p.lastName != 'Smith'})
  ->project([p|$p.firstName, p|$p.lastName, p|$p.age], ['first', 'last', 'age'])

// GroupBy with aggregation
Trade.all()
  ->project([t|$t.trader.desk.name, t|$t.notional], ['desk', 'notional'])
  ->groupBy([{r|$r.desk}], [{r|$r.notional->sum()}], ['desk', 'totalNotional'])

// Sort + limit
Trade.all()
  ->project([t|$t.notional, t|$t.tradeDate], ['notional', 'date'])
  ->sort(descending('notional'))
  ->limit(10)

// Graph fetch (nested/structured results)
Person.all()->graphFetch(#{Person{firstName, lastName, addresses{city, street}}}#)

// Conditional logic
Person.all()->project({p | if($p.age < 18, |'Minor', |'Adult')}, 'category')

// String operations
Person.all()->project({p | $p.firstName->toUpper() + ' ' + $p.lastName->toLower()}, 'name')
```

---

## Testing

```bash
# Engine: full test suite (955 tests, ~19s clean / ~8s incremental)
mvn clean test -pl engine

# Engine: specific test class
mvn test -pl engine -Dtest="DuckDBIntegrationTest"

# NLQ eval: sales-trading model (requires GEMINI_API_KEY)
GEMINI_API_KEY=your-key mvn test -pl nlq -Dtest="NlqFullPipelineEvalTest"

# NLQ eval: CDM model (741 classes)
GEMINI_API_KEY=your-key mvn test -pl nlq -Dtest="NlqCdmEvalTest"

# PCT compatibility tests
mvn test -pl pct
```

---

## NLQ Configuration

### Environment Variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `GEMINI_API_KEY` | Yes (NLQ only) | — | Google Gemini API key |
| `LLM_PROVIDER` | No | `gemini` | LLM provider |
| `GEMINI_MODEL` | No | `gemini-3-flash-preview` | Gemini model name |

### NLQ Annotations

Pure models can include metadata via tagged values to improve NLQ retrieval and accuracy:

```pure
Profile nlq {
  tags: [description, synonyms, businessDomain, importance, exampleQuestions,
         displayName, sampleValues, unit];
}

Class {nlq.description = 'Daily profit and loss by trader'} model::DailyPnL {
  {nlq.synonyms = 'PnL, P&L, daily P&L'} totalPnL: Float[1];
}
```

---

## Project Stats

| Metric | Value |
|--------|-------|
| Engine source files | 110 |
| Engine test files | 41 |
| Engine LOC (source) | ~25,000 |
| Total project LOC | ~88,000 |
| Engine tests | 955 |
| Clean build + test | ~19 seconds |
| Incremental test | ~8 seconds |
| Java version | 21 |
| Dependencies | DuckDB 1.5, ANTLR 4.13.1, JUnit 5.11 |

---

## License

Apache 2.0
