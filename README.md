# Legend Lite

A clean-room implementation of the FINOS Legend Engine, designed for **100% SQL push-down** execution. Includes an HTTP server for [Studio Lite](https://github.com/neema2/studio-lite) (the frontend) and an **NLQ (Natural Language Query)** pipeline that translates English questions into Pure queries using LLMs.

## Modules

| Module | Description |
|--------|-------------|
| `engine/` | Pure parser, compiler, SQL generator, DuckDB/SQLite execution, HTTP server |
| `nlq/` | NLQ-to-Pure pipeline — semantic retrieval, LLM routing, query planning, Pure generation |
| `pct/` | Pure Compatibility Tests (PCT) against the Legend specification |

---

## Quick Start

### Prerequisites

- **Java 21+**
- **Maven 3.9+**
- **GEMINI_API_KEY** environment variable (required for NLQ features)

### 1. Build

```bash
mvn clean install -DskipTests
```

### 2. Start the Backend Server

**Without NLQ** (engine only — LSP, query execution, SQL, diagrams):

```bash
mvn exec:java -pl engine \
  -Dexec.mainClass="org.finos.legend.engine.server.LegendHttpServer"
```

**With NLQ** (adds the `POST /engine/nlq` endpoint):

```bash
GEMINI_API_KEY=your-key-here \
mvn exec:java -pl nlq \
  -Dexec.mainClass="org.finos.legend.engine.nlq.NlqHttpServer"
```

Both start on **port 8080** by default. Pass a port number as the first argument to override:

```bash
mvn exec:java -pl nlq \
  -Dexec.mainClass="org.finos.legend.engine.nlq.NlqHttpServer" \
  -Dexec.args="9090"
```

### 3. Start the Frontend

See [studio-lite](https://github.com/neema2/studio-lite) — it connects to `http://localhost:8080` by default.

---

## HTTP API

All endpoints are served from the same server on port 8080.

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/lsp` | LSP JSON-RPC — diagnostics, completions, hover |
| `POST` | `/engine/execute` | Compile + execute a Pure query against DuckDB |
| `POST` | `/engine/sql` | Execute raw SQL against a Runtime's connection |
| `POST` | `/engine/diagram` | Extract class diagram data from Pure model |
| `POST` | `/engine/nlq` | **NLQ** — translate natural language to Pure query |
| `GET`  | `/health` | Health check |

### NLQ Endpoint

**Request:**

```json
{
  "code": "Class model::Trade { ... } ...",
  "question": "show me total notional by desk",
  "domain": "Trading"
}
```

- `code` — full Pure model source (classes, mappings, connections, runtime)
- `question` — natural language question
- `domain` — optional hint to narrow semantic search

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

### Execute Endpoint

**Request:**

```json
{
  "code": "Class model::Person { ... }\nDatabase store::DB ( ... )\nMapping ...\nRuntime ...\n\nPerson.all()->filter({p|$p.age > 30})->project({p|$p.firstName})"
}
```

The `code` field contains the full Pure source with the query expression appended at the end (after the Runtime definition).

---

## NLQ Pipeline

The NLQ module implements a 4-step pipeline:

```
Question ──► Semantic Retrieval ──► LLM Router ──► Query Planner ──► Pure Generator ──► Parse Validation
                 (TF-IDF)          (root class)    (structured plan)   (Pure syntax)     (PureParser)
```

1. **Semantic Retrieval** — TF-IDF index over class names, descriptions, and property metadata. Returns top-K candidate classes.
2. **Semantic Router** — LLM identifies the root class from candidates (with retry on unparsable responses).
3. **Query Planner** — LLM builds a structured JSON plan (projections, filters, aggregations, sorts).
4. **Pure Generator** — LLM generates Pure syntax from the plan (with retry on parse failures).
5. **Parse Validation** — `PureParser.parse()` hard gate rejects syntactically invalid queries.

### Environment Variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `GEMINI_API_KEY` | Yes (for NLQ) | — | Google Gemini API key |
| `LLM_PROVIDER` | No | `gemini` | LLM provider (`gemini`) |
| `GEMINI_MODEL` | No | `gemini-3-flash-preview` | Gemini model name |

### NLQ Annotations

Pure models can include NLQ metadata via tagged values to improve retrieval and routing:

```
Profile nlq {
  tags: [description, synonyms, businessDomain, importance, exampleQuestions, displayName, sampleValues, unit];
}

Class {nlq.description = 'Daily profit and loss by trader'} model::DailyPnL {
  {nlq.synonyms = 'PnL, P&L, daily P&L'} totalPnL: Float[1];
}
```

---

## Testing

```bash
# All engine tests (~900 tests, ~10s)
mvn test -pl engine

# NLQ eval — sales-trading model (requires GEMINI_API_KEY)
GEMINI_API_KEY=your-key mvn test -pl nlq -Dtest="NlqFullPipelineEvalTest"

# NLQ eval — CDM model (741 classes, requires GEMINI_API_KEY)
GEMINI_API_KEY=your-key mvn test -pl nlq -Dtest="NlqCdmEvalTest"

# PCT compatibility tests
mvn test -pl pct
```

---

## Pure Language Quick Reference

```
// Class filter + project
Trade.all()
  ->filter({t|$t.status == 'CONFIRMED'})
  ->project([t|$t.trader.name, t|$t.notional], ['trader', 'notional'])

// GroupBy (always after project)
Trade.all()
  ->project([t|$t.trader.desk.name, t|$t.notional], ['desk', 'notional'])
  ->groupBy([{r|$r.desk}], [{r|$r.notional->sum()}], ['desk', 'totalNotional'])

// Sort + limit
Trade.all()
  ->project([t|$t.notional, t|$t.tradeDate], ['notional', 'tradeDate'])
  ->sort(descending('notional'))
  ->limit(10)
```

All queries are translated to native SQL and executed entirely in the database — no data is fetched into the JVM.

---

## License

Apache 2.0
