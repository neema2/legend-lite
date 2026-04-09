# Legend Lite

A **clean-room reimplementation** of the [FINOS Legend](https://legend.finos.org/) Engine in ~35K lines of modern Java 21. Legend Lite compiles Pure models and queries to SQL and executes them entirely inside the database — **zero rows are ever fetched into the JVM**.

---

## Architecture

The pipeline has two phases: **model loading** (once per source file) and **query execution** (once per query). The model phase parses, resolves names, and builds the in-memory model. The query phase normalizes mappings for the selected runtime, type-checks, resolves physical storage, generates SQL, and executes.

```
═══════════════════════════ MODEL PHASE (once per source) ═══════════════════

┌─────────────────────────────────────────────────────────────────────────────┐
│                           Pure Source Code                                  │
│  import model::*;                                                          │
│  Class model::Person { name: String[1]; }                                  │
│  Database store::DB ( Table T_PERSON (ID INT, NAME VARCHAR) )              │
│  Mapping model::M ( Person: Relational { ... } )                           │
│  Runtime model::RT { mappings: [M]; connections: [DB: [env: Conn]]; }      │
│                                                                             │
│  Person.all()->filter({p|$p.name == 'Alice'})->project(~[name: p|$p.name]) │
└─────────────────┬───────────────────────────────────────────────────────────┘
                  │
                  ▼
┌─────────────────────────────────────────────┐
│  1. PARSER                                  │
│     PureParser (model) + ANTLR (queries)    │
│                                             │
│  ┌─ Input:  Pure source text               │
│  └─ Output: Untyped AST (records)          │
│     • AppliedFunction, AppliedProperty      │
│     • LambdaFunction, Variable, ColSpec     │
│     • ClassInstance, literals               │
│     • Model definitions (classes, mappings) │
│     • ImportScope (import declarations)     │
│                                             │
│  No type checking. No validation.           │
│  Syntactic structure only.                  │
└─────────────────┬───────────────────────────┘
                  │
                  ▼
┌─────────────────────────────────────────────┐
│  2. NAME RESOLVER                           │
│     NameResolver                            │
│                                             │
│  ┌─ Input:  Untyped AST + ImportScope      │
│  └─ Output: AST with FQN references        │
│                                             │
│  Resolves simple names to fully-qualified   │
│  names using import declarations:           │
│     Person → model::Person                  │
│     M      → model::M                       │
│                                             │
│  Walks both model definitions (class names, │
│  superclass refs, property types, mapping   │
│  class refs) and query AST (class refs in   │
│  getAll, graphFetch, cast targets).         │
│  0 matches → keep as-is (primitives).       │
│  1 match → resolve. >1 → ambiguity error.   │
└─────────────────┬───────────────────────────┘
                  │
                  ▼
┌─────────────────────────────────────────────┐
│  3. MODEL BUILDER                           │
│     PureModelBuilder                        │
│                                             │
│  ┌─ Input:  Resolved AST                   │
│  └─ Output: In-memory model                │
│     • ClassDefinition (props, associations) │
│     • MappingDefinition (relational, M2M)   │
│     • DatabaseDefinition (tables, joins)    │
│     • RuntimeDefinition (connections)       │
│     • SymbolTable (intern all names → IDs)  │
│                                             │
│  Immutable model. Reused across queries.    │
└─────────────────┬───────────────────────────┘
                  │
═══════════════════════════ QUERY PHASE (once per query) ════════════════════
                  │
                  ▼
┌─────────────────────────────────────────────┐
│  4. MAPPING NORMALIZER                      │
│     MappingNormalizer → NormalizedMapping    │
│                                             │
│  ┌─ Input:  Model + Runtime's mappingNames │
│  └─ Output: NormalizedMapping (immutable)   │
│     • Resolved M2M chains (A→B→C→table)    │
│     • MappingExpression per class           │
│     • Association extends (traverse nodes)  │
│     • Pre-converted join conditions (VS)    │
│                                             │
│  Desugars M2M mappings into synthetic       │
│  chains: getAll(src) → filter → extend(~[]) │
│  Per-query because different Runtimes       │
│  select different mapping sets.             │
└─────────────────┬───────────────────────────┘
                  │
                  ▼
┌─────────────────────────────────────────────┐
│  5. COMPILER                                │
│     TypeChecker + 36 Checkers               │
│                                             │
│  ┌─ Input:  Untyped query AST + Model      │
│  │          + MappingExpressions            │
│  └─ Output: TypeInfo sidecar per AST node  │
│     • ExpressionType (type + multiplicity)  │
│     • SortSpec, FrameSpec, etc.             │
│                                             │
│  Single source of truth for types.          │
│  Resolves all associations, validates all   │
│  function signatures, stamps every node.    │
│  If TypeInfo is missing → compiler bug.     │
└─────────────────┬───────────────────────────┘
                  │
                  ▼
┌─────────────────────────────────────────────┐
│  6. MAPPING RESOLVER                        │
│     MappingResolver                         │
│                                             │
│  ┌─ Input:  Typed AST + NormalizedMapping  │
│  └─ Output: StoreResolution per getAll()   │
│     • Table name, column mappings           │
│     • JoinResolution (target table, params, │
│       join condition, sourceColumns,        │
│       isToMany, nested targetResolution)    │
│     • DynaFunction property resolutions     │
│                                             │
│  Resolves logical properties to physical    │
│  storage: which table, which columns,       │
│  which joins. Stamps TypeInfo on join       │
│  condition nodes. Recurses into nested      │
│  associations (targetResolution).           │
└─────────────────┬───────────────────────────┘
                  │
                  ▼
┌─────────────────────────────────────────────┐
│  7. PLAN GENERATOR                          │
│     PlanGenerator → SqlExpr tree            │
│                                             │
│  ┌─ Input:  AST (structure) + TypeInfo      │
│  │          + StoreResolution               │
│  └─ Output: SqlExpr nodes (semantic SQL)    │
│     • SqlExpr.Column, FunctionCall, Cast    │
│     • SqlExpr.JsonObject, JsonArrayAgg      │
│     • SqlBuilder (SELECT/JOIN/WHERE/etc.)   │
│                                             │
│  Reads AST for structure (ColSpec names,    │
│  lambda bodies, function names).            │
│  Reads TypeInfo for types and resolutions.  │
│  Does NO type inference. Emits SqlExpr —    │
│  never raw SQL strings.                     │
└─────────────────┬───────────────────────────┘
                  │
                  ▼
┌─────────────────────────────────────────────┐
│  8. DIALECT                                 │
│     DuckDBDialect / SQLiteDialect           │
│                                             │
│  ┌─ Input:  SqlExpr tree                   │
│  └─ Output: SQL string                     │
│     • Type mapping: Integer → BIGINT        │
│     • Function mapping: plus → +            │
│     • Dialect decompositions (WEEK diff,    │
│       timeBucket, lpad safe, etc.)          │
│                                             │
│  All SQL-specific decisions live here.      │
│  PlanGenerator is dialect-agnostic.         │
└─────────────────┬───────────────────────────┘
                  │
                  ▼
┌─────────────────────────────────────────────┐
│  9. EXECUTOR                                │
│     PlanExecutor → DuckDB/SQLite            │
│                                             │
│  ┌─ Input:  SQL string + Connection        │
│  └─ Output: ExecutionResult                │
│     • TabularResult: columns + typed rows   │
│     • GraphResult: JSON string              │
│                                             │
│  Sends SQL via JDBC. Maps ResultSet to      │
│  typed Java objects. Streams results for    │
│  JSON/CSV serialization.                    │
└─────────────────────────────────────────────┘
```

### Architectural Invariants

1. **Compiler does ALL typing** — single source of truth. If TypeInfo is missing, fix the Compiler.
2. **PlanGenerator does NO type inference** — reads AST for structure, TypeInfo for types.
3. **Dialect owns ALL SQL rendering** — type names, function names, decompositions.
4. **No fallbacks, no defaulting** — fail loudly. Every default branch is a hidden bug.

---

## What is Legend?

[Legend](https://legend.finos.org/) is an open-source data management platform created by Goldman Sachs and donated to FINOS. Define your data model once in Pure, map it to databases, query it with type-safe semantics.

Legend Lite reimplements this in a fraction of the code, with 100% SQL push-down.

| Dimension | Legend Engine (FINOS) | Legend Lite |
|-----------|----------------------|-------------|
| **Codebase** | ~2M LOC, 400+ Maven modules | ~35K LOC, 3 modules |
| **Execution** | Mixed: some SQL, some in-memory | 100% SQL push-down — always |
| **Build time** | 15–30 minutes | **27 seconds** (clean build + 2208 tests) |
| **Dependencies** | Hundreds of JARs | DuckDB, ANTLR, JUnit |
| **Java version** | Java 11 | Java 21 (records, sealed interfaces, pattern matching) |
| **Databases** | Postgres, Databricks, Snowflake, etc. | DuckDB (primary), SQLite |

---

## Feature Set

### Pure Language Support

| Feature | Status | Details |
|---------|--------|---------|
| **Classes** | ✅ | Properties with multiplicities (`[1]`, `[0..1]`, `[*]`), inheritance |
| **Associations** | ✅ | Bidirectional, to-one and to-many, navigable in queries |
| **Enumerations** | ✅ | Enum types with values |
| **Databases** | ✅ | Tables, columns, primary keys, joins |
| **Relational mappings** | ✅ | `~mainTable`, column mappings, `~filter`, `~distinct` |
| **M2M mappings** | ✅ | `~src`, expressions, conditionals, `~filter` |
| **Chained M2M** | ✅ | Multi-hop: A→B→C resolves to single SQL |
| **Association mappings** | ✅ | `@JoinName` traversal in M2M and relational |
| **Runtimes & connections** | ✅ | DuckDB InMemory, file-based, SQLite |
| **Imports** | ✅ | `import package::*;` |

### Query Operations (Relation API)

| Operation | SQL Translation | Example |
|-----------|----------------|---------|
| `filter({p\| ...})` | `WHERE` | `->filter({p\|$p.age > 30})` |
| `project(~[...])` | `SELECT` | `->project(~[name: p\|$p.name])` |
| `extend(~[...])` | `SELECT ... , expr AS col` | `->extend(~[full: p\|$p.first + ' ' + $p.last])` |
| `groupBy(~[keys], ~[aggs])` | `GROUP BY` + aggregates | `->groupBy(~[dept], ~[avg: x\|$x.salary : y\|$y->average()])` |
| `sort(ascending(~col))` | `ORDER BY` | `->sort(ascending(~name))` |
| `limit(n)` / `drop(n)` | `LIMIT` / `OFFSET` | `->limit(10)->drop(5)` |
| `slice(start, end)` | `LIMIT + OFFSET` | `->slice(0, 100)` |
| `distinct()` | `SELECT DISTINCT` | `->distinct()` |
| `select(~[cols])` | Column selection | `->select(~[name, age])` |
| `rename(~old, ~new)` | `AS` alias | `->rename(~firstName, ~first)` |
| `concatenate(rel)` | `UNION ALL` | `->concatenate(other)` |
| `join(rel, JoinKind)` | `JOIN` | `->join(other, JoinKind.INNER, ...)` |
| `size()` | `COUNT(*)` | `->size()` |
| `if(cond, \|then, \|else)` | `CASE WHEN` | `if($p.age < 18, \|'Minor', \|'Adult')` |
| `fold({e,a\| ...}, init)` | `LIST_REDUCE` | `->fold({e,a\|$a + $e}, 0)` |
| `match([...])` | `CASE WHEN` chain | Type-matching dispatch |

### Aggregate Functions

`sum`, `average`, `mean`, `count`, `min`, `max`, `stdDevPopulation`, `stdDevSample`, `variancePopulation`, `varianceSample`, `percentile`, `joinStrings`

### Scalar Functions

| Category | Functions |
|----------|-----------|
| **String** | `toLower`, `toUpper`, `trim`, `ltrim`, `rtrim`, `length`, `substring`, `indexOf`, `contains`, `startsWith`, `endsWith`, `replace`, `lpad`, `rpad`, `split`, `format`, `matches` (regex) |
| **Math** | `plus`, `minus`, `times`, `divide`, `abs`, `ceiling`, `floor`, `round`, `sqrt`, `pow`, `mod`, `rem`, `exp`, `log`, `log10`, `cbrt` |
| **Date/Time** | `today`, `now`, `adjust`, `dateDiff`, `datePart`, `year`, `month`, `dayOfMonth`, `dayOfWeek`, `dayOfYear`, `hour`, `minute`, `second`, `epochSecond`, `firstDayOfWeek`, `firstDayOfMonth`, `firstDayOfQuarter`, `firstDayOfYear`, `timeBucket` |
| **Boolean** | `and`, `or`, `not`, `isEmpty`, `isNotEmpty` |
| **Comparison** | `equal`, `lessThan`, `greaterThan`, `lessThanEqual`, `greaterThanEqual`, `in`, `isNull`, `isNotNull` |
| **Type** | `cast`, `toOne`, `toOneMany`, `toString`, `toInteger`, `toFloat`, `toDecimal`, `toBoolean`, `toDate`, `toDateTime` |
| **Collection** | `first`, `last`, `at`, `size`, `contains`, `range`, `zip`, `flatten` |
| **Variant** | `get`, `to(@Type)`, `toMany(@Type)`, `toVariant` |

### Window Functions

| Feature | Example |
|---------|---------|
| **Partition + sort** | `over(~dept, ascending(~salary))` |
| **Row number** | `extend(over(...), ~rn: {p,w,r\|$r->rowNumber($w)})` |
| **Rank / Dense rank** | `$r->rank($w)`, `$r->denseRank($w)` |
| **Ntile** | `$r->ntile($w, 4)` |
| **Lead / Lag** | `$r->lead($w, ~col, 1)`, `$r->lag($w, ~col, 1)` |
| **Running aggregates** | `$r->runningSum($w, ~col)`, `$r->runningAverage($w, ~col)`, `$r->runningCount($w)`, `$r->runningMin/Max($w, ~col)` |
| **Frame specs** | `rows(preceding(2), following(1))`, `range(preceding(unbounded()), current())` |

### Graph Fetch (Nested JSON)

Compiles the entire object graph to a **single SQL statement** using correlated subqueries:

```pure
Person.all()
  ->graphFetch(#{ Person {
      name,
      firm { legalName },
      dept { name, org { name } },
      addresses { city, country { name }, tags { label } }
  } }#)
  ->serialize(...)
```

| Feature | Status |
|---------|--------|
| Scalar properties | ✅ |
| To-one nesting (correlated subquery → `json_object`) | ✅ |
| To-many nesting (correlated subquery → `json_group_array`) | ✅ |
| **N-level recursive nesting** | ✅ |
| Multiple disjoint branches (firm + dept + addresses) | ✅ |
| Nested to-many inside to-many (addresses[*] → tags[*]) | ✅ |
| NULL FK handling at any depth | ✅ |
| `serialize()` pass-through | ✅ |
| Composition with `filter`, `sort` | ✅ |

### Association Navigation

| Pattern | SQL Strategy |
|---------|-------------|
| To-one in `project` | `LEFT OUTER JOIN` |
| To-many in `project` | `LEFT OUTER JOIN` (row expansion) |
| To-one in `filter` | `WHERE EXISTS (...)` — no row explosion |
| To-many in `filter` | `WHERE EXISTS (...)` — no row explosion |
| Multi-hop (Person→Dept→Org) | Chained JOINs |
| Self-join (Employee→Manager) | Aliased self-JOIN |
| Graph fetch nesting | Correlated subqueries |

### Model-to-Model (M2M)

| Feature | Status |
|---------|--------|
| Simple property transform (`$src.prop`) | ✅ |
| Expression transforms (`$src.a + ' ' + $src.b`) | ✅ |
| Conditional (`if/else → CASE WHEN`) | ✅ |
| Mapping filter (`~filter`) | ✅ |
| Chained M2M (A→B→C→relational) | ✅ |
| Association navigation via `@JoinName` | ✅ |
| Deep fetch (nested objects through M2M) | ✅ |
| Graph fetch through M2M | ✅ |

---

## Package Structure

```
engine/src/main/java/com/gs/legend/
├── ast/          25 files   Value specification AST (sealed interface + records)
├── antlr/         2 files   ANTLR visitors → AST
├── parser/        3 files   Recursive-descent model parser + name resolver
├── model/        45 files   PureModelBuilder, MappingNormalizer, definitions
├── compiler/     50 files   TypeChecker + 36 checkers (one per function group)
├── plan/          5 files   PlanGenerator → SqlExpr tree
├── sql/          10 files   SqlBuilder + SqlExpr (structural SQL AST)
├── sqlgen/        5 files   DuckDBDialect, SQLiteDialect, SQL rendering
├── exec/          6 files   PlanExecutor, JDBC execution, result mapping
├── serial/        5 files   JSON + CSV result serializers
├── server/        6 files   HTTP server, LSP, endpoints
└── service/       3 files   QueryService orchestration
                 165 files   ~35K lines
```

---

## Quick Start

### Prerequisites

- **Java 21+** — records, sealed interfaces, pattern matching
- **Maven 3.9+**
- **GEMINI_API_KEY** — required only for NLQ features

### Build & Test

```bash
mvn clean install -DskipTests        # ~8 seconds
mvn clean test -pl engine             # 2208 tests in ~27 seconds
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

## HTTP API

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/lsp` | LSP JSON-RPC — diagnostics, completions, hover |
| `POST` | `/engine/execute` | Compile + execute Pure query → tabular or graph result |
| `POST` | `/engine/sql` | Raw SQL against a Runtime's connection |
| `POST` | `/engine/diagram` | Extract class diagram from Pure model |
| `POST` | `/engine/nlq` | Natural language → Pure query (NLQ module) |
| `GET` | `/health` | Health check |

### Execute Endpoint

```json
{
  "code": "Class Person { name: String[1]; } ... Person.all()->project(~[name: p|$p.name])"
}
```

The `code` field contains the full Pure source — model definitions + query as the last expression. Returns tabular results (columns + rows) or graph results (JSON).

---

## The Pure Language

Pure is Legend's modeling and query language. Define **what your data looks like** (classes), **where it lives** (databases + mappings), and **how to access it** (runtimes). Query with type-safe expressions that compile to SQL.

### Classes & Associations

```pure
Class model::Person {
    firstName: String[1];
    lastName:  String[1];
    age:       Integer[1];
}

Class model::Address {
    street: String[1];
    city:   String[1];
}

Association model::Person_Address {
    person:    Person[1];
    addresses: Address[*];
}
```

Multiplicities: `[1]` required, `[0..1]` optional, `[*]` collection.
Types: `String`, `Integer`, `Float`, `Decimal`, `Boolean`, `Date`, `DateTime`, `StrictDate`.

### Databases & Mappings

```pure
Database store::DB (
    Table T_PERSON (ID INTEGER PRIMARY KEY, FIRST_NAME VARCHAR(100), LAST_NAME VARCHAR(100), AGE INTEGER)
    Table T_ADDRESS (ID INTEGER PRIMARY KEY, PERSON_ID INTEGER, STREET VARCHAR(200), CITY VARCHAR(100))
    Join Person_Address(T_PERSON.ID = T_ADDRESS.PERSON_ID)
)

Mapping model::M (
    Person: Relational {
        ~mainTable [store::DB] T_PERSON
        firstName: [store::DB] T_PERSON.FIRST_NAME,
        lastName:  [store::DB] T_PERSON.LAST_NAME,
        age:       [store::DB] T_PERSON.AGE
    }
    Address: Relational {
        ~mainTable [store::DB] T_ADDRESS
        street: [store::DB] T_ADDRESS.STREET,
        city:   [store::DB] T_ADDRESS.CITY
    }
    model::Person_Address: AssociationMapping (
        person:    [store::DB]@Person_Address,
        addresses: [store::DB]@Person_Address
    )
)
```

### Model-to-Model Mappings

```pure
Class model::RawPerson { firstName: String[1]; lastName: String[1]; age: Integer[1]; isActive: Boolean[1]; }
Class model::Person    { fullName: String[1]; ageGroup: String[1]; }

Mapping model::M2M (
    Person: Pure {
        ~src RawPerson
        ~filter $src.isActive == true
        fullName: $src.firstName + ' ' + $src.lastName,
        ageGroup: if($src.age < 18, |'Minor', |if($src.age < 65, |'Adult', |'Senior'))
    }
)
```

Chained M2M: `PersonSummary →(Pure) Person →(Pure) RawPerson →(Relational) T_PERSON` — resolves to a single SQL statement.

### Runtimes & Connections

```pure
RelationalDatabaseConnection store::Conn { type: DuckDB; specification: InMemory { }; auth: NoAuth { }; }
Runtime test::RT { mappings: [model::M]; connections: [store::DB: [environment: store::Conn]]; }
```

### Queries

```pure
// Tabular
Person.all()->filter({p|$p.age > 30})->project(~[name: p|$p.firstName, age: p|$p.age])

// Association navigation
Person.all()->filter({p|$p.addresses.city == 'NYC'})->project(~[name: p|$p.firstName])

// GroupBy
Person.all()->project(~[dept: p|$p.dept.name, sal: p|$p.salary])
  ->groupBy(~[dept], ~[avg: x|$x.sal : y|$y->average()])

// Graph fetch — N-level nested JSON
Person.all()
  ->graphFetch(#{ Person { name, firm { legalName }, addresses { city, country { name } } } }#)
  ->serialize(#{ Person { name, firm { legalName }, addresses { city, country { name } } } }#)
```

Every query compiles to a **single SQL statement**. Associations → JOINs, to-many filters → EXISTS, graph fetch → correlated subqueries, M2M → inline expressions.

---

## NLQ (Natural Language Query)

Translates English questions to executable Pure queries via a 4-step LLM pipeline:

```
Question → Semantic Retrieval → LLM Router → Query Planner → Pure Generator → Parse Validation
              (TF-IDF)          (root class)   (JSON plan)    (Pure syntax)    (PureParser)
```

```
Input:  "show me total notional by desk"
Output: Trade.all()->project([t|$t.trader.desk.name, t|$t.notional], ['desk', 'notional'])
          ->groupBy([{r|$r.desk}], [{r|$r.notional->sum()}], ['desk', 'totalNotional'])
```

Annotate models with `Profile nlq { tags: [description, synonyms, ...]; }` for better accuracy.

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `GEMINI_API_KEY` | Yes (NLQ only) | — | Google Gemini API key |
| `GEMINI_MODEL` | No | `gemini-3-flash-preview` | Model name |

---

## Testing

```bash
mvn clean test -pl engine                              # 2208 tests, ~27s
mvn test -pl engine -Dtest="AssociationIntegrationTest" # Single class
GEMINI_API_KEY=key mvn test -pl nlq -Dtest="NlqFullPipelineEvalTest"
mvn test -pl pct                                        # PCT compatibility
```

---

## Project Stats

| Metric | Value |
|--------|-------|
| Engine source files | 165 |
| Engine test files | 76 |
| Engine source LOC | ~35,000 |
| Engine test LOC | ~52,000 |
| Engine tests | 2,208 |
| Clean build + test | ~27 seconds |
| Java version | 21 |
| Dependencies | DuckDB 1.5, ANTLR 4.13.1, JUnit 5.11 |

---

## License

Apache 2.0
