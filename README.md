# Legend Lite

A **clean-room reimplementation** of the [FINOS Legend](https://legend.finos.org/) Engine in modern Java 21 — ~120K lines of compiler. Legend Lite compiles Pure models and queries to SQL and executes them entirely inside the database — **zero rows are ever fetched into the JVM**.

**Start here:** [`AGENTS.md`](AGENTS.md) for the invariants, [`core/README.md`](core/README.md) for the per-package spec, [`docs/GATES.md`](docs/GATES.md) for what must be green.

---

## Architecture

Legend Lite compiles Pure models and queries to SQL and executes them entirely
inside the database — **zero rows are ever fetched into the JVM**.

The compiler is `core/src/main/java/com/legend/`. One driver — `com.legend.Compiler` —
owns step ordering across eleven phases. Every phase is the same method its own
unit tests exercise; there is no orchestrator-only code path.

```
text                                                             [FRONTEND]
  A  lexer/            Lexer.tokenize                 text → TokenStream
  B  parser/           ElementParser.parse            tokens → ParsedModel
  C  parser/           SpecParser.parse               tokens → ValueSpecification
  D  compiler/         NameResolver.resolve           simple name → FQN
  E  normalizer/       ModelNormalizer.normalize      ParsedModel → NormalizedModel
  F  compiler/element/ PureModelContext.from          → TypedElement + ModelContext
  G  compiler/spec/    SpecCompiler                   spec + model → TypedSpec
  G½ compiler/spec/    UserCallInliner.inlineBody     β-inline user calls
                                                                 [MIDEND]
  H  resolver/         StoreResolver.resolve          logical → physical TypedSpec
  I  lowering/         Lowerer.lower                  TypedSpec → sql.SqlQuery
                                                                 [BACKEND]
  J  sql/dialect/      SqlDialect.render              SqlQuery → SQL string
                                                                 [RUNTIME]
  K  exec/             Executor.execute               SQL + JDBC → ExecutionResult
```

**Phases A–F are model loading** — once per source. **G–K are query
execution** — once per query. `ModelContext` is the boundary: built once,
reused across queries.

Two things worth knowing before reading the code:

- **The parser is hand-written recursive descent.** There is no ANTLR in any
  pom. It is a byte-for-byte drop-in candidate for legend-engine's ANTLR
  parser — 22,725/22,725 elements currently emit identical protocol JSON
  (`docs/PARSER_DROP_IN.md`).
- **Errors carry a phase, not a letter.** `error/LegendCompileException.Phase`
  is `PARSE, RESOLVE, NORMALIZE, MODEL, TYPE, MAPPING, LOWER, EXECUTE`.

### Architectural invariants

The full list, with honest per-rule enforcement status, is in
[`AGENTS.md`](AGENTS.md). The four that matter most:

1. **The frontend does ALL typing** — single source of truth. If a type is
   missing downstream, fix the frontend.
2. **The Lowerer does NO type inference** — it reads typed HIR for structure
   and never infers. No SQL syntax, no SQL function names, no dialect import.
3. **The dialect owns ALL SQL rendering.** Core has exactly one entry point:
   `SqlDialect.render(SqlQuery)`. The MIR under `sql/` is sealed, pure data —
   no `toSql()`, no `String` field encoding a SQL operation.
4. **No fallbacks, no defaulting** — fail loudly. Every default branch is a
   hidden bug.

> **`engine/src/main/java/com/gs/legend/` is a frozen legacy implementation.**
> Its parser, compiler, model, plan and sqlgen packages are superseded by
> `core`. Only the HTTP server, LSP, diagram service, JSON writer and result
> serializers are still live. See `AGENTS.md` for when opening it is legitimate.

---

## What is Legend?

[Legend](https://legend.finos.org/) is an open-source data management platform created by Goldman Sachs and donated to FINOS. Define your data model once in Pure, map it to databases, query it with type-safe semantics.

Legend Lite reimplements this in a fraction of the code, with 100% SQL push-down.

| Dimension | Legend Engine (FINOS) | Legend Lite |
|-----------|----------------------|-------------|
| **Codebase** | ~2M LOC, 400+ Maven modules | ~171K LOC, 5 modules (compiler: 120K) |
| **Execution** | Mixed: some SQL, some in-memory | 100% SQL push-down — always |
| **Parser** | ANTLR4 (156 `.g4`, 14,568 generated lines) | hand-written recursive descent, **no ANTLR** |
| **Dependencies** | Hundreds of JARs | DuckDB, H2, JUnit |
| **Java version** | Java 11 | Java 21 (records, sealed interfaces, pattern matching) |
| **Databases** | Postgres, Databricks, Snowflake, etc. | DuckDB (primary), H2, SQLite |

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

The compiler lives in `core/`. See [`core/README.md`](core/README.md) for the
per-package contracts and [`AGENTS.md`](AGENTS.md) for the invariants.

```
core/src/main/java/com/legend/          418 files   120,629 LOC
├── lexer/          5 files    1,176   text → tokens (JDK-only)
├── parser/         9 files    9,287   ElementParser, SpecParser (hand-written
│                                      recursive descent — no ANTLR anywhere)
├── protocol/      35 files    5,318   upstream-shaped wire records + emitter
├── model/         40 files    3,649   parsed element definitions
├── compiler/     154 files   20,312   NameResolver; element/ (phase F);
│                                      spec/ (phase G — Typer, InferenceKernel,
│                                      32 per-construct checkers)
├── normalizer/    18 files   10,781   legacy mapping DSL → synthesized functions
├── resolver/      26 files   24,953   StoreResolver — logical → physical (phase H)
├── lowering/      34 files   11,483   Lowerer — TypedSpec → SqlQuery (phase I)
├── sql/           33 files    7,290   MIR (sealed, pure data) + dialect/ (phase J)
├── exec/          15 files    4,260   Executor — SQL + JDBC → ExecutionResult
├── plan/           5 files    1,601   plan representation
├── builtin/        1 file     1,959   the platform prelude (Pure.java)
├── lineage/        3 files    3,043   column-level lineage
├── testdatagen/    1 file     1,656   test-data generation
├── harness/       14 files    6,832   test-support surfaces
├── validation/     3 files      675   desugar validation
├── ide/            4 files      616   demand-driven indexing (dormant)
├── values/ error/ cache/               leaves
└── Compiler.java                      the one driver; StatementExecutor is
                                       package-private behind it
```

`engine/src/main/java/com/gs/legend/` (341 files, 47,949 LOC) is the **frozen
legacy implementation**. Only its HTTP server, LSP, diagram service, JSON
writer and result serializers are still live — everything else has a `core`
counterpart that supersedes it. See `AGENTS.md`.

---

## Quick Start

### Prerequisites

- **[Bazelisk](https://github.com/bazelbuild/bazelisk)** (as `bazel`) — it runs the
  Bazel release `.bazelversion` pins. Nothing else: Bazel fetches the JDK, every jar
  (MODULE.bazel, pinned by lock files) and the legend-engine / legend-pure release
  sources the tests read as the spec (pinned by sha256).
- An IDE: IntelliJ with the Bazel plugin opens the BUILD files as the project.

### Build & Test

```bash
bazel build //...                 # everything
bazel test //core:core_tests      # the compiler suite (+ NullAway, which runs on every compile)
bazel test //...                  # every gate, and every generated file checked against its generator
bazel run //:update_generated     # regenerate every generated file from the pinned upstream release
bazel run //tools/bump -- 4.146.0 # move upstream to another release (docs/GATES.md)
```

Each gate is one test target; `docs/GATES.md` lists them.

### Run the Server

```bash
bazel run //core:server                  # LSP, query execution, SQL, diagrams
bazel build //core:server_deploy.jar     # the one self-contained jar to ship
```

The server starts on **port 8080**. Connect [Studio Lite](https://github.com/neema2/studio-lite) (the React IDE) to `http://localhost:8080`.

---

## HTTP API

| Method | Endpoint | Description | Served by |
|--------|----------|-------------|-----------|
| `POST` | `/lsp` | LSP JSON-RPC — diagnostics, completions, hover | legacy |
| `POST` | `/engine/execute` | Compile + execute Pure query → tabular or graph | **core** (legacy front/back) |
| `POST` | `/engine/sql` | Raw SQL against a Runtime's connection | legacy |
| `POST` | `/engine/diagram` | Extract class diagram from Pure model | legacy |
| `GET` | `/health` | Health check | — |

Only `/engine/execute` reaches the live compiler, and only for the compile and
execute steps — the request is still framed, connection-resolved and serialized
by `engine/`. Everything else on this table runs entirely on the frozen legacy
implementation.

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

## Testing

```bash
bazel test //...                  # all of it
bazel test //core:core_tests      # gate 1: the compiler suite + guardrails
bazel test //spec:judge_lanes     # the corpus on DuckDB and H2 under both judges
bazel test //pct:pct_duckdb       # the engine's own PCT suites against legend-lite
bazel test //parser-equivalence:parser_parity   # byte-equivalence vs legend-engine's parser
```

The upstream sources are declared inputs, so these never skip for a missing
checkout, and never run against the wrong one. Test logs and outputs land in
`bazel-testlogs/<package>/<target>/`.

---

## Project Stats

Derived 2026-08-06. Counts are `find src/main -name '*.java'`; test totals are
from surefire reports.

| Metric | Value |
|--------|-------|
| `core` (the compiler) | 418 files / 120,629 LOC main; 91 test files |
| `engine` (legacy + server) | 341 files / 47,949 LOC main; 104 test files |
| Total main source | ~171,000 LOC across 5 modules |
| `core` tests | 1,713 |
| `engine` tests | 2,730 (default suite; `heavy` group excluded) |
| PCT | 1,109 run, 0 failures, 36 ledgered expected failures, **nothing skipped** |
| Parser equivalence | **22,725 / 22,725** byte-identical, 0 DIFF |
| Rejection parity | 43 / 43 pins, 40 line-exact, 28 column-exact |
| Relational corpus | 2,575 run / 2,298 pass (2,798 / 2,398 incl. excluded stereotypes) |
| Java version | 21 |
| Dependencies | DuckDB, H2, JUnit 5 — **no ANTLR** |

---

## License

Apache 2.0
