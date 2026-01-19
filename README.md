# Legend Lite

A clean-room implementation of the FINOS Legend Engine core, designed for **100% SQL Push-Down** execution.

## Architecture

Legend Lite uses a **three-server architecture** for query execution:

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│ MetadataService │     │  QueryService   │     │ ServiceExecutor │
│   (Stateful)    │     │  (Stateless)    │     │  (Stateless)    │
├─────────────────┤     ├─────────────────┤     ├─────────────────┤
│ • Holds model   │     │ • Ad-hoc queries│     │ • Service exec  │
│ • Caches plans  │     │ • One-shot      │     │ • Plan playback │
│ • Service defs  │     │ • No state      │     │ • No compilation│
└─────────────────┘     └─────────────────┘     └─────────────────┘
```

### MetadataService (Stateful)

Manages the compiled Pure model and caches execution plans.

```java
MetadataService metadata = new MetadataService(pureSource);

// Generate and cache execution plan
ExecutionPlan plan = metadata.generatePlan("app::MyService", "app::MyRuntime");
```

**Use cases:**
- Long-running server with pre-deployed models
- Service definitions that are executed repeatedly
- Plan caching for performance

### QueryService (Stateless)

Compiles and executes ad-hoc queries in a single call. No state retained.

```java
QueryService query = new QueryService();

// Execute ad-hoc query
RelationResult result = query.execute(
    pureSource,
    "Person.all()->filter({p | $p.age > 30})->project(...)",
    "app::MyRuntime"
);

// Or just compile without executing
ExecutionPlan plan = query.compile(pureSource, query, runtimeName);
```

**Use cases:**
- Exploratory data analysis
- One-off queries
- Serverless/lambda execution

### ServiceExecutor (Stateless)

Executes pre-compiled plans. No compilation, just execution.

```java
ServiceExecutor executor = new ServiceExecutor();

// Execute a pre-compiled plan
RelationResult result = executor.execute(plan, connectionOverride);
```

**Use cases:**
- Plan playback from cache
- Distributed execution
- Edge deployment

---

## Quick Start

### Define Your Model (Pure Syntax)

```
Class model::Person {
    firstName: String[1];
    lastName: String[1];
    age: Integer[1];
}

Database store::PersonDB (
    Table T_PERSON (
        ID INTEGER PRIMARY KEY,
        FIRST_NAME VARCHAR(100),
        LAST_NAME VARCHAR(100),
        AGE INTEGER
    )
)

Mapping model::PersonMapping (
    Person: Relational {
        firstName: T_PERSON.FIRST_NAME,
        lastName: T_PERSON.LAST_NAME,
        age: T_PERSON.AGE
    }
)

Connection store::MyConnection {
    database: DuckDB;
    specification: InMemory;
}

Runtime app::MyRuntime {
    mappings: [ model::PersonMapping ];
    connections: [ store::PersonDB: store::MyConnection ];
}
```

### Execute a Query

```java
QueryService qs = new QueryService();

RelationResult result = qs.execute(
    pureSource,
    "Person.all()->filter({p | $p.lastName == 'Smith'})->project({p | $p.firstName})",
    "app::MyRuntime"
);

for (List<Object> row : result.rows()) {
    System.out.println(row.get(0)); // firstName
}
```

---

## FAQ

### What databases are supported?

| Database | Status |
|----------|--------|
| DuckDB | ✅ Full support |
| SQLite | ✅ Full support |
| H2 | ✅ Basic support |
| PostgreSQL | 🚧 In progress |
| Snowflake | 📋 Planned |

### What's the difference between QueryService and MetadataService?

| Feature | QueryService | MetadataService |
|---------|--------------|-----------------|
| State | Stateless | Stateful |
| Plan caching | No | Yes |
| Use case | Ad-hoc queries | Production services |
| Startup cost | Higher (compiles each time) | Lower (cached plans) |

### How does SQL push-down work?

All queries are translated to native SQL and executed entirely in the database:

```
Pure Query:
Person.all()
    ->filter({p | $p.age > 30})
    ->project({p | $p.firstName})

Generated SQL:
SELECT "t0"."FIRST_NAME" AS "firstName"
FROM "T_PERSON" AS "t0"
WHERE "t0"."AGE" > 30
```

No data is fetched into the JVM for filtering or projection.

### What about associations/joins?

Associations are fully supported with automatic EXISTS optimization:

```
// Filter through to-many association (uses EXISTS)
Person.all()
    ->filter({p | $p.addresses.city == 'NYC'})
    ->project({p | $p.firstName})

// Project through association (uses LEFT JOIN)
Person.all()
    ->project({p | $p.firstName}, {p | $p.addresses.street})
```

---

## Building

```bash
mvn clean install
```

## Testing

```bash
mvn test
```

All 123 tests run in ~3 seconds.

---

## License

Apache 2.0
