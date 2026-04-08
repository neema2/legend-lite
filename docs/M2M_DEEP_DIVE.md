# Deep Dive: M2M Mappings in legend-engine/legend-pure vs legend-lite

> Research doc for M2M2J (JSON-source M2M) feature.  
> Answers: "Is legend-lite modeling M2M correctly?" and "What's needed for JSON sources?"

---

## 1. Upstream Metamodel (legend-pure)

The canonical M2M class hierarchy in `platform_dsl_mapping/grammar/mapping.pure`:

```
SetImplementation                      (id, root, class, parent)
  └─ InstanceSetImplementation         (mappingClass, aggregateSpecification) + PropertyMappingsImplementation (stores, propertyMappings)
       └─ PureInstanceSetImplementation  (srcClass: Type[0..1], filter: LambdaFunction<Any>[0..1])

PropertyMapping                        (owner, targetSetImplementationId, sourceSetImplementationId, property, localMappingProperty, store)
  └─ PurePropertyMapping               (transform: LambdaFunction<Any>[1], transformer: ValueTransformer<Any>[0..1], explodeProperty: Boolean[0..1])
```

### Key Fields

| Field | Description |
|-------|-------------|
| `srcClass` | The source class (optional — `[0..1]`). When present, the mapping pulls data from instances of this class. |
| `filter` | Optional filter lambda on source instances (predicate over `$src`). |
| `transform` | The property mapping expression. A lambda `{src: SrcClass[1] \| ...}` computing one target property from `$src`. |
| `transformer` | Optional `EnumerationMapping` for enum-valued properties. |
| `explodeProperty` | For `*` multiplicity explosion (one-to-many from single source). |
| `targetSetImplementationId` | For class-typed properties: which `SetImplementation` maps the return type. |
| `sourceSetImplementationId` | Which set impl this property mapping belongs to (for disambiguation). |

### Processor Flow

`PureInstanceSetImplementationProcessor.process()`:
1. Resolves `srcClass` via `ImportStub`
2. For `filter`: adds `$src` param to lambda, then full-matches
3. For each `PurePropertyMapping`: adds `$src` param to `transform` lambda, full-matches
4. If `transformer` present (enum mapping), processes the `EnumerationMapping`

The `$src` parameter is **injected by the processor** — the grammar doesn't include it in the lambda signature. The processor creates a `VariableExpression` named `"src"` typed to `srcClass`.

### Validator Rules

`PureInstanceSetImplementationValidator`:
- Filter must return `Boolean`
- DataType properties: expression type must be compatible with property type
- Class-typed properties: `targetSetImplementationId` must exist; target set impl's `srcClass` must match expression return type
- Multiplicity: expression multiplicity must be subsumed by property multiplicity (unless `explodeProperty`)

---

## 2. Connection & Store Model (legend-engine)

### ModelStore

```pure
Class meta::external::store::model::ModelStore extends Store
{
  toString(){'ModelStore'}:String[1];
}
```

The "store" for M2M is a **phantom store** — it doesn't represent a real database. It's a marker that routes execution to the M2M engine.

### Connection Hierarchy

```
Connection
  └─ PureModelConnection (base)
       ├─ ModelConnection        (instances: Map<Class, List<Any>>)     — in-memory objects
       ├─ JsonModelConnection    (class: Class, url: String)            — JSON data via URL
       ├─ XmlModelConnection     (class: Class, url: String)            — XML data via URL
       └─ ModelQueryConnection   (instancesProvider: Map<Class, Func>)  — lazy providers
  └─ ModelChainConnection       (mappings: Mapping[*])                  — M2M chain
```

### Connection Resolution (Root Graph Fetch)

In `planRootGraphFetchExecutionInMemory`:
1. Gets `srcClass` from `PureInstanceSetImplementation.srcClass`
2. Searches runtime's `connectionStores` for a match:
   - `JsonModelConnection.class == srcClass` → use this connection
   - `ModelConnection.instances.get(srcClass).isNotEmpty()` → use this
   - `ModelChainConnection` → separate chain handling
3. Falls back to chain connection if no direct match

### Chain Execution Pattern

```pure
// Runtime setup for a 3-step chain: JSON → Mapping1 → Mapping2
^Runtime(connectionStores = [
    ^ConnectionStore(element=^ModelStore(),
        connection=^ModelChainConnection(mappings=[SrcToBridgeMapping])),
    ^ConnectionStore(element=^ModelStore(),
        connection=^JsonModelConnection(class=SourceClass, url='data:...'))
])
```

Execution via `executeChain()`:
1. `allReprocess()` rewrites the query through each mapping in the chain
2. For each mapping, the query's class references are rewritten to the **source** class
3. Property access (`$src.firstName`) is replaced with `eval(transformLambda, $srcExpr)`
4. The final query targets the last mapping in the chain
5. That query is routed to the terminal connection (e.g., `JsonModelConnection`)

---

## 3. Execution Modes

### In-Memory Execution (interpretive)

`executeInMemory()` / `inMemoryGetAll()`:
1. Get source instances from `ModelConnection.instances.get(srcClass)`
2. Apply filter if present
3. For each instance, call `transformWithMapping(src, impl, routedSpec, staticData)`
4. `transformWithMapping` applies each `PurePropertyMapping.transform` lambda to the source instance
5. For class-typed properties, recursively resolves the target mapping and transforms

This is **fully interpretive** — Pure lambdas are evaluated against live objects.

### Graph Fetch Execution Plan

`planRootGraphFetchExecutionInMemory()` creates:
- `StoreStreamReadingExecutionNode` — reads source data (JSON deserialization)
- `InMemoryRootGraphFetchExecutionNode` — applies M2M transforms
- `InMemoryPropertyGraphFetchExecutionNode` — nested property graph fetch

The Java execution engine deserializes JSON → objects → applies transforms → serializes result.

---

## 4. legend-lite's M2M Implementation — Comparison

### What legend-lite has

| Concept | legend-engine | legend-lite |
|---------|---------------|-------------|
| M2M mapping record | `PureInstanceSetImplementation` | `PureClassMapping` |
| Source class | `srcClass: Type[0..1]` | `sourceClassName: String` |
| Property transform | `PurePropertyMapping.transform: LambdaFunction` | `propertyExpressions: Map<String, ValueSpecification>` |
| Filter | `filter: LambdaFunction` | `filter: ValueSpecification` |
| Source mapping link | Resolved via `targetSetImplementationId` + mapping lookup | `sourceMapping: ClassMapping` (resolved eagerly) |
| Execution | In-memory (interpretive) | **Compiled to SQL** (relational pipeline) |

### How legend-lite executes M2M today

1. **MappingNormalizer**: resolves M2M chains — finds the terminal relational source. Each M2M mapping's source class must ultimately bottom out at a relational mapping.
2. **GetAllChecker**: dispatches on `MappingExpression.M2M`, compiles property expressions with `$src` param typed as `ClassType(sourceClassName)`
3. **MappingResolver**: creates `PropertyResolution.M2MExpression(expression, sourceResolution)` where `sourceResolution` is the resolved **relational** source's `StoreResolution`
4. **PlanGenerator**: for `M2MExpression`, calls `generateScalar(expression, "src", sourceResolution, alias)` — generates SQL column expressions applied directly to the source table

### Key Insight: legend-lite compiles M2M to SQL

This is fundamentally different from legend-engine's approach. In legend-engine, M2M is interpretive (objects in memory). In legend-lite, M2M expressions become SQL:

```pure
// M2M mapping:
Person: Pure { ~src RawPerson; fullName: $src.firstName + ' ' + $src.lastName }

// legend-lite generates:
SELECT ("t0"."FIRST_NAME" || ' ' || "t0"."LAST_NAME") AS "fullName" FROM "T_RAW_PERSON" AS "t0"
```

The M2M property expressions (`$src.firstName + ' ' + $src.lastName`) are compiled to SQL column expressions where `$src.firstName` resolves via the source relational mapping to `t0.FIRST_NAME`.

---

## 5. Assessment: Is legend-lite Modeling M2M Correctly?

### ✅ Correct

- **`PureClassMapping` structure** — mirrors `PureInstanceSetImplementation` correctly: target class, source class, property expressions, filter
- **`$src` variable binding** — `GetAllChecker` creates the lambda param correctly, matching `PureInstanceSetImplementationProcessor`'s injection of `$src`
- **M2M chain resolution** — `MappingNormalizer` resolves chains (M2M → M2M → Relational) correctly, matching legend-engine's `allReprocess` chain walking
- **Association navigation in M2M** — `$src.rawAddresses` correctly resolves to join navigation via `resolveM2MAssociationNavigations`
- **Filter support** — M2M filter (`~filter $src.isActive == true`) compiled correctly
- **Property Resolution** — `M2MExpression` correctly stores both the expression AST and the source's `StoreResolution`, enabling recursive scalar generation

### ⚠️ Differences (by design)

- **No interpretive execution** — legend-lite always compiles to SQL. This is by design and is actually more efficient.
- **No `ModelStore` / `ModelConnection`** — legend-lite doesn't need these because M2M is compiled away. The runtime connection points to the actual relational database.
- **No `targetSetImplementationId` routing** — legend-lite resolves source mapping eagerly in the compiler (`sourceMapping: ClassMapping`), not at runtime via set IDs.
- **`propertyExpressions: Map<String, ValueSpecification>`** instead of `List<PurePropertyMapping>` with individual `transform` lambdas — slightly different structure but functionally equivalent.

### ❌ Missing/Gaps

- **`explodeProperty`** — not modeled. Used for multiplicity explosion in legend-engine.
- **`transformer` (EnumerationMapping in M2M)** — not supported in M2M context (only relational enum mappings work).
- **`srcClass` optionality** — legend-engine allows `srcClass[0..1]` (omitted = "auto-map" / pass-through). legend-lite requires `sourceClassName`.
- **M2M Embedded** (`M2MEmbeddedSetImplementation`) — not modeled.
- **Union/Merge** (`OperationSetImplementation`) — not modeled.

---

## 6. The Type Algebra: Relation Space vs Object Space

### Two type spaces

There are two fundamental data representations:
- **Relation** — rows & columns (tabular)
- **C[*]** — class instances with properties (objects / graph)

The mapping type determines which space you start in:
- **Relational mapping**: source = `#{table}` → you start in **Relation space**
- **M2M mapping**: source = `SourceClass.all()` → you start in **C[*] space** (objects)

### Operations and their type signatures

```
                    Relation space          C[*] space
                    ──────────────          ──────────
  extend            Rel -> Rel              C[*] -> C[*]       (stay within)
  filter            Rel -> Rel              C[*] -> C[*]       (stay within)
  ─────────────────────────────────────────────────────────────
  project           Rel -> Rel              C[*] -> Relation    (exit object space)
  graphFetch        Rel -> C[*]             C[*] -> C[*]        (enter object space)
```

`project` and `graphFetch` are **inverses** — they bridge between the two spaces:
```
  project:     C[*] -> Relation     (flatten: objects -> table)
  graphFetch:  Relation -> C[*]     (assemble: table -> objects)
```

### The key design trick: `extend(C[*], ColSpecArray) -> C[*]`

Today, `extend` only works on Relations. For M2M, we need a new overload:
```
extend(Relation, ColSpecArray) -> Relation    // existing — adds columns to a table
extend(C[*], ColSpecArray)     -> C[*]        // NEW — adds computed properties to objects
```

**`extend` on C[*] stays in object space** — it does NOT flatten to Relation.
This is distinct from `project(C[*])` which DOES flatten:
```
project(C[*], ColSpecArray) -> Relation       // existing — objects -> table (QUERY)
extend(C[*], ColSpecArray)  -> C[*]           // NEW — objects -> objects (MAPPING)
filter(C[*], Function)      -> C[*]           // existing — already has Class overload
```

The same `extend(C[*])` overload handles both scalar properties AND associations:

```pure
// FULL M2M desugared form — everything stays in object space:
Person: RawPerson.all()                                                    // C[*]
    ->filter(src | $src.age > 18)                                          // C[*] -> C[*]
    ->extend([~fullName: src | $src.firstName + ' ' + $src.lastName])      // C[*] -> C[*]  (scalar)
    ->extend([~upperLast: src | $src.lastName->toUpper()])                 // C[*] -> C[*]  (scalar)
    ->extend([~address: {-> traverse(T_ADDR, {p,h | $p.ID == $h.FK})}])   // C[*] -> C[*]  (assoc)

// Compare: FULL Relational desugared form — everything stays in Relation space:
Person: #{T_PERSON}                                                        // Relation
    ->filter(row | $row.AGE > 18)                                          // Rel -> Rel
    ->extend([~fullName: row | $row.FIRST_NAME])                           // Rel -> Rel    (column)
    ->extend([~upperLast: row | toUpper($row.LAST_NAME)])                  // Rel -> Rel    (column)
    ->extend([~address: {-> traverse(T_ADDR, {p,h | $p.ID == $h.FK})}])   // Rel -> Rel    (join)
```

Same shape, same operations, different type space.

### How this maps to mapping + query

The mapping DEFINES property resolutions (via `extend`).
The query BRIDGES type spaces (via `project` or `graphFetch`):

```
Relational mapping + project:      Rel -> Rel -> Rel          (natural, stays in Rel)
Relational mapping + graphFetch:   Rel -> Rel -> C[*]         (bridge into object space)
M2M mapping + graphFetch:          C[*] -> C[*] -> C[*]       (natural, stays in C[*])
M2M mapping + project:             C[*] -> C[*] -> Relation   (bridge into Rel space)
```

### What a mapping IS

A mapping is **source + property resolutions** (metadata). NOT a Relation expression.

```
MAPPING = source + property resolutions

Relational:
  source:     tableReference("T_PERSON")              <- Relation (columns)
  properties: {firstName -> Column("FIRST_NAME")}     <- column refs

M2M:
  source:     getAll("RawPerson")                      <- C[*] (object properties)
  properties: {fullName -> $src.firstName + ' ' + ...} <- object property expressions
```

Both `project` and `graphFetch` use the same property resolutions — they just produce
different output shapes. The mapping doesn't prescribe the output shape; the query does.

---

## 7. JSON Internalization: DuckDB 1.5 VARIANT Column

When `JsonModelConnection` provides JSON data as the M2M source, we load it into a
temp table with a single **VARIANT** column. DuckDB 1.5 (March 2026) introduced the
native VARIANT type — unlike JSON (stored as text/VARCHAR), VARIANT stores **typed binary
data** with automatic columnar shredding. This gives **10-100x faster** queries vs JSON.

| | JSON type | VARIANT type (DuckDB 1.5+) |
|---|---|---|
| **Storage** | Text (VARCHAR) | Typed binary, per-row type info |
| **Shredding** | None — full text parsed each access | Auto-shreds into columnar storage |
| **Extraction** | `data->>'key'` (text parse) | `data.key` (dot notation, binary read) |
| **Performance** | Baseline | 10-100x faster for selective queries |

### How it works end-to-end

```sql
-- 1. Load JSON into VARIANT temp table (pre-compilation step):
CREATE TEMP TABLE _json_RawPerson("data" VARIANT);
INSERT INTO _json_RawPerson VALUES ('{"firstName":"John","lastName":"Doe","age":30}'::VARIANT);

-- 2. M2M property expression $src.firstName + ' ' + $src.lastName compiles to:
SELECT (_j."data".firstName || ' ' || _j."data".lastName) AS "fullName"
FROM _json_RawPerson AS _j

-- DuckDB auto-shreds VARIANT — only reads firstName and lastName from storage (skips age)
```

### Why VARIANT (not JSON)

- **10-100x faster** — typed binary + auto-shredding vs text parsing
- **Columnar selective reads** — only accessed fields read from storage
- **Reuses existing pipeline** — `ExpressionAccess` + `VariantTextExtract` already works
- **Dialect-agnostic** — Pure type = `Variant`; DuckDB = `VARIANT`, Snowflake = `VARIANT`
- **Nested objects** — `data.address.city` via dot notation
- **All URL types** — inline `data:`, `file:`, `http:` — just fetch and INSERT

---

## 8. Legend-Engine vs Legend-Lite: JSON Source Handling

### Legend-engine: in-memory Java objects

```
JsonModelConnection(class=RawPerson, url='data:...')
    → deserialize JSON → List<RawPerson> Java objects
    → $src.firstName = rawPerson.getFirstName()  (Java getter)
    → NO mapping for source class, NO SQL, NO table
```

### Legend-lite: compiled to SQL via VARIANT temp table

```
JsonModelConnection(class=RawPerson, url='data:...')
    → load JSON into VARIANT temp table (pre-compilation)
    → synthesize MappingExpression.Relational for RawPerson → tableReference("_json_RawPerson")
    → $src.firstName = variant_extract(data, 'firstName')  (SQL expression)
```

### Where data loading belongs

Data loading is a **pre-compilation step** — NOT in QueryService itself:

```
Parser → [DataLoader] → Compiler → PlanGenerator → Dialect → Execute
```

`DataLoader` reads bytes from external sources and streams them into DuckDB temp
tables. Each source type has a handler:

| Source type | Handler | Materialization |
|-------------|---------|----------------|
| `JsonModelConnection` (URL) | `JsonDataHandler` | JSON → VARIANT temp table |
| REST API | `RestDataHandler` | HTTP GET → VARIANT temp table |
| Flat file (CSV) | `CsvDataHandler` | `read_csv()` → temp table |
| `^Class` instances | `NewInstanceHandler` | objects → VARIANT temp table |

For JSON (the first handler we'll build):
1. Inspects parsed runtime for `JsonModelConnection` records
2. Fetches JSON from URL (data URI → parse inline; file: → read; http: → GET)
3. Creates VARIANT temp table: `CREATE TEMP TABLE _json_{class}("data" VARIANT)`
4. Inserts rows: `INSERT INTO _json_{class} VALUES (?::VARIANT)`
5. Returns `DataRegistry` mapping `className → tableName`
6. MappingNormalizer uses registry to synthesize sourceRelation for loaded classes

---

## 9. Implementation Plan

### Phase 1: `extend(C[*])` overload

Register the new overload in `BuiltinFunctionRegistry`:
```java
extend(C[*], ColSpecArray) -> C[*]
extend(C[*], FuncColSpecArray) -> C[*]    // with lambda expressions
```

**Files to change:**
- `BuiltinFunctionRegistry` — add overload signatures
- `ExtendChecker` — handle ClassType source (similar to how `ProjectChecker`
  handles ClassType via `checkClassSource()` and `GroupByChecker` does the same)
- `PlanGenerator` — handle extend on ClassType source (emit the same SQL as today's
  M2M path, since internally it all compiles to SQL over the source table)

**Test:** Write M2M mapping in desugared Relation API form, verify same SQL output:
```pure
// DSL form:
Person: Pure { ~src RawPerson; fullName: $src.firstName + ' ' + $src.lastName }

// Desugared form (should produce identical SQL):
Person: RawPerson.all()->extend([~fullName: src | $src.firstName + ' ' + $src.lastName])
```

### Phase 2: Align M2M record shape with Relational

Inline the filter into a source chain (like Relational does), keep property
resolutions separate (also like Relational):

```java
// TODAY:
MappingExpression.M2M(sourceClassName, propertyExpressions, filter)
//                     string          separate              separate

// AFTER — mirror Relational pattern:
MappingExpression.M2M(sourceChain, propertyExpressions)
//                     ValSpec chain   separate (same as today)
// sourceChain = getAll("RawPerson") -> filter($src.age > 18)
// filter inlined into chain, not a separate field
```

This mirrors Relational exactly:
```
Relational:  sourceChain = tableReference("T") -> filter() -> extend(traverse())
             properties  = {firstName → Column("FIRST_NAME")}    (in StoreResolution)

M2M:         sourceChain = getAll("RawPerson") -> filter($src.age > 18)
             properties  = {fullName → $src.firstName + ' ' + ...} (propertyExpressions)
```

Both: **filter inlined in source chain** + **property resolutions separate**.
Property resolutions are separate because `project` and `graphFetch` consume them
individually per-property.

The source class resolves through its OWN mapping entry — which is either:
- A user-written `MappingExpression.Relational` (RawPerson has a `~mainTable`)
- A synthesized `MappingExpression.Relational` (RawPerson is JSON-backed, Phase 5)
- Another `MappingExpression.M2M` (chain continues)

**Files to change:**
- `MappingExpression.M2M` record — replace `(sourceClassName, propertyExprs, filter)`
  with `(sourceChain, propertyExprs)` where sourceChain is a ValueSpec
- `MappingNormalizer` — build sourceChain: `getAll(className)`, append filter if present
- `GetAllChecker` / `MappingResolver` — extract source class name from `getAll()` node
  in sourceChain instead of reading field directly

### Phase 3: Grammar — `JsonModelConnection`

Parse legend-engine syntax in runtime declarations:
```pure
Runtime test::TestRuntime
{
    mappings: [model::PersonMapping];
    connections: [
        ModelStore: [
            json: JsonModelConnection {
                class: model::RawPerson;
                url: 'data:application/json,[{"firstName":"John","lastName":"Doe"}]';
            }
        ]
    ];
}
```

**Files to change:**
- `PureParser` — parse `JsonModelConnection` block (class + url)
- New model record: `JsonModelConnection(String className, String url)`
- `RuntimeDefinition` — store alongside existing `ConnectionStore` records

### Phase 4: `DataLoader` (pre-compilation)

Reads bytes from external sources, streams them into DuckDB temp tables.
Handler interface:

```java
interface DataHandler {
    boolean canHandle(ConnectionDefinition connection);
    String load(ConnectionDefinition connection, java.sql.Connection jdbc);
    // returns temp table name, e.g. "_json_RawPerson"
}

public class DataLoader {
    private final List<DataHandler> handlers;  // JsonDataHandler first, others later

    public DataRegistry load(RuntimeDefinition runtime, java.sql.Connection jdbc) {
        var registry = new DataRegistry();  // className → tableName
        for (var conn : runtime.nonRelationalConnections()) {
            var handler = handlers.stream()
                .filter(h -> h.canHandle(conn)).findFirst().orElseThrow();
            String tableName = handler.load(conn, jdbc);
            registry.put(conn.className(), tableName);
        }
        return registry;
    }
}
```

First handler — `JsonDataHandler`:
```java
public class JsonDataHandler implements DataHandler {
    public boolean canHandle(ConnectionDefinition conn) {
        return conn instanceof JsonModelConnection;
    }
    public String load(ConnectionDefinition conn, java.sql.Connection jdbc) {
        var jmc = (JsonModelConnection) conn;
        String tableName = "_json_" + jmc.className();
        String json = fetchUrl(jmc.url());           // data: | file: | http:
        jdbc.execute("CREATE TEMP TABLE " + tableName + "(\"data\" VARIANT)");
        for (var row : parseJsonArray(json)) {
            jdbc.execute("INSERT INTO " + tableName + " VALUES (?::VARIANT)", row);
        }
        return tableName;
    }
}
```

**Integration point:** `QueryService` calls `DataLoader` after parsing, before
compilation. Passes the `DataRegistry` to `MappingNormalizer`.

### Phase 5: Relational mapping for loaded source classes

Loaded data is physically stored in a temp table — this IS a relational table.
So the source class gets a real `MappingExpression.Relational` (not "fake" —
the data genuinely lives in a table). MappingNormalizer synthesizes it from the registry:

```java
// For each entry in JsonSourceRegistry:
String tableName = registry.get("RawPerson");  // "_json_RawPerson"
var sourceRel = new AppliedFunction("tableReference", List.of(new CString(tableName)));
expressions.put(rawPersonId, new MappingExpression.Relational("RawPerson", sourceRel));
```

MappingResolver builds `StoreResolution` with VARIANT extraction for each class property:
```
firstName → ExpressionAccess(column="data", key="firstName", type="String")
lastName  → ExpressionAccess(column="data", key="lastName",  type="String")
age       → ExpressionAccess(column="data", key="age",       type="Integer")
```

M2M chain resolution finds this as the terminal source — unchanged.

### Phase 6: Dialect update for VARIANT dot notation

Update `DuckDBDialect.renderVariantTextAccess()`:
```java
// Current (JSON type):  expr->>'key'
// New (VARIANT):        variant_extract(expr, 'key')   or   expr.key
```

`SqlExpr.VariantTextExtract` node stays the same. Only rendering changes.

### Phase 7: Nested JSON (associations)

- **Nested objects**: `data.address.city` — VARIANT supports dot notation natively
- **To-many arrays**: `UNNEST(data.addresses)` via lateral flatten for array associations

### What's Already in Place

| Component | Status |
|-----------|--------|
| `ExpressionAccess` + `VariantTextExtract` pipeline | ✅ |
| `PropertyResolution.Expression` in StoreResolution | ✅ |
| M2M chain resolution (`MappingNormalizer.resolveM2MChain()`) | ✅ |
| `M2MExpression` in `MappingResolver.resolveM2M()` | ✅ |
| `generateScalar` M2M path in PlanGenerator | ✅ |
| MappingNormalizer sourceRelation synthesis pattern (relational) | ✅ |
| `filter(C[*])` overload | ✅ |
| `project(C[*])` overload | ✅ |
| `DuckDBDialect` VARIANT rendering | ⚠️ needs dot notation update |

### What Needs to Be Built

| Phase | Component | Files | Risk |
|-------|-----------|-------|------|
| 1 | `extend(C[*])` overload | Registry, ExtendChecker, PlanGenerator | Low — follows project(C[*]) pattern |
| 2 | M2M record: sourceChain + propertyExprs | MappingExpression, MappingNormalizer, GetAllChecker, MappingResolver | Low — structural change, mirrors Relational |
| 3 | `JsonModelConnection` grammar | PureParser, new model record, RuntimeDefinition | Low — parsing only |
| 4 | `DataLoader` + `JsonDataHandler` | New classes, QueryService integration | Low — isolated pre-step |
| 5 | JSON source resolution | MappingNormalizer, MappingResolver | Medium — synthesize mapping + VARIANT extraction |
| 6 | Dialect VARIANT dot notation | DuckDBDialect | Low — rendering change |
| 7 | Nested JSON | DuckDBDialect, MappingResolver | Medium — UNNEST for arrays |

---

## 10. Summary

| Question | Answer |
|----------|--------|
| Is legend-lite's M2M correct? | **Yes.** Structurally mirrors upstream. SQL compilation vs interpretation is by design. |
| Relation vs Object space? | Two type spaces. Relational starts in Relation, M2M starts in C[*]. `extend`/`filter` stay within. `project`/`graphFetch` bridge between. |
| `extend(C[*])` overload? | **Key design trick.** Adds computed properties to objects without flattening. Handles both scalar exprs and association traversals. Same shape as `extend(Relation)`. |
| What is a mapping? | **Source + property resolutions.** Both `project` and `graphFetch` use the same resolutions — query decides output shape. |
| How does legend-engine handle JSON? | No mapping for source class. JSON → Java objects. `$src.prop` = Java getter. |
| How does legend-lite handle JSON? | VARIANT temp table. `$src.prop` → `variant_extract(data, 'prop')`. 10-100x faster than JSON text via auto-shredding. |
| Where does data loading happen? | Pre-compilation step (`DataLoader`). Reads bytes, streams into DuckDB temp tables. JSON is first handler. Extensible to REST, CSV, `^Class` instances. |
| MappingNormalizer changes? | M2M stays as-is (source class name + property resolutions). JSON source classes get synthesized `MappingExpression.Relational` because the data IS in a table (VARIANT temp table). Chain resolution finds it as terminal source. |
