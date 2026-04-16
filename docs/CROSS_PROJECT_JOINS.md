# Cross-Project Joins in Legend: Includes vs Model Joins

Cross-project joins — where one team's query needs to join data from another team's tables — are the #1 source of leaky abstraction in Legend's dependency model. This document compares the two approaches, explains why the current engine forces unnecessary compile-time dependencies, and describes how legend-lite can fix this.

## Table of Contents

1. [The Problem](#1-the-problem)
2. [Approach A: Database + Mapping Includes (Today)](#2-approach-a-database--mapping-includes-today)
3. [Approach B: XStore Model Joins (Target State)](#3-approach-b-xstore-model-joins-target-state)
4. [What the Current Engine Enforces at Compile Time](#4-what-the-current-engine-enforces-at-compile-time)
5. [Legend-Lite Compiler Opportunity](#5-legend-lite-compiler-opportunity)
6. [Support Matrix: Both Approaches in Legend-Lite](#6-support-matrix-both-approaches-in-legend-lite)
7. [Impact on Element Categories](#7-impact-on-element-categories)

---

## 1. The Problem

Legend elements fall into categories with different dependency profiles:

| Category | Elements | Ideal: needed for compilation? | Ideal: needed for runtime? |
|---|---|---|---|
| **Model** | Class, Enum, Association | Yes (type resolution) | Yes |
| **Functions** | Function (signature) | Yes (type-checking calls) | Yes (body for execution) |
| **Store** | Database, Table, Join | No | Yes (SQL generation) |
| **Mapping** | Mapping, PropertyMapping | No | Yes (query routing) |
| **Runtime** | Runtime, Connection | No | Yes (execution) |
| **Service** | Service | No | Yes (service execution) |

In the ideal world, downstream compilation only touches **Model + Function** elements. A mapping change in `refdata` doesn't trigger downstream rebuilds. A database schema change in `refdata` is invisible to the compiler.

**But cross-project joins break this.** When Team B (trading) needs to join against Team A's (refdata) tables, the traditional approach pulls Team A's stores and mappings into Team B's compile scope — turning "No" into "Yes" for Store and Mapping.

---

## 2. Approach A: Database + Mapping Includes (Today)

### Database `include`

A Database can include another Database to access its tables and joins:

```pure
###Relational
Database refdata::RefdataDB
(
  Schema Refdata
  (
    Table Sector (id INT PRIMARY KEY, name VARCHAR(100))
    Table Currency (code VARCHAR(3) PRIMARY KEY, name VARCHAR(100))
  )
)

###Relational
Database trading::TradingDB
(
  include refdata::RefdataDB          // <-- pulls in refdata's tables

  Schema Trades
  (
    Table Trade (id INT PRIMARY KEY, sector_id INT, amount DECIMAL)
  )

  // Cross-DB join referencing refdata's table
  Join Trade_Sector(Trades.Trade.sector_id = Refdata.Sector.id)
)
```

**What this means:** `TradingDB` now has a compile-time dependency on `RefdataDB`. The join definition references refdata's table schema. If refdata renames a column, trading's Database breaks at compile time.

Under the hood, `Database.getAllIncludedDBs()` recursively resolves includes. Methods like `Database.findTable()` and `Database.findJoin()` search through the entire include chain:

```java
// legend-pure: Database.java
public static Join findJoin(Database database, String joinName, ProcessorSupport ps) {
    for (Database db : getAllIncludedDBs(database, ps)) {  // walks includes!
        Join join = db.getValueInValueForMetaPropertyToManyWithKey(
            M2RelationalProperties.joins, M3Properties.name, joinName);
        if (join != null) return join;
    }
}
```

### Mapping `include` + store substitution

A Mapping can include another Mapping, optionally swapping stores:

```pure
###Mapping
Mapping refdata::RefdataMapping
(
  refdata::Sector[refdata_sector]: Relational
  {
    ~mainTable [refdata::RefdataDB]Refdata.Sector
    id:   [refdata::RefdataDB]Refdata.Sector.id,
    name: [refdata::RefdataDB]Refdata.Sector.name
  }
)

###Mapping
Mapping trading::TradingMapping
(
  include refdata::RefdataMapping         // <-- pulls in refdata's class mappings

  trading::Trade[trade]: Relational
  {
    ~mainTable [trading::TradingDB]Trades.Trade
    id:     [trading::TradingDB]Trades.Trade.id,
    amount: [trading::TradingDB]Trades.Trade.amount
  }

  // Association mapping using the cross-DB join
  trading::Trade_Sector: Relational
  {
    AssociationMapping
    (
      sector[trade, refdata_sector]: [trading::TradingDB]@Trade_Sector
    )
  }
)
```

**What this means:** `TradingMapping` has a compile-time dependency on `RefdataMapping` (and transitively on `RefdataDB`). The `MappingProcessor` resolves includes at compile time, walking the include hierarchy to build a map of all class mapping IDs:

```java
// legend-pure: Mapping.java — collectMappingEntitiesById
for (MappingInclude include : currentMapping._includes()) {
    Mapping includedMapping = ImportStub.withImportStubByPass(
        include._includedCoreInstance(), processorSupport);
    collectMappingEntitiesById(map, includedMapping, ...);  // recursive!
}
```

Store substitution (`include RefdataMapping[RefdataDB -> TradingDB]`) lets you swap the underlying store, but you still need the mapping include for the class mapping IDs.

### The dependency chain

```
trading::TradingMapping
  ─include─→ refdata::RefdataMapping          (mapping dep)
               └──→ refdata::RefdataDB        (store dep, transitive)
  ──────────→ trading::TradingDB
               └──include──→ refdata::RefdataDB  (store dep, direct)
```

**Result:** Stores and mappings become compile-time dependencies. The clean separation in the element categories table breaks down.

### Why this is problematic

The entire point of Pure's data modeling is that **the model IS the API to the data.** A Class defines the contract: its properties, types, and relationships. The Mapping and Store behind it are implementation details — how that data is physically stored and retrieved. Consumers should depend on the API (model), never on the implementation (store/mapping). This is the same encapsulation principle behind interfaces in Java, protocols in Swift, or API contracts in microservices.

**Database/Mapping includes shatter this encapsulation completely.** When Team B includes Team A's Database or Mapping, they are reaching through the API and coupling directly to Team A's implementation:

- **Broken encapsulation:** The model was supposed to be the boundary. Includes let consumers depend on internal store schemas (table names, column names, join definitions) that should be invisible to them. Team A can no longer treat their store as a private implementation detail.
- **Cascade rebuilds:** A column rename in `RefdataDB` — a purely internal refactoring that changes zero model contracts — triggers recompilation of every downstream mapping that includes it. Implementation changes cascade as if they were API changes.
- **Tight coupling:** Teams can't change their store schema without coordinating with every consumer. This is exactly the coupling that model-as-API was designed to prevent. In practice, teams stop refactoring their stores because the blast radius is too large.
- **Dependency graph bloat:** Bazel can't skip rebuilds for store/mapping changes because they're wired into the compile action. The per-element architecture's key benefit (mapping change = zero downstream rebuilds) is negated.
- **Violates per-element isolation:** The whole point of per-element files is that a mapping change shouldn't cascade to downstream compilers. Includes turn what should be a runtime-only element into a compile-time dependency.

---

## 3. Approach B: XStore Model Joins (Target State)

### The key idea

Instead of joining at the store level (Database includes + relational joins), define the relationship at the **model level** using:

1. An **Association** connecting two classes — a pure model element, no store reference
2. **Local properties** (`+propName: Type[1]`) that expose FK columns as typed model properties
3. An **XStore cross-expression** — a lambda over model properties that defines the join condition

### Concrete example

Same Team A (refdata) / Team B (trading) scenario, but using XStore:

```pure
###Pure
// MODEL LAYER — these are model elements (Class, Association)

Class refdata::LegalEntity
{
  entityId: String[1];
  name: String[1];
}

Class trading::Trade
{
  id: String[1];
  value: Integer[1];
}

// Association at model level — no store reference!
Association trading::Trade_LegalEntity
{
  client: refdata::LegalEntity[1];
  trades: trading::Trade[*];
}
```

```pure
###Relational
// TEAM A's store — completely standalone
Database refdata::EntityDatabase
(
  Schema Entity
  (
    Table LegalEntity (ENTITY_ID VARCHAR(32) PRIMARY KEY, name VARCHAR(32))
  )
)

// TEAM B's store — NO include of refdata's DB!
Database trading::TradesDatabase
(
  Schema Trades
  (
    Table Trade (id VARCHAR(32) PRIMARY KEY, value INT, ENTITY_ID_FK VARCHAR(32))
  )
)
```

```pure
###Mapping
// TEAM A's mapping — standalone, internal to refdata
Mapping refdata::LegalEntityMapping
(
  refdata::LegalEntity[legal_entity]: Relational
  {
    ~mainTable [refdata::EntityDatabase]Entity.LegalEntity
    entityId: [refdata::EntityDatabase]Entity.LegalEntity.ENTITY_ID,
    name:     [refdata::EntityDatabase]Entity.LegalEntity.name
  }
)

###DataSpace
// TEAM A publishes a DataSpace — the "API" to their data
DataSpace refdata::LegalEntityDataSpace
{
  executionContexts:
  [
    {
      name: 'default';
      mapping: refdata::LegalEntityMapping;
      defaultRuntime: refdata::LegalEntityRuntime;
    }
  ];
  defaultExecutionContext: 'default';
}

// TEAM B's mapping — includes the DataSpace, not the raw mapping
Mapping trading::XStoreTradesMapping
(
  include refdata::LegalEntityDataSpace  // DataSpace include — one level of indirection (see §4)

  trading::Trade[trade]: Relational
  {
    ~mainTable [trading::TradesDatabase]Trades.Trade
    id:    [trading::TradesDatabase]Trades.Trade.id,
    value: [trading::TradesDatabase]Trades.Trade.value,
    +entityIdFk: String[1]: [trading::TradesDatabase]Trades.Trade.ENTITY_ID_FK   // LOCAL property!
  }

  // XStore: join defined at MODEL level using properties
  trading::Trade_LegalEntity: XStore
  {
    client[trade, legal_entity]: $this.entityIdFk == $that.entityId,
    trades[legal_entity, trade]: $this.entityId   == $that.entityIdFk
  }
)
```

### What XStore eliminates

| Aspect | Old (includes) | XStore model join |
|---|---|---|
| Database `include` | Required | **Eliminated** — each team's DB is standalone |
| Cross-DB join definition | `[DB]@JoinName` referencing other team's tables | `$this.prop == $that.prop` over model properties |
| Store compile dep | Yes | **No** |
| Mapping `include` | Required | Still required (see §4) |
| SQL output | `SELECT ... JOIN ...` | **Identical** — engine converts at plan time |

### How the engine transforms XStore → SQL

At execution planning time, `localizeXStoreAssociation()` in `modelJoins.pure`:

1. Takes the XStore association implementation
2. Looks up the relational property mappings for each local property (e.g., `+entityIdFk` → `ENTITY_ID_FK` column)
3. Converts the model-level `$this.entityIdFk == $that.entityId` into a relational `Join`
4. Creates a synthetic `Database(includes=[$store1, $store2])` at runtime
5. Builds a `RelationalAssociationImplementation` with the generated join

**Net effect:** The query planner produces the **exact same SQL** as the traditional relational join. This is verified by the engine test `testPersonToFirmUsingFromProject`, which asserts SQL equality between the XStore and traditional approaches.

---

## 4. What the Current Engine Enforces at Compile Time

XStore model joins **should** only need model elements (Classes + Associations) at compile time. But the current legend-pure engine enforces more.

### The compile-time validation chain

In `XStoreProcessor.process()`:

```java
// Step 1: Build map of ALL class mapping IDs (walks includes recursively)
MapIterable<String, SetImplementation> setImpl =
    Mapping.getClassMappingsByIdIncludeEmbedded(mapping, processorSupport);

// Step 2: Validate source and target set implementation IDs exist
InstanceSetImplementation sourceSetImpl =
    MappingValidator.validateId(..., propertyMapping._sourceSetImplementationId(), "source", ...);
InstanceSetImplementation targetSetImpl =
    MappingValidator.validateId(..., propertyMapping._targetSetImplementationId(), "target", ...);
// ^^^ THROWS PureCompilationException if ID not found!

// Step 3: Get Class from set implementation (to type $this/$that)
Class<?> srcClass = getSetImplementationClass(sourceSetImpl, processorSupport);
Class<?> targetClass = getSetImplementationClass(targetSetImpl, processorSupport);
```

**Line 69-70 are the constraint:** `validateId()` demands that both `sourceSetImplementationId` ("trade") and `targetSetImplementationId` ("legal_entity") exist in the mapping's class mapping index — which includes everything pulled in via `include`.

If `legal_entity` isn't visible (because you didn't `include LegalEntityMapping`), the compiler throws:

```
Unable to find target class mapping (id:legal_entity) for property 'client'
in Association mapping 'Trade_LegalEntity'. Make sure that you have specified
a valid Class mapping id...
```

### What the cross-expression actually references

Look at what the XStore lambda actually needs:

```pure
client[trade, legal_entity]: $this.entityIdFk == $that.entityId
```

- **`$this.entityIdFk`** — a **local property** defined in YOUR mapping (`+entityIdFk`). Always local.
- **`$that.entityId`** — a regular property of the `LegalEntity` **Class**. A model element.

The types of `$this` and `$that` can be derived from the **Association** itself:

```pure
Association Trade_LegalEntity {
  client: LegalEntity[1];   // → $that is LegalEntity
  trades: Trade[*];         // → $this is Trade
}
```

### Summary: needed vs enforced

| What | Actually needed to type-check? | Currently enforced by engine? |
|---|---|---|
| Target **Class** (LegalEntity) | Yes — to type-check `$that.entityId` | Yes (via set impl) |
| **Association** (Trade_LegalEntity) | Yes — defines the relationship | Yes |
| Target **Mapping** (LegalEntityMapping) | **No** — only needed for set impl ID lookup | **Yes** (validateId throws) |
| Target **Store** (EntityDatabase) | No — not referenced in XStore expression | No |

The mapping include is an **implementation artifact** — not a fundamental requirement of the XStore design.

### What safety do we preserve vs lose?

A fair question: does dropping the mapping include at compile time cost us anything?

**The `MappingClass` mechanism.** When a class mapping defines local properties (`+entityIdFk`), the engine creates a `MappingClass` — a synthetic subclass that adds those local properties as real Class properties. The `XStoreProcessor` uses this to type-check the cross-expression:

```java
// XStoreProcessor.getSetImplementationClass
MappingClass<?> mappingClass = setImpl._mappingClass();
return mappingClass == null
    ? (Class<?>) ImportStub.withImportStubByPass(setImpl._classCoreInstance(), processorSupport)
    : mappingClass;  // includes local properties!
```

This seems like it would require the mapping — but look at which side uses local properties in practice:

```pure
// Direction 1: client[trade, legal_entity]
$this.entityIdFk == $that.entityId
//     ^^^^^^^^^^^         ^^^^^^^^
//     LOCAL prop          REGULAR Class property
//     (source = trade)    (target = legal_entity)

// Direction 2: trades[legal_entity, trade]
$this.entityId == $that.entityIdFk
//     ^^^^^^^^         ^^^^^^^^^^^
//     REGULAR prop     LOCAL prop
//     (source = legal_entity)  (target = trade)
```

**In both directions, local properties are only referenced on YOUR OWN side** — the side that defines the XStore mapping. The dependency side is only accessed through regular Class properties (`entityId`, `name`). This makes architectural sense: local properties expose YOUR FK columns for building the join condition. You'd never reference someone else's local properties — you don't even know they exist.

**What we preserve** (compile-time, model-only):
- Full type-checking of `$that.entityId` — validated against the `LegalEntity` Class
- Full type-checking of `$this.entityIdFk` — validated against your own MappingClass (always local)
- Association structure validation — property directions, multiplicities
- Type compatibility of the cross-expression (Boolean result, correct parameter types)

**What we defer to runtime:**
- Set implementation ID existence check (`legal_entity` exists as a class mapping) — today this is a compile error, would become a runtime error. Acceptable because:
  - The ID is a string reference, not a type — it can't participate in further type inference
  - Runtime catches it immediately before any query executes
  - The much more valuable check (does `LegalEntity` have property `entityId`?) stays at compile time

### DataSpace: a related indirection mechanism

Legend-engine also has **DataSpace** — a higher-level element that bundles a default mapping, runtime, and test data as a published execution context. Instead of `include refdata::RefdataMapping`, you can write `include refdata::RefdataDataSpace`, and `DataSpaceIncludedMappingHandler` resolves it to the DataSpace's default mapping:

```java
// DataSpaceIncludedMappingHandler.resolveMapping
Root_meta_pure_metamodel_dataSpace_DataSpace dataSpace = context.pureModel.getPackageableElement(mappingInclude.getFullName());
return dataSpace._defaultExecutionContext()._mapping();
```

DataSpace is aligned with the model-as-API direction — it's closer to a "published contract" than a raw mapping include. But today it still resolves to a mapping include under the hood. In legend-lite's model-join world, the DataSpace concept could evolve to be purely a runtime/execution context artifact, with no compile-time mapping dependency at all.

---

## 5. Legend-Lite Compiler Opportunity

Legend-lite's compiler can break this unnecessary coupling. The XStore cross-expression type-checking only needs model elements.

### Design: model-only XStore compilation

1. **Type `$this`/`$that` from the Association**, not from set implementations:
   ```
   Association Trade_LegalEntity {
     client: LegalEntity[1];  → $that typed as LegalEntity
     trades: Trade[*];        → $this typed as Trade
   }
   ```

2. **Source-side local properties** (`$this.entityIdFk`) — resolved from the local mapping, which is always available (it's your own mapping)

3. **Target-side properties** (`$that.entityId`) — validated against the `LegalEntity` **Class** definition (a model element), not against the target mapping

4. **Defer set implementation ID resolution to runtime** — when both mappings are available for execution planning via `localizeXStoreAssociation()`

### What this achieves

```
COMPILE TIME (legend-lite):
  Team B writes XStore mapping referencing Trade_LegalEntity association
  Compiler needs:
    ✓ Trade class              (model element — from own project)
    ✓ LegalEntity class        (model element — from dep project)
    ✓ Trade_LegalEntity assoc  (model element — from model layer)
    ✗ LegalEntityMapping       (NOT needed — deferred to runtime)
    ✗ EntityDatabase           (NOT needed — never was)

RUNTIME (plan time):
  Engine resolves XStore → relational join
  Needs both mappings + both stores
  All available at execution time
```

**Result:** The element categories table becomes **truthful** — downstream compilation only needs Model + Function elements, even with XStore cross-project joins. No mapping include needed at compile time.

---

## 6. Support Matrix: Both Approaches in Legend-Lite

Legend-lite **must support both** approaches. Thousands of existing projects use Database/Mapping includes. We can't abandon them. But we want to push new projects toward model joins.

### Legacy includes (must support)

For existing projects using Database/Mapping includes:
- Mapping + Store element files become **compile-time inputs** to the downstream `legend_library` action
- Bazel tracks them as deps; changes trigger rebuilds (correct behavior — these ARE real dependencies)
- `unused_inputs_list` still provides granularity: if Team B's compile action didn't touch `refdata__RefdataDB.json`, it's listed as unused and future changes skip the rebuild

### XStore model joins (recommended for new projects)

For new projects using XStore:
- Only Model element files are compile-time inputs
- Store and Mapping elements are runtime-only inputs (to `legend_test` actions)
- A mapping change in refdata → zero downstream rebuilds
- A store change in refdata → zero downstream rebuilds

### Migration path

Existing include-based joins can be incrementally converted to XStore:

1. **Add an Association** at the model layer connecting the two classes
2. **Add local properties** to the source mapping exposing FK columns
3. **Replace the relational AssociationMapping** with an XStore block
4. **Remove the Database `include`** (no longer needed)
5. **Keep the Mapping `include`** for now (current engine requires it) — legend-lite compiler removes this requirement

Each step is independently testable. The SQL output is identical (verified by engine tests).

### Decision table

| Scenario | Recommended approach | Store at compile? | Mapping at compile? |
|---|---|---|---|
| New project, new joins | XStore model join | **No** | **No** (in legend-lite) |
| Existing project, existing joins | Keep includes (migrate later) | Yes | Yes |
| Existing project, new joins | XStore model join | **No** | **No** (in legend-lite) |

---

## 7. Impact on Element Categories

With XStore model joins and legend-lite's compiler, the element categories table in the [Bazel Dependency Proposal](./BAZEL_DEPENDENCY_PROPOSAL.md) is accurate as the target state:

| Category | Elements | Needed for compilation? | Needed for runtime? |
|---|---|---|---|
| **Model** | Class, Enum, Association | Yes | Yes |
| **Functions** | Function (signature) | Yes | Yes |
| **Store** | Database, Table, Join | **No** (with model joins) | Yes |
| **Mapping** | Mapping, PropertyMapping | **No** (with model joins) | Yes |
| **Runtime** | Runtime, Connection | No | Yes |
| **Service** | Service | No | Yes |

**Caveat:** Projects using legacy Database/Mapping includes will have Store and Mapping as compile-time deps until migrated to XStore. Legend-lite supports both, but the architecture is designed for and optimized for the model-join world.

The per-element file architecture works correctly in both cases — the difference is only which element files are inputs to compile actions vs test/execution actions.
