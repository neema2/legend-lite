# XStore, Local Properties, Source/Target IDs & Cross-DB — Rosetta Stone

> Covers: **E3** (xStore), **C5** (local mapping properties), **A10** (source/target IDs), **A11** (cross-DB reference)

---

## What It Does

- **XStore** — cross-store association mapping. Joins data across different physical stores (e.g., relational DB + service store, or two different databases). Uses `$this`/`$that` predicate syntax.
- **Local Properties** — `+prop: Type[m]: expr` — properties declared in the mapping that do NOT exist on the class. Invisible to user queries. Primary use: XStore join keys.
- **Source/Target IDs** — `prop[sourceId, targetId]: @Join` — disambiguates which class mappings are connected when multiple exist for the same class.
- **Cross-DB reference** — `[otherDb]Table.col` — references a column from a different database within the same mapping.

---

## C5 + E3 Are Tightly Coupled

Local properties exist **only** to serve XStore. Without XStore, local properties are dead code — they're computed in the mapping but filtered out of all execution plans and invisible to user queries.

### Why they're coupled

1. Local properties provide join keys that don't belong in the domain model
2. XStore `$this`/`$that` expressions reference these local properties
3. Without XStore consuming them, local properties have no observable effect

---

## How legend-engine Implements Local Properties (C5)

### Syntax

```pure
###Mapping
Mapping test::M
(
    *test::Person[p]: Pure {
        ~src test::Person
        +firmName: String[0..1] : '',     // LOCAL — not on Person class
        fullName: $src.fullName,
        addressName: $src.addressName
    }

    test::Firm[f]: Pure {
        ~src test::Firm
        name: $src.name
    }

    // XStore uses the local property as join key
    test::Firm_Person: XStore {
        employer[p, f]: $this.firmName == $that.name
    }
)
```

Key: `+firmName` does NOT exist on `Person`. It exists only in this mapping.

### Compile-time: Synthetic MappingClass

legend-engine creates a **synthetic subclass** of the mapped class:

```
HelperRelationalBuilder.java (lines 1160-1183):
  MappingClass mappingClass = new Root_meta_pure_mapping_MappingClass_Impl<>(...);
  mappingClass._generalizations(Lists.immutable.with(generalization_to_Person));
  mappingClass._properties(localPropertyMappings.collect(pm -> new Property(pm.name, pm.type, pm.multiplicity)));
  base._mappingClass(mappingClass);
```

This `MappingClass` extends `Person` and adds `firmName` as a real property. This lets the compiler type-check `$this.firmName` in XStore expressions — `$this` is typed as the MappingClass, not `Person`.

### Runtime: Filtered from execution, materialized for XStore

**Not returned to users** — execution plan generation explicitly filters them out:
```pure
// precaForRelationalSetImpl (multiple protocol versions):
let propertyMappings = $setImplementation->allPropertyMappings()
    ->filter(pm|$pm.localMappingProperty==false && ...)
```

**Materialized for XStore** — when hydrating objects, local property values ARE computed and cached on the in-memory object in `mappingPropertyValues`. When an XStore join fires:

```pure
// XStore.pure lines 204-208 (reprocessElement):
if($sourceType == $f.parametersValues->at(0).genericType.rawType,
    |let owner = $f.func->cast(@Property<Nil,Any|*>).owner;
     if ($owner->isEmpty() || $owner->toOne()->instanceOf(MappingClass),
         |pair(true, ^InstanceValue(
            values=$extra.mappingPropertyValues->filter(k|$k.key == $f.func.name).value,
```

This reads `firmName` from the cached values on the source object and substitutes it as a literal into the target query.

### XStore Runtime Execution Model

XStore is a **multi-query runtime orchestrator**, not compile-time SQL. The flow for
`Person.all()->graphFetch(#{Person { employer { legalName } }}#)`:

```
Step 1: Execute query against Person's store
        → SELECT NAME, FIRM_ID FROM PersonTable
        → Hydrate Person objects (including local properties like firmName)

Step 2: For each Person, read firmName from hydrated object
        → person.firmName = "Goldman Sachs"

Step 3: Build NEW query against Firm's store
        → Firm.all()->filter(f|$f.name == 'Goldman Sachs')

Step 4: Execute Step 3 against Firm's store
        → SELECT LEGAL_NAME FROM FirmTable WHERE NAME = 'Goldman Sachs'

Step 5: Stitch Person + Firm results together
```

---

## How legend-lite Would Implement This

### Local properties: extend without class binding

In legend-lite's multi-pass architecture, local properties can be simpler than legend-engine's MappingClass approach:

1. **Builder** extracts `+prop` → stores type/multiplicity, marks as local
2. **MappingNormalizer** emits it as an extend ColSpec in the sourceRelation (same as dynaFunction properties today)
3. **TypeChecker** blocks user access naturally — `$p.firmName` fails because `firmName` isn't on the class
4. No synthetic MappingClass needed — our pass separation gives us the scoping for free

**FACT**: Steps 1-3 follow directly from existing code patterns. MappingNormalizer already creates extends for computed properties. TypeChecker already rejects properties not found on the class.

### XStore: unsolved design problem

The extend-without-class-binding works for the physical SQL side. But XStore `$this.firmName` resolution is NOT solved:

- In legend-engine, `$this` is typed as MappingClass → compiler resolves `firmName`
- In legend-lite, `$this` would be typed as `Person` → TypeChecker rejects `firmName`
- **Some mechanism** is needed to make local properties visible in XStore expression compilation

**Possible approaches (not yet designed):**
- MappingResolver compiles XStore expressions in a mapping-aware context that knows about local extends
- A lightweight "mapping type overlay" that augments the class with local properties during XStore expression compilation only
- Compile XStore predicates as lambda expressions with access to the sourceRelation column list

This is a design decision to make when we actually build E3.

---

## Upstream Mapping Syntax (legend-engine)

### E3: XStore
```pure
###Mapping
Mapping test::CrossStoreMapping
(
    *test::Person[person_relational]: Relational {
        name: [relationalDb]PersonTable.NAME,
        firmId: [relationalDb]PersonTable.FIRM_ID
    }

    *test::Firm[firm_serviceStore]: ServiceStore {
        ~service test::FirmService
        id: $src.id,
        legalName: $src.legalName
    }

    // E3: XStore association mapping — cross-store join
    test::PersonFirm: XStore {
        firm[person_relational, firm_serviceStore]: $this.firmId == $that.id,
        employees[firm_serviceStore, person_relational]: $this.id == $that.firmId
    }
)
```

### C5: Local Properties (+ prefix)
```pure
###Pure
Class test::Trade
{
    id: Integer[1];
    product: String[1];
    quantity: Integer[1];
    price: Float[1];
}

###Relational
Database db(
    Table TradeTable (ID INT PK, PRODUCT VARCHAR(200), QTY INT, PRICE DECIMAL, TAX_RATE DECIMAL)
)

###Mapping
Mapping test::M
(
    *test::Trade: Relational {
        id: [db]TradeTable.ID,
        product: [db]TradeTable.PRODUCT,
        quantity: [db]TradeTable.QTY,
        price: [db]TradeTable.PRICE,

        // C5: Local properties — not on the Trade class
        +taxRate: Float[1]: [db]TradeTable.TAX_RATE,
        +totalValue: Float[1]: multiply([db]TradeTable.QTY, [db]TradeTable.PRICE),
        +taxAmount: Float[1]: multiply([db]TradeTable.QTY, multiply([db]TradeTable.PRICE, [db]TradeTable.TAX_RATE))
    }
)
```

Key constraint: `+taxRate` does NOT exist on `Trade`. A user query `Trade.all()->project(~taxRate:t|$t.taxRate)` would **fail** — the compiler won't find `taxRate` on the class.

### A10: Source/Target IDs
```pure
###Mapping
Mapping test::MultiMapping
(
    // Two different Person mappings for different contexts
    test::Person[personA]: Relational { name: [db]PersonTableA.NAME }
    test::Person[personB]: Relational { name: [db]PersonTableB.NAME }

    test::Firm[firm]: Relational {
        legalName: [db]FirmTable.LEGAL_NAME,

        // A10: Source/Target IDs — specify which Person mapping to use
        ceo[firm, personA]: [db]@FirmCeo,       // CEO comes from PersonTableA
        employees[firm, personB]: [db]@FirmEmp   // Employees from PersonTableB
    }
)
```

### A11: Cross-DB reference
```pure
###Mapping
Mapping test::CrossDbMapping
(
    *test::Person: Relational {
        name: [primaryDb]PersonTable.NAME,
        // A11: Column from a different database
        externalId: [externalDb]ExternalIds.EXT_ID
    }
)
```

---

## Relation API Equivalent

### ❌ Not Yet Implemented

### E3: XStore → executor federation

```pure
// The Relation expression is self-describing — store info is in #>{store}#
function model::personWithFirm(): Relation<(name:String, firmName:String)>[1] {
    #>{db1.PERSON}#->extend(~[
        name: r|$r.NAME,
        // Cross-store traverse — executor sees two different stores
        firmName: r|$r->traverse(#>{db2.FIRM}#, {a,b|$a.FIRM_ID == $b.ID}).LEGAL_NAME
    ])
}
```

The Relation API doesn't need special XStore syntax — the `#>{store}#` accessor carries store info. The executor must federate: execute each store's portion separately, join in-memory.

### C5: Local properties → no Relation equivalent needed

Local properties don't translate to the Relation API. They're a mapping-layer mechanism for internal join keys. In Relation DSL, if you need an intermediate value, you `extend()` it — but it's always visible (no hidden scoping).

### A10: Source/Target IDs → named functions

```pure
// Two different Person relations — no ambiguity because they're different functions
function model::personA(): Relation<...>[1] {
    #>{db.PersonTableA}#->extend(~name: r|$r.NAME)
}
function model::personB(): Relation<...>[1] {
    #>{db.PersonTableB}#->extend(~name: r|$r.NAME)
}

// Firm explicitly chooses which Person function to traverse to
function model::firm(): Relation<(legalName:String, ceoName:String)>[1] {
    #>{db.FirmTable}#->extend(~[
        legalName: r|$r.LEGAL_NAME,
        ceoName: r|$r->traverse(model::personA(), {a,b|$a.CEO_ID == $b.ID}).name
    ])
}
```

### A11: Cross-DB → inferred from store accessor

```pure
// No special syntax needed — the store accessor carries the info
function model::person(): Relation<(name:String, externalId:String)>[1] {
    #>{db1.PERSON}#->extend(~[
        name: r|$r.NAME,
        externalId: r|$r->traverse(#>{db2.ExternalIds}#, {a,b|$a.ID == $b.PERSON_ID}).EXT_ID
    ])
}
// Executor sees db1 and db2 are different → federates automatically
```

---

## Tests in legend-lite

**Not implemented (❌) — existing stubs only.**

| Test | File | What it covers |
|------|------|----------------|
| `testLocalProperty` | [RelationalMappingIntegrationTest.java:2874](../../engine/src/test/java/com/gs/legend/test/RelationalMappingIntegrationTest.java) | C5: local property (disabled) |
| `testLocalPropertyWithJoin` | [RelationalMappingIntegrationTest.java:4822](../../engine/src/test/java/com/gs/legend/test/RelationalMappingIntegrationTest.java) | C5+E3: local property used in filter with join (disabled) |
| `testLocalMappingProperty` | [MappingDefinitionExtractionTest.java:824](../../engine/src/test/java/com/gs/legend/test/MappingDefinitionExtractionTest.java) | C5: parser extracts `+prop` prefix |
| (source/target ID) | [MappingDefinitionExtractionTest.java:851](../../engine/src/test/java/com/gs/legend/test/MappingDefinitionExtractionTest.java) | A10: parser extraction of `prop[srcId, tgtId]` syntax |

**Test strategy for E3 (XStore):**
1. **Grammar test**: Parse `XStore` association mapping with `$this.id == $that.personId`; verify AST extraction
2. **Plan generation test**: Verify executor detects two different stores and creates a federated plan (two separate SQL queries + in-memory join)
3. **Integration test**: Two DuckDB databases. Query Person→Firm via XStore. Verify correct results after federation
4. **XStore + local property test**: `$this.localProp == $that.name` resolves local property from source mapping

**Test strategy for C5 (Local Properties):**
1. Builder extracts `+prop` prefix, type, and multiplicity from grammar
2. MappingNormalizer emits local property as sourceRelation extend ColSpec
3. Verify local property is NOT accessible from user query (`$p.localProp` → compilation error)
4. Verify local property IS accessible in XStore `$this`/`$that` expressions

**Test strategy for A10 (Source/Target IDs):**
1. Define property mapping `employees[firmSet, empSet]: @FirmPerson` with explicit source/target IDs
2. Verify builder resolves `firmSet` and `empSet` to the correct class mappings
3. Verify the join is wired correctly in both directions

**Test strategy for A11 (Cross-DB refs):**
- Same infrastructure as XStore. Once E3 federation works, A11 is just `[otherDb]T.col` resolving to a different store in the traversal.

---

## Key Findings from legend-engine Source

### LocalMappingPropertyInfo protocol object
```java
// legend-engine: LocalMappingPropertyInfo.java
public class LocalMappingPropertyInfo {
    public String type;           // e.g., "String"
    public Multiplicity multiplicity;  // e.g., [0..1]
    public SourceInformation sourceInformation;
}
```

Every `PropertyMapping` has an optional `localMappingProperty` field. When non-null, the property is local.

### MappingClass creation
```java
// legend-engine: HelperMappingBuilder.java lines 561-597
public static void buildMappingClassOutOfLocalProperties(SetImplementation set, ...) {
    List<PropertyMapping> localProps = propertyMappings.toList()
        .filter(x -> x._localMappingProperty() != null && x._localMappingProperty());
    if (!localProps.isEmpty()) {
        MappingClass mappingClass = new Root_meta_pure_mapping_MappingClass_Impl<>(...);
        mappingClass._generalizations(Lists.immutable.with(generalization_to_originalClass));
        mappingClass._properties(localProps.collect(pm -> pm._property()));
        ((InstanceSetImplementation) set)._mappingClass(mappingClass);
    }
}
```

### XStore `$this` type uses MappingClass
```pure
// XStore.pure line 74:
let sourceType = ^GenericType(rawType=
    if($sourceSetImpl.mappingClass->isEmpty(),
       |$sourceSetImpl.class,
       |$sourceSetImpl.mappingClass->toOne()));
```

If a MappingClass exists (because there are local properties), `$this` is typed as the MappingClass (which includes local properties). Otherwise `$this` is the raw class.

### XStore cross-store join test (from TestMappingCompilationFromGrammar.java)
```java
// Person mapping with local property
"   *test::Person[p]: Pure {\n" +
"      ~src test::Person\n" +
"      +firmName: String[0..1] : '',\n" +    // LOCAL
"      fullName: $src.fullName\n" +
"   }\n" +
// XStore uses local property as join key
"   test::Firm_Person: XStore {\n" +
"      employer[p, f]: $this.firmName == $that.name\n" +
"   }"
```

---

## Implementation Notes

| Feature | Status | Depends On | What to Build |
|---------|--------|------------|---------------|
| C5 Local properties | ❌ | E3 | Builder: extract `+prop` prefix, type, multiplicity. MappingNormalizer: emit as extend ColSpec. Without E3, these are computed but unused. |
| E3 XStore | ❌🔧 | C5, B8 | Grammar: `XStore` keyword + `$this`/`$that` expressions. Executor federation: detect cross-store, execute each separately, join in-memory. Design needed: how to type-check `$this.localProp` without synthetic MappingClass. |
| A10 Source/Target IDs | ❌ | B8 | Builder must wire source/target IDs to resolve ambiguous class mapping references. Named functions solve this naturally in Relation API. |
| A11 Cross-DB | ❌ | E3 | No new syntax — executor must detect different stores and federate. Same infrastructure as XStore. |

### Open design questions

1. **How does `$this.localProp` type-check in legend-lite?** Legend-engine uses synthetic MappingClass. We could: (a) compile XStore expressions in a mapping-aware context, (b) create a lightweight type overlay, or (c) skip type-checking XStore predicates and validate structurally.
2. **Runtime model**: legend-engine hydrates full objects then builds new queries at runtime. In legend-lite's SQL-first model, can we instead decompose into two SQL queries at plan-generation time (no runtime query building)?
3. **Single-store XStore**: Two relational mappings in the same database with XStore between them — can this collapse to a single SQL query with a JOIN?

**Priority**: E3 is Hard infrastructure. C5 is trivial to parse/emit but useless without E3. A10 needs B8 (set ID lookup). A11 shares E3 infrastructure.
