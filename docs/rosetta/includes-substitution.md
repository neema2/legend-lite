# Mapping Includes & Store Substitution — Rosetta Stone

> Covers: **C1** (mapping includes), **C2** (store substitution)

---

## What It Does

- **Mapping includes** — imports all class/association/enumeration mappings from another mapping. Enables modular composition: define Person mapping separately, include it in a larger mapping.
- **Store substitution** — when including a mapping, swap the database references. Enables reusing the same mapping logic against different physical databases.

---

## Upstream Mapping Syntax (legend-engine)

```pure
###Mapping
Mapping test::PersonMapping
(
    *test::Person[person]: Relational {
        ~mainTable [PersonDB]PersonTable
        name: [PersonDB]PersonTable.NAME
    }
)

###Mapping
Mapping test::FirmMapping
(
    *test::Firm[firm]: Relational {
        ~mainTable [FirmDB]FirmTable
        legalName: [FirmDB]FirmTable.LEGAL_NAME
    }
)

###Mapping
Mapping test::FullMapping
(
    // C1: Mapping includes
    include test::PersonMapping
    include test::FirmMapping

    // C2: Store substitution — swap PersonDB → FullDB
    include test::PersonMapping [PersonDB -> FullDB]
    include test::FirmMapping   [FirmDB   -> FullDB]

    // Multiple substitutions on one include
    include test::CrossMapping  [Db1 -> FullDb, Db2 -> FullDb]
)
```

---

## Simplified Example

```
###Relational
Database db1(Table PERSON (ID INT PK, NAME VARCHAR(200)))
Database db2(Table PERSON (ID INT PK, NAME VARCHAR(200)))

###Mapping
Mapping test::PersonBase
(
    *test::Person: Relational {
        ~mainTable [db1]PERSON
        name: [db1]PERSON.NAME
    }
)

###Mapping
Mapping test::Combined
(
    // C1: Include — brings in Person mapping as-is
    include test::PersonBase

    // C2: Include with store substitution — rewires db1 → db2
    include test::PersonBase [db1 -> db2]
)
```

---

## Relation API Equivalent

### ✅ C1: Mapping includes → function composition

Mapping includes are just importing definitions. In Relation API, this is calling another function:

```pure
// PersonBase mapping → a function
function model::personRel(): Relation<(name:String)>[1] {
    #>{db1.PERSON}#->extend(~name: r|$r.NAME)
}

// "Including" it = just calling the function
function model::combined(): Relation<(name:String, firmName:String)>[1] {
    model::personRel()  // "include" — reuse the person relation
      ->extend(
          traverse(#>{db1.FIRM}#, {a,b|$a.FIRM_ID == $b.ID}),
          ~firmName: {s,t|$t.LEGAL_NAME}
      )
}
```

### ❌ C2: Store substitution → parameterized functions

Proposed design from the Rosetta Stone plan:

```pure
// Parameterized store — function takes a database parameter
function model::personRel($store: Database[1]): Relation<(name:String)>[1] {
    #>{$store.PERSON}#->extend(~name: r|$r.NAME)
}

// Use with different stores:
model::personRel(db1)   // → SELECT NAME FROM db1.PERSON
model::personRel(db2)   // → SELECT NAME FROM db2.PERSON
```

The `#>{$store.TABLE}#` syntax enables store substitution without any special `include` mechanism.

### Key files in legend-lite

- **PureModelBuilder.java** — parses mapping includes
- **MappingRegistry.java** — resolves included mappings
- **RelationalMappingIntegrationTest.java** — `testMappingInclude` (✅)

### Tests in legend-lite

**Implemented (✅):**
| Test | File | What it covers |
|------|------|----------------|
| `testMappingInclude` | [RelationalMappingIntegrationTest.java:2231](../../engine/src/test/java/com/gs/legend/test/RelationalMappingIntegrationTest.java) | C1: include pulls class mappings from other mapping |
| `testSimpleInclude` | [MappingDefinitionExtractionTest.java:925](../../engine/src/test/java/com/gs/legend/test/MappingDefinitionExtractionTest.java) | C1: parser extraction |
| `testIncludeWithStoreSubstitution` | [MappingDefinitionExtractionTest.java:950](../../engine/src/test/java/com/gs/legend/test/MappingDefinitionExtractionTest.java) | C2: parser extraction (syntax parsed) |

**Not implemented (❌) — disabled stubs:**
| Test | File | What it covers |
|------|------|----------------|
| `testStoreSubstitution` | [RelationalMappingIntegrationTest.java:2268](../../engine/src/test/java/com/gs/legend/test/RelationalMappingIntegrationTest.java) | C2: store substitution (disabled) |

**Test strategy for C2 (store substitution):**
1. Define `BaseMapping` against `store::DevDB`
2. Define `ProdMapping` including `BaseMapping[store::DevDB -> store::ProdDB]`
3. Verify queries through `ProdMapping` read from `ProdDB` tables
4. Verify SQL references `ProdDB` table names, not `DevDB`

---

## Implementation Notes

| Feature | Status | What to Build |
|---------|--------|---------------|
| C1 Mapping includes | ✅ | — |
| C2 Store substitution | ❌ | 1. Parameterized store accessor syntax `#>{$store.T}#`. 2. Builder must resolve store parameter references. 3. Runtime must substitute store at query time. |

**Priority**: C2 is Medium — depends on parameterized function support.
