# Embedded, Otherwise, Inline — Rosetta Stone

> Covers: **A6** (embedded), **A7** (otherwise embedded), **A8** (inline embedded)

---

## What It Does

These three features handle **nested object property mappings** — mapping a complex property (like `person.address` or `person.firm`) without requiring a separate ClassMapping.

- **Embedded** — inlines sub-property mappings directly in the parent: `address() { street: T.STREET, city: T.CITY }`. Columns come from the SAME table (denormalized).
- **Otherwise** — embedded with a fallback join: `address() { street: T.STREET } Otherwise([addrMapping]: @PersonAddr)`. Some sub-properties come from the parent table, others via a join.
- **Inline** — reuses another ClassMapping by ID: `address() Inline[addrMapping]`. Equivalent to copy-pasting that mapping's property mappings.

---

## Upstream Mapping Syntax (legend-engine)

From `core_relational/relational/tests/mapping/embedded/`:

```pure
###Relational
Database db(
    Table PERSON_FIRM_DENORM (
        ID INT PRIMARY KEY,
        PERSON_FIRSTNAME VARCHAR(200),
        PERSON_ADDRESS_NAME VARCHAR(200),
        PERSON_ADDRESS_TYPE INT,
        FIRM_LEGALNAME VARCHAR(200),
        FIRM_ADDRESS_NAME VARCHAR(200),
        FIRM_ID INT
    )
    Table ADDRESS (ID INT PRIMARY KEY, NAME VARCHAR(200), TYPE INT)
    Join PersonAddress(PERSON_FIRM_DENORM.ID = ADDRESS.ID)
)

###Mapping
Mapping test::embeddedMapping
(
    // A6: Embedded — all columns from the denormalized table
    *test::Person[person]: Relational {
        firstName: [db]PERSON_FIRM_DENORM.PERSON_FIRSTNAME,
        address(
            name: [db]PERSON_FIRM_DENORM.PERSON_ADDRESS_NAME,
            type: EnumerationMapping GeoType: [db]PERSON_FIRM_DENORM.PERSON_ADDRESS_TYPE
        ),
        firm(
            legalName: [db]PERSON_FIRM_DENORM.FIRM_LEGALNAME,
            address(
                name: [db]PERSON_FIRM_DENORM.FIRM_ADDRESS_NAME
            )
        )
    }
)
```

Otherwise example:

```pure
Mapping test::otherwiseMapping
(
    // A7: Otherwise — embedded sub-properties + fallback join
    *test::Person: Relational {
        firstName: [db]PERSON_FIRM_DENORM.PERSON_FIRSTNAME,
        address(
            name: [db]PERSON_FIRM_DENORM.PERSON_ADDRESS_NAME
        ) Otherwise([addrMapping]: [db]@PersonAddress)
    }

    test::Address[addrMapping]: Relational {
        name: [db]ADDRESS.NAME,
        type: EnumerationMapping GeoType: [db]ADDRESS.TYPE
    }
)
```

Inline example:

```pure
Mapping test::inlineMapping
(
    // A8: Inline — reuses another mapping by ID
    *test::Person: Relational {
        firstName: [db]PERSON.FIRSTNAME,
        address() Inline[addrMapping]
    }

    test::Address[addrMapping]: Relational {
        name: [db]ADDRESS.NAME,
        type: [db]ADDRESS.TYPE
    }
)
```

---

## Simplified Example

```
###Relational
Database db(
    Table PERSON (ID INT PK, NAME VARCHAR(200), ADDR_STREET VARCHAR(200), FIRM_NAME VARCHAR(200), ADDR_ID INT)
    Table ADDRESS (ID INT PK, STREET VARCHAR(200), CITY VARCHAR(200))
    Join PersonAddr(PERSON.ADDR_ID = ADDRESS.ID)
)

###Mapping
Mapping test::M
(
    // A6: Embedded
    *test::Person[p1]: Relational {
        name: [db]PERSON.NAME,
        address(
            street: [db]PERSON.ADDR_STREET
        )
    }

    // A7: Otherwise — street from parent table, city from join
    *test::Person[p2]: Relational {
        name: [db]PERSON.NAME,
        address(
            street: [db]PERSON.ADDR_STREET
        ) Otherwise([addr]: [db]@PersonAddr)
    }

    // A8: Inline — reuse addr mapping
    *test::Person[p3]: Relational {
        name: [db]PERSON.NAME,
        address() Inline[addr]
    }

    test::Address[addr]: Relational {
        street: [db]ADDRESS.STREET,
        city: [db]ADDRESS.CITY
    }
)
```

---

## Relation API Equivalent

### ✅ Already Implemented

All three variants work end-to-end in legend-lite.

```pure
// A6: Embedded — 0-param lambda wrapping ColSpecArray
function model::personEmbedded(): Relation<(name:String, address:...)>[1] {
    #>{db.PERSON}#
      ->extend(~name: r|$r.NAME)
      ->extend(~address:
          {-> ~[                           // 0-param lambda = "embedded" marker
              street: row|$row.ADDR_STREET
          ]}
      )
}
// Generated SQL: SELECT "root".NAME, "root".ADDR_STREET FROM PERSON — NO JOIN

// A7: Otherwise — TWO separate extends (enables join pruning)
function model::personOtherwise(): Relation<(name:String, address:...)>[1] {
    #>{db.PERSON}#
      ->extend(~name: r|$r.NAME)
      ->extend(~address:                   // 1) embedded sub-properties (no join)
          {-> ~[street: row|$row.ADDR_STREET]}
      )
      ->extend(                            // 2) fallback — join for remaining props
          traverse(#>{db.ADDRESS}#, {a,b|$a.ADDR_ID == $b.ID}),
          ~address: {src,tgt|$tgt.CITY}
      )
}
// If query only accesses address.street → JOIN is PRUNED (0 JOINs)
// If query accesses address.city → JOIN is included (1 JOIN)

// A8: Inline — call the reusable function via traverse
function model::addrRel(): Relation<(street:String, city:String)>[1] {
    #>{db.ADDRESS}#->extend(~[
        street: r|$r.STREET,
        city: r|$r.CITY
    ])
}

function model::personInline(): Relation<(name:String, address:...)>[1] {
    #>{db.PERSON}#
      ->extend(~name: r|$r.NAME)
      ->extend(
          traverse(model::addrRel(), {a,b|$a.ADDR_ID == $b.ID}),
          ~address: {src,tgt|$tgt}   // all columns from addrRel
      )
}
```

### Why two extends for Otherwise?

**Join pruning.** When a query only accesses embedded properties (e.g., `$p.address.street`), the planner eliminates the join entirely — the association extend is unused, so the JOIN never appears in SQL.

Tests confirm: `testOtherwisePruningEmbeddedOnly` = **0 JOINs**, `testOtherwisePruningFallbackOnly` = 1 JOIN.

### Key files in legend-lite

- **MappingNormalizer.java** — `addEmbeddedExtends()` creates 0-param lambda ColSpecArray
- **ExtendChecker.java** — `isEmbeddedExtend()` detects embedded pattern
- **MappingResolver.java** — `resolveAssociationJoins()` handles otherwise fallback
- **PlanGenerator.java** — generates correlated subqueries for nested graphFetch

### Tests in legend-lite

**Embedded (§28):**
| Test | File | What it covers |
|------|------|----------------|
| `testEmbeddedSingleProperty` | [RelationalMappingIntegrationTest.java:4298](../../engine/src/test/java/com/gs/legend/test/RelationalMappingIntegrationTest.java) | Project one embedded sub-property, verify 0 JOINs |
| `testEmbeddedMultiProperty` | [RelationalMappingIntegrationTest.java:4309](../../engine/src/test/java/com/gs/legend/test/RelationalMappingIntegrationTest.java) | Multiple embedded sub-properties |
| `testEmbeddedFilter` | [RelationalMappingIntegrationTest.java:4325](../../engine/src/test/java/com/gs/legend/test/RelationalMappingIntegrationTest.java) | Filter on embedded prop, 0 JOINs |
| `testEmbeddedNoJoinSql` | [RelationalMappingIntegrationTest.java:4335](../../engine/src/test/java/com/gs/legend/test/RelationalMappingIntegrationTest.java) | SQL assertion: no JOIN, correct column ref |
| `testEmbeddedAndAssociationTogether` | [RelationalMappingIntegrationTest.java:4361](../../engine/src/test/java/com/gs/legend/test/RelationalMappingIntegrationTest.java) | Embedded + association coexistence (1 JOIN) |

**Otherwise (§31):**
| Test | File | What it covers |
|------|------|----------------|
| `testOtherwiseEmbeddedProperty` | [RelationalMappingIntegrationTest.java:4580](../../engine/src/test/java/com/gs/legend/test/RelationalMappingIntegrationTest.java) | Embedded sub-prop from parent table |
| `testOtherwiseFallbackProperty` | [RelationalMappingIntegrationTest.java:4592](../../engine/src/test/java/com/gs/legend/test/RelationalMappingIntegrationTest.java) | Fallback sub-prop via join |
| `testOtherwiseMixed` | [RelationalMappingIntegrationTest.java:4607](../../engine/src/test/java/com/gs/legend/test/RelationalMappingIntegrationTest.java) | Both embedded + fallback in one query |
| `testOtherwisePruningEmbeddedOnly` | [RelationalMappingIntegrationTest.java:4651](../../engine/src/test/java/com/gs/legend/test/RelationalMappingIntegrationTest.java) | **0 JOINs** when only embedded accessed |
| `testOtherwisePruningFallbackOnly` | [RelationalMappingIntegrationTest.java:4662](../../engine/src/test/java/com/gs/legend/test/RelationalMappingIntegrationTest.java) | **1 JOIN** when fallback accessed |
| `testOtherwisePruningMixed` | [RelationalMappingIntegrationTest.java:4673](../../engine/src/test/java/com/gs/legend/test/RelationalMappingIntegrationTest.java) | 1 JOIN when both accessed |

**Inline (§30):**
| Test | File | What it covers |
|------|------|----------------|
| `testInlineSingleProperty` | [RelationalMappingIntegrationTest.java:4493](../../engine/src/test/java/com/gs/legend/test/RelationalMappingIntegrationTest.java) | Inline set reference, 0 JOINs |
| `testInlineMultiProperty` | [RelationalMappingIntegrationTest.java:4503](../../engine/src/test/java/com/gs/legend/test/RelationalMappingIntegrationTest.java) | Multiple inline props |

See also: [MappingDefinitionExtractionTest.java](../../engine/src/test/java/com/gs/legend/test/MappingDefinitionExtractionTest.java) — parser extraction tests for embedded (line 687), inline (line 723), otherwise (line 751).

---

## Implementation Notes

**Status: ✅ All implemented.**

The Relation API representation for embedded is the 0-param lambda wrapping a ColSpecArray — an internal AST marker that `ExtendChecker.isEmbeddedExtend()` recognizes. This is more verbose than the mapping DSL but enables join pruning optimization for otherwise mappings.
