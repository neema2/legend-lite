# Class Mapping Directives — Rosetta Stone

> Covers: **B1** (~mainTable), **B2** (~filter), **B3** (~filter via join), **B4** (~distinct), **B5** (~groupBy), **B6** (~primaryKey), **B10** (scope blocks)

---

## What It Does

Class mapping directives are optional modifiers on a `Relational` class mapping that control table selection, row filtering, deduplication, and grouping:

- **~mainTable** — specifies the primary table for the mapping
- **~filter** — applies a named database filter to all queries
- **~filter via join** — applies a filter on a joined table
- **~distinct** — adds SELECT DISTINCT
- **~groupBy** — adds GROUP BY
- **~primaryKey** — overrides the default primary key columns
- **scope blocks** — sets DB/schema/table context for nested property mappings (conciseness sugar)

---

## Upstream Mapping Syntax (legend-engine)

```pure
###Relational
Database db(
    Schema base(
        Table PersonTable (ID INT PK, NAME VARCHAR(200), FIRM_ID INT, ACTIVE INT)
        Table FirmTable (ID INT PK, LEGALNAME VARCHAR(200), REGION VARCHAR(50))
    )
    Join PersonFirm(base.PersonTable.FIRM_ID = base.FirmTable.ID)
    Filter ActiveFilter(base.PersonTable.ACTIVE = 1)
    Filter USFirmFilter(base.FirmTable.REGION = 'US')
)

###Mapping
Mapping test::M
(
    // B1: ~mainTable
    *test::Person: Relational {
        ~mainTable [db]base.PersonTable
        name: [db]base.PersonTable.NAME
    }

    // B2: ~filter (named database filter)
    *test::ActivePerson: Relational {
        ~filter [db]ActiveFilter
        name: [db]base.PersonTable.NAME
    }

    // B3: ~filter via join (filter on joined table)
    *test::USPerson: Relational {
        ~filter [db]@PersonFirm [db]USFirmFilter
        name: [db]base.PersonTable.NAME
    }

    // B4: ~distinct
    *test::UniqueFirm: Relational {
        ~distinct
        legalName: [db]base.FirmTable.LEGALNAME
    }

    // B5: ~groupBy
    *test::FirmSummary: Relational {
        ~groupBy([db]base.PersonTable.FIRM_ID)
        firmId: [db]base.PersonTable.FIRM_ID
    }

    // B6: ~primaryKey
    *test::PersonWithPK: Relational {
        ~primaryKey([db]base.PersonTable.ID)
        name: [db]base.PersonTable.NAME
    }

    // B10: scope blocks
    *test::PersonScoped: Relational {
        scope([db]base.PersonTable)
        (
            name: NAME,          // no need to repeat [db]base.PersonTable.
            firmId: FIRM_ID
        )
    }
)
```

---

## Simplified Example

```
###Relational
Database db(
    Table PERSON (ID INT PK, NAME VARCHAR(200), FIRM_ID INT, ACTIVE INT)
    Table FIRM (ID INT PK, LEGAL_NAME VARCHAR(200), REGION VARCHAR(50))
    Join PersonFirm(PERSON.FIRM_ID = FIRM.ID)
    Filter ActiveFilter(PERSON.ACTIVE = 1)
)

###Mapping
Mapping test::M
(
    *test::Person: Relational {
        ~mainTable [db]PERSON
        ~filter [db]ActiveFilter
        ~primaryKey([db]PERSON.ID)
        name: [db]PERSON.NAME,
        firmName: [db]@PersonFirm | FIRM.LEGAL_NAME
    }
)
```

---

## Relation API Equivalent

### ✅ B1: ~mainTable → `#>{db.TABLE}#`

```pure
// ~mainTable [db]PersonTable
// becomes the store accessor:
#>{db.PersonTable}#
```

### ✅ B2: ~filter → `->filter()`

```pure
// ~filter [db]ActiveFilter (where ActiveFilter = PersonTable.ACTIVE = 1)
// becomes:
#>{db.PersonTable}#->filter(r|$r.ACTIVE == 1)
```

### ❌ B3: ~filter via join → `->filter()` with traverse

```pure
// ~filter [db]@PersonFirm [db]USFirmFilter
// Proposed:
#>{db.PersonTable}#
  ->filter(r|$r->traverse(#>{db.FIRM}#, {a,b|$a.FIRM_ID == $b.ID}).REGION == 'US')
```

The engine should auto-generate an EXISTS subquery:
```sql
SELECT * FROM PERSON WHERE EXISTS (
    SELECT 1 FROM FIRM WHERE PERSON.FIRM_ID = FIRM.ID AND FIRM.REGION = 'US'
)
```

### ✅ B4: ~distinct → `->distinct()`

```pure
// ~distinct
#>{db.FirmTable}#->distinct()
```

### ✅ B5: ~groupBy → `->groupBy()`

```pure
// ~groupBy([db]PersonTable.FIRM_ID)
#>{db.PersonTable}#->groupBy(~FIRM_ID, ~[count: r|$r.ID : y|$y->count()])
```

### ❌ B6: ~primaryKey → `#>{db.TABLE | [pk]}#`

Proposed PK annotation on the store accessor:
```pure
// ~primaryKey([db]PersonTable.ID)
#>{db.PersonTable | [ID]}#->extend(~[name: r|$r.NAME])
```

This provides identity info for graphFetch batching and dedup.

### ❌ B10: scope blocks → just write full paths

```pure
// scope([db]base.PersonTable) (name: NAME, firmId: FIRM_ID)
// In Relation API, you always write the full reference:
#>{db.base.PersonTable}#->extend(~[
    name: r|$r.NAME,
    firmId: r|$r.FIRM_ID
])
```

Scope blocks are pure syntactic sugar — they save repetition of `[db]base.PersonTable.` in the mapping. The Relation API doesn't need this because the table reference is at the top (`#>{db.TABLE}#`) and column access is via `$r.COL`.

### Key files in legend-lite

- **MappingNormalizer.java** — processes ~mainTable, ~filter, ~distinct, ~groupBy
- **PlanGenerator.java** — generates DISTINCT, GROUP BY, WHERE clauses
- **RelationalMappingIntegrationTest.java** — filter, distinct, groupBy tests

### Tests in legend-lite

**Implemented (✅):**
| Test | File | What it covers |
|------|------|----------------|
| `testMappingFilter` | [RelationalMappingIntegrationTest.java:2101](../../engine/src/test/java/com/gs/legend/test/RelationalMappingIntegrationTest.java) | B2: ~filter adds WHERE clause |
| `testMappingDistinct` | [RelationalMappingIntegrationTest.java:2140](../../engine/src/test/java/com/gs/legend/test/RelationalMappingIntegrationTest.java) | B4: ~distinct adds DISTINCT |
| `testMappingFilterExtraction` | [MappingDefinitionExtractionTest.java:441](../../engine/src/test/java/com/gs/legend/test/MappingDefinitionExtractionTest.java) | B2: parser extraction |
| `testDistinctExtraction` | [MappingDefinitionExtractionTest.java:466](../../engine/src/test/java/com/gs/legend/test/MappingDefinitionExtractionTest.java) | B4: parser extraction |
| `testGroupByExtraction` | [MappingDefinitionExtractionTest.java:488](../../engine/src/test/java/com/gs/legend/test/MappingDefinitionExtractionTest.java) | B5: parser extraction |
| `testPrimaryKeyExtraction` | [MappingDefinitionExtractionTest.java:511](../../engine/src/test/java/com/gs/legend/test/MappingDefinitionExtractionTest.java) | B6: parser extraction (parsed, not wired) |

**Not implemented (❌) — disabled stubs:**
| Test | File | What it covers |
|------|------|----------------|
| `testStoreSubstitution` | [RelationalMappingIntegrationTest.java:2268](../../engine/src/test/java/com/gs/legend/test/RelationalMappingIntegrationTest.java) | B3: filter via join (disabled) |
| `testScopeBlock` | [RelationalMappingIntegrationTest.java:2276](../../engine/src/test/java/com/gs/legend/test/RelationalMappingIntegrationTest.java) | B10: scope block (disabled) |

**Test strategy for B3 (~filter via join):** Add test that defines a `Filter` on a joined table, applies `~filter [db]@J [db]Filter`, and verifies the generated SQL contains an EXISTS subquery (not a plain WHERE).

**Test strategy for B6 (~primaryKey):** Verify PK annotation flows through to graphFetch batching identity. Test that dedup uses the declared PK columns.

**Test strategy for B10 (scope blocks):** Verify that `scope([db]T) (prop: col)` produces identical SQL to `prop: [db]T.col`.

---

## Implementation Notes

| Feature | Status | What to Build |
|---------|--------|---------------|
| B1 ~mainTable | ✅ | — |
| B2 ~filter | ✅ | — |
| B3 ~filter via join | ❌ | Filter-on-traverse → EXISTS generation in PlanGenerator |
| B4 ~distinct | ✅ | — |
| B5 ~groupBy | ✅ | — |
| B6 ~primaryKey | ❌ | Parser extension: `#>{db.T \| [pk]}#` syntax |
| B10 scope | ❌ | Grammar rule for scope keyword (not needed in Relation API — it's syntax sugar) |
