# Database Objects — Rosetta Stone

> Covers: **D1** (table), **D6** (filter), **D7** (view), **D8** (schema), **D9** (database include), **D10** (tabular function), **D11** (multi-grain filter)

---

## What It Does

The `###Relational` Database block defines the physical schema: tables, views, joins, filters, and their organization into schemas. These are referenced by `###Mapping` blocks.

---

## Upstream Mapping Syntax (legend-engine)

```pure
###Relational
Database test::db
(
    // D8: Schema grouping
    Schema trading
    (
        // D1: Table definition
        Table TradeTable (
            ID INT PRIMARY KEY,
            PRODUCT VARCHAR(200),
            QTY INT,
            PRICE DECIMAL,
            TRADER_ID INT,
            STATUS VARCHAR(10)
        )

        // D7: View — precomputed relation
        View TradeView
        (
            ~filter ActiveTradesFilter
            tradeId: TradeTable.ID,
            product: TradeTable.PRODUCT,
            totalValue: multiply(TradeTable.QTY, TradeTable.PRICE)
        )
    )

    // D1: Another table (default schema)
    Table TraderTable (ID INT PRIMARY KEY, NAME VARCHAR(200))

    // Join definition
    Join Trade_Trader(trading.TradeTable.TRADER_ID = TraderTable.ID)

    // D6: Named filter
    Filter ActiveTradesFilter(trading.TradeTable.STATUS = 'ACTIVE')

    // D11: Multi-grain filter
    MultiGrainFilter PersonGrain(PersonFirmDenorm.DLEVEL = 'P')
    MultiGrainFilter FirmGrain(PersonFirmDenorm.DLEVEL = 'F')

    // D10: Tabular function (references an existing DB function)
    TabularFunction myFunction (id Integer, name VARCHAR(200))

    // D9: Database include
    include test::otherDb
)
```

---

## Simplified Examples

### D1: Table → `#>{db.TABLE}#`
```pure
// Mapping:  ~mainTable [db]TradeTable
// Relation:
#>{db.TradeTable}#
```

### D6: Filter → `->filter()`
```pure
// Database: Filter ActiveFilter(TradeTable.STATUS = 'ACTIVE')
// Mapping:  ~filter [db]ActiveFilter
// Relation:
#>{db.TradeTable}#->filter(r|$r.STATUS == 'ACTIVE')
```

### D7: View → named function
```pure
// Database View: View TradeView(tradeId: T.ID, total: multiply(T.QTY, T.PRICE))
// Relation:
function model::tradeView(): Relation<(tradeId:Integer, total:Float)>[1] {
    #>{db.TradeTable}#
      ->filter(r|$r.STATUS == 'ACTIVE')
      ->extend(~[
          tradeId: r|$r.ID,
          total: r|$r.QTY * $r.PRICE
      ])
}
```

### D8: Schema → part of table path
```pure
// Database: Schema trading (Table TradeTable ...)
// Relation:
#>{db.trading.TradeTable}#
```

### D9: Database include → resolved at compilation
```pure
// Database: include test::otherDb
// In Relation API, this is transparent — you reference any table from any included DB
#>{db.TableFromOtherDb}#
```

### D10: Tabular function → parameterized store accessor
```pure
// Database: TabularFunction myFunction(id Integer, name VARCHAR(200))
// Proposed Relation:
#>{db.myFunction('param1', 42)}#

// Or wrap in a Pure function:
function model::myTableFn(p1: String[1]): Relation<(id:Integer, name:String)>[1] {
    #>{db.myFunction($p1)}#
}
```

### D11: Multi-grain filter → `->filter()` with discriminator
```pure
// Database: MultiGrainFilter PersonGrain(DENORM.DLEVEL = 'P')
// Relation:
function model::personRel(): Relation<(name:String)>[1] {
    #>{db.PersonFirmDenorm}#
      ->filter(r|$r.DLEVEL == 'P')
      ->extend(~name: r|$r.PERSON_NAME)
}

function model::firmRel(): Relation<(legalName:String)>[1] {
    #>{db.PersonFirmDenorm}#
      ->filter(r|$r.DLEVEL == 'F')
      ->extend(~legalName: r|$r.FIRM_NAME)
}
```

---

## Relation API Equivalent — Summary

| DB Object | Relation Equivalent | Status |
|-----------|-------------------|--------|
| **Table** | `#>{db.Table}#` | ✅ |
| **Filter** | `->filter(predicate)` | ✅ |
| **View** | Named function returning `Relation<...>[1]` | ✅ |
| **Schema** | Part of table path: `#>{db.schema.Table}#` | ✅ |
| **DB include** | Transparent — resolved at compilation | ✅ |
| **TabularFunction** | `#>{db.funcName(args)}#` — parameterized accessor | ❌ |
| **MultiGrainFilter** | `->filter(r\|$r.DISCRIMINATOR == 'value')` | ❌ (trivial) |

### Key files in legend-lite

- **PackageableElementBuilder.java** — parses Database definitions
- **MappingNormalizer.java** — resolves table, filter, view references
- **ViewIntegrationTest.java** — comprehensive view tests

### Tests in legend-lite

| Test | File | What it covers |
|------|------|----------------|
| `testStringColumn` | [RelationalMappingIntegrationTest.java:112](../../engine/src/test/java/com/gs/legend/test/RelationalMappingIntegrationTest.java) | D1: table as data source |
| `testMappingFilter` | [RelationalMappingIntegrationTest.java:2101](../../engine/src/test/java/com/gs/legend/test/RelationalMappingIntegrationTest.java) | D6: filter on table |
| `testViewAsDataSource` | [RelationalMappingIntegrationTest.java:2358](../../engine/src/test/java/com/gs/legend/test/RelationalMappingIntegrationTest.java) | D7: view as mainTable |
| `testViewAllFeatures` | [RelationalMappingIntegrationTest.java:2393](../../engine/src/test/java/com/gs/legend/test/RelationalMappingIntegrationTest.java) | D7: view with join + DynaFunction |
| `testViewJoinPruning` | [RelationalMappingIntegrationTest.java:2438](../../engine/src/test/java/com/gs/legend/test/RelationalMappingIntegrationTest.java) | D7: unused view join → 0 JOINs |
| `testViewEndToEnd` | [RelationalMappingIntegrationTest.java:2487](../../engine/src/test/java/com/gs/legend/test/RelationalMappingIntegrationTest.java) | D7: filter+join+groupBy+distinct |
| `testViewWithGroupBy` | [RelationalMappingIntegrationTest.java:2729](../../engine/src/test/java/com/gs/legend/test/RelationalMappingIntegrationTest.java) | D7: view with ~groupBy |
| `testSchemaTable` | [RelationalMappingIntegrationTest.java:2773](../../engine/src/test/java/com/gs/legend/test/RelationalMappingIntegrationTest.java) | D8: schema-qualified table |
| `testDatabaseInclude` | [RelationalMappingIntegrationTest.java:2824](../../engine/src/test/java/com/gs/legend/test/RelationalMappingIntegrationTest.java) | D9: database include merges tables |
| `testFilterExtraction` | [MappingDefinitionExtractionTest.java:267](../../engine/src/test/java/com/gs/legend/test/MappingDefinitionExtractionTest.java) | D6: parser extraction |
| `testViewExtraction` | [MappingDefinitionExtractionTest.java:229](../../engine/src/test/java/com/gs/legend/test/MappingDefinitionExtractionTest.java) | D7: parser extraction |
| `testSchemaPreservation` | [MappingDefinitionExtractionTest.java:313](../../engine/src/test/java/com/gs/legend/test/MappingDefinitionExtractionTest.java) | D8: parser extraction |
| `testDatabaseIncludes` | [MappingDefinitionExtractionTest.java:293](../../engine/src/test/java/com/gs/legend/test/MappingDefinitionExtractionTest.java) | D9: parser extraction |

**Not implemented — test strategy:**
- **D10 TabularFunction**: Add grammar rule for `TabularFunction F(arg1 TYPE, ...) (col: expr)`. Test that it produces a parameterized relation function with SQL `TABLE(F(...))`. Verify argument substitution.
- **D11 MultiGrainFilter**: Add test with discriminator column filter. Since this is just a `->filter()` with a grain discriminator, verify the WHERE clause matches the grain value.

---

## Implementation Notes

| Feature | Status | What to Build |
|---------|--------|---------------|
| D1 Table | ✅ | — |
| D6 Filter | ✅ | — |
| D7 View | ✅ | — |
| D8 Schema | ✅ | — |
| D9 DB include | ✅ | — |
| D10 TabularFunction | ❌ | Parameterized store accessor syntax `#>{db.func(args)}#`. Legend only references table functions — doesn't create them. |
| D11 MultiGrainFilter | ❌ | Just `->filter()` with a discriminator predicate. Trivial — no new primitive needed. |

**Priority**: D10 (Low), D11 (Low — purely syntactic sugar)
