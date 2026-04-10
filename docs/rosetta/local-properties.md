# Local Properties — Rosetta Stone

> Covers: **C5** (local mapping properties)

---

## What It Does

Local properties are columns that exist ONLY in the mapping — they are NOT defined on the class. Declared with the `+` prefix syntax, they create computed or auxiliary columns visible only when querying with this specific mapping.

Use cases:
- XStore join keys (internal columns needed for cross-store joins)
- Computed values not in the domain model
- Intermediate values for complex property derivations

---

## Upstream Mapping Syntax (legend-engine)

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

        // C5: Local property — not on the Trade class, only visible in this mapping
        +taxRate: Float[1]: [db]TradeTable.TAX_RATE,
        +totalValue: Float[1]: multiply([db]TradeTable.QTY, [db]TradeTable.PRICE),
        +taxAmount: Float[1]: multiply([db]TradeTable.QTY, multiply([db]TradeTable.PRICE, [db]TradeTable.TAX_RATE))
    }
)
```

Key constraint: `+taxRate` does NOT exist on `Trade` class. It only exists in this mapping context.

---

## Simplified Example

```
###Pure
Class test::Order { id: Integer[1]; amount: Float[1]; }

###Relational
Database db(Table ORDERS (ID INT PK, AMOUNT DECIMAL, TAX_RATE DECIMAL, DISCOUNT DECIMAL))

###Mapping
Mapping test::M
(
    *test::Order: Relational {
        id: [db]ORDERS.ID,
        amount: [db]ORDERS.AMOUNT,
        +taxRate: Float[1]: [db]ORDERS.TAX_RATE,
        +netAmount: Float[1]: multiply([db]ORDERS.AMOUNT, subtract(1, [db]ORDERS.DISCOUNT))
    }
)
```

---

## Relation API Equivalent

### ❌ Not Yet Implemented (but no new primitive needed)

In Relation API, local properties are just `extend()` computed columns. There's no concept of "mapping-scoped visibility" — the column exists because you created it.

```pure
function model::orderWithTaxRate(): Relation<(id:Integer, amount:Float, taxRate:Float, netAmount:Float)>[1] {
    #>{db.ORDERS}#->extend(~[
        id: r|$r.ID,
        amount: r|$r.AMOUNT,

        // Local property → just another extend column
        taxRate: r|$r.TAX_RATE,
        netAmount: r|$r.AMOUNT * (1 - $r.DISCOUNT)
    ])
}
```

The Relation API is MORE explicit: you see exactly what columns exist because you wrote the `extend()`. The "only visible in this mapping" scoping is an OO concept that doesn't apply to tabular relations.

**Generated SQL**:
```sql
SELECT "root".ID AS "id",
       "root".AMOUNT AS "amount",
       "root".TAX_RATE AS "taxRate",
       ("root".AMOUNT * (1 - "root".DISCOUNT)) AS "netAmount"
FROM ORDERS AS "root"
```

### Key files in legend-lite

- **MappingNormalizer.java** — would need to process `+prop` syntax as additional extend columns
- **PackageableElementBuilder.java** — parses `+` prefix in property mappings

### Tests in legend-lite

**Not implemented (❌) — disabled stubs:**
| Test | File | What it covers |
|------|------|----------------|
| `testLocalProperty` | [RelationalMappingIntegrationTest.java:2874](../../engine/src/test/java/com/gs/legend/test/RelationalMappingIntegrationTest.java) | C5: local property (disabled) |
| `testLocalMappingProperty` | [MappingDefinitionExtractionTest.java:824](../../engine/src/test/java/com/gs/legend/test/MappingDefinitionExtractionTest.java) | C5: parser extracts `+prop` prefix |

**Test strategy for C5 (local properties):**
1. Define mapping with `+fullAddress: String[1]: concat([db]T.STREET, ', ', [db]T.CITY)`
2. Verify `fullAddress` appears as projected column in output
3. Verify `fullAddress` is NOT in the class definition (only in mapping)
4. Verify SQL generates `CONCAT(STREET, ', ', CITY) AS fullAddress`
5. Test filtering on local property: `->filter({p|$p.fullAddress->startsWith('123')})`

---

## Implementation Notes

| Feature | Status | What to Build |
|---------|--------|---------------|
| C5 Local properties | ❌ | 1. Builder must recognize `+prop` prefix and extract type/multiplicity. 2. MappingNormalizer emits extra `extend()` columns for local properties. 3. These columns should be available for downstream query (filter, project, etc.). |

**Priority**: Medium. No new Relation API primitive needed — `extend()` already handles this. The gap is purely in the mapping DSL parser/builder, not in the Relation engine.

**Key insight**: The Relation API naturally handles this because ALL columns are explicit. There's no separate "local" concept — if you want a column, you `extend()` it.
