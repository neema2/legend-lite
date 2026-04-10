# Milestoning — Rosetta Stone

> Covers: **G** (business/processing milestoning)

---

## What It Does

Milestoning handles **temporal data** — rows with validity periods (from_z/thru_z) or snapshot dates. The engine auto-injects date filters so queries only see data valid at a specific point in time.

Two distinct layers:
1. **Table definition** — annotates which columns are milestoning columns
2. **Query time** — `->asOf(date)` propagates temporal filters through the entire query, including joins

### Milestoning types
- **Business temporal** — `business(BUS_FROM=col, BUS_THRU=col)` — effective date range
- **Business snapshot** — `business(BUS_SNAPSHOT_DATE=col)` — point-in-time snapshot
- **Processing temporal** — `processing(PROCESSING_IN=col, PROCESSING_OUT=col)` — audit trail
- **Bi-temporal** — both processing and business together

---

## Upstream Mapping Syntax (legend-engine)

### Table definition with milestoning
```pure
###Relational
Database db(
    Table ProductTable(
        milestoning(
            business(BUS_FROM=from_z, BUS_THRU=thru_z, INFINITY_DATE=%9999-12-31T00:00:00)
        )
        id INT PRIMARY KEY,
        name VARCHAR(200),
        price DECIMAL,
        from_z DATE,
        thru_z DATE
    )

    Table OrderTable(
        milestoning(
            processing(PROCESSING_IN=in_z, PROCESSING_OUT=out_z, INFINITY_DATE=%9999-12-31T00:00:00)
        )
        id INT PRIMARY KEY,
        product_id INT,
        quantity INT,
        in_z TIMESTAMP,
        out_z TIMESTAMP
    )

    Table BiTemporalTable(
        milestoning(
            business(BUS_FROM=bus_from, BUS_THRU=bus_thru),
            processing(PROCESSING_IN=proc_in, PROCESSING_OUT=proc_out)
        )
        ...
    )

    Join Order_Product(OrderTable.product_id = ProductTable.id)
)
```

### Query-time milestoning
```pure
// Business milestoning — auto-filters to valid-at date
Product.all(%2024-01-15)
// Generated SQL: SELECT * FROM ProductTable WHERE from_z <= '2024-01-15' AND thru_z > '2024-01-15'

// Processing milestoning
Order.all(%2024-06-30)
// Generated SQL: SELECT * FROM OrderTable WHERE in_z <= '2024-06-30' AND out_z > '2024-06-30'

// Bi-temporal — two dates
BiTemporal.all(%2024-06-30, %2024-01-15)

// Key: milestoning propagates through joins
Order.all(%2024-01-15).product(%2024-01-15)
// Generated SQL: SELECT ... FROM OrderTable o
//   JOIN ProductTable p ON o.product_id = p.id
//   WHERE o.in_z <= '2024-01-15' AND o.out_z > '2024-01-15'
//     AND p.from_z <= '2024-01-15' AND p.thru_z > '2024-01-15'

// allVersions() skips the milestoning filter
Product.allVersions()
// Generated SQL: SELECT * FROM ProductTable (no date filter)
```

---

## Simplified Example

```
###Relational
Database db(
    Table PRODUCT(
        milestoning(business(BUS_FROM=VALID_FROM, BUS_THRU=VALID_THRU, INFINITY_DATE=%9999-12-31))
        ID INT PK, NAME VARCHAR(200), PRICE DECIMAL, VALID_FROM DATE, VALID_THRU DATE
    )
)

// Query: Product.all(%2024-06-15)
// SQL: SELECT * FROM PRODUCT WHERE VALID_FROM <= '2024-06-15' AND VALID_THRU > '2024-06-15'
```

---

## Relation API Equivalent

### ❌ Not Yet Implemented — Two-Layer Design

### Manual (works today)
```pure
// You CAN do milestoning manually with filter:
#>{db.PRODUCT}#
  ->filter(r|$r.VALID_FROM <= %2024-01-15 &&
             ($r.VALID_THRU > %2024-01-15 || $r.VALID_THRU == %9999-12-31))
  ->extend(~[name: r|$r.NAME, price: r|$r.PRICE])
```

### Proposed: `->asOf()` convenience
```pure
// Layer 1: Table-level milestoning annotation (in Database definition)
// Already declared in ###Relational block — engine reads this metadata

// Layer 2: Query-time convenience function
#>{db.PRODUCT}#
  ->asOf(%2024-01-15)                    // auto-injects: from_z <= date AND thru_z > date
  ->extend(~[name: r|$r.NAME, price: r|$r.PRICE])

// Bi-temporal:
#>{db.PRODUCT}#
  ->asOf(today(), %2024-06-30)           // processing date, business date

// The KEY insight: asOf propagates through traverse chains
#>{db.ORDER}#
  ->asOf(%2024-01-15)
  ->extend(
      traverse(#>{db.PRODUCT}#, {a,b|$a.PROD_ID == $b.ID}),
      ~prodName: {s,t|$t.NAME}
  )
// asOf ALSO injects filter on PRODUCT — propagates through traverse

// allVersions — skip milestoning
#>{db.PRODUCT}#
  ->allVersions()                         // no date filter injected
  ->extend(~[name: r|$r.NAME, validFrom: r|$r.VALID_FROM, validThru: r|$r.VALID_THRU])

// allVersionsInRange
#>{db.PRODUCT}#
  ->allVersionsInRange(%2023-01-01, %2024-01-01)
```

### Key design decisions
- `asOf()` reads milestoning column metadata from the table definition
- Propagation through `traverse()` is automatic — the engine injects date filters on joined tables
- `allVersions()` / `allVersionsInRange()` opt out of milestoning
- Inclusivity flags (`THRU_IS_INCLUSIVE`) affect the generated comparison operators

### Tests in legend-lite

**Not implemented (❌) — no existing tests.**

See also: [AsOfJoinCheckerTest.java](../../engine/src/test/java/com/gs/legend/test/AsOfJoinCheckerTest.java) — validates `asOf` join syntax in the Relation API (partially related to milestoning).

**Test strategy for G (Milestoning):**
1. **Annotation parsing test**: Define `Table T (ID INT, FROM_Z DATE, THRU_Z DATE) [milestoning: business(FROM_Z, THRU_Z)]`; verify the parser extracts business milestoning metadata on the table
2. **Auto-filter injection test**: Query a milestoned class with `Person.all(%2024-01-01)`. Verify the generated SQL contains `WHERE FROM_Z <= '2024-01-01' AND THRU_Z > '2024-01-01'`
3. **Traverse propagation test**: Navigate milestoned `Person` → milestoned `Firm` via join. Verify both tables get date filters injected
4. **allVersions() test**: `Person.allVersions()` should NOT inject any date filter — verify full table scan
5. **allVersionsInRange() test**: Verify the range-based filter variant
6. **Bi-temporal test**: Table with business + processing milestoning. Verify two date parameters are required and both pairs of columns get filtered

---

## Implementation Notes

| Feature | Status | What to Build |
|---------|--------|---------------|
| G Milestoning | ❌ | 1. Table-level milestoning annotation parsing (from Database definition). 2. `asOf()` function — reads metadata, injects filter predicates. 3. Traverse propagation — `asOf` context flows through traverse chains. 4. `allVersions()` / `allVersionsInRange()` — opt-out functions. 5. Bi-temporal support (two dates). |

**Priority**: Medium (Hard complexity).

**Key files to modify**:
- PackageableElementBuilder — parse milestoning annotations in Table definitions
- MappingNormalizer or a new MilestoningResolver — inject date filters
- ExtendChecker / TraverseChecker — propagate asOf context through traverse chains
- PlanGenerator — render the injected filter predicates
