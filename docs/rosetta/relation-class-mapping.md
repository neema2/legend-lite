# Relation Class Mapping — Rosetta Stone

> Covers: **E5** (Relation keyword mapping)

---

## What It Does

The `Relation` class mapping type maps a class to a **Pure function returning `Relation<Any>[1]`** rather than directly to a database table. This is a newer addition to Legend, distinct from the traditional `Relational` keyword.

It represents Legend's own movement toward the Relation API — the data source is a Relation-producing function, and property mappings are just column-name-only references.

---

## Upstream Mapping Syntax (legend-engine)

```pure
###Pure
function my::personRelation(): Relation<(FIRSTNAME:String, AGE:Integer, FIRM_NAME:String)>[1]
{
    #>{my::db.PersonTable}#
      ->filter(r | $r.active == true)
      ->project(~[FIRSTNAME: r | $r.first_name, AGE: r | $r.age])
      ->join(
          #>{my::db.FirmTable}#->project(~[FIRM_NAME: r | $r.legal_name, FIRM_ID: r | $r.id]),
          JoinType.LEFT_OUTER,
          {a, b | $a.firm_id == $b.FIRM_ID}
      )
}

###Mapping
Mapping my::PersonMapping
(
    // E5: Relation class mapping
    *Person[person]: Relation
    {
        ~func my::personRelation__Relation_1_   // zero-arg function returning Relation
        firstName : FIRSTNAME,                   // property : COLUMN_NAME
        age       : AGE,
        firmName  : FIRM_NAME
    }
)
```

---

## Simplified Example

```
###Pure
function test::tradeRelation(): Relation<(TRADE_ID:Integer, PRODUCT:String, QTY:Integer)>[1]
{
    #>{db.TradeTable}#
      ->filter(r | $r.STATUS == 'ACTIVE')
      ->project(~[TRADE_ID: r | $r.ID, PRODUCT: r | $r.PRODUCT, QTY: r | $r.QUANTITY])
}

###Mapping
Mapping test::TradeMapping
(
    *test::Trade: Relation
    {
        ~func test::tradeRelation__Relation_1_
        id       : TRADE_ID,
        product  : PRODUCT,
        quantity : QTY
    }
)
```

---

## Relation API Equivalent

### ❌ Not Yet Implemented (grammar gap)

The Relation class mapping IS essentially the Relation API. The `~func` body is already a Relation expression. The mapping adds a thin layer: mapping class properties to column names.

```pure
// The ~func IS the Relation API expression:
function model::trade(): Relation<(id:Integer, product:String, quantity:Integer)>[1] {
    #>{db.TradeTable}#
      ->filter(r|$r.STATUS == 'ACTIVE')
      ->extend(~[
          id: r|$r.ID,
          product: r|$r.PRODUCT,
          quantity: r|$r.QUANTITY
      ])
}

// The Relation class mapping just adds property→column renaming:
// id → TRADE_ID, product → PRODUCT, quantity → QTY
// Which is equivalent to:
function model::tradeWithRename(): Relation<(TRADE_ID:Integer, PRODUCT:String, QTY:Integer)>[1] {
    model::trade()
      ->rename(~id, ~TRADE_ID)
      ->rename(~product, ~PRODUCT)
      ->rename(~quantity, ~QTY)
}
```

**Key constraints of Relation class mapping:**
- `~func` is required, must be first line, must reference a named zero-arg function
- Property mappings are **column-name-only** — no DynaFunction expressions (transforms go in `~func` body)
- Only primitive properties supported (no joins, no embedded, no complex navigation)
- The `~func` body can contain any Relation-producing expression

### Comparison table

| | `Relational` | `Relation` | Relation API (no mapping) |
|---|---|---|---|
| Data source | `###Relational` Database table | Function returning `Relation<Any>[1]` | `#>{db.T}#` directly |
| Property mapping | Column expr + DynaFunctions | Bare column name only | `extend(~prop: r\|...)` |
| Joins | Via `@JoinName` | Not supported — go in `~func` | `traverse()` |
| Nested properties | Embedded, Otherwise, Inline | Not supported | 0-param lambda pattern |

### Tests in legend-lite

**Not implemented (❌) — no existing tests.**

**Test strategy for E5 (Relation class mapping):**
1. **Grammar test**: Parse `Class: Relation { ~func model::myFunc, prop1: col1, prop2: col2 }`; verify AST extraction of function reference and column-to-property mappings
2. **Function resolution test**: Define a Pure function `function model::emp(): Relation<(ID:Integer, NAME:String)>[1] { #>{db.EMP}# }`. Create Relation mapping pointing to it. Verify the builder resolves the function and extracts return type columns
3. **Rename chain test**: Verify property mapping `empName: NAME` generates `->rename(~NAME, ~empName)` in the normalized output
4. **E2E test**: Full query through Relation class mapping → verify SQL and correct data

---

## Implementation Notes

| Feature | Status | What to Build |
|---------|--------|---------------|
| E5 Relation class mapping | ❌🔧 | 1. Grammar: `Relation` keyword + `~func` directive. 2. Builder: resolve function reference, extract column names. 3. MappingNormalizer: emit rename chain from property→column mappings. |

**Priority**: Medium🔧.

**Key insight**: The Relation class mapping validates our thesis — Legend is already building the bridge between Mappings and Relations. The mapping layer adds property naming + class typing on top of the underlying Relation expression. In a pure Relation API world, you'd just use the function directly.
