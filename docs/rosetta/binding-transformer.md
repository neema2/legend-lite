# Binding Transformer — Rosetta Stone

> Covers: **A9** (binding transformer)

---

## What It Does

Binding transformers deserialize semi-structured data (JSON, XML) stored in database columns into typed Pure objects. The `Binding` keyword in a property mapping tells the engine to parse the column value using a specific format binding.

Use cases:
- JSON columns in relational databases
- XML payloads stored as strings
- Any column that stores serialized data needing deserialization

---

## Upstream Mapping Syntax (legend-engine)

```pure
###Pure
Class test::Person
{
    name: String[1];
    address: test::Address[0..1];
}

Class test::Address
{
    street: String[1];
    city: String[1];
}

###ExternalFormat
Binding test::JsonBinding
    contentType: 'application/json';
    modelIncludes: [test::Address];

###Relational
Database db(
    Table PersonTable (
        ID INT PRIMARY KEY,
        NAME VARCHAR(200),
        ADDRESS_JSON VARCHAR(4000)   // JSON column
    )
)

###Mapping
Mapping test::M
(
    *test::Person: Relational {
        name: [db]PersonTable.NAME,
        // A9: Binding transformer — deserialize JSON column
        address: Binding test::JsonBinding: [db]PersonTable.ADDRESS_JSON
    }
)
```

The engine reads the `ADDRESS_JSON` column, parses it as JSON using the `JsonBinding` schema, and populates the `Address` object.

---

## Simplified Example

```
###Pure
Class test::Trade { id: Integer[1]; metadata: test::TradeMetadata[0..1]; }
Class test::TradeMetadata { source: String[1]; priority: Integer[1]; }

###ExternalFormat
Binding test::MetadataBinding
    contentType: 'application/json';
    modelIncludes: [test::TradeMetadata];

###Relational
Database db(Table TRADE (ID INT PK, METADATA_JSON VARCHAR(4000)))

###Mapping
Mapping test::M
(
    *test::Trade: Relational {
        id: [db]TRADE.ID,
        metadata: Binding test::MetadataBinding: [db]TRADE.METADATA_JSON
    }
)
```

---

## Relation API Equivalent

### ❌ Not Yet Implemented

### Proposed: `toVariant()` + variant access

The Relation API already has `toVariant()` (casts to JSON/Variant type) and variant access functions (`->get()`, `->to()`).

```pure
function model::trade(): Relation<(id:Integer, source:String, priority:Integer)>[1] {
    #>{db.TRADE}#->extend(~[
        id: r|$r.ID,

        // Parse JSON column → variant, then extract fields
        _meta: r|$r.METADATA_JSON->toVariant(),
        source: r|$r.METADATA_JSON->toVariant()->get('source')->to(@String),
        priority: r|$r.METADATA_JSON->toVariant()->get('priority')->to(@Integer)
    ])
}
```

**Generated SQL** (DuckDB):
```sql
SELECT "root".ID AS "id",
       CAST("root".METADATA_JSON AS JSON)->>'source' AS "source",
       CAST(CAST("root".METADATA_JSON AS JSON)->>'priority' AS INTEGER) AS "priority"
FROM TRADE AS "root"
```

### For nested objects:

```pure
function model::personWithAddress(): Relation<(name:String, street:String, city:String)>[1] {
    #>{db.PersonTable}#->extend(~[
        name: r|$r.NAME,
        street: r|$r.ADDRESS_JSON->toVariant()->get('street')->to(@String),
        city: r|$r.ADDRESS_JSON->toVariant()->get('city')->to(@String)
    ])
}
```

### Key difference from Binding

The mapping `Binding` approach:
1. Parses the entire JSON into a typed Pure object
2. Properties of that object are accessible via normal property access
3. Requires an `###ExternalFormat` Binding definition

The Relation API approach:
1. Treats JSON as a variant (semi-structured type)
2. Extracts individual fields with `->get('field')`
3. No external format definition needed
4. More explicit but doesn't produce typed objects

### Tests in legend-lite

**Not implemented (❌) — no Binding-specific tests.** However, related variant infrastructure exists:

| Test | File | What it covers |
|------|------|----------------|
| (variant tests) | [VariantIntegrationTest.java](../../engine/src/test/java/com/gs/legend/test/VariantIntegrationTest.java) | `toVariant()` / variant access for JSON columns |
| (DuckDB variant) | [DuckDBVariantLoadTest.java](../../engine/src/test/java/com/gs/legend/test/DuckDBVariantLoadTest.java) | Variant column loading from JSON |

**Test strategy for A9 (Binding transformer):**

*Approach (a) — variant access (recommended):*
1. Define table with JSON column: `Table T (ID INT, PAYLOAD VARCHAR)`
2. Map property via variant: `name: toVariant([db]T.PAYLOAD)->get('name')->toString()`
3. Verify SQL generates `CAST(PAYLOAD::JSON->>'name' AS VARCHAR)` (dialect-dependent)
4. Test nested access: `address.city` → `->get('address')->get('city')`
5. Test array access: `tags[0]` → `->get('tags')->get(0)`

*Approach (b) — full Binding (deferred):*
1. Define `###ExternalFormat` Binding for JSON schema
2. Map class via `Binding my::JsonBinding: [db]T.PAYLOAD`
3. Verify deserialization produces typed Pure objects
4. Requires ExternalFormat grammar + deserialization infrastructure

---

## Implementation Notes

| Feature | Status | What to Build |
|---------|--------|---------------|
| A9 Binding transformer | ❌ | Two approaches: (a) `toVariant()` + `get()` + `to()` for field-by-field extraction (partially available), or (b) full Binding support with ExternalFormat parsing + object hydration (much harder). |

**Priority**: Medium.

**Approach (a)** — variant access — is mostly available. Gaps:
- Verify `toVariant()` handles string→JSON parsing (vs just type casting)
- Nested variant access for deep JSON structures
- Array access for JSON arrays

**Approach (b)** — full Binding — requires:
- ExternalFormat grammar and model
- Deserialization infrastructure
- Post-processing layer to hydrate objects from JSON

For the Relation API, approach (a) is preferred — it's more composable and doesn't require object hydration.
