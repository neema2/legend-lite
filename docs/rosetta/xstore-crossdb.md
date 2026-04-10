# XStore, Source/Target IDs & Cross-DB — Rosetta Stone

> Covers: **E3** (xStore), **A10** (source/target IDs), **A11** (cross-DB reference)

---

## What It Does

- **XStore** — cross-store association mapping. Joins data across different physical stores (e.g., relational DB + service store, or two different databases). Uses `$this`/`$that` predicate syntax.
- **Source/Target IDs** — `prop[sourceId, targetId]: @Join` — disambiguates which class mappings are connected when multiple exist for the same class.
- **Cross-DB reference** — `[otherDb]Table.col` — references a column from a different database within the same mapping.

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

## Simplified Example

```
###Relational
Database db1(Table PERSON (ID INT PK, NAME VARCHAR(200), FIRM_ID INT))
Database db2(Table FIRM (ID INT PK, LEGAL_NAME VARCHAR(200)))

###Mapping
Mapping test::M
(
    test::Person: Relational { name: [db1]PERSON.NAME }
    test::Firm: Relational { legalName: [db2]FIRM.LEGAL_NAME }

    // XStore: cross-database join
    test::PersonFirm: XStore {
        firm[person, firm]: $this.firmId == $that.id
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

### Tests in legend-lite

**Not implemented (❌) — no existing tests.**

See also: [MappingDefinitionExtractionTest.java:851](../../engine/src/test/java/com/gs/legend/test/MappingDefinitionExtractionTest.java) — parser extraction of `prop[srcId, tgtId]` syntax.

**Test strategy for E3 (XStore):**
1. **Grammar test**: Parse `XStore` association mapping with `$this.id == $that.personId`; verify AST extraction
2. **Plan generation test**: Verify executor detects two different stores and creates a federated plan (two separate SQL queries + in-memory join)
3. **Integration test**: Two DuckDB databases. Query Person→ExternalFirm. Verify correct results returned after federation

**Test strategy for A10 (Source/Target IDs):**
1. Define property mapping `employees[firmSet, empSet]: @FirmPerson` with explicit source/target IDs
2. Verify builder resolves `firmSet` and `empSet` to the correct class mappings
3. Verify the join is wired correctly in both directions

**Test strategy for A11 (Cross-DB refs):**
- Same infrastructure as XStore. Once E3 federation works, A11 is just `[otherDb]T.col` resolving to a different store in the traversal.

---

## Implementation Notes

| Feature | Status | What to Build |
|---------|--------|---------------|
| E3 XStore | ❌🔧 | 1. Grammar: `XStore` keyword. 2. Executor federation layer: detect cross-store traversals, execute each store separately, join in-memory. |
| A10 Source/Target IDs | ❌ | Named functions solve this naturally. For mapping DSL: builder must wire source/target IDs to resolve ambiguous class mapping references. |
| A11 Cross-DB | ❌ | No new syntax — executor must detect different stores from `#>{store}#` and federate. Same infrastructure as XStore. |

**Priority**: E3/A11 both need executor federation (Hard — infrastructure). A10 is Low (named functions solve it in Relation API).
