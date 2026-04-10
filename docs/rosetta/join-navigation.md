# Join Navigation — Rosetta Stone

> Covers: **A2** (join chain + column), **D2** (simple join), **D3** (complex/multi-column join), **D4** (function in join), **D5** (self-join)

---

## What It Does

Joins are declared in the `###Relational` Database block and referenced in mappings via `@JoinName` syntax. They enable property mappings to reach columns across tables. Variants:

- **Simple join** — `Join J(T1.col = T2.col)` — standard equi-join
- **Complex join** — `Join J(T1.A = T2.A and T1.B = T2.B)` — multi-column condition
- **Function in join** — `Join J(concat(T1.PREFIX, '_', T1.CODE) = T2.KEY)` — DynaFunction in condition
- **Self-join** — `Join J(T.PARENT_ID = {target}.ID)` — `{target}` aliases the other side
- **Join chain** — `@J1 > @J2 | T.col` — multi-hop traversal through joins

---

## Upstream Mapping Syntax (legend-engine)

From `core_relational/relational/tests/` store and mapping definitions:

```pure
###Relational
Database db(
    Table PersonTable (ID INT PRIMARY KEY, FIRSTNAME VARCHAR(200), FIRM_ID INT, ADDR_ID INT)
    Table FirmTable (ID INT PRIMARY KEY, LEGALNAME VARCHAR(200), CEO_ID INT)
    Table AddressTable (ID INT PRIMARY KEY, NAME VARCHAR(200), TYPE INT)
    Table OrgTable (ID INT PRIMARY KEY, NAME VARCHAR(200), PARENT_ID INT)

    // D2: Simple join
    Join Firm_Person(PersonTable.FIRM_ID = FirmTable.ID)

    // D3: Complex multi-column join
    Join Firm_Person_Multi(PersonTable.FIRM_ID = FirmTable.ID and PersonTable.ADDR_ID = AddressTable.ID)

    // D4: Function in join
    Join Codes_Join(concat(CodesTable.PREFIX, '_', CodesTable.CODE) = MasterTable.KEY)

    // D5: Self-join
    Join Org_Parent(OrgTable.PARENT_ID = {target}.ID)

    // Used in join chains
    Join Person_Address(PersonTable.ADDR_ID = AddressTable.ID)
)

###Mapping
Mapping test::M
(
    *test::Person: Relational {
        firstName: [db]PersonTable.FIRSTNAME,

        // A2: Single join
        firmName: [db]@Firm_Person | FirmTable.LEGALNAME,

        // A2: Multi-hop join chain
        firmAddress: [db]@Firm_Person > @Firm_Address | AddressTable.NAME,

        // D5: Self-join (in a property mapping for Org)
        parentName: [db]@Org_Parent | OrgTable.NAME
    }
)
```

---

## Simplified Example

```
###Relational
Database db(
    Table PERSON (ID INT PK, NAME VARCHAR(200), FIRM_ID INT, ADDR_ID INT)
    Table FIRM (ID INT PK, LEGAL_NAME VARCHAR(200))
    Table ADDRESS (ID INT PK, STREET VARCHAR(200), CITY VARCHAR(200))
    Table ORG (ID INT PK, NAME VARCHAR(200), PARENT_ID INT)

    Join PersonFirm(PERSON.FIRM_ID = FIRM.ID)
    Join PersonAddr(PERSON.ADDR_ID = ADDRESS.ID)
    Join OrgParent(ORG.PARENT_ID = {target}.ID)
)

###Mapping
Mapping test::M
(
    *test::Person: Relational {
        name:       [db]PERSON.NAME,
        firmName:   [db]@PersonFirm | FIRM.LEGAL_NAME,           // single hop
        city:       [db]@PersonAddr | ADDRESS.CITY,              // single hop
        parentOrg:  [db]@OrgParent | ORG.NAME                   // self-join
    }
)
```

---

## Relation API Equivalent

### ✅ Already Implemented

All join variants work end-to-end via `traverse()`.

```pure
function model::person(): Relation<(name:String, firmName:String, city:String)>[1] {
    #>{db.PERSON}#->extend(~[
        name: r|$r.NAME,

        // A2: Single-hop join → traverse
        firmName: r|$r->traverse(#>{db.FIRM}#, {a,b|$a.FIRM_ID == $b.ID}).LEGAL_NAME,

        // A2: Another single-hop
        city: r|$r->traverse(#>{db.ADDRESS}#, {a,b|$a.ADDR_ID == $b.ID}).CITY
    ])
}

// D3: Multi-column join condition
function model::withMultiColJoin(): Relation<(...)>[1] {
    #>{db.T1}#->extend(~[
        val: r|$r->traverse(#>{db.T2}#, {a,b|$a.COL_A == $b.COL_A && $a.COL_B == $b.COL_B}).VALUE
    ])
}

// D4: Function in join condition
function model::withFuncJoin(): Relation<(...)>[1] {
    #>{db.CODES}#->extend(~[
        master: r|$r->traverse(#>{db.MASTER}#,
            {a,b| ($a.PREFIX + '_' + $a.CODE) == $b.KEY}).VALUE
    ])
}

// D5: Self-join
function model::org(): Relation<(name:String, parentName:String)>[1] {
    #>{db.ORG}#->extend(~[
        name: r|$r.NAME,
        parentName: r|$r->traverse(#>{db.ORG}#, {a,b|$a.PARENT_ID == $b.ID}).NAME
    ])
}

// A2: Multi-hop join chain → chained traverse
function model::personWithFirmAddr(): Relation<(name:String, firmCity:String)>[1] {
    #>{db.PERSON}#->extend(~[
        name: r|$r.NAME,
        firmCity: r|$r
            ->traverse(#>{db.FIRM}#, {a,b|$a.FIRM_ID == $b.ID})
            ->traverse(#>{db.ADDRESS}#, {a,b|$a.ADDR_ID == $b.ID})
            .CITY
    ])
}
```

**Generated SQL** (single-hop):
```sql
SELECT "root".NAME AS "name",
       "firm_0".LEGAL_NAME AS "firmName"
FROM PERSON AS "root"
LEFT OUTER JOIN FIRM AS "firm_0" ON ("root".FIRM_ID = "firm_0".ID)
```

### Key files in legend-lite

- **ExtendChecker.java** — `compileTraverseClause` parses traverse chains
- **PlanGenerator.java** — generates flat LEFT JOINs from TraversalSpec
- **MappingNormalizer.java** — converts `@JoinName` references to `traverse()` nodes

### Tests in legend-lite

| Test | File | What it covers |
|------|------|----------------|
| `projectDept` | [RelationalMappingCompositionTest.java:196](../../engine/src/test/java/com/gs/legend/test/RelationalMappingCompositionTest.java) | D2: single-hop join, join pruning |
| `projectOrg` | [RelationalMappingCompositionTest.java:272](../../engine/src/test/java/com/gs/legend/test/RelationalMappingCompositionTest.java) | A2: multi-hop 2-join chain |
| `projectBothChains` | [RelationalMappingCompositionTest.java:343](../../engine/src/test/java/com/gs/legend/test/RelationalMappingCompositionTest.java) | Mixed chain selective activation |
| `testJoinChainPropertyMappingFilter` | [RelationalMappingIntegrationTest.java:2037](../../engine/src/test/java/com/gs/legend/test/RelationalMappingIntegrationTest.java) | A2: join chain + filter |
| `testMultiColumnJoin` | [RelationalMappingIntegrationTest.java:2281](../../engine/src/test/java/com/gs/legend/test/RelationalMappingIntegrationTest.java) | D3: multi-column join condition |
| `testSelfJoin` | [RelationalMappingIntegrationTest.java:2318](../../engine/src/test/java/com/gs/legend/test/RelationalMappingIntegrationTest.java) | D5: self-join with `{target}` |
| `testSimpleEquiJoin` | [MappingDefinitionExtractionTest.java:85](../../engine/src/test/java/com/gs/legend/test/MappingDefinitionExtractionTest.java) | D2: parser extraction |
| `testMultiColumnJoin` | [MappingDefinitionExtractionTest.java:118](../../engine/src/test/java/com/gs/legend/test/MappingDefinitionExtractionTest.java) | D3: parser extraction |
| `testFunctionJoin` | [MappingDefinitionExtractionTest.java:157](../../engine/src/test/java/com/gs/legend/test/MappingDefinitionExtractionTest.java) | D4: function in join parser |
| `testSelfJoin` | [MappingDefinitionExtractionTest.java:194](../../engine/src/test/java/com/gs/legend/test/MappingDefinitionExtractionTest.java) | D5: self-join parser |

---

## Implementation Notes

**Status: ✅ All implemented.**

The key difference: Legend Mapping declares joins once in the Database block and references them by name (`@PersonFirm`). The Relation API inlines the join predicate at each use site via `traverse()`. This is more verbose but self-contained — you can read the Relation expression without cross-referencing the Database definition.

Multi-hop chains (`@J1 > @J2 | T.col`) become chained `traverse()` calls, which the PlanGenerator flattens to a single SQL query with multiple LEFT JOINs.
