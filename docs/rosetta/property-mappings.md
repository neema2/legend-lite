# Property Mappings — Rosetta Stone

> Covers: **A1** (simple column), **A3** (DynaFunction), **A4** (DynaFunction + join)

---

## What It Does

Property mappings are the core building block of Legend mappings. They declare how each class property maps to a column (or expression) in the database. Three variants:

- **Simple column** — direct column reference: `prop: [db]Table.column`
- **DynaFunction** — SQL expression transform: `prop: concat([db]T.first, ' ', [db]T.last)`
- **DynaFunction + join** — transform on a joined column: `prop: upper(@PersonFirm | FirmTable.name)`

---

## Upstream Mapping Syntax (legend-engine)

From `core_relational/relational/tests/simpleRelationalMapping.pure`:

```pure
###Mapping
Mapping meta::relational::tests::simpleRelationalMapping
(
    meta::relational::tests::model::simple::Person: Relational
    {
        scope([db]default.personTable)
        (
            firstName: firstName,
            lastName: lastName,
            age: age
        ),
        firm: [db]@Firm_Person
    }

    meta::relational::tests::model::simple::Firm: Relational
    {
        legalName: [db]default.firmTable.legalName,
        employees: [db]@Firm_Person
    }
)
```

DynaFunction example (from various upstream tests):

```pure
Person: Relational
{
    // Simple column
    firstName: [db]PersonTable.FIRSTNAME,

    // DynaFunction — concat two columns
    fullName: concat([db]PersonTable.FIRSTNAME, ' ', [db]PersonTable.LASTNAME),

    // DynaFunction — year extract
    yearOfBirth: year([db]PersonTable.BIRTH_DATE),

    // DynaFunction + join — transform on joined column
    firmNameUpper: upper([db]@PersonFirm | FirmTable.LEGALNAME)
}
```

---

## Simplified Example

```
###Relational
Database db(
    Table PersonTable (ID INT PRIMARY KEY, FIRSTNAME VARCHAR(200), LASTNAME VARCHAR(200), AGE INT, BIRTH_DATE DATE, FIRM_ID INT)
    Table FirmTable (ID INT PRIMARY KEY, LEGALNAME VARCHAR(200))
    Join PersonFirm(PersonTable.FIRM_ID = FirmTable.ID)
)

###Mapping
Mapping test::PersonMapping
(
    *test::Person: Relational {
        firstName: [db]PersonTable.FIRSTNAME,
        lastName: [db]PersonTable.LASTNAME,
        fullName: concat([db]PersonTable.FIRSTNAME, ' ', [db]PersonTable.LASTNAME),
        yearOfBirth: year([db]PersonTable.BIRTH_DATE),
        firmName: [db]@PersonFirm | FirmTable.LEGALNAME
    }
)
```

---

## Relation API Equivalent

### ✅ Already Implemented

All three variants work end-to-end in legend-lite.

```pure
// Simple column → extend with column reference
// DynaFunction → extend with expression
// DynaFunction + join → extend with traverse + expression

function model::person(): Relation<(firstName:String, lastName:String,
    fullName:String, yearOfBirth:Integer, firmName:String)>[1] {

    #>{db.PersonTable}#
      ->extend(~[
          // A1: Simple column mapping
          firstName: r|$r.FIRSTNAME,
          lastName: r|$r.LASTNAME,

          // A3: DynaFunction — concat
          fullName: r|$r.FIRSTNAME + ' ' + $r.LASTNAME,

          // A3: DynaFunction — year extract
          yearOfBirth: r|$r.BIRTH_DATE->year(),

          // A4: DynaFunction + join — property from joined table
          firmName: r|$r->traverse(#>{db.FirmTable}#, {a,b|$a.FIRM_ID == $b.ID}).LEGALNAME
      ])
}
```

**Generated SQL** (for `model::person()->select(~[firstName, fullName, firmName])`):
```sql
SELECT "root".FIRSTNAME AS "firstName",
       CONCAT("root".FIRSTNAME, ' ', "root".LASTNAME) AS "fullName",
       "firmtable_0".LEGALNAME AS "firmName"
FROM PersonTable AS "root"
LEFT OUTER JOIN FirmTable AS "firmtable_0" ON ("root".FIRM_ID = "firmtable_0".ID)
```

### Key files in legend-lite

- **MappingNormalizer.java** — converts property mappings to `extend()` nodes
- **MappingResolver.java** — resolves DynaFunction → `DynaFunction` PropertyResolution
- **PlanGenerator.java** — `resolveColumnExpr` handles Column, DynaFunction, and JoinResolution cases

### Tests in legend-lite

| Test | File | What it covers |
|------|------|----------------|
| `testStringColumn` | [RelationalMappingIntegrationTest.java:112](../../engine/src/test/java/com/gs/legend/test/RelationalMappingIntegrationTest.java) | A1: simple column |
| `testMultipleColumns` | [RelationalMappingIntegrationTest.java:180](../../engine/src/test/java/com/gs/legend/test/RelationalMappingIntegrationTest.java) | A1: multi-column same table |
| `testColumnNameDifferentFromProperty` | [RelationalMappingIntegrationTest.java:194](../../engine/src/test/java/com/gs/legend/test/RelationalMappingIntegrationTest.java) | A1: rename column→property |
| `testConcat` | [DynaFunctionIntegrationTest.java:99](../../engine/src/test/java/com/gs/legend/test/DynaFunctionIntegrationTest.java) | A3: concat DynaFunction |
| `testToLower` | [DynaFunctionIntegrationTest.java:118](../../engine/src/test/java/com/gs/legend/test/DynaFunctionIntegrationTest.java) | A3: toLower DynaFunction |
| `testSubstring` | [DynaFunctionIntegrationTest.java:172](../../engine/src/test/java/com/gs/legend/test/DynaFunctionIntegrationTest.java) | A3: substring DynaFunction |
| `testIfEqual` | [DynaFunctionIntegrationTest.java:492](../../engine/src/test/java/com/gs/legend/test/DynaFunctionIntegrationTest.java) | A3: conditional DynaFunction |
| `testPlus` | [DynaFunctionIntegrationTest.java:376](../../engine/src/test/java/com/gs/legend/test/DynaFunctionIntegrationTest.java) | A3: arithmetic DynaFunction |

See also: [MappingDefinitionExtractionTest.java](../../engine/src/test/java/com/gs/legend/test/MappingDefinitionExtractionTest.java) — parser-level extraction for simple columns and DynaFunctions.

---

## Implementation Notes

**Status: ✅ All implemented.** No gaps.

The mapping DSL is strictly more concise — `firstName: [db]T.FIRSTNAME` vs `firstName: r|$r.FIRSTNAME` — but the Relation form is more explicit about what's happening and composes naturally with `filter()`, `groupBy()`, etc.
