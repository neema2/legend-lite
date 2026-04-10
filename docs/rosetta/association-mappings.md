# Association Mappings — Rosetta Stone

> Covers: **C3** (association mapping)

---

## What It Does

Association mappings define how relationships between classes are navigated at the store level. They declare which join connects two class mappings, enabling property navigation like `$person.firm` to generate the correct SQL JOIN.

Two sides are mapped — each direction of the association references the same join, with source/target IDs to disambiguate which class mappings are connected.

---

## Upstream Mapping Syntax (legend-engine)

```pure
###Pure
Association test::PersonFirm
{
    employees: test::Person[*];
    firm: test::Firm[0..1];
}

###Relational
Database db(
    Table PersonTable (ID INT PK, NAME VARCHAR(200), FIRM_ID INT)
    Table FirmTable (ID INT PK, LEGAL_NAME VARCHAR(200))
    Join PersonFirm(PersonTable.FIRM_ID = FirmTable.ID)
)

###Mapping
Mapping test::M
(
    *test::Person[person]: Relational {
        name: [db]PersonTable.NAME
    }

    *test::Firm[firm]: Relational {
        legalName: [db]FirmTable.LEGAL_NAME
    }

    // C3: Association mapping
    test::PersonFirm: Relational {
        AssociationMapping (
            firm[person, firm]: [db]@PersonFirm,
            employees[firm, person]: [db]@PersonFirm
        )
    }
)
```

The `[person, firm]` syntax specifies `[sourceSetId, targetSetId]` — which class mapping the join connects FROM and TO.

---

## Simplified Example

```
###Pure
Class test::Person { name: String[1]; }
Class test::Firm { legalName: String[1]; }
Association test::PersonFirm { employees: test::Person[*]; firm: test::Firm[0..1]; }

###Relational
Database db(
    Table PERSON (ID INT PK, NAME VARCHAR(200), FIRM_ID INT)
    Table FIRM (ID INT PK, LEGAL_NAME VARCHAR(200))
    Join PersonFirm(PERSON.FIRM_ID = FIRM.ID)
)

###Mapping
Mapping test::M
(
    *test::Person: Relational { name: [db]PERSON.NAME }
    *test::Firm: Relational { legalName: [db]FIRM.LEGAL_NAME }
    test::PersonFirm: Relational {
        AssociationMapping (
            firm: [db]@PersonFirm,
            employees: [db]@PersonFirm
        )
    }
)
```

---

## Relation API Equivalent

### ✅ Already Implemented

Association mappings become explicit `traverse()` calls in `extend()`:

```pure
function model::person(): Relation<(name:String, firmName:String)>[1] {
    #>{db.PERSON}#->extend(~[
        name: r|$r.NAME,
        // Association → explicit traverse with join predicate
        firmName: r|$r->traverse(#>{db.FIRM}#, {a,b|$a.FIRM_ID == $b.ID}).LEGAL_NAME
    ])
}

function model::firm(): Relation<(legalName:String)>[1] {
    #>{db.FIRM}#->extend(~[
        legalName: r|$r.LEGAL_NAME
        // To get employees, reverse the traverse:
        // employees would be a to-many — handled via graphFetch or separate query
    ])
}
```

**Generated SQL**:
```sql
SELECT "root".NAME AS "name",
       "firm_0".LEGAL_NAME AS "firmName"
FROM PERSON AS "root"
LEFT OUTER JOIN FIRM AS "firm_0" ON ("root".FIRM_ID = "firm_0".ID)
```

The key difference: in Legend Mapping, the association is declared once and the engine resolves `$person.firm` → join. In Relation API, the join is explicit at each use site via `traverse()`.

### Key files in legend-lite

- **MappingNormalizer.java** — `addAssociationExtends()` converts association refs to traverse nodes
- **MappingResolver.java** — resolves association joins to `JoinResolution`
- **PlanGenerator.java** — generates LEFT JOIN from traverse

### Tests in legend-lite

| Test | File | What it covers |
|------|------|----------------|
| `testProjectToOne` | [AssociationIntegrationTest.java:214](../../engine/src/test/java/com/gs/legend/test/AssociationIntegrationTest.java) | C3: Person→Firm to-one projection |
| `testFilterToOne` | [AssociationIntegrationTest.java:227](../../engine/src/test/java/com/gs/legend/test/AssociationIntegrationTest.java) | C3: Filter on associated class |
| `testMultipleToOne` | [AssociationIntegrationTest.java:256](../../engine/src/test/java/com/gs/legend/test/AssociationIntegrationTest.java) | C3: Two independent to-one associations |
| `testProjectToMany` | [AssociationIntegrationTest.java:288](../../engine/src/test/java/com/gs/legend/test/AssociationIntegrationTest.java) | C3: To-many LEFT JOIN row expansion |
| `testFilterToManyExists` | [AssociationIntegrationTest.java:308](../../engine/src/test/java/com/gs/legend/test/AssociationIntegrationTest.java) | C3: To-many filter produces EXISTS |
| `testTwoHopToOne` | [AssociationIntegrationTest.java:452](../../engine/src/test/java/com/gs/legend/test/AssociationIntegrationTest.java) | C3: Person→Dept→Org two-hop |
| `testToOneAndToMany` | [AssociationIntegrationTest.java:513](../../engine/src/test/java/com/gs/legend/test/AssociationIntegrationTest.java) | C3: to-one + to-many in same query |
| `testAssociationMappingExtraction` | [MappingDefinitionExtractionTest.java:882](../../engine/src/test/java/com/gs/legend/test/MappingDefinitionExtractionTest.java) | C3: parser extraction |

---

## Implementation Notes

**Status: ✅ Implemented.**

The Relation API makes joins explicit, which is more verbose but clearer about what SQL will be generated. The mapping DSL's implicit join resolution via `$person.firm` is ergonomic sugar that compiles to the same underlying join.
