# Enumeration Mappings — Rosetta Stone

> Covers: **A5** (enum transformer on property), **C4** (enumeration mapping definition)

---

## What It Does

Enumeration mappings define how raw database values (strings, integers) map to Pure enum members. Two parts:

1. **EnumerationMapping definition** (C4) — declares the mapping from source values to enum members
2. **Enum transformer on property** (A5) — references an EnumerationMapping when mapping a property

Supports single-value mapping (`'FTC'` → `CONTRACT`) and multi-value collapse (`['P', 'PEND', 'PENDING']` → `PENDING`). Source values can be strings or integers.

---

## Upstream Mapping Syntax (legend-engine)

From `core_relational/relational/tests/mapping/enumeration/`:

```pure
###Pure
Enum test::EmployeeType
{
    CONTRACT,
    FULL_TIME
}

Enum test::YesNo
{
    YES,
    NO
}

###Relational
Database test::myDB
(
    Table employeeTable (
        id INT PRIMARY KEY,
        name VARCHAR(200),
        type VARCHAR(10),
        active INT
    )
)

###Mapping
Mapping test::employeeTestMapping
(
    // C4: Enumeration mapping definition — string source
    test::EmployeeType: EnumerationMapping
    {
        CONTRACT: ['FTC'],
        FULL_TIME: ['FTE']
    }

    // C4: Enumeration mapping definition — integer source
    test::YesNo: EnumerationMapping
    {
        YES: [1],
        NO: [0]
    }

    // A5: Property using enum transformer
    *test::Employee: Relational {
        name: [db]employeeTable.name,
        type: EnumerationMapping test::EmployeeType: [db]employeeTable.type,
        active: EnumerationMapping test::YesNo: [db]employeeTable.active
    }
)
```

Multi-value collapse example:

```pure
test::TradeStatus: EnumerationMapping
{
    PENDING:   ['P', 'PEND', 'PENDING'],
    CONFIRMED: ['C', 'CONF'],
    SETTLED:   'S',
    CANCELLED: ['X', 'CANC', 'CANCEL']
}
```

---

## Simplified Example

```
###Pure
Enum test::Status { ACTIVE, INACTIVE }

###Relational
Database db(Table PERSON (ID INT PK, NAME VARCHAR(200), STATUS_CODE VARCHAR(5)))

###Mapping
Mapping test::M
(
    test::Status: EnumerationMapping { ACTIVE: ['A', 'ACT'], INACTIVE: ['I', 'INA'] }

    *test::Person: Relational {
        name: [db]PERSON.NAME,
        status: EnumerationMapping test::Status: [db]PERSON.STATUS_CODE
    }
)
```

---

## Relation API Equivalent

### ✅ Already Implemented

Enum mappings translate to `if()/case()` chains in `extend()`.

```pure
function model::person(): Relation<(name:String, status:String)>[1] {
    #>{db.PERSON}#->extend(~[
        name: r|$r.NAME,

        // Enum transformer → case/if chain
        status: r|if($r.STATUS_CODE->in(['A', 'ACT']),
                     |'ACTIVE',
                     |if($r.STATUS_CODE->in(['I', 'INA']),
                         |'INACTIVE',
                         |''))
    ])
}
```

For simple single-value enums:

```pure
// EnumerationMapping { CONTRACT: 'FTC', FULL_TIME: 'FTE' }
// becomes:
type: r|if($r.TYPE == 'FTC', |'CONTRACT', |if($r.TYPE == 'FTE', |'FULL_TIME', |''))
```

For integer enums:

```pure
// EnumerationMapping { YES: 1, NO: 0 }
// becomes:
active: r|if($r.ACTIVE == 1, |'YES', |if($r.ACTIVE == 0, |'NO', |''))
```

**Generated SQL**:
```sql
SELECT "root".NAME AS "name",
       CASE WHEN "root".STATUS_CODE IN ('A', 'ACT') THEN 'ACTIVE'
            WHEN "root".STATUS_CODE IN ('I', 'INA') THEN 'INACTIVE'
            ELSE '' END AS "status"
FROM PERSON AS "root"
```

### Key files in legend-lite

- **MappingNormalizer.java** — converts EnumerationMapping references to case/if expressions
- **PureModelBuilder.java** — parses EnumerationMapping definitions
- **EnumIntegrationTest.java** — comprehensive enum mapping tests

### Tests in legend-lite

| Test | File | What it covers |
|------|------|----------------|
| `testEnumParsedAndStored` | [EnumIntegrationTest.java:52](../../engine/src/test/java/com/gs/legend/test/EnumIntegrationTest.java) | C4: Enum parsed into registry |
| `testMultipleEnums` | [EnumIntegrationTest.java:103](../../engine/src/test/java/com/gs/legend/test/EnumIntegrationTest.java) | C4: Multiple enums in one source |
| `testCompleteModelWithEnum` | [EnumIntegrationTest.java:179](../../engine/src/test/java/com/gs/legend/test/EnumIntegrationTest.java) | C4: Full model with enum + class + DB |
| `testQueryWithEnumFilter` | [EnumIntegrationTest.java:364](../../engine/src/test/java/com/gs/legend/test/EnumIntegrationTest.java) | A5: Filter on enum-mapped column |
| `testQueryWithEnumInClause` | [EnumIntegrationTest.java:446](../../engine/src/test/java/com/gs/legend/test/EnumIntegrationTest.java) | A5: IN clause with enum values |
| `testQueryProjectingEnumColumn` | [EnumIntegrationTest.java:521](../../engine/src/test/java/com/gs/legend/test/EnumIntegrationTest.java) | A5: Project enum-transformed column |
| `testEnumerationMappingTranslation` | [EnumIntegrationTest.java:607](../../engine/src/test/java/com/gs/legend/test/EnumIntegrationTest.java) | A5: DB codes → enum values E2E |
| `testEnumerationMappingMultipleSources` | [EnumIntegrationTest.java:697](../../engine/src/test/java/com/gs/legend/test/EnumIntegrationTest.java) | A5: Multi-value collapse |

See also: [MappingDefinitionExtractionTest.java:613](../../engine/src/test/java/com/gs/legend/test/MappingDefinitionExtractionTest.java) — parser extraction for enum transformer.

---

## Implementation Notes

**Status: ✅ All implemented.**

The Relation API doesn't have a first-class `EnumerationMapping` concept — it's desugared to `if()`/`case()` chains. This is more verbose but fully equivalent. The mapping DSL's enum transformer is pure syntactic sugar over conditional expressions.
