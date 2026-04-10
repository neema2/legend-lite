# Identity & Extends — Rosetta Stone

> Covers: **B7** (root marker), **B8** (set ID / mapping ID), **B9** (extends)

---

## What It Does

These features control class mapping identity and inheritance:

- **Root marker** (`*`) — marks a mapping as the default/root for a class. When you query `Person.all()`, the root mapping is used.
- **Mapping ID** (`[myId]`) — names a mapping so it can be referenced by Inline, Otherwise, Extends, union, association.
- **Extends** — a child mapping inherits all property mappings from a parent, can override or add properties. Upstream **enforces same table** — child cannot introduce a different ~mainTable.

---

## Upstream Mapping Syntax (legend-engine)

```pure
###Relational
Database db(
    Table ABC (ID INT PK, ANAME VARCHAR(200), BNAME VARCHAR(200), CNAME VARCHAR(200))
)

###Mapping
Mapping test::M
(
    // B7: Root marker (*) + B8: Mapping ID ([a])
    *test::A[a]: Relational {
        id: [db]ABC.ID,
        aName: [db]ABC.ANAME
    }

    // B9: Extends — inherits id, aName from [a]; adds bName; overrides aName
    *test::B[b] extends [a]: Relational {
        bName: [db]ABC.BNAME,
        aName: concat('bName_', [db]ABC.ANAME)   // override inherited property
    }

    // Chained extends
    *test::C[c] extends [b]: Relational {
        cName: [db]ABC.CNAME,
        aName: concat('cName_', [db]ABC.ANAME),   // override again
        bName: concat('cName_', [db]ABC.BNAME)     // override again
    }
)
```

From upstream: `RelationalInstanceSetImplementationProcessor.java` line 140 throws `"Cannot specify main table explicitly for extended mapping"` — same table is enforced.

---

## Simplified Example

```
###Relational
Database db(Table EMPLOYEE (ID INT PK, NAME VARCHAR(200), TITLE VARCHAR(100), SALARY DECIMAL))

###Mapping
Mapping test::M
(
    *test::Person[person]: Relational {
        id: [db]EMPLOYEE.ID,
        name: [db]EMPLOYEE.NAME
    }

    // Extends: inherits id, name; adds title, salary
    *test::Employee[emp] extends [person]: Relational {
        title: [db]EMPLOYEE.TITLE,
        salary: [db]EMPLOYEE.SALARY
    }
)
```

---

## Relation API Equivalent

### ✅ B7: Root marker → named function (implicit)

```pure
// The "root" mapping is simply the function you call:
function model::person(): Relation<(id:Integer, name:String)>[1] {
    #>{db.EMPLOYEE}#->extend(~[id: r|$r.ID, name: r|$r.NAME])
}
// There's no explicit root marker — the function IS the entry point
```

### ⚠️ B8: Mapping ID → named function / let binding

```pure
// Set ID = function name:
function model::person(): Relation<...>[1] { ... }  // this IS the "person" ID

// Or let binding for local reuse:
{|
    let personRel = #>{db.EMPLOYEE}#->extend(~[id: r|$r.ID, name: r|$r.NAME]);
    $personRel->filter(r|$r.name == 'Alice')->from(test::RT)
}
```

Mapping IDs are extracted in legend-lite but not yet queryable by ID at runtime.

### ❌ B9: Extends → function composition + `extend()` / `redefine()`

**Proposed design**: Since extends enforces the same table, function composition works naturally. The child calls the parent function and chains additional `extend()` (add) and `redefine()` (override) calls.

```pure
// Parent: A
function model::aRel(): Relation<(id:Integer, aName:String)>[1] {
    #>{db.ABC}#->extend(~[id: r|$r.ID, aName: r|$r.ANAME])
}

// Child: B extends A — call parent, redefine aName, extend bName
function model::bRel(): Relation<(id:Integer, aName:String, bName:String)>[1] {
    model::aRel()
      ->redefine(~aName: r|'bName_' + $r.ANAME)   // override (error if aName missing)
      ->extend(~bName: r|$r.BNAME)                 // add new (error if bName exists)
}

// Grandchild: C extends B
function model::cRel(): Relation<(id:Integer, aName:String, bName:String, cName:String)>[1] {
    model::bRel()
      ->redefine(~aName: r|'cName_' + $r.ANAME)
      ->redefine(~bName: r|'cName_' + $r.BNAME)
      ->extend(~cName: r|$r.CNAME)
}
```

**Generated SQL** (for `model::bRel()`):
```sql
SELECT "root".ID AS "id",
       CONCAT('bName_', "root".ANAME) AS "aName",
       "root".BNAME AS "bName"
FROM ABC AS "root"
```

### Design: `extend()` + `redefine()` split

Two functions with compile-time guards:
- **`extend(~col)`** — add-only. Compile error if column already exists → catches accidental shadowing
- **`redefine(~col)`** — replace-only. Compile error if column does NOT exist → catches typos

```pure
model::aRel()->extend(~bName: ...)     // ✅ bName is new
model::aRel()->extend(~aName: ...)     // ❌ COMPILE ERROR: aName already exists
model::aRel()->redefine(~aName: ...)   // ✅ aName exists, replace it
model::aRel()->redefine(~aNme: ...)    // ❌ COMPILE ERROR: aNme doesn't exist (typo!)
```

### Key files in legend-lite

- **MappingNormalizer.java** — would need to process `extends` clause
- **PureModelBuilder.java** — parses extends syntax
- **RelationalMappingIntegrationTest.java** — `testMappingExtends` (disabled)

### Tests in legend-lite

**Implemented (✅):**
| Test | File | What it covers |
|------|------|----------------|
| `testSetIdsAndRoot` | [RelationalMappingIntegrationTest.java:2178](../../engine/src/test/java/com/gs/legend/test/RelationalMappingIntegrationTest.java) | B7+B8: root marker selects correct mapping |
| `testSetIdExtraction` | [MappingDefinitionExtractionTest.java:376](../../engine/src/test/java/com/gs/legend/test/MappingDefinitionExtractionTest.java) | B8: parser extracts set ID |
| `testRootMarkerExtraction` | [MappingDefinitionExtractionTest.java:397](../../engine/src/test/java/com/gs/legend/test/MappingDefinitionExtractionTest.java) | B7: parser extracts root marker |
| `testExtendsExtraction` | [MappingDefinitionExtractionTest.java:419](../../engine/src/test/java/com/gs/legend/test/MappingDefinitionExtractionTest.java) | B9: parser extracts extends clause |
| `testFilterOnInheritedProperty` | [InheritanceIntegrationTest.java:170](../../engine/src/test/java/com/gs/legend/test/InheritanceIntegrationTest.java) | Class-level inheritance E2E |
| `testProjectMixedProperties` | [InheritanceIntegrationTest.java:213](../../engine/src/test/java/com/gs/legend/test/InheritanceIntegrationTest.java) | Own + inherited property projection |

**Not implemented (❌) — disabled stubs:**
| Test | File | What it covers |
|------|------|----------------|
| `testMappingExtends` | [RelationalMappingIntegrationTest.java:2224](../../engine/src/test/java/com/gs/legend/test/RelationalMappingIntegrationTest.java) | B9: mapping extends (disabled) |

**Test strategy for B9 (extends):**
1. Define parent mapping `[a]` with properties `id`, `aName`
2. Define child mapping `[b] extends [a]` adding `bName` and overriding `aName`
3. Verify child has all 3 properties, with `aName` using the override expression
4. Verify SQL is flat (no subquery — same table enforced)
5. Add `redefine()` compile-error test: error when column doesn't exist

---

## Implementation Notes

| Feature | Status | What to Build |
|---------|--------|---------------|
| B7 Root marker | ✅ | — |
| B8 Mapping ID | ⚠️ | Set ID lookup by ID for union/inline/otherwise references |
| B9 Extends | ❌ | 1. Builder must process `extends` clause and merge parent PMs. 2. New `redefine()` function — compile guard that column MUST exist. 3. MappingNormalizer emits parent function call + redefine/extend chain. |

**Priority**: B8 (Low), B9 (Medium — depends on B8)
