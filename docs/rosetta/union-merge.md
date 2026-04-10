# Union & Merge — Rosetta Stone

> Covers: **E6** (union), **E7** (merge/intersection)

---

## What It Does

- **Union** — combines multiple class mappings into one, producing the union of all rows. Used when a class is stored across multiple tables (e.g., Person in both PersonTableA and PersonTableB).
- **Merge/Intersection** — combines multiple class mappings with a merge lambda to resolve conflicts when the same object appears in multiple sources.

Both are **OperationSetImplementations** — they operate on other SetImplementations rather than mapping directly to stores.

---

## Upstream Mapping Syntax (legend-engine)

### Union
```pure
###Mapping
Mapping test::UnionMapping
(
    test::Person[personA]: Relational {
        ~mainTable [db]PersonTableA
        name: [db]PersonTableA.NAME,
        age: [db]PersonTableA.AGE
    }

    test::Person[personB]: Relational {
        ~mainTable [db]PersonTableB
        name: [db]PersonTableB.NAME,
        age: [db]PersonTableB.AGE
    }

    // E6: Union — combines both mappings
    *test::Person: Operation {
        meta::pure::router::operations::union_OperationSetImplementation_1__SetImplementation_MANY_(personA, personB)
    }
)
```

### Merge/Intersection
```pure
###Mapping
Mapping test::MergeMapping
(
    test::Person[personSource1]: Relational { ... }
    test::Person[personSource2]: Relational { ... }

    // E7: Merge — combine with conflict resolution
    *test::Person: Operation {
        meta::pure::router::operations::merge_OperationSetImplementation_1__SetImplementation_MANY_(
            personSource1, personSource2
        )
        {
            // Merge lambda — resolves conflicts when same person appears in both
            // Priority: take from source1, fill gaps from source2
            p: test::Person[1] | $p
        }
    }
)
```

---

## Simplified Example

```
###Relational
Database db(
    Table PERSON_US (ID INT PK, NAME VARCHAR(200), REGION VARCHAR(10))
    Table PERSON_EU (ID INT PK, NAME VARCHAR(200), REGION VARCHAR(10))
)

###Mapping
Mapping test::M
(
    test::Person[us]: Relational {
        ~mainTable [db]PERSON_US
        name: [db]PERSON_US.NAME,
        region: [db]PERSON_US.REGION
    }

    test::Person[eu]: Relational {
        ~mainTable [db]PERSON_EU
        name: [db]PERSON_EU.NAME,
        region: [db]PERSON_EU.REGION
    }

    *test::Person: Operation {
        meta::pure::router::operations::union_OperationSetImplementation_1__SetImplementation_MANY_(us, eu)
    }
)
```

---

## Relation API Equivalent

### ❌ Not Yet Implemented

### Union → `->concatenate()`

```pure
function model::allPersons(): Relation<(name:String, region:String)>[1] {
    // Union = concatenate two relations
    #>{db.PERSON_US}#->extend(~[
        name: r|$r.NAME,
        region: r|$r.REGION
    ])
    ->concatenate(
        #>{db.PERSON_EU}#->extend(~[
            name: r|$r.NAME,
            region: r|$r.REGION
        ])
    )
}
```

**Generated SQL**:
```sql
SELECT "root".NAME AS "name", "root".REGION AS "region" FROM PERSON_US AS "root"
UNION ALL
SELECT "root".NAME AS "name", "root".REGION AS "region" FROM PERSON_EU AS "root"
```

### Merge → `->join()` with merge logic

```pure
function model::mergedPerson(): Relation<(name:String, region:String, score:Float)>[1] {
    // Merge = join on identity + coalesce
    #>{db.PERSON_SOURCE1}#->extend(~[name: r|$r.NAME, score1: r|$r.SCORE])
      ->join(
          #>{db.PERSON_SOURCE2}#->extend(~[name: r|$r.NAME, score2: r|$r.SCORE]),
          JoinType.FULL_OUTER,
          {a,b|$a.ID == $b.ID}
      )
      ->extend(~[
          name: r|coalesce($r.name_left, $r.name_right),
          score: r|coalesce($r.score1, $r.score2)
      ])
}
```

### Key files in legend-lite

- **PureModelBuilder.java** — would need to parse `Operation` class mappings
- **MappingNormalizer.java** — emit `concatenate()` for union, `join()` for merge

### Tests in legend-lite

**Not implemented (❌) — no existing tests.**

**Test strategy for E6 (Union):**
1. Define two class mappings for `Person` — `[emp]` from `T_EMPLOYEE` and `[ctr]` from `T_CONTRACTOR`
2. Define `Operation { union(emp, ctr) }` mapping
3. Verify `Person.all()` query returns rows from both tables
4. Verify SQL contains `UNION ALL` of both subqueries
5. Test with different column sets — verify NULL padding for missing columns

**Test strategy for E7 (Merge):**
1. Define two class mappings with overlapping PKs
2. Define `Operation { merge([a, b], {x,y|$x.id == $y.id}) }` mapping
3. Verify result contains merged rows (no duplicates)
4. Verify SQL contains a JOIN between the two subqueries

---

## Implementation Notes

| Feature | Status | What to Build |
|---------|--------|---------------|
| E6 Union | ❌ | 1. Builder must parse `Operation { union(...) }` syntax. 2. MappingNormalizer emits `->concatenate()` of the referenced set implementations. 3. Column alignment between the union branches. |
| E7 Merge | ❌ | 1. Builder must parse `Operation { merge(...) }` with merge lambda. 2. MappingNormalizer emits `->join()` + merge/coalesce logic. 3. More complex — merge lambda defines conflict resolution. |

**Priority**: E6 Union (Medium), E7 Merge (Medium)

The Relation API primitives already exist (`concatenate()`, `join()`). The gap is in parsing the `Operation` class mapping syntax and wiring it to the right Relation operators.
