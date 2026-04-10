# Model-to-Model (M2M) Mapping — Rosetta Stone

> Covers: **E2** (Pure/M2M mapping)
>
> Two source flavors: **M2M→R** (relational source) and **M2M→J** (JSON source)

---

## What It Does

Pure (Model-to-Model) mappings transform objects from one Pure class to another without involving a physical store. The source is another Pure class (from another mapping), and property mappings use `$src` to reference source properties.

Use cases:
- Transform between internal and external models
- Aggregate or reshape data (rename, compute, filter)
- Multi-hop mapping chains (Table → M2M → M2M → M2M → output)
- Cross-source abstraction (relational or JSON → same M2M target class)

---

## 1. Architecture: Two Execution Paths

### M2M→R (Relational source)

```
   Query                 M2M                  Relational          Physical
┌───────────┐     ┌─────────────┐     ┌──────────────────┐    ┌─────────┐
│StaffCard  │────→│ StaffMember │────→│   Employee       │───→│T_EMPLOYEE│
│.all()     │     │ Pure ~src   │     │   Relational     │    │ (DuckDB) │
│->project()│     │ Employee    │     │   ~mainTable     │    └─────────┘
└───────────┘     └─────────────┘     └──────────────────┘
```

Everything compiles to a **single SQL query**. Each M2M hop inlines as `extend()` + `select()` — the optimizer flattens it.

### M2M→J (JSON source)

```
   Query                 M2M                  JSON
┌───────────┐     ┌─────────────┐     ┌──────────────────┐
│StaffCard  │────→│ StaffMember │────→│  RawPerson       │
│.all()     │     │ Pure ~src   │     │  JsonModelConn   │
│->project()│     │ RawPerson   │     │  data:app/json,… │
└───────────┘     └─────────────┘     └──────────────────┘
```

JSON data is materialized as an **inline subquery** (`FROM (VALUES (...))`) — then the same M2M extend/select logic applies.

---

## 2. Single-hop M2M — Step by Step

### Legend Mapping Syntax

```pure
Class Employee { firstName: String[1]; lastName: String[1]; age: Integer[1]; dept: String[1]; }
Class StaffMember { fullName: String[1]; firstName: String[1]; age: Integer[1]; dept: String[1]; }

Mapping model::StaffMemberMapping (
    StaffMember: Pure {
        ~src Employee
        fullName:  $src.firstName + ' ' + $src.lastName,   // computed
        firstName: $src.firstName,                          // passthrough
        age:       $src.age,                                // passthrough
        dept:      $src.dept                                // passthrough
    }
)
```

### What Happens at Each Pipeline Stage

| Stage | What | Example |
|-------|------|---------|
| **Parser** | Extracts `~src Employee`, property expressions | `$src.firstName + ' ' + $src.lastName` → concat AST |
| **GetAllChecker** | `compileM2MGetAll`: resolves source class, compiles each `$src.prop` expression | TypeInfo stamped on every node |
| **MappingResolver** | Discovers M2M chain: StaffMember → Employee → T_EMPLOYEE | Chains resolved to single query |
| **PlanGenerator** | Emits `extend()` for computed + passthrough columns, then `select()` to drop raw cols | Flat SQL with column aliases |
| **Dialect** | Renders SQL | `CONCAT(FIRST_NAME, ' ', LAST_NAME) AS "fullName"` |

### Query + Output (from tests)

```pure
StaffMember.all()->project(~[fullName:x|$x.fullName, dept:x|$x.dept])
```

| fullName | dept |
|----------|------|
| Alice Smith | Engineering |
| Bob Jones | Sales |
| Carol White | Engineering |
| Dave Brown | Marketing |

**Generated SQL (2R):**
```sql
SELECT (("t0"."FIRST_NAME" || ' ') || "t0"."LAST_NAME") AS "fullName",
       "t0"."DEPT" AS "dept"
FROM "T_EMPLOYEE" AS "t0"
```

**Generated SQL (2J):**
```sql
SELECT ((CAST((("t0"."data")->>'firstName') AS VARCHAR) || ' ')
       || CAST((("t0"."data")->>'lastName') AS VARCHAR)) AS "fullName",
       CAST((("t0"."data")->>'dept') AS VARCHAR) AS "dept"
FROM (SELECT unnest(CAST('[{"firstName":"Alice",...}]' AS JSON[])) AS "data") AS "t0"
```

### Test references

| Test | File | Variant |
|------|------|---------|
| `testProjectSimple` | [M2MChainIntegrationTest.java:430](../../engine/src/test/java/com/gs/legend/test/M2MChainIntegrationTest.java) | 2R: project |
| `testGraphFetch` | [M2MChainIntegrationTest.java:452](../../engine/src/test/java/com/gs/legend/test/M2MChainIntegrationTest.java) | 2R: graphFetch |
| `testProjectAllProps` | [JsonM2MChainIntegrationTest.java:360](../../engine/src/test/java/com/gs/legend/test/JsonM2MChainIntegrationTest.java) | 2J: same from JSON |
| `testGraphFetch` | [JsonM2MChainIntegrationTest.java:389](../../engine/src/test/java/com/gs/legend/test/JsonM2MChainIntegrationTest.java) | 2J: graphFetch from JSON |

---

## 3. Multi-hop Chains — M2M→M2M→…→Source

### 3.1 Two-hop chain (L2 → L1 → Source)

```
  StaffCard            StaffMember           Employee         T_EMPLOYEE
  ─────────           ────────────          ──────────       ───────────
  displayName ◀──┐    fullName ◀──┐        firstName        FIRST_NAME
  department  ◀──┤    dept     ◀──┤        lastName         LAST_NAME
                 │               │         dept              DEPT
          toUpper(fullName)   firstName+' '+lastName
```

**Mapping chain:**
```pure
// L1: Employee → StaffMember
StaffMember: Pure { ~src Employee
    fullName: $src.firstName + ' ' + $src.lastName,
    dept: $src.dept
}

// L2: StaffMember → StaffCard
StaffCard: Pure { ~src StaffMember
    displayName: $src.fullName->toUpper(),
    department:  $src.dept
}
```

**Query + Output:**
```pure
StaffCard.all()->project(~[displayName:x|$x.displayName, department:x|$x.department])
```

| displayName | department |
|-------------|-----------|
| ALICE SMITH | Engineering |
| BOB JONES | Sales |
| CAROL WHITE | Engineering |
| DAVE BROWN | Marketing |

**Generated SQL (2R)** — 2 M2M hops collapse into one flat query against the table:
```sql
SELECT UPPER((("t0"."FIRST_NAME" || ' ') || "t0"."LAST_NAME")) AS "displayName",
       "t0"."DEPT" AS "department"
FROM "T_EMPLOYEE" AS "t0"
```

**Generated SQL (2J)** — same logic, but source is unnested inline JSON:
```sql
SELECT UPPER(((CAST((("t0"."data")->>'firstName') AS VARCHAR) || ' ')
           || CAST((("t0"."data")->>'lastName') AS VARCHAR))) AS "displayName",
       CAST((("t0"."data")->>'dept') AS VARCHAR) AS "department"
FROM (SELECT unnest(CAST('[{"firstName":"Alice",...}]' AS JSON[])) AS "data") AS "t0"
```

**Tests:**
| Test | File | Variant |
|------|------|---------|
| `testProjectTwoHop` | [M2MChainIntegrationTest.java:514](../../engine/src/test/java/com/gs/legend/test/M2MChainIntegrationTest.java) | 2R |
| `testProjectTwoHop` | [JsonM2MChainIntegrationTest.java:502](../../engine/src/test/java/com/gs/legend/test/JsonM2MChainIntegrationTest.java) | 2J |

### 3.2 Three-hop chain (L3 → L2 → L1 → Source)

```
  DirectoryEntry        StaffCard           StaffMember         Employee        T_EMPLOYEE
  ──────────────       ──────────          ────────────        ──────────      ───────────
  entry ◀──────────── displayName ◀────── fullName ◀───────── firstName       FIRST_NAME
                       department ◀────── dept     ◀───────── dept            DEPT
              displayName + ' - ' + department
```

**Query + Output:**
```pure
DirectoryEntry.all()->project(~[entry:x|$x.entry])
```

| entry |
|-------|
| ALICE SMITH - Engineering |
| BOB JONES - Sales |
| CAROL WHITE - Engineering |
| DAVE BROWN - Marketing |

**Generated SQL (2R)** — 3 M2M hops, still one flat query:
```sql
SELECT ((UPPER((("t0"."FIRST_NAME" || ' ') || "t0"."LAST_NAME")) || ' - ') || "t0"."DEPT") AS "entry"
FROM "T_EMPLOYEE" AS "t0"
```

**Generated SQL (2J)** — same 3-hop logic over unnested JSON:
```sql
SELECT ((UPPER(((CAST((("t0"."data")->>'firstName') AS VARCHAR) || ' ')
              || CAST((("t0"."data")->>'lastName') AS VARCHAR))) || ' - ')
       || CAST((("t0"."data")->>'dept') AS VARCHAR)) AS "entry"
FROM (SELECT unnest(CAST('[{...}]' AS JSON[])) AS "data") AS "t0"
```

**Tests:**
| Test | File | Variant |
|------|------|---------|
| `testProjectThreeHop` | [M2MChainIntegrationTest.java:581](../../engine/src/test/java/com/gs/legend/test/M2MChainIntegrationTest.java) | 2R |
| `testProjectThreeHop` | [JsonM2MChainIntegrationTest.java:576](../../engine/src/test/java/com/gs/legend/test/JsonM2MChainIntegrationTest.java) | 2J |

### 3.3 Four-hop chain (L4 → L3 → L2 → L1 → Source)

```
  Badge → DirectoryEntry → StaffCard → StaffMember → Employee/RawPerson
  text = '[' + entry + ']'
```

**Query + Output:**
```pure
Badge.all()->project(~[text:x|$x.text])
```

| text |
|------|
| [ALICE SMITH - Engineering] |
| [BOB JONES - Sales] |
| [CAROL WHITE - Engineering] |
| [DAVE BROWN - Marketing] |

**Generated SQL (2R)** — 4 M2M hops, single flat query:
```sql
SELECT (('[' || ((UPPER((("t0"."FIRST_NAME" || ' ') || "t0"."LAST_NAME"))
       || ' - ') || "t0"."DEPT")) || ']') AS "text"
FROM "T_EMPLOYEE" AS "t0"
```

**Generated SQL (2J)** — 4-hop over unnested JSON:
```sql
SELECT (('[' || ((UPPER(((CAST((("t0"."data")->>'firstName') AS VARCHAR) || ' ')
              || CAST((("t0"."data")->>'lastName') AS VARCHAR))) || ' - ')
       || CAST((("t0"."data")->>'dept') AS VARCHAR))) || ']') AS "text"
FROM (SELECT unnest(CAST('[{...}]' AS JSON[])) AS "data") AS "t0"
```

**Tests:**
| Test | File | Variant |
|------|------|---------|
| `testProjectFourHop` | [M2MChainIntegrationTest.java:639](../../engine/src/test/java/com/gs/legend/test/M2MChainIntegrationTest.java) | 2R |
| `testGraphFetchFourHop` | [M2MChainIntegrationTest.java:651](../../engine/src/test/java/com/gs/legend/test/M2MChainIntegrationTest.java) | 2R: graphFetch |
| `testProjectFourHopWithFilter` | [M2MChainIntegrationTest.java:663](../../engine/src/test/java/com/gs/legend/test/M2MChainIntegrationTest.java) | 2R: filter at L4 |
| `testProjectFourHop` | [JsonM2MChainIntegrationTest.java:622](../../engine/src/test/java/com/gs/legend/test/JsonM2MChainIntegrationTest.java) | 2J |
| `testProjectFourHopWithFilter` | [JsonM2MChainIntegrationTest.java:646](../../engine/src/test/java/com/gs/legend/test/JsonM2MChainIntegrationTest.java) | 2J: filter at L4 |

---

## 4. Mapping Filters + User Filters — Composition

### 4.1 Mapping-level filter (`~filter`)

```pure
ActiveStaff: Pure {
    ~src Employee
    ~filter $src.isActive == true         // ← mapping-level filter
    fullName: $src.firstName + ' ' + $src.lastName,
    dept: $src.dept
}
```

**Effect:** Every query on `ActiveStaff` automatically excludes inactive employees. Carol (`isActive=false`) never appears.

```pure
ActiveStaff.all()->project(~[fullName:x|$x.fullName])
// → 3 rows: Alice, Bob, Dave (Carol excluded)
```

### 4.2 User query filter on top of M2M

```pure
StaffMember.all()
    ->filter({x|$x.dept == 'Engineering'})
    ->project(~[fullName:x|$x.fullName])
// → 2 rows: Alice Smith, Carol White
```

### 4.3 User filter + mapping filter compose

```pure
ActiveStaff.all()
    ->filter({x|$x.dept == 'Engineering'})
    ->project(~[fullName:x|$x.fullName])
// → 1 row: Alice Smith (Carol is inactive, excluded by mapping filter)
```

**Generated SQL:**
```sql
SELECT CONCAT("root".FIRST_NAME, ' ', "root".LAST_NAME) AS "fullName"
FROM T_EMPLOYEE AS "root"
WHERE "root".IS_ACTIVE = true           -- mapping filter
  AND "root".DEPT = 'Engineering'       -- user filter
```

### 4.4 Filter propagates through multi-hop chains

```pure
// StaffBadge ~src ActiveStaff (~filter isActive) → Employee
StaffBadge.all()->project(~[label:x|$x.label])
// → 3 rows: "Alice Smith [Engineering]", "Bob Jones [Sales]", "Dave Brown [Marketing]"
// Carol excluded — the filter from L1 propagates through the L2 chain
```

**Tests:**
| Test | File | Variant |
|------|------|---------|
| `testProjectWithFilter` | [M2MChainIntegrationTest.java:465](../../engine/src/test/java/com/gs/legend/test/M2MChainIntegrationTest.java) | 2R: mapping filter |
| `testProjectWithUserFilter` | [M2MChainIntegrationTest.java:489](../../engine/src/test/java/com/gs/legend/test/M2MChainIntegrationTest.java) | 2R: user filter |
| `testProjectUserFilterPlusMappingFilter` | [M2MChainIntegrationTest.java:499](../../engine/src/test/java/com/gs/legend/test/M2MChainIntegrationTest.java) | 2R: both compose |
| `testProjectTwoHopWithFilterAtL1` | [M2MChainIntegrationTest.java:539](../../engine/src/test/java/com/gs/legend/test/M2MChainIntegrationTest.java) | 2R: filter at L1, query at L2 |
| `testProjectMappingFilter` | [JsonM2MChainIntegrationTest.java:437](../../engine/src/test/java/com/gs/legend/test/JsonM2MChainIntegrationTest.java) | 2J: mapping filter |
| `testProjectMappingAndUserFilter` | [JsonM2MChainIntegrationTest.java:449](../../engine/src/test/java/com/gs/legend/test/JsonM2MChainIntegrationTest.java) | 2J: both compose |

---

## 5. M2M with 1-to-Many — Deep Fetch via Association Navigation

This is the most complex M2M pattern: the M2M mapping navigates an **association** on the source class, producing **nested objects** in the output.

### 5.1 Data setup

```
T_EMPLOYEE                               T_DEPARTMENT
┌────┬───────┬───────┬─────────┐         ┌────┬─────────────┬───────────────┐
│ ID │ FIRST │ LAST  │ DEPT_ID │         │ ID │ NAME        │ LOCATION      │
├────┼───────┼───────┼─────────┤         ├────┼─────────────┼───────────────┤
│  1 │ Alice │ Smith │    1    │──┐      │  1 │ Engineering │ San Francisco │
│  2 │ Bob   │ Jones │    2    │──┤      │  2 │ Sales       │ New York      │
│  3 │ Carol │ White │    1    │──┘      │  3 │ Marketing   │ Chicago       │
│  4 │ Dave  │ Brown │    3    │──┘      └────┴─────────────┴───────────────┘
└────┴───────┴───────┴─────────┘
         ↑                                    ↑
         └──── Join EmpDept(DEPT_ID = ID) ────┘
```

### 5.2 Mapping definition

```pure
Association model::EmpDept {
    employees:  Employee[*];
    department: Department[1];
}

// Relational mapping with association join
Employee: Relational {
    ~mainTable [CompanyDB] T_EMPLOYEE
    firstName: [CompanyDB] T_EMPLOYEE.FIRST_NAME,
    department: [CompanyDB] @EmpDept            // ← association navigation
}

// M2M: Employee → StaffWithDept (navigates association)
StaffWithDept: Pure {
    ~src Employee
    fullName: $src.firstName + ' ' + $src.lastName,
    department: $src.department                  // ← 1-to-many through association!
}
DeptInfo: Pure {
    ~src Department
    name: $src.name,
    location: $src.location
}
```

### 5.3 Query + Output

```pure
StaffWithDept.all()
    ->graphFetch(#{ StaffWithDept { fullName, department { name, location } } }#)
    ->serialize(#{ StaffWithDept { fullName, department { name, location } } }#)
```

**JSON output:**
```json
[
  {"fullName": "Alice Smith", "department": [{"name": "Engineering", "location": "San Francisco"}]},
  {"fullName": "Bob Jones",   "department": [{"name": "Sales",       "location": "New York"}]},
  {"fullName": "Carol White", "department": [{"name": "Engineering", "location": "San Francisco"}]},
  {"fullName": "Dave Brown",  "department": [{"name": "Marketing",   "location": "Chicago"}]}
]
```

**Generated SQL (2R)** — single query with correlated subquery for nested association:
```sql
SELECT json_group_array(
    json_object(
        'fullName', "fullName",
        'department', (
            SELECT json_group_array(
                json_object('name', "c_department"."NAME",
                            'location', "c_department"."LOCATION"))
            AS "_arr"
            FROM "T_DEPARTMENT" AS "c_department"
            WHERE "_sub"."DEPT_ID" = "c_department"."ID"
        )
    )
) AS "result"
FROM (
    SELECT (("t0"."FIRST_NAME" || ' ') || "t0"."LAST_NAME") AS "fullName",
           "t0"."DEPT_ID" AS "DEPT_ID"
    FROM "T_EMPLOYEE" AS "t0"
) AS "_sub"
```

**Key observations:**
1. Root query selects `fullName` + `DEPT_ID` (the FK needed for the correlated subquery)
2. Correlated subquery joins `T_DEPARTMENT` on `DEPT_ID = ID` to produce the nested array
3. `json_group_array(json_object(...))` produces the nested JSON array

### 5.4 Pruning: requesting only root props → 0 JOINs

```pure
StaffWithDept.all()
    ->graphFetch(#{ StaffWithDept { fullName } }#)
    ->serialize(#{ StaffWithDept { fullName } }#)
```

**Generated SQL (2R)** — no correlated subquery, no FK column:
```sql
SELECT json_group_array(json_object('fullName', "fullName")) AS "result"
FROM (
    SELECT (("t0"."FIRST_NAME" || ' ') || "t0"."LAST_NAME") AS "fullName"
    FROM "T_EMPLOYEE" AS "t0"
) AS "_sub"
```

**Tests:**
| Test | File | What it covers |
|------|------|----------------|
| `testDeepFetchViaAssociation` | [M2MChainIntegrationTest.java:880](../../engine/src/test/java/com/gs/legend/test/M2MChainIntegrationTest.java) | Full nested fetch |
| `testDeepFetchRootOnlyNoJoin` | [M2MChainIntegrationTest.java:896](../../engine/src/test/java/com/gs/legend/test/M2MChainIntegrationTest.java) | Root only → 0 JOINs |
| `testDeepFetchAllEmployeesDepts` | [M2MChainIntegrationTest.java:910](../../engine/src/test/java/com/gs/legend/test/M2MChainIntegrationTest.java) | All 4 employees correct dept |
| `testDeepFetchPartialNestedSelection` | [M2MChainIntegrationTest.java:927](../../engine/src/test/java/com/gs/legend/test/M2MChainIntegrationTest.java) | Only location, no name |

### 5.5 Disjoint 1-to-many — Two Independent Associations

The M2M target navigates **two separate** association properties on the source class, each resolved via a **different JOIN**. The engine fires independent correlated subqueries for each.

```
T_EMPLOYEE ──@EmpDept──→ T_DEPARTMENT     (1 dept per employee)
T_EMPLOYEE ──@EmpProject──→ T_PROJECT     (0..N projects per employee)
```

**Data:**
| Employee | Department | Projects |
|----------|-----------|----------|
| Alice Smith | Engineering | Alpha ($500K), Beta ($300K) |
| Bob Jones | Sales | Gamma ($750K) |
| Carol White | Engineering | *(none)* |
| Dave Brown | Marketing | Delta ($1.2M) |

**Mapping:**
```pure
StaffComplete: Pure { ~src Employee
    fullName: $src.firstName + ' ' + $src.lastName,
    department: $src.department,   // ← @EmpDept (1-to-many #1)
    projects: $src.projects        // ← @EmpProject (1-to-many #2, disjoint)
}
DeptInfo: Pure { ~src Department  name: $src.name, location: $src.location }
ProjectInfo: Pure { ~src Project  name: $src.name, budget: $src.budget }
```

**Query — both associations:**
```pure
StaffComplete.all()
    ->graphFetch(#{ StaffComplete { fullName, department { name }, projects { name, budget } } }#)
    ->serialize(#{ StaffComplete { fullName, department { name }, projects { name, budget } } }#)
```

**JSON output:**
```json
[
  {"fullName": "Alice Smith",
   "department": [{"name": "Engineering"}],
   "projects":  [{"name": "Alpha", "budget": 500000}, {"name": "Beta", "budget": 300000}]},
  {"fullName": "Bob Jones",
   "department": [{"name": "Sales"}],
   "projects":  [{"name": "Gamma", "budget": 750000}]},
  {"fullName": "Carol White",
   "department": [{"name": "Engineering"}],
   "projects":  []},
  {"fullName": "Dave Brown",
   "department": [{"name": "Marketing"}],
   "projects":  [{"name": "Delta", "budget": 1200000}]}
]
```

**Generated SQL (2R)** — one root query, **two independent** correlated subqueries:
```sql
SELECT json_group_array(
    json_object(
        'fullName', "fullName",
        'department', (
            SELECT json_group_array(json_object('name', "c_department"."NAME"))
            AS "_arr"
            FROM "T_DEPARTMENT" AS "c_department"
            WHERE "_sub"."DEPT_ID" = "c_department"."ID"
        ),
        'projects', (
            SELECT json_group_array(
                json_object('name', "c_projects"."NAME",
                            'budget', "c_projects"."BUDGET"))
            AS "_arr"
            FROM "T_PROJECT" AS "c_projects"
            WHERE "_sub"."ID" = "c_projects"."EMP_ID"
        )
    )
) AS "result"
FROM (
    SELECT (("t0"."FIRST_NAME" || ' ') || "t0"."LAST_NAME") AS "fullName",
           "t0"."DEPT_ID" AS "DEPT_ID",
           "t0"."ID" AS "ID"
    FROM "T_EMPLOYEE" AS "t0"
) AS "_sub"
```

**Key observations:**
1. Root query selects `fullName` + **both** FK columns: `DEPT_ID` (for `@EmpDept`) and `ID` (for `@EmpProject`)
2. Two independent correlated subqueries: one to `T_DEPARTMENT`, one to `T_PROJECT`
3. Each produces its own `json_group_array` nested inside the root `json_object`

**Pruning — only projects requested:**
```sql
SELECT json_group_array(
    json_object(
        'fullName', "fullName",
        'projects', (
            SELECT json_group_array(
                json_object('name', "c_projects"."NAME",
                            'budget', "c_projects"."BUDGET"))
            AS "_arr"
            FROM "T_PROJECT" AS "c_projects"
            WHERE "_sub"."ID" = "c_projects"."EMP_ID"
        )
    )
) AS "result"
FROM (
    SELECT (("t0"."FIRST_NAME" || ' ') || "t0"."LAST_NAME") AS "fullName",
           "t0"."ID" AS "ID"
    FROM "T_EMPLOYEE" AS "t0"
) AS "_sub"
```
→ No `DEPT_ID` in root, no `T_DEPARTMENT` subquery at all.

**Pruning — root only:**
```sql
SELECT json_group_array(json_object('fullName', "fullName")) AS "result"
FROM (
    SELECT (("t0"."FIRST_NAME" || ' ') || "t0"."LAST_NAME") AS "fullName"
    FROM "T_EMPLOYEE" AS "t0"
) AS "_sub"
```
→ Zero subqueries, zero FK columns.

**Tests:**
| Test | File | What it covers |
|------|------|----------------|
| `testDisjointOneToMany` | [M2MChainIntegrationTest.java:944](../../engine/src/test/java/com/gs/legend/test/M2MChainIntegrationTest.java) | Both dept + projects in one fetch |
| `testDisjointOnlyDept` | [M2MChainIntegrationTest.java:967](../../engine/src/test/java/com/gs/legend/test/M2MChainIntegrationTest.java) | Only dept → no project subquery |
| `testDisjointOnlyProjects` | [M2MChainIntegrationTest.java:980](../../engine/src/test/java/com/gs/legend/test/M2MChainIntegrationTest.java) | Only projects → no dept subquery |
| `testDisjointRootOnly` | [M2MChainIntegrationTest.java:993](../../engine/src/test/java/com/gs/legend/test/M2MChainIntegrationTest.java) | Root only → 0 subqueries |
| `testDisjointZeroProjects` | [M2MChainIntegrationTest.java:1007](../../engine/src/test/java/com/gs/legend/test/M2MChainIntegrationTest.java) | Carol has 0 projects |
| `testDisjointMultipleProjects` | [M2MChainIntegrationTest.java:1019](../../engine/src/test/java/com/gs/legend/test/M2MChainIntegrationTest.java) | Alice has 2 projects + filter |

---

## 6. M2M with Traverse Columns — Scalar Join Properties

Unlike deep fetch (nested objects), traverse columns "flatten" join data into scalar columns on the same row.

### 6.1 Single-hop traverse through M2M

```pure
// Relational mapping: Employee.deptName comes from a JOIN
Employee: Relational {
    deptName: [CompanyDB] @EmpDept | T_DEPARTMENT.NAME,          // scalar column via join
    deptLocation: [CompanyDB] @EmpDept | T_DEPARTMENT.LOCATION
}

// M2M references these join-resolved columns
StaffDetail: Pure { ~src Employee
    fullName: $src.firstName + ' ' + $src.lastName,
    deptName: $src.deptName,
    deptLocation: $src.deptLocation
}
```

**Query + Output:**
```pure
StaffDetail.all()->project(~[fullName:x|$x.fullName, deptName:x|$x.deptName, deptLocation:x|$x.deptLocation])
```

| fullName | deptName | deptLocation |
|----------|----------|--------------|
| Alice Smith | Engineering | San Francisco |
| Bob Jones | Sales | New York |
| Carol White | Engineering | San Francisco |
| Dave Brown | Marketing | Chicago |

**Generated SQL (2R)** — subquery wraps the JOIN, M2M projects from it:
```sql
SELECT (("proj_src"."FIRST_NAME" || ' ') || "proj_src"."LAST_NAME") AS "fullName",
       "proj_src"."deptName" AS "deptName",
       "proj_src"."deptLocation" AS "deptLocation"
FROM (
    SELECT "t0".*,
           "t1"."NAME" AS "deptName",
           "t1"."LOCATION" AS "deptLocation"
    FROM "T_EMPLOYEE" AS "t0"
    LEFT OUTER JOIN "T_DEPARTMENT" AS "t1" ON "t0"."DEPT_ID" = "t1"."ID"
) AS proj_src
```

### 6.2 Multi-hop traverse through M2M

```pure
Employee: Relational {
    orgName: [CompanyDB] @EmpDept > @DeptOrg | T_ORGANIZATION.NAME  // 2-hop join chain
}
StaffProfile: Pure { ~src Employee
    fullName: $src.firstName + ' ' + $src.lastName,
    orgName: $src.orgName
}
```

**Generated SQL (2R)** — subquery wraps both JOINs in the chain:
```sql
SELECT (("proj_src"."FIRST_NAME" || ' ') || "proj_src"."LAST_NAME") AS "fullName",
       "proj_src"."orgName" AS "orgName"
FROM (
    SELECT "t0".*,
           "t2"."NAME" AS "orgName"
    FROM "T_EMPLOYEE" AS "t0"
    LEFT OUTER JOIN "T_DEPARTMENT" AS "t1" ON "t0"."DEPT_ID" = "t1"."ID"
    LEFT OUTER JOIN "T_ORGANIZATION" AS "t2" ON "t1"."ORG_ID" = "t2"."ID"
) AS proj_src
```

### 6.3 Mixed: scalar traverse + association in same M2M

```pure
StaffFull: Pure { ~src Employee
    fullName: $src.firstName + ' ' + $src.lastName,
    deptName: $src.deptName,              // scalar (join-resolved, flat column)
    department: $src.department            // association (nested objects)
}
```

This produces a graphFetch with **both** flat columns and nested objects:
```json
{"fullName": "Alice Smith", "deptName": "Engineering", "department": [{"name": "Engineering", "location": "San Francisco"}]}
```

**Generated SQL (2R)** — traverse column uses LEFT JOIN subquery, association uses correlated subquery:
```sql
SELECT json_group_array(
    json_object(
        'fullName', "fullName",
        'deptName', "deptName",
        'department', (
            SELECT json_group_array(
                json_object('name', "c_department"."NAME",
                            'location', "c_department"."LOCATION"))
            AS "_arr"
            FROM "T_DEPARTMENT" AS "c_department"
            WHERE "_sub"."DEPT_ID" = "c_department"."ID"
        )
    )
) AS "result"
FROM (
    SELECT (("gf_src"."FIRST_NAME" || ' ') || "gf_src"."LAST_NAME") AS "fullName",
           "gf_src"."deptName" AS "deptName",
           "gf_src"."DEPT_ID" AS "DEPT_ID"
    FROM (
        SELECT "t0".*, "t1"."NAME" AS "deptName"
        FROM "T_EMPLOYEE" AS "t0"
        LEFT OUTER JOIN "T_DEPARTMENT" AS "t1" ON "t0"."DEPT_ID" = "t1"."ID"
    ) AS gf_src
) AS "_sub"
```

**Key insight:** The traverse column `deptName` resolves via LEFT JOIN in an inner subquery (`gf_src`), while the nested `department` object resolves via correlated subquery against `T_DEPARTMENT`. Both coexist in the same SQL statement.

**Tests:**
| Test | File | What it covers |
|------|------|----------------|
| `testProjectSingleHopTraverse` | [M2MChainIntegrationTest.java:622](../../engine/src/test/java/com/gs/legend/test/M2MChainIntegrationTest.java) | Flat traverse cols |
| `testProjectMultiHopTraverse` | [M2MChainIntegrationTest.java:662](../../engine/src/test/java/com/gs/legend/test/M2MChainIntegrationTest.java) | 2-hop join chain |
| `testProjectTraverseWithFilter` | [M2MChainIntegrationTest.java:652](../../engine/src/test/java/com/gs/legend/test/M2MChainIntegrationTest.java) | Filter on traverse col |
| `testGraphFetchMixedTraverseAndAssociation` | [M2MChainIntegrationTest.java:673](../../engine/src/test/java/com/gs/legend/test/M2MChainIntegrationTest.java) | Scalar + nested combo |

---

## 7. M2M→R vs M2M→J — Side-by-Side Comparison

Both paths use **identical M2M mapping definitions** — only the source class resolution differs.

### Model (identical for both)

```pure
StaffMember: Pure { ~src Employee/RawPerson
    fullName: $src.firstName + ' ' + $src.lastName, ...
}
StaffCard: Pure { ~src StaffMember
    displayName: $src.fullName->toUpper(), ...
}
DirectoryEntry: Pure { ~src StaffCard
    entry: $src.displayName + ' - ' + $src.department
}
```

### Source resolution

| Aspect | M2M→R (Relational) | M2M→J (JSON inline) | M2M→J (JSON file) |
|--------|---------------------|---------------------|---------------------|
| **Source data** | SQL table (T_EMPLOYEE) | Inline JSON via `data:application/json,...` | External `.json` file on disk |
| **Source mapping** | `Employee: Relational { ... }` | `variantIdentity` (auto-generated) | `variantIdentity` (auto-generated) |
| **Connection** | `RelationalDatabaseConnection` | `JsonModelConnection` | `JsonModelConnection` |
| **SQL FROM clause** | `FROM "T_EMPLOYEE" AS "t0"` | `FROM (SELECT unnest(CAST('...' AS JSON[]))) AS "t0"` | `FROM (SELECT json AS "data" FROM read_json_objects('...')) AS "t0"` |
| **Joins available** | ✅ `@EmpDept` → LEFT JOIN | ❌ No joins (flat JSON only) | ❌ No joins (flat JSON only) |
| **Deep fetch** | ✅ Nested objects via correlated subquery | ❌ Only flat properties | ❌ Only flat properties |
| **Chain depth tested** | L1, L2, L3, L4 | L1, L2, L3, L4 | L1, L2 (×3 formats) |

### Output (identical for same data)

Both produce the exact same results given equivalent data:
```
StaffCard.all()->project(~[displayName:x|$x.displayName])
→ ALICE SMITH, BOB JONES, CAROL WHITE, DAVE BROWN
```

### 7.1 M2M→J from External JSON Files — All 3 DuckDB Formats

Instead of inlining JSON in a `data:` URI, you can point `JsonModelConnection` at an external file.
DuckDB's `read_json_objects()` **auto-detects** the format, so the same `renderSourceUrl()` call
works transparently for all 3 JSON file layouts:

#### The 3 JSON file formats

**1. NDJSON** (newline-delimited) — `persons-ndjson.txt`:
```json
{"firstName":"Alice","lastName":"Smith","age":30,"dept":"Engineering","salary":95000,"active":true}
{"firstName":"Bob","lastName":"Jones","age":45,"dept":"Sales","salary":120000,"active":true}
{"firstName":"Carol","lastName":"White","age":28,"dept":"Engineering","salary":85000,"active":false}
{"firstName":"Dave","lastName":"Brown","age":55,"dept":"Marketing","salary":110000,"active":true}
```

**2. JSON Array** — `persons-array.json`:
```json
[
  {"firstName":"Alice","lastName":"Smith","age":30,"dept":"Engineering","salary":95000,"active":true},
  {"firstName":"Bob","lastName":"Jones","age":45,"dept":"Sales","salary":120000,"active":true},
  {"firstName":"Carol","lastName":"White","age":28,"dept":"Engineering","salary":85000,"active":false},
  {"firstName":"Dave","lastName":"Brown","age":55,"dept":"Marketing","salary":110000,"active":true}
]
```

**3. Unstructured** (whitespace-separated objects) — `persons-unstructured.txt`:
```json
{"firstName":"Alice","lastName":"Smith","age":30,"dept":"Engineering","salary":95000,"active":true}
  {"firstName":"Bob","lastName":"Jones","age":45,"dept":"Sales","salary":120000,"active":true}
    {"firstName":"Carol","lastName":"White","age":28,"dept":"Engineering","salary":85000,"active":false}
{"firstName":"Dave","lastName":"Brown","age":55,"dept":"Marketing","salary":110000,"active":true}
```

#### Pure connection — identical for all 3

Only the `url:` path changes; no format hint needed:
```pure
JsonModelConnection {
    class: model::RawPerson;
    url: 'file:///path/to/test-data/persons.json';           // or persons-array.json or persons-unstructured.json
}
```

#### Generated SQL — identical for all 3 formats

The SQL is exactly the same regardless of file format — only the filename differs:

**L1 project:**
```sql
SELECT ((CAST((("t0"."data")->>'firstName') AS VARCHAR) || ' ')
       || CAST((("t0"."data")->>'lastName') AS VARCHAR)) AS "fullName",
       CAST((("t0"."data")->>'age') AS BIGINT) AS "age",
       CAST((("t0"."data")->>'dept') AS VARCHAR) AS "dept"
FROM (
    SELECT json AS "data"
    FROM read_json_objects('/path/to/test-data/persons.json')
) AS "t0"
```

**L2 chain (2-hop):**
```sql
SELECT UPPER(((CAST((("t0"."data")->>'firstName') AS VARCHAR) || ' ')
             || CAST((("t0"."data")->>'lastName') AS VARCHAR))) AS "displayName",
       CAST((("t0"."data")->>'dept') AS VARCHAR) AS "department"
FROM (
    SELECT json AS "data"
    FROM read_json_objects('/path/to/test-data/persons.json')
) AS "t0"
```

**L2 with filter:**
```sql
SELECT UPPER(((CAST((("ext"."data")->>'firstName') AS VARCHAR) || ' ')
             || CAST((("ext"."data")->>'lastName') AS VARCHAR))) AS "displayName"
FROM (
    SELECT *
    FROM (SELECT json AS "data"
          FROM read_json_objects('/path/to/test-data/persons.json')) AS "t0"
) AS ext
WHERE CAST((("ext"."data")->>'dept') AS VARCHAR) = 'Engineering'
```

#### Format comparison

| | Inline (`data:`) | File NDJSON | File Array | File Unstructured |
|-|-------------------|-------------|------------|-------------------|
| **FROM clause** | `unnest(CAST('[...]' AS JSON[]))` | `read_json_objects('...')` | `read_json_objects('...')` | `read_json_objects('...')` |
| **File layout** | N/A (inline string) | One object per line | Top-level `[{...},{...}]` | Whitespace-separated objects |
| **DuckDB auto-detect** | N/A | `newline_delimited` | `array` | `unstructured` |
| **M2M transform** | Identical | Identical | Identical | Identical |
| **Parallelizable** | No | ✅ Yes (DuckDB can split by line) | No (must parse whole array) | No |
| **When to use** | Tests, tiny data | Production, large files | Standard JSON APIs | Legacy / irregular files |

**Key insight:** The M2M mapping, compilation, and SQL generation are **completely format-agnostic**.
The only thing that changes is the DuckDB reader function inside the FROM subquery. This is handled
entirely by `DuckDBDialect.renderSourceUrl()` — no other code in the pipeline knows or cares about
the JSON file format.

#### Tests (5 per format × 3 formats = 15 tests)

| Format | Test | File:Line | What it covers |
|--------|------|-----------|----------------|
| **NDJSON** | `testNdjsonProject` | [JsonM2MChainIntegrationTest.java:1051](../../engine/src/test/java/com/gs/legend/test/JsonM2MChainIntegrationTest.java) | L1 project |
| **NDJSON** | `testNdjsonGraphFetch` | [JsonM2MChainIntegrationTest.java:1057](../../engine/src/test/java/com/gs/legend/test/JsonM2MChainIntegrationTest.java) | L1 graphFetch |
| **NDJSON** | `testNdjsonFilter` | [JsonM2MChainIntegrationTest.java:1063](../../engine/src/test/java/com/gs/legend/test/JsonM2MChainIntegrationTest.java) | L1 filter |
| **NDJSON** | `testNdjsonTwoHop` | [JsonM2MChainIntegrationTest.java:1069](../../engine/src/test/java/com/gs/legend/test/JsonM2MChainIntegrationTest.java) | L2 chain |
| **NDJSON** | `testNdjsonTwoHopFilter` | [JsonM2MChainIntegrationTest.java:1075](../../engine/src/test/java/com/gs/legend/test/JsonM2MChainIntegrationTest.java) | L2 filter |
| **Array** | `testArrayProject` | [JsonM2MChainIntegrationTest.java:1083](../../engine/src/test/java/com/gs/legend/test/JsonM2MChainIntegrationTest.java) | L1 project |
| **Array** | `testArrayGraphFetch` | [JsonM2MChainIntegrationTest.java:1089](../../engine/src/test/java/com/gs/legend/test/JsonM2MChainIntegrationTest.java) | L1 graphFetch |
| **Array** | `testArrayFilter` | [JsonM2MChainIntegrationTest.java:1095](../../engine/src/test/java/com/gs/legend/test/JsonM2MChainIntegrationTest.java) | L1 filter |
| **Array** | `testArrayTwoHop` | [JsonM2MChainIntegrationTest.java:1101](../../engine/src/test/java/com/gs/legend/test/JsonM2MChainIntegrationTest.java) | L2 chain |
| **Array** | `testArrayTwoHopFilter` | [JsonM2MChainIntegrationTest.java:1107](../../engine/src/test/java/com/gs/legend/test/JsonM2MChainIntegrationTest.java) | L2 filter |
| **Unstructured** | `testUnstructuredProject` | [JsonM2MChainIntegrationTest.java:1115](../../engine/src/test/java/com/gs/legend/test/JsonM2MChainIntegrationTest.java) | L1 project |
| **Unstructured** | `testUnstructuredGraphFetch` | [JsonM2MChainIntegrationTest.java:1121](../../engine/src/test/java/com/gs/legend/test/JsonM2MChainIntegrationTest.java) | L1 graphFetch |
| **Unstructured** | `testUnstructuredFilter` | [JsonM2MChainIntegrationTest.java:1127](../../engine/src/test/java/com/gs/legend/test/JsonM2MChainIntegrationTest.java) | L1 filter |
| **Unstructured** | `testUnstructuredTwoHop` | [JsonM2MChainIntegrationTest.java:1133](../../engine/src/test/java/com/gs/legend/test/JsonM2MChainIntegrationTest.java) | L2 chain |
| **Unstructured** | `testUnstructuredTwoHopFilter` | [JsonM2MChainIntegrationTest.java:1139](../../engine/src/test/java/com/gs/legend/test/JsonM2MChainIntegrationTest.java) | L2 filter |

---

## 8. M2M2R Tabular Pipeline — project/filter/sort/groupBy/distinct

Beyond graphFetch, M2M supports the full tabular pipeline:

| Operation | Example | Test |
|-----------|---------|------|
| **project** | `Person.all()->project(~[fullName])` | [M2M2RTabularTest.java:215](../../engine/src/test/java/com/gs/legend/test/M2M2RTabularTest.java) |
| **filter** | `->filter(x\|$x.fullName->startsWith('J'))` | [M2M2RTabularTest.java:278](../../engine/src/test/java/com/gs/legend/test/M2M2RTabularTest.java) |
| **sort** | `->sort(asc(~fullName))` | [M2M2RTabularTest.java:375](../../engine/src/test/java/com/gs/legend/test/M2M2RTabularTest.java) |
| **take** | `->sort(asc(~fullName))->take(2)` | [M2M2RTabularTest.java:453](../../engine/src/test/java/com/gs/legend/test/M2M2RTabularTest.java) |
| **groupBy** | `->groupBy(~[group], ~cnt:…->count())` | [M2M2RTabularTest.java:390](../../engine/src/test/java/com/gs/legend/test/M2M2RTabularTest.java) |
| **distinct** | `->distinct()` | [M2M2RTabularTest.java:434](../../engine/src/test/java/com/gs/legend/test/M2M2RTabularTest.java) |
| **extend** | `->extend(~label:…)` | [M2M2RTabularTest.java:479](../../engine/src/test/java/com/gs/legend/test/M2M2RTabularTest.java) |
| **chained M2M** | `PersonSummary.all()` (2-hop) | [M2M2RTabularTest.java:524](../../engine/src/test/java/com/gs/legend/test/M2M2RTabularTest.java) |
| **full pipeline** | project→filter→sort→take | [M2M2RTabularTest.java:501](../../engine/src/test/java/com/gs/legend/test/M2M2RTabularTest.java) |

---

## 9. Relation API Equivalent

### M2M as function composition

```pure
// Source: raw data from DB
function model::employee(): Relation<(firstName:String, lastName:String, age:Integer, dept:String)>[1] {
    #>{CompanyDB.T_EMPLOYEE}#->extend(~[
        firstName: r|$r.FIRST_NAME,
        lastName:  r|$r.LAST_NAME,
        age:       r|$r.AGE,
        dept:      r|$r.DEPT
    ])
}

// L1: Employee → StaffMember (computed fullName)
function model::staffMember(): Relation<(fullName:String, dept:String)>[1] {
    model::employee()
      ->extend(~fullName: r|$r.firstName + ' ' + $r.lastName)
      ->select(~[fullName, dept])
}

// L2: StaffMember → StaffCard (toUpper)
function model::staffCard(): Relation<(displayName:String, department:String)>[1] {
    model::staffMember()
      ->extend(~displayName: r|$r.fullName->toUpper())
      ->rename(~dept, ~department)
      ->select(~[displayName, department])
}

// L3: StaffCard → DirectoryEntry (concat)
function model::directoryEntry(): Relation<(entry:String)>[1] {
    model::staffCard()
      ->extend(~entry: r|$r.displayName + ' - ' + $r.department)
      ->select(~[entry])
}
```

The SQL optimizer flattens all intermediate layers — `model::directoryEntry()` produces a single query hitting T_EMPLOYEE directly.

### M2M with filter as `->filter()`

```pure
function model::activeStaff(): Relation<(fullName:String, dept:String)>[1] {
    model::employee()
      ->filter(r|$r.isActive == true)    // mapping-level filter
      ->extend(~fullName: r|$r.firstName + ' ' + $r.lastName)
      ->select(~[fullName, dept])
}
```

---

## 10. Key Files in legend-lite

| File | Role |
|------|------|
| **GetAllChecker.java** | `compileM2MGetAll` — resolves `~src`, compiles `$src.prop` expressions |
| **MappingResolver.java** | `resolveM2MAssociationNavigations()` — wires `$src.assocProp` to JOINs |
| **PlanGenerator.java** | Emits extend/select for M2M, correlated subquery for deep fetch |
| **M2MIntegrationTest.java** | 18 tests: flat, deep-fetch 1-to-1, 1-to-many, multiplicity violation |
| **M2MChainIntegrationTest.java** | 38 tests: L1–L4 chains, filters, edge cases, traverse columns, deep fetch, disjoint 1-to-many |
| **JsonM2MChainIntegrationTest.java** | 52 tests: L1–L4 chains, filters, edge cases, multiple JSON sources, 3 file formats (NDJSON/Array/Unstructured) |
| **M2M2RTabularTest.java** | 15 tests: project, filter, sort, groupBy, distinct, extend, chained M2M |

---

## 11. Comprehensive Test Matrix

### M2M→R (Relational source)

| Category | Test | File:Line | What it proves |
|----------|------|-----------|----------------|
| **L1 project** | `testProjectSimple` | [M2MChainIntegrationTest.java:430](../../engine/src/test/java/com/gs/legend/test/M2MChainIntegrationTest.java) | Single-hop computed + passthrough |
| **L1 graphFetch** | `testGraphFetch` | [M2MChainIntegrationTest.java:452](../../engine/src/test/java/com/gs/legend/test/M2MChainIntegrationTest.java) | JSON serialization |
| **L1 mapping filter** | `testProjectWithFilter` | [M2MChainIntegrationTest.java:465](../../engine/src/test/java/com/gs/legend/test/M2MChainIntegrationTest.java) | `~filter` excludes rows |
| **L1 user filter** | `testProjectWithUserFilter` | [M2MChainIntegrationTest.java:489](../../engine/src/test/java/com/gs/legend/test/M2MChainIntegrationTest.java) | User `->filter()` on M2M output |
| **L1 filter compose** | `testProjectUserFilterPlusMappingFilter` | [M2MChainIntegrationTest.java:499](../../engine/src/test/java/com/gs/legend/test/M2MChainIntegrationTest.java) | Both compose in SQL |
| **L2 project** | `testProjectTwoHop` | [M2MChainIntegrationTest.java:514](../../engine/src/test/java/com/gs/legend/test/M2MChainIntegrationTest.java) | 2-hop chain flattens |
| **L2 filter@L1** | `testProjectTwoHopWithFilterAtL1` | [M2MChainIntegrationTest.java:539](../../engine/src/test/java/com/gs/legend/test/M2MChainIntegrationTest.java) | Filter propagates through chain |
| **L3 project** | `testProjectThreeHop` | [M2MChainIntegrationTest.java:581](../../engine/src/test/java/com/gs/legend/test/M2MChainIntegrationTest.java) | 3-hop chain, single SQL |
| **L3 filter** | `testProjectThreeHopWithUserFilter` | [M2MChainIntegrationTest.java:607](../../engine/src/test/java/com/gs/legend/test/M2MChainIntegrationTest.java) | User filter at L3 |
| **L4 project** | `testProjectFourHop` | [M2MChainIntegrationTest.java:639](../../engine/src/test/java/com/gs/legend/test/M2MChainIntegrationTest.java) | 4-hop chain, single SQL |
| **L4 graphFetch** | `testGraphFetchFourHop` | [M2MChainIntegrationTest.java:651](../../engine/src/test/java/com/gs/legend/test/M2MChainIntegrationTest.java) | 4-hop JSON serialization |
| **L4 filter** | `testProjectFourHopWithFilter` | [M2MChainIntegrationTest.java:663](../../engine/src/test/java/com/gs/legend/test/M2MChainIntegrationTest.java) | User filter at L4 |
| **Edge: empty** | `testFilterEliminatesAll` | [M2MChainIntegrationTest.java:678](../../engine/src/test/java/com/gs/legend/test/M2MChainIntegrationTest.java) | Filter produces 0 rows |
| **Edge: single** | `testFilterKeepsOne` | [M2MChainIntegrationTest.java:685](../../engine/src/test/java/com/gs/legend/test/M2MChainIntegrationTest.java) | Exactly 1 match |
| **Edge: compound** | `testCompoundFilter` | [M2MChainIntegrationTest.java:693](../../engine/src/test/java/com/gs/legend/test/M2MChainIntegrationTest.java) | `age > 25 && dept == 'Engineering'` |
| **Edge: sort** | `testSortThroughChain` | [M2MChainIntegrationTest.java:703](../../engine/src/test/java/com/gs/legend/test/M2MChainIntegrationTest.java) | Sort on M2M output |
| **Edge: 2-hop sort** | `testTwoHopSort` | [M2MChainIntegrationTest.java:712](../../engine/src/test/java/com/gs/legend/test/M2MChainIntegrationTest.java) | Sort at L2 |
| **Traverse 1-hop** | `testProjectSingleHopTraverse` | [M2MChainIntegrationTest.java:622](../../engine/src/test/java/com/gs/legend/test/M2MChainIntegrationTest.java) | Scalar join columns through M2M |
| **Traverse 2-hop** | `testProjectMultiHopTraverse` | [M2MChainIntegrationTest.java:662](../../engine/src/test/java/com/gs/legend/test/M2MChainIntegrationTest.java) | 2-hop join chain → M2M |
| **Traverse + filter** | `testProjectTraverseWithFilter` | [M2MChainIntegrationTest.java:652](../../engine/src/test/java/com/gs/legend/test/M2MChainIntegrationTest.java) | Filter on traverse column |
| **Mixed** | `testGraphFetchMixedTraverseAndAssociation` | [M2MChainIntegrationTest.java:673](../../engine/src/test/java/com/gs/legend/test/M2MChainIntegrationTest.java) | Scalar + nested in one query |
| **Deep fetch** | `testDeepFetchViaAssociation` | [M2MChainIntegrationTest.java:880](../../engine/src/test/java/com/gs/legend/test/M2MChainIntegrationTest.java) | 1-to-many nested objects |
| **Deep pruning** | `testDeepFetchRootOnlyNoJoin` | [M2MChainIntegrationTest.java:896](../../engine/src/test/java/com/gs/legend/test/M2MChainIntegrationTest.java) | No subquery if nested not requested |
| **Disjoint 2×1:N** | `testDisjointOneToMany` | [M2MChainIntegrationTest.java:944](../../engine/src/test/java/com/gs/legend/test/M2MChainIntegrationTest.java) | dept + projects in one fetch |
| **Disjoint dept-only** | `testDisjointOnlyDept` | [M2MChainIntegrationTest.java:967](../../engine/src/test/java/com/gs/legend/test/M2MChainIntegrationTest.java) | Only dept → 0 project subqueries |
| **Disjoint proj-only** | `testDisjointOnlyProjects` | [M2MChainIntegrationTest.java:980](../../engine/src/test/java/com/gs/legend/test/M2MChainIntegrationTest.java) | Only projects → 0 dept subqueries |
| **Disjoint root-only** | `testDisjointRootOnly` | [M2MChainIntegrationTest.java:993](../../engine/src/test/java/com/gs/legend/test/M2MChainIntegrationTest.java) | Root only → 0 subqueries |
| **Disjoint 0-proj** | `testDisjointZeroProjects` | [M2MChainIntegrationTest.java:1007](../../engine/src/test/java/com/gs/legend/test/M2MChainIntegrationTest.java) | Employee with 0 projects |
| **Disjoint multi-proj** | `testDisjointMultipleProjects` | [M2MChainIntegrationTest.java:1019](../../engine/src/test/java/com/gs/legend/test/M2MChainIntegrationTest.java) | Alice: 2 projects + filter |

### M2M→J (JSON source)

| Category | Test | File:Line | What it proves |
|----------|------|-----------|----------------|
| **L1 project** | `testProjectAllProps` | [JsonM2MChainIntegrationTest.java:360](../../engine/src/test/java/com/gs/legend/test/JsonM2MChainIntegrationTest.java) | All 5 properties from JSON |
| **L1 graphFetch** | `testGraphFetch` | [JsonM2MChainIntegrationTest.java:389](../../engine/src/test/java/com/gs/legend/test/JsonM2MChainIntegrationTest.java) | JSON → graphFetch → JSON |
| **L1 filter string** | `testProjectFilterStringEq` | [JsonM2MChainIntegrationTest.java:417](../../engine/src/test/java/com/gs/legend/test/JsonM2MChainIntegrationTest.java) | String equality on M2M output |
| **L1 filter numeric** | `testProjectFilterNumeric` | [JsonM2MChainIntegrationTest.java:427](../../engine/src/test/java/com/gs/legend/test/JsonM2MChainIntegrationTest.java) | `age > 40` |
| **L1 mapping filter** | `testProjectMappingFilter` | [JsonM2MChainIntegrationTest.java:437](../../engine/src/test/java/com/gs/legend/test/JsonM2MChainIntegrationTest.java) | `~filter $src.active == true` |
| **L1 filter compose** | `testProjectMappingAndUserFilter` | [JsonM2MChainIntegrationTest.java:449](../../engine/src/test/java/com/gs/legend/test/JsonM2MChainIntegrationTest.java) | mapping + user filter |
| **L1 numeric** | `testProjectNumericComputation` | [JsonM2MChainIntegrationTest.java:483](../../engine/src/test/java/com/gs/legend/test/JsonM2MChainIntegrationTest.java) | `age*12`, `salary*100` |
| **L2 project** | `testProjectTwoHop` | [JsonM2MChainIntegrationTest.java:502](../../engine/src/test/java/com/gs/legend/test/JsonM2MChainIntegrationTest.java) | 2-hop from JSON |
| **L2 filter@L1** | `testProjectTwoHopWithFilterAtL1` | [JsonM2MChainIntegrationTest.java:526](../../engine/src/test/java/com/gs/legend/test/JsonM2MChainIntegrationTest.java) | Filter at L1, query at L2 |
| **L2 filter computed** | `testProjectTwoHopFilterOnComputed` | [JsonM2MChainIntegrationTest.java:559](../../engine/src/test/java/com/gs/legend/test/JsonM2MChainIntegrationTest.java) | Filter on `contains('BOB')` |
| **L3 project** | `testProjectThreeHop` | [JsonM2MChainIntegrationTest.java:576](../../engine/src/test/java/com/gs/legend/test/JsonM2MChainIntegrationTest.java) | 3-hop from JSON |
| **L4 project** | `testProjectFourHop` | [JsonM2MChainIntegrationTest.java:622](../../engine/src/test/java/com/gs/legend/test/JsonM2MChainIntegrationTest.java) | 4-hop from JSON |
| **L4 filter** | `testProjectFourHopWithFilter` | [JsonM2MChainIntegrationTest.java:646](../../engine/src/test/java/com/gs/legend/test/JsonM2MChainIntegrationTest.java) | Filter at L4 |
| **Edge: single row** | `testSingleElement` | [JsonM2MChainIntegrationTest.java:661](../../engine/src/test/java/com/gs/legend/test/JsonM2MChainIntegrationTest.java) | 1-element JSON array |
| **Edge: empty** | `testFilterEliminatesAll` | [JsonM2MChainIntegrationTest.java:682](../../engine/src/test/java/com/gs/legend/test/JsonM2MChainIntegrationTest.java) | 0 results |
| **Edge: compound** | `testCompoundFilter` | [JsonM2MChainIntegrationTest.java:697](../../engine/src/test/java/com/gs/legend/test/JsonM2MChainIntegrationTest.java) | `age > 25 && dept == 'Engineering'` |
| **Multi-source** | `testBothSourcesIndependent` | [JsonM2MChainIntegrationTest.java:880](../../engine/src/test/java/com/gs/legend/test/JsonM2MChainIntegrationTest.java) | 2 JSON classes in same runtime |
| **NDJSON: L1** | `testNdjsonProject` | [JsonM2MChainIntegrationTest.java:1051](../../engine/src/test/java/com/gs/legend/test/JsonM2MChainIntegrationTest.java) | L1 from NDJSON file |
| **NDJSON: L2** | `testNdjsonTwoHop` | [JsonM2MChainIntegrationTest.java:1069](../../engine/src/test/java/com/gs/legend/test/JsonM2MChainIntegrationTest.java) | 2-hop from NDJSON |
| **NDJSON: filter** | `testNdjsonFilter` | [JsonM2MChainIntegrationTest.java:1063](../../engine/src/test/java/com/gs/legend/test/JsonM2MChainIntegrationTest.java) | Filter from NDJSON |
| **Array: L1** | `testArrayProject` | [JsonM2MChainIntegrationTest.java:1083](../../engine/src/test/java/com/gs/legend/test/JsonM2MChainIntegrationTest.java) | L1 from JSON array file |
| **Array: L2** | `testArrayTwoHop` | [JsonM2MChainIntegrationTest.java:1101](../../engine/src/test/java/com/gs/legend/test/JsonM2MChainIntegrationTest.java) | 2-hop from JSON array |
| **Array: filter** | `testArrayFilter` | [JsonM2MChainIntegrationTest.java:1095](../../engine/src/test/java/com/gs/legend/test/JsonM2MChainIntegrationTest.java) | Filter from JSON array |
| **Unstructured: L1** | `testUnstructuredProject` | [JsonM2MChainIntegrationTest.java:1115](../../engine/src/test/java/com/gs/legend/test/JsonM2MChainIntegrationTest.java) | L1 from unstructured file |
| **Unstructured: L2** | `testUnstructuredTwoHop` | [JsonM2MChainIntegrationTest.java:1133](../../engine/src/test/java/com/gs/legend/test/JsonM2MChainIntegrationTest.java) | 2-hop from unstructured |
| **Unstructured: filter** | `testUnstructuredFilter` | [JsonM2MChainIntegrationTest.java:1127](../../engine/src/test/java/com/gs/legend/test/JsonM2MChainIntegrationTest.java) | Filter from unstructured |

### M2M2R Tabular (basic M2M→R, full pipeline)

| Category | Test | File:Line |
|----------|------|-----------|
| **project** | `testProjectFullName` | [M2M2RTabularTest.java:215](../../engine/src/test/java/com/gs/legend/test/M2M2RTabularTest.java) |
| **multi-col** | `testProjectMultipleColumns` | [M2M2RTabularTest.java:236](../../engine/src/test/java/com/gs/legend/test/M2M2RTabularTest.java) |
| **conditional** | `testConditionalAgeGroup` | [M2M2RTabularTest.java:309](../../engine/src/test/java/com/gs/legend/test/M2M2RTabularTest.java) |
| **sort** | `testSortOnM2MColumn` | [M2M2RTabularTest.java:375](../../engine/src/test/java/com/gs/legend/test/M2M2RTabularTest.java) |
| **groupBy** | `testGroupByOnConditional` | [M2M2RTabularTest.java:390](../../engine/src/test/java/com/gs/legend/test/M2M2RTabularTest.java) |
| **distinct** | `testDistinctOnM2M` | [M2M2RTabularTest.java:434](../../engine/src/test/java/com/gs/legend/test/M2M2RTabularTest.java) |
| **chained** | `testChainedM2M` | [M2M2RTabularTest.java:524](../../engine/src/test/java/com/gs/legend/test/M2M2RTabularTest.java) |

---

## 12. Implementation Notes

**Status: ✅ Implemented.**

M2M in Relation API is just function composition — the "source mapping" becomes a function call, and the "M2M transform" becomes additional `extend()` + `select()`. No special primitive needed.

The Relation API is arguably cleaner for M2M because the data flow is explicit: `rawData() -> transform() -> output()`. In the mapping DSL, the `~src` directive and `$src.property` syntax are implicit — you have to know which mapping provides the source class.

**Key architectural insight:** The M2M→R and M2M→J paths share the same M2M compilation logic (GetAllChecker, MappingResolver, PlanGenerator). The only difference is how the leaf source is resolved — relational mapping produces `FROM "TABLE"`, inline JSON produces `FROM (SELECT unnest(CAST('...' AS JSON[])))`, file JSON produces `FROM (SELECT json AS "data" FROM read_json_objects('...'))`. Everything above the leaf is identical.
