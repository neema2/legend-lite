# Q8: what legend-engine answers for an Integer property over a DECIMAL column (2026-11-19)

The PR #118 follow-up's Q8 (`claude/followup-q8-q9`) made lite's decode read an Integer property's DECIMAL-slot cell
by its value: a whole value is its Integer, a fraction is refused by name, on every route. The orchestrator's ruling
(2)(c) asked for upstream's answer to be read before any further change. This record keeps that answer as evidence.

The answers were read from legend-engine 4.145.0's jars by slice R's engine runner (`claude/presentation-r`), not
recalled. The register row they call for (R-a, below) goes to the user, with the fixer's recommendation.

## How it was asked

- The model is the one below, on H2 (legend-engine's own patched 2.1.214).
- Each lambda went in as a pure/v1 execute request, the way Studio sends one:
  - the function from legend-engine's `parseLambda(code, "", false)`;
  - `"mapping":"p::M"`, and `"runtime":{"_type":"runtimePointer","runtime":"p::RT"}`;
  - the model as `{"_type":"text"}`, `"clientVersion":"vX_X_X"`, a `BaseExecutionContext`.
- It was planned by `Execute.buildPlan`'s calls, the plan's JSON executed by `PlanExecutor`, and the answer written as
  `ResultManager` writes it (`SerializationFormat.DEFAULT`).
- Each lambda ran in a fresh JVM: the seed's `CREATE TABLE` has no `DROP`.

```
###Pure
Class p::S { id: Integer[1]; f: Integer[1]; n: Integer[1]; }
###Relational
Database p::DB ( Table W (ID INTEGER PRIMARY KEY, F DECIMAL(10,2), N DECIMAL(10,0)) )
###Mapping
Mapping p::M ( p::S: Relational { ~mainTable [p::DB] W id: [p::DB] W.ID, f: [p::DB] W.F, n: [p::DB] W.N } )
###Connection
RelationalDatabaseConnection p::C { store: p::DB; type: H2; specification: LocalH2 { testDataSetupSqls: ['CREATE TABLE W (ID INTEGER PRIMARY KEY, F DECIMAL(10,2), N DECIMAL(10,0))', 'INSERT INTO W VALUES (1, 5.50, 5)']; }; auth: DefaultH2; }
###Runtime
Runtime p::RT { mappings: [p::M]; connections: [ p::DB: [ c0: p::C ] ]; }
```

## The answers

### 1. The relation: `|p::S.all()->project([x|$x.f, x|$x.n], ['f', 'n'])`

generatePlan answered 200 and execute 200. The bytes, with the trace id masked:

```
{"builder": {"_type":"tdsBuilder","columns":[{"name":"f","type":"Integer","relationalType":"DECIMAL(10,2)"},{"name":"n","type":"Integer","relationalType":"DECIMAL(10,0)"}]}, "activities": [{"_type":"relational","comment":"-- \"executionTraceID\" : \"<id>\"","sql":"select \"root\".F as \"f\", \"root\".N as \"n\" from W as \"root\""}], "result" : {"columns" : ["f","n"], "rows" : [{"values": [5.50,5]}]}}
```

The fraction comes back as the driver hands it, `5.50`, in a column the builder types Integer: no rounding, no
refusal. The whole value comes back as `5`.

### 2. The object read: `|p::S.all()->graphFetch(#{p::S{f, n}}#)->serialize(#{p::S{f, n}}#)`

generatePlan answered 200; execute answered 500, refusing when the object is read:

```
{"code":-1,"message":"RuntimeException: Error reading in property 'f' of type Integer from SQL column of type 'DECIMAL'.","status":"error","trace":"java.lang.RuntimeException: Error reading in property 'f' of type Integer from SQL column of type 'DECIMAL'.\n<frames>"}
```

The variation over `n` alone (`|p::S.all()->graphFetch(#{p::S{n}}#)->serialize(#{p::S{n}}#)`, `N` = 5 at
DECIMAL(10,0)) is refused the same way: `"RuntimeException: Error reading in property 'n' of type Integer from SQL
column of type 'DECIMAL'."`.

It is thrown by the plan's generated Java, in `Specifics.getResultSetPropertyGetterForIntegerProperty`
(`Specifics.java:203`). That getter is called from `prepare`, before any row is read. So the column's SQL type decides,
not the value: every DECIMAL column under an Integer property is refused, a whole one included.

### 3. A test body: `|assertEq(5, p::S.all()->filter(x|$x.id == 1)->toOne().f)`

Through the execute route, generatePlan answers 500, because legend-engine cannot plan it:

```
{"code":-1,"message":"PureExecutionException: Execution error at (resource:/core_external_language_java/generation/expressionGeneration.pure line:117 column:22), \"Match failure: StoreMappingClusteredValueSpecificationObject instanceOf StoreMappingClusteredValueSpecification\"","status":"error",...}
```

The runner has no legend-pure test runner over a model, so upstream gives no verdict for the body itself.

## Lite after Q8, against upstream

| Route | legend-engine 4.145.0 | lite after Q8 |
|---|---|---|
| Relation, a whole DECIMAL (`n` = 5) | `5` | `5` |
| Relation, a fraction (`f` = 5.50) | `5.50`, under a column typed Integer | refused by name (the decode) |
| Object read, any DECIMAL column | refused by the column's type | decided by the value: 5 answered, a fraction refused (lite's graphFetch path not probed) |
| A test body under the judges | not planned | HOST: 5, the fraction refused; DATABASE compares in SQL ("expected 5, got 5.50"); BOTH disagree on the fraction (Q8's open finding) |

Upstream has no single answer: its relation passes the value through, and its object read refuses the column type.
Lite takes one rule, by the value, on every route. Measured against upstream, that is two divergences:
- the relation's fraction, which lite refuses and legend-engine answers;
- the object read's whole decimal, which lite answers and legend-engine refuses.

## R-a, for the user's ruling (not yet a row of docs/SEMANTICS_REGISTER.md)

"An Integer property over a DECIMAL column: lite decides by the value on every route (a whole value is its Integer, a
fraction is refused by name). legend-engine's relation answers the decimal as written (5.50 under an Integer column);
its object read refuses the column's type outright."

The fixer recommends keeping lite's one rule, ledgered as this row with both of upstream's answers quoted. An Integer
column holding 5.50 is a wrong type, and upstream itself refuses it on its object road. The alternatives:
- follow upstream per route: a route-dependent decode, and a relation column typed Integer that holds a Decimal;
- refuse in the plan for every judge and route: a planner change.
