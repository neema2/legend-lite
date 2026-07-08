# The Corpus Scoreboard

The engine test corpus (2721 tests, 94+ classes) runs AS-IS against core via
the QueryService bridge (docs/PHASE_K_EXECUTION.md). This file is the run
history and bucket ledger; `tools/scoreboard.py --record` appends a run after:

    mvn -pl engine test -Dmaven.test.failure.ignore=true

**THE LOOP RULE:** fix → engine suite → scoreboard --record → verify deltas →
commit code AND scoreboard row together. Never commit a "fix" the scoreboard
has not confirmed.

Bucket meanings:
- **H: class sources / property nav / mappings** — Phase H's parity meter
  (TypedGetAll, association navigation, mapping resolution, legacyNavigate's
  unbound T). Burns down when Phase H lands, not before.
- **CORE: scalar/agg function registrations** — the Scalars/Aggregates long
  tail; each item is a small identity-keyed registration with corpus-verified
  expected outputs.
- **CORE: unlowered constructs** — real lowering work (serialize/GRAPH,
  TypedUserCall = user-function inlining [design moment, not a hack],
  sortBy, match, scalar literals).
- **CORE(G): overload/typing gaps** — catalog signatures the corpus
  exercises that G lacks (fold/extend 3-arg shapes, JoinKind values).
- **CORE(parse): query syntax gaps** — concretely: trailing-token cases and
  FQN references inside collection literals ([a::b::C.VAL]).
- **FIXTURE: unknown refs** — model shapes to diagnose (test::Person in
  RelationalMappingIntegrationTest inner fixtures; T_EVENTS/EventDatabase).
- **OTHER** — incl. one suspected G bug (ColSpecArray<X⊆T> vs ColSpec).

Governance tail (decisions, not code): ~86 assertion FAILURES are mostly
tests that bypass the bridge and pin ENGINE internals (FlatSql suites
asserting engine's own SQL text; PureCompileException type expectations) —
they need re-pin-to-core or exclude decisions. The asOfJoin prefix failures
are our DOCUMENTED deliberate divergence behaving as expected.

## History

### Run 2026-07-05 @ baseline (pre-fix, commit 026ef9d era)

| tests | pass | failures | errors | skipped | green classes |
|---|---|---|---|---|---|
| 2721 | **804** | 80 | 1838 | 19 | 130 |

Dominated by four systemic causes: ModelElement missing from the catalog
(1093), synthesized bare Boolean (202), lambda-root queries (69), bare-name
corpus queries (~150).

### Run 2026-07-05 @ ce63bf7 (post systemic fixes)

| tests | pass | failures | errors | skipped | green classes |
|---|---|---|---|---|---|
| 2721 | **1027** | 86 | 1589 | 19 | 152 |

| bucket | exception lines |
|---|---|
| H: class sources / property nav / mappings | 577 |
| CORE: scalar/agg function registrations | 487 |
| CORE: unlowered constructs | 186 |
| CORE(G): overload/typing gaps | 121 |
| CORE(parse): query syntax gaps | 108 |
| FIXTURE: unknown refs (to diagnose) | 77 |
| OTHER | 37 |

All three systemic error families at zero; bare-name errors moved into the
H bucket where they belong (TypedGetAll 343→412 across rounds).

### Run 2026-07-05 @ 4d0c0ed

| tests | pass | failures | errors | skipped | green classes |
|---|---|---|---|---|---|
| 2721 | **1176** | 94 | 1432 | 19 | 169 |

| bucket | exception lines |
|---|---|
| H: class sources / property nav / mappings | 578 |
| CORE: scalar/agg function registrations | 380 |
| CORE: unlowered constructs | 197 |
| CORE(parse): query syntax gaps | 117 |
| FIXTURE: unknown refs (to diagnose) | 73 |
| OTHER | 45 |
| CORE(G): overload/typing gaps | 25 |

Top detail:
```
  412  lowering not yet implemented for TypedGetAll
   53  scalar lowering not yet implemented for TypedSerialize
   40  class test::Person has no property 'firm'
   28  'test::Person' is not a known class, mapping, runtime, connection, or 
   24  unknown table 'T_EVENTS' in database 'EventDatabase'
   22  lowering not yet implemented for TypedSortBy
   22  unbound type variable T
   18  scalar lowering not yet implemented for TypedEnumValue
   16  scalar lowering not yet implemented for TypedNewInstance
   16  lowering not yet implemented for TypedUserCall
   14  [1:5] trailing tokens after expression: VALID_STRING ('x')
   14  class model::Person has no property 'addresses'
   14  class model::Emp has no property 'dept'
   13  scalar lowering not yet implemented for TypedMatch
   12  [1:5] trailing tokens after expression: VALID_STRING ('a')
   11  class test::Person has no property 'addresses'
   10  graphFetch tree: class test::Person has no property 'addresses'
   10  no aggregate lowering registered for resolved overload 'size'
   10  no scalar lowering registered for resolved overload 'toString' with 1 
   10  'test::Order' is not a known class, mapping, runtime, connection, or d
    9  no overload of 'concatenate' matches 2 argument(s) of these shapes
    9  no compiled mapping function for class fetch: test::Person
    8  [plangen-c0954a] Not yet ported: TypedPropertyAccess (stage-3-relation
    8  no aggregate lowering registered for resolved overload 'joinStrings'
    8  scalar lowering not yet implemented for TypedIf
```

### Run 2026-07-05 @ 976c408

| tests | pass | failures | errors | skipped | green classes |
|---|---|---|---|---|---|
| 2721 | **1193** | 94 | 1415 | 19 | 173 |

| bucket | exception lines |
|---|---|
| H: class sources / property nav / mappings | 580 |
| CORE: scalar/agg function registrations | 389 |
| CORE: unlowered constructs | 211 |
| CORE(parse): query syntax gaps | 75 |
| FIXTURE: unknown refs (to diagnose) | 73 |
| OTHER | 45 |
| CORE(G): overload/typing gaps | 25 |

Top detail:
```
  413  lowering not yet implemented for TypedGetAll
   53  scalar lowering not yet implemented for TypedSerialize
   40  class test::Person has no property 'firm'
   28  'test::Person' is not a known class, mapping, runtime, connection, or 
   24  unknown table 'T_EVENTS' in database 'EventDatabase'
   22  lowering not yet implemented for TypedSortBy
   22  unbound type variable T
   18  scalar lowering not yet implemented for TypedEnumValue
   18  scalar lowering not yet implemented for TypedNewInstance
   16  lowering not yet implemented for TypedUserCall
   14  class model::Person has no property 'addresses'
   14  class model::Emp has no property 'dept'
   13  scalar lowering not yet implemented for TypedMatch
   11  class test::Person has no property 'addresses'
   11  scalar lowering not yet implemented for TypedIf
   10  graphFetch tree: class test::Person has no property 'addresses'
   10  scalar lowering not yet implemented for TypedFilter
   10  no aggregate lowering registered for resolved overload 'size'
   10  no scalar lowering registered for resolved overload 'toString' with 1 
   10  'test::Order' is not a known class, mapping, runtime, connection, or d
    9  no overload of 'concatenate' matches 2 argument(s) of these shapes
    9  no compiled mapping function for class fetch: test::Person
    9  no scalar lowering registered for resolved overload 'zip' with 2 param
    8  [plangen-c0954a] Not yet ported: TypedPropertyAccess (stage-3-relation
    8  no aggregate lowering registered for resolved overload 'joinStrings'
```

### Run 2026-07-05 @ 00f0b33

| tests | pass | failures | errors | skipped | green classes |
|---|---|---|---|---|---|
| 2721 | **1400** | 106 | 1196 | 19 | 188 |

| bucket | exception lines |
|---|---|
| H: class sources / property nav / mappings | 580 |
| CORE: unlowered constructs | 211 |
| CORE: scalar/agg function registrations | 98 |
| OTHER | 85 |
| CORE(parse): query syntax gaps | 76 |
| FIXTURE: unknown refs (to diagnose) | 73 |
| CORE(G): overload/typing gaps | 25 |

Top detail:
```
  413  lowering not yet implemented for TypedGetAll
   53  scalar lowering not yet implemented for TypedSerialize
   40  class test::Person has no property 'firm'
   31  no SQL type for Pure type meta::pure::metamodel::type::Any at the lowe
   28  'test::Person' is not a known class, mapping, runtime, connection, or 
   24  unknown table 'T_EVENTS' in database 'EventDatabase'
   22  lowering not yet implemented for TypedSortBy
   22  unbound type variable T
   18  scalar lowering not yet implemented for TypedEnumValue
   18  scalar lowering not yet implemented for TypedNewInstance
   16  lowering not yet implemented for TypedUserCall
   14  class model::Person has no property 'addresses'
   14  class model::Emp has no property 'dept'
   13  scalar lowering not yet implemented for TypedMatch
   11  class test::Person has no property 'addresses'
   11  scalar lowering not yet implemented for TypedIf
   10  graphFetch tree: class test::Person has no property 'addresses'
   10  scalar lowering not yet implemented for TypedFilter
   10  'test::Order' is not a known class, mapping, runtime, connection, or d
    9  no overload of 'concatenate' matches 2 argument(s) of these shapes
    9  no compiled mapping function for class fetch: test::Person
    8  [plangen-c0954a] Not yet ported: TypedPropertyAccess (stage-3-relation
    7  [plangen-c0954a] Not yet ported: TypedProject (stage-3-relation). Vari
    7  scalar lowering not yet implemented for TypedUserCall
    6  class test::Person has no property 'dept'
```

### Run 2026-07-06 @ 2c225e2

| tests | pass | failures | errors | skipped | green classes |
|---|---|---|---|---|---|
| 2721 | **1402** | 106 | 1194 | 19 | 188 |

| bucket | exception lines |
|---|---|
| H: class sources / property nav / mappings | 541 |
| CORE: unlowered constructs | 232 |
| CORE: scalar/agg function registrations | 98 |
| CORE(parse): query syntax gaps | 92 |
| OTHER | 85 |
| FIXTURE: unknown refs (to diagnose) | 73 |
| CORE(G): overload/typing gaps | 25 |

Top detail:
```
  501  lowering not yet implemented for TypedGetAll
   70  scalar lowering not yet implemented for TypedSerialize
   31  no SQL type for Pure type meta::pure::metamodel::type::Any at the lowe
   28  'test::Person' is not a known class, mapping, runtime, connection, or 
   26  unbound type variable T
   26  lowering not yet implemented for TypedSortBy
   24  unknown table 'T_EVENTS' in database 'EventDatabase'
   18  scalar lowering not yet implemented for TypedEnumValue
   18  scalar lowering not yet implemented for TypedNewInstance
   17  in call to 'equal', argument 1: expected at most one value, got many (
   16  lowering not yet implemented for TypedUserCall
   13  scalar lowering not yet implemented for TypedMatch
   11  scalar lowering not yet implemented for TypedIf
   10  scalar lowering not yet implemented for TypedFilter
   10  'test::Order' is not a known class, mapping, runtime, connection, or d
    9  no overload of 'concatenate' matches 2 argument(s) of these shapes
    9  no compiled mapping function for class fetch: test::Person
    8  [plangen-c0954a] Not yet ported: TypedPropertyAccess (stage-3-relation
    7  [plangen-c0954a] Not yet ported: TypedProject (stage-3-relation). Vari
    7  scalar lowering not yet implemented for TypedUserCall
    6  no aggregate lowering registered for resolved overload 'wavg'
    6  unknown SQL data type: 'JSON'
    6  Cannot invoke "String.equals(Object)" because "db" is null
    5  in call to 'concatenate', argument 1: expected a Relation, got Integer
    5  scalar lowering not yet implemented for TypedTds
```

### Run 2026-07-06 @ 4ce5869

| tests | pass | failures | errors | skipped | green classes |
|---|---|---|---|---|---|
| 2721 | **1402** | 106 | 1194 | 19 | 188 |

| bucket | exception lines |
|---|---|
| H: class sources / property nav / mappings | 564 |
| CORE: unlowered constructs | 234 |
| CORE: scalar/agg function registrations | 98 |
| OTHER | 79 |
| CORE(parse): query syntax gaps | 73 |
| FIXTURE: unknown refs (to diagnose) | 73 |
| CORE(G): overload/typing gaps | 25 |

Top detail:
```
  524  lowering not yet implemented for TypedGetAll
   70  scalar lowering not yet implemented for TypedSerialize
   31  no SQL type for Pure class meta::pure::metamodel::type::Any at the low
   28  'test::Person' is not a known class, mapping, runtime, connection, or 
   27  lowering not yet implemented for TypedSortBy
   26  unbound type variable T
   24  unknown table 'T_EVENTS' in database 'EventDatabase'
   18  scalar lowering not yet implemented for TypedEnumValue
   18  scalar lowering not yet implemented for TypedNewInstance
   16  lowering not yet implemented for TypedUserCall
   13  scalar lowering not yet implemented for TypedMatch
   11  scalar lowering not yet implemented for TypedIf
   10  scalar lowering not yet implemented for TypedFilter
   10  'test::Order' is not a known class, mapping, runtime, connection, or d
    9  no overload of 'concatenate' matches 2 argument(s) of these shapes
    9  no compiled mapping function for class fetch: test::Person
    8  [plangen-c0954a] Not yet ported: TypedPropertyAccess (stage-3-relation
    8  scalar lowering not yet implemented for TypedUserCall
    7  [plangen-c0954a] Not yet ported: TypedProject (stage-3-relation). Vari
    6  no aggregate lowering registered for resolved overload 'wavg'
    6  unknown SQL data type: 'JSON'
    5  in call to 'concatenate', argument 1: expected a Relation, got Integer
    5  scalar lowering not yet implemented for TypedTds
    5  multi-column pivot is not lowered yet
    5  [6:86] expected BRACKET_CLOSE but found PATH_SEPARATOR ('::')
```

### Run 2026-07-06 @ 9ff15ac

| tests | pass | failures | errors | skipped | green classes |
|---|---|---|---|---|---|
| 2721 | **1402** | 106 | 1194 | 19 | 188 |

| bucket | exception lines |
|---|---|
| H: class sources / property nav / mappings | 564 |
| CORE: unlowered constructs | 234 |
| CORE: scalar/agg function registrations | 98 |
| OTHER | 79 |
| CORE(parse): query syntax gaps | 73 |
| FIXTURE: unknown refs (to diagnose) | 73 |
| CORE(G): overload/typing gaps | 25 |

Top detail:
```
  524  lowering not yet implemented for TypedGetAll
   70  scalar lowering not yet implemented for TypedSerialize
   31  no SQL type for Pure class meta::pure::metamodel::type::Any at the low
   28  'test::Person' is not a known class, mapping, runtime, connection, or 
   27  lowering not yet implemented for TypedSortBy
   26  unbound type variable T
   24  unknown table 'T_EVENTS' in database 'EventDatabase'
   18  scalar lowering not yet implemented for TypedEnumValue
   18  scalar lowering not yet implemented for TypedNewInstance
   16  lowering not yet implemented for TypedUserCall
   13  scalar lowering not yet implemented for TypedMatch
   11  scalar lowering not yet implemented for TypedIf
   10  scalar lowering not yet implemented for TypedFilter
   10  'test::Order' is not a known class, mapping, runtime, connection, or d
    9  no overload of 'concatenate' matches 2 argument(s) of these shapes
    9  no compiled mapping function for class fetch: test::Person
    8  [plangen-c0954a] Not yet ported: TypedPropertyAccess (stage-3-relation
    8  scalar lowering not yet implemented for TypedUserCall
    7  [plangen-c0954a] Not yet ported: TypedProject (stage-3-relation). Vari
    6  no aggregate lowering registered for resolved overload 'wavg'
    6  unknown SQL data type: 'JSON'
    5  in call to 'concatenate', argument 1: expected a Relation, got Integer
    5  scalar lowering not yet implemented for TypedTds
    5  multi-column pivot is not lowered yet
    5  [6:86] expected BRACKET_CLOSE but found PATH_SEPARATOR ('::')
```

### Run 2026-07-07 @ ab8fc05

| tests | pass | failures | errors | skipped | green classes |
|---|---|---|---|---|---|
| 2721 | **1402** | 106 | 1194 | 19 | 188 |

| bucket | exception lines |
|---|---|
| H: class sources / property nav / mappings | 564 |
| CORE: unlowered constructs | 234 |
| CORE: scalar/agg function registrations | 98 |
| OTHER | 79 |
| CORE(parse): query syntax gaps | 73 |
| FIXTURE: unknown refs (to diagnose) | 73 |
| CORE(G): overload/typing gaps | 25 |

Top detail:
```
  524  lowering not yet implemented for TypedGetAll
   70  scalar lowering not yet implemented for TypedSerialize
   31  no SQL type for Pure class meta::pure::metamodel::type::Any at the low
   28  'test::Person' is not a known class, mapping, runtime, connection, or 
   27  lowering not yet implemented for TypedSortBy
   26  unbound type variable T
   24  unknown table 'T_EVENTS' in database 'EventDatabase'
   18  scalar lowering not yet implemented for TypedEnumValue
   18  scalar lowering not yet implemented for TypedNewInstance
   16  lowering not yet implemented for TypedUserCall
   13  scalar lowering not yet implemented for TypedMatch
   11  scalar lowering not yet implemented for TypedIf
   10  scalar lowering not yet implemented for TypedFilter
   10  'test::Order' is not a known class, mapping, runtime, connection, or d
    9  no overload of 'concatenate' matches 2 argument(s) of these shapes
    9  no compiled mapping function for class fetch: test::Person
    8  [plangen-c0954a] Not yet ported: TypedPropertyAccess (stage-3-relation
    8  scalar lowering not yet implemented for TypedUserCall
    7  [plangen-c0954a] Not yet ported: TypedProject (stage-3-relation). Vari
    6  no aggregate lowering registered for resolved overload 'wavg'
    6  unknown SQL data type: 'JSON'
    5  in call to 'concatenate', argument 1: expected a Relation, got Integer
    5  scalar lowering not yet implemented for TypedTds
    5  multi-column pivot is not lowered yet
    5  [6:86] expected BRACKET_CLOSE but found PATH_SEPARATOR ('::')
```

### Run 2026-07-07 @ 281fa0a

| tests | pass | failures | errors | skipped | green classes |
|---|---|---|---|---|---|
| 2721 | **1402** | 106 | 1194 | 19 | 188 |

| bucket | exception lines |
|---|---|
| H: class sources / property nav / mappings | 564 |
| CORE: unlowered constructs | 234 |
| CORE: scalar/agg function registrations | 98 |
| CORE(parse): query syntax gaps | 84 |
| OTHER | 79 |
| FIXTURE: unknown refs (to diagnose) | 73 |
| CORE(G): overload/typing gaps | 25 |

Top detail:
```
  524  lowering not yet implemented for TypedGetAll
   70  scalar lowering not yet implemented for TypedSerialize
   31  no SQL type for Pure class meta::pure::metamodel::type::Any at the low
   28  'test::Person' is not a known class, mapping, runtime, connection, or 
   27  lowering not yet implemented for TypedSortBy
   26  unbound type variable T
   24  unknown table 'T_EVENTS' in database 'EventDatabase'
   18  scalar lowering not yet implemented for TypedEnumValue
   18  scalar lowering not yet implemented for TypedNewInstance
   16  lowering not yet implemented for TypedUserCall
   13  scalar lowering not yet implemented for TypedMatch
   11  scalar lowering not yet implemented for TypedIf
   10  scalar lowering not yet implemented for TypedFilter
   10  'test::Order' is not a known class, mapping, runtime, connection, or d
    9  no overload of 'concatenate' matches 2 argument(s) of these shapes
    9  no compiled mapping function for class fetch: test::Person
    8  [plangen-c0954a] Not yet ported: TypedPropertyAccess (stage-3-relation
    8  scalar lowering not yet implemented for TypedUserCall
    7  [plangen-c0954a] Not yet ported: TypedProject (stage-3-relation). Vari
    6  no aggregate lowering registered for resolved overload 'wavg'
    6  unknown SQL data type: 'JSON'
    5  in call to 'concatenate', argument 1: expected a Relation, got Integer
    5  expected a Relation, got Integer
    5  scalar lowering not yet implemented for TypedTds
    5  multi-column pivot is not lowered yet
```

### Run 2026-07-08 @ e222e39

| tests | pass | failures | errors | skipped | green classes |
|---|---|---|---|---|---|
| 2721 | **1402** | 106 | 1194 | 19 | 188 |

| bucket | exception lines |
|---|---|
| H: class sources / property nav / mappings | 564 |
| CORE: unlowered constructs | 233 |
| CORE: scalar/agg function registrations | 98 |
| OTHER | 76 |
| CORE(parse): query syntax gaps | 73 |
| FIXTURE: unknown refs (to diagnose) | 73 |
| CORE(G): overload/typing gaps | 25 |

Top detail:
```
  524  lowering not yet implemented for TypedGetAll
   69  scalar lowering not yet implemented for TypedSerialize
   31  no SQL type for Pure class meta::pure::metamodel::type::Any at the low
   28  'test::Person' is not a known class, mapping, runtime, connection, or 
   27  lowering not yet implemented for TypedSortBy
   26  unbound type variable T
   24  unknown table 'T_EVENTS' in database 'EventDatabase'
   18  scalar lowering not yet implemented for TypedEnumValue
   18  scalar lowering not yet implemented for TypedNewInstance
   16  lowering not yet implemented for TypedUserCall
   13  scalar lowering not yet implemented for TypedMatch
   11  scalar lowering not yet implemented for TypedIf
   10  scalar lowering not yet implemented for TypedFilter
   10  'test::Order' is not a known class, mapping, runtime, connection, or d
    9  no overload of 'concatenate' matches 2 argument(s) of these shapes
    9  no compiled mapping function for class fetch: test::Person
    8  [plangen-c0954a] Not yet ported: TypedPropertyAccess (stage-3-relation
    8  scalar lowering not yet implemented for TypedUserCall
    7  [plangen-c0954a] Not yet ported: TypedProject (stage-3-relation). Vari
    6  no aggregate lowering registered for resolved overload 'wavg'
    6  unknown SQL data type: 'JSON'
    5  in call to 'concatenate', argument 1: expected a Relation, got Integer
    5  scalar lowering not yet implemented for TypedTds
    5  multi-column pivot is not lowered yet
    5  [6:86] expected BRACKET_CLOSE but found PATH_SEPARATOR ('::')
```

### Run 2026-07-08 @ d645ace

| tests | pass | failures | errors | skipped | green classes |
|---|---|---|---|---|---|
| 2721 | **1402** | 106 | 1194 | 19 | 188 |

| bucket | exception lines |
|---|---|
| H: class sources / property nav / mappings | 564 |
| CORE: unlowered constructs | 233 |
| CORE: scalar/agg function registrations | 98 |
| OTHER | 76 |
| CORE(parse): query syntax gaps | 73 |
| FIXTURE: unknown refs (to diagnose) | 73 |
| CORE(G): overload/typing gaps | 25 |

Top detail:
```
  524  lowering not yet implemented for TypedGetAll
   69  scalar lowering not yet implemented for TypedSerialize
   31  no SQL type for Pure class meta::pure::metamodel::type::Any at the low
   28  'test::Person' is not a known class, mapping, runtime, connection, or 
   27  lowering not yet implemented for TypedSortBy
   26  unbound type variable T
   24  unknown table 'T_EVENTS' in database 'EventDatabase'
   18  scalar lowering not yet implemented for TypedEnumValue
   18  scalar lowering not yet implemented for TypedNewInstance
   16  lowering not yet implemented for TypedUserCall
   13  scalar lowering not yet implemented for TypedMatch
   11  scalar lowering not yet implemented for TypedIf
   10  scalar lowering not yet implemented for TypedFilter
   10  'test::Order' is not a known class, mapping, runtime, connection, or d
    9  no overload of 'concatenate' matches 2 argument(s) of these shapes
    9  no compiled mapping function for class fetch: test::Person
    8  [plangen-c0954a] Not yet ported: TypedPropertyAccess (stage-3-relation
    8  scalar lowering not yet implemented for TypedUserCall
    7  [plangen-c0954a] Not yet ported: TypedProject (stage-3-relation). Vari
    6  no aggregate lowering registered for resolved overload 'wavg'
    6  unknown SQL data type: 'JSON'
    5  in call to 'concatenate', argument 1: expected a Relation, got Integer
    5  scalar lowering not yet implemented for TypedTds
    5  multi-column pivot is not lowered yet
    5  [6:86] expected BRACKET_CLOSE but found PATH_SEPARATOR ('::')
```

### Run 2026-07-08 @ 48e5aab

| tests | pass | failures | errors | skipped | green classes |
|---|---|---|---|---|---|
| 2721 | **1402** | 106 | 1194 | 19 | 188 |

| bucket | exception lines |
|---|---|
| H: class sources / property nav / mappings | 564 |
| CORE: unlowered constructs | 233 |
| CORE: scalar/agg function registrations | 98 |
| OTHER | 76 |
| CORE(parse): query syntax gaps | 73 |
| FIXTURE: unknown refs (to diagnose) | 73 |
| CORE(G): overload/typing gaps | 25 |

Top detail:
```
  524  lowering not yet implemented for TypedGetAll
   69  scalar lowering not yet implemented for TypedSerialize
   31  no SQL type for Pure class meta::pure::metamodel::type::Any at the low
   28  'test::Person' is not a known class, mapping, runtime, connection, or 
   27  lowering not yet implemented for TypedSortBy
   26  unbound type variable T
   24  unknown table 'T_EVENTS' in database 'EventDatabase'
   18  scalar lowering not yet implemented for TypedEnumValue
   18  scalar lowering not yet implemented for TypedNewInstance
   16  lowering not yet implemented for TypedUserCall
   13  scalar lowering not yet implemented for TypedMatch
   11  scalar lowering not yet implemented for TypedIf
   10  scalar lowering not yet implemented for TypedFilter
   10  'test::Order' is not a known class, mapping, runtime, connection, or d
    9  no overload of 'concatenate' matches 2 argument(s) of these shapes
    9  no compiled mapping function for class fetch: test::Person
    8  [plangen-c0954a] Not yet ported: TypedPropertyAccess (stage-3-relation
    8  scalar lowering not yet implemented for TypedUserCall
    7  [plangen-c0954a] Not yet ported: TypedProject (stage-3-relation). Vari
    6  no aggregate lowering registered for resolved overload 'wavg'
    6  unknown SQL data type: 'JSON'
    5  in call to 'concatenate', argument 1: expected a Relation, got Integer
    5  scalar lowering not yet implemented for TypedTds
    5  multi-column pivot is not lowered yet
    5  [6:86] expected BRACKET_CLOSE but found PATH_SEPARATOR ('::')
```

### Run 2026-07-08 @ 67f8789

| tests | pass | failures | errors | skipped | green classes |
|---|---|---|---|---|---|
| 2721 | **1402** | 106 | 1194 | 19 | 188 |

| bucket | exception lines |
|---|---|
| H: class sources / property nav / mappings | 564 |
| CORE: unlowered constructs | 233 |
| CORE: scalar/agg function registrations | 98 |
| OTHER | 76 |
| CORE(parse): query syntax gaps | 73 |
| FIXTURE: unknown refs (to diagnose) | 73 |
| CORE(G): overload/typing gaps | 25 |

Top detail:
```
  524  lowering not yet implemented for TypedGetAll
   69  scalar lowering not yet implemented for TypedSerialize
   31  no SQL type for Pure class meta::pure::metamodel::type::Any at the low
   28  'test::Person' is not a known class, mapping, runtime, connection, or 
   27  lowering not yet implemented for TypedSortBy
   26  unbound type variable T
   24  unknown table 'T_EVENTS' in database 'EventDatabase'
   18  scalar lowering not yet implemented for TypedEnumValue
   18  scalar lowering not yet implemented for TypedNewInstance
   16  lowering not yet implemented for TypedUserCall
   13  scalar lowering not yet implemented for TypedMatch
   11  scalar lowering not yet implemented for TypedIf
   10  scalar lowering not yet implemented for TypedFilter
   10  'test::Order' is not a known class, mapping, runtime, connection, or d
    9  no overload of 'concatenate' matches 2 argument(s) of these shapes
    9  no compiled mapping function for class fetch: test::Person
    8  [plangen-c0954a] Not yet ported: TypedPropertyAccess (stage-3-relation
    8  scalar lowering not yet implemented for TypedUserCall
    7  [plangen-c0954a] Not yet ported: TypedProject (stage-3-relation). Vari
    6  no aggregate lowering registered for resolved overload 'wavg'
    6  unknown SQL data type: 'JSON'
    5  in call to 'concatenate', argument 1: expected a Relation, got Integer
    5  scalar lowering not yet implemented for TypedTds
    5  multi-column pivot is not lowered yet
    5  [6:86] expected BRACKET_CLOSE but found PATH_SEPARATOR ('::')
```

### Run 2026-07-08 @ 4cc8e81

| tests | pass | failures | errors | skipped | green classes |
|---|---|---|---|---|---|
| 2721 | **1402** | 106 | 1194 | 19 | 188 |

| bucket | exception lines |
|---|---|
| H: class sources / property nav / mappings | 564 |
| CORE: unlowered constructs | 233 |
| CORE: scalar/agg function registrations | 98 |
| OTHER | 76 |
| CORE(parse): query syntax gaps | 73 |
| FIXTURE: unknown refs (to diagnose) | 73 |
| CORE(G): overload/typing gaps | 25 |

Top detail:
```
  524  lowering not yet implemented for TypedGetAll
   69  scalar lowering not yet implemented for TypedSerialize
   31  no SQL type for Pure class meta::pure::metamodel::type::Any at the low
   28  'test::Person' is not a known class, mapping, runtime, connection, or 
   27  lowering not yet implemented for TypedSortBy
   26  unbound type variable T
   24  unknown table 'T_EVENTS' in database 'EventDatabase'
   18  scalar lowering not yet implemented for TypedEnumValue
   18  scalar lowering not yet implemented for TypedNewInstance
   16  lowering not yet implemented for TypedUserCall
   13  scalar lowering not yet implemented for TypedMatch
   11  scalar lowering not yet implemented for TypedIf
   10  scalar lowering not yet implemented for TypedFilter
   10  'test::Order' is not a known class, mapping, runtime, connection, or d
    9  no overload of 'concatenate' matches 2 argument(s) of these shapes
    9  no compiled mapping function for class fetch: test::Person
    8  [plangen-c0954a] Not yet ported: TypedPropertyAccess (stage-3-relation
    8  scalar lowering not yet implemented for TypedUserCall
    7  [plangen-c0954a] Not yet ported: TypedProject (stage-3-relation). Vari
    6  no aggregate lowering registered for resolved overload 'wavg'
    6  unknown SQL data type: 'JSON'
    5  in call to 'concatenate', argument 1: expected a Relation, got Integer
    5  scalar lowering not yet implemented for TypedTds
    5  multi-column pivot is not lowered yet
    5  [6:86] expected BRACKET_CLOSE but found PATH_SEPARATOR ('::')
```

### Run 2026-07-08 @ 4cc8e81

| tests | pass | failures | errors | skipped | green classes |
|---|---|---|---|---|---|
| 2721 | **1402** | 106 | 1194 | 19 | 188 |

| bucket | exception lines |
|---|---|
| H: class sources / property nav / mappings | 561 |
| CORE: unlowered constructs | 233 |
| CORE: scalar/agg function registrations | 98 |
| OTHER | 76 |
| CORE(parse): query syntax gaps | 74 |
| FIXTURE: unknown refs (to diagnose) | 73 |
| CORE(G): overload/typing gaps | 27 |

Top detail:
```
  521  lowering not yet implemented for TypedGetAll
   69  scalar lowering not yet implemented for TypedSerialize
   31  no SQL type for Pure class meta::pure::metamodel::type::Any at the low
   28  'test::Person' is not a known class, mapping, runtime, connection, or 
   27  lowering not yet implemented for TypedSortBy
   26  unbound type variable T
   24  unknown table 'T_EVENTS' in database 'EventDatabase'
   18  scalar lowering not yet implemented for TypedEnumValue
   18  scalar lowering not yet implemented for TypedNewInstance
   16  lowering not yet implemented for TypedUserCall
   13  scalar lowering not yet implemented for TypedMatch
   11  scalar lowering not yet implemented for TypedIf
   10  scalar lowering not yet implemented for TypedFilter
   10  'test::Order' is not a known class, mapping, runtime, connection, or d
    9  no overload of 'concatenate' matches 2 argument(s) of these shapes
    9  no compiled mapping function for class fetch: test::Person
    8  [plangen-c0954a] Not yet ported: TypedPropertyAccess (stage-3-relation
    8  scalar lowering not yet implemented for TypedUserCall
    7  [plangen-c0954a] Not yet ported: TypedProject (stage-3-relation). Vari
    6  no aggregate lowering registered for resolved overload 'wavg'
    6  unknown SQL data type: 'JSON'
    5  in call to 'concatenate', argument 1: expected a Relation, got Integer
    5  scalar lowering not yet implemented for TypedTds
    5  multi-column pivot is not lowered yet
    5  [6:86] expected BRACKET_CLOSE but found PATH_SEPARATOR ('::')
```
