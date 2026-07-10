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

### Run 2026-07-08 @ 4842d8c

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

### Run 2026-07-08 @ 200713f

| tests | pass | failures | errors | skipped | green classes |
|---|---|---|---|---|---|
| 2721 | **1402** | 105 | 1195 | 19 | 188 |

| bucket | exception lines |
|---|---|
| H: class sources / property nav / mappings | 561 |
| CORE: unlowered constructs | 233 |
| CORE: scalar/agg function registrations | 105 |
| OTHER | 75 |
| FIXTURE: unknown refs (to diagnose) | 73 |
| CORE(parse): query syntax gaps | 69 |
| CORE(G): overload/typing gaps | 27 |

Top detail:
```
  521  lowering not yet implemented for TypedGetAll
   69  scalar lowering not yet implemented for TypedSerialize
   55  no scalar lowering registered for resolved overload 'meta::pure::funct
   34  no aggregate lowering registered for resolved overload 'meta::pure::fu
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
    6  unknown function 'meta::pure::functions::collection::concatenate'
    6  unknown SQL data type: 'JSON'
    5  scalar lowering not yet implemented for TypedTds
    5  multi-column pivot is not lowered yet
```

### Run 2026-07-09 @ b0618ad

| tests | pass | failures | errors | skipped | green classes |
|---|---|---|---|---|---|
| 2721 | **1402** | 105 | 1195 | 19 | 188 |

| bucket | exception lines |
|---|---|
| H: class sources / property nav / mappings | 561 |
| CORE: unlowered constructs | 233 |
| CORE: scalar/agg function registrations | 105 |
| OTHER | 75 |
| FIXTURE: unknown refs (to diagnose) | 73 |
| CORE(parse): query syntax gaps | 69 |
| CORE(G): overload/typing gaps | 27 |

Top detail:
```
  521  lowering not yet implemented for TypedGetAll
   69  scalar lowering not yet implemented for TypedSerialize
   55  no scalar lowering registered for resolved overload 'meta::pure::funct
   34  no aggregate lowering registered for resolved overload 'meta::pure::fu
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
    6  unknown function 'meta::pure::functions::collection::concatenate'
    6  unknown SQL data type: 'JSON'
    5  scalar lowering not yet implemented for TypedTds
    5  multi-column pivot is not lowered yet
```

### Run 2026-07-09 @ 1f93d67

| tests | pass | failures | errors | skipped | green classes |
|---|---|---|---|---|---|
| 2721 | **1402** | 105 | 1195 | 19 | 188 |

| bucket | exception lines |
|---|---|
| H: class sources / property nav / mappings | 561 |
| CORE: unlowered constructs | 233 |
| CORE: scalar/agg function registrations | 105 |
| OTHER | 75 |
| FIXTURE: unknown refs (to diagnose) | 73 |
| CORE(parse): query syntax gaps | 69 |
| CORE(G): overload/typing gaps | 27 |

Top detail:
```
  521  lowering not yet implemented for TypedGetAll
   69  scalar lowering not yet implemented for TypedSerialize
   55  no scalar lowering registered for resolved overload 'meta::pure::funct
   34  no aggregate lowering registered for resolved overload 'meta::pure::fu
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
    6  unknown function 'meta::pure::functions::collection::concatenate'
    6  unknown SQL data type: 'JSON'
    5  scalar lowering not yet implemented for TypedTds
    5  multi-column pivot is not lowered yet
```

### Run 2026-07-09 @ c9405bb

| tests | pass | failures | errors | skipped | green classes |
|---|---|---|---|---|---|
| 2721 | **1402** | 105 | 1195 | 19 | 188 |

| bucket | exception lines |
|---|---|
| H: class sources / property nav / mappings | 560 |
| CORE: unlowered constructs | 233 |
| CORE: scalar/agg function registrations | 105 |
| OTHER | 76 |
| FIXTURE: unknown refs (to diagnose) | 73 |
| CORE(parse): query syntax gaps | 69 |
| CORE(G): overload/typing gaps | 27 |

Top detail:
```
  520  lowering not yet implemented for TypedGetAll
   69  scalar lowering not yet implemented for TypedSerialize
   55  no scalar lowering registered for resolved overload 'meta::pure::funct
   34  no aggregate lowering registered for resolved overload 'meta::pure::fu
   28  'test::Person' is not a known class, mapping, runtime, connection, or 
   27  lowering not yet implemented for TypedSortBy
   26  unbound type variable T
   25  no SQL type for Pure class meta::pure::metamodel::type::Any at the low
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
    6  unknown function 'meta::pure::functions::collection::concatenate'
    6  unknown SQL data type: 'JSON'
    5  scalar lowering not yet implemented for TypedTds
    5  multi-column pivot is not lowered yet
```

### Run 2026-07-09 @ 52bb5d7

| tests | pass | failures | errors | skipped | green classes |
|---|---|---|---|---|---|
| 2721 | **1402** | 105 | 1195 | 19 | 188 |

| bucket | exception lines |
|---|---|
| H: class sources / property nav / mappings | 560 |
| CORE: unlowered constructs | 233 |
| CORE: scalar/agg function registrations | 105 |
| OTHER | 76 |
| FIXTURE: unknown refs (to diagnose) | 73 |
| CORE(parse): query syntax gaps | 69 |
| CORE(G): overload/typing gaps | 27 |

Top detail:
```
  520  lowering not yet implemented for TypedGetAll
   69  scalar lowering not yet implemented for TypedSerialize
   55  no scalar lowering registered for resolved overload 'meta::pure::funct
   34  no aggregate lowering registered for resolved overload 'meta::pure::fu
   28  'test::Person' is not a known class, mapping, runtime, connection, or 
   27  lowering not yet implemented for TypedSortBy
   26  unbound type variable T
   25  no SQL type for Pure class meta::pure::metamodel::type::Any at the low
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
    6  unknown function 'meta::pure::functions::collection::concatenate'
    6  unknown SQL data type: 'JSON'
    5  scalar lowering not yet implemented for TypedTds
    5  multi-column pivot is not lowered yet
```

### Run 2026-07-09 @ 3793477

| tests | pass | failures | errors | skipped | green classes |
|---|---|---|---|---|---|
| 2721 | **1412** | 104 | 1186 | 19 | 190 |

| bucket | exception lines |
|---|---|
| H: class sources / property nav / mappings | 560 |
| CORE: unlowered constructs | 207 |
| CORE: scalar/agg function registrations | 122 |
| OTHER | 76 |
| FIXTURE: unknown refs (to diagnose) | 73 |
| CORE(parse): query syntax gaps | 69 |
| CORE(G): overload/typing gaps | 27 |

Top detail:
```
  520  lowering not yet implemented for TypedGetAll
   72  no scalar lowering registered for resolved overload 'meta::pure::funct
   69  scalar lowering not yet implemented for TypedSerialize
   34  no aggregate lowering registered for resolved overload 'meta::pure::fu
   28  'test::Person' is not a known class, mapping, runtime, connection, or 
   27  lowering not yet implemented for TypedSortBy
   26  unbound type variable T
   25  no SQL type for Pure class meta::pure::metamodel::type::Any at the low
   24  unknown table 'T_EVENTS' in database 'EventDatabase'
   18  scalar lowering not yet implemented for TypedNewInstance
   16  lowering not yet implemented for TypedUserCall
   13  scalar lowering not yet implemented for TypedFilter
   13  scalar lowering not yet implemented for TypedMatch
   10  'test::Order' is not a known class, mapping, runtime, connection, or d
    9  no overload of 'concatenate' matches 2 argument(s) of these shapes
    9  no compiled mapping function for class fetch: test::Person
    8  [plangen-c0954a] Not yet ported: TypedPropertyAccess (stage-3-relation
    8  scalar lowering not yet implemented for TypedUserCall
    7  [plangen-c0954a] Not yet ported: TypedProject (stage-3-relation). Vari
    6  unknown function 'meta::pure::functions::collection::concatenate'
    6  unknown SQL data type: 'JSON'
    5  scalar lowering not yet implemented for TypedTds
    5  multi-column pivot is not lowered yet
    5  [6:86] expected BRACKET_CLOSE but found PATH_SEPARATOR ('::')
    5  Unresolved type for property: legalName
```

### Run 2026-07-09 @ 75d90a3

| tests | pass | failures | errors | skipped | green classes |
|---|---|---|---|---|---|
| 2721 | **1584** | 101 | 1017 | 19 | 208 |

| bucket | exception lines |
|---|---|
| OTHER | 403 |
| CORE(parse): query syntax gaps | 151 |
| CORE: scalar/agg function registrations | 145 |
| FIXTURE: unknown refs (to diagnose) | 103 |
| CORE: unlowered constructs | 96 |
| H: class sources / property nav / mappings | 38 |
| CORE(G): overload/typing gaps | 29 |

Top detail:
```
   93  no scalar lowering registered for resolved overload 'meta::pure::funct
   68  class query under TypedSerialize is not resolvable yet (H2 vocabulary)
   58  class query under TypedGroupBy is not resolvable yet (H2 vocabulary)
   34  no aggregate lowering registered for resolved overload 'meta::pure::fu
   29  property 'firm' of class 'test::Person' is not mapped in mapping 'test
   29  class query under TypedSortBy is not resolvable yet (H2 vocabulary)
   28  'test::Person' is not a known class, mapping, runtime, connection, or 
   28  in function 'model::PersonMapping$class$model::Person': unknown table 
   26  unbound type variable T
   25  no SQL type for Pure class meta::pure::metamodel::type::Any at the low
   24  unknown table 'T_EVENTS' in database 'EventDatabase'
   21  resolver bug: mapping pipeline for 'model::StaffMember' in 'model::Sta
   19  class query under TypedExtendWindow is not resolvable yet (H2 vocabula
   18  scalar lowering not yet implemented for TypedNewInstance
   16  lowering not yet implemented for TypedUserCall
   13  property mapped through join slot 'Person_Dept' of class 'model::Perso
   13  scalar lowering not yet implemented for TypedMatch
   12  property 'dept' of class 'model::Emp' is not mapped in mapping 'model:
   11  property 'addresses' of class 'model::Person' is not mapped in mapping
   11  resolver bug: mapping pipeline for 'model::StaffCard' in 'model::Staff
   10  resolver bug: mapping pipeline for 'model::Person' in 'model::PersonM2
   10  'test::Order' is not a known class, mapping, runtime, connection, or d
    9  property 'addresses' of class 'test::Person' is not mapped in mapping 
    9  no overload of 'concatenate' matches 2 argument(s) of these shapes
    9  no compiled mapping function for class fetch: test::Person
```

### Run 2026-07-10 @ 6a5040a

| tests | pass | failures | errors | skipped | green classes |
|---|---|---|---|---|---|
| 2721 | **1605** | 102 | 995 | 19 | 210 |

| bucket | exception lines |
|---|---|
| OTHER | 381 |
| CORE(parse): query syntax gaps | 151 |
| CORE: scalar/agg function registrations | 145 |
| FIXTURE: unknown refs (to diagnose) | 103 |
| CORE: unlowered constructs | 96 |
| H: class sources / property nav / mappings | 38 |
| CORE(G): overload/typing gaps | 29 |

Top detail:
```
   93  no scalar lowering registered for resolved overload 'meta::pure::funct
   68  class query under TypedSerialize is not resolvable yet (H2 vocabulary)
   59  class query under TypedGroupBy is not resolvable yet (H2 vocabulary)
   34  no aggregate lowering registered for resolved overload 'meta::pure::fu
   32  property 'firm' of class 'test::Person' is not mapped in mapping 'test
   29  class query under TypedSortBy is not resolvable yet (H2 vocabulary)
   28  'test::Person' is not a known class, mapping, runtime, connection, or 
   28  in function 'model::PersonMapping$class$model::Person': unknown table 
   26  unbound type variable T
   25  no SQL type for Pure class meta::pure::metamodel::type::Any at the low
   24  unknown table 'T_EVENTS' in database 'EventDatabase'
   21  resolver bug: mapping pipeline for 'model::StaffMember' in 'model::Sta
   19  class query under TypedExtendWindow is not resolvable yet (H2 vocabula
   18  scalar lowering not yet implemented for TypedNewInstance
   16  lowering not yet implemented for TypedUserCall
   14  property 'addresses' of class 'model::Person' is not mapped in mapping
   13  scalar lowering not yet implemented for TypedMatch
   12  property 'dept' of class 'model::Emp' is not mapped in mapping 'model:
   11  resolver bug: mapping pipeline for 'model::StaffCard' in 'model::Staff
   10  resolver bug: mapping pipeline for 'model::Person' in 'model::PersonM2
   10  'test::Order' is not a known class, mapping, runtime, connection, or d
    9  property 'addresses' of class 'test::Person' is not mapped in mapping 
    9  no overload of 'concatenate' matches 2 argument(s) of these shapes
    9  no compiled mapping function for class fetch: test::Person
    8  [plangen-c0954a] Not yet ported: TypedPropertyAccess (stage-3-relation
```

### Run 2026-07-10 @ d0f19ec

| tests | pass | failures | errors | skipped | green classes |
|---|---|---|---|---|---|
| 2721 | **1622** | 102 | 978 | 19 | 216 |

| bucket | exception lines |
|---|---|
| OTHER | 359 |
| CORE(parse): query syntax gaps | 156 |
| CORE: scalar/agg function registrations | 145 |
| FIXTURE: unknown refs (to diagnose) | 103 |
| CORE: unlowered constructs | 96 |
| H: class sources / property nav / mappings | 38 |
| CORE(G): overload/typing gaps | 29 |

Top detail:
```
   93  no scalar lowering registered for resolved overload 'meta::pure::funct
   68  class query under TypedSerialize is not resolvable yet (H2 vocabulary)
   59  class query under TypedGroupBy is not resolvable yet (H2 vocabulary)
   34  no aggregate lowering registered for resolved overload 'meta::pure::fu
   33  property 'firm' of class 'test::Person' is not mapped in mapping 'test
   28  'test::Person' is not a known class, mapping, runtime, connection, or 
   28  in function 'model::PersonMapping$class$model::Person': unknown table 
   26  unbound type variable T
   25  no SQL type for Pure class meta::pure::metamodel::type::Any at the low
   24  unknown table 'T_EVENTS' in database 'EventDatabase'
   23  resolver bug: mapping pipeline for 'model::StaffMember' in 'model::Sta
   19  class query under TypedExtendWindow is not resolvable yet (H2 vocabula
   18  scalar lowering not yet implemented for TypedNewInstance
   16  lowering not yet implemented for TypedUserCall
   14  property 'addresses' of class 'model::Person' is not mapped in mapping
   13  resolver bug: mapping pipeline for 'model::StaffCard' in 'model::Staff
   13  property 'dept' of class 'model::Emp' is not mapped in mapping 'model:
   13  scalar lowering not yet implemented for TypedMatch
   10  resolver bug: mapping pipeline for 'model::Person' in 'model::PersonM2
   10  'test::Order' is not a known class, mapping, runtime, connection, or d
    9  property 'addresses' of class 'test::Person' is not mapped in mapping 
    9  no overload of 'concatenate' matches 2 argument(s) of these shapes
    9  no compiled mapping function for class fetch: test::Person
    8  [plangen-c0954a] Not yet ported: TypedPropertyAccess (stage-3-relation
    7  [plangen-c0954a] Not yet ported: TypedProject (stage-3-relation). Vari
```

### Run 2026-07-10 @ a223670

| tests | pass | failures | errors | skipped | green classes |
|---|---|---|---|---|---|
| 2721 | **1674** | 102 | 926 | 19 | 221 |

| bucket | exception lines |
|---|---|
| OTHER | 307 |
| CORE(parse): query syntax gaps | 156 |
| CORE: scalar/agg function registrations | 145 |
| FIXTURE: unknown refs (to diagnose) | 103 |
| CORE: unlowered constructs | 96 |
| H: class sources / property nav / mappings | 38 |
| CORE(G): overload/typing gaps | 29 |

Top detail:
```
   93  no scalar lowering registered for resolved overload 'meta::pure::funct
   68  class query under TypedSerialize is not resolvable yet (H2 vocabulary)
   59  class query under TypedGroupBy is not resolvable yet (H2 vocabulary)
   34  no aggregate lowering registered for resolved overload 'meta::pure::fu
   28  'test::Person' is not a known class, mapping, runtime, connection, or 
   28  in function 'model::PersonMapping$class$model::Person': unknown table 
   27  navigation of association end '$addresses' (multiplicity [*]) is not s
   26  unbound type variable T
   25  no SQL type for Pure class meta::pure::metamodel::type::Any at the low
   24  unknown table 'T_EVENTS' in database 'EventDatabase'
   23  resolver bug: mapping pipeline for 'model::StaffMember' in 'model::Sta
   19  class query under TypedExtendWindow is not resolvable yet (H2 vocabula
   18  scalar lowering not yet implemented for TypedNewInstance
   16  lowering not yet implemented for TypedUserCall
   13  resolver bug: mapping pipeline for 'model::StaffCard' in 'model::Staff
   13  scalar lowering not yet implemented for TypedMatch
   10  resolver bug: mapping pipeline for 'model::Person' in 'model::PersonM2
   10  'test::Order' is not a known class, mapping, runtime, connection, or d
    9  no overload of 'concatenate' matches 2 argument(s) of these shapes
    9  no compiled mapping function for class fetch: test::Person
    8  [plangen-c0954a] Not yet ported: TypedPropertyAccess (stage-3-relation
    7  [plangen-c0954a] Not yet ported: TypedProject (stage-3-relation). Vari
    6  unknown function 'meta::pure::functions::collection::concatenate'
    6  scalar lowering not yet implemented for TypedFilter
    6  unknown SQL data type: 'JSON'
```

### Run 2026-07-10 @ 7dd15a7

| tests | pass | failures | errors | skipped | green classes |
|---|---|---|---|---|---|
| 2721 | **1674** | 102 | 926 | 19 | 221 |

| bucket | exception lines |
|---|---|
| OTHER | 307 |
| CORE(parse): query syntax gaps | 156 |
| CORE: scalar/agg function registrations | 145 |
| FIXTURE: unknown refs (to diagnose) | 103 |
| CORE: unlowered constructs | 96 |
| H: class sources / property nav / mappings | 38 |
| CORE(G): overload/typing gaps | 29 |

Top detail:
```
   93  no scalar lowering registered for resolved overload 'meta::pure::funct
   68  class query under TypedSerialize is not resolvable yet (H2 vocabulary)
   59  class query under TypedGroupBy is not resolvable yet (H2 vocabulary)
   34  no aggregate lowering registered for resolved overload 'meta::pure::fu
   28  'test::Person' is not a known class, mapping, runtime, connection, or 
   28  in function 'model::PersonMapping$class$model::Person': unknown table 
   27  navigation of association end '$addresses' (multiplicity [*]) is not s
   26  unbound type variable T
   25  no SQL type for Pure class meta::pure::metamodel::type::Any at the low
   24  unknown table 'T_EVENTS' in database 'EventDatabase'
   23  resolver bug: mapping pipeline for 'model::StaffMember' in 'model::Sta
   19  class query under TypedExtendWindow is not resolvable yet (H2 vocabula
   18  scalar lowering not yet implemented for TypedNewInstance
   16  lowering not yet implemented for TypedUserCall
   13  resolver bug: mapping pipeline for 'model::StaffCard' in 'model::Staff
   13  scalar lowering not yet implemented for TypedMatch
   10  resolver bug: mapping pipeline for 'model::Person' in 'model::PersonM2
   10  'test::Order' is not a known class, mapping, runtime, connection, or d
    9  no overload of 'concatenate' matches 2 argument(s) of these shapes
    9  no compiled mapping function for class fetch: test::Person
    8  [plangen-c0954a] Not yet ported: TypedPropertyAccess (stage-3-relation
    7  [plangen-c0954a] Not yet ported: TypedProject (stage-3-relation). Vari
    6  unknown function 'meta::pure::functions::collection::concatenate'
    6  scalar lowering not yet implemented for TypedFilter
    6  unknown SQL data type: 'JSON'
```

### Run 2026-07-10 @ e1d5008

| tests | pass | failures | errors | skipped | green classes |
|---|---|---|---|---|---|
| 2721 | **1714** | 100 | 888 | 19 | 229 |

| bucket | exception lines |
|---|---|
| OTHER | 269 |
| CORE(parse): query syntax gaps | 156 |
| CORE: scalar/agg function registrations | 145 |
| FIXTURE: unknown refs (to diagnose) | 103 |
| CORE: unlowered constructs | 96 |
| H: class sources / property nav / mappings | 38 |
| CORE(G): overload/typing gaps | 29 |

Top detail:
```
   93  no scalar lowering registered for resolved overload 'meta::pure::funct
   68  class query under TypedSerialize is not resolvable yet (H2 vocabulary)
   59  class query under TypedGroupBy is not resolvable yet (H2 vocabulary)
   34  no aggregate lowering registered for resolved overload 'meta::pure::fu
   28  'test::Person' is not a known class, mapping, runtime, connection, or 
   28  in function 'model::PersonMapping$class$model::Person': unknown table 
   26  unbound type variable T
   25  no SQL type for Pure class meta::pure::metamodel::type::Any at the low
   24  unknown table 'T_EVENTS' in database 'EventDatabase'
   23  resolver bug: mapping pipeline for 'model::StaffMember' in 'model::Sta
   19  class query under TypedExtendWindow is not resolvable yet (H2 vocabula
   18  scalar lowering not yet implemented for TypedNewInstance
   16  lowering not yet implemented for TypedUserCall
   13  resolver bug: mapping pipeline for 'model::StaffCard' in 'model::Staff
   13  scalar lowering not yet implemented for TypedMatch
   10  resolver bug: mapping pipeline for 'model::Person' in 'model::PersonM2
   10  'test::Order' is not a known class, mapping, runtime, connection, or d
    9  no overload of 'concatenate' matches 2 argument(s) of these shapes
    9  no compiled mapping function for class fetch: test::Person
    8  [plangen-c0954a] Not yet ported: TypedPropertyAccess (stage-3-relation
    7  [plangen-c0954a] Not yet ported: TypedProject (stage-3-relation). Vari
    6  unknown function 'meta::pure::functions::collection::concatenate'
    6  scalar lowering not yet implemented for TypedFilter
    6  unknown SQL data type: 'JSON'
    5  class query under TypedExtend is not resolvable yet (H2 vocabulary)
```

### Run 2026-07-10 @ 2b5b8de

| tests | pass | failures | errors | skipped | green classes |
|---|---|---|---|---|---|
| 2721 | **1794** | 98 | 810 | 19 | 243 |

| bucket | exception lines |
|---|---|
| OTHER | 270 |
| CORE: scalar/agg function registrations | 152 |
| FIXTURE: unknown refs (to diagnose) | 103 |
| CORE: unlowered constructs | 96 |
| CORE(parse): query syntax gaps | 69 |
| H: class sources / property nav / mappings | 38 |
| CORE(G): overload/typing gaps | 29 |

Top detail:
```
   93  no scalar lowering registered for resolved overload 'meta::pure::funct
   68  class query under TypedSerialize is not resolvable yet (H2 vocabulary)
   39  no aggregate lowering registered for resolved overload 'meta::pure::fu
   28  'test::Person' is not a known class, mapping, runtime, connection, or 
   28  in function 'model::PersonMapping$class$model::Person': unknown table 
   26  unbound type variable T
   25  no SQL type for Pure class meta::pure::metamodel::type::Any at the low
   24  unknown table 'T_EVENTS' in database 'EventDatabase'
   23  model-to-model mapping: 'model::StaffMember' in 'model::StaffMemberMap
   18  scalar lowering not yet implemented for TypedNewInstance
   16  lowering not yet implemented for TypedUserCall
   13  model-to-model mapping: 'model::StaffCard' in 'model::StaffCardMapping
   13  scalar lowering not yet implemented for TypedMatch
   10  model-to-model mapping: 'model::Person' in 'model::PersonM2MMapping' m
   10  'test::Order' is not a known class, mapping, runtime, connection, or d
    9  no overload of 'concatenate' matches 2 argument(s) of these shapes
    9  no compiled mapping function for class fetch: test::Person
    8  [plangen-c0954a] Not yet ported: TypedPropertyAccess (stage-3-relation
    7  [plangen-c0954a] Not yet ported: TypedProject (stage-3-relation). Vari
    6  unknown function 'meta::pure::functions::collection::concatenate'
    6  scalar lowering not yet implemented for TypedFilter
    6  unknown SQL data type: 'JSON'
    6  model-to-model mapping: 'model::PersonView' in 'model::PersonViewMappi
    5  multi-hop navigation dept.org.name is not supported yet
    5  scalar lowering not yet implemented for TypedTds
```

### Run 2026-07-10 @ 1382e76

| tests | pass | failures | errors | skipped | green classes |
|---|---|---|---|---|---|
| 2721 | **1866** | 56 | 780 | 19 | 259 |

| bucket | exception lines |
|---|---|
| OTHER | 297 |
| CORE: scalar/agg function registrations | 153 |
| CORE: unlowered constructs | 84 |
| CORE(parse): query syntax gaps | 71 |
| FIXTURE: unknown refs (to diagnose) | 58 |
| H: class sources / property nav / mappings | 34 |
| CORE(G): overload/typing gaps | 30 |

Top detail:
```
   94  no scalar lowering registered for resolved overload 'meta::pure::funct
   68  class query under TypedSerialize is not resolvable yet (H2 vocabulary)
   39  no aggregate lowering registered for resolved overload 'meta::pure::fu
   35  'test::Person' is not a known class, mapping, runtime, connection, or 
   33  unbound type variable T
   27  object-space expression node TypedUserCall is not substitutable yet (H
   25  no SQL type for Pure class meta::pure::metamodel::type::Any at the low
   23  model-to-model mapping: 'model::StaffMember' in 'model::StaffMemberMap
   18  scalar lowering not yet implemented for TypedNewInstance
   16  lowering not yet implemented for TypedUserCall
   13  model-to-model mapping: 'model::StaffCard' in 'model::StaffCardMapping
   13  scalar lowering not yet implemented for TypedMatch
   10  model-to-model mapping: 'model::Person' in 'model::PersonM2MMapping' m
   10  'test::Order' is not a known class, mapping, runtime, connection, or d
    9  no overload of 'concatenate' matches 2 argument(s) of these shapes
    9  unknown SQL data type: 'JSON'
    6  unknown function 'meta::pure::functions::collection::concatenate'
    6  multi-column pivot is not lowered yet
    6  scalar lowering not yet implemented for TypedFilter
    6  model-to-model mapping: 'model::PersonView' in 'model::PersonViewMappi
    6  scalar lowering not yet implemented for TypedUserCall
    5  multi-hop navigation dept.org.name is not supported yet
    5  scalar lowering not yet implemented for TypedTds
    5  [6:86] expected BRACKET_CLOSE but found PATH_SEPARATOR ('::')
    5  model-to-model mapping: 'model::DirectoryEntry' in 'model::DirectoryMa
```

### Run 2026-07-10 @ fc64ce9

| tests | pass | failures | errors | skipped | green classes |
|---|---|---|---|---|---|
| 2721 | **1887** | 56 | 759 | 19 | 259 |

| bucket | exception lines |
|---|---|
| OTHER | 297 |
| CORE: scalar/agg function registrations | 162 |
| CORE: unlowered constructs | 84 |
| FIXTURE: unknown refs (to diagnose) | 60 |
| CORE(parse): query syntax gaps | 39 |
| H: class sources / property nav / mappings | 34 |
| CORE(G): overload/typing gaps | 30 |

Top detail:
```
   96  no scalar lowering registered for resolved overload 'meta::pure::funct
   68  class query under TypedSerialize is not resolvable yet (H2 vocabulary)
   39  no aggregate lowering registered for resolved overload 'meta::pure::fu
   37  'test::Person' is not a known class, mapping, runtime, connection, or 
   33  unbound type variable T
   27  object-space expression node TypedUserCall is not substitutable yet (H
   25  no SQL type for Pure class meta::pure::metamodel::type::Any at the low
   23  model-to-model mapping: 'model::StaffMember' in 'model::StaffMemberMap
   18  scalar lowering not yet implemented for TypedNewInstance
   16  lowering not yet implemented for TypedUserCall
   13  model-to-model mapping: 'model::StaffCard' in 'model::StaffCardMapping
   13  scalar lowering not yet implemented for TypedMatch
   10  model-to-model mapping: 'model::Person' in 'model::PersonM2MMapping' m
   10  'test::Order' is not a known class, mapping, runtime, connection, or d
    9  no overload of 'concatenate' matches 2 argument(s) of these shapes
    9  unknown SQL data type: 'JSON'
    6  unknown function 'meta::pure::functions::collection::concatenate'
    6  multi-column pivot is not lowered yet
    6  no scalar lowering registered for resolved overload 'meta::legend::lit
    6  scalar lowering not yet implemented for TypedFilter
    6  model-to-model mapping: 'model::PersonView' in 'model::PersonViewMappi
    6  scalar lowering not yet implemented for TypedUserCall
    5  multi-hop navigation dept.org.name is not supported yet
    5  scalar lowering not yet implemented for TypedTds
    5  model-to-model mapping: 'model::DirectoryEntry' in 'model::DirectoryMa
```

### Run 2026-07-10 @ 935598f

| tests | pass | failures | errors | skipped | green classes |
|---|---|---|---|---|---|
| 2721 | **1894** | 57 | 751 | 19 | 260 |

| bucket | exception lines |
|---|---|
| OTHER | 307 |
| CORE: scalar/agg function registrations | 166 |
| CORE: unlowered constructs | 86 |
| FIXTURE: unknown refs (to diagnose) | 60 |
| H: class sources / property nav / mappings | 34 |
| CORE(G): overload/typing gaps | 30 |
| CORE(parse): query syntax gaps | 13 |

Top detail:
```
   96  no scalar lowering registered for resolved overload 'meta::pure::funct
   68  class query under TypedSerialize is not resolvable yet (H2 vocabulary)
   43  no aggregate lowering registered for resolved overload 'meta::pure::fu
   37  'test::Person' is not a known class, mapping, runtime, connection, or 
   33  unbound type variable T
   27  object-space expression node TypedUserCall is not substitutable yet (H
   25  no SQL type for Pure class meta::pure::metamodel::type::Any at the low
   23  model-to-model mapping: 'model::StaffMember' in 'model::StaffMemberMap
   18  scalar lowering not yet implemented for TypedNewInstance
   16  lowering not yet implemented for TypedUserCall
   13  model-to-model mapping: 'model::StaffCard' in 'model::StaffCardMapping
   13  scalar lowering not yet implemented for TypedMatch
   10  model-to-model mapping: 'model::Person' in 'model::PersonM2MMapping' m
   10  'test::Order' is not a known class, mapping, runtime, connection, or d
    9  no overload of 'concatenate' matches 2 argument(s) of these shapes
    9  unknown SQL data type: 'JSON'
    6  unknown function 'meta::pure::functions::collection::concatenate'
    6  multi-column pivot is not lowered yet
    6  no TDS cell rendering for Pure type meta::pure::metamodel::variant::Va
    6  no scalar lowering registered for resolved overload 'meta::legend::lit
    6  scalar lowering not yet implemented for TypedFilter
    6  model-to-model mapping: 'model::PersonView' in 'model::PersonViewMappi
    6  scalar lowering not yet implemented for TypedUserCall
    5  multi-hop navigation dept.org.name is not supported yet
    5  scalar lowering not yet implemented for TypedTds
```

### Run 2026-07-10 @ 451f572

| tests | pass | failures | errors | skipped | green classes |
|---|---|---|---|---|---|
| 2721 | **1922** | 57 | 723 | 19 | 263 |

| bucket | exception lines |
|---|---|
| OTHER | 279 |
| CORE: scalar/agg function registrations | 166 |
| CORE: unlowered constructs | 86 |
| FIXTURE: unknown refs (to diagnose) | 60 |
| H: class sources / property nav / mappings | 34 |
| CORE(G): overload/typing gaps | 30 |
| CORE(parse): query syntax gaps | 13 |

Top detail:
```
   96  no scalar lowering registered for resolved overload 'meta::pure::funct
   43  no aggregate lowering registered for resolved overload 'meta::pure::fu
   37  'test::Person' is not a known class, mapping, runtime, connection, or 
   33  unbound type variable T
   30  model-to-model mapping: 'model::StaffMember' in 'model::StaffMemberMap
   27  object-space expression node TypedUserCall is not substitutable yet (H
   25  no SQL type for Pure class meta::pure::metamodel::type::Any at the low
   18  scalar lowering not yet implemented for TypedNewInstance
   17  model-to-model mapping: 'model::Person' in 'model::PersonM2MMapping' m
   16  lowering not yet implemented for TypedUserCall
   15  model-to-model mapping: 'model::StaffCard' in 'model::StaffCardMapping
   13  scalar lowering not yet implemented for TypedMatch
   10  'test::Order' is not a known class, mapping, runtime, connection, or d
    9  no overload of 'concatenate' matches 2 argument(s) of these shapes
    9  unknown SQL data type: 'JSON'
    7  model-to-model mapping: 'model::ActiveStaff' in 'model::ActiveStaffMap
    7  model-to-model mapping: 'model::DirectoryEntry' in 'model::DirectoryMa
    6  unknown function 'meta::pure::functions::collection::concatenate'
    6  multi-column pivot is not lowered yet
    6  no TDS cell rendering for Pure type meta::pure::metamodel::variant::Va
    6  no scalar lowering registered for resolved overload 'meta::legend::lit
    6  scalar lowering not yet implemented for TypedFilter
    6  model-to-model mapping: 'model::Badge' in 'model::BadgeMapping' maps f
    6  model-to-model mapping: 'model::PersonView' in 'model::PersonViewMappi
    6  in function 'model::DisjointDeepFetchMapping$class$model::StaffComplet
```

### Run 2026-07-10 @ d24ecb7

| tests | pass | failures | errors | skipped | green classes |
|---|---|---|---|---|---|
| 2721 | **1950** | 57 | 695 | 19 | 271 |

| bucket | exception lines |
|---|---|
| OTHER | 282 |
| CORE: scalar/agg function registrations | 160 |
| CORE: unlowered constructs | 87 |
| FIXTURE: unknown refs (to diagnose) | 60 |
| H: class sources / property nav / mappings | 34 |
| CORE(parse): query syntax gaps | 11 |
| CORE(G): overload/typing gaps | 6 |

Top detail:
```
   96  no scalar lowering registered for resolved overload 'meta::pure::funct
   43  no aggregate lowering registered for resolved overload 'meta::pure::fu
   37  'test::Person' is not a known class, mapping, runtime, connection, or 
   33  unbound type variable T
   30  model-to-model mapping: 'model::StaffMember' in 'model::StaffMemberMap
   27  object-space expression node TypedUserCall is not substitutable yet (H
   25  no SQL type for Pure class meta::pure::metamodel::type::Any at the low
   18  scalar lowering not yet implemented for TypedNewInstance
   17  model-to-model mapping: 'model::Person' in 'model::PersonM2MMapping' m
   16  lowering not yet implemented for TypedUserCall
   15  model-to-model mapping: 'model::StaffCard' in 'model::StaffCardMapping
   13  scalar lowering not yet implemented for TypedMatch
   10  'test::Order' is not a known class, mapping, runtime, connection, or d
    9  unknown SQL data type: 'JSON'
    7  model-to-model mapping: 'model::ActiveStaff' in 'model::ActiveStaffMap
    7  model-to-model mapping: 'model::DirectoryEntry' in 'model::DirectoryMa
    6  multi-column pivot is not lowered yet
    6  no TDS cell rendering for Pure type meta::pure::metamodel::variant::Va
    6  no scalar lowering registered for resolved overload 'meta::legend::lit
    6  scalar lowering not yet implemented for TypedFilter
    6  model-to-model mapping: 'model::Badge' in 'model::BadgeMapping' maps f
    6  model-to-model mapping: 'model::PersonView' in 'model::PersonViewMappi
    6  in function 'model::DisjointDeepFetchMapping$class$model::StaffComplet
    6  scalar lowering not yet implemented for TypedUserCall
    5  multi-hop navigation dept.org.name is not supported yet
```

### Run 2026-07-10 @ 332ca8f

| tests | pass | failures | errors | skipped | green classes |
|---|---|---|---|---|---|
| 2721 | **2020** | 57 | 625 | 19 | 279 |

| bucket | exception lines |
|---|---|
| OTHER | 212 |
| CORE: scalar/agg function registrations | 160 |
| CORE: unlowered constructs | 87 |
| FIXTURE: unknown refs (to diagnose) | 60 |
| H: class sources / property nav / mappings | 34 |
| CORE(parse): query syntax gaps | 11 |
| CORE(G): overload/typing gaps | 6 |

Top detail:
```
   96  no scalar lowering registered for resolved overload 'meta::pure::funct
   55  resolver bug: row-read rewrite hit TypedCast, outside the normalizer's
   43  no aggregate lowering registered for resolved overload 'meta::pure::fu
   37  'test::Person' is not a known class, mapping, runtime, connection, or 
   33  unbound type variable T
   27  object-space expression node TypedUserCall is not substitutable yet (H
   25  no SQL type for Pure class meta::pure::metamodel::type::Any at the low
   18  scalar lowering not yet implemented for TypedNewInstance
   16  lowering not yet implemented for TypedUserCall
   13  scalar lowering not yet implemented for TypedMatch
   10  'test::Order' is not a known class, mapping, runtime, connection, or d
    9  unknown SQL data type: 'JSON'
    6  multi-column pivot is not lowered yet
    6  no TDS cell rendering for Pure type meta::pure::metamodel::variant::Va
    6  no scalar lowering registered for resolved overload 'meta::legend::lit
    6  scalar lowering not yet implemented for TypedFilter
    6  model-to-model binding of 'model::StaffComplete' in 'model::DisjointDe
    6  scalar lowering not yet implemented for TypedUserCall
    5  multi-hop navigation dept.org.name is not supported yet
    5  scalar lowering not yet implemented for TypedTds
    5  'test::Sale' is not a known class, mapping, runtime, connection, or da
    5  aggregate reduce must be a native reducer call, got TypedCast
    5  scalar lowering not yet implemented for TypedPropertyAccess
    4  no aggregate lowering registered for resolved overload 'meta::legend::
    4  in call to 'meta::pure::functions::collection::add', argument 2: type 
```

### Run 2026-07-10 @ bf1c476

| tests | pass | failures | errors | skipped | green classes |
|---|---|---|---|---|---|
| 2721 | **2053** | 57 | 592 | 19 | 282 |

| bucket | exception lines |
|---|---|
| OTHER | 212 |
| CORE: scalar/agg function registrations | 160 |
| CORE: unlowered constructs | 87 |
| FIXTURE: unknown refs (to diagnose) | 60 |
| CORE(parse): query syntax gaps | 11 |
| CORE(G): overload/typing gaps | 6 |
| H: class sources / property nav / mappings | 1 |

Top detail:
```
   96  no scalar lowering registered for resolved overload 'meta::pure::funct
   55  resolver bug: row-read rewrite hit TypedCast, outside the normalizer's
   43  no aggregate lowering registered for resolved overload 'meta::pure::fu
   37  'test::Person' is not a known class, mapping, runtime, connection, or 
   27  object-space expression node TypedUserCall is not substitutable yet (H
   25  no SQL type for Pure class meta::pure::metamodel::type::Any at the low
   18  scalar lowering not yet implemented for TypedNewInstance
   16  lowering not yet implemented for TypedUserCall
   13  scalar lowering not yet implemented for TypedMatch
   10  'test::Order' is not a known class, mapping, runtime, connection, or d
    9  unknown SQL data type: 'JSON'
    6  multi-column pivot is not lowered yet
    6  no TDS cell rendering for Pure type meta::pure::metamodel::variant::Va
    6  no scalar lowering registered for resolved overload 'meta::legend::lit
    6  scalar lowering not yet implemented for TypedFilter
    6  model-to-model binding of 'model::StaffComplete' in 'model::DisjointDe
    6  scalar lowering not yet implemented for TypedUserCall
    5  multi-hop navigation dept.org.name is not supported yet
    5  scalar lowering not yet implemented for TypedTds
    5  'test::Sale' is not a known class, mapping, runtime, connection, or da
    5  aggregate reduce must be a native reducer call, got TypedCast
    5  scalar lowering not yet implemented for TypedPropertyAccess
    4  no aggregate lowering registered for resolved overload 'meta::legend::
    4  in call to 'meta::pure::functions::collection::add', argument 2: type 
    4  Invalid Input Error: Invalid type specifier "{" for formatting a value
```

### Run 2026-07-10 @ f445e47

| tests | pass | failures | errors | skipped | green classes |
|---|---|---|---|---|---|
| 2721 | **2053** | 57 | 592 | 19 | 282 |

| bucket | exception lines |
|---|---|
| OTHER | 212 |
| CORE: scalar/agg function registrations | 160 |
| CORE: unlowered constructs | 87 |
| FIXTURE: unknown refs (to diagnose) | 60 |
| CORE(parse): query syntax gaps | 11 |
| CORE(G): overload/typing gaps | 6 |
| H: class sources / property nav / mappings | 1 |

Top detail:
```
   96  no scalar lowering registered for resolved overload 'meta::pure::funct
   55  resolver bug: row-read rewrite hit TypedCast, outside the normalizer's
   43  no aggregate lowering registered for resolved overload 'meta::pure::fu
   37  'test::Person' is not a known class, mapping, runtime, connection, or 
   27  object-space expression node TypedUserCall is not substitutable yet (H
   25  no SQL type for Pure class meta::pure::metamodel::type::Any at the low
   18  scalar lowering not yet implemented for TypedNewInstance
   16  lowering not yet implemented for TypedUserCall
   13  scalar lowering not yet implemented for TypedMatch
   10  'test::Order' is not a known class, mapping, runtime, connection, or d
    9  unknown SQL data type: 'JSON'
    6  multi-column pivot is not lowered yet
    6  no TDS cell rendering for Pure type meta::pure::metamodel::variant::Va
    6  no scalar lowering registered for resolved overload 'meta::legend::lit
    6  scalar lowering not yet implemented for TypedFilter
    6  model-to-model binding of 'model::StaffComplete' in 'model::DisjointDe
    6  scalar lowering not yet implemented for TypedUserCall
    5  multi-hop navigation dept.org.name is not supported yet
    5  scalar lowering not yet implemented for TypedTds
    5  'test::Sale' is not a known class, mapping, runtime, connection, or da
    5  aggregate reduce must be a native reducer call, got TypedCast
    5  scalar lowering not yet implemented for TypedPropertyAccess
    4  no aggregate lowering registered for resolved overload 'meta::legend::
    4  in call to 'meta::pure::functions::collection::add', argument 2: type 
    4  Invalid Input Error: Invalid type specifier "{" for formatting a value
```

### Run 2026-07-10 @ 95b683a

| tests | pass | failures | errors | skipped | green classes |
|---|---|---|---|---|---|
| 2721 | **2173** | 61 | 468 | 19 | 289 |

| bucket | exception lines |
|---|---|
| OTHER | 223 |
| CORE: unlowered constructs | 87 |
| FIXTURE: unknown refs (to diagnose) | 60 |
| CORE: scalar/agg function registrations | 22 |
| CORE(parse): query syntax gaps | 11 |
| CORE(G): overload/typing gaps | 6 |
| H: class sources / property nav / mappings | 1 |

Top detail:
```
   55  resolver bug: row-read rewrite hit TypedCast, outside the normalizer's
   37  'test::Person' is not a known class, mapping, runtime, connection, or 
   27  object-space expression node TypedUserCall is not substitutable yet (H
   26  no SQL type for Pure class meta::pure::metamodel::type::Any at the low
   18  scalar lowering not yet implemented for TypedNewInstance
   16  lowering not yet implemented for TypedUserCall
   13  scalar lowering not yet implemented for TypedMatch
   12  no scalar lowering registered for resolved overload 'meta::pure::funct
   10  'test::Order' is not a known class, mapping, runtime, connection, or d
    9  unknown SQL data type: 'JSON'
    6  multi-column pivot is not lowered yet
    6  no TDS cell rendering for Pure type meta::pure::metamodel::variant::Va
    6  scalar lowering not yet implemented for TypedFilter
    6  model-to-model binding of 'model::StaffComplete' in 'model::DisjointDe
    6  scalar lowering not yet implemented for TypedUserCall
    5  multi-hop navigation dept.org.name is not supported yet
    5  scalar lowering not yet implemented for TypedTds
    5  'test::Sale' is not a known class, mapping, runtime, connection, or da
    5  aggregate reduce must be a native reducer call, got TypedCast
    5  scalar lowering not yet implemented for TypedPropertyAccess
    4  in call to 'meta::pure::functions::collection::add', argument 2: type 
    4  Invalid Input Error: Invalid type specifier "{" for formatting a value
    3  aggregate reducer argument of kind TypedCast is not supported (literal
    3  in call to 'meta::pure::functions::collection::concatenate', argument 
    3  class java.lang.String cannot be cast to class java.lang.Number (java.
```

### Run 2026-07-10 @ 9dbd73a

| tests | pass | failures | errors | skipped | green classes |
|---|---|---|---|---|---|
| 2721 | **2223** | 60 | 419 | 19 | 297 |

| bucket | exception lines |
|---|---|
| OTHER | 196 |
| CORE: unlowered constructs | 64 |
| FIXTURE: unknown refs (to diagnose) | 61 |
| CORE: scalar/agg function registrations | 22 |
| CORE(parse): query syntax gaps | 11 |
| CORE(G): overload/typing gaps | 6 |
| H: class sources / property nav / mappings | 1 |

Top detail:
```
   55  resolver bug: row-read rewrite hit TypedCast, outside the normalizer's
   37  'test::Person' is not a known class, mapping, runtime, connection, or 
   26  no SQL type for Pure class meta::pure::metamodel::type::Any at the low
   18  scalar lowering not yet implemented for TypedNewInstance
   13  scalar lowering not yet implemented for TypedMatch
   12  no scalar lowering registered for resolved overload 'meta::pure::funct
   10  'test::Order' is not a known class, mapping, runtime, connection, or d
    9  unknown SQL data type: 'JSON'
    6  multi-column pivot is not lowered yet
    6  no TDS cell rendering for Pure type meta::pure::metamodel::variant::Va
    6  scalar lowering not yet implemented for TypedFilter
    6  model-to-model binding of 'model::StaffComplete' in 'model::DisjointDe
    5  multi-hop navigation dept.org.name is not supported yet
    5  scalar lowering not yet implemented for TypedTds
    5  'test::Sale' is not a known class, mapping, runtime, connection, or da
    5  aggregate reduce must be a native reducer call, got TypedCast
    5  scalar lowering not yet implemented for TypedPropertyAccess
    4  in call to 'meta::pure::functions::collection::add', argument 2: type 
    4  class query under TypedNativeCall is not resolvable yet (H2 vocabulary
    4  Invalid Input Error: Invalid type specifier "{" for formatting a value
    3  aggregate reducer argument of kind TypedCast is not supported (literal
    3  in call to 'meta::pure::functions::collection::concatenate', argument 
    3  class java.lang.String cannot be cast to class java.lang.Number (java.
    3  graph child 'department' of class 'model::StaffWithDept' is mapped as 
    3  class query under TypedIf is not resolvable yet (H2 vocabulary)
```

### Run 2026-07-10 @ 5a2f547

| tests | pass | failures | errors | skipped | green classes |
|---|---|---|---|---|---|
| 2721 | **2223** | 60 | 419 | 19 | 297 |

| bucket | exception lines |
|---|---|
| OTHER | 196 |
| CORE: unlowered constructs | 64 |
| FIXTURE: unknown refs (to diagnose) | 61 |
| CORE: scalar/agg function registrations | 22 |
| CORE(parse): query syntax gaps | 11 |
| CORE(G): overload/typing gaps | 6 |
| H: class sources / property nav / mappings | 1 |

Top detail:
```
   55  resolver bug: row-read rewrite hit TypedCast, outside the normalizer's
   37  'test::Person' is not a known class, mapping, runtime, connection, or 
   26  no SQL type for Pure class meta::pure::metamodel::type::Any at the low
   18  scalar lowering not yet implemented for TypedNewInstance
   13  scalar lowering not yet implemented for TypedMatch
   12  no scalar lowering registered for resolved overload 'meta::pure::funct
   10  'test::Order' is not a known class, mapping, runtime, connection, or d
    9  unknown SQL data type: 'JSON'
    6  multi-column pivot is not lowered yet
    6  no TDS cell rendering for Pure type meta::pure::metamodel::variant::Va
    6  scalar lowering not yet implemented for TypedFilter
    6  model-to-model binding of 'model::StaffComplete' in 'model::DisjointDe
    5  multi-hop navigation dept.org.name is not supported yet
    5  scalar lowering not yet implemented for TypedTds
    5  'test::Sale' is not a known class, mapping, runtime, connection, or da
    5  aggregate reduce must be a native reducer call, got TypedCast
    5  scalar lowering not yet implemented for TypedPropertyAccess
    4  in call to 'meta::pure::functions::collection::add', argument 2: type 
    4  class query under TypedNativeCall is not resolvable yet (H2 vocabulary
    4  Invalid Input Error: Invalid type specifier "{" for formatting a value
    3  aggregate reducer argument of kind TypedCast is not supported (literal
    3  in call to 'meta::pure::functions::collection::concatenate', argument 
    3  class java.lang.String cannot be cast to class java.lang.Number (java.
    3  graph child 'department' of class 'model::StaffWithDept' is mapped as 
    3  class query under TypedIf is not resolvable yet (H2 vocabulary)
```
