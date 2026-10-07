# Phase 3b census: why upstream's code fails to type, and who can reach it (2026-10-07)

`FailureCensus.java` runs the reference lane's own load-and-type loop (`OurResolutions.dump`, core_relational's
27-module closure) after Phase 3 and records why each source file is dropped and why each body fails to type:
`failures.tsv` (kind, subject, error, message, the first legend-lite frame). Run it from a probe directory on the
reference lane's classpath, with the action's `-Dlegend.engine.root` / `-Dlegend.pure.root` pointing at the real
checkout directories (the census's file walk does not follow the execroot's symlinks).

## Who can reach the 1,469 bodies that fail

| bodies | distinct functions | what they are |
| --- | --- | --- |
| 931 | 364 | engine machinery the platform never runs: protocol translators (16 versions of about 40 functions), the router, the engine's SQL generator, plan binding, JSON and graph-fetch internals, model-to-model internals |
| 460 | 460 | upstream's own test functions (the corpus runs some; those are in its rosters) |
| 52 | 52 | library functions user code could call: mostly legacy TDS functions the platform's forms handle, or reflection the platform does not run |
| 26 | 26 | other |

## The causes (bodies / distinct functions)

- 409 / 70 unknown function: 391 are `serializerExtension`, a qualified property of `Extension` that shares its name
  with a plain property (a dot call finds the property, misses the qualified one, then looks for a function).
- 131 unknown element: mostly elements in dropped files; `system::imports::coreImport` as a value (31); functions
  referenced by their mangled id as values (21).
- 121 / ~30 type-variable binding stricter than legend-pure (`Class<Any>` against `Type` and similar).
- 106 / 62 no version with that many arguments (`toJsonBeta`/2 60, `routeFunction`/2,4,7,8, `newMap`/2, `eval`/8, ...).
- about 220 a form refusing an older shape (`project` 65, `groupBy`'s `agg` 56, `match` 53, ...); 50 normalize-required
  functions not inlinable; 44 a JSON key-value restriction; 41 `validate` in a body; 27 walls enforced while typing;
  about 270 in some 30 smaller causes (9 index-out-of-bounds crashes).

## The 32 dropped files

18 use model-to-model mapping features the platform does not support yet (set-routed bindings, enum transformers,
explosions, two others): the model build refuses the whole file. 7 are "defined more than once": five upstream platform
files whose functions the boot layer also declares (the twin check fails), two the view lifted twice (F-L1). 7 name an
unknown type: units of measure (3), `Runtime`/`Mapping` (3), `HealthProfile` (1).

## The decision

With the user (2026-10-07): Phase 3b does what the bump and users need (the twins, the dropped mapping files, the
qualified-property lookup, a user-impact review of the 58 OVERLOAD and 14 PACKAGE rows) and drops typing the machinery
and upstream's unrun tests, and the lane instrumentation. See the plan's Phase 3b.
