# Evidence: the order census (2026-10-10)

**The question** (the user, 2026-10-10, before any TeaVM fix): does lite ever depend on the order of something that has
no order guarantee? Java promises no iteration order for `HashMap`/`HashSet` and their views, `ConcurrentHashMap`,
`IdentityHashMap` (order varies from run to run on the JVM), `Set.of`/`Map.of`/`Map.ofEntries`/`Set.copyOf`/`Map.copyOf`
(the JVM randomises their order on every start), the hash collections the `Collectors` build without a factory,
parallel streams, directory listings, reflection's member lists and identity hash codes. The TeaVM conformance test
(`//wasm:conformance_test`) found TeaVM's `HashMap` and `HashSet` iterate in a different order from the JVM's in 147 of
166 cases, so a dependence would show as a difference between the server and the tab.

**Method.** Every line of lite's main Java sources (`core`, `json`, `base`, `wasm`, `native`, `sdlc-server`,
`depot-server`, `warehouse`, `testing`; main at 0cbed2b9a) that creates or uses one of those constructs, found by
pattern (`inventory.txt`: 913 lines, then 279 more in the fully qualified forms, `new java.util.HashMap<>()` and
statically imported collectors, which the first pattern missed). Each site was traced by a reviewing agent (nine shares,
each reading the code from the collection to every use, across classes where it escapes one) and put in one class:
LOOKUP (never iterated), ITERATED-SAFE (iterated, but nothing can see the order: an order-free result, or sorted first),
ESCAPES (the order reaches something observable: output, a decision, a message, or test tooling only) or UNSURE. Every
ESCAPES row that reaches output or a decision was checked against the code by hand.

**Not covered:** test sources; the TypeScript and Python code (JavaScript's `Map`, `Set` and object keys iterate in
insertion order, apart from integer-like object keys); collections a library hands back (JDBC metadata, DuckDB).

## Result (`sites.tsv`, one row per site)

| Class | Sites |
|---|---|
| LOOKUP | 713 |
| ITERATED-SAFE | 431 |
| ESCAPES | 48 (output 3, behaviour 3, message 21, test-only 21) |
| UNSURE | 0 |

Where the order reaches output or a decision:

| Site | What happens | Varies |
|---|---|---|
| `compiler/element/PureModelContext.java:338` (`elementFqns()`, a `HashSet`) via `resolver/GenericTypeReflection.java:94` and `model/ClassMapping.java:74` (`classOfWitnessPrefix`) | first match wins: when two class names mint the same subtype-column prefix (`my::A__B` and `my::A::B`, which the method's own comment calls lossy), hash order picks which simple name a query result's type cell carries | JVM against the tab |
| `server/DiagramService.java:243` (`resolve`, a `HashSet`) | first match wins: a bare class name written in a diagram resolves to whichever same-named class in another package comes first; it becomes the generalisation parent or association end the diagram JSON names | JVM against the tab |
| `warehouse/.../WarehouseServer.java:108`, `:960` (`Map.copyOf` of the command line's ordered Postgres catalogs) | the catalogs attach in a per-run order; with two failing, which is reported or asked a password for first changes | every JVM start |
| `server/SavedQueries.java:134` (`Map.of`) | the HTTP 500 body's JSON keys come out in a per-run order | every JVM start |
| `server/PureLspServer.java:24` (`documents`, a `HashMap`) | diagnostics for several open documents go out in hash order of their URIs | JVM against the tab |

Where it reaches only a message's text: `normalizer/MappingPrePass.java:231` (which class a circular `~src` chain is
named from), `parser/section/ConnectionSectionGrammar.java:791` (which of several unknown mapper keys is named),
`server/serial/SerializerRegistry.java:19` (the order "available formats" are listed in), `sql/dialect/StoredReads.java:62`
(the columns a PIVOT refusal lists), and `platform/CoreFn.java` `OWNS` (17 `Set.of` rows: which name a duplicate-owner
error would name -- none exists, or the class would not load). The 21 test-only rows are census, probe and
"dangling registrations" listings, seen only in a failing build or a debug run.

Latent, not escaping today: eleven sites copy a hash-ordered collection into a `LinkedHashMap`/`LinkedHashSet`, which
keeps the hash order while looking ordered (only looked up today); `Pure.nativeClassFqns()` and `nativeEnumFqns()`
return hash-ordered key sets (no caller); `PersistenceReader.kindOf`'s refusal would name a `Map.ofEntries` order (no
colliding entry exists); `Json`'s Javadoc example writes a `Map.of`, whose keys would come out in a per-run order.
