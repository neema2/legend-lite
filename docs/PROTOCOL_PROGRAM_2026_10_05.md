# The protocol program: one typed hub for text and JSON (2026-10-05)

The user, 2026-10-05: "Yes let's do the protocol program the right way as experts." It replaces the Studio plan's B1
(`docs/STUDIO_FULL_PLAN_2026_10_04.md`) as the one program for everything that turns Pure text into protocol JSON
and back. Owner: `com.legend.protocol` in core. Branch `protocol` (on the Studio line's `studio-engine`); core work
announced in `docs/IN_FLIGHT.md` before each landing.

## 1. Why

Studio edits text; legend-sdlc, legend-depot and legend-engine speak protocol JSON (entities, PMCD). Opening a real
project means JSON to text; saving means text to JSON; and the two must agree exactly (S5: JSON to text to JSON gives
equal records). Today the layer that does this works three different ways:

| Leg | Today | Works on |
|---|---|---|
| parse: text to records | `PmcdParser`, `ElementParser`, `SpecParser` | typed records (`Protocol.*`, `protocol.spec.*`) |
| emit: records to JSON | `ProtocolEmitter` (+ `TailEmitter`, `MappingEmitter`, `ConnectionEmitters`, `GqlEmitter`) | typed records; byte parity with the engine over the corpus |
| read: JSON to records | `ProtocolReader` | typed, but **lambdas and value specifications only** |
| compose: JSON to text | `PureComposer` (values), `ModelComposer` + family composers (elements, B1) | **untyped JSON** (`Json.Obj`, fields by name) |

Half typed, half not, and no model reader. Every consumer picks its own path (the server's `grammarToJson` and
`jsonToGrammar` routes, the WebAssembly exports, the apps).

## 2. The architecture

One hub, the typed protocol records; four legs, each a pure function, each with its own exact oracle:

```
text ──parse──▶ records ──emit──▶ JSON
JSON ──read───▶ records ──compose─▶ text
```

**Invariants**

1. **Records are the only hub.** Every leg starts or ends at `Protocol.PureModelContextData` / `Protocol.Element` /
   `protocol.spec.ValueSpecification`. No consumer composes from JSON or emits from text; JSON is a wire format only.
2. **One printer.** `PureComposer` (values) and the element composers are one composer over records: one set of
   conventions (indentation, quoting, identifier rules, styles STANDARD/PRETTY), split into files by family only for size.
   Values inside elements print through the value composer, never a second way.
3. **One reader.** JSON to records for models and lambdas alike, through `ProtocolUpgrade` first (older wire forms
   brought current, as upstream's converters do). Numbers exact (a decimal's digits as written; the user chose exact
   decimals, `ComposerParityTest.EXACT_DECIMAL`). Unknown `_type`: refused by name -- never dropped, never guessed.
4. **Exactness over the whole corpus,** both against the engine (where the engine is the spec) and against ourselves
   (the round trips). An element a leg cannot handle is refused by name; plan S19: Studio then opens it read-only as
   JSON and preserves it byte for byte.
5. **Conversions in their layers, one contract, many transports** (revised 2026-10-09, the user; was "one public
   face", `com.legend.protocol.ProtocolText`, which the layering forbids: `protocol` may not call the parser,
   ArchitectureTest 7b, and the parser may not read JSON, 7c). Three kinds of thing, each with one home:
   - **The four conversions** -- a model's and a lambda's text to JSON, a model's and a lambda's JSON to text -- each
     one function taking its options, in the layer that owns its input, as legend-engine's parser takes
     `returnSourceInformation` and its composer the render style. Text to JSON is the parser's (source information
     on or off); JSON to text is protocol's (from the JSON's text, at protocol's depth limit, STANDARD or PRETTY).
     A failure is typed where it happens: a parse error with its position, a refusal naming what it refuses.
   - **legend-engine's `pure/v1` contract** -- its paths, query parameters, statuses, error body and request shapes --
     implemented once, by `PureV1Api`, over the conversions. Nothing else knows any of it.
   - **The transports** that reach the contract: lite's HTTP server, Python's engine (`//native:compiler`), and the
     tab. The apps' in-tab engine sends the same `pure/v1` requests as their remote one, so the tab and a server
     cannot answer differently, refusals included.
   - **One boundary for the embedded hosts, thin adapters** (the user, 2026-10-09). The tab and Python reach lite
     through one class, `planner.Boundary` (`//wasm:boundary`): plain Java, no host's annotations and no text
     encoding -- strings in, a string or an answer out, or an exception -- each operation once. legend-engine's
     operations it offers only as `pureV1(path, query, body)`; lite's own (planning a query written as text, test
     data's SQL, a Database from a catalog, a session's setup, warming a model, ...) by name. Each host has one
     adapter that is nothing but one-line delegations, and its list is that host's API: the tab's
     (`planner.TabExports`, TeaVM's entry class, each function `@JSExport`) and Python's (`native/`'s
     `nativelib.Compiler`, each `@CEntryPoint`). Both cross a strings-only line, so both use one text encoding,
     `planner.Folded` (`OK\n<answer>`, `ERR\n<class>\n<message>`, a `pure/v1` answer as `OK\n<status>\n<type>\n<body>`).
     The JVM differential compares the tab adapter's answers across the two builds. No adapter offers its own
     version of a `pure/v1` endpoint: Python's own grammar calls (`parse`, `print_tree`, `model_elements`) ask
     `pure/v1` too.
   In-process Java that is not a client of the contract -- the SDLC server, tools, tests -- calls the conversions
   directly, behind an interface of its own at its edge (leg 7), as legend-sdlc embeds the engine's grammar.
6. **Reference checkouts are spec and oracle only** (AGENTS.md): the engine's composer and serializer are compared
   against in `parser-equivalence`, never loaded by core.

**Source information.** Records carry `SourceInfo` (the parser fills it; the emitter writes it when asked). The reader
fills it from JSON when present; equality for the round trips is on records with source information stripped, as the
existing tests do (`SourceInformation.strip`), and the named spans (`classSourceInformation`, ...) with it. A span may
be absent (a model read without source information), and the emitter then omits its key, as the engine's `NON_NULL`
does; so the emitter cannot catch a parse site that forgot its span. Gate 8 does: the corpus sweep's claim 1a compares
the parser's emitted JSON, every span included, byte for byte with the engine's. The reader takes spans all or none
where a record keeps a part's position relative to its whole (a path literal's segments and arguments): a mix could
not be written back, so it is refused.

## 3. The oracles

| Leg | Oracle | Pin |
|---|---|---|
| emit | the engine's serializer on the engine's parse of the same text (exists: gate 8) | byte parity, up-only matched |
| read | `emit(read(J)) == J` byte for byte for every J the emitter produces over the corpus; and for the engine's own JSON of every corpus source (the engine parses, lite reads and emits) | up-only matched, zero mismatches |
| compose | the engine's `PureGrammarComposer` on the same JSON (exists: `ModelComposerParityTest`, `ComposerParityTest`), now fed `compose(read(J))` | the counts held exactly through the retarget, then up-only |
| round trip | `emit(parse(compose(read(J)))) == J` (sources stripped) over the corpus and the showcase projects | zero mismatches |

## 4. The legs, in order

1. **Read (the big new piece).** `ProtocolReader` grows from lambdas to models: every `Element` record, every nested
   record (mappings and their class/property mappings, stores, connections, runtimes, services and their tests, data,
   data spaces, diagrams, persistence, data quality, external formats, activators, ...). Structured like the emitter it
   mirrors (one reader per emitter family), so each pair can be read side by side. Proven by `emit(read(J)) == J` over
   the corpus before anything consumes it.
2. **Compose over records.** `PureComposer` and every family composer move from `Json.Obj` to records. Rules unchanged;
   field access changes. The parity counts must hold exactly at every step (a family at a time, the test run each time).
   The JSON-taking entry points remain only as `compose(read(J))` wrappers for the routes.
   **Decided 2026-10-08 (the user), before it starts:**
   - *JSON the reader does not know.* The reader reads every field and older shape legend-engine 4.145.0 reads (its
     deprecated protocol fields, about 26 across 35 classes, and its two protocol converters, already ported; and the
     older expression shapes `PureComposer` prints today that have no record and that the emitter never writes --
     `qualifiedProperty`, `hackedClass`/`hackedUnit`, the `class`/`enum`/`mappingInstance`/`primitiveType` pointers,
     `unitInstance`, `listInstance`, `aggregateValue`, the `tdsOlap*` shapes, legacy `values` arrays -- each read as
     the engine reads it, brought up to today's record where the engine brings it up, so `compose(read(J))` loses
     none of what `compose(J)` prints); where
     the engine silently ignores an unknown field (its about 24 `@JsonIgnoreProperties(ignoreUnknown = true)` protocol
     classes: Service, DataSpace, Diagram, the model context, ...), the reader refuses it, naming the field and the
     element, rather than drop it: nothing a person wrote is lost silently. Proven by the read oracle
     (`emit(read(J)) == J`, which catches a field read and not written back) and a parity test feeding the same JSON
     to the engine and to lite; the one difference (refuse where the engine drops) is a `SEMANTICS_REGISTER.md` row.
     (The other choices were: strict, refusing the engine's older fields too; and the engine exactly, dropping
     unknown fields where it does.)
   - *A table reference keeps how it was written.* `#>{db.schema.table}#` and the ordinary call
     `tableReference(db, 'schema.table')` are one record, told apart today only by whether the table name has a
     source position, which JSON without positions (Depot entities, a browser's save) does not carry. The record
     gets a written-form flag beside `propertyCall`, `grouped` and `infix`, set by the parser's island rule and the
     reader's `classInstance ">"` (through `AppliedFunction.tableReference`), read by the emitter and the printer;
     the compiler ignores it. The field is agreed with the compiler line first (the record is its W2.3a's). (The other
     choices were: a separate record for the island, as path literals have; and inferring from positions, wrong
     without them.)

   **Step 2, the older expression shapes, rule by rule (2026-10-08).** Each older shape is brought to the record that
   means what legend-engine 4.145.0 makes of it, and the evidence is the engine's own code (paths under
   `legend-engine-core-language-pure/`, `pp` = `legend-engine-protocol-pure/src/main/java/.../protocol/pure/`). Three
   kinds:

   | Older JSON | What the engine does with it | Lite reads it as |
   |---|---|---|
   | `class`, `enum`, `mappingInstance` `{fullPath}` | its reader turns it into an element pointer (`pp/v1/.../deprecated/Class.java`, `PackageableElementPtr.convert`) | the element pointer |
   | `primitiveType` `{name` or `fullPath}` | the same, `name` first (`PrimitiveType.java`) | the element pointer; both fields at once is refused |
   | `unitType` `{unitType` or `fullPath}` | keeps a unit pointer, `unitType` first (`raw/UnitType.java`) | the unit pointer |
   | `hackedClass` `{fullPath}`, `hackedUnit` `{unitType` or `fullPath}`, `genericTypeInstance` `{fullPath}` | its reader turns each into a type annotation (`HackedClass.java`, `HackedUnit.java`, `GenericTypeInstance.java`) | the `@Type` record |
   | `var` `{class}` (the type as a name) | read as the variable's type (`Variable.java`, "backward compatibility") | the typed variable; `class` with a type beside it is refused |
   | a multiplicity upper bound of `2147483647` | "many" (`Multiplicity.java`) | "many" |
   | a literal with `values: [...]` | none is an empty collection, one is the literal, more a collection (`PrimitiveValueSpecification.customParsePrimitive`) | the same; on `strictTime` and `byteArray`, where the engine drops the list, it is refused |
   | `path`, `rootGraphFetchTree`, `listInstance` written as their own `_type` | read as the `classInstance` of that kind, chosen by which fields are present, in the engine's order (`ClassInstanceWrapper.java`) | the same order, then the `classInstance` rule |
   | `qualifiedProperty` | kept, and compiled exactly as a property access with arguments (`ValueSpecificationBuilder` `processProperty`) | the property access |
   | `aggregateValue`, `tdsAggregateValue`, `tdsColumnInformation`, `tdsSortInformation`, `tdsOlapRank`, `tdsOlapAggregation`, `pair`, `listInstance` (as `classInstance` or their own `_type`), `unitInstance` (its own `_type` only) | kept, and compiled to the object that a library function builds: `agg`, `tds::agg`, `tds::col`, `tds::asc`/`desc`, `tds::func` (both), `pair`, `list`, `newUnit` (each function's body in `core/pure/tds/tds.pure`, `corefunctions/collectionExtension.pure`) | that function's call |
   | `runtimeInstance`, `executionContextInstance`, `alloySerializationConfig`, `whatever`, `unknownFunc` | kept; the engine's printers cannot write them as Pure (`DEPRECATED_PureGrammarComposerCore`, the Pure `toPure`), and the last two are marked "should not be coming to the system"; no test of the engine's carries one | refused by name: no text means them |

   **Decided 2026-10-08 (the user), refining the decision above:** the second kind is brought up to the call that
   builds the same object even though the engine keeps it as written (one record per meaning; text round trip holds;
   the engine reads and compiles the call the same) -- and so is a path literal's empty name, which the engine keeps
   and its library reads as no name (`tds.pure` `buildColumnNameOutOfPath`). A field the engine keeps and never acts
   on -- `fControl` on a call (it only logs a warning when the call resolves elsewhere: `CompileContext.testFunction`)
   and `class` on a property access (no compile step reads it) -- is kept on the record as a written form, ignored by
   the compiler, and written back. (The other choices were: keep each older shape as its own record, printed as the
   engine prints it; and read those two fields without keeping them.)

   A `multiplicity` on a single value, which the engine discards, is accepted when it says what the value already is
   and refused otherwise. Source positions follow the engine where it drops them (the legacy empty string, an empty
   `values` list). A list the JSON leaves out is the empty list wherever the engine's class starts it empty (nearly
   every list of every element), and the older layouts of elements (supertypes, a property's type and a function's
   return type written as names; the model's `domain`, `mappings`, `stores`, ... sections, merged in the engine's
   order) read as the engine reads them. Each deliberate difference is a `SEMANTICS_REGISTER.md` row (S29 to S34).
   The engine's printer mis-prints two of these shapes (`olapGroupBy(f)` for an olap rank, which is a different
   function; spacing in `agg('n',m, a)` and `list([a,b])`); lite prints the call the shape means, also a register
   row. The oracle (parser-equivalence): every engine test file holding older JSON, read by the engine and written
   back, against lite's read and emit -- the same JSON for the first kind, the named call for the second, the named
   refusal for the third; counted, matched up-only.

   **Step 2's outcome (2026-10-08).** `OlderJsonParityTest` over legend-engine's own test JSON (275 files, 119
   models): 772 elements written as the engine writes them, 332 as the documented upgrade of what it writes (the
   upgrade coded apart from the reader, two of its rules through the engine's own `HelperModelBuilder.getSignature`
   and `LegacyRuntime.toEngineRuntime`), 0 mismatched, 89 whole models read with their envelope (`serializer`,
   `origin`, now records); 19 elements refused by name -- 4 that the engine itself discards (S29), 11 that no record
   carries (S33), 4 open pending the engine's evidence (S33). Of the whole models, 89 read and 28 are refused (pinned
   down-only): 17 for the top-level `version` the engine discards (S29), the rest for an element refused above.
   `OlderShapesReadTest` (core) pins each expression shape against
   the text it means. The model context now carries `serializer` and `origin` (a model from an SDLC or Depot), and
   `AppliedFunction`/`AppliedProperty`, the class and association records, the table pointer and the relational
   association and embedded mappings carry the written details older JSON has (each excluded from equality where it
   is a record the compiler reads). Found on the way: the engine's own write-back changes a plain integer enum source
   value into a string (S32).

   **Step 3's outcome (2026-10-09).** Every printer in `core/src/main/java/com/legend/protocol/` prints the records:
   all 30 `*Composer.java` files (the plan said 31; one fewer exists), with `RelationalOperations`,
   `ElementFamilies` and `Composing`, moved in 16 commits a family at a time, leaves first, each
   step run against the parity tests. Every step held the counts exactly: `ModelComposerParityTest` 31,452 elements
   and 14,383 models matched, 0 mismatched; `ComposerParityTest` 56,988 lambdas; the reader oracles unchanged. JSON
   now reaches a printer only through four entries, each reading first: `ModelComposer.model` and `element` (the
   tests, and the routes of leg 3) and `PureComposer.lambda` and `valueSpecification` (the server's grammar route and
   the tests). One temporary bridge was used on the way (the test-data printer writing a service store's data back to
   JSON for the service store printer, not yet moved) and removed when that printer moved. Each family's JSON entry
   was removed once nothing called it, with `Composing`'s JSON helpers and `FunctionNames` (the printer's own second
   computation of a function's mangled name: a function prints under the declared name the reader keeps). The
   persistence sub-DSL record is a generic tree (a grammar kind and keyed entries); its printers look entries up by
   the grammar's keys and choose a printer by the grammar's kind. What a printer refused by name because the engine's
   printer cannot print it (Elasticsearch `ignore_above` and the like, MongoDB numeric bounds, a data quality
   persistence strategy, post-deployment actions) has no reader rule, so the reader refuses the same elements.

   One count moved, and why: a list value in a relational operation, such as `in(firmTable.ID, [2,3,4])`. The engine
   writes a list's items without a `_type`; the old JSON printer looked for one and refused the list. The reader
   already reads those items, and the record printer prints `[2, 3, 4]`, as the engine prints the same list straight
   from its grammar. The engine cannot print it from its own JSON (the items read back as maps), so in the parity
   table it is "upstream cannot, lite prints": 3 to 4 elements and 6 to 8 models (`testInClauseForJoinsAndFilters.pure`'s
   database, with and without its section index). `ModelComposerRoundTripTest` pins the list form. On older JSON,
   two prints now follow the engine where the JSON printer did not: a service without `autoActivateUpdates` prints
   `true` (the engine's default, which the reader keeps), and a service whose execution carries an older
   `legacyRuntime` prints it as the runtime the engine makes of it (`LegacyRuntime.toEngineRuntime`) instead of being
   refused.

   **The step's audit (2026-10-09)** found no blocker and no change in what the corpus prints. It found that the
   JSON printers printed a dozen shapes the reader refused: JSON that the engine writes for none of the corpus, but
   that it reads, from an older engine or a person. The user chose to fix them on this branch: under decision C the
   reader reads each, as the engine's code reads it, the writer writes it back, and the printer prints it as the
   engine's printer does. They are a function test's `doc` and an empty `assertions` list; a data quality tree node's
   alias and arguments; a data space's `featuredDiagrams` (diagrams titled `''`, S35); `extends` on every kind of
   class mapping; a MongoDB mapping without `~mainCollection`; a service store path segment's arguments; a generation
   node without its id (S35); file-generation settings that are decimals (kept as their text, as the engine keeps
   them) or `null` (Java null in the engine, which Jackson never hands its value deserializer: printed bare,
   `name: null;`, and written back as `null`); a hosted service's user list, a service test's keys and a column's
   `nullable` left out (or, for `nullable`, written null: false, the engine's primitive); persistence's optional
   output targets, test batches, test data, connection and assertions, and its `isTestDataFromServiceOutput` (true
   when left out; written `null`, not printed and written back as `null`, S37); a CSV table without values; a
   relational decimal spelled `1.50` or `1e3`. `OlderModelShapesTest` has one case for each, checking the JSON
   written back exactly for each shape lite keeps, and in today's form for each it brings up. Refused by name still: what the engine reads but
   cannot compile or print (S36), and what it refuses itself (an object where a generation element's path belongs; a
   settings list or map holding anything but strings). One case needed care: an operation mapping's `extends`, which
   the engine's grammar drops and lite's model keeps, is written back only when it came from JSON. The audit's other
   findings were fixed as well: a section index naming an older function beside a current one of the same name, the
   model printer's record entry building its index for a model without one, and the WebAssembly export reading each
   model twice. A second audit, of those fixes, found the `null` setting read as the text `'null'` (corrected, as
   above, after running the engine's own Jackson on it) and that lite's grammar dropped an aggregation-aware mapping's
   `extends`, which the engine keeps and also gives its nested set implementations (it parses them against the outer
   mapping's header): `MappingProtocolParser` now passes it to both. Found on the way: lite's persistence grammar
   accepts `];` closing a persistence's `tests`, which the engine's does not (a test snippet used it and was
   corrected); and lite's grammar takes no `doc` on a function test, which the engine's does.
3. **Folded into leg 4** (2026-10-09, the user): no new class (invariant 5 as revised).
4. **The four conversions complete, the model route, and the tab on `pure/v1`'s grammar** (invariant 5), in order:
   1. **The conversions with their options**: text to JSON with source information on or off (no caller strips it
      afterwards); JSON to text from the JSON's text at one depth limit per kind, in either style.
   2. **PRETTY for models.** legend-engine's `jsonToGrammar/model` prints in the request's `renderStyle`, PRETTY
      unless asked; lite's model printer is proven in STANDARD only (`ModelComposerParityTest` runs the engine's
      printer at its default). The style is passed down through every element printer to every value it prints, as
      the engine passes its composer context, and the parity test gains a PRETTY pass over the same corpus, exact.
   3. **`pure/v1/grammar/jsonToGrammar/model`** in `PureV1Api`: a `PureModelContextData` in, its text out,
      `renderStyle` as the engine reads it, a refusal in the engine's error shape. The engine has no batch form of it
      (checked, 4.145.0). `PureV1Api`'s grammar routes call the conversions and nothing else.
   4. **The tab speaks `pure/v1`'s grammar**: `pureV1OrError` exported to the tab (+13 KB, +8 KB gzipped).
      engine-client's in-tab grammar is the server's own client over the planner: `HttpEngine` over `plannerFetch`, a
      `fetch` the planner answers, so the requests, answers and refusals are a server's (a parse error is 400
      `PARSER`, as over the network). The worker and every test's in-process port answer through one function
      (`planner-answer.ts`), not each its own switch. DataCube's planner (`datacube/`, the DataCube + Python line's, its
      diffs reviewed by that line) asks the same route. The `Grammar` interface gains `modelText` (the new route),
      answered by a server and by the tab alike.
   4b. **The one boundary** (invariant 5): `planner.Wasm` split into `planner.Boundary` (the operations),
      `planner.Folded` (the encoding) and `planner.TabExports` (the tab's adapter, TeaVM's entry class: the export
      names the apps call are unchanged); `native/`'s entry points delegate to the boundary through the same encoding
      (their own copy of it goes). The four grammar twins go from every adapter: `jsonToGrammarModelOrError`,
      `modelJsonOrError`, `lambdaJsonOrError`, `composeLambdaOrError` from the tab's, and `lite_lambda_json`,
      `lite_compose`, `lite_model_json` from Python's, whose `parse`, `print_tree` and `model_elements` ask
      `lite_pure_v1`. Python's `LegendError.kind` for those three becomes the engine's refusal kind (`PARSER`, or
      the status) where it was a Java class name: the DataCube + Python line's decision, as `python/` and `native/`
      are theirs (their diffs reviewed by that line). A Bazel change: the boundary's sources and the TeaVM entry
      class (`wasm/BUILD.bazel`), reviewed by the Bazel program's session.
   - **Refusal texts that changed** (found by the leg's audit; no test or caller read the old ones): a lambda's JSON
     that is not an object is refused in one text everywhere, `protocol JSON: a lambda is not a JSON object` (it was
     `the request body is not a JSON object` on `jsonToGrammar/lambda`, `lambda JSON: not a JSON object` in the tab's
     and Python's planning calls); and the tab's grammar refusals (DataCube's and the apps' print and parse) are a
     server's -- a parse error 400 `PARSER`, any other refusal 500 with its exception's simple name before its message
     (`IllegalArgumentException: ...`) -- where they were the folded `ERR` text.
   5. The SDLC server's `CoreGrammar.modelJson` calls the text-to-JSON conversion (its edge moves in leg 7).
5. **Round trip proven** over the corpus and the showcase projects (plan S5), in the JVM and in the tab (the
   WebAssembly build of the same code, a differential run as `//wasm:differential_test` does for the planner).
   Design (2026-10-09; scope the user's: the upstream showcase projects included):
   - **Three inputs, all pinned.** legend-engine's test collection (`@legend_engine_src`, the corpus the parity tests
     read); lite's own projects (`projects/`, 60); and the upstream Legend showcase projects -- the eight the Legend
     docs list (`legend.finos.org/docs/showcases/showcase-projects`: logical modeling, the installer demo, relational
     mapping, service store, external formats, the query demo, GraphQL, the service execution jar), each a GitLab
     archive pinned by commit and sha256 among `MODULE.bazel`'s pinned downloads (not keyed on the engine release, so
     not `release.MODULE.bazel`'s; a Bazel change: the Bazel review, and a full CI run). They are SDLC projects in
     their Pure-file layout (123 `.pure` files, one element each, its section header first; no JSON entities), so each
     project is read as Studio would open it: every file parsed on its own, the elements gathered into one model with
     no section index (an SDLC's JSON carries none); files the engine's own parser refuses are counted apart, not
     tried.
   - **The round trip, per input, in the JVM** (`RoundTripProofTest`, `//parser-equivalence`): text → JSON → text →
     JSON, the two JSONs equal (no source information); and the JSON → text → JSON closure, in each style
     (STANDARD, PRETTY): reading the JSON and printing it, then parsing the print, gives the same records. An input
     the engine itself refuses is not counted; one lite refuses by name (S36, the printer's named refusals) is
     counted and listed as such, never as a pass; anything else is a failure, listed. The counts are pinned per input
     (matched up-only, failed down-only, 0 failed the target), as the other parity tests pin theirs.
   - **The same answers in the tab.** The JVM writes, as a build action (`java_run`, as `//wasm:jvm_answers`), each
     input and the tab adapter's own answers to it -- `grammarToJson/model` without source information, then
     `jsonToGrammar/model` in each style, through `TabExports.pureV1OrError`; a test loads the WebAssembly module,
     asks the same requests, and compares byte for byte (`//wasm:round_trip_test`). The whole of the three inputs;
     its time measured before it is placed in a lane (the heavy lane if it does not fit a pushed run's).
   - **The two parser gaps closed** (found by step 3): lite's grammar takes a function test's `doc` (the engine's
     does), and refuses `];` after a persistence's tests (the engine's does), each with the engine's own text as its
     test.
   - **Done when** the three inputs round-trip with equal records, in both styles, in the JVM and in the tab, every
     exception named, the counts pinned.

   **Leg 5's outcome (2026-10-09).** In the JVM (`RoundTripProofTest`, its own target
   `//parser-equivalence:round_trip_proof` beside `parser_parity`): the engine's collection 6,905 matched, 40 refused by
   name (the engine's own printer cannot print them), 270 where the engine's own print does not read back (counted
   apart, never as a pass), 0 failed; lite's projects 222 and the showcase projects 131 matched, 0 failed, each
   file and each project as one model; the counts pinned, the engine's own non-round-trips (270) among them, down
   only. In the tab (`//wasm:round_trip_test`, in the `parser_equivalence` lane; its JVM half the build action
   `//parser-equivalence:tab_round_trip`): all 9,423 inputs -- 9,134 of the collection, refusals included, 166 and 123
   project files -- answered as the JVM answers, in about 26 seconds. One difference from the design's wording: the JVM
   half writes each answer's SHA-256, not the answer (the answers would be several times the 48 MB of input text); the
   module's half asks the same requests and compares the digests, so a byte of difference still shows. Found and fixed
   on the way:
   - **Text to JSON without source information** kept the named spans the engine leaves out (3,241 corpus sources
     differed), and the engine keeps spans in five places whatever it is asked (`SourceInformation.withoutSpans`, from
     the engine's source; `CorpusSweepTest`'s claim 1c, 6,755 of 6,755).
   - **Where a brace-less lambda ends.** Lite read it by two rules of thumb; the engine's grammar decides it by the
     statement's position (probed in every position): in a sequence's first statement the lambda takes the `;` and the
     statements after it, in a later one the `;` is the sequence's (`SpecParser.readBraceLessBlock`).
   - **The engine's printer drops braces where its own parser then reads the print changed**: a brace-less lambda's
     body reads on as far as the grammar lets it, so it takes in what follows it -- the rest of a sequence's first
     statement (`let q = {|1}; ...`), or an arrow, a dot or an operator after it (`{x|...}->cast(@T)`, found by the
     parity test's statement-shape check); and a lambda whose one statement is a parameterless lambda prints `||...`,
     the or operator, which does not parse at all (found by the second audit). Lite keeps those braces, closed tight,
     and only those (the user's decision, `docs/SEMANTICS_REGISTER.md` S38); the parity tests count those prints apart
     (475 per style for models, 20 lambdas), each checked to differ only by the braces, to read differently in the
     engine's own parser (or upstream's not to read), and to read back with the statement boundaries of the JSON
     printed. With the braces kept, 83 sources whose print the engine's dropped braces had broken round-trip: the
     engine's-own-print count fell from 354 to 271 (and to the 270 above with S39 below).
   - **Five grammar gaps a print reaches:** a merge mapping's brace-less validation, a service's post-validation
     assertions separated by commas, a function test's `doc`, persistence's `];` (refused, as the engine refuses it), and
     a path literal across lines (its spans, by the engine's island rule, probed).
   - **TeaVM's class library answers two things differently from the JDK** (the tab's round trip found both):
     `String.isBlank()` counts only `' '` (a ModelStore island refused in the tab), and a double's text can end a digit
     off (diagram coordinates), and reading one back can land a unit in the last place off (the protocol twin test,
     `//pure-protocol:twins_test`). Lite now writes blank as `strip().isEmpty()` everywhere, and converts a double's
     text both ways through `com.legend.json.PortableText` -- `doubleText` (the JDK 19 definition) and `doubleOf`
     (the nearest double, ties to even), computed exactly, held to the JDK by `PortableTextTest` -- in the parser
     (through the protocol's `NumberText`, so the parser's dependency surface stays pinned) and the protocol, which
     reads a JSON number by its own text, never by the JSON library's double; `ArchitectureTest` guards both, the
     concatenations it cannot see read from the class files' constant pools. The twin test's accepted TeaVM
     divergences (30 doubles spelled differently, 2 read one unit off; the user, 2026-09-28) are gone: it now holds
     every double byte for byte. `doubleOf` takes a float literal's `f`/`F` (the engine's grammar writes it; the
     third audit's blocker) and computes the common case -- digits of at most 53 bits and a power of ten up to 10^22
     -- with one exact IEEE operation (Clinger's fast path), the rest with big integers: about 2.5 times
     `BigDecimal.doubleValue`'s cost on result-like values (measured 2026-10-09: 26 against 10 ms per 200,000), paid
     only where a model or code holds a number. The JSON library's own reader keeps the JDK's conversion (the user,
     2026-10-09): the protocol does not use it, and on the server it would only slow the data it reads; in the tab, a
     number read through it by anything but the protocol can still land a unit off, with PARK-23's writer.
   - **A column spec as a let's value** carried the let's span on its value too, where the engine keeps the value's
     own (as for a path literal and a graph fetch); found by the own corpus on S38's test text.
   Counts moved: `own_corpus.matched` (the new test texts); `ComposerParityTest`'s floor by the 20 S38 prints.
   - **The engine's printer drops a typed column spec's multiplicity** (`~a:Integer[1]` prints `~a:Integer`), so that
     print reads back without it: lite writes it (the user's decision, `docs/SEMANTICS_REGISTER.md` S39, "so our round
     trip is exact"); the form is written in five texts of the corpus (legend-pure's `TestColumnBuilders` and
     `AbstractTestColSpecAnnotations`), whose prints the model parity test counts apart (24 per style).
   Parked, found here: engine JSON with spans for a path literal across lines is refused by lite's reader, the spans
   not saying where the literal's lines break (`docs/PARKED_WORK_LEDGER.md` PARK-22, designed with leg 8); and
   `com.legend.json.Json`'s writer still writes a double the platform's way, which can differ by a digit in the tab
   (PARK-23: lite's exact writer, `PortableText.doubleText`, measured 85 times the JDK's cost on result-like values and
   up to 600 on the hardest, too slow for the server's results; the fix is a fast exact spelling, next after leg 5).
   Revised 2026-10-10: the fix goes into TeaVM's class library, after a conformance test of it against the JVM (the
   user kept the tab on TeaVM after a GraalVM Web Image spike, `docs/WEB_IMAGE_SPIKE_2026_10_10.md`). Done the same
   day (`third_party/teavm_classlib`): TeaVM's conversions are the JDK's, so `PortableText` was retired and the protocol
   and the parser use the JDK's own again, on the server at the JDK's speed.
6. **One whole-model compile, and the tab's other `pure/v1` twins on the route.** "Compile a whole model" (its
   elements, then every body in it) is strung together three times today: `PureV1Api.compile`, the tab's
   `compileOrError` and the SDLC server's `CoreGrammar.compile`. It becomes one function on the compiler's front door
   (`com.legend.Compiler`), and `compilation/compile` calls it. The boundary's remaining operations that duplicate a
   `pure/v1` endpoint -- `compile` (C1), and whichever of `planJson` and `relationTypeJson` are true twins of E9 and
   E5 (checked first: `planJson` answers `{sql, type}`, not the engine's plan) -- go from both adapters (the tab's
   `compileOrError`, `planJsonOrError`, `relationTypeJsonOrError`; Python's `lite_plan_json`,
   `lite_relation_type_json`), their callers moved onto the route as in leg 4. The boundary's own operations that
   are not `pure/v1` endpoints stay.
   **Decided 2026-10-10 (the user), at the leg's start:** `compile` is not a true twin -- the tab's answers every
   error, legend-engine's `compilation/compile` its first. So: one implementation that answers every error, and two
   routes over it on the one dispatcher every host serves (lite's server, the tab's `pureV1OrError`, Python's
   `lite_pure_v1`): `/api/pure/v1/compilation/compile`, legend-engine's exact answer (OK or the first error), and
   `/api/lite/v1/compilation/compile`, lite's own (every error; legend-engine has no such route, so a client asks it
   first and falls back to the engine's). The tab's `compileOrError` and the SDLC server's copy go; Studio asks the lite
   route in the tab and on a lite server alike. `relationTypeJson` is E5's true twin (the same computation, the same
   renderer; checked): it goes from both adapters. `planJson` stays (not E9's). Every error means today's: the first
   element error alone, else every body's; collecting every element error (the tolerant build) is a later step.
7. **The SDLC server's rules free of Pure.** SDLC's rules (`//sdlc-server:rules`) depend on all of `//core` today,
   because the class that answers their two Pure questions (`CoreGrammar`) sits in the same library; nothing stops
   the rules from calling the compiler directly, and the SDLC server carries the whole engine. The rules keep asking
   through interfaces -- read a file's text into an entity; check a tree before a review lands or a version is cut
   -- and depend on no Pure code. A separate adapter library answers them: reading through the text-to-JSON
   conversion, checking through leg 6's compile in-process, or through an engine's `compilation/compile` over HTTP
   where a deployment wants it (legend-sdlc leaves compiling to the project's build). The server binary wires them,
   and the build enforces the split: `//sdlc-server:rules` with no `//core` dependency, a Bazel change reviewed by
   the Bazel program's session.
8. **The server reads models as records.** `PureV1Api.connectionOf` finds a runtime's connection by reading the JSON
   the parser wrote, where invariant 1 says to read the records; and the compile, plan and execute routes take a model
   as text only (`modelText`: "the PMCD reader is not built", which it now is), where legend-engine also takes a
   `PureModelContextData`, as upstream Studio sends. How the compiler takes a model given as records -- straight into
   its model (the direction ArchitectureTest 7c sets the parser) or through the text printed from them -- is decided
   at the leg's start, before code.

Then the Studio plan's B2 to B4 (real legend-sdlc, Depot and engine) build on it: entities JSON read and printed in the
tab, text parsed and emitted on save.

## 5. Guards and process

- Core's guard tests apply unchanged (file-size caps, no regex in the protocol package, the literal-name census, the
  layering file); no pin is raised -- a guard that flags is answered by restructuring.
- Gate 8 carries the parity and round-trip oracles; core's own tests carry the record-level round trips on
  representative models.
- Each leg lands on `protocol` with its oracle green and the local gate green; Bazel changes reviewed by the Bazel
  program's session before a push.

## 6. Parked behind this program

**User tests** (the Studio plan's A4): tests run in the tab and on a server must give the same verdict, so they get one
architecture end to end -- the pipeline (plan, provision, execute, serialize, judge), the owner of each step, execution
and serialization parity, where judging lives (the database owner's paused `//core:judge` question), the test runtime,
mapping/function/service suites and every assertion kind, and a both-ways parity proof -- designed after this program,
on the round trip's foundation. The provisional work is kept on `studio-tests-parked`.
