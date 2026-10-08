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
5. **One public face**: `com.legend.protocol.ProtocolText` (name to settle in leg 3) -- `grammarToJson(model|lambda)`,
   `jsonToGrammar(model|lambda, style)`, `read`/`emit`/`compose`/`parse` on records -- and every surface calls it: the
   server's `pure/v1/grammar/*` routes, the WebAssembly exports, the tests. Same code in the tab and on a server.
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
     deprecated protocol fields, about 26 across 35 classes, and its two protocol converters, already ported); where
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
3. **One public face** (invariant 5), and the consumers moved onto it: `PureV1Api`'s grammar routes, `Wasm.java`'s
   `modelJsonOrError` / `lambdaJsonOrError` / `composeLambdaOrError` / `jsonToGrammarModelOrError`, and the apps'
   clients (engine-client's grammar interface) unchanged in shape.
4. **The model routes**: `pure/v1/grammar/jsonToGrammar/model` on lite's server (and its batch form), the WebAssembly
   export already added in B1 moved onto the face.
5. **Round trip proven** over the corpus and the showcase projects (plan S5), in the JVM and in the tab (the
   WebAssembly build of the same code, a differential run as `//wasm:differential_test` does for the planner).

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
