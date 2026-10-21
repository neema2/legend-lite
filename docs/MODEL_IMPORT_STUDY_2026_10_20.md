# A model from a standard schema, mapped with SQL: import, map, join (2026-10-20)

**Status: steps 1 and 2 built on the branch `claude/model-import-sql-mapping` (2026-10-20), with the end-to-end test
green; then, the same day, YAML (§4.1), unions read as inheritance (§4.2) and lists as Variants (§4.3); then the reader
made whole against the real world (§4.4-§4.6): every JSON Schema keyword read, carried or noted, validations and logic
as constraints, and two corpora measured (973 of SchemaStore's 1,001 schemas, 614 of 701 OpenAPI documents). Nothing
else is scheduled.** It answers the user's question of 2026-10-20: import a data model from a standard
schema language (JSON Schema, OpenAPI, protobuf, GraphQL SDL, ...) and map it with SQL, "instead of having to learn
Pure". §0 is the answer; §1-§2 the brief and the spike; §3-§6 the design as built; §7 what proves it; §8 the open
decisions; §9 the roadmap.

---

## 0. Start here

1. **Three calls make a working model, and no class or mapping is written by hand.** The store, its connection and
   its runtime still are (§8, D9).
   - `importSchema`: a JSON Schema or OpenAPI document's schemas become Pure classes, enumerations and associations,
     in `<pkg>::domain`, where Create a Class writes its classes.
   - `mapClass`: a class is given its rows by a Postgres `SELECT`, checked against the class before anything is
     written.
   - `joinAssociation`: an association is joined on a property or key of each end's class.

   The model they write is ordinary Pure, read and reviewed as Pure. Queries, DataCube, Legend Query and the SQL
   islands all read it like any other model.
2. **Nothing new in Pure.** A SQL mapping is a function whose body is the `#SQL{...}#` island
   (docs/LEGEND_SQL_STUDY_2026_10_11.md §9). The class is mapped to that function as upstream's promotion maps one:
   `Relation { ~func ... }`, in Create a Class's layout. An association is joined by a ModelJoin, as Create an
   Association joins one. The spike (§2) found the compiler already plans all of this. The new code is the readers
   and the writer's checks.
3. **Carried, or said; never dropped.** Every keyword of a schema is read: carried as Pure says it, or as near as
   Pure can and noted. A validation is a constraint, and so is the schema's logic (`if`/`then`/`else`, `not`, ...): run
   on DuckDB, the constraints keep exactly the rows a JSON Schema validator keeps (§4.4). A JSON value of no one Pure
   type (a union not of objects, several types, a tuple) is a `Variant`, the type Pure holds JSON in, and noted. A
   schema is refused only when nothing in it can be imported, and a SELECT that does not fit its class is refused by
   name: every reason at once, and nothing written.
4. **One model, many schema languages.** A schema reader produces an `ImportedModel` (§3), and one writer turns it
   into Pure. GraphQL SDL, OData CSDL, Prisma, dbt semantic models and FINOS CDM are each one more reader (§9).

## 1. What binds this design (the user's words, 2026-10-20)

- "adding protobuf or json schema or something else very standard to build/import data models into legend-lite ...
  GraphQL sdl could be another? Maybe all of these and more as an import functionality?"
- "That plus a Mapping function in SQL could be powerful tool instead of having to learn pure".
- On the roadmap proposed in reply (start with the SQL mapping, then one importer end to end, then more formats):
  "Okay let's start with mapping sql and the json schema/open API to start and see if we can get an end to end".
- On the first build: "Let's do yaml; why one of/anyof refused is that different from inheritance? Why list columns
  can't be variant?" (§4.1, §4.2, §4.3).

## 2. What existed: the spike (2026-10-20)

Two classes mapped by `~func` to functions whose bodies are `#SQL{...}#` islands, and a ModelJoin between them, were
run on DuckDB with no change to the compiler. They returned the right rows:
- a projection of one class;
- a to-one navigation (a LEFT OUTER JOIN of the two SELECTs);
- an `exists` over a to-many navigation (a semi-join);
- a to-many navigation as a column.

An enumeration property read through an `EnumerationMapping` inside the same Relation mapping ran too. Measured gaps,
which the writer now closes (§5):

| A SELECT that ... | The compiler alone | `mapClass` |
|---|---|---|
| leaves a property without a column | compiles; fails at query time ("has no binding in mapping") | refused, naming the property and the SELECT's columns |
| gives a property a column of another type | refused, but the message prints the internal tree (`AppliedFunction[...]`) | refused: "Trade.quantity is Float, its column 'quantity' is String: cast it in the SQL" |
| names a column the class does not | refused against a synthesized function name (`test::M$class$test::Trade`) | refused, naming the class and property |
| spells `AS customerId` unquoted (Postgres folds it to `customerid`) | read: the island's identifier rule (the SQL study's D1) | read the same way |
| reads a nullable column into a `[1]` property | compiles silently | noted (§8, D3) |

Which column types a property takes was measured on the compiler (2026-10-20): its own type, or a kind of it. An
`Integer`, `Float` or decimal column fits a `Number` property. A `NUMERIC(p,s)` column fits `Decimal`. A `DATE` or
`TIMESTAMP` column fits `Date`. Nothing else fits: an `Integer` column does not fit a `Float` property. `MapClass.fits`
spells exactly that.

## 3. One model, many schema languages

`com.legend.protocol.schema.ImportedModel` is what every reader produces. It holds:
- **classes**: each with its superclasses, and its properties in schema order;
- **enumerations**;
- **notes**.

Each property is a primitive (or `Variant`), an enumeration, or a class of the model. A property typed by a class is
a REFERENCE, and the writer makes it an association.

`server/ImportSchema` is the one writer:
- Every element goes in `<pkg>::domain`, as Pure text, and is read back through the parser, the same door every
  model goes through.
- Each file is printed by the model printer and checked to read back as itself (`ModelChanges.file`).
- The whole project then compiles once, or nothing is written.

The reader lives in `protocol` because it depends on nothing but JSON (`ArchitectureTest.protocolIsTheBottomLayer`).
It is compiled to WebAssembly with the planner, so the page can import a schema in the tab.

**Why a reference becomes an association, not a class-typed property.** A Relation-mapped class reaches another class
only through a ModelJoin, and a ModelJoin joins an association. As an association, the reference:
- is mapped by `joinAssociation`;
- is navigable both ways;
- needs no change to the compiler.

The way back is named after the owner:
- plural and to many for a reference to a schema of its own (`trades: Trade[*]`);
- singular and to one for an inline object the owner holds (`order: Order[0..1]`);
- `<plural>As<Property>` when the plural would be taken: two references to one class (`tradesAsBuyer`,
  `tradesAsSeller`), or a class referring to itself (`employeesAsManager`).

Register row S89.

## 4. JSON Schema and OpenAPI (`JsonSchemaReader`)

The reader is three classes of `protocol/schema`: `SchemaDocuments` (the documents, their places, `$id`s and
`$anchor`s, and what a reference names), `SchemaNames` (the names it gives), and `JsonSchemaReader` (the model).

**Documents**, each written in JSON or YAML (§4.1). The reader takes three kinds of document:
- **OpenAPI 3.x**: its `components.schemas`; with none, or asked (`"operations": true`), each operation's request
  and response bodies too, as classes named after the operation (`CreatePetRequest`, `CreatePetResponseItem`);
- **Swagger 2**: its `definitions`, and its operations' body parameters and responses the same way;
- **JSON Schema**, drafts 4 to 2020-12: its root object (named by its `title`, or the request's `name`), `$defs` and
  `definitions`.

**References** reach any place in a document: a JSON pointer (`#/definitions/A/properties/b`, percent-encoded, `~0`
and `~1` escaped), an `$anchor` (`#name`), an `$id` (draft 4's `id`, and its `#name` plain fragments),
`$dynamicRef` and `$recursiveRef` (read as references), and another document. The other documents a reference names
are given in the request (`"documents": {"common.yaml": "..."}`), by the name a reference spells them (`./common.yaml`
and `common.yaml` are one; an `$id` names one too). A place reached twice is one class. A request may name the schemas
to import (`schemas`), and each brings the schemas it reaches.

**A named schema is made:**

| Schema | Pure |
|---|---|
| has `properties` (its own, or in a condition), or an `allOf` naming an object schema | a class; each `allOf` reference to an object schema a superclass, each inline part's properties its own |
| a union of objects (`oneOf` / `anyOf`) | a class, the superclass of its alternatives (§4.2) |
| `enum` of text, or a union of `const` texts | an enumeration (an inline `enum` of one text is a constant, a rule); each value documented by its `const`'s description, or `x-enum-descriptions`; named by `x-enum-varnames` (or jsonschema2pojo's `javaEnumNames`) when given; a value Pure cannot hold renamed (`buy-side` is `buy_side`, noted) |
| anything else (a number, a list, a free-form object) | no element; a reference to it reads as what it is |

**The root of a JSON Schema document** is a class when it is an object. When it is not:
- a list of objects: its item is the class (`RowsItem`);
- a map (`additionalProperties`, `patternProperties`): a class of its entries, `<Root>Entry {key: String[1]; value}`,
  the key's pattern (a lone `patternProperties`, `propertyNames`) its rules, an object value a class of its own
  (`<Root>Value`). A map below the root is a `Variant`, the JSON object it is (register S94);
- a reference alone: what it names (another document's root, given in `documents`). One naming a document not given
  is refused, naming the reference, when nothing else in the document can be imported.

**A property is:**

| Schema | Pure type |
|---|---|
| `string` (`format: date`, `date-time`, `time`; any other format is an annotation) | `String` (`StrictDate`, `DateTime`, `StrictTime`) |
| `integer`; `number` (`format: decimal`; `int32`, `int64`); both | `Integer`; `Float` (`Decimal`; `Integer`); `Number` |
| `boolean` | `Boolean` |
| `const`, or an `enum` not an enumeration (of numbers, of booleans, of one text) | its values' type, the values a rule; values of several types a `Variant` |
| `$ref` to a class / an enumeration | an association end / the enumeration |
| an inline object, an inline `enum` | a class / enumeration of its own, named after its owner and the property in upper camel case (`OrderShipping`, `OrderStatus`; Stripe's `Customer.tax_exempt` is `CustomerTaxExempt`); a list's item in the singular (`OrderLine`); one already made for the same text, below |
| a union of objects, or of objects and values | a superclass, one for each set of alternatives, a value alternative a class wrapping it (§4.2) |
| an `allOf` of no object | the one part that says a shape, or the shape its parts share |
| `true`, `{}`, a free-form `object`, a map; a union of no object, several types, a tuple (`prefixItems`), a list of lists | `meta::pure::metamodel::variant::Variant`, as Pure holds JSON (the last four noted) |
| `false`, `{type: null}` | no property: it can hold no value (noted) |
| `array` | the item's type, many; or, asked (`"lists": "variant"`), one `Variant` (§4.3) |

**Twins.** An object or an `enum` written in place whose JSON text is another's, in the same document, is that
one's class or enumeration, named after the first place: an API's request and response bodies repeat their inline
objects and enums, and openapi-generator's inline model resolver reuses them the same way. A schema the document
names (`$defs`, `components.schemas`, an operation's body) is never another's: its name is the author's; and a twin
that says one thing more (a description, `readOnly`) is its own. Each owner holds the shared class by its own
association, so a class written in place in many responses has a way back to each (loket.nl's response `Version`,
566). On the corpora (§4.5) this writes a third fewer classes and enumerations.

**Multiplicity:**
- `[1]` when the property is `required` (draft 3's `required: true` too) and not nullable; else `[0..1]`.
- A list is `[*]`, bounded by `minItems` (when required) and `maxItems`.
- Nullable is spelled five ways, and all are read: `type: [..., "null"]`, `nullable: true` (OpenAPI 3.0),
  `x-nullable` (Swagger 2), `anyOf` of a schema and `{type: null}`, and an enum holding `null`.
- A property a condition declares (`then`, `else`, `dependentSchemas`; a reference or a union inside one) is optional,
  noted: a value has it only when the condition holds. One branch forbidding a property does not hide another's
  declaring it.

**Rules**, each a class constraint named after its property (`tradeIdMinimum`), an optional property's guarded
(`$this.x->isEmpty() || ...`), a list's each value's (`->forAll(v|...)`):

| Keyword | Constraint |
|---|---|
| `minimum`, `maximum`, `exclusiveMinimum`, `exclusiveMaximum` (draft 4's booleans too) | `>=`, `<=`, `>`, `<`; a number past a 64-bit integer spelled as a float |
| `minLength`, `maxLength` | `->length() >= n`, `<= n` |
| `pattern` (ECMA-262's, matching anywhere) | `->regexpLike('<pattern in RE2>')`: the platform's regex dialect, matching anywhere (`server/EcmaPattern` writes ECMA's own spellings as RE2's: `\uXXXX`, `\cX`, an identity escape, `[^]`, a named group, `\p{Script=...}`); a lookaround, a backreference, a count past 1,000 noted, not carried |
| `multipleOf` of an integer | `rem(x, n) == 0`; of a fraction, noted |
| `const`; an `enum` not an enumeration | `->in([...])` |
| `uniqueItems` of primitives or enumeration values | `->removeDuplicates()->size() == ->size()`; of objects, noted (Pure tells objects apart by identity) |
| `dependentRequired`; draft 7's `dependencies` naming properties | `$this.a->isEmpty() \|\| ($this.b->isNotEmpty() && ...)` |

A schema's logic over a whole value is a constraint too (§4.4). A default (`default`) and `deprecated` are carried:
`= 3`, `= pkg::Mode.slow`, `<<doc.deprecated>>`. The documentation is the `description` (VS Code's
`markdownDescription`).

**Annotations, as legend-engine's import writes them** (its profiles `meta::json::schema::JSONSchemaGeneration`,
`...JSONSchemaTypeExtension`, `...JSONSchemaOpenAPIExtension`; §4.7):
- a name Pure cannot hold (a class's, a property's, an enumeration value's): the name in the schema,
  `{JSONSchemaGeneration.name = 'trade-id'}`, and `mapClass` reads an enumeration's values by it;
- `title` (where it is not the name), the first example, a default that is no Pure value: `title`, `example`,
  `defaultValue` tags;
- `readOnly`, `writeOnly`, `additionalProperties: false`: `<<readOnly>>`, `<<writeOnly>>`, `<<noAdditionalProperties>>`;
- `format` `int32`, `int64`, `double`, `byte`, `binary`, and nullable: `<<JSONSchemaTypeExtension.int64>>`, `<<...null>>`;
- OpenAPI's `discriminator`: `<<JSONSchemaOpenAPIExtension.discriminatorProperty>>` on its property, and each
  subclass's value, `{discriminatorName = 'cat'}` (its `mapping`'s key, else its name): what a subclass's SELECT tells
  its rows apart by.

A rename is no note: what it renamed is kept.

**Noted, not carried:** a validation Pure states no constraint for here (`minProperties`, `maxProperties`,
`contains`, `minContains`, `maxContains`, `propertyNames` below the root); logic that says what a constraint cannot
(§4.4); a map's values beside an object's properties; a default that is not one value of its property; a reference to
nothing, or to a document not given (a `Variant`); a class that would be its own superclass (the superclass closing
the circle dropped; an `allOf` naming its own schema skipped); a name taken twice (numbered). Names are kept when Pure
can hold them; else upstream's camel case, and a type's first letter upper-cased, the name in the schema a tag. A
note is said once, with the first place it was met and how many times, and carries a code (§6).

**Read as annotations, not noted:** JSON Schema's own (`examples`, `readOnly`, `$comment`, `contentMediaType`, ...),
OpenAPI's (`discriminator`, `xml`, `externalDocs`), `x-` extensions, and the keywords of the vocabularies schemas are
written with: VS Code's JSON service (`markdownDescription` read as the description, `deprecationMessage` as
deprecated, `markdownEnumDescriptions` as the values' documentation, `defaultSnippets`, `errorMessage`, ...),
jsonschema2pojo's (`javaType`, `javaEnumNames` read as the values' names, ...), json-schema-to-typescript's, and
Hyper-Schema's. **Any other keyword** is noted and not read: JSON Schema's own rule for a keyword a reader does not
know.

**Refused** (every reason at once): a document that is not JSON or YAML (with the YAML reader's line and column), one
whose root is not a mapping, one that defines no object (a root that is a reference to a document not given says
which), and a schema asked for (`schemas`) that the document does not have.

### 4.1 YAML (`com.legend.json.Yaml`)

Most OpenAPI documents are written in YAML. `Yaml.parse` reads a YAML document to the same `Json.Node` tree
`Json.parse` reads, so every reader of JSON reads YAML. It sits in the `json` module, beside the JSON reader, for the
dbt line's project files too. It has no dependency, and it is compiled to WebAssembly with the planner.

- **Read:**
  - block mappings and sequences, including a sequence under a key at the key's own indentation;
  - flow collections across lines;
  - plain and quoted scalars, folded as YAML folds them;
  - literal and folded block scalars, with chomping and indentation indicators;
  - comments, anchors, aliases, merge keys (`<<`);
  - the core schema's tags; `---`, `...` and directives; Windows line ends.
- **Typing, YAML 1.2's core schema:** `null`, `~`; `true` and `false`; integers (decimal, `0x`, `0o`); floats; else
  text. So `on`, `yes` and `012` are YAML 1.1's spellings, read as text, text and 12. A key is text, as JSON's are.
- **Refused, with line and column:**
  - a second document;
  - a complex key (`?`), or a collection as a key;
  - a key twice in one mapping;
  - a tag outside the core schema;
  - an alias to no anchor;
  - a tab in indentation;
  - `.inf` and `.nan`, which JSON cannot hold;
  - nesting deeper than 512;
  - bad indentation.
- **Checked against PyYAML 6.0.3 (2026-10-20):**
  - every YAML file in the repository (135) and the real Petstore OpenAPI document read the same, but the four GitHub
    workflows' `on:` key (PyYAML is YAML 1.1, and reads `on` as `true`);
  - 34 snippets of the hard cases read the same, but three where YAML 1.1 and 1.2 differ (`1e3`, `1.5e3`, a
    timestamp);
  - the 701 YAML documents of APIs-guru's OpenAPI directory (§4.5): every one reads, and the same as PyYAML's but
    where YAML 1.1 types a plain scalar (a timestamp, `yes`, `on`, a sexagesimal `23:59`, `2016_01_28`); PyYAML
    refuses 14 that are good YAML 1.2 (a timestamp out of range, a tab);
  - the Petstore document's YAML and JSON spellings import as the same model.
- **Three faults the directory found, fixed with a test each (`YamlTest`):** a plain scalar's further lines may
  start with an indicator (`` ` ``, `[`, `"`, `- `), only its first may not; an escaped space (`\ `) before a line
  break is the scalar's, not trailing space to trim; a literal block's line of more spaces than its indentation is
  text, not an empty line.

Register row S92 records that upstream's YAML readers are YAML 1.1.

### 4.2 Unions are inheritance

`allOf: [Base, {...}]` says "I am a Base, and more". `oneOf: [Cat, Dog]` says "I am exactly one of these". An object
model says both the same way: a superclass and its subclasses. So a union whose alternatives (but `null`) are two or
more, one of them an object at least, becomes a class:
- **A named union** (`Pet: {oneOf: [Cat, Dog]}`) becomes the class `Pet`, with any properties of its own. Each
  referenced alternative gains `Pet` as a superclass.
- **A union written on a property** becomes a superclass. Unions of the same alternatives in a document are ONE
  superclass, wherever they are written and however each is described: they are one type. It is named after its
  members when each object among them is a schema the document names, three at most (`CatOrDog`,
  `CustomerOrDeletedCustomerOrString`); else after the owner and the property where it is first met
  (`PaymentMethod`). The property is the association to it.
- **An object written in place as an alternative** becomes a subclass of its own, named after the union and its
  `title`, or `Option1`, `Option2`, ....
- **A value as an alternative** (a primitive, an enumeration, a list of them) becomes a class wrapping it, a
  subclass like the others, its one property `value` (with the alternative's rules), named after the union's objects
  and the value's type. Pure has no union of a primitive and a class; a superclass of classes is a union every reader
  of Pure reads, legend-engine's and Studio's included. Stripe's expandable fields (an object, or its id):

  ```
  Class CustomerOrDeletedCustomerOrString { }
  Class CustomerOrDeletedCustomerString extends CustomerOrDeletedCustomerOrString
  [ valueMaxLength: $this.value->length() <= 5000 ]
  { value: String[1]; }
  Class Customer extends CustomerOrString, CustomerOrDeletedCustomerOrString { ... }
  ```

  Each of Stripe's sixteen `customer` properties is an association to the one union, and `Customer` extends the two
  unions it is a member of, where it extended a superclass per property before unions were merged.

Three things are lost, and the register's S91 records them:
- **Closedness.** A union is closed (exactly these), and a superclass is open.
- **`anyOf`'s overlap.** An `anyOf` allows a value to match several alternatives, but a Pure instance has one class.
  An `anyOf` of objects is read as a `oneOf`, and noted.
- **The `discriminator`.** It is read as an annotation. Telling the subclasses' rows apart is the mapping's job (each
  subclass's own SELECT, `... WHERE pet_type = 'cat'`).

A union, or a union beside an `allOf`, that would make a class its own superclass loses the superclass closing the
circle, noted (`A: a class that would be its own superclass (A extends B extends A) does not extend B`). An `allOf`
naming its own schema names no superclass, and is skipped, noted.

**Not built: mapping a union's superclass, and querying through it polymorphically** (§8, D8). Each subclass maps and
queries as any class does, its inherited properties included (`ModelImportTest.unionsImportAsInheritance`).

A union of no object (`string | integer`) has no class form: its value is the primitive its alternatives share
(`Number`), else a `Variant`, as are several types, a tuple and a list of lists. A union one of whose alternatives
says no value (`oneOf: [{required: [a]}, ...]`) is logic (§4.4).

### 4.3 Lists and Variant

A list of primitives (`photoUrls: [string]`) is, faithfully, `String[*]`. But a Relation mapping reads one column as
one value, so no SELECT can give a `[*]` property its values. Measured 2026-10-20 on DuckDB:
- `String[*]` from a JSON column is refused by the compiler ("expected String, got Variant");
- `Variant[0..1]` from the same column reads the list whole, and `get(0)` and `toMany(@String)->size()` run.

Two ways, and both are in reach:
- **Built: `"lists": "variant"`.** Asked, an import makes a list of primitives or enumeration values one `Variant`:
  the JSON list a column holds, read by `mapClass` like any column (`ModelImportTest`'s YAML end-to-end test). The
  element type is said in the notes. A list of objects stays an association. The default stays `"many"`, the faithful
  type.
- **Proposed, the compiler's (§8, D7): a `[*]` property read from a JSON-list column.** The relation mapping's
  instance builder (`normalizer/MappingNormalizer`) reads the column as `->toMany(@T)`, and the database unnests it.
  The class keeps `String[*]`. It extends upstream's Relation mapping, so it would be a register row, and it is the
  compiler's owners' to take.

**Found on the way, for the compiler's owners:** a `Variant[*]` property mapped to one JSON column compiles. Each
instance then has one value, the whole list, where `[*]` promises its elements. `mapClass` refuses a many-valued
property, so its own mappings never write this.

### 4.4 Logic as constraints

A schema says more than shapes: `if`/`then`/`else`, `not`, a `oneOf` of requirements, `dependentSchemas`. Each is a
condition on a whole value, and the reader writes it as a constraint on the class's properties
(`ImportedModel.Condition`: a property present, a property's value keeping a rule, and `not`, `all`, `any` of them):

| Logic | Constraint |
|---|---|
| `if: A, then: B, else: C` | `(!A \|\| B) && (A \|\| C)`, named `condition` |
| `not: A` | `!A`, named `exclusion` |
| `oneOf: [A, B]`, of requirements (not a union of objects, §4.2) | exactly one: `(A && !B) \|\| (B && !A)`, up to six alternatives; `anyOf`, any |
| `dependentSchemas: {a: A}`; draft 7's `dependencies` with a schema | `!a? \|\| A`, named `aDependency` |
| a property's own `not`, `if` | `aNot`, `aCondition` |

Each condition is read as JSON Schema means it:
- **`required: [a]`** is `$this.a->isNotEmpty()`, always true of a `[1]` property;
- **`properties: {a: S}`** holds of a value without `a` (JSON Schema's `properties` says nothing of a property
  absent): `$this.a->isEmpty() || <S of $this.a>`. So `if: {properties: {kind: {const: "a"}}}` holds when `kind` is
  absent, as JSON Schema says;
- **into an object-typed property**, by its path: `$this.config.mode->toOne()->in([...])`;
- **references** are followed, a reference to itself not;
- a type the property's type always has is true; one it never has, false; one it may have (an integer of a `Float`)
  cannot be said.

The condition is simplified (`a && (!a || b)` is `a && b`), and a value is read (`toOne`) only after its presence is
tested, so the `||` and `&&` before it decide. A condition that holds of every value is no constraint; one of no
value is noted, not written.

**What a constraint cannot say is noted, and the condition not carried**, with why: a Variant's type or value, the
properties of each object of a list, a property the class does not have (a `then` re-typing a generic property
through a reference, the commonest), `minItems` or `contains` inside a condition, `additionalProperties: false`.

**Proven on DuckDB and on Postgres 16 (`ModelImportTest.constraintsKeepWhatTheSchemaKeeps`,
`ModelImportPostgresTest`).** A schema of every kind of rule and logic is imported, mapped by
`SELECT * FROM tb('store::DB.PAYMENTS')`, and each of its twelve constraints run as a query's
`filter(this|<constraint>)` over 18 rows, each row breaking one rule. On each database, each constraint rejects
exactly the rows it should, and together they keep rows 1, 3 and 14: the rows python-jsonschema 4.26.0 (Draft
2020-12) accepts. Register row S93.

**Patterns, checked against an ECMA engine (`EcmaPatternTest`).** Each of the 1,260 patterns in SchemaStore's first
1,001 schemas, against the strings the schemas give beside it (`examples`, `enum`, `default`) and 37 probes, 49,101
tests: Node 22's `RegExp` (with `u`) on the pattern, DuckDB's `regexp_matches` on its RE2 translation. The 1,217
translated agree on every string; the 43 not translated each look ahead. The first build wrapped a pattern in
`(?s).*(?:...).*` for a whole-text `matches`, which changed what `.` matched inside it, and let lookaheads through to
fail at query time on DuckDB.

### 4.5 Two corpora (2026-10-20)

Each document imported whole through the route's code (`ModelChanges.importSchema`: the reader, the writer, the
whole model compiled), one at a time with a minute each, by a scratch harness (not committed: the corpora are third
parties'):
- **SchemaStore.org's catalogue** (`src/schemas/json`, the first 1,001 by name), each with the sibling documents its
  references name;
- **APIs-guru's OpenAPI directory**, 701 documents (OpenAPI 3 and Swagger 2, mostly YAML).

| | SchemaStore | OpenAPI directory |
|---|---|---|
| imported | **973** of 1,001 | **614** of 701 |
| refused | 20 roots that are a reference to a document not given (each named); 8 that define no object (a root `anyOf` of texts, ...) | 87 that name no schema and whose bodies are free-form objects or examples only |
| the writer's defect (the model did not compile) | 0 | 0 |
| crashed, or over a minute | 0 | 0 |
| written | 40,362 classes, 6,454 enumerations, 44,909 associations; 23,597 rules and 390 conditions as constraints | 49,869 classes, 7,503 enumerations, 49,634 associations; 26,770 rules and 7 conditions as constraints |
| notes (each said once, counted) | 3,412 | 369 |
| time, a document | median 22 ms, the 99th percentile 0.5 s, the largest 5 s | median 22 ms, the 99th percentile 1.2 s, the largest 7 s |

At the start of the day the same SchemaStore run imported 511. The 736 conditions noted as not carried are §4.4's:
generic properties re-typed per branch, Variants, lists of objects. Before twins (§4) were one class the same
documents wrote 102,581 and 58,465 classes, 11,042 and 12,382 enumerations; before unions were one class for each set
of alternatives and a value alternative wrapped (§4.2, 2026-10-21), 65,649 and 48,457 classes, no outcome changing. Times are of one run among others on a
shared machine: forty typical documents, imported six times over in one process, take the same ~22 ms each before and
after this round's changes.

### 4.6 What the corpora taught

Each of these was found by the corpora and is built, with a test (`JsonSchemaReaderTest`, `YamlTest`):
- references by JSON pointer anywhere, anchors, `$id`s, draft 4's `id`, other documents (`documents`);
- boolean schemas; `const`, and `enum`s of numbers, as rules; a union of `const`s an enumeration;
- an OpenAPI document of no component schema read through its operations' bodies;
- roots that are lists, maps or a reference alone;
- conditions' properties, through references and unions, after the class's own; a branch forbidding a property not
  hiding another's;
- a class that would be its own superclass: the circle broken, noted, where it had refused the whole document;
- a whole number past a 64-bit integer (`maximum: 18446744073709551615`) spelled as a float, where it had failed to
  compile;
- an association's name numbered when another element has it, its way back numbered per class;
- the keywords of VS Code's, jsonschema2pojo's and json-schema-to-typescript's vocabularies read, not noted;
- `ModelChanges.put`, which checked each new element against every other (quadratic: 26 s for the largest document),
  answering from an index of packages and keeping each printed file (that document now under 5 s);
- the writer reading each element back three times: an element the importer writes as the printer prints it (98.8%
  of loket.nl's) reads back as itself by construction, so it is read once (`ModelChanges.put(String)`); an
  association's path was parsed back from its own text, and its ways back found by a scan of every reference; and
  twins (§4) written once. loket.nl (OpenAPI, 1.6 MB of YAML) imports in 7 s, from 13 s, writing 2.4 million
  characters of Pure where it wrote 5.1;
- twins: loket.nl wrote 5,120 classes, 4,386 of them another's body, mostly inline objects its operations repeat;
- YAML's three faults (§4.1);
- after the first build, by a generator (`ImportSchemaFuzzTest`, 250 seeded documents a build; 70,000 run once): two
  references whose association names collide in one import (`name` and `Name`, both `Root_Name`), which wrote two
  elements of one name. Nothing else in 70,000 documents of every keyword, malformed ones (a `properties` that is a
  list, an `enum` that is an object), self-references and OpenAPI operations crashed or wrote a model that did not
  compile.

### 4.7 legend-engine's import, compared (4.145.0, 2026-10-20)

Upstream generates a model from a JSON Schema through `###ExternalFormat`'s schema-to-model generation
(`legend-engine-xts-json/legend-engine-xt-json-pure/src/main/resources/core_external_format_json/transformation/
toBeRefactored/fromJSONSchema.pure`, its profiles in `models.pure`; read, not run: the reference tree is spec, never a
runtime part). Where the two agree, lite writes what upstream writes; where they differ, a register row says why.

| | legend-engine | lite |
|---|---|---|
| a `$ref` to an object | a class-typed property | an association (S89) |
| an inline object | `Any`, `<<JSONSchemaTypeExtension.object>>` | a class of its own (one for each text: twins, §4) |
| an inline `enum` | `String` and an `in([...])` constraint | an enumeration (two texts or more); one text a rule |
| a renamed enumeration value | `_1x`, `{JSONSchemaGeneration.name = '1x'}` | the same |
| a class's, a property's name | verbatim; a class named after its file | a Pure name, the schema's a `name` tag (S95) |
| `title`, `example`, `default`; `readOnly`, `writeOnly`; `additionalProperties: false`; `int32` ...; nullable; the discriminator | the `JSONSchemaGeneration`, `JSONSchemaTypeExtension`, `JSONSchemaOpenAPIExtension` profiles | the same profiles and names (a default that is a Pure value is the property's default) |
| `format: time`, `decimal`; integer with number | String; Float; not read | StrictTime; Decimal; Number |
| `type: [string, null]` on a property | not read | read |
| validations | one `<prop>_inLine` constraint, their `and` | a constraint each (S93) |
| `pattern` | `matches` (the whole text) | `regexpLike` in RE2 (anywhere, as JSON Schema's) |
| `if`/`then`/`else`, `not`, `dependentRequired`, `dependentSchemas` | ignored | constraints (§4.4) |
| `additionalProperties` schema, `patternProperties` | not supported | a root's: an entry class (S94); else a `Variant` |
| `oneOf`/`anyOf` | at the root, a Boolean function (`or`, `oneOf` of `instanceOf`s) | inheritance, a value alternative wrapped (S91) |
| unknown keywords, `x-` extensions | tags of a generated profile | noted (unknown), ignored (`x-`) |

**Found on the way, for the compiler's owners:** lite's compiler accepts a stereotype or a tag of a profile no model
declares (`{meta::nope::P.name = 'x'}` compiles, measured 2026-10-20), and upstream's three JSON Schema profiles are
declared nowhere in lite. The import's annotations compile by that leniency (D11).

## 5. Mapping with SQL (`MapClass`, `JoinAssociation`)

**`mapClass`** writes three elements, each in Create a Class's layout:
- the function `<pkg>::function::<Name>Fn`, its body the island, its return type the SELECT's columns;
- the mapping `<pkg>::mapping::<Name>Mapping`, holding the class's Relation mapping and its enumeration mappings;
- its include in `<pkg>::mapping::Model`.

It also makes the runtime it is given name the model's mapping. With no runtime given, it uses the one runtime that
already names the model's mapping.

Before writing, the SELECT is typed by the compiler over the project, and it must fit the class:
- **Each property gets one column.** The property's own class's and its superclasses' properties all count. The
  column is found, in order: by the property's name; by the name in another case; by the name upstream's camel case
  makes it (`trade_date` is `tradeDate`). Each match must be unique.
- **Types.** The column's type is the property's, or a kind of it (§2).
- **Enumerations.** An enumeration's property reads a text column through an enumeration mapping. Each value is read
  from its own name, or from the texts the request gives (`"enumerations": {"trades::domain::Side": {"BUY": ["B"]}}`).
- **What one column cannot hold.** A property holding many values, or a class, is refused, or left out by name
  (`"unmapped"`).

**Every column no property reads becomes a key**, a local property of the mapping
(`+customerId: Integer[0..1]: customer_id`). This is how a SELECT carries the foreign key an imported class does not
show: the schema said `customer: Customer`, and the table has `cust`.

**`joinAssociation`** writes the ModelJoin of an association the project already has:
`{customer: Customer[1], trades: Trade[1] | $customer.id == $trades.customerId}`, in the model's mapping. Each end is
named by its class, or by its end's name when both ends are one class. It is joined on a property or key of the end's
class, both of one type. The join is then walked both ways and planned before the answer.

## 6. The routes

All three are `POST /api/lite/v1/model/...` routes beside `createClass`, answered by `ModelChanges`:
- **On success:** the changes in the SDLC's shape (`{type: CREATE|MODIFY, path, pureCode}`), what was made, and the
  notes.
- **On refusal:** the answer is a model that does not compile (400, `COMPILATION`), and nothing is written.

| Route | Request |
|---|---|
| `importSchema` | `{"project", "package", "schema": {"format": "openApi"\|"jsonSchema", "content", "name"?, "schemas"?, "lists"?: "many"\|"variant", "operations"?, "documents"?: {name: content}}}` |
| `mapClass` | `{"project", "package", "class", "sql", "runtime"?, "unmapped"?, "enumerations"?}` |
| `joinAssociation` | `{"project", "package", "association", "ends": [{"class"?, "end"?, "on"}, {...}]}` |

**`importSchema`'s findings.** Beside the notes (one line each, for a person), its answer lists them as `findings`,
`{code, where, message, count}`, for a page to group, filter or link: `where` is the place in the document
(`#/components/schemas/Pet/properties/tag`), or the class a constraint was not written on. The codes:

| Code | What was not carried, or carried otherwise |
|---|---|
| `rule.not-carried` | a validation with no constraint here (`minProperties`, `not`, a pattern RE2 cannot run, ...) |
| `logic.not-carried`, `logic.never-holds`, `condition.optional-properties` | logic a constraint cannot say; a condition no value meets; a condition's properties optional (§4.4) |
| `variant.union`, `variant.types`, `variant.tuple`, `variant.list`, `variant.lists-option`, `variant.map-values`, `variant.enum`, `variant.file`, `variant.unknown-type`, `variant.not-a-schema`, `variant.self-reference` | a value of no one Pure type, held as a `Variant` |
| `property.no-value` | a property that can hold no value (`false`, `{type: null}`) is none |
| `reference.unresolved`, `root.unresolved` | a reference to nothing, or to a document not given |
| `inheritance.any-of`, `inheritance.cycle`, `inheritance.self`, `inheritance.not-object` | `anyOf` read as `oneOf`; a superclass dropped to break a circle; an `allOf` part not carried |
| `map.not-carried`, `root.map-entries` | a map beside properties; a root map read as its entries |
| `name.numbered` | a name another element has, numbered |
| `keyword.unknown` | a keyword no vocabulary the reader knows has |

## 7. Proven

- **`json/.../YamlTest`**: each YAML construct read as the JSON it is, the core schema, each refusal with its line
  and column, and the three faults the OpenAPI directory found. The differential runs against PyYAML are in §4.1.
- **`core/.../protocol/schema/JsonSchemaReaderTest`** (25 tests): each schema's shape, the types and multiplicities,
  inheritance, the spellings of emptiness, inline classes and enumerations and their twins, Variant (and the values of no one type),
  unions as inheritance, a cycle broken, lists as Variants, what has no class of its own (a Variant or a rule, noted),
  validations as rules and renames noted, `const`s and `enum`s, every kind of reference, boolean schemas, roots that
  are lists, maps or references, operations' bodies, defaults and deprecation, other vocabularies' keywords,
  conditions' properties, logic as conditions (through paths, and what cannot be said, noted), chosen schemas and what
  they reach, and a YAML document reading as its JSON does; upstream's annotations, and each note's code.
- **`core/.../server/ModelImportTest`** (11 tests):
  - an OpenAPI document imported as Pure (a validation a constraint), and imported again with no change;
  - **end to end**: imported, two classes mapped with SQL (one through an enumeration's codes, a column read by camel
    case, a key kept), the association joined, then two queries run on DuckDB through the association both ways, rows
    checked;
  - **constraints keep what the schema keeps** (§4.4): every rule and every kind of logic, run on DuckDB as filters,
    against python-jsonschema's verdict, patterns among them (RE2's, as `EcmaPattern` writes them); the one not
    carried, a lookahead, a finding with its code;
  - an enumeration's values renamed for Pure (`buy-side`), mapped from a table by their schema texts, and queried;
  - a SELECT that does not fit, refused with every misfit;
  - SQL that does not compile;
  - ways back named apart;
  - inherited properties and plain enumerations;
  - a join that cannot be;
  - **end to end from YAML**: a pet store written in YAML, imported with its lists as Variants, mapped from a JSON
    column, joined, and queried on DuckDB (a list's first element and size, the category through the association);
  - a union imported as inheritance, and a subclass mapped with its inherited property;
  - both routes.
- **`core/.../server/ModelImportPostgresTest`** (its own target, `//core:model_import_postgres_test`, in the gate's
  core lane): the same constraints on a real Postgres, through its dialect (RE2's patterns in Postgres's flavour),
  reject the same rows.
- **`core/.../server/EcmaPatternTest`**: what ECMA-262 and RE2 spell alike kept, what ECMA spells otherwise written as
  RE2 reads it, what RE2 cannot say said why; the translation checked differentially against Node's `RegExp` over
  SchemaStore's 1,260 patterns (49,101 strings, every translated pattern agreeing on every string).
- **`core/.../server/ImportSchemaFuzzTest`**: 250 seeded generated documents a build (70,000 run once), of every
  keyword, malformed ones, self-references, OpenAPI operations: each imported or refused by name, none crashing, none
  writing a model that does not compile.
- **The real Petstore** (`petstore3.swagger.io`, OpenAPI 3.0.4, JSON and YAML) imports whole with no refusal and no
  note, maps with SQL, joins and queries on DuckDB (a scratch driver, not committed: the document is a third party's).
- **The two corpora** (§4.5).

On 2026-10-20, under Bazel, in a cloud session set up by docs/CLOUD_SESSIONS.md: `//json:tests`,
`//core:core_tests_protocol`, `//core:core_tests_server`, `//core:core_tests_architecture`, `//core:guardrails` and
`//tools/deps:core_layering_test` passed, with NullAway and Error Prone; `//wasm:planner` built. Then the local gate's
`checks` and `core` lanes (`//gates:checks //gates:core`, 188 tests): 187 passed, the seven Postgres tests run as an
unprivileged user as `tools/postgres/as_unprivileged.sh` says (the container runs as root, which `initdb` refuses),
and `//tools/guards:classpath_test` is incompatible with this platform. Its one finding, parser-equivalence's
`own_corpus.matched` (2882 to 2892: `ModelImportTest`'s models matching the reference parser), is regenerated. The
gate's other lanes (`warehouse_jvm`, `datacube`, `ui`, `sdlc`) did not fit the session's disk; this line changes none
of their code. The guardrails found
what the earlier, outside-Bazel runs had not: the drop-in surface's text rule (`DropInSurfaceTextRuleTest`, no regex
in `protocol`: the reader's nine regex calls, four of them from the first build, are now character tests) and
TeaVM's `isBlank`.

The second round (patterns, upstream's conventions, Postgres, the fuzzer, twins, the writer's speed, the reader's
split), the same day, under Bazel: `//core:core_tests_protocol`, `//core:core_tests_server`,
`//core:core_tests_architecture`, `//core:guardrails`, `//tools/deps:core_layering_test`, `//json:tests`, and
`//core:model_import_postgres_test` (as an unprivileged user) passed; `//wasm:planner` built. The guardrails found a
helper the camel-cased names left unused, and the importer's notes built as `protocol` records in `server`
(`ArchitectureTest`'s protocol-construction ratchet), now its own `Finding`. Parser-equivalence's
`own_corpus.matched` moved 2892 to 2893 (the enumeration test's model matching the reference parser), regenerated.
Outside Bazel: the fuzzer's generator over 80 seeds (20,000 documents), none failing.

## 8. Decisions

**Decided by the user, 2026-10-20:**
- start with the SQL mapping and JSON Schema / OpenAPI, end to end;
- YAML ("Let's do yaml"): built, §4.1 (formerly D5).

**Open:**

- **D1. Import once, or bind live.** Built: once. The importer writes Pure the project then owns, and importing again
  writes the same elements over the ones it wrote. A live link (the schema file as the source of truth, re-read each
  build) could later be upstream's `###ExternalFormat` Binding (register S13).
- **D2. Where keys come from.** Today a key is a column the SELECT keeps (§5), as Create a Class keeps one; nothing
  writes `<<equality.Key>>`. Proposed: `<<equality.Key>>` only, each importer proposing it from what the format
  declares (an OpenAPI `x-` extension, GraphQL Federation's `@key`, CSDL's `Key`). This is the same decision as the
  SQL study's D6.
- **D3. A `[1]` property read from a nullable column.** Built: noted, not refused. The compiler cannot show a SELECT's
  column is never empty unless its table declares `NOT NULL`. Alternatives: refuse (strict, but most stores declare no
  `NOT NULL`), or check at run time.
- **D4. Measures.** Where a semantic layer's measures live (qualified properties, or DataCube's measures). This blocks
  the semantic-layer importers (§9) only.
- **D6. Validations as constraints.** Built (2026-10-20, the user's "super bulletproof with all the features"):
  every validation Pure states is a constraint, and so is the schema's logic (§4.4). Open: whether `mapClass` should
  check a mapping's rows against them (today they are the model's, for whatever validates instances).
- **D7. A list from a JSON column, typed.** The compiler change of §4.3, for the compiler's owners. Until it lands, a
  list is `[*]` and left unmapped, or imported as a `Variant`.
- **D8. Mapping a union's superclass.** Each subclass maps with its own SELECT. The superclass itself, and a query
  over it that returns each instance as its subclass, need a union of the subclasses' mappings. This line has not
  tried that in lite yet. The corpus census lists an inheritance-mapping wall
  (docs/FULL_RESIDUE_CENSUS_2026_08_30.md).
- **D9. The store.** The Database, its connection and runtime are still written as Pure, or made by DataCube's
  Create a Class from a source. A SQL DDL reader (the SQL study's phase 4) or a database's catalog would make them.
- **D11. Upstream's JSON Schema profiles.** The import writes `meta::json::schema::JSONSchemaGeneration` and its two
  siblings by name, as legend-engine's import does, and lite declares none of them (§4.7). Either the platform
  declares them (a declaration, as the platform ships its other profiles' shapes; verified against `models.pure`), or
  the compiler stays lenient on profiles. Until one lands, the annotations rest on the leniency.
- **D10. A Variant in a condition.** A condition naming a `Variant` property's type or value (about two in five of those
  not carried) needs a type test on a Variant the compiler does not have yet. For the compiler's owners.

## 9. The roadmap

| Step | What | Proven by |
|---|---|---|
| 1-2. **Built** | SQL mapping (§5); JSON Schema and OpenAPI (§4) | §7 |
| 2b. **Built** | YAML (§4.1); unions as inheritance (§4.2); lists as Variants (§4.3) | §7 |
| 2c. **Built** | every keyword read; validations and logic as constraints (§4.4); two corpora (§4.5) | §7 |
| 3. Use it | Python (`legend_lite`: `import_schema`, `map_class`, `join`), DataCube's "Map with SQL" and "Import a model", Studio; the store from DDL or a catalog (D9) | the page's tests |
| 4. Semantic layer | dbt semantic models (MetricFlow YAML), then Open Semantic Interchange, as classes, SQL mappings and ModelJoins, plus measures (D4) | `jaffle_shop`-class projects run end to end |
| 5. App developers | Prisma (its own small grammar); Pydantic, SQLAlchemy and Django by reflection in Python | real schemas |
| 6. Faithful metamodels | GraphQL SDL, OData CSDL (`Key` is identity: D2), Ecore / UML XMI (with Diagram elements) | golden tests, refusal tests, round trips |
| 7. Finance | XSD (ISO 20022, FpML), FINOS CDM (Rune DSL) | the CDM imports whole |
| 8. On demand | ODCS data contracts (to DataQuality), ISO GQL graph types (for the Cypher island), protobuf, Avro, Palantir Foundry's ontology metadata (its public API: keys and links) | per format |

**Every reader:**
- produces an `ImportedModel`, or refuses by name;
- gets a register row for each direction upstream does not have;
- gets a corpus run (for JSON Schema, SchemaStore.org's catalogue and APIs-guru's directory, §4.5); a committed
  ratchet over it waits on vendoring a sample (the corpora are third parties');
- gets the round trip where upstream generates the format from a model: the model imported from what upstream
  generates is the model.
