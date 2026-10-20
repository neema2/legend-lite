# A model from a standard schema, mapped with SQL: import, map, join (2026-10-20)

**Status: steps 1 and 2 built on the branch `claude/model-import-sql-mapping` (2026-10-20), with the end-to-end test
green. Nothing else is scheduled.** It answers the user's question of 2026-10-20: import a data model from a standard
schema language (JSON Schema, OpenAPI, protobuf, GraphQL SDL, ...) and map it with SQL, "instead of having to learn
Pure". §0 is the answer; §1-§2 the brief and the spike; §3-§6 the design as built; §7 what proves it; §8 the open
decisions; §9 the roadmap.

---

## 0. Start here

1. **Three calls make a working model, and no Pure is written by hand.**
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
3. **Refused, never dropped.** A schema construct with no Pure form, or a SELECT that does not fit its class, is
   refused by name: every reason at once, and nothing written. Validation keywords and renames are said in the
   answer's notes.
4. **One model, many schema languages.** A schema reader produces an `ImportedModel` (§3), and one writer turns it
   into Pure. GraphQL SDL, OData CSDL, Prisma, dbt semantic models and FINOS CDM are each one more reader (§9).

## 1. What binds this design (the user's words, 2026-10-20)

- "adding protobuf or json schema or something else very standard to build/import data models into legend-lite ...
  GraphQL sdl could be another? Maybe all of these and more as an import functionality?"
- "That plus a Mapping function in SQL could be powerful tool instead of having to learn pure".
- On the roadmap proposed in reply (start with the SQL mapping, then one importer end to end, then more formats):
  "Okay let's start with mapping sql and the json schema/open API to start and see if we can get an end to end".

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

**Documents.** The reader takes three kinds of document:
- **OpenAPI 3.x**: its `components.schemas`;
- **Swagger 2**: its `definitions`;
- **JSON Schema**: its root object (named by its `title`, or the request's `name`), `$defs` and `definitions`.

Only references within the document are read. A request may name the schemas to import (`schemas`), and each brings
the schemas it reaches.

**A named schema is made:**

| Schema | Pure |
|---|---|
| has `properties`, or an `allOf` naming an object schema | a class; each `allOf` reference to an object schema a superclass, each inline part's properties its own |
| has `enum` of text | an enumeration; a value Pure cannot hold renamed (`buy-side` is `buy_side`, noted) |
| anything else (a number, a list, a free-form object) | no element; a reference to it reads as what it is |

**A property is:**

| Schema | Pure type |
|---|---|
| `string` (`format: date`, `date-time`) | `String` (`StrictDate`, `DateTime`) |
| `integer` | `Integer` |
| `number` (`format: decimal`) | `Float` (`Decimal`) |
| `boolean` | `Boolean` |
| `$ref` to a class / an enumeration | an association end / the enumeration |
| an inline object, an inline `enum` | a class / enumeration of its own, named after its owner and the property (`OrderShipping`, `OrderStatus`); a list's item in the singular (`OrderLine`) |
| a free-form `object`, a map (`additionalProperties` alone), `{}` | `meta::pure::metamodel::variant::Variant`, as Pure holds JSON |
| `array` | the item's type, many |

**Multiplicity:**
- `[1]` when the property is `required` and not nullable; else `[0..1]`.
- A list is `[*]`, bounded by `minItems` (when required) and `maxItems`.
- Nullable is spelled four ways, and all are read: `type: [..., "null"]`, `nullable: true` (OpenAPI 3.0), `anyOf` of
  a schema and `{type: null}`, and an enum holding `null`.

**Refused, every one at once, each with the JSON pointer where the schema has it:**
- a union (`oneOf` / `anyOf` other than the nullable idiom);
- a tuple (`items` as a list);
- a value of several types;
- a list of lists;
- a reference outside the document, or to no schema of it;
- an enumeration of values that are not text;
- a keyword the reader does not know (`not`, `if`, `patternProperties`, ...);
- a document that is not JSON. **YAML is not read yet** (§8, D5).

**Noted, not carried:**
- a validation keyword (`minimum`, `maxLength`, `pattern`, ...): §8, D6;
- an object's schema-valued `additionalProperties` beside its properties;
- a rename. Names are kept when Pure can hold them; else upstream's camel case, and a type's first letter
  upper-cased.

**Ignored:** annotations (`description` is read as the element's `doc.doc`), and `x-` extensions.

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
| `importSchema` | `{"project", "package", "schema": {"format": "openApi"\|"jsonSchema", "content", "name"?, "schemas"?}}` |
| `mapClass` | `{"project", "package", "class", "sql", "runtime"?, "unmapped"?, "enumerations"?}` |
| `joinAssociation` | `{"project", "package", "association", "ends": [{"class"?, "end"?, "on"}, {...}]}` |

## 7. Proven

- **`core/.../protocol/schema/JsonSchemaReaderTest`**: each schema's shape, the types and multiplicities,
  inheritance, the four spellings of emptiness, inline classes and enumerations, Variant, every refusal at once,
  validations and renames noted, chosen schemas and what they reach, and YAML refused.
- **`core/.../server/ModelImportTest`**:
  - an OpenAPI document imported as Pure, and imported again with no change;
  - **end to end**: imported, two classes mapped with SQL (one through an enumeration's codes, a column read by camel
    case, a key kept), the association joined, then two queries run on DuckDB through the association both ways, rows
    checked;
  - a SELECT that does not fit, refused with every misfit;
  - SQL that does not compile;
  - ways back named apart;
  - inherited properties and plain enumerations;
  - a join that cannot be;
  - both routes.

On 2026-10-20 they ran outside Bazel (javac and the JUnit console, against the pinned DuckDB and JUnit jars), together
with:
- `CreateClassTest`, `ModelChangesTest` and `PureV1ApiTest`;
- `ArchitectureTest`'s rules;
- the source-scanning guards (`JavaEvalLedgerTest`'s funnel register gained the three new server classes, with a
  dated reason);
- NullAway and Error Prone, with the repository's flags, over the `protocol` and `pure_v1` sources.

The Bazel gate (`//gates:local`) is CI's to run on the branch.

## 8. Decisions

**Decided by the user, 2026-10-20:** start with the SQL mapping and JSON Schema / OpenAPI, end to end.

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
- **D5. YAML.** Most OpenAPI documents are YAML. A YAML reader (a JSON-compatible subset, shared with the dbt line's
  `dbt_project.yml` and `schema.yml`) is the next step of this importer.
- **D6. Validations as constraints.** `minimum`, `maxLength`, `pattern` and the like could become class constraints
  (`$this.quantity >= 0`). Built: noted, not carried.

## 9. The roadmap

| Step | What | Proven by |
|---|---|---|
| 1-2. **Built** | SQL mapping (§5); JSON Schema and OpenAPI (§4) | §7 |
| 3. Use it | Python (`legend_lite`: `import_schema`, `map_class`, `join`), DataCube's "Map with SQL" and "Import a model", Studio; YAML (D5) | the page's tests |
| 4. Semantic layer | dbt semantic models (MetricFlow YAML), then Open Semantic Interchange, as classes, SQL mappings and ModelJoins, plus measures (D4) | `jaffle_shop`-class projects run end to end |
| 5. App developers | Prisma (its own small grammar); Pydantic, SQLAlchemy and Django by reflection in Python | real schemas |
| 6. Faithful metamodels | GraphQL SDL, OData CSDL (`Key` is identity: D2), Ecore / UML XMI (with Diagram elements) | golden tests, refusal tests, round trips |
| 7. Finance | XSD (ISO 20022, FpML), FINOS CDM (Rune DSL) | the CDM imports whole |
| 8. On demand | ODCS data contracts (to DataQuality), ISO GQL graph types (for the Cypher island), protobuf, Avro, Palantir Foundry's ontology metadata (its public API: keys and links) | per format |

**Every reader:**
- produces an `ImportedModel`, or refuses by name;
- gets a register row for each direction upstream does not have;
- gets a corpus with a pass-rate ratchet (for JSON Schema, SchemaStore.org's catalogue);
- gets the round trip where upstream generates the format from a model: the model imported from what upstream
  generates is the model.
