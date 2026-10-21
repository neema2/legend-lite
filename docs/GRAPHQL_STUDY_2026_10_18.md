# GraphQL in Pure: the typed object query over the class model, generated from the model, valid everywhere

**Status: study with the user's decisions (2026-10-18). Nothing is scheduled or announced in `docs/IN_FLIGHT.md`;
§4's decisions are the user's rulings of 2026-10-18 and are locked; the slices of §10 are proposals.** This is the
fourth study of its kind, after `docs/LEGEND_SQL_STUDY_2026_10_11.md` (Postgres SQL, `#SQL{...}#`),
`docs/IQ_SQL_STUDY_2026_10_13.md` (SAP IQ, `#IQ{...}#`) and `docs/CYPHER_STUDY_2026_10_15.md` (Neo4j Cypher,
`#Cypher{...}#`). It answers the user's ask of 2026-10-18: after a relational frontend and a graph frontend, an
**object** frontend, standards-based, returning trees, that humans and agents will understand and like.

Two things make this line different from the three before it, and both were the user's call:

- **No upstream compatibility.** legend-engine has a GraphQL extension (`legend-engine-xts-graphQL`, the `#GQL{...}#`
  island, a Query class whose functions are root fields, Hasura's `where` operators, `@totalCount`). lite already
  parses that island byte-exactly for the wire (`parser/GqlParser.java`, `protocol/spec/GqlIsland.java`) and refuses
  to compile it (`compiler/spec/Typer.java`, the `GqlIsland` arm). The user ruled on 2026-10-18 that the engine's
  design constrains nothing here: the goal is the best protocol, not the compatible one. The engine's extension is
  read as prior art only (§3.4).
- **The language is standard; the dialect is the schema.** The user wanted three things at once: typed, not strings;
  valid GraphQL; nice. GraphQL has exactly one typed, valid extension mechanism, the input objects, enums and custom
  scalars a schema declares. So every convention of this line (filters, sorting, paging, aggregates, groups,
  inheritance) is a rule of the **schema generator** that reads a Pure model, and every query is plain GraphQL that
  graphql-js parses, any GraphQL tool validates against the generated SDL, and lite's compiler types against the model.
  Nothing in a query is a string that lite parses; nothing in a query is an expression.

§0 is the answer; §1 the method; §2 what the frontend is; §3 the spec, the prior art and the oracles; §4 the
decisions; §5 the generated schema, rule by rule; §6 the translator; §7 the worked example; §8 the protocol around
it; §9 the harness; §10 the slices; §11 the risks; §12 the sources; §13 the record of landings.

---

## 0. Start here

1. **What it is.** A GraphQL endpoint per data space, whose schema is generated from the Pure model, and the same
   document inside Pure as the `#GQL{...}#` island. A query selects a tree of objects with typed filters, sorting,
   paging, aggregates and (later) groups at every level; the answer is nested JSON the database builds:

   ```graphql
   {
     Firm(where: { legalName: { startsWith: "A" } }, orderBy: [{ legalName: ASC }], limit: 10) {
       legalName
       employees(where: { age: { gt: 30 } }, orderBy: [{ salary: DESC }], limit: 3) { fullName salary }
       headcount: employeesAggregate { count }
     }
   }
   ```

   The translator turns the document into Pure (`Firm.all()->graphFetch(tree)->serialize(tree)` with the clauses on
   the tree's nodes, §6); the compiler types it once, like any query; the graph emitter lowers each node to a
   correlated subselect and the database returns the JSON (`resolver/GraphEmission.java`, which exists).
2. **Why GraphQL, and why not GraphQL as the vendors extend it.** The research (§3) found that LLM accuracy at writing
   a query language tracks its volume in training data, not its typedness; that text beats JSON query objects; that
   validation before execution is the single biggest accuracy lever; and that GraphQL has no standard for filtering,
   sorting, paging or aggregation, so each vendor bolts on its own object dialect, Hasura's underscored one being the
   most copied and the least liked. The selection set is the one tree syntax every human and every agent already
   reads. The bolt-on is where the choice lies, and the user chose the most-seen typed filter shape in training data,
   Prisma's, over Hasura's (G2).
3. **The dialect is a generator, not a parser.** Per class the generator emits the object type, `XWhere`, `XOrderBy`,
   `XAggregate` with its `XSum`/`XAvg`/`XMin`/`XMax`, `XGroups` with `XGroup`/`XGroupKey`/`XGroupHaving`/
   `XGroupOrderBy`, and `XInterface` where the class has subclasses; per collection property the `pAggregate` and
   `pGroups` siblings; per subclass the `asY` downcast in the inputs. The names are reserved; a model whose property
   collides is refused by name (G11). Agents learn one rule: everything about `Person` is `Person` plus a suffix.
4. **Inheritance is GraphQL's one real mismatch with Pure, and it is settled.** GraphQL interfaces are abstract and
   every name in a schema is unique, so a concrete base class cannot be both the interface and the object type. The
   ruling (G8, G9, G10): a class with subclasses gets `XInterface`; its concrete type keeps the class name, so
   `__typename` is always the exact class; inputs carry `asY` downcasts since interfaces are never inputs; and lite's
   validator refuses the one silent trap GraphQL leaves open (an inline fragment on a non-leaf concrete type inside an
   ancestor-typed selection), with the fix in the message.
5. **Two free oracles, both real.** graphql-js (MIT) judges validity: a differential over random documents against the
   generated SDLs, lite's acceptance a subset of graphql-js's, never more. Prisma (Apache 2.0) over the Postgres the
   lanes already embed judges semantics: because the filter, sort, paging, aggregate and group vocabularies are
   Prisma's, each case carries a mechanical Prisma twin, snapped locally and committed, with the IQ line's STALE,
   UNSNAPPED and GONE guards. Inheritance, which Prisma lacks, is judged by SQL twins on the Postgres lane.
6. **The agent surface is four tools.** `describe` (the SDL for the classes asked about), `validate` (the typed result
   shape, or every error with phase, position, message and the nearest valid name), `query` (read-only, row-capped,
   a warning when `limit` lacks `orderBy`) and `explain` (the Pure and the SQL). Apollo's MCP server and GraphiQL work
   against the endpoint unchanged, because it is GraphQL.
7. **Size, and nothing new in the language.** Smaller than the Cypher line by a wide margin: the reader exists (720
   lines), the tree egress exists, and the dialect is a generator over `ModelContext` plus a structural walk over typed
   input objects. Every shape the schema needs is a generated Pure class in a companion model per data space, and every
   clause is a derived body composed from Pure's own functions (G15, G16, §5.8, §6.2); no new native and no new
   spelling. The new pieces are the generator, the validator's rules beyond graphql-js, the translator, three small
   lowering increments, and the harness: six landings (§10), the bet measured at the second.
8. **The one-line rule of the line,** inherited: zero FAIL, zero BUG, zero ERROR, always; a refusal by name beats a
   possibly wrong answer; Java computes no value.

---

## 1. The method this line inherits

From the three studies before it (their §1s, the Cypher study's §8 and §9):

- **One translator into one target.** The document becomes a Pure `ValueSpecification` the Typer types once; nothing
  is re-spelled as text and nothing is typed twice (AGENTS.md invariant 1). The generated Pure is dumped per case so a
  review reads it.
- **The spec is real; the oracle is real; CI judges from a file.** Here the spec is the GraphQL specification
  (September 2025 edition) for the language and this document for the generated schema; the oracles are graphql-js
  and Prisma over Postgres (§3.3); answers are committed with the STALE, UNSNAPPED and GONE guards.
- **Statuses that move only with a dated entry:** PASS, FAIL, BUG, ERROR, REFUSED, BOTH_ERROR, SKIP per case per lane,
  in a ledger the test ratchets.
- **Java never computes a value.** Every comparison has one SQL lowering; every aggregate is the database's; the
  endpoint carries JSON, it does not build it.
- **Randomized differentials and independent reviews.** A generator of type-correct documents over a generated
  schema; each landing reviewed by an agent that wrote none of it.

What is new in method: **no corpus exists for this frontend** (GraphQL has no conformance suite beyond graphql-js's
own tests, and the dialect is ours), so the cases are lite's own from the first landing, as the Postgres line's
`lite/*.yaml` were, and the randomized generator carries more of the weight than it did for Cypher, whose TCK had
1,615 scenarios.

---

## 2. What the frontend is

### 2.1 Valid GraphQL, exactly

A document is what the September 2025 edition of the specification calls an executable document: operations
(anonymous or named, `query` only; no mutations or subscriptions in this line), variables with declared types,
selection sets, aliases, arguments, named and inline fragments, `@include` and `@skip`, `__typename`, and the
introspection fields. lite's `GqlParser` reads all of it today. What a query may NOT contain is anything the
specification does not: no expressions, no operators, no paths in field names. The whole dialect is in the arguments'
input types and the fields' names, which the schema declares and introspection shows.

The request and the response are the GraphQL-over-HTTP specification's: `POST` with `{ "query", "variables",
"operationName" }`, an answer of `{ "data", "errors", "extensions" }`. Studio, the Query app, DataCube, Python and any
agent speak to the endpoint as to any GraphQL API, with any client.

### 2.2 What a person or an agent sees

- **A root field per class**, named as the class: `Person(where: PersonWhere, orderBy: [PersonOrderBy!], limit: Int,
  offset: Int): [PersonInterface!]!`, and a root per Pure function of the data space with its parameters typed.
- **Filters that read like Prisma's:** `where: { age: { gt: 30 }, firm: { legalName: { startsWith: "A" } },
  employees: { some: { salary: { gte: 100000 } } }, OR: [...] }`.
- **The same four arguments on every collection field**, root or to-many property.
- **Aggregates and groups beside every collection**, `employeesAggregate { count sum { salary } }`,
  `PersonGroups { key { address { country } } count avg { age } }`.
- **Inheritance in GraphQL's own idiom on the way out** (`... on Employee { salary }`, `__typename`) and as typed
  downcasts on the way in (`where: { asEmployee: { salary: { gt: 100000 } } }`).
- **The model's power as fields:** a derived property with parameters is a field with typed arguments
  (`salaryOn(date: $d)`); a milestoned class takes its business and processing dates as typed arguments; constraints
  come back as checked defects when asked; Pure's `doc.doc` tagged values are the SDL descriptions agents read.
- **Errors with a fix in them,** from a compiler that knows the model: the phase, the line and column, the message,
  and the nearest valid name.

### 2.3 What it is not

- Not legend-engine's GraphQL: no Query class to write by hand, no `_eq`, no `@totalCount` (a count is a sibling
  field), no plan-per-root-field quirks. The island keeps the name `#GQL{...}#` because the name is good, not for
  compatibility.
- Not an expression language. A computed value, a cross-field comparison, a bucket beyond `truncate` is a derived
  property in the Pure model, exposed as a typed field or argument. That is the division of labour the research
  argued for: the model author writes Pure once, agents and people compose fields (§3.2, G12).
- Not Relay: no `edges { node }`, no cursors in this line. `limit`, `offset` and the sibling `count` page a list.
- Not a write path. Mutations wait on the writes program (F1 of `docs/BI_AND_ETL_PLAN_2026_09_29.md`).

---

## 3. The spec, the prior art and the oracles

Taken on 2026-10-18 from three research passes whose reports, with every URL and every claim they could not verify
marked, are `docs/GRAPHQL_STUDY_2026_10_18_RESEARCH.md`; the sources are listed in §12. The adoption and accuracy
figures below are those reports' and are second-hand; the facts about lite are read from its code.

### 3.1 The GraphQL specification

- The **September 2025 edition** is the latest, the first since October 2021: OneOf input objects, schema
  coordinates, descriptions on executable documents, full Unicode. The working draft is the living text. The
  specification is licensed under the Open Web Foundation agreement (OWFa 1.0); the GraphQL Foundation (Linux
  Foundation) governs it, with a budget that shrank in 2026 and a conference "right-sized".
- **graphql-js** (MIT) is the reference implementation, v17 GA 2026-06-15; its `src/__tests__` corpus is the de facto
  conformance suite for parsing, validation and execution. The language-independent `graphql-cats` suite (Unlicense)
  has been dormant since 2018. There is no Foundation conformance suite.
- **The gaps that matter here**: the specification defines no filtering, sorting, pagination or aggregation. The
  Composite Schemas working group covers federation only. OpenCRUD is dormant. Every vendor invents its own input
  dialect, and the dialects disagree on the one thing an agent must guess, the operator spellings: Hasura `_eq`,
  Prisma `equals`, PostGraphile `equalTo`, Neo4j `eq` (and three incompatible generations between its versions 3
  and 7), AppSync `eq`.

### 3.2 What the research found about humans and agents

- **Adoption plateaued, not dead.** Postman's 2025 survey: REST 93%, GraphQL 33% (29% in 2023). Shopify went
  GraphQL-only for new public apps in 2025; GitHub's v4 continues; the State of GraphQL survey has not run since 2022.
- **LLM accuracy tracks training volume, not typedness.** The one benchmark that asks identical questions in four
  languages (SM3-Text-to-Query, NeurIPS 2024) finds zero-shot SQL 47%, Cypher 34%, MongoDB 22%, SPARQL 3%, and
  attributes MongoDB's gap to verbosity: a JSON query dialect costs tokens. A novel syntax starts near zero and
  recovers only with a reference page and examples (a vendor benchmark took its own language from 25% to 75% with a
  skill document, and to 92% with a validate-and-retry loop, because the typed language rejects wrong queries before
  they run while SQL answers them wrongly and silently). IBM's text-to-GraphQL dataset (EMNLP 2024 Industry) reports
  about 50% at best with one example, and names few public schemas in training data as the cause.
- **The ecosystem converged on three or four tools**: a schema tool, a validate tool, one read-only query tool taking
  the language as text, optionally explain. Supabase, Neo4j, BigQuery and Apollo's MCP servers all have this shape;
  Anthropic's and OpenAI's tool-design guidance says fewer, broader tools with strict schemas.
- **Hence the design:** GraphQL for the shape (the strongest tree prior); Prisma's vocabulary for the typed inputs
  (the strongest typed-filter prior: Prisma's client is downloaded about 11 million times a week); validation as the
  product; no expressions, since the typed inputs and the model carry them.

### 3.3 The oracles

| Oracle | Judges | License | Where it runs |
|---|---|---|---|
| **graphql-js** | validity: does a document validate against the generated SDL; introspection's shape | MIT | a Node test target (rules_js is in `MODULE.bazel` already for DataCube) in the heavy lane; the differential is a committed JSONL of documents and verdicts, judged in every lane |
| **Prisma** over Postgres 16 | semantics of `where`, `orderBy`, `limit`, `offset`, `aggregate`, `groupBy`, `having`: null handling, `mode: insensitive`, `some`/`every`/`none` on empty collections, ordering with nulls | Apache 2.0 | locally, `tools/prisma-oracle/snap.mjs` over the embedded Postgres the lanes use, a Prisma schema generated from the same Pure model; answers committed as `prisma-answers.jsonl`; STALE, UNSNAPPED, GONE |
| **Postgres 16** by SQL twin | everything Prisma cannot express: inheritance (`__typename`, `... on`, `asY`), derived properties, milestoning, `truncate`, checked defects | PostgreSQL | the Postgres lane, as the SQL island's cases |
| **legend-engine** | nothing. Its GraphQL extension is prior art read for its SDL binding and its root-field design, never loaded | | |

A case's status is judged by exactly one oracle, named in the ledger (`prisma`, `sql`, `validity`).

### 3.4 Prior art, read and not copied

- **legend-engine's `legend-engine-xts-graphQL`**: ANTLR grammar; `#GQL{}#` island; a synthetic Query class whose
  functions are root fields; `graphQLExecutableToPure` builds a `RootGraphFetchTree` per root field;
  `addDynamicFilter` maps a `where` argument with `_and, _or, _eq, _exists, _in, _contains, _lt, _lte, _gt, _gte,
  _eqIgnoreCase`; introspection generates `<Class>_bool_exp`; `@totalCount` as a second plan; limit only after sort;
  no `order_by`, no nested arguments, no aggregates, no mutations. Its SDL binding (`binding/toPure/sdl`) is the
  reference for the easy half of the generator: multiplicities to nullability, enums, scalars.
- **Hasura v2/v3**, **PostGraphile v5**, **Prisma**, **Neo4j GraphQL v7**, **AppSync**: the dialects compared in §3.1.
  Prisma is the model for inputs; Hasura's `_aggregate { aggregate { } }` double wrapper is what `XAggregate` avoids;
  PostGraphile v5's `@interface mode:relational` is the one generator that models inheritance, and it makes the user
  name every concrete type, which is the suffix-on-the-concrete-type design this line rejected (G8).
- **OData 4.01** (OASIS, ISO): the only ratified standard with inheritance, navigation, `$expand` trees and `$apply`
  aggregation; a reader of it onto the same translator is the obvious second input spelling if enterprise consumers
  ask (§10, Later). Its oracle situation is poor (Olingo retired to the Apache Attic in December 2025), which is one
  reason it is second.
- **EdgeQL** (Apache 2.0): the best semantic match to a class model (object types, inheritance, links, nested shapes
  with filter, order and limit at every level, computed fields, `group`); its vendor shut down in December 2025 and
  it has almost no training-data presence. Its nested-clause design is what §5 and §6 give GraphQL.

---

## 4. Decisions (the user's rulings, 2026-10-18)

`G` for GraphQL, distinct from the Postgres study's `D`, the IQ study's `I` and the Cypher study's `C`.

- **G1. The query language is valid GraphQL** (September 2025 edition). Every document parses with graphql-js and
  validates against the generated SDL. lite's validator may be stricter than graphql-js (G10), never looser: the
  differential (§9.3) requires lite's acceptance to be a subset of graphql-js's.
- **G2. The filter vocabulary is Prisma's, exactly:** `equals`, `not` (the nested form only), `in`, `notIn`, `lt`,
  `lte`, `gt`, `gte`, `contains`, `startsWith`, `endsWith`, `mode: insensitive` (an enum `QueryMode { default,
  insensitive }`); for to-one relations `is`, `isNot`; for to-many `some`, `every`, `none`; `AND`, `OR`, `NOT`. An
  explicit `equals: null` is `IS NULL` and `not: null` is `IS NOT NULL`, as Prisma reads them. No bare-value shorthand
  (a GraphQL input field has one type). No `like`. The user's reason, over the `eq` of the GraphQL-world convention:
  Prisma is the most-seen typed filter in training data and one consistent set.
- **G3. The four arguments** on every collection field, root or to-many: `where`, `orderBy` (a list of single-key
  objects, `[{ lastName: ASC }, { age: DESC }]`, because GraphQL input objects are unordered maps), `limit: Int`,
  `offset: Int`.
- **G4. Roots are the classes, named as the classes.** `Person`, not `persons` or `people`: no pluralization to get
  wrong, and the name is the one the model and `describe` show. Pure functions of the data space are extra roots,
  camelCase as Pure spells them, with typed parameters.
- **G5. Scalars.** Pure `Integer` is a custom `BigInt` (64-bit; JSON number); GraphQL's `Int` is 32-bit by
  specification and appears only for `limit` and `offset`. `Float` is `Float`; `Decimal` a custom `Decimal`; `String`,
  `Boolean` themselves; `StrictDate` a custom `Date`; `DateTime` and Pure's abstract `Date` a custom `DateTime`;
  `StrictTime` a custom `Time`; Pure enumerations are GraphQL enums. The custom scalars accept the literal an agent
  writes (`"2024-01-01"`, `30`); the schema types it. Typed by the schema is not defaulting (AGENTS.md invariant 4).
- **G6. Aggregates are a sibling field per collection:** `PersonAggregate` beside the `Person` root,
  `employeesAggregate` beside `employees`, taking the collection's `where`, returning `{ count: BigInt!, sum: XSum,
  avg: XAvg, min: XMin, max: XMax }` whose sub-objects have one field per numeric (sum, avg) or comparable (min,
  max) property. The sub-objects exist because one scalar field cannot carry a Float sum and a Date minimum.
- **G7. Groups, designed now and built in PR5.** A sibling `PersonGroups` / `employeesGroups`. **The selection under
  `key` is the grouping**; there is no `by` argument to keep in sync. `XGroupKey` holds the groupable fields only
  (scalars, enums, to-one paths through their own key types, derived properties without parameters; never a
  to-many); a date key takes `truncate: DateUnit` (`YEAR, QUARTER, MONTH, WEEK, DAY, HOUR`), the place a typed
  bucket belongs, where a numeric `bucket:` can join later. Arguments: `where`, `having: XGroupHaving` (filters on
  count, sum, avg, min, max, with `AND`/`OR`/`NOT`), `orderBy: [XGroupOrderBy!]` (count, key, sum, avg, min, max),
  `limit`, `offset`. `XAggregate` is the degenerate group with an empty key and stays as the convenient form.
- **G8. Inheritance, output side.** A class with subclasses gets `interface XInterface` carrying its fields; its
  concrete type keeps the class name `X` and implements it. A leaf class is a plain object type and nothing else. An
  abstract class (Pure's `typemodifiers.abstract` stereotype) gets the interface only, still named `XInterface` so
  the rule is predictable. lite reads the stereotype by name, as it reads `<<equality.Key>>`
  (`PlatformTypes.EQUALITY_PROFILE`) and the `doc.doc` tag (`protocol/Documentation`); nothing reads it today, so
  PR3 adds the read. Multi-level hierarchies use interfaces implementing interfaces, with the transitive
  `implements` list GraphQL requires written out; Pure's multiple inheritance is `implements A & B`. Every position
  typed `X[*]` or `X[0..1]` in Pure is `XInterface` in the schema where it exists, else `X`. `__typename` is always
  the exact class, decided by the mapping's set implementation (a constant per union arm, as `ClassConcatenates`
  already builds). **The suffix is `Interface`, not the `I` prefix:** `PersonInterface` sorts beside `Person`,
  `PersonAggregate` and `PersonWhere`, reads as one rule, and cannot be mistaken for a class name.
- **G9. Inheritance, input side.** Inputs never mention interfaces. `XWhere` and `XOrderBy` carry `asY: YWhere` /
  `asY: YOrderBy` for every transitive subclass `Y`; `asY: { isNot: null }` is membership, as `firm: { isNot: null }`
  is "has a firm". No `subtype` enum: `__typename` covers the output side.
- **G10. The fragment trap is refused.** An inline fragment matches an object type exactly, so `... on Employee {
  salary }` inside a `Person` selection silently skips a `Manager`, and graphql-js accepts it. lite refuses an inline
  fragment on a concrete type that has subclasses when the selection is typed at an ancestor, naming the fix: `... on
  EmployeeInterface`, or `... on Employee` beside `... on Manager` if the exact split is wanted.
- **G11. Reserved generated names.** Per class `XInterface`, `XWhere`, `XOrderBy`, `XAggregate`, `XSum`, `XAvg`,
  `XMin`, `XMax`, `XGroups`, `XGroup`, `XGroupKey`, `XGroupHaving`, `XGroupOrderBy`; per collection property
  `pAggregate`, `pGroups`; per subclass `asY`; the scalar and enum names of G5 and G7. The generator refuses a model
  whose own element collides, naming it. Nothing is renamed silently.
- **G12. No expressions in a query.** Computed values, cross-field comparisons and buckets beyond `truncate` are
  derived properties in the Pure model, exposed as typed fields and arguments.
- **G13. Oracles:** graphql-js for validity, Prisma over Postgres for the dialect's semantics, SQL twins on the
  Postgres lane for what Prisma lacks (§3.3).
- **G14. The agent surface is four tools:** `describe`, `validate`, `query`, `explain` (§8.4).
- **G15. The companion model, not a tree extension** (the user, 2026-10-18, on "classes cannot change shape at
  query time"). Pure classes are nominal and closed; a graph-fetch tree prunes a class and never adds to it; Pure's
  own device for a query-time shape is the Relation API's structural row type. So the shapes the schema needs are
  **generated Pure classes** in a companion model per data space (`PersonAggregate`, `PersonSum`, `PersonGroup`,
  `PersonGroupKey`, ...), and the members the schema needs are **generated qualified properties** beside the stored
  ones (`employeesAggregate(where)`, `asEmployee()`, `__typename()`), exactly as milestoning generates
  `employees(businessDate)` beside `employees` and lite already reads it. Nothing changes shape at query time; the
  query supplies arguments. A clause on an existing collection (`where`, `orderBy`, `limit`, `offset`) is an
  anonymous derived body the translator composes from standard functions, `filter`, `sortBy`, `sortByReversed`,
  `drop`, `limit`, placed on the tree node (§6.2): the body is Pure as it stands, only its position is lite's.
- **G16. Sorting is Pure's sort family, chained.** `sortBy` ascending, `sortByReversed` descending, a key lambda
  that is any property chain, several keys by chaining under Pure's stable-sort semantics
  (`->sortByReversed(e | $e.age)->sortBy(e | $e.lastName)` is `ORDER BY lastName, age DESC`). lite's resolver reads
  all of these today (`ClassSorts`); the Relation `sort` already carries a sorted source's keys as tie-breakers; the
  collection `sortBy` must do the same (one function in `lowering/Sorts`). No native is added.
- **G17. Resource limits are part of the protocol.** Per data space: a ceiling on selection depth and on list fields
  per level, enforced by `validate` before anything runs; a row cap and a timeout on the endpoint, not only on the
  `query` tool. An agent writes the six-level fan-out that melts a warehouse; the validator refuses it by name.
- **G18. Execution is all or nothing.** GraphQL executors resolve field by field and return partial `data` beside
  `errors`; lite compiles the whole document into plans, so a compile error returns no data and a database error
  fails the request. A deliberate difference, recorded in the register.
- **G19. Introspection teaches the idiom; meta-fields are never keys.** The generated description on a concrete type
  with subclasses says "exact `Employee` instances only; spread on `EmployeeInterface` to include `Manager`", so the
  G10 error is met rarely. `__typename` (which Apollo Client adds to every selection) is never a group key under
  `key { }`, and every generated type answers it.
- **G20. The companion is synthetic and reserved.** Generated elements live under a reserved package per data space,
  are hidden from model listings and Studio's class views as milestoning's generated properties are, and stay
  callable from plain Pure, which is a feature. Collisions with a model's own names are refused by name (G11).
- **G21. The Prisma lane is a dialect oracle.** It judges operators, nulls, ordering and the quantifiers over a
  table-per-class fixture. It judges nothing about mappings, unions, milestoning or inheritance; those are SQL-twin
  cases on the Postgres lane.
- **G22. Authorization is out of scope.** The data space is the boundary; row-level access belongs to the warehouse;
  nothing in this line changes either.

Open, to be decided by the landing that meets them: the compact form `describe` returns beside the SDL (§8.4); whether
the island's typed result is `String[1]` (the serialized JSON, as `serialize` answers) or a Variant (§8.3).

---

## 5. The generated schema, rule by rule

The generator reads a data space's `ModelContext` (`compiler/element`) and emits a schema as data: an SDL text for
`describe` and goldens, and the introspection answer the endpoint serves. It is deterministic: Pure declaration order
for fields, class-name order for types; a golden per test model pins it (§9).

### 5.1 Types and fields

| Pure | GraphQL |
|---|---|
| `Class demo::Person` (leaf) | `type Person { ... }` |
| `Class demo::Person` with subclasses | `interface PersonInterface { ... }` and `type Person implements PersonInterface { ... }` (G8) |
| `Class <<typemodifiers.abstract>> demo::Shape` | `interface ShapeInterface { ... }` only |
| `Class demo::Manager extends demo::Employee` (Employee extends Person) | `type Manager implements EmployeeInterface & PersonInterface { ... }`; `interface EmployeeInterface implements PersonInterface` |
| property `name: String[1]` | `name: String!` |
| property `age: Integer[0..1]` | `age: BigInt` |
| property `firm: Firm[0..1]` | `firm: Firm` (or `FirmInterface`) |
| property `employees: Person[*]` or `[1..*]` | `employees(where: PersonWhere, orderBy: [PersonOrderBy!], limit: Int, offset: Int): [PersonInterface!]!`, plus `employeesAggregate(where: PersonWhere): PersonAggregate!` and `employeesGroups(...)` |
| derived property `fullName(): String[1]` | `fullName: String!`; also a `fullName: StringFilter` in `PersonWhere` and `fullName: SortDirection` in `PersonOrderBy` (no parameters, so it lowers like a property) |
| derived property `salaryOn(d: Date[1]): Float[1]` | `salaryOn(d: Date!): Float!`; output only (a filter cannot supply its arguments) |
| milestoned class | `businessDate: Date` and/or `processingDate: DateTime` as arguments on its root and on every navigation into it |
| `Enum demo::Country { UK, US }` | `enum Country { UK US }` |
| `doc.doc` tagged value | the SDL description, `"""..."""`, on the type or field; the generator adds its own on a concrete type with subclasses: "exact instances only; spread on `XInterface` to include them" (G19) |
| `doc.deprecated` stereotype | `@deprecated` |
| the package | not in the name: `demo::Person` is `Person`; two classes of one simple name in one data space are refused by name (G11), never disambiguated by package |

Names: Pure's spelling, unchanged. GraphQL names are `[_A-Za-z][_0-9A-Za-z]*`; a Pure name outside that set is refused
by name.

### 5.2 Roots

```graphql
type Query {
  Person(where: PersonWhere, orderBy: [PersonOrderBy!], limit: Int, offset: Int): [PersonInterface!]!
  PersonAggregate(where: PersonWhere): PersonAggregate!
  PersonGroups(where: PersonWhere, having: PersonGroupHaving, orderBy: [PersonGroupOrderBy!], limit: Int, offset: Int): [PersonGroup!]!
  ...                                   # one triple per class of the data space
  topFirms(n: BigInt!): [Firm!]!        # a Pure function of the data space
}
```

A Pure function root takes its parameters as typed arguments and returns its declared type; a function returning a
class collection gets no `where`/`orderBy` of its own in this line (its body is the query).

### 5.3 Filters

```graphql
input PersonWhere {
  fullName: StringFilter   age: BigIntFilter   born: DateFilter   country: CountryFilter
  firm: FirmWhere                          # to-one: nested conditions; `is` / `isNot` on the relation object
  employees: PersonListFilter              # to-many: { some: PersonWhere, every: PersonWhere, none: PersonWhere }
  asEmployee: EmployeeWhere  asManager: ManagerWhere      # G9
  AND: [PersonWhere!]  OR: [PersonWhere!]  NOT: PersonWhere
}
input StringFilter { equals: String  not: StringFilter  in: [String!]  notIn: [String!]  lt: String  lte: String  gt: String  gte: String  contains: String  startsWith: String  endsWith: String  mode: QueryMode }
input BigIntFilter { equals: BigInt  not: BigIntFilter  in: [BigInt!]  notIn: [BigInt!]  lt: BigInt  lte: BigInt  gt: BigInt  gte: BigInt }
input FirmWhere { ...  is: FirmWhere  isNot: FirmWhere }   # Prisma's relation filters; `isNot: null` and `is: null` take an explicit null
enum QueryMode { default insensitive }
```

Semantics are SQL's and Prisma's: three-valued logic, `in: []` is false, `mode: insensitive` applies to the string
comparisons of that object, `some` over an empty collection is false and `every` is true, `not` nests. Each operator
has one lowering; the Prisma oracle pins each on Postgres.

### 5.4 Sorting and paging

```graphql
input PersonOrderBy { fullName: SortDirection  age: SortDirection  born: SortDirection  firm: FirmOrderBy  asEmployee: EmployeeOrderBy  employeesAggregate: PersonAggregateOrderBy }
enum SortDirection { ASC DESC }
input PersonAggregateOrderBy { count: SortDirection  sum: PersonSumOrderBy  avg: PersonAvgOrderBy  min: PersonMinOrderBy  max: PersonMaxOrderBy }
```

A list of single-key objects gives key order; an object with more than one key is refused by name (the order would be
unspecified). Nulls sort as the database sorts them, recorded per dialect in the register, not corrected. `limit`
without `orderBy` is a warning from `validate`, never an error: a person may want any ten.

### 5.5 Aggregates

```graphql
type PersonAggregate { count: BigInt!  sum: PersonSum  avg: PersonAvg  min: PersonMin  max: PersonMax }
type PersonSum { age: BigInt  salaryOnRecord: Float }     # numeric properties and parameterless numeric derived properties
type PersonAvg { age: Float  ... }
type PersonMin { age: BigInt  born: Date  fullName: String  ... }   # every comparable property
type PersonMax { ... }
```

`sum` and `avg` over an empty or all-null collection are null; `count` is the row count, never null. A subclass's own
properties aggregate through the subclass's root or collection (`EmployeeAggregate { sum { salary } }`), since
`PersonSum` holds Person's properties only.

### 5.6 Groups (G7; built in PR5)

```graphql
type PersonGroup { key: PersonGroupKey!  count: BigInt!  sum: PersonSum  avg: PersonAvg  min: PersonMin  max: PersonMax }
type PersonGroupKey { fullName: String  age: BigInt  born(truncate: DateUnit): Date  country: Country  firm: FirmGroupKey  address: AddressGroupKey }
input PersonGroupHaving { count: BigIntFilter  sum: PersonSumFilter  avg: PersonAvgFilter  min: PersonMinFilter  max: PersonMaxFilter  AND: [PersonGroupHaving!]  OR: [PersonGroupHaving!]  NOT: PersonGroupHaving }
input PersonGroupOrderBy { count: SortDirection  key: PersonOrderBy  sum: PersonSumOrderBy  avg: PersonAvgOrderBy  min: PersonMinOrderBy  max: PersonMaxOrderBy }
enum DateUnit { YEAR QUARTER MONTH WEEK DAY HOUR }
```

The fields selected under `key` are the group keys; a `key` selection that is empty is a validation error naming
`PersonAggregate` as the form for one group; an `orderBy: { key: ... }` naming a field not selected under `key` is an
error. `truncate` is a field argument so that a key can be bucketed without leaving the type system. Meta-fields
under `key`, which some clients add to every selection, are never keys (G19).

### 5.7 The reserved names and the refusals

The generator fails the whole data space, naming the element, when: a property is named like a generated sibling
(`employeesAggregate`, `asEmployee`, `__typename`); two classes share a simple name; a name is not a GraphQL name; a
property's type is one lite cannot map (a `Map`, a function type, a `Variant` until a scalar for it is decided). A
refused data space has no endpoint and `describe` says why. No renaming, no skipping, no defaulting.

### 5.8 The companion model (G15, G20)

The same generator that writes the SDL writes, per data space, a **companion model**: Pure, generated at
model-compile time, under a reserved package, synthetic (hidden from listings, callable from Pure). It holds the
shapes the schema needs as classes and the members it needs as qualified properties, so that every GraphQL document
is an ordinary graph-fetch tree over nominal classes. For the model of §7:

```
Class graphql::demo::PersonAggregate
{
  items: demo::Person[*];
  count() { $this.items->size() }: Integer[1];
  sum() { ^graphql::demo::PersonSum(age = $this.items.age->sum()) }: graphql::demo::PersonSum[1];
  avg() { ... }: graphql::demo::PersonAvg[1];
  min() { ... }: graphql::demo::PersonMin[1];
  max() { ... }: graphql::demo::PersonMax[1];
}
Class graphql::demo::PersonSum { age: Integer[0..1]; }
Class graphql::demo::PersonGroup { key: graphql::demo::PersonGroupKey[1]; ...the aggregate members... }

// generated beside the stored properties, as milestoning generates employees(businessDate):
Class demo::Firm
{
  employeesAggregate() { ^graphql::demo::PersonAggregate(items = $this.employees) }: graphql::demo::PersonAggregate[1];
  employeesAggregate(where: Function<{demo::Person[1]->Boolean[1]}>[1])
    { ^graphql::demo::PersonAggregate(items = $this.employees->filter($where)) }: graphql::demo::PersonAggregate[1];
}
Class demo::Person
{
  asEmployee() { if($this->instanceOf(demo::Employee), | $this->cast(@demo::Employee), | []) }: demo::Employee[0..1];
  __typename() { ... the set implementation's class name, a spelled constant per union arm ... }: String[1];
}
```

The precedent is milestoning: Pure generates qualified properties the source never spells, the compiler reads them as
any other, and the tree takes their arguments; both arities are generated, as milestoning generates `employees` and
`employees(date)`. One register row the companion needs from the start: Pure's `sum` is typed `[1]` and answers 0 over
an empty collection, SQL's and Prisma's answer null; the companion's `sum()` and `avg()` are typed `[0..1]` and mean
SQL's (G6), a declared difference from the Pure function the body spells. The constructor in a body (`^PersonAggregate(items = ...)`) is never
built: a constructor followed by a read of the field it set reduces to the field's expression before lowering, a
structural rewrite that computes nothing (§6.2). A clause on an existing collection is not in the companion, because a
fixed body cannot take a variable-length `orderBy`; it is the anonymous body of §6.2.

---

## 6. The translator

```
document ─▶ GqlParser (exists) ─▶ Gql.Document ─▶ GqlValidator: graphql-js's rules + G10 + G11 over the generated schema
                                                   │
                                                   ▼
                                     GqlToPure: one Pure ValueSpecification per root field
                                     (Class.all() ─▶ graphFetch(tree with clauses) ─▶ serialize(tree))
                                                   │
                                                   ▼
                                     Typer types it once ─▶ G½ … K (the graph emitter builds the JSON in SQL)
```

### 6.1 Construct by construct

| GraphQL | Pure |
|---|---|
| root `Person(where: W, orderBy: K, limit: n, offset: m) { sel }` | `Person.all()->filter(p \| W)->sortByReversed(...)->sortBy(...)->drop(m)->limit(n)->graphFetch(#{Person{sel}}#)->serialize(#{Person{sel}}#)`: Pure's own collection functions, chained for several keys (G16) |
| root `topFirms(n: 3) { sel }` | `topFirms(3)->graphFetch(...)->serialize(...)` |
| a scalar field, an enum field | a tree leaf |
| `alias: field` | the tree's alias (`'alias': field`, which lite's tree parses) |
| a to-one field `firm { sel }` | a tree node |
| a to-many field with arguments | a tree node whose value is a composed body over `$this` (§6.2): `$this.employees->filter(W)->sortByReversed(...)->sortBy(...)->drop(m)->limit(n)` |
| `salaryOn(d: $d)` | a tree node with property parameters, which lite's tree parses (`k(%2024-01-01)`) |
| milestoning arguments | the tree's temporal parameters, which lite's tree parses |
| `... on Employee { sel }` on a `PersonInterface` selection | the tree's `->subType(@Employee) { sel }` entry, which lite's tree parses |
| `__typename` | the companion's `__typename()` (§5.8): the class name of the set implementation that produced the row, a spelled constant per union arm (`ClassConcatenates`; C6.2 of the tenet charter: a token compared, never computed) |
| `where: { age: { gt: 30 } }` | `p \| $p.age > 30` |
| `where: { firm: { legalName: { startsWith: "A" } } }` | `p \| $p.firm.legalName->startsWith('A')` with the to-one null carried as SQL carries it |
| `where: { employees: { some: W } }` / `every` / `none` | `p \| $p.employees->exists(e \| W)` / `->forAll` / `!->exists`; the resolver's correlated subselects (`CorrelatedSubselects`) lower them |
| `where: { asEmployee: W }` | the companion's `asEmployee()` (§5.8) as a to-one hop: `instanceOf` (a registered native the resolver answers from the mapping's discriminator) and `cast(@Employee)` |
| `where: { fullName: { equals: "x" } }` (a derived property) | the derived body inlined, as any derived property is |
| `equals: null` / `not: null` | `->isEmpty()` / `->isNotEmpty()` |
| `mode: insensitive` | `toLower` on both sides of that object's string comparisons |
| `orderBy: [{ firm: { legalName: ASC } }, { age: DESC }]` | `->sortByReversed(e \| $e.age)->sortBy(e \| $e.firm.legalName)`: the key is any property chain, the last-applied sort is the primary key (G16) |
| `limit`, `offset` | `->drop(m)->limit(n)`, at the root and inside a composed body alike |
| `employeesAggregate(where: W) { count sum { salary } }` | the companion's `employeesAggregate(W)` over `EmployeeAggregate`, whose `count()` and `sum()` are derived leaves; after the constructor fusion (§6.2) the emitter's correlated subselect with `count(*)`, `sum(salary)` |
| `PersonAggregate(where: W) { ... }` | the Relation API: `Person.all()->filter(W)->groupBy(~[], ~[count: ..., ...])`, one row serialized as the object |
| `PersonGroups(...) { key { ... } ... }` | the Relation API: `project` the keys and the aggregates, `groupBy`, `filter` for `having`, `sort`, `drop`, `limit`; the rows serialized to `{ key: {...}, count, sum: {...} }` by the key paths (PR5) |
| `employeesGroups` under a parent | the same, correlated to the parent row and folded into its JSON array by the emitter (PR5) |
| variables | Pure function parameters, typed by the declaration (`$w: PersonWhere!` is a typed input value, substituted into the filter at plan time) |
| `@include(if: $x)` / `@skip` | applied before typing: the shape depends on the variables, so a plan is per distinct shape |
| named fragments | inlined before typing |
| several root fields in one document | one Pure expression per root field, one request, the endpoint carrying each answer under its alias (`extensions` names the plan per root) |

### 6.2 What is a desugar, what the companion carries, and the three lowering increments

The user's challenge (2026-10-18): is a nested `where` not just property navigation, `Firm.employees.name == 'John'`?
Two different things hide under "nested where", and the code answers each.

**(a) Navigation inside a condition on the parent.** `Firm(where: { employees: { some: { fullName: { equals:
"John" } } } })` is `Firm.all()->filter(f | $f.employees->exists(e | $e.fullName == 'John'))`: plain Pure, deep
navigation in a filter, lowered today through the resolver's correlated subselects (`CorrelatedSubselects`). Every
firm with such an employee comes back, with all of its employees under it. Nothing to add.

**(b) A clause on the collection in the result.** `Firm { legalName employees(where: { fullName: { equals: "John"
} }) { fullName } }` keeps every firm and filters what is serialized under each. Pure's spelling for an expression at
a tree node is a **derived property**, and lite handles derived properties in trees end to end today:
`GraphFetchChecker` types a node's arguments and they ride the tree; `GraphEmission.derivedChild` compiles a derived
to-many body, substitutes the arguments, recognises its navigation head and its filter (and a chained two-hop body),
and builds the correlated child pipeline; `derivedLeaf` inlines a scalar derived body such as `$this.employees->size()`
as a correlated scalar subquery, which `GraphAggDecorrelate` turns into a join. So every nested clause and every
aggregate of this line **desugars to a derived property**, and the companion model (§5.8) declares the ones with a
fixed body:

| GraphQL | Pure |
|---|---|
| `employees(where: W, orderBy: [{ lastName: ASC }, { age: DESC }], limit: n, offset: m) { ... }` | an anonymous body on the node: `$this.employees->filter(W)->sortByReversed(e \| $e.age)->sortBy(e \| $e.lastName)->drop(m)->limit(n)` (G16: several keys by chaining, stable) |
| `employeesAggregate(where: W) { count sum { age } }` | the companion's `employeesAggregate(W)` over `PersonAggregate`, whose `count()` and `sum()` are derived leaves |
| `... on Employee { salary }` | `->subType(@Employee) { salary }`, which the tree has |
| `where: { asEmployee: { salary: { gt: 1 } } }` | the companion's `asEmployee()` as a to-one hop in the filter |
| `__typename` | the companion's `__typename()`, a spelled constant per union arm |

Pure as an object language expresses all of it. The semantics are Pure's, typed as any derived body is typed, with
`$this` bound to the node's object; no new native and no new spelling. What is lite's is (i) the companion itself, a
generated model the way milestoning's properties are generated, and (ii) the **position** of a composed clause body:
a tree node is a property name plus arguments (`GraphFetchLiteral.Node`: property, parameters, alias, subtype view,
children), and a query-time `where` with a variable-length `orderBy` cannot be a fixed-body property, so the
translator composes the body and places it on the node. The composer prints that node as an aliased expression over
`$this`, which is ordinary Pure expression syntax in a tree position; the parser reads it back; the `LEGEND_ENGINE`
surface keeps refusing it; the own-corpus census declares it (LITE-DESIGN-graphql-tree, `docs/OWN_CORPUS_DECISIONS.md`).

The three lowering increments, each small, each measured by PR2's first probes:

| Increment | Where | What exists |
|---|---|---|
| a lambda as a tree argument: `employeesAggregate(e \| $e.age >= 50)` | `GraphFetchChecker` types node arguments by plain synthesis; a lambda needs the qualified property's declared function type as its expectation so `e` is typed | after substitution the inliner sees a literal filter, the shape `derivedChild` already recognises |
| sort, limit and offset in a derived body | `derivedChild` recognises the navigation head and its filter today, not a `sortBy`, `take`, `limit` or `drop`; and `lowering/Sorts.sortBy` must carry a sorted source's keys as tie-breakers, as `Sorts.sort` does for the Relation sort ("Pure's sort is stable") | `SqlSelect` has `orderBy`, `limit`, `offset`; `GraphAggDecorrelate` already steps around a subselect that carries them; `ClassSorts` reads every spelling of G16 |
| constructor fused with navigation | `^PersonAggregate(items = X).count` reduces to `X->size()` before lowering: a constructor followed by a read of a field it set is replaced by that field's expression, recursively through `sum()` and `PersonSum` | a structural rewrite in the resolver; the result is the derived-leaf route that exists; Java builds no instance |

Not an extension, because Pure spells it and lite lowers it today: (a) above; `some`, `every`, `none`; subtype
selection; derived properties with arguments; milestoning arguments; root filter, limit and offset; root sorts with
direction and several keys (G16); the root aggregate and the root groups (the Relation API); `truncate` (the
natives `firstDayOfYear`, `firstDayOfQuarter`, `firstDayOfMonth`, `firstDayOfWeek`, `firstHourOfDay`,
`firstMinuteOfHour`, all registered); `cast` and `instanceOf` (both registered natives; the resolver answers
`instanceOf` from the mapping's discriminator; PR3 confirms the null-returning `asY` form on mapped instances); the
insensitive match; the null tests.

The dumped Pure of a case, which a review reads beside the query that produced it:

```
Firm.all()->filter(f | $f.legalName->startsWith('A'))->sortBy(f | $f.legalName)->limit(10)->graphFetch(#{
  Firm {
    legalName,
    'top3': $this.employees->filter(e | $e.age > 30)->sortByReversed(e | $e.salary)->limit(3) { fullName, salary },
    'headcount': employeesAggregate { count }
  }
}#)->serialize(...)
```

Java still computes nothing: the database runs the filter, the sort, the limit and the aggregate inside the subselect
the emitter already builds per node.

### 6.3 Validation beyond graphql-js

lite's validator runs the specification's validation rules (the ones graphql-js runs: fields exist, arguments typed,
fragments spread on possible types, variables used and defined, leaf fields have no selection, composite fields have
one) and then its own: G10 (the exact-type fragment inside an ancestor selection), G11 (collisions are a schema-time
refusal, so a query never meets them), the group rules of §5.6, and the warnings (`limit` without `orderBy`; a
deprecated field). Every error carries `LegendCompileException.Phase` (`PARSE`, `TYPE`, ...), line and column in the
document, the message, and where a name is wrong, the nearest valid name of that type. `validate` returns all of them,
not the first.

### 6.4 Constraints

- `protocol/**` depends only on protocol, values, JSON and the JDK (invariant 7b); the generator lives with the
  compiler (`compiler/graphql/`), since it reads `ModelContext`; the endpoint in `server`.
- TeaVM and GraalVM: the generator and the translator are JDK-only; each landing builds `//sdlc-server:page` and
  `//wasm:planner`.
- `//parser-equivalence:diagnostics` on every landing that touches `SpecParser`, `PureComposer` or a protocol record.
- Java never computes a value: `__typename` is a spelled constant per arm; counts, sums and `truncate` are the
  database's; the endpoint concatenates answers, it does not build them.

---

## 7. The worked example: every feature in one model

The model (`graphql-parity/models/demo.pure` in the harness):

```
Class demo::Person
{
  firstName: String[1];
  lastName: String[1];
  age: Integer[1];
  born: StrictDate[0..1];
  fullName() { $this.firstName + ' ' + $this.lastName }: String[1];
}
Class demo::Employee extends demo::Person
{
  salary: Float[1];
  manager: demo::Employee[0..1];
  salaryOn(d: StrictDate[1]) { ... }: Float[1];
}
Class demo::Manager extends demo::Employee { budget: Float[1]; }
Class demo::Firm { legalName: String[1]; founded: StrictDate[1]; }
Class demo::Address { city: String[1]; country: demo::Country[1]; }
Enum demo::Country { UK, US, JP }
Association demo::Employment { firm: demo::Firm[0..1]; employees: demo::Person[*]; }
Association demo::Residence { address: demo::Address[0..1]; residents: demo::Person[*]; }
```

The generated schema, abridged:

```graphql
scalar BigInt  scalar Date  scalar DateTime  scalar Decimal
enum Country { UK US JP }   enum SortDirection { ASC DESC }   enum QueryMode { default insensitive }   enum DateUnit { YEAR QUARTER MONTH WEEK DAY HOUR }

interface PersonInterface {
  firstName: String!  lastName: String!  age: BigInt!  born: Date  fullName: String!
  firm: Firm  address: Address
}
type Person implements PersonInterface { ...the same fields... }
interface EmployeeInterface implements PersonInterface { ...Person's fields...  salary: Float!  manager: EmployeeInterface  salaryOn(d: Date!): Float! }
type Employee implements EmployeeInterface & PersonInterface { ... }
type Manager implements EmployeeInterface & PersonInterface { ...  budget: Float! }
type Firm {
  legalName: String!  founded: Date!
  employees(where: PersonWhere, orderBy: [PersonOrderBy!], limit: Int, offset: Int): [PersonInterface!]!
  employeesAggregate(where: PersonWhere): PersonAggregate!
  employeesGroups(where: PersonWhere, having: PersonGroupHaving, orderBy: [PersonGroupOrderBy!], limit: Int, offset: Int): [PersonGroup!]!
}
type Address { city: String!  country: Country!  residents(...): [PersonInterface!]!  residentsAggregate(...): PersonAggregate!  residentsGroups(...): [PersonGroup!]! }

input PersonWhere { firstName: StringFilter  lastName: StringFilter  age: BigIntFilter  born: DateFilter  fullName: StringFilter
                    firm: FirmWhere  address: AddressWhere  asEmployee: EmployeeWhere  asManager: ManagerWhere
                    AND: [PersonWhere!]  OR: [PersonWhere!]  NOT: PersonWhere }
input PersonOrderBy { firstName: SortDirection  lastName: SortDirection  age: SortDirection  born: SortDirection  fullName: SortDirection
                      firm: FirmOrderBy  address: AddressOrderBy  asEmployee: EmployeeOrderBy  asManager: ManagerOrderBy }
type PersonAggregate { count: BigInt!  sum: PersonSum  avg: PersonAvg  min: PersonMin  max: PersonMax }
type PersonSum { age: BigInt }   type PersonAvg { age: Float }
type PersonMin { firstName: String  lastName: String  age: BigInt  born: Date  fullName: String }   type PersonMax { ...same... }
type PersonGroup { key: PersonGroupKey!  count: BigInt!  sum: PersonSum  avg: PersonAvg  min: PersonMin  max: PersonMax }
type PersonGroupKey { firstName: String  lastName: String  age: BigInt  born(truncate: DateUnit): Date  fullName: String  firm: FirmGroupKey  address: AddressGroupKey }

type Query {
  Person(where: PersonWhere, orderBy: [PersonOrderBy!], limit: Int, offset: Int): [PersonInterface!]!
  PersonAggregate(where: PersonWhere): PersonAggregate!
  PersonGroups(where: PersonWhere, having: PersonGroupHaving, orderBy: [PersonGroupOrderBy!], limit: Int, offset: Int): [PersonGroup!]!
  Employee(...): [EmployeeInterface!]!   EmployeeAggregate(...): EmployeeAggregate!   EmployeeGroups(...): [EmployeeGroup!]!
  Manager(...): [Manager!]!   ...   Firm(...): [Firm!]!   ...   Address(...): [Address!]!   ...
}
```

**1. Filter, sort, page, navigate.**

```graphql
{
  Person(
    where: { age: { gt: 30 }, address: { country: { equals: UK } } }
    orderBy: [{ lastName: ASC }, { age: DESC }]
    limit: 10
  ) { fullName age address { city } firm { legalName } }
}
```

```json
{ "data": { "Person": [ { "fullName": "Ann Adams", "age": 41, "address": { "city": "Leeds" }, "firm": { "legalName": "Acme" } }, ... ] } }
```

**2. Nested clauses and aggregates.**

```graphql
{
  Firm(orderBy: [{ legalName: ASC }]) {
    legalName
    headcount: employeesAggregate { count }
    seniors: employeesAggregate(where: { age: { gte: 50 } }) { count }
    oldest: employeesAggregate { max { age born } }
    top3: employees(orderBy: [{ asEmployee: { salary: DESC } }], limit: 3) { fullName ... on EmployeeInterface { salary } }
  }
}
```

```json
{ "data": { "Firm": [ { "legalName": "Acme", "headcount": { "count": 12 }, "seniors": { "count": 4 },
                        "oldest": { "max": { "age": 63, "born": "1961-02-11" } },
                        "top3": [ { "fullName": "A. Lee", "salary": 410000.0 }, ... ] } ] } }
```

**3. Inheritance, a typed variable reused, a total beside a page.**

```graphql
query ($w: PersonWhere!) {
  people: Person(where: $w, limit: 20) {
    __typename
    fullName
    ... on EmployeeInterface { salary manager { fullName } }
    ... on Manager { budget }
  }
  total: PersonAggregate(where: $w) { count }
}
```

with `{ "w": { "OR": [ { "age": { "gt": 60 } }, { "asEmployee": { "salary": { "gt": 200000 } } } ] } }`:

```json
{ "data": { "people": [ { "__typename": "Manager", "fullName": "A. Lee", "salary": 410000.0, "manager": null, "budget": 2.0e6 },
                        { "__typename": "Employee", "fullName": "B. Roy", "salary": 230000.0, "manager": { "fullName": "A. Lee" } },
                        { "__typename": "Person", "fullName": "C. Oda" } ],
            "total": { "count": 3 } } }
```

`... on Employee { salary }` in place of `... on EmployeeInterface` is refused by lite (G10): "`Employee` has the
subclass `Manager`; inside a `Person` selection `... on Employee` matches exact Employees only; write `... on
EmployeeInterface`, or `... on Employee` beside `... on Manager`."

**4. Derived properties with arguments, milestoning, insensitive match.**

```graphql
{
  Employee(businessDate: "2024-01-01", where: { lastName: { contains: "lee", mode: insensitive } }) {
    fullName
    salaryOn(d: "2024-06-30")
  }
}
```

**5. Groups (PR5).**

```graphql
{
  PersonGroups(
    where: { age: { gt: 18 } }
    having: { count: { gt: 10 } }
    orderBy: [{ count: DESC }, { key: { address: { country: ASC } } }]
    limit: 20
  ) {
    key { address { country } born(truncate: YEAR) }
    count
    avg { age }
  }
}
```

```json
{ "data": { "PersonGroups": [ { "key": { "address": { "country": "UK" }, "born": "1980-01-01" }, "count": 212, "avg": { "age": 44.1 } }, ... ] } }
```

**6. The same document inside Pure**, as the island, typed `String[1]`:

```
function demo::ukAdults(): String[1]
{
  #GQL{ { Person(where: { age: { gte: 18 }, address: { country: { equals: UK } } }) { fullName } } }#
}
```

---

## 8. The protocol around the language

### 8.1 The endpoint

`POST /graphql/{dataSpace}` on the lite server (`server/LegendHttpServer`, beside `PureV1Api` and `QueryService`),
per the GraphQL-over-HTTP specification: `application/json` in, `application/graphql-response+json` out; `GET` for
introspection and GraphiQL. One schema per data space (its classes, its mapping and runtime, its functions); the
request names nothing else. `data` carries one entry per root field under its alias; `errors` the positioned errors;
`extensions.plans` the Pure and the SQL per root when `explain` was asked.

**Limits (G17).** Each data space declares a maximum selection depth and a maximum number of list fields per level;
`validate` refuses a document beyond them, naming the limit. The endpoint applies a row cap per root field and a
timeout per request, answering `truncated: true` in `extensions` when the cap cut a list. The `query` tool's cap sits
inside the endpoint's. **Authorization (G22)** is not this line's: the data space is the boundary, and row-level
access is the warehouse's.

### 8.2 Results

Nested JSON, to-many fields always arrays (empty, never null), to-one fields an object or null, nulls present rather
than omitted, keys in the order written, `BigInt` and `Decimal` as JSON numbers, dates as ISO strings, enums as their
names. Shape is the contract: agents rely on it more than people do. **All or nothing (G18):** a document that
fails validation returns `data: null` with every error; a database failure fails the whole request. No partial
`data` beside `errors`, which GraphQL permits and lite does not produce.

### 8.3 The island

`#GQL{ document }#` inside Pure is the same document; its type is the serialized JSON, `String[1]`, as `serialize`
answers (open: a Variant instead, decided when the Variant scalar of §5.7 is). It runs under the enclosing function's
mapping and runtime, in Studio, the Query app, DataCube and Python without any of them learning GraphQL. The
`LEGEND_ENGINE` surface keeps its refusal; the own-corpus census declares the island as lite's (LITE-DESIGN-gql-island).

### 8.4 The four tools (G14)

| Tool | Takes | Returns |
|---|---|---|
| `describe` | the data space; optionally a class list or a search term | the SDL for those classes and what they reach in one hop, with descriptions; never the whole schema of a large model unasked (the research's finding: accuracy collapses on large schemas) |
| `validate` | a document and its variables | on success the typed result shape (`[{ fullName: String!, firm: { legalName: String! }? }]`); on failure every error with phase, line, column, message, nearest valid name; warnings beside |
| `query` | a document, variables, a row cap | the answer, `truncated: true` when the cap cut it, the count of rows per root |
| `explain` | a document and variables | the Pure per root field and the SQL per dialect, as `validate` plus the plans |

As HTTP routes first (`/graphql/{ds}/schema`, `/validate`, `/graphql`, `/explain`), as MCP tools in PR6 with the
one-page reference and five examples (three right, two labelled wrong: `where: { age: { _gt: 30 } }` and `where: {
age: 30 }`) in the tool descriptions. Apollo's MCP server runs its `introspect`, `validate`, `execute` against the
endpoint without any of this, which is the point of being GraphQL.

### 8.5 Studio

GraphiQL (MIT) in the Query app over the endpoint: introspection-driven completion, the docs panel showing the
generated types beside their class. Later.

---

## 9. The harness

### 9.1 Files: `core/src/test/resources/graphql-parity/`

| File | Contents |
|---|---|
| `models/*.pure` and `models/*.rows.yaml` | the models and rows the cases run over: `demo` (§7), a milestoned model, a model with every scalar and enum, a three-level hierarchy with multiple inheritance, a model with a Pure function root |
| `schema/*.graphql` | the generated SDL per model, golden; a change is a reviewed diff |
| `lite/*.yaml` | lite's own cases: per case the document, the variables, the oracle (`prisma`, `sql`, `validity`), and for a `sql` case the SQL twin; for a `validity` case the expected verdict and message |
| `prisma-answers.jsonl` | Prisma's answer per `prisma` case: the rows, the Prisma version, the Postgres version, the case's hash |
| `validity.jsonl` | graphql-js's verdict per document of the randomized differential: valid, or the rule and message |
| `ledger.tsv` | per case: the DuckDB status, the Postgres status, the oracle |
| `decisions.tsv` | every REFUSED case with its reason and register row |

### 9.2 The Prisma oracle: `tools/prisma-oracle/`

`schema.mjs` writes a Prisma schema from a model's generated SDL (classes to models, scalars, enums, to-one and to-many
relations with the harness's keys; inheritance flattened to the base model with the subclass columns nullable, since
Prisma has none, which is why inheritance cases are `sql` cases). `snap.mjs` loads the rows into the embedded Postgres,
runs each case's Prisma twin, which the YAML spells as a Prisma call (`findMany({ where, orderBy, take, skip, select
})`, `aggregate`, `groupBy`), and writes the answers. `--check` re-runs and diffs. Node only, Prisma pinned in
`package.json`; nothing of Prisma enters core. **The lane's scope (G21):** it judges the dialect, operators, nulls,
ordering and the quantifiers, over a table-per-class fixture; mappings, unions, milestoning and inheritance are
judged by SQL twins on the Postgres lane.

### 9.3 The judge and the differential

`GraphqlParity` reuses `SqlParity`'s grid, statuses, ratchet, ledger I/O and YAML reader, with its own `outcome()`:
lite's JSON against the oracle's rows, keys by name, lists in order where an `orderBy` fixes it and as sets otherwise,
doubles within tolerance, dates as ISO, nulls as nulls. The validity differential generates documents over a golden
schema (random selections, arguments drawn from the input types, variables, fragments, the G10 trap on purpose), runs
graphql-js in the Node target to record verdicts, and asserts in every lane that lite rejects everything graphql-js
rejects and accepts everything it accepts except the rules of §6.3, each of which the case names.

### 9.4 Lanes and guards

- `//core:graphql_parity_test` (DuckDB) and `//core:graphql_parity_postgres_test` (embedded Postgres 16), in
  `//gates:core`; `//core:graphql_schema_golden_test`; `//core:graphql_validity_test` (reads `validity.jsonl`);
  `//tools/graphql-js:differential` (Node; writes `validity.jsonl`; heavy lane).
- UNSNAPPED, STALE, GONE as `iq-parity`; an answer snapped under another Prisma or Postgres version than the pinned
  one is STALE.
- The randomized check as the Postgres line's: type-correct documents over `demo`, run on Prisma and both lanes.
- The size guard: generated SQL size per case from the first landing; nested clauses and aggregates multiply
  correlated subselects, and the IQ line met a fourfold growth per level.

---

## 10. Milestones and slices

Each landing: `//gates:local`, the TeaVM builds, one full CI run, an independent review by an agent that wrote none
of it, a GATES entry, §13 here, `IN_FLIGHT`.

| Slice | Content | Cases | Needs |
|---|---|---|---|
| **PR1** the schema and the validator | this study; `compiler/graphql/SchemaGenerator` (§5, every rule but groups' execution), the SDL and the introspection answer, the goldens; `GqlValidator` (§6.3) over the generated schema including G10 and G11; the routes `/graphql/{ds}/schema` and `/validate` with the typed result shape; the island's Typer arm to the validator (typed, not yet executed: a refusal by name after validation); the resource limits (G17); the graphql-js differential and `validity.jsonl`; the own-corpus declaration | `schema/*.graphql`; `lite/validity.yaml`; the differential | the Typer arm, the server route; nothing in lowering |
| **PR2** execution | `GqlToPure` for selections, aliases, variables, fragments, `@include`/`@skip`; the root and nested `where`, `orderBy`, `limit`, `offset` as composed clause bodies on tree nodes (§6.2: the node's expression position, composer, parser, checker); the lambda-typed tree argument; `sortBy` carrying tie-breakers and the derived-child inliner recognising sort, limit and offset; scalars and enums; derived properties with and without parameters; milestoning arguments; the endpoint, the envelope, the limits and the all-or-nothing rule; the island executing; the Prisma oracle tool and `prisma-answers.jsonl`; **the bet measured:** twenty natural-language tasks over `demo`, first-attempt validity and accuracy with and without `validate`, recorded as a measurement | `lite/basics.yaml`, `filters.yaml`, `nested.yaml`, `scalars.yaml`, `derived.yaml`; the randomized check; `eval/tasks.yaml` | the shared files announced in `IN_FLIGHT` (`GraphFetchLiteral`, `SpecParser`, `PureComposer`, `GraphFetchChecker`, `GraphEmission`, `Sorts`) |
| **PR3** inheritance | interfaces and implementing types; the companion's `__typename()` and `asY()` qualified properties (§5.8); `... on`, `asY` in `where` and `orderBy`, the G10 refusal end to end, the G19 descriptions, abstract classes, multiple inheritance | `lite/inheritance.yaml` (SQL twins) | the mapping's set-implementation discriminator exposed to the translator; the `typemodifiers.abstract` stereotype read by name (new: nothing reads it today) |
| **PR4** aggregates | the companion's aggregate classes and `pAggregate(where)` properties (§5.8), the constructor-navigate fusion (§6.2), `XAggregate` at the root and on collections, `orderBy` by an aggregate | `lite/aggregates.yaml` (Prisma `aggregate` twins) | |
| **PR5** groups | the companion's group classes, `XGroups`, `key` as the grouping (meta-fields never keys, G19), `truncate`, `having`, group `orderBy`, nested groups per parent | `lite/groups.yaml` (Prisma `groupBy` twins; `sql` twins for `truncate` and nested groups) | the Relation-rows-to-JSON fold in the emitter |
| **PR6** the agent surface | `explain`; the MCP wrapper over the four routes; the tool descriptions as the one-page reference; GraphiQL in the Query app; the PR2 evaluation repeated at forty tasks over the full feature set | `eval/tasks.yaml` | |
| **Later** | an OData reader onto the same translator (`$select`/`$expand` as a selection, `$filter` as a where, `$apply` as groups); cursor paging if a client needs it; `@defer`; mutations after F1; a `Variant` scalar; a numeric `bucket` on group keys; a schema per data space version served from Depot | | |

**First slice: PR1 alone.** It ships the generated schema, introspection and a validator that returns typed result
shapes, which is already the thing agents need most, before any row is fetched.

---

## 11. Risks

- **Schema size on large models.** A model of five hundred classes generates several thousand types; introspection
  is one answer of megabytes. `describe` is scoped from PR1; introspection is served whole because GraphiQL and Apollo
  expect it, and its size is measured per test model from the first landing.
- **Name collisions and the refusal.** G11 refuses a data space for one colliding property. Real models will meet it
  (a property named `count` is fine; one named `employeesAggregate` is not). The refusal names the element and the
  rule; a rename is the model author's.
- **The fragment trap is lite's to catch.** G10 is a lite rule; graphql-js will keep accepting the query. GraphiQL
  will not show the error until the request runs; the Query app's editor should ask `validate` as it types.
- **Prisma as oracle has its own semantics.** `mode: insensitive`, `in: []`, `every` on an empty relation, nulls in
  `orderBy`: each is measured on Postgres and recorded; where Prisma and SQL disagree the register row says which lite
  follows (SQL's, with the Prisma case marked REFUSED by name). Prisma's version is pinned; a bump is a landing.
- **64-bit integers in JSON.** `BigInt` serializes as a JSON number; JavaScript clients lose precision past 2^53. The
  alternative, strings, breaks every other client. Documented in the register; the Python client reads exact.
- **Variables and shape.** `@include(if: $x)` makes the plan depend on the variables; a plan cache keyed by shape, not
  by document, from PR2.
- **The lowering increments touch shared files.** `protocol/spec/GraphFetchLiteral`, `SpecParser`, `PureComposer`,
  `GraphFetchChecker`, `GraphEmission`, `lowering/Sorts`: announced in `IN_FLIGHT` before PR2, as the Cypher
  plumbing was. The `sortBy` tie-breaker carry changes an existing lowering for every Pure user, toward Pure's
  semantics; its own cases pin it.
- **The companion changes what `ModelContext` holds.** Generated classes and qualified properties are visible to the
  compiler for every query over the data space, not only GraphQL ones. That is the milestoning precedent and a
  feature, but a model whose own names collide meets G11's refusal, and a listing that forgets the synthetic flag
  would show generated classes to a person. The goldens pin both.
- **The bet may fail.** If PR2's twenty tasks show agents writing Hasura's dialect anyway, the fix is the tool
  description and examples first, the schema second; the evaluation sits before inheritance, aggregates and groups
  so that a change is cheap.
- **Trademark and names.** GraphQL is a trademark of the GraphQL Foundation; Prisma's of Prisma Data, Inc.; neither
  name appears in a type or a field. The dialect has no name of its own: it is "lite's GraphQL schema", generated.

---

## 12. Sources

- **GraphQL:** the specification, September 2025 edition and the working draft (spec.graphql.org); "The September
  2025 Edition" (graphql.org/blog, 2025-09-08); "Introducing GraphQL.js v17" (2026-06-15); the Foundation's 2026
  budget post (2026-04-09); GraphQL over HTTP (graphql.github.io/graphql-over-http); graphql-js `src/__tests__`;
  graphql-cats; the Composite Schemas roadmap.
- **The dialects:** Hasura v2 and v3 filter, pagination and aggregation references; PostGraphile v5 filtering and
  polymorphism; Prisma Client reference (filtering and sorting, aggregation, `groupBy`); Neo4j GraphQL library v3 to
  v7 filtering and migration guides; AWS Amplify GraphQL transformer; Relay Cursor Connections; Shopify search
  syntax; GitHub GraphQL pagination.
- **Adoption and agents:** Postman State of the API 2025; SM3-Text-to-Query (arXiv 2411.05521); IBM text-to-GraphQL
  (ACL Anthology 2024.emnlp-industry.117); CypherBench (arXiv 2412.18702); the TypeDB query-generation benchmark
  (2026); Elastic's text-to-ES|QL benchmark; Anthropic "Writing tools for agents" and "Advanced tool use"; OpenAI
  function-calling guidance; the Model Context Protocol specification (2025-06-18); Apollo MCP Server docs; Supabase,
  Neo4j, BigQuery and MongoDB MCP servers; Microsoft Graph MCP server; Hasura PromptQL.
- **Alternatives surveyed:** OData 4.01 and the Data Aggregation extension (OASIS); Apache Olingo's retirement
  (attic.apache.org); JSON:API 1.1; PostgREST; MongoDB specifications (CC BY-NC-SA) and FerretDB; EdgeQL and the Gel
  sunset notice; PartiQL and its license; ISO/IEC 39075:2024; Jakarta Data and Query; Cube and Malloy; Open Semantic
  Interchange.
- **legend-engine:** `legend-engine-xts-graphQL/` and `docs/graphQL/graphQL.md` (read for prior art; nothing loaded).
- **This repository:** `docs/LEGEND_SQL_STUDY_2026_10_11.md`, `docs/IQ_SQL_STUDY_2026_10_13.md`,
  `docs/CYPHER_STUDY_2026_10_15.md` (§1, §5.3, §8, §9), `docs/TENET_CHARTER.md`, `docs/WORLD_MAP.md`,
  `docs/OWN_CORPUS_DECISIONS.md`, `docs/BI_AND_ETL_PLAN_2026_09_29.md` (F1), `core/.../parser/GqlParser.java`,
  `core/.../protocol/spec/Gql.java`, `core/.../protocol/spec/GqlIsland.java`,
  `core/.../protocol/spec/GraphFetchLiteral.java`, `core/.../compiler/spec/GraphFetchChecker.java`,
  `core/.../resolver/GraphEmission.java`, `core/.../resolver/ClassConcatenates.java`,
  `core/.../resolver/CorrelatedSubselects.java`, `core/src/test/java/com/legend/testcases/SqlParity.java`,
  `core/src/test/resources/cypher-parity/`, `tools/iq-oracle/`, `MODULE.bazel` (rules_js).

## 13. The record of landings

### 13.1 PR1a, 2026-10-18: the generated schema, the validator, describe and validate, the island's arm

Branch `claude/graphql-study-2026-10-18`; the GATES entry "GraphQL: the first landing". PR1 of §10 less the
graphql-js differential (PR1b), introspection (PR2) and the composer's rule for the island (PR2).

- **What the code read changed in the plan.** Nothing in the generator's rules; one scoping rule written down
  here: an association whose two classes are not both in scope injects nothing (a data space scoped to `Firm` has no
  `employees`), where a declared property whose class is out of scope, or a superclass out of scope, is refused
  (§5.7). Errors carry the selection path, `Person(where).agee`, `Person.firm.nope`; line and column wait on the
  AST carrying positions, which PR2 adds with the Typer's needs (the engine's AST carries none).
- **Measured.** 923 lines of SDL for the five classes of §7, 103 types. The validator's rules each pinned by a
  case (`GqlValidatorTest`); the five packages touched and then every core package and the layering policy green;
  the full local gate once whole (371 of 383, the rest fixed or environmental and rerun green: the GATES entry).
- **What the gate taught.** TeaVM's JDK is smaller than the JVM's (`Collections.unmodifiableSortedSet`); the
  compiler layer's tests may not import the parser; a new server class registers in the funnel register with its
  tenet argument; the schema's type references are the schema's own records, not the protocol's nodes; a scope's
  platform classes are a set lookup, never a package prefix tested as text.
- **Not done, on purpose.** The graphql-js differential (its Node target is PR1b); the data space's own limits
  (G17 takes the request's `limits`, the default 8 deep and 25 list fields per level, until a data space can
  declare them); function roots (G4's Pure functions of the data space; PR2); milestoning arguments (PR2).

### 13.2 PR2, 2026-10-19: execution — the translator, the island, the composed node, the endpoint, introspection

Branch `claude/graphql-study-2026-10-18`; the GATES entry "GraphQL: the second landing". PR2 of §10 less the Prisma
oracle and the twenty-task evaluation (moved to PR6 with the agent surface, which they measure), function roots,
milestoning arguments and `Gql.Field` positions (below).

- **What the probes found.** The study's first probe (a model-declared `top3()` with a sort and a take, fetched in
  a tree) stopped exactly where §6.2 said: `GraphEmission.derivedChild` recognised a navigation head and its filter
  and nothing after them, so the body fell into the association path and failed as "property not mapped". The
  increment was the one named: the ordering tail (`sortBy`/`sortByReversed`/`sort`, `take`/`limit`, `drop`,
  `slice`) peeled off the body and re-applied over the correlated child relation, each key substituted through the
  target's bindings as the filter is; the correlated emission already ordered and limited the subselect. The second
  increment named, `Sorts.sortBy` carrying a sorted source's keys as tie-breakers, was a sort folding onto the sorted
  select (the new key first, the source's after it) — a chain of `sortBy` is one `ORDER BY` with several keys, as
  G16 wants. `reverse()` has no relation shape, so the translator never spells it: a descending key is
  `sortByReversed`.
- **Where the translator lives, and why.** `protocol/graphql/GqlToPure`, beside `protocol/cypher/CypherToPure`:
  the architecture guard says protocol nodes are built by the parser, the normalizer and the protocol package only
  (`ArchitectureTest.protocolNodesAreConstructedOnlyByTheParserAndTheNormalizer`), and the compiler's layer may not
  reach the parser. The schema's records moved beside it (`protocol/graphql/GqlSchema`: JDK and the GraphQL AST
  only), and the guard's rule was narrowed with a dated justification: a schema record is data the generator computes
  from the model, not a program node. The compiler's seam (`compiler/graphql/GqlIslands`) validates, translates, and
  resolves names as the Cypher seam does; the Typer types the result once.
- **The composed node (§6.2 (ii)), as built.** `GraphFetchLiteral.Node` carries an optional body — an expression over
  `$this` whose value is the node's collection — keyed by its alias, its `property` the body's navigation head (so
  the emitter demands the association's parent keys as it does for the association). The parser reads `'top3':
  $this.employees->filter(...)->sortByReversed(...)->limit(3) { fullName }`; the composer prints it; the wire carries
  it as a lite-only `body` key; the checker types it with `$this` the node's object, as a derived body is typed;
  `GraphEmission.bodyChild` (the second half of `derivedChild`, which a derived property now shares) builds the
  correlated child. The own-corpus census declares it (LITE-DESIGN-graphql-tree,
  `docs/OWN_CORPUS_DECISIONS.md`); the LEGEND_ENGINE surface has no such node.
- **What the island means now.** One root field over the model, its value the serialized tree; the island's
  variables are the enclosing Pure scope's (`$n` in the document is the function's `$n`); several roots, or
  introspection, are the endpoint's. The island's errors carry the selection path (`Person(where).lastName.equals`),
  not yet a line and column (the AST carries no positions: deferred, below).
- **The endpoint.** `POST /api/lite/v1/graphql/query` with the GraphQL-over-HTTP envelope: `data` keyed by response
  key, `errors` with `path` and `extensions.code` (`REQUEST`, `VALIDATION`, `INVALID`, `UNSUPPORTED`, `EXECUTION`),
  `extensions.pure` the Pure each root ran, `extensions.warnings` the validator's; a refused request is 400 with no
  `data`; a root that fails at execution is 200 with `data: null` (all or nothing, §8.2). Introspection
  (`__schema`, `__type`) is answered from the generated schema by projecting the client's selection over the
  schema's own graph (`protocol/graphql/Introspection`), so GraphiQL's introspection query and every agent's read
  exactly the fields they asked. The data-space path (`/graphql/{dataSpace}`) and the `describe`/`explain` tool
  names wait on PR6 with the MCP wrapper; the route takes `dataSpace` or `classes` today.
- **Declared.** `docs/SEMANTICS_REGISTER.md` S92 (`every` and `none` as negated `exists`), S93 (a null under
  `orderBy` sits where the database puts it), S94 (a derived leaf is keyed by the field's name), S95 (`is`/`isNot`
  on a relation and at the root), S96 (the composed node).
- **Measured.** 21 documents on DuckDB through the island (`GqlExecutionTest`: root clauses, two keys through a
  to-one chain, paging, nodes with aliases, `some`/`every`/`none`, string operations with `mode: insensitive`,
  null tests, `NOT`/`OR`, enumerations, dates, a derived leaf, fragments and directives, scope variables, nested
  clauses with two keys and paging, a nested clause under a nested node, the composed node spelled in Pure), the
  three derived-tail probes, 6 route cases (`GraphqlApiTest`: two roots under their keys, variables, every
  validation error, a later landing named, introspection twice), the parser/composer agreement
  (`ComposedTreeNodeTest`); the graphql, parser, protocol, compiler, resolver, server, lowering and integration test
  packages green; `//core:guardrails`, `//core:census`, `//tools/deps:core_layering_test`,
  `//parser-equivalence:diagnostics` green; the full local gate (the GATES entry).
- **Not done, on purpose.** Function roots (G4's Pure functions of the data space: the generator has no root for
  them yet; PR6 with the data space's declaration); milestoning arguments on a node (the tree carries them, the
  schema does not declare them yet); `Gql.Field` positions for line and column in errors (the parser's AST carries
  none; the path names the selection); the Prisma oracle and `prisma-answers.jsonl`, and the twenty-task evaluation
  (PR6, with the forty); the composer's rule for the island inside a model (it still refuses to print a model
  holding one); subtype fragments, `__typename` and `asY` (PR3); aggregates (PR4); groups (PR5).

### 13.3 PR3, 2026-10-19: inheritance — the companion's `__typename()` and `as<Sub>()`, subtype fragments, `asSub` in `where` and `orderBy`

Branch `claude/graphql-study-2026-10-18`; the GATES entry "GraphQL: the second and third landings". PR3 of §10 less
the SQL-twin cases on the Postgres lane (below).

- **The companion, as built (G15, G20).** `normalizer/CompanionSynthesis` writes the members into the parsed
  classes after the knowledge layer's adoption and before the one index reads them (`Compiler.normalizeLayer`, the
  graph layer only; the boot layer takes none): `__typename(): String[1]`, `if($this->instanceOf(S), |'S', |...
  |'C')` over the transitive subclasses deepest first, and `asS(): S[0..1]`, `if($this->instanceOf(S),
  |$this->cast(@S), |[])`, on each hierarchy's ROOT class (a subclass inherits them; declaring them again would
  carry two members of one name). Each carries `<<meta::legend::lite::graphql.companion>>`, by which the generator
  knows them (never fields of the type) and a listing may hide them. A class declaring one of the names is refused
  (G11). Pure may call them. One row of the register names the one visible consequence: every root class has one
  more derived property (`DuckDBIntegrationTest` counts the declared stored ones).
- **The exact class, answered from the row.** The tree's `__typename` leaf inlines the companion body as any
  derived leaf, and `GraphEmission.inlineThis` answers its two forms from the union row: `instanceOf($this, T)` is
  the witness column tested (partial membership), true for the class and its ancestors or where the row carries
  T's columns and no witness, false where the mapping maps no T on the row (the mapping's own statement);
  `cast($this, @T).p` is T's thread-local column read. C6.2 holds: a constant per arm, a column compared.
- **Fragments and the first-match CASE.** The emitter's `->subType(@X)` patches are a CASE over disjoint members;
  GraphQL's fragments accumulate down the hierarchy. The translator folds that structurally: each subtype entry
  carries the selections of every fragment its class satisfies, entries deepest first, so the first match is the
  exact class with everything it inherits (`... on EmployeeInterface { salary } ... on Manager { budget }` gives
  a Manager both). A fragment on `Employee` and one on `EmployeeInterface` are one class's entry. Under a node, the
  tree has only the subtype VIEW (every child the subtype's): a fragment beside other fields, or two fragments,
  under a nested node is refused by name (the not-done list).
- **The input side (G9).** `asSub: W` is `$x->instanceOf(Sub) && if($x->instanceOf(Sub), |W($x->cast(@Sub)),
  |false)` -- the cast's reads stand behind the membership test, which the database's CASE evaluates lazily;
  `asSub: { isNot: null }` is the membership test alone, `is: null` its negation. A sort key `asSub: { p: DESC }`
  is `if($x->instanceOf(Sub), |$x->cast(@Sub).p, |[])->toOne()`: the key where the row is a member, a null
  elsewhere, where the database puts it (S93). No companion member is spelled on the input side: `instanceOf` and
  `cast` are the resolver's own forms over the row.
- **What the probes found in the union lowering, fixed.** (1) `StackBuilder`'s membership witness skipped an arm
  of the union's own class, so over `union(p, e, m)` of Person, Employee[e], Manager[m] the membership of Employee
  read as total and `instanceOf(Employee)` held on every Person: a correctness bug, now an arm conforms only when
  its class is the target or below it (S99). (2) A sorted union's graph fetch raised an unfoldable reference to the
  serial ordinal (`u_serial_ord__`): the injection now bails at an ordered or limited pass-through, as its own
  contract says, and the envelope's witness keys yield to an explicit sort as the primary-key keys already did
  (S100). The DuckDB corpus is green on both.
- **Measured.** `GqlInheritanceTest`: 11 cases on DuckDB over a union of a class's and its subclasses' sets
  (table per class), the SQL-twin shape: the members on the class, `__typename` in Pure and in GraphQL, subtype
  fragments, `asSub` in `where` three ways and in `orderBy`, the exact-type trap refused with its fix (G10), a
  reserved name refused, and four baselines of the union model (a plain tree, a sorted tree, an `instanceOf`
  filter, a projection). 50 GraphQL tests in all; the compiler, parser, protocol, normalizer, server, resolver,
  lowering and integration packages green; the DuckDB corpus green; `//core:guardrails`, `//core:census`,
  `//tools/deps:core_layering_test` green; the full local gate (the GATES entry).
- **Not done, on purpose.** The Postgres lane's SQL-twin run of the inheritance cases (the DuckDB run is the
  judge here; the Postgres targets of the gate run unprivileged); a fragment on a subtype beside other fields under
  a nested node (the tree has the view only; a nested subtype entry is a later protocol step); multiple inheritance
  as a case (`implements A & B` prints; a diamond's two roots would each generate `__typename`, which the index
  would see twice: the refusal of that case is not pinned); an abstract class as a case (the generator reads
  `typemodifiers.abstract` since PR1a).

### 13.4 PR4, 2026-10-19: aggregates — the companion's aggregate classes, `pAggregate(where)` fused, the aggregate root as a relation

Branch `claude/graphql-study-2026-10-18`; the GATES entry "GraphQL: the fourth landing". PR4 of §10.

- **The companion's aggregate classes (G6, G15, §5.5, §5.8).** `normalizer/CompanionSynthesis` generates, for every
  class `T` of the model, `meta::legend::lite::graphql::companion::<T>Aggregate { items: T[*]; count(): Integer[1];
  sum(): <T>Sum[1]; avg(): <T>Avg[1]; min(): <T>Min[1]; max(): <T>Max[1] }` and the four fold classes, one
  `[0..1]` field per to-one scalar of the fold's kind (sum and avg over numbers; min and max over numbers and
  dates), each field's expression the catalog's own fold over `$this.items.<p>` by the scalar's exact overload
  (`sum` of Integer is `SUM__INTEGER_MANY`; a Decimal folds through the Number overload and the field is `Number`;
  `avg` is `average`, a Float). `count()` is `$this.items->map(x|1)->count()`: the reducer over the navigation, 0
  over no items. A class with a to-many property `p` of class `T` (stored, or an association's end injected onto
  it) carries `pAggregate(): TAggregate[1]`, `^TAggregate(items = $this.p)`. Every generated class carries
  `<<meta::legend::lite::graphql.companion>>`: the schema's scope skips them (never a root, never a type), the
  generator hides the members, a class naming `pAggregate` is refused (G11). The names are one catalog,
  `protocol/graphql/Companion` (the package, `items`, `count`, the `Fold` enum with each fold's field, class suffix
  and Pure name), which the normalizer writes by and the translator and the generator read by.
- **The nested aggregate, fused (§6.2).** `employeesAggregate { count sum { salary } }` is the companion member as a
  tree node; with a clause, the translator spells the composed body `^PersonAggregate(items =
  $this.employees->filter(W))` on the node (PR2c's body). The emitter's `companionChild` fuses a node whose body
  is a constructor of a companion class into an inline same-row object: each selected member's compiled body is
  read with the constructor's bound fields in place of `$this.<field>` (`substituteFields`), so `count` is the
  reducer over the navigation (`NavReducer`, the correlated subquery), and a member that is itself a constructor
  (`sum()` is `^PersonSum(salary = $this.items.salary->sum())`) is a nested inline object. The instance is never
  built; the database computes each leaf. C6.2 holds: the emitter rearranges shapes and compares nothing.
- **The aggregate root, a relation (§6.1).** `PersonAggregate(where: W) { count sum { salary } }` is the Relation
  API: `Person.all()->filter(W)->project(~[__one: x|1, salary: x|$x.salary])->aggregate(~[count: x|$x.__one :
  y|$y->count(), 'sum.salary': x|$x.salary : y|$y->sum()])`, one row whose columns are the response's paths joined by
  `.` (`__` until 2026-10-20: an alias may contain `__`, a dot no GraphQL name contains). The island's value is that relation (`Root.relation`), typed `Relation<(count: Integer[1], ...)>`; a Pure
  caller reads the row; the endpoint arranges the one row into the response object by the paths (`arranged`).
- **The schema.** `min`/`max` over numbers and dates only (`comparableProps`): a string's min has no Pure
  spelling in the catalog (the not-done list). The golden `demo.graphql` regenerated: `AddressMin`/`AddressMax`
  gone (a city is its one scalar), the string fields gone from every `Min`/`Max`.
- **What the suites found, fixed.** The fold overload by the scalar's exact kind (a Decimal `notional` typed
  "expected Decimal, got Number" against the Number overload); the inherited-scalar walk guarded against an
  inheritance cycle (two compiler tests that refuse a cycle overflowed in the companion before the index refused
  it); the schema's `Fold` moved to the protocol's catalog as an enum so no layer compares a fold's name as a string
  (`IdentityGuardrailTest` LOCAL_NAME_COMPARE, shrink-only); the Companion catalog allowlisted by name in
  `PlatformNamesGuardrailTest` (its package spelling reads as a function's to the regex; the file names no
  function); the facade test counts the declared stored properties (the companion's derived members stand beside
  them since PR3).
- **Measured.** `GqlAggregateTest`: 4 cases on DuckDB (the companion's classes and members, with and without a
  clause over Acme/Globex/Initech, the root as a relation through the island); `GraphqlApiTest`: the route arranges
  an aggregate root's row (+1); `SchemaGeneratorTest`: the companion's name refused by the model (+1). 56 GraphQL
  tests in all. Green: the compiler, parser, protocol, normalizer, resolver, lowering, model, server, ide, lineage,
  cache, exec, sql, builtin, platform, architecture and integration packages; `//core:guardrails`, `//core:census`,
  `//tools/deps:core_layering_test`; the DuckDB corpus and the planner-on-java.base check (the GATES entry).
- **Not done, on purpose.** `orderBy` by a nested aggregate (`employeesAggregate: { count: DESC }`: the schema
  spells `PersonAggregateOrderBy`, the translator refuses it by name); a string's min and max; `avg` on an
  Integer root column answers a Float of the database's rounding; the fold classes are generated for every class
  (a class no to-many property targets carries an unused aggregate class: a listing may hide it by stereotype);
  the SQL-twin run on the Postgres lane.

### 13.5 PR5, 2026-10-19: groups — `XGroups` through the Relation API, `key` as the grouping, `truncate`, `having`, group `orderBy`

Branch `claude/graphql-study-2026-10-18`; the GATES entry "GraphQL: the fifth landing". PR5 of §10 less nested
groups (below).

- **The groups root, a relation (G7, §6.1).** `PersonGroups(where: W, having: H, orderBy: [...], limit: n, offset:
  m) { key { firm { legalName } born(truncate: YEAR) } count avg { age } }` is `Person.all()->filter(W)->project(~[
  'key.firm.legalName': x|$x.firm.legalName, 'key.born': x|$x.born->firstDayOfYear(), __one: x|1, age: x|$x.age])
  ->groupBy(~['key.firm.legalName', 'key.born'], ~[count: x|$x.__one : y|$y->count(), 'avg.age': x|$x.age :
  y|$y->average()])->filter(g|H)->sort([...])->drop(m)->limit(n)`: one row per group whose columns are the
  response's paths joined by `__`, the response keys spelling them (an aliased `k: key { ... }` is `k__...`). The
  island's value is that relation (`Root.Shape.ROWS`); a Pure caller reads the rows; the endpoint arranges each
  row into the response object by the paths (`arrangedRows`). The selection under `key` IS the grouping; a to-one
  property nests through its own key type (`firm { legalName }` is the column `$x.firm.legalName`, null where the
  row has no firm, a group of its own); an enum key is the enum's column; meta-fields under `key` are never keys
  (G19).
- **`truncate`.** `born(truncate: YEAR)` is the catalog's first-day-of head over the read (`firstDayOfYear`,
  `firstDayOfQuarter`, `firstDayOfMonth`, `firstDayOfWeek`, `firstHourOfDay` for `DAY`, `firstMinuteOfHour` for
  `HOUR`), each the lowering's `DATE_TRUNC`; the read made one where the property is optional (the heads take
  `Date[1]`; an empty read is the null the database groups).
- **`having` and `orderBy` over the grouped row.** `having` is the group row's filter: `count` and each fold's
  properties through the scalar filters of G5 (`sum: { salary: { gt: 200000 } }` is `$g.'sum.salary' > 200000`),
  with `AND`/`OR`/`NOT`. `orderBy` is the relation's `sort([~c->descending(), ...])`, the list's first key
  primary: `count`, a key's path (selected under `key`, else refused by name), or a fold's property. A column
  `having` or `orderBy` asks that the selection does not carry is aggregated under a hidden name (`_sum.salary`)
  and dropped by a trailing `select` of the selected columns, so the row is the selection's.
- **The endpoint.** A root's value has a `Shape`: a graph root's serialized TREE, an aggregate root's one ROW, a
  groups root's ROWS; the endpoint arranges the latter two by the columns' paths. Nothing else of the envelope
  changes.
- **Measured.** `GqlGroupsTest`: 7 cases on DuckDB through the island (the key as the grouping with a count and a
  fold sorted by both; `where` before the grouping and `having` after it; `having` on a fold the selection does
  not carry, the hidden column dropped; an enum key through a to-one path; a date key truncated, sorted and paged;
  `orderBy` a fold with an aliased key; a key not selected refused). `GraphqlApiTest`: the route arranges a groups
  root's rows (+1). 64 GraphQL tests in all. Green: the compiler, parser, protocol, normalizer, resolver, lowering,
  server, root and integration packages; `//core:guardrails`, `//core:census`, `//tools/deps:core_layering_test`;
  the DuckDB corpus; the planner-on-java.base check; the full local gate (the GATES entry).
- **Not done, on purpose: nested groups** (built 2026-10-21, §13.8). `employeesGroups` under a parent object
  (`Meaning.GroupsOf`) is refused
  by name: a group's rows under a parent need a RELATION-VALUED child in the emitter (a correlated
  `project->groupBy` pipeline serialized into the parent's JSON array, the §6.1 row's "folded into its JSON array
  by the emitter"), a mechanism the tree does not have (its to-many children are navigations with filter and
  ordering tails). The refusal names the root form with a `where` on the parent as the way to ask today
  (`PersonGroups(where: { firm: { legalName: { equals: "Acme" } } })`). The validator accepts the nested form
  (its rules are the schema's), so a client learns the refusal from the translator, with code `UNSUPPORTED`.
  Also not done: a numeric `bucket:` beside `truncate` (G7 names the place); the SQL-twin run on the Postgres lane.

### 13.6 PR6 and PR1b, 2026-10-19: the agent surface — `explain`, the MCP wrapper, the one-page reference, GraphiQL, the forty tasks; the graphql-js differential

Branch `claude/graphql-study-2026-10-18`; the GATES entry "GraphQL: the sixth landing". PR6 of §10 and PR1b.

- **`explain` (G14, §8.4).** `POST /api/lite/v1/graphql/explain`: the verdict `validate` answers and, per root
  field, its key, the shape of its value (`TREE`, `ROW`, `ROWS`), the Pure it means (the composer's text) and the
  SQL it runs on the runtime, planned and never run (the planner touches no database, C2a). A document the
  translator refuses is answered invalid with the refusal among its errors and its code.
- **The MCP wrapper (G14).** `server/GraphqlMcp`: the Model Context Protocol's Streamable HTTP transport at
  `POST /mcp` on the lite server, one JSON-RPC 2.0 message per request (`initialize`, `notifications/initialized`,
  `ping`, `tools/list`, `tools/call`; a notification is 202 with no body; the protocol's errors by code). The four
  tools are the four routes: a call's arguments become the route's body and the route's answer the tool's text
  content, `isError` where the route refused. A server started with `--graphql-model FILE` answers calls that name
  no model; a call's own `model` wins. Nothing of the wrapper plans or runs a query. Apollo's MCP server over the
  endpoint needs none of this, which is the point of being GraphQL; this is for a client that speaks MCP and has no
  GraphQL client of its own.
- **The one-page reference.** `server/GraphqlReference.TEXT`: the dialect on a page (roots, selections, `where`
  with Prisma's names, `orderBy`, paging, aggregates, groups, inheritance, results) with five examples, three right
  and two labelled wrong (`_gt`, a bare value), as the MCP server's `instructions`, the `describe` tool's
  `reference`, and the tool descriptions' idiom. The schema is the authority; the page is the idiom.
- **GraphiQL in the Query app (§8.5).** `query/demo/graphiql.html`: GraphiQL (MIT, pinned from unpkg) over the lite
  server's query route; the page reads the model files it is told (`?models=`), sends the model beside every
  request, introspection included (the route takes a model, there is no server-side store), and names the runtime
  and the scope from its query string. It is a page of the static site, not of the in-browser engine: the GraphQL
  routes are the server's. Not run in a browser in this session (the not-done list).
- **The forty tasks (§9).** `graphql-parity/eval/tasks.json`: forty asks in words over the full feature set
  (roots, `where` in every operator family, `orderBy`, paging, nesting, derived properties, aliases, fragments,
  variables with defaults, `@include`, aggregates at the root and nested, groups with `having`, `orderBy` and
  `truncate`, inheritance), each with its reference document and what its answer holds. `GqlEvaluationTest`
  PROVES the references (every one runs on DuckDB through the island and answers as said) and holds the judge,
  `score`: a candidate document per task, marked by its answer against the reference's. The agent run itself (a
  model given the tool descriptions and the asks, its forty documents scored) is not in this session: no model is
  called from the test tree; the judge and the tasks are the harness it needs.
- **The graphql-js differential (PR1b, §9.3, G1).** `tools/graphql-js/differential.mjs`, a hand tool outside the
  Bazel build (graphql 17.0.2 pinned, the lock committed; the npm registry is reachable from the session): the
  thirty-two base documents of `documents.json` over the golden `demo.graphql`, as written and under a seeded set
  of sixteen mutations (a near-miss field, Hasura's `_gt`, a bare value, an object without a selection, an unknown
  argument, a fragment on the wrong type, a string for an Int, two keys in one sort object, a selection on a
  scalar, two fields under one key, an undefined variable, an unused fragment, a mutation operation, an empty
  selection, an argument twice, an introspection near-miss), graphql-js's verdict on each as the JSON lines of
  `validity/validity.jsonl` (260 documents: 33 valid, 227 invalid). `GqlValidityTest` is lite's half: lite rejects
  everything graphql-js rejects, and where lite is stricter the mutation is named with its §6.3 rule
  (one document, `twoKeysInOneSortObject`, G16: a sort key names exactly one property). Every base document is valid for both. **What it found:** the validator let an introspection
  near-miss through (`__schema { queryTypes { name } }`; the endpoint's answerer refused it, the validator did
  not), which graphql-js refuses: the validator now runs the answerer over the introspection roots and reports
  its refusal as the verdict's error (G1 holds on all 260).
- **What the full gate of the fifth landing found, fixed here.** PR4's companion CLASSES, generated for every class
  of every model, stood among the classes a label-less Cypher node reads and among the graph's edge classes:
  198 Cypher parity cases regressed (an unmapped `...::g::AAggregate` with "0 mappings"; `DurationAvg.months:
  Float` beside `Duration.months: Integer`). The study's own rule (§5.8: synthetic, hidden from listings,
  callable from Pure) is code now: `ModelContext.isSynthetic(fqn)`, read by the Cypher labels, the edge classes
  and the GraphQL scope; `CypherParityTest` is green again on DuckDB and on Postgres. **The audit after it** (the
  user's ask): every reader of the compiled model's element index was walked. Four list classes and read the
  predicate now (the Cypher labels, the untyped relationship's ends read associations only, the graph's edge
  classes, the GraphQL scope, the lineage scan's short-name resolution: a user's `demo::PersonAggregate` is no
  longer ambiguous against the companion's); the others read the element index for a name lookup, not a listing
  (the inliner's ancestors, generic-type reflection, the package index). Every app listing reads the PARSED model
  (the diagram service, the Query app's explorer over `grammarToJson/model`, DataCube's model changes), which
  carries no companion; the browser planner and the Python client list no classes. `SyntheticClassesTest` pins
  the rule on each listing.
- **Measured.** `GraphqlApiTest` +1 (explain), `GraphqlMcpTest` 5, `GqlEvaluationTest` 3 (the forty references
  on DuckDB, the judge marking a wrong and a missing answer wrong), `GqlValidityTest` 2: 87 GraphQL tests in
  all. Green: the server, root, compiler, protocol and normalizer packages; `//core:guardrails`, `//core:census`,
  `//tools/deps:core_layering_test`; the full local gate (the GATES entry).
- **The trading probe (2026-10-19, after the landing).** The generator and the endpoint run over the Query app's
  trading demo (`query/demo/models`: six classes, three enums, three associations, two temporal classes, a data
  space, its `###Data` rows): the SDL of its data space is 947 lines, and twenty-three documents ran against the
  rows (roots, every filter family, to-one paths in `where` and `orderBy`, a derived property as a sort key,
  nested clauses, quantifiers two deep, aliases, fragments, variables, the root aggregate, nested aggregates,
  groups by an enum, by a to-one path with `having`, by a week, by a two-level path, introspection, `explain`).
  Two bugs found and fixed with their regression cases: an enum list in `in`/`notIn` was refused as one value
  (`GqlToPure.literal`); a qualifier calling a qualifier (`notionalAt(rate)` over `notional()`) was refused as
  not inlinable on the derived-leaf paths (`GraphEmission.derivedLeaf` β-inlines the callee first, as the node-body
  path did). Two gaps confirmed, open by name: a nested aggregate's fold over a DERIVED property
  (`tradesAggregate { sum { notional } }`) fails in the companion's fusion path, while the same fold at the root
  works through the Relation API (one more reason for the relation-valued node); and a milestoned to-one
  (`Firm.rating`) is offered by the schema and refused at execution for want of a business date (milestoning
  arguments on nodes).
- **Not done, on purpose.** The agent run of the forty tasks (needs a model and its key; the harness is here);
  GraphiQL opened in a browser; the data-space path (`/graphql/{ds}`) as a route of its own (the routes take
  `dataSpace` in the body, the MCP tools too); the differential as a Bazel target in the heavy lane (it would add
  an npm package to `MODULE.bazel`; the hand tool and the committed lines are the record); the Prisma oracle
  (`tools/prisma-oracle/`, §9.2) and `lite/*.yaml` cases (the DuckDB tests and the SQL twins judge today); a
  numeric `bucket:` on group keys; nested groups.

### 13.7 The hardening line, 2026-10-20: a reference evaluator, a fuzzer, a corpus, and what they found

Branch `claude/graphql-study-2026-10-18`; the user's ask: "make GraphQL amazing and expert and super bulletproof
with lot more variety of use cases and test cases that work for real and fix all the bugs". No PR (the user's
instruction); each landing is a commit on the branch, its checks named in its message.

- **The method.** `core/src/test/java/com/legend/graphql/hardening/`: a shop fixture built to break things
  (`ShopFixture`: every scalar kind, nulls in every nullable column, empty lists, ties on every sortable column,
  quotes, `%` and `_`, unicode and case variants, a 64-bit integer past 2^53, a leap day, an orphan order, referral
  chains three deep, a many-to-many through a join table, derived properties of each kind, and a class hierarchy:
  `Payment` with `CardPayment` and `VoucherPayment`, each on its own table, the base's extent their union, the
  association mapped per member set); an INDEPENDENT reference evaluator (`Reference`, `ReferenceModel`,
  `ShopReference`: the same rows in Java, each derived property in its Java meaning, the contract's rules written
  once: two-valued comparisons, existential to-one conditions, `mode` inherited, nulls sorting largest, keys in
  selection order, folds skipping nulls, `count` 0 and every other fold absent over no rows, `avg` a float); a
  seeded fuzzer (`Fuzz`: roots, nested lists with clauses, aggregates and their sort keys, groups with `having` and
  fold sorts, aliases on every member, fragments and directives, variables, subtype fragments, `as<Sub>` conditions
  and sort keys, sort paths three to-one hops deep) with a shrinker that keeps a failure's signature; and a corpus
  of hand-written cases (`shop-cases.graphql`, 209 cases, errors included). `HardeningSuite` runs both on DuckDB
  (`GraphQLHardeningTest`, in `core_tests_root`) and on the embedded Postgres 16
  (`//core:graphql_hardening_postgres_test`, the reference folding case for ASCII only, as the C-locale server does).
  `FuzzProbeTest`, `ReproProbeTest`, `PureProbeTest` and `ModelProbeTest` are the developer's explorers
  (`-Dgql.probe=true`).
- **What it found and what was fixed**, by landing (each commit's message has the full list):
  - Round one (`cb19b014`): an order lost through `json_group_array` once a nested count decorrelated into a join
    (wrong data); an alias with `__` split into nested objects (the path separator is now `.`, S103); derived counts
    over navigations and bridge tables, composed nested lists with navigating filters and sort keys, a collection
    function over a to-one hop, companion aggregates over derived collections and of derived leaves, `__typename` on
    the generated types (S115), `mode: insensitive` over an optional string, aggregates and joins inside nested
    quantifiers (refusals made to answer).
  - Round two (`257f7bf9`): a presence guard over a derived count read by the exists-to-join rewrite as "a source
    row exists" (wrong data); a date key truncated to `DAY` answering a date-time; sorts through a to-one hop into a
    many-to-many count and through two hops into a count.
  - Postgres (`b9a6e3a0`): a pruned qualified star whose readers kept the star's spelling (`column t3.id does not
    exist`); a root-package class with a derived property (the companion's `__typename()` made every root-package
    class fail, the PCT's `Class String {}` among them).
  - Round four (`932773bf`): sorting by a nested aggregate (S116) and the resolver shapes it needed: `min`/`max`
    over a to-many behind a to-one hop fanned the rows out and dropped the aggregate (wrong data); chain keys past
    several to-one mids; computed mappers over chains; filtered derived collections under a graph root; a reducer
    of a reducer (each aggregate's target a scope of its own, its inner reducers' grouped joins folded onto it); an
    absent element counted through an inner reducer's empty value.
  - `599016ba`: a string's `min` and `max` (S104).
  - The merge of `origin/main` (`c6c4f496`): the two lines had used register rows S76 to S88 for different rows;
    the GraphQL rows are S89 to S116 since, their citations renumbered.
  - `6b6114e5`: emptiness over three to-one hops (a derived leaf's guard that deep).
  - The hierarchy (`831481c0`): subtype entries on a nested node (S117: fragments on subtypes under
    a nested list keep every element, the engine's view kept only one subtype's; fragments beside other fields were
    refused); a member's keys in its fragments' order (S114); `__typename` on a subclass's own extent (a sibling's
    `instanceOf` is false); a subclass's derived leaf inside a fragment; a subtype test inside a quantifier; an
    inherited association read through `as<Sub>` (the cast folded where the row's own class has the property); a
    quantifier inside a quantifier over a union; an empty `as<Sub>: {}` filtering nothing (S95). The full
    validation of that landing caught two regressions of its own, fixed in it: the nested scope's subtype tables
    registered strictly, refusing 14 relational corpus tests over the plan-node hierarchy (the rosters stayed as
    committed); and register row S90's claim that the drop-in parser refuses an inline fragment, which no code kept
    (`GqlParser` now refuses one on the LEGEND_ENGINE surface; docs/OWN_CORPUS_DECISIONS.md §16).
  - Nested groups (§13.8).
- **Measured.** The corpus's 222 cases agree on both lanes; the suite's own fuzz (three seeds of 150) agrees on
  both; the explorer's campaigns agree on every document: 6,000 before the widened generator, 3,000 after it with
  variables, 3,000 with the hierarchy (seeds 1 to 10), 2,000 after the hierarchy's validation fixes (seeds 31 to
  35), and 6,000 with nested groups (seeds 41 to 55).
- **Not done, on purpose or still open.** The JSON wire's refusal of a class-level `sortBy` whose key the projection
  renames (not GraphQL's path); the Pure text parser reading nested subtype entries (S117: the engine's check
  stands); the Prisma oracle; the root aggregate and groups as the relation node without a parent (§13.8).

### 13.8 The relation node, 2026-10-21: groups under a parent object

The H5 spike of the redesign the user approved on 2026-10-20 (a query-time summary is a table the database
computes; the tree gains a node whose value is one). The kill switch was "three days without a correlated grouped
subselect on DuckDB"; the first build answered.

- **The node (register S118).** A composed node's body (S96) may be a relation over `$this`. The checker
  (`GraphFetchChecker.columns`) types the tree under it against the table's columns: a leaf is a column under its
  response key (`'status': key_status`), a sub-tree an object of the same row's columns (`key { ... }`), and a call,
  a body or a subtype view under it is refused by name. The resolver (`GraphEmission.relationChild`) peels the
  relation operators down to the `project` whose source is the class part; the class part (a navigation from
  `$this`, its filter, its ordering) correlates to the parent row exactly as every composed node's does (the
  extracted `headedRel`); the projection's columns read the target's row through its bindings, as a filter or a
  sort key does; every operator above (`groupBy`, `aggregate`, `filter`, `sort`, `drop`, `limit`, `extend`,
  `select`, a second `project`) is the same operator over that source (each `TypedRelationOp` keeps its source as
  its first child). The envelope reads columns; a sub-tree is an inline object over the same row. The lowering
  needed nothing new but one fix: a sorted relation under a renaming projection lost its order's ordinal
  (`GraphSortOrdinal` now passes it through a plain projection; the fuzzer's first nested-groups campaign found it).
- **GraphQL.** `pGroups(where, having, orderBy, limit, offset)` under a parent object is the root groups' pipeline
  (`GqlToPure.groupsPipeline`, shared) over `$this.p` with the parent's `where`, its path-named columns renamed to
  identifiers by one last `project` (`key.product.category` becomes `key_product_category`), and the tree the
  selection's shape over them. Its `explain` text parses back and runs. A parent with no elements answers `[]`.
  `GraphqlReference` (the agent page) says so; the translator's one remaining refusal by name is a clause on a
  derived collection that takes arguments.
- **Measured.** `GqlExecutionTest`: the node spelled in Pure (a whole-collection aggregate as a one-row table;
  groups with a key object, a count, a max under an alias, a sort, a firm with none), its refusals, and nested
  groups through GraphQL (+4). `GraphqlApiTest`: the route answers nested groups, `explain` shows the node.
  `GqlIslandsTest`: an island with nested groups types. The corpus's 13 `nested groups:` cases (a filter before and
  `having` after, a key through a to-one hop, a month key offset and limited, `having` on an unselected fold,
  aliases and `__typename` throughout, a derived collection, groups two levels down, beside the aggregate and the
  list of the same collection, a union hierarchy grouped by a derived key, a fragment with a variable, under a
  to-one hop, an empty collection) agree on DuckDB and Postgres; the fuzzer generates nested groups (15% of to-many
  selections): 6,000 documents agree.
- **Aggregates onto the node; the generated classes deleted** (the same day). `pAggregate(where)` under a parent is
  the root aggregate's pipeline over `$this.p` (`GqlToPure.aggregatePipeline`, shared), one object: a table whose
  operators make one row (a whole-collection `aggregate` under row-keeping operators: `GraphEmission.oneRow`)
  serializes as that row. With it the companion's generated aggregate and fold classes and the `pAggregate()` members
  are gone from the model (`CompanionSynthesis` keeps the hierarchy root's `__typename()` and `as<Sub>()`), and so
  is the predicate that hid them (`ModelContext.isSynthetic`; the Cypher labels, the graph's edges, the GraphQL scope
  and the lineage scan read the model as on `main` again). Register rows S101 and S111 are removed (their
  differences are), S102, S114 and S115 rewritten; `Companion.java` became the fold vocabulary (`GqlFold.java`). The
  constructor fusion stays, its gate widened from "a generated class" to any constructed class, for the shape classes
  a modeler writes (`payroll() { ^Payroll(headcount = $this.employees->size(), total = ...) }`, with derived members
  over the fields; `GqlExecutionTest.aConstructedShapeClassFusesWithItsMembers`). A property named like the
  aggregate field is refused by the schema generator, as a groups field's twin always was.
  Measured: the corpus agrees on both lanes; 3,200 fuzzer documents agree (seeds 61 to 68).
- **Next.** The root forms (`PAggregate`, `PGroups`) as the node without a parent, finished with a serialize over
  the table (deleting the endpoint's Java arrangement of their rows); then relation-typed derived properties in the
  model (the modeler's `payroll()` as a table).

### 13.9 Temporal classes, 2026-10-21: every read names its dates

The open gap the trading probe named (§13.6): the schema offered a temporal class's root and every field reaching
one, and execution refused them for want of a date. Register row S119.

- **The schema.** A root over a temporal class (its list, `CAggregate`, `CGroups`) and a field reaching one take its
  dates as required arguments (`SchemaGenerator.withDates`): `businessDate: Date!` for a business-temporal class,
  `processingDate: DateTime!` for a processing-temporal one, both for a bitemporal one, the processing date first
  (Pure's argument order). A temporal object answers `businessDate` / `processingDate`, the dates it was read as of
  (Pure's generated members). No input object names a dated navigation (`ProductWhere` has no `listing`): a
  condition, a sort key or a group key has no place for a date (`reachesTemporal`). The engine's propagation of a
  temporal owner's dates to a temporal child is not offered: every read names its dates.
- **Dated lists (the same day).** A to-many navigation to a temporal class takes its clauses after its dates
  (`stock(businessDate: d, where: ..., orderBy: ..., limit: n)`), and its summaries take the dates too
  (`stockAggregate(businessDate: d)`, `stockGroups(businessDate: d, ...)`). The translator heads the composed body
  and the relation node with the dated navigation (`$this.stock(%d)`, the dot-spelled property call Pure parses it
  to); the resolver already windowed such a head. Two fixes: the Pure text parser reads a dated head as the
  composed node's navigation (`SpecParser.navigationHead`), and a composed body's dated head is the node's own
  temporal spec (`GraphEmission.datedHead`): its child's `businessDate` answered the root's date, or the version's
  start under a non-temporal root (the fuzzer's first campaign over dated lists, seven documents of 3,200).
- **The translation.** A root is `C.all(%bd)` or `C.all(%pd, %bd)` (`GqlToPure.extent`); a field is the tree node
  `listing(%bd)`, keyed by its response key (it was keyed `listing(2024-02-29)`, Pure's call spelling).
- **Found and fixed (wrong data, Pure trees too):** one milestoned property at two dates in one tree (`before:
  listing(businessDate: d1) after: listing(businessDate: d2)`) read the first date's version for both: the tree's
  scan keyed the dates by property, the first winning. `GraphEmission.graphChild` now windows a node with its own
  dates (`TemporalFrame.withSpec`).
- **The fixture.** The shop gains a business-temporal `Listing` (a product's price and channel as of a date: a
  version boundary on 2024-01-01, a February-only listing, a one-day version on the leap day, a listing ended in
  2020, a product never listed) and a business-temporal `Stock` (a product's stock per warehouse bin, to-many: a
  sell-out, a one-day leap-day bin, a bin held at zero); the reference reads it as of a date (`ReferenceModel.DatedToOne`,
  `Cls.datedRoots`), a version holding from its first day up to, not on, its last; the fuzzer asks dated roots and
  dated navigations at boundary dates, now and then the same field at two dates.
- **Measured.** 16 corpus cases (`temporal:` and three refusals) and 6 more (`temporal lists:`); 2,000 fuzzer
  documents, 341 of them dated, 3,200 more (seeds 71 to 78), and 4,000 with dated lists (seeds 91 to 100, 1,158 of
  them dated) agree; `GqlTemporalTest` (a bitemporal limit and a processing-temporal list: the
  schema's arguments and their order, a correction recorded later read at two processing dates, a list as of a
  processing date, a missing date refused by name).

### 13.10 A real model, 2026-10-21: the stress corpus's services asked through GraphQL

The hardening line's fixture is small by design; the user asked for realistic use cases too. The stress corpus is
the realistic model the repository has: a 7,600-element financial model (trading, positions, counterparties,
reference data, risk, settlement and fifty more domains), its mappings (unions, filters, inheritance with filtered
subclass sets, joins through sets of other mappings, enumeration mappings, embedded and otherwise sets, qualified
properties, temporal classes), and a seed with the corpus's adversarial rows, every service's answer computed by an
independent oracle (`scripts/corpus/oracle.py`). The new lane asks those services' questions as the GraphQL a client
would write, and judges each answer by the service's own expectation.

- **The twins** (`ServiceTwins`). A graph-fetch service (`C.all()->graphFetch(tree)->serialize(tree)`) is the list
  root `C` with the tree as its selection, a subtype entry an inline fragment; it runs as the service with its query
  replaced and is judged by the service's suite. A TDS service (`filter -> project -> [filter] -> [groupBy] -> sort
  -> [limit]` over to-one paths) is the list root with a `where` (comparisons, `AND`/`OR`/`NOT`, `in`, string
  operators, a Boolean property, presence), an `orderBy` (one key or several), a `limit`, a temporal root's dates,
  and the columns' paths merged into one selection; a qualified property's arguments are named by its parameters
  (`netExposureIn(fxRate: 0.79)`); `->count()`, `->isEmpty()` and `->isNotEmpty()` of a path read a collection's
  `pAggregate { count }` or an object's presence; a `groupBy` is the class's `Groups` root, its keys under `key`,
  `count` and the folds by name, sorted by key or fold. The answer flattens back to the service's columns by path and
  is judged by the oracle's rows (unordered, `EqualToJson`) and then by the order of the sort's keys.
- **The scope.** One schema cannot cover the model (88 simple names repeat across packages; the generator refuses
  a scope where two classes share a name, rightly): each twin's schema is generated for its classes closed under
  superclasses and the classes their properties reach (`ServiceTwins.closure`), which is how a real deployment
  scopes by data space.
- **What it found, in lite's core (the stress model reached shapes no smaller model had):**
  - An `exists` under an `exists` over a navigation ROUTED to a set of another mapping
    (`book[positions_Book]: @AgencylendingBaseBook` in the agencylending mapping, the set in the positions mapping):
    the routed row carries the class's properties and the route's key only, and the nested scope's association
    joined, and its inner `exists` correlated, on the row's own `DESK_ID`. 1,415 twins walled `UnfoldableRef:
    DESK_ID` (GraphQL's `where: { book: { desk: { ... } } }` is exactly that nested `exists`); so did the same Pure.
    The nested scope now widens a routed row for the keys its exists correlate on and its association joins read,
    the root's join-key widening applied to a nested scope (`StoreResolver.scopeMaterials`,
    `CorrelatedSubselects.nestedAssocMaterials`; `StackBuilder.demandBelow` reads a lone projection as the
    one-thread union `demandForKeys` already did).
  - A graph fetch of a derived property whose body reads scalars mapped through joins (`backConverted()` over
    `notional: @PricedTrade_Trade | TRADE.NOTIONAL`) without the scalars selected: the leaf demanded no join slot
    and walled. A derived leaf now demands the object's own scalars its body reads (`GraphEmission.derivedLeafPaths`);
    a navigation its body takes keeps its own route (demanding it joined a milestoned navigation at the root and
    moved the relational corpus's `testMilestonedClassAtRootQualifierWithBD` into the engine-order register: the
    first cut, narrowed before landing).
  - Both pinned by `RoutedNavigationIntegrationTest` (each fails without its fix).
- **What it confirmed by design.** GraphQL's comparisons are two-valued (register S112): `NOT` keeps an absent
  value, where the engine's SQL `<>` drops it, so the twin of `$x.p != v` asks for the value present as well. A graph
  fetch of an otherwise-embedded property reads the fallback set (the engine's `testOtherwiseEmbeddedMapping`) where
  the projection reads the inline columns, so `CB_Otherwise` is not twinned. Graph answers spell a DateTime without
  the `+0000` the service-test channel appends (`Fold`'s graph leaf), so a tree twin's expectation is read in that
  spelling.
- **Measured.** 445 graph-fetch services twinned: 436 agree with the oracle, and the 9 others fail in lite as their
  services do (provisioning over several stores, the service-test `+0000` channel, external-format data, a
  JSON-mapped property); `G2` (`all(%latest)`: GraphQL has no latest date literal) and `MU0` (its class is in an
  excluded file) are not twinned. 4,287 TDS services: 4,274 twinned, 4,261 agree in rows and order, and the 13
  others fail as their services do (the same causes, and a Float past its sixth digit); 13 are not twinned, each
  counted by reason in the lane's outputs (a computed column, a second project, `first()`, a dated read through a
  variable). The stress gate's own count is unchanged (4,705 of 4,736). The lane is `//core:stress_graphql` in gate
  10 (`//gates:stress`), held by two shrink-proof floors in `StressGraphqlTwinsTest`; it runs in about four and a
  half minutes.
