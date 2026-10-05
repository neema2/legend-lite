# The project contract

Every project in `projects/<name>/` must satisfy all of this. `bazel test //projects:tests`
enforces the parts that can be checked mechanically (`projects/BUILD.bazel`).

## Layout

    projects/<name>/model.pure      ###Pure    -- classes, enums, profiles, functions
    projects/<name>/store.pure      ###Relational -- the Database
    projects/<name>/mapping.pure    ###Mapping -- the Mapping
    projects/<name>/MANIFEST.md     what this project exports, for downstream projects

Split by section, one section per file, each file starting with its `###` header. A file
without a header inherits the previous file's section and fails in a way that names neither
file.

## Naming, which is a hard contract

The package root is the project name with `-` replaced by `_`:

    core-types      ->  core_types::
    trade-capture   ->  trade_capture::

Fixed element names, so a downstream project can refer to them without reading this project:

    <pkg>::Store      the Database          (if the project exports a store)
    <pkg>::Mapping    the Mapping           (if the project exports a mapping)

Everything else -- class names, table names, join names, filter names, set ids -- is yours,
but must be PREFIXED so it cannot collide with another project:

    tables      <PFX>_TABLE_NAME       e.g. CT_CURRENCY, TC_TRADE
    joins       <Pfx>_JoinName         e.g. Ct_CurrencyCountry
    filters     <Pfx>FilterName        e.g. CtActiveRows
    set ids     <pfx>SetName           e.g. ctCurrency

where `<PFX>` is a 2-4 letter abbreviation of the project name. Set ids and filter names are
GLOBAL namespaces in Legend: one collision fails the entire graph at compile with a message
that names the id and not the project.

## Dependencies

Refer to a dependency's elements directly -- there is no import statement in Legend:

    Class mine::X extends dep_pkg::Base { ... }          // model dependency
    Database mine::Store ( include dep_pkg::Store ... )  // store dependency
    Mapping mine::Mapping ( include dep_pkg::Mapping )   // mapping dependency
    Association mine::L { a: dep_pkg::Base[0..1]; b: mine::X[*]; }

All ten cross-project forms are verified in `scripts/corpus/probe_project_deps.py`.

You may ONLY refer to projects listed as your dependencies: your row of `PROJECT_DEPS` in
`projects/BUILD.bazel` (your MANIFEST.md's opening paragraph names them too, for a reader;
no test reads Markdown). Referring to anything else is the
undeclared-dependency defect this graph exists to detect, and `//projects:<name>_test` will
catch it by compiling your project with its declared closure and nothing more.

## Data

Do NOT write a `###Data` element or a Runtime. These projects are compiled, not executed;
the executable corpus is `core/src/test/resources/stress/`. Keep tables declared and unseeded.

## MANIFEST.md

One markdown table, so a downstream project can be written without reading your source:

    | element | kind | note |
    | --- | --- | --- |
    | core_types::Currency | enum | ISO 4217, 12 values |
    | core_types::Store | store | tables CT_CURRENCY, CT_COUNTRY |

## Verify

    bazel test //projects:<name>_test     # alone, with exactly the declared closure
    bazel test //projects:tests           # every project, the graph together, the file checks

Each must pass. That is the whole acceptance test. (`scripts/projects/check.py` compiles with
legend-engine instead and also checks each project's size band; it stays until the script
review, P7-01.)

## Things the first wave of projects learned the hard way

* A store `Filter` will not take a BOOLEAN LITERAL. `Filter XActive(T.IS_ACTIVE = true)`
  fails with `Unexpected token 'true'`. Use a null test or a string comparison instead:
  `Filter XOpen(T.CLOSE_DATE is null)`.
* If a project names its set ids explicitly — `core_party::LegalEntity[cpLegalEntity]` — then
  the DEFAULT id (`core_party_LegalEntity`) does not exist, and any downstream
  `extends [...]` or cross-project `AssociationMapping` must name the explicit one. Read the
  dependency's MANIFEST for the ids rather than guessing.
* An `AssociationMapping` end must name `[sourceSetId, targetSetId]` whenever either side has
  an explicit set id. The bare `prop: [db]@Join` form only resolves against default ids.
* `/` in Pure widens to Float even when both operands are Integer, so a derived property that
  divides must be declared `Float[1]`.
* `Integer * Float` types as `Number`, and `Number` is not a subtype of `Float`, so a derived
  property multiplying them must write `$this.count->toFloat() * $this.rate`. Note this is
  NOT symmetrical with division: `Integer / Integer` widens to `Float` on its own.
* Keep a join condition on ONE line. A wrapped one does compile -- I checked, and an earlier
  version of this contract said otherwise, which was wrong. The one-line rule is a house
  style here because the CORPUS's own reader (scripts/corpus/model.py) refuses a wrapped
  join rather than silently dropping it, and these projects are meant to stay readable by it.
* `||` compiles. One project reported it failing; the failure there was something else in the
  same expression, so if you hit it, narrow it before believing the operator is at fault.
* A `~filter` reached through a JOIN needs a database pointer on BOTH sides:
  `~filter [db]@MyJoin | [db]MyFilterRows`. Written `[db]@MyJoin | MyFilterRows` it is a parse
  error naming neither the filter nor the project.
* A property declared on a SUBCLASS cannot be mapped on the parent's set. Give the subclass
  its own set (`Sub[mySub] extends [myParent]`) and map the property there. The error names
  the property, not the set it was wrongly written in.
* A `~distinct` set needs an explicit `~primaryKey` naming the column you are deduplicating
  ON. Keyed on the table's own key it collapses nothing, and nothing says so.
* An association end must NOT also be declared as a property in the class body. Declaring it
  in both places is a duplicate-property failure. Two projects wrote their first draft that
  way, having copied a project that maps its edges as PLAIN PROPERTIES over joins rather than
  as associations -- both styles are valid and they cannot be mixed on one property.
* A class CONSTRAINT can only compare `[1]` properties. A `>= 0` over a `[0..1]` does not
  type-check; express the "is it set" rule as a derived property using `->isEmpty()` instead.
* A list literal element must be PARENTHESISED if it is an infix expression.
  `[0.0, $this.a - $this.b]->max()` is a parse error; `[0.0, ($this.a - $this.b)]->max()` is
  fine, and so is `[0.0, $this.a]->max()`. The message is `Unexpected token` with a stack in
  `expressionsArray`, naming neither the class nor the property.
* Do not use the SQL type `REAL` — it is accepted by the grammar and cannot be read at
  execution (docs/UPSTREAM_FINDINGS.md F53). Use `FLOAT` or `DOUBLE`.
