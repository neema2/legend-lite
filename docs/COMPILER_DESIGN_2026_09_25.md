# The compiler, designed from the language: how legend-lite should bind, type, evaluate and lower Pure

> **Corrected by `PLAN_AUDIT_2026_09_26.md` (2026-09-26).** §1 and §5 are wrong about the engine and the corpus: the engine's handler map is the whole namespace for engine input, and the corpus is Pure source that never touches it (audit §1 row 7). §3.3's element and syntax rules, §3.4's "bound twice is an error", §3.5's evaluator scope, §3.6's "Plan" (the MIR already is it) and its null-semantics sentence, and §4's dialect capability are corrected in audit §1 rows 4–6 and §2. The stages, the principles and the migration shape stand.

Written 2026-09-25. This is the design the untangle should have started from. It is written from
three vantage points at once — how Pure the language actually works, how legend-engine actually
compiles and plans it, and what a compiler that emits SQL has to hold at each stage — and it says,
for every stage, what data it takes, what data it produces, and what it is allowed to decide.
`REAL_PLAN_2026_09_25.md` is the order of work; this is what the work builds. The charter's rulings
stand.

Facts about this repo below are measured (see "What this rests on"); facts about upstream are
verified in the pinned checkouts where marked, and stated as belief where not.

## 0. The words

- **Declaration.** Upstream's statement that something exists: a class, an enum, a property, a
  function overload, a store, a mapping. A function declaration's identity is its full signature
  string (`meta::pure::functions::collection::map_T_MANY__Function_1__V_MANY_`); that string is the
  element's own name in Pure, and every overload is its own element. We hold it whole as
  `FunctionId`.
- **The world.** Everything loaded for one compilation: the declarations of the platform's
  modules and the user's, and each source section's import lines.
- **Spelling.** The text a person wrote for a name: `filter`, `Person`, `meta::pure::tds::project`.
- **Binding.** Replacing a spelling with a reference to a declaration, once, at one stage.
- **Overload.** One of several function declarations sharing a name; the type checker picks one
  by the arguments' types and multiplicities.
- **Multiplicity.** Pure's count on every value: `[1]`, `[0..1]`, `[*]`, `[m]` (a variable). It is
  part of the type, not a nullability flag, and SQL has to be told what to do with it.
- **Form.** A function whose result type depends on its arguments in a way its signature cannot
  express: `project`, `extend`, `groupBy`, `join` and their relatives. Upstream has a typing rule
  per such function, keyed by the function's id (verified: `Handlers.java` registers a return-type
  inference function beside each of these ids).
- **Lowering.** Turning a typed Pure program into a relational plan and then into a dialect's SQL.
- **Oracle.** What decides whether the compiler is right: the relational corpus judged against the
  real engine's outputs on DuckDB and H2, the conformance suites, byte parity with the parser, and
  the census that types upstream's whole world.

## 1. What the language is, and what we must not pretend it is

**Pure is a typed, functional, first-order-with-lambdas language over a metamodel.** Every value has
a type and a multiplicity. Functions are elements with signature-named identities. Overloading is
resolved by matching argument types and multiplicities against candidate signatures, most specific
wins, ties are errors. Generics have type parameters *and* multiplicity parameters (`map<T,V|m>`
returns `V[m]`). Function types are first class (`Function<{Person[1]->String[1]}>`). Classes have
properties, derived properties with bodies, constraints with bodies; associations add properties to
both ends. Milestoning adds implicit date parameters. Relations (`Relation<T>`) carry a column list
as a type argument and column specifications (`~name`) are typed against it.

**Name resolution in Pure is per source section.** A section's `import x::*;` lines widen bare-name
lookup over what is already loaded. Nothing is loaded by an import. What is loaded is decided by
the module manifests (`<name>.definition.json`, verified in the pinned tree), which name each
module's dependencies; the engine loads the closure.

**legend-engine adds a second front door.** Query text and model text written in the engine's
grammar are compiled by a different compiler (the one under `legend-engine-language-pure-compiler`)
that resolves a bare function name against its handler registry FIRST, then the graph. That
registry is keyed by bare name and maps each to signature ids, with a return-type inference rule
for the forms (verified: `Handlers.java`, 836 entries at 4.145.0, generated into
`engine-handlers.tsv`). So there are two resolution rules for a bare name, one per input language,
and a compiler that serves both must know which one it is applying.

**Plan generation is a Pure program.** Upstream's router and its `pureToSqlQuery` are Pure code run
by the Pure runtime at plan time; functions marked normalise-required are inlined by that runtime
before planning; static values (column lists, literals) are simply evaluated. That is why a SQL
compiler for Pure needs an *evaluator* over typed terms, not a table of foldable functions.

**SQL is not Pure.** A Pure `[0..1]` is a value that may be absent; SQL has NULL with three-valued
logic. Pure's `==` on empties, `isEmpty`, `if`, `in`, `contains`, aggregation over empties, and
sort with empties all have defined semantics that SQL only reproduces if the lowering says so
explicitly at each use. Relational operators (join kinds, window frames, grouping) map cleanly;
scalar semantics do not, and every dialect renders differently. The lowering therefore has two
halves that must be kept apart: a dialect-independent relational plan with Pure's semantics made
explicit, and a rendering per dialect that is data.

## 2. The principles

1. **Identity is the declaration.** After binding, the program never names a function, class or
   property by its spelling again. It holds the declaration. Everything downstream asks the
   declaration. (The identity pins exist only because this is not yet true.)
2. **Each stage owns one question and produces one immutable value.** Load owns "which files".
   Parse owns "what tree". Bind owns "which declaration". Type owns "which overload, what type".
   Evaluate owns "what does this reduce to at plan time". Lower owns "what SQL". No stage re-asks
   another's question.
3. **A failure is reported at the stage that owns it, with the source span.** An unknown name is a
   bind error at its span. An ambiguous overload is a type error naming the candidates. An
   unsupported function is a lowering error naming the declaration and the dialect. Nothing falls
   through silently to a later, vaguer error.
4. **Everything the platform knows about upstream is generated from the pinned checkouts and
   verified by a test that reads the same checkout.** Declarations, handler surface, dynafunction
   registry, dialect renderings' spelling. Nothing about upstream is typed by hand.
5. **Everything the platform decides for itself is a registration, in one registry, and the
   registry is total.** Which declarations it implements, how, where, and which it refuses and why.
6. **The oracle is the arbiter.** A change is right when the corpus and conformance rosters do not
   lose a row and the census's numbers do not grow. A change that needs a special case to keep the
   rosters is wrong, and stops.
7. **No tolerant modes.** A file that does not parse is a wall with a reason. A world that does
   not load is an error.

## 3. The stages and their data

```
   text ──parse──▶ Syntax tree ──bind──▶ Bound tree ──type──▶ Typed tree ──evaluate──▶ Typed tree'
                                  ▲                                                      │
   manifests ──load──▶ World ─────┘                                              lower ──▶ Plan ──render──▶ SQL
```

### 3.1 Load: the World

Input: a module name (for the corpus: `core_relational`; for a user program: its own module).
Output: an immutable `World`: for every file in the manifest closure, its parsed sections; a
`DeclarationTable` (every class, enum, association, function overload, store, mapping, profile,
measure — by identity); per section, its import group (the packages its imports name). Walls: a
file that does not parse is recorded with its reason and excluded; the count is pinned shrink-only.

Rules: the closure is read from the manifests, strictly; the platform's own declarations (the
catalog of natives, generated from upstream) are a module like any other; a declaration twice under
one identity is an error (upstream refuses it too), not a first-wins.

What this replaces: the hand lists of library and shape files, the prelude's membership lists, and
the prelude as a hand-built world. The prelude survives only as "what the platform's Java itself
needs at boot", which is small.

### 3.2 Parse: the Syntax tree

Input: text in one of two grammars (engine grammar; Pure source). Output: a tree of syntax nodes
with source spans. The parser knows keywords, operators and literals. It does not know functions:
`a + b` is a call spelled `plus`, `$x->filter(...)` is a call spelled `filter`, `Person.all()` is a
call spelled `getAll` on an element reference spelled `Person`. Every spelling is a `Name(text,
span)`, and the tree records which grammar produced it.

What is already true: the parser is at byte parity with upstream's and stays as is. What changes:
the syntax tree's call node stops carrying "candidate FQNs"; it carries only the spelling.

### 3.3 Bind: the Bound tree

Input: a Syntax tree, the World, the section's import group, the input grammar. Output: a Bound
tree where every reference to a *declared thing* is a declaration reference, and every function
call carries its **candidate set**: the function declarations the spelling may mean here.

The rules, exactly:

- **Element references** (`Person`, `my::Mapping`, an enum value's enumeration): resolved to the
  one declaration the spelling denotes in scope: qualified spellings by identity; bare ones by the
  section's package, then its imports, then Pure's core import group. Two matches is an error
  naming both. None is an error at the span. (Today `NameResolver` does this; it stays, producing
  references instead of spellings.)
- **Function calls**: the candidate set is every function declaration the spelling may denote:
  a qualified spelling denotes its own identities (all overloads at that name); a bare spelling
  denotes, for **Pure source**, the overloads at `<package>::<name>` for the section's package,
  its imports and the core import group, and for **engine input** those plus the handler surface's
  ids for that name. The candidate set is a set of declarations, ordered by tier (surface, then
  scope) only because a same-shape tie is broken deterministically. Empty is a bind error:
  "no function named X is declared here", with the span. (Today: `BareNames` produces spellings
  and the merge point looks each up; this becomes one lookup into the World's index of declared
  names by package, built once per World.)
- **Variables and lambda parameters**: bound to their binding occurrence (a symbol object), so
  renaming is never by name and capture is impossible by construction. (Today three inliners each
  alpha-rename by text.)
- **Property access (`$p.name`)**: NOT bound here. Member lookup is type-directed in Pure (`name`
  is a property of whatever `$p` turns out to be), so the binder leaves a `Member(receiver,
  Name)` node and the type checker binds it to the property declaration. This is the one place a
  spelling legitimately survives binding, and it survives only until the type checker has the
  receiver's type.
- **Special syntax** (`if`, `let`, `match`, `new`, `cast`, column specs, tree fetch): the parser
  produces distinct node kinds for the ones that are syntax, not functions (`let`, `new`, `cast`,
  `@Type`, `~col`, `#{...}#`); the ones that are functions in Pure (`if`, `match`, `filter`) are
  ordinary calls with candidate sets, and become forms only at typing.

Diagnostics: a bind error carries the span, the spelling, the scope searched (package, imports,
core group, surface) and the nearest declared names.

What this deletes when it lands: every identity pin that counts a function spelling (mints by
name, catalog lookups by name, the 20 `function()` compares); the `candidateFqns` string list on
the call node; the resolver's captured-native merge; the courtesy and hand lists around bare
names. The 175 element-name compares go when element references are bound the same way, which is
the same mechanism applied to classes, enums, stores and mappings.

### 3.4 Type: the Typed tree

Input: a Bound tree and the World. Output: a Typed tree where every node has a type and a
multiplicity, every call holds exactly one function declaration and its instantiated signature,
and every member access holds its property declaration.

**Inference.** Pure's typing is generic-instantiation with two kinds of variables (type,
multiplicity), subtyping on classes (`extends`), the top type `Any` and bottom `Nil`, function
types with their own parameter and return positions, and relation types carrying column lists. The
right mechanism is ordinary unification with constraints: a call's argument types unify against
the chosen candidate's parameter types, binding the candidate's variables; multiplicity variables
unify the same way over intervals; a lambda argument is typed *after* the parameters it flows into
are known (bidirectional: expected type flows into the lambda). Overload choice among the
candidate set is: keep the candidates whose parameters accept the arguments; among them the most
specific (each parameter at least as specific, one strictly); none is an error naming the
candidates and the argument types; more than one is an error naming the tie. This is what
upstream's compiler does; the census's 164 kernel failures are exactly the places where our
kernel binds a variable twice, leaves a multiplicity unbound, or cannot unify a function type —
each is a missing rule in this one place, not a missing checker.

**Forms.** For a declaration the platform registers a typing rule for (`project`, `extend`,
`groupBy`, `join`, column-spec-taking functions), the rule is a function from the argument types
to the result type, registered *against the declaration id* in the registry, exactly as upstream
registers return-type inference against the handler id. The type checker dispatches to it because
the chosen declaration has a rule, never because a spelling matched. Everything else about the
call — argument typing, overload choice, error reporting — is the generic path. (Today: about
forty checkers dispatched by spelling, each owning its argument typing too. Each becomes a typing
rule row, and the ones that exist only because the kernel could not type the call retire as the
kernel gains the rule.)

**Members.** `Member(receiver, Name)` is bound here to the property declaration on the receiver's
class (or a derived property, or an association end, or a milestoned property with its implicit
date arguments made explicit). Unknown member is a type error naming the class.

Output invariants, tested: no node without a type; no call without exactly one declaration; no
member without its property declaration; no spelling anywhere.

### 3.5 Evaluate: the Typed tree, reduced

Input: a Typed tree. Output: a Typed tree in which everything that can be decided at plan time has
been decided, by ONE evaluator over typed terms. This is the stage upstream performs by running
Pure; we perform it by an evaluator that understands typed terms and the registry.

What the evaluator does, each a rule keyed by declaration:

- **Inlining.** A call to a declaration with a body and a `Body` row is replaced by its body with
  the arguments substituted (symbols, not names, so no renaming). Normalise-required functions
  always; other user functions by the policy the lowering needs (today: always, because SQL has no
  functions to call). A call to a declaration with a `Refused` row is an error here, naming the
  reason. **Sharing:** an expansion is computed once per (declaration, argument terms) and shared;
  a budget on expansion depth and size turns a runaway into a diagnostic naming the declaration.
- **Plan-time evaluation.** A term whose free variables are all bound to static values reduces:
  literals, collections, the column list of a static relation, string and arithmetic natives on
  literals, `if` on a static condition. The rules for natives are the registry's `Intrinsic` rows'
  evaluators, one per declaration, generated where upstream's semantics are testable (the
  conformance suites are the oracle for these).
- **Desugars.** `validate`, constraints, milestoning date propagation, legacy TDS spellings:
  rewrites over typed terms, keyed by the declaration they rewrite, registered as rows.

What this deletes: the four inliners, the fold table, the alpha-renamers, the statement-level
program inliner, the validate desugar as a separate pass, and the class of bugs where two of them
disagree or one bypasses another's wall.

### 3.6 Lower: Plan, then SQL

Input: a reduced Typed tree with a root that is a relation-valued or scalar-valued expression.
Output: a **Plan**, dialect-independent, and then SQL text per dialect.

**The Plan is relational algebra with Pure's semantics explicit.** Operators: scan (a store
table or a view), filter, project, extend (computed columns), join (kinds, on a predicate),
group-by with aggregates, window, sort, limit, union, distinct, correlated subquery, values. Scalar
expressions: column, literal, call to a scalar declaration, case, cast, and the explicit
null-semantics nodes: `coalesce`, `is null`, `nullif`. The lowering of a typed call is a **rule
registered against the declaration id and its position** (scalar, aggregate, window, relation
operator) — the registry's `Intrinsic` row *is* the rule. Multiplicity becomes nullability here,
and every Pure semantic that SQL does not share is made explicit at this point: `isEmpty` on a
`[0..1]` is `is null`; `==` between two `[0..1]` values is the Pure equality (empty equals empty),
rendered as `is not distinct from`; aggregation over an empty relation yields Pure's result, not
SQL's; sort places empties where Pure does.

**Rendering is data.** Each dialect (DuckDB, H2, Postgres, …) renders each Plan node and each
scalar rule. Where upstream defines the spelling (`dynaFnToSql` per dialect), the rendering is
generated from it and verified; where it does not, ours is registered with its reason. A rule that
a dialect lacks is a lowering error naming the declaration and the dialect, before any SQL is
emitted.

What this deletes: lowering rules keyed by bare-name registrations (the `nativeKeysAt` tables),
the string-keyed rule maps, the separate translator's dynafunction spellings; and the registry
becomes the one place that says "what does the platform do for this declaration".

## 4. The one registry

The implementation table becomes total and authoritative: one row per declaration in the World's
declaration table, each row one of:

- `Intrinsic(typingRule?, evaluator?, lowering per position, dialect renderings)` — the platform
  implements it;
- `Form(typingRule, lowering)` — a form, the same shape with a required typing rule;
- `Body` — upstream's body is the implementation (inlined by the evaluator);
- `Refused(reason, message)` — the platform will not run it, and says why at the point of use;
- `Unimplemented` — declared upstream, no decision yet; a use is a lowering error naming it.

Everything that today holds ownership by another means — the platform-owned list, the walled
bodies, the lite surface and internal sets, the legacy TDS vocabulary, the subsumed registry, the
handler surface's declared column, the members a family implements, the dynafunction column, the
claims ledger — is a *source of rows* for this table at build time, or is deleted. The table is
generated, its kind counts pinned exactly, and its rows verified against the pinned upstream
declarations by identity.

## 5. Diagnostics and the two languages

Every stage returns diagnostics with spans, never exceptions with strings. A compile of a corpus
test that fails at bind, type, evaluate or lower is attributed to the stage in the roster, so the
census can be bucketed by stage and cause without parsing messages (today `reasonClass` greps
messages).

The binder is told which grammar produced the tree and applies that grammar's bare-name rule.
Engine input gets the handler surface; Pure source does not. The corpus is engine input; the
platform's modules and the standard library are Pure source; the prelude, once it is only what
boots the platform, is Pure source.

## 6. What this rests on (measured), and what is judgement

Measured in this repo: the tree carries spellings (`AppliedFunction.function` is a String, its
candidates are Strings); four inliners (statement-level, normalise-required, user-call, the
folder's two, now sharing one callee selection); about forty forms dispatched by spelling; the
identity pins (207 accessor compares of which 20 on functions and 175 on element names, 143 mints
by name, 170 catalog lookups by name, 21 form dispatches by name, 90 local compares); the lowering
rule maps keyed by signature strings built from 158 bare registrations; the implementation table's
kinds (Intrinsic 664, Form 217, Refused 20, Body 2194, Unimplemented 71); the manifest closure
(27 modules, 1,772 files, 32 walls, 164 kernel failures among 1,447); the corpus lanes' curves
(receipts). Verified upstream: manifests as the loading rule; the handler registry keyed by bare
name with return-type inference per id; function identity as the signature string.

Judgement: that unification with two kinds of variables covers the 164 (the homework for step D
classifies them first); that one evaluator can replace the four inliners without a roster loss
(step C measures which fired where first); that the Plan's operator set above is sufficient for
the corpus (the existing lowering's operators are the evidence, to be enumerated).

## 7. Migration: the new front end grows inside the old one

Nothing is rewritten. Each stage above is built beside its current counterpart, consumers move one
at a time, and every move is gated by the oracle. In the plan's order:

- **A1.** The binder produces candidate sets of declarations; the merge point reads the World's
  declaration table for them. Delete the platform-owned list and the stereotype check. The drift's
  33 lookups disappear here, because the candidate set is one index lookup.
- **A2.** The typed call holds its declaration; lowering rules are registered and looked up by
  declaration id. Delete the bare registration index.
- **A3.** Compiler-minted calls spell declarations. Delete mints by name.
- **A4 (new, the second half of A).** Element references and member accesses are bound to
  declarations; the 175 element-name compares retire.
- **B.** Forms become typing-rule rows dispatched by the chosen declaration.
- **C.** One evaluator replaces the four inliners and the fold table, with sharing and a budget.
- **D.** The kernel's rules, driven by the 164, retiring checkers as it grows.
- **E.** One registry: every other ownership mechanism becomes a source of rows or is deleted.
- **F.** Load by manifest, walls to zero, strict.
- **G.** The stages become packages; the three files at the size guard split along them.

Each step: a homework note that re-measures what it will change; the probe or census before; the
switch; the oracle; the deletion in the same commit; the record; the push. A step that needs a
special case to keep the rosters is wrong.

## 8. What stays as it is

The parser and its parity test. The judges and the rosters. The SQL renderings' text where the
corpus proved them (they move into the registry as data, unchanged). The conformance suites.
DataCube. The wasm build of the planner (which becomes smaller: it carries the stages, not the
pins).
