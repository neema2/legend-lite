# Object identity in the product (2026-10-20)

**Status: steps 1 to 3 and 6 landed 2026-10-20 (§5): the struct fallback is deleted; where identity is not tracked, reading it raises by name (§4).** The user's ruling of 2026-10-20: "lets fix identity
properly that's a huge product fix/win". This document says what identity is in Pure, what the product did before
(measured), and how the product gives every object its identity on every lane.

## 1. Pure's rule

- Every evaluation of `^C(...)` or `^$x(...)` makes a **new object**. `let a = ^C(...)` names one object; `$a`
  read twice is that one object twice.
- `eq(a, b)` is **identity**, always.
- `equal(a, b)` / `==`: a class with `<<equality.Key>>` properties compares **its keys alone**; a class without keys
  compares **identity**.
- Everything built on `equal` follows it: `contains`, `in`, `removeDuplicates`, `distinct`, `indexOf`, `union`,
  `intersection`, `difference` on objects.

## 2. What the product did (measured on DuckDB, the plan route, 2026-10-20), and does since steps 1 and 2

| Expression | Pure | Before | Since |
|---|---|---|---|
| `let a = ^P(n=1); let b = ^P(n=1); eq($a, $b)` | false | **true** | false |
| `let a = ^P(n=1); let b = ^P(n=1); $a == $b` (no keys) | false | **true** | false |
| `[1, 2]->map(i \| ^P(n=1))->removeDuplicates()->size()` | 2 | **1** | 2 |
| `let a = ^P(n=1); [$a, $a, ^P(n=1)]->removeDuplicates()->size()` | 2 | **1** | 2 |
| `let a = ^P(n=1); [^P(n=1)]->contains($a)` | false | **true** | false |
| `[^Side(id='a', n=1), ^Side(id='a', n=2)]->removeDuplicates()->size()` (key `id`) | 1 | **2** | 1 |
| `[^Side(id='a', n=1)]->contains(^Side(id='a', n=2))` | true | true | true |
| `^Side(id='a', n=1) == ^Side(id='a', n=2)` | true | true (fixed earlier the same day: `withInstanceKeys` armed on the plan path) | true |
| `mk() == mk()` (`mk` builds `^P(n=1)`) | false | **true** | false |
| `let a = ^P(n=1); [1, 2]->map(i \| $a)->removeDuplicates()->size()` | 1 | 1 (2 with a naive position) | 1 |
| `[1, 2]->map(i \| let a = ^P(n=$i); $a == $a)` | [true, true] | [true, true] (structs) | [true, true] |
| `let f = {\| ^P(n=1)}; $f->eval() == $f->eval()` | false | **true** | false |

All 23 of `ObjectIdentityTest`'s cases are Pure's answers on DuckDB and SQLite. H2 holds no struct (its PCT lane runs
the relation suite alone), so an object is no value there.

Why: an object is a struct of its properties and nothing else. The identity machinery exists
(`ClassLayouts.SYNTHETIC_ID`, `InstanceEquality`, `exec.InstanceIds`) but only the legacy executor's assertion judge
turns it on, and where it is off the equality arms **fall back to comparing the structs** (`InstanceEquality`: "value-lane
keyless shapes keep the generic rule"). That fallback is the bug (AGENTS.md invariant 4).

Even where it is on, the id is minted **per construction site, never per evaluation** (`InstanceIds`): every object one
`^P(...)` builds inside a `map` gets the same id, so they count as one object. Channel B passes the PCT's identity tests
only because those tests build each object at its own `let`.

## 3. The design

**3.1 Every object carries its identity.** Every class struct carries `__id` (and `__type`, its own class) on every lane:
the identity layouts (`ClassLayouts.layoutOf(ctx, t, true)`) become the only layouts. `__canon` stays where it is used.
The plan path, the legacy executor and the judges lower with the same layouts.

**3.2 The id is minted where the object is built, once per evaluation** (landed 2026-10-20). Measured first: a site
per tree NODE (`InstanceIds`, the first sketch) is not an evaluation. A let in a lambda's body is spliced into its uses
before typing (`SourceSubst.inlineLets`), so `$a` read twice is typed as two constructions; a callee body is one set of
nodes for every call, so `mk() == mk()` was true; and a let built outside a map but read inside it would take the
map's element positions. So each construction carries its evaluation as data (`compiler/spec/typed/Evaluation`):

- **origin**: the source node the typer read it from (`NewChecker`): every copy of a spliced let has the one origin.
- **path**: the instantiations around it, set by the inliner (G½) where its walk reaches the construction
  (`UserCallInliner.reached`): each call inlined, each lambda applied (`eval`, a map over exactly one value), each
  element of a spelled list unrolled is one more. A let's value is reduced, so stamped, where it is bound, before it is
  spliced: every use reads the one object. Stamps are never redone.
- **a lambda's body is evaluated where the lambda is used**: inside a lambda the path counts from the lambda, and the
  inliner's last pass (`UserCallInliner.finish`) completes it where each lambda stands in the reduced tree, each a use
  of its own. So a let-bound lambda mapped twice, or a callee whose map is inlined twice, gives each use its own
  objects; a lambda applied by `eval` is completed at the application.
- **iterations**: the element parameters of the maps the construction is evaluated in, outermost first, recorded by
  that last pass. The lowering (`Lowerer.objectId`, `Lowerer.iterating`) gives a map's mapper the element's 1-based
  position as a second parameter, which every dialect's `list_transform` takes (DuckDB's own; Trino's `zip_with` over
  the positions, ClickHouse's `arrayEnumerate`, Postgres's `WITH ORDINALITY`, MariaDB's, SQLite's and MongoDB's
  forms), and reads it into the id: `'o3#2'`, nested `'o3#2.1'`.

The **site** (origin and complete path) is a value: two runs of the inliner over the same statements (the statement
executor inlines each statement with its let prefix) give a let's construction the same site, so channel B's verdict
sides agree. The id is deterministic, numbered in lowering order (`ObjectSites`, `exec.InstanceIds`): a value the SQL
reads twice (a spliced `let`, a subquery written twice) has the same id in both readings, which a database-generated
`uuid()` would not give.

**3.3 Every operation built on equality reads it.** `InstanceEquality` owns the relation and loses its fallback:
`eq` compares `__id`; `equal`/`==` compares a keyed class's keys and a keyless class's `__id`; a static classifier
mismatch is false. `contains`, `in`, `removeDuplicates`, `distinct`, `indexOf`, `union`, `intersection` and `difference`
on objects go through it (today `removeDuplicates` and `distinct` compare whole structs). A shape it cannot claim is
refused by name, never compared as a struct.

**3.4 No output shows the synthetic fields.** The text carrier already skips them (`LiteralSpelling.synthetic`); the JSON,
CSV and TDS writers and the typed rows must too (a gate: every output of an object, every lane).

## 4. Where the product's identity is not Pure's (named; register row S104)

Where the product does not track an object's evaluations it mints **no id** for it (`Lowerer.objectId`, the plan path),
and **reading the identity of such an object raises by name** in the database (`CanonicalRenderSql.trackedId`: every
identity comparison, `toString`'s `@_` name, a key canon's JSON-carried object): "an object's identity is read where
the product does not track it". Its values read as ever. Never an answer from an id shared by every row, iteration or
copy (2026-10-20, the user's "fix all those": before, cases 2 to 4 answered silently).

1. **An object built per row of a relation** (`$rel->map(r | ^P(name = $r.name))`): a row has no position that survives
   the relation being read twice. Refused by name where the id is minted ("an object built for each element of a map
   over $r that does not lower to a list ... has no identity of its own yet"). Proposal for later: the row's position in
   the relation's own order, an identity comparison across two readings of an unordered relation refused by name.
2. **Objects the resolver builds from a store's rows** (an embedded class's object, the pairs of a zip over rows, a
   construction re-expressed over a row): built after the inliner, one construction for every row, so untracked
   (`Evaluation.complete()` false). legend-engine gives each row's object the mapping's primary key
   (`getHiddenPayload().pk`): a later phase, measured against the engine first. A class's objects read whole from a
   store (`Person.all()`) travel as JSON without an id on the plan route; `removeDuplicates` over them is the store's
   own `DISTINCT` (the resolver's, not this program's).
3. **An object built in a lambda of a fold the database runs** (`UserCallInliner.finish`: the reducer and the
   map-reduce transform): one evaluation for every iteration, since no position is read for a fold, so untracked. A
   fold over a spelled list unrolls (each element its own evaluation). Found on the way, not identity's: a fold the
   database runs whose accumulator is a list of objects built from other elements does not lower on DuckDB (its reduce
   takes an accumulator of the elements' own type: "The initial value type must be the same as the list child type",
   and the map-reduce form's `list_concat(list, element)`); it fails loudly, a lowering defect of its own.
4. **A lambda body's let** is spliced into its uses before typing (`SourceSubst.inlineLets`), so each use is typed as
   its own copy. The copies are one evaluation where they can be (2026-10-20): a construction's copies share its origin
   and path; a call's copies share its instantiation (`TypedUserCall.origin`, the source node the typer read it from;
   `UserCallInliner.instantiated(call, ...)` numbers a call once per source node at a point of its frame), so
   `[1, 2]->map(i | let a = mk(); [$a, $a])` has two objects and `$a == $a` is true, as in Pure. Where they cannot be,
   untracked (`SourceSubst.untrackedLetNodes`, marked where the typer folds the body: `Overloads.typeLambda`,
   `LambdaBodies.synthBody`): a let read inside a nested lambda (a use there is evaluated per the inner lambda's
   elements), and a let read more than once whose value holds a lambda (each copy applies it anew). The proper form is
   the let kept to the inliner, reduced once where it is bound (as a query's lets are) with its objects completed in
   the frame they were reached in; it needs the typer to keep a parameterized lambda's lets as statements, which some
   forty checkers that read a lambda's single body do not take yet. (A `NormalizeRequired` function's body is folded the
   same way at typing; only the platform's prelude declares one, and none of its ten has a let.)

## 5. Order of work

1. **Landed 2026-10-20.** The layouts carry `__id`/`__type` on every lane; outputs skip them (`MixedEncoding.declaredOnly`).
2. **Landed 2026-10-20.** The id per evaluation (§3.2), and `removeDuplicates` on objects through `InstanceEquality`.
   Found on the way: a map whose mapper's value is a list (a many-valued property, an inner map or filter) did not
   flatten (`[1, 2]->map(i | [3, 4]->map(j | $i + $j))` had two elements); it does (`ListEncodings.map`).
3. **Landed 2026-10-20.** `InstanceEquality` owns `distinct` (so `union`, which the typer desugars to it),
   `isDistinct`, the list `indexOf` and `==` over collections of objects, beside `eq`, `equal`, `contains`, `in` and
   `removeDuplicates`; a type value (a model element) is no object here: it travels as its path and compares by it
   (S82). **The struct fallback is deleted**: a claimed shape it cannot lower is refused by name. Measured first, with
   the fallback made to throw: the PCT on DuckDB and channel B never reached it; the statement executor's value lane
   did (it lowered with the plain layout, so a keyless object had no id), and so did three of its suites' cases that
   asserted the struct answers (`StructValueTest`, `TypeInferenceIntegrationTest`: now Pure's, and the test model's
   `CO_Firm` keyed as Pure's own). That lane now lowers with the object layout and its env's ids, and its scalar root
   shows the declared properties alone (`Lowerer.withDeclaredEgress`).
4. Tests: **landed 2026-10-20**, `ObjectIdentityTest` (§2's table and more, on DuckDB and SQLite, through the plan);
   channel B's identity tests on the plan route with step 10 of the plan path program.
5. Register rows: **S104** (§4's four, 2026-10-20).
6. **Landed 2026-10-20** (the user: "fix all those"): §4's cases 2 to 4 refuse by name where they answered silently,
   and a lambda body's let read at one level is one evaluation (§4.4); `ObjectIdentityTest`'s
   `anUntrackedObjectsIdentityIsRefusedByName` and its let cases.

Channel A: the reference interpreter's objects never cross the wire as objects, so the adapter binds each captured
object once by a let at the front of the text (`pct_adapter.pure`, `objectLets`), and the product's identity judges
the rest (testEqVarIdentity, testEqNonPrimitive and their siblings pass on DuckDB since 2026-10-20).
