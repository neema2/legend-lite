# M8c — the canon over the product columns (B8, MV4): a design question for the user (2026-11-01)

The plan's §7 M8 names, after M8a (the host judgment as data) and M8b (the database verdict row self-contained), **M8c:
the canon over the product columns** (B8; R10 "the canon computed over the product plan's columns, fully"; MV4
"verdicts and messages carrying a canon; untracked evaluations"). This note records what the code says before the
step is built, because one point needs a ruling (R21: posted, not decided by a session).

## What the canon is today

- The verdict lanes' sides lower on the IDENTITY LANE: `BodyPlanner.lowererFor(..., identity=true)` gives the Lowerer
  `ClassLayouts.layoutOf(ctx, t, true)` -- the object layout (declared properties, `__id`, `__type`) plus a synthetic
  `__canon` field -- and `withInstanceIds` (identities minted per construction site). The product lane lowers with
  `objectLayoutOf` (no `__canon`) and `withObjectIdentity` (`docs/OBJECT_IDENTITY_2026_10_20.md`).
- `__canon` is filled AT THE CONSTRUCTION SITE (`Lowerer` lines ~3449 and ~3507, `ConstructionCanon.bind`,
  `CanonicalRenderSql.constructionCanon`): a keyed class's key tree with each key value's canon read off the value
  itself -- a nested constructed instance contributes its OWN `__canon` ("built before its parent, so recursive
  polymorphic shapes stay finite"), a JSON-carried one the `__canon` in its object, a scalar its literal spelling; a
  keyless class its identity.
- The verdict wrap (`CanonicalRenderSql.wrapWithCanon`) reads a class-kind side's canon by one of three arms: F10
  proper -- the side's struct carries `__canon`: the canon IS it; X5 -- a keyed instance without `__canon`:
  `instanceCanon` renders the key tree over the struct's fields in SQL (static expansion from the model's key tree:
  `taggedLeaf(element, type, k.nested())`); F13 -- a keyless instance: `identityCanon` (`_type`, `_id`).

## What "over the product columns" can and cannot do

The product's object layout carries `__id`, `__type` and the declared properties: everything a canon needs EXCEPT the
nested canon of a value in a slot whose declared class is keyless or Any (a polymorphic slot). There the value's OWN
class decides its keys (`__type`), and the static expansion must branch on `__type` over the slot's declared subtree --
finite for a non-recursive model (M8a's `HostJudgment.Keys` collects exactly that set) -- but a RECURSIVE class
(`DynaFunction.parameters : RelationalOperationElement[*]` holding `DynaFunction`s; `x::Call.args : x::Node[*]` holding
`x::Call`s) has no finite static expansion: SQL has no recursive scalar expression (DuckDB macros do not recurse;
lambdas cannot), and a depth bound is a guess. The construction-site canon sidesteps this because each nested LITERAL
instance's canon was computed when its struct was built, bottom-up in the query text. A store-read or JSON-carried
nested instance has no `__canon` either way (today: its identity).

So "the canon over the product columns, fully" is achievable for every non-recursive shape, and for a recursive shape
only by (a) keeping the construction-site canon for recursive classes alone (a named residue, the layout difference
between the lanes kept for them), (b) a depth bound taken from the statement's own construction tree (sound for
literal trees, which is where `__canon` carries today; not for store reads, where there is no canon today either), or
(c) a dialect-specific recursive form (DuckDB `WITH RECURSIVE` over an exploded tree) -- work, per dialect.

## Who rides it, measured

On the DuckDB database pass (`sql-census`): 3,030 `assertEquals`, 756 `assertSameElements`, 6 `assertEq` adjudicated
in the database; the class-kind sides among them are the `toPostgresModel` and `debugPrint` suites (nested
`Node[*]`/`RelationalOperationElement[*]` trees: the recursive case) and the keyed metamodel pairs. Dropping `__canon`
without (a)-(c) would move those outcomes from held to `unjudged` (the X5 arm marks an unclaimable nested leaf and the
verdict declines). M8c moves no body by itself: the bodies the class-kind arms gate (`databasePlansWithoutReading`: the
grid pair, the rendered text, class-kind `assertEq`, 102 on the database pass) move at M10c.

## The question

Which of (a), (b), (c) -- or "the construction canon stays on the identity lane; the product lane is unchanged; MV4 is
withdrawn" -- does the user rule? Until then M8c is OPEN; M10a (the system database on the test road: 392 system-read
bodies and the mixed ones) proceeds, as it depends on nothing here (R23).
