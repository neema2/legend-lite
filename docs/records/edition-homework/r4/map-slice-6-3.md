# Slice 6.3 — the map (read-only research, 2026-11-26, tree /home/user/ll-62 at 818c3a878)

Paths are relative to /home/user/ll-62. Line numbers are current at that HEAD. Nothing was built or run. Where I
say "did not trace", I did not.

Spec read: AGENTS.md "The equality rule — strict typing" (items: different types never compare; the literal
rule; inside functions and mapping code too, with the generic-body gap "to close with step 6 (register S127)";
run-time types carry their type; classes: run-time class, then keys; doors carry upstream's looseness), the
invariants on the Lowerer (2: no type inference, no model lookups) and the fold reading ("the compiler may fold
constants and facts fixed by the types, never row data"). The design's 5b-3 text is
`docs/records/edition-homework/b3-step5bc-design.md:211-217` (5b-4 at :219-224). The plan row is
`docs/records/edition-plan-to-finish-stage-b-2026-11-25.md:201` (there is no "GENERIC_BODY" in that record; the
word lives only in the code). D5 is `/home/user/legend-lite/docs/records/edition-homework/open-decisions-2026-11-26.md:113-123`
(that file is on `/home/user/legend-lite`, not in ll-62).

Upstream's meaning, READ (not recalled): legend-pure's interpreted `EqualityUtilities.equal`
(the pinned 5.99.0 source in the Bazel repo cache,
`.../legend-pure-runtime-java-engine-interpreted/.../EqualityUtilities.java:92-134`): same object -> true; two
primitives -> `eq` (which compares the primitive types, :69-75); `if (leftType != getClassifier(right)) return
false;` (:107); then the keys of the LEFT RUNTIME classifier (`_Class.getEqualityKeyProperties(leftType)`, :122),
each compared by `equal`; a class with no keys -> false. So: class first, by the run-time classifier, and the key
set is the run-time class's. Pair is a class (keys first/second), so `pair(1, 1.0) == pair(1, 1)` is false there.

---

## A. Every code site involved

### A1. The type rule: `compiler/spec/EqualityClasses.java` (129 lines)

- `crossing(...)` :77-96 — the one entry. It reads the two sides' types, maps each through `of`, and marks only
  when both are known and differ; two classes are exempt when `related` (:92-94).
  ```java
  Optional<Type> lc = of(l);
  Optional<Type> rc = of(r);
  if (lc.isEmpty() || rc.isEmpty() || lc.get().equals(rc.get())) {
      return Optional.empty();
  }
  if (l instanceof Type.ClassType lt && r instanceof Type.ClassType rt && related(lt.fqn(), rt.fqn(), ctx)) {
  ```
  For `contains`/`in`/`indexOf` the collection side's type is the element type (a `[*]` side's `info().type()`).
- `related(...)` :100-106 — subtype either way, or a shared subclass (`ctx.subtree`).
- `of(Type)` :109-128 — every generic is run time:
  ```java
  case Type.TypeVar ignored -> Optional.empty();
  case Type.GenericType ignored -> Optional.empty();   // :123  <- Pair, List, Map: never marked today
  ```
  `PrecisionDecimal` -> DECIMAL (:115); Any/Nil/TDSNull/SQLNull/Enum-class -> empty (:120-121).
- Only caller of `crossing`: `Typer.java:233`. `of` has no caller outside the file (grep).
- Carriers it must recognise: `PlatformTypes.isPairCarrier` :771 (raw `...collection::Pair`, 2 args),
  `isListCarrier` :765 (`...collection::List`, 1 arg), `isMapCarrier` :777 (`...collection::Map`, 2 args).
  `Type.GenericType` is `record GenericType(String rawFqn, List<Type> arguments, List<Multiplicity> multArguments)`
  (`compiler/element/type/Type.java:337`); its `typeName()` prints `Pair<Integer, Float>` (:356-362), so the
  refusal text is readable as is.

### A2. Whose code is typed: `compiler/spec/Typer.java`

- `enum Compared` :173-186, with `GENERIC_BODY` :182-185:
  ```java
  /** A body typed into its call, over the caller's own columns (a function generic over relations): its column
   *  positions are generic ones ({@link CrossTypeRule#inGenericBody}). */
  GENERIC_BODY
  ```
- `inFunctionScope` :152-159 types a function's body as `bodyOf(fn)` = BODY or UNREAD (relational-language
  synthesized functions, `bodyOf` :167-170).
- `compared(...)` :228-250 — the one hook:
  ```java
  if (compared == Compared.UNREAD || compared != Compared.QUERY && !crossTypes.readsBodies()) {
      return call;
  }
  java.util.Optional<EqualityClasses.Crossing> crossing = EqualityClasses.crossing(chosen, args, ctx);
  ...
  if (compared == Compared.BODY) {
      return crossTypes.inBody(written);
  }
  if (compared == Compared.GENERIC_BODY) {
      return crossTypes.inGenericBody(written);
  }
  TypedSpec answer = crossTypes.inQuery(written);
  queryAnswers.add(answer);
  ```
  `pos` is the comparison call's `af.pos()` (Overloads :145): the position of the comparison in the text it was
  written in (for a generic body, the function's own text; StaticFold's rebuild keeps it, `foldCall` ends in
  `af.withParameters(...)`, StaticFold.java:246).
- `queryAnswers` :195-196 and `holdsAQueryAnswer` :253-266: an argument written in the query keeps the query's
  answer when a generic body is typed around it (used at Overloads :382-387).
- The mutable field `Typer.compared` is in `CodeShapeGuardrailTest.MUTABLE_FIELD_ALLOWLIST` (CodeShapeGuardrailTest.java:59).

### A3. Where a body is typed into its call: `compiler/spec/Overloads.java`

- `applyGeneric` :105-146, the switch into GENERIC_BODY :129-134:
  ```java
  if (requiresNormalization(a.chosen()) && storedLambdas.isEmpty()) {
      return t.comparing(t.bodyOf(a.chosen()) == Typer.Compared.UNREAD ? Typer.Compared.UNREAD
              : Typer.Compared.GENERIC_BODY, () -> inlineNormalized(af, a, env));
  }
  ...
  return t.compared(call, a.chosen(), a.args(), af.pos());   // :145
  ```
  NOTE: it sets GENERIC_BODY whatever the enclosing state is: a generic call met while the typer is UNREAD (an
  island's translation, `Typer.synth`'s island arm) types the body GENERIC_BODY all the same (inFunctionScope does
  the same for BODY, :156-158). Today harmless (lite's `inGenericBody` answers the call); once 6.3 makes it answer
  anything else, the #TS translation's library calls are reached (see E7).
- `requiresNormalization` :266-277: the three kinds of "generic body":
  ```java
  boolean stereotyped = f.definition().stereotypes().stream()
          .anyMatch(s -> s.stereotypeName().equals("NormalizeRequiredFunction"));
  return stereotyped || RelationGenerics.of(f) || f.parameters().stream()
          .anyMatch(p -> isSchemaErased(p.type()))
          || isSchemaErased(f.returnType());
  ```
  (`isSchemaErased` :279-311: a late-bound RelationType, TDS, TDSRow, TDSColumn, ColumnSpecification...).
- `inlineNormalized` :348-416: the body is SOURCE-substituted with the caller's raw arguments, statically folded,
  then typed in the CALLER's env:
  ```java
  ValueSpecification body = SourceSubst.substitute(hygienic.body(), subst);
  normalizing.push(key);
  try {
      TypedSpec typed = t.withPretyped(typedArgs, () -> synth(new StaticFold(t, env).fold(body), env));
  ```
  So at the typer every node of the body carries THIS call's types; nothing tells a side written in the function
  from a side whose type came from the call. `SourceSubst.substitute` splices the caller's node by identity
  (`env.getOrDefault(var.name(), var)`, SourceSubst.java:465), but `StaticFold.fold` replaces every static subtree
  by a fresh literal (`reify(eval(v, scope))`, StaticFold.java:138), so identity does not survive for literals.
- `RelationGenerics` (compiler/spec/RelationGenerics.java:36-42, javadoc :12-22): such a body "is checked at each
  call, over that call's tables, never once against its parameters -- the typer's schema algebra solves concrete
  relations, not open ones"; `Compiler.compileAllBodies` skips it (Compiler.java:1022-1027). So "type the body once
  as written" is not available as such.

### A4. The setting and its three implementations

- Interface `compiler/spec/CrossTypeRule.java` :28-76; the generic hook :68-75:
  ```java
  default TypedSpec inGenericBody(TypedCrossTypeComparison crossing) {
      return inBody(crossing);
  }
  ```
  `AS_WRITTEN` :37-52 (readsBodies false).
- Lite `compiler/spec/LiteCrossTypeRule.java`: `inQuery` :35-38 (literal rule, else the marker), `readsBodies`
  true :41-43, `inBody` = `inQuery` :46-48, and the generic exception :57-61:
  ```java
  public TypedSpec inGenericBody(TypedCrossTypeComparison crossing) {
      TypedSpec read = NumberLiterals.read(crossing);
      return read != null ? read : crossing.comparison();
  }
  ```
- legend-engine `edge/EngineComparisons.java`: `inQuery` = call :53-55, `readsBodies` false :58-60, `inBody` = call
  :63-65. No `inGenericBody` override. Because readsBodies is false, `Typer.compared` never asks it of a body
  (Typer :230). Its doors: `EngineComparisons.query` :42-50, `asTheDatabases` :31-33 (pure/v1 routes, TestDataRoutes,
  UpstreamTestBodies = the corpus's test-body door).
- legend-pure `edge/PureComparisons.java`: `inQuery` -> `answer` :54-56, :68-80 (EQUALITY false where a side is
  never empty, else left as a marker and refused by `asPures` :44-51; MEMBERSHIP false; INDEX -1; ORDERING the
  call); `readsBodies` false :59-61; `inBody` = call :64-66.

### A5. The marker `compiler/spec/typed/TypedCrossTypeComparison.java` and who consumes it

- Record :28-72 (`comparison, kind, operator, left, right, pos`), `refusal()` :45-50 (TYPE phase, "`op` compares
  L with R: two values are equal only when ..."). Permitted in `TypedSpec`'s sealed list (TypedSpec.java:80).
- Refused by name: `planner/PlanMaker.java:96` (`CrossTypeComparisons.refuse(query.body())`, every query's planning
  entry), `planner/ProductStatements.java:507`, `planner/SidePlanner.java:92`, `testplan/TestPlanner.java:251`
  (lifted test body), `compiler/spec/SpecCompiler.java:121` (a body typed whole: "the function's compile error").
- Seen in passing: `lowering/Lowerer.java:3646-3647` (`case TypedCrossTypeComparison x -> throw x.refusal();`, inside
  `scalarInner`), `lowering/CollectionLanes.java:137` (`valueLane(x.comparison())`).
- Read by the edge: `PureComparisons.asPures` :46 (`CrossTypeComparisons.in`), `kind()`/`operator()` (both READ_API
  rows of EdgeOnlyMembersTest :111-114).
- A marker inside a generic body ends up inside the QUERY's typed tree (the body is typed as part of the call), so
  lite's planner refuses it at PlanMaker :96 with the query: a "compile error of that function" today has no frame
  naming the function (the refusal text names only the two types; `pos` is in the function's text).

### A6. Instances: `lowering/InstanceEquality.java` (485 lines) and its canon

- Dispatch: `lowering/Lowerer.java:4127-4129` (statically disjoint, in `scalarRelationalArms`) and :4307-4319 (the
  claim, in `scalarValueTailArms`; an unclaimable shape is refused by name: "... over objects of [...] that carry no
  identity here: they are compared by their keys or their identity, never as structs").
- `claims` :62-93 (type-only gate). Platform carriers (Pair/List/Map/TDS) are not claimed (`classInstanceFqn`
  :378-392), so a pair's `==` never reaches here: it is Scalars' `equal` rule (Scalars.java:115-191), ending in
  `NullSemantics.equalNullArms` over the two structs = the database's struct `=`.
- `equality` :127-176 — the class is the STATIC class, and differing static classes are false only on the
  identity lane:
  ```java
  String lf = Objects.requireNonNull(instanceFqn(l));
  String rf = Objects.requireNonNull(instanceFqn(r));
  ...
  if (!lf.equals(rf)) {
      return hasIdentityField(lt) && hasIdentityField(rt)
              ? new SqlExpr.BoolLit(false) : null;
  }
  EqualityKeys keys = keysOf.apply(l.info().type());
  ```
- The same static gate in `contains` :191-197 (`if (!cf.equals(nf)) return hasIdentityField(lt) ? false : null`),
  `indexOf` :315-318 (`-1`), `collectionsEqual` :342-344 (`!lf.equals(rf)` -> null -> refused).
  `removeDuplicates` :256-277 and `isDistinct` :280-295 use the collection's static class for every element.
- `hasIdentityField` :353-356 (struct has `__id`).
- The run-time class is never read: the keyed canon writes the STATIC class as a literal,
  `lowering/CanonicalRenderSql.java` `instanceCanon` :707-760:
  ```java
  List<SqlExpr> kv = new java.util.ArrayList<>();
  kv.add(new SqlExpr.StringLit("_type"));
  kv.add(new SqlExpr.StringLit(keys.classFqn()));      // :747-748 (and :730-731 for the List<T> bare array)
  ```
  `instanceEqualityCanon` :679-685 (keyed -> `instanceCanon`, keyless -> `identityCanon`); `trackedId` :662-664.
  Only a JSON-carried slot reads its `__type`/`__canon` from the value (`jsonSlotCanon` :985-1000).
- Where `__type` exists: `compiler/element/ClassLayouts.java:122-125` (identity layouts add `__id`, `__type`,
  `__canon`), `objectLayoutOf` :128-141 ("every object carries both on every lane", minus `__canon`),
  `syntheticOnlyLayout` :156-163. Written at construction through `MixedEncoding.syntheticField`
  (Lowerer.java:3972-3976). The judges already read it: VerdictSql.java:419-447 (typed leaves start with
  `__type`), testexec/HostJudgments.java:144, :206 (`restrictOne`: classifier first).
- Keys: `compiler/element/EqualityKeys.java` `resolve(ctx, Type)` :124-131 and `collect` :197-244: own class first,
  then supertypes, deduped by NAME — so a subclass that redeclares a key property without the stereotype removes
  it (InstanceIdentityTest's `m::Sub` redeclares `sides`: Sub's keys are `n`, `name`; Top's are `name`, `sides`).
  The key set therefore depends on the run-time class. `EqualityKeys` has no fact about subclasses.
- `staticallyDisjoint` :413-442 (Variant, two enumerations, enum vs non-String, instance literal vs primitive) and
  `disjointAnswer` :447-453: not about classes vs classes; unchanged by 6.3.

### A7. The literal rule: `compiler/spec/NumberLiterals.java` (189 lines)

- `read` :51-80; it reaches only Integer/Float/Decimal sides (`number(...)` :82-84), so a crossing of two generic
  types is never reread (returns null):
  ```java
  if (!(crossing.comparison() instanceof TypedNativeCall call) || call.args().size() != 2
          || !number(crossing.left()) || !number(crossing.right())) {
      return null;
  }
  ...
  if (leftWritten == written(r)) {
      // two literals (a Decimal literal is one: 1D == 1), or none: nothing gives one side the other's type
      return null;
  ```
- In a generic body it is applied to the SUBSTITUTED tree (LiteCrossTypeRule :59): a literal the caller passed for a
  `Number` parameter looks, at the typer, exactly like a literal the function's author wrote.

### A8. The folds that answer a comparison before the setting is asked

- `compiler/spec/StaticFold.java` — runs on a generic body's SOURCE before it is typed (Overloads :391). It
  evaluates `==`, `in`, `contains`, `indexOf` over static values with Java `equals`:
  ```java
  case EQUAL -> {                                   // :608-612
      List<Object> args = evalAll(ps, scope);
      return args != null && args.size() == 2
              ? staticEquals(args.get(0), args.get(1)) : null;
  }
  ...
  private static @Nullable Boolean staticEquals(Object a, Object b) {    // :877-890
      ...
      return a.equals(b);
  }
  ```
  `IN` :642-649 (`coll.contains(x)`), `CONTAINS` :658-667, `INDEX_OF` :668-675, `PAIR` :584 (a `StaticFold.Pair`
  record, :75). `eval` :383-417: `CInteger` -> `Long`, `CFloat` -> `Double`, `CDecimal` not static. Consequence
  inside a generic body: `1 == 1.0` -> `Long(1).equals(Double(1.0))` = **false**, `1->in([1.0])` false,
  `pair(1, 1.0) == pair(1, 1)` false (record equals), all BEFORE the typer — no marker, no compile error, and at
  legend-engine's door a value the database would answer true (that is the fold computing a cross-type answer: a
  standing defect, step 10's fold cut, also noted in the plan record at :407).
- `compiler/spec/LiteralUnroll.java` — runs on the TYPED tree in G½ (after the typer's marker exists):
  - `equalityFold` :391-428: answers only two `Scalar` literals of one class (`l.cls().equals(r.cls())` and
    `comparedHere`), never two `Instance` literals — so `pair(...) == pair(...)` is not folded (it reaches the
    `Instance` branch of `literal` :101-104 and falls to `c`).
  - `comparedHere` :446-449 (String, Boolean, enum value, Integer).
  - `inFold` :482-499: a whole number against a list of another number type is left to the database (:490-494).
  - the `contains` fold inside `nativeFold` :545-557.
  - `nativeFold` is **246 lines** (:520-765) by CodeShapeGuardrailTest's measure: 4 lines of headroom.
- `compiler/spec/NormalizeFolds.java` (118 lines): folds `eq/equal` over two integer literals only (:29) — not
  affected.

### A9. D5's shape: a function over `Number`, not a generic body

`function f(x: Number[1]): Boolean[1] { $x->in([0, 1]) }` is not `requiresNormalization` (no type parameter, no
erased type): it is a BODY, typed once with `x: Number` (`EqualityClasses.of(NUMBER)` empty, :113) — no crossing,
no marker. G½'s UserCallInliner pastes it; the lowering then sees `1.0->in([0, 1])`: `inFold` gives it back (the
value is a Float, :483), `Scalars` IN rule (Scalars.java:2690-2710) has `MixedEncoding.kindMismatch` (MixedEncoding.java:823-829,
primitive vs class only) false, so the database answers `1.0 IN (0, 1)` = **true**. The mirror `f(x: Number[1])
{ $x->in([0.0, 1.0]) }` with `f(1)`: `inFold` :490-494 returns the call, database `1 IN (0.0, 1.0)` = true. Today's
answer is D5's option (a) in both directions.

---

## B. What must change, the smallest change, what it could break

### B(a) Generic containers compared by the types inside them

**Smallest change:** in `EqualityClasses.crossing` replace the `of(l)`/`of(r)` equality test by a recursive
`crosses(Type l, Type r, ctx)` (about 30 lines in EqualityClasses, no other file):
- two primitives/enums/classes: today's rule (`of` both present and differ, classes not `related`);
- two `GenericType`s whose raw is one of Pair/List/Map (`isPairCarrier`/`isListCarrier`/`isMapCarrier`) with the
  same raw: cross where ANY argument pair crosses (recursively, so `Pair<List<Integer>,_>` reaches inside); no
  crossing when every argument pair is equal or has a run-time side (Number, Any, Nil, a TypeVar...);
- a carrier against another carrier raw, or against a class: compare the raws as classes (`related` on rawFqn);
- every other `GenericType` (a user's `Box<T>`, `Relation`, `Function`, `Class<T>`) stays run time (open question
  E4).
- The `Crossing` keeps the two whole types (`left`/`right` = `Pair<Integer, Float>` ...), so the message reads
  "== compares Pair<Integer, Float> with Pair<Integer, Integer>".
Nothing else changes: `Typer.compared` already wraps, both doors already answer markers, the planner already
refuses. NumberLiterals returns null for a generic crossing (A7), so the literal rule does not reach inside a
container (E4 asks whether it should).

**Hand traces (lite's door = a received query, `compared == QUERY`):**
- `pair(1, 1.0) == pair(1, 1)`: `equal(Any[*], Any[*])` with args `Pair<Integer, Float>`, `Pair<Integer, Integer>`
  (did not trace the kernel's inference of `pair`'s U,V; assumed the arguments' own types). Today: `of` empty -> the
  call -> Scalars `equal` -> struct `=`: DuckDB true (did not run). After: `Float` vs `Integer` crosses -> marker ->
  `NumberLiterals.read` null -> PlanMaker :96 refuses: TYPE "== compares Pair<Integer, Float> with Pair<Integer,
  Integer> ...". legend-pure's door: `answer` EQUALITY, `neverEmpty` (both `[1]`) -> `false` (matches
  EqualityUtilities). legend-engine's door: the call (unchanged, the database's struct `=`).
- `[pair(1,'a')]->contains(pair(1.0,'a'))`: `contains(Any[*], Any[1])`, collection element `Pair<Integer,String>`,
  needle `Pair<Float,String>` -> MEMBERSHIP marker -> lite: "contains compares Pair<Integer, String> with
  Pair<Float, String>"; pure door: false; engine door: the call (Scalars' contains arm :2236-2355; which branch a
  pair takes there: did not trace).
- `newMap(pair(1,'a')) == newMap(pair(1.0,'a'))`: `Map<Integer,String>` vs `Map<Float,String>` -> crosses on the key
  argument -> marker; pure door false; engine door the call (the map's `=`: did not trace).
- `list([1, 2]) == list([1.0, 2.0])`: `List<Integer>` vs `List<Float>` -> marker. (The bare collections
  `[1, 2] == [1.0, 2.0]` are already marked today: Integer[*] vs Float[*].)
- Run time inside: `pair(1, $n) == pair(1, 1)` with `$n: Number[1]`: `Pair<Integer, Number>` vs
  `Pair<Integer, Integer>` -> no argument pair crosses (Number is run time) -> the call -> today the database's
  struct `=`, which compares 1.0 = 1 as true. The run-time rule (two types never equal) says false: this needs the
  struct compared field by field with 6.2's run-time kind per field. That is lowering work 6.3 does not have yet
  (Scalars' equal arm has no per-field path for carriers; 6.4 is "the product's == finished"). Smallest honest
  choice for 6.3: leave a run-time position to the database and record it (or refuse by name where a field's SQL
  type is a promotion, as 6.2 does for scalars) — see E4.
- `Pair<Number, String>` vs `Pair<Integer, Boolean>`: the second argument crosses although the first is run time ->
  marker (no value can be both).

**Could break:**
- `core/src/test/java/com/legend/testplan/AssertBarTest.java:985` `"{|assert(pair(1, '1'->fromJson()) == pair(1, 1))}"`
  is pinned to `refused` = "NotImplementedException: an equality between two whole Variants is not computed" (:953):
  `Pair<Integer, Variant>` vs `Pair<Integer, Integer>` now crosses (Variant is a static type, EqualityClasses :120)
  -> TYPE ModelException at TestPlanner :251. It flips (to the compile error). :992 (`Pair<Integer, Any>` both sides)
  must not move.
- PCT: I searched legend-pure m3-core's PCT tests for `==`/`!=`/contains/in/indexOf over `pair(`/`list([`/`newMap(`
  and found none (only `if.pure` pairs of lambdas). Asserts (`assertEquals(pair(...), pair(...))`) do not reach the
  rule: `EqualityClasses.COMPARISONS` :46-66 has no assert ids (the design's Q1, still open; the one assert that
  goes through the setting is assertSize's, `VerdictQueries.sameValue` :514, called from DatabaseJudge.java:424).
  Expected PCT moves ~0; measure.
- Corpus: none expected (the corpus's door is legend-engine's, which answers the call).
- The kernel's least upper bound of a list of pairs (`[pair(1,'a'), pair(1.0,'b')]` -> `Pair<Number, String>`?):
  did not trace; if it is `Pair<Any,...>` or `Any`, a membership over it stays run time, fine either way.

### B(b) Instances: run-time class first, on every path, then the keys

**What is wrong today** (A6): the class compared is the static one. Two consequences:
1. Same static class, different run-time classes (`$t: Top` holding a `Sub`, against a `Top`): canons both say
   `_type: m::Top` -> compared by Top's keys only -> can answer **true** where Pure answers false (classifiers
   differ).
2. Related static classes (`let x = ^Sub(...)->cast(@Top); $x == ^Sub(<same keys>)`): `lf != rf` -> identity lane
   **false** (Pure: true, one classifier, equal keys); value lane refused by name.
And the key set: Pure takes the RUN-TIME class's keys; lite the static class's `EqualityKeys`, which differ when a
subclass adds keys or un-keys one (InstanceIdentityTest's Sub).

**Smallest change that is right:**
1. `CanonicalRenderSql.instanceCanon` :730-731 and :747-748: the `_type` entry reads the struct's own `__type`
   (`new SqlExpr.StructGet(v, ClassLayouts.SYNTHETIC_TYPE)`) where the layout `st` carries that field, the literal
   `keys.classFqn()` otherwise. One edit, and every path that builds a keyed canon gets "class first" for free:
   `equality`, `contains`/`in`, `indexOf`, `removeDuplicates`, `isDistinct`, `collectionsEqual`, and the nested keyed
   leaves (:860, :962). No new SqlFn (StructGet is a node), so SqlFnSortedTest's writer sets do not move. Keyless
   classes compare by `__id`, which already implies one class.
2. InstanceEquality's four static gates (`equality` :151-158, `contains` :191-197, `indexOf` :315-318,
   `collectionsEqual` :342-344): two RELATED static classes (the typer has marked unrelated ones at lite's door; at
   the other doors an unrelated pair still reaches here and keeps today's static false) no longer answer false or
   refuse; they compare canons (which now carry the run-time class), with the narrower static class's keys.
3. Refuse by name where class-first cannot be answered:
   - the struct carries no `__type` and the static class has a subclass in the model (the static class is then not
     the run-time class); a class with no subclass is its own run-time class and needs no `__type`;
   - the key set is not one across the static class's subtree (a subclass adds or un-keys a key): the struct of the
     static type does not even carry the subclass's key fields.
   Both are facts of the model, fixed by the types. The lowering may not look them up (invariant 2), so they must
   arrive the way `instanceKeysOf` does (Lowerer.java:376, :474): e.g. a flag on `EqualityKeys` computed in
   `EqualityKeys.resolve` (compiler/element, which may read `ctx.subtree`) — "this class's keys are every class's
   below it" and "this class has a subclass". About 20 lines there, 10-15 in InstanceEquality.

**Hand trace — subclass against superclass, equal keys:**
- `^m::Sub(name='t', n=1, sides=[]) == ^m::Top(name='t', sides=[])` (InstanceIdentityTest's model): typer: Sub ⊑
  Top, related -> no marker (CrossTypeComparisonsTest :67 pins `^x::A(n=1) == ^x::B(n=1)` unmarked). Lowering today:
  `lf` Sub != `rf` Top -> identity lane `false` (correct by luck: literals are their own class); value lane refused.
  After: canons carry `__type` 'm::Sub' and 'm::Top' -> false on both lanes — but Top's subtree has a different key
  set (Sub un-keys `sides`, adds `n`) -> by rule 3 this is refused by name unless the key-set question (E5) is ruled
  otherwise. For a model where Sub only inherits Top's keys: false, answered.
- `let x = ^Sub(name='t', n=1)->cast(@Top); $x == ^Sub(name='t', n=1)`: today identity lane false (wrong); after:
  `__type` equal and keys equal -> true. Whether `cast(@Top)` keeps the Sub struct (and its `__type`) or rebuilds a
  Top layout: did not trace.

**Could break:**
- The canon text changes from a constant to a read of `__type` wherever the struct carries it: SQL size grows a
  little per keyed compare (a StructGet instead of a literal); the lean-SQL size pins of instance families may move up
  (needs the user's word per the AGENTS rules).
- VerdictSql/the database judge byte-compare canons built by the same `instanceCanon`
  (CarrierPurityRatchetTest.java:171-176, :203-209 describe it): an expected literal side and an actual side read from
  a store with no `__type` would now be spelled from different sources — the same text only where the run-time class
  is the static one. Measure the judges' agreement.
- Rule 3's refusals: a keyed class with subclasses compared over store rows that carry no `__type` (graph fetch,
  M2M, relational class queries) becomes a refusal where it answers today. The corpus runs through this lowering
  too (the lowering is door-free): risk of corpus moves on graphFetch/M2M lanes (the 2026-08-27 G4 regression, 11
  graphFetch rows, InstanceEquality.java:455-460, is the precedent). This is the risk the design names
  ("a polymorphic store row with no `__type`", b3-step5bc-design.md:217). Measure before choosing (E6).
- TypeInferenceIntegrationTest.java:1679-1684 (`eq` of two constructions false) must not move.

### B(c) A function typed into each call held to the strict rule

Two halves, and the hardest part of the slice is telling them apart.

**(c1) a comparison of two types known AS WRITTEN is the function's compile error.**
The typer sees only the substituted tree (A3), so it cannot tell `1 == 1.0` written by the author from `$v == 1`
where the caller passed `1.0` for `v: Number[1]`. Options (E3): 
- **P-a (recommended): check the body as written, once per function, before substitution.** Walk the function's
  own `body()` (ValueSpecification) for comparison applications (the ids of `EqualityClasses.COMPARISONS`) whose two
  sides are typeable without the call: no free variable, or free variables only of parameters whose declared type
  holds no type variable and is not schema-erased (`v: Integer[1]`, an enumeration parameter), never a lambda's
  parameter (a row). Type those two sides alone (in a scratch env of those declared types, UNREAD, never asking the
  setting), ask `EqualityClasses.crossing`, and keep the crossings' source positions (`af.pos()`), memoized by
  `FunctionId`. Then in `Typer.compared`'s GENERIC_BODY branch: a crossing whose `pos` is in the function's set is a
  body's crossing (`crossTypes.inBody`: lite's marker, refused with the query by PlanMaker :96); any other is a
  generic position (`inGenericBody`). Cost: a new walker (~60-90 lines, a new class beside RelationGenerics), a memo
  field on Typer or Overloads (final map: no MUTABLE_FIELD entry), no protocol construction (ArchitectureTest's
  PROTOCOL_DESUGAR_DEBT: Overloads 2, StaticFold 12, Typer 17, SourceSubst 10, ArchitectureTest.java:4540-4560, must
  not grow: walk, never rebuild). It can also run in `Compiler.compileAllBodies` for the generic bodies it skips
  (Compiler.java:1022-1027), making it a real compile error of the function (E3's second half).
- P-b: provenance through the substitution (wrap each caller argument, or carry positions): fails for literals,
  which StaticFold re-mints (A3), and for row types (a lambda parameter over the caller's table).
- P-c: "both sides closed after substitution": wrong — a caller's literal for a `Number` parameter looks closed.

StaticFold must stop answering such comparisons first, or P-a's markers and (c2) never see them (A8): make
`staticEquals` :877-890 and the `IN`/`CONTAINS`/`INDEX_OF` arms :642-675 return null (not static) when the two values
are of different number kinds (`Long` vs `Double`, recursively through `StaticFold.Pair` and lists) — with
`instanceof` checks, never class comparisons (CodeShapeGuardrailTest.classNamesAreNeverLogic, :580-585, pinned at 0).
This also ends the fold's cross-type `false` at legend-engine's door in generic bodies (a meaning change there: the
database's answer replaces the fold's; measure the corpus's NormalizeRequired helpers — did not trace whether any
compares Long with Double in schema code; the schema vocabulary compares Strings and type tokens).

**(c2) a crossing at generic positions compares by the run-time rule.**
Today lite answers it with the call (LiteCrossTypeRule :60, the database: `1 = 1.0` true). At a generic position
the two types ARE known at this call (that is why `crossing` fired), so the run-time rule's answer is fixed by the
types: EQUALITY false where a side is never empty (else both-empty, as `disjointAnswer`, InstanceEquality.java:447-453),
MEMBERSHIP false, INDEX -1, ORDERING by value then type (D3, open). That is exactly `PureComparisons.answer`'s shape
for the first three; ORDERING needs D3's rule (for Integer vs Float: `<` becomes value `<=`, `>` stays `>`, `<=`
becomes `<=`, `>=` becomes `>`; for a day vs a date-time 6.2's order). The literal rule keeps its place first
(`NumberLiterals.read`, so `$x.n == 1.0` over an Integer column stays true). Smallest change: `LiteCrossTypeRule.
inGenericBody` returns a typed answer (a shared helper in compiler/spec, ~25 lines; PureComparisons could call it
for its own three — but it is in edge and reads only READ_API members, so keep it separate or move the answer into
compiler/spec and let the edge call it: an EdgeOnlyMembersTest row if only the edge calls a new public member).
D5 interacts: under D5(a), a literal written in the body against a `Number` parameter reads as the value's run-time
type; after substitution both sides may be literals (`$v == 1` with `v = 1.0`), which the literal rule refuses to
reread ("two literals", NumberLiterals :59-62), and (c2) would answer false. To keep D5(a), P-a's pass must also
record, per comparison position, which side is a literal as written, and (c2) answer by value when that side is the
literal (the database's comparison, as today). Under D5(b) (strict), (c2) answers false and nothing more is needed.

**Hand traces:**
- `x::same<T,P,Q>(r: Relation<T>[1], a: ColSpec<P⊆T>[1], b: ColSpec<Q⊆T>[1]) { $r->filter(x | $x->read($a)->toOne() ==
  $x->read($b)->toOne()) }` (CrossTypeRuleTest.java:36-37) called `#TDS n, f 1, 1.5#->x::same(~n, ~f)` (an Integer
  and a Float column): requiresNormalization (RelationGenerics) -> GENERIC_BODY -> `equal` args Integer, Float ->
  crossing -> P-a: both sides read the lambda parameter `x` -> generic positions -> (c2). Today: the call, database
  `n = f` (true on a row 1, 1.0). After: `false` (no SQL comparison at all), unless E2 says keep the database's.
  The CrossTypeRuleTest assertion (no marker, :164-165) still holds; its answer moves.
- `f(x: Number[1]) { $x->in([0, 1]) }` called `f(1.0)`: a BODY, not a generic body — (c) does not touch it; today
  true (A9). D5(a) keeps true and needs nothing; D5(b) needs the run-time rule at the inlined `1.0 IN (0, 1)` (the
  lowering would have to know `$x` was a run-time position, which the inlined tree has lost: it sees Float vs
  Integer with no marker), i.e. a new mechanism (5b-4's node, b3-step5bc-design.md:219-224) — expensive.
- The same written as a generic body, `g<T>(r: Relation<T>[1], v: Number[1]) { $r->filter(x | $v->in([0, 1])) }`,
  `g(1.0)`: StaticFold sees `1.0->in([0, 1])` (both static after substitution) -> `IN` arm :642-649 -> `[0L,1L]
  .contains(1.0d)` = **false**, folded before typing. So TODAY the same program answers true as a plain function and
  false as a generic one. Fixed by the StaticFold change (then: crossing Float vs Integer -> P-a: `$v` is a `Number`
  parameter -> generic position -> (c2): D5(a) true by value / D5(b) false).
- `1 == 1.0` written in a generic body: today StaticFold false; after StaticFold + P-a: a compile error (TYPE).
- The timeseries library's general `meet` (projects/timeseries/model.pure:2041-2048) compares
  `$a->read($period)->toOne() == $b->read($yPeriod)->toOne()` and `$r->read($at)->toOne() >= $r->read($yAt)->toOne()`:
  generic positions; with (c2), a day against a date-time there answers false instead of the database (DuckDB ties a
  day with its midnight), and the D4 refusal (oneGrain, :2068-2071, chosen by the typed overloads :2055/:2061) still
  raises at run time; TsIslandTest.java:1780-1793 must not move.

**Could break:**
- Step 4's exposure (docs/records/edition-homework/step4-exposure-2026-11-26.md:31-60): 65 comparisons in the
  timeseries library reached as generic bodies (2,627 bodies in the JVM lanes, 876 in DataCube's wasm, 93 in python);
  63 are a Float against an Integer literal (the literal rule keeps them), 2 a day against a date-time (now (c2)'s
  answer). Any comparison in it of two caller columns of different types at a call changes answer under (c2). Measure
  with the same probe (`probe-crossings-outside-the-query.patch.txt` plus a scope stack).
- DataCube's copy of the library (`datacube/src/pure-library-text.ts`) is the same text: the wasm lanes judge it too.
- The 32-tests record (C5) is the precedent: a marker reaching a typing the door did not receive. P-a only marks
  as-written crossings, and only under a setting that reads bodies (lite's); legend-engine's and legend-pure's settings
  read no body, so the corpus cannot be reached through GENERIC_BODY.

---

## C. Tests that pin today's behaviour

| Test (file:line) | Assertion today | After 6.3 |
|---|---|---|
| core/src/test/java/com/legend/compiler/spec/CrossTypeComparisonsTest.java:48-59 | 11 queries marked once each | unchanged; add the pair/list/map/contains cases here (a) |
| CrossTypeComparisonsTest.java:64-70 | `^x::A(n=1) == ^x::B(n=1)`, `$l->at(0) == 1` (Number), ... unmarked | must not move (B ⊑ A related; Number run time) |
| CrossTypeComparisonsTest.java:75-80 | `x::loose(1)` unmarked; PQ multi-supertype unmarked; an argument's marker kept through a generic call | must not move |
| core/src/test/java/com/legend/compiler/spec/CrossTypeRuleTest.java:114-125 | a recording setting is asked "body: Integer == Float" for `x::ones` (GENERIC_BODY routed to `inBody` by the interface default) and "query:" for an argument | the routing moves if P-a sends as-written crossings to `inBody` and generic ones to `inGenericBody`: `x::ones`' `$x.n == 1.0` reads the row, so it is a generic position -> a recording setting's default `inGenericBody` (= `inBody`) still records "body: ..." — stays green only while the default stays `inBody` |
| CrossTypeRuleTest.java:158-171 | `x::same(~n, ~f)` no marker; the query's `$x.n == $x.f` one marker; `x::ones()` no marker | stays (no marker); `x::same`'s ANSWER changes under (c2) — add an answer assertion; add a case `1 == 1.0` in a generic body = refused |
| CrossTypeRuleTest.java:139-156 | lite's setting: `x::loose` no marker (literal rule), `x::A$prop$big` refused, compileAllBodies says so | must not move |
| CrossTypeRuleTest.java:174-181 | `compileErrors(MODEL)` has the Float error; `AS_WRITTEN` none | must not move (if P-a joins compileAllBodies, MODEL's generic functions hold no as-written crossing, so no new entry) |
| core/src/test/java/com/legend/compiler/spec/NumberLiteralsTest.java (108 lines) | the literal rule | must not move |
| core/src/test/java/com/legend/integration/UserFunctionIntegrationTest.java:1780-1783 | `compileErrors(modelWith(LIBRARY)) == []` | must not move (watch it if P-a joins compileAllBodies) |
| UserFunctionIntegrationTest.java:1951 | an enum value a generic body compares a mapped column to reads as the database's code (StaticFold writes the enum back) | must not move: StaticFold's enum arm of `staticEquals` (:878-880) is untouched by the number-kind change |
| core/src/test/java/com/legend/integration/TimeseriesLibraryTest.java (712 lines) | the library's rows and SQL on every in-process database; the layered model's same SQL (:257-275) | rows must not move except where (c2) changes a cross-typed comparison; SQL text may move (a comparison folded to false) |
| core/src/test/java/com/legend/integration/TsIslandTest.java:1780-1793 | D4's refusal "a moment is no day" | must not move |
| core/src/test/java/com/legend/testplan/AssertBarTest.java:985 | `assert(pair(1, '1'->fromJson()) == pair(1, 1))` refused "an equality between two whole Variants is not computed" | **flips** to the TYPE compile error "== compares Pair<Integer, Variant> with Pair<Integer, Integer>" |
| AssertBarTest.java:992 | `pair(1, ...->cast(@Any)) == pair(1, 1->cast(@Any))` refused (Variants) | must not move (Pair<Integer, Any> both) |
| core/src/test/java/com/legend/testexec/InstanceIdentityTest.java:146-160, :186-210, :220-242, :244-253, :299-330 | keyed equality, `Sub` un-keys `sides` (:231-238: `u1 == u2` by `n` and `name`), membership, value-position equality | Sub vs Sub must not move. A new case Top-typed Sub vs Sub belongs here. If rule 3's key-set refusal is taken literally, any comparison typed `m::Top` in this model refuses (Top's subtree has two key sets): the existing Top-vs-Top cases (:85-91 of the method at :223) would flip to refused — that is E5's cost |
| InstanceIdentityTest.java:255-286 (`pairOfPairsKeys`) | `equal(pair(pair(1,'a'), 2), ...)` true/false, both judges | must not move (same types) |
| core/src/test/java/com/legend/integration/TypeInferenceIntegrationTest.java:1679-1684, :1687-1692 | `eq` of two constructions false; `[Firm...]->contains(3)` refused by the rule | must not move |
| PCT `Test_LegendLite_LiteRules_*_PCT.java` (pct/src/test/java/org/finos/legend/lite/pct/) | 16 "compares" pins (Essential :52; Grammar :37, :42-50; Standard :34-39): enums, classes vs classes/primitives, numbers, dates | none involves Pair/List/Map; none should move. testEqualNonPrimitive (Grammar :45) stays: LeftClass vs RightClass unrelated |
| PCT channel B | no expected text mentions `compares Pair/List/Map` (grep of pct/, spec/, core/src/test) | none expected |
| core/src/test/java/com/legend/equalitymeasure/EqualityMeasure.java:161 | the measuring probe's `"pair(1, 1D) == pair(1, 1)"` | a probe, not a pin: becomes a compile error under LITE |

**The "32 corpus tests" record** — `docs/records/compat-b3-progress.md:470-477` (copied in
`docs/records/edition-homework/handover/s4-wip.patch.txt:2377` and cited by b3-step5bc-design.md:299): on 2026-11-25
(step 5a, before door settings existed) the typer marked every comparison across types wherever it ran. About 32
corpus tests per database broke because the marker reached typings done AFTER the door: a function's body typed when
the planner inlines it, an `execute(...)` frame typed at planning, and a relational mapping's normalized operations
(the relational language's own `=`). The fix then was to mark only "the query or test body a door receives"
(`SpecCompiler.typeQueryBodyMarked`), typing everything else unmarked (`Typer.inFunctionScope`). Since step 2b the
setting is per door, and the corpus's door (legend-engine's) reads no body, so 6.3's body rules cannot reach the
corpus by that route; the relational language's synthesized functions are UNREAD (`Typer.bodyOf` :167-170); planner
own-account typings use `CrossTypeRule.AS_WRITTEN` (Compiler.java:916, :949). The live analogue of the old failure
is E7 (an island's translation reaching a generic body). A second, unrelated "30 corpus tests a lane" note in the
same handover patch (:391) is B3 step 4's ordered-asserts measurement, not this.

---

## D. Guards that will trip (exact current values)

- **JavaEvalLedgerTest.EVICT_SIZE** (core/src/test/java/com/legend/JavaEvalLedgerTest.java:84-…): none of the files
  6.3 needs is pinned — no row for EqualityClasses, Typer, Overloads, CrossTypeRule, LiteCrossTypeRule,
  NumberLiterals, StaticFold, LiteralUnroll, InstanceEquality, CanonicalRenderSql, EqualityKeys, ClassLayouts. Rows
  that WOULD trip if touched, all at their pin exactly (measured now with the test's own stripping): 
  `testexec/Equality.java` 319 = 319 (the host judge; 6.3 should not need it: HostJudgments already compares the
  classifier first, :144, :206), `testexec/HostJudgments.java` 649 = 649, `lowering/VerdictSql.java` 2263 = 2263
  (:611; the design's 2,286 is stale), `testplan/JudgmentFacts.java` 355 = 355, `testplan/DatabaseJudge.java` 827 =
  827. Any growth is a dated bump with the user's word.
- **JavaEvalLedgerTest FUNNEL_PACKAGE_REGISTERS** (:1874-2170): `edge` is registered (EngineComparisons,
  PureComparisons listed at :2157-2158); a NEW class in `edge` needs a row; compiler/spec and lowering are not
  registered.
- **CodeShapeGuardrailTest.METHOD_LIMIT = 250** (:37; allowlist empty, :41). Current lengths by the test's own rule
  (signature lines with an `@` annotation are not matched by its SIG regex, :463-466, so `InstanceEquality.equality/
  contains/...`, `CanonicalRenderSql.instanceCanon`, `StaticFold.eval/staticEquals`, `NumberLiterals.read` are not
  measured): `Typer.compared` 23, `Typer.comparing` 9, `Typer.inFunctionScope` 9, `Overloads.applyGeneric` 41,
  `Overloads.inlineNormalized` 69, `Overloads.requiresNormalization` 12, `EqualityClasses.crossing` 20, `.of` 20,
  `LiteCrossTypeRule.inGenericBody` 4, `PureComparisons.answer` 13, `StaticFold.foldCall` 46, `StaticFold.fold` 16,
  `LiteralUnroll.equalityFold` 38, `LiteralUnroll.inFold` 17, **`LiteralUnroll.nativeFold` 246 (4 left)**,
  `InstanceEquality.claims` 32, `.staticallyDisjoint` 30; the Lowerer methods that hold the dispatch:
  `scalarInner` 228 (the marker arm :3646), `scalarRelationalArms` 70 (:4127), **`scalarValueTailArms` 225** (the
  InstanceEquality claim :4307), **`scalarStructural` 249 (1 left)** — do not add an arm there.
- **CodeShapeGuardrailTest MUTABLE_FIELD_ALLOWLIST** (:52-…): `Typer.compared` is listed (:59); a new non-final field
  on Typer/Overloads needs a dated entry — use a final map.
- **CodeShapeGuardrailTest.classNamesAreNeverLogic** (:546-590), pinned at zero: no `getClass() ==` /
  `getClass().equals` / `getSimpleName().equals` in the StaticFold kind check.
- **EdgeOnlyMembersTest** (core/src/test/java/com/legend/EdgeOnlyMembersTest.java): READ_API rows :111-114
  (`TypedCrossTypeComparison#kind()`, `#operator()`), :120-125 (`CrossTypeComparisons#in`, `TypedQuery#comparing`).
  A new public product member called only by the edge (e.g. a shared "answer by the types" helper PureComparisons
  calls) needs a row; if lite's setting calls it too, no row.
- **SqlFnSortedTest** (core/src/test/java/com/legend/SqlFnSortedTest.java): `BOTH_WRITERS` per-SqlFn writer sets.
  InstanceEquality is in COALESCE (:240), EQUAL (:267-), LIST_TRANSFORM (:362) and the list at :277; it is NOT in
  AND (:216-…). Class-first via `__type` inside the canon adds no SqlFn; an `AND` of a `__type` compare and the canon
  written in InstanceEquality would need `"com.legend.lowering.InstanceEquality"` added to AND's set.
- **CarrierPurityRatchetTest** (core/src/test/java/com/legend/CarrierPurityRatchetTest.java:160-215): counts LIST_*
  carrier idioms in lowering; StructGet is not one — no move expected.
- **ArchitectureTest**: `PROTOCOL_DESUGAR_DEBT` (:4505-…; Overloads 2, SourceSubst 10, StaticFold 12, Typer 17,
  :4550-4560) — a walker that builds protocol nodes would grow a row; the `legendEnginesDoorTypesAQueryThroughItsSetting`
  rule (:740-760) names EngineComparisons — untouched unless the edge types a query anew.
- **LiteralUnrollLedgerTest** (core/src/test/java/com/legend/LiteralUnrollLedgerTest.java:68-106): the pinned set of
  natives LiteralUnroll folds; 6.3 should not touch LiteralUnroll.
- **ErrorShapeGuardrailTest** :229 (`StaticFold.java` 1 catch) — no new catch.
- **CompatConstantPoolTest** :68, :72 (TDSNull borrowed constants in LiteralUnroll/InstanceEquality) — untouched.
- **PlannerSurfaceTest** :1057-1086, :1220 pin `TypedQuery#comparing(CrossTypeRule)` and `compileErrors(...,
  CrossTypeRule)` signatures: keep the signatures.
- Lean-SQL size pins (the product's SQL size, "may only go down"): (b) adds a StructGet per keyed canon; (c2) removes
  comparisons (folded to a constant). Measure both.

---

## E. Open questions for the owner

1. **D5 — a literal against a value typed only at run time inside a function** (open-decisions :113-123). (a) the
   literal reads as the value's run-time number type (today's answer for a plain function, A9; free there; in a
   generic body it needs P-a to remember which side was the written literal, ~15 more lines, because substitution
   makes both sides literals); (b) strict false (pure reading; for a plain function it needs 5b-4's run-time-position
   node — days of work; in a generic body it is free). Recommendation as in D5: (a).
2. **(c2): a crossing at generic positions, known at the call.** (i) answer by the types (false / false / -1; D3 for
   orderings): the run-time rule, no SQL, but the timeseries library's cross-typed column comparisons and the two
   day-vs-date-time places change answer (measure first); (ii) keep the database's comparison (today) and record a
   register row (S127's gap stays open — AGENTS.md says "a gap to close with step 6"). ORDERING under (i) waits for D3.
3. **(c1): what is "known as written", and where it is reported.** Sides with no free variable only (smallest; misses
   `$v == 'a'` for a declared `v: Integer[1]`), or also parameters with a concrete declared type (recommended), or a
   full taint of row types (too large). Reported only when a query calls the function (the planner's refusal, no
   frame naming the function), or also by `Compiler.compileAllBodies` over the generic bodies it skips today (the
   function's own compile error; the SDLC check would then see it; UserFunctionIntegrationTest :1780 is the pin to
   keep).
4. **(a) scope.** Pair/List/Map only (recommended) or every generic class (`Box<T>`, a user's parameterized class:
   unknown exposure). Does the literal rule reach inside a container (`$p == pair(1, 0)` with `$p: Pair<Integer,
   Float>`: today it would be a compile error)? A run-time argument inside a container (`Pair<Integer, Number>`):
   database's struct `=` recorded until 6.4, or refused by name where the field's SQL type is a promotion?
5. **(b) the key set.** Pure takes the run-time class's keys. Options: (i) refuse by name where the static class's
   subtree has more than one key set (cheap; refuses every `Top`-typed comparison in InstanceIdentityTest's model);
   (ii) a CASE over the subtree's classes on `__type`, one canon per class (SQL grows with the subtree; the struct of
   the static type may not carry a subclass's key fields, so it may still need (i)); (iii) the construction-stamped
   `__canon`, which rides only the identity layouts (ClassLayouts :38-46).
6. **(b) a row with no `__type` whose class has a subclass.** Refuse by name (the slice's text; risk on graphFetch/
   M2M/relational corpus lanes), or trust the static class and record it. Measure the corpus before ruling.
7. **A generic body reached from an island's translation** (Overloads :129-134 and Typer :152-159 ignore an
   enclosing UNREAD): is a library function's body strict when a #TS translation calls it (consistent with plain
   bodies today, which are BODY there), or the island's rule? Under 6.3 this decides whether #TS queries change.
8. **The design's Q1** (asserts across known types: `assertEquals(pair(1,1.0), pair(1,1))`): still not a compile
   error (no assert id in EqualityClasses). Leave to 6.5 or rule now.
9. legend-pure's door in bodies (`PureComparisons.inBody` = the call, `readsBodies` false): 6.3 does not change it;
   (c2)'s "answer by types" is legend-pure's own answer there — a later row, each test read first.

**Could not determine:** the kernel's inferred type of `pair(1, 1.0)` and of a list of mixed pairs (assumed
`Pair<Integer, Float>`); which Scalars membership branch a pair or a map takes at legend-engine's door; whether
`cast(@Top)` keeps a Sub's struct and `__type`; whether store-read objects carry `__type` (objectLayoutOf says every
object does on every lane, but the value lane's plain layouts exist, ClassLayouts :54-55); how many of the
timeseries library's comparisons are two caller columns of different types at a real call; whether any
NormalizeRequired helper of the corpus compares a Long with a Double in StaticFold.

---

## F. Proposed order (each step small, each with its narrowest proof)

0. **Measure, no commit** (a worker session): the step-4 probe with a scope stack, counting in GENERIC_BODY each
   crossing as as-written / generic position (by P-a's rule), over the timeseries lanes, DataCube wasm, python, core;
   and a census of InstanceEquality's calls by lane, by "static class has a subclass", by "subtree has one key set",
   by "struct carries `__type`", over core, PCT LITE and both corpora. This decides E2, E5, E6.
1. **(a) generic containers** — `EqualityClasses.crosses` (one file). Proof: `CrossTypeComparisonsTest` (new
   pair/list/map/contains/nested/run-time-argument cases), then `AssertBarTest` (the :985 flip, :992 holds),
   `PureComparisonsTest` (legend-pure's door answers false).
2. **StaticFold stops answering across number kinds** (`staticEquals`, IN/CONTAINS/INDEX_OF). Proof:
   `CrossTypeRuleTest` (a recording setting is asked of `1 == 1.0` in a generic body), then
   `UserFunctionIntegrationTest` (the enum arm :1951 holds) and one corpus test with a NormalizeRequired helper
   through `//spec:corpus_one`.
3. **(c1) the as-written check (P-a)** and the GENERIC_BODY routing in `Typer.compared`. Proof: `CrossTypeRuleTest`
   (`1 == 1.0` and `$v == 'a'` for `v: Integer[1]` in a generic body refused; `x::same` and `x::ones` unmarked), then
   `TimeseriesLibraryTest`.
4. **(c2) the run-time rule at generic positions** (after E1, E2, D3). Proof: `CrossTypeRuleTest` (`x::same(~n, ~f)`
   answers false; `x::ones` true by the literal rule), then `TimeseriesLibraryTest`, `TsIslandTest` (D4 refusal).
5. **(b) class first** — `instanceCanon`'s `_type` from `__type`; the four static gates; the model facts on
   `EqualityKeys`; the refusals (after E5, E6). Proof: `InstanceIdentityTest` (new: Top-typed Sub vs Sub true, Sub vs
   Top false, the refusal by name), then `TypeInferenceIntegrationTest` (`testInstanceEquality`), then the graphFetch
   corpus tests through `//spec:corpus_one`.
6. The register rows (S127's generic-body gap closed or recorded; the class-first line; D5's ruling), the full round,
   the review, the lanes (instance canons can differ by database carrier: H2/SQLite JSON structs).
