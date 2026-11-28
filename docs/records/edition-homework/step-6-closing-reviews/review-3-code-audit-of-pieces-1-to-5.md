# The independent CODE audit of the closing batch's pieces 1 to 5 (of `38e4fd7b8..d791f5242`)

Read-only, by an independent reviewer (a subagent given the code, the design and the rules, never the author's
conclusions), 2026-11-27. Kept verbatim as evidence. Nothing in it was run by the reviewer: each finding is from
reading. Paths under `/home/user/ll-audit...` are a read-only worktree of this repository.

---

## Review of `38e4fd7b8..d791f5242`: read only, nothing run

I found 2 BLOCKERs, 5 SHOULD-FIXes and 7 NOTEs. Both blockers are wrong answers or regressions on ordinary input. I ran nothing, so each one is "very likely" from reading the code, not confirmed.

### BLOCKER

**B1. Two written lists of Float, Decimal or date are now refused on SQLite, MariaDB and MySQL, at every door. Before the diff they were answered correctly.**
- Where: `SameValuesForm.refuseTextOnlyLists`, called from `SQLite.java:1919` and `MariaDb.java:2141`.
- Example: `|[1.5, 2.5] == [1.5, 2.5]`, or `assert([%2020-01-01] == [%2020-01-01])`.
- Why it reaches the refusal:
  - `RootComparison.whole()` treats a side made only of literals as not whole, so the comparison is never planned as two whole sides.
  - The fold rule leaves a Float comparison to the database.
  - `ArrayLit`'s type comes from its elements (`SqlTyping.arrayLitType`), so the operand is `Array(DOUBLE)` and the refusal fires.
- Before: `json(..) = json(..)` answered true. Now it is a `DialectCapability`, whose text says the sides "read the row they stand in", which is false here.
- The dialect check fires at the legend-pure and legend-engine doors too. Their `==` row (`NUMBERS_OF_THREE_TYPES`) ends in the same plain array `EQUAL`.
- The test only covers the per-row case. It also departs from design 5.6, which asked for:
  - null-safe forms and membership to be covered too;
  - an operand with no type fact to be refused. The code lets it through, which is a fallback (line 59 says so).

**B2. At both compatibility doors, a date-time built with `date()` is now never equal to a TIMESTAMP, even at the same precision.**
- Where: `Equalities.aPartialDateTimeAgainstAFullOne` (`Equalities.java:253`) and `Scalars.java:3872`.
- The check treats the text-carried type `TEMPORAL_TEXT` as meaning "built to the hour or minute". It is not:
  - `DateCtorRule` also uses it for `date()` with fractional seconds, which is full precision.
  - `DateShifts` uses it for date-times with more than 6 sub-second digits, and for the `%latest` sentinel.
  - An hour-precision literal is a `TimestampLit` (`MatchFold.dateLit` pads it).
- Wrong answers that follow:
  - `date(2015, 4, 16, 14) == %2015-04-16T14` → lite now false. legend-pure says true: same precision, same value.
  - `date(2015,4,16,14,30,15.5) == %2015-04-16T14:30:15.5` → false, where upstream says true.
  - Step 6's instant arm answered both of these correctly until this commit.
- The original B22 case `%2015-04-16T14 == %2015-04-16T14:00:00` (two literals) is still true at the doors, where upstream says false.
- Inside a store query at legend-engine's door the new false was never measured upstream.
- The fix needs the written precision of each side, not the carrier type.

### SHOULD-FIX

**S1. Row B18 was not built.**
- `Number` sides held as an Integer column on one side and a Float column on the other:
  - as single values, `HeldKinds` refuses them in lite's mode;
  - as whole sides (lists, and `hasSameElements`), `SideEqualities.values` turns this into "same only where both are empty" (`SideEqualities.java:157`) and answers false.
- So one comparison gets two answers depending on where it stands, against the equality rule ("refused ... for 'the same' and for an ordering alike"). B18 said "Build: the same refusal".

**S2. The fresh-draw fallback looks at every let, not only the ones the sides read.**
- Where: `RootComparison.plan:73-74`. It checks all of `env.queryLets()` (new) and all of the statement's lets (older).
- For `==` it then silently returns null: the plain database list comparison, which also skips `refuseUnordered`. That is invariant 4.
- `{| let g = generateGuid(); let n = p::Party.all()->map(p|$p.name); $n == ['A','B'] }` is compared in arrival order instead of being refused.
- `hasSameElements` is refused because of a draw it never reads.
- The check should cover only the lets the two sides read (`FreeVars`).

**S3. `Lowerer.everyValueRead` (3809) refuses more than designed.**
- `bothMayBeEmpty` (3778) forces a planner statement even for harmless sides.
- Lowerers made with no planner (`Compiler.lowerResolved`, `EnginePlans`, the `SeedableLets` probe) now throw on any two-optional-sides comparison of two types. That is the refusal design 6.4 withdrew:
  - the lineage road turns it into quietly empty lineage;
  - the probe turns it into "not seedable".
- A plain property list read per row (`x::sameAnys($r.ints, $r.strs)` in a filter) is refused, although its emptiness could be read in place. Before, it got the database's comparison.
- The message always says "computed from the row", even when the real cause is no planner or no typed column.

**S4. Values are cast to text without regard to their type, against design 5.7.**
- `SideEqualities.everyValueRead` (190) builds `CAST(o AS VARCHAR)` whatever `o` is: JSON, a struct or a tagged value.
- Design 5.7 says "never a cast to text of an `Any` side that may hold an instance".
- Example: `x::sameAnys(<computed list of class instances>, ['a'])`. Whether each dialect renders a cast of JSON or a struct to VARCHAR (BigQuery especially) is unverified.

**S5. The door rewrite of `assert($a == $b)` also applies to tables** (`UpstreamTestBodies:88`).
- `UpstreamTiesTest` exercises exactly that: relation sides become `assertSameElements`.
- Register S430 says legend-pure's `==` on two tables is identity: two different table expressions are false.
- So the verdict moves from upstream's fail to pass. This needs the user's ruling, and the register row should say so.
- The failure message also changes from `assert`'s to `assertSameElements`'s.

### NOTE

1. **The positive-condition marks are keyed by node identity** (`positiveConditions`, line 348). The inliner places one let term by reference at every read. A term read under `&&`/`||` and again under `toString()` in the same filter keeps its mark at both places. Example: inside the lambda, `let c = $o.nick == 'a'; $c || $c->toString() == 'false'` drops the row whose nick is empty; Pure keeps it. This is an edge case.
2. **`whereLambda` negates with `enumInvolved = false`** (5089). A guarded enum equality under `forAll` becomes the null-safe not-equal instead of the expanded arms. The answer is the same; only the SQL shape differs.
3. **Root comparisons are newly planned on the test road** (`BodyPlanner.lowerAndPrepare`, through `wholeAnswer`). The result is a top-level `WITH`, with no pruning, `KeyedCounts` or `LetBinding`. Design 5.2 item 4 intends this, but asked for probes (a nested `WITH` on H2, MySQL and MariaDB) that I found no record of.
4. **The legacy TDS sort is now SORTED instead of DEFINED** (`OrderView:43`). `ReplayGates` and `HostJudge.planRendered` read SORTED as "the tail sort holds", so door verdicts on legacy TDS sorts can move. List them in the landing summary.
5. **Two places where the code disagrees with its comments or the design.**
   - The `Scalars.doorsRule` javadoc says "a row replaces the call's own rule, all of it". But `written()` applies the empty-side rule after a door's row; that is the door's own question, so the javadoc is inaccurate.
   - `NUMBERS_OF_THREE_TYPES` now also carries a date rule.
6. **Design 6.3's "a variable counted harmless whose let value computes"** (probe, then fix or record) is neither fixed nor recorded.
7. **Weak tests:**
   - The parameter case passes on any exception whose message lacks "order".
   - The map refusal accepts any `NotImplementedException`.
   - `PARTY` has one row, so the "sorted, compared" cases prove only that nothing was refused.
   - Missing cases:
     - written-list `==` on SQLite (B1);
     - same-precision `date()` against a literal at the doors (B2);
     - a join or as-of ON with one optional side staying a plain `=`;
     - an empty computed list against a non-empty side of another type.

### Checked and sound

- **`==`/`eq` at the doors is never planned as whole sides.** `UpstreamTies` names a row for them before both tables' stamp and database checks. `hasSameElements` is planned as lite's own at every door.
- **Root planning matches the old pre-pass** on the product road: seeded lets, slots, sides lowered with streaming off, post-processing, and the planned statement returned without pruning. A body whose last statement is not such a comparison is unaffected.
- **The recursion ends.** Each side is a strictly smaller subtree and can never be a comparison root itself.
- **`Scalars.lower` has one caller, guarded by a test.** The verbatim and legacy null-unsafe forms are kept.
- **Negation is right:** `!` of a guarded equality becomes the null-safe not-equal, and the `forAll` negation is correct.
- **The positive roots are marked:** filters (including the window and isolate retries), joins and as-of joins, `exists` and `find`, and `if` (in the scalar, reducer and window forms). Missed roots only cost SQL size.
- **`hasSameElements` is under lite's rule everywhere:** through `Typer.ruleFor` and `atTheCall`, excluded from `PureComparisons.in`, and still refused by the planner's `CrossTypeComparisons.refuse`. `NumberLiterals` uses `withChildren`, so its rewrites keep the callee.
- **The fresh-draw list check is correct** on the cases I traced.
- **`leastOrdered`:** never SORTED; lambda and match parameters are treated as unseen; `zip`, `fold`, `if`, `reverse` and `removeDuplicates` behave as stated. I found no false refusal for named order-keeping chains over sorted inputs.
- **`EngineTable.emptySideIsFalse = outsideStore()`** matches what was measured.

### Could not determine by reading

- Whether PCT or the corpus contain written Float/Decimal/date list `==` (this decides B1's blast radius).
- How each dialect renders `CAST(json, struct or tagged AS VARCHAR)`.
- The H2/MySQL shape of the test road's new root `WITH`.
- What `ReplayGates` does with the extra INCIDENTAL and SORTED answers.
