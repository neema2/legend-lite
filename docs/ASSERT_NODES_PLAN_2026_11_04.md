# Asserts as plan nodes (planned 2026-11-04)

**The user's design (2026-11-04):** "we really want to keep the host judge - i was hoping that in the plan of an assert
there would be a single sql lowering plan that db judge can run AND in same plan have the two sides with the enum of
the assert function that the host judge can run". The bar it serves (the same day): "anyone can write a NEW test
function using any of the assert* constructs and they should just do what they say they do on the tin".

**Where we are:** the plan record already has the shape -- `ExecutionPlan.Verdict(name, arm, base, wrapper)`: the
sides the host reads and the one statement the database judge runs (BOTH builds exactly that, `AgreementAsserts`).
What fails the bar is around it (`docs/ASSERT_WALL_LEANSQL_AUDIT_2026_11_04.md` §1): a verdict is made only for an
assert standing alone as a statement, by shape; the host's judgment is Java chosen by shape beside the plan, not data
in it; the message argument is read by no family; work happens at judge time (fallbacks, appeals).

**Upstream's model (read in the pinned legend-pure, `essential/tests/*.pure`):** `assert(condition,
messageFunction)` is the ONE native; every other `assert*` is a Pure function: a condition from ordinary functions
(`equal`, `eq`, `is`, `instanceOf`, `contains`, `isEmpty`, `size`, `abs(e - a) <= abs(d)`) and a message from `format`
over `toRepresentation` (`'\nexpected: %r\nactual:   %r'`, `'expected size: %s, actual size: %s'`, `'%s does not
contain %r'`, `'%r should not equal %r'`, ...), the caller's message (a String, a format and its arguments, or a
function) replacing the default.

## The node

One per assert call, wherever the call appears:

| Field | What |
|---|---|
| `fn` | the assert function: `NativeFn.Verdict` (the derived asserts -- `assertNotEq`, `assertNotSize`, `assertNotContains`, `assertIsNot` -- reduce to upstream's base form, e.g. `assertNotEq` is `assert(!eq(a, b))` with its own message) |
| operands | the sides upstream's body reads (expected and actual; collection and size; instance and type; condition; delta) |
| message | a String side: the caller's message (a String, a format of its arguments, a function's value), else upstream's default message expression for that overload -- planned like any expression, so both judges print the same words by construction |
| guard | a Boolean side, the conjunction of the `if` conditions the assert sits under; none at a statement |
| source | where the assert sits in a lambda over a collection (`forAll`, `map`): the operands, message and guard are per-element columns over it; the node fails at the first failing element in the source's order |

**The database judge** runs the node's one statement: per row, `holds` = the guard implies the enum's predicate over
the operands (today's `VerdictSql` comparisons, kept), `message` = the message side. **The host judge** reads the
operands, guard and message rows and applies the enum's predicate in Java (`Equality` and the lattice), failing with
the message row's text. **Nothing at judge time:** the sink reads rows and decides; a fallback is a planned column or
step, never a statement planned or sent from the sink.

**Fused (the user, 2026-11-04: "the 'fuse' step to do all assert together in one go"):** the database judge sends a
segment's asserts as ONE statement (`VerdictBatch`'s fused verdict statement): each node's row -- its holds and its
message, the caller's message a column of its own row -- is a member of it; pinned by
`AssertBarTest#aSegmentsAssertsAreOneStatementUnderTheDatabaseJudge` (three asserts with custom, format and function
messages: one statement). The node also carries its OWN planned statement, so where the database rejects the fused
statement (the split rung) the runner sends planned statements -- today those wrappers are planned at judge time (the
audit's L1), which this program ends.

**The default message** stays each family's own, which both judges spell from the operands (the cost of two judges,
accepted 2026-11-04); only a CALLER's message is a side -- a literal folds to a constant and costs no statement.

**The planning rule (one rule, no shapes):** an assert's value, whenever execution continues past it, is `true`. The
planner lifts each assert into a verdict step at its place in the program -- under its `if` conditions as a guard,
over its lambda's source as a vector -- and puts `true` where it stood. Helpers and prelude functions that contain
asserts are inlined like any function.

## The order

1. **The bar as a ratchet** (`AssertBarTest`): the audit's probes, each with upstream's outcome and exact words, under
   the host judge, the database judge alone and BOTH; the probes that miss today in a shrink-only list.
2. **Messages as sides** (custom messages, the format and function overloads, the per-family defaults).
3. **Helpers and the derived asserts inlined uniformly.**
4. **The lift**: a let-bound assert, an assert under an `if`, an assert followed by a query.
5. **Vector nodes over any source**, operands computed per element.
6. **Leniency scopes back to their rulings** (the multiset never over a collection `concatenate` or `union`; S132's
   float tokens for rendered texts only), `assertIs` decided at run time, `assertError`'s message any String.
7. **The judge matrix in the gate**: core under the database judge and BOTH, not only the host.
8. **Delete the shape recognizers the node made redundant.**

Then the wall (§2 of the audit) and the lean SQL (§3), then the corpus.
