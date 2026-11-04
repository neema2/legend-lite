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

## Progress

| Step | State | Commit |
|---|---|---|
| 1 the ratchet | 37 probes, 27 gaps | `046a1825` |
| 2 messages as sides | a caller's message (String, format, function) one side; size and contains in upstream's words | `c461710e` |
| 3 helpers inlined | any helper whose root is an assert, or an assert the compiler decided; `format`'s `%r` by type and `%s` over a number | `20bdc834` |
| 4 the lift (part) | a `let`-bound assert lifted (`compiler.spec.AssertLift`, applied at the test door's entry, `BodyRouter`). "Asserts followed by a query" was admitted and reverted the same day (`5149f1a8`): it changed the routing of bodies ending in a statement the program cannot plan (the corpus's 55 test-data generation bodies); it waits on a lazy last statement | `26a6f561`, `5149f1a8` |
| 3b the corpus regression | the batch lost 61 corpus bodies, bisected to two causes: the judgment's wrappers (`Compat`, `Messaged`) hid its type from the planners (`HostJudgment.core` unwraps them), and a caller's message planned eagerly (a message that reads -- a variable, a query -- is planned only where it is SPELLED, literals and calls over them; the rest wait on a lazy side); `assertIs` decided statically only for a construction (`aaa8badb`) lost two PCT tests and was reverted (docs/GATES.md, 2026-11-04, batch 1) | `35e9b2e5`, `656ec117` |
| 4 the lift: `if` | lite's plan's `Branch` node (`ExecutionPlan.Branch`: a condition side, the then and else sides' steps -- verdict groups and branches): the runner reads the condition's one Boolean and runs the side it names, the other never sent, exactly as upstream never evaluates it. A side is a BODY of its own (`driver/AssertBranch`: the side's statement over the lets before the branch), planned by the same judge's planner as any body -- the host's, the database's (its own segments, the segment before the branch closed at it) and the agreement's (assembled from the host half's branch) -- so an assert under an `if` is planned exactly as one at a statement's root. Test-body plans alone carry it (a product query's `if` stays a SQL `CASE`); the engine-protocol rendering is untouched. Not yet: a side after a frame let (its CTE would compose into the side's statement), a constant side of an `if` that is the body's answer | `8de96db7` |
| 6 backwards-compatibility mode | the user (2026-11-04): "I want ours to be super clean, but be able to pass PCT/rcorpus in backwards compat mode": `ExecuteOptions.withEngineCompat` -- the PCT channel B and corpus harnesses set it; lite's own runs judge exactly. Today it scopes S132's float tokens inside a String (`HostJudgment.Compat`, `Equality.compat`); S15's Float 2 ULP (both judges) and S131's multiset over an unsorted query are candidates measured next | this |
| 6b the explicit rule | the user (2026-11-04): "yes explicit rule" -- `ExecutionPlan.JudgingRules` (`LITE`, `COMPILED_PURE_COMPAT`), a required field of every verdict step, chosen upfront by `ExecuteOptions.judgingRules()`, read by both judges; register S127 ruled (lite: different number kinds never equal; compat: an Integer and a Decimal by text) | `09104091`, `d24e84dc` |
| 5 vector nodes: per element | a quantified equality judged element by element, its operands computed by the database: the pairs vector `source->map(x | pair(e, a))`, the host reading the pairs as rows, the database composing the first failing element's words (`VerdictSql.perElement`) | `5eac7e30` |
| the bar's next round | a caller's message that READS (a let, a variable, a format over them) carried as a side -- every judge had dropped it; the database judge writes each side as upstream's message writes it (`VerdictSql.Message`: toRepresentation per element, assertSameElements in Pure's sort order, assertEquals' `%r` only where both sides hold one value; assertEmpty, assertNotEmpty, assertContains in their `.pure` texts) and the host's assertEquals took the one-value form from the pair; assertError over an assert; `assertIs` over two primitives judged as `assertEq` (compiled pure's `Is` native: `left == right`); an `if` closing the body whose other side is `true`; asserts then a query the query program answers | `6038d060` |
| the message read only where the assert fails | a verdict's caller's message is its `message` side (`ExecutionPlan.Verdict.message`), which the runner never reads on its own: the host judge reads it through the program's reader (`PlanRunner.ProgramSink#reader`) only where the assert fails; the database judge's batch sends the message's row at the failing root's place alone (`VerdictBatch.message`), its fused statement unchanged. A message that would raise where the assert holds sends nothing; one that raises where the assert fails is the failure, as upstream's | this |
