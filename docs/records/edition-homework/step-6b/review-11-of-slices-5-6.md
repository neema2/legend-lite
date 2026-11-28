# Independent review of the additions to slices 5 and 6 (2026-11-28, about 08:10 UTC)

*An Opus reviewer, read-only, given the diff `913523817..24fd21d6e -- core/src/main`, AGENTS.md's invariants, tenets
and equality rule, and the author's intent for orientation only. It read the code and ran nothing. Shortened; the
author's disposition under each.*

**No blocker.**

## Should fix

1. **`Substitution.appliedWhereItHoldsAValue` threw where a substitution carries no callees** (a nested filter's own
   scope, an association's join condition: `Registries.NONE`, or built with `null, null`): a `map` over an optional
   primitive there died with an internal error where it used to inline.
   *Fixed: no throw. Such a scope applies the function as before (the old, unguarded answer): a KNOWN GAP, on the
   user's page. A missing `isNotEmpty` callee is how other rewrites there know to stand aside, so it is not handed
   in for this.*
2. **`assertContains`'s new path does not show a `[1]` side as "at most one"**, as the equality path does.
   *Left: the product's `contains` answers two-valued for a side that yields nothing (`COALESCE(..., false)`, an
   EXISTS over no rows); probed. Recorded.*
3. **A literal cast to `Number` or `Date` is no longer folded: could a compile-time recursion stop unrolling?**
   *Probed (`x::keep([1, 2], 5->cast(@Number))`, recursing over the list with the cast literal carried): answers on
   DuckDB and H2. The full round runs the recursion suites.*

## Notes

4. `MapChecker`: `[2]` with a `[0..1]` function gives `[0..2]`, more precise than legend-pure's `[*]`, harmless; it
   can now refuse code lite accepted (a `[1]` source with a `[0..1]` function passed where `[1]` is wanted), which
   legend-pure refuses too: expect test movement there, not a bug.
5. The guarded application's count allowed "at least one" for a many-valued function. *Fixed: lower bound 0.*
6. The rows form of a membership also replaces Postgres's and BigQuery's own `= ANY` for a BOUND side (an assert):
   same meaning; their sessions see it in the landing's round.
7. H2's "the list as written" applied to any written list given a type. *Fixed: the empty list alone.*
8. A membership the author wrote whose call is rebuilt without a source position loses the "two kinds" reading.
   The assert's own `contains` has none, and its gate asks one declared type. *Recorded; a position goes on the
   assert's call when values typed at run time move to this path.*
9. Two javadocs displaced by insertions. *To fix with the next edit of those files.*

## Found nothing in

`Membership.rows` through every visitor, rewriter and the plan's JSON (SQLite rebuilding one without its rows is
harmless: the rows form was already written); the native-list dialects; `amongRows`' gate; `writtenEmpty`; the Nil
sides through `asSameElements` and `boundOnce`; the gates' precedence; the compaction; no `default ->`, no SQL text
or function names in the lowering, no type inference added there.

## Not checked by the reviewer

`native-claims.tsv`; `CollectionLanes.buildsEmptySlots`; a NULL verdict in `VerdictSql.typed`; `SidePlanner.overSides`;
the dialect passes that walk the new sub-query child; MongoDB; whether any corpus or PCT test reaches finding 1.
