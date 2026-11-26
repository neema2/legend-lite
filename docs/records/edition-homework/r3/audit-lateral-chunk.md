# The independent audit of H2's lateral pass and of `chunk`'s literal pattern (2026-11-26)

*An independent reviewer (Opus 5.5, high effort) read `sql/dialect/CorrelatedFiltersAsJoins` with its tests and the
`chunk` rule, given the code and the rules, not the author's conclusions. Both had landed on the session branch after
targeted runs and lane rounds, without a review. Its findings, set out, with what was done about each.*

| # | Severity | Finding | Done |
|---|---|---|---|
| 1 | **Blocker** | **`chunk` above size 1 answered wrongly on MariaDB and MySQL.** Their `regexp_extract_all` finds the k-th match by asking whether the text splits into k matches (`s REGEXP ^(?:[\s\S]*?(?:p)){k}`), which is the left-to-right scan's k-th only where every match has one length. `'abcdefghijklmnop'->chunk(5)` gave 16 elements for Pure's 4; `'abc'->chunk(5)` gave `['abc','c','c']`. Until `chunk`'s written size became a literal pattern the two databases refused it by name (a pattern that is not a literal), so the change turned a refusal into a wrong answer; `chunk(1)`, a fixed length, is right, which is why the ten PCT tests pass. The same emulation is wrong for any pattern of the user's whose matches differ in length (`a+`) | the emulation takes each earlier match as an atomic group (`(?>...)`, which PCRE and ICU both have), so a match the scan has taken is not split again and the count is the scan's. A first fix, refusing by name every pattern whose matches may differ in length, was too wide: it refused PCT's own `regexpIndexOf` cases on both databases (the product's SQL size baseline counts refusals, and caught it). The MariaDB and MySQL lane is asked to confirm on the real databases, with direct probes of the two forms and its PCT round |
| 2 | Should fix | On SQLite a written size of 0 wrote `.{1,0}`, which its parser reads as one character a chunk (Pure raises "Invalid chunk size: 0"); a size past 2^31 crashed its parser; a large size writes one automaton copy a repetition | the literal pattern is written for sizes 1 to 1000 alone; any other size keeps the composed form it always had |
| 3 | Should fix (from before the change; both forms alike) | A negative size answers `[]` where Pure raises; RE2 databases refuse a bound over 1000 where Pure answers; `.` skips a newline, so `'ab\ncd'->chunk(5)` is `['ab','cd']` for Pure's `['ab\ncd']`; the engines count code points where Pure counts UTF-16 units | not this change's: recorded here as open product gaps of `chunk`, for a register row and a fix of their own (`(?s)` in the pattern, a raise below 1) |
| 4 | Note | The pass's reader did not count a row's position (`RowOrder`), a star or a star-except as a read of their table, nor see into raw SQL | they are reads now; a subquery holding raw SQL is left as it was |
| 5 | Note | A column with no table was allowed in a conjunct that stays inside; inside a correlated subquery it could bind to another scope once the lateral is a join | a subquery with any column of no table is left as it was |
| 6 | Note | The filtered subquery keeps outputs the removed filter had narrowed to not-null | left: the rows it concerns fail the join's ON; recorded |
| 7 | Note | A value drawn afresh (`RANDOM`, `GUID`) in a conjunct that stays inside was drawn once a row before it and would be drawn once overall | a subquery with one is left as it was |
| 8 | Tests | Untested: a LEFT lateral, a comparison that is no equality, and the bails | added: a LEFT lateral as a LEFT join with a `<` moved; the two new bails. Not added: a rendered statement run on H2 (the four PCT tests on H2's two channels are that) |

**Found sound by the reviewer:** for a filter-only subquery of plain-column projections, the uncorrelated conjuncts
kept inside and the correlated ones in the ON is exact for CROSS to INNER and LEFT to LEFT, for any Boolean predicate;
aggregates and windows are excluded; chains and inner laterals are handled bottom-up; the ON reads only aliases to
its left; column order and names are unchanged; no hash-ordered iteration; the literal pattern is textually what the
composed form produced for every size.

**Not verified without a database, by its own account:** how RE2, PCRE and ICU treat `{1,-1}` and `{1,0}`; whether H2
re-runs the derived table once a row before it.

**The lesson for the process:** both changes had passed their PCT tests on every lane, and the wrong answers were on
inputs the tests do not hold (a chunk longer than one character on two databases). The lanes measure what the tests
ask; the review is what read the emulation. A change to what reaches a dialect's emulation gets its review before
its push, not after.
