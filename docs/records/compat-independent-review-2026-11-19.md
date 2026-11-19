# Independent review of the compatibility program's design and plan (2026-11-19)

Reviewer: an independent session on branch `claude/compat-review`, asked by the program's orchestrator
(`session_0195rQEoSpv1FxC3WndEk5ZH`) for the user. I wrote none of the program. Review only: no product code,
pin, ceiling, ledger or allowlist moved.

**Read at these heads:** `main` `b75d6c73a`; the architecture under test at
`origin/claude/refactor-code-quality-review-9fqhug`; H1-H4 at `origin/claude/compat-homework-h{1..4}`; the census and its
SQL-text probe at `origin/claude/census-b2`; the oracle at `origin/claude/presentation-r`; the plan (r5) and log at
`origin/claude/compat-program` `933ee1f1`.

## 1. My own view (written from the evidence and the code, before I read the plan)

**The right architecture.** The architecture under test is right in its shape: one meaning in the core, compatibility
as translation at the two boundaries. The evidence supports it better than I expected. H2's count is the strongest
single fact in the homework: upstream programs essentially never spell a value as text and then compute with it (14
feeding sites in 1,012 upstream spelling sites; 0 over Float, Decimal, Date, DateTime or Boolean). So the feared R1
"print the way legend-pure does, inside SQL" family has almost no real demand; the presenter covers PCT's print tests.
What remains hard is not text but MEANING: substring, unchecked operations, numeric literal typing, null order, scan
order. Those are where the design will be tested.

What I would insist on:
1. **The translator works on resolved names, not on spellings.** A rewrite keyed on `substring` the token is the JSON
   rename H3 used, and H3 found it untyped and refusing by the wrong name. A rewrite keyed on the resolved FQN and the
   chosen overload needs name resolution (phase D) and, for overloads, typing (phase G). So the translator either
   reimplements part of the frontend (a second compiler, by the back door) or the core exposes a narrow entry that
   lets the edge run D, substitute FQNs, then continue. Neither is free, and H3 did not prototype it ("the model-level
   ENGINE adapter ... Not prototyped (time box)"). This is the single largest unknown in the design.
2. **Compatibility capabilities, if any, are named by what they compute, never by whose they are.** A native called
   `pureFloatText` is upstream meaning in the core under a new name; the census's M2 lens would (rightly) flag it.
3. **The presenter reads three inputs (typed result, plan, typed query facts), and the plan must already carry what
   it reads.** H3 (c) showed this. It means CP-6-style typed facts are product plan data, and their cost to the lean
   plan JSON is real.
4. **The judges: compatibility judging host-only is right, but it removes the database judge from the two biggest
   suites (PCT, the corpus).** G2 parity then rests on whatever still runs under LITE's rules. That has to be planned,
   not discovered.

**What I would do first.** De-risk the riskiest leg before any deletion: a two-day spike of the MODEL-level, typed,
target-aware ENGINE translator over the 15 corpus tests H3 (a) broke, through the public doors only. If it needs a new
core entry, the user should see that before the plan commits 64 session-days to an architecture whose input half is
unproven at the scale that matters. In parallel, the deletions that need nothing (the engine-text printers, rule 4)
are cheap and measured (B2's probe), and can go early.

**What I expected to go wrong.**
- The translator needs an entry into the core that does not exist today, or grows into a second frontend.
- The plan's sizes are optimistic for the judges' and translator's slices, and the lane times (corpus host passes,
  PCT, stress) dominate the calendar, not session-days.
- Deleting the edge's SQL shapes moves H2 row order for unordered results, and the corpus's positional asserts catch
  it before the multiset judge lands.
- "No special functions" plus "no old database versions" quietly moves the ENGINE audience's answers away from
  legend-engine in more places than counted, and the per-audience "move only toward the oracle" check cannot pass as
  worded.

