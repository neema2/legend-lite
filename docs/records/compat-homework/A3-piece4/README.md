# A3 piece 4, for the reviewer (census B2, 2026-11-19)

The compatibility program's A3 piece 4 covers three things: the differences page, the A3 register rows and the
rulebook amendment. It is docs only.

It is built on `claude/compat-a3-docs`. That branch is cut from A2's head `6551c2036` and stays LOCAL until the
review. Its head is `9dc385235`.

This folder carries the change for the reviewer to read:

- `A3-piece4.patch.txt`: the whole change, `git diff 6551c2036 9dc385235`. It touches six files:
  - `AGENTS.md`: the upstream-fidelity tenet is amended, and the six tenets sit beside it;
  - `docs/GOALS_OF_RECORD_2026_11_07.md`: G8's first bullet;
  - `docs/SEMANTICS_REGISTER.md`:
    - S130 and S134 rewritten;
    - S382 to S387 new and provisional;
  - `docs/DIFFERENCES_FROM_LEGEND_ENGINE.md`: new;
  - `docs/records/compat-rules-of-record-2026-11-19.md`: carried from `claude/compat-program`;
  - `docs/TOTAL_WALL_START_HERE_2026_11_11.md`: a dated pointer.
- `A3-piece4-since-review.patch.txt`: the delta since the first review, `git diff c957c2b49 9dc385235`. It holds two
  commits:
  - `5b337b867`:
    - firstDayOfWeek corrected: the page's item 13, the Gaps bullet removed, S387 rewritten;
    - the user's 2026-09 rulings cited in S386 and S387;
    - two long lines rewrapped.
  - `9dc385235`:
    - item 3's `x / 0`, legend-pure read to the end (the review's S4);
    - H2's count, "147 or 148, settled by the merged batch's H2 lane" (item 2 and S380);
    - S386's cite (re-check 1's nit).
- `DIFFERENCES_FROM_LEGEND_ENGINE.md`: the new page as it will land, for reading whole.
- `first-day-of-week-probe.md`: S387's evidence.
  - Each database was asked directly under three Java locales: H2 2.1.214 and 2.5.252, and DuckDB 1.5.6.0.
  - The record holds the sources and the outputs.
- `divide-by-zero-probe.md`: item 3's evidence for `x / 0`.
  - legend-pure 5.99.0 read in both modes.
  - Its `NumericAccumulator` compiled exactly as pinned and run.

`//gates:checks` on `9dc385235`: 197 pass, 1 skipped.
