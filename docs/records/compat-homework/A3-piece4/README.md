# A3 piece 4, for the reviewer (census B2, 2026-11-19)

The compatibility program's A3 piece 4: the differences page, the A3 register rows and the rulebook amendment. Docs
only. Built on `claude/compat-a3-docs`, which is cut from A2's head `6551c2036` and stays LOCAL until the review (its head a0505ebe6). This
folder carries it for the reviewer to read:

- `A3-piece4.patch.txt`: the whole change, `git diff 6551c2036 a0505ebe6`. It touches five files (the fifth: `docs/records/compat-rules-of-record-2026-11-19.md`, carried from `claude/compat-program`):
  - `AGENTS.md`, the upstream-fidelity tenet amended and the six tenets beside it;
  - `docs/GOALS_OF_RECORD_2026_11_07.md`, G8's first bullet;
  - `docs/SEMANTICS_REGISTER.md`: S130 and S134 rewritten, and S382 to S385 new and provisional;
  - `docs/DIFFERENCES_FROM_LEGEND_ENGINE.md`, new.
- `DIFFERENCES_FROM_LEGEND_ENGINE.md`: the new page, as it will land, for reading whole.

`//gates:checks` on the branch: 197 pass, 1 skipped.
