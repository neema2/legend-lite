# A3 piece 4 (census B2, 2026-11-19)

The compatibility program's A3 piece 4: the differences page, the A3 register rows and the rulebook amendment. Docs
only. It is on `claude/compat-a3-docs`, cut from A2's head `6551c2036`, head `caca6a69b`. The branch stays local until
the user's word in census B2's session.

The independent reviewer approved it at `9dc385235` (re-check 3, recorded at `7fc86465` on `claude/compat-review`).
Two commits follow, both the compat owner's relays:
- `3780307f1`: item 3's `x / 0` in one statement, item 13's When, and the Gaps' H2 week defect.
- `caca6a69b`: the user's rulings of ~20:05 and ~20:35:
  - S128 amended for `println`;
  - S316 amended for `toCSV`, in `docs/records/d-register-rows-2026-11-16.md`;
  - item 1's `engine` setting;
  - the switch rule, cited from AGENTS.md's tenet 6, with the rules-of-record copy refreshed;
  - the accepted divergences in item 2 and S380.

Files:
- `A3-piece4.patch.txt`: the whole change, `git diff 6551c2036 caca6a69b`.
- `A3-piece4-since-approval.patch.txt`: `git diff 9dc385235 caca6a69b`.
- `DIFFERENCES_FROM_LEGEND_ENGINE.md`: the page as it will land.
- `first-day-of-week-probe.md` and `divide-by-zero-probe.md`: the evidence for items 13 and 3.

`//gates:checks` on `caca6a69b`: 197 pass, 1 skipped.
