# The compatibility program: the final plan (2026-11-19)

Written fresh after phase 0, once the user had ruled the goal, D1 and D2 (~13:25 UTC: "Okay agree with all"). It
replaces the working plan (`compat-program-plan-2026-11-19.md`, history now) as the thing to build from.
- **The evidence:** `compat-design-and-plan-plain-2026-11-19.md` (the design, what we lose, every measurement in plain
  English) and `compat-program-log-2026-11-19.md` (every ruling verbatim, every finding, every session), both on
  `claude/compat-program`.
- **Status:** for the independent reviewer's check, then for the user to ratify **stage A**. Stage B is ratified
  separately, after stage A's stop.

## 1. The goal (ruled)

**Lite follows Pure. legend-engine's formats are matched, and its answers where they agree with Pure. The rest is
recorded.**

So, at the end:
- lite's core (compiler, planner, plan, exec) means one thing, Pure's definitions, and holds no mode, no switch and no
  upstream-specific code. A guard keeps it that way;
- everything that serves legend-engine's and legend-pure's clients lives at the edges: the door that reads their
  input, and the formatters that write their output after the run;
- every place lite's answer differs from legend-engine's is a register row, listed for the user before it lands, and
  collected on one "differences from legend-engine" page.

**The rulebook change.** AGENTS.md's upstream-fidelity tenet and G8 of `docs/GOALS_OF_RECORD_2026_11_07.md` are
amended to say this, in the first stage-A code PR (the wording drafted in the log at ~13:15 UTC). No PR of its own.

## 2. The design in four parts

1. **The core means one thing: Pure's definitions.**
2. **The door accepts legend-engine's input unchanged.** It does not translate programs. It keeps a short, fixed list
   of typed rules: the corpus's H2 setup SQL converted for DuckDB; SQLite's Decimal as floating point for legend-pure
   programs; four small rewrites of legend-engine's corpus test bodies (stage B). It adds one detector (D1).
3. **Formatters after the run** write legend-engine's and legend-pure's output from typed results: result envelopes,
   printed tables and values, error words.
4. **legend-pure-style checking at the edge.** Lite's judge emits plain verdict records; a formatter words them. The
   SQL-text fallback and its referee (legend-engine's own H2) are kept unchanged and decide every SQL-text assert by
   its answer.

## 3. The rulings this plan carries

| Ruling | What it means for the build |
|---|---|
| The goal (above) | the rulebook amendment; every departure a register row |
| R4 / D1: `substring` and `indexOf` follow Pure, **loud** | A3 switches the core; a detector at the legend-engine server door warns by default, or refuses by a server setting, when a query sent to the database calls them with positions and the request has not set legend-engine's corrected option. No translator |
| D2: remove both number shortcuts | B4 deletes the exact-decimal literal and the unchecked-maths set (9 legend-engine answers stop matching, each reproducible by the user; 10 PCT and 2 corpus tests gained) |
| SQL text never promised (CP-4) | A2 deletes legend-engine's SQL spelling; the fallback decides SQL-text asserts by answer |
| The fallback and referee are kept | untouched by every step |
| No legacy database versions (CP-8) | B4 drops legend-engine's H2 legacy settings (1 corpus test); the referee keeps its own H2 2.1.214 |
| E-R2 | the legend-engine route answers everything lite can |
| CP-13 | plan export and import deferred; lite's own host drift is the product track's |
| CP-14 (iii), CP-6, CP-7, CP-11 | the judge as records plus a formatter; typed facts on results; error words never in the plan; SQLite Decimal kept as a door rule |
| The differences page | written from the register rows, landed with A3, kept current by every later step |

## 4. Stage A: measured and low risk (about 2 to 3 weeks)

**Order:** A1 first. Then A2 and A3, in either order. Then A4.
**Proven together already:** the combined dry run (H12) applied A2's and A3's changes and D2's at once on the real
suites. Every moved test was predicted from the single measurements, except 3 per corpus database (text-only tests
hidden behind an old `indexOf` assert, now counted). On legend-engine's PCT route, 22 tests move, every one toward
PCT's expected answer.

| Step | What | Done when | Size | Needs the user's word on | Builder |
|---|---|---|---|---|---|
| **A1** | **The safety nets.** The edge net (every answer on the legend-engine and legend-pure routes, recorded: PR #123). The legend-engine check (PCT classified against legend-engine's own results on DuckDB's five suites and H2's three, plus the batteries: PR #124). PCT under lite's own two judges (the product's judges track) | each PR green on every OS and reviewed; each net shown to catch a planted mistake; the Maven download fix on `main` first | built; fixes in flight | each PR's guarded-file moves (named in its body); whether masks number distinct ids; the Windows maths digits (an overlay, if that is the cause) | B1, B1-R, the product's judges track |
| **A2** | **Delete legend-engine's SQL spelling.** Four whole files (`EngineStyleH2`, `EngineStyleDB2`, `EngineStyleComposite`, `EngineTextDialect`: 2,990 lines, reached only through two factories), the printer seams of the plan-printout family (17 call sites), the dead flag on `Compiler.lowerResolved`, `PureMessages.sentence`. Afterwards `toSQLString` and plan text answer lite's own SQL on all 12 of lite's dialects (today only H2's, DB2's and Composite's engine text), and a database lite has no dialect for is refused by name. Shape note: `A2-shape-note.md` and `A2-roster.tsv` on `claude/census-b2` (`1e8e817ee`) | the corpus moves exactly the roster's tests (H2 147 today, 150 once A3 lands; DuckDB 152 and 155), each with its reason, reconciled with the dry run (0 mismatches); lite's SQL size only shrinks; a planted Studio "generate plan" request shows an ordinary query's plan byte-identical, and one that asks for SQL text answering lite's text | two PRs, about 5 days (the printers out with every move, 2 to 3 days; then the family's spelling-only code, about 2) | the roster; 9 guard tests (each shrinks; one ratchet from 17 to 14), the rosters, the edge nets and the lean-SQL baseline, each with its dated note; two planner signatures | to be briefed |
| **A3** | **`substring` and `indexOf` follow Pure, loud.** First the product's fix for comparing text with a number. Then lite's Cypher, SQL and IQ frontends (47 places) and the mapping language's own functions keep their meaning explicitly. Then the core switches. Then the detector at the legend-engine door. Then the differences page | lite's frontend checks unchanged (Cypher 4,267, IQ 5,984, SQL 1,651 cases); PCT's 3 known `indexOf` failures pass; on legend-engine's PCT route 12 tests move to PCT's answer and none the other way; the corpus loses exactly its 4 per database; the detector warns on a planted request and stays quiet with the corrected option | about 4 to 5 days | the negative-start error; one native-catalog ceiling; 17 of lite's own tests that pin the old meaning; PCT's 3 known-failure rows; the detector's response header shown harmless to Studio | to be briefed |
| **A4** | **legend-pure-style checking leaves lite's database judge.** Its comparison rules stay in the Java-side check until stage B (the corpus needs them) | PCT unchanged; lite's two judges agree on the new PCT suite | about 2 days | none expected | to be briefed |

**Stop. The results go to the user before stage B.**

## 5. Stage B: the bigger moves (about 3 to 4 weeks), ratified after the stop

**Order:** B1, then B3, then B2 (the formatters ride the connections B3 reshapes), then B4, then B5.

| Step | What | Prototype | Open before it is built |
|---|---|---|---|
| **B1** | **Errors become plain records** (kind, place, literal values); each side's formatter words them | H13: **every pinned message identical on every lane of the net** (PCT DuckDB, SQLite, H2; channel B; the server route), with 33 lines in lite's own lowering for 6 tests and 15 in the database judge for 4. Five switches deleted, no core hook. Lite's plan JSON +0.06%, its SQL unchanged | a typed date operand; one size the dialect builds; the defect cell's size; the guarded files |
| **B3** | **The fallback and legend-pure-style checking move behind three narrow connections**, with the four corpus test-body rewrites at the door | H10: PCT and the H2 corpus identical; DuckDB moves only the 2 expected | where the four rewrites run, since the corpus enters through lite's own test runner (an edge-owned door is being measured; no core hook) |
| **B2** | **Formatters replace SQL-built text** (about 2,000 lines deleted): printed tables and values, result envelopes | H11: 431 of 431 printed tables byte-identical on DuckDB, 342 of 342 on H2, 419 of 420 SQLite, 427 of 430 Postgres; no core hook | the product's window-in-list fix first; the corpus half (running); single-value prints. 6 to 8 days |
| **B4** | **Remove what is left:** H2's legacy settings, the two number shortcuts (D2), DataCube's legend-engine workarounds moved to its legend-engine side | measured (B2's (j), H12) | each removal's moved tests listed before it lands |
| **B5** | **Delete the switch machinery** (legend-engine's feature flags in the core included) **and turn on the guards** | the ratchet is being prototyped (B2) | the counts reach zero: 49 parameters and 50 calls of the switch today; 142 places of upstream-specific code |

## 6. Not in this program

- **Deferred:** exporting plans to legend-engine and importing its plans.
- **The product track's, found by phase 0:** the 33 H2 function gaps (and H2's essential and standard suites joining
  lite's own PCT); the window-in-list defect (gates B2); `firstDayOfWeek` on H2; 5 judge-parity bugs; comparing text
  with a number (gates A3); lite's own rules copied across Python and the browser tab; the Maven download fix.

## 7. The safeguards

- Nothing merges without the user's word. Every step is a PR, green on every OS, reviewed by a session that wrote none
  of it.
- A pin, ceiling, ledger, golden or allowlist entry moves only with the user's word and a dated note; each PR names
  its moves.
- A step that would need a new switch or hook in lite's core stops and comes to the user.
- Every difference from legend-engine or legend-pure a step creates is a register row listed for the user before it
  lands, and goes on the differences page.
- Upstream's answers are read from the pinned sources or asked of legend-engine itself, never recalled.
- The stop after stage A.
