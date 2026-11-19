# H13: the error-records spike (2026-11-19)

**What this is.** A scratch prototype of stage B, step B1 (slice 1.2 of `compat-program-plan-2026-11-19.md`): a raise
site carries its kind, its place and its literal operands as typed data; each audience's formatter words it in Java
after the run; upstream words never enter a plan. It is a measuring instrument. It is never proposed for merge.

- **Branch:** `claude/compat-error-records-spike`, from `main` `b75d6c73a`.
- **Session:** `session_0195rQEoAgK9WB7AM5Cowp2f`. **Orchestrator:** `session_0195rQEoSpv1FxC3WndEk5ZH`.
- **Rules kept:** no PR, no force push, `main` untouched. No golden, ceiling, ledger or allowlist is moved by this
  session (section 9 says exactly which guard files the branch differs in, and why).
- **Order:** findings are appended as they are found. The newest is last.

## 1. The base: B1's S1 applies to `main` almost cleanly

- `git cherry-pick 70592a5bb` (B1's S1, from `claude/presentation-b1`) onto `main` `b75d6c73a`: 79 files, +663 / -853.
- **3 conflicts, all trivial:**
  - `edge/EdgeServer.java` and `pct/.../PctPlanRoute.java` named S1m's `Modes.ENGINE.computation()` and
    `Modes.PURE.computation()`. S1m is not carried, so the spike keeps `main`'s `PureText.PURE_V1` and
    `PureText.UPSTREAM`.
  - `ModeBranchCensusTest.java` is S0's (not on `main`). It is not carried.
- No other file of S1 refers to S1m. So **S1's deletions can be re-done on `main` without S0 or S1m** (commit
  `5c34feb2`).

## 2. The record, as prototyped

`com.legend.executionplan.Operand`, a sealed interface, product data:

| Operand | Built from | JSON (one key per operand) |
|---|---|---|
| `Text(String)` | `SqlExpr.StringLit` | `{"string":"..."}` |
| `Int(long)` | `SqlExpr.IntLit` | `{"integer":5}` |
| `Flt(double)` | `SqlExpr.FloatLit` | `{"float":"-1.0"}` (Java's shortest round-trip text, never a JSON number) |
| `Dec(BigDecimal)` | `SqlExpr.DecimalLit` | `{"decimal":"1.500"}` (digits and scale as written) |
| `Bool(boolean)` | `SqlExpr.BoolLit` | `{"boolean":true}` |
| `Day(String iso)` | `SqlExpr.DateLit` | `{"date":"2014-02-29"}` |
| `Instant(String iso)` | `SqlExpr.TimestampLit` | `{"dateTime":"2014-01-01 10:10:10.123"}` |
| `Computed()` | any other expression | `{"computed":true}`: the position is kept, the value is never carried (A3) |

- `ExecutionPlan.Site` is `(n, kind, where, message, List<Operand> operands)`. S1's `List<Extension> extensions` is
  gone.
- `PlanMaker.sites` builds the operands with `Operand.of(SqlExpr)` from `RaiseSite.operands()`. **It asks no style.**
  The record is the same under every style.
- `PlanJson` writes `operands` natively (no codec, no ServiceLoader) and omits the key when a site has none. Its
  reader accepts the key and refuses an operand of any other shape by name.
- Deleted on top of S1: `planner.SiteFacts` (S1's two-form hook), `edge.SiteWords`, the `siteWords` tag of
  `EdgeCodec`, `PureMessages.wordable`, `words` and `wordsOrNull` (the planning-time worder), and the edge dialects'
  choice between a site and a sentence in SQL.
