# B1's backlog for stage B's error records (B1 of the plan: the database raises, compatibility words upstream's sentence in Java from the typed record)

No change now. Each row is a fact the error-record slice must take in when it re-words a raise at the edge. Every row
cites the pinned sources: legend-pure 5.99.0 and legend-engine 4.145.0 (`release.MODULE.bazel:24-25`).

## 1. Division by zero: two wording errors in `edge/PureMessages.java` (2026-11-19)

**Found by** B2's `x / 0` reading at the legend-pure pin (`divide-by-zero-probe.md` on `claude/census-b2`,
`f6dc36008`), passed on by the program's owner (2026-11-19 ~20:01 UTC). **Checked here** against the pinned source.

| # | lite today | legend-pure at the pin | fix in the error-record slice |
|---|---|---|---|
| 1.1 | `PureMessages.java:26-29` words Integer / Integer's raise, which has no operand, as `Division by zero`. The comment calls it "the interpreter's BigDecimal 'Division by zero' (its NumericAccumulator; the D0 inventory's 1.2)", and the D0 inventory's row 1.2 says the same | **compiled** legend-pure, the mode legend-engine runs, says `Cannot divide 0 by zero`: `CompiledSupport.divide`'s `throw new PureExecutionException(..., "Cannot divide " + right + " by zero")` prints the DIVISOR (`legend-pure-runtime-java-engine-compiled/.../support/CompiledSupport.java:1481-1486`, the throw at 1485). The **interpreter** at the pin does Integer / Integer as a double and fails with a `NumberFormatException` (B2's record; not re-run here) | word Integer / Integer's raise as compiled legend-pure does: `Cannot divide 0 by zero`, the divisor printed. Correct the D0 inventory's row 1.2 to match |
| 1.2 | the same comment cites `CompiledSupport:1485` for `rem`'s and `quotient`'s `Cannot divide <dividend> by zero` | line 1485 is `divide`'s, which prints the divisor. Compiled `rem`'s message is `CoreHelper.rem`: `"Cannot divide " + dividend.toString() + " by zero"` (`legend-pure-runtime-java-engine-compiled/.../compiled/CoreHelper.java:540-545`, the throw at 544) | cite `CoreHelper.java:544` for rem's sentence, and check `quotient`'s own site at the pin before citing it |

**What the slice must keep straight.**
- `divide` prints the divisor (always `0`).
- `rem` prints the dividend.
- Lite's `DIVIDE_BY_ZERO` raise distinguishes the two today only by whether it carries an operand. The typed record
  should name the operation (divide, rem, quotient), so the edge words each from upstream's own site.
