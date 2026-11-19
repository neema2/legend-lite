# legend-pure's x / 0: the reading and the run (A3 piece 4, 2026-11-19)

Read for the differences page's item 3 (the review's S4). The source is legend-pure 5.99.0, the tag lite pins
(`release.MODULE.bazel`: `LEGEND_PURE_RELEASE = "5.99.0"`). Paths are relative to legend-pure.

## The reading

- **The function.** `divide(left:Number[1], right:Number[1]):Float[1]` is a native
  (`legend-pure-core/legend-pure-m3-core/src/main/resources/platform/pure/grammar/functions/math/operation/divide.pure:44-50`).
  Its six PCT tests divide by no zero.
- **Compiled** (`legend-pure-runtime-java-engine-compiled/.../generation/processors/support/CompiledSupport.java`):
  - `divide` checks the divisor first, at :1481-1486:
    `if (right.doubleValue() == 0) throw new PureExecutionException(sourceInformation, "Cannot divide " + right + " by zero")`.
    The message prints the DIVISOR, so it reads "Cannot divide 0 by zero" (or "0.0").
  - This is the mode legend-engine runs Pure in: `PureModel.java:149` and `:206` build a `CompiledExecutionSupport`
    (legend-engine 4.145.0, `legend-engine-language-pure-compiler`).
  - `divideDecimal`, the 3-argument `divide(Decimal, Decimal, scale)`, has no check, at :1496-1499:
    `left.divide(right, (int) scale, RoundingMode.HALF_UP)`. Java raises `ArithmeticException: / by zero`.
  - For comparison, compiled `rem` raises "Cannot divide <dividend> by zero", printing the dividend
    (`compiled/CoreHelper.java:544`).
- **Interpreted** (`legend-pure-runtime-java-engine-interpreted/.../natives/grammar/math/operation/`):
  - `Divide.java:48-56` has no check. It starts a `NumericAccumulator` with the first operand, divides it by the next,
    and makes `getValue().toString()` the name of a new Float instance (:56).
  - The operands become Java numbers by `NumericUtilities.toJavaNumber` (`natives/NumericUtilities.java:214-236`).
    An Integer is an `Integer`, `Long` or `BigInteger` (`m3/navigation/PrimitiveUtilities.java:130-155`). A Float and a
    Decimal are both `BigDecimal` (`PrimitiveUtilities.java:80-83`, `:90-93`).
  - An Integer divided by an Integer goes through `LongWrapper.divide(long)` (`NumericAccumulator.java:1562-1565`):
    `new DoubleWrapper((double) this.value).divide((double) number)`. That is `this.value /= number` (:1156-1161,
    "TODO check for underflow and overflow"). The result is `Infinity`, `-Infinity` or `NaN`.
  - The Float instance is then made by `ModelRepository.newFloatCoreInstance(String)`, which is
    `new BigDecimal(name)` (`legend-pure-m4/.../m4/ModelRepository.java:547-550`). `new BigDecimal("Infinity")` throws
    Java's `NumberFormatException`.
  - With a Float or Decimal operand, the accumulator is a `BigDecimalWrapper`:
    `this.value.divide(number, new MathContext(16, RoundingMode.HALF_UP))` (`NumericAccumulator.java:895`). Java
    raises `ArithmeticException: Division by zero`, or `Division undefined` for 0 / 0.
  - `DivideDecimal.java:54`, the 3-argument form, is `left.divide(right, scale, RoundingMode.HALF_UP)`, which raises
    `ArithmeticException: / by zero`.

## What it means

legend-pure raises for every zero divisor, in both modes. No mode answers a value. Only compiled mode raises a
Pure error with a written message: "Cannot divide 0 by zero". The interpreter's failures are Java exceptions that it
does not catch.

## The run

`NumericAccumulator.java` imports only `java.math`. It was compiled exactly as pinned and driven as `Divide.java:48-56`
drives it (`AccProbe.java`, below). The last step parses the value as a Pure Float, as
`ModelRepository.newFloatCoreInstance` does. `Div.java` gives Java 21's messages for the same operations.

```
Integer 1 / Integer 0 -> value Infinity; as a Pure Float: java.lang.NumberFormatException: Character I is neither a decimal digit number, decimal point, nor "e" notation exponential mark.
Integer 0 / Integer 0 -> value NaN; as a Pure Float: java.lang.NumberFormatException: Character N is neither a decimal digit number, decimal point, nor "e" notation exponential mark.
Integer -1 / Integer 0 -> value -Infinity; as a Pure Float: java.lang.NumberFormatException: Character I is neither a decimal digit number, decimal point, nor "e" notation exponential mark.
Integer 1 / Float 0.0 -> java.lang.ArithmeticException: Division by zero
Float 1.0 / Integer 0 -> java.lang.ArithmeticException: Division by zero
Float 1.0 / Float 0.0 -> java.lang.ArithmeticException: Division by zero
Float 0.0 / Float 0.0 -> java.lang.ArithmeticException: Division undefined
Decimal 1.0D / Decimal 0.0D -> java.lang.ArithmeticException: Division by zero
Integer 7 / Integer 2 (control) -> value 3.5; as a Pure Float: 3.5

(double) 1L / (double) 0L -> Infinity
(double) 0L / (double) 0L -> NaN
new BigDecimal("Infinity") -> java.lang.NumberFormatException: Character I is neither a decimal digit number, decimal point, nor "e" notation exponential mark.
new BigDecimal("NaN") -> java.lang.NumberFormatException: Character N is neither a decimal digit number, decimal point, nor "e" notation exponential mark.
1.0.divide(0, MathContext(16, HALF_UP)) -> java.lang.ArithmeticException: Division by zero
0.0.divide(0, MathContext(16, HALF_UP)) -> java.lang.ArithmeticException: Division undefined
1.0.divide(0.0, 2, HALF_UP) -> java.lang.ArithmeticException: / by zero
0.0.divide(0.0, 2, HALF_UP) -> java.lang.ArithmeticException: / by zero
"Cannot divide " + 0L + " by zero" -> Cannot divide 0 by zero
"Cannot divide " + 0.0 + " by zero" -> Cannot divide 0.0 by zero
```

## The sources

### AccProbe.java

```java
import java.math.BigDecimal;
import org.finos.legend.pure.runtime.java.interpreted.natives.grammar.math.operation.NumericAccumulator;
public class AccProbe {
  // as Divide.java:48-56 does: the first operand starts the accumulator, the next divides it, the value's toString()
  // becomes a Float instance's name, which ModelRepository.newFloatCoreInstance parses as new BigDecimal(name)
  static void t(String what, Number left, Number right) {
    String out;
    try {
      NumericAccumulator acc = NumericAccumulator.newAccumulator(left);
      acc.divide(right);
      String name = acc.getValue().toString();
      try { out = "value " + name + "; as a Pure Float: " + new BigDecimal(name); }
      catch (NumberFormatException e) { out = "value " + name + "; as a Pure Float: " + e.getClass().getName() + ": " + e.getMessage(); }
    } catch (RuntimeException e) { out = e.getClass().getName() + ": " + e.getMessage(); }
    System.out.println(what + " -> " + out);
  }
  public static void main(String[] a) {
    // Pure Integer is a Java Integer/Long (PrimitiveUtilities.getIntegerValue); Pure Float and Decimal are BigDecimal
    // (PrimitiveUtilities.getFloatValue / getDecimalValue)
    t("Integer 1 / Integer 0", Integer.valueOf(1), Integer.valueOf(0));
    t("Integer 0 / Integer 0", Integer.valueOf(0), Integer.valueOf(0));
    t("Integer -1 / Integer 0", Integer.valueOf(-1), Integer.valueOf(0));
    t("Integer 1 / Float 0.0", Integer.valueOf(1), new BigDecimal("0.0"));
    t("Float 1.0 / Integer 0", new BigDecimal("1.0"), Integer.valueOf(0));
    t("Float 1.0 / Float 0.0", new BigDecimal("1.0"), new BigDecimal("0.0"));
    t("Float 0.0 / Float 0.0", new BigDecimal("0.0"), new BigDecimal("0.0"));
    t("Decimal 1.0D / Decimal 0.0D", new BigDecimal("1.0"), new BigDecimal("0.0"));
    t("Integer 7 / Integer 2 (control)", Integer.valueOf(7), Integer.valueOf(2));
  }
}
```

### Div.java

```java
import java.math.*;
public class Div {
  static void t(String what, java.util.concurrent.Callable<Object> c) {
    try { System.out.println(what + " -> " + c.call()); }
    catch (Exception e) { System.out.println(what + " -> " + e.getClass().getName() + ": " + e.getMessage()); }
  }
  public static void main(String[] a) {
    MathContext mc = new MathContext(16, RoundingMode.HALF_UP);
    t("(double) 1L / (double) 0L", () -> (double) 1L / (double) 0L);
    t("(double) 0L / (double) 0L", () -> (double) 0L / (double) 0L);
    t("new BigDecimal(\"Infinity\")", () -> new BigDecimal("Infinity"));
    t("new BigDecimal(\"NaN\")", () -> new BigDecimal("NaN"));
    t("1.0.divide(0, MathContext(16, HALF_UP))", () -> new BigDecimal("1.0").divide(new BigDecimal(0), mc));
    t("0.0.divide(0, MathContext(16, HALF_UP))", () -> new BigDecimal("0.0").divide(new BigDecimal(0), mc));
    t("1.0.divide(0.0, 2, HALF_UP)", () -> new BigDecimal("1.0").divide(new BigDecimal("0.0"), 2, RoundingMode.HALF_UP));
    t("0.0.divide(0.0, 2, HALF_UP)", () -> new BigDecimal("0.0").divide(new BigDecimal("0.0"), 2, RoundingMode.HALF_UP));
    t("\"Cannot divide \" + 0L + \" by zero\"", () -> "Cannot divide " + Long.valueOf(0) + " by zero");
    t("\"Cannot divide \" + 0.0 + \" by zero\"", () -> "Cannot divide " + Double.valueOf(0.0) + " by zero");
  }
}
```
