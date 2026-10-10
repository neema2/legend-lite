# TeaVM's class library, corrected where lite needs it exact

The browser build (`//wasm:planner` and every other `teavm_wasm` target) is TeaVM's compile of lite's Java. TeaVM
cannot compile the JDK's own class library, so it ships its own rewrite, and the conformance test
(`//wasm:conformance_test`, against the JDK) found that rewrite answering differently where lite needs it exact. This
package corrects the number conversions (PARK-23; `docs/WEB_IMAGE_SPIKE_2026_10_10.md`, step 2). `teavm_wasm`
(`//tools/teavm/defs.bzl`) puts this package's jar first on TeaVM's class path, so these classes are the ones compiled.

## What is here

| File | From | What changed |
|---|---|---|
| `src/.../impl/text/ExactDecimal.java` | new | The decimal `Double.toString`/`Float.toString` write and the value `Double.valueOf`/`Float.valueOf` read, exactly as the Java SE specification says |
| `src/.../java/lang/TAbstractStringBuilder.java` | TeaVM 0.15.0 | `append(float)` and `append(double)` take their digits from `ExactDecimal` (`FloatAnalyzer`/`DoubleAnalyzer` could choose a longer decimal or the other last digit) |
| `src/.../java/lang/TDouble.java` | TeaVM 0.15.0 | `parseDouble` reads through `ExactDecimal` (it kept 19 digits and dropped the rest; `DoubleSynthesizer` could land a unit off; hex floats were refused) |
| `src/.../java/lang/TFloat.java` | TeaVM 0.15.0 | `parseFloat` likewise, rounded once from the text |
| `src/.../java/math/TBigDecimal.java` | TeaVM 0.15.0 | `equals` compares bit lengths first (`0` equalled a large value); `dividePrimitiveLongs`'s half-way test no longer overflows past 2^62; `precision()` and the rounding's digit counts are exact (the string constructor stored a miscounted precision, `-0.000123` counting 7), and `inplaceRound` no longer skips a value its lower-bound estimate undercounts; `doubleValue`/`floatValue` are the nearest value |

The four TeaVM files are TeaVM 0.15.0's own (`teavm-classlib-0.15.0-sources.jar`, SHA-1
`219bbc2b52db85becfab06140f042c6925a53258`), Apache 2.0: their headers are kept, each carries a notice of what changed,
and `NOTICE` is TeaVM's. The rest of each file is as TeaVM wrote it (one Error Prone check is off for the package, for
TeaVM's `a != a` NaN test).

## Clean room

`ExactDecimal` was written from the Java SE 25 API specification of the four methods, IEEE 754's round-to-nearest-even,
Giulietti, "The Schubfach way to render doubles" (2020, for how the interval that rounds to a value is set out) and
Clinger, "How to read floating point numbers accurately" (PLDI 1990) -- never from OpenJDK's code (GPL 2 with the
Classpath Exception) or any other implementation's. The error messages are the JDK's as observed from outside. Its tests
compare with the JDK's behaviour; their inputs are generated, not taken from OpenJDK's tests.

## How it is held

- `:tests` (JVM): `ExactDecimal` against the JDK -- random bits, every power of two, decimals as people write them,
  random decimals of up to 25 digits, the decimals exactly halfway between neighbours and a hair either side, the
  specification's grammar with its edge cases, `BigDecimal.doubleValue`/`floatValue` -- and the corrected `TBigDecimal`
  (plain Java, so it runs on the JVM) against `java.math.BigDecimal`: precision and rounding to a precision, both
  divisions, `equals`, the conversions, at every long extreme and each digit count's edge. Before landing, the same
  comparison ran three times at nine million values per family without a difference
  (`docs/build-inventory/program/evidence/teavm-numbers/`).
- `//wasm:conformance_test` (the compiled module): every number family it probes (a double's and a float's text both
  ways, BigDecimal) equal to the JDK; what still differs elsewhere is `//wasm:conformance-known.tsv`, which only
  shrinks.

## Upstream, and when this goes

Each fix is offered to TeaVM under its own licence (TeaVM issue #735 is the double round trip). When a TeaVM release
carries a fix, its file here is deleted in the same change as the version bump; when none is left, so is the package
and `teavm_wasm`'s `_classlib_fixes`. A TeaVM bump must re-port these files onto the new version's sources first: they
replace TeaVM's classes whole.
