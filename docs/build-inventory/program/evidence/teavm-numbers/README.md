# Evidence: ExactDecimal against the JDK, nine million values per family (2026-10-10)

Before `third_party/teavm_classlib`'s `ExactDecimal` landed, it was run on the JVM against the JDK at a scale its unit
test (`//third_party/teavm_classlib:tests`, seconds) does not reach. `ExactCheck.java` is the harness, unchanged;
`results.txt` is what three runs printed.

Each run, `ExactCheck <count> <seed>` with count 3,000,000, compared:
- the decimal written for `count` random double bit patterns and `count` random float bit patterns (with the JDK's
  `Double.toString`/`Float.toString`, as digits and exponent), every power of two with its neighbours (double and
  float), every subnormal power of two, and `count` decimals as people write them (up to 5 + 6 digits);
- the value read (double and float) for `count` random decimals of up to 25 digits at exponents -360 to 339; for
  `count / 10` random doubles and as many random floats, the decimal exactly halfway to the next value up and a unit of
  its last digit either side; and the grammar's forms and edges (hex floats, suffixes, whitespace, NaN and Infinity,
  overflow and underflow);
- `BigDecimal.doubleValue`/`floatValue` for `count / 10` random BigDecimals (up to 120 bits, scale -400 to 399).

Seeds 11, 12345 and 99: no difference in any. Run on the pinned JDK 25 (Zulu 25.0.4), Apple Silicon:

```bash
javac -d out third_party/teavm_classlib/src/org/teavm/classlib/impl/text/ExactDecimal.java ExactCheck.java
java -cp out ExactCheck 3000000 11
```
