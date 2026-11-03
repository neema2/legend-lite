// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

import com.legend.testing.SourceFiles;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * THE WRITER CARRIAGE GUARD (docs/RESULT_BOUNDARY_PROGRAM_2026_10_26.md R3, ruled 2026-10-26; built with the start
 * page's S1.4 item 3, approved by the user 2026-10-28: "yes add this to the plan"). Formatting at the edge is
 * serialization, not computation: a writer spells the typed value it was handed, losslessly, in its consumer's
 * layout -- no arithmetic, no comparison, no conversion between kinds, no default for a null, no zone shift, and no
 * spelling chosen by a Pure rule. The test of the rule: swap the writer and every value is unchanged. The writer
 * that compares or computes decides output the plan never typed, and nothing shows it but a judge that happens to
 * cover the case (the midnight rule 6811256b fixed was caught by PCT only by the luck of a fixture).
 *
 * <p>The scan: the result package's sources ({@code core/src/main/java/com/legend/result}, the typed batch, the row
 * decode, the cell texts and the three writers), comments and string literals stripped, searched for the vocabulary of
 * value arithmetic and comparison and of conversion between kinds as Java spells it ({@link #VOCABULARY}). ZERO is
 * allowed. What stands is REGISTERED by name, per file and spelling, exact both ways ({@link #REGISTERED}): each a
 * representation operation -- digits chosen for a value held whole (a Float's shortest digits, a Float held exactly
 * as a decimal read as the double it is, a decimal's plain digits, a token's integral flag, a day's digits), a value's
 * own precision read (a date literal's, carried in the value), a decode's carrier read (a JSON token as the number its
 * slot says). (The one decision by a value's clock that stood here on 2026-10-28, an abstract Date at midnight written
 * as its day, went the next day under the upstream-fidelity tenet.) A new hit is a
 * writer deciding a value: route the decision to the database or to a fact of the plan
 * ({@code ExecutionPlan.Column}: its Pure type, slot, store type and written precision). A row here moves only with a
 * dated justification (AGENTS.md, "Guard files are load-bearing").
 *
 * <p>Limits, stated: a scan catches drift, not an adversary (bare primitive arithmetic and comparison are invisible to
 * it, as the JUDGMENT-ONLY pin in {@code JavaEvalLedgerTest} records of itself; a {@code BigDecimal.add} on a receiver
 * that reads like a list too, since {@code add} is a collection's word here). The dependency half of R3 is
 * {@code ArchitectureTest.writersAreCarriageOnly}.
 */
@Tag("guardrail")
class WriterCarriageGuardTest {

    private static final String RESULT = "core/src/main/java/com/legend/result";

    /** The vocabulary, each match named by its method ({@code group 1}: a number's or a date's instance method --
     *  arithmetic, comparison, scale, a clock's fields, a kind conversion, a precision read), {@code Math.}, a number
     *  built from another kind ({@code new BigDecimal(}), or a static read of one ({@code BigDecimal.valueOf},
     *  {@code Double.parseDouble}, {@code Double.isFinite} ...). */
    static final Pattern VOCABULARY = Pattern.compile(
            "\\.(subtract|multiply|divide|remainder|pow|negate|signum|abs|compareTo|setScale|stripTrailingZeros"
            + "|toBigInteger|toBigIntegerExact|bitLength|doubleValue|longValue|intValue|floatValue|scale"
            + "|getHour|getMinute|getSecond|getNano|getYear|getMonthValue|getDayOfMonth|isBefore|isAfter|plusDays"
            + "|minusDays|atLeast|toInstantFloor|presentedAt|trimmed)\\("
            + "|\\b(Math)\\.\\w+\\("
            + "|\\bnew (BigDecimal|BigInteger)\\("
            + "|\\b(BigDecimal|Double|Float|Long|Integer)\\.(valueOf|parseDouble|parseInt|parseLong|isFinite|isNaN"
            + "|isInfinite)\\(");

    /** What stands, by file and spelling, with its reason; exact both ways (a shrink re-pins, with its date). */
    private static final Map<String, Map<String, Integer>> REGISTERED = Map.of(
            "CellText.java", Map.ofEntries(
                    // a Float's text (R5, the Float rule of 2026-10-26): Double.toString's shortest digits laid out
                    // positionally through a BigDecimal of that text, its mantissa's trailing zero stripped (1.0E-4 is
                    // 0.0001: the user, 2026-10-28, S1.4 item 2); a Float the database holds exactly (a decimal slot
                    // under a Float column) written without trailing zeros. A representation operation: no rounding
                    Map.entry("new BigDecimal", 3),       // the exponent form's layout (1); a BigInteger token's exact value (2)
                    Map.entry("stripTrailingZeros", 2),
                    // the JSON number token: its exact decimal value beside its text (Json.Num), the double's own
                    // (BigDecimal.valueOf, exact) and a token's integral flag (fits a long: bitLength)
                    Map.entry("BigDecimal.valueOf", 3),
                    Map.entry("toBigInteger", 1),
                    Map.entry("bitLength", 1),
                    Map.entry("longValue", 1),
                    // a Float held exactly as a decimal, read as the double it is for legend-engine's Double.toString
                    // token (1), and the Num's double field (1): the declared kind's own value, no other kind
                    Map.entry("doubleValue", 2),
                    // NaN and the infinities have no JSON number token: written as their names in a string
                    // (3 -> 4, 2026-10-29, S1.5's D14: a relation's non-finite Float cell is null in LEGACY, as
                    // legend-engine's convertPurePrimitiveToJson writes it -- CellText.engineCell reads the finiteness
                    // the value carries, the engine's own test)
                    Map.entry("Double.isFinite", 4),
                    // a date's own precision, carried in the value (a day or less; from a minute on, legend-pure's zone
                    // rule, 2026-10-29): Pure's print form and legend-engine's day rule read it, never a clock
                    Map.entry("atLeast", 5),
                    // a value root's instant, and a nested value's, without the fraction's trailing zeros: the value
                    // lane's canon (register S118); a relation's cell keeps its nine digits, untrimmed
                    Map.entry("trimmed", 2),
                    // a day's digits (year, month, day), spelled from the value's floor
                    // (2 -> 1 and the four clock reads getHour, getMinute, getSecond, getNano GONE, 2026-10-29: the
                    // midnight rule, the one decision by a value's clock, deleted under the upstream-fidelity tenet --
                    // neither legend-pure nor legend-engine prints a DateTime as its day)
                    Map.entry("toInstantFloor", 1),
                    Map.entry("getYear", 1),
                    Map.entry("getMonthValue", 1),
                    Map.entry("getDayOfMonth", 1)),
            "ArrowWriter.java", Map.ofEntries(
                    // Arrow's representation of a value held whole (track A, 2026-10-29): a decimal's unscaled digits
                    // AT THE COLUMN'S SCALE (setScale adds zeros to a cell written at fewer places; a cell with more
                    // is refused, never rounded: scale read twice to decide), the 128-bit fit (bitLength); a day as
                    // days since the epoch and an instant as nanoseconds since it (Math.toIntExact, multiplyExact,
                    // addExact: exact, an overflow refused by name); the written subsecond digits and a slot's (p, s)
                    // read as the numbers they spell (Long.parseLong, Integer.parseInt)
                    Map.entry("setScale", 1),
                    Map.entry("scale", 2),
                    Map.entry("bitLength", 1),
                    Map.entry("Math.", 3),
                    Map.entry("Long.parseLong", 2),
                    Map.entry("Integer.parseInt", 2),
                    // a date's own precision, carried in the value: whether it has a time of day (a timestamp's
                    // cell), as CellText reads it (2 until track F, 2026-10-29: the union's member was inferred from
                    // the carrier's precision; the batch's tag names it now, D58 (3))
                    Map.entry("atLeast", 1)),
            "RowDecode.java", Map.ofEntries(
                    // a decode's carrier reads (the plan path program's step 6; moved here 2026-10-27): a JSON token as
                    // the number its slot says, one conversion each, nothing computed -- a long or a double from a
                    // Json.Num; a decimal from its own digits (new BigDecimal of the token, exact) or the double's where
                    // the token is gone; an Integer past a long as a BigInteger, the long otherwise (bitLength); an
                    // integral decimal token under an integer slot as the integer it spells (stripTrailingZeros, scale,
                    // toBigIntegerExact); a Float the wire spells as text (Double.valueOf); a REAL's own four-byte
                    // digits where a host widened it (Double.parseDouble of Float.toString); a tag as the int it is
                    Map.entry("longValue", 7),
                    Map.entry("doubleValue", 3),
                    Map.entry("intValue", 1),
                    Map.entry("new BigDecimal", 3),
                    Map.entry("new BigInteger", 1),
                    Map.entry("BigDecimal.valueOf", 2),
                    Map.entry("bitLength", 1),
                    Map.entry("stripTrailingZeros", 1),
                    Map.entry("scale", 1),
                    Map.entry("toBigIntegerExact", 1),
                    Map.entry("Double.valueOf", 1),
                    Map.entry("Double.isFinite", 1),
                    Map.entry("Double.parseDouble", 1),
                    // a tagged DateTime(n) slot's kind: n, the written subsecond digits the kind carries, restored on the
                    // instant (presentedAt; docs/RESULTS_AND_RENDERERS_2026_10_22.md §5a) -- the data carrying a
                    // precision, read
                    Map.entry("Integer.parseInt", 1),
                    Map.entry("presentedAt", 1)),
            "EngineArrowWriter.java", Map.ofEntries(
                    // legend-engine's ARROW_IPC answer as upstream writes it (2026-11-03, R4 step 3, D17: an edge
                    // writer exactly as upstream's RelationalResultToArrowIPCSerializer): a number narrowed to its
                    // field's width as upstream's setValue narrows it (Number.intValue, longValue, floatValue,
                    // doubleValue; byteValue and shortValue beside them); a decimal at the REPORTED scale, HALF_UP
                    // (setScale: D18 A, a misreported scale's loss copied at the edge), a value of another kind read
                    // from its text as upstream reads it (new BigDecimal of toString); the report's precision and
                    // scale bounded as upstream's jdbcToArrow bounds them (Math.min, Math.max; the report's scale read
                    // there and to pick the encoder) and a whole decimal's scale read for its text; a day as days since
                    // the epoch and an instant as microseconds (Math.toIntExact, multiplyExact, addExact: exact, an
                    // overflow refused by name). 2026-11-03, R4's audit fold: a JSON column's text re-spelled as
                    // upstream's Jackson writes the tree it reads (readTree(text).toString()) -- a whole number within
                    // a long its value's digits (longValue 1 -> 2), one past it its BigInteger's (new BigInteger), a
                    // fraction or an exponent the double Jackson reads (Double.parseDouble), a list's double element
                    // written only when finite (Double.isFinite); and the size of Arrow's copy of the metadata map
                    // (Math.max, 5 -> 6)
                    Map.entry("intValue", 1),
                    Map.entry("longValue", 2),
                    Map.entry("floatValue", 1),
                    Map.entry("doubleValue", 1),
                    Map.entry("setScale", 1),
                    Map.entry("new BigDecimal", 1),
                    Map.entry("new BigInteger", 1),
                    Map.entry("Double.parseDouble", 1),
                    Map.entry("Double.isFinite", 1),
                    Map.entry("scale", 3),
                    Map.entry("Math.", 6)),
            "ColumnReport.java", Map.ofEntries(
                    // a DuckDB type name's precision and scale (DECIMAL(p,s)) read as the numbers they spell, as the
                    // JDBC driver reports them (2026-11-03, R4 step 3: a host's report of its database's columns)
                    Map.entry("Integer.parseInt", 2)),
            "ArrowRows.java", Map.ofEntries(
                    // a decode's carrier reads, the JDBC session decode's arms over a host's Arrow (2026-11-03,
                    // docs/HOST_EXECUTORS_PROGRAM_2026_11_02.md R3, the core's Arrow reader): an Arrow value read as the
                    // value it is -- a decimal's unscaled two's-complement bytes at its type's scale (new BigInteger,
                    // new BigDecimal, the Arrow type's scale), an unsigned 64-bit integer past 63 bits as the
                    // BigInteger it is (new BigInteger of its unsigned digits), a day of milliseconds and an instant
                    // of its unit as its day and its UTC wall clock (Math.floorDiv, floorMod; 5 -> 6 the same day,
                    // 2026-11-03 by R3's audit: a day of milliseconds checked whole, floorMod), a large text's 64-bit
                    // offsets as indices (Math.toIntExact: exact, an overflow refused) -- then each carrier
                    // conversion the JDBC decode makes, one each: an Integer past a long as a BigInteger, the long
                    // otherwise (bitLength, longValue); an integral decimal (DuckDB's HUGEINT) as the integer it
                    // spells (stripTrailingZeros, scale, toBigIntegerExact); a Float from a decimal or a BigInteger
                    // (doubleValue); a REAL's own four-byte digits (Double.parseDouble of Float.toString); a Decimal
                    // from an integer or a double (BigDecimal.valueOf, new BigDecimal); a text's digits read as the
                    // number a numeric Pure type says (Double.valueOf, new BigDecimal, new BigInteger); a tag as the
                    // int it is (intValue). 2026-11-03, D20 (b), the columnar fills, each the same read again in the
                    // column's own loop: an unsigned 64-bit value past 63 bits (new BigInteger 3 -> 4), a 128-bit
                    // decimal that fits 64 bits as the BigDecimal of its long at the type's scale (BigDecimal.valueOf
                    // 3 -> 4; the type's scale read to choose the fill, scale 2 -> 4), a text's 64-bit offsets as
                    // indices and the batch's cut (Math.toIntExact twice, Math.min: Math. 6 -> 9)
                    Map.entry("new BigInteger", 4),
                    Map.entry("new BigDecimal", 5),
                    Map.entry("scale", 4),
                    Map.entry("Math.", 9),
                    Map.entry("bitLength", 3),
                    Map.entry("longValue", 3),
                    Map.entry("stripTrailingZeros", 1),
                    Map.entry("toBigIntegerExact", 1),
                    Map.entry("doubleValue", 2),
                    Map.entry("Double.parseDouble", 3),
                    Map.entry("BigDecimal.valueOf", 4),
                    Map.entry("Double.valueOf", 1),
                    Map.entry("intValue", 1)));

    @Test
    void theWritersCarryAndDecideNothing() throws IOException {
        List<Path> files = new java.util.ArrayList<>(
                SourceFiles.under(RESULT).stream().filter(p -> p.toString().endsWith(".java")).toList());
        // track Z, 2026-10-29 (the final audit's finding 2): the service test runner spelled a program's last side
        // itself; since door 1 (2026-10-29) it reads its answer from the boundary's writers -- scanned here with an
        // empty register so that it never spells a cell again
        files.add(SourceFiles.file("core/src/main/java/com/legend/test/ServiceTestRunner.java"));
        GuardCoverage.assertFloor("WriterCarriageGuardTest", files.size(), 10);
        StringBuilder drift = new StringBuilder();
        for (Path f : files) {
            String name = f.getFileName().toString();
            Map<String, Integer> found = new TreeMap<>();
            Matcher m = VOCABULARY.matcher(stripped(Files.readString(f)));
            while (m.find()) {
                found.merge(spelling(m), 1, Integer::sum);
            }
            Map<String, Integer> registered = REGISTERED.getOrDefault(name, Map.of());
            for (var e : found.entrySet()) {
                int pinned = registered.getOrDefault(e.getKey(), 0);
                if (e.getValue() > pinned) {
                    drift.append("\n  ").append(name).append(": ").append(e.getKey()).append(" x").append(e.getValue())
                            .append(pinned == 0 ? " -- a writer deciding a value (R3): route it to the database or to a"
                                    + " fact of the plan; a representation operation registers by name, with its reason"
                                    : " > " + pinned + " registered -- grew: the same");
                } else if (e.getValue() < pinned) {
                    drift.append("\n  ").append(name).append(": ").append(e.getKey()).append(" x").append(e.getValue())
                            .append(" < ").append(pinned).append(" registered -- shrank: re-pin, with the date");
                }
            }
            for (var e : registered.entrySet()) {
                if (!found.containsKey(e.getKey())) {
                    drift.append("\n  ").append(name).append(": ").append(e.getKey()).append(" registered x")
                            .append(e.getValue()).append(" and GONE -- delete its row");
                }
            }
        }
        for (String name : REGISTERED.keySet()) {
            if (files.stream().noneMatch(p -> p.getFileName().toString().equals(name))) {
                drift.append("\n  ").append(name).append(" is registered and GONE -- delete its rows");
            }
        }
        assertTrue(drift.length() == 0, "the writers are carriage only (R3) -- drift:" + drift);
    }

    /** The match's spelling as the register names it. */
    static String spelling(Matcher m) {
        if (m.group(1) != null) {
            return m.group(1);
        }
        if (m.group(2) != null) {
            return "Math.";
        }
        if (m.group(3) != null) {
            return "new " + m.group(3);
        }
        return m.group(4) + "." + m.group(5);
    }

    /** The source without its comments and string literals (a word inside either is no call). */
    static String stripped(String source) {
        return source.replaceAll("(?s)/\\*.*?\\*/", "")
                .replaceAll("//.*", "")
                .replaceAll("\"(?:\\\\.|[^\"\\\\])*\"", "\"\"");
    }
}
