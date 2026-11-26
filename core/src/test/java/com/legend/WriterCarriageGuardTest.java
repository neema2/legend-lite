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
 * decode, the cell texts and the three writers) and exec's in every source tree ({@link #EXEC}: the driver decodes and
 * the statements, E2, 2026-11-12), comments and string literals stripped, searched for the vocabulary of
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

    /** Exec's decoders and statements, in each source tree that holds them (E2, 2026-11-12: G3's guard, "the writer
     *  guard extended to exec's decode"; until then only {@link #RESULT} was scanned, and the decoders' zone shift, era
     *  repair and kind-from-text stood unseen). */
    private static final List<String> EXEC = List.of("core/src/main/java/com/legend/exec",
            "core/src/main/mongodb/com/legend/exec", "core/src/main/duckdb/com/legend/exec",
            "core/src/main/postgres/com/legend/exec");

    /** The vocabulary, each match named by its method ({@code group 1}: a number's or a date's instance method --
     *  arithmetic, comparison, scale, a clock's fields, a kind conversion, a precision read), {@code Math.}, a number
     *  built from another kind ({@code new BigDecimal(}), or a static read of one ({@code BigDecimal.valueOf},
     *  {@code Double.parseDouble}, {@code Double.isFinite} ...). */
    static final Pattern VOCABULARY = Pattern.compile(
            "\\.(subtract|multiply|divide|remainder|pow|negate|signum|abs|compareTo|setScale|stripTrailingZeros"
            + "|toBigInteger|toBigIntegerExact|bitLength|doubleValue|longValue|intValue|floatValue|scale"
            + "|getHour|getMinute|getSecond|getNano|getYear|getMonthValue|getDayOfMonth|isBefore|isAfter|plusDays"
            + "|minusDays|atLeast|toInstantFloor|presentedAt|trimmed"
            // the decoder's words (E2, 2026-11-12, docs/LEAN_PRODUCT_P3_2026_11_10.md §8: the vocabulary written for
            // writers missed a decoder's zone shift, era repair and kind-from-text): a zone or a calendar moved
            + "|withOffsetSameInstant|withOffsetSameLocal|withZoneSameInstant|withYear|withNano|atZone|atOffset)\\("
            + "|\\b(Math)\\.\\w+\\("
            + "|\\bnew (BigDecimal|BigInteger)\\("
            + "|\\b(BigDecimal|Double|Float|Long|Integer)\\.(valueOf|parseDouble|parseInt|parseLong|isFinite|isNaN"
            + "|isInfinite)\\("
            // a calendar's fields, and a REAL respelled through its four-byte shortest text (E2, 2026-11-12)
            + "|\\b(Calendar\\.|Float\\.toString\\()"
            // a text tested against a literal (comments and literals are stripped first, so the literal reads ""):
            // a kind chosen from a cell's text is one (E2, 2026-11-12); a plan fact's own name (a slot word, a Pure
            // type's path) tested is registered by name as one
            + "|\\.(startsWith|endsWith|contains|indexOf)\\(\"\"\\)");

    /** What stands, by file and spelling, with its reason; exact both ways (a shrink re-pins, with its date). */
    private static final Map<String, Map<String, Integer>> REGISTERED = Map.ofEntries(Map.entry(
            "CellText.java", Map.ofEntries(
                    // a Float's text (R5, the Float rule of 2026-10-26): Double.toString's shortest digits laid out
                    // positionally through a BigDecimal of that text, its mantissa's trailing zero stripped (1.0E-4 is
                    // 0.0001: the user, 2026-10-28, S1.4 item 2); a Float the database holds exactly (a decimal slot
                    // under a Float column) written without trailing zeros. A representation operation: no rounding
                    Map.entry("new BigDecimal", 3),       // the exponent form's layout (1); a BigInteger token's exact value (2)
                    Map.entry("stripTrailingZeros", 2),
                    // the double's own decimal (BigDecimal.valueOf, exact)
                    // 3 -> 2 (2026-11-16, W8 of docs/records/d-plan-2026-11-16.md: legend-engine's tokens and legend-pure's date text moved to edge.EngineCells (edge.Presenter since Phase 3 of the edge program, 2026-11-16), registered below)
                    Map.entry("BigDecimal.valueOf", 2),
                    // toBigInteger, bitLength, longValue and doubleValue GONE (2026-11-18, J36a of the edge audit:
                    // CellText.number, a JSON number token's exact value and integral flag, whose one caller was the
                    // edge's Presenter, is the JSON library's Json.Num.ofToken -- the Num's own constructor, outside the
                    // writers this register reads)
                    // NaN and the infinities have no JSON number token: written as their names in a string
                    // (3 -> 4, 2026-10-29, S1.5's D14: a relation's non-finite Float cell is null in LEGACY, as
                    // legend-engine's convertPurePrimitiveToJson writes it -- CellText.engineCell reads the finiteness
                    // the value carries, the engine's own test)
                    // 4 -> 2 (2026-11-16, W8 of docs/records/d-plan-2026-11-16.md: legend-engine's tokens and legend-pure's date text moved to edge.EngineCells (edge.Presenter since Phase 3 of the edge program, 2026-11-16), registered below)
                    Map.entry("Double.isFinite", 2),
                    // a date's own precision, carried in the value (a day or less; from a minute on, legend-pure's zone
                    // rule, 2026-10-29): Pure's print form and legend-engine's day rule read it, never a clock
                    // 5 -> 7 (2026-11-16, W4's audit): legend-pure's date text for the pure/v1 Arrow route
                    // (CellText.DateText.LEGEND_PURE) beside the product's, each reading the value's own precision
                    // 7 -> 3, and trimmed 2 GONE (2026-11-16, W8 of docs/records/d-plan-2026-11-16.md: legend-engine's tokens and legend-pure's date text moved to edge.EngineCells (edge.Presenter since Phase 3 of the edge program, 2026-11-16), registered below)
                    Map.entry("atLeast", 3),
                    // a day's digits (year, month, day), spelled from the value's floor
                    // (2 -> 1 and the four clock reads getHour, getMinute, getSecond, getNano GONE, 2026-10-29: the
                    // midnight rule, the one decision by a value's clock, deleted under the upstream-fidelity tenet --
                    // neither legend-pure nor legend-engine prints a DateTime as its day)
                    Map.entry("toInstantFloor", 1),
                    Map.entry("getYear", 1),
                    Map.entry("getMonthValue", 1),
                    Map.entry("getDayOfMonth", 1),
                    // text tests (E2, 2026-11-12, the vocabulary extended): a slot's own word (a structure's closing
                    // '>'), a pivot column's name holding the separator and its quotes (D1's edge, P3-R5), and a
                    // Float's own digits laid out (the '.' a whole double's text gains)
                    // contains 3 -> 2, endsWith 2 -> 1, startsWith 1 GONE (2026-11-16, W8 of docs/records/d-plan-2026-11-16.md: legend-engine's tokens and legend-pure's date text moved to edge.EngineCells (edge.Presenter since Phase 3 of the edge program, 2026-11-16), registered below: the pivot name's quotes)
                    Map.entry("contains(\"\")", 2),
                    Map.entry("endsWith(\"\")", 1))),
            // renamed from EngineCells.java (2026-11-16, Phase 3 of docs/records/total-wall-edge-program-2026-11-16.md: the
            // presenter absorbs EngineCells; a file moving, no pin changed)
            Map.entry("Presenter.java", Map.ofEntries(
                    // legend-engine's and legend-pure's text of a cell, moved from CellText (W8 of
                    // docs/records/d-plan-2026-11-16.md, 2026-11-16), the same representation operations: a BigInteger
                    // token's exact value and the double's own (exact) beside its text (Json.Num); a Float held exactly as
                    // a decimal read as the double it is for Double.toString's token; a NaN or an infinity, which has no
                    // JSON number token (null in a relation's cell, its name nested); a date's own precision (legend-
                    // engine's day rule, legend-pure's zone from a minute on); a value at its written precision without
                    // the fraction's trailing zeros (legend-pure's print form, register S118); a pivot column's name
                    // holding the separator and its quotes
                    Map.entry("new BigDecimal", 2),
                    Map.entry("BigDecimal.valueOf", 1),
                    Map.entry("doubleValue", 1),
                    Map.entry("Double.isFinite", 2),
                    // 4 -> 5 (2026-11-16, W8: legend-engine's JSON of a date, PureDateLiteral.toEngineJson until then)
                    Map.entry("atLeast", 5),
                    Map.entry("trimmed", 2),
                    Map.entry("contains(\"\")", 1),
                    Map.entry("endsWith(\"\")", 1),
                    Map.entry("startsWith(\"\")", 1))),
            // added 2026-11-21 (stage B's BE, the one catalog, the user's ruling that day: upstream's sentence wherever
            // upstream has one): a planned raise's sentence prints each operand as legend-pure's sentence prints it,
            // the operations legend-pure's own printing performs and the edge's PureMessages performed until then (its
            // helpers deleted) -- a Float's plain digits (BigDecimal.valueOf, exact: a PLAIN slot), a Decimal read as
            // the Float a FLOAT slot prints (doubleValue), a date's own precision from a minute on (atLeast: legend-
            // pure's +0000). It decides nothing: the kind's form names the slot. 2026-11-21 (the BE audit 2's S2): a
            // fractional second's whole part, as legend-pure validates it (toBigInteger, exact, of a Float's own decimal
            // -- BigDecimal.valueOf -- and of a Decimal), in place of its plain digits. 2026-11-21 (the BE audit 3's S3):
            // that whole part is a Decimal's intValue, as upstream's CoreHelper.newDate takes it (a Double's is Java's
            // narrowing, no operation listed here) -- BigDecimal.valueOf and toBigInteger gone
            // added 2026-11-24 (stage B's B2, B2b S1: legend-pure's text of a relation written in Java from typed rows at
            // the edge, the plan of record's B2 and the user's ruling of 2026-11-19 ~18:12, "Yes record it"): the
            // representation operations legend-pure's own printing performs, each read from the pinned sources -- a
            // date's own carried precision, its day or its time of day (s.pure's toString: atLeast; toCSV's hasHour is
            // read from the value's own kind); the escaping of a text by what the text holds (s.pure quotes a String
            // holding a brace or a bracket, helperFunctions.pure's escapeCSVString one holding a comma, a quote, a line
            // feed or a carriage return: contains), and a column name already quoted (toString.pure:28: startsWith); a
            // Float column's cell read as the double the column declares, whatever kind the database held it in (an
            // integer, a decimal an aggregate collapsed to: doubleValue, as the edge's SQL cast it to DOUBLE and
            // legend-engine's TDS reads it), and a Number column's decimal as the Float it prints as. 2026-11-24, the
            // independent review of S1: the Float print became legend-pure's own (CompiledSupport's
            // DecimalFormat("0.0")), and an integer under a Float is the double, not the exact decimal (new BigDecimal
            // gone, doubleValue 5). It decides nothing about a value: the database computed each one
            // 2026-11-25, B2b S6a (the orchestrator's agreement, G1: one edge value printer): the date's precision read
            // (atLeast 1) and the Float cell's kind conversions (doubleValue 5) MOVED, unchanged, with the cell rules to
            // PureValueText -- a move, no growth: the two files together hold what PureRelationText held
            Map.entry("PureRelationText.java", Map.ofEntries(
                    Map.entry("contains(\"\")", 6),
                    Map.entry("startsWith(\"\")", 1))),
            // legend-pure's text of a value, the edge's value printer (B2b S6a's legend-pure door, 2026-11-25): a
            // relation's cell, the rules moved from PureRelationText (above); a last step's Float print, lifted by S6a,
            // held with the edge's value passes (the design audit of 2026-11-25; its pureFloat reused, no count moved)
            Map.entry("PureValueText.java", Map.ofEntries(
                    Map.entry("atLeast", 1),
                    Map.entry("doubleValue", 5))),
            // the tail of a lifted print (the edition program's step 8, 2026-11-26; the user's words that day, "maybe
            // small set up string/count functions chained should all be in java"): indexOf's start read as an int, as
            // legend-pure's own Java reads it (CompiledSupport.indexOf: from.intValue()) -- the program's literal, not
            // a value the database computed
            Map.entry("PrintTail.java", Map.of("intValue", 1)),
            Map.entry("RaiseWords.java", Map.ofEntries(
                    Map.entry("intValue", 1),
                    Map.entry("doubleValue", 1),
                    Map.entry("atLeast", 1))),
            Map.entry("ArrowWriter.java", Map.ofEntries(
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
                    Map.entry("atLeast", 1),
                    // text tests (E2, 2026-11-12, the vocabulary extended): the plan's Pure type's own name (Decimal,
                    // DateTime with its precision, a class's path) and a decimal's own digits' sign
                    Map.entry("startsWith(\"\")", 3),
                    Map.entry("contains(\"\")", 1))),
            Map.entry("RowDecode.java", Map.ofEntries(
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
                    Map.entry("presentedAt", 1),
                    // E2, 2026-11-12 (the vocabulary extended): the REAL's Float.toString beside its Double.parseDouble
                    // above (site 1, P3-R1, after D1); text tests of the plan's Pure type and slot names (Decimal,
                    // DateTime(n), a structure's slot word), and a Float literal token's own D suffix
                    Map.entry("Float.toString", 1),
                    Map.entry("startsWith(\"\")", 4),
                    Map.entry("endsWith(\"\")", 3))),
            // EXEC (E2, 2026-11-12, G3's guard: today's hits, each registered by name; the P3 items take theirs with
            // them -- site 1 the Float.toString and Double.parseDouble, site 2 the setScale, scale and Integer.parseInt,
            // site 4 the getYear, withYear and Calendar)
            Map.entry("JdbcDecode.java", Map.ofEntries(
                    // carrier reads: a driver's number as the carrier its slot says, one conversion each (a long from
                    // a BigInteger that fits, bitLength; an integral decimal as its integer; a decimal from a long, a
                    // double or its own digits; a Float slot's decimal as the double it is)
                    Map.entry("BigDecimal.valueOf", 4),
                    Map.entry("new BigDecimal", 3),
                    Map.entry("new BigInteger", 1),
                    Map.entry("bitLength", 3),
                    Map.entry("longValue", 3),
                    Map.entry("doubleValue", 2),
                    Map.entry("Double.valueOf", 1),
                    Map.entry("stripTrailingZeros", 1),
                    Map.entry("toBigIntegerExact", 1),
                    Map.entry("intValue", 1),
                    // SQLite's date-time text read as the database's timestamp (the floor of its instant)
                    Map.entry("toInstantFloor", 1),
                    // site 1 (a REAL's four-byte shortest text, P3-R1): after D1
                    Map.entry("Float.toString", 5),
                    Map.entry("Double.parseDouble", 3),
                    // site 2 (BigQuery's dropped decimal zeros padded to the slot's scale, P3-R2): after D1
                    Map.entry("setScale", 1),
                    Map.entry("scale", 2),
                    Map.entry("Integer.parseInt", 1),
                    // site 4 (a BC year's era repair, P3-R4): after D1
                    Map.entry("getYear", 2),
                    Map.entry("withYear", 2),
                    Map.entry("Calendar.", 1),
                    // text tests of the plan's slot and Pure type names (DECIMAL(p,s), Decimal(p,s)), never a cell
                    Map.entry("startsWith(\"\")", 2),
                    Map.entry("endsWith(\"\")", 1))),
            Map.entry("MongoDbDecode.java", Map.ofEntries(
                    // MongoDB's carriers (a BSON number as its slot's carrier; Decimal128 as its exact decimal, its
                    // integral form as its integer; a negative zero's sign read, negate)
                    Map.entry("BigDecimal.valueOf", 4),
                    Map.entry("longValue", 4),
                    Map.entry("scale", 2),
                    Map.entry("stripTrailingZeros", 2),
                    Map.entry("toBigIntegerExact", 2),
                    Map.entry("bitLength", 1),
                    Map.entry("doubleValue", 1),
                    Map.entry("intValue", 1),
                    Map.entry("negate", 1),
                    Map.entry("new BigDecimal", 1),
                    Map.entry("new BigInteger", 1),
                    Map.entry("Double.valueOf", 1),
                    // the plan's Pure type name (Decimal(p,s))
                    Map.entry("startsWith(\"\")", 1))),
            Map.entry("PlanParameters.java", Map.ofEntries(
                    // a parameter's checks before it binds: a year outside 1-9999 refused, a non-finite Float refused,
                    // a decimal's negative scale written as its plain digits, a multiplicity's upper bound read
                    Map.entry("getYear", 4),
                    Map.entry("Double.isFinite", 1),
                    Map.entry("Integer.valueOf", 1),
                    Map.entry("scale", 1),
                    Map.entry("setScale", 1))),
            // a raised error's planned position (line:column) read from the sentinel-delimited message the plan wrote
            Map.entry("RaisedErrors.java", Map.of("Integer.parseInt", 2)),
            // a list parameter's date-time finer than its database keeps refused, never cut (S181)
            Map.entry("JdbcStatements.java", Map.of("getNano", 1)),
            // a date-time parameter finer than milliseconds refused (S181) (Integer.parseInt x1 GONE 2026-11-12: the
            // server's version check deleted by the C final audit's J3)
            Map.entry("MongoDbStatements.java", Map.of("getNano", 1)),
            // a source's scheme (file:), a location, never a cell
            Map.entry("SourceAdmission.java", Map.of("startsWith(\"\")", 1)),
            // a reported decimal's scale floored at zero; a reported type NAME tested for a zone (E1 item 3)
            Map.entry("WireTypes.java", Map.of("Math.", 1, "startsWith(\"\")", 1, "contains(\"\")", 1,
                    "endsWith(\"\")", 2)));

    @Test
    void theWritersCarryAndDecideNothing() throws IOException {
        List<Path> files = new java.util.ArrayList<>(
                SourceFiles.under(RESULT, EXEC.toArray(String[]::new)).stream()
                        .filter(p -> p.toString().endsWith(".java")).toList());
        // track Z, 2026-10-29 (the final audit's finding 2): the service test runner spelled a program's last side
        // itself; since door 1 (2026-10-29) it reads its answer from the boundary's writers -- scanned here with an
        // empty register so that it never spells a cell again
        files.add(SourceFiles.file("core/src/main/java/com/legend/edge/ServiceTestRunner.java"));
        // legend-engine's PURE_TDSOBJECT writer, moved from result/ to the edge with its runner (D1.4, 2026-11-15)
        files.add(SourceFiles.file("core/src/main/java/com/legend/edge/TdsObjectsWriter.java"));
        // legend-engine's pure/v1 writer and its receivers, and legend-engine's and legend-pure's cell text, moved from
        // result/ to the edge (W8 of docs/records/d-plan-2026-11-16.md, 2026-11-16): scanned where they live
        files.add(SourceFiles.file("core/src/main/java/com/legend/edge/PureV1Writer.java"));
        files.add(SourceFiles.file("core/src/main/java/com/legend/edge/EngineAnswers.java"));
        files.add(SourceFiles.file("core/src/main/java/com/legend/edge/Presenter.java"));
        // legend-pure's text of a relation written in Java from typed rows, and its run receiver (stage B's B2, B2b S1,
        // 2026-11-24): scanned with the writers they are
        files.add(SourceFiles.file("core/src/main/java/com/legend/edge/PureRelationText.java"));
        files.add(SourceFiles.file("core/src/main/java/com/legend/edge/PureRelationTextWriter.java"));
        // the edge's value printer, the cell rules moved out of PureRelationText (B2b S6a, 2026-11-25)
        files.add(SourceFiles.file("core/src/main/java/com/legend/edge/PureValueText.java"));
        // the tail of a lifted print (the edition program's step 8, 2026-11-26): scanned with the printers it follows
        files.add(SourceFiles.file("core/src/main/java/com/legend/edge/PrintTail.java"));
        // 10 -> 50 (E2, 2026-11-12): exec's sources joined the scan
        GuardCoverage.assertFloor("WriterCarriageGuardTest", files.size(), 50);
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
        if (m.group(4) != null) {
            return m.group(4) + "." + m.group(5);
        }
        if (m.group(6) != null) {
            return m.group(6).endsWith("(") ? m.group(6).substring(0, m.group(6).length() - 1) : m.group(6);
        }
        return m.group(7) + "(\"\")";
    }

    /** The source without its comments and string literals (a word inside either is no call). */
    static String stripped(String source) {
        return source.replaceAll("(?s)/\\*.*?\\*/", "")
                .replaceAll("//.*", "")
                .replaceAll("\"(?:\\\\.|[^\"\\\\])*\"", "\"\"");
    }
}
