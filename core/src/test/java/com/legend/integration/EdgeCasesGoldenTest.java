// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.integration;

import static org.junit.jupiter.api.Assertions.assertFalse;

import com.legend.database.PlanStyle;
import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.testbackend.StorelessRuntime;
import com.legend.testbackend.TestBackend;
import com.legend.testbackend.TestBackends;
import com.legend.testing.Lean;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * THE EDGE GOLDEN NET's MISSING CASES (S0 of the presentation program, 2026-11-18,
 * docs/records/presentation-program-design-2026-11-18.md §4 (d)): the texts the PR #118 audit found wrong or
 * unwatched (V-EDGE), each answered on DuckDB and H2 under both of the edge's styles and written under {@code
 * -Dlegend.edge.cases=<file>}, so a later slice that moves one shows it as a listed move:
 * <ul>
 *   <li>EDGE-1: a Float cell in a relation's {@code toString} and in a TDS's {@code toCSV}, at the five values the
 *       audit measured (fixed by F7 in the fix round: the net pins the fix; since B2b S5a, 2026-11-24, a last step's
 *       text is the doors' Java printer's, as the doors answer it);</li>
 *   <li>EDGE-2: a DateTime in a TDS's {@code toCSV} (upstream's {@code yyyy-MM-dd HH:mm:ss} since B2b S5a, the doors'
 *       printer: a correction, "(b) needs no ruling", the user's answer relayed 2026-11-25; the edge's SQL wrote ISO
 *       text until then);</li>
 *   <li>EDGE-3: {@code format('%r', ...)} of a string holding a newline (raw today, upstream's {@code \n}: pinned as
 *       it is).</li>
 *   <li>STEP 8's (2026-11-26, the audit's re-check of slice 2: one recorded answer per case per database, where a
 *       test had accepted any of several): {@code tail.*}, the calls of a print's tail ({@code edge.PrintTail}) as
 *       each database's own SQL answers them over a text, on the inputs where Java -- legend-pure's answer,
 *       {@code PrintTailTest} -- and a database part (a character above U+FFFF, an empty target, a control character
 *       in {@code trim}, a start past an int) and some where they must not (an empty needle, an empty text, a needle
 *       that a pattern would read specially); and {@code float.*}, a Float the DATABASE computes, printed at a last step: legend-pure's print
 *       in Java under the edge's legend-pure style, whose door lifts it, and the database's own text under
 *       legend-engine's -- a negative zero, the largest and the smallest doubles, the exponent-form boundaries, a
 *       whole value, an overflow and a division by zero as each database answers them.</li>
 * </ul>
 * Since that day the net runs on every database the platform runs in-process (SQLite joined DuckDB and H2), and a
 * last step is lifted as each style's own door lifts it: a value's print at legend-pure's alone.
 * A case that fails is recorded with its message: the net pins what the edge answers, not what it should.
 */
class EdgeCasesGoldenTest {

    private static final String CSV = "->meta::relational::tests::csv::toCSV()";

    /** name -> the query. */
    private static final Map<String, String> CASES = new LinkedHashMap<>();

    static {
        for (String v : List.of("0.00001", "0.0005", "25000000.0", "10000000000000000.0", "100000000000000000000.0")) {
            CASES.put("edge1.toString." + v, "|#TDS\n  x:Float\n  " + v + "\n#->toString()");
            CASES.put("edge1.toCSV." + v, "|#TDS\n  x:Float\n  " + v + "\n#" + CSV);
        }
        CASES.put("edge2.toCSV.dateTime", "|#TDS\n  id:Integer, at:DateTime\n  1, 2024-01-02T10:30:00.123+0000\n#" + CSV);
        CASES.put("edge3.formatRepr.newline", "|format('%r', ['a\\nb'])");
        // step 8 (2026-11-26): the tail's calls over a text, in each database's own SQL
        String astral = "a\uD83D\uDE00b";
        CASES.put("tail.length.aboveFFFF", "|'" + astral + "'->length()");
        CASES.put("tail.indexOf.aboveFFFF", "|'" + astral + "'->indexOf('b')");
        CASES.put("tail.replace.emptyTarget", "|'abc'->replace('', 'x')");
        CASES.put("tail.trim.controlCharacters", "|'\\ta\\n'->trim()->length()");
        CASES.put("tail.indexOf.fromPastAnInt", "|'abcb'->indexOf('b', 2147483648)");
        CASES.put("tail.indexOf.emptyNeedle", "|'abc'->indexOf('')");
        CASES.put("tail.startsWith.emptyNeedle", "|'abc'->startsWith('')");
        CASES.put("tail.startsWith.trailingSpace", "|'a'->startsWith('a ')");
        CASES.put("tail.endsWith.trailingSpace", "|'a'->endsWith('a ')");
        CASES.put("tail.replace.aDot", "|'a.b'->replace('.', 'x')");
        CASES.put("tail.contains.aDollar", "|'a$b'->contains('$')");
        CASES.put("tail.contains.aPercent", "|'a%b'->contains('%')");
        CASES.put("tail.startsWith.anUnderscore", "|'ab'->startsWith('_')");
        CASES.put("tail.indexOf.emptyNeedlePastTheEnd", "|'abc'->indexOf('', 5)");
        CASES.put("tail.endsWith.emptyNeedle", "|'abc'->endsWith('')");
        CASES.put("tail.contains.emptyNeedle", "|'abc'->contains('')");
        CASES.put("tail.indexOf.fromPastTheEnd", "|'abcb'->indexOf('b', 99)");
        CASES.put("tail.indexOf.fromBeforeTheStart", "|'abcb'->indexOf('b', -5)");
        CASES.put("tail.emptyText.length", "|''->length()");
        CASES.put("tail.emptyText.replaceEmpty", "|''->replace('', 'x')");
        CASES.put("tail.emptyText.indexOfEmpty", "|''->indexOf('')");
        CASES.put("tail.emptyText.contains", "|''->contains('a')");
        CASES.put("tail.emptyText.trim", "|''->trim()->length()");
        // step 8: a Float the database computes, printed at a last step
        CASES.put("float.negativeZero", "|(-1.0 * 0.0)->toString()");
        CASES.put("float.largest", "|(1.7976931348623157E308 * 1.0)->toString()");
        CASES.put("float.smallest", "|(4.9E-324 * 1.0)->toString()");
        CASES.put("float.1e21", "|(1.0E21 * 1.0)->toString()");
        CASES.put("float.1e-7", "|(1.0E-7 * 1.0)->toString()");
        CASES.put("float.whole", "|(2.0 * 3.0)->toString()");
        CASES.put("float.fromAnInteger", "|(3->toFloat() * 1)->toString()");
        CASES.put("float.overflow", "|(1.7976931348623157E308 * 10.0)->toString()");
        CASES.put("float.zeroByZero", "|(0.0 / 0.0)->toString()");
        CASES.put("float.negativeLiteral", "|(-1.3421e-8)->toString()");
        // Java's own switch to and from the exponent form, a negative overflow, and a Float computed inside a
        // relation's print and one a cell holds as a whole number (the review of the re-check's fixes)
        CASES.put("float.1e7", "|(1.0E7 * 1.0)->toString()");
        CASES.put("float.1e-3", "|(1.0E-3 * 1.0)->toString()");
        CASES.put("float.negativeOverflow", "|(-1.7976931348623157E308 * 10.0)->toString()");
        CASES.put("float.inARelation.computed",
                "|#TDS\n  x:Float\n  0.1\n#->extend(~y: r|$r.x->toOne() * 3.0)->toString()");
        CASES.put("float.inARelation.whole", "|#TDS\n  x:Float\n  1000000\n#->toString()");
        // the texts of a Float and of a DateTime that are lite's own at the doors since step 8's deletions (the
        // outside audit of slices 3 and 4, 2026-11-26: each move held as it is, for the register's amendments) --
        // format's directives and a table's text used further; and a Float's toRepresentation, which is lifted
        CASES.put("float.format.r", "|format('%r', [(1.0 / 3.0)])");
        CASES.put("float.format.s.small", "|format('%s', [(1.5e-8 * 1.0)])");
        CASES.put("float.format.s.1e-5", "|format('%s', [(1.0e-5 * 1.0)])");
        CASES.put("float.format.f", "|format('%f', [(1.0 / 3.0)])");
        CASES.put("float.toRepresentation", "|toRepresentation(0.1 + 0.2)");
        // a last step's format beside other arguments and directives, and the shapes the door leaves to the run
        // (step 8, 2026-11-28, D7: %s and %r of a computed Float are Java's; %f, a precision and a format used
        // further are lite's own)
        CASES.put("float.format.mixed", "|format('%s is %r of %d (%s)', [(1.0 / 3.0), (1.5e-8 * 1.0), 3, 'x'])");
        CASES.put("float.format.precision", "|format('%.2f', [(1.0 / 3.0)])");
        CASES.put("float.format.usedFurther", "|format('%s', [(1.0 / 3.0)])->length()");
        CASES.put("float.format.throughALet", "{|let x = 1.0 / 3.0; format('%r;', [$x]);}");
        CASES.put("float.inATableTextUsedFurther", "|#TDS\n  x:Float\n  0.00001\n#->toString() + ';'");
        CASES.put("float.format.r.heldWhole",
                "|#TDS\n  x:Float\n  1000000\n#->extend(~t: r|format('%r', [$r.x->toOne()]))->select(~t)");
        CASES.put("dateTime.format.s.inAStoreQuery", "|#TDS\n  at:DateTime\n  2014-01-01T10:20:30.25\n#"
                + "->extend(~t: r|format('%s', [$r.at->toOne()]))->select(~t)->from(" + StorelessRuntime.RUNTIME + ")");
        CASES.put("dateTime.toString.inAStoreQuery", "|#TDS\n  at:DateTime\n  2014-01-01T10:20:30.25\n#"
                + "->extend(~t: r|$r.at->toOne()->toString())->select(~t)->from(" + StorelessRuntime.RUNTIME + ")");
    }

    private static final Map<String, PlanStyle> STYLES = new LinkedHashMap<>();

    static {
        STYLES.put("upstream", com.legend.edge.PureText.UPSTREAM);
        STYLES.put("pure_v1", com.legend.edge.PureText.PURE_V1);
    }

    private static final java.util.regex.Pattern IDS = java.util.regex.Pattern.compile(
            "[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}");

    private static String flat(String s) {
        // a backslash escaped first: a raw newline (EDGE-3) and an escaped one are two texts
        return IDS.matcher(s).replaceAll("<id>").replace("\\", "\\\\").replace('\t', ' ').replace("\r", "")
                .replace("\n", "\\n");
    }

    private static String answer(TestBackend backend, PlanStyle style, String query) {
        String model = backend.withStorelessRuntime("Class s::P { name: String[1]; }\n");
        try {
            // a last step that prints a relation answers as legend-pure's and legend-engine's doors answer it (stage B's
            // B2b S5a, 2026-11-24): lifted (edge.PrintedRelations, the one rule B3's lift reads too), the relation run and
            // printed in Java from its rows -- until then the edge composed the text in SQL, which S5a deleted
            com.legend.TypedQuery asked = com.legend.Compiler.query(com.legend.Compiler.compileModel(model), query);
            // legend-pure's door reads a last step's format first (step 8, the user's ruling of 2026-11-28, D7): each
            // computed Float under %s or %r run as a query of its own and printed in Java (edge.PrintedFormats)
            com.legend.TypedQuery typed = style != com.legend.edge.PureText.UPSTREAM ? asked
                    : com.legend.edge.UpstreamPrints.readLastFormat(asked, (read, answer) -> {
                        try (java.sql.Connection c = backend.open(style)) {
                            com.legend.Execution.run(read, StorelessRuntime.RUNTIME,
                                    com.legend.exec.SessionSource.given(com.legend.testbackend.TestSessions.over(c)),
                                    com.legend.exec.PlanSessions.of(com.legend.testbackend.TestSessions.over(c)),
                                    com.legend.exec.ProductRun.Establish.DECLARED_SETUP, java.util.Set.of(), style,
                                    answer, com.legend.result.Batcher.DEFAULT_CAPACITY);
                        } catch (java.sql.SQLException | IOException e) {
                            throw new IllegalStateException(e);
                        }
                    });
            // each style's own door: legend-pure's (UPSTREAM) lifts a value's print and the whole tail, legend-engine's
            // a relation's print with its replaces
            com.legend.edge.PrintedRelations.Lifted lifted = com.legend.edge.PrintedRelations.lift(typed,
                    style == com.legend.edge.PureText.UPSTREAM);
            if (lifted != null && lifted.written() != null) {
                // a literal's print, written at the door: nothing is run
                return "VALUE\t" + lifted.answers() + ":" + lifted.answer(lifted.written()) + ";";
            }
            if (lifted != null) {
                List<String> text = new java.util.ArrayList<>();
                try (java.sql.Connection c = backend.open(style)) {
                    com.legend.Execution.run(lifted.relation(), StorelessRuntime.RUNTIME,
                            com.legend.exec.SessionSource.given(com.legend.testbackend.TestSessions.over(c)),
                            com.legend.exec.PlanSessions.of(com.legend.testbackend.TestSessions.over(c)),
                            com.legend.exec.ProductRun.Establish.DECLARED_SETUP, java.util.Set.of(), style,
                            new com.legend.result.Output.Rows(lifted.writer(text::add)),
                            com.legend.result.Batcher.DEFAULT_CAPACITY);
                } catch (java.sql.SQLException | IOException e) {
                    throw new IllegalStateException(e);
                }
                if (text.size() != 1) {
                    // the harness's fault, never a recorded answer
                    throw new AssertionError("a lifted print answers one text; got " + text + " for " + query);
                }
                return "VALUE\t" + lifted.answers() + ":" + lifted.answer(text.get(0)) + ";";
            }
            Lean.Result r = Lean.runInStyle(model, query, StorelessRuntime.RUNTIME, backend, style);
            StringBuilder cells = new StringBuilder(r.shape().name()).append('\t');
            for (List<Object> row : r.rows()) {
                for (Object v : row) {
                    cells.append(v == null ? "null" : v.getClass().getSimpleName() + ":" + v).append(';');
                }
            }
            return cells.toString();
        } catch (RuntimeException e) {
            return "THREW\t" + e.getClass().getSimpleName() + ": " + e.getMessage();
        }
    }

    @Test
    void everyCasesAnswer() throws IOException {
        StringBuilder out = new StringBuilder();
        // the platform's own engine and legend-engine's database, by role (the dialect-leak ban: a test names no
        // database; the census's DialectLeakRatchetTest caught the names here in the compatibility program's slice 0.2,
        // 2026-11-19): Databases.REPLAY_REFEREE is the type legend-engine's tests run on, lite's own session of it here
        List<DatabaseType> types = new java.util.ArrayList<>(List.of(com.legend.database.Databases.PLATFORM,
                com.legend.database.Databases.REPLAY_REFEREE));
        // and every other database the platform runs in-process (step 8, 2026-11-26), after the two the net began with
        for (TestBackend inProcess : TestBackends.inProcess()) {
            if (!types.contains(inProcess.type())) {
                types.add(inProcess.type());
            }
        }
        for (DatabaseType type : types) {
            TestBackend backend = TestBackends.of(type);
            for (Map.Entry<String, PlanStyle> style : STYLES.entrySet()) {
                for (Map.Entry<String, String> c : CASES.entrySet()) {
                    out.append(type.name().toLowerCase(java.util.Locale.ROOT)).append('\t').append(style.getKey())
                            .append('\t').append(c.getKey()).append('\t')
                            .append(flat(answer(backend, style.getValue(), c.getValue()))).append('\n');
                }
            }
        }
        String path = System.getProperty("legend.edge.cases");
        if (path != null) {
            Files.writeString(Path.of(path), out, StandardCharsets.UTF_8);
        }
        assertFalse(out.isEmpty());
    }
}
