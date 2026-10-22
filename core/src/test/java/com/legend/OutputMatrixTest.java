// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0
package com.legend;

import com.legend.executionplan.ExecutionPlan;
import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.testcases.PlanCases;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * THE OUTPUT MATRIX (docs/OUTPUT_LAYER_DESIGN_2026_10_10.md §5 step 1; the plan path program's step 2,
 * docs/PLAN_PATH_PROGRAM_2026_10_17.md): for each kind of value -- a whole, a fractional and an extreme Float, a Float
 * after arithmetic, a Decimal literal and after arithmetic, a DateTime with no, three and nine fraction digits, a
 * StrictDate, each as a value root and as a column -- and each of lite's own outputs on the plan path (CSV, the JSON
 * wire, a stream of JSON rows, Pure's TDS text) on DuckDB and H2 (OutputMatrixH2Test), the text lite writes beside interpreted Pure's
 * (the reference: {@code docs/execution-plan-boundary-2026-10-05/probes/engine-reference/pure-interpreted-results.txt},
 * PureEval.java, 2026-10-10). Every cell is judged: a cell equal to Pure's text passes; a cell that differs is
 * either PINNED here with the text it writes today -- a deviation named, shrink-only, each later landing of the
 * output layer removing the pins it fixes -- or a failure. A pinned cell that starts writing Pure's text fails too,
 * so a fix is recorded by deleting its pin. The whole table is printed on every run.
 */
class OutputMatrixTest {

    /** A table of one row, {@code ID = 2}, for the column forms. */
    private static final String MODEL = """
            ###Relational
            Database s::DB ( Table T ( ID INTEGER PRIMARY KEY ) )
            ###Connection
            RelationalDatabaseConnection s::Conn
            {
                store: s::DB;
                type: %s;
                specification: LocalH2 { testDataSetupCSV: 'default\\nT\\nID\\n2\\n'; };
                auth: DefaultH2;
            }
            ###Runtime
            Runtime s::RT { mappings: []; connections: [ s::DB: [ c1: s::Conn ] ]; }
            """;

    /** A kind: its name, the expression as a value root, the same value as a column, interpreted Pure's text of the
     *  value, and a relation cell's text in Pure's TDS text ({@code tds}; the one difference: a DateTime cell's three
     *  fraction digits at least, legend-engine's {@code testDayOfWeek_Relation} -- {@code LiteralSpelling
     *  .dateTimeTextTds}, 2026-10-18; a value root's one cell keeps the value's own text, which the reader reads as
     *  the value). */
    private record Kind(String name, String value, String column, String pure, String tds) {
        Kind(String name, String value, String column, String pure) {
            this(name, value, column, pure, pure);
        }
    }

    private static final String T = "#>{s::DB.T}#->extend(~v: r|";

    private static final List<Kind> KINDS = List.of(
            new Kind("float_whole", "|2 * 1.5", "|" + T + "$r.ID * 1.5)->select(~[v])", "3.0"),
            new Kind("float_fraction", "|1.1", "|" + T + "$r.ID * 0.55)->select(~[v])", "1.1"),
            new Kind("float_arithmetic", "|3 * 1.1", "|" + T + "$r.ID * 1.65)->select(~[v])", "3.3"),
            new Kind("float_big", "|1.5e15", "|" + T + "$r.ID * 7.5e14)->select(~[v])", "1500000000000000.0"),
            new Kind("float_small", "|2.5e-7", "|" + T + "$r.ID * 1.25e-7)->select(~[v])", "0.00000025"),
            new Kind("decimal_literal", "|2.50D", "|" + T + "$r.ID * 1.25D)->select(~[v])", "2.50"),
            new Kind("decimal_arithmetic", "|2 * 2.50D", "|" + T + "$r.ID * 2.50D)->select(~[v])", "5.00"),
            new Kind("decimal_times_decimal", "|1.50D * 2.50D", "|" + T + "1.50D * 2.50D)->select(~[v])",
                    "3.7500"),
            new Kind("datetime_whole", "|%2024-01-02T10:30:00", "|" + T + "%2024-01-02T10:30:00)->select(~[v])",
                    "2024-01-02T10:30:00+0000", "2024-01-02T10:30:00.000+0000"),
            new Kind("datetime_millis", "|%2024-01-02T10:30:00.123",
                    "|" + T + "%2024-01-02T10:30:00.123)->select(~[v])", "2024-01-02T10:30:00.123+0000"),
            new Kind("datetime_nanos", "|%2024-01-02T10:30:00.123456789",
                    "|" + T + "%2024-01-02T10:30:00.123456789)->select(~[v])",
                    "2024-01-02T10:30:00.123456789+0000"),
            new Kind("date", "|%2024-01-02", "|" + T + "%2024-01-02)->select(~[v])", "2024-01-02"));

    private static final List<TypedQuery.Output> OUTPUTS = List.of(TypedQuery.Output.CSV, TypedQuery.Output.JSON,
            TypedQuery.Output.STREAMED_JSON, TypedQuery.Output.TDS_TEXT);

    /**
     * Today's deviations from Pure's text, by {@code kind/form/output/database}, each the text lite writes.
     * Measured 2026-10-18 at the matrix's first run: 76 (docs/OUTPUT_LAYER_DESIGN_2026_10_10.md §3 names the four
     * gaps); 44 after the Float's landing; NONE after the DateTime's (the one owner, 2026-10-18): every cell writes
     * Pure's text. Shrink-only: a landing of the output layer deletes the pins it fixes, a pin that no longer holds
     * fails, and a cell that starts deviating fails -- a new pin needs a dated reason naming its gap.
     */
    private static final Map<String, String> PINNED = Map.ofEntries();

    @Test
    void everyKindOnEveryOutputWritesPuresText_orIsPinnedByName() throws Exception {
        assertEveryKindWritesPuresText(com.legend.testbackend.TestBackends.platform());    // and on H2: its own test
    }

    /** {@link #everyKindOnEveryOutputWritesPuresText_orIsPinnedByName} on {@code backend}, against its own pins (main's
     *  merge, 2026-10-22: one test named DuckDB and H2; each database's run is its own test's). */
    static void assertEveryKindWritesPuresText(com.legend.testbackend.TestBackend backend) throws Exception {
        List<String> table = new ArrayList<>();
        List<String> failures = new ArrayList<>();
        Map<String, String> pins = new LinkedHashMap<>();
        PINNED.forEach((key, cell) -> {
            if (key.endsWith("/" + backend.type())) {
                pins.put(key, cell);
            }
        });
        {
            DatabaseType type = backend.type();
            var ctx = Compiler.compileModel(MODEL.formatted(type.name()));
            for (Kind kind : KINDS) {
                for (String form : List.of("value", "column")) {
                    String query = form.equals("value") ? kind.value() : kind.column();
                    for (TypedQuery.Output output : OUTPUTS) {
                        String key = kind.name() + "/" + form + "/" + output + "/" + type;
                        String cell;
                        try (Connection c = backend.open()) {
                            ExecutionPlan plan = Compiler.query(ctx, query).executionPlan("s::RT", output);
                            cell = cellOf(PlanCases.run(plan, c), output);
                        } catch (RuntimeException e) {
                            cell = "ERROR " + e.getClass().getSimpleName() + ": " + firstLine(e.getMessage());
                        }
                        String pinned = pins.remove(key);
                        String verdict;
                        String reference = output == TypedQuery.Output.TDS_TEXT && form.equals("column")
                                ? kind.tds() : kind.pure();
                        if (cell.equals(reference)) {
                            verdict = pinned == null ? "pure" : "PIN STALE";
                            if (pinned != null) {
                                failures.add(key + " writes Pure's text now: delete its pin");
                            }
                        } else if (pinned != null && pinned.equals(cell)) {
                            verdict = "pinned";
                        } else if (pinned != null) {
                            verdict = "PIN MOVED";
                            failures.add(key + " writes '" + cell + "', pinned as '" + pinned + "'");
                        } else {
                            verdict = "DEVIATES";
                            failures.add(key + " writes '" + cell + "' where Pure writes '" + reference + "'");
                        }
                        table.add(key + "\t" + reference + "\t" + cell + "\t" + verdict);
                    }
                }
            }
        }
        for (String stale : pins.keySet()) {
            failures.add(stale + " is pinned but not in the matrix");
        }
        System.out.println("[output matrix] key\tpure\tlite\tverdict");
        table.forEach(System.out::println);
        assertTrue(failures.isEmpty(), String.join("\n", failures));
    }

    /** The one cell of a one-row, one-column answer in {@code output}'s text, as the value's own text. */
    private static String cellOf(String text, TypedQuery.Output output) {
        return switch (output) {
            case CSV, TDS_TEXT -> {
                String[] lines = text.split("\n");
                yield lines.length > 1 ? lines[1].trim() : "<no row: " + text.replace("\n", "\\n") + ">";
            }
            case JSON, STREAMED_JSON -> {
                java.util.regex.Matcher m = java.util.regex.Pattern.compile("\"(?:v|value)\":(\"(?:[^\"\\\\]|\\\\.)*\"|[^,}\\]]*)")
                        .matcher(text);
                if (!m.find()) {
                    yield "<no cell: " + text.replace("\n", "\\n") + ">";
                }
                String token = m.group(1);
                yield token.startsWith("\"") ? token.substring(1, token.length() - 1) : token;
            }
        };
    }

    private static String firstLine(String s) {
        return s == null ? "" : s.split("\n")[0];
    }
}
