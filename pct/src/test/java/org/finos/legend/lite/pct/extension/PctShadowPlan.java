// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package org.finos.legend.lite.pct.extension;

import com.legend.Compiler;
import com.legend.TypedQuery;
import com.legend.exec.ExecutionResult;
import com.legend.exec.PlanRunner;
import com.legend.exec.PlanSessions;
import com.legend.executionplan.ExecutionPlan;

import java.io.IOException;
import java.io.StringWriter;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The shadow run of the plan path ({@code -Dlegend.diagnostics=pct-shadow-plan}; the MongoDB line's homework 2,
 * 2026-10-17, docs/MONGODB_PLAN_2026_10_16.md §11.2): beside each case's result from the legacy route, the same
 * expression planned to Pure's TDS text ({@link TypedQuery.Output#TDS_TEXT}, under the same feature flags) and run by
 * the plan runner on the lane's storeless runtime; the two compared, and one line per case written to
 * {@code pct-shadow-plan.tsv} in the suite's outputs -- the verdict, the legacy result's kind, a detail, the
 * expression. A relation's TDS text is compared whole; a scalar's or a collection's rows against the legacy values'
 * own text (loosely: {@code String.valueOf}), since the legacy route never spells those as a TDS. It measures; it
 * decides nothing and fails nothing.
 */
final class PctShadowPlan {

    private static final String DIR = !com.legend.diagnostics.Diagnostics.on("pct-shadow-plan") ? null
            : System.getenv("TEST_UNDECLARED_OUTPUTS_DIR");

    private PctShadowPlan() {
    }

    static void record(String model, String expression, ExecutionResult legacy) {
        if (DIR == null) {
            return;
        }
        String kind = legacy.getClass().getSimpleName();
        String verdict;
        String detail;
        try {
            ExecutionPlan plan = Compiler.query(Compiler.compileModel(PctBackend.withStorelessRuntime(model)), expression)
                    .executionPlan(com.legend.test.StorelessRuntime.RUNTIME, TypedQuery.Output.TDS_TEXT,
                            Set.of(com.legend.platform.Feature.CORRECT_SQL_SUBSTRING_INDEXING));
            StringWriter out = new StringWriter();
            PlanRunner.run(plan, Map.of(), PlanSessions.shared(), out);
            String planned = out.toString();
            switch (legacy) {
                case ExecutionResult.TdsText t -> {
                    verdict = t.text().equals(planned) ? "EQUAL" : "DIFFERENT";
                    detail = verdict.equals("EQUAL") ? "" : "legacy=" + flat(t.text()) + " plan=" + flat(planned);
                }
                case ExecutionResult.Scalar s -> {
                    String want = s.value() == null ? "null" : String.valueOf(s.value());
                    verdict = rows(planned).equals(List.of(want)) ? "VALUE_EQUAL" : "VALUE_DIFFERENT";
                    detail = "legacy=" + flat(want) + " plan=" + flat(planned);
                }
                case ExecutionResult.Collection c -> {
                    List<String> want = c.values().stream().map(v -> v == null ? "null" : String.valueOf(v)).toList();
                    verdict = rows(planned).equals(want) ? "VALUE_EQUAL" : "VALUE_DIFFERENT";
                    detail = "legacy=" + flat(String.join("\n", want)) + " plan=" + flat(planned);
                }
                case ExecutionResult.Graph g -> {
                    verdict = "GRAPH";
                    detail = "plan=" + flat(planned);
                }
                case ExecutionResult.Tabular t -> {
                    verdict = "TABULAR";
                    detail = "plan=" + flat(planned);
                }
            }
        } catch (com.legend.error.NotImplementedException | com.legend.sql.dialect.DialectCapability e) {
            verdict = "REFUSED";
            detail = first(e.getMessage());
        } catch (RuntimeException | IOException e) {
            verdict = "ERROR";
            detail = e.getClass().getSimpleName() + ": " + first(e.getMessage());
        }
        String line = verdict + "\t" + kind + "\t" + detail.replace('\t', ' ') + "\t"
                + Base64.getEncoder().encodeToString(expression.getBytes(StandardCharsets.UTF_8)) + "\n";
        try {
            Files.writeString(Path.of(DIR, "pct-shadow-plan.tsv"), line, StandardOpenOption.CREATE,
                    StandardOpenOption.APPEND);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** The TDS text's rows: the lines after the header. */
    private static List<String> rows(String tds) {
        int nl = tds.indexOf('\n');
        return nl < 0 ? List.of() : List.of(tds.substring(nl + 1).split("\n", -1));
    }

    private static String flat(String s) {
        return s.replace("\n", "\\n");
    }

    private static String first(@com.legend.base.Nullable String message) {
        if (message == null) {
            return "";
        }
        int nl = message.indexOf('\n');
        return nl < 0 ? message : message.substring(0, nl);
    }
}
