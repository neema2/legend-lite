// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.integration;

import com.legend.executionplan.ExecutionPlan;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * A #TS ANSWER ON ANOTHER DATABASE, HELD TO DUCKDB'S (docs/TS_DECISIONS_2026_11_07.md items 9 and 9b; the tolerance
 * ruled 2026-11-07, §4 there): an answer's columns and rows as text, and where one database's answer differs from
 * DuckDB's run beside it -- the same dates and lines exactly (every column of no figure type), each figure within
 * 1e-9 of its series' own scale (the largest magnitude of DuckDB's figures in that column), so a sum taken in another
 * order passes and a wrong figure does not. The gallery's replay ({@link TsReplayTest}) and the documents' examples on
 * a lane's database ({@link TsDocumentsTest}) read it.
 */
final class TsAnswers {

    private TsAnswers() {
    }

    /** An answer: its columns and types, and its rows' cells as text (null where none). */
    record Answer(List<ExecutionPlan.Column> columns, List<List<String>> rows) {
    }

    /** {@code plan}'s answer, written as {@code csv} (PlanCases' CSV writer). */
    static Answer of(ExecutionPlan plan, String csv) {
        List<ExecutionPlan.Column> columns = java.util.Objects.requireNonNull(plan.answerColumns(),
                "a #TS query's columns");
        List<String> lines = new ArrayList<>(Arrays.asList(csv.split("\r\n")));
        lines.remove(0);
        List<List<String>> rows = new ArrayList<>();
        for (String line : lines) {
            if (line.isEmpty()) {
                continue;
            }
            rows.add(Arrays.stream(line.split(",", -1)).map(x -> x.isEmpty() ? null : x).toList());
        }
        return new Answer(columns, rows);
    }

    private static final Set<String> FIGURES = Set.of("Float", "Number", "Decimal");

    /** Where {@code got} differs from {@code want} (DuckDB's): null where it does not. */
    static String difference(Answer want, Answer got) {
        List<String> names = want.columns().stream().map(ExecutionPlan.Column::name).toList();
        if (!names.equals(got.columns().stream().map(ExecutionPlan.Column::name).toList())) {
            return "the columns " + got.columns().stream().map(ExecutionPlan.Column::name).toList() + ", DuckDB's "
                    + names;
        }
        List<Integer> keys = new ArrayList<>();
        List<Integer> figures = new ArrayList<>();
        for (int i = 0; i < want.columns().size(); i++) {
            (FIGURES.contains(want.columns().get(i).type()) ? figures : keys).add(i);
        }
        Map<List<String>, List<String>> a = keyed(want, keys, figures);
        Map<List<String>, List<String>> b = keyed(got, keys, figures);
        if (a == null || b == null) {
            return a == null && b == null && want.rows().size() == got.rows().size() ? null
                    : "rows of one date and line twice: " + want.rows().size() + " on DuckDB, " + got.rows().size()
                            + " here";
        }
        if (!a.keySet().equals(b.keySet())) {
            List<List<String>> only = a.keySet().stream().filter(k -> !b.containsKey(k)).limit(3).toList();
            List<List<String>> extra = b.keySet().stream().filter(k -> !a.containsKey(k)).limit(3).toList();
            return "the rows' dates and lines: " + a.size() + " on DuckDB, " + b.size() + " here; DuckDB's alone "
                    + only + ", these alone " + extra;
        }
        for (int f = 0; f < figures.size(); f++) {
            double scale = 0;
            for (List<String> v : a.values()) {
                if (v.get(f) != null) {
                    scale = Math.max(scale, Math.abs(Double.parseDouble(v.get(f))));
                }
            }
            for (Map.Entry<List<String>, List<String>> e : a.entrySet()) {
                String x = e.getValue().get(f);
                String y = b.get(e.getKey()).get(f);
                boolean same = x == null ? y == null
                        : y != null && Math.abs(Double.parseDouble(x) - Double.parseDouble(y)) <= 1e-9 * scale;
                if (!same) {
                    return names.get(figures.get(f)) + " at " + e.getKey() + ": " + y + ", DuckDB's " + x
                            + " (scale " + scale + ")";
                }
            }
        }
        return null;
    }

    /** {@code answer}'s figures by its keys' cells (a date and a line), in their order; null where two rows share
     *  their keys (an answer of no figure keyed by date alone: compared by its rows' count then). */
    private static Map<List<String>, List<String>> keyed(Answer answer, List<Integer> keys, List<Integer> figures) {
        Map<List<String>, List<String>> out = new TreeMap<>(java.util.Comparator.comparing(Object::toString));
        for (List<String> row : answer.rows()) {
            List<String> key = keys.stream().map(row::get).toList();
            if (out.put(key, figures.stream().map(row::get).toList()) != null) {
                return null;
            }
        }
        return out;
    }
}
