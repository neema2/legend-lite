// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.graphql;

import com.legend.exec.ExecutionResult;
import com.legend.json.Json;
import com.legend.server.QueryService;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

/**
 * THE FORTY-TASK EVALUATION (docs/GRAPHQL_STUDY_2026_10_18.md §9, §10 PR6): {@code eval/tasks.json} holds forty
 * asks over the full feature set (roots, where, orderBy, paging, nesting, derived properties, fragments,
 * variables, directives, aggregates, groups, inheritance), each with its reference document and what the answer
 * holds. This test PROVES the references (every one runs on DuckDB through the island and answers as said);
 * {@link #score} judges a candidate document per task by its answer against the reference's, which is how an
 * agent's forty answers are marked: the model, the tool descriptions and the asks are the inputs; the candidates
 * come from outside this test (no model is called here).
 */
class GqlEvaluationTest {

    static final QueryService qs = new QueryService();

    record Task(String id, String model, String ask, String query, List<String> contains, List<String> absent,
            List<String> order) {
    }

    static List<Task> tasks() {
        Json.Obj doc = (Json.Obj) Json.parse(SchemaGeneratorTest.resource("eval/tasks.json"));
        List<Task> out = new ArrayList<>();
        for (Json.Node n : doc.getArr("tasks").items()) {
            Json.Obj t = (Json.Obj) n;
            out.add(new Task(t.getString("id"), t.getString("model"), t.getString("ask"), t.getString("query"),
                    strings(t, "contains"), strings(t, "absent"), strings(t, "order")));
        }
        return out;
    }

    private static List<String> strings(Json.Obj t, String key) {
        return t.has(key) ? t.getStringArray(key) : List.of();
    }

    /** A document's answer as text: a tree root's serialized JSON; a relation root's rows as JSON objects. */
    static String answer(String model, String document) throws SQLException {
        ExecutionResult r;
        if (model.equals("inheritance")) {
            GqlInheritanceTest.open();
            try {
                r = qs.execute(GqlInheritanceTest.MODEL, "#GQL{ " + document + " }#", "demo::Runtime",
                        GqlInheritanceTest.conn);
            } finally {
                GqlInheritanceTest.close();
            }
        } else {
            GqlExecutionTest.open();
            try {
                r = qs.execute(GqlExecutionTest.MODEL, "#GQL{ " + document + " }#", "demo::Runtime",
                        GqlExecutionTest.conn);
            } finally {
                GqlExecutionTest.close();
            }
        }
        if (r instanceof ExecutionResult.Graph g) {
            return g.json();
        }
        List<Map<String, Object>> rows = new ArrayList<>();
        for (var row : r.rows()) {
            Map<String, Object> o = new LinkedHashMap<>();
            for (int c = 0; c < r.columns().size(); c++) {
                Object v = row.get(c);
                if (v instanceof Number n && !(n instanceof Long) && !(n instanceof Integer)
                        && n.doubleValue() == Math.rint(n.doubleValue()) && Math.abs(n.doubleValue()) < 1e15) {
                    v = n.longValue();
                }
                o.put(r.columns().get(c).name(), v == null ? null : v instanceof Number ? v : String.valueOf(v));
            }
            rows.add(o);
        }
        return Json.toCompact(rows);
    }

    /** What a task's answer must hold; the failures named, none for a pass. */
    static List<String> check(Task t, String answer) {
        List<String> problems = new ArrayList<>();
        for (String s : t.contains()) {
            if (!answer.contains(s)) {
                problems.add(t.id() + ": missing " + s);
            }
        }
        for (String s : t.absent()) {
            if (answer.contains(s)) {
                problems.add(t.id() + ": has " + s + " which it must not");
            }
        }
        int at = -1;
        for (String s : t.order()) {
            int i = answer.indexOf(s, at + 1);
            if (i < 0) {
                problems.add(t.id() + ": " + s + " not after the previous");
                break;
            }
            at = i;
        }
        return problems;
    }

    /**
     * The judge of an agent's answers: per task, the candidate document's answer equal to the reference's (a
     * candidate that fails to run fails the task). The map is by task id; a task without a candidate fails.
     */
    static Map<String, Boolean> score(Map<String, String> candidates) throws SQLException {
        Map<String, Boolean> out = new LinkedHashMap<>();
        for (Task t : tasks()) {
            String candidate = candidates.get(t.id());
            if (candidate == null) {
                out.put(t.id(), false);
                continue;
            }
            String reference = answer(t.model(), t.query());
            String got;
            try {
                got = answer(t.model(), candidate);
            } catch (RuntimeException | SQLException e) {
                out.put(t.id(), false);
                continue;
            }
            out.put(t.id(), reference.equals(got));
        }
        return out;
    }

    @Test
    void fortyTasksOverTheFullFeatureSet() {
        List<Task> tasks = tasks();
        assertEquals(40, tasks.size());
        java.util.Set<String> ids = new java.util.TreeSet<>();
        for (Task t : tasks) {
            assertTrue(ids.add(t.id()), "task ids are unique: " + t.id());
            assertTrue(!t.ask().isBlank() && !t.query().isBlank(), t.id());
            assertTrue(!t.contains().isEmpty() || !t.order().isEmpty(), t.id() + " says what its answer holds");
        }
    }

    @Test
    void everyReferenceAnswersAsSaid() throws SQLException {
        List<String> problems = new ArrayList<>();
        for (Task t : tasks()) {
            String answer;
            try {
                answer = answer(t.model(), t.query());
            } catch (RuntimeException e) {
                problems.add(t.id() + ": " + e.getMessage());
                continue;
            }
            List<String> p = check(t, answer);
            if (!p.isEmpty()) {
                problems.add(String.join("; ", p) + " -- in " + answer);
            }
        }
        assertEquals(List.of(), problems);
    }

    @Test
    void theJudgeMarksTheReferencesRightAndAWrongAnswerWrong() throws SQLException {
        Map<String, String> candidates = new LinkedHashMap<>();
        for (Task t : tasks()) {
            candidates.put(t.id(), t.query());
        }
        // T03 answered with the two OLDEST: wrong; T04 left unanswered: wrong
        candidates.put("T03", "{ Person(orderBy: [{ age: DESC }], limit: 2) { firstName } }");
        candidates.remove("T04");
        Map<String, Boolean> marks = score(candidates);
        assertEquals(40, marks.size());
        int right = 0;
        for (var e : marks.entrySet()) {
            if (e.getValue()) {
                right++;
            }
        }
        assertEquals(38, right, marks.toString());
        assertEquals(false, marks.get("T03"));
        assertEquals(false, marks.get("T04"));
    }
}
