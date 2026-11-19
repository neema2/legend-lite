// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge;

import com.legend.executionplan.ExecutionPlan;
import com.legend.sql.RaiseKind;
import com.legend.sql.SqlExpr;
import com.legend.sql.dialect.RaiseSite;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * THE EDGE'S RECORDER OF RAISE SITES' WORDS (K10b of the edge audit, the orchestrator's ruling (A) of 2026-11-17): one
 * per planning call, its key {@code (statement text, n)}, two different words of one key refused by name at planning,
 * read once and then sealed, and two planning calls' tables never mixed.
 */
class SiteWordsRecorderTest {

    private static RaiseSite site(int n, RaiseKind kind, SqlExpr... operands) {
        return new RaiseSite(n, kind, null, List.of(operands));
    }

    @Test
    void twoWordsOfOneKeyAreRefusedByName_equalWordsAreOneRecord() {
        SiteWordsRecorder recording = PureText.recording(PureText.UPSTREAM);
        recording.siteWords("SELECT x", List.of(site(1, RaiseKind.DIVIDE_BY_ZERO)));
        // the same key, the same words: one record
        recording.siteWords("SELECT x", List.of(site(1, RaiseKind.DIVIDE_BY_ZERO)));
        // the same key, other words: refused at planning, never overwritten
        IllegalStateException e = assertThrows(IllegalStateException.class, () -> recording.siteWords("SELECT x",
                List.of(site(1, RaiseKind.DIVIDE_BY_ZERO, new SqlExpr.IntLit(7)))));
        assertTrue(e.getMessage().contains("worded differently") && e.getMessage().contains("SELECT x"),
                e.getMessage());
        assertEquals(Map.of(1, "Division by zero"), recording.read().get("SELECT x"));
    }

    @Test
    void aRecorderIsReadOnceThenSealed() {
        SiteWordsRecorder recording = PureText.recording(PureText.UPSTREAM);
        recording.siteWords("SELECT x", List.of(site(1, RaiseKind.DIVIDE_BY_ZERO)));
        recording.read();
        IllegalStateException offered = assertThrows(IllegalStateException.class, () -> recording.siteWords(
                "SELECT y", List.of(site(1, RaiseKind.DIVIDE_BY_ZERO))));
        assertTrue(offered.getMessage().contains("sealed"), offered.getMessage());
        IllegalStateException again = assertThrows(IllegalStateException.class, recording::read);
        assertTrue(again.getMessage().contains("read twice"), again.getMessage());
    }

    @Test
    void onlyTheEdgesTemplatesAreRecorded() {
        assertThrows(com.legend.error.NotImplementedException.class,
                () -> PureText.recording(PureText.recording(PureText.UPSTREAM)), "a recorder over a recorder");
        assertThrows(com.legend.error.NotImplementedException.class,
                () -> PureText.recording(com.legend.database.PlanStyle.PRODUCT), "a recorder over the product's");
    }

    private static final String MODEL = """
            ###Relational
            Database s::DB ( Table T ( ID INTEGER PRIMARY KEY, D INTEGER NOT NULL ) )
            ###Connection
            RelationalDatabaseConnection s::Conn
            {
                store: s::DB;
                type: DuckDB;
                specification: LocalH2 { testDataSetupCSV: 'default\\nT\\nID,D\\n1,0\\n'; };
                auth: DefaultH2;
            }
            ###Runtime
            Runtime s::RT { mappings: []; connections: [ s::DB: [ c1: s::Conn ] ]; }
            """;

    /** Two bodies planned with two recorders: each table holds its own plan's statements alone, never the other's. */
    @Test
    void twoPlanningCallsTablesNeverMix() {
        var ctx = com.legend.Compiler.compileModel(MODEL);
        SiteWordsRecorder first = PureText.recording(PureText.UPSTREAM);
        SiteWordsRecorder second = PureText.recording(PureText.UPSTREAM);
        ExecutionPlan a = com.legend.Compiler.query(ctx, "|#>{s::DB.T}#->extend(~r: r|$r.ID->bitShiftLeft(70))"
                + "->select(~r)").executionPlan("s::RT", Set.of(), first);
        ExecutionPlan b = com.legend.Compiler.query(ctx, "|#>{s::DB.T}#->extend(~r: r|$r.ID / $r.D)->select(~r)")
                .executionPlan("s::RT", Set.of(), second);
        Map<String, Map<Integer, String>> fromA = first.read();
        Map<String, Map<Integer, String>> fromB = second.read();
        assertFalse(fromA.isEmpty(), "the first body's sites are worded");
        assertFalse(fromB.isEmpty(), "the second body's sites are worded");
        Set<String> textsA = texts(a.root());
        Set<String> textsB = texts(b.root());
        assertTrue(textsA.containsAll(fromA.keySet()), "the first table holds its own plan's statements: " + fromA);
        assertTrue(textsB.containsAll(fromB.keySet()), "the second table holds its own plan's statements: " + fromB);
        Set<String> both = new HashSet<>(fromA.keySet());
        both.retainAll(fromB.keySet());
        assertTrue(both.isEmpty(), "two planning calls' tables mixed: " + both);
    }

    private static Set<String> texts(ExecutionPlan.Node node) {
        List<String> out = new ArrayList<>();
        switch (node) {
            case ExecutionPlan.Sequence s -> s.steps().forEach(step -> out.addAll(texts(step)));
            case ExecutionPlan.TdsResult t -> out.add(t.sql().statement());
            case ExecutionPlan.Keyed k -> {
                out.add(k.keys().statement());
                out.add(k.body().statement());
            }
            case ExecutionPlan.Update u -> out.add(u.sql().statement());
            case ExecutionPlan.Check c -> out.add(c.sql().statement());
            case ExecutionPlan.Effect f -> out.add(f.sql().statement());
            case ExecutionPlan.Load l -> {
            }
            case ExecutionPlan.Constant c -> {
            }
        }
        return new HashSet<>(out);
    }
}
