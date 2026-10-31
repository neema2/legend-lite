// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.planner;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.legend.Compiler;
import com.legend.executionplan.ExecutionPlan;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;

/**
 * One object-id minter per PROGRAM (the test-is-the-product plan's B9, R10, 2026-10-31): an object's identity is its
 * evaluation, and one program is the scope in which its objects meet. The statements of a program that writes
 * ({@code PlanMaker.program}) are inlined and lowered one by one; they name their objects' sites by one counter, and
 * each statement's own evaluations are marked with its position, so two constructions in two statements -- a
 * function's, called in both, included -- are two objects, and a let's construction read by two statements is one.
 * Until then each statement's lowering minted its own ids from {@code o1}, and one function's construction called in
 * two statements had one site.
 */
class ProgramObjectSitesTest {

    private static final String MODEL = """
            Class s::P { name: String[1]; }
            function s::mk(): s::P[1] { ^s::P(name='m') }
            ###Relational
            Database s::DB ( Table T ( ID INTEGER PRIMARY KEY, NAME VARCHAR(20) ) )
            ###Connection
            RelationalDatabaseConnection s::Conn
            {
                store: s::DB;
                type: DuckDB;
                specification: LocalH2 { };
                auth: DefaultH2;
            }
            ###Runtime
            Runtime s::RT { mappings: [ ]; connections: [ s::DB: [ c1: s::Conn ] ]; }
            """;

    private static final Pattern ID = Pattern.compile("'__id': '(o[0-9]+)'");

    private static ExecutionPlan plan(String program) {
        return Compiler.query(Compiler.compileModel(MODEL), program).executionPlan("s::RT");
    }

    /** The object ids the program's steps spell, step by step, in the order each spells them. */
    private static List<List<String>> ids(ExecutionPlan plan) {
        if (!(plan.root() instanceof ExecutionPlan.Sequence seq)) {
            throw new AssertionError("a program that writes plans as a sequence: " + plan.root());
        }
        List<List<String>> out = new ArrayList<>();
        for (ExecutionPlan.Node step : seq.steps()) {
            List<String> ids = new ArrayList<>();
            Matcher m = ID.matcher(com.legend.executionplan.PlanJson.write(new ExecutionPlan(List.of(), step)));
            while (m.find()) {
                ids.add(m.group(1));
            }
            out.add(ids);
        }
        return out;
    }

    @Test
    void twoStatementsConstructionsAreTwoObjects() {
        List<List<String>> ids = ids(plan("{|^s::P(name='a'); #>{s::DB.T}#->delete(r|$r.ID == 9); ^s::P(name='b');}"));
        assertEquals(List.of(List.of("o1"), List.of(), List.of("o2")), ids);
    }

    @Test
    void aFunctionsConstructionCalledInTwoStatementsIsTwoObjects() {
        // one source construction (mk's body), two evaluations: one per statement that calls it
        List<List<String>> ids = ids(plan("{|s::mk(); #>{s::DB.T}#->delete(r|$r.ID == 9); s::mk();}"));
        assertEquals(List.of(List.of("o1"), List.of(), List.of("o2")), ids);
    }

    @Test
    void aLetsConstructionReadByTwoStatementsIsOneObject() {
        // the last statement builds another object first, so a counter per statement would spell the let's o2 there
        List<List<String>> ids = ids(plan("{|let p = ^s::P(name='a'); $p; #>{s::DB.T}#->delete(r|$r.ID == 9);"
                + " [^s::P(name='z'), $p];}"));
        assertEquals(List.of(List.of("o1"), List.of(), List.of("o2", "o1")), ids);
    }

    @Test
    void planningTwiceSpellsTheSameIds() {
        String program = "{|s::mk(); let p = ^s::P(name='a'); $p; #>{s::DB.T}#->delete(r|$r.ID == 9); [s::mk(), $p];}";
        assertEquals(com.legend.executionplan.PlanJson.write(plan(program)),
                com.legend.executionplan.PlanJson.write(plan(program)));
    }
}
