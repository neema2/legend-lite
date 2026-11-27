// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.legend.testbackend.TestBackends;
import com.legend.testing.Lean;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

/**
 * A SIDE OF AN EQUALITY HOLDS NO VALUE (the edition program's step 6; the user, 2026-11-27: "Yes do the empty side
 * fix", then "why cant we do the work we promised to get to one equality?"). Pure's {@code ==} is two-valued:
 * {@code $x.nick == 'a'} where {@code nick} is empty is false ({@code [] == 'a'}), and two empty sides are equal
 * ({@code [] == []}), wherever the comparison stands. A database's {@code =} answers "no value" there, which is the
 * same as false only where the comparison is a POSITIVE condition (a filter's, a join's, an {@code if}'s, and
 * {@code &&}, {@code ||} beneath them). So the lowering leaves those as they are written ({@code Lowerer
 * .asCondition}) and answers every other equality for its empty side where it is written ({@code Lowerer.written},
 * {@code Equalities.emptySidesAsPure}): a column, a query's answer, an operand, a window's column, an aggregate's
 * input, a list lambda's result -- and anything under a {@code !} or a {@code forAll}.
 *
 * <p>Measured before (2026-11-27, on DuckDB and SQLite): each "kept value" case below answered no value (the count
 * 1 of 2, the mapped list one element of two); {@code filter(o | !($o.nick == 'a' || $o.id == 7))} dropped the row
 * whose nick is empty, and so it had before step 6; a {@code forAll} over such a comparison answered true.
 */
class EmptySideEqualityTest {

    private static final String MODEL = """
            ###Pure
            Class p::O { id: Integer[1]; nick: String[0..1]; n: Integer[0..1]; }
            function p::either(b: Boolean[1]): Boolean[1] { $b || ($b == false) }
            function p::listed(b: Boolean[1]): Boolean[1] { [$b]->exists(x | $x == false || $b) }
            ###Relational
            Database p::DB ( Table OPT (ID INTEGER PRIMARY KEY, NICK VARCHAR(20), N INTEGER) )
            ###Mapping
            Mapping p::M ( p::O: Relational { ~mainTable [p::DB] OPT id: OPT.ID, nick: OPT.NICK, n: OPT.N } )
            ###Connection
            RelationalDatabaseConnection p::C { store: p::DB; type: %s;
                specification: LocalH2 { testDataSetupCSV: 'default\\nOPT\\nID,NICK,N\\n1,a,1\\n2,,\\n'; };
                auth: DefaultH2; }
            ###Runtime
            Runtime p::RT { mappings: [p::M]; connections: [ p::DB: [ c0: p::C ] ]; }
            """;

    @Test
    void anEmptySideIsNotEqualToAValueWhereverTheComparisonStands() throws Exception {
        Map<String, Object> cases = new LinkedHashMap<>();
        // as a column's value: row 2's nick and n hold nothing
        cases.put("|p::O.all()->project(~[id: o|$o.id, e: o|$o.nick == 'a'])->sort(~id->ascending())",
                List.of(List.of(1L, true), List.of(2L, false)));
        cases.put("|p::O.all()->project(~[id: o|$o.id, e: o|'a' == $o.nick])->sort(~id->ascending())",
                List.of(List.of(1L, true), List.of(2L, false)));
        cases.put("|p::O.all()->project(~[id: o|$o.id, e: o|$o.n == $o.id])->sort(~id->ascending())",
                List.of(List.of(1L, true), List.of(2L, false)));
        cases.put("|p::O.all()->project(~[id: o|$o.id, e: o|!($o.nick == 'a')])->sort(~id->ascending())",
                List.of(List.of(1L, false), List.of(2L, true)));
        cases.put("|p::O.all()->project(~[id: o|$o.id, e: o|($o.nick == 'a') || ($o.id == 7)])->sort(~id->ascending())",
                List.of(List.of(1L, true), List.of(2L, false)));
        cases.put("|p::O.all()->project(~[id: o|$o.id, e: o|if($o.id > 0, |$o.nick == 'a', |true)])"
                + "->sort(~id->ascending())", List.of(List.of(1L, true), List.of(2L, false)));
        // a comparison used as an operand, and read as text
        cases.put("|p::O.all()->project(~[id: o|$o.id, e: o|($o.nick == 'a') == false])->sort(~id->ascending())",
                List.of(List.of(1L, false), List.of(2L, true)));
        cases.put("|p::O.all()->project(~[id: o|$o.id, e: o|($o.nick == 'a')->toString()])->sort(~id->ascending())",
                List.of(List.of(1L, "true"), List.of(2L, "false")));
        // a window's column, and an aggregate's input (both rows are counted: a false is a value)
        cases.put("|p::O.all()->project(~[id: o|$o.id, nick: o|$o.nick])"
                + "->extend(over(~id), ~e: {p, w, r | $r.nick == 'a'})->select(~[id, e])->sort(~id->ascending())",
                List.of(List.of(1L, true), List.of(2L, false)));
        cases.put("|p::O.all()->project(~[id: o|$o.id, nick: o|$o.nick])"
                + "->groupBy(~[], ~[c: r | $r.nick == 'a' : y | $y->count()])", List.of(List.of(2L)));
        // BOTH sides may be empty: two empties are equal, as a column and in a filter
        String either = "if($o.id > 100, |'zz', |$o.nick) == $o.nick";
        cases.put("|p::O.all()->project(~[id: o|$o.id, e: o|" + either + "])->sort(~id->ascending())",
                List.of(List.of(1L, true), List.of(2L, true)));
        cases.put("|p::O.all()->filter(o|" + either + ")->project(~[id: o|$o.id])->sort(~id->ascending())",
                List.of(List.of(1L), List.of(2L)));
        // ONE comparison read at two places, as a condition and as a value (a function reads its argument twice; the
        // architecture audit of step 6): false || (false == false) is true, so the row whose nick is empty is kept
        cases.put("|p::O.all()->filter(o|p::either($o.nick == 'a'))->project(~[id: o|$o.id])->sort(~id->ascending())",
                List.of(List.of(1L), List.of(2L)));
        // under a negation "no value" is not false: the row whose nick is empty is kept, as Pure keeps it
        cases.put("|p::O.all()->filter(o|!($o.nick == 'a' || $o.id == 7))->project(~[id: o|$o.id])",
                List.of(List.of(2L)));
        cases.put("|p::O.all()->filter(o|!($o.nick == 'a' && $o.id == 1))->project(~[id: o|$o.id])",
                List.of(List.of(2L)));
        // every row's comparison must hold: the row whose NICK is empty answers false, so no row passes the filter
        // (it was "no value", which a NOT EXISTS of the rows that fail did not count: both rows passed)
        String rows = "#>{p::DB.OPT}#";
        cases.put("|" + rows + "->filter(r | " + rows + "->forAll(o | $o.NICK == 'a'))->select(~[ID])", List.of());
        cases.put("|" + rows + "->filter(r | " + rows + "->exists(o | $o.NICK == 'a'))->select(~[ID])"
                + "->sort(~ID->ascending())", List.of(List.of(1L), List.of(2L)));
        // as a query's answer
        Map<String, Object> answers = new LinkedHashMap<>();
        answers.put("|[1, 2]->filter(x | $x > 5)->first() == 1", List.of(List.of(false)));
        answers.put("|!([1, 2]->filter(x | $x > 5)->first() == 1)", List.of(List.of(true)));
        answers.put("|[1, 2]->filter(x | $x > 1)->first() == 2", List.of(List.of(true)));
        answers.put("|([1, 2]->filter(x | $x > 5)->first() == 1)->toString()", List.of(List.of("false")));
        // a list lambda's result: an element for each, none lost
        answers.put("|[1, 2]->map(x | [1]->filter(y | $y == $x)->first() == 1)", List.of(List.of(true), List.of(false)));
        answers.put("|[1, 2]->forAll(x | [1]->filter(y | $y == $x)->first() == 1)", List.of(List.of(false)));
        answers.put("|[1, 2]->exists(x | [1]->filter(y | $y == $x)->first() == 1)", List.of(List.of(true)));
        // ONE comparison read in an exists' list and in its lambda (the final review of the closing batch, S4): the
        // list's element is a kept value, false where nick is empty, so [false]->exists(x | $x == false || ...) holds
        answers.put("|p::O.all()->filter(o|p::listed($o.nick == 'a'))->project(~[id: o|$o.id])->sort(~id->ascending())",
                List.of(List.of(1L), List.of(2L)));
        cases.putAll(answers);
        // as a condition: as it was
        cases.put("|p::O.all()->filter(o|$o.nick == 'a')->project(~[id: o|$o.id])", List.of(List.of(1L)));
        cases.put("|p::O.all()->filter(o|!($o.nick == 'a'))->project(~[id: o|$o.id])", List.of(List.of(2L)));
        cases.put("|p::O.all()->project(~[id: o|$o.id, c: o|if($o.nick == 'a', |'same', |'other')])"
                + "->sort(~id->ascending())", List.of(List.of(1L, "same"), List.of(2L, "other")));
        List<String> wrong = new ArrayList<>();
        for (var backend : TestBackends.inProcess()) {
            String model = MODEL.formatted(backend.type());
            for (Map.Entry<String, Object> c : cases.entrySet()) {
                if (answers.containsKey(c.getKey()) && "H2".equals(String.valueOf(backend.type()))) {
                    // (the first element of a filtered list is a list read inside an expression, which H2 holds no
                    // form for: refused there by name, before step 6 and after -- the database follow-up)
                    continue;
                }
                try (Connection connection = backend.open()) {
                    Lean.Result r = Lean.established(model, c.getKey(), "p::RT", connection);
                    if (!c.getValue().equals(r.rows())) {
                        wrong.add(backend.type() + ": " + c.getKey() + " -> " + r.rows());
                    }
                } catch (RuntimeException e) {
                    wrong.add(backend.type() + ": " + c.getKey() + " -> " + e.getClass().getSimpleName() + ": "
                            + String.valueOf(e.getMessage()).lines().findFirst().orElse(""));
                }
            }
        }
        assertEquals(List.of(), wrong, "every case on every database");
    }
}
