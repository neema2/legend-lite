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
 * ONE SIDE OF AN EQUALITY HOLDS NO VALUE (the edition program's step 6; the user, 2026-11-27: "Yes do the empty side
 * fix", then "why cant we do the work we promised to get to one equality?"): {@code $x.nick == 'a'} where {@code nick}
 * is empty is false, as Pure says ({@code [] == 'a'}), wherever the comparison stands. As a condition -- a filter's,
 * an {@code if}'s -- it was right already; where its VALUE is kept (a column of it, a query's answer) it was the
 * database's NULL, a cell with no value, at every door (measured 2026-11-27). The lowering now marks the equalities a
 * kept value is made of ({@code Lowerer.keepingItsValue}) and each answers false where one side is empty
 * ({@code Equalities.falseWhereASideIsEmpty}); a condition's statement is as it was.
 */
class EmptySideEqualityTest {

    private static final String MODEL = """
            ###Pure
            Class p::O { id: Integer[1]; nick: String[0..1]; n: Integer[0..1]; }
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
        // as a query's answer
        Map<String, Object> answers = new LinkedHashMap<>();
        answers.put("|[1, 2]->filter(x | $x > 5)->first() == 1", List.of(List.of(false)));
        answers.put("|!([1, 2]->filter(x | $x > 5)->first() == 1)", List.of(List.of(true)));
        answers.put("|[1, 2]->filter(x | $x > 1)->first() == 2", List.of(List.of(true)));
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
