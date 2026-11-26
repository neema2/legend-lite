// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.integration;

import com.legend.edge.PrintTail;
import com.legend.edge.PureRelationText;
import com.legend.testbackend.StorelessRuntime;
import com.legend.testbackend.TestBackend;
import com.legend.testing.LaneFloor;
import com.legend.testing.Lean;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * THE TAIL OF A PRINT AGAINST THE DATABASE (the edition program's step 8, 2026-11-26): each member of the tail's
 * closed set ({@code edge.PrintTail}), applied in Java, answers what lite's own SQL answers for the same Pure call on
 * the same text, on every lane this test runs on -- so taking a tail off a program at a compatibility door moves no
 * answer on ordinary text. The texts are plain ones as a printed table holds: letters, digits, commas, spaces and line
 * breaks. Where Java and lite's SQL part, Java's is legend-pure's own answer and {@code PrintTailTest} holds the case:
 * a character outside the basic plane (Java counts two, a database one), and {@code trim} of a text that begins or
 * ends with a line break or another character below a space -- legend-pure's {@code trim} is Java's
 * {@code String.trim}, which strips them, where lite's own SQL strips spaces alone (measured here 2026-11-26 on DuckDB,
 * H2 and SQLite: {@code 'id,name\n1,A\n2,B\n'->trim()} keeps its last line break in lite's SQL). So {@code trim} is compared
 * on the texts whose ends are spaces or printable.
 */
@org.junit.jupiter.api.extension.ExtendWith(LaneFloor.class)
class PrintTailParityTest {

    @Test
    void eachMemberAnswersAsLitesOwnSqlDoes() throws Exception {
        TestBackend backend = LaneFloor.backend();
        String model = backend.withStorelessRuntime("Class s::P { name: String[1]; }\n");
        // the call as Pure writes it after the text -> the step
        Map<String, PrintTail> steps = new LinkedHashMap<>();
        steps.put("->replace('\\n', ';')", new PureRelationText.Replace("\n", ";"));
        steps.put("->replace(',', '')", new PureRelationText.Replace(",", ""));
        steps.put("->length()", new PrintTail.Length());
        steps.put("->trim()", new PrintTail.Trim());
        steps.put("->startsWith('#TDS')", new PrintTail.StartsWith("#TDS"));
        steps.put("->startsWith('id')", new PrintTail.StartsWith("id"));
        steps.put("->endsWith('#')", new PrintTail.EndsWith("#"));
        steps.put("->endsWith('B')", new PrintTail.EndsWith("B"));
        steps.put("->contains('1,A')", new PrintTail.Contains("1,A"));
        steps.put("->contains('1,a')", new PrintTail.Contains("1,a"));
        steps.put("->indexOf(',')", new PrintTail.IndexOf(",", null));
        steps.put("->indexOf('zz')", new PrintTail.IndexOf("zz", null));
        steps.put("->indexOf(',', 4)", new PrintTail.IndexOf(",", 4L));
        List<String> texts = List.of("#TDS\n   id,name\n   1,A\n   2,B\n#", "id,name\n1,A\n2,B\n", "  1,A  ", "x");
        try (Connection c = backend.open()) {
            for (String text : texts) {
                for (var e : steps.entrySet()) {
                    if (e.getValue() instanceof PrintTail.Trim && (text.charAt(0) < ' '
                            || text.charAt(text.length() - 1) < ' ')) {
                        continue;
                    }
                    String query = "|'" + text.replace("\n", "\\n") + "'" + e.getKey();
                    Object database = Lean.run(model, query, StorelessRuntime.RUNTIME, c).rows().get(0).get(0);
                    assertEquals(String.valueOf(e.getValue().applied(text)), String.valueOf(database),
                            backend.name() + ": " + query);
                    // and what legend-pure's door itself answered for the call before the tail was lifted: the
                    // edge's own plan of it (the independent audit of slice 2, 2026-11-26)
                    Object edges = Lean.runInStyle(model, query, StorelessRuntime.RUNTIME, backend,
                            com.legend.edge.PureText.UPSTREAM).rows().get(0).get(0);
                    assertEquals(String.valueOf(e.getValue().applied(text)), String.valueOf(edges),
                            backend.name() + " at legend-pure's door: " + query);
                }
            }
        }
    }

    /**
     * WHERE JAVA AND LITE'S SQL PART (the independent audit of slice 2, 2026-11-26, its S4 and S7): the same call,
     * Java's answer (legend-pure's own: {@code PrintTailTest}) beside what each lane's database answers for it at
     * legend-pure's door, where it stands one call away from a print. Measured on every lane 2026-11-26; each
     * database's answer is one of the forms listed, and none is a register row yet (they are the product's
     * behaviours, older than the tail, drafted for the user in
     * docs/records/edition-homework/step8/slice2-repair-2026-11-26.md):
     * <ul>
     *   <li>a character above U+FFFF: Java counts two units; a database counts as it counts, one or two;</li>
     *   <li>{@code replace('', x)}: Java puts {@code x} at every position; lite's SQL leaves the text as it is;</li>
     *   <li>{@code trim}: Java strips every character up to a space; lite's SQL strips spaces alone;</li>
     *   <li>{@code indexOf} from a start past an int: Java wraps the start, as legend-pure's {@code intValue()} does,
     *       and searches from the beginning; lite's SQL answers -1, or the database refuses the number.</li>
     * </ul>
     * Where they agree on an input the earlier test left out (an empty needle, a start outside the text), that is
     * held too.
     */
    @Test
    void whereJavaAndLitesSqlPart_eachIsHeld() throws Exception {
        TestBackend backend = LaneFloor.backend();
        String model = backend.withStorelessRuntime("Class s::P { name: String[1]; }\n");
        String astral = "a\uD83D\uDE00b";
        record Case(String query, Object javas, List<String> databases) {
        }
        List<Case> cases = List.of(
                new Case("|'" + astral + "'->length()", new PrintTail.Length().applied(astral), List.of("3", "4")),
                new Case("|'" + astral + "'->indexOf('b')", new PrintTail.IndexOf("b", null).applied(astral),
                        List.of("2", "3")),
                new Case("|'abc'->replace('', 'x')", new PureRelationText.Replace("", "x").applied("abc"),
                        List.of("abc")),
                new Case("|'\\ta\\n'->trim()->length()", (long) ((String) new PrintTail.Trim().applied("\ta\n")).length(),
                        List.of("3")),
                new Case("|'abcb'->indexOf('b', 2147483648)", new PrintTail.IndexOf("b", 2147483648L).applied("abcb"),
                        List.of("-1", "REFUSED")),
                // agreed, on inputs the first test left out
                new Case("|'abc'->indexOf('')", new PrintTail.IndexOf("", null).applied("abc"), List.of("0")),
                new Case("|'abc'->indexOf('', 5)", new PrintTail.IndexOf("", 5L).applied("abc"), List.of("3")),
                new Case("|'abc'->startsWith('')", new PrintTail.StartsWith("").applied("abc"), List.of("true")),
                new Case("|'abc'->endsWith('')", new PrintTail.EndsWith("").applied("abc"), List.of("true")),
                new Case("|'abc'->contains('')", new PrintTail.Contains("").applied("abc"), List.of("true")),
                new Case("|'abcb'->indexOf('b', 99)", new PrintTail.IndexOf("b", 99L).applied("abcb"), List.of("-1")),
                new Case("|'abcb'->indexOf('b', -5)", new PrintTail.IndexOf("b", -5L).applied("abcb"), List.of("1")));
        List<Object> javas = List.of(4L, 3L, "xaxbxcx", 1L, 1L, 0L, 3L, true, true, true, -1L, 1L);
        for (int i = 0; i < cases.size(); i++) {
            Case c = cases.get(i);
            assertEquals(javas.get(i), c.javas(), "Java's, legend-pure's: " + c.query());
            String database;
            try {
                database = String.valueOf(Lean.runInStyle(model, c.query(), StorelessRuntime.RUNTIME, backend,
                        com.legend.edge.PureText.UPSTREAM).rows().get(0).get(0));
            } catch (RuntimeException refused) {
                database = "REFUSED";
            }
            org.junit.jupiter.api.Assertions.assertTrue(c.databases().contains(database),
                    backend.name() + " " + c.query() + ": " + database + ", not one of " + c.databases());
        }
    }
}
