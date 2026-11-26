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
                }
            }
        }
    }
}
