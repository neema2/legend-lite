// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.integration;

import com.legend.edge.PrintTail;
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
 * ends with a control character that is no white space (below U+0009, or U+000E to U+001F) -- legend-pure's
 * {@code trim} is Java's {@code String.trim}, which strips every character up to a space, where lite's SQL strips
 * the six white-space characters among them (a tab, the line breaks, a space: the user's ruling of 2026-11-28, D8;
 * until then spaces alone, and {@code 'id,name\n1,A\n2,B\n'->trim()} kept its last line break, measured here
 * 2026-11-26 on DuckDB, H2 and SQLite). So {@code trim} is compared on texts whose ends are white space or printable.
 *
 * <p>WHERE THEY PART, each database's answer is RECORDED, one per case per database (the audit's re-check of slice 2,
 * 2026-11-26, its S7: a test here had accepted any of several answers and any exception): the {@code tail.*} cases of
 * {@code EdgeCasesGoldenTest}, in {@code edge_cases_answers.tsv}, on every database the platform runs in-process; and
 * Java's, legend-pure's own answer for the same inputs, is {@code PrintTailTest}'s.
 */
@org.junit.jupiter.api.extension.ExtendWith(LaneFloor.class)
class PrintTailParityTest {

    @Test
    void eachMemberAnswersAsLitesOwnSqlDoes() throws Exception {
        TestBackend backend = LaneFloor.backend();
        String model = backend.withStorelessRuntime("Class s::P { name: String[1]; }\n");
        // the call as Pure writes it after the text -> the step
        Map<String, PrintTail> steps = new LinkedHashMap<>();
        steps.put("->replace('\\n', ';')", new PrintTail.Replace("\n", ";"));
        steps.put("->replace(',', '')", new PrintTail.Replace(",", ""));
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
        // (a text that begins or ends with a line break, a tab or a carriage return is trimmed alike since the
        // user's ruling of 2026-11-28, D8: lite's trim strips what Pure's does; until then such a text was skipped
        // under trim here, lite's SQL keeping it)
        List<String> texts = List.of("#TDS\n   id,name\n   1,A\n   2,B\n#", "id,name\n1,A\n2,B\n", "  1,A  ", "x",
                "\t 1,A \r\n", "\n\n", " \t ");
        try (Connection c = backend.open()) {
            for (String text : texts) {
                for (var e : steps.entrySet()) {
                    String query = "|'" + text.replace("\n", "\\n").replace("\t", "\\t").replace("\r", "\\r") + "'"
                            + e.getKey();
                    Object database = Lean.run(model, query, StorelessRuntime.RUNTIME, c).rows().get(0).get(0);
                    assertEquals(String.valueOf(e.getValue().applied(text)), String.valueOf(database),
                            backend.name() + ": " + query);
                    // and the edge's own plan of the same call over the same text (the independent audit of slice 2,
                    // 2026-11-26): legend-pure's door ran the call in the edge's SQL before the tail was lifted --
                    // over lite's text of the relation there, over this literal here
                    Object edges = Lean.runInStyle(model, query, StorelessRuntime.RUNTIME, backend,
                            com.legend.edge.PureText.UPSTREAM).rows().get(0).get(0);
                    assertEquals(String.valueOf(e.getValue().applied(text)), String.valueOf(edges),
                            backend.name() + " at legend-pure's door: " + query);
                }
            }
        }
    }
}
