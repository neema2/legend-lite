// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.integration;

import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.testbackend.StorelessRuntime;
import com.legend.testbackend.TestBackend;
import com.legend.testing.LaneFloor;
import com.legend.testing.Lean;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A WINDOW'S COLUMN READ THROUGH A LIST OPERATION (2026-11-19, the compatibility program's H11 record §8 P1): a relation
 * with a window column, and a later column that reads it through {@code split}, {@code sort} and {@code joinStrings},
 * read as rows at the statement's root. The lowering computes the window in a select of its own and the list
 * operation over its column; SQLite and Postgres write a list operation as a query of its own (a correlated subquery
 * over {@code json_each}, a recursive split, an {@code unnest}), so a window merged into that operation
 * ({@code SelectMerge}) is computed there, over one row: SQLite and Postgres answered {@code A} and {@code E} for the
 * group whose answer is {@code AE}, and Postgres refused the chunked form ("window functions are not allowed in
 * functions in FROM"). The expected rows are PCT's, legend-engine 4.145.0's
 * {@code composition::testExtendJoinStringOnNull} and {@code extend::testOLAPAggStringWithPartitionAndUnboundedWindow}.
 * Run on each lane's database (DuckDB, H2, SQLite) and on the embedded Postgres
 * ({@code WindowReadThroughAListPostgresTest}); H2 has no list encoding and refuses each by name.
 */
@org.junit.jupiter.api.extension.ExtendWith(LaneFloor.class)
class WindowReadThroughAListTest {

    /** composition::testExtendJoinStringOnNull's relation and columns, sorted as its assertion sorts them. */
    static final String SPLIT_SORTED = "{|#TDS\n"
            + "  id, grp, name\n"
            + "  1, 2, A\n"
            + "  2, 1, null\n"
            + "  3, 3, C\n"
            + "  4, 4, V\n"
            + "  5, 2, E\n"
            + "  6, 1, F\n"
            + "  7, 3, G\n"
            + "  8, 1, null\n"
            + "  9, 5, I\n"
            + "  10, 0, J\n"
            + "#->extend(over(~grp), ~newCol:{p,w,r|$r.name}:y|$y->joinStrings(':'))"
            + "->extend(~newColSorted:r|$r.newCol->toOne()->split(':')->sort()->joinStrings(''))"
            + "->select(~[id, grp, name, newColSorted])"
            + "->sort([~grp->ascending(), ~id->ascending()])}";

    static final List<List<Object>> SPLIT_SORTED_ROWS = List.of(
            row(10, 0, "J", "J"), row(2, 1, null, "F"), row(6, 1, "F", "F"), row(8, 1, null, "F"),
            row(1, 2, "A", "AE"), row(5, 2, "E", "AE"), row(3, 3, "C", "CG"), row(7, 3, "G", "CG"),
            row(4, 4, "V", "V"), row(9, 5, "I", "I"));

    /** extend::testOLAPAggStringWithPartitionAndUnboundedWindow's: a whole-partition frame, the text chunked. */
    static final String CHUNKED = "{|#TDS\n"
            + "  id, grp, name\n"
            + "  1, 2, A\n"
            + "  2, 1, B\n"
            + "  3, 3, C\n"
            + "  4, 4, D\n"
            + "  5, 2, E\n"
            + "  6, 1, F\n"
            + "  7, 3, G\n"
            + "  8, 1, H\n"
            + "  9, 5, I\n"
            + "  10, 0, J\n"
            + "#->extend(over(~grp, rows(meta::pure::functions::relation::unbounded(),"
            + " meta::pure::functions::relation::unbounded())), ~newCol:{p,w,r|$r.name}:y|$y->joinStrings(''))"
            + "->extend(~newColSorted:r|$r.newCol->toOne()->chunk(1)->sort()->joinStrings(''))"
            + "->sort([~grp->ascending(), ~id->ascending()])->select(~[id, grp, name, newColSorted])}";

    static final List<List<Object>> CHUNKED_ROWS = List.of(
            row(10, 0, "J", "J"), row(2, 1, "B", "BFH"), row(6, 1, "F", "BFH"), row(8, 1, "H", "BFH"),
            row(1, 2, "A", "AE"), row(5, 2, "E", "AE"), row(3, 3, "C", "CG"), row(7, 3, "G", "CG"),
            row(4, 4, "D", "D"), row(9, 5, "I", "I"));

    /** The same read of a GROUP's value (an aggregate, the other value of a select's level): the groups' names joined,
     *  then split and sorted per row. */
    static final String GROUPED = "{|#TDS\n"
            + "  id, grp, name\n"
            + "  1, 2, E\n"
            + "  2, 1, B\n"
            + "  3, 2, A\n"
            + "  4, 1, C\n"
            + "  5, 3, D\n"
            + "#->groupBy(~grp, ~names:x|$x.name:y|$y->joinStrings(':'))"
            + "->extend(~sorted:r|$r.names->toOne()->split(':')->sort()->joinStrings(''))"
            + "->select(~[grp, sorted])->sort([~grp->ascending()])}";

    static final List<List<Object>> GROUPED_ROWS = List.of(row(1, "BC"), row(2, "AE"), row(3, "D"));

    private static List<Object> row(Object... cells) {
        List<Object> r = new ArrayList<>(cells.length);
        for (Object c : cells) {
            r.add(c instanceof Integer i ? (Object) i.longValue() : c);
        }
        return r;
    }

    /** Each shape on {@code backend}'s database: PCT's rows, or (H2) the capability refusal it names. */
    static void answered(TestBackend backend) throws Exception {
        String model = backend.withStorelessRuntime("");
        try (Connection c = backend.open()) {
            for (var shape : List.of(List.of(SPLIT_SORTED, SPLIT_SORTED_ROWS), List.of(CHUNKED, CHUNKED_ROWS),
                    List.of(GROUPED, GROUPED_ROWS))) {
                String query = (String) shape.get(0);
                @SuppressWarnings("unchecked")
                List<List<Object>> expected = (List<List<Object>>) shape.get(1);
                if (backend.type() == DatabaseType.H2) {
                    // H2 has no list encoding: refused by name, never answered (the channel B ledger's H2 rows)
                    Exception e = assertThrows(Exception.class,
                            () -> Lean.run(model, query, StorelessRuntime.RUNTIME, c), query);
                    assertTrue(String.valueOf(e.getMessage()).contains("reached a dialect without a list encoding"),
                            query + ": " + e.getMessage());
                    continue;
                }
                Lean.Result r = Lean.run(model, query, StorelessRuntime.RUNTIME, c);
                assertEquals(expected, normalized(r.rows()), backend.name() + ": " + query);
            }
        }
    }

    /** Cells as the expected rows spell them: an integer a long. */
    private static List<List<Object>> normalized(List<List<Object>> rows) {
        List<List<Object>> out = new ArrayList<>(rows.size());
        for (List<Object> r : rows) {
            out.add(row(r.stream().map(x -> x instanceof Number n && !(x instanceof Long)
                    ? (Object) n.longValue() : x).toArray()));
        }
        return out;
    }

    @Test
    @DisplayName("P1: a window's column read through a list operation answers per partition, not per row")
    void onTheLane() throws Exception {
        answered(LaneFloor.backend());
    }
}
