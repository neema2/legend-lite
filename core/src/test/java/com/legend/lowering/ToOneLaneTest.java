// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lowering;

import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.test.StorelessRuntime;

import com.legend.Execution;
import com.legend.exec.ExecutionResult;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * COMPILER_SHORTCUT_AUDIT §1a (Blocker 2) — the {@code toOne} checked
 * lane is decided from the TYPED operand ({@link CollectionLanes}),
 * never by sniffing the emitted SQL. The audit's table: every row here
 * previously flowed a multi-element list back as a scalar because the
 * shape predicate (ArrayLit / producesList) missed the emission —
 * Case (slice's bound-check wrapper, lowered ifs), RANGE_FN,
 * descending sort. Value-lane collections carry pure's raising
 * semantics; size != 1 raises IN THE DATABASE with pure's message.
 */
class ToOneLaneTest {

    private static Connection conn;

    @BeforeAll
    static void open() throws Exception {
        conn = DriverManager.getConnection("jdbc:duckdb:");
    }

    @AfterAll
    static void close() throws Exception {
        conn.close();
    }

    private static Object run(String query) throws Exception {
        ExecutionResult r = Execution.execute(StorelessRuntime.with("", DatabaseType.DuckDB), query, StorelessRuntime.RUNTIME, conn);
        return r instanceof ExecutionResult.Scalar s ? s.value()
                : r instanceof ExecutionResult.Collection c ? c.values() : r;
    }

    private static void assertSizeError(String query, int size) {
        var ex = assertThrows(Exception.class, () -> run(query));
        assertTrue(ex.getMessage().contains(
                        "size " + size + " to multiplicity [1]"),
                "expected pure's size-" + size + " cast error for "
                        + query + ", got: " + ex.getMessage());
    }

    @Test
    @DisplayName("audit rows: literal and Call shapes still raise")
    void literalAndCallShapes() throws Exception {
        assertSizeError("{|[1,2]->toOne()}", 2);
        assertSizeError("{|[1,2]->filter(x|true)->toOne()}", 2);
        assertSizeError("{|[3,1,2]->sort()->toOne()}", 3);
    }

    @Test
    @DisplayName("audit row: slice's Case wrapper no longer hides the list")
    void sliceRaises() throws Exception {
        assertSizeError("{|[1,2]->slice(0,2)->toOne()}", 2);
    }

    @Test
    @DisplayName("audit row: if-branch collections raise")
    void ifRaises() throws Exception {
        assertSizeError("{|if(true,|[1,2],|[3,4])->toOne()}", 2);
    }

    @Test
    @DisplayName("audit row: range() raises")
    void rangeRaises() throws Exception {
        assertSizeError("{|range(1,5)->toOne()}", 4);
    }

    @Test
    @DisplayName("audit row: descending sort has the same guarantee as ascending")
    void sortDescRaises() throws Exception {
        assertSizeError("{|[3,1,2]->sort({a,b|$b->compare($a)})->toOne()}", 3);
        assertSizeError("{|[3,1,2]->reverse()->toOne()}", 3);
    }

    @Test
    @DisplayName("audit row: zip raises (audit-of-blockers probe, pinned)")
    void zipRaises() throws Exception {
        assertSizeError("{|[1,2]->zip([3,4])->toOne()}", 2);
    }

    @Test
    @DisplayName("DEEP_AUDIT R1: take/limit carry the lane (the whitelist's TypedLimit hole)")
    void takeLimitCarryTheLane() throws Exception {
        assertEquals(1L, ((Number) run("{|[1,2]->take(1)->toOne()}"))
                .longValue());
        assertSizeError("{|[1,2]->take(2)->toOne()}", 2);
        assertSizeError("{|[1,2]->limit(2)->toOne()}", 2);
        // (value-lane sortBy itself is an honest unimplemented wall —
        // its lane arm is in place for when the emission lands)
    }

    @Test
    @DisplayName("a literal of optional elements is refused, as legend-engine and legend-pure refuse it")
    void optionalElementsAreRefused() {
        Exception e = assertThrows(Exception.class, () -> run("{|[[]->first(), 'a']->toOne()}"));
        assertTrue(e.getMessage().contains("Collection element must have a multiplicity [1]"), e.getMessage());
    }

    @Test
    @DisplayName("size-1 value-lane collections extract through every shape")
    void sizeOneExtracts() throws Exception {
        assertEquals(1L, ((Number) run("{|[1,2]->slice(0,1)->toOne()}"))
                .longValue());
        assertEquals(7L, ((Number) run("{|if(true,|[7],|[8])->toOne()}"))
                .longValue());
        assertEquals(3L, ((Number) run("{|range(3,4)->toOne()}"))
                .longValue());
    }
}
