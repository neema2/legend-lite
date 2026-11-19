package com.legend.exec;

import com.legend.database.Databases;
import com.legend.testbackend.StorelessRuntime;
import com.legend.testbackend.TestBackends;
import com.legend.testing.EdgeProgram;
import com.legend.testing.Program;
import org.junit.jupiter.api.Test;

import java.sql.Connection;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * TaggedListTest's one case on the compatibility edge (F's census line A5, 2026-11-17: split out of the product's test so
 * that test names no edge class; B17's raw-grid pin): in the edge's style a raw grid's text is refused by name -- its
 * header is written from columns that exist only when it runs. The product refuses any relation's toString at lowering
 * (TaggedListTest).
 */
class TaggedListEdgeTest {

    @Test
    void aRawGridsTextIsRefusedByNameOnTheEdge() throws Exception {
        try (Connection c = TestBackends.platform().open()) {
            Exception grid = assertThrows(Exception.class, () -> com.legend.testing.EdgeProgram.last(StorelessRuntime.with(
                    TaggedListTest.MODEL, Databases.PLATFORM), TaggedListTest.CONN_LET + "$rs->toString();}",
                    StorelessRuntime.RUNTIME, c, com.legend.testing.EdgeProgram.upstream()));
            assertTrue(String.valueOf(grid.getMessage()).contains("toString over a raw grid"), grid.toString());
        }
    }
}
