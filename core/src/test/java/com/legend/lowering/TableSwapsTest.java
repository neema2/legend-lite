// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lowering;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.legend.compiler.element.type.ExprType;
import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.typed.TypedCInteger;
import com.legend.compiler.spec.typed.TypedTableReference;
import com.legend.compiler.spec.typed.TypedTableSwap;
import com.legend.error.NotImplementedException;
import com.legend.sql.NameQuoting;
import com.legend.sql.SqlQuery;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlSource;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

/**
 * THE READ-ONLY RELOAD'S SWAP, BY TYPED IDENTITY (B2d.2's audit, M1, 2026-11-11): a typed table reference whose
 * {@code store|physical name} the swap reads (a table of the reloaded database or of one it includes, whatever database
 * names it) is read from the slice's rows for its physical name; another database's table is read as it is, unless it
 * shares a reloaded table's physical name; a reloaded table left read by its name alone is refused by name.
 */
class TableSwapsTest {

    private static final ExprType ANY = ExprType.one(Type.Primitive.INTEGER);
    private static final SqlQuery PERSON_ROWS = SqlSelect.starOf(new SqlSource.Dual());
    private static final TypedTableSwap SWAP = new TypedTableSwap(new TypedCInteger(1L, ANY), Set.of("t::DB|PERSON",
            "t::Included|S.FIRM", "t::Including|PERSON", "t::DB|GHOST"), Map.of("PERSON", PERSON_ROWS, "S.FIRM",
            SqlSelect.starOf(new SqlSource.Dual())), ANY);

    @Test
    void aReloadedTableIsReadFromTheSliceWhateverDatabaseNamesIt() {
        assertSame(PERSON_ROWS, TableSwaps.rows(new TypedTableReference("t::DB", "PERSON", ANY), SWAP));
        // a table of an included database, and an including database's reference to the reloaded one's
        assertSame(SWAP.tables().get("S.FIRM"), TableSwaps.rows(new TypedTableReference("t::Included", "S.FIRM", ANY),
                SWAP));
        assertSame(PERSON_ROWS, TableSwaps.rows(new TypedTableReference("t::Including", "PERSON", ANY), SWAP));
        // a swap that reads a table it holds no rows for: the planner's bug, never a fallback
        assertThrows(IllegalStateException.class,
                () -> TableSwaps.rows(new TypedTableReference("t::DB", "GHOST", ANY), SWAP));
    }

    @Test
    void anotherDatabasesTableIsReadAsItIsUnlessItSharesAReloadedName() {
        assertNull(TableSwaps.rows(new TypedTableReference("t::Other", "ACCOUNT", ANY), SWAP));
        NotImplementedException e = assertThrows(NotImplementedException.class,
                () -> TableSwaps.rows(new TypedTableReference("t::Other", "PERSON", ANY), SWAP));
        assertEquals("the query run over the test-data generator's reloaded slice reads PERSON of t::Other, a table the"
                + " reload does not replace by the physical name of one it does: not planned (B2d)", e.getMessage());
    }

    @Test
    void theRecordedScanIsSwappedUnderItsAlias() {
        SqlSource.Table person = table("PERSON", "_a1", false);
        SqlSelect swapped = TableSwaps.swapped(SqlSelect.starOf(person), SWAP, Map.of("_a1", PERSON_ROWS));
        assertTrue(swapped.from() instanceof SqlSource.Subselect s && s.inner() == PERSON_ROWS
                && "_a1".equals(s.alias()), String.valueOf(swapped.from()));
        // the scan's columns are the table's: what the source reads it by
        assertEquals(SqlSelect.starOf(person).outputs(), swapped.outputs());
    }

    @Test
    void aReloadedTableLeftReadByItsNameIsRefusedByName() {
        // a scan the lowering did not record (a calendar's table, a name a post-processor wrote)
        NotImplementedException e = assertThrows(NotImplementedException.class,
                () -> TableSwaps.swapped(SqlSelect.starOf(table("PERSON", "_a2", false)), SWAP, Map.of()));
        assertEquals("the query run over the test-data generator's reloaded slice reads the table PERSON by its name,"
                + " which the read-only reload cannot answer: not planned (B2d)", e.getMessage());
        // a tabular function's call and another table: untouched
        SqlSelect call = SqlSelect.starOf(table("PERSON", "_a3", true));
        assertSame(call.from(), TableSwaps.swapped(call, SWAP, Map.of()).from());
        SqlSelect other = SqlSelect.starOf(table("ACCOUNT", "_a4", false));
        assertSame(other.from(), TableSwaps.swapped(other, SWAP, Map.of()).from());
    }

    private static SqlSource.Table table(String name, String alias, boolean call) {
        return new SqlSource.Table(name, alias, List.of(), call, Map.of(), NameQuoting.BARE);
    }
}
