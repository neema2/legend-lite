package com.legend.compiler.spec;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.legend.Compiler;
import com.legend.compiler.element.ModelContext;
import org.junit.jupiter.api.Test;

/**
 * The ERASED ROW (docs/TDS_ERASURE_DESIGN_2026_09_11.md §4b) is read only through its owner's
 * accessors, as on real pure's nominal TDSRow. Before, it was the raw-SQL grid's wildcard and
 * trusted any column name: the relation {@code filter} refused {@code $x.nope}, the overload
 * rollback accepted {@code meta::pure::tds::filter} over the erased row, and the query typed --
 * where legend-engine refuses it ("The column 'nope' can't be found in the relation").
 */
class TdsRowReceiverTest {

    private static final String MODEL = """
            ###Relational
            Database t::DB
            (
                Table TRADES (id INTEGER PRIMARY KEY, desk VARCHAR(32), qty INTEGER)
            )
            """;

    private static final ModelContext CTX = Compiler.compileModel(MODEL);

    private static void type(String query) {
        Compiler.query(CTX, query).resultType();
    }

    @Test
    void aFilterOnAColumnTheRelationLacksIsRefusedAtTyping() {
        RuntimeException e = assertThrows(RuntimeException.class,
                () -> type("|#>{t::DB.TRADES}#->filter(x|$x.nope == 1)"));
        assertTrue(e.getMessage().contains("relation has no column 'nope'"), e.getMessage());
    }

    @Test
    void aFilterOnItsOwnColumnsTypes() {
        assertDoesNotThrow(() -> type("|#>{t::DB.TRADES}#->filter(x|$x.desk == 'FX')"));
    }

    @Test
    void anAccessorReadOnAnErasedRowTypes() {
        assertDoesNotThrow(() -> type("|#>{t::DB.TRADES}#->filter({r:meta::pure::tds::TDSRow[1]|$r.getString('desk') == 'FX'})"));
    }

    @Test
    void aTdsRowReadOnAKnownRelationStillTypes() {
        assertDoesNotThrow(() -> type("|#>{t::DB.TRADES}#->filter(r|$r.getString('desk') == 'FX')"));
    }
}
