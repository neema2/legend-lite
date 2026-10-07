package com.legend.compiler.spec;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.legend.Compiler;
import com.legend.compiler.element.ModelContext;
import org.junit.jupiter.api.Test;

/**
 * The ERASED ROW (docs/TDS_ERASURE_DESIGN_2026_09_11.md §4b) is read only through its owner's
 * accessors, as legend-pure's nominal TDSRow is (the engine's core/pure/tds/tds.pure:76-120 declares
 * {@code get}, {@code isNull}, {@code isNotNull} and the typed getters as TDSRow's qualified
 * properties, and no bare column). Before, the erased row was the raw-SQL grid's wildcard and trusted
 * any column name: the relation {@code filter} refused {@code $x.nope}, the overload rollback accepted
 * {@code meta::pure::tds::filter} over the erased row, and the query typed where legend-engine refuses
 * it at typing ("The column 'nope' can't be found in the relation"). The parity claim is the outcome
 * and the error class (plan rule 0b.13); a message text is a secondary pin.
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

    /** A lambda over a declared TDSRow types at its own let against the erased row
     *  (Pure.java's note on let-bound TDSRow lambdas), with no relation in sight. */
    private static final String ROW = "{r:meta::pure::tds::TDSRow[1]|";

    private static void type(String query) {
        Compiler.query(CTX, query).resultType();
    }

    @Test
    void aFilterOnAColumnTheRelationLacksIsRefusedAtTyping() {
        TypeInferenceException e = assertThrows(TypeInferenceException.class,
                () -> type("|#>{t::DB.TRADES}#->filter(x|$x.nope == 1)"));
        // the first-ranked candidate's refusal (the relation filter's) is what surfaces when
        // every candidate fails (Overloads rethrows firstFailure); the erased-row refusal of
        // the rollback candidate is what makes every candidate fail
        assertTrue(e.getMessage().contains("relation has no column 'nope'"), e.getMessage());
    }

    @Test
    void aFilterOnItsOwnColumnsTypes() {
        assertDoesNotThrow(() -> type("|#>{t::DB.TRADES}#->filter(x|$x.desk == 'FX')"));
    }

    @Test
    void aTypedGetterOnAKnownRelationTypes() {
        assertDoesNotThrow(() -> type("|#>{t::DB.TRADES}#->filter(r|$r.getString('desk') == 'FX')"));
    }

    @Test
    void aBareColumnOnAnErasedRowIsRefused() {
        TypeInferenceException e = assertThrows(TypeInferenceException.class,
                () -> type("|let f = " + ROW + "$r.desk == 'FX'}; true;"));
        assertTrue(e.getMessage().contains("meta::pure::tds::TDSRow has no property 'desk'"), e.getMessage());
    }

    @Test
    void theTypedGettersReadAnErasedRow() {
        assertDoesNotThrow(() -> type("|let f = " + ROW + "($r.getString('desk') == 'FX') && ($r.getInteger('qty') > 1)}; true;"));
    }

    /** The shape of legend-engine's own m2m filter tests ({@code $res.values->at(0).rows->at(0).get('legalName')}):
     *  the untyped accessors are desugars that read a cell; on an erased row that read is the
     *  accessor's cell, never a bare column (the first cut refused these: reference lane, 2 bodies). */
    @Test
    void theUntypedAccessorsReadAnErasedRow() {
        assertDoesNotThrow(() -> type("|let f = " + ROW + "$r.isNotNull('desk') && $r.isNull('qty')"
                + " && ($r.get('desk') == 'FX') && ($r.get('desk')->toString() == 'FX')}; true;"));
    }
}
