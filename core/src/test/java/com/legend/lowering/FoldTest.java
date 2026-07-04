package com.legend.lowering;

import com.legend.sql.SqlExpr;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlSource;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The fold authority tested DIRECTLY: occupancy state in, placement out —
 * including the HAVING/QUALIFY slots that integration tests cannot reach
 * until groupBy/window lowering exists. Every rule and its boundary.
 */
class FoldTest {

    private static final SqlSelect BARE = SqlSelect.starOf(
            new SqlSource.Table("T", "t0", List.of()));

    private static SqlExpr col(String n) {
        return new SqlExpr.Column("t0", n);
    }

    @Test
    @DisplayName("filter slots: WHERE bare; HAVING over groupBy; QUALIFY over window refs")
    void filterSlots() {
        assertEquals(Fold.FilterSlot.WHERE, Fold.filterSlot(BARE, false));
        // ORDER BY does NOT force isolation — filtering commutes with sorting.
        assertEquals(Fold.FilterSlot.WHERE,
                Fold.filterSlot(BARE.withOrderBy(List.of(SqlSelect.SortKey.asc(col("A")))), false));
        assertEquals(Fold.FilterSlot.HAVING,
                Fold.filterSlot(BARE.withGroupBy(List.of(col("A"))), false));
        assertEquals(Fold.FilterSlot.QUALIFY, Fold.filterSlot(BARE, true));
    }

    @Test
    @DisplayName("filter isolates on every truncation/dedup boundary")
    void filterBoundaries() {
        assertEquals(Fold.FilterSlot.ISOLATE, Fold.filterSlot(BARE.withLimit(5L), false));
        assertEquals(Fold.FilterSlot.ISOLATE, Fold.filterSlot(BARE.withOffset(2L), false));
        assertEquals(Fold.FilterSlot.ISOLATE, Fold.filterSlot(BARE.withDistinct(), false));
        // ...even when the predicate would otherwise take HAVING or QUALIFY.
        assertEquals(Fold.FilterSlot.ISOLATE,
                Fold.filterSlot(BARE.withGroupBy(List.of(col("A"))).withLimit(1L), false));
        assertEquals(Fold.FilterSlot.ISOLATE, Fold.filterSlot(BARE.withLimit(1L), true));
    }

    @Test
    @DisplayName("projection folds until DISTINCT or truncation")
    void projectionRules() {
        assertTrue(Fold.projectionFolds(BARE));
        assertTrue(Fold.projectionFolds(BARE.withOrderBy(List.of(SqlSelect.SortKey.asc(col("A"))))));
        assertFalse(Fold.projectionFolds(BARE.withDistinct()));
        assertFalse(Fold.projectionFolds(BARE.withLimit(1L)));
        assertFalse(Fold.projectionFolds(BARE.withOffset(1L)));
    }

    @Test
    @DisplayName("narrowing distinct requires every ORDER BY key to survive")
    void distinctNarrowRules() {
        SqlSelect sorted = BARE.withOrderBy(List.of(SqlSelect.SortKey.asc(col("AGE"))));
        assertTrue(Fold.distinctNarrowFolds(sorted, List.of("AGE", "NAME")));
        assertFalse(Fold.distinctNarrowFolds(sorted, List.of("NAME")));
        assertTrue(Fold.distinctNarrowFolds(BARE, List.of("NAME")), "no ORDER BY, nothing to lose");
    }

    @Test
    @DisplayName("single-slot rules: sort, limit, offset, distinct")
    void singleSlotRules() {
        assertTrue(Fold.sortFolds(BARE));
        assertFalse(Fold.sortFolds(BARE.withOrderBy(List.of(SqlSelect.SortKey.asc(col("A"))))));
        assertTrue(Fold.limitFolds(BARE));
        assertFalse(Fold.limitFolds(BARE.withLimit(1L)));
        assertTrue(Fold.offsetFolds(BARE));
        assertFalse(Fold.offsetFolds(BARE.withOffset(1L)));
        assertFalse(Fold.offsetFolds(BARE.withLimit(1L)), "offset after limit shrinks the window");
        assertTrue(Fold.distinctFolds(BARE));
        assertFalse(Fold.distinctFolds(BARE.withOrderBy(List.of(SqlSelect.SortKey.asc(col("A"))))));
        assertFalse(Fold.distinctFolds(BARE.withGroupBy(List.of(col("A")))));
        assertFalse(Fold.distinctFolds(BARE.withLimit(1L)));
    }

    @Test
    @DisplayName("groupBy folds only onto a clean select; extend only minds DISTINCT")
    void groupByAndExtendRules() {
        assertTrue(Fold.groupByFolds(BARE));
        assertFalse(Fold.groupByFolds(BARE.withGroupBy(List.of(col("A")))));
        assertFalse(Fold.groupByFolds(BARE.withDistinct()));
        assertFalse(Fold.groupByFolds(BARE.withOrderBy(List.of(SqlSelect.SortKey.asc(col("A"))))),
                "grouping does not preserve order");
        assertFalse(Fold.groupByFolds(BARE.withLimit(1L)));

        assertTrue(Fold.extendFolds(BARE));
        assertTrue(Fold.extendFolds(BARE.withLimit(1L)),
                "extend commutes with truncation — row count untouched");
        assertTrue(Fold.extendFolds(BARE.withOrderBy(List.of(SqlSelect.SortKey.asc(col("A"))))));
        assertFalse(Fold.extendFolds(BARE.withDistinct()),
                "extending a deduped set would dedup WITH the new column");
    }

    @Test
    @DisplayName("resolveInto sees THROUGH a star projection to source columns")
    void resolveThroughStar() {
        SqlSelect extended = BARE.withProjections(List.of(
                new SqlSelect.Projection(new SqlExpr.Star("t0"), null),
                new SqlSelect.Projection(SqlExpr.Call.of("plus", col("A"), col("B")), "computed")),
                List.of());
        assertEquals(col("AGE"), Fold.resolveInto(extended, "AGE"),
                "unclaimed names pass through the star to the source");
        assertNull(Fold.resolveInto(extended, "computed"),
                "the computed column still refuses substitution");
    }

    @Test
    @DisplayName("resolveInto: star → source column; plain projection → substituted; computed → null")
    void resolveIntoRules() {
        assertEquals(col("AGE"), Fold.resolveInto(BARE, "AGE"));
        SqlSelect projected = BARE.withProjections(List.of(
                new SqlSelect.Projection(col("AGE"), "YEARS"),
                new SqlSelect.Projection(col("NAME"), null),
                new SqlSelect.Projection(SqlExpr.Call.of("plus", col("A"), col("B")), "SUM_AB")),
                List.of());
        assertEquals(col("AGE"), Fold.resolveInto(projected, "YEARS"),
                "renamed column substitutes to its source");
        assertEquals(col("NAME"), Fold.resolveInto(projected, "NAME"));
        assertNull(Fold.resolveInto(projected, "SUM_AB"),
                "computed projection cannot fold — caller isolates");
        assertNull(Fold.resolveInto(projected, "DROPPED"),
                "a column not in the projection is gone");
    }
}
