// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lowering;

import com.legend.compiler.element.type.Type;
import com.legend.database.PlanStyle;
import com.legend.error.LegendCompileException;
import com.legend.error.ModelException;
import com.legend.sql.SqlExpr;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * THE PRODUCT REFUSES EACH OF legend-pure's ONLY TEXTS BY NAME (census B8, HookRulingTest's RULED entry; C5's step A,
 * 2026-11-17, the orchestrator's condition on VerdictSql's HOOK-RULED line): every member of {@link PureOnlyTexts} --
 * representationOf and formatR among them -- is reached only through the style's facet, asked with its call's refusal,
 * and the product's style (both {@link Styled#PRODUCT} and the one {@code PlanStyle.PRODUCT} configures) throws that
 * refusal before any member is reached. The member list is pinned, so a new member is seen and given its refusal here.
 */
class PureOnlyTextsRefusalTest {

    static final String REPRESENTATION = "toRepresentation is legend-pure's text, the compatibility edge's: the product"
            + " writes a value's text by toString and format's %s, %d, %f and %t{}";

    /** Each member of the facet, with the call it makes once the product's style hands the facet out (it never does). */
    private static final Map<String, Function<PureOnlyTexts, SqlExpr>> MEMBERS = Map.of(
            "formatPadded", t -> t.formatPadded(new SqlExpr.IntLit(1), new SqlExpr.IntLit(2)),
            "instanceText", t -> t.instanceText(new SqlExpr.StringLit("id")),
            "representationOf", t -> t.representationOf(Styled.PRODUCT, Type.Primitive.STRING, new SqlExpr.StringLit("a")),
            "formatR", t -> t.formatR(Styled.PRODUCT, null, new SqlExpr.StringLit("a")),
            "representation", t -> t.representation(new SqlExpr.StringLit("'a'")));

    @Test
    void theMembersArePinned() {
        assertEquals(MEMBERS.keySet(), Arrays.stream(PureOnlyTexts.class.getDeclaredMethods())
                .map(java.lang.reflect.Method::getName).collect(java.util.stream.Collectors.toSet()));
    }

    @Test
    void theProductsStyleRefusesEachMemberWithItsCallsRefusal() {
        Styled configured = new Styled(r -> PlanStyle.PRODUCT.facet(PureOnlyTexts.class, r), Styled.PRODUCT.plainRaises(),
                Styled.LITE_DATE_TIMES, ProductListTexts.INSTANCE);
        for (Styled styled : List.of(Styled.PRODUCT, configured)) {
            MEMBERS.forEach((name, call) -> {
                ModelException refusal = TextRefusals.representation();
                ModelException e = assertThrows(ModelException.class,
                        () -> call.apply(styled.pureOnly().apply(() -> refusal)), name);
                assertSame(refusal, e, name);
            });
        }
    }

    /** representationOf's two product call sites (Scalars' toRepresentation, VerdictSql's compatibility message) ask
     *  with this refusal: its sentence and phase. */
    @Test
    void representationsRefusalIsNamed() {
        ModelException e = assertThrows(ModelException.class,
                () -> Styled.PRODUCT.pureOnly().apply(TextRefusals::representation)
                        .representationOf(Styled.PRODUCT, Type.Primitive.STRING, new SqlExpr.StringLit("a")));
        assertEquals(REPRESENTATION, e.getMessage());
        assertEquals(LegendCompileException.Phase.LOWER, e.phase());
    }
}
