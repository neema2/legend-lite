// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lowering;

import com.legend.Compiler;
import com.legend.compiler.element.type.ExprType;
import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.typed.TypedJsonResult;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.database.PlanStyle;
import com.legend.error.LegendCompileException;
import com.legend.error.ModelException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * THE PRODUCT REFUSES legend-engine's RESULT ENVELOPE (Phase 6's B5, 2026-11-17, the orchestrator's ruling, told to the
 * user): an {@code executeLegendQuery}'s TDS, class or serialized envelope is the compatibility edge's
 * ({@link ResultEnvelopes}); the product's style refuses it at LOWER, by name, pointing to its result writers. Pinned
 * here: the sentence and the phase, on a Lowerer the product's style configured and on one no planner configured.
 */
class ResultEnvelopeRefusalTest {

    static final String SENTENCE = "executeLegendQuery's result envelope is legend-engine's JSON, the compatibility"
            + " edge's: the product writes a query's rows by its result writers (JSON, CSV, Arrow)";

    private static TypedSpec expression(String query) {
        return Compiler.query(Compiler.compileModel(""), query).expression();
    }

    private static void refused(Lowerer lowerer, TypedJsonResult.Kind kind, String query) {
        TypedSpec envelope = new TypedJsonResult(expression(query), kind, null, ExprType.one(Type.Primitive.STRING));
        ModelException e = assertThrows(ModelException.class, () -> lowerer.lower(envelope), kind.name());
        assertEquals(LegendCompileException.Phase.LOWER, e.phase(), kind.name());
        assertEquals(SENTENCE, e.getMessage(), kind.name());
    }

    @Test
    void theProductsStyleRefusesTheEnvelopeAtLowerByName() {
        for (boolean styled : new boolean[] {true, false}) {
            Lowerer lowerer = new Lowerer(PlatformRegistrations.catalogTable()).withStyled(com.legend.lowering.Styled.PRODUCT);
            if (styled) {
                lowerer = lowerer.withResultEnvelopes(r -> PlanStyle.PRODUCT.facet(ResultEnvelopes.class, r));
            }
            refused(lowerer, TypedJsonResult.Kind.STYLED, "|#TDS\n  a:Integer\n  1\n#");
        }
        // a serialized, relation or class root's: refused at PLANNING by the product's form of the planner's
        // ExecutedValues kind (train 17 item 2, 2026-11-18), the same sentence and phase, and no activity rendered
        TypedSpec chain = expression("|'x'");
        com.legend.planner.ExecutedValues form = PlanStyle.PRODUCT.twoForm(com.legend.planner.ExecutedValues.class,
                com.legend.planner.RefusedExecutedValues.REFUSED);
        java.util.function.Supplier<String> never = () -> {
            throw new AssertionError("the product renders no activity SQL");
        };
        for (org.junit.jupiter.api.function.Executable refusal : java.util.List.<org.junit.jupiter.api.function.Executable>of(
                () -> form.values(chain, Compiler.compileModel("")), () -> form.relation(chain, never),
                () -> form.classes(chain, never))) {
            ModelException e = assertThrows(ModelException.class, refusal);
            assertEquals(LegendCompileException.Phase.LOWER, e.phase());
            assertEquals(SENTENCE, e.getMessage());
        }
    }
}
