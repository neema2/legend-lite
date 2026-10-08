// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

import com.legend.compiler.element.ModelContext;
import com.legend.compiler.element.type.Multiplicity;
import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.typed.TypedLambda;
import com.legend.executionplan.ExecutionPlan;

import java.util.ArrayList;
import java.util.List;

/**
 * A query's declared parameters, read ONCE (docs/EXECUTION_PLAN_BOUNDARY_2026_10_05.md §9, step 2: one parameter list).
 * Every plan reads them here: the lite plan's declarations ({@link Declared#declaration}) and the legacy,
 * legend-engine-shaped plan's two forms — its text and the node model Pure code walks — which spell the same facts
 * as template parameters ({@link Declared#planParam}) and signatures ({@link Declared#signature}), and whose enum
 * template functions are emitted for the enum-typed ones (PlanAllocations).
 */
public final class QueryParameters {

    private QueryParameters() {
    }

    /** One declared parameter: its name, its type and its multiplicity, as the query's lambda declares them. */
    public record Declared(String name, Type type, Multiplicity multiplicity) {

        /** {@code [0..1]}: a value or none. */
        public boolean optional() {
            return multiplicity instanceof Multiplicity.Bounded b && b.lower() == 0
                    && Integer.valueOf(1).equals(b.upper());
        }

        /** The type's Pure name: a primitive's ({@code String}, {@code StrictDate}, ...) or an enumeration's path. */
        public String pureType() {
            return com.legend.plan.PlanText.pureTypeName(type);
        }

        /** {@code Type[multiplicity]}, as legend-engine's plans and messages spell a parameter. */
        public String signature() {
            return pureType() + "[" + com.legend.plan.PurePrint.sizeRange(multiplicity) + "]";
        }

        /** The legacy plan's template parameter: the placeholder kind its type spells, whether it is optional, and
         *  for an enum the template function that maps it ({@code enumMapFn}, the printer's; null otherwise). */
        public com.legend.sql.SqlExpr.PlanParam planParam(@com.legend.base.Nullable String enumMapFn) {
            return new com.legend.sql.SqlExpr.PlanParam(name, com.legend.lowering.PlanParams.kindOf(type), optional(),
                    enumMapFn);
        }

        /** The lite plan's declaration: the type's Pure name, the multiplicity's bounds and, for an enumeration, the
         *  names a value may take — what the runner checks a caller's value against, with no model. */
        public ExecutionPlan.Parameter declaration(ModelContext ctx) {
            if (!(multiplicity instanceof Multiplicity.Bounded b)) {
                throw new IllegalStateException("parameter '" + name + "': multiplicity " + multiplicity
                        + " is not a declared bound");
            }
            List<String> enumValues = type instanceof Type.EnumType et
                    ? ctx.findEnum(et.fqn()).orElseThrow(() -> new IllegalStateException("parameter '" + name
                            + "': enumeration " + et.fqn() + " is not in the model")).values()
                    : List.of();
            return new ExecutionPlan.Parameter(name, pureType(), new ExecutionPlan.Multiplicity(b.lower(), b.upper()),
                    enumValues);
        }
    }

    /** A typed lambda's parameters, in declaration order. */
    public static List<Declared> of(TypedLambda lambda) {
        var params = lambda.functionType().params();
        List<Declared> out = new ArrayList<>(lambda.parameters().size());
        for (int i = 0; i < lambda.parameters().size(); i++) {
            out.add(new Declared(lambda.parameters().get(i), params.get(i).type(), params.get(i).multiplicity()));
        }
        return out;
    }
}
