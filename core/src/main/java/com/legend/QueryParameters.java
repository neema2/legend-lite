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

        /** The lite plan's slot: the value the statement binds where the parameter is used, typed as a literal of its
         *  declared type is ({@link #valueType}), which a dialect that types a placeholder writes; for a list, one
         *  array of such values (docs/EXECUTION_PLAN_BOUNDARY_2026_10_05.md §9, step 2's landing 2). */
        public com.legend.sql.SqlExpr.PlanParam slot() {
            if (type instanceof Type.EnumType && optional()) {
                // its ABSENCE has three candidate answers: legend-engine's plan writes `0 = 1` (no row for ==, every
                // row for !=), Pure's equality holds for [] == [], and today's let path compares a NULL; unmeasured
                throw new com.legend.error.NotImplementedException("parameter '" + name + "' (" + type.typeName()
                        + "[0..1]): an optional enumeration's absence is not bound until its answer is measured against"
                        + " legend-engine (PARK-21)");
            }
            com.legend.sql.TypeFact value = valueType();
            if (multiplicity instanceof Multiplicity.Bounded b && Integer.valueOf(1).equals(b.upper())) {
                return new com.legend.sql.SqlExpr.PlanParam(name, com.legend.lowering.PlanParams.kindOf(type),
                        optional(), null, value);
            }
            // a list: ONE array of its values, its element type named for the driver
            if (!(value instanceof com.legend.sql.TypeFact.Typed element)) {
                throw new com.legend.error.NotImplementedException("parameter '" + name + "' (" + type.typeName()
                        + "[*]): a list of decimals, Dates or Numbers has no one element type a driver's array keeps"
                        + " exactly (DuckDB's rounds a decimal to 3 places): not bound (PARK-20)");
            }
            return new com.legend.sql.SqlExpr.PlanParam(name, com.legend.lowering.PlanParams.kindOf(type), false, null,
                    com.legend.sql.SqlTyping.typed(new com.legend.sql.SqlType.Array(element.type())));
        }

        /** One value's type, as a literal of the declared type carries it: an enumeration's is its NAME's
         *  ({@code VARCHAR}; a comparison with a mapped column translates it through that place's value table,
         *  {@code EnumValueTables}); a decimal literal's type is its own digits', a Date's or a Number's value decides
         *  its kind, so theirs is unknown. */
        private com.legend.sql.TypeFact valueType() {
            if (type instanceof Type.EnumType) {
                return com.legend.sql.SqlTyping.typed(com.legend.sql.SqlType.Scalar.VARCHAR);
            }
            if (!(type instanceof Type.Primitive primitive)) {
                // Pure takes a class instance as a parameter (the legacy printer writes its properties, ${p.name});
                // a lite plan binds plain values only (§9, step 2's decisions)
                throw new com.legend.error.NotImplementedException("parameter '" + name + "' (" + type.typeName()
                        + "): a class instance is not bound as a plan's parameter, only plain values -- a primitive, an"
                        + " enumeration's value, a list of them (PARK-21)");
            }
            return switch (primitive) {
                case INTEGER -> com.legend.sql.SqlTyping.typed(com.legend.sql.SqlType.Scalar.BIGINT);
                case STRING -> com.legend.sql.SqlTyping.typed(com.legend.sql.SqlType.Scalar.VARCHAR);
                case BOOLEAN -> com.legend.sql.SqlTyping.typed(com.legend.sql.SqlType.Scalar.BOOLEAN);
                case STRICT_DATE -> com.legend.sql.SqlTyping.typed(com.legend.sql.SqlType.Scalar.DATE);
                case DATE_TIME -> com.legend.sql.SqlTyping.typed(com.legend.sql.SqlType.Scalar.TIMESTAMP);
                case FLOAT, DECIMAL, DATE, NUMBER -> com.legend.sql.SqlTyping.UNKNOWN;
                case BYTE, LATEST_DATE, STRICT_TIME -> throw new com.legend.error.NotImplementedException("parameter '"
                        + name + "' (" + type.typeName() + "): a Byte, LatestDate or StrictTime value is not bound as a"
                        + " plan's parameter (PARK-21)");
            };
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
