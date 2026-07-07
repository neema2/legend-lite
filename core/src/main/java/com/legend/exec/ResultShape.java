package com.legend.exec;

import com.legend.compiler.element.type.Multiplicity;
import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.ExprType;

/**
 * THE result-shape classification (PHASE_HIJ_LOWERING.md, "a first-class
 * axis"): decided ONCE at the query root from its Pure type — a closed
 * switch, never a heuristic — and drives the SQL envelope and the
 * {@link ExecutionResult} variant. Per-op lowering never branches on it.
 */
public enum ResultShape {
    TABULAR, GRAPH, COLLECTION, SCALAR;

    public static ResultShape of(ExprType root) {
        if (root.type() instanceof Type.RelationType) {
            return TABULAR;
        }
        if (root.type() instanceof Type.ClassType) {
            return GRAPH;
        }
        return isMany(root.multiplicity()) ? COLLECTION : SCALAR;
    }

    private static boolean isMany(Multiplicity m) {
        return m.requireBounded("ResultShape").isMany();
    }
}
