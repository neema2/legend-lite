package com.legend.exec;

import com.legend.compiler.element.type.Multiplicity;
import com.legend.compiler.element.type.Type;
import com.legend.compiler.element.type.ExprType;

/**
 * THE result-shape classification (PHASE_HIJ_LOWERING.md, "a first-class
 * axis"): decided ONCE at the query root from its Pure type — a closed
 * switch, never a heuristic — and drives the SQL envelope and the
 * {@link ExecutionResult} variant. Per-op lowering never branches on it.
 */
public enum ResultShape {
    TABULAR, GRAPH, COLLECTION, SCALAR;

    /**
     * Node-aware classification (post-H roots): GRAPH is the RESOLVER's
     * envelope — a class-typed root WITHOUT one is an instance VALUE (a
     * struct: {@code find} over instance literals, a constructed pair) and
     * takes the value shapes. The type alone cannot distinguish the two.
     */
    public static ResultShape of(com.legend.compiler.spec.typed.TypedSpec root) {
        if (root instanceof com.legend.compiler.spec.typed.TypedFrom f) {
            return of(f.source());
        }
        if (root instanceof com.legend.compiler.spec.typed.TypedSerializeGraph) {
            return GRAPH;
        }
        if (root.info().type() instanceof Type.ClassType ct && !ct.fqn().endsWith("::Variant")
                && !ct.fqn().equals("meta::pure::metamodel::type::Nil")) {
            return isMany(root.info().multiplicity()) ? COLLECTION : SCALAR;
        }
        return of(root.info());
    }

    public static ResultShape of(ExprType root) {
        if (root.type() instanceof Type.RelationType) {
            return TABULAR;
        }
        if (root.type() instanceof Type.ClassType ct) {
            // Variant is a SCALAR JSON value, not an object graph — the
            // lowering types it as JSON (audit L8); every other class value
            // is a graph fetch.
            if (ct.fqn().endsWith("::Variant")) {
                return isMany(root.multiplicity()) ? COLLECTION : SCALAR;
            }
            // Nil is the []-born BOTTOM type — an always-empty VALUE
            // ([]->head() is Nil[0..1] = null), never an object graph.
            if (ct.fqn().equals("meta::pure::metamodel::type::Nil")) {
                return isMany(root.multiplicity()) ? COLLECTION : SCALAR;
            }
            return GRAPH;
        }
        return isMany(root.multiplicity()) ? COLLECTION : SCALAR;
    }

    private static boolean isMany(Multiplicity m) {
        return m.requireBounded("ResultShape").isMany();
    }
}
