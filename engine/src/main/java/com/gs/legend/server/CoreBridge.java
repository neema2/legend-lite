package com.gs.legend.server;

import com.gs.legend.exec.Column;
import com.gs.legend.exec.ExecutionResult;
import com.gs.legend.exec.Row;
import com.gs.legend.model.m3.Primitive;
import com.gs.legend.model.m3.Type;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The core→engine result bijection (PHASE_K_EXECUTION.md, "THE BRIDGE RULE"):
 * ZERO decisions — field-by-field re-wrapping plus one static type table.
 * Representations are identical BY DESIGN, so nothing here converts values.
 * If a corpus test failure tempts a fix in this class, the fix belongs in
 * core. This class dies with the engine module; nothing gets ported.
 */
public final class CoreBridge {

    private CoreBridge() {
    }

    /**
     * The core&rarr;engine PLAN bijection: field-by-field re-wrapping of
     * {@link com.legend.exec.QueryPlan} &mdash; SQL verbatim, root type
     * through {@link #type}, multiplicity bound-for-bound, shape&rarr;format
     * by the static table below. Nothing is invented; an unresolved
     * multiplicity var at a plan root is a compiler bug, said loudly.
     */
    public static com.gs.legend.plan.SingleExecutionPlan toPlan(com.legend.exec.QueryPlan p) {
        com.gs.legend.model.m3.Multiplicity mult = switch (p.rootType().multiplicity()) {
            case com.legend.compiler.element.type.Multiplicity.Bounded b ->
                    new com.gs.legend.model.m3.Multiplicity.Bounded(b.lower(), b.upper());
            case com.legend.compiler.element.type.Multiplicity.Var v ->
                    throw new IllegalStateException("unresolved multiplicity var '"
                            + v.name() + "' at the plan root — compiler bug");
        };
        com.gs.legend.plan.ResultFormat format = switch (p.shape()) {
            case TABULAR -> new com.gs.legend.plan.ResultFormat.Tabular();
            case GRAPH -> new com.gs.legend.plan.ResultFormat.Graph();
            case COLLECTION, SCALAR -> new com.gs.legend.plan.ResultFormat.Scalar();
        };
        return new com.gs.legend.plan.SingleExecutionPlan(
                new com.gs.legend.plan.SQLExecutionNode(p.sql(), null, null),
                new com.gs.legend.compiler.ExpressionType(type(p.rootType().type()), mult),
                format);
    }

    static ExecutionResult toEngine(com.legend.exec.ExecutionResult r) {
        return switch (r) {
            case com.legend.exec.ExecutionResult.Scalar s ->
                    new ExecutionResult.ScalarResult(s.value(), type(s.returnType()));
            case com.legend.exec.ExecutionResult.Collection c ->
                    new ExecutionResult.CollectionResult(c.values(), type(c.returnType()));
            case com.legend.exec.ExecutionResult.Tabular t ->
                    new ExecutionResult.TabularResult(
                            t.columns().stream().map(CoreBridge::column).toList(),
                            t.rows().stream().map(row -> new Row(row.values())).toList(),
                            schema(t.columns()),
                            type(t.returnType()));
            case com.legend.exec.ExecutionResult.Graph g ->
                    new ExecutionResult.GraphResult(g.json(), type(g.returnType()));
        };
    }

    private static Column column(com.legend.exec.Column c) {
        return new Column(c.name(), c.sqlType(),
                c.pureType() == null ? null : c.pureType().typeName());
    }

    private static Type.Schema schema(List<com.legend.exec.Column> columns) {
        Map<String, Type> cols = new LinkedHashMap<>();
        for (com.legend.exec.Column c : columns) {
            cols.put(c.name(), c.pureType() == null ? Primitive.ANY : type(c.pureType()));
        }
        return new Type.Schema(cols, List.of());
    }

    /** The static type table — a bijection over what results can carry. */
    static Type type(com.legend.compiler.element.type.Type t) {
        return switch (t) {
            case com.legend.compiler.element.type.Type.Primitive p -> switch (p) {
                case NUMBER -> Primitive.NUMBER;
                case INTEGER -> Primitive.INTEGER;
                case FLOAT -> Primitive.FLOAT;
                case DECIMAL -> Primitive.DECIMAL;
                case STRING -> Primitive.STRING;
                case BOOLEAN -> Primitive.BOOLEAN;
                case DATE -> Primitive.DATE;
                case STRICT_DATE -> Primitive.STRICT_DATE;
                case DATE_TIME -> Primitive.DATE_TIME;
                case STRICT_TIME -> Primitive.STRICT_TIME;
                // Engine has no BYTE/LATEST_DATE result primitives; a result
                // root should never carry them — loud, not lossy.
                default -> throw new IllegalStateException(
                        "no engine Primitive for core " + p.name());
            };
            case com.legend.compiler.element.type.Type.PrecisionDecimal d ->
                    new Type.PrecisionDecimal(d.precision(), d.scale());
            case com.legend.compiler.element.type.Type.ClassType c ->
                    new Type.ClassType(c.fqn());
            // A parameterized class result root (Pair<Integer,String> from
            // zip) keeps its ARGUMENTS — the PCT wire rebuilds instances and
            // the interpreted cast validates the generics.
            case com.legend.compiler.element.type.Type.GenericType g ->
                    new Type.GenericType(new Type.ClassType(g.rawFqn()),
                            g.arguments().stream().map(CoreBridge::type).toList());
            case com.legend.compiler.element.type.Type.EnumType e ->
                    new Type.EnumType(e.fqn());
            case com.legend.compiler.element.type.Type.RelationType rt -> {
                Map<String, Type> cols = new LinkedHashMap<>();
                rt.columns().forEach(c -> cols.put(c.name(), type(c.type())));
                yield new Type.Relation(new Type.Schema(cols, List.of()));
            }
            default -> throw new IllegalStateException(
                    "no engine mapping for core result type " + t.typeName()
                            + " — a result root should never carry it");
        };
    }
}
