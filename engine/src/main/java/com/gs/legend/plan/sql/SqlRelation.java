package com.gs.legend.plan.sql;

import com.gs.legend.model.m3.Type;
import com.gs.legend.sqlgen.SqlExpr;

import java.util.List;
import java.util.Map;

/**
 * Relational-algebra MIR sitting between typed HIR ({@link com.gs.legend.compiler.typed.TypedSpec})
 * and dialect-specific SQL text. Immutable algebraic tree.
 *
 * <p><strong>Invariants</strong>:
 * <ul>
 *   <li>All variants are immutable records.</li>
 *   <li>No dialect imports; no {@code toSql()}. Dialect-specific rendering lives in
 *       {@code com.gs.legend.plan.printing.SqlRelationPrinter}.</li>
 *   <li>Every relation knows its {@link #outputs()} schema so downstream passes can
 *       resolve column references without re-walking the tree.</li>
 *   <li>Scalar sub-expressions are {@link SqlExpr} (already a clean sealed hierarchy).</li>
 * </ul>
 *
 * <p>Lowering (HIR &rarr; MIR) is the job of {@code plan/lowering/**}. Printing
 * (MIR &rarr; text) is the job of {@code plan/printing/**}. This split moves the
 * spaghetti in today's {@code SqlBuilder} into two small, testable concerns with
 * a stable in-between IR.
 *
 * <p>Stage 1 skeleton: records carry their canonical fields; some helpers'
 * {@code outputs()} return empty lists until the corresponding lowering rule is
 * ported.
 */
public sealed interface SqlRelation {

    /**
     * Ordered output column schema. Downstream ops reference columns by name;
     * the printer uses this to stamp aliases and fuse/wrap decisions.
     */
    List<OutputCol> outputs();

    /**
     * Top-level SQL alias at which this relation's columns can be addressed.
     * {@code null} when this relation is not itself a FROM-clause-ready term (e.g.,
     * a raw {@link Filter}, {@link Project}, {@link Sort}, &hellip;) — wrap in a
     * {@link SubqueryRel} first to obtain an alias. Sources ({@link TableRef},
     * {@link Values}, {@link SourceExprRel}) and {@link SubqueryRel} override.
     */
    default String alias() { return null; }

    // ==================== Sources ====================

    /** Physical table reference. */
    record TableRef(String schema, String table, String alias,
                    List<OutputCol> outputs) implements SqlRelation {}

    /** Inline {@code VALUES (...)} table literal. */
    record Values(List<List<SqlExpr>> rows, List<String> columnNames,
                  String alias, List<OutputCol> outputs) implements SqlRelation {}

    /**
     * Scalar-as-relation: wraps a {@link SqlExpr} so a whole-plan root that is
     * really a scalar query (e.g., {@code 1 + 1}, {@code 'hello'}) can flow through
     * the same pipeline as a real relation. Printer emits {@code SELECT <expr> AS <alias>}.
     */
    record SourceExprRel(SqlExpr expr, String alias,
                         List<OutputCol> outputs) implements SqlRelation {}

    // ==================== Unary relational ops ====================

    record Filter(SqlRelation source, SqlExpr predicate) implements SqlRelation {
        @Override public List<OutputCol> outputs() { return source.outputs(); }
    }

    record Project(SqlRelation source, List<Projection> projections) implements SqlRelation {
        @Override public List<OutputCol> outputs() {
            return projections.stream().map(p -> new OutputCol(p.alias(), null)).toList();
        }
    }

    record Sort(SqlRelation source, List<SortKey> keys) implements SqlRelation {
        @Override public List<OutputCol> outputs() { return source.outputs(); }
    }

    /** {@code n < 0} means no limit. {@code offset < 0} means no offset. */
    record Limit(SqlRelation source, long n, long offset) implements SqlRelation {
        @Override public List<OutputCol> outputs() { return source.outputs(); }
    }

    record Distinct(SqlRelation source) implements SqlRelation {
        @Override public List<OutputCol> outputs() { return source.outputs(); }
    }

    record Rename(SqlRelation source, Map<String, String> renames) implements SqlRelation {
        @Override public List<OutputCol> outputs() {
            return source.outputs().stream()
                    .map(c -> new OutputCol(renames.getOrDefault(c.name(), c.name()), c.type()))
                    .toList();
        }
    }

    record Select(SqlRelation source, List<String> columns) implements SqlRelation {
        @Override public List<OutputCol> outputs() {
            return source.outputs().stream().filter(c -> columns.contains(c.name())).toList();
        }
    }

    record Extend(SqlRelation source, List<ExtendCol> cols) implements SqlRelation {
        @Override public List<OutputCol> outputs() {
            var out = new java.util.ArrayList<>(source.outputs());
            for (var c : cols) out.add(new OutputCol(c.name(), null));
            return List.copyOf(out);
        }
    }

    record GroupBy(SqlRelation source, List<SqlExpr> keys, List<Agg> aggs) implements SqlRelation {
        @Override public List<OutputCol> outputs() {
            var out = new java.util.ArrayList<OutputCol>();
            for (int i = 0; i < keys.size(); i++) out.add(new OutputCol("k" + i, null));
            for (var a : aggs) out.add(new OutputCol(a.alias(), null));
            return List.copyOf(out);
        }
    }

    record Aggregate(SqlRelation source, List<Agg> aggs) implements SqlRelation {
        @Override public List<OutputCol> outputs() {
            return aggs.stream().map(a -> new OutputCol(a.alias(), null)).toList();
        }
    }

    record Flatten(SqlRelation source, String arrayColumn, String elementAlias,
                   List<OutputCol> outputs) implements SqlRelation {}

    record Pivot(SqlRelation source, PivotSpec spec, List<OutputCol> outputs) implements SqlRelation {}

    // ==================== Binary relational ops ====================

    record Join(SqlRelation left, SqlRelation right, JoinType type, SqlExpr on) implements SqlRelation {
        @Override public List<OutputCol> outputs() {
            var out = new java.util.ArrayList<>(left.outputs());
            out.addAll(right.outputs());
            return List.copyOf(out);
        }
    }

    record AsOfJoin(SqlRelation left, SqlRelation right, AsOfSpec spec) implements SqlRelation {
        @Override public List<OutputCol> outputs() {
            var out = new java.util.ArrayList<>(left.outputs());
            out.addAll(right.outputs());
            return List.copyOf(out);
        }
    }

    record Union(SqlRelation left, SqlRelation right, boolean all) implements SqlRelation {
        @Override public List<OutputCol> outputs() { return left.outputs(); }
    }

    // ==================== Wrapping ====================

    record SubqueryRel(SqlRelation inner, String alias) implements SqlRelation {
        @Override public List<OutputCol> outputs() { return inner.outputs(); }
    }

    record WithCtes(List<Cte> ctes, SqlRelation body, boolean recursive) implements SqlRelation {
        @Override public List<OutputCol> outputs() { return body.outputs(); }
    }

    // ==================== Helper records ====================

    /** Single output column: name + optional carried Pure type (may be null before typing). */
    record OutputCol(String name, Type type) {}

    /** Projected column: {@code expr AS alias}. */
    record Projection(String alias, SqlExpr expr) {}

    /** Sort key: expression, direction, null ordering. */
    record SortKey(SqlExpr expr, Direction direction, NullOrder nulls) {
        public enum Direction { ASC, DESC }
        public enum NullOrder { DEFAULT, FIRST, LAST }
    }

    /** Aggregate: {@code function(args...) AS alias}. {@code distinct} covers {@code COUNT(DISTINCT …)}. */
    record Agg(String alias, String function, List<SqlExpr> args, boolean distinct) {}

    /** Extend column: new column defined by a scalar expression or window call. */
    record ExtendCol(String name, SqlExpr expr) {}

    /** Kinds of joins. */
    enum JoinType { INNER, LEFT, RIGHT, FULL, CROSS }

    /** Placeholder for pivot specs until {@code TypedPivot} is ported (Stage 3). */
    record PivotSpec(List<String> groupingKeys, String pivotKey, List<SqlExpr> pivotValues,
                     List<Agg> aggs) {}

    /** Placeholder for as-of-join specs until {@code TypedAsOfJoin} is ported (Stage 3). */
    record AsOfSpec(SqlExpr matchPredicate, List<SortKey> orderKeys) {}

    /** Named CTE. */
    record Cte(String name, SqlRelation body) {}
}
