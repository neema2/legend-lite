// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.driver;

import com.legend.compiler.element.type.ExprType;
import com.legend.compiler.element.type.Type;
import com.legend.exec.Census;
import com.legend.exec.PlanRunner;
import com.legend.exec.PlanSessions;
import com.legend.exec.Statements;
import com.legend.exec.WireTypes;
import com.legend.executionplan.ExecutionPlan;
import com.legend.sql.OutputCol;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlQuery;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * THE WIRE PROBE (the probe half of {@code exec.WireTypes} until D71, 2026-10-30, the seam-and-wall program's B1):
 * a plan's outputs reconciled to the kinds the database REPORTS for them -- a prepare through the plan runner, no
 * execution ({@link PlanRunner#describe}) -- where the compiler's declared kind is wire-decided, and a plan the
 * compiler could not type given the database's columns ({@link #staticized}). Planning work: it renders through the
 * dialect and reads the compiler's types, so it lives with the driver, not in the runtime; the wire TABLE (JDBC's type
 * codes to the SQL vocabulary, {@link WireTypes}) stays in {@code exec}.
 *
 * <p>CENSUS (audit §4y, printed by the corpus lanes): a wire-decided BARE store column whose stamped kind differed
 * from the kind the database reported and was RE-TYPED to the wire ({@code WIRE_RETYPED}); and a column the
 * reconciliation LEAVES (a computed expression, a label carrier) whose reported kind differs from its slot's -- the
 * slot stands, the disagreement counted ({@code WIRE_SLOT_SKEW}).
 */
public final class WireProbe {
    private WireProbe() {
    }

    /** The compiler's type for a SQL type the database reports: the plan's NAME of it ({@link WireTypes#pureTypeNameOf},
     *  the wire table's) rebuilt as the typer's primitive on the driver side (D72, 2026-10-30: the runtime names a type,
     *  never holds one); an unmapped SQL type is refused by the table, by name. */
    public static Type pureOfSqlType(String sqlType) {
        return primitive(WireTypes.pureTypeNameOf(sqlType));
    }

    /** {@link #pureOfSqlType}'s lookup: null for an unmapped SQL type (a late-bound grid's column then the trusted Any). */
    public static @com.legend.base.Nullable Type pureOfSqlTypeOrNull(String sqlType) {
        String name = WireTypes.pureTypeNameOrNull(sqlType);
        return name == null ? null : primitive(name);
    }

    private static Type primitive(String name) {
        for (Type.Primitive p : Type.Primitive.values()) {
            if (p.typeName().equals(name)) {
                return p;
            }
        }
        throw new IllegalStateException("the wire table names '" + name + "', no primitive of the typer's");
    }

    /** A plan the compiler could not type (a raw {@code executeInDb} grid: no
     * outputs) framed as {@code SELECT * FROM (plan)} with the database's
     * reported columns as its outputs — the LIMIT-0 probe's own pattern
     * (ColumnProbe) for a staticized pivot; a column outside the vocabulary
     * carries VARCHAR (its kind is the golden's to infer). {@code plan}
     * itself when the database reports nothing. */
    public static SqlQuery staticized(SqlQuery plan, com.legend.sql.dialect.SqlDialect dialect,
            PlanSessions.Source sessions, ExecutionPlan.Target target, Map<String, List<WireTypes.ReportedColumn>> memo) {
        List<WireTypes.ReportedColumn> reported = memo.computeIfAbsent(dialect.render(plan),
                s -> reported(s, sessions, target));
        if (reported.isEmpty()) {
            return plan;
        }
        List<OutputCol> outs = new ArrayList<>(reported.size());
        for (WireTypes.ReportedColumn r : reported) {
            outs.add(new OutputCol(r.name(), r.type() == null ? SqlType.Scalar.VARCHAR : r.type(),
                    r.nullable()));
        }
        return SqlSelect.starOf(new com.legend.sql.SqlSource.Subselect(plan, "side", null))
                .withOutputs(outs);
    }

    /** {@code plan} with its wire-decided outputs reconciled to the
     * database's reported types, or {@code plan} itself when nothing is
     * wire-decided or the plan is not a projection frame. */
    public static SqlQuery reconcile(SqlQuery plan, ExprType shapeInfo,
            com.legend.sql.dialect.SqlDialect dialect, PlanSessions.Source sessions, ExecutionPlan.Target target,
            Map<String, List<WireTypes.ReportedColumn>> memo) {
        return reconcile(plan, plan, shapeInfo, dialect, sessions, target, memo);
    }

    /** With the frame definitions the plan may reference (leg 3.4 step 2):
     * the plan stays bare; the prepare sees them at the statement's head. */
    public static SqlQuery reconcile(SqlQuery plan, ExprType shapeInfo,
            com.legend.sql.dialect.SqlDialect dialect, PlanSessions.Source sessions, ExecutionPlan.Target target,
            Map<String, List<WireTypes.ReportedColumn>> memo, Map<String, SqlQuery> frames) {
        return reconcile(plan, com.legend.sql.FrameCtes.attach(plan, frames), shapeInfo,
                dialect, sessions, target, memo);
    }

    private static SqlQuery reconcile(SqlQuery plan, SqlQuery whole, ExprType shapeInfo,
            com.legend.sql.dialect.SqlDialect dialect, PlanSessions.Source sessions, ExecutionPlan.Target target,
            Map<String, List<WireTypes.ReportedColumn>> memo) {
        if (!(plan instanceof SqlSelect ps) || ps.projections().isEmpty()
                || ps.projections().size() != plan.outputs().size()) {
            return plan;
        }
        List<Type> declared = declaredKinds(shapeInfo, plan.outputs().size());
        if (declared == null || declared.stream().noneMatch(Type::wireDecided)) {
            return plan;
        }
        String sql = dialect.render(whole);
        List<WireTypes.ReportedColumn> reported = memo.computeIfAbsent(sql, s -> reported(s, sessions, target));
        if (reported.size() != plan.outputs().size()) {
            return plan;
        }
        List<SqlSelect.Projection> projections = new ArrayList<>(ps.projections());
        List<OutputCol> outputs = new ArrayList<>(plan.outputs());
        boolean changed = false;
        for (int i = 0; i < outputs.size(); i++) {
            SqlType wire = reported.get(i).type();
            if (wire == null || !Type.wireDecided(declared.get(i))) {
                continue;
            }
            SqlSelect.Projection p = projections.get(i);
            OutputCol col = outputs.get(i);
            Type wireKind = Type.kindOfSqlType(wire);
            if (wireKind == null || wireKind == Type.kindOfSqlType(col.type())) {
                continue;
            }
            // a BARE store-column reference only: its stamp is the store's
            // declaration, the one fact the fixture can contradict. A
            // computed expression keeps the compiler's own type — and is
            // never cast: H2 reports a computed DECIMAL at scale 0 while
            // the cell carries the value's real scale (the calendar rows)
            if (!(p.expr() instanceof SqlExpr.Column ref) || labelCarrier(col.type())) {
                Census.inc(Census.Key.WIRE_SLOT_SKEW);   // the slot stands; counted
                continue;
            }
            Census.inc(Census.Key.WIRE_RETYPED);
            if (com.legend.diagnostics.Diagnostics.dumpSql()) {
                System.err.println("[wire] " + col.name() + ": stamped " + col.type()
                        + ", the database reports " + wire);
            }
            // the reference re-typed, the value untouched (no cast)
            SqlExpr.Column retyped = SqlExpr.Column.of(ref.table(), ref.name(), wire,
                    col.nullable(), java.util.Objects.requireNonNullElse(ref.origin(),
                            OutputCol.Origin.DERIVED));
            projections.set(i, new SqlSelect.Projection(retyped, p.alias(),
                    p.out() == null ? null : withType(p.out(), wire)));
            outputs.set(i, withType(col, wire));
            changed = true;
        }
        return changed ? ps.withProjections(projections, outputs) : plan;
    }

    private static OutputCol withType(OutputCol c, SqlType t) {
        return new OutputCol(c.name(), t, c.nullable(), c.origin());
    }

    /** The declared kind per output: the relation's columns for a grid,
     * the root type for a one-column value; null when the shape does not
     * name one kind per output. */
    private static @com.legend.base.Nullable List<Type> declaredKinds(ExprType shapeInfo, int width) {
        Type.RelationType schema = Type.schemaView(shapeInfo.type());
        if (schema != null) {
            if (schema.isLateBound() || schema.columns().size() != width) {
                return null;
            }
            List<Type> kinds = new ArrayList<>(width);
            for (Type.Column c : schema.columns()) {
                kinds.add(c.type());
            }
            return kinds;
        }
        return width == 1 ? List.of(shapeInfo.type()) : null;
    }

    private static boolean labelCarrier(SqlType t) {
        return t == SqlType.Scalar.TEMPORAL_TEXT
                || t == SqlType.Scalar.DECIMAL_TEXT || t == SqlType.Scalar.JSON;
    }

    /** The database's reported type per column — a prepare, no execution;
     * a column outside the vocabulary reports null; a statement the
     * database cannot prepare raises the data error the verdict would have. */
    private static List<WireTypes.ReportedColumn> reported(String sql, PlanSessions.Source sessions,
            ExecutionPlan.Target target) {
        // a plan statement of the target, DESCRIBED by the plan runner on a session of the source, never run
        // (PlanRunner.describe: the doors, 2026-10-29; on the session seam since track B's B1a, until then a prepare
        // on a JDBC connection here) -- a statement the database cannot describe raises the data error the verdict
        // would have; none reported where the driver withholds them, as before
        List<WireTypes.ReportedColumn> out = new ArrayList<>();
        for (Statements.Reported r : PlanRunner.describe(ExecutionPlan.Sql.of(sql, target), sessions)) {
            out.add(new WireTypes.ReportedColumn(r.name(), r.type(), r.nullable()));
        }
        return out;
    }

}
