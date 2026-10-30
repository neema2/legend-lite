// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.driver;

import com.legend.compiler.element.type.Type;
import com.legend.exec.PlanRunner;
import com.legend.exec.PlanSessions;
import com.legend.exec.Statements;
import com.legend.exec.WireTypes;
import com.legend.executionplan.ExecutionPlan;
import com.legend.sql.OutputCol;
import com.legend.sql.SqlQuery;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * THE WIRE PROBE (the probe half of {@code exec.WireTypes} until D71, 2026-10-30, the seam-and-wall program's B1):
 * a plan the compiler could not type given the database's columns ({@link #staticized}) -- a prepare through the plan
 * runner, no execution ({@link PlanRunner#describe}). Planning work: it renders through the dialect and reads the
 * compiler's types, so it lives with the driver, not in the runtime; the wire TABLE (JDBC's type codes to the SQL
 * vocabulary, {@link WireTypes}) stays in {@code exec}. Its reconcile (a wire-decided bare store column re-typed to the
 * kind the database reported) was deleted by the planner-driver seam program's S3b (2026-10-30): the canon asks the
 * value in the statement ({@code CanonicalRenderSql.textOrStamp}).
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
