// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.driver;

import com.legend.exec.PlanRunner;
import com.legend.exec.PlanSessions;
import com.legend.exec.Statements;

import com.legend.executionplan.ExecutionPlan;
import com.legend.sql.OutputCol;
import com.legend.sql.PlanProbe;
import com.legend.sql.SqlQuery;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlSource;
import com.legend.sql.SqlType;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** The LIMIT-0 metadata probe (E1; named for what it does, not for the harness, 2026-10-17): a plan's CONCRETE columns asked of the database -- — a staticized plan's CONCRETE
 * output columns (pivot-generated names included). A SCHEMA read at
 * the execution seam (DynamicPivot's own two-phase discipline), never
 * value sniffing. */
public final class ColumnProbe {

    private ColumnProbe() {
    }

    /** The probe as a plan statement of {@code target}, run by the plan runner on a session of {@code sessions}
     *  ({@link PlanRunner#columns}: the doors, 2026-10-29; track B's B1a put it on the session seam, and until then
     *  this class ran the probe on a JDBC connection itself). */
    public static PlanProbe probe(SqlQuery plan, com.legend.sql.dialect.SqlDialect dialect,
            PlanSessions.Source sessions, ExecutionPlan.Target target) {
        SqlSelect probe = SqlSelect.starOf(new SqlSource.Subselect(plan,
                "_pct_probe", null)).withLimit(0L);
        List<Statements.Reported> reported = PlanRunner.columns(ExecutionPlan.Sql.of(dialect.render(probe), target),
                sessions);
        List<OutputCol> outs = new ArrayList<>(reported.size());
        Map<String, String> typeNames = new LinkedHashMap<>();
        for (Statements.Reported r : reported) {
            // the driver's type name read by the dialect (the dialect program's Phase 4)
            String tn = dialect.wireTypeName(r.driverType());
            SqlType slot = tn.equals("DATE") ? SqlType.Scalar.DATE
                    : tn.startsWith("TIMESTAMP")
                            ? SqlType.Scalar.TIMESTAMP
                            : SqlType.Scalar.VARCHAR;
            outs.add(new OutputCol(r.name(), slot, true));
            // typed by its dialect's typing reading; a name that does not say the type (Snowflake's NUMBER)
            // types nothing, and the column is absent (PlanProbe.typeName refuses it)
            String typing = dialect.typingWireName(r.driverType());
            if (typing != null) {
                typeNames.put(r.name(), typing);
            }
        }
        return new PlanProbe(outs, typeNames);
    }
}
