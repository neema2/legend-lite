// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

import com.legend.sql.OutputCol;
import com.legend.sql.PlanProbe;
import com.legend.sql.SqlQuery;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlSource;
import com.legend.sql.SqlType;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
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

    public static PlanProbe probe(SqlQuery plan,
            com.legend.sql.dialect.SqlDialect dialect, Connection conn) {
        SqlSelect probe = SqlSelect.starOf(new SqlSource.Subselect(plan,
                "_pct_probe", null)).withLimit(0L);
        try (var st = conn.prepareStatement(dialect.render(probe));
                ResultSet rs = st.executeQuery()) {
            var md = rs.getMetaData();
            List<OutputCol> outs = new ArrayList<>(md.getColumnCount());
            Map<String, String> typeNames = new LinkedHashMap<>();
            for (int i = 1; i <= md.getColumnCount(); i++) {
                // the driver's type name read by the dialect (the dialect program's Phase 4)
                String driverType = md.getColumnTypeName(i);
                String tn = dialect.wireTypeName(driverType);
                SqlType slot = tn.equals("DATE") ? SqlType.Scalar.DATE
                        : tn.startsWith("TIMESTAMP")
                                ? SqlType.Scalar.TIMESTAMP
                                : SqlType.Scalar.VARCHAR;
                outs.add(new OutputCol(md.getColumnName(i), slot, true));
                // typed by its dialect's typing reading; a name that does not say the type (Snowflake's NUMBER)
                // types nothing, and the column is absent (PlanProbe.typeName refuses it)
                String typing = dialect.typingWireName(driverType);
                if (typing != null) {
                    typeNames.put(md.getColumnName(i), typing);
                }
            }
            return new PlanProbe(outs, typeNames);
        } catch (java.sql.SQLException e) {
            // the seam: java.sql stops at this probe's boundary
            throw new com.legend.error.DataError(
                    String.valueOf(e.getMessage()), e);
        }
    }
}
