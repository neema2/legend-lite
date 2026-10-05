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

/** E1: the LIMIT-0 metadata probe — a staticized plan's CONCRETE
 * output columns (pivot-generated names included). A SCHEMA read at
 * the execution seam (DynamicPivot's own two-phase discipline), never
 * value sniffing. */
public final class PctProbe {

    private PctProbe() {
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
                String tn = md.getColumnTypeName(i) == null ? ""
                        : md.getColumnTypeName(i).toUpperCase(java.util.Locale.ROOT);
                SqlType slot = tn.equals("DATE") ? SqlType.Scalar.DATE
                        : tn.startsWith("TIMESTAMP")
                                ? SqlType.Scalar.TIMESTAMP
                                : SqlType.Scalar.VARCHAR;
                outs.add(new OutputCol(md.getColumnName(i), slot, true));
                typeNames.put(md.getColumnName(i), tn);
            }
            return new PlanProbe(outs, typeNames);
        } catch (java.sql.SQLException e) {
            // the seam: java.sql stops at this probe's boundary
            throw new com.legend.error.DataError(
                    String.valueOf(e.getMessage()), e);
        }
    }
}
