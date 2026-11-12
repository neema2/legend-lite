// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * H2'S SETTINGS READ-BACK ANSWERS EVERY REQUIRED SETTING THE DATABASE DOES NOT HAVE (the C final audit's J4,
 * 2026-11-12): one it has with another value, and one it does not report at all -- until then the read-back answered
 * only a setting reported with another value, so a required setting the database did not report passed.
 */
class H2SettingsReadBackTest {

    private static List<String> answers(Connection c, String sql) throws Exception {
        List<String> out = new ArrayList<>();
        try (Statement st = c.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            while (rs.next()) {
                out.add(rs.getString(1));
            }
        }
        return out;
    }

    @Test
    void aSettingTheDatabaseDoesNotReportIsAnswered_asOneWithAnotherValueIs() throws Exception {
        SqlDialect.DatabaseSettings settings = H2.differing(List.of(new SqlDialect.Setting("MODE", "LEGACY"),
                new SqlDialect.Setting("DEFAULT_NULL_ORDERING", "HIGH"), new SqlDialect.Setting("NO_SUCH", "X")));
        try (Connection made = DriverManager.getConnection("jdbc:h2:mem:readback_made;MODE=LEGACY;"
                + "DEFAULT_NULL_ORDERING=HIGH", "sa", "");
                Connection bare = DriverManager.getConnection("jdbc:h2:mem:readback_bare", "sa", "")) {
            assertEquals(List.of("NO_SUCH=(not reported)"), answers(made, settings.readBack()));
            assertEquals(List.of("DEFAULT_NULL_ORDERING=LOW", "MODE=REGULAR", "NO_SUCH=(not reported)"),
                    answers(bare, settings.readBack()));
        }
        // the dialect's own: none where the database has both
        try (Connection made = DriverManager.getConnection("jdbc:h2:mem:readback_dialect;MODE=LEGACY;"
                + "DEFAULT_NULL_ORDERING=HIGH", "sa", "")) {
            assertEquals(List.of(), answers(made, new H2().databaseSettings().readBack()));
        }
    }
}
