// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.planner;

import com.legend.executionplan.ExecutionPlan;

import java.util.List;

/**
 * A PLAN'S TARGET AS ITS DIALECT WRITES IT (C2, 2026-11-12, D4: the charter's A3(c)): the database, the statements every
 * connection to it runs first ({@code SqlDialect.sessionSetup}), its loads, and the two statements exec sends after a
 * failure to read back a raise the database keeps in the session ({@code SqlDialect.raiseReadBack}: MariaDB's and
 * MySQL's), and the settings its database must have ({@code SqlDialect.databaseSettings}: H2's) -- every fact the dialect
 * decides, written into the plan, so exec sends nothing the plan does not carry.
 */
public final class Targets {

    private Targets() {
    }

    /** {@code database}'s target as {@code dialect} writes it, establishing {@code loads}. */
    public static ExecutionPlan.Target of(ExecutionPlan.Database database, com.legend.sql.dialect.SqlDialect dialect,
            List<ExecutionPlan.Load> loads) {
        return new ExecutionPlan.Target(database, dialect.sessionSetup(), loads, readBack(dialect), settings(dialect));
    }

    /** {@code database}'s target as {@code dialect} writes it, with no load. */
    public static ExecutionPlan.Target of(ExecutionPlan.Database database, com.legend.sql.dialect.SqlDialect dialect) {
        return of(database, dialect, List.of());
    }

    /** The settings {@code dialect} declares its database must have, as plan data; null where it needs none. */
    static ExecutionPlan.@com.legend.base.Nullable DatabaseSettings settings(com.legend.sql.dialect.SqlDialect dialect) {
        com.legend.sql.dialect.SqlDialect.DatabaseSettings declared = dialect.databaseSettings();
        return declared == null ? null : new ExecutionPlan.DatabaseSettings(declared.required().stream()
                .map(s -> new ExecutionPlan.Setting(s.name(), s.value())).toList(), declared.readBack());
    }

    /** The read-back {@code dialect} declares, as plan data; null where its database's errors carry a raise. */
    static ExecutionPlan.@com.legend.base.Nullable RaiseReadBack readBack(com.legend.sql.dialect.SqlDialect dialect) {
        com.legend.sql.dialect.SqlDialect.RaiseReadBack declared = dialect.raiseReadBack();
        return declared == null ? null : new ExecutionPlan.RaiseReadBack(declared.read(), declared.reset());
    }
}
