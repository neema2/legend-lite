// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

import com.legend.setup.CsvSeed.Step;

import java.util.List;

/**
 * Runs a connection's setup on a session: the statements and rows the plan side writes
 * ({@link com.legend.setup.CsvSeed}), executed here under the {@code SEED} origin. The text half moved to the plan
 * side on 2026-10-08 (docs/EXECUTION_PLAN_BOUNDARY_2026_10_05.md §9); this is the half that holds a connection.
 */
public final class SetupRunner {

    private SetupRunner() {
    }

    /** Runs a connection's setup steps on the session, as the engine does when it
     *  ESTABLISHES the connection; {@code recorder} is the referee's ledger (null = none). */
    public static void run(List<Step> setups, java.sql.Connection connection,
            com.legend.sql.dialect.SqlDialect dialect,
            com.legend.sql.dialect.RawSqlBoundary.@com.legend.base.Nullable Recorder recorder) {
        for (Step step : setups) {
            switch (step) {
                case Step.Sql blob -> {
                    for (String stmt : com.legend.sql.RawSql.splitStatements(blob.text())) {
                        boolean query;
                        try (var __o = StatementOrigin.enter(StatementOrigin.SEED)) {
                            query = Executor.executeRaw(connection, com.legend.setup.CsvSeed.adaptRaw(stmt, dialect));
                        }
                        if (recorder != null) {
                            recorder.recordExecuted(stmt, query);
                        }
                    }
                }
                // test data's ROWS: the engine's bulk load when it has one; the
                // referee's ledger records the one insert that lands the same rows
                case Step.Rows rows -> {
                    try (var __o = StatementOrigin.enter(StatementOrigin.SEED)) {
                        Executor.load(connection, dialect, rows.load());
                    }
                    if (recorder != null) {
                        recorder.recordExecuted(rows.text(dialect), false);
                    }
                }
            }
        }
    }
}
