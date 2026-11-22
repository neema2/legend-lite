// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testexec;

import com.legend.testexecutionplan.RootLoop;


import com.legend.testexecutionplan.TypedSide;
import com.legend.testexecutionplan.Verdicts;
import java.util.List;

final class DatabaseVerdicts {

    private DatabaseVerdicts() {
    }


    /**
     * The verdict of one TYPED row (item 3's S4, 2026-11-21, the user's Q-A): {@code [verdict, expected, actual]}, the
     * verdict the database's over the typed values. Where it holds, nothing; where it fails, the failure is the record
     * {@link AssertRecords#ofTypedRow} chooses from the values the database returned -- the one choice the host judge
     * makes over the values it read (the user, 2026-11-22: one translation from a failure to its words).
     */
    static TypedSide typedVerdictOf(String name, List<Object> row, RootLoop.Report report) {
        com.legend.diagnostics.VerdictCensus.sqlJudgedInDatabase(name);
        return Boolean.TRUE.equals(row.get(0)) ? Verdicts.ok() : Verdicts.fail(AssertRecords.ofTypedRow(report, row));
    }

    /** The database judge's typed assertError row ({@code VerdictSql.assertError}: its verdict, then what f raised --
     *  message, line, column): the database's verdict; where it fails, the record {@link AssertRecords#errorFailure}
     *  names over the values the row carries (one choice of the words, the host's). */
    static void errorOfRow(com.legend.testexecutionplan.HostJudgment.AssertError a, List<Object> row) {
        if (Boolean.TRUE.equals(row.get(0))) {
            return;
        }
        com.legend.error.ErrorRecord failure = AssertRecords.errorFailure(a.expected(), a.line(), a.column(),
                (String) row.get(1), row.get(2) == null ? null : ((Number) row.get(2)).longValue(),
                row.get(3) == null ? null : ((Number) row.get(3)).longValue());
        throw new com.legend.error.AssertFailed(java.util.Objects.requireNonNull(failure,
                "the database's assertError verdict failed over values that hold"));
    }
}
