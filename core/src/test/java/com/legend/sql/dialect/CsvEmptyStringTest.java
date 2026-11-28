// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.testbackend.StorelessRuntime;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The CSV's empty String where the review of stage B's B2b S5b found it lost (2026-11-25, its must-fix; register S420):
 * on MariaDB and MySQL a PAD SPACE collation finds {@code ' ' = ''} true, so lite's {@code toCSV}
 * ({@code lowering.ProductRelationText}) tests whether to quote an empty String by its length, {@code CHAR_LENGTH}
 * there, and never compares the text with {@code ''}. Planned, not run: no lane holds MariaDB or MySQL, so the pin
 * stands where the defect was (the review of S5c asked for it); a dialect's test, as it names the two databases.
 */
class CsvEmptyStringTest {

    @Test
    void theCsvsEmptyStringIsTestedByItsLengthOnMariaDbAndMySql() {
        for (DatabaseType t : List.of(DatabaseType.MariaDB, DatabaseType.MySQL)) {
            // a server's coordinates, declared as they are; planning opens no session
            String model = StorelessRuntime.onServer("", t, "localhost", 3306, "plan");
            String plan = com.legend.executionplan.PlanJson.write(com.legend.Compiler.query(
                    com.legend.Compiler.compileModel(model),
                    "|#TDS\n  s:String\n  a\n#->meta::relational::tests::csv::toCSV()")
                    .executionPlan(StorelessRuntime.RUNTIME));
            assertTrue(plan.contains("CHAR_LENGTH("), t + ": " + plan);
            assertFalse(plan.contains("= ''"), t + ": " + plan);
        }
    }
}
