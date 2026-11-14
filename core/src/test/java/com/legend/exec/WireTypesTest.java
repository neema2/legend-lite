// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

import com.legend.sql.SqlType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** The wire TABLE (JDBC's type codes to the SQL vocabulary): its test stays with it in exec when the probe half left
 *  for the root as {@code WireProbe} (D71, 2026-10-30; {@code WireProbeTest} holds the probe's). */
class WireTypesTest {

    @Test
    void jdbcVocabulary() {
        assertEquals(SqlType.Scalar.VARCHAR, WireTypes.sqlTypeOf(java.sql.Types.VARCHAR, 200, 0));
        assertEquals(new SqlType.Decimal(18, 3), WireTypes.sqlTypeOf(java.sql.Types.DECIMAL, 18, 3));
        assertEquals(SqlType.Scalar.INTEGER, WireTypes.sqlTypeOf(java.sql.Types.SMALLINT, 0, 0));
        assertEquals(null, WireTypes.sqlTypeOf(java.sql.Types.ARRAY, 0, 0));
    }
}
