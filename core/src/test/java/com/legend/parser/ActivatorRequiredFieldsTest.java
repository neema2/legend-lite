// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.parser;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The field each function activator's walker requires before its function (legend-engine 4.145.0's
 * {@code validateAndExtractRequiredField}): SnowflakeApp's {@code applicationName}, SnowflakeM2MUdf's {@code udfName},
 * MemSqlFunction's and BigQueryFunction's {@code functionName}, HostedService's {@code pattern}. Four of them were
 * unchecked until gate 8's mutants of a documented SnowflakeApp found the first (Studio's grammar roundtrip,
 * 2026-11-14); the refusal is anchored at the element, its documentation included.
 */
class ActivatorRequiredFieldsTest {

    @Test
    @DisplayName("an activator without its name field is refused at the element, naming the field")
    void theNameFieldIsRequired() {
        String common = "   function : a::f():Integer[1];\n   ownership : Deployment { identifier: '1' };\n}\n";
        // section, kind, the name field, the kind's other required keys
        String[][] activators = {
                {"Snowflake", "SnowflakeApp", "   applicationName : 'x';\n", ""},
                {"Snowflake", "SnowflakeM2MUdf", "   udfName : 'x';\n",
                        "   deploymentSchema : 'x';\n   deploymentStage : 'x';\n"},
                {"MemSql", "MemSqlFunction", "   functionName : 'x';\n", ""},
                {"BigQuery", "BigQueryFunction", "   functionName : 'x';\n", ""},
                {"HostedService", "HostedService", "   pattern : '/x';\n", ""},
        };
        for (String[] a : activators) {
            String head = "###" + a[0] + "\n'''\nDocumented.\n'''\n" + a[1] + " a::A\n{\n";
            PmcdParser.parseModel(head + a[2] + a[3] + common);
            ParseException e = assertThrows(ParseException.class,
                    () -> PmcdParser.parseModel(head + a[3] + common), a[1]);
            String field = a[2].substring(3, a[2].indexOf(' ', 3));
            assertEquals("[2:1] Field '" + field + "' is required", e.getMessage(), a[1]);
        }
    }
}
