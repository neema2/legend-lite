package org.finos.legend.pure.dsl;

import java.util.List;

/**
 * Specification for value window functions.
 * 
 * Value functions access values from other rows in the window,
 * such as previous/next rows (LAG/LEAD) or first/last rows.
 * 
 * Examples:
 * - lag(~salary, 1)->over(~department, ~date->asc())
 * - first(~salary)->over(~department, ~salary->desc(), rows(unbounded(), 0))
 */
public record ValueFunctionSpec(
        ValueFunction function,
        String column, // Column to get value from
        Integer offset, // For LAG/LEAD (default 1)
        List<String> partitionBy,
        List<WindowFunctionSpec.WindowSortSpec> orderBy,
        WindowFunctionSpec.WindowFrameSpec frame) implements WindowFunctionSpec {

    /**
     * Value function types.
     */
    public enum ValueFunction {
        LAG,
        LEAD,
        FIRST_VALUE,
        LAST_VALUE,
        NTH_VALUE;

        /**
         * Parses a Pure function name to ValueFunction.
         */
        public static ValueFunction fromName(String name) {
            return switch (name.toLowerCase()) {
                case "lag" -> LAG;
                case "lead" -> LEAD;
                case "first", "first_value" -> FIRST_VALUE;
                case "last", "last_value" -> LAST_VALUE;
                case "nth", "nth_value" -> NTH_VALUE;
                default -> null;
            };
        }

        /**
         * Returns the SQL function name.
         */
        public String toSql() {
            return name();
        }
    }

    /**
     * Creates a LAG/LEAD function spec with offset.
     */
    public static ValueFunctionSpec lagLead(ValueFunction function, String column, int offset,
            List<String> partitionBy, List<WindowFunctionSpec.WindowSortSpec> orderBy) {
        return new ValueFunctionSpec(function, column, offset, partitionBy, orderBy, null);
    }

    /**
     * Creates a FIRST_VALUE/LAST_VALUE function spec.
     */
    public static ValueFunctionSpec firstLast(ValueFunction function, String column,
            List<String> partitionBy, List<WindowFunctionSpec.WindowSortSpec> orderBy,
            WindowFunctionSpec.WindowFrameSpec frame) {
        return new ValueFunctionSpec(function, column, null, partitionBy, orderBy, frame);
    }
}
