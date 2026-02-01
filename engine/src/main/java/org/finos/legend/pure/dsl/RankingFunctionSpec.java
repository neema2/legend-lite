package org.finos.legend.pure.dsl;

import java.util.List;

/**
 * Specification for ranking window functions.
 * 
 * Ranking functions compute a position or distribution value for each row
 * based on ordering within the partition. They don't require an aggregate
 * column.
 * 
 * Examples:
 * - row_number()->over(~department, ~salary->desc())
 * - dense_rank()->over(~department, ~salary->desc())
 * - ntile(4)->over(~department, ~salary->desc())
 */
public record RankingFunctionSpec(
        RankingFunction function,
        List<String> partitionBy,
        List<WindowFunctionSpec.WindowSortSpec> orderBy,
        WindowFunctionSpec.WindowFrameSpec frame,
        Integer ntileBuckets // Only for NTILE
) implements WindowFunctionSpec {

    /**
     * Ranking function types.
     */
    public enum RankingFunction {
        ROW_NUMBER,
        RANK,
        DENSE_RANK,
        PERCENT_RANK,
        CUME_DIST,
        NTILE;

        /**
         * Parses a Pure function name to RankingFunction.
         * Supports both camelCase (Pure) and snake_case (SQL) naming.
         */
        public static RankingFunction fromName(String name) {
            return switch (name.toLowerCase()) {
                case "rownumber", "row_number" -> ROW_NUMBER;
                case "rank" -> RANK;
                case "denserank", "dense_rank" -> DENSE_RANK;
                case "percentrank", "percent_rank" -> PERCENT_RANK;
                case "cumulativedistribution", "cume_dist" -> CUME_DIST;
                case "ntile" -> NTILE;
                default -> null;
            };
        }

        /**
         * Returns the SQL function name.
         */
        public String toSql() {
            return name(); // Enum names match SQL (e.g., ROW_NUMBER, DENSE_RANK)
        }
    }

    /**
     * Creates a simple ranking function spec (no NTILE buckets).
     */
    public static RankingFunctionSpec of(RankingFunction function,
            List<String> partitionBy, List<WindowFunctionSpec.WindowSortSpec> orderBy) {
        return new RankingFunctionSpec(function, partitionBy, orderBy, null, null);
    }

    /**
     * Creates an NTILE function spec with bucket count.
     */
    public static RankingFunctionSpec ntile(int buckets,
            List<String> partitionBy, List<WindowFunctionSpec.WindowSortSpec> orderBy) {
        return new RankingFunctionSpec(RankingFunction.NTILE, partitionBy, orderBy, null, buckets);
    }
}
