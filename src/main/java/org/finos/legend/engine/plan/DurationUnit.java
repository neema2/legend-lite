package org.finos.legend.engine.plan;

/**
 * Represents duration units for date arithmetic and diff operations.
 * 
 * Used by dateDiff(d1, d2, DurationUnit.DAYS) and adjust(date, 5,
 * DurationUnit.DAYS)
 */
public enum DurationUnit {
    YEARS("year"),
    MONTHS("month"),
    WEEKS("week"),
    DAYS("day"),
    HOURS("hour"),
    MINUTES("minute"),
    SECONDS("second");

    private final String sql;

    DurationUnit(String sql) {
        this.sql = sql;
    }

    /**
     * @return The SQL representation for use in DATE_DIFF/DATE_ADD
     */
    public String sql() {
        return sql;
    }

    /**
     * Parse a duration unit from Pure syntax.
     * Handles both DurationUnit.DAYS and just DAYS.
     */
    public static DurationUnit fromPure(String name) {
        String normalized = name.toUpperCase()
                .replace("DURATIONUNIT.", "")
                .replace("DURATION_UNIT.", "");
        return switch (normalized) {
            case "YEARS", "YEAR" -> YEARS;
            case "MONTHS", "MONTH" -> MONTHS;
            case "WEEKS", "WEEK" -> WEEKS;
            case "DAYS", "DAY" -> DAYS;
            case "HOURS", "HOUR" -> HOURS;
            case "MINUTES", "MINUTE" -> MINUTES;
            case "SECONDS", "SECOND" -> SECONDS;
            default -> throw new IllegalArgumentException("Unknown duration unit: " + name);
        };
    }
}
