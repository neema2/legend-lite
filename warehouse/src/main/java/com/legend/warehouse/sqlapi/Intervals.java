package com.legend.warehouse.sqlapi;

/**
 * An INTERVAL's text as DuckDB spells it ({@code 1 year 2 months 3 days 04:05:06.5}): the JSON API's
 * value for an interval, and what DuckDB's JDBC driver returns for one. DuckDB's own rule
 * (IntervalToStringCast), pinned against DuckDB's cast over many intervals by
 * {@code WarehouseJdbcTest.intervalsSpellAsDuckDBCastsThem}.
 */
public final class Intervals {

    private Intervals() {
    }

    public static String text(int months, int days, long micros) {
        StringBuilder sb = new StringBuilder();
        if (months != 0) {
            int years = months / 12;
            int rest = months - years * 12;
            if (years != 0) part(sb, years, "year");
            if (rest != 0) part(sb, rest, "month");
        }
        if (days != 0) part(sb, days, "day");
        if (micros != 0) {
            if (!sb.isEmpty()) sb.append(' ');
            java.math.BigInteger m = java.math.BigInteger.valueOf(micros);
            if (micros < 0) {
                sb.append('-');
                m = m.negate();   // Long.MIN_VALUE has no negative long
            }
            long all = m.longValue();
            boolean min = micros == Long.MIN_VALUE;
            long hours = min ? m.divide(java.math.BigInteger.valueOf(3_600_000_000L)).longValue() : all / 3_600_000_000L;
            long rest = min ? m.mod(java.math.BigInteger.valueOf(3_600_000_000L)).longValue() : all % 3_600_000_000L;
            long minutes = rest / 60_000_000L;
            rest %= 60_000_000L;
            long seconds = rest / 1_000_000L;
            long fraction = rest % 1_000_000L;
            sb.append(hours < 10 ? "0" + hours : Long.toString(hours)).append(':')
                    .append(two(minutes)).append(':').append(two(seconds));
            if (fraction != 0) {
                String f = String.format("%06d", fraction);
                int end = f.length();
                while (f.charAt(end - 1) == '0') end--;
                sb.append('.').append(f, 0, end);
            }
        } else if (sb.isEmpty()) {
            sb.append("00:00:00");
        }
        return sb.toString();
    }

    private static void part(StringBuilder sb, long n, String unit) {
        if (!sb.isEmpty()) sb.append(' ');
        sb.append(n).append(' ').append(unit);
        if (n != 1 && n != -1) sb.append('s');
    }

    private static String two(long n) {
        return n < 10 ? "0" + n : Long.toString(n);
    }
}
