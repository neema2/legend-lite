package conformance;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

/**
 * The time family: LocalDateTime's text both ways and its arithmetic, and a time moved between zones -- which holds
 * the module's timezone database (a resource, not code) to the JDK's, rule by rule, across daylight-saving changes.
 */
final class Time {

    private Time() {
    }

    static final String[] ZONES = {"UTC", "Z", "GMT", "Europe/London", "Europe/Dublin", "Europe/Paris",
        "Europe/Moscow", "America/New_York", "America/Los_Angeles", "America/St_Johns", "America/Sao_Paulo",
        "America/Mexico_City", "Asia/Kolkata", "Asia/Kathmandu", "Asia/Tehran", "Asia/Tokyo", "Australia/Sydney",
        "Australia/Lord_Howe", "Pacific/Chatham", "Pacific/Apia", "Africa/Casablanca", "Antarctica/Troll",
        "Etc/GMT+5", "+05:30", "-08:00", "UTC+01:00"};

    static String parseAndPrint() {
        Out out = new Out();
        String[] texts = {"2026-01-15T12:00", "2026-01-15T12:00:00", "2026-01-15T12:00:00.1", "2026-01-15T12:00:00.12",
            "2026-01-15T12:00:00.123", "2026-01-15T12:00:00.1234", "2026-01-15T12:00:00.123456789",
            "2026-01-15T12:00:00.1234567891", "2026-02-29T00:00", "2024-02-29T00:00", "2026-02-30T00:00",
            "2026-13-01T00:00", "+12026-01-01T00:00", "12026-01-01T00:00", "-0001-01-01T00:00", "0000-01-01T00:00",
            "2026-1-5T1:2", "2026-01-15 12:00", "2026-01-15T24:00", "2026-01-15T23:59:60", "2026-01-15T12:00Z",
            "2026-01-15", " 2026-01-15T12:00", "٢٠٢٦-01-15T12:00"};
        for (String s : texts) {
            try {
                out.add("parse " + s, LocalDateTime.parse(s).toString());
            } catch (RuntimeException e) {
                out.add("parse " + s, Out.err(e));
            }
        }
        Rng r = new Rng(11);
        int[] nanos = {0, 1, 1_000, 1_000_000, 100_000_000, 120_000_000, 123_000_000, 123_400_000, 123_456_000,
            123_456_789, 999_999_999};
        for (int i = 0; i < 3_000; i++) {
            int year = i % 50 == 0 ? r.below(40_000) - 20_000 : 1 + r.below(9998);
            LocalDateTime t = LocalDateTime.of(year, 1 + r.below(12), 1 + r.below(28), r.below(24), r.below(60),
                    r.below(60), nanos[r.below(nanos.length)]);
            long minutes = (r.next() >> r.below(64)) % 100_000_000L;
            String moved;
            try {
                moved = t.minusMinutes(minutes).toString();
            } catch (RuntimeException e) {
                moved = Out.err(e);
            }
            out.add(t.getYear() + " " + t.getMonthValue() + " " + t.getDayOfMonth() + " " + t.getHour() + " "
                    + t.getMinute() + " " + t.getSecond() + " " + t.getNano() + " -" + minutes, t + " " + moved);
        }
        return out.toString();
    }

    /** Each zone at times around its daylight-saving changes and across two centuries, both ways. */
    static String zones() {
        Out out = new Out();
        for (String z : ZONES) {
            ZoneId zone;
            try {
                zone = ZoneId.of(z);
                out.add("zone " + z, zone.toString());
            } catch (RuntimeException e) {
                // a zone one side lacks: its cases answer the refusal, so no other line moves
                out.add("zone " + z, Out.err(e));
                zone = ZoneOffset.UTC;
            }
            for (int year : new int[] {1900, 1920, 1945, 1950, 1970, 1980, 1990, 2000, 2005, 2010, 2011, 2014,
                2016, 2019, 2022, 2023, 2024, 2025, 2026, 2027, 2030, 2037, 2038, 2050, 2100}) {
                for (int month = 1; month <= 12; month++) {
                    convert(out, z, zone, LocalDateTime.of(year, month, 15, 12, 0));
                }
            }
            for (int year = 2022; year <= 2028; year++) {
                for (int month : new int[] {3, 4, 9, 10, 11, 12}) {
                    for (int day = 1; day <= 31; day++) {
                        if (day > 30 && (month == 4 || month == 9 || month == 11)) {
                            continue;
                        }
                        convert(out, z, zone, LocalDateTime.of(year, month, day, 1, 30));
                        convert(out, z, zone, LocalDateTime.of(year, month, day, 2, 30));
                    }
                }
            }
        }
        for (String z : new String[] {"EST", "Not/AZone", "Europe/Kiev", "Asia/Calcutta", "US/Eastern", "+25:00", ""}) {
            try {
                out.add("zone " + z, ZoneId.of(z).toString());
            } catch (RuntimeException e) {
                out.add("zone " + z, Out.err(e));
            }
        }
        return out.toString();
    }

    private static void convert(Out out, String name, ZoneId zone, LocalDateTime t) {
        try {
            ZonedDateTime fromUtc = t.atZone(ZoneOffset.UTC).withZoneSameInstant(zone);
            ZonedDateTime local = t.atZone(zone);
            out.add(name + " " + t, fromUtc + " " + local + " " + local.withZoneSameInstant(ZoneOffset.UTC)
                    .toLocalDateTime());
        } catch (RuntimeException e) {
            out.add(name + " " + t, Out.err(e));
        }
    }
}
