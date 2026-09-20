package planner;

/**
 * The JVM half of the TIMEZONE differential.
 *
 * <p>{@code LiteralSpelling.inZone} is the planner's only
 * {@code ZoneId.of}, and a timezone database is a RESOURCE rather than
 * code — so whether it survives the WASM build is a question no amount
 * of reading the source can answer. {@code zoneprobe.mjs} asks the
 * built module the same eight questions this asks the JVM.
 */
public final class ZoneMain {

    private ZoneMain() {
    }

    static final String[][] CASES = {
        {"2026-01-15T12:00:00", "UTC"},
        {"2026-01-15T12:00:00", "America/New_York"},    // EST, -5
        {"2026-07-15T12:00:00", "America/New_York"},    // EDT, -4 — a DST rule
        {"2026-01-15T12:00:00", "Asia/Tokyo"},          // +9, no DST
        {"2026-01-15T12:00:00", "Asia/Kolkata"},        // +5:30, half-hour offset
        {"2026-01-15T12:00:00", "Australia/Lord_Howe"}, // +11, half-hour DST
        {"2026-01-15T12:00:00.123", "Europe/London"},   // sub-second shape kept
        {"2026-01-15T12:00:00", "Not/AZone"},           // must refuse, identically
    };

    public static void main(String[] args) {
        for (String[] c : CASES) {
            System.out.println(c[0] + "\t" + c[1] + "\t" + Wasm.zoneProbe(c[0], c[1]));
        }
    }
}
