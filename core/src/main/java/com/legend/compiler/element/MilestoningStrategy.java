package com.legend.compiler.element;

/**
 * A temporal class's milestoning strategy (engine {@code temporal.*}
 * stereotypes) &mdash; the TYPED form of what was a nullable raw string
 * compared at 56 literal sites (remediation T2.3). {@code null} still means
 * "not temporal" (callers' presence checks are unchanged); every VALUE
 * comparison is now an enum dispatch, so a new strategy breaks every
 * switch at compile time instead of silently leaving an extent unfiltered.
 */
public enum MilestoningStrategy {
    BUSINESS, PROCESSING, BITEMPORAL;

    /** One axis of a (possibly bitemporal) strategy. */
    public enum Dimension { BUSINESS, PROCESSING }

    /**
     * The stereotype&rarr;strategy funnel &mdash; the ONLY place the surface
     * spellings live. Null for non-temporal profiles and for unrecognized
     * {@code temporal.*} stereotype names (matching the historical
     * set-membership tolerance).
     */
    public static @com.legend.base.Nullable MilestoningStrategy ofStereotypeOrNull(
            String profileName,
            String stereotypeName) {
        if (!com.legend.compiler.element.type.PlatformTypes.isProfile(profileName,
                com.legend.compiler.element.type.PlatformTypes.TEMPORAL_PROFILE)) {
            return null;
        }
        return switch (stereotypeName) {
            case "businesstemporal" -> BUSINESS;
            case "processingtemporal" -> PROCESSING;
            case "bitemporal" -> BITEMPORAL;
            default -> null;
        };
    }

    /** Whether this strategy carries the given temporal axis. */
    public boolean has(Dimension d) {
        return switch (this) {
            case BITEMPORAL -> true;
            case BUSINESS -> d == Dimension.BUSINESS;
            case PROCESSING -> d == Dimension.PROCESSING;
        };
    }

    /** The single-axis strategy of one dimension (a bitemporal class's halves). */
    public static MilestoningStrategy of(Dimension d) {
        return d == Dimension.BUSINESS ? BUSINESS : PROCESSING;
    }
}
