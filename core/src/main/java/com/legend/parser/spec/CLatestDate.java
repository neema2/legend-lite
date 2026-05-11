package com.legend.parser.spec;

/**
 * LatestDate marker &mdash; the Pure sentinel {@code %latest}, representing
 * "the latest available milestoning date" in a milestoning context.
 *
 * <p>Carries no value because the sentinel is fully determined by its
 * syntax; semantic interpretation (which milestone, in which store)
 * happens at compile / resolve time. Mirrors the engine record exactly.
 */
public record CLatestDate() implements ValueSpecification {
}
