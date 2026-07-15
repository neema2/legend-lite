package com.legend.model.spec;

import com.legend.values.PureTimeLiteral;
import java.util.Objects;

/**
 * Pure strict-time literal carrier &mdash; a {@link ValueSpecification}
 * wrapping a structured {@link PureTimeLiteral}. Source-form
 * {@code %10:30}, {@code %10:30:45}, {@code %10:30:45.123} all parse
 * to one of these.
 *
 * <p>Replaces the previous {@code CStrictTime(String value)}. The
 * structured value is validated at construction; downstream consumers
 * pattern-match on {@link PureTimeLiteral} variants instead of
 * re-parsing the string.
 */
public record CTime(PureTimeLiteral value) implements ValueSpecification {
    public CTime {
        Objects.requireNonNull(value, "value");
    }
}
