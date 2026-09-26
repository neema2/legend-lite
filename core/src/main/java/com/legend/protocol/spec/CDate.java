package com.legend.protocol.spec;

import com.legend.values.PureDateLiteral;
import java.util.Objects;

/**
 * Pure date literal carrier &mdash; a {@link ValueSpecification}
 * wrapping a structured {@link PureDateLiteral}. Source-form
 * {@code %2024}, {@code %2024-01-15}, {@code %2024-01-15T10:30:45.123},
 * {@code %-44-03-15}, etc. all parse to one of these.
 *
 * <p>Replaces the previous trio of {@code CDateTime}, {@code CStrictDate},
 * and the year-only / year-month forms that previously masqueraded as
 * {@code CStrictDate}. The Pure {@code Class} type of a literal
 * ({@code StrictDate}, {@code DateTime}, etc.) is derived from the
 * variant of {@link PureDateLiteral} at type-check time; the parser
 * commits only to structural shape.
 *
 * <p>See {@link PureDateLiteral} for the variant hierarchy, parse
 * semantics (including timezone normalisation to GMT), and validation
 * rules.
 *
 * <p>Note: the {@code %latest} sentinel is a distinct token and
 * remains carried by {@link CLatestDate}.
 */
public record CDate(PureDateLiteral value, @com.legend.base.Nullable String written,
        @com.legend.base.Nullable com.legend.protocol.SourceInfo pos)
        implements ValueSpecification {
    public CDate {
        Objects.requireNonNull(value, "value");
    }

    /** Position-free convenience constructor. */
    public CDate(PureDateLiteral value) {
        this(value, null, null);
    }

    /** Span-only form for the let-rule override. */
    public CDate(PureDateLiteral value, @com.legend.base.Nullable com.legend.protocol.SourceInfo pos) {
        this(value, null, pos);
    }

    /** Position is excluded from equality — see {@code ValueSpecEqualityTest}. */
    @Override
    public boolean equals(Object o) {
        return o instanceof CDate other && value.equals(other.value());
    }

    @Override
    public int hashCode() {
        return value.hashCode();
    }
}
