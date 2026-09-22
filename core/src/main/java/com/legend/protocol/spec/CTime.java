package com.legend.protocol.spec;

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
public record CTime(@com.legend.base.Nullable PureTimeLiteral value,
        @com.legend.base.Nullable String written,
        @com.legend.base.Nullable com.legend.protocol.SourceInfo pos)
        implements ValueSpecification {
    public CTime {
        if (value == null && written == null) {
            throw new NullPointerException("a CTime needs a value or its written form");
        }
    }

    /** Position-free convenience constructor. */
    public CTime(PureTimeLiteral value) {
        this(value, null, null);
    }

    /** The STRUCTURED value; throws on an out-of-range literal the engine's parser
     *  admits ({@code %200:12:22} — validation is the compiler's job, not the
     *  parser's). */
    public PureTimeLiteral requireValue() {
        if (value == null) {
            throw new IllegalStateException(
                    "time literal '%" + written + "' is out of range");
        }
        return value;
    }

    /** Position is excluded from equality — see {@code ValueSpecEqualityTest}. */
    @Override
    public boolean equals(Object o) {
        return o instanceof CTime other
                && Objects.equals(value, other.value())
                && (value != null || Objects.equals(written, other.written()));
    }

    @Override
    public int hashCode() {
        return Objects.hash(value, value == null ? written : null);
    }
}
