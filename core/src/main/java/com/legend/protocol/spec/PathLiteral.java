package com.legend.protocol.spec;

import java.util.List;
import java.util.Objects;

/**
 * A navigation-path literal {@code #/Root/prop1/prop2#} — the parse product keeps BOTH
 * representations: the wire pieces (start type, segments with their character offsets
 * inside the literal) for {@code ProtocolEmitter}, and the desugared lambda
 * ({@code {_path: Root | $_path.prop1.prop2}}) that legend-lite's compiler consumes.
 * {@code NameResolver} replaces the node with its {@link #desugared()} lambda on first
 * touch, so nothing downstream of resolution ever sees it.
 *
 * <p>The wire shape (ProbeWireShapes "path offsets"): a {@code classInstance} of type
 * {@code path} whose spans are SHIFTED RIGHT by exactly the literal's length — an engine
 * island-reparse artifact reproduced faithfully: for a literal starting at column {@code s}
 * with length {@code len}, the outer span is {@code [s+len, s+2*len+2]} and a segment at
 * 0-based inclusive character range {@code [a,b]} spans {@code [s+len+a-2, s+len+b-1]}.
 *
 * <p>Positions and offsets are excluded from equality, matching every other spec record.
 */
public record PathLiteral(
        String startType,
        List<Segment> segments,
        LambdaFunction desugared,
        @com.legend.base.Nullable String alias,
        boolean hasDatedSegment,
        @com.legend.base.Nullable com.legend.protocol.SourceInfo pos,
        int literalLength) implements ValueSpecification {

    public PathLiteral {
        Objects.requireNonNull(startType, "startType");
        Objects.requireNonNull(segments, "segments");
        Objects.requireNonNull(desugared, "desugared");
        segments = List.copyOf(segments);
    }

    /** One property segment: the name plus its 0-based inclusive char range inside the
     *  literal text. A DATED segment ({@code prop(%latest)}, {@code prop(%2017-6-10)},
     *  {@code prop(Enum.VALUE)}) records each argument as a typed {@link PathArg}; an
     *  argument outside that set marks the segment unsupported and the emitter walls. */
    public record Segment(String name, int innerStart, int innerEnd,
                          List<PathArg> args, boolean unsupportedArg) {
        public Segment {
            args = List.copyOf(args);
        }

        public Segment(String name, int innerStart, int innerEnd) {
            this(name, innerStart, innerEnd, List.of(), false);
        }
    }

    /**
     * One dated-segment argument. Latest and DateArg carry their 0-based inclusive char
     * range inside the literal text (both emit under the {@code a-1} shift rule — the span
     * starts one char BEFORE the argument); an enum argument is span-less on the wire
     * (ProbeWireShapes "path dated and enum args").
     */
    public sealed interface PathArg {
        /** {@code %latest} — a {@code latestDate} node. */
        record Latest(int start, int end) implements PathArg {
        }

        /** {@code %date} — a {@code dateTime} node; {@code value} is the date text
         *  WITHOUT the {@code %}, emitted verbatim; the range starts at the {@code %}. */
        record DateArg(String value, int start, int end) implements PathArg {
        }

        /** {@code Enum.VALUE} — an {@code enumValue} node with NO sourceInformation. */
        record EnumArg(String fullPath, String value) implements PathArg {
        }

        /** An integer literal argument — same {@code a-1} shift as dates. */
        record IntArg(long value, int start, int end) implements PathArg {
        }

        /** A string literal argument; the range includes the quotes. */
        record StrArg(String value, int start, int end) implements PathArg {
        }

        /** {@code [a, b]} — a span-less collection of scalar arguments. */
        record CollectionArg(java.util.List<PathArg> elements) implements PathArg {
            public CollectionArg {
                elements = java.util.List.copyOf(elements);
            }
        }
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof PathLiteral other
                && startType.equals(other.startType())
                && segments.equals(other.segments())
                && desugared.equals(other.desugared())
                && Objects.equals(alias, other.alias())
                && hasDatedSegment == other.hasDatedSegment();
    }

    @Override
    public int hashCode() {
        return Objects.hash(startType, segments, desugared, alias, hasDatedSegment);
    }
}
