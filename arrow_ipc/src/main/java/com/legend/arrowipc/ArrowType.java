// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.arrowipc;

import com.legend.base.Nullable;

/**
 * An Arrow data type as the IPC schema spells it (Arrow's {@code Schema.fbs}, the {@code Type} union): pure data,
 * one record per variant this framer writes; {@link ArrowIpc} writes each as its FlatBuffers table. No Arrow library:
 * the result boundary program's R9 (docs/RESULT_BOUNDARY_PROGRAM_2026_10_26.md), the warehouse's own framer shared
 * with core's writers (track A, 2026-10-29).
 */
public sealed interface ArrowType {

    /** The unit of a time, a timestamp or a duration. */
    enum TimeUnit { SECOND, MILLISECOND, MICROSECOND, NANOSECOND }

    /** A floating point's width. */
    enum FloatPrecision { HALF, SINGLE, DOUBLE }

    /** A date's unit: days since the epoch (32 bits), or milliseconds (64 bits). */
    enum DateUnit { DAY, MILLISECOND }

    /** An interval's layout. */
    enum IntervalUnit { YEAR_MONTH, DAY_TIME, MONTH_DAY_NANO }

    /** Null: no value at all. */
    record Null() implements ArrowType { }

    /** A bit per value. */
    record Bool() implements ArrowType { }

    /** A signed or unsigned integer of {@code bits} (8, 16, 32, 64). */
    record Int(int bits, boolean signed) implements ArrowType { }

    record FloatingPoint(FloatPrecision precision) implements ArrowType { }

    /** A fixed-point decimal of {@code precision} digits and {@code scale}, {@code bits} wide (128 or 256). */
    record Decimal(int precision, int scale, int bits) implements ArrowType { }

    /** UTF-8 text: 32-bit offsets and bytes. */
    record Utf8() implements ArrowType { }

    /** Bytes: 32-bit offsets and bytes. */
    record Binary() implements ArrowType { }

    record Date(DateUnit unit) implements ArrowType { }

    /** A time of day in {@code unit}, {@code bits} wide (32 for seconds and milliseconds, 64 for finer). */
    record Time(TimeUnit unit, int bits) implements ArrowType { }

    /** An instant in {@code unit} since the epoch; {@code timezone} names the zone the instant is read in, null for a
     *  zone-less local time. */
    record Timestamp(TimeUnit unit, @Nullable String timezone) implements ArrowType { }

    record Interval(IntervalUnit unit) implements ArrowType { }

    /** A variable-length list: 32-bit offsets and one child, the element. */
    record List() implements ArrowType { }

    /** A list of exactly {@code size} elements per value: one child, no offsets. */
    record FixedSizeList(int size) implements ArrowType { }

    /** One child per field. */
    record Struct() implements ArrowType { }

    /** Entries: 32-bit offsets and one child, a non-nullable struct of {@code key} and {@code value}. */
    record Map(boolean keysSorted) implements ArrowType { }

    /** A union of its children: a type id per value naming the child (and, when dense, an offset into it);
     *  {@code typeIds} the ids in the children's order, or null for 0, 1, 2, ... */
    record Union(boolean dense, int @Nullable [] typeIds) implements ArrowType { }
}
