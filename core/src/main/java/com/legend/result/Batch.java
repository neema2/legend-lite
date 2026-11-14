// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.result;

import com.legend.executionplan.ExecutionPlan;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Rows of a typed result, held column by column (docs/RESULT_BOUNDARY_PROGRAM_2026_10_26.md R8 and §8 item 4, ruled
 * 2026-10-26): one typed array per plan column with a null mask, behind ONE accessor, so no writer knows the
 * representation. A column of Pure type {@code Integer} is held as {@code long}s, {@code Float} as {@code double}s,
 * {@code Boolean} as {@code boolean}s, every other kind as the cell the session decoded (an exact {@code BigDecimal},
 * a date literal, a text, a structure's map or list); an Integer past 63 bits is held exactly beside the {@code long}s.
 * Each cell is the session's carrier ({@code exec.Statements#rows}), carried as it is: nothing here converts a value.
 * A tagged column's cell arrives as a {@link TaggedCell}, the slot index beside the slot's carrier (track F,
 * 2026-10-29): {@link #get} is the carrier, {@link #tag} the slot.
 *
 * <p>A batch is immutable once built ({@link Builder#build}).
 */
public final class Batch {

    private final List<ExecutionPlan.Column> columns;
    private final Vector[] vectors;
    private final int size;

    private Batch(List<ExecutionPlan.Column> columns, Vector[] vectors, int size) {
        this.columns = columns;
        this.vectors = vectors;
        this.size = size;
    }

    /** The columns, with their three type facts. */
    public List<ExecutionPlan.Column> columns() {
        return columns;
    }

    /** How many rows. */
    public int size() {
        return size;
    }

    /** How many columns. */
    public int width() {
        return vectors.length;
    }

    /** Whether row {@code row}'s cell of column {@code column} is null. */
    public boolean isNull(int row, int column) {
        check(row);
        return vectors[column].isNull(row);
    }

    /** Row {@code row}'s cell of column {@code column}: the session's carrier, or null. */
    public @com.legend.base.Nullable Object get(int row, int column) {
        check(row);
        return vectors[column].get(row);
    }

    /** Row {@code row}'s tag of tagged column {@code column}: the slot index its cell is in ({@link TaggedCell}), or
     *  -1 for a null cell; refused by name on a column that carries no tag. */
    public int tag(int row, int column) {
        check(row);
        return vectors[column].tag(row, columns.get(column));
    }

    /** Row {@code row}'s cells, in column order (a collector's and a test's convenience). */
    public List<Object> row(int row) {
        check(row);
        List<Object> out = new ArrayList<>(vectors.length);
        for (Vector v : vectors) {
            out.add(v.get(row));
        }
        return Collections.unmodifiableList(out);
    }

    private void check(int row) {
        if (row < 0 || row >= size) {
            throw new IndexOutOfBoundsException("row " + row + " of a batch of " + size);
        }
    }

    /** A batch of {@code columns} being filled, up to {@code capacity} rows. */
    public static Builder builder(List<ExecutionPlan.Column> columns, int capacity) {
        return new Builder(columns, capacity);
    }

    /** Fills a batch row by row; {@link #build} hands it over and the builder is spent. */
    public static final class Builder {
        private final List<ExecutionPlan.Column> columns;
        private final Vector[] vectors;
        private final int capacity;
        private int size;
        private boolean built;

        private Builder(List<ExecutionPlan.Column> columns, int capacity) {
            if (capacity < 1) {
                throw new IllegalArgumentException("a batch holds at least one row: capacity " + capacity);
            }
            this.columns = List.copyOf(columns);
            this.capacity = capacity;
            this.vectors = new Vector[this.columns.size()];
            for (int i = 0; i < vectors.length; i++) {
                vectors[i] = Vector.of(this.columns.get(i), capacity);
            }
        }

        /** Adds one row, its cells in column order. */
        public void add(List<?> cells) {
            if (built) {
                throw new IllegalStateException("a built batch takes no more rows");
            }
            if (cells.size() != vectors.length) {
                throw new IllegalArgumentException("a row of " + cells.size() + " cells for " + vectors.length
                        + " columns " + columns.stream().map(ExecutionPlan.Column::name).toList());
            }
            if (size == capacity) {
                throw new IllegalStateException("the batch is full: " + capacity + " rows");
            }
            for (int i = 0; i < vectors.length; i++) {
                vectors[i].set(size, cells.get(i), columns.get(i));
            }
            size++;
        }

        public int size() {
            return size;
        }

        public boolean full() {
            return size == capacity;
        }

        public Batch build() {
            if (built) {
                throw new IllegalStateException("a batch is built once");
            }
            built = true;
            return new Batch(columns, vectors, size);
        }
    }

    /** One column's cells: a typed array with a null mask. */
    private abstract static sealed class Vector permits Longs, Doubles, Booleans, Cells, TaggedCells {
        final boolean[] nulls;

        Vector(int capacity) {
            nulls = new boolean[capacity];
        }

        static Vector of(ExecutionPlan.Column c, int capacity) {
            if (RowDecode.slotWord(c.slot()).equals("TAGGED")) {
                // a tagged column: the carrier and the tag apart (TaggedCell; track F, 2026-10-29)
                return new TaggedCells(capacity);
            }
            if (c.multiplicity().upper() == null || c.multiplicity().upper() > 1) {
                // a many-valued column (an ARRAY slot): each cell a list of its element type's carriers (track A,
                // 2026-10-29: the Arrow writer's gate holds one; an Integer[*] column refused its lists here)
                return new Cells(capacity);
            }
            return switch (c.type()) {
                case "Integer" -> new Longs(capacity);
                case "Float" -> new Doubles(capacity);
                case "Boolean" -> new Booleans(capacity);
                default -> new Cells(capacity);
            };
        }

        boolean isNull(int row) {
            return nulls[row];
        }

        abstract @com.legend.base.Nullable Object get(int row);

        abstract void set(int row, @com.legend.base.Nullable Object cell, ExecutionPlan.Column c);

        int tag(int row, ExecutionPlan.Column c) {
            throw new IllegalStateException("column '" + c.name() + "' (" + c.type() + ", slot " + c.slot()
                    + ") carries no tag: it is no tagged column");
        }

        static IllegalArgumentException refused(@com.legend.base.Nullable Object cell, ExecutionPlan.Column c) {
            return new IllegalArgumentException("column '" + c.name() + "' (" + c.type() + ", slot " + c.slot()
                    + "): a cell " + (cell == null ? "null" : cell.getClass().getSimpleName() + " " + cell)
                    + " is not its type's carrier");
        }
    }

    /** An Integer column: {@code long}s, and the exact value of a cell past 63 bits beside them; a text the database
     *  holds under an Integer column (the session's rule: {@code JdbcDecode}) is carried as the text. */
    private static final class Longs extends Vector {
        private final long[] values;
        private Object @com.legend.base.Nullable [] other;

        Longs(int capacity) {
            super(capacity);
            values = new long[capacity];
        }

        @Override
        @com.legend.base.Nullable Object get(int row) {
            if (nulls[row]) {
                return null;
            }
            return other != null && other[row] != null ? other[row] : (Object) values[row];
        }

        @Override
        void set(int row, @com.legend.base.Nullable Object cell, ExecutionPlan.Column c) {
            switch (cell) {
                case null -> nulls[row] = true;
                case Long l -> values[row] = l;
                case BigInteger b -> held(row, b);
                case String s -> held(row, s);
                default -> throw refused(cell, c);
            }
        }

        private void held(int row, Object cell) {
            if (other == null) {
                other = new Object[values.length];
            }
            other[row] = cell;
        }
    }

    /** A Float column: {@code double}s, bit for bit; a text the database holds under a Float column carried as it is. */
    private static final class Doubles extends Vector {
        private final double[] values;
        private Object @com.legend.base.Nullable [] other;

        Doubles(int capacity) {
            super(capacity);
            values = new double[capacity];
        }

        @Override
        @com.legend.base.Nullable Object get(int row) {
            if (nulls[row]) {
                return null;
            }
            return other != null && other[row] != null ? other[row] : (Object) values[row];
        }

        @Override
        void set(int row, @com.legend.base.Nullable Object cell, ExecutionPlan.Column c) {
            switch (cell) {
                case null -> nulls[row] = true;
                case Double d -> values[row] = d;
                case String s -> {
                    if (other == null) {
                        other = new Object[values.length];
                    }
                    other[row] = s;
                }
                default -> throw refused(cell, c);
            }
        }
    }

    /** A Boolean column. */
    private static final class Booleans extends Vector {
        private final boolean[] values;

        Booleans(int capacity) {
            super(capacity);
            values = new boolean[capacity];
        }

        @Override
        @com.legend.base.Nullable Object get(int row) {
            return nulls[row] ? null : (Object) values[row];
        }

        @Override
        void set(int row, @com.legend.base.Nullable Object cell, ExecutionPlan.Column c) {
            switch (cell) {
                case null -> nulls[row] = true;
                case Boolean b -> values[row] = b;
                default -> throw refused(cell, c);
            }
        }
    }

    /** Any other column: the session's carrier for each cell. */
    private static final class Cells extends Vector {
        private final Object[] values;

        Cells(int capacity) {
            super(capacity);
            values = new Object[capacity];
        }

        @Override
        @com.legend.base.Nullable Object get(int row) {
            return nulls[row] ? null : values[row];
        }

        @Override
        void set(int row, @com.legend.base.Nullable Object cell, ExecutionPlan.Column c) {
            if (cell == null) {
                nulls[row] = true;
            } else {
                values[row] = cell;
            }
        }
    }

    /** A tagged column: each cell's carrier in its slot's own kind, and the slot index beside it (the tag the decoders
     *  read with the row: {@link TaggedCell}); a cell that is no tagged cell refused by name. */
    private static final class TaggedCells extends Vector {
        private final Object[] values;
        private final int[] tags;

        TaggedCells(int capacity) {
            super(capacity);
            values = new Object[capacity];
            tags = new int[capacity];
        }

        @Override
        @com.legend.base.Nullable Object get(int row) {
            return nulls[row] ? null : values[row];
        }

        @Override
        void set(int row, @com.legend.base.Nullable Object cell, ExecutionPlan.Column c) {
            switch (cell) {
                case null -> nulls[row] = true;
                case TaggedCell t -> {
                    values[row] = t.value();
                    tags[row] = t.slot();
                }
                default -> throw new IllegalArgumentException("column '" + c.name() + "' (" + c.type() + ", slot "
                        + c.slot() + "): a tagged column's cell carries its tag (TaggedCell); a bare "
                        + cell.getClass().getSimpleName() + " " + cell + " does not");
            }
        }

        @Override
        int tag(int row, ExecutionPlan.Column c) {
            return nulls[row] ? -1 : tags[row];
        }
    }

    @Override
    public String toString() {
        StringBuilder b = new StringBuilder("Batch").append(columns.stream().map(ExecutionPlan.Column::name).toList())
                .append(" of ").append(size).append(':');
        for (int r = 0; r < size; r++) {
            b.append(' ').append(Arrays.toString(row(r).toArray()));
        }
        return b.toString();
    }
}
