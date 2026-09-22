package com.legend.protocol.spec;

import java.util.List;
import java.util.Objects;

/**
 * Bracketed list of {@link ColSpec}s. Source form: {@code ~[a, b, c]}
 * or {@code ~[a:x|$x.foo, b:x|$x.bar]}. Each element parses as a
 * full {@link ColSpec} (bare, mapped, or aggregate), so the array
 * can hold a mix of forms.
 *
 * <p>Used in relation-API positions that take multiple columns
 * uniformly: {@code project(~[name, age, salary])}, {@code groupBy(~[…])}.
 *
 * <h2>Element-list semantics</h2>
 *
 * <p>The element list is wrapped immutable on construction. An
 * empty array ({@code ~[]}) is structurally legal in the parser
 * (any downstream rejection lives in the type-checker), so the
 * record does not enforce a minimum size.
 *
 * @param colSpecs the column specs in source order; never {@code null},
 *                 may be empty, immutable after construction
 */
public record ColSpecArray(List<ColSpec> colSpecs,
        @com.legend.base.Nullable com.legend.protocol.SourceInfo pos) implements ColumnInstance {

    /** Position-free form for synthesis and tests. */
    public ColSpecArray(List<ColSpec> colSpecs) {
        this(colSpecs, null);
    }

    /** Position is excluded from equality. */
    @Override
    public boolean equals(Object o) {
        return o instanceof ColSpecArray other && colSpecs.equals(other.colSpecs());
    }

    @Override
    public int hashCode() {
        return colSpecs.hashCode();
    }


    public ColSpecArray {
        Objects.requireNonNull(colSpecs, "colSpecs");
        colSpecs = List.copyOf(colSpecs);
    }
}
