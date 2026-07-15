package com.legend.model;

import java.util.List;
import java.util.Objects;

/**
 * A view's {@code ~filter ...} clause &mdash; <em>how</em> the filter is
 * reached. Sealed because the two grammar forms have structurally different
 * fields and resolution semantics, not just different field populations:
 *
 * <ul>
 *   <li>{@link Direct}: {@code ~filter FilterName} or
 *       {@code ~filter [DB] FilterName} &mdash; the filter is applied to the
 *       view's main table directly. Carries a single {@link FilterPointer}.</li>
 *   <li>{@link JoinMediated}: {@code ~filter [DB1] @J1 > @J2 | [DB2]? FilterName}
 *       &mdash; the filter applies to a row set reached by walking a join
 *       chain starting in {@code sourceDb}. Carries the source database, the
 *       join chain, and the target {@link FilterPointer}.</li>
 * </ul>
 *
 * <p>The {@code sourceDb} on {@link JoinMediated} is always present (engine's
 * grammar requires {@code [DB1]} for the join-mediated form), so it's modeled
 * as a non-nullable {@code String}.
 *
 * <p>Mirrors FINOS {@code legend-engine}'s {@code FilterMapping}/{@code
 * FilterPointer} pair. Engine encodes both forms as one {@code FilterMapping}
 * record with an optional {@code joins} list; we encode the structural split
 * as a sealed type so consumers cannot accidentally treat a join-mediated
 * filter as a direct one (or vice versa). The two encodings are isomorphic.
 */
public sealed interface FilterMapping permits FilterMapping.Direct, FilterMapping.JoinMediated {

    /** The filter being referenced; non-null on every variant. */
    FilterPointer filter();

    /**
     * {@code ~filter FilterName} or {@code ~filter [DB] FilterName}: the
     * filter is applied to the view's main table without traversing any join
     * chain.
     */
    record Direct(FilterPointer filter) implements FilterMapping {
        public Direct {
            Objects.requireNonNull(filter, "Filter pointer cannot be null");
        }
    }

    /**
     * {@code ~filter [DB1] @J1 > @J2 | [DB2]? FilterName}: walk
     * {@code joins} starting in {@code sourceDb}, then apply {@code filter}.
     */
    record JoinMediated(
            String sourceDb,
            List<JoinChainElement> joins,
            FilterPointer filter) implements FilterMapping {
        public JoinMediated {
            Objects.requireNonNull(sourceDb, "Source database cannot be null for join-mediated filter");
            Objects.requireNonNull(joins, "Joins cannot be null");
            Objects.requireNonNull(filter, "Filter pointer cannot be null");
            if (joins.isEmpty()) {
                throw new IllegalArgumentException(
                        "Join-mediated filter must have at least one join hop; "
                        + "use Direct for unmediated filter references");
            }
            joins = List.copyOf(joins);
        }
    }
}
