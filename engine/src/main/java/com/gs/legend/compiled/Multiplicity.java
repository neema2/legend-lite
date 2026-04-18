package com.gs.legend.compiled;

/**
 * Compiled-state multiplicity: inclusive lower bound plus optional upper bound.
 *
 * <p>{@code upperBound == null} means unbounded ({@code *}). Common shapes:
 * <ul>
 *   <li>{@link #ONE} — {@code [1]}</li>
 *   <li>{@link #ZERO_ONE} — {@code [0..1]}</li>
 *   <li>{@link #MANY} — {@code [*]}</li>
 *   <li>{@link #ZERO_MANY} — {@code [0..*]}</li>
 * </ul>
 */
public record Multiplicity(int lowerBound, Integer upperBound) {

    public static final Multiplicity ONE = new Multiplicity(1, 1);
    public static final Multiplicity ZERO_ONE = new Multiplicity(0, 1);
    public static final Multiplicity MANY = new Multiplicity(1, null);
    public static final Multiplicity ZERO_MANY = new Multiplicity(0, null);

    /** {@code true} if this multiplicity permits more than one value. */
    public boolean isMany() {
        return upperBound == null || upperBound > 1;
    }

    /** {@code true} if this multiplicity allows zero values. */
    public boolean isOptional() {
        return lowerBound == 0;
    }
}
