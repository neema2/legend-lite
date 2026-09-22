package com.legend.model;

/**
 * The NAMING of a lifted derived property: {@code <owner>$prop$<name>} — one
 * spelling, read by the compiler's lifting ({@code DerivedProps}) and by the
 * builtin registry of platform-implemented accessors ({@code NativeFn.RowGetter}).
 */
public final class DerivedPropertyNames {

    /** The sigil between owner and property name. */
    public static final String SIGIL = "$" + SynthHat.PROP.segment() + "$";

    private DerivedPropertyNames() {
    }

    public static String lifted(String ownerFqn, String name) {
        return ownerFqn + SIGIL + name;
    }

    /** {owner, name}, or null when {@code fqn} is not a lifted derived property. */
    public static String @com.legend.base.Nullable [] split(String fqn) {
        int i = fqn.indexOf(SIGIL);
        return i <= 0 ? null
                : new String[] {fqn.substring(0, i), fqn.substring(i + SIGIL.length())};
    }
}
