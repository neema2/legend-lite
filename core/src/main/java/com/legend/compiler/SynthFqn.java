package com.legend.compiler;

import com.legend.parser.element.SynthHat;

/**
 * The single source of truth for synthesized-function FQN conventions &mdash;
 * the reserved {@code $}-sigil names that Phase E (externalization) <em>writes</em>
 * and Phase F (the typed model) <em>references</em>.
 *
 * <p>These two phases must agree byte-for-byte: {@code ModelNormalizer}
 * synthesizes a {@code <owner>$prop$<name>} function, and
 * {@code PureModelContext} points {@code Property.Derived#bodyFunctionFqn} at
 * the identical string. Encoding the convention in one place removes the drift
 * risk of duplicating the literal {@code "$prop$"} / {@code "$constraint$"} on
 * both sides.
 *
 * <p>The {@code $} sigil is non-user-writable in a Pure identifier, so a synth
 * FQN can never collide with a user-authored function name in the shared
 * {@code findFunction} lookup slot.
 */
public final class SynthFqn {

    private SynthFqn() {
    }

    /** {@code <owner>$prop$<name>} &mdash; an externalized derived-property body (§1.5). */
    public static String prop(String ownerFqn, String propertyName) {
        return hatFqn(ownerFqn, SynthHat.PROP, propertyName);
    }

    /** {@code <owner>$constraint$<name>} &mdash; an externalized constraint predicate (§1.4). */
    public static String constraint(String ownerFqn, String constraintName) {
        return hatFqn(ownerFqn, SynthHat.CONSTRAINT, constraintName);
    }

    /** {@code <svc>$query} &mdash; an externalized service query (§1). */
    public static String query(String serviceFqn) {
        check(serviceFqn, "serviceFqn");
        return serviceFqn + "$" + SynthHat.QUERY.segment();
    }

    /**
     * {@code <mapping>$class$<classFqn>} &mdash; a lifted class-mapping
     * realizing function ({@code docs/CLEAN_SHEET_INVERSION.md} &sect;3).
     *
     * <p><strong>The full class FQN is embedded, not its simple name.</strong>
     * The name must be a total injective function of (mapping, class): the
     * lifted function lives in the one {@code findFunction} index keyed by this
     * string, so two distinct class mappings that produced the same name would
     * silently collide. Dropping the package is <em>not</em> injective &mdash;
     * {@code a::Person} and {@code b::Person} mapped in one mapping would both
     * become {@code <mapping>$class$Person}. Embedding the full FQN is exactly
     * what {@link #prop} / {@link #constraint} / {@link #query} already do
     * (they carry the full owner FQN); this keeps the whole synth-FQN family
     * collision-free by construction. Nothing parses the package out of a
     * synth FQN (the {@code $} sigil is non-user-writable and the names are
     * opaque keys), so the embedded {@code ::} is harmless.
     *
     * <p>The {@code [$<setId>]} discriminator of the &sect;3 scheme &mdash; for
     * multiple set-ID'd mappings of the <em>same</em> class &mdash; is deferred
     * until set-ID dispatch lands; the full class FQN already makes distinct
     * classes distinct.
     */
    public static String mappingClass(String mappingFqn, String classFqn) {
        return hatFqn(mappingFqn, SynthHat.CLASS, classFqn);
    }

    /**
     * {@code <mapping>$assoc$<associationFqn>} &mdash; a lifted
     * association-mapping predicate function
     * ({@code docs/CLEAN_SHEET_INVERSION.md} &sect;3). Embeds the full
     * association FQN for the same injectivity reason as {@link #mappingClass};
     * the {@code $assoc$} hat segment keeps it distinct from a class transform
     * of the same target FQN.
     */
    public static String mappingAssoc(String mappingFqn, String associationFqn) {
        return hatFqn(mappingFqn, SynthHat.ASSOC, associationFqn);
    }

    /**
     * {@code <ownerFqn>$<hat.segment()>$<name>}. The {@link SynthHat} segment is
     * the one source of truth shared with the provenance tag. Guards against
     * {@code null} components and against a {@code $} already in an operand
     * (the sigil is non-user-writable, so a {@code $} here means a caller bug)
     * &mdash; failing at construction, not at a later {@code findFunction} miss.
     */
    private static String hatFqn(String ownerFqn, SynthHat hat, String name) {
        check(ownerFqn, "ownerFqn");
        check(name, "name");
        return ownerFqn + "$" + hat.segment() + "$" + name;
    }

    private static void check(String s, String what) {
        if (s == null) {
            throw new IllegalArgumentException("synth-FQN " + what + " is null");
        }
        if (s.indexOf('$') >= 0) {
            throw new IllegalArgumentException(
                    "synth-FQN " + what + " contains the reserved '$' sigil: " + s);
        }
    }
}
