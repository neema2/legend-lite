package com.legend.model;

import java.util.Locale;

/**
 * The kind of body site a synthesized ("lifted") function came from &mdash; the
 * "hat" a function wears in the structure-vs-behavior model. This enum is the
 * <strong>single source of truth</strong> for both:
 * <ol>
 *   <li>the provenance tag on {@link FunctionDefinition.Synthesized}, and</li>
 *   <li>the {@code $}-sigil segment in the lifted FQN
 *       ({@code <owner>$<segment>$<name>}) built by {@code SynthFqn}.</li>
 * </ol>
 *
 * <p>Folding these into one enum removes the prior stringly-typed coupling:
 * the FQN segment and the provenance hat were separate string literals
 * (<code>"$prop$"</code> in one file, <code>"prop"</code> in another) that
 * could silently drift. Now {@link #segment()} drives both, so they cannot.
 * Pinned by a consistency test.
 */
public enum SynthHat {
    /** A derived property body: {@code <class>$prop$<name>}. */
    PROP,
    /** A class constraint predicate: {@code <class>$constraint$<name>}. */
    CONSTRAINT,
    /** A service query: {@code <svc>$query}. */
    QUERY,
    /** A class-mapping realizing function: {@code <mapping>$class$<classFqn>}. */
    CLASS,
    /** An association-mapping predicate: {@code <mapping>$assoc$<assocFqn>}. */
    ASSOC,
    /** A class constraint {@code ~message}: {@code <class>$constraintMsg$<name>}. */
    CONSTRAINT_MSG,
    /** A store VIEW lifted as a zero-arg relation function (E.5): {@code <db>$view$<name>}. */
    VIEW,
    /** A property's DEFAULT value, typed as the property: {@code <class>$default$<property>} (E.3b, 2026-10-21). */
    DEFAULT,
    /** A function's PRE-condition, over its parameters: {@code <function>$pre$<name>} (E.3c, 2026-10-21). */
    PRE,
    /** A function's POST-condition, over its parameters and {@code $return}: {@code <function>$post$<name>}. */
    POST;

    /** The lowercase {@code $}-sigil segment for this hat (e.g. {@code CLASS -> "class"}). */
    public String segment() {
        return this == CONSTRAINT_MSG ? "constraintMsg"
                : name().toLowerCase(Locale.ROOT);
    }
}
