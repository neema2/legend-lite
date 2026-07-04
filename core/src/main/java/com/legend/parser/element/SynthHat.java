package com.legend.parser.element;

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
    ASSOC;

    /** The lowercase {@code $}-sigil segment for this hat (e.g. {@code CLASS -> "class"}). */
    public String segment() {
        return name().toLowerCase(Locale.ROOT);
    }
}
