package com.legend.parser.element;

import java.util.Objects;

/**
 * A reference to a named {@code Filter} declared in a
 * {@link DatabaseDefinition}. Sealed because the two grammar forms have
 * <em>different resolution semantics</em>, not just different field
 * populations:
 *
 * <ul>
 *   <li>{@link Local}: the user wrote {@code ~filter FilterName} with no
 *       {@code [DB]} qualifier. The resolver searches the enclosing database
 *       <em>and every database listed in its {@code include} declarations</em>
 *       for a filter by that name.</li>
 *   <li>{@link Cross}: the user wrote {@code ~filter [DB] FilterName}. The
 *       resolver looks only in the named database; if absent there, it's an
 *       error (does <em>not</em> fall back to the enclosing scope).</li>
 * </ul>
 *
 * <p>Encoding this distinction as a sealed type rather than a nullable
 * {@code db} field is deliberate: a consumer that pattern-matches will be
 * forced by the compiler to handle both resolution paths. A nullable field
 * would invite a single resolver branch with hidden "if db == null"
 * semantics &mdash; the bug-class we want to make impossible.
 *
 * <p>Engine's protocol uses one {@code FilterPointer { String db; String
 * name; }} class with a nullable {@code db}. We diverge structurally
 * (sealed-2-variant vs flat-with-null) while preserving the wire-level
 * information; the two encodings are isomorphic.
 */
public sealed interface FilterPointer permits FilterPointer.Local, FilterPointer.Cross {

    /** The filter name; non-null on every variant. */
    String name();

    /**
     * {@code ~filter FilterName}: ambient lookup against the enclosing
     * database and its included databases. The resolver chooses which
     * declaring database wins.
     */
    record Local(String name) implements FilterPointer {
        public Local {
            Objects.requireNonNull(name, "Filter name cannot be null");
        }
    }

    /**
     * {@code ~filter [DB] FilterName}: explicit cross-database reference.
     * The resolver must find {@code FilterName} in {@code db} specifically;
     * it does not fall back to the enclosing scope on miss.
     */
    record Cross(String db, String name) implements FilterPointer {
        public Cross {
            Objects.requireNonNull(db, "Cross-db filter must name a database");
            Objects.requireNonNull(name, "Filter name cannot be null");
        }
    }
}
