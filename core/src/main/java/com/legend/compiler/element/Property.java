package com.legend.compiler.element;

import com.legend.compiler.element.type.Multiplicity;
import com.legend.compiler.element.type.Type;

import java.util.List;
import java.util.Objects;

/**
 * A class member returned by {@code ModelContext.findProperty} &mdash; the
 * polymorphic result of a member-walk. A caller does not know stored-vs-derived
 * until the property is found, so both are one sealed type with a uniform
 * {@code name}/{@code type}/{@code multiplicity} surface (doc §4).
 *
 * <ul>
 *   <li>{@link Stored} &mdash; a regular stored property.</li>
 *   <li>{@link Derived} &mdash; a computed property; its <em>signature</em>
 *       lives here (so {@code $p.fullName} types from the class alone), while
 *       its <em>body</em> is an externalized function referenced by
 *       {@link Derived#bodyFunctionFqn()} and resolved via the one
 *       {@code findFunction} (doc §4, §1.5).</li>
 * </ul>
 *
 * <p>Association-injected navigation properties are <strong>not</strong> a
 * variant here: they are resolved at lookup time from the association index,
 * never stored on {@link TypedClass} (doc §5 discipline 3).
 */
public sealed interface Property permits Property.Stored, Property.Derived {

    /** Property name (unqualified). */
    String name();

    /** Classified value type (FQN-only for nominal kinds). */
    Type type();

    /** Classified multiplicity. */
    Multiplicity multiplicity();

    /** A regular stored property: {@code name: Type[multiplicity];}.
     *
     * <p>{@code equalityKey} carries the {@code <<equality.Key>>}
     * stereotype (X5): the engine's instance equality
     * (EqualityUtilities.equal) compares keyed classes BY THEIR KEY
     * PROPERTIES and refuses value equality for keyless ones — the
     * flag is the model fact that rule reads. Association-injected
     * navigation properties are never keys (they are not declared on
     * the class body). */
    record Stored(String name, Type type, Multiplicity multiplicity,
                  boolean equalityKey, boolean hasDefault,
                  com.legend.protocol.spec.@com.legend.base.Nullable ValueSpecification defaultValue)
            implements Property {
        public Stored {
            Objects.requireNonNull(name, "name");
            Objects.requireNonNull(type, "type");
            Objects.requireNonNull(multiplicity, "multiplicity");
        }

        public Stored(String name, Type type, Multiplicity multiplicity,
                      boolean equalityKey, boolean hasDefault) {
            this(name, type, multiplicity, equalityKey, hasDefault, null);
        }

        /** A defaulted declaration ({@code distinct: Boolean[1] =
         * false}) exempts the property from ^new's missing-required
         * validation — pure NewValidator parity. */
        public Stored(String name, Type type, Multiplicity multiplicity,
                      boolean equalityKey) {
            this(name, type, multiplicity, equalityKey, false);
        }

        public Stored(String name, Type type, Multiplicity multiplicity) {
            this(name, type, multiplicity, false, false);
        }
    }

    /**
     * A derived (computed) property. The body is externalized (Phase E) into a
     * synthesized function; this record keeps only its signature plus the
     * function's FQN.
     *
     * @param name             property name
     * @param type             classified return type
     * @param multiplicity     classified return multiplicity
     * @param parameters       declared parameters (a derived property may be
     *                         parameterized, hence overloadable)
     * @param bodyFunctionFqn  FQN of the externalized body function
     *                         (e.g. {@code model::Person$prop$fullName}, §1.5)
     */
    record Derived(
            String name,
            Type type,
            Multiplicity multiplicity,
            List<TypedParameter> parameters,
            String bodyFunctionFqn) implements Property {
        public Derived {
            Objects.requireNonNull(name, "name");
            Objects.requireNonNull(type, "type");
            Objects.requireNonNull(multiplicity, "multiplicity");
            Objects.requireNonNull(bodyFunctionFqn, "bodyFunctionFqn");
            parameters = parameters == null ? List.of() : List.copyOf(parameters);
        }
    }
}
