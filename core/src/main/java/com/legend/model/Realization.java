package com.legend.model;

import com.legend.model.spec.ValueSpecification;

import java.util.List;
import java.util.Objects;

/**
 * How a behavior binding is realized &mdash; the one shared, member-level
 * sealed choice (docs/CLEAN_SHEET_INVERSION.md §1.5.2) used by <em>every</em>
 * binding site: mapping class/association bindings, and the class/service
 * "hats" (derived properties, constraints, service queries). It is the literal
 * embodiment of "everything is a function, bound by ref-or-inline":
 * <ul>
 *   <li>{@link Ref} &mdash; a function reference by FQN (clean-sheet "Door 1/4",
 *       and the form <em>every</em> binding has after Phase E once inline bodies
 *       are lifted).</li>
 *   <li>{@link Inline} &mdash; a raw Pure expression body (sugar / "Door 3").
 *       The normalizer lambda-lifts it into an ordinary function and (for
 *       mappings) rewrites the binding to a {@link Ref}.</li>
 * </ul>
 * Sharing one type across all sites avoids N identical copies of the sealed
 * union (cf. CSI-11's {@code MappingInclude} promotion).
 */
public sealed interface Realization permits Realization.Ref, Realization.Inline {

    /** A realizing function named by FQN. */
    record Ref(String functionFqn) implements Realization {
        public Ref {
            Objects.requireNonNull(functionFqn, "functionFqn");
        }
    }

    /** An inline expression body (one or more statements), lifted by Phase E. */
    record Inline(List<ValueSpecification> body) implements Realization {
        public Inline {
            body = body == null ? List.of() : List.copyOf(body);
        }
    }
}
