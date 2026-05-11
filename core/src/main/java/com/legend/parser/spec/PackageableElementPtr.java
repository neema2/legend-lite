package com.legend.parser.spec;

import java.util.Objects;

/**
 * Reference to a packageable element by FQN &mdash; a class, association,
 * function, store, mapping, runtime, profile, etc., named at expression
 * position. Source forms:
 *
 * <ul>
 *   <li>Simple: {@code Person} &rarr;
 *       {@code PackageableElementPtr("Person")}</li>
 *   <li>Qualified: {@code my::app::Person} &rarr;
 *       {@code PackageableElementPtr("my::app::Person")}</li>
 * </ul>
 *
 * <p>The {@code fullPath} field carries the name <em>exactly</em> as
 * written in source: simple names stay simple, qualified names stay
 * qualified. FQN resolution against the import scope and the model
 * happens in the next pipeline stage (Phase D
 * {@code NameResolver}); the parser deliberately does not consult any
 * model.
 *
 * <p>Mirrors the engine record verbatim &mdash; the engine shape was
 * already minimal data and there is nothing to strip.
 *
 * @param fullPath  source-level name (simple or {@code ::}-qualified)
 */
public record PackageableElementPtr(String fullPath) implements ValueSpecification {
    public PackageableElementPtr {
        Objects.requireNonNull(fullPath, "fullPath");
    }
}
