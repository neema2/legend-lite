package com.legend.compiler.element;

import java.util.Objects;
import java.util.Optional;

/**
 * A compiled class constraint &mdash; FINOS-functional-parity metadata kept on
 * the class, with only its <em>bodies</em> externalized into functions (doc §1.4).
 *
 * <p>FINOS {@code Constraint} is {@code {name, functionDefinition, externalId,
 * enforcementLevel, messageFunction}}, evaluated as
 * {@code predicate(this) -> Boolean[1]}. We keep every feature but store the
 * predicate and message as externalized function FQNs and the level as an enum.
 *
 * @param name           constraint name (required)
 * @param externalId     optional external identifier (FINOS parity)
 * @param level          enforcement level (default {@link EnforcementLevel#ERROR})
 * @param predicateFqn   FQN of the synthesized {@code this -> Boolean[1]} predicate
 *                       function (e.g. {@code model::Person$constraint$validAge}, §1.5)
 * @param messageFqn     optional FQN of the synthesized {@code this -> String[1]}
 *                       message function ({@code …$constraintMsg$validAge})
 */
public record TypedConstraint(
        String name,
        Optional<String> externalId,
        EnforcementLevel level,
        String predicateFqn,
        Optional<String> messageFqn) {

    /** Constraint enforcement level. FINOS stores a String; we use an enum. */
    public enum EnforcementLevel { ERROR, WARN }

    public TypedConstraint {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(level, "level");
        Objects.requireNonNull(predicateFqn, "predicateFqn");
        externalId = externalId == null ? Optional.empty() : externalId;
        messageFqn = messageFqn == null ? Optional.empty() : messageFqn;
    }

    /**
     * The common case: a named, {@code ERROR}-level constraint with no external
     * id and no custom message.
     */
    public static TypedConstraint of(String name, String predicateFqn) {
        return new TypedConstraint(name, Optional.empty(), EnforcementLevel.ERROR,
                predicateFqn, Optional.empty());
    }
}
