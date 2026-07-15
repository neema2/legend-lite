package com.legend.model.spec;

import java.util.Objects;

/**
 * Property access on a receiver expression. Source forms:
 * <ul>
 *   <li>{@code $x.name} &rarr; {@code AppliedProperty($x, "name")}</li>
 *   <li>{@code $x.foo.bar} &rarr; nested
 *       {@code AppliedProperty(AppliedProperty($x, "foo"), "bar")}</li>
 *   <li>{@code $x.'My Name'} (quoted) &rarr;
 *       {@code AppliedProperty($x, "My Name")} &mdash; the surrounding
 *       quotes are stripped and standard escapes are resolved, matching
 *       string-literal handling.</li>
 * </ul>
 *
 * <h2>Deliberate divergence from engine's {@code AppliedProperty}</h2>
 *
 * <p>The engine record carries the receiver inside a
 * {@code List<ValueSpecification> parameters} of always-size-1
 * (engine's own doc: "The receiver as a single-element list:
 * {@code [Variable("x")]}"). The list wrapper is uniformity for
 * uniformity's sake: a property has exactly one receiver, so the list
 * is structurally always trivial and consumers iterating it are doing
 * theatre. We make the field explicit so the code reads left-to-right
 * matching the source: {@code receiver.property}.
 *
 * <p>Property invocations with extra arguments (e.g. qualified
 * properties) are represented as {@link AppliedFunction} nodes &mdash;
 * the property name becomes the function name, the receiver becomes
 * parameter 0, additional args follow &mdash; matching how the parser
 * dispatches when a {@code (...)} follows the property name in source.
 *
 * @param receiver  the expression the property is accessed on
 * @param property  the property name as written in source (unquoted)
 */
public record AppliedProperty(
        ValueSpecification receiver,
        String property) implements ValueSpecification {

    public AppliedProperty {
        Objects.requireNonNull(receiver, "receiver");
        Objects.requireNonNull(property, "property");
    }
}
