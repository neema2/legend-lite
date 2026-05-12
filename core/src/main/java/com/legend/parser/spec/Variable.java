package com.legend.parser.spec;

import java.util.Objects;

/**
 * Variable reference in a Pure expression. Source form: {@code $name}.
 *
 * <p>The {@code name} field excludes the {@code $} prefix and is just
 * the source-level identifier (e.g. {@code "this"} for {@code $this}).
 * Variables introduced by lambda parameter declarations may carry a
 * declared {@code typeName} (e.g. {@code "Integer"}) and a structured
 * {@code multiplicity} (e.g. {@link Multiplicity.Concrete#PURE_ONE}).
 * For free-standing variable references &mdash; which is what C.1
 * produces &mdash; both auxiliary fields are {@code null}.
 *
 * <h2>C.5: structured multiplicity</h2>
 *
 * <p>C.1 originally stored multiplicity as a raw {@link String}
 * because nothing in core consumed the structure. C.5 introduces
 * typed lambda parameters ({@code {p: Integer[1] | body}}) whose
 * parser-emitted Variables carry the declared multiplicity in a
 * shape the downstream type-checker can consume directly. Engine's
 * lambda-param processing reads structured bounds, not raw text;
 * matching that here avoids a re-parse layer.
 *
 * @param name         identifier text, without the {@code $} prefix
 * @param typeName     declared element/primitive name, or {@code null}
 *                     for free references
 * @param multiplicity declared multiplicity annotation, or {@code null}
 *                     for untyped variables; one of
 *                     {@link Multiplicity.Concrete} or
 *                     {@link Multiplicity.Parameter}
 */
public record Variable(
        String name,
        String typeName,
        Multiplicity multiplicity) implements ValueSpecification {

    public Variable {
        Objects.requireNonNull(name, "name");
    }

    /** Convenience constructor for untyped free references. */
    public Variable(String name) {
        this(name, null, null);
    }
}
