package com.legend.parser.spec;

import java.util.Objects;

/**
 * Variable reference in a Pure expression. Source form: {@code $name}.
 *
 * <p>The {@code name} field excludes the {@code $} prefix and is just
 * the source-level identifier (e.g. {@code "this"} for {@code $this}).
 * Variables introduced by lambda parameter declarations may carry a
 * declared {@code typeName} (e.g. {@code "Integer"}) and a
 * {@code multiplicity} string (e.g. {@code "1"}, {@code "0..1"},
 * {@code "*"}). For free-standing variable references &mdash; which is
 * all C.1 produces &mdash; both auxiliary fields are {@code null}.
 *
 * <p>Multiplicity is currently a raw {@link String} mirroring the
 * source-level multiplicity form; a structured {@code Multiplicity}
 * type can be introduced later without changing the parser contract
 * (the parser would then populate it directly). Engine carries a
 * structured form already; core deliberately stays string-typed in
 * C.1 because nothing in core consumes the structure yet.
 *
 * @param name         identifier text, without the {@code $} prefix
 * @param typeName     declared element/primitive name, or {@code null}
 *                     for free references
 * @param multiplicity raw multiplicity text (e.g. {@code "1"},
 *                     {@code "0..1"}), or {@code null} for untyped
 */
public record Variable(
        String name,
        String typeName,
        String multiplicity) implements ValueSpecification {

    public Variable {
        Objects.requireNonNull(name, "name");
    }

    /** Convenience constructor for untyped free references &mdash; the C.1 case. */
    public Variable(String name) {
        this(name, null, null);
    }
}
