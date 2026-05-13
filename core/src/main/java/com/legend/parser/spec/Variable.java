package com.legend.parser.spec;

import com.legend.parser.Multiplicity;
import com.legend.parser.TypeExpression;

import java.util.Objects;

/**
 * Variable reference in a Pure expression. Source form: {@code $name}.
 *
 * <p>The {@code name} field excludes the {@code $} prefix and is just
 * the source-level identifier (e.g. {@code "this"} for {@code $this}).
 * Variables introduced by lambda parameter declarations may carry a
 * declared {@code type} (a structured {@link TypeExpression}) and a
 * {@code multiplicity}. For free-standing variable references &mdash;
 * which is what bare {@code $x} produces &mdash; both auxiliary fields
 * are {@code null}.
 *
 * <h2>Structured type (formerly {@code String typeName})</h2>
 *
 * <p>Pre-NameResolver, the parser captured lambda-param types as raw
 * source text ({@code "Integer"}, {@code "List<Integer>"},
 * {@code "Map<String, List<V>>"}) and deferred parsing to a later
 * stage. That string carrier was symmetric to the
 * {@code CDateTime(String)} / {@code CStrictDate(String)} carriers
 * we since replaced with {@link PureDateLiteral} / {@link CDate}, and
 * had the same drawbacks: whitespace was lost
 * ({@code "List<T>"} vs {@code "List< T >"}), generic structure was
 * opaque, and {@link com.legend.parser.NameResolver} would have had
 * to re-parse the string to rewrite simple names to FQNs.
 *
 * <p>The current shape stores a structured {@link TypeExpression}
 * (shared with {@link com.legend.parser.element} property /
 * parameter / return types), so a single tree-walk in NameResolver
 * rewrites every name reference once and for all.
 *
 * <h2>Structured multiplicity</h2>
 *
 * <p>Same reasoning: a sealed {@link Multiplicity} (Concrete or
 * Parameter) replaces the original raw {@link String} so the
 * downstream type-checker pattern-matches structure instead of
 * re-parsing. Engine's lambda-param processing reads structured
 * bounds; matching that here avoids a re-parse layer.
 *
 * @param name         identifier text, without the {@code $} prefix
 * @param type         declared type, or {@code null} for free
 *                     references (bare {@code $x} with no annotation)
 * @param multiplicity declared multiplicity annotation, or {@code null}
 *                     for untyped variables; one of
 *                     {@link Multiplicity.Concrete} or
 *                     {@link Multiplicity.Parameter}
 */
public record Variable(
        String name,
        TypeExpression type,
        Multiplicity multiplicity) implements ValueSpecification {

    public Variable {
        Objects.requireNonNull(name, "name");
    }

    /** Convenience constructor for untyped free references. */
    public Variable(String name) {
        this(name, null, null);
    }
}
