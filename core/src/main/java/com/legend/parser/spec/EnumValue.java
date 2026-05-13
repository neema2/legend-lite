package com.legend.parser.spec;

import com.legend.parser.Multiplicity;

import java.util.Objects;

/**
 * Reference to a named enumeration value as a value expression.
 *
 * <p>In Pure, an enum literal looks syntactically like property access
 * on a packageable element: {@code JoinKind.INNER},
 * {@code DurationUnit.DAYS}, {@code my::pkg::Status.ACTIVE}. Without
 * a model lookup the parser cannot know whether the left-hand side is
 * an enum type or a class (for which the same source form would be a
 * property access). The disambiguation rule the parser <em>can</em>
 * apply is structural: if the receiver of a {@code .}-postfix has
 * already parsed as a {@link PackageableElementPtr} (i.e., the source
 * is a bare qualified-name without a call suffix), then the dot form
 * is an enum-value reference &mdash; property access only makes sense
 * on a value, not on a class-name.
 *
 * <p>This matches engine-lite's emission rule verbatim: in
 * {@code parsePropertyExpression}, when the receiver is a
 * {@link PackageableElementPtr}, the parser returns an
 * {@code EnumValue} rather than an {@link AppliedProperty}. Keeping
 * the shapes distinct at parse time avoids a downstream re-walk to
 * re-classify on every property access.
 *
 * <h2>Arity</h2>
 *
 * <p>Exactly two fields &mdash; the fully-qualified enum type path
 * ({@code "my::pkg::JoinKind"}) and the value name ({@code "INNER"}).
 * No type arguments, no multiplicity, no receiver chain. Enum
 * references don't carry those slots in Pure.
 *
 * @param fullPath fully-qualified enum type, verbatim as written
 *                 (no shortening, no import resolution &mdash; that
 *                 happens in {@code NameResolver})
 * @param value    the enum value name as written
 */
public record EnumValue(String fullPath, String value)
        implements ValueSpecification {
    public EnumValue {
        Objects.requireNonNull(fullPath, "fullPath");
        Objects.requireNonNull(value, "value");
    }
}
