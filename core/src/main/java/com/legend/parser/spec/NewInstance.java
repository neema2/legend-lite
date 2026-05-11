package com.legend.parser.spec;

import java.util.List;
import java.util.Objects;

/**
 * New-instance expression &mdash; the Pure {@code ^} operator. Source
 * forms:
 *
 * <ul>
 *   <li>{@code ^MyClass(name='Alice', age=30)} &rarr;
 *       {@code NewInstance("MyClass", [], [KeyExpression("name", ...), KeyExpression("age", ...)])}</li>
 *   <li>{@code ^my::pkg::MyClass()} &rarr;
 *       {@code NewInstance("my::pkg::MyClass", [], [])} (zero
 *       bindings is legal)</li>
 *   <li>{@code ^Pair<Integer, String>(first=1, second='a')} &rarr;
 *       {@code NewInstance("Pair", ["Integer", "String"], [...])}
 *       (parametric class with type arguments)</li>
 * </ul>
 *
 * <p>Type arguments are preserved as source-level strings (e.g.
 * {@code "Integer"}, {@code "my::pkg::Foo"}); structural type
 * resolution happens in the Element / Expression compiler.
 *
 * <h2>Deliberate divergence from engine's {@code NewInstance}</h2>
 *
 * <p>Engine's record exposes properties as a
 * {@code Map<String, ValueSpecification>}, with insertion order
 * preserved <em>by convention</em> via {@code LinkedHashMap}. Two
 * problems with that shape for parser data:
 *
 * <ol>
 *   <li><strong>Order is not enforced by the type system.</strong>
 *       The field type is just {@code Map}; nothing stops a
 *       consumer from constructing a {@code HashMap} and losing
 *       source order. Bug surface for free.</li>
 *   <li><strong>Duplicate bindings are silently collapsed.</strong>
 *       Pure forbids {@code ^Foo(x=1, x=2)}, but engine's map
 *       silently keeps only the second binding, hiding the error
 *       from any later validator. Real parser data should preserve
 *       what the user wrote and let a downstream phase report the
 *       duplicate with correct source location.</li>
 * </ol>
 *
 * <p>{@code List<KeyExpression>} fixes both: order is structural
 * (no convention needed), duplicates are preserved verbatim and
 * remain visible to validators.
 *
 * @param className      class FQN as written in source (simple or
 *                       {@code ::}-qualified)
 * @param typeArguments  source-level type-argument names (empty
 *                       list when the class is non-parametric)
 * @param keyExpressions property bindings in source order; may
 *                       contain duplicate keys if the source did
 */
public record NewInstance(
        String className,
        List<String> typeArguments,
        List<KeyExpression> keyExpressions) implements ValueSpecification {

    public NewInstance {
        Objects.requireNonNull(className, "className");
        Objects.requireNonNull(typeArguments, "typeArguments");
        Objects.requireNonNull(keyExpressions, "keyExpressions");
        typeArguments = List.copyOf(typeArguments);
        keyExpressions = List.copyOf(keyExpressions);
    }
}
