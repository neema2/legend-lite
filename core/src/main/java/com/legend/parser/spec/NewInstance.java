package com.legend.parser.spec;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Payload describing the structured contents of a {@code ^Class(...)}
 * struct-literal expression. The Pure {@code ^} surface syntax is
 * desugared at parse time to a function call
 * {@code AppliedFunction("new", [PackageableElementPtr(className),
 * NewInstance(...)])} &mdash; the outer wrapper makes new-expressions
 * slot into the uniform &ldquo;everything is a function&rdquo;
 * dispatch pipeline, while this record carries the named-binding
 * data that a positional-arg function cannot express.
 *
 * <p>Source forms (assuming the {@code ^Foo(...)} desugar wrapper is
 * applied around each):
 *
 * <ul>
 *   <li>{@code ^MyClass(name='Alice', age=30)} &rarr;
 *       {@code NewInstance("MyClass", [], {name -> KeyExpression(...),
 *       age -> KeyExpression(...)})}</li>
 *   <li>{@code ^my::pkg::MyClass()} &rarr;
 *       {@code NewInstance("my::pkg::MyClass", [], {})} (zero
 *       bindings is legal)</li>
 *   <li>{@code ^Pair<Integer, String>(first=1, second='a')} &rarr;
 *       {@code NewInstance("Pair", ["Integer", "String"], {...})}
 *       (parametric class with type arguments)</li>
 *   <li>{@code ^MyClass(xs += 1, xs += 2)} &rarr;
 *       <em>only the second binding survives</em>: parse-time
 *       last-wins via the underlying {@link LinkedHashMap}, matching
 *       engine-lite. The retained binding's {@link KeyExpression}
 *       carries {@code isAdd=true}.</li>
 * </ul>
 *
 * <p>Type arguments are preserved as source-level strings (e.g.
 * {@code "Integer"}, {@code "my::pkg::Foo"}); structural type
 * resolution happens in the Element / Expression compiler.
 *
 * <h2>Shape rationale</h2>
 *
 * <p>{@code Map<String, KeyExpression>} (LinkedHashMap to preserve
 * source order) is a deliberate hybrid of engine-lite and engine-pure:
 *
 * <ul>
 *   <li><strong>From engine-lite</strong>: properties as a {@code Map}
 *       keyed by property name. Direct lookup for the typechecker;
 *       silent last-wins on duplicates (engine never validates
 *       duplicates anyway &mdash; verified in {@code NewValidator}).</li>
 *   <li><strong>From engine-pure / legend-engine protocol</strong>:
 *       each binding carries the {@code isAdd} flag (preserved via
 *       {@link KeyExpression}). Engine-lite drops this distinction;
 *       we keep it because {@code prop += val} (append) and
 *       {@code prop = val} (assign) are different operations whose
 *       collapse is a semantic loss, not a cosmetic simplification.</li>
 * </ul>
 *
 * <p>This is the second decision-reversal in the rewrite: the C.3
 * commit shipped with {@code List<KeyExpression>} (over-engineered
 * for hypothetical duplicate-detection); a subsequent investigation
 * showed neither engine actually validates duplicates, and that
 * engine-lite's {@code Map} shape with the {@code isAdd} flag added
 * back is the cleanest synthesis. Documented inline so a future
 * reader sees the rationale rather than re-litigating.
 *
 * @param className      class FQN as written in source (simple or
 *                       {@code ::}-qualified)
 * @param typeArguments  source-level type-argument names (empty
 *                       list when the class is non-parametric)
 * @param properties     property bindings keyed by source name;
 *                       insertion order preserved via {@link LinkedHashMap}
 */
public record NewInstance(
        String className,
        List<String> typeArguments,
        Map<String, KeyExpression> properties) implements ValueSpecification {

    public NewInstance {
        Objects.requireNonNull(className, "className");
        Objects.requireNonNull(typeArguments, "typeArguments");
        Objects.requireNonNull(properties, "properties");
        typeArguments = List.copyOf(typeArguments);
        // Preserve insertion order; Map.copyOf returns an unordered
        // copy so we explicitly wrap a LinkedHashMap with
        // unmodifiableMap to keep both immutability AND source order.
        properties = Collections.unmodifiableMap(new LinkedHashMap<>(properties));
    }
}
