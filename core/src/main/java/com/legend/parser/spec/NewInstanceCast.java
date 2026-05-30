package com.legend.parser.spec;

import com.legend.parser.TypeExpression;

import java.util.List;
import java.util.Objects;

/**
 * Positional-cast form of the {@code ^Class(...)} constructor:
 * {@code ^Class($srcExpr)} &mdash; feeds {@code $srcExpr} through
 * Class's active mapping to produce a Class instance. Distinct from
 * {@link NewInstance} (named-args explicit construction) because the
 * lowering is fundamentally different:
 *
 * <ul>
 *   <li>{@link NewInstance} carries a structured map of property
 *       bindings; lowering beta-reduces field accesses through the
 *       explicit bindings ({@code ^C(field=expr, ...).field &rarr; expr}).</li>
 *   <li>{@link NewInstanceCast} carries a single source expression;
 *       lowering looks up the target class's synthesized mapping
 *       function and substitutes {@code $src := <srcExpr>} into its
 *       body (Pass 2 in the lowering pipeline).</li>
 * </ul>
 *
 * Same source family ({@code ^}) and same {@code new} desugar wrapper
 * ({@code AppliedFunction("new", [PackageableElementPtr(class), <this>])}),
 * but different downstream behavior. Sealed-hierarchy pattern matching
 * is the discriminator.
 *
 * <p>Source forms:
 *
 * <ul>
 *   <li>{@code ^Firm($emp.firm)} &rarr;
 *       {@code NewInstanceCast("Firm", [], AppliedProperty(Variable("emp"), "firm"))}</li>
 *   <li>{@code ^my::pkg::Firm($x)} &rarr;
 *       {@code NewInstanceCast("my::pkg::Firm", [], Variable("x"))}</li>
 *   <li>{@code ^Pair<Integer, String>($p)} &rarr;
 *       {@code NewInstanceCast("Pair", [NameRef("Integer"), NameRef("String")], Variable("p"))}</li>
 * </ul>
 *
 * <p>Disambiguated at parse time from the named-args form by lookahead:
 * a {@code (} followed by anything that doesn't begin a property
 * binding ({@code identifier (= | += | . identifier ...)}) parses as
 * a cast. {@code ^Class()} (empty parens) remains a
 * {@link NewInstance} with no bindings &mdash; the empty
 * default-constructor form, never a cast.
 *
 * <p>The cast form is only legal in the class-literal {@code ^Class(...)}
 * surface form; it does not apply to the copy-with-update
 * {@code ^$var(...)} form, which always takes named-args.
 *
 * @param className     class FQN as written in source (simple or
 *                      {@code ::}-qualified); never {@code null}
 * @param typeArguments structured type-argument expressions; empty
 *                      list when the class is non-parametric;
 *                      never {@code null}
 * @param src           the source expression to feed through Class's
 *                      mapping; never {@code null}
 */
public record NewInstanceCast(
        String className,
        List<TypeExpression> typeArguments,
        ValueSpecification src) implements ValueSpecification {

    public NewInstanceCast {
        Objects.requireNonNull(className, "className");
        Objects.requireNonNull(typeArguments, "typeArguments");
        Objects.requireNonNull(src, "src");
        typeArguments = List.copyOf(typeArguments);
    }
}
