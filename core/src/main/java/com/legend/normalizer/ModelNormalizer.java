package com.legend.normalizer;

import com.legend.parser.NormalizedModel;
import com.legend.builtin.Pure;
import com.legend.compiler.ModelBuilder;
import com.legend.compiler.SynthFqn;
import com.legend.parser.Multiplicity;
import com.legend.parser.ParsedModel;
import com.legend.parser.TypeExpression;
import com.legend.parser.element.ClassDefinition;
import com.legend.parser.element.ClassDefinition.ConstraintDefinition;
import com.legend.parser.element.ClassDefinition.DerivedPropertyDefinition;
import com.legend.parser.element.FunctionDefinition;
import com.legend.parser.element.PackageableElement;
import com.legend.parser.element.Realization;
import com.legend.parser.element.ServiceDefinition;
import com.legend.parser.element.SynthHat;
import com.legend.parser.spec.LambdaFunction;
import com.legend.parser.spec.PackageableElementPtr;
import com.legend.parser.spec.ValueSpecification;
import com.legend.parser.spec.Variable;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Phase E entry point &mdash; post-parse, post-name-resolution model
 * normalization. For <em>every</em> Pure body site (mapping transform, derived
 * property, constraint, service query) it lifts the body into an ordinary
 * top-level {@link com.legend.parser.element.FunctionDefinition} appended to
 * the {@link NormalizedModel} element list, tagged with
 * {@code synthesizedFrom} provenance.
 *
 * <h2>Lifted-to-element-list contract</h2>
 *
 * <p>Phase E is <strong>additive at the model level</strong>: it appends
 * lifted functions as new elements; it never alters, removes, or rewraps a
 * structural element's declarations (a class keeps its derived-property
 * bodies, a mapping keeps its declarations &mdash; the faithful source image;
 * see {@code docs/CLEAN_SHEET_INVERSION.md} &sect;1.5). Lifted functions are
 * ordinary elements: Phase F ingests them through the same
 * {@code case FunctionDefinition} arm as user functions, with no bespoke
 * flatten. Ownership for invalidation is the derived
 * {@link NormalizedModel#liftedByOwner()} index, recomputed from provenance
 * &mdash; never state written into parser records.
 *
 * <p>The ValueSpec-free guarantee lives one layer down, at the <em>compiled</em>
 * model (Phase F / {@code ModelContext}): there the structural typed elements
 * ({@code TypedClass}, &hellip;) carry only <em>signatures + function-FQN refs</em>
 * (e.g. {@code Property.Derived.bodyFunctionFqn()}), and bodies live solely on
 * {@code TypedFunction}s &mdash; type-checked <em>demand-driven</em> (per query,
 * or {@code compileAll}), never eagerly at F (AGENTS.md F-must-not-trigger-G).
 *
 * <p><strong>Single body-compilation path.</strong> Every body site &mdash;
 * lifted or user-written &mdash; is compiled through the one
 * {@code findFunction} index. The element-embedded copy a structural element
 * still carries (e.g. a class's derived-property body) is inert: it exists for
 * source fidelity and is <em>never</em> compiled directly (doing so would
 * double-compile, in the wrong scope &mdash; implicit {@code $this} vs the
 * lifted function's explicit {@code this} receiver). Lifted FQNs use the
 * reserved {@code $} sigil, so they never collide with a user-writable
 * function name in the shared slot.
 *
 * <p>Single entry point composing four sub-slices. The three simpler body
 * sites are handled inline here; the complex legacy-mapping desugaring is
 * delegated to {@link MappingNormalizer} (split out purely because of its
 * size, not because it is a different stage).
 *
 * <pre>
 *   E.2  derived-property bodies   -&gt; &lt;owner&gt;$prop$&lt;name&gt; functions
 *   E.3  constraint predicates     -&gt; &lt;owner&gt;$constraint$&lt;name&gt; functions (Boolean[1] of this)
 *   E.4  service queries           -&gt; &lt;svc&gt;$query function (Any[*])
 *   E.1  legacy mapping DSL        -&gt; MappingNormalizer (lifted class/assoc functions)
 * </pre>
 *
 * <p>The sub-slices are mutually independent (they touch disjoint def kinds),
 * so ordering among them is free; all run before F.
 *
 * <h2>Current status</h2>
 *
 * <p>All four sub-slices are implemented: E.1 ({@link MappingNormalizer}),
 * E.2 (derived properties), E.3 (constraints), and E.4 (service queries).
 */
public final class ModelNormalizer {

    private ModelNormalizer() {}

    /**
     * Normalize a name-resolved {@link ParsedModel} by lifting a function for
     * every body site into the element list. Structural elements pass through
     * untouched. Re-normalization is impossible at the type level: the result
     * is a {@link NormalizedModel}, which this method does not accept.
     */
    public static NormalizedModel normalize(ParsedModel parsed) {
        Objects.requireNonNull(parsed, "parsed");
        // Build the resolution index ONCE, shared across every sub-slice.
        // Each lifter (E.2-E.4) and the legacy-mapping desugarer (E.1)
        // resolves classes/associations/joins/filters against the same view;
        // none self-builds. The index reflects the model as parsed — every
        // sub-slice is append-only (it lifts functions, never mutates a
        // resolution target: stored properties, supertypes, joins are all
        // untouched). Phase F (element-compile) rebuilds its own index from the
        // normalized output to pick up the lifted functions.
        ModelBuilder model = ModelBuilder.from(parsed);
        // E.1 rewrites MappingDefinitions (extends flattening, multi-hop
        // association injection) and lifts the mapping functions.
        NormalizedModel normalized = MappingNormalizer.normalize(parsed, model);
        List<FunctionDefinition> lifted = new ArrayList<>();
        liftDerivedProperties(parsed, lifted);  // E.2
        liftConstraints(parsed, lifted);        // E.3
        liftServiceQueries(parsed, lifted);     // E.4
        if (lifted.isEmpty()) return normalized;
        List<PackageableElement> elements =
                new ArrayList<>(normalized.elements().size() + lifted.size());
        elements.addAll(normalized.elements());
        elements.addAll(lifted);
        return new NormalizedModel(elements, normalized.imports());
    }

    /**
     * E.2 &mdash; for each {@code DerivedPropertyDefinition}, lift a
     * {@code <owner>$prop$<name>(this:Owner[1], <params>):T[m]} function
     * carrying its body. The owning class is untouched (its declarations and
     * inline bodies stay &mdash; the faithful source image); ownership is
     * recoverable via {@code synthesizedFrom} /
     * {@link NormalizedModel#liftedByOwner()}.
     *
     * <p>The lifted function resolves through {@code findFunction} like any
     * user function. Phase F's {@code Property.Derived} references the lifted
     * FQN by the identical {@code <owner>$prop$<name>} convention.
     */
    private static void liftDerivedProperties(
            ParsedModel parsed, List<FunctionDefinition> lifted) {
        for (PackageableElement el : parsed.elements()) {
            if (el instanceof ClassDefinition cd && !cd.derivedProperties().isEmpty()) {
                TypeExpression thisType = receiverType(cd);
                for (DerivedPropertyDefinition dp : cd.derivedProperties()) {
                    // Only the sugar (inline) form lifts; a Door-4 function-ref
                    // binding is already realized by the user's function.
                    if (dp.realization() instanceof Realization.Inline) {
                        lifted.add(synthDerivedFunction(cd, dp, thisType));
                    }
                }
            }
        }
    }

    /**
     * Build {@code <owner>$prop$<name>(this:Owner[1], <params>):T[m]} carrying
     * the derived property's body. The leading {@code this} receiver binds
     * {@code $this} in the body; the class's type parameters are propagated so
     * a generic owner's {@code T} stays in scope. The FQN uses the reserved
     * {@code $} sigil and MUST match {@code PureModelContext}'s
     * {@code <owner>$prop$<name>} reference.
     */
    private static FunctionDefinition synthDerivedFunction(
            ClassDefinition cd, DerivedPropertyDefinition dp, TypeExpression thisType) {
        List<FunctionDefinition.ParameterDefinition> params =
                new ArrayList<>(dp.parameters().size() + 1);
        params.add(new FunctionDefinition.ParameterDefinition(
                "this", thisType, Multiplicity.Concrete.PURE_ONE));
        for (ClassDefinition.ParameterDefinition p : dp.parameters()) {
            params.add(new FunctionDefinition.ParameterDefinition(
                    p.name(), p.type(), p.multiplicity()));
        }
        return new FunctionDefinition(
                SynthFqn.prop(cd.qualifiedName(), dp.name()),
                cd.typeParams(),
                List.of(),
                params,
                dp.type(),
                dp.multiplicity(),
                dp.expression(),
                List.of(), List.of())
                .withSynthesizedFrom(new FunctionDefinition.Synthesized(
                        SynthHat.PROP, cd.qualifiedName(), dp.name()));
    }

    /**
     * The {@code this} receiver type: the bare class FQN, or
     * {@code Owner<T, ...>} when the owner is generic (so the body's
     * {@code $this} carries the class's type parameters).
     */
    private static TypeExpression receiverType(ClassDefinition cd) {
        if (cd.typeParams().isEmpty()) {
            return new TypeExpression.NameRef(cd.qualifiedName());
        }
        List<TypeExpression> args = new ArrayList<>(cd.typeParams().size());
        for (String tp : cd.typeParams()) {
            args.add(new TypeExpression.NameRef(tp));
        }
        return new TypeExpression.Generic(cd.qualifiedName(), args);
    }

    /**
     * E.3 &mdash; for each {@code ConstraintDefinition}, lift a
     * {@code <owner>$constraint$<name>(this:Owner[1]):Boolean[1]} predicate
     * function carrying the constraint expression. The owning class keeps its
     * {@code constraints()} declarations untouched, exactly like E.2.
     *
     * <p>The predicate is a function of {@code this} returning {@code Boolean[1]}
     * (the constraint "protocol" hat). It surfaces in {@code findFunction} as an
     * ordinary element.
     */
    private static void liftConstraints(
            ParsedModel parsed, List<FunctionDefinition> lifted) {
        for (PackageableElement el : parsed.elements()) {
            if (el instanceof ClassDefinition cd && !cd.constraints().isEmpty()) {
                TypeExpression thisType = receiverType(cd);
                for (ConstraintDefinition c : cd.constraints()) {
                    // Only the sugar (inline) form lifts; a Door-4 ref binding
                    // is already realized by the user's predicate function.
                    if (!(c.realization() instanceof Realization.Inline)) continue;
                    lifted.add(synthConstraintFunction(cd, c, thisType));
                }
            }
        }
    }

    /**
     * Build {@code <owner>$constraint$<name>(this:Owner[1]):Boolean[1]} carrying
     * the constraint predicate as its (single-statement) body. The leading
     * {@code this} receiver binds {@code $this} in the predicate; the class's
     * type parameters are propagated for a generic owner. The return type uses
     * the {@code Boolean} primitive FQN so Phase F classification resolves it
     * (a bare {@code "Boolean"} would not be a known primitive). FQN uses the
     * reserved {@code $} sigil.
     */
    private static FunctionDefinition synthConstraintFunction(
            ClassDefinition cd, ConstraintDefinition c, TypeExpression thisType) {
        FunctionDefinition.ParameterDefinition self =
                new FunctionDefinition.ParameterDefinition(
                        "this", thisType, Multiplicity.Concrete.PURE_ONE);
        return new FunctionDefinition(
                SynthFqn.constraint(cd.qualifiedName(), c.name()),
                cd.typeParams(),
                List.of(),
                List.of(self),
                new TypeExpression.NameRef(Pure.BOOLEAN.qualifiedName()),
                Multiplicity.Concrete.PURE_ONE,
                List.of(c.expression()),
                List.of(), List.of())
                .withSynthesizedFrom(new FunctionDefinition.Synthesized(
                        SynthHat.CONSTRAINT, cd.qualifiedName(), c.name()));
    }

    /**
     * E.4 &mdash; lift each {@code ServiceDefinition}'s query into a
     * {@code <svc>$query(...):Any[*]} function. The service keeps its
     * {@code functionBody} + config untouched.
     *
     * <p>The return type is declared {@code Any[*]} (the most general): a
     * service query's concrete result type is only known after type-checking
     * the body (Phase G, demand-driven), not structurally here.
     */
    private static void liftServiceQueries(
            ParsedModel parsed, List<FunctionDefinition> lifted) {
        for (PackageableElement el : parsed.elements()) {
            if (el instanceof ServiceDefinition sd) {
                // Door 4: a bare-FQN query (`query: my::funcs::q`) binds the
                // service to a user function and lifts nothing; any other query
                // expression is the sugar form, lifted to <svc>$query.
                if (sd.functionBody() instanceof PackageableElementPtr) continue;
                lifted.add(synthServiceQuery(sd));
            }
        }
    }

    /**
     * Build {@code <svc>$query(<params>):Any[*]} from the service's query.
     *
     * <p>When the query is written as a typed lambda ({@code {p: T[m] | body}}
     * or the {@code p: T[m] | body} shorthand), its parameters become the
     * function's parameters and its body the function body. The bare
     * {@code |body} form is a zero-parameter query (any free variables are
     * runtime-injected path params, out of scope here). FQN uses the reserved
     * {@code $} sigil.
     */
    private static FunctionDefinition synthServiceQuery(ServiceDefinition sd) {
        List<FunctionDefinition.ParameterDefinition> params = new ArrayList<>();
        List<ValueSpecification> body;
        if (sd.functionBody() instanceof LambdaFunction lf) {
            for (Variable v : lf.parameters()) {
                if (v.type() == null || v.multiplicity() == null) {
                    throw new com.legend.error.ModelException(
                            com.legend.error.LegendCompileException.Phase.NORMALIZE,
                            "Service '" + sd.qualifiedName() + "' query parameter '"
                                  + v.name() + "' must be typed (name: Type[mult]) to "
                                  + "externalize into " + SynthFqn.query(sd.qualifiedName()));
                }
                params.add(new FunctionDefinition.ParameterDefinition(
                        v.name(), v.type(), v.multiplicity()));
            }
            body = lf.body();
        } else {
            body = List.of(sd.functionBody());
        }
        return new FunctionDefinition(
                SynthFqn.query(sd.qualifiedName()),
                List.of(),
                List.of(),
                params,
                new TypeExpression.NameRef(Pure.ANY.qualifiedName()),
                Multiplicity.Concrete.ZERO_MANY,
                body,
                List.of(), List.of())
                .withSynthesizedFrom(new FunctionDefinition.Synthesized(
                        SynthHat.QUERY, sd.qualifiedName(), "query"));
    }
}
