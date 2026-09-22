package com.legend.normalizer;

import com.legend.builtin.Pure;
import com.legend.compiler.KnowledgeLayer;
import com.legend.compiler.ModelBuilder;
import com.legend.compiler.SynthFqn;
import com.legend.error.LegendCompileException;
import com.legend.error.ModelException;
import com.legend.protocol.Multiplicity;
import com.legend.model.NormalizedModel;
import com.legend.model.ParsedModel;
import com.legend.protocol.TypeExpression;
import com.legend.model.AssociationDefinition;
import com.legend.protocol.ConstraintDefinition;
import com.legend.protocol.DerivedPropertyDefinition;
import com.legend.model.ClassDefinition;
import com.legend.model.DatabaseDefinition;
import com.legend.model.FunctionDefinition;
import com.legend.model.PackageableElement;
import com.legend.protocol.Realization;
import com.legend.model.ServiceDefinition;
import com.legend.model.SynthHat;
import com.legend.protocol.spec.LambdaFunction;
import com.legend.protocol.spec.PackageableElementPtr;
import com.legend.protocol.spec.ValueSpecification;
import com.legend.protocol.spec.Variable;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
/**
 * Phase E entry point &mdash; post-parse, post-name-resolution model
 * normalization. For <em>every</em> Pure body site (mapping transform, derived
 * property, constraint, service query) it lifts the body into an ordinary
 * top-level {@link com.legend.model.FunctionDefinition} appended to
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
     *
     * <p>{@code model} is THE graph's one index (T4.1 step 2), built by the
     * driver from this same {@code parsed} BEFORE this phase; every sub-slice
     * resolves classes/associations/joins/filters against it and none
     * self-builds. This phase READS the index and writes nothing into it:
     * what it learns rides its products (the compiled mappings' facts, the
     * lifted functions), which the driver ADDS to the same index at the
     * E&rarr;F gate. A non-null {@code wallSink} (module compile) collects
     * per-mapping normalization walls &mdash; failing mappings are walled
     * and excluded in one pass instead of throwing on the first.
     */
    public static NormalizedModel normalize(ParsedModel parsed, ModelBuilder model,
            java.util.@com.legend.base.Nullable Map<String, String> wallSink) {
        Objects.requireNonNull(parsed, "parsed");
        Objects.requireNonNull(model, "model");
        // Association QUALIFIED properties were adopted into their owning
        // classes by the knowledge layer (F1, KnowledgeLayer) BEFORE this
        // phase — the single class-derived funnel (E.2, findProperty,
        // $prop$ lifting) covers them with no second path. A model that
        // arrives un-adopted is a pipeline-order bug: loud, never a
        // silently missing property.
        requireQualifiedPropertiesAdopted(parsed, wallSink);
        // E.1 rewrites MappingDefinitions (extends flattening, multi-hop
        // association injection) and lifts the mapping functions.
        NormalizedModel normalized = MappingNormalizer.normalize(parsed, model, wallSink);
        List<FunctionDefinition> lifted = new ArrayList<>();
        liftDerivedProperties(parsed, lifted);  // E.2
        liftConstraints(parsed, lifted);        // E.3
        liftServiceQueries(parsed, lifted);     // E.4
        liftViews(parsed, model, lifted, wallSink); // E.5
        if (lifted.isEmpty()) return normalized;
        List<PackageableElement> elements =
                new ArrayList<>(normalized.elements().size() + lifted.size());
        elements.addAll(normalized.elements());
        elements.addAll(lifted);
        return new NormalizedModel(elements, normalized.imports(), normalized.legacySurfaces());
    }

    /**
     * The knowledge layer ran: every association qualified property whose
     * owning class is in this model is held by that class (by identity —
     * the adoption appends the very definition). An association a
     * tolerant build walled (no unique owning end) is exempt; in a strict
     * build such an association cannot reach here, so its presence is the
     * same pipeline-order bug.
     */
    private static void requireQualifiedPropertiesAdopted(ParsedModel parsed,
            java.util.@com.legend.base.Nullable Map<String, String> wallSink) {
        Map<String, ClassDefinition> classes = null;
        for (PackageableElement el : parsed.elements()) {
            if (!(el instanceof AssociationDefinition ad)
                    || ad.derivedProperties().isEmpty()
                    || (wallSink != null && wallSink.containsKey(ad.qualifiedName()))) {
                continue;
            }
            if (classes == null) {
                classes = new LinkedHashMap<>();
                for (PackageableElement e : parsed.elements()) {
                    if (e instanceof ClassDefinition cd) {
                        classes.put(cd.qualifiedName(), cd);
                    }
                }
            }
            for (DerivedPropertyDefinition dp : ad.derivedProperties()) {
                String owner = KnowledgeLayer.qualifiedPropertyOwner(ad, dp).orElseThrow(() -> MissProbe.neverFired("ModelNormalizer#1"));
                ClassDefinition cd = owner == null ? null : classes.get(owner);
                if (owner == null || (cd != null
                        && cd.derivedProperties().stream().noneMatch(d -> d == dp))) {
                    throw new IllegalStateException("normalize before the knowledge layer:"
                            + " association '" + ad.qualifiedName() + "' qualified property '"
                            + dp.name() + "' is not adopted by its owner"
                            + " (KnowledgeLayer.adoptAssociationQualifiedProperties)");
                }
            }
        }
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
    /**
     * E.5 — every store VIEW is a zero-arg relation function
     * {@code <db>$view$<name>(): Any[*]} whose single body expression is the
     * view's relation ({@code tableReference(root) -> [~filter] -> (groupBy |
     * project) -> [~distinct]}, join-navigating columns as join slots). The
     * engine's View IS a relational mapping specification planned as an
     * inline select; ours is the {@code ~func} relation-function shape the
     * mapping route already consumes. Eager like E.2–E.4; a view whose
     * translation walls is a WALLED element (the module's poison-don't-drop
     * contract: the failure fires at use), never a dropped one.
     * docs/VIEWS_COMPILED_ONCE_HOMEWORK_2026_09_22.md §7.
     */
    private static void liftViews(ParsedModel parsed, ModelBuilder model,
            List<FunctionDefinition> lifted,
            java.util.@com.legend.base.Nullable Map<String, String> wallSink) {
        for (PackageableElement el : parsed.elements()) {
            if (!(el instanceof DatabaseDefinition db)) {
                continue;
            }
            for (DatabaseDefinition.ViewDefinition v : db.views()) {
                liftView(db, v, v.name(), model, lifted, wallSink);
            }
            for (DatabaseDefinition.SchemaDefinition s : db.schemas()) {
                for (DatabaseDefinition.ViewDefinition v : s.views()) {
                    liftView(db, v, s.name() + "." + v.name(), model, lifted, wallSink);
                }
            }
        }
    }

    private static void liftView(DatabaseDefinition db, DatabaseDefinition.ViewDefinition view,
            String viewName, ModelBuilder model, List<FunctionDefinition> lifted,
            java.util.@com.legend.base.Nullable Map<String, String> wallSink) {
        String fqn = SynthFqn.view(db.qualifiedName(), viewName);
        ValueSpecification body;
        try {
            body = ViewRelation.viewRelationExpr(view, viewName, db.qualifiedName(), model, null);
        } catch (ModelException | com.legend.error.NotImplementedException e) {
            if (wallSink == null) {
                throw e;
            }
            wallSink.putIfAbsent(fqn, String.valueOf(e.getMessage()));
            return;
        }
        lifted.add(new FunctionDefinition(fqn, List.of(), List.of(), List.of(),
                new TypeExpression.NameRef(com.legend.compiler.element.type.PlatformTypes.ANY),
                Multiplicity.Concrete.ZERO_MANY, List.of(body), List.of(), List.of())
                .withSynthesizedFrom(new FunctionDefinition.Synthesized(
                        SynthHat.VIEW, db.qualifiedName(), viewName)));
    }

    private static void liftDerivedProperties(
            ParsedModel parsed, List<FunctionDefinition> lifted) {
        for (PackageableElement el : parsed.elements()) {
            if (el instanceof ClassDefinition cd && !cd.derivedProperties().isEmpty()) {
                for (DerivedPropertyDefinition dp : cd.derivedProperties()) {
                    // Only the sugar (inline) form lifts; a Door-4 function-ref
                    // binding is already realized by the user's function.
                    if (dp.realization() instanceof Realization.Inline) {
                        lifted.add(com.legend.compiler.DerivedProps.lift(cd, dp));
                    }
                }
            }
        }
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
                    if (c.message() != null) {
                        lifted.add(synthConstraintMsgFunction(cd, c, thisType));
                    }
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

    /** The {@code ~message} sibling: {@code <owner>$constraintMsg$<name>
     * (this:Owner[1]):String[1]} carrying the message expression. */
    private static FunctionDefinition synthConstraintMsgFunction(
            ClassDefinition cd, ConstraintDefinition c, TypeExpression thisType) {
        FunctionDefinition.ParameterDefinition self =
                new FunctionDefinition.ParameterDefinition(
                        "this", thisType, Multiplicity.Concrete.PURE_ONE);
        return new FunctionDefinition(
                SynthFqn.constraintMsg(cd.qualifiedName(), c.name()),
                cd.typeParams(),
                List.of(),
                List.of(self),
                new TypeExpression.NameRef(Pure.STRING.qualifiedName()),
                Multiplicity.Concrete.PURE_ONE,
                List.of(c.message()),
                List.of(), List.of())
                .withSynthesizedFrom(new FunctionDefinition.Synthesized(
                        SynthHat.CONSTRAINT_MSG, cd.qualifiedName(), c.name()));
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
                    throw new ModelException(
                            LegendCompileException.Phase.NORMALIZE,
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
                new TypeExpression.NameRef(com.legend.compiler.element.type.PlatformTypes.ANY),
                Multiplicity.Concrete.ZERO_MANY,
                body,
                List.of(), List.of())
                .withSynthesizedFrom(new FunctionDefinition.Synthesized(
                        SynthHat.QUERY, sd.qualifiedName(), "query"));
    }
}
