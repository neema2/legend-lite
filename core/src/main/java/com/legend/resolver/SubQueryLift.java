// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.resolver;

import com.legend.compiler.element.ModelContext;
import com.legend.compiler.element.type.ExprType;
import com.legend.compiler.element.type.Multiplicity;
import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.SpecCompiler;
import com.legend.compiler.spec.typed.TypedFrom;
import com.legend.compiler.spec.typed.TypedFuncCol;
import com.legend.compiler.spec.typed.TypedGraphFetch;
import com.legend.compiler.spec.typed.TypedLambda;
import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.compiler.spec.typed.TypedPackageableRef;
import com.legend.compiler.spec.typed.TypedProject;
import com.legend.compiler.spec.typed.TypedPropertyAccess;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.compiler.spec.typed.TypedVariable;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * IN-QUERY CLASS SUBQUERY (the XStore result-sourcing idiom): a
 * {@code <chain>.all()->toOne().prop} read INSIDE another query's lambda
 * resolves RECURSIVELY under the SAME execution context into a
 * {@code [0..1]}-stamped single-column projection — consumed by the
 * lowerer as a correlated-free SCALAR SUBQUERY (the filteredNavLeafRead
 * emission idiom). Engine parity: the router clusters the sub-expression
 * per store and feeds the value back; our one-SQL-plan doctrine inlines
 * it as a scalar subselect. Shapes beyond the scalar read keep their
 * loud walls.
 */
final class SubQueryLift {

    private SubQueryLift() {
    }

    /** Rewrite value-position class subqueries UNDER LAMBDAS in {@code n};
     * returns {@code n} unchanged when none match. */
    static TypedSpec lift(TypedSpec n, StoreResolver.Context context,
            ModelContext ctx, SpecCompiler specs,
            Map<String, TypedSpec> letBindings,
            java.util.function.Predicate<TypedSpec> storeRooted) {
        return walk(n, context, ctx, specs, letBindings, storeRooted, false);
    }

    private static final java.util.Set<String> EXISTENCE = java.util.Set.of(
            "meta::pure::functions::collection::isEmpty",
            "meta::pure::functions::collection::isNotEmpty");

    private static TypedSpec walk(TypedSpec n, StoreResolver.Context context,
            ModelContext ctx, SpecCompiler specs,
            Map<String, TypedSpec> letBindings,
            java.util.function.Predicate<TypedSpec> storeRooted, boolean underLambda) {
        if (n instanceof com.legend.compiler.spec.typed.TypedFrom) {
            // a from() carries its OWN mapping/runtime context: its
            // subqueries lift when the resolver reaches it (the TypedFrom
            // arm lifts under fromContext) — never under the enclosing
            // statement's context (batch 69c: the driver-route statement
            // `toCSV(from(...))` lifted the calendar read with no mapping)
            return n;
        }
        if (underLambda && n instanceof TypedPropertyAccess pa) {
            // a LET-BOUND instance read ($reportEndDate.day where
            // `let reportEndDate = FiscalCalendarDate.all()->filter(..)
            // ->toOne()` is the query lambda's own statement let — batch
            // 69c, datePeriods): the engine runs the let as its own
            // statement and inlines the value; our one plan reads it as
            // the same scalar subquery a written-out chain gets
            TypedSpec src = pa.source() instanceof TypedVariable lv
                    && letBindings.containsKey(lv.name())
                    ? letBindings.get(lv.name()) : pa.source();
            TypedSpec chain = peelScalarWraps(src);
            if (chain != null && StoreResolver.containsGetAll(chain)
                    && uncorrelated(chain,
                            new java.util.LinkedHashSet<>(
                                    letBindings.keySet()))) {
                return resolveScalarRead(chain, pa, context, ctx, specs,
                        letBindings);
            }
        }
        if (underLambda && n instanceof TypedNativeCall ec && ec.args().size() == 1
                && EXISTENCE.contains(ec.callee().qualifiedName())
                && Type.asClassType(ec.args().get(0).info().type()) instanceof Type.ClassType
                && storeRooted.test(ec.args().get(0))
                && uncorrelated(ec.args().get(0),
                        new java.util.LinkedHashSet<>(letBindings.keySet()))) {
            // an EXISTENCE test of a class query that reads no enclosing
            // row: [NOT] EXISTS over the query resolved as a relation (the
            // lowerer's relation-predicate family) — the engine's
            // isEmpty/isNotEmpty over the fetched instances
            return new TypedNativeCall(ec.callee(), List.of(resolveExistsRelation(
                    ec.args().get(0), context, ctx, specs, letBindings)), ec.info());
        }
        boolean nowUnder = underLambda || n instanceof TypedLambda;
        return SyntheticHeads.rebuildChildren(n,
                c -> walk(c, context, ctx, specs, letBindings, storeRooted, nowUnder));
    }

    /** {@code toOne/first/graphFetch} wrappers peel down to the
     * object-space chain; any other source shape returns null. */
    private static @com.legend.base.Nullable TypedSpec peelScalarWraps(TypedSpec s) {
        TypedSpec cur = s;
        boolean peeled = false;
        while (true) {
            if (cur instanceof TypedGraphFetch gf) {
                cur = gf.source();
                peeled = true;
                continue;
            }
            if (cur instanceof TypedNativeCall c && c.args().size() == 1
                    && (com.legend.builtin.Pure.isToOneCall(c.callee().qualifiedName())
                        || c.callee().qualifiedName().equals(
                            "meta::pure::functions::collection::first"))) {
                cur = c.args().get(0);
                peeled = true;
                continue;
            }
            break;
        }
        return peeled && Type.asClassType(cur.info().type()) instanceof Type.ClassType
                ? cur : null;
    }

    /** Resolve SELF-CONTAINED sub-queries under a DATA lambda: a
     * {@code TypedFrom}-wrapped chain that reads no lambda-bound
     * variable carries its own mapping+runtime and has no other
     * resolution owner (the assert-splice shape — forAll/contains over
     * a spliced execute() frame). Shadow-aware via {@link #uncorrelated}. */
    static TypedSpec resolveClosed(TypedSpec n, java.util.Set<String> bound,
            java.util.function.UnaryOperator<TypedSpec> resolveFrom) {
        if (n instanceof TypedLambda inner) {
            java.util.Set<String> b2 = new java.util.LinkedHashSet<>(bound);
            b2.addAll(inner.parameters());
            return inner.mapChildren(c -> resolveClosed(c, b2, resolveFrom));
        }
        if (n instanceof com.legend.compiler.spec.typed.TypedFrom
                && uncorrelated(n, bound)) {
            return resolveFrom.apply(n);
        }
        return n.mapChildren(c -> resolveClosed(c, bound, resolveFrom));
    }

    /** UNCORRELATED check, SHADOW-AWARE: every variable read is either
     * bound by a lambda INSIDE the chain (a filter predicate's own
     * parameter — {@code FiscalCalendarDate.all()->filter(d|$d.date ==
     * $endDate)->toOne()}) or a top-level LET name the recursive
     * resolution serves through {@code letBindings}. Any other read means
     * the chain reads the enclosing row — a correlated class subquery,
     * its own rung, loud downstream. */
    static boolean uncorrelated(TypedSpec n,
            java.util.Set<String> bound) {
        if (n instanceof TypedVariable v) {
            return bound.contains(v.name());
        }
        if (n instanceof TypedLambda l) {
            java.util.Set<String> inner =
                    new java.util.LinkedHashSet<>(bound);
            inner.addAll(l.parameters());
            for (TypedSpec c : l.children()) {
                if (!uncorrelated(c, inner)) {
                    return false;
                }
            }
            return true;
        }
        for (TypedSpec c : n.children()) {
            if (!uncorrelated(c, bound)) {
                return false;
            }
        }
        return true;
    }

    /** getAllForEachDate's DATES argument as a RESOLVED one-column
     *  relation ({@code Calendar.all()->filter(...).calendarDate} — the
     *  property-terminal chain becomes a project, resolved through a
     *  fresh resolver exactly like the scalar-subquery path, but KEEPS
     *  its many-row relation identity: it is the for-each FROM root). */
    static TypedSpec resolveDatesRelation(TypedSpec datesArg,
            StoreResolver.Context context, ModelContext ctx,
            SpecCompiler specs, Map<String, TypedSpec> letBindings) {
        if (!(datesArg instanceof TypedPropertyAccess pa)
                || !(Type.asClassType(pa.source().info().type()) instanceof Type.ClassType ct)) {
            throw new com.legend.error.NotImplementedException(
                    "getAllForEachDate dates argument shape "
                    + datesArg.getClass().getSimpleName()
                    + " is not supported yet (property-terminal class"
                    + " chains only)");
        }
        var one = Multiplicity.Bounded.ONE;
        String v = "_fed";
        TypedSpec read = new TypedPropertyAccess(
                new TypedVariable(v, new ExprType(ct, one)),
                pa.property(), pa.info());
        TypedLambda mapper = new TypedLambda(List.of(v), List.of(read),
                new ExprType(new Type.FunctionType(
                        List.of(new Type.Param(ct, one)),
                        new Type.Param(read.info().type(),
                                read.info().multiplicity())), one));
        Type.RelationType row = new Type.RelationType(List.of(
                new Type.Column(pa.property(), read.info().type(), one)));
        TypedProject proj = new TypedProject(pa.source(),
                List.of(new TypedFuncCol(pa.property(), mapper)),
                new ExprType(Type.relation(row), Multiplicity.Bounded.ZERO_MANY));
        Optional<TypedPackageableRef> m = context.explicitMapping() == null
                ? Optional.empty()
                : Optional.of(new TypedPackageableRef(
                        context.explicitMapping(), proj.info()));
        Optional<TypedPackageableRef> r = context.runtimeFqn() == null
                ? Optional.empty()
                : Optional.of(new TypedPackageableRef(
                        context.runtimeFqn(), proj.info()));
        TypedSpec wrapped = new TypedFrom(proj, com.legend.compiler.spec.typed.ExecutionContext.of(
                m, r, context.chainMappings(), context.jsonSources()), proj.info());
        TypedSpec resolved = new StoreResolver(ctx, specs)
                .withLetBindings(letBindings)
                .resolve(List.of(wrapped), null).get(0);
        while (resolved instanceof TypedFrom fr) {
            resolved = fr.source();
        }
        return resolved;
    }

    /** A class query as the relation an existence test reads: one constant
     * column per fetched instance, resolved through a fresh resolver under
     * the same context (the scalar-read path's recursion). */
    private static TypedSpec resolveExistsRelation(TypedSpec chain,
            StoreResolver.Context context, ModelContext ctx, SpecCompiler specs,
            Map<String, TypedSpec> letBindings) {
        Type ct = chain.info().type();
        var one = Multiplicity.Bounded.ONE;
        String v = "_ex";
        TypedSpec mark = new com.legend.compiler.spec.typed.TypedCInteger(1L,
                new ExprType(com.legend.compiler.element.type.Type.Primitive.INTEGER, one));
        TypedLambda mapper = new TypedLambda(List.of(v), List.of(mark),
                new ExprType(new Type.FunctionType(
                        List.of(new Type.Param(ct, one)),
                        new Type.Param(mark.info().type(), one)), one));
        Type.RelationType row = new Type.RelationType(List.of(
                new Type.Column("_ex", mark.info().type(), one)));
        TypedProject proj = new TypedProject(chain,
                List.of(new TypedFuncCol("_ex", mapper)),
                new ExprType(Type.relation(row), Multiplicity.Bounded.ZERO_MANY));
        Optional<TypedPackageableRef> m = context.explicitMapping() == null
                ? Optional.empty()
                : Optional.of(new TypedPackageableRef(context.explicitMapping(), proj.info()));
        Optional<TypedPackageableRef> r = context.runtimeFqn() == null
                ? Optional.empty()
                : Optional.of(new TypedPackageableRef(context.runtimeFqn(), proj.info()));
        TypedSpec wrapped = new TypedFrom(proj, com.legend.compiler.spec.typed.ExecutionContext.of(
                m, r, context.chainMappings(), context.jsonSources()), proj.info());
        TypedSpec resolved = new StoreResolver(ctx, specs)
                .withLetBindings(letBindings)
                .resolve(List.of(wrapped), null).get(0);
        while (resolved instanceof TypedFrom fr) {
            resolved = fr.source();
        }
        return resolved;
    }

    private static TypedSpec resolveScalarRead(TypedSpec chain,
            TypedPropertyAccess pa, StoreResolver.Context context,
            ModelContext ctx, SpecCompiler specs,
            Map<String, TypedSpec> letBindings) {
        Type.ClassType ct = (Type.ClassType) chain.info().type();
        var one = Multiplicity.Bounded.ONE;
        var optional = Multiplicity.Bounded.ZERO_ONE;
        String v = "_sq";
        TypedSpec read = new TypedPropertyAccess(
                new TypedVariable(v, new ExprType(ct, one)),
                pa.property(), pa.info());
        TypedLambda mapper = new TypedLambda(List.of(v), List.of(read),
                new ExprType(new Type.FunctionType(
                        List.of(new Type.Param(ct, one)),
                        new Type.Param(read.info().type(),
                                read.info().multiplicity())), one));
        Type.RelationType row = new Type.RelationType(List.of(
                new Type.Column(pa.property(), read.info().type(),
                        read.info().multiplicity())));
        TypedProject proj = new TypedProject(chain,
                List.of(new TypedFuncCol(pa.property(), mapper)),
                new ExprType(Type.relation(row), optional));
        // recursion through a FRESH resolver (shared per-chain state —
        // temporal frames, synthetic heads — must not leak); the TypedFrom
        // wrapper threads the FULL context including chain mappings
        Optional<TypedPackageableRef> m = context.explicitMapping() == null
                ? Optional.empty()
                : Optional.of(new TypedPackageableRef(
                        context.explicitMapping(), proj.info()));
        Optional<TypedPackageableRef> r = context.runtimeFqn() == null
                ? Optional.empty()
                : Optional.of(new TypedPackageableRef(
                        context.runtimeFqn(), proj.info()));
        TypedSpec wrapped = new TypedFrom(proj, com.legend.compiler.spec.typed.ExecutionContext.of(
                m, r, context.chainMappings(), context.jsonSources()), proj.info());
        TypedSpec resolved = new StoreResolver(ctx, specs)
                .withLetBindings(letBindings)
                .resolve(List.of(wrapped), null).get(0);
        while (resolved instanceof TypedFrom fr) {
            resolved = fr.source();
        }
        // the [0..1] stamp IS the scalar-subquery contract
        // (filteredNavLeafRead idiom): value = the single column
        if (resolved instanceof TypedProject rp) {
            return new TypedProject(rp.source(), rp.columns(),
                    new ExprType(rp.info().type(), optional));
        }
        return resolved;
    }
}
