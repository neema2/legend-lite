// SPDX-License-Identifier: Apache-2.0

package com.legend.resolver;

import com.legend.compiler.element.ModelContext;
import com.legend.compiler.element.TypedFunction;
import com.legend.compiler.element.type.ExprType;
import com.legend.compiler.element.type.Multiplicity;
import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.typed.TypedCString;
import com.legend.compiler.spec.typed.TypedFilter;
import com.legend.compiler.spec.typed.TypedGetAll;
import com.legend.compiler.spec.typed.TypedLambda;
import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.compiler.spec.typed.TypedPackageableRef;
import com.legend.compiler.spec.typed.TypedPropertyAccess;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.compiler.spec.typed.TypedVariable;
import com.legend.error.MappingResolutionException;
import com.legend.error.NotImplementedException;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Supplier;

/**
 * Metamodel-as-relations, resolver side (step 3, 2026-09-02).
 *
 * <p><b>D3 — ELEMENT REFERENCE = ROW.</b> A reference to a registry-tracked,
 * system-mapped element ({@code B1Mapping}, typed as its metaclass) IS that
 * metaclass's extent restricted to the element's primary key (the D2
 * identity: the FQN), so navigations off a named element are ordinary
 * store reads. The restriction is an OBJECT-SPACE filter over the
 * primary-key pseudo-binding ({@code ClassMapping.primaryKeyBinding}), so
 * it rides every position a class filter rides. A bare reference (an
 * argument, a let) stays a value.
 *
 * <p><b>Chain-position casts.</b> {@code ->cast(@Sub)} over a chain is a
 * re-typing when the mapping PROVES every row conforms: the navigation is
 * ROUTED to one member set whose class conforms ({@code prop[setId]: @J}),
 * or the class's Operation extent (a union's declared members, an
 * inheritance op's mapped subclasses) is all conforming. Anything partial
 * stays loud (the witness-gated chain cast is not built).
 */
final class ElementReferences {

    private final ModelContext ctx;
    private final ClassSources sources;
    private final BiFunction<StoreResolver.Context, String, String> dispatch;
    private final Supplier<@com.legend.base.Nullable TypedFunction> equalCallee;

    ElementReferences(ModelContext ctx, ClassSources sources,
            BiFunction<StoreResolver.Context, String, String> dispatch,
            Supplier<@com.legend.base.Nullable TypedFunction> equalCallee) {
        this.ctx = ctx;
        this.sources = sources;
        this.dispatch = dispatch;
        this.equalCallee = equalCallee;
    }

    /** The metaclass FQN when {@code pr} references a tracked, system-mapped
     * element (a seeded extent AND a row: a Database reference is a value
     * today — no rows); else null. */
    @com.legend.base.Nullable String trackedElementClass(TypedPackageableRef pr) {
        // BARE metaclass type only: a CLASS reference is typed Class<X>
        // (Typer.classReference) and stays a VALUE at a chain root
        // (PCT letFn: TestClass->removeDuplicates() returns the element,
        // never its Class row's key); identity equality reads its Class
        // row explicitly (ChainNormalizer.identityEquality). The one site
        // where Type.asClassType's raw reading does not apply — receipt.
        return pr.info().type() instanceof Type.ClassType ct
                && ctx.tracksClassifier(ct.fqn())
                && sources.binds(com.legend.builtin.SystemMetamodel.MAPPING_FQN,
                        ct.fqn()) ? ct.fqn() : null;
    }

    /** The element's row as an object-space chain head. A composite key
     * has no element spelling — loud. */
    TypedSpec elementRow(TypedPackageableRef pr, String classFqn,
            StoreResolver.Context context, Supplier<String> freshVar) {
        return elementRowByKey(pr.fullPath(), classFqn, context, freshVar);
    }

    /** {@code classFqn}'s extent restricted to the row keyed {@code key}
     * (an element's path, a constructed instance's content id). */
    TypedSpec elementRowByKey(String key, String classFqn,
            StoreResolver.Context context, Supplier<String> freshVar) {
        String mappingFqn = dispatch.apply(context, classFqn);
        List<String> pk = new ArrayList<>();
        var md = ctx.findMapping(mappingFqn).orElse(null);
        if (md != null) {
            for (var cb : md.classBindingsWithIncludes(ctx::findMapping)) {
                if (cb.classFqn().equals(classFqn)) {
                    pk = cb.primaryKeyColumns();
                    break;
                }
            }
        }
        if (pk.size() != 1) {
            throw new NotImplementedException("element reference '"
                    + key + "': the metaclass row of " + classFqn
                    + " keys on " + pk + " — one FQN key column is required");
        }
        var one = Multiplicity.Bounded.ONE;
        Type.ClassType ct = new Type.ClassType(classFqn);
        TypedGetAll all = new TypedGetAll(classFqn, List.of(), false, false,
                new ExprType(ct, Multiplicity.Bounded.ZERO_MANY));
        String v = freshVar.get();
        TypedSpec keyRead = new TypedPropertyAccess(
                new TypedVariable(v, new ExprType(ct, one)),
                com.legend.model.ClassMapping.primaryKeyBinding(pk.get(0)),
                new ExprType(Type.Primitive.STRING, one));
        TypedSpec pred = new TypedNativeCall(java.util.Objects.requireNonNull(
                equalCallee.get(), "resolver bug: no equal registration"),
                List.of(keyRead, new TypedCString(key,
                        new ExprType(Type.Primitive.STRING, one))),
                new ExprType(Type.Primitive.BOOLEAN, one));
        var fn = new Type.FunctionType(List.of(new Type.Param(ct, one)),
                new Type.Param(Type.Primitive.BOOLEAN, one));
        return new TypedFilter(all, new TypedLambda(List.of(v), List.of(pred),
                new ExprType(fn, one)), all.info());
    }

    /** A navigation ROUTED to one member set lands on that set's class:
     * total when it conforms to {@code target}. False when the source is
     * not a routed class-typed hop (the extent rule decides then). */
    boolean castTotalByRoute(StoreResolver.Context context, TypedSpec source,
            String target) {
        String oc = source instanceof TypedPropertyAccess hp
                ? Type.classFqn(hp.source().info().type()) : null;
        if (!(source instanceof TypedPropertyAccess hp) || oc == null) {
            return false;
        }
        String mappingFqn;
        try {
            mappingFqn = dispatch.apply(context, oc);
        } catch (MappingResolutionException e) {
            return false;
        }
        String routed = ctx.routedTargetClass(mappingFqn, oc, hp.property());
        return routed != null && ctx.isSubtype(routed, target);
    }

    /** Whether every row of {@code srcClass}'s extent in the context's
     * mapping conforms to {@code target}: a UNION op's declared members,
     * else an INHERITANCE op's mapped subclasses (includes closed). */
    boolean totalMembershipCast(StoreResolver.Context context, String srcClass,
            String target) {
        if (ctx.isSubtype(srcClass, target)) {
            return true;   // an upcast: every row conforms by declaration
        }
        // a DOWNCAST or a CROSS-cast (pure: any class the run-time type
        // conforms to — SetImplementation->cast(@PropertyMappingsImplementation),
        // siblings under PropertyOwnerImplementation that
        // InstanceSetImplementation joins; mapping leg B): the mapped
        // members decide, exactly as for a downcast
        String mappingFqn;
        try {
            mappingFqn = dispatch.apply(context, srcClass);
        } catch (MappingResolutionException e) {
            return false;
        }
        List<String> unionMembers = ctx.unionMemberClasses(mappingFqn, srcClass);
        if (unionMembers != null) {
            for (String m : unionMembers) {
                if (!ctx.isSubtype(m, target)) {
                    return false;
                }
            }
            return !unionMembers.isEmpty();
        }
        var md = ctx.findMapping(mappingFqn).orElse(null);
        if (md == null) {
            return false;
        }
        boolean any = false;
        for (var cb : md.classBindingsWithIncludes(ctx::findMapping)) {
            if (cb.classFqn().equals(srcClass)
                    || !ctx.isSubtype(cb.classFqn(), srcClass)) {
                continue;
            }
            any = true;
            if (!ctx.isSubtype(cb.classFqn(), target)) {
                return false;
            }
        }
        return any;
    }
    /** "intrinsic" = bound in the SYSTEM mapping (the registry's extents
     * are a subset: every seeded metaclass is mapped there, and so are
     * the metaclasses reached by navigation — SetImplementation, Table —
     * whose rows the seed derives). */
    boolean intrinsicClass(String classFqn) {
        if (ctx.tracksClassifier(classFqn) || sources.binds(
                com.legend.builtin.SystemMetamodel.MAPPING_FQN, classFqn)) {
            return true;
        }
        // an ABSTRACT metaclass between an inheritance op and its bound
        // member (PropertyMappingsImplementation): its extent is its
        // bound subclasses' — the same store
        var md = ctx.findMapping(com.legend.builtin.SystemMetamodel.MAPPING_FQN)
                .orElse(null);
        if (md == null || ctx.findClass(classFqn).isEmpty()) {
            return false;
        }
        for (var cb : md.classBindings()) {
            if (!cb.classFqn().equals(classFqn)
                    && ctx.isSubtype(cb.classFqn(), classFqn)) {
                return true;
            }
        }
        return false;
    }


    static final String TABLE_METACLASS = "meta::relational::metamodel::relation::Table";

    /** {@code db->schema('S')->table('T')} (StoreElementIdentity): the row
     * id of that table in the system store; null otherwise. */
    @com.legend.base.Nullable String storeTableKey(TypedSpec n) {
        var r = com.legend.compiler.spec.typed.StoreElementIdentity.tableRef(n,
                java.util.function.UnaryOperator.identity());
        if (r == null) {
            return null;
        }
        // the Table row is keyed by its DECLARING database: a database
        // reaches an included database's tables through its includes
        // (functions.pure:227 — schema() concatenates the includes' schemas)
        String declaring = declaringDatabase(r.dbFqn(), r.schema(), r.table(),
                new java.util.HashSet<>());
        return com.legend.compiler.element.RelationalOpRows.tableId(
                declaring == null ? r.dbFqn() : declaring, r.schema(), r.table());
    }

    private @com.legend.base.Nullable String declaringDatabase(String dbFqn, String schema,
            String table, java.util.Set<String> seen) {
        if (!seen.add(dbFqn)) {
            return null;
        }
        var db = ctx.findDatabase(dbFqn).orElse(null);
        if (db == null) {
            return null;
        }
        if ("default".equals(schema)
                && db.tables().stream().anyMatch(t -> t.name().equals(table))) {
            return dbFqn;
        }
        for (var s : db.schemas()) {
            if (s.name().equals(schema)
                    && s.tables().stream().anyMatch(t -> t.name().equals(table))) {
                return dbFqn;
            }
        }
        for (String inc : db.includes()) {
            String found = declaringDatabase(inc, schema, table, seen);
            if (found != null) {
                return found;
            }
        }
        return null;
    }

    /** A chain ROOT the store carries as rows: the re-rooted head and the
     * context it resolves under. */
    record RootRow(TypedSpec row, StoreResolver.Context context) {
    }

    /**
     * The row-root arms of the chain walk (StoreResolver.collectOpChain):
     * an ELEMENT REFERENCE (D3 — the metaclass extent keyed by FQN), a
     * PLAN HANDLE (PlanRows under the handle's content id), a FUNCTION
     * VALUE's body ($f.expressionSequence over a lambda — FunctionBodyRows
     * under the lambda's scope, registered on first meeting) and a
     * CONSTRUCTED instance (the tree's scope). Null when {@code cur} is
     * none of them.
     */
    @com.legend.base.Nullable RootRow rowRoot(TypedSpec cur, StoreResolver.Context context,
            ConstructedInstances constructed,
            java.util.function.Predicate<TypedNativeCall> planHandle,
            Supplier<String> freshVar) {
        if (cur instanceof TypedPackageableRef pr && trackedElementClass(pr) != null) {
            return new RootRow(elementRow(pr, java.util.Objects.requireNonNull(
                    trackedElementClass(pr)), context, freshVar), context);
        }
        // a STORE TABLE named by its accessors — db->schema('S')->table('T')
        // (through toOne peels) — is an element reference too (D2: the
        // Table row keyed RelationalOpRows.tableId; the accessor calls are
        // the identity spelling the inliner keeps closed)
        String tableKey = storeTableKey(cur);
        if (tableKey != null) {
            return new RootRow(elementRowByKey(tableKey,
                    TABLE_METACLASS, context, freshVar), context);
        }
        if (cur instanceof TypedNativeCall pn && planHandle.test(pn)) {
            String scope = com.legend.plan.PlanRows.scopeId(pn);
            StoreResolver.Context inner = context.withConstructedScope(scope);
            return new RootRow(elementRowByKey(scope, java.util.Objects.requireNonNull(
                    com.legend.compiler.element.type.PlatformTypes.handleRowClass(pn.callee().qualifiedName(), pn.callee().returnType())), inner, freshVar), inner);
        }
        if (cur instanceof TypedLambda flam) {
            String scope = FunctionBodyRows.scopeId(flam);
            if (!constructed.has(scope)) {
                constructed.register(scope, FunctionBodyRows.rows(scope, flam, ctx));
            }
            StoreResolver.Context inner = context.withConstructedScope(scope);
            return new RootRow(elementRowByKey(scope,
                    "meta::pure::metamodel::function::FunctionDefinition", inner,
                    freshVar), inner);
        }
        if (cur instanceof com.legend.compiler.spec.typed.TypedNewInstance cni
                && constructed.rowId(cni) != null) {
            String scope = java.util.Objects.requireNonNull(constructed.rowId(cni));
            StoreResolver.Context inner = context.withConstructedScope(scope);
            return new RootRow(elementRowByKey(scope, cni.classFqn(), inner, freshVar),
                    inner);
        }
        return null;
    }
}
